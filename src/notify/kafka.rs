// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Notifications published to an Apache Kafka topic.
//!
//! The payload is the same [`Notification`] JSON that the file and Pulsar
//! notifiers produce; only the transport differs.
//!
//! ## Partitioning
//!
//! A record goes to the partition [`TopicPartitions::for_height`] assigns to
//! `heightStart` — the **first height of the notified range**, not necessarily
//! a block of its own. For `stream` and `archive` runs a notification covers
//! one block, so all the events about that block share a partition and stay in
//! order relative to each other. A `compact` run notifies a whole range at
//! once and is placed by the range start instead, so its notification is
//! ordered against the other notifications of that range start, not against
//! the per-block ones it supersedes.
//!
//! ## Failures
//!
//! A notification that never reaches the topic leaves the archive and its
//! consumers permanently out of sync — the data is in the archive but nothing
//! downstream knows about it. So, unlike the Pulsar notifier, a failed send is
//! not shrugged off: the first one signals shutdown and is reported by
//! [`Notifier::flush`], which fails the run.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rskafka::client::Client;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::record::Record;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use crate::kafka::{BootstrapBrokers, TopicPartitions};
use crate::notify::{Notification, Notifier};

/// How long [`Notifier::flush`] waits for the queued notifications once the
/// run is over. Only a sender that outlived the command can hold it up, so
/// this is a backstop against hanging the process at exit, not a normal wait.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

///
/// Sends [`Notification`]s to a Kafka topic.
pub struct KafkaNotifier {
    client: Arc<Client>,
    topic: TopicPartitions,
    messages_sent: Arc<AtomicUsize>,
    /// First delivery failure seen by the background task, reported by
    /// [`Notifier::flush`].
    failure: Arc<Mutex<Option<anyhow::Error>>>,
    /// The background task, awaited by [`Notifier::flush`].
    task: Mutex<Option<JoinHandle<()>>>,
}

impl KafkaNotifier {
    ///
    /// Connect to the cluster and resolve the topic partitions. Both are
    /// verified upfront so a bad configuration fails the run at startup
    /// instead of dropping notifications later.
    pub async fn new(brokers: BootstrapBrokers, topic: String) -> Result<Self> {
        let client = brokers.connect().await?;
        let topic = TopicPartitions::discover(&client, &topic).await?;
        Ok(Self {
            client: Arc::new(client),
            topic,
            messages_sent: Arc::new(AtomicUsize::new(0)),
            failure: Arc::new(Mutex::new(None)),
            task: Mutex::new(None),
        })
    }

    pub fn get_messages_sent(&self) -> usize {
        self.messages_sent.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait]
impl Notifier for KafkaNotifier {
    fn start(&self) -> Sender<Notification> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let client = self.client.clone();
        let topic = self.topic.clone();
        let counter = self.messages_sent.clone();
        let failure = self.failure.clone();
        let task = tokio::spawn(async move {
            let mut producers: HashMap<i32, Arc<PartitionClient>> = HashMap::new();

            while let Some(notification) = rx.recv().await {
                match publish(&client, &topic, &mut producers, &notification).await {
                    Ok(_) => {
                        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::error!("Failed to send notification to Kafka: {:#}", e);
                        let mut failure = failure.lock().unwrap();
                        if failure.is_none() {
                            *failure = Some(e);
                        }
                        crate::global::get_shutdown().signal();
                        // dropping the receiver makes the archiver's pending
                        // sends fail immediately instead of queueing for a
                        // topic that isn't taking them
                        return;
                    }
                }
            }
        });
        *self.task.lock().unwrap() = Some(task);
        tx
    }

    async fn flush(&self) -> Result<()> {
        let task = self.task.lock().unwrap().take();
        if let Some(task) = task {
            match tokio::time::timeout(FLUSH_TIMEOUT, task).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(anyhow!("Kafka notifier stopped unexpectedly: {}", e)),
                Err(_) => tracing::warn!(
                    "Gave up waiting for the pending Kafka notifications after {:?}",
                    FLUSH_TIMEOUT
                ),
            }
        }
        let failure = self.failure.lock().unwrap().take();
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

///
/// Produce one notification to the partition that carries its range.
async fn publish(
    client: &Client,
    topic: &TopicPartitions,
    producers: &mut HashMap<i32, Arc<PartitionClient>>,
    notification: &Notification,
) -> Result<()> {
    let record = Record::try_from(notification)?;
    let partition = topic.for_height(notification.height_start);
    let producer = producer_for(client, topic, partition, producers).await?;
    // a notification is a single small JSON, compressing it costs more than it
    // saves
    producer
        .produce(vec![record], Compression::NoCompression)
        .await
        .map_err(|e| anyhow!("Rejected by topic {} partition {}: {}", topic.topic(), partition, e))?;
    Ok(())
}

///
/// Producer for the given partition, reusing the one opened before if any.
/// A [`PartitionClient`] is a dedicated broker connection, so it's kept for the
/// whole run rather than rebuilt per notification.
async fn producer_for(
    client: &Client,
    topic: &TopicPartitions,
    partition: i32,
    producers: &mut HashMap<i32, Arc<PartitionClient>>,
) -> Result<Arc<PartitionClient>> {
    if let Some(producer) = producers.get(&partition) {
        return Ok(producer.clone());
    }
    // the topic existed when the notifier started, so if it's unknown now it
    // was deleted under us — report that instead of waiting for it to reappear
    let producer = client
        .partition_client(topic.topic(), partition, UnknownTopicHandling::Error)
        .await
        .map_err(|e| anyhow!("Failed to open topic {} partition {}: {}", topic.topic(), partition, e))?;
    let producer = Arc::new(producer);
    producers.insert(partition, producer.clone());
    Ok(producer)
}

impl TryFrom<&Notification> for Record {
    type Error = anyhow::Error;

    fn try_from(notification: &Notification) -> Result<Self> {
        let payload = serde_json::to_vec(notification)
            .map_err(|e| anyhow!("Failed to serialize notification: {}", e))?;
        Ok(Record {
            // no key: the partition is chosen by height, and one height
            // produces several notifications — so a key could only mislead a
            // compacted topic into dropping all but the last of them
            key: None,
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: notification.ts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use crate::archiver::datakind::DataKind;
    use crate::notify::{Location, RunMode};
    use crate::testing::start_kafka;

    fn notification() -> Notification {
        Notification {
            version: Notification::version(),
            ts: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            blockchain: "ETHEREUM".to_string(),
            file_type: DataKind::Blocks,
            run: RunMode::Archive,
            height_start: 100,
            height_end: 120,
            maturity: None,
            location: Location::File {
                url: "file://archive/range-100_120.blocks.avro".to_string(),
            },
        }
    }

    #[test]
    fn record_keeps_json_payload() {
        let notification = notification();
        let record = Record::try_from(&notification).unwrap();

        assert_eq!(record.key, None);
        assert_eq!(record.timestamp, notification.ts);
        assert!(record.headers.is_empty());

        let value = String::from_utf8(record.value.unwrap()).unwrap();
        assert_eq!(value, serde_json::to_string(&notification).unwrap());
    }

    #[tokio::test]
    async fn sends_to_topic() {
        // more than one partition, so the height-to-partition mapping matters
        let (container, brokers) = start_kafka(3, true).await;
        let topic_name = "test-notifications";
        // the topic doesn't exist yet: the broker auto-creates it while
        // KafkaNotifier resolves the partitions
        let notifier = KafkaNotifier::new(brokers.clone(), topic_name.to_string()).await.unwrap();
        let sender = notifier.start();

        let sent = notification();
        sender.send(sent.clone()).await.unwrap();

        // dropping the only sender ends the background loop, so the flush both
        // waits for the delivery and reports whether it worked
        drop(sender);
        notifier.flush().await.unwrap();
        assert_eq!(notifier.get_messages_sent(), 1);

        let client = brokers.connect().await.unwrap();
        let topic = TopicPartitions::discover(&client, topic_name).await.unwrap();
        assert_eq!(topic.count(), 3);

        let consumer = client
            .partition_client(topic_name, topic.for_height(sent.height_start), UnknownTopicHandling::Error)
            .await
            .unwrap();
        let (records, _watermark) = consumer.fetch_records(0, 1..1_000_000, 5_000).await.unwrap();

        assert_eq!(records.len(), 1);
        let received: Notification = serde_json::from_slice(records[0].record.value.as_ref().unwrap()).unwrap();
        assert_eq!(received.height_start, sent.height_start);
        assert_eq!(received.height_end, sent.height_end);
        assert_eq!(received.blockchain, sent.blockchain);
        assert_eq!(received.location, sent.location);
        assert_eq!(records[0].record.timestamp, sent.ts);

        container.stop().await.unwrap();
    }

    /// With `auto.create.topics.enable` off, the archive creates the topic
    /// itself — and leaves its shape to the broker rather than naming a
    /// partition count of its own.
    #[tokio::test]
    async fn creates_a_missing_topic_with_broker_defaults() {
        // not the default of 1, so a partition count we picked ourselves
        // would show up below
        let (container, brokers) = start_kafka(2, false).await;
        let topic_name = "created-on-demand";
        KafkaNotifier::new(brokers.clone(), topic_name.to_string())
            .await
            .expect("Expected the missing topic to be created");

        let client = brokers.connect().await.unwrap();
        let topic = TopicPartitions::discover(&client, topic_name).await.unwrap();
        assert_eq!(topic.count(), 2, "topic should take the broker's num.partitions");

        container.stop().await.unwrap();
    }
}
