// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Apache Kafka plumbing shared by the notification producer and the
//! streaming target: how a URL becomes a connected client, and how a block
//! height picks the partition it is published to.
//!
//! rskafka ships no producer-side partitioner — a
//! [`PartitionClient`](rskafka::client::partition::PartitionClient) is bound to
//! a single partition and the caller decides which one. Both Kafka users need
//! the same rule, so it lives here.

use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Result};
use rskafka::BackoffConfig;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::{Client, ClientBuilder};

/// Reported to the broker, so a Kafka operator can tell our connections apart.
const CLIENT_ID: &str = "dshackle-archive";

/// Bounds the retries rskafka performs inside every broker call — connecting,
/// reading metadata, producing.
///
/// Without a deadline it retries connection and IO errors forever, so a broker
/// that goes away would leave the archive waiting silently instead of
/// reporting the failure. Generous enough to ride out a leader election, short
/// enough that an unreachable broker is reported rather than waited on.
const BROKER_DEADLINE: Duration = Duration::from_secs(30);

/// A broker that auto-creates topics does so only when it sees a metadata
/// request naming the topic, and answers that first request with "unknown
/// topic". Retrying the lookup covers that round trip plus the metadata
/// propagation that follows it on a multi-broker cluster.
const TOPIC_LOOKUP_ATTEMPTS: usize = 5;
const TOPIC_LOOKUP_DELAY: Duration = Duration::from_secs(1);

///
/// Addresses of the Kafka bootstrap brokers, as provided on the command line.
///
/// Accepts both a plain address list (`host:9092,host2:9092`) and the
/// scheme-prefixed form used by `--stream.url` (`kafka://host:9092`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapBrokers(Vec<String>);

impl FromStr for BootstrapBrokers {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        // per entry, not once for the whole string: a list is as likely to be
        // written `kafka://one:9092,kafka://two:9092` as with a single prefix
        let addresses: Vec<String> = value
            .split(',')
            .map(str::trim)
            .map(|address| address.strip_prefix("kafka://").unwrap_or(address))
            .filter(|address| !address.is_empty())
            .map(String::from)
            .collect();
        if addresses.is_empty() {
            return Err(anyhow!("No Kafka broker address specified: {}", value));
        }
        Ok(Self(addresses))
    }
}

impl Display for BootstrapBrokers {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join(","))
    }
}

impl BootstrapBrokers {
    /// Connect to the cluster. Fails when none of the bootstrap brokers can be
    /// reached within [`BROKER_DEADLINE`] — a misconfigured broker must stop
    /// the run, not degrade it.
    pub async fn connect(&self) -> Result<Client> {
        ClientBuilder::new(self.0.clone())
            .client_id(CLIENT_ID)
            .backoff_config(BackoffConfig {
                deadline: Some(BROKER_DEADLINE),
                ..BackoffConfig::default()
            })
            .build()
            .await
            .map_err(|e| anyhow!("Failed to connect to Kafka at {}: {}", self, e))
    }
}

///
/// Partitions of a single topic, and the rule that assigns a block height to
/// one of them.
#[derive(Clone, Debug)]
pub struct TopicPartitions {
    topic: String,
    /// Partition ids as reported by the broker, ascending. Never empty.
    partitions: Vec<i32>,
}

impl TopicPartitions {
    ///
    /// Read the partitions of `topic` from the cluster metadata, giving a
    /// broker with `auto.create.topics.enable` a chance to create the topic
    /// first.
    ///
    /// Errors when the topic still doesn't exist afterwards: Kafka clusters
    /// commonly have auto-creation disabled, and silently producing nowhere
    /// would be worse than not starting.
    pub async fn discover(client: &Client, topic: &str) -> Result<Self> {
        let mut last_error = None;
        for attempt in 0..TOPIC_LOOKUP_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(TOPIC_LOOKUP_DELAY).await;
            }
            // asking for a partition client is what puts the topic name into a
            // metadata request; it's also the only call that reports why the
            // topic is unusable, which `list_topics` below cannot tell apart
            // from a topic that simply isn't there
            if let Err(e) = client
                .partition_client(topic, 0, UnknownTopicHandling::Error)
                .await
            {
                last_error = Some(e);
            }

            let known = client
                .list_topics()
                .await
                .map_err(|e| anyhow!("Failed to read Kafka topics: {}", e))?;
            let found = known
                .into_iter()
                .find(|known| known.name == topic)
                .filter(|known| !known.partitions.is_empty());
            if let Some(found) = found {
                return Ok(Self {
                    topic: found.name,
                    partitions: found.partitions.into_iter().collect(),
                });
            }
        }
        Err(match last_error {
            Some(e) => anyhow!(
                "Kafka topic {} is not available: {}. Create it, or enable topic auto-creation on the broker",
                topic, e
            ),
            None => anyhow!(
                "Kafka topic {} doesn't exist. Create it, or enable topic auto-creation on the broker",
                topic
            ),
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn count(&self) -> usize {
        self.partitions.len()
    }

    ///
    /// Partition that carries the data of the given block height.
    ///
    /// Kafka orders records within a partition only, so every message of one
    /// block — and any later re-org replacement for it — must land on the same
    /// partition. Selecting by height (instead of hashing the record key, which
    /// is what a standard Kafka producer would do) also spreads consecutive
    /// blocks evenly, and lets a consumer compute where a height went.
    pub fn for_height(&self, height: u64) -> i32 {
        self.partitions[(height % self.partitions.len() as u64) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_address() {
        let brokers: BootstrapBrokers = "localhost:9092".parse().unwrap();
        assert_eq!(brokers, BootstrapBrokers(vec!["localhost:9092".to_string()]));
    }

    #[test]
    fn parses_list_with_scheme() {
        let brokers: BootstrapBrokers = "kafka://one:9092, two:9092".parse().unwrap();
        assert_eq!(
            brokers,
            BootstrapBrokers(vec!["one:9092".to_string(), "two:9092".to_string()])
        );
        assert_eq!(brokers.to_string(), "one:9092,two:9092");
    }

    #[test]
    fn parses_list_with_scheme_on_each_entry() {
        let brokers: BootstrapBrokers = "kafka://one:9092,kafka://two:9092".parse().unwrap();
        assert_eq!(
            brokers,
            BootstrapBrokers(vec!["one:9092".to_string(), "two:9092".to_string()])
        );
    }

    #[test]
    fn fails_on_empty_address() {
        assert!("kafka://".parse::<BootstrapBrokers>().is_err());
    }

    #[test]
    fn spreads_heights_over_partitions() {
        let topic = TopicPartitions {
            topic: "archive-eth-blocks".to_string(),
            partitions: vec![0, 1, 2],
        };
        assert_eq!(topic.for_height(100), 1);
        assert_eq!(topic.for_height(101), 2);
        assert_eq!(topic.for_height(102), 0);
        // same height always goes to the same partition, incl. a re-org replacement
        assert_eq!(topic.for_height(100), 1);
    }

    #[test]
    fn uses_actual_partition_ids() {
        let topic = TopicPartitions {
            topic: "archive-eth-blocks".to_string(),
            partitions: vec![3, 7],
        };
        assert_eq!(topic.for_height(100), 3);
        assert_eq!(topic.for_height(101), 7);
    }
}
