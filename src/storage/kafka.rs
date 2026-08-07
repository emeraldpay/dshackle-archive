// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Apache Kafka streaming target.
//!
//! Implements [`WriteTarget`] only — topics are append-only logs, so
//! `archive`, `fix`, `verify`, and `compact` are rejected upfront in
//! [`crate::main`]. The counterpart for Pulsar is
//! [`crate::storage::pulsar`]; everything above the broker API (row encoding,
//! topic label set, ordering contract) is shared between the two.
//!
//! ## Topic layout
//!
//! One topic per field, named by [`TopicSet`], so only topics relevant to the
//! running blockchain and the user's `--tables` / `--fields.trace` selection
//! are used. Every topic is resolved at startup, which is also what fails the
//! run early on a missing topic.
//!
//! ## Partitioning & ordering
//!
//! rskafka ships no producer-side partitioner: a
//! [`PartitionClient`] is bound to one partition and the caller picks it. That
//! makes the ordering story simpler than Pulsar's — the partition is chosen by
//! block height (see [`TopicPartitions::for_height`]), so every message of one
//! block, and any same-height re-org replacement, lands on the same partition.
//!
//! There is no lock around a producer, because nothing publishes to one topic
//! twice at a time: [`WriteTarget::needs_ordering`] is `true`, so every
//! `append` reaches the writer from [`OrderedSink`]'s single drain task, one
//! `append` issues at most one produce per topic, and the writers of the
//! different [`DataKind`]s own disjoint topic labels.
//!
//! ## Batching
//!
//! A row's records are grouped per topic and produced as one Kafka record
//! batch each. Kafka appends a batch to its partition all-or-nothing, so the
//! records of one row that share a topic — a block's uncles, above all — can't
//! land half-published. The per-topic produces then run concurrently, since
//! distinct topics are independent; a transaction's three fields cost one
//! broker round trip between them instead of three.
//!
//! [`OrderedSink`]: crate::archiver::order::OrderedSink
//!
//! ## Re-org handling
//!
//! Nothing Kafka-specific: the archiver's cancel path abandons the in-flight
//! ordered sink (see [`crate::archiver::order::OrderedSink::abandon`]) and the
//! replacement block re-publishes with a fresh `dedup-key`, which travels both
//! in the record headers and in the payload. Transient node failures that
//! would otherwise leave a gap are absorbed by
//! [`crate::global::RetryPolicy::Forever`], the default for streaming targets.
//!
//! What batching cannot cover is a row whose *topics* disagree — the block
//! landed but its uncles were rejected. A topic is append-only, so there is
//! nothing to roll back; the writer records what did land, logs it, and fails
//! the run so the partial state is visible rather than silent.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::Record;

use crate::archiver::datakind::DataKind;
use crate::archiver::range::Range;
use crate::formats::stream::{self, StreamMessage};
use crate::formats::topics::TopicSet;
use crate::kafka::{BootstrapBrokers, ResolvedTopic, TopicPartitions};
use crate::notify::{Location, MessageRef};
use crate::record::ArchiveRow;
use crate::storage::{TargetFile, TargetFileWriter, WriteTarget};

/// Apache Kafka streaming target. See the module-level doc for the topic
/// layout and the ordering contract.
pub struct KafkaStorage {
    topics: Arc<TopicSet>,
    /// Producers of every topic this run publishes to, keyed by field label.
    producers: Arc<HashMap<&'static str, TopicProducers>>,
    /// Resolved once from `--compression`: rskafka takes the codec per
    /// `produce` call, so the writers carry it along.
    compression: Compression,
}

impl KafkaStorage {
    /// Connect to the cluster and open a producer for every partition of every
    /// topic in `topics`.
    ///
    /// Resolving all of it upfront means a missing topic or an unreachable
    /// broker stops the run before the first block is fetched, rather than
    /// midway through a stream.
    pub async fn new(brokers: BootstrapBrokers, topics: TopicSet) -> Result<Self> {
        let client = brokers.connect().await?;
        let names: Vec<String> = topics.iter().map(|(_, name)| name).collect();
        let resolved = ResolvedTopic::discover(&client, &names).await?;

        let mut producers = HashMap::new();
        for ((label, _), resolved) in topics.iter().zip(resolved) {
            let opened = TopicProducers::open(&client, resolved).await?;
            tracing::info!(
                "Kafka producer: {} ({} partitions)",
                opened.topic(),
                opened.count()
            );
            producers.insert(label, opened);
        }

        Ok(Self {
            topics: Arc::new(topics),
            producers: Arc::new(producers),
            compression: crate::global::get_kafka_compression(),
        })
    }

    /// The topic name a given field publishes to. Exposed primarily for tests
    /// and log messages.
    pub fn topic_for(&self, label: &str) -> String {
        self.topics.name_for(label)
    }
}

/// Every partition of one topic, ready to produce to.
struct TopicProducers {
    partitions: TopicPartitions,
    /// One client per partition — rskafka binds a client to a single
    /// partition. Unlocked; see the module doc for why one topic is never
    /// published to twice at a time.
    producers: HashMap<i32, PartitionClient>,
}

impl TopicProducers {
    async fn open(client: &Client, topic: ResolvedTopic) -> Result<Self> {
        let (partitions, probe) = topic.into_parts();
        let mut producers = HashMap::new();
        // resolving the topic already opened a client on one of its
        // partitions; keep it rather than opening a second for that partition
        if let Some((partition, probe)) = probe.filter(|(id, _)| partitions.contains(*id)) {
            producers.insert(partition, probe);
        }
        for partition in partitions.ids() {
            if producers.contains_key(&partition) {
                continue;
            }
            // the topic was just resolved from cluster metadata, so an unknown
            // topic here means it was deleted under us — report that instead
            // of waiting for it to reappear
            let producer = client
                .partition_client(partitions.topic(), partition, UnknownTopicHandling::Error)
                .await
                .map_err(|e| {
                    anyhow!(
                        "Failed to open topic {} partition {}: {}",
                        partitions.topic(),
                        partition,
                        e
                    )
                })?;
            producers.insert(partition, producer);
        }
        Ok(Self { partitions, producers })
    }

    fn topic(&self) -> &str {
        self.partitions.topic()
    }

    fn count(&self) -> usize {
        self.partitions.count()
    }

    /// The partition that carries the given block height.
    fn partition_for(&self, height: u64) -> i32 {
        self.partitions.for_height(height)
    }

    fn producer_for(&self, partition: i32) -> &PartitionClient {
        self.producers
            .get(&partition)
            .expect("a producer is opened for every partition of the topic")
    }
}

#[async_trait]
impl WriteTarget for KafkaStorage {
    type Writer = KafkaWriter;

    /// Hands out a writer sharing the global producer set. `overwrite` is
    /// ignored (append-only log); `range` is retained for the notification
    /// payload via [`KafkaWriter::get_url`].
    async fn create(
        &self,
        kind: DataKind,
        range: &Range,
        _overwrite: bool,
    ) -> Result<Option<Self::Writer>> {
        Ok(Some(KafkaWriter {
            kind,
            range: range.clone(),
            topics: self.topics.clone(),
            producers: self.producers.clone(),
            compression: self.compression,
            published: std::sync::Mutex::new(Vec::new()),
        }))
    }

    /// Kafka requires chain-natural order; see [`WriteTarget::needs_ordering`].
    fn needs_ordering(&self) -> bool {
        true
    }
}

/// Per-(kind, range) writer. The producers it uses are owned by
/// [`KafkaStorage`] and shared across all writers; per-session it only
/// accumulates the broker receipts for the notification report.
pub struct KafkaWriter {
    kind: DataKind,
    range: Range,
    topics: Arc<TopicSet>,
    producers: Arc<HashMap<&'static str, TopicProducers>>,
    compression: Compression,
    /// Broker receipts of this session, for the notification report. Bounded
    /// by the writer's own range — one block in `stream` mode — and released
    /// with the writer once [`TargetFileWriter::locations`] has read it.
    published: std::sync::Mutex<Vec<PublishedMessage>>,
}

/// A broker-acknowledged message, tagged with the height it belongs to so the
/// notification report can group messages per height.
struct PublishedMessage {
    height: u64,
    message: MessageRef,
}

/// One [`StreamMessage`] together with the block time it belongs to.
///
/// A Kafka record carries a timestamp of its own, and the block time is the
/// meaningful one for a consumer indexing by time — so it travels with the
/// message instead of defaulting to the produce time.
struct BlockMessage {
    message: StreamMessage,
    timestamp: DateTime<Utc>,
}

impl From<BlockMessage> for Record {
    fn from(value: BlockMessage) -> Self {
        Record {
            // no key: the partition is chosen by height, and one height
            // produces many records — so a key could only mislead a compacted
            // topic into dropping all but the last record of the block
            key: None,
            value: Some(value.message.payload),
            headers: value
                .message
                .properties
                .into_iter()
                .map(|(name, value)| (name, value.into_bytes()))
                .collect::<BTreeMap<_, _>>(),
            timestamp: value.timestamp,
        }
    }
}

/// Standard Kafka string form of a record address within its topic.
fn format_message_id(partition: i32, offset: i64) -> String {
    format!("{}:{}", partition, offset)
}

impl TargetFile for KafkaWriter {
    /// Used as the `location` field in notifications. We surface the topic
    /// prefix and the range the writer covers, since there's no single
    /// addressable artifact like a file URL.
    fn get_url(&self) -> String {
        format!("kafka:{}?range={}", self.topics.prefix(), self.range)
    }
}

/// One row's records for a single topic, ready to go as one Kafka batch.
struct TopicBatch<'a> {
    field: &'static str,
    producers: &'a TopicProducers,
    partition: i32,
    records: Vec<Record>,
    bytes: usize,
}

impl KafkaWriter {
    /// Split a row into one batch per topic, keeping the order
    /// [`stream::encode_row`] produced.
    fn batches(&self, row: &ArchiveRow) -> Result<Vec<TopicBatch<'_>>> {
        let mut batches: Vec<TopicBatch> = Vec::new();
        for message in stream::encode_row(row) {
            let field = message.field;
            let producers = self
                .producers
                .get(field)
                .ok_or_else(|| anyhow!("No producer registered for field {:?}", field))?;
            let partition = producers.partition_for(message.partition_key);
            let bytes = message.payload.len();
            let record = Record::from(BlockMessage {
                message,
                timestamp: row.timestamp,
            });
            match batches.iter_mut().find(|batch| batch.field == field) {
                Some(batch) => {
                    batch.records.push(record);
                    batch.bytes += bytes;
                }
                None => batches.push(TopicBatch {
                    field,
                    producers,
                    partition,
                    records: vec![record],
                    bytes,
                }),
            }
        }
        Ok(batches)
    }

    /// Record the broker receipts of one produced batch, for the notification
    /// report. Offsets come back in the order the records were sent.
    fn record_receipts(&self, row: &ArchiveRow, batch_field: &str, partition: i32, offsets: Vec<i64>) {
        let topic = self.topics.name_for(batch_field);
        let mut published = self.published.lock().unwrap();
        for offset in offsets {
            published.push(PublishedMessage {
                height: row.height,
                message: MessageRef {
                    topic: topic.clone(),
                    field: batch_field.to_string(),
                    tx_id: row.tx_id.clone(),
                    message_id: Some(format_message_id(partition, offset)),
                },
            });
        }
    }
}

#[async_trait]
impl TargetFileWriter for KafkaWriter {
    async fn append(&self, row: ArchiveRow) -> Result<()> {
        let batches = self.batches(&row)?;
        // distinct topics are independent, so their round trips overlap
        let produced = futures_util::future::join_all(batches.into_iter().map(|batch| async move {
            let result = batch
                .producers
                .producer_for(batch.partition)
                .produce(batch.records, self.compression)
                .await
                .map_err(|e| {
                    anyhow!(
                        "Rejected by topic {} partition {}: {}",
                        batch.producers.topic(),
                        batch.partition,
                        e
                    )
                });
            (batch.field, batch.partition, batch.bytes, result)
        }))
        .await;

        let mut failure: Option<anyhow::Error> = None;
        let mut published = 0;
        for (field, partition, bytes, result) in produced {
            match result {
                Ok(offsets) => {
                    published += offsets.len();
                    self.record_receipts(&row, field, partition, offsets);
                    crate::progress::on_bytes(bytes);
                    crate::metrics::add_bytes(&self.kind, crate::metrics::Direction::Write, bytes);
                }
                Err(e) => failure = failure.or(Some(e)),
            }
        }

        if let Some(failure) = failure {
            // a topic can't be rolled back, so say what did land: a consumer
            // is already seeing part of this row and nothing else will report it
            if published > 0 {
                tracing::warn!(
                    height = row.height,
                    tx_id = row.tx_id.as_deref().unwrap_or("-"),
                    "{} record(s) of this row reached their topics before the failure below",
                    published
                );
            }
            return Err(failure);
        }

        crate::progress::on_record();
        crate::metrics::add_items(&self.kind, crate::metrics::Direction::Write, 1);
        Ok(())
    }

    fn locations(&self) -> Vec<(Range, Location)> {
        let mut by_height: BTreeMap<u64, Vec<MessageRef>> = BTreeMap::new();
        for published in self.published.lock().unwrap().iter() {
            by_height
                .entry(published.height)
                .or_default()
                .push(published.message.clone());
        }
        by_height
            .into_iter()
            .map(|(height, messages)| (
                Range::Single(height.into()),
                Location::Kafka { messages },
            ))
            .collect()
    }

    async fn close(self) -> Result<()> {
        // Producers are shared and outlive the writer; nothing to flush here
        // because every `append` already awaited its broker ack.
        Ok(())
    }
}

// Kafka is intentionally not a [`ScanTarget`] / [`ReadTarget`]: enumerating or
// reading back records would mean consuming the topic — a different mechanism
// entirely, and stub impls would just turn "missing capability" into runtime
// errors.

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    use crate::record::{BlockchainType, Field};

    /// A record header, as the UTF-8 string it was written from.
    fn header(record: &Record, name: &str) -> Option<String> {
        record
            .headers
            .get(name)
            .map(|value| String::from_utf8(value.clone()).unwrap())
    }

    /// The per-topic grouping `KafkaWriter::batches` performs, without the
    /// broker connections a real writer needs.
    fn group_records(row: &ArchiveRow) -> Vec<(&'static str, Vec<Record>)> {
        let mut grouped: Vec<(&'static str, Vec<Record>)> = Vec::new();
        for message in stream::encode_row(row) {
            let field = message.field;
            let record = Record::from(BlockMessage {
                message,
                timestamp: row.timestamp,
            });
            match grouped.iter_mut().find(|(topic, _)| *topic == field) {
                Some((_, records)) => records.push(record),
                None => grouped.push((field, vec![record])),
            }
        }
        grouped
    }

    /// A block that also carries uncles — the case where one row produces
    /// several records for a single topic.
    fn block_row_with_uncles(height: u64) -> ArchiveRow {
        ArchiveRow {
            fields: vec![
                Field::BlockJson(format!("{{\"h\":{}}}", height).into_bytes()),
                Field::Uncle { index: 0, json: b"{\"u\":0}".to_vec() },
                Field::Uncle { index: 1, json: b"{\"u\":1}".to_vec() },
            ],
            ..block_row(height)
        }
    }

    fn tx_row(height: u64, tx_id: &str) -> ArchiveRow {
        ArchiveRow {
            kind: DataKind::Transactions,
            tx_index: Some(0),
            tx_id: Some(tx_id.to_string()),
            fields: vec![
                Field::TxJson(b"{\"a\":1}".to_vec()),
                Field::TxRaw(vec![0xde, 0xad]),
                Field::Receipt(b"{\"r\":1}".to_vec()),
            ],
            ..block_row(height)
        }
    }

    fn block_row(height: u64) -> ArchiveRow {
        ArchiveRow {
            kind: DataKind::Blocks,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc::now(),
            height,
            block_id: format!("0xblock{}", height),
            timestamp: Utc.timestamp_opt(0x689aad27, 0).unwrap(),
            parent_id: Some(format!("0xparent{}", height.saturating_sub(1))),
            tx_index: None,
            tx_id: None,
            tx_count: Some(1),
            fields: vec![Field::BlockJson(format!("{{\"h\":{}}}", height).into_bytes())],
        }
    }

    #[test]
    fn record_carries_payload_and_block_time() {
        let row = block_row(42);
        let message = stream::encode_row(&row).remove(0);
        let payload = message.payload.clone();
        let record = Record::from(BlockMessage {
            message,
            timestamp: row.timestamp,
        });

        assert_eq!(record.value, Some(payload));
        // block time, not produce time
        assert_eq!(record.timestamp, row.timestamp);
    }

    /// A block publishes many records, all to one partition. Giving them a
    /// key would let a compacted topic keep only the last of them.
    #[test]
    fn record_has_no_key() {
        let row = block_row(42);
        let record = Record::from(BlockMessage {
            message: stream::encode_row(&row).remove(0),
            timestamp: row.timestamp,
        });

        assert_eq!(record.key, None);
    }

    /// The records of one row that share a topic must go out as one batch:
    /// Kafka appends a batch all-or-nothing, which is what stops a block's
    /// uncle set from landing half-published.
    #[test]
    fn groups_a_rows_records_per_topic() {
        let grouped = group_records(&block_row_with_uncles(42));

        assert_eq!(grouped.len(), 2, "one batch per topic, not per record");
        assert_eq!(grouped[0].0, "blocks");
        assert_eq!(grouped[0].1.len(), 1);
        assert_eq!(grouped[1].0, "blocks-uncles");
        assert_eq!(grouped[1].1.len(), 2, "both uncles in one atomic batch");
    }

    /// Every record of a row carries the same height, so they all resolve to
    /// the same partition — including a same-height re-org replacement.
    #[test]
    fn every_record_of_a_row_shares_the_partition_key() {
        let row = tx_row(42, "0xabc");
        let keys: Vec<u64> = stream::encode_row(&row)
            .into_iter()
            .map(|m| m.partition_key)
            .collect();

        assert_eq!(keys, vec![42, 42, 42]);
    }

    /// End-to-end against a real broker: what the writer produces is what a
    /// consumer reads back, on the topics and partition the layout promises,
    /// and `locations()` says where each record went.
    ///
    /// Auto-creation is off, so this also covers the archive creating its own
    /// topics; three partitions, so the height picks one that isn't 0.
    #[tokio::test(flavor = "multi_thread")]
    async fn writes_block_and_tx_records_to_per_field_topics() {
        crate::testing::start_test();
        let (container, brokers) = crate::testing::start_kafka(3, false).await;

        use crate::archiver::datakind::{BlockOptions, DataOptions, TraceOptions, TxOptions};
        let full_options = DataOptions {
            overwrite: true,
            block: Some(BlockOptions::default()),
            tx: Some(TxOptions::default()),
            trace: Some(TraceOptions {
                include_trace: true,
                include_state_diff: true,
            }),
        };
        let topics = TopicSet::new(
            "dshackle-archive-test".to_string(),
            BlockchainType::Ethereum,
            &full_options,
        );
        let storage = KafkaStorage::new(brokers.clone(), topics)
            .await
            .expect("Kafka connect");

        // 43 % 3 == 1, so a mapping that ignored the height would be visible
        let height = 43;
        let partition = 1;

        let writer = storage
            .create(DataKind::Blocks, &Range::Single(height.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(block_row_with_uncles(height)).await.unwrap();
        let block_locations = writer.locations();
        writer.close().await.unwrap();

        let writer = storage
            .create(DataKind::Transactions, &Range::Single(height.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(tx_row(height, "0xabc")).await.unwrap();
        let tx_locations = writer.locations();
        writer.close().await.unwrap();

        let client = brokers.connect().await.unwrap();

        // Block topic — the node response is embedded under `value`.
        let blocks = fetch_all(&client, &storage.topic_for("blocks"), partition).await;
        assert_eq!(blocks.len(), 1);
        let entry = parse(&blocks[0]);
        assert_eq!(entry["field"], "blocks");
        assert_eq!(entry["table"], "blocks");
        assert_eq!(entry["height"], height);
        assert_eq!(entry["blockId"], format!("0xblock{}", height));
        assert_eq!(entry["value"], serde_json::json!({"h": height}));
        assert_eq!(header(&blocks[0], "height"), Some(height.to_string()));
        assert_eq!(
            header(&blocks[0], "dedup-key"),
            Some(format!("blocks:0xblock{}", height))
        );
        // the broker sees no key: one would let a compacted topic keep only
        // the last record of the block
        assert_eq!(blocks[0].key, None);

        // Both uncles, in the order the row produced them.
        let uncles = fetch_all(&client, &storage.topic_for("blocks-uncles"), partition).await;
        assert_eq!(uncles.len(), 2);
        assert_eq!(parse(&uncles[0])["uncleIndex"], 0);
        assert_eq!(parse(&uncles[1])["uncleIndex"], 1);

        // Transaction fields, each on its own topic.
        let tx_json = fetch_all(&client, &storage.topic_for("tx-json"), partition).await;
        assert_eq!(tx_json.len(), 1);
        let entry = parse(&tx_json[0]);
        assert_eq!(entry["field"], "tx-json");
        assert_eq!(entry["txIndex"], 0);
        assert_eq!(entry["txId"], "0xabc");
        assert_eq!(entry["value"], serde_json::json!({"a": 1}));
        assert_eq!(
            header(&tx_json[0], "dedup-key"),
            Some(format!("tx-json:0xblock{}:tx-0xabc", height))
        );
        let tx_raw = fetch_all(&client, &storage.topic_for("tx-raw"), partition).await;
        assert_eq!(parse(&tx_raw[0])["value"], "0xdead");
        let receipts = fetch_all(&client, &storage.topic_for("tx-receipts"), partition).await;
        assert_eq!(parse(&receipts[0])["value"], serde_json::json!({"r": 1}));

        // Notification locations name the topic and the record's address.
        assert_eq!(block_locations.len(), 1);
        assert_eq!(block_locations[0].0, Range::Single(height.into()));
        let messages = match &block_locations[0].1 {
            Location::Kafka { messages } => messages,
            other => panic!("Expected a kafka location, got {:?}", other),
        };
        assert_eq!(messages.len(), 3, "the block and both its uncles");
        assert_eq!(messages[0].topic, storage.topic_for("blocks"));
        assert_eq!(messages[0].field, "blocks");
        assert_eq!(messages[0].tx_id, None);
        assert_eq!(messages[0].message_id.as_deref(), Some("1:0"));
        // the two uncles are consecutive offsets of their own topic
        assert_eq!(messages[1].message_id.as_deref(), Some("1:0"));
        assert_eq!(messages[2].message_id.as_deref(), Some("1:1"));

        let messages = match &tx_locations[0].1 {
            Location::Kafka { messages } => messages,
            other => panic!("Expected a kafka location, got {:?}", other),
        };
        assert_eq!(messages.len(), 3, "tx json, raw and receipt");
        for message in messages {
            assert_eq!(message.tx_id.as_deref(), Some("0xabc"));
            assert_eq!(message.message_id.as_deref(), Some("1:0"));
        }

        container.stop().await.unwrap();
    }

    /// Read every record of one topic partition back off the broker.
    async fn fetch_all(client: &Client, topic: &str, partition: i32) -> Vec<Record> {
        let consumer = client
            .partition_client(topic, partition, UnknownTopicHandling::Error)
            .await
            .expect("open partition for reading");
        let (records, _watermark) = consumer
            .fetch_records(0, 1..1_000_000, 5_000)
            .await
            .expect("fetch records");
        records.into_iter().map(|r| r.record).collect()
    }

    fn parse(record: &Record) -> serde_json::Value {
        serde_json::from_slice(record.value.as_ref().expect("record has a payload"))
            .expect("payload is JSON")
    }

    #[test]
    fn record_headers_carry_stream_properties() {
        let row = block_row(42);
        let record = Record::from(BlockMessage {
            message: stream::encode_row(&row).remove(0),
            timestamp: row.timestamp,
        });

        assert_eq!(header(&record, "height"), Some("42".to_string()));
        assert_eq!(header(&record, "block-id"), Some("0xblock42".to_string()));
        assert_eq!(
            header(&record, "dedup-key"),
            Some("blocks:0xblock42".to_string())
        );
        assert_eq!(
            header(&record, "timestamp"),
            Some("2025-08-12T02:55:35+00:00".to_string())
        );
    }
}
