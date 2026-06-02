// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Apache Pulsar streaming target.
//!
//! Implements [`WriteTarget`] only — by design topics are append-only logs,
//! so this target intentionally does not implement [`crate::storage::ScanTarget`]
//! or [`crate::storage::ReadTarget`]. The trait surface enforces that only the
//! `stream` command can run against a Pulsar target; `archive`, `fix`,
//! `verify`, and `compact` are rejected upfront in [`crate::main`].
//!
//! ## Topic layout
//!
//! One topic per field. Each topic name is `<prefix>-<field>` where the
//! field labels come from [`crate::formats::stream::topic_labels_for`] —
//! filtered by the running blockchain (Bitcoin omits receipts / uncles /
//! traces) and the caller's `--tables` / `--fields.trace` selection.
//! Producers for the relevant set are created eagerly at startup so the
//! writer never has to deal with first-write latency.
//!
//! ## Ordering
//!
//! Pulsar guarantees per-partition publish order *as long as the producer
//! submits messages in order*. The archiver fetches transactions in parallel,
//! so multiple [`PulsarWriter::append`] calls can race for the same topic at
//! the same time. A per-producer [`tokio::sync::Mutex`] serializes the
//! send-and-await-ack step for each topic — the work inside the lock is just
//! enqueueing one message, so contention is minimal.
//!
//! Messages are keyed by block height, so:
//! - all fields belonging to one block land in the same partition;
//! - a same-height re-org's replacement messages land in the same partition,
//!   after the previous ones.
//!
//! ## v1 scope (no recovery yet)
//!
//! The basic implementation just streams new blocks; it does not read tail
//! offsets to compute a resume point. Backwards compatibility hooks (the
//! `dedup-key` and per-message metadata produced by
//! [`crate::formats::stream`]) are already populated so a future resume run
//! can dedup re-emitted messages.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use pulsar::producer::{Producer, ProducerOptions};
use pulsar::{Pulsar, TokioExecutor};
use tokio::sync::Mutex;

use crate::archiver::datakind::DataKind;
use crate::archiver::range::Range;
use crate::formats::stream;
use crate::record::ArchiveRow;
use crate::storage::{TargetFile, TargetFileWriter, WriteTarget};

/// Apache Pulsar streaming target.
///
/// Holds one [`Producer`] per topic label that the running configuration
/// actually publishes to (see
/// [`crate::formats::stream::topic_labels_for`]), each behind its own
/// [`tokio::sync::Mutex`] so concurrent writers from the archiver serialize
/// their sends per topic without blocking sends to other topics. Topics
/// that aren't relevant for the running blockchain or the user's
/// `--tables` selection are NOT created — e.g. Bitcoin runs never create
/// the receipt / trace / uncle topics, and a `--tables blocks` run skips
/// the tx-* topics entirely.
pub struct PulsarStorage {
    topic_prefix: String,
    /// Pre-created producers keyed by topic label. The label set is the
    /// output of [`crate::formats::stream::topic_labels_for`] for the
    /// running blockchain + `DataOptions`. Created in
    /// [`PulsarStorage::new`] so per-topic startup latency doesn't show up
    /// on the first append. The producers internally keep the broker
    /// connection alive — we don't need a separate handle to the
    /// [`Pulsar`] client.
    producers: Arc<HashMap<&'static str, Arc<Mutex<Producer<TokioExecutor>>>>>,
}

impl PulsarStorage {
    /// Connect to the Pulsar broker and create one producer per
    /// per-field topic in `labels`. Each topic is named
    /// `<topic_prefix>-<label>`.
    ///
    /// `labels` is typically the output of
    /// [`crate::formats::stream::topic_labels_for`] — accepting it as a
    /// parameter (rather than hard-coding the maximal set) lets us avoid
    /// creating dead topics for blockchains that don't produce a kind
    /// (Bitcoin → no receipts/uncles/traces) or for runs that don't
    /// archive a kind (no `traces` in `--tables`).
    pub async fn new(
        broker_url: String,
        topic_prefix: String,
        labels: &[&'static str],
    ) -> Result<Self> {
        let client: Pulsar<TokioExecutor> = Pulsar::builder(broker_url, TokioExecutor)
            .build()
            .await
            .map_err(|e| anyhow!("Failed to connect to Pulsar: {:?}", e))?;

        // Same compression the Avro path uses (selected via `--compression`).
        // Reuses the global state so a single archive run is consistent
        // across whichever target it writes to.
        let compression = crate::global::get_pulsar_compression();
        let producer_options = ProducerOptions {
            compression: Some(compression),
            ..Default::default()
        };
        let mut producers = HashMap::new();
        for label in labels {
            let topic = format!("{}-{}", topic_prefix, label);
            tracing::info!("Pulsar producer: {}", topic);
            let producer = client
                .producer()
                .with_topic(&topic)
                .with_options(producer_options.clone())
                .build()
                .await
                .map_err(|e| anyhow!("Failed to create Pulsar producer for {}: {:?}", topic, e))?;
            producers.insert(*label, Arc::new(Mutex::new(producer)));
        }

        Ok(Self {
            topic_prefix,
            producers: Arc::new(producers),
        })
    }

    /// The topic name a given field publishes to. Exposed primarily for tests
    /// and log messages.
    pub fn topic_for(&self, label: &str) -> String {
        format!("{}-{}", self.topic_prefix, label)
    }
}

#[async_trait]
impl WriteTarget for PulsarStorage {
    type Writer = PulsarWriter;

    /// Pulsar has no concept of a per-(kind, range) file, so this just hands
    /// out a writer that shares the global per-topic producer set. The
    /// `overwrite` flag is irrelevant for an append-only log and is ignored;
    /// `range` is kept for the [`PulsarWriter::get_url`] notification payload.
    async fn create(
        &self,
        kind: DataKind,
        range: &Range,
        _overwrite: bool,
    ) -> Result<Option<Self::Writer>> {
        Ok(Some(PulsarWriter {
            kind,
            range: range.clone(),
            producers: self.producers.clone(),
            topic_prefix: self.topic_prefix.clone(),
        }))
    }

    /// Pulsar partitions preserve messages in publish order; the archiver
    /// must deliver them in chain-natural order or consumers would see
    /// re-ordered tx/trace streams within a block. See [`WriteTarget::needs_ordering`].
    fn needs_ordering(&self) -> bool {
        true
    }
}

/// Per-(kind, range) writer. Carries no per-session state — the producers it
/// uses are owned by [`PulsarStorage`] and shared across all writers.
pub struct PulsarWriter {
    kind: DataKind,
    range: Range,
    producers: Arc<HashMap<&'static str, Arc<Mutex<Producer<TokioExecutor>>>>>,
    topic_prefix: String,
}

impl TargetFile for PulsarWriter {
    /// Used as the `location` field in notifications. We surface the topic
    /// prefix and the range the writer covers, since there's no single
    /// addressable artifact like a file URL.
    fn get_url(&self) -> String {
        format!("pulsar:{}?range={}", self.topic_prefix, self.range)
    }
}

#[async_trait]
impl TargetFileWriter for PulsarWriter {
    async fn append(&self, row: ArchiveRow) -> Result<()> {
        for msg in stream::encode_row(&row) {
            let producer = self
                .producers
                .get(&msg.field)
                .ok_or_else(|| anyhow!("No producer registered for field {:?}", msg.field))?
                .clone();
            let payload_len = msg.payload.len();

            // Hold the per-topic lock from enqueue through broker ack — that's
            // what guarantees the broker sees messages in the order this
            // writer produced them. Different topics' producers are independent.
            let mut producer = producer.lock().await;
            let mut builder = producer
                .create_message()
                .with_content(msg.payload)
                .with_partition_key(msg.partition_key.clone());
            for (k, v) in &msg.properties {
                builder = builder.with_property(k.clone(), v.clone());
            }
            let send_future = builder
                .send_non_blocking()
                .await
                .map_err(|e| anyhow!("Pulsar send failed: {:?}", e))?;
            // Block on the broker ack before releasing the lock — otherwise a
            // later message could overtake this one on the broker side.
            send_future
                .await
                .map_err(|e| anyhow!("Pulsar ack failed: {:?}", e))?;

            crate::progress::on_bytes(payload_len);
            crate::metrics::add_bytes(&self.kind, crate::metrics::Direction::Write, payload_len);
        }
        crate::progress::on_record();
        crate::metrics::add_items(&self.kind, crate::metrics::Direction::Write, 1);
        Ok(())
    }

    async fn close(self) -> Result<()> {
        // Producers are shared and outlive the writer; nothing to flush here
        // because every `append` already awaited its broker ack.
        Ok(())
    }
}

// Pulsar is intentionally not a ScanTarget or ReadTarget. Topics are
// append-only logs — listing existing data requires a topic-tail read which
// will be the resume mechanism added in a follow-up. Stub trait impls would
// just mask "missing capability" as runtime errors, so they're omitted.

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    use crate::archiver::datakind::DataKind;
    use crate::archiver::range::Range;
    use crate::record::{ArchiveRow, BlockchainType, Field};

    fn block_row(height: u64) -> ArchiveRow {
        ArchiveRow {
            kind: DataKind::Blocks,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc::now(),
            height,
            block_id: format!("0xblock{}", height),
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
            parent_id: Some(format!("0xparent{}", height.saturating_sub(1))),
            tx_index: None,
            tx_id: None,
            fields: vec![Field::BlockJson(format!("{{\"h\":{}}}", height).into_bytes())],
        }
    }

    fn tx_row(height: u64, tx_id: &str) -> ArchiveRow {
        ArchiveRow {
            kind: DataKind::Transactions,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc::now(),
            height,
            block_id: format!("0xblock{}", height),
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
            parent_id: None,
            tx_index: Some(0),
            tx_id: Some(tx_id.to_string()),
            fields: vec![
                Field::TxJson(b"{\"a\":1}".to_vec()),
                Field::TxRaw(vec![0xde, 0xad, 0xbe, 0xef]),
                Field::Receipt(b"{\"r\":1}".to_vec()),
            ],
        }
    }

    /// End-to-end: writer publishes per-field messages to Pulsar, and a
    /// reader subscription receives them with the expected payloads and
    /// properties.
    #[tokio::test(flavor = "multi_thread")]
    async fn writes_block_and_tx_messages_to_per_field_topics() {
        crate::testing::start_test();

        let image = GenericImage::new("apachepulsar/pulsar", "3.3.3")
            .with_exposed_port(6650.tcp())
            .with_exposed_port(8080.tcp())
            .with_wait_for(WaitFor::message_on_stdout("became the leader"));
        let container = image
            .with_cmd(vec![
                "bin/pulsar".to_string(),
                "standalone".to_string(),
            ])
            .start()
            .await
            .unwrap();
        let uri = format!(
            "pulsar://{}:{}",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(6650).await.unwrap()
        );

        // Use a non-persistent prefix so the test doesn't write to disk on
        // the broker; same as the existing notify::pulsar test.
        let prefix = "non-persistent://public/default/dshackle-archive-test".to_string();

        // The integration test exercises the full Ethereum producer set so
        // we cover blocks + tx-* + trace-* topics with one container start.
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
        let labels =
            crate::formats::stream::topic_labels_for(BlockchainType::Ethereum, &full_options);
        let storage = PulsarStorage::new(uri.clone(), prefix.clone(), &labels)
            .await
            .expect("Pulsar connect");

        let blocks_topic = storage.topic_for("blocks");
        let txes_topic = storage.topic_for("tx-json");

        // Subscribe before writing so we don't lose the messages.
        let client = Pulsar::builder(uri, TokioExecutor)
            .build()
            .await
            .expect("client");
        use futures_util::StreamExt;
        use pulsar::SubType;
        let mut blocks_consumer: pulsar::Consumer<Vec<u8>, _> = client
            .consumer()
            .with_topic(&blocks_topic)
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("test-blocks")
            .build()
            .await
            .expect("blocks consumer");
        let mut txes_consumer: pulsar::Consumer<Vec<u8>, _> = client
            .consumer()
            .with_topic(&txes_topic)
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("test-txes")
            .build()
            .await
            .expect("txes consumer");

        // Write a block + a transaction.
        let writer = storage
            .create(DataKind::Blocks, &Range::Single(42.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(block_row(42)).await.unwrap();
        writer.close().await.unwrap();
        let writer = storage
            .create(DataKind::Transactions, &Range::Single(42.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(tx_row(42, "0xabc")).await.unwrap();
        writer.close().await.unwrap();

        // Block message — payload is the wrapping JSON envelope, with the
        // node response embedded under `value`.
        let msg = blocks_consumer
            .next()
            .await
            .expect("blocks msg available")
            .expect("blocks msg ok");
        let entry: serde_json::Value = serde_json::from_slice(&msg.payload.data)
            .expect("blocks payload is JSON");
        assert_eq!(entry["field"], "blocks");
        assert_eq!(entry["kind"], "blocks");
        assert_eq!(entry["height"], 42);
        assert_eq!(entry["blockId"], "0xblock42");
        assert_eq!(entry["value"], serde_json::json!({"h": 42}));
        let props: HashMap<_, _> = msg
            .payload
            .metadata
            .properties
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        assert_eq!(props.get("height").map(|s| s.as_str()), Some("42"));
        assert_eq!(props.get("block-id").map(|s| s.as_str()), Some("0xblock42"));
        assert_eq!(
            props.get("dedup-key").map(|s| s.as_str()),
            Some("blocks:0xblock42")
        );

        // Tx message — node JSON embedded under `value`, txid in dedup key.
        let msg = txes_consumer
            .next()
            .await
            .expect("tx msg available")
            .expect("tx msg ok");
        let entry: serde_json::Value = serde_json::from_slice(&msg.payload.data)
            .expect("tx payload is JSON");
        assert_eq!(entry["field"], "tx-json");
        assert_eq!(entry["txIndex"], 0);
        assert_eq!(entry["txId"], "0xabc");
        assert_eq!(entry["value"], serde_json::json!({"a": 1}));
        let props: HashMap<_, _> = msg
            .payload
            .metadata
            .properties
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        assert_eq!(
            props.get("dedup-key").map(|s| s.as_str()),
            Some("tx-json:0xblock42:tx-0xabc")
        );
    }
}
