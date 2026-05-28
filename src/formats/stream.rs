// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Streaming-target row encoding.
//!
//! Where [`crate::formats::json`] turns an [`ArchiveRow`] into per-field *files*,
//! this module turns the same row into per-field *messages* destined for one of
//! the streaming brokers (Pulsar today, Kafka next).
//!
//! Each [`Field`] variant maps to a stable topic label that the storage backend
//! appends to the user-supplied topic prefix, producing the per-field topic
//! name. Payloads are the original node response (for `*.json` fields) or the
//! reconstructed hex string with chain-appropriate prefix (for raw transactions),
//! mirroring the JSON file layout so consumers see the same bytes regardless of
//! which target the data lands on.
//!
//! Per-message properties carry enough metadata to drive future resume logic and
//! consumer-side dedup without parsing the payload — notably `dedup-key`,
//! `height`, and `tx-index`.

use std::collections::HashMap;

use crate::archiver::datakind::DataKind;
use crate::record::{ArchiveRow, BlockchainType, Field};

/// Every topic label the streaming layout publishes to. Used by
/// [`crate::storage::pulsar`] to pre-create one producer per label at startup
/// so the first append doesn't pay broker-side topic-creation latency.
///
/// Kept in sync with [`crate::record::Field::name`] by construction —
/// `topic_labels_constant_covers_all_field_variants` asserts that every
/// publishable variant's name appears here.
pub const TOPIC_LABELS: &[&str] = &[
    "blocks",
    "blocks-uncles",
    "tx-json",
    "tx-raw",
    "tx-receipts",
    "trace-calls",
    "trace-statediff",
];

/// One message destined for a single per-field topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMessage {
    /// Topic label (one of [`TOPIC_LABELS`]). The backend builds the full
    /// topic name as `<prefix>-<field>`.
    pub field: &'static str,
    /// Exact bytes to publish.
    pub payload: Vec<u8>,
    /// Partition key. Always the stringified block height so every message for
    /// a given block — including same-height re-orgs — lands in the same
    /// partition and is consumed in publish order.
    pub partition_key: String,
    /// Properties attached to the broker message. See module docs.
    pub properties: HashMap<String, String>,
}

/// Convert an [`ArchiveRow`] into the per-field messages it produces under the
/// streaming layout. The caller (the broker writer) decides which topic each
/// `field` maps to.
pub fn encode_row(row: &ArchiveRow) -> Vec<StreamMessage> {
    row.fields
        .iter()
        .filter_map(|f| encode_field(row, f))
        .collect()
}

fn encode_field(row: &ArchiveRow, field: &Field) -> Option<StreamMessage> {
    // Per-variant data needed to build the message: payload bytes, whether
    // this field is keyed by `tx_id`, and the uncle index when applicable.
    // The topic label itself comes from [`Field::name`] — there's no
    // per-variant string here, so a new Field variant gets a label "for free"
    // once it's added to [`Field::name`].
    let (payload, tx_keyed, uncle_index): (Vec<u8>, bool, Option<u8>) = match field {
        Field::BlockJson(bytes) => (bytes.clone(), false, None),
        Field::Uncle { index, json } => (json.clone(), false, Some(*index)),
        Field::TxJson(bytes) => (bytes.clone(), true, None),
        Field::TxRaw(bytes) => (encode_tx_raw(bytes, row.blockchain_type), true, None),
        Field::Receipt(bytes) => (bytes.clone(), true, None),
        // From/To duplicate values already inside the tx JSON; intentionally
        // skipped, matching the JSON-file layout. They still carry a name on
        // [`Field`] for completeness, just no producer is registered for them.
        Field::From(_) | Field::To(_) => return None,
        Field::Trace(bytes) => (bytes.clone(), true, None),
        Field::StateDiff(bytes) => (bytes.clone(), true, None),
    };

    let label = field.name();
    let tx_id = if tx_keyed { row.tx_id.as_deref() } else { None };

    let mut properties = HashMap::new();
    properties.insert("blockchain".to_string(), row.blockchain_id.clone());
    properties.insert("timestamp".to_string(), row.timestamp.to_rfc3339());
    properties.insert("kind".to_string(), kind_label(row.kind).to_string());
    properties.insert("field".to_string(), label.to_string());
    properties.insert("height".to_string(), row.height.to_string());
    properties.insert("block-id".to_string(), row.block_id.clone());
    if let Some(tx_index) = row.tx_index {
        properties.insert("tx-index".to_string(), tx_index.to_string());
    }
    if let Some(i) = uncle_index {
        properties.insert("uncle-index".to_string(), i.to_string());
    }
    properties.insert(
        "dedup-key".to_string(),
        dedup_key(row, label, tx_id, uncle_index),
    );

    Some(StreamMessage {
        field: label,
        payload,
        partition_key: row.height.to_string(),
        properties,
    })
}

fn encode_tx_raw(bytes: &[u8], blockchain_type: BlockchainType) -> Vec<u8> {
    let hex_str = hex::encode(bytes);
    match blockchain_type {
        BlockchainType::Ethereum => format!("0x{}", hex_str).into_bytes(),
        BlockchainType::Bitcoin => hex_str.into_bytes(),
    }
}

fn kind_label(kind: DataKind) -> &'static str {
    match kind {
        DataKind::Blocks => "blocks",
        DataKind::Transactions => "transactions",
        DataKind::TransactionTraces => "traces",
    }
}

/// Deterministic key used by consumers to dedup re-emitted messages (e.g.,
/// after a future restart-with-resume run that has to re-publish the tail of
/// a partial block).
///
/// Format: `<field>:<block-id>[:tx-<id>][:uncle-<i>]`.
///
/// **Why `block-id` and not `height`:** a chain re-org produces a *different*
/// block at an existing height, frequently with overlapping tx hashes. From a
/// consumer's perspective those are not duplicates — they replace the previous
/// block's state. Keying on the block hash makes the dedup key
/// reorg-correct: same height with a different block id gets a different
/// dedup key and is preserved, while a true re-emission of the *same*
/// (block-id, tx-id) pair correctly collapses.
///
/// **Why no `blockchain`:** each topic is single-blockchain by construction
/// (different chains run as separate processes and write to disjoint topic
/// prefixes), so a blockchain prefix here would be dead weight.
fn dedup_key(
    row: &ArchiveRow,
    label: &'static str,
    tx_id: Option<&str>,
    uncle_index: Option<u8>,
) -> String {
    let mut parts: Vec<String> = vec![label.to_string(), row.block_id.clone()];
    if let Some(tx_id) = tx_id {
        parts.push(format!("tx-{}", tx_id));
    }
    if let Some(i) = uncle_index {
        parts.push(format!("uncle-{}", i));
    }
    parts.join(":")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::collections::HashSet;

    use crate::archiver::datakind::DataKind;
    use crate::record::BlockchainType;

    fn row(kind: DataKind, tx_id: Option<&str>, fields: Vec<Field>) -> ArchiveRow {
        ArchiveRow {
            kind,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc::now(),
            height: 100,
            block_id: "0xblock".to_string(),
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
            parent_id: Some("0xparent".to_string()),
            tx_index: tx_id.map(|_| 7),
            tx_id: tx_id.map(|s| s.to_string()),
            fields,
        }
    }

    #[test]
    fn block_row_emits_blocks_and_uncle_messages() {
        let r = row(
            DataKind::Blocks,
            None,
            vec![
                Field::BlockJson(b"B".to_vec()),
                Field::Uncle { index: 0, json: b"U0".to_vec() },
                Field::Uncle { index: 1, json: b"U1".to_vec() },
            ],
        );
        let msgs = encode_row(&r);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].field, "blocks");
        assert_eq!(msgs[1].field, "blocks-uncles");
        assert_eq!(msgs[1].properties.get("uncle-index").map(|s| s.as_str()), Some("0"));
        assert_eq!(msgs[2].properties.get("uncle-index").map(|s| s.as_str()), Some("1"));
    }

    #[test]
    fn tx_row_emits_per_field_messages_with_metadata() {
        let r = row(
            DataKind::Transactions,
            Some("0xabc"),
            vec![
                Field::TxJson(b"TX".to_vec()),
                Field::TxRaw(vec![0xde, 0xad, 0xbe, 0xef]),
                Field::Receipt(b"R".to_vec()),
                Field::From("0xfrom".to_string()),
                Field::To("0xto".to_string()),
            ],
        );
        let msgs = encode_row(&r);
        // From/To are intentionally skipped.
        assert_eq!(msgs.len(), 3);
        let tx_msg = msgs.iter().find(|m| m.field == "tx-json").unwrap();
        assert_eq!(tx_msg.partition_key, "100");
        assert_eq!(tx_msg.properties.get("blockchain").unwrap(), "ETH");
        assert_eq!(tx_msg.properties.get("height").unwrap(), "100");
        assert_eq!(tx_msg.properties.get("tx-index").unwrap(), "7");
        assert_eq!(tx_msg.properties.get("block-id").unwrap(), "0xblock");
        assert_eq!(tx_msg.properties.get("field").unwrap(), "tx-json");
        assert_eq!(
            tx_msg.properties.get("dedup-key").unwrap(),
            "tx-json:0xblock:tx-0xabc"
        );
        let raw_msg = msgs.iter().find(|m| m.field == "tx-raw").unwrap();
        // Ethereum row → `0x` prefix added on the way out.
        assert_eq!(raw_msg.payload, b"0xdeadbeef");
    }

    #[test]
    fn bitcoin_tx_raw_omits_0x_prefix() {
        let mut r = row(
            DataKind::Transactions,
            Some("abc"),
            vec![Field::TxRaw(vec![0xde, 0xad, 0xbe, 0xef])],
        );
        r.blockchain_type = BlockchainType::Bitcoin;
        let msgs = encode_row(&r);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, b"deadbeef");
    }

    #[test]
    fn trace_row_emits_calls_and_statediff_messages() {
        let r = row(
            DataKind::TransactionTraces,
            Some("0xabc"),
            vec![
                Field::Trace(b"T".to_vec()),
                Field::StateDiff(b"S".to_vec()),
            ],
        );
        let msgs = encode_row(&r);
        assert_eq!(msgs.len(), 2);
        let trace = msgs.iter().find(|m| m.field == "trace-calls").unwrap();
        assert_eq!(
            trace.properties.get("dedup-key").unwrap(),
            "trace-calls:0xblock:tx-0xabc"
        );
        let state = msgs.iter().find(|m| m.field == "trace-statediff").unwrap();
        assert_eq!(
            state.properties.get("dedup-key").unwrap(),
            "trace-statediff:0xblock:tx-0xabc"
        );
    }

    #[test]
    fn uncle_dedup_key_carries_uncle_index() {
        let r = row(
            DataKind::Blocks,
            None,
            vec![Field::Uncle { index: 1, json: b"U".to_vec() }],
        );
        let msgs = encode_row(&r);
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            msgs[0].properties.get("dedup-key").unwrap(),
            "blocks-uncles:0xblock:uncle-1"
        );
    }

    #[test]
    fn partition_key_is_height() {
        let r = row(DataKind::Blocks, None, vec![Field::BlockJson(b"B".to_vec())]);
        let msgs = encode_row(&r);
        assert_eq!(msgs[0].partition_key, "100");
    }

    /// Re-org regression: two blocks at the *same height* with different
    /// block ids — even when they share the same tx hash — must produce
    /// *different* dedup keys. Otherwise consumers would silently treat the
    /// replacement block's txes as duplicates and drop real state.
    #[test]
    fn dedup_key_differs_across_same_height_reorg() {
        let original = ArchiveRow {
            kind: DataKind::Transactions,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc::now(),
            height: 100,
            block_id: "0xAAA".to_string(),
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
            parent_id: None,
            tx_index: Some(0),
            tx_id: Some("0xtx".to_string()),
            fields: vec![Field::TxJson(b"v1".to_vec())],
        };
        let reorged = ArchiveRow {
            block_id: "0xBBB".to_string(),
            fields: vec![Field::TxJson(b"v2".to_vec())],
            ..original.clone()
        };
        let original_key = encode_row(&original)[0]
            .properties
            .get("dedup-key")
            .cloned()
            .unwrap();
        let reorged_key = encode_row(&reorged)[0]
            .properties
            .get("dedup-key")
            .cloned()
            .unwrap();
        assert_ne!(
            original_key, reorged_key,
            "same height + same tx hash on different blocks must not collide"
        );
        assert_eq!(original_key, "tx-json:0xAAA:tx-0xtx");
        assert_eq!(reorged_key, "tx-json:0xBBB:tx-0xtx");
    }

    /// Sanity: every label produced by [`encode_field`] for any [`Field`]
    /// variant must appear in [`TOPIC_LABELS`], otherwise the storage layer
    /// won't have pre-created a producer for it and `append` will fail.
    #[test]
    fn topic_labels_constant_covers_all_field_variants() {
        // One representative row for each publishable Field variant.
        let r = ArchiveRow {
            kind: DataKind::Transactions,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc::now(),
            height: 1,
            block_id: "0x".to_string(),
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
            parent_id: None,
            tx_index: Some(0),
            tx_id: Some("0xabc".to_string()),
            fields: vec![
                Field::BlockJson(vec![]),
                Field::Uncle { index: 0, json: vec![] },
                Field::TxJson(vec![]),
                Field::TxRaw(vec![]),
                Field::Receipt(vec![]),
                Field::From("from".to_string()),
                Field::To("to".to_string()),
                Field::Trace(vec![]),
                Field::StateDiff(vec![]),
            ],
        };
        let produced: HashSet<&'static str> =
            encode_row(&r).into_iter().map(|m| m.field).collect();
        let declared: HashSet<&'static str> = TOPIC_LABELS.iter().copied().collect();
        assert_eq!(produced, declared);
    }
}
