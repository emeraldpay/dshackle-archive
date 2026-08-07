// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Streaming-target row encoding.
//!
//! Where [`crate::formats::json`] turns an [`ArchiveRow`] into per-field *files*,
//! this module turns the same row into per-field *messages* destined for one of
//! the streaming brokers (Pulsar, Kafka).
//!
//! Each [`Field`] variant maps to a stable topic label; turning that label into
//! the topic it belongs to is [`crate::formats::topics::TopicSet`]'s job. The
//! message payload is a JSON object — an [`Entry`] — that wraps the
//! original node response (or hex string for raw transactions) together with
//! the metadata a consumer needs to route, filter or dedup the message without
//! relying on broker headers. Header-only metadata used to be enough, but
//! several downstream sinks (notably Pulsar IO connectors) only forward the
//! message body, so the wrapping struct keeps the contract self-describing
//! regardless of what the consumer sees.
//!
//! A small subset of metadata is *also* attached as broker properties
//! (`dedup-key`, `timestamp`, `height`, `block-id`) so brokers and lightweight
//! consumers (server-side selectors, simple log tailers) can route and dedup
//! without parsing JSON.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use serde_json::value::{to_raw_value, RawValue};

use crate::archiver::datakind::DataKind;
use crate::record::{ArchiveRow, BlockchainType, Field};

/// One message destined for a single per-field topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMessage {
    /// Topic label — one of the labels
    /// [`topic_labels_for`](crate::formats::topics::topic_labels_for) produces
    /// for the running blockchain.
    pub field: &'static str,
    /// JSON bytes of the serialized [`Entry`] — the node response wrapped
    /// alongside its routing metadata.
    pub payload: Vec<u8>,
    /// Block height, which is what every broker partitions on: every message
    /// of one block — including a same-height re-org — must land in the same
    /// partition and be consumed in publish order. Brokers that hash a key
    /// (Pulsar) render it; those that address partitions directly (Kafka) use
    /// the number.
    pub partition_key: u64,
    /// Broker-side properties retained for fast filtering and dedup at the
    /// broker layer. Full metadata also lives inside the payload [`Entry`],
    /// so consumers that only see the body still have everything they need.
    pub properties: HashMap<String, String>,
}

/// Envelope written to the wire as the message payload.
///
/// Wraps the original node response (or, for raw transactions, the
/// chain-formatted hex string) together with the metadata a consumer needs
/// to route, filter or dedup without parsing the inner value. Compound field
/// names follow the same camelCase convention as
/// [`crate::notify::Notification`], so a downstream that already speaks one
/// Dshackle Archive JSON flavour stays consistent across both.
///
/// `value` is held as a [`RawValue`] so the original node JSON is embedded
/// byte-for-byte instead of being parsed-and-reserialized.
#[derive(Debug, Serialize)]
struct Entry<'a> {
    /// Blockchain id (`ETH`, `BTC`, …) — mirrors `blockchain_id` on the row.
    blockchain: &'a str,
    /// Block timestamp as reported by the node, serialized as RFC 3339.
    timestamp: DateTime<Utc>,
    /// Logical table this row belongs to: `blocks`, `transactions`, or
    /// `traces`. Plural matches the canonical table naming used for the
    /// Avro files and the JSON layout's per-kind directories. Serialized
    /// via [`serialize_table`] to call [`DataKind::table`] directly,
    /// decoupling Entry's on-the-wire format from any future change to
    /// `DataKind`'s default `serde(rename)` (which is also used by
    /// `Notification` and could drift).
    #[serde(serialize_with = "serialize_table")]
    table: DataKind,
    /// Field label — same value as the enclosing message's topic suffix.
    field: &'static str,
    /// Block height.
    height: u64,
    /// Block hash. Distinguishes same-height re-orgs.
    #[serde(rename = "blockId")]
    block_id: &'a str,
    /// Parent block hash.
    #[serde(rename = "parentId", skip_serializing_if = "Option::is_none")]
    parent_id: Option<&'a str>,
    /// Transaction index within the block. Present on tx/trace rows.
    #[serde(rename = "txIndex", skip_serializing_if = "Option::is_none")]
    tx_index: Option<u64>,
    /// Total number of transactions in the block — pairs with `txIndex`
    /// to give a tx/trace consumer its position (`N` of `txCount`), and
    /// surfaces the block's tx volume on block rows.
    #[serde(rename = "txCount", skip_serializing_if = "Option::is_none")]
    tx_count: Option<u64>,
    /// Transaction id (hash). Present on tx/trace rows.
    #[serde(rename = "txId", skip_serializing_if = "Option::is_none")]
    tx_id: Option<&'a str>,
    /// Uncle index. Present on Ethereum uncle messages only.
    #[serde(rename = "uncleIndex", skip_serializing_if = "Option::is_none")]
    uncle_index: Option<u8>,
    /// Original node response. JSON for `*.json` fields; a JSON string
    /// (chain-prefixed hex) for raw transactions.
    value: &'a RawValue,
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
    // Per-variant data needed to build the message: the JSON-encoded value to
    // embed, whether this field is keyed by `tx_id`, and the uncle index when
    // applicable. The topic label itself comes from [`Field::name`] — there's
    // no per-variant string here, so a new Field variant gets a label "for
    // free" once it's added to [`Field::name`].
    let (value, tx_keyed, uncle_index): (Box<RawValue>, bool, Option<u8>) = match field {
        Field::BlockJson(bytes) => (raw_value_from_bytes(bytes)?, false, None),
        Field::Uncle { index, json } => (raw_value_from_bytes(json)?, false, Some(*index)),
        Field::TxJson(bytes) => (raw_value_from_bytes(bytes)?, true, None),
        Field::TxRaw(bytes) => (raw_value_from_tx_raw(bytes, row.blockchain_type), true, None),
        Field::Receipt(bytes) => (raw_value_from_bytes(bytes)?, true, None),
        // From/To duplicate values already inside the tx JSON; intentionally
        // skipped, matching the JSON-file layout. They still carry a name on
        // [`Field`] for completeness, just no producer is registered for them.
        Field::From(_) | Field::To(_) => return None,
        Field::Trace(bytes) => (raw_value_from_bytes(bytes)?, true, None),
        Field::StateDiff(bytes) => (raw_value_from_bytes(bytes)?, true, None),
    };

    let label = field.topic_label();
    let tx_id = if tx_keyed { row.tx_id.as_deref() } else { None };

    let entry = Entry {
        blockchain: &row.blockchain_id,
        timestamp: row.timestamp,
        table: row.kind,
        field: label,
        height: row.height,
        block_id: &row.block_id,
        parent_id: row.parent_id.as_deref(),
        tx_index: row.tx_index,
        tx_count: row.tx_count,
        tx_id,
        uncle_index,
        value: &value,
    };

    let payload = match serde_json::to_vec(&entry) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(field = label, error = %e, "Failed to serialize stream entry");
            return None;
        }
    };

    let mut properties = HashMap::new();
    properties.insert("timestamp".to_string(), row.timestamp.to_rfc3339());
    properties.insert("height".to_string(), row.height.to_string());
    properties.insert("block-id".to_string(), row.block_id.clone());
    properties.insert(
        "dedup-key".to_string(),
        dedup_key(row, label, tx_id, uncle_index),
    );

    Some(StreamMessage {
        field: label,
        payload,
        partition_key: row.height,
        properties,
    })
}

/// Serialize a [`DataKind`] as its plural table name (see
/// [`DataKind::table`]). Used on [`Entry::table`] via
/// `#[serde(serialize_with = ...)]` so the wire format is pinned to
/// `DataKind::table()` rather than the derive's `#[serde(rename)]`.
fn serialize_table<S: Serializer>(kind: &DataKind, ser: S) -> Result<S::Ok, S::Error> {
    ser.serialize_str(kind.table())
}

/// Wrap bytes from a node JSON response as a [`RawValue`] without
/// reserializing. Returns `None` if the bytes are not valid UTF-8 or not
/// valid JSON — that shouldn't happen for live node data, but a single
/// corrupt row shouldn't poison the whole stream.
fn raw_value_from_bytes(bytes: &[u8]) -> Option<Box<RawValue>> {
    let s = std::str::from_utf8(bytes).ok()?;
    RawValue::from_string(s.to_string()).ok()
}

/// Build a [`RawValue`] containing a JSON string of the hex-encoded raw tx,
/// with the chain-appropriate prefix (`0x` for Ethereum, none for Bitcoin).
fn raw_value_from_tx_raw(bytes: &[u8], blockchain_type: BlockchainType) -> Box<RawValue> {
    let hex_str = hex::encode(bytes);
    let s = match blockchain_type {
        BlockchainType::Ethereum => format!("0x{}", hex_str),
        BlockchainType::Bitcoin => hex_str,
    };
    // Infallible: a `String` always serializes to a valid JSON value.
    to_raw_value(&s).expect("string serializes to RawValue")
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
    use serde_json::Value;
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
            tx_count: Some(12),
            fields,
        }
    }

    fn parse(payload: &[u8]) -> Value {
        serde_json::from_slice(payload).expect("payload is valid JSON")
    }

    #[test]
    fn block_row_emits_blocks_and_uncle_messages() {
        let r = row(
            DataKind::Blocks,
            None,
            vec![
                Field::BlockJson(b"{\"h\":1}".to_vec()),
                Field::Uncle { index: 0, json: b"{\"u\":0}".to_vec() },
                Field::Uncle { index: 1, json: b"{\"u\":1}".to_vec() },
            ],
        );
        let msgs = encode_row(&r);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].field, "blocks");
        assert_eq!(msgs[1].field, "blocks-uncles");
        let u0 = parse(&msgs[1].payload);
        assert_eq!(u0["uncleIndex"], 0);
        let u1 = parse(&msgs[2].payload);
        assert_eq!(u1["uncleIndex"], 1);
    }

    #[test]
    fn tx_row_emits_per_field_messages_with_metadata() {
        let r = row(
            DataKind::Transactions,
            Some("0xabc"),
            vec![
                Field::TxJson(b"{\"a\":1}".to_vec()),
                Field::TxRaw(vec![0xde, 0xad, 0xbe, 0xef]),
                Field::Receipt(b"{\"r\":1}".to_vec()),
                Field::From("0xfrom".to_string()),
                Field::To("0xto".to_string()),
            ],
        );
        let msgs = encode_row(&r);
        // From/To are intentionally skipped.
        assert_eq!(msgs.len(), 3);
        let tx_msg = msgs.iter().find(|m| m.field == "tx-json").unwrap();
        assert_eq!(tx_msg.partition_key, 100);
        let entry = parse(&tx_msg.payload);
        assert_eq!(entry["blockchain"], "ETH");
        assert_eq!(entry["table"], "transactions");
        assert_eq!(entry["field"], "tx-json");
        assert_eq!(entry["height"], 100);
        assert_eq!(entry["blockId"], "0xblock");
        assert_eq!(entry["parentId"], "0xparent");
        assert_eq!(entry["txIndex"], 7);
        assert_eq!(entry["txId"], "0xabc");
        // Inner value is embedded as JSON, not as a string.
        assert_eq!(entry["value"], serde_json::json!({"a": 1}));
        // Header subset still surfaced for broker-side filtering.
        assert_eq!(tx_msg.properties.get("height").unwrap(), "100");
        assert_eq!(tx_msg.properties.get("block-id").unwrap(), "0xblock");
        assert_eq!(
            tx_msg.properties.get("dedup-key").unwrap(),
            "tx-json:0xblock:tx-0xabc"
        );
        // Properties no longer carry the full envelope.
        assert!(tx_msg.properties.get("blockchain").is_none());
        assert!(tx_msg.properties.get("table").is_none());
        assert!(tx_msg.properties.get("field").is_none());
        assert!(tx_msg.properties.get("tx-index").is_none());

        // Raw tx is wrapped as a JSON string with the `0x` prefix for Ethereum.
        let raw_msg = msgs.iter().find(|m| m.field == "tx-raw").unwrap();
        let raw_entry = parse(&raw_msg.payload);
        assert_eq!(raw_entry["value"], "0xdeadbeef");
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
        let entry = parse(&msgs[0].payload);
        assert_eq!(entry["value"], "deadbeef");
    }

    #[test]
    fn trace_row_emits_calls_and_statediff_messages() {
        let r = row(
            DataKind::TransactionTraces,
            Some("0xabc"),
            vec![
                Field::Trace(b"{\"t\":1}".to_vec()),
                Field::StateDiff(b"{\"s\":1}".to_vec()),
            ],
        );
        let msgs = encode_row(&r);
        assert_eq!(msgs.len(), 2);
        let trace = msgs.iter().find(|m| m.field == "trace-calls").unwrap();
        assert_eq!(
            trace.properties.get("dedup-key").unwrap(),
            "trace-calls:0xblock:tx-0xabc"
        );
        let trace_entry = parse(&trace.payload);
        assert_eq!(trace_entry["table"], "traces");
        assert_eq!(trace_entry["value"], serde_json::json!({"t": 1}));
        let state = msgs.iter().find(|m| m.field == "trace-statediff").unwrap();
        assert_eq!(
            state.properties.get("dedup-key").unwrap(),
            "trace-statediff:0xblock:tx-0xabc"
        );
        let state_entry = parse(&state.payload);
        assert_eq!(state_entry["value"], serde_json::json!({"s": 1}));
    }

    #[test]
    fn uncle_payload_carries_uncle_index() {
        let r = row(
            DataKind::Blocks,
            None,
            vec![Field::Uncle { index: 1, json: b"{\"u\":1}".to_vec() }],
        );
        let msgs = encode_row(&r);
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            msgs[0].properties.get("dedup-key").unwrap(),
            "blocks-uncles:0xblock:uncle-1"
        );
        let entry = parse(&msgs[0].payload);
        assert_eq!(entry["uncleIndex"], 1);
        // Block-kind row → no tx-* fields in the envelope.
        assert!(entry.get("txIndex").is_none());
        assert!(entry.get("txId").is_none());
    }

    #[test]
    fn partition_key_is_height() {
        let r = row(
            DataKind::Blocks,
            None,
            vec![Field::BlockJson(b"{\"h\":1}".to_vec())],
        );
        let msgs = encode_row(&r);
        assert_eq!(msgs[0].partition_key, 100);
    }

    #[test]
    fn timestamp_serializes_as_iso_8601() {
        // Real Ethereum block timestamp value (0x689aad27 = 1754967335 s
        // since epoch) — matches eth_getBlockByNumber's `timestamp` field
        // for block 23110555 on mainnet.
        let ts = Utc.timestamp_opt(0x689aad27, 0).unwrap();
        let mut r = row(
            DataKind::Blocks,
            None,
            vec![Field::BlockJson(b"{\"h\":1}".to_vec())],
        );
        r.timestamp = ts;
        let msgs = encode_row(&r);
        let entry = parse(&msgs[0].payload);
        // chrono's serde impl uses the `Z` form for UTC...
        assert_eq!(entry["timestamp"], "2025-08-12T02:55:35Z");
        // ...whereas `to_rfc3339()` (what we use for the broker header)
        // spells the same offset as `+00:00`. Both are valid RFC 3339; the
        // test pins the current behaviour so a future codec swap doesn't
        // silently change the on-the-wire format.
        assert_eq!(
            msgs[0].properties.get("timestamp").map(|s| s.as_str()),
            Some("2025-08-12T02:55:35+00:00")
        );
    }

    /// Visual snapshot: serializing a row whose value is a JSON object (the
    /// common case — block JSON, tx JSON, receipts, traces, state diff)
    /// must produce the exact envelope shape consumers depend on. Pinning
    /// the literal output here makes any accidental field rename, reorder
    /// or whitespace drift fail loudly and be reviewable by eye in the diff.
    #[test]
    fn serializes_object_value_to_expected_json() {
        let r = ArchiveRow {
            kind: DataKind::Transactions,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc.timestamp_opt(0, 0).unwrap(),
            height: 23110555,
            block_id: "0xbbb".to_string(),
            timestamp: Utc.timestamp_opt(0x689aad27, 0).unwrap(),
            parent_id: Some("0xparent".to_string()),
            tx_index: Some(3),
            tx_id: Some("0xaaa".to_string()),
            tx_count: Some(25),
            fields: vec![Field::TxJson(
                br#"{"hash":"0xaaa","nonce":"0x1","input":"0x"}"#.to_vec(),
            )],
        };
        let msgs = encode_row(&r);
        assert_eq!(msgs.len(), 1);
        let payload = std::str::from_utf8(&msgs[0].payload).expect("utf-8 json");
        assert_eq!(
            payload,
            r#"{"blockchain":"ETH","timestamp":"2025-08-12T02:55:35Z","table":"transactions","field":"tx-json","height":23110555,"blockId":"0xbbb","parentId":"0xparent","txIndex":3,"txCount":25,"txId":"0xaaa","value":{"hash":"0xaaa","nonce":"0x1","input":"0x"}}"#
        );
    }

    /// Visual snapshot for the raw-tx variant: `value` is a JSON *string*
    /// (the chain-prefixed hex), not an object. This pins the envelope's
    /// behaviour for non-object values so a future regression that wraps
    /// the hex in `{...}` or strips the prefix is caught here.
    #[test]
    fn serializes_string_value_to_expected_json() {
        let r = ArchiveRow {
            kind: DataKind::Transactions,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc.timestamp_opt(0, 0).unwrap(),
            height: 23110555,
            block_id: "0xbbb".to_string(),
            timestamp: Utc.timestamp_opt(0x689aad27, 0).unwrap(),
            parent_id: Some("0xparent".to_string()),
            tx_index: Some(3),
            tx_id: Some("0xaaa".to_string()),
            tx_count: Some(25),
            fields: vec![Field::TxRaw(vec![0xde, 0xad, 0xbe, 0xef])],
        };
        let msgs = encode_row(&r);
        assert_eq!(msgs.len(), 1);
        let payload = std::str::from_utf8(&msgs[0].payload).expect("utf-8 json");
        assert_eq!(
            payload,
            r#"{"blockchain":"ETH","timestamp":"2025-08-12T02:55:35Z","table":"transactions","field":"tx-raw","height":23110555,"blockId":"0xbbb","parentId":"0xparent","txIndex":3,"txCount":25,"txId":"0xaaa","value":"0xdeadbeef"}"#
        );
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
            tx_count: None,
            fields: vec![Field::TxJson(b"{\"v\":1}".to_vec())],
        };
        let reorged = ArchiveRow {
            block_id: "0xBBB".to_string(),
            fields: vec![Field::TxJson(b"{\"v\":2}".to_vec())],
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
    /// variant must appear in the maximal
    /// [`topic_labels_for`](crate::formats::topics::topic_labels_for) output —
    /// otherwise the storage layer wouldn't have pre-created a producer for
    /// it and `append` would fail. Uses Ethereum + all-options as the
    /// superset since that's the largest possible producer set.
    #[test]
    fn every_encoded_field_has_a_corresponding_topic() {
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
            tx_count: None,
            fields: vec![
                Field::BlockJson(b"{}".to_vec()),
                Field::Uncle { index: 0, json: b"{}".to_vec() },
                Field::TxJson(b"{}".to_vec()),
                Field::TxRaw(vec![]),
                Field::Receipt(b"{}".to_vec()),
                Field::From("from".to_string()),
                Field::To("to".to_string()),
                Field::Trace(b"{}".to_vec()),
                Field::StateDiff(b"{}".to_vec()),
            ],
        };
        use crate::archiver::datakind::{BlockOptions, DataOptions, TraceOptions, TxOptions};
        let max_options = DataOptions {
            overwrite: true,
            block: Some(BlockOptions::default()),
            tx: Some(TxOptions::default()),
            trace: Some(TraceOptions {
                include_trace: true,
                include_state_diff: true,
            }),
        };
        let produced: HashSet<&'static str> =
            encode_row(&r).into_iter().map(|m| m.field).collect();
        let declared: HashSet<&'static str> =
            crate::formats::topics::topic_labels_for(BlockchainType::Ethereum, &max_options)
                .into_iter()
                .collect();
        // The two sides must match exactly: any label encode_row produces
        // for the full Ethereum row must be pre-created by Pulsar, and we
        // shouldn't be pre-creating dead topics either.
        assert_eq!(produced, declared);
    }
}
