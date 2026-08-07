// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Where archived data landed, as reported inside a
//! [`Notification`](crate::notify::Notification).
//!
//! Each target type addresses its output differently — a row-batched file is
//! one URL, the JSON layout is a set of per-field files, a streaming broker is
//! a set of message ids. [`Location`] carries all of them under a single
//! `type`-tagged JSON object, so consumers can dispatch on `location.type` and
//! new target types extend the enum without breaking the overall notification
//! schema.

use serde::{Deserialize, Serialize};
use crate::archiver::range::Range;

///
/// Address of the archived data inside the target storage.
///
/// Serialized with a `type` tag so a JSON consumer can distinguish the
/// location flavours, and ignore (or fail on) flavours added after it was
/// written.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Location {
    /// A single row-batched file — the Avro layout (and any future format
    /// that keeps one file per (kind, range)).
    File {
        /// URL of the file, e.g. `s3://bucket/eth/012000000/range-012345000_012345999.txes.avro`
        url: String,
    },
    /// Per-field files of a single height — the JSON layout. One group per
    /// block or per transaction, each pointing to the individual files.
    Files {
        files: Vec<FileGroup>,
    },
    /// Messages published to per-field topics of a streaming broker
    /// (Apache Pulsar). Each message names its actual topic — a consumer
    /// can address it directly, with no topic-name construction on its side.
    Pulsar {
        messages: Vec<MessageRef>,
    },
    /// Same as [`Location::Pulsar`], for messages published to Apache Kafka.
    /// Kept as its own variant rather than a shared "broker" one so a consumer
    /// knows how to read `messageId` without guessing.
    Kafka {
        messages: Vec<MessageRef>,
    },
}

///
/// Files produced for one logical entity — a block or a single transaction —
/// under the per-field JSON layout. Only the fields that were actually
/// produced are present in the JSON.
#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FileGroup {
    /// Transaction id. Present on per-transaction groups only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_id: Option<String>,
    /// URL of the block JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<String>,
    /// URLs of the uncle JSONs, in uncle-index order.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub uncles: Vec<String>,
    /// URL of the transaction JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,
    /// URL of the raw transaction (hex).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>,
    /// URL of the transaction receipt JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt: Option<String>,
    /// URL of the `callTracer` trace JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calls: Option<String>,
    /// URL of the `prestateTracer` trace JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_diff: Option<String>,
}

///
/// The [`FileGroup`] field a produced file belongs to. Lets the format layer
/// say _what_ a file is without the writer matching on filenames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileSlot {
    Block,
    Uncle,
    Tx,
    Raw,
    Receipt,
    Calls,
    StateDiff,
}

impl FileGroup {
    pub fn set(&mut self, slot: FileSlot, url: String) {
        match slot {
            FileSlot::Block => self.block = Some(url),
            FileSlot::Uncle => self.uncles.push(url),
            FileSlot::Tx => self.tx = Some(url),
            FileSlot::Raw => self.raw = Some(url),
            FileSlot::Receipt => self.receipt = Some(url),
            FileSlot::Calls => self.calls = Some(url),
            FileSlot::StateDiff => self.state_diff = Some(url),
        }
    }

    /// True when the group points to no files at all (`tx_id` is metadata,
    /// not a file). Such a group carries nothing to notify about.
    pub fn is_empty(&self) -> bool {
        self.block.is_none()
            && self.uncles.is_empty()
            && self.tx.is_none()
            && self.raw.is_none()
            && self.receipt.is_none()
            && self.calls.is_none()
            && self.state_diff.is_none()
    }
}

///
/// One message published to a streaming broker.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageRef {
    /// Full topic name the message went to.
    pub topic: String,
    /// Field label — the topic suffix (`blocks`, `tx-json`, ...), same values
    /// as the `field` of the message payload itself.
    pub field: String,
    /// Transaction id. Present on per-transaction messages only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_id: Option<String>,
    /// Broker message id. For Pulsar: `ledgerId:entryId:partition[:batchIndex]`;
    /// for Kafka: `partition:offset`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
}

///
/// The files one archived row produced, tagged with the position of the row,
/// so rows appended in fetch-completion order can be regrouped per height.
#[derive(Clone, Debug)]
pub struct RowFiles {
    pub height: u64,
    /// Transaction index within the block; `None` for the block row itself.
    pub tx_index: Option<u64>,
    pub group: FileGroup,
}

impl Location {
    ///
    /// Group per-row file groups into one `files` location per height, in
    /// chain-natural order (block entry first, then transactions by index).
    ///
    /// This is what keeps the JSON layout at no more than one notification
    /// per kind per height, no matter how many files a height produced.
    pub fn files_per_height(mut rows: Vec<RowFiles>) -> Vec<(Range, Location)> {
        // file targets append rows as fetches complete, not in chain order
        rows.sort_by_key(|r| (r.height, r.tx_index.map(|i| i + 1).unwrap_or(0)));

        let mut by_height: std::collections::BTreeMap<u64, Vec<FileGroup>> = std::collections::BTreeMap::new();
        for row in rows {
            by_height.entry(row.height).or_default().push(row.group);
        }
        by_height.into_iter()
            .map(|(height, files)| (Range::Single(height.into()), Location::Files { files }))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_location_json() {
        let location = Location::File {
            url: "s3://bucket/eth/012000000/range-012345000_012345999.txes.avro".to_string(),
        };
        let json = serde_json::to_string(&location).unwrap();
        assert_eq!(
            json,
            r#"{"type":"file","url":"s3://bucket/eth/012000000/range-012345000_012345999.txes.avro"}"#
        );
    }

    #[test]
    fn files_location_json() {
        let mut group = FileGroup {
            tx_id: Some("0xabc".to_string()),
            ..Default::default()
        };
        group.set(FileSlot::Tx, "s3://b/eth/.../tx-0xabc.json".to_string());
        group.set(FileSlot::Raw, "s3://b/eth/.../raw-0xabc.hex".to_string());
        group.set(FileSlot::Receipt, "s3://b/eth/.../receipt-0xabc.json".to_string());
        let location = Location::Files { files: vec![group] };

        let json = serde_json::to_string(&location).unwrap();
        assert_eq!(
            json,
            r#"{"type":"files","files":[{"txId":"0xabc","tx":"s3://b/eth/.../tx-0xabc.json","raw":"s3://b/eth/.../raw-0xabc.hex","receipt":"s3://b/eth/.../receipt-0xabc.json"}]}"#
        );
    }

    #[test]
    fn block_files_location_json() {
        let mut group = FileGroup::default();
        group.set(FileSlot::Block, "file:///archive/.../block.json".to_string());
        group.set(FileSlot::Uncle, "file:///archive/.../uncle-0.json".to_string());
        group.set(FileSlot::Uncle, "file:///archive/.../uncle-1.json".to_string());
        let location = Location::Files { files: vec![group] };

        let json = serde_json::to_string(&location).unwrap();
        assert_eq!(
            json,
            r#"{"type":"files","files":[{"block":"file:///archive/.../block.json","uncles":["file:///archive/.../uncle-0.json","file:///archive/.../uncle-1.json"]}]}"#
        );
    }

    #[test]
    fn pulsar_location_json() {
        let location = Location::Pulsar {
            messages: vec![MessageRef {
                topic: "persistent://public/default/archive-eth-blocks".to_string(),
                field: "blocks".to_string(),
                tx_id: None,
                message_id: Some("125:4:-1".to_string()),
            }],
        };
        let json = serde_json::to_string(&location).unwrap();
        assert_eq!(
            json,
            r#"{"type":"pulsar","messages":[{"topic":"persistent://public/default/archive-eth-blocks","field":"blocks","messageId":"125:4:-1"}]}"#
        );
    }

    #[test]
    fn kafka_location_json() {
        let location = Location::Kafka {
            messages: vec![MessageRef {
                topic: "archive-eth-tx-json".to_string(),
                field: "tx-json".to_string(),
                tx_id: Some("0xabc".to_string()),
                message_id: Some("2:1041".to_string()),
            }],
        };
        let json = serde_json::to_string(&location).unwrap();
        assert_eq!(
            json,
            r#"{"type":"kafka","messages":[{"topic":"archive-eth-tx-json","field":"tx-json","txId":"0xabc","messageId":"2:1041"}]}"#
        );
    }

    #[test]
    fn parses_back_by_type_tag() {
        let json = r#"{"type":"file","url":"s3://bucket/file.avro"}"#;
        let location: Location = serde_json::from_str(json).unwrap();
        assert_eq!(location, Location::File { url: "s3://bucket/file.avro".to_string() });
    }

    #[test]
    fn groups_rows_per_height_in_chain_order() {
        let tx = |height: u64, index: u64, id: &str| RowFiles {
            height,
            tx_index: Some(index),
            group: FileGroup {
                tx_id: Some(id.to_string()),
                ..Default::default()
            },
        };
        let block = |height: u64| RowFiles {
            height,
            tx_index: None,
            group: FileGroup::default(),
        };

        // out of order on purpose: completion order is not chain order
        let rows = vec![tx(101, 1, "0xb"), tx(100, 0, "0xa"), block(101), tx(101, 0, "0xc")];
        let locations = Location::files_per_height(rows);

        assert_eq!(locations.len(), 2);
        assert_eq!(locations[0].0, Range::Single(100.into()));
        assert_eq!(locations[1].0, Range::Single(101.into()));
        match &locations[1].1 {
            Location::Files { files } => {
                assert_eq!(files.len(), 3);
                assert_eq!(files[0].tx_id, None);
                assert_eq!(files[1].tx_id, Some("0xc".to_string()));
                assert_eq!(files[2].tx_id, Some("0xb".to_string()));
            }
            other => panic!("Expected files location, got {:?}", other),
        }
    }
}
