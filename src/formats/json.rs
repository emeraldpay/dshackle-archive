// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! JSON-per-field file format.
//!
//! Each [`Field`] in an [`ArchiveRow`] becomes its own file. The file extension
//! is `.json` when the payload is the original JSON response from the node, and
//! `.hex` for the raw-transaction field (which is a hex string, not JSON).
//!
//! Filenames are derived from the field name (plus, for per-tx fields, the
//! txid):
//!
//! | Field        | Filename                  |
//! |--------------|---------------------------|
//! | BlockJson    | `block.json`              |
//! | Uncle{N}     | `uncle-N.json`            |
//! | TxJson       | `tx-<HASH>.json`          |
//! | TxRaw        | `raw-<HASH>.hex`          |
//! | Receipt      | `receipt-<HASH>.json`     |
//! | Trace        | `trace-<HASH>.json`       |
//! | StateDiff    | `statediff-<HASH>.json`   |
//!
//! `From` and `To` are convenience fields surfaced as separate Avro columns; they
//! are already part of the transaction JSON, so the JSON file layout skips them.
//!
//! Payloads are written **byte-for-byte** as the node returned them — no
//! re-encoding, no whitespace normalization.

use std::collections::HashMap;

use crate::archiver::datakind::{DataKind, DataOptions};
use crate::archiver::range::Range;
use crate::archiver::range_bag::RangeBag;
use crate::record::{ArchiveRow, BlockchainType, Field};

/// A single file produced by [`encode_row`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonFieldFile {
    /// Bare filename (no directory component).
    pub filename: String,
    /// Exact bytes to write to the file.
    pub payload: Vec<u8>,
}

/// Convert an [`ArchiveRow`] into the set of per-field files it produces under
/// the JSON layout. The caller decides where (which directory) to write them.
pub fn encode_row(row: &ArchiveRow) -> Vec<JsonFieldFile> {
    let tx_id = row.tx_id.as_deref();
    let blockchain_type = row.blockchain_type;
    row.fields
        .iter()
        .filter_map(|f| encode_field(f, tx_id, blockchain_type))
        .collect()
}

fn encode_field(
    field: &Field,
    tx_id: Option<&str>,
    blockchain_type: BlockchainType,
) -> Option<JsonFieldFile> {
    match field {
        Field::BlockJson(bytes) => Some(JsonFieldFile {
            filename: "block.json".to_string(),
            payload: bytes.clone(),
        }),
        Field::Uncle { index, json } => Some(JsonFieldFile {
            filename: format!("uncle-{}.json", index),
            payload: json.clone(),
        }),
        Field::TxJson(bytes) => tx_id.map(|id| JsonFieldFile {
            filename: format!("tx-{}.json", id),
            payload: bytes.clone(),
        }),
        // Raw transaction is stored decoded to save memory; re-encode to the
        // node's wire format here (Ethereum prefixes with `0x`, Bitcoin does not).
        Field::TxRaw(bytes) => tx_id.map(|id| JsonFieldFile {
            filename: format!("raw-{}.hex", id),
            payload: encode_tx_raw(bytes, blockchain_type),
        }),
        Field::Receipt(bytes) => tx_id.map(|id| JsonFieldFile {
            filename: format!("receipt-{}.json", id),
            payload: bytes.clone(),
        }),
        // Convenience-only fields that are already present inside the parent JSON.
        Field::From(_) | Field::To(_) => None,
        Field::Trace(bytes) => tx_id.map(|id| JsonFieldFile {
            filename: format!("trace-{}.json", id),
            payload: bytes.clone(),
        }),
        Field::StateDiff(bytes) => tx_id.map(|id| JsonFieldFile {
            filename: format!("statediff-{}.json", id),
            payload: bytes.clone(),
        }),
    }
}

fn encode_tx_raw(bytes: &[u8], blockchain_type: BlockchainType) -> Vec<u8> {
    let hex_str = hex::encode(bytes);
    match blockchain_type {
        BlockchainType::Ethereum => format!("0x{}", hex_str).into_bytes(),
        BlockchainType::Bitcoin => hex_str.into_bytes(),
    }
}

/// Per-height completeness check used by the JSON storage backends.
///
/// Given the set of filenames present in a single height's directory, return
/// the [`DataKind`]s that are missing or incomplete given the requested
/// [`DataOptions`]. This is the heuristic the Fix command relies on:
///
/// - **Blocks** is missing when `block.json` is absent.
/// - **Transactions** is missing when no `tx-*.json` or no `raw-*.hex` is
///   present. (Partial-tx coverage is not detected — Fix re-archives the
///   height and the writer skips files that already exist.)
/// - **TransactionTraces** is missing when the requested trace kinds
///   (`trace-*.json` and/or `statediff-*.json` depending on
///   [`DataOptions::trace`]) are absent.
pub fn missing_kinds_at_height<I, S>(entries: I, options: &DataOptions) -> Vec<DataKind>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let entries: Vec<String> = entries
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();
    let mut missing = Vec::new();

    if options.include_block() && !entries.iter().any(|n| n == "block.json") {
        missing.push(DataKind::Blocks);
    }
    if options.include_tx() {
        let has_any_tx = entries
            .iter()
            .any(|n| n.starts_with("tx-") && n.ends_with(".json"));
        let has_any_raw = entries
            .iter()
            .any(|n| n.starts_with("raw-") && n.ends_with(".hex"));
        if !has_any_tx || !has_any_raw {
            missing.push(DataKind::Transactions);
        }
    }
    if options.include_trace() {
        // include_trace() guarantees `trace` is Some.
        let trace_opts = options.trace.as_ref().unwrap();
        let need_trace = trace_opts.include_trace;
        let need_state = trace_opts.include_state_diff;
        let has_any_trace = entries
            .iter()
            .any(|n| n.starts_with("trace-") && n.ends_with(".json"));
        let has_any_state = entries
            .iter()
            .any(|n| n.starts_with("statediff-") && n.ends_with(".json"));
        if (need_trace && !has_any_trace) || (need_state && !has_any_state) {
            missing.push(DataKind::TransactionTraces);
        }
    }

    missing
}

/// Collapse a per-height missing-kinds map into the shape that
/// [`crate::storage::ScanTarget::find_incomplete_tables`] returns: one
/// `(Range, Vec<DataKind>)` entry per contiguous run of heights that share the
/// same missing-kinds set.
pub fn collapse_missing(per_height: HashMap<u64, Vec<DataKind>>) -> Vec<(Range, Vec<DataKind>)> {
    let mut by_kinds: HashMap<Vec<DataKind>, RangeBag> = HashMap::new();
    for (height, kinds) in per_height {
        by_kinds
            .entry(kinds)
            .or_insert_with(RangeBag::new)
            .append(Range::Single(height.into()));
    }
    let mut result = Vec::new();
    for (kinds, bag) in by_kinds {
        for range in bag.compact().ranges {
            result.push((range, kinds.clone()));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

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
            tx_index: tx_id.map(|_| 0),
            tx_id: tx_id.map(|s| s.to_string()),
            fields,
        }
    }

    #[test]
    fn block_row_emits_block_and_uncle_files() {
        let r = row(
            DataKind::Blocks,
            None,
            vec![
                Field::BlockJson(b"BLOCK".to_vec()),
                Field::Uncle { index: 0, json: b"U0".to_vec() },
                Field::Uncle { index: 1, json: b"U1".to_vec() },
            ],
        );
        let files = encode_row(&r);
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].filename, "block.json");
        assert_eq!(files[0].payload, b"BLOCK");
        assert_eq!(files[1].filename, "uncle-0.json");
        assert_eq!(files[2].filename, "uncle-1.json");
    }

    #[test]
    fn tx_row_emits_per_field_files_keyed_by_txid() {
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
        let files = encode_row(&r);
        // From/To are intentionally skipped.
        assert_eq!(files.len(), 3);
        let names: Vec<_> = files.iter().map(|f| f.filename.as_str()).collect();
        assert!(names.contains(&"tx-0xabc.json"));
        assert!(names.contains(&"raw-0xabc.hex"));
        assert!(names.contains(&"receipt-0xabc.json"));
        let raw = files.iter().find(|f| f.filename == "raw-0xabc.hex").unwrap();
        // Ethereum row → `0x` prefix added on the way out.
        assert_eq!(raw.payload, b"0xdeadbeef");
    }

    #[test]
    fn bitcoin_raw_tx_has_no_0x_prefix() {
        let mut r = row(
            DataKind::Transactions,
            Some("abc"),
            vec![Field::TxRaw(vec![0xde, 0xad, 0xbe, 0xef])],
        );
        r.blockchain_type = BlockchainType::Bitcoin;
        let files = encode_row(&r);
        let raw = files.iter().find(|f| f.filename == "raw-abc.hex").unwrap();
        assert_eq!(raw.payload, b"deadbeef");
    }

    #[test]
    fn trace_row_emits_trace_and_statediff_files() {
        let r = row(
            DataKind::TransactionTraces,
            Some("0xabc"),
            vec![
                Field::Trace(b"T".to_vec()),
                Field::StateDiff(b"S".to_vec()),
            ],
        );
        let files = encode_row(&r);
        assert_eq!(files.len(), 2);
        let names: Vec<_> = files.iter().map(|f| f.filename.as_str()).collect();
        assert!(names.contains(&"trace-0xabc.json"));
        assert!(names.contains(&"statediff-0xabc.json"));
    }

    #[test]
    fn per_tx_fields_without_tx_id_are_skipped() {
        let r = row(
            DataKind::Transactions,
            None,
            vec![Field::TxJson(b"X".to_vec())],
        );
        assert!(encode_row(&r).is_empty());
    }
}
