// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Avro encoding of [`ArchiveRow`]s.
//!
//! [`encode_row`] converts a format-neutral row into an `apache_avro::types::Record`
//! that conforms to the schema for the row's [`DataKind`]. The Avro schemas
//! themselves live in [`schema`] and are the same schemas the archive has used since
//! day one — we keep the on-disk format byte-compatible with prior releases.

pub mod schema;

use anyhow::{anyhow, Result};
use apache_avro::types::{Record, Value};

use crate::archiver::datakind::DataKind;
use crate::record::{ArchiveRow, Field};

pub use schema::{BLOCK_SCHEMA, TX_SCHEMA, TX_TRACE_SCHEMA};

/// Returns the Avro schema for the given [`DataKind`].
pub fn schema_for(kind: DataKind) -> &'static apache_avro::Schema {
    match kind {
        DataKind::Blocks => &*BLOCK_SCHEMA,
        DataKind::Transactions => &*TX_SCHEMA,
        DataKind::TransactionTraces => &*TX_TRACE_SCHEMA,
    }
}

/// Encode an [`ArchiveRow`] into an Avro [`Record`] ready to be appended to an Avro writer.
///
/// The row's [`DataKind`] selects the schema. Common columns (blockchain, height, timestamps)
/// come from the row's top-level fields; per-kind columns are matched against the row's
/// [`Field`] variants. Nullable columns default to `Value::Union(0, Null)` when the row
/// omits them.
pub fn encode_row(row: &ArchiveRow) -> Result<Record<'static>> {
    match row.kind {
        DataKind::Blocks => encode_block(row),
        DataKind::Transactions => encode_tx(row),
        DataKind::TransactionTraces => encode_trace(row),
    }
}

fn encode_block(row: &ArchiveRow) -> Result<Record<'static>> {
    let mut record = Record::new(&BLOCK_SCHEMA)
        .ok_or_else(|| anyhow!("Failed to allocate Avro record for Blocks"))?;
    set_common(&mut record, row);
    record.put(
        "parentId",
        row.parent_id.clone().unwrap_or_default(),
    );

    let block_json = row
        .fields
        .iter()
        .find_map(|f| match f {
            Field::BlockJson(b) => Some(b.clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow!("Block row missing BlockJson field"))?;
    record.put("json", Value::Bytes(block_json));

    let uncles_count = row
        .fields
        .iter()
        .filter(|f| matches!(f, Field::Uncle { .. }))
        .count() as i32;
    record.put("unclesCount", uncles_count);

    // The schema supports up to two uncle JSON columns. Both are nullable.
    for i in 0..=1u8 {
        let column = format!("uncle{}Json", i);
        let uncle = row.fields.iter().find_map(|f| match f {
            Field::Uncle { index, json } if *index == i => Some(json.clone()),
            _ => None,
        });
        match uncle {
            Some(bytes) => record.put(
                column.as_str(),
                Value::Union(1, Box::new(Value::Bytes(bytes))),
            ),
            None => record.put(column.as_str(), Value::Union(0, Box::new(Value::Null))),
        }
    }

    Ok(record)
}

fn encode_tx(row: &ArchiveRow) -> Result<Record<'static>> {
    let mut record = Record::new(&TX_SCHEMA)
        .ok_or_else(|| anyhow!("Failed to allocate Avro record for Transactions"))?;
    set_common(&mut record, row);
    let tx_index = row
        .tx_index
        .ok_or_else(|| anyhow!("Transaction row missing tx_index"))?;
    let txid = row
        .tx_id
        .as_ref()
        .ok_or_else(|| anyhow!("Transaction row missing tx_id"))?
        .clone();
    record.put("index", tx_index as i64);
    record.put("txid", txid);

    let tx_json = row
        .fields
        .iter()
        .find_map(|f| match f {
            Field::TxJson(b) => Some(b.clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow!("Transaction row missing TxJson field"))?;
    record.put("json", Value::Bytes(tx_json));

    let tx_raw = row
        .fields
        .iter()
        .find_map(|f| match f {
            Field::TxRaw(b) => Some(b.clone()),
            _ => None,
        })
        .ok_or_else(|| anyhow!("Transaction row missing TxRaw field"))?;
    record.put("raw", Value::Bytes(tx_raw));

    let from = row.fields.iter().find_map(|f| match f {
        Field::From(s) => Some(s.clone()),
        _ => None,
    });
    record.put("from", optional_string(from));

    let to = row.fields.iter().find_map(|f| match f {
        Field::To(s) => Some(s.clone()),
        _ => None,
    });
    record.put("to", optional_string(to));

    let receipt = row.fields.iter().find_map(|f| match f {
        Field::Receipt(b) => Some(b.clone()),
        _ => None,
    });
    record.put("receiptJson", optional_bytes(receipt));

    Ok(record)
}

fn encode_trace(row: &ArchiveRow) -> Result<Record<'static>> {
    let mut record = Record::new(&TX_TRACE_SCHEMA)
        .ok_or_else(|| anyhow!("Failed to allocate Avro record for TransactionTraces"))?;
    set_common(&mut record, row);
    let tx_index = row
        .tx_index
        .ok_or_else(|| anyhow!("Trace row missing tx_index"))?;
    let txid = row
        .tx_id
        .as_ref()
        .ok_or_else(|| anyhow!("Trace row missing tx_id"))?
        .clone();
    record.put("index", tx_index as i64);
    record.put("txid", txid);

    let trace = row.fields.iter().find_map(|f| match f {
        Field::Trace(b) => Some(b.clone()),
        _ => None,
    });
    record.put("traceJson", optional_bytes(trace));

    let state_diff = row.fields.iter().find_map(|f| match f {
        Field::StateDiff(b) => Some(b.clone()),
        _ => None,
    });
    record.put("stateDiffJson", optional_bytes(state_diff));

    Ok(record)
}

fn set_common(record: &mut Record<'_>, row: &ArchiveRow) {
    record.put("blockchainType", row.blockchain_type.as_avro_symbol());
    record.put("blockchainId", row.blockchain_id.clone());
    record.put("archiveTimestamp", row.archive_ts.timestamp_millis());
    record.put("height", row.height as i64);
    record.put("blockId", row.block_id.clone());
    record.put("timestamp", row.timestamp.timestamp_millis());
}

/// Encode an optional `["null","string"]` Avro union from an `Option<String>`.
fn optional_string(value: Option<String>) -> Value {
    match value {
        Some(s) => Value::Union(1, Box::new(Value::String(s))),
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

/// Encode an optional `["null","bytes"]` Avro union from an `Option<Vec<u8>>`.
fn optional_bytes(value: Option<Vec<u8>>) -> Value {
    match value {
        Some(b) => Value::Union(1, Box::new(Value::Bytes(b))),
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use crate::record::BlockchainType;

    fn sample_block() -> ArchiveRow {
        ArchiveRow {
            kind: DataKind::Blocks,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: chrono::Utc.timestamp_millis_opt(1_000).unwrap(),
            height: 12345,
            block_id: "0xblock".to_string(),
            timestamp: chrono::Utc.timestamp_millis_opt(2_000).unwrap(),
            parent_id: Some("0xparent".to_string()),
            tx_index: None,
            tx_id: None,
            tx_count: None,
            fields: vec![Field::BlockJson(b"{}".to_vec())],
        }
    }

    #[test]
    fn block_encodes_with_common_fields() {
        let row = sample_block();
        let record = encode_row(&row).unwrap();
        let fields: std::collections::HashMap<_, _> =
            record.fields.iter().map(|(n, v)| (n.clone(), v.clone())).collect();
        assert_eq!(fields["blockchainId"], Value::String("ETH".to_string()));
        assert_eq!(fields["height"], Value::Long(12345));
        assert_eq!(fields["parentId"], Value::String("0xparent".to_string()));
        assert_eq!(fields["unclesCount"], Value::Int(0));
        assert_eq!(fields["uncle0Json"], Value::Union(0, Box::new(Value::Null)));
        assert_eq!(fields["uncle1Json"], Value::Union(0, Box::new(Value::Null)));
    }

    #[test]
    fn block_with_uncles_sets_count_and_payloads() {
        let mut row = sample_block();
        row.fields.push(Field::Uncle { index: 0, json: b"u0".to_vec() });
        row.fields.push(Field::Uncle { index: 1, json: b"u1".to_vec() });
        let record = encode_row(&row).unwrap();
        let fields: std::collections::HashMap<_, _> =
            record.fields.iter().map(|(n, v)| (n.clone(), v.clone())).collect();
        assert_eq!(fields["unclesCount"], Value::Int(2));
        assert_eq!(
            fields["uncle0Json"],
            Value::Union(1, Box::new(Value::Bytes(b"u0".to_vec())))
        );
        assert_eq!(
            fields["uncle1Json"],
            Value::Union(1, Box::new(Value::Bytes(b"u1".to_vec())))
        );
    }

    #[test]
    fn tx_requires_index_and_id() {
        let row = ArchiveRow {
            kind: DataKind::Transactions,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: chrono::Utc.timestamp_millis_opt(1_000).unwrap(),
            height: 12345,
            block_id: "0xblock".to_string(),
            timestamp: chrono::Utc.timestamp_millis_opt(2_000).unwrap(),
            parent_id: None,
            tx_index: Some(3),
            tx_id: Some("0xtx".to_string()),
            tx_count: None,
            fields: vec![
                Field::TxJson(b"{}".to_vec()),
                Field::TxRaw(vec![1, 2, 3]),
                Field::From("0xfrom".to_string()),
            ],
        };
        let record = encode_row(&row).unwrap();
        let fields: std::collections::HashMap<_, _> =
            record.fields.iter().map(|(n, v)| (n.clone(), v.clone())).collect();
        assert_eq!(fields["index"], Value::Long(3));
        assert_eq!(fields["txid"], Value::String("0xtx".to_string()));
        assert_eq!(fields["raw"], Value::Bytes(vec![1, 2, 3]));
        assert_eq!(
            fields["from"],
            Value::Union(1, Box::new(Value::String("0xfrom".to_string())))
        );
        assert_eq!(fields["to"], Value::Union(0, Box::new(Value::Null)));
        assert_eq!(fields["receiptJson"], Value::Union(0, Box::new(Value::Null)));
    }
}
