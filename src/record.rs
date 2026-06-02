// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Format-neutral representation of an archive record.
//!
//! `ArchiveRow` is what blockchain providers (e.g., [`crate::blockchain::ethereum::EthereumData`])
//! emit and what target writers (e.g., Avro file, JSON file, Pulsar topic) consume.
//! It carries one logical row (one block, one transaction, or one trace) as a set of
//! typed [`Field`]s. Each output format decides how to lay the fields out:
//!
//! - **Row-batched formats** (Avro, Parquet): collapse the fields into one row,
//!   one column per [`Field`] variant.
//! - **Fan-out formats** (JSON files, Pulsar/Kafka): write each [`Field`] to its
//!   own destination — a separate file or a separate topic.
//!
//! Using a single row type across blockchains keeps format adapters chain-agnostic.
//! Bitcoin rows simply contain a smaller set of [`Field`] variants than Ethereum.

use chrono::{DateTime, Utc};

use crate::archiver::datakind::DataKind;

/// Discriminator for the blockchain family (matches the existing Avro `BlockchainType` enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockchainType {
    Ethereum,
    Bitcoin,
}

impl BlockchainType {
    /// Symbol used in the Avro `BlockchainType` enum field.
    pub fn as_avro_symbol(&self) -> &'static str {
        match self {
            BlockchainType::Ethereum => "ETHEREUM",
            BlockchainType::Bitcoin => "BITCOIN",
        }
    }
}

/// A format-neutral row representing one archive record (block, transaction, or trace).
///
/// One row carries one *unit* of data:
/// - [`DataKind::Blocks`]: one block. `parent_id` is set; `tx_index`/`tx_id` are `None`.
/// - [`DataKind::Transactions`]: one transaction. `tx_index`/`tx_id` are set; `parent_id` is `None`.
/// - [`DataKind::TransactionTraces`]: one trace. `tx_index`/`tx_id` are set; `parent_id` is `None`.
///
/// Per-kind payload lives in [`fields`](ArchiveRow::fields), which carries one or more
/// [`Field`]s. Fan-out formats route fields to separate destinations based on the
/// [`Field`] variant.
#[derive(Debug, Clone)]
pub struct ArchiveRow {
    pub kind: DataKind,
    pub blockchain_type: BlockchainType,
    pub blockchain_id: String,
    /// Wall-clock time at which the archiver produced this row.
    pub archive_ts: DateTime<Utc>,
    pub height: u64,
    pub block_id: String,
    /// Block timestamp as reported by the blockchain node.
    pub timestamp: DateTime<Utc>,

    /// Parent block hash.
    pub parent_id: Option<String>,
    /// Tx/Trace-kind only: transaction index within the block.
    pub tx_index: Option<u64>,
    /// Tx/Trace-kind only: transaction id (hash).
    pub tx_id: Option<String>,

    pub fields: Vec<Field>,
}

/// One typed value attached to an [`ArchiveRow`].
///
/// Each variant pairs a logical field name with the exact value type that makes
/// sense for it: JSON bytes for JSON fields, raw binary for raw payloads, a
/// string for textual fields. This makes invalid combinations (e.g. "From
/// contains JSON bytes") unrepresentable at the type level.
///
/// The field is implicitly keyed by the enclosing row's `(height, tx_id?)`.
/// Fan-out formats combine that key with the field name to produce a per-field
/// destination (filename or topic name).
///
/// New blockchains may emit different subsets of these variants; the set is
/// intentionally closed so format adapters can exhaustively match on it.
#[derive(Debug, Clone)]
pub enum Field {
    // ---- Block-kind fields ----
    /// The full JSON of the block as returned by the node.
    BlockJson(Vec<u8>),
    /// Ethereum uncle JSON, keyed by uncle index within the block.
    Uncle { index: u8, json: Vec<u8> },

    // ---- Transaction-kind fields ----
    /// JSON of the transaction as returned by the node.
    TxJson(Vec<u8>),
    /// Raw transaction bytes (binary, not JSON).
    ///
    /// Stored decoded to save memory — the wire format is a hex string, but its
    /// presence/absence of an `0x` prefix is fully determined by the blockchain
    /// (Ethereum uses `0x`, Bitcoin does not), so format adapters can reconstruct
    /// the wire form from the row's [`crate::record::BlockchainType`] when
    /// writing it back out (e.g., the JSON layout writes `raw-<HASH>.hex` as a
    /// hex string with the appropriate prefix).
    TxRaw(Vec<u8>),
    /// Ethereum-only: the transaction receipt JSON.
    Receipt(Vec<u8>),
    /// Ethereum-only: `from` address as a dedicated column for table
    /// formats (Avro). Streaming and per-field JSON skip it — already in
    /// the tx JSON.
    From(String),
    /// Ethereum-only: `to` address. Same usage as [`Field::From`].
    To(String),

    // ---- Trace-kind fields ----
    /// `debug_traceTransaction` with `callTracer`.
    Trace(Vec<u8>),
    /// `debug_traceTransaction` with `prestateTracer`.
    StateDiff(Vec<u8>),
}

impl Field {
    /// Singular content-type identifier — the kind of value this variant
    /// carries, independent of any table context.
    ///
    /// Suitable for callers where the enclosing table is already known
    /// (e.g. directory-based layouts where the table appears in the path):
    /// the name doesn't need to repeat it. Plural is reserved for variants
    /// whose payload is itself a collection (`calls` — the callTracer
    /// returns a nested call tree).
    pub fn name(&self) -> &'static str {
        match self {
            Field::BlockJson(_) => "block",
            Field::Uncle { .. } => "uncle",
            Field::TxJson(_) => "tx",
            Field::TxRaw(_) => "raw",
            Field::Receipt(_) => "receipt",
            Field::From(_) => "from",
            Field::To(_) => "to",
            Field::Trace(_) => "calls",
            Field::StateDiff(_) => "statediff",
        }
    }

    /// Streaming topic suffix for this variant. Used by
    /// [`crate::formats::stream`] as the per-field topic label
    /// (`<prefix>-<topic_label>`), where topics share a flat namespace and
    /// the table context isn't otherwise carried. Keep the values stable
    /// — they're consumer-visible.
    pub fn topic_label(&self) -> &'static str {
        match self {
            Field::BlockJson(_) => "blocks",
            Field::Uncle { .. } => "blocks-uncles",
            Field::TxJson(_) => "tx-json",
            Field::TxRaw(_) => "tx-raw",
            Field::Receipt(_) => "tx-receipts",
            Field::From(_) => "tx-from",
            Field::To(_) => "tx-to",
            Field::Trace(_) => "trace-calls",
            Field::StateDiff(_) => "trace-statediff",
        }
    }
}
