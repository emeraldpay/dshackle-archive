// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Format-neutral representation of an archive record.
//!
//! `ArchiveRow` is what blockchain providers (e.g., [`crate::blockchain::ethereum::EthereumData`])
//! emit and what target writers (e.g., Avro file, JSON file, Pulsar topic) consume.
//! It carries one logical row (one block, one transaction, or one trace) as a set of
//! named fields. Each output format decides how to lay the fields out:
//!
//! - **Row-batched formats** (Avro, Parquet): collapse the fields into one row,
//!   one column per [`FieldName`].
//! - **Fan-out formats** (JSON files, Pulsar/Kafka): write each [`Field`] to its
//!   own destination — a separate file or a separate topic.
//!
//! Using a single row type across blockchains keeps format adapters chain-agnostic.
//! Bitcoin rows simply contain a smaller set of [`FieldName`] variants than Ethereum.

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
/// [`Field`]s. Fan-out formats route fields to separate destinations based on
/// [`FieldName`].
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

    /// Block-kind only: parent block hash.
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
    /// Raw bytes of the transaction (binary, not JSON).
    TxRaw(Vec<u8>),
    /// Ethereum-only: the transaction receipt JSON.
    Receipt(Vec<u8>),
    /// Ethereum-only: convenience field carrying the `from` address.
    From(String),
    /// Ethereum-only: convenience field carrying the `to` address.
    To(String),

    // ---- Trace-kind fields ----
    /// `debug_traceTransaction` with `callTracer`.
    Trace(Vec<u8>),
    /// `debug_traceTransaction` with `prestateTracer`.
    StateDiff(Vec<u8>),
}
