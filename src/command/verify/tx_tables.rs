// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Verification of the tables with a record per transaction, i.e., txes and traces.
//!
//! Each file is verified on its own against the transactions of the blocks it covers (see
//! [`BlockTxes`]), and only the files found wrong are archived again. A range that is not
//! compacted yet consists of a file per block, and a single broken file must not cost
//! re-archiving the whole range.

use std::str::FromStr;
use apache_avro::types::{Record, Value};
use crate::{avros, global, progress};
use crate::archiver::datakind::{DataKind, TraceOptions, TxOptions};
use crate::archiver::range::Range;
use crate::blockchain::BlockchainTypes;
use crate::storage::{FileReference, ReadTarget, TargetFileReader};
use super::block_txes::BlockTxes;
use super::{get_field, verify_field_exist, verify_field_non_null, RangeVerification, VerifyFailure};

///
/// The checks of a record specific to the table, made in addition to the transaction it
/// belongs to.
pub(super) trait TxRecordCheck: Sync {
    fn check_record(&self, record: &Record) -> Result<(), String>;
}

impl TxRecordCheck for TxOptions {
    fn check_record(&self, record: &Record) -> Result<(), String> {
        //TODO blockchain specific verification
        verify_field_non_null(record, "json")?;
        verify_field_exist(record, "raw")
    }
}

impl TxRecordCheck for TraceOptions {
    fn check_record(&self, record: &Record) -> Result<(), String> {
        if self.include_trace {
            verify_field_non_null(record, "traceJson")?;
        }
        if self.include_state_diff {
            verify_field_non_null(record, "stateDiffJson")?;
        }
        Ok(())
    }
}

///
/// Verify the `files` of a per-transaction table of the `range`.
///
/// Goes through all the files even after a broken or an unreadable one, so a single run finds
/// all of them. It stops only on shutdown, and then the result tells nothing.
pub(super) async fn verify_tx_tables<'a, B, TS>(
    kind: DataKind,
    storage: &TS,
    range: &Range,
    files: &[&'a FileReference],
    expected_txes: &BlockTxes<B::TxId>,
    checks: &dyn TxRecordCheck,
) -> RangeVerification<'a>
where
    B: BlockchainTypes,
    TS: ReadTarget,
{
    let shutdown = global::get_shutdown();
    let mut broken = vec![];
    let mut incomplete = false;

    for file in files {
        if shutdown.is_signalled() {
            return RangeVerification::nothing_to_do();
        }
        tracing::debug!(range = %file.range, "Processing {} file: {}", kind, file.path);
        let result = verify_tx_file::<B, TS>(storage, file, expected_txes, checks).await;
        if shutdown.is_signalled() {
            // the file was read only in part, which doesn't mean it is wrong
            return RangeVerification::nothing_to_do();
        }
        match result {
            Ok(()) => {},
            Err(VerifyFailure::Invalid(reason)) => {
                tracing::error!(range = %range, "Corrupted {} file {}: {}", kind, file.path, reason);
                broken.push(*file);
            },
            Err(VerifyFailure::Unverifiable(reason)) => {
                tracing::error!(range = %range, "Cannot verify {} file {}, keeping it as is: {}", kind, file.path, reason);
                incomplete = true;
            },
        }
    }

    RangeVerification { broken, incomplete }
}

///
/// Read a single file and check that it contains exactly the transactions of its range, each
/// passing the table `checks`. Stops at the first invalid record.
///
/// A file that ends early because it doesn't decode (e.g., truncated) is reported as missing
/// transactions, as the reader treats it as an incomplete file.
async fn verify_tx_file<B, TS>(
    storage: &TS,
    file: &FileReference,
    expected_txes: &BlockTxes<B::TxId>,
    checks: &dyn TxRecordCheck,
) -> Result<(), VerifyFailure>
where
    B: BlockchainTypes,
    TS: ReadTarget,
{
    let shutdown = global::get_shutdown();
    let mut file_txes = expected_txes.expect_in(&file.range);
    let mut records = storage.open(file)
        .await.map_err(|e| VerifyFailure::Unverifiable(format!("Failed to open: {}", e)))?
        .read().map_err(|e| VerifyFailure::Unverifiable(format!("Failed to read: {}", e)))?;

    loop {
        let record = tokio::select! {
            _ = shutdown.signalled() => return Ok(()),
            record = records.recv() => record,
        };
        let Some(record) = record else {
            break;
        };
        let record = record?;
        progress::on_record();

        let height = avros::get_height(&record).map_err(|e| format!("Failed to get height: {}", e))?;
        let txid = get_field(&record, "txid")?;
        let txid_str = match txid {
            Value::String(s) => s.as_str(),
            _ => return Err(VerifyFailure::Invalid(format!("Invalid txid type: {:?}", txid))),
        };
        let txid = B::TxId::from_str(txid_str)
            .map_err(|_| format!("Invalid txid: {}", txid_str))?;
        file_txes.check_off(height, txid)?;
        checks.check_record(&record)
            .map_err(|e| format!("{} (txid {})", e, txid_str))?;
    }

    file_txes.finish()?;
    Ok(())
}
