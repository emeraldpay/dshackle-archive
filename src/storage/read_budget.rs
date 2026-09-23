// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Limits how much of an archive file is read ahead of its consumer by the size of the records.
//!
//! The read-ahead is bounded by the record count too, but the count alone doesn't bound the memory:
//! a single trace may take gigabytes, and a run of them in the read-ahead is enough to OOM the process.

use std::ops::Deref;
use std::sync::Arc;
use apache_avro::types::{Record, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Bytes a reader may send ahead of its consumer (plus one decoded record waiting for them, see [`ReadBudget::reserve_blocking`])
pub(super) const READ_AHEAD_BYTES: u32 = 256 * 1024 * 1024;

///
/// The memory a reader may use for the records its consumer hasn't processed yet
pub(super) struct ReadBudget {
    semaphore: Arc<Semaphore>,
    limit: u32,
}

impl ReadBudget {
    pub(super) fn new(limit: u32) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limit as usize)),
            limit,
        }
    }

    ///
    /// Reserve the memory taken by the record, blocking the (non-async) reader thread until the consumer drops enough
    /// of the previous records. A record larger than the whole budget takes all of it, so it's the only one sent ahead.
    ///
    /// The record is already decoded when it's reserved, so while it waits, it's in memory beyond the budget.
    /// I.e., a reader may hold the budget plus one record of any size.
    pub(super) fn reserve_blocking(&self, record: Record<'static>) -> ReadRecord {
        let size = record.fields.iter().map(|(_, value)| payload_size(value)).sum::<usize>();
        let size = size.min(self.limit as usize) as u32;
        let reservation = tokio::runtime::Handle::current()
            .block_on(self.semaphore.clone().acquire_many_owned(size))
            .expect("Read budget is never closed");
        ReadRecord {
            record,
            reservation: ReadReservation(reservation),
        }
    }
}

fn payload_size(value: &Value) -> usize {
    match value {
        Value::Bytes(b) => b.len(),
        Value::String(s) => s.len(),
        Value::Union(_, inner) => payload_size(inner),
        _ => 0,
    }
}

///
/// A record read from an archive file. Keeps its part of the reader's [`ReadBudget`] until dropped.
#[derive(Debug)]
pub struct ReadRecord {
    record: Record<'static>,
    reservation: ReadReservation,
}

/// The part of a [`ReadBudget`] taken by a record, returned to the reader on drop
#[derive(Debug)]
pub struct ReadReservation(#[allow(dead_code)] OwnedSemaphorePermit);

impl ReadRecord {
    ///
    /// Take the record out, when it's needed by value (e.g., to append it to another file).
    /// The reservation should be kept until the record is processed, otherwise the reader goes ahead while the record is still in memory.
    pub fn into_parts(self) -> (Record<'static>, ReadReservation) {
        (self.record, self.reservation)
    }
}

impl Deref for ReadRecord {
    type Target = Record<'static>;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use apache_avro::types::Record;
    use crate::avros::TX_TRACE_SCHEMA;
    use super::*;

    fn trace_record(size: usize) -> Record<'static> {
        let mut record = Record::new(&TX_TRACE_SCHEMA).unwrap();
        record.put("traceJson", Value::Union(1, Box::new(Value::Bytes(vec![0; size]))));
        record
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reader_waits_until_consumer_drops_records() {
        let budget = ReadBudget::new(100);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::task::spawn_blocking(move || {
            for _ in 0..3 {
                let _ = tx.send(budget.reserve_blocking(trace_record(60)));
            }
        });

        let first = rx.recv().await.unwrap();
        // 60 + 60 bytes doesn't fit into 100, so the second record waits for the first one
        let waiting = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(waiting.is_err(), "Reader went beyond the budget");

        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
        assert!(second.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn record_larger_than_budget_is_read_alone() {
        let budget = ReadBudget::new(100);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::task::spawn_blocking(move || {
            for _ in 0..2 {
                let _ = tx.send(budget.reserve_blocking(trace_record(1000)));
            }
        });

        let first = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        let waiting = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(waiting.is_err(), "Reader went beyond the budget");

        let (record, reservation) = first.into_parts();
        drop(record);
        drop(reservation);
        let second = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
        assert!(second.unwrap().is_some());
    }
}
