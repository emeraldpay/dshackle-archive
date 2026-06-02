// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Per-writer ordering layer for parallel fetches.
//!
//! The archiver fetches blocks, transactions, and traces in parallel
//! (`JoinSet` + a semaphore), then forwards each result to a
//! [`crate::storage::TargetFileWriter`] as soon as the fetch finishes. For
//! file-based targets (Avro, JSON) the order of `append` calls doesn't matter —
//! the file just collects records. For streaming targets (Pulsar today, Kafka
//! next) the broker preserves messages in *publish* order, so the order in
//! which we hand records to the writer becomes the order consumers see.
//!
//! [`OrderedSink`] sits between the parallel fetchers and the writer: each
//! fetcher submits its row tagged with a logical index, and a single drain
//! task forwards rows to the underlying writer in ascending index order. Rows
//! that arrive before their predecessor are buffered (not "until the end" —
//! only until the missing earlier index lands) so the first in-order run is
//! released as early as possible.
//!
//! Indices are chain-natural:
//!
//! - For blocks within a range, the index is the block height.
//! - For transactions / traces within a range, the index is a flat
//!   `(block_position, tx_index)` ordinal so that block N's txes are all
//!   emitted before block N+1's, and within each block they go in
//!   `tx_index` order.
//!
//! The sink reports an error if the channel closes with any "future" rows
//! still pending — i.e., a fetcher silently dropped its row and the sequence
//! has a gap.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::record::ArchiveRow;
use crate::storage::TargetFileWriter;

/// Dispatching sink used by the archiver fan-out sites.
///
/// Targets that need strict in-order delivery
/// (see [`crate::storage::WriteTarget::needs_ordering`]) get the
/// [`OrderedSink`] path with its drain task and buffer; targets that don't
/// (file backends) get a thin pass-through that calls `writer.append`
/// directly, avoiding the per-row channel hop. Both variants expose the same
/// `append_at(idx, row)` API so the call site stays uniform.
pub enum AppendSink<W: TargetFileWriter + Send + Sync + 'static> {
    Ordered(OrderedSink),
    /// Pass-through. `append_at`'s `idx` is ignored — the writer accepts rows
    /// in whatever order the parallel fetchers complete.
    Direct(Arc<W>),
}

impl<W: TargetFileWriter + Send + Sync + 'static> AppendSink<W> {
    /// Build the appropriate sink variant. `ordered` typically comes from
    /// [`crate::storage::WriteTarget::needs_ordering`]; `start_index` is the
    /// first valid index in the ordered case and unused otherwise.
    pub fn new(writer: Arc<W>, start_index: u64, ordered: bool) -> Self {
        if ordered {
            AppendSink::Ordered(OrderedSink::new(writer, start_index))
        } else {
            AppendSink::Direct(writer)
        }
    }

    /// Submit `row` at logical position `index`. In [`AppendSink::Ordered`]
    /// mode the row may be buffered until earlier indices land; in
    /// [`AppendSink::Direct`] mode it is forwarded to the writer immediately.
    pub async fn append_at(&self, index: u64, row: ArchiveRow) -> Result<()> {
        match self {
            AppendSink::Ordered(s) => s.append_at(index, row).await,
            AppendSink::Direct(w) => w.append(row).await,
        }
    }

    /// Finalize the sink. For [`AppendSink::Ordered`] this awaits the drain
    /// task and surfaces any gap/duplicate errors. For [`AppendSink::Direct`]
    /// it drops the inner `Arc<W>` clone so the caller's outer Arc is the
    /// sole reference for the file `close()`.
    pub async fn close(self) -> Result<()> {
        match self {
            AppendSink::Ordered(s) => s.close().await,
            AppendSink::Direct(_) => Ok(()),
        }
    }

    /// Abandon the sink: stop the drain task immediately, drop any pending
    /// rows without raising a gap error. Use this when the upstream work was
    /// cancelled (e.g. a re-org invalidated the block) — completing the run
    /// is meaningless, and the unfilled gap is expected, not an error.
    pub async fn abandon(self) -> Result<()> {
        match self {
            AppendSink::Ordered(s) => s.abandon().await,
            AppendSink::Direct(_) => Ok(()),
        }
    }
}

/// Ordering wrapper around a [`TargetFileWriter`].
///
/// Construction spawns a background drain task that owns an `Arc<W>` and
/// forwards rows in strict ascending order of the submitted index. Cloning
/// the sink (via `Arc::clone`) hands the channel sender to additional
/// producers; closing it (via [`OrderedSink::close`]) flushes the buffer and
/// propagates any error from the drain task.
pub struct OrderedSink {
    tx: mpsc::Sender<(u64, ArchiveRow)>,
    handle: JoinHandle<Result<()>>,
}

impl OrderedSink {
    /// Wrap `writer` in an ordering layer that releases rows starting at
    /// `start_index` and counts up by one for each consecutive entry.
    ///
    /// Submissions arrive over a bounded channel — large enough to absorb the
    /// burst from a typical block's worth of concurrent fetches without
    /// stalling, small enough to apply backpressure if the writer is slow.
    pub fn new<W>(writer: Arc<W>, start_index: u64) -> Self
    where
        W: TargetFileWriter + Send + Sync + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<(u64, ArchiveRow)>(32);
        let handle = tokio::spawn(async move {
            let mut next = start_index;
            let mut pending: BTreeMap<u64, ArchiveRow> = BTreeMap::new();
            while let Some((idx, row)) = rx.recv().await {
                if idx < next {
                    return Err(anyhow!(
                        "OrderedSink received index {} below cursor {} (already forwarded)",
                        idx,
                        next
                    ));
                }
                if pending.insert(idx, row).is_some() {
                    return Err(anyhow!(
                        "OrderedSink received duplicate index {}",
                        idx
                    ));
                }
                // Drain whatever consecutive run is now available at the head.
                while let Some(row) = pending.remove(&next) {
                    writer.append(row).await?;
                    next += 1;
                }
            }
            if !pending.is_empty() {
                let smallest = pending.keys().next().copied().unwrap_or(0);
                return Err(anyhow!(
                    "OrderedSink closed with a gap: cursor at {}, {} row(s) pending starting at {}",
                    next,
                    pending.len(),
                    smallest
                ));
            }
            Ok(())
        });
        Self { tx, handle }
    }

    /// Submit `row` at logical position `index`. If `index` is the next
    /// expected one it is forwarded to the writer immediately; otherwise it
    /// is buffered until its predecessor arrives.
    pub async fn append_at(&self, index: u64, row: ArchiveRow) -> Result<()> {
        self.tx
            .send((index, row))
            .await
            .map_err(|_| anyhow!("OrderedSink drain task has ended"))
    }

    /// Close the sink: drop the sender, await the drain task, propagate any
    /// inner-writer or gap errors. Must be called for clean shutdown.
    pub async fn close(self) -> Result<()> {
        drop(self.tx);
        self.handle
            .await
            .map_err(|e| anyhow!("OrderedSink drain task panicked: {}", e))?
    }

    /// Abandon the sink: abort the drain task and discard any pending rows.
    ///
    /// Unlike [`close`](Self::close), this masks the "closed with gap"
    /// diagnostic — the caller is acknowledging that the run was cut short
    /// (typically because a re-org invalidated the block) and that whatever
    /// is still buffered is now meaningless. Real writer failures that
    /// surfaced before abandon was called are still propagated so the caller
    /// can distinguish "cancelled cleanly" from "broker rejected publish".
    pub async fn abandon(self) -> Result<()> {
        drop(self.tx);
        self.handle.abort();
        match self.handle.await {
            // Drain finished naturally before abort took effect. The Result it
            // returned reflects whether the underlying writer succeeded — if a
            // prior append failed we must surface it; the gap diagnostic is
            // already masked because `drop(self.tx)` lets the drain loop exit
            // cleanly.
            Ok(Ok(())) => Ok(()),
            Ok(Err(writer_err)) => Err(writer_err),
            // Drain task was actually aborted mid-iteration; that's the
            // intended outcome of `abandon`.
            Err(je) if je.is_cancelled() => Ok(()),
            Err(je) => Err(anyhow!("OrderedSink drain task panicked: {}", je)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};

    use crate::archiver::datakind::DataKind;
    use crate::record::{ArchiveRow, BlockchainType, Field};
    use crate::storage::{TargetFile, TargetFileWriter};

    /// Recording writer: appends are captured in a Mutex<Vec> so tests can
    /// assert the order rows reach the writer regardless of submission order.
    struct Recorder {
        appended: Arc<Mutex<Vec<u64>>>,
    }

    impl TargetFile for Recorder {
        fn get_url(&self) -> String {
            "test://recorder".to_string()
        }
    }

    #[async_trait]
    impl TargetFileWriter for Recorder {
        async fn append(&self, row: ArchiveRow) -> Result<()> {
            self.appended.lock().unwrap().push(row.height);
            Ok(())
        }
        async fn close(self) -> Result<()> {
            Ok(())
        }
    }

    fn row(height: u64) -> ArchiveRow {
        ArchiveRow {
            kind: DataKind::Blocks,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "X".to_string(),
            archive_ts: Utc::now(),
            height,
            block_id: format!("0xB{}", height),
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
            parent_id: None,
            tx_index: None,
            tx_id: None,
            fields: vec![Field::BlockJson(b"x".to_vec())],
        }
    }

    #[tokio::test]
    async fn forwards_in_order_when_submissions_are_already_in_order() {
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink = OrderedSink::new(writer, 0);
        for i in 0..5 {
            sink.append_at(i, row(i)).await.unwrap();
        }
        sink.close().await.unwrap();
        assert_eq!(*appended.lock().unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn buffers_out_of_order_and_releases_when_gap_fills() {
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink = OrderedSink::new(writer, 0);
        // Submit out of order: 2, 0, 3, 1, 4
        sink.append_at(2, row(2)).await.unwrap();
        sink.append_at(0, row(0)).await.unwrap();
        sink.append_at(3, row(3)).await.unwrap();
        sink.append_at(1, row(1)).await.unwrap();
        sink.append_at(4, row(4)).await.unwrap();
        sink.close().await.unwrap();
        // Writer must have seen them in strict ascending order.
        assert_eq!(*appended.lock().unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn streams_eagerly_without_waiting_for_late_indices() {
        // Submit 0..=2 immediately; index 4 only after waiting. Assert that
        // 0/1/2 are released *before* we submit 4 — i.e., the sink doesn't
        // buffer to the end.
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink = OrderedSink::new(writer, 0);
        for i in 0..=2 {
            sink.append_at(i, row(i)).await.unwrap();
        }
        // Give the drain task a chance to forward 0/1/2.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(*appended.lock().unwrap(), vec![0, 1, 2]);
        // Submit a row at 4 (gap at 3) — should NOT be released yet.
        sink.append_at(4, row(4)).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(*appended.lock().unwrap(), vec![0, 1, 2]);
        // Now fill the gap.
        sink.append_at(3, row(3)).await.unwrap();
        sink.close().await.unwrap();
        assert_eq!(*appended.lock().unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn closing_with_unfilled_gap_returns_error() {
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink = OrderedSink::new(writer, 0);
        sink.append_at(0, row(0)).await.unwrap();
        sink.append_at(2, row(2)).await.unwrap();
        // Index 1 never submitted.
        let err = sink.close().await.unwrap_err();
        assert!(
            err.to_string().contains("gap"),
            "unexpected error: {}",
            err
        );
        // 0 went through; 2 was held back.
        assert_eq!(*appended.lock().unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn duplicate_future_index_returns_error() {
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink = OrderedSink::new(writer, 0);
        // Two submissions at index 5 with the slot still pending (0..=4 never
        // arrive). The drain task sees both in pending and reports the
        // duplicate; the send itself succeeds because the channel buffered it.
        sink.append_at(5, row(5)).await.unwrap();
        sink.append_at(5, row(5)).await.unwrap();
        let err = sink.close().await.unwrap_err();
        assert!(
            err.to_string().contains("duplicate"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn direct_sink_forwards_in_arrival_order_without_buffering() {
        // Pass-through variant: index is ignored, rows reach the writer in
        // the order they were submitted. Verifies that targets opting out of
        // ordering don't accidentally pay the buffering tax.
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink: AppendSink<Recorder> = AppendSink::new(writer, 0, false);
        // Submit out of order — direct sink does NOT re-order.
        sink.append_at(2, row(2)).await.unwrap();
        sink.append_at(0, row(0)).await.unwrap();
        sink.append_at(1, row(1)).await.unwrap();
        sink.close().await.unwrap();
        assert_eq!(*appended.lock().unwrap(), vec![2, 0, 1]);
    }

    #[tokio::test]
    async fn ordered_sink_via_appendsink_reorders() {
        // Sanity that AppendSink dispatches to the ordered path correctly.
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink: AppendSink<Recorder> = AppendSink::new(writer, 0, true);
        sink.append_at(2, row(2)).await.unwrap();
        sink.append_at(0, row(0)).await.unwrap();
        sink.append_at(1, row(1)).await.unwrap();
        sink.close().await.unwrap();
        assert_eq!(*appended.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn abandon_drops_pending_without_gap_error() {
        // Two rows submitted with a gap at index 1 — `close` would return a
        // gap error, but `abandon` is the explicit "this run is doomed" path
        // and must succeed quietly.
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink = OrderedSink::new(writer, 0);
        sink.append_at(0, row(0)).await.unwrap();
        sink.append_at(2, row(2)).await.unwrap();
        // Give the drain task a chance to forward index 0.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        // Index 1 never arrives — abandon must not raise the gap error.
        sink.abandon().await.unwrap();
        // Whatever was already forwarded stays; pending rows are discarded.
        assert_eq!(*appended.lock().unwrap(), vec![0]);
    }

    /// Writer that fails its first `append` call. Used to assert that
    /// `abandon` surfaces real writer errors instead of masking them as
    /// "clean cancellation".
    struct FailingWriter;

    impl TargetFile for FailingWriter {
        fn get_url(&self) -> String {
            "test://failing".to_string()
        }
    }

    #[async_trait]
    impl TargetFileWriter for FailingWriter {
        async fn append(&self, _row: ArchiveRow) -> Result<()> {
            Err(anyhow!("simulated broker reject"))
        }
        async fn close(self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn abandon_propagates_writer_error_that_surfaced_before_cancel() {
        // The underlying writer fails on append; the drain task returns that
        // failure. If abandon swallows it as a clean cancellation, callers
        // would never learn the broker rejected real messages — exactly the
        // failure mode we want to prevent.
        let writer = Arc::new(FailingWriter);
        let sink = OrderedSink::new(writer, 0);
        sink.append_at(0, row(0)).await.unwrap();
        // Give the drain task a chance to consume and fail.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let err = sink.abandon().await.unwrap_err();
        assert!(
            err.to_string().contains("simulated broker reject"),
            "abandon must surface the underlying writer error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn appendsink_abandon_is_a_noop_for_direct() {
        // Pass-through variant has nothing to abandon — call must succeed
        // and not affect already-forwarded rows.
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink: AppendSink<Recorder> = AppendSink::new(writer, 0, false);
        sink.append_at(0, row(0)).await.unwrap();
        sink.abandon().await.unwrap();
        assert_eq!(*appended.lock().unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn replay_below_cursor_returns_error() {
        let appended = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Recorder {
            appended: appended.clone(),
        });
        let sink = OrderedSink::new(writer, 0);
        sink.append_at(0, row(0)).await.unwrap();
        // Give the drain task a chance to forward index 0 and advance cursor.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        // Now re-submit at index 0 — already past the cursor, so it's a
        // logic error from the caller.
        sink.append_at(0, row(0)).await.unwrap();
        let err = sink.close().await.unwrap_err();
        assert!(
            err.to_string().contains("below cursor"),
            "unexpected error: {}",
            err
        );
    }
}
