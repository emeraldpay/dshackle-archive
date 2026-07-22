use std::sync::Arc;
use anyhow::anyhow;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use crate::archiver::archiver::Archiver;
use crate::archiver::order::AppendSink;
use crate::archiver::{BlockTransactions, ProcessOutcome};
use crate::blockchain::{BlockReference, BlockchainData, BlockchainTypes};
use crate::archiver::datakind::{DataKind, DataOptions};
use crate::notify::NotificationBuilder;
use crate::archiver::range::{Height, Range};
use crate::global;
use crate::storage::{TargetFileWriter, WriteTarget};

impl<B: BlockchainTypes, TS: WriteTarget> Archiver<B, TS> {

    ///
    /// Archive the blocks and return all the blocks in that the archive for reference in other tables.
    /// Results are sorted by height regardless of fetch order.
    ///
    /// The `cancel` token lets the caller abandon the run mid-flight — used by
    /// live streaming when the re-org follower learns the block has been
    /// replaced. Non-streaming callers pass a fresh, never-cancelled token.
    pub async fn process_blocks(
        &self,
        blocks: Range,
        template: NotificationBuilder,
        options: &DataOptions,
        cancel: &CancellationToken,
    ) -> anyhow::Result<ProcessOutcome<BlockTransactions<B>>> {
        let shutdown = global::get_shutdown();
        if shutdown.is_signalled() {
            return Ok(ProcessOutcome::Completed {
                value: vec![],
                notifications: vec![],
            });
        }
        let dry_run = global::is_dry_run();
        let file = self.target.create(DataKind::Blocks, &blocks, options.overwrite)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        if file.is_none() {
            // note even though we skip the file, we still fetch the blocks to return them
            tracing::debug!(range = %blocks, "Skipping existing file");
        }
        let file = file.map(Arc::new);
        // Order block appends by height when the target requires it
        // (streaming backends). For file backends `needs_ordering()` is false
        // and the sink falls through to a thin pass-through that hands rows
        // to the writer in arrival order, without the channel/buffer hop.
        let ordered = self.target.needs_ordering();
        let sink = file
            .clone()
            .map(|f| Arc::new(AppendSink::new(f, blocks.start(), ordered)));

        let mut jobs = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(global::get_threads().blocks));
        for height in blocks.iter_height().collect::<Vec<Height>>() {
            let provider = self.data_provider.clone();
            let sink = sink.clone();
            let shutdown = shutdown.clone();
            let semaphore = semaphore.clone();
            let block_height = height.height;
            let cancel = cancel.clone();
            jobs.spawn(async move {
                if shutdown.is_signalled() {
                    return Ok(None);
                }
                let _permit = semaphore.acquire().await.unwrap();
                // Prefer a hash-pinned lookup when the follower gave us one,
                // so we don't race a re-org between subscription and fetch.
                // Falls back to height-only when no hash is present (e.g.,
                // batch archive over a numeric range). See the
                // `From<Height> for BlockReference` impl in blockchain/mod.rs.
                let block_ref: BlockReference<B::BlockHash> = height.into();
                let work = async {
                    let (record, block, txes) = provider.fetch_block(&block_ref).await?;
                    if !dry_run {
                        if let Some(sink) = &sink {
                            sink.append_at(block_height, record).await?;
                        }
                    }
                    crate::progress::on_record();
                    crate::metrics::add_items(&DataKind::Blocks, crate::metrics::Direction::Write, 1);
                    Ok::<_, anyhow::Error>(Some((block_height, block, txes)))
                };
                // Race the fetch against the cancel signal. On cancel the
                // task returns `None`, the drain loop counts it as a no-data
                // result, and the outer function inspects `cancel.is_cancelled()`
                // after the drain to decide whether to abandon or close.
                tokio::select! {
                    _ = cancel.cancelled() => Ok(None),
                    r = work => r,
                }
            });
        }

        let mut results: Vec<(u64, B::BlockParsed, Vec<B::TxId>)> = Vec::new();
        while let Some(res) = jobs.join_next().await {
            let item = res.map_err(|e| anyhow!("Task failed: {}", e))??;
            if let Some(entry) = item {
                results.push(entry);
            }
        }

        // Cancel-aware shutdown: skip the commit + notification path entirely
        // when the run was abandoned. `abandon` aborts the ordering layer's
        // drain task so any rows still buffered for the doomed block don't
        // raise a "closed with a gap" error. The file is intentionally NOT
        // closed: file-backend Drop impls remove the partial artifact (so a
        // subsequent re-org replacement with `overwrite=false` isn't blocked
        // by an empty/half-written file), and streaming-broker `close` is a
        // no-op anyway since messages have already left the producer.
        if cancel.is_cancelled() {
            if !dry_run {
                if let Some(sink) = sink {
                    let sink = Arc::into_inner(sink)
                        .ok_or_else(|| anyhow!("AppendSink still referenced after all tasks completed"))?;
                    sink.abandon().await?;
                }
                // Drop the writer Arc unconditionally — its Drop impl deletes
                // the partial file on disk (FsFileWriter / JsonFsWriter) or
                // abandons the S3 multipart upload (ObjectsStorage). For
                // Pulsar the drop is a no-op.
                drop(file);
            }
            return Ok(ProcessOutcome::Cancelled);
        }

        results.sort_by_key(|(height, _, _)| *height);
        let results: BlockTransactions<B> = results.into_iter()
            .map(|(_, block, txes)| (block, txes))
            .collect();

        // Build the notifications (one per location the writer reports) but
        // defer the send to `archive()`. If a concurrent process_txes /
        // process_traces ends up cancelled, the archiver will discard all
        // queued notifications atomically so the consumer never sees a torn
        // notification stream for the doomed block.
        let notifications = if !dry_run {
            // Close the ordering layer first — it drains any buffered rows
            // into the underlying writer (no-op for the direct variant). Only
            // then is it safe to take the sole reference to the writer, ask it
            // where the data landed, and close it.
            if let Some(sink) = sink {
                let sink = Arc::into_inner(sink)
                    .ok_or_else(|| anyhow!("AppendSink still referenced after all tasks completed"))?;
                sink.close().await?;
            }
            if let Some(file) = file {
                let file = Arc::into_inner(file)
                    .ok_or_else(|| anyhow!("File writer still referenced after all tasks completed"))?;
                let locations = file.locations();
                let _ = file.close().await?;
                locations
            } else {
                vec![]
            }
        } else {
            // dry-run writes nothing, so there is nothing to notify about
            vec![]
        };
        let notifications = notifications
            .into_iter()
            .map(|(range, location)| template.notification(DataKind::Blocks, &range, location))
            .collect();
        Ok(ProcessOutcome::Completed {
            value: results,
            notifications,
        })
    }
}
