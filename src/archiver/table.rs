use std::sync::Arc;
use anyhow::anyhow;
use chrono::Utc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use crate::archiver::archiver::Archiver;
use crate::archiver::order::AppendSink;
use crate::archiver::{BlockTransactions, ProcessOutcome};
use crate::blockchain::{BlockchainData, BlockchainTypes};
use crate::archiver::datakind::{DataKind, DataOptions};
use crate::notify::Notification;
use crate::archiver::range::Range;
use crate::global;
use crate::storage::{TargetFile, TargetFileWriter, WriteTarget};


impl<B: BlockchainTypes, TS: WriteTarget> Archiver<B, TS> {
    pub async fn process_traces(
        &self,
        range: Range,
        notification: Notification,
        blocks: &BlockTransactions<B>,
        options: &DataOptions,
        cancel: &CancellationToken,
    ) -> anyhow::Result<ProcessOutcome<()>> {
        let shutdown = global::get_shutdown();
        if shutdown.is_signalled() {
            return Ok(ProcessOutcome::Completed {
                value: (),
                notification: None,
            });
        }
        let dry_run = global::is_dry_run();
        let file = self.target.create(DataKind::TransactionTraces, &range, options.overwrite)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        if file.is_none() {
            tracing::debug!(range = %range, "Skipping existing file");
            return Ok(ProcessOutcome::Completed {
                value: (),
                notification: None,
            });
        }
        let file = file.unwrap();
        let options = options.trace.as_ref().unwrap();

        let file_url = file.get_url();
        let file = Arc::new(file);
        // Order traces by a flat `(block_position, tx_index)` ordinal so block
        // N's traces are all published before block N+1's, and within a block
        // traces follow tx_index order. The sink only buffers/reorders when
        // the target asks for ordering — file backends get a pass-through.
        // See `process_txes` for the same pattern.
        let sink = Arc::new(AppendSink::new(
            file.clone(),
            0,
            self.target.needs_ordering(),
        ));

        let mut jobs = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(global::get_threads().trace));
        let options = Arc::new(options.clone());
        let mut flat_index: u64 = 0;
        for (block, txes) in blocks.iter() {
            let block = Arc::new(block.clone());
            for tx_index in 0..txes.len() {
                let block = block.clone();
                let provider = self.data_provider.clone();
                let options = options.clone();
                let sink = sink.clone();
                let shutdown = shutdown.clone();
                let semaphore = semaphore.clone();
                let order_idx = flat_index;
                let cancel = cancel.clone();
                flat_index += 1;
                jobs.spawn(async move {
                    if shutdown.is_signalled() {
                        return Ok(());
                    }
                    let _permit = semaphore.acquire().await.unwrap();
                    let work = async {
                        let data = provider.fetch_traces(&block, tx_index, &options).await?;
                        if !dry_run {
                            sink.append_at(order_idx, data).await?;
                        }
                        crate::progress::on_record();
                        crate::metrics::add_items(&DataKind::TransactionTraces, crate::metrics::Direction::Write, 1);
                        Ok::<_, anyhow::Error>(())
                    };
                    tokio::select! {
                        _ = cancel.cancelled() => Ok(()),
                        r = work => r,
                    }
                });
            }
        }

        while let Some(res) = jobs.join_next().await {
            res.map_err(|e| anyhow!("Task failed: {}", e))??;
        }

        // See `process_blocks` for the rationale: on cancel we abandon the
        // ordering layer and let the writer Drop clean up (delete the
        // partial file on disk / abandon the multipart upload on S3 / no-op
        // for streaming brokers whose messages have already been published).
        if cancel.is_cancelled() {
            if !dry_run {
                let sink = Arc::into_inner(sink)
                    .ok_or_else(|| anyhow!("AppendSink still referenced after all tasks completed"))?;
                sink.abandon().await?;
                drop(file);
            }
            return Ok(ProcessOutcome::Cancelled);
        }

        if !dry_run {
            let sink = Arc::into_inner(sink)
                .ok_or_else(|| anyhow!("AppendSink still referenced after all tasks completed"))?;
            sink.close().await?;
            let file = Arc::into_inner(file)
                .ok_or_else(|| anyhow!("File writer still referenced after all tasks completed"))?;
            let _ = file.close().await?;
        }
        let notification = Notification {
            file_type: DataKind::TransactionTraces,
            location: file_url,
            ts: Utc::now(),
            ..notification
        };
        Ok(ProcessOutcome::Completed {
            value: (),
            notification: Some(notification),
        })
    }

    pub async fn process_txes(
        &self,
        range: Range,
        notification: Notification,
        blocks: &BlockTransactions<B>,
        options: &DataOptions,
        cancel: &CancellationToken,
    ) -> anyhow::Result<ProcessOutcome<()>> {
        let shutdown = global::get_shutdown();
        if shutdown.is_signalled() {
            return Ok(ProcessOutcome::Completed {
                value: (),
                notification: None,
            });
        }
        let dry_run = global::is_dry_run();
        let file = self.target.create(DataKind::Transactions, &range, options.overwrite)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        if file.is_none() {
            tracing::debug!(range = %range, "Skipping existing file");
            return Ok(ProcessOutcome::Completed {
                value: (),
                notification: None,
            });
        }
        let file = file.unwrap();

        let file_url = file.get_url();
        let file = Arc::new(file);
        // Tx ordering: a flat `(block_position, tx_index)` ordinal across the
        // whole range. `blocks` is already sorted by height by
        // `process_blocks`, so walking it linearly yields chain order;
        // numbering tasks 0, 1, 2, … as we enumerate guarantees that block N's
        // txes (in their natural tx_index order) precede block N+1's, even
        // when fetches finish out of order. The sink only buffers/reorders
        // when the target asks for ordering (streaming backends); file
        // backends get a pass-through.
        let sink = Arc::new(AppendSink::new(
            file.clone(),
            0,
            self.target.needs_ordering(),
        ));

        let mut jobs = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(global::get_threads().tx));
        let mut flat_index: u64 = 0;
        for (block, txes) in blocks.iter() {
            let block = Arc::new(block.clone());
            for tx_index in 0..txes.len() {
                let block = block.clone();
                let provider = self.data_provider.clone();
                let sink = sink.clone();
                let shutdown = shutdown.clone();
                let semaphore = semaphore.clone();
                let order_idx = flat_index;
                let cancel = cancel.clone();
                flat_index += 1;
                jobs.spawn(async move {
                    if shutdown.is_signalled() {
                        return Ok(());
                    }
                    let _permit = semaphore.acquire().await.unwrap();
                    let work = async {
                        let data = provider.fetch_tx(&block, tx_index).await?;
                        if !dry_run {
                            sink.append_at(order_idx, data).await?;
                        }
                        crate::progress::on_record();
                        crate::metrics::add_items(&DataKind::Transactions, crate::metrics::Direction::Write, 1);
                        Ok::<_, anyhow::Error>(())
                    };
                    tokio::select! {
                        _ = cancel.cancelled() => Ok(()),
                        r = work => r,
                    }
                });
            }
        }

        while let Some(res) = jobs.join_next().await {
            res.map_err(|e| anyhow!("Task failed: {}", e))??;
        }

        // See `process_blocks` for the rationale: on cancel we abandon the
        // ordering layer and let the writer Drop clean up the partial
        // artifact. The notification is built but only published by
        // `archive()` once the entire run is known to be uncancelled.
        if cancel.is_cancelled() {
            if !dry_run {
                let sink = Arc::into_inner(sink)
                    .ok_or_else(|| anyhow!("AppendSink still referenced after all tasks completed"))?;
                sink.abandon().await?;
                drop(file);
            }
            return Ok(ProcessOutcome::Cancelled);
        }

        if !dry_run {
            let sink = Arc::into_inner(sink)
                .ok_or_else(|| anyhow!("AppendSink still referenced after all tasks completed"))?;
            sink.close().await?;
            let file = Arc::into_inner(file)
                .ok_or_else(|| anyhow!("File writer still referenced after all tasks completed"))?;
            let _ = file.close().await?;
        }
        let notification = Notification {
            file_type: DataKind::Transactions,
            location: file_url,
            ts: Utc::now(),
            ..notification
        };
        Ok(ProcessOutcome::Completed {
            value: (),
            notification: Some(notification),
        })
    }
}
