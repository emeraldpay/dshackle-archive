use std::sync::Arc;
use anyhow::anyhow;
use chrono::Utc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use crate::archiver::archiver::Archiver;
use crate::archiver::BlockTransactions;
use crate::blockchain::{BlockReference, BlockchainData, BlockchainTypes};
use crate::archiver::datakind::{DataKind, DataOptions};
use crate::notify::Notification;
use crate::archiver::range::{Height, Range};
use crate::global;
use crate::storage::{TargetFile, TargetFileWriter, TargetStorage};

impl<B: BlockchainTypes, TS: TargetStorage> Archiver<B, TS> {

    ///
    /// Archive the blocks and return all the blocks in that the archive for reference in other tables.
    /// Results are sorted by height regardless of fetch order.
    pub async fn process_blocks(&self, blocks: Range, notification: Notification, options: &DataOptions) -> anyhow::Result<BlockTransactions<B>> {
        let shutdown = global::get_shutdown();
        if shutdown.is_signalled() {
            return Ok(vec![]);
        }
        let dry_run = global::is_dry_run();
        let file = self.target.create(DataKind::Blocks, &blocks, options.overwrite)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        if file.is_none() {
            // note even though we skip the file, we still fetch the blocks to return them
            tracing::debug!(range = %blocks, "Skipping existing file");
        }
        let file_url = file.as_ref().map(|f| f.get_url());
        let file = file.map(Arc::new);

        let mut jobs = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(global::get_threads().blocks));
        for height in blocks.iter_height().collect::<Vec<Height>>() {
            let provider = self.data_provider.clone();
            let file = file.clone();
            let shutdown = shutdown.clone();
            let semaphore = semaphore.clone();
            let block_height = height.height;
            jobs.spawn(async move {
                if shutdown.is_signalled() {
                    return Ok(None);
                }
                let _permit = semaphore.acquire().await.unwrap();
                let block_ref = BlockReference::Height(height);
                let (record, block, txes) = provider.fetch_block(&block_ref).await?;
                if !dry_run {
                    if let Some(file) = &file {
                        file.append(record).await?;
                    }
                }
                crate::progress::add_block();
                crate::metrics::add_items(&DataKind::Blocks, 1);
                Ok::<_, anyhow::Error>(Some((block_height, block, txes)))
            });
        }

        let mut results: Vec<(u64, B::BlockParsed, Vec<B::TxId>)> = Vec::new();
        while let Some(res) = jobs.join_next().await {
            let item = res.map_err(|e| anyhow!("Task failed: {}", e))??;
            if let Some(entry) = item {
                results.push(entry);
            }
        }
        results.sort_by_key(|(height, _, _)| *height);
        let results: BlockTransactions<B> = results.into_iter()
            .map(|(_, block, txes)| (block, txes))
            .collect();

        if !dry_run {
            if let Some(file) = file {
                let file = Arc::into_inner(file)
                    .ok_or_else(|| anyhow!("File writer still referenced after all tasks completed"))?;
                let _ = file.close().await?;
            }
        }
        if let Some(file_url) = file_url {
            let notification_tx = Notification {
                file_type: DataKind::Blocks,
                location: file_url,
                ts: Utc::now(),

                ..notification
            };
            if !dry_run {
                let _ = self.notifications.send(notification_tx).await;
            }
        }
        Ok(results)
    }
}
