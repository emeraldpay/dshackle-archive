use anyhow::anyhow;
use chrono::Utc;
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
    /// Archive the blocks and return all the blocks in that the archive for reference in other tables
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

        let mut results = Vec::new();
        for height in blocks.iter_height().collect::<Vec<Height>>() {
            if shutdown.is_signalled() {
                tracing::info!("Shutdown signalled, stopping");
                break;
            }
            let block_ref = BlockReference::Height(height);
            let (record, block, txes) = self.data_provider.fetch_block(&block_ref).await?;
            if !dry_run && let Some(file) = &file {
                let _ = file.append(record).await?;
            }
            results.push((block, txes));
        }

        if !dry_run && file.is_some() {
            let _ = file.unwrap().close().await?;
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
