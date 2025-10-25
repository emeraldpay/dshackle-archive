use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use crate::archiver::archiver::Archiver;
use crate::archiver::BlockTransactions;
use crate::blockchain::{BlockReference, BlockchainData, BlockchainTypes};
use crate::archiver::datakind::DataKind;
use crate::notify::Notification;
use crate::archiver::range::{Height, Range};
use crate::global;
use crate::storage::{TargetFile, TargetFileWriter, TargetStorage};

///
/// Processes blocks and stores them in the archive.
#[async_trait]
pub trait ArchiveBlock<B: BlockchainTypes> {
    ///
    /// @param blocks - which blocks to process (by height or hash)
    async fn process_blocks(&self, blocks: Range, notification: Notification) -> anyhow::Result<BlockTransactions<B>>;
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> ArchiveBlock<B> for Archiver<B, TS> {
    async fn process_blocks(&self, blocks: Range, notification: Notification) -> anyhow::Result<BlockTransactions<B>> {
        let shutdown = global::get_shutdown();
        if shutdown.is_signalled() {
            return Ok(vec![]);
        }
        let dry_run = global::is_dry_run();
        let file = self.target.create(DataKind::Blocks, &blocks)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let file_url = file.get_url();

        let mut results = Vec::new();
        for height in blocks.iter_height().collect::<Vec<Height>>() {
            if shutdown.is_signalled() {
                tracing::info!("Shutdown signalled, stopping");
                break;
            }
            let block_ref = BlockReference::Height(height);
            let (record, block, txes) = self.data_provider.fetch_block(&block_ref).await?;
            if !dry_run {
                let _ = file.append(record).await?;
            }
            results.push((block, txes));
        }

        if !dry_run {
            let _ = file.close().await?;
        }
        let notification_tx = Notification {
            file_type: DataKind::Blocks,
            location: file_url,
            ts: Utc::now(),

            ..notification
        };
        if !dry_run {
            let _ = self.notifications.send(notification_tx).await;
        }
        Ok(results)
    }
}
