use std::str::FromStr;
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use crate::archiver::archiver::Archiver;
use crate::archiver::BlockTransactions;
use crate::blockchain::{BlockReference, BlockchainData, BlockchainTypes, MultiBlockReference};
use crate::archiver::datakind::DataKind;
use crate::notify::Notification;
use crate::archiver::range::Range;
use crate::storage::{TargetFile, TargetFileWriter, TargetStorage};

///
/// Processes blocks and stores them in the archive.
#[async_trait]
pub trait ArchiveBlock<B: BlockchainTypes> {
    ///
    /// @param blocks - which blocks to process (by height or hash)
    async fn process_blocks(&self, blocks: MultiBlockReference, notification: Notification) -> anyhow::Result<BlockTransactions<B>>;
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> ArchiveBlock<B> for Archiver<B, TS> {
    async fn process_blocks(&self, blocks: MultiBlockReference, notification: Notification) -> anyhow::Result<BlockTransactions<B>> {
        let range: Range = blocks.clone().into();
        let file = self.target.create(DataKind::Blocks, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let file_url = file.get_url();

        let heights = match blocks {
            MultiBlockReference::Single(h) => {
                let r = if let Some(hash) = &h.hash {
                    BlockReference::Hash(
                        B::BlockHash::from_str(hash).map_err(|_| anyhow!("Not a valid hash"))?
                    )
                } else {
                    BlockReference::height(h.height)
                };
                vec![r]
            },
            MultiBlockReference::Range(range) => {
                range.iter().map(|h| BlockReference::height(h)).collect()
            }
        };

        let mut results = Vec::new();
        for height in heights {
            let (record, block, txes) = self.data_provider.fetch_block(&height).await?;
            let _ = file.append(record).await?;
            results.push((block, txes));
        }

        let _ = file.close().await?;
        let notification_tx = Notification {
            file_type: DataKind::Blocks,
            location: file_url,
            ts: Utc::now(),

            ..notification
        };
        let _ = self.notifications.send(notification_tx).await;
        Ok(results)
    }
}
