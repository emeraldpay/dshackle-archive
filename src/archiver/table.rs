use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use crate::archiver::archiver::Archiver;
use crate::archiver::BlockTransactions;
use crate::blockchain::{BlockchainData, BlockchainTypes};
use crate::archiver::datakind::{DataKind, TraceOptions, TxOptions};
use crate::notify::Notification;
use crate::archiver::range::Range;
use crate::storage::{TargetFile, TargetFileWriter, TargetStorage};

///
/// Archiver of a specific table `<T>` based on the type of options.
/// Ex. one for `TraceOptions`, another for `TxOptions`, etc.
#[async_trait]
pub trait ArchiveTable<T, B: BlockchainTypes> {
    ///
    /// @param range - range of blocks being processed (could be a single block)
    /// @param notification - a notification struct with base fields filled (run mode, blockchain, heights)
    /// @param blocks - which blocks and their transaction must be archived
    /// @param options - specific options for the table being archived
    async fn process_table(&self, range: Range, notification: Notification, blocks: &BlockTransactions<B>, options: &T) -> anyhow::Result<()>;
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> ArchiveTable<TraceOptions, B> for Archiver<B, TS> {
    async fn process_table(&self, range: Range, notification: Notification, blocks: &BlockTransactions<B>, options: &TraceOptions) -> anyhow::Result<()> {
        let file = self.target.create(DataKind::TransactionTraces, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let file_url = file.get_url();
        for (block, txes) in blocks.iter() {
            for tx_index in 0..txes.len() {
                let data = self.data_provider.fetch_traces(&block, tx_index, &options).await?;
                let _ = file.append(data).await?;
            }
        }

        let _ = file.close().await?;
        let notification_tx = Notification {
            file_type: DataKind::TransactionTraces,
            location: file_url,
            ts: Utc::now(),

            ..notification
        };
        let _ = self.notifications.send(notification_tx).await;
        Ok(())
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> ArchiveTable<TxOptions, B> for Archiver<B, TS> {
    async fn process_table(&self, range: Range, notification: Notification, blocks: &BlockTransactions<B>, _options: &TxOptions) -> anyhow::Result<()> {
        let file = self.target.create(DataKind::Transactions, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let file_url = file.get_url();
        for (block, txes) in blocks.iter() {
            for tx_index in 0..txes.len() {
                let data = self.data_provider.fetch_tx(&block, tx_index).await?;
                let _ = file.append(data).await?;
            }
        }
        let _ = file.close().await?;
        let notification_tx = Notification {
            file_type: DataKind::Transactions,
            location: file_url,
            ts: Utc::now(),

            ..notification
        };
        let _ = self.notifications.send(notification_tx).await;
        Ok(())
    }
}
