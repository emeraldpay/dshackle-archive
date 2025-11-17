use anyhow::anyhow;
use chrono::Utc;
use crate::archiver::archiver::Archiver;
use crate::archiver::BlockTransactions;
use crate::blockchain::{BlockchainData, BlockchainTypes};
use crate::archiver::datakind::{DataKind, DataOptions};
use crate::notify::Notification;
use crate::archiver::range::Range;
use crate::global;
use crate::storage::{TargetFile, TargetFileWriter, TargetStorage};


impl<B: BlockchainTypes, TS: TargetStorage> Archiver<B, TS> {
    pub async fn process_traces(&self, range: Range, notification: Notification, blocks: &BlockTransactions<B>, options: &DataOptions) -> anyhow::Result<()> {
        let shutdown = global::get_shutdown();
        if shutdown.is_signalled() {
            return Ok(());
        }
        let file = self.target.create(DataKind::TransactionTraces, &range, options.overwrite)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        if file.is_none() {
            tracing::debug!(range = %range, "Skipping existing file");
            return Ok(());
        }
        let file = file.unwrap();
        let options = options.trace.as_ref().unwrap();

        let file_url = file.get_url();
        for (block, txes) in blocks.iter() {
            for tx_index in 0..txes.len() {
                if shutdown.is_signalled() {
                    tracing::info!("Shutdown signalled, stopping");
                    return Ok(());
                }
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

    pub async fn process_txes(&self, range: Range, notification: Notification, blocks: &BlockTransactions<B>, options: &DataOptions) -> anyhow::Result<()> {
        let shutdown = global::get_shutdown();
        if shutdown.is_signalled() {
            return Ok(());
        }
        let dry_run = global::is_dry_run();
        let file = self.target.create(DataKind::Transactions, &range, options.overwrite)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        if file.is_none() {
            tracing::debug!(range = %range, "Skipping existing file");
            return Ok(());
        }
        let file = file.unwrap();

        let file_url = file.get_url();
        for (block, txes) in blocks.iter() {
            for tx_index in 0..txes.len() {
                if shutdown.is_signalled() {
                    tracing::info!("Shutdown signalled, stopping");
                    return Ok(());
                }
                let data = self.data_provider.fetch_tx(&block, tx_index).await?;
                if !dry_run {
                    let _ = file.append(data).await?;
                }
            }
        }
        if !dry_run {
            let _ = file.close().await?;
        }
        let notification_tx = Notification {
            file_type: DataKind::Transactions,
            location: file_url,
            ts: Utc::now(),

            ..notification
        };
        if !dry_run {
            let _ = self.notifications.send(notification_tx).await;
        }
        Ok(())
    }
}
