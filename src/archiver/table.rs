use std::sync::Arc;
use anyhow::anyhow;
use chrono::Utc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
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
        let dry_run = global::is_dry_run();
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
        let file = Arc::new(file);

        let mut jobs = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(4));
        let options = Arc::new(options.clone());
        for (block, txes) in blocks.iter() {
            let block = Arc::new(block.clone());
            for tx_index in 0..txes.len() {
                let block = block.clone();
                let provider = self.data_provider.clone();
                let options = options.clone();
                let file = file.clone();
                let shutdown = shutdown.clone();
                let semaphore = semaphore.clone();
                jobs.spawn(async move {
                    if shutdown.is_signalled() {
                        return Ok(());
                    }
                    let _permit = semaphore.acquire().await.unwrap();
                    let data = provider.fetch_traces(&block, tx_index, &options).await?;
                    if !dry_run {
                        file.append(data).await?;
                    }
                    crate::metrics::add_items(&DataKind::TransactionTraces, 1);
                    Ok::<_, anyhow::Error>(())
                });
            }
        }

        while let Some(res) = jobs.join_next().await {
            res.map_err(|e| anyhow!("Task failed: {}", e))??;
        }

        if !dry_run {
            let file = Arc::into_inner(file)
                .ok_or_else(|| anyhow!("File writer still referenced after all tasks completed"))?;
            let _ = file.close().await?;
        }
        let notification_tx = Notification {
            file_type: DataKind::TransactionTraces,
            location: file_url,
            ts: Utc::now(),

            ..notification
        };
        if !dry_run {
            let _ = self.notifications.send(notification_tx).await;
        }
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
        let file = Arc::new(file);

        let mut jobs = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(16));
        for (block, txes) in blocks.iter() {
            let block = Arc::new(block.clone());
            for tx_index in 0..txes.len() {
                let block = block.clone();
                let provider = self.data_provider.clone();
                let file = file.clone();
                let shutdown = shutdown.clone();
                let semaphore = semaphore.clone();
                jobs.spawn(async move {
                    if shutdown.is_signalled() {
                        return Ok(());
                    }
                    let _permit = semaphore.acquire().await.unwrap();
                    let data = provider.fetch_tx(&block, tx_index).await?;
                    if !dry_run {
                        file.append(data).await?;
                    }
                    crate::metrics::add_items(&DataKind::Transactions, 1);
                    Ok::<_, anyhow::Error>(())
                });
            }
        }

        while let Some(res) = jobs.join_next().await {
            res.map_err(|e| anyhow!("Task failed: {}", e))??;
        }

        if !dry_run {
            let file = Arc::into_inner(file)
                .ok_or_else(|| anyhow!("File writer still referenced after all tasks completed"))?;
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
