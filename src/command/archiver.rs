use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use anyhow::anyhow;
use chrono::Utc;
use futures_util::future::join_all;
use tokio::sync::mpsc::Sender;
use crate::blockchain::{BlockReference, BlockchainData, BlockchainTypes};
use crate::blockchain::connection::{Height};
use crate::command::{ArchivesList, Blocks};
use crate::datakind::DataKind;
use crate::global;
use crate::notify::{Notification, Notifier, RunMode};
use crate::notify::empty::EmptyNotifier;
use crate::range::Range;
use crate::storage::TargetStorage;
use crate::storage::TargetFile;
use crate::storage::TargetFileWriter;

#[derive(Clone)]
pub struct Archiver<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    pub target: Arc<TS>,
    pub data_provider: Arc<B::DataProvider>,
    pub notifications: Sender<Notification>,
}

impl<B: BlockchainTypes, TS: TargetStorage> Archiver<B, TS> {

    pub fn new_simple(target: Arc<TS>, data_provider: Arc<B::DataProvider>) -> Self {
        Self::new(
            target,
            data_provider,
            EmptyNotifier::default().start(),
        )
    }

    pub fn new(target: Arc<TS>,
                     data_provider: Arc<B::DataProvider>,
                     notifications: Sender<Notification>,
    ) -> Self {
        Self {
            b: PhantomData,
            target,
            data_provider,
            notifications,
        }
    }

    pub async fn append_block(&self, block_file: &TS::Writer, height: &Height) -> anyhow::Result<(B::BlockParsed, Vec<B::TxId>)> {
        let block_ref = if let Some(hash) = &height.hash {
            BlockReference::Hash(
                B::BlockHash::from_str(hash).map_err(|_| anyhow!("Not a valid hash"))?
            )
        } else {
            BlockReference::height(height.height)
        };
        let (record, block, txes) = self.data_provider.fetch_block(&block_ref).await?;
        block_file.append(record).await?;
        Ok((block, txes))
    }

    pub async fn append_txes(&self, tx_file: &TS::Writer, block: &B::BlockParsed, txes: &Vec<B::TxId>) -> anyhow::Result<()> {
        for tx_index in 0..txes.len() {
            let data = self.data_provider.fetch_tx(&block, tx_index).await?;
            let _ = tx_file.append(data).await?;
        }
        Ok(())
    }

    ///
    /// Archive a single block with all the transactions
    pub async fn archive_single(&self, height: Height) -> anyhow::Result<()> {
        let start_time = Utc::now();
        let range = Range::Single(height.height);
        let block_file = self.target.create(DataKind::Blocks, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let block_file_url = block_file.get_url();

        let (block, txes) = self.append_block(&block_file, &height).await?;
        block_file.close().await?;

        let notification_block = Notification {
            // common fields
            version: Notification::version(),
            ts: Utc::now(),
            blockchain: self.data_provider.blockchain_id(),
            run: RunMode::Stream,
            height_start: height.height,
            height_end: height.height,

            // specific fields
            file_type: DataKind::Blocks,
            location: block_file_url,
        };
        let _ = self.notifications.send(notification_block.clone()).await;

        let tx_file = self.target.create(DataKind::Transactions, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let tx_file_url = tx_file.get_url();

        self.append_txes(&tx_file, &block, &txes).await?;

        let _ = tx_file.close().await?;
        let notification_tx = Notification {
            file_type: DataKind::Transactions,
            location: tx_file_url,

            ..notification_block
        };
        let _ = self.notifications.send(notification_tx).await;

        let duration = Utc::now().signed_duration_since(start_time);
        tracing::info!("Block {} is archived in {}ms", height.height, duration.num_milliseconds());
        Ok(())
    }

    pub async fn ensure_all(&self, blocks: Range) -> anyhow::Result<()> {
        tracing::info!("Check if blocks are fully archived in range: {}", blocks);
        let mut existing = self.target.list(blocks.clone())?;
        let mut archived = ArchivesList::new();
        let shutdown = global::get_shutdown();
        while let Some(file) = existing.recv().await {
            archived.append(file)?;
        }
        let completed_heights: Vec<u64> = archived.iter()
            .filter(|a| a.is_complete())
            .flat_map(|a| a.range.iter())
            .collect();
        let mut missing = Vec::new();
        for height in blocks.iter() {
            if !completed_heights.contains(&height) {
                missing.push(height);
            }
        }
        if missing.is_empty() {
            tracing::info!("Previous blocks are archived");
            return Ok(());
        }
        tracing::debug!("Missing blocks to download {} (sample: {:?},...)", missing.len(), missing.iter().take(5).collect::<Vec<_>>());

        // Process 10 blocks at a time just to make the output more predictable, b/c otherwise it fills the gaps in a random order and it looks confusing
        let mut heights = missing.chunks(10);
        while let Some(heights_next) = heights.next() {
            let mut jobs = Vec::new();
            for height in heights_next {
                jobs.push(self.archive_single(Height { height: *height, hash: None }));
            }
            tokio::select! {
                _ = shutdown.signalled() => {
                    tracing::info!("Shutdown signal received");
                    return Ok(());
                }
                _ = join_all(jobs) => {}
            }
        }

        tracing::info!("Previous blocks are archived");
        Ok(())
    }

    pub async fn get_range(&self, blocks: &Blocks) -> anyhow::Result<Range> {
        let range = match blocks {
            Blocks::Tail(n) => {
                // for the tail it's possibly that some data is still being written
                // and so for the tail range we don't touch the last 4 blocks
                // TODO blocks length should be configurable
                let height = self.data_provider.height().await?.0 - 4;
                let start = if height > *n {
                    height - n
                } else {
                    0
                };
                Range::new(start, height)
            }
            Blocks::Range(range) => range.clone()
        };
        Ok(range)
    }
}
