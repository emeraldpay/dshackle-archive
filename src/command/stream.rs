use std::collections::HashMap;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use async_trait::async_trait;
use crate::{
    range::Range,
    errors::Error,
    datakind::DataKind,
    command::CommandExecutor,
    args::Args,
    blockchain::{
        BlockchainData,
        BlockReference,
        connection::{Blockchain, Height}
    },
    storage::TargetStorage
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use futures_util::future::join_all;
use shutdown::Shutdown;
use tokio::sync::mpsc::Sender;
use crate::blockchain::BlockchainTypes;
use crate::notify::{Notification, Notifier, RunMode};

///
/// Provides `stream` command.
/// It appends fresh blocks one by one to the archive
///
#[derive(Clone)]
pub struct StreamCommand<B: BlockchainTypes> {
    b: PhantomData<B>,
    blockchain: Arc<Blockchain>,
    target: Arc<Box<dyn TargetStorage>>,
    archiver: Arc<B::DataProvider>,
    shutdown: Shutdown,
    continue_blocks: Option<u64>,
    notifications: Sender<Notification>,
}

impl<B: BlockchainTypes> StreamCommand<B> {
    pub async fn new(config: &Args,
                     shutdown: Shutdown,
                     target: Box<dyn TargetStorage>,
                     archiver: B::DataProvider,
                     notifier: Box<dyn Notifier>,
    ) -> Result<Self, Error> {
        let blockchain = Blockchain::new(&config.connection, config.as_dshackle_chain()?).await?;
        let continue_blocks = if config.continue_last {
            Some(100)
        } else {
            None
        };
        let notifications = notifier.start();
        Ok(Self {
            b: PhantomData,
            blockchain: Arc::new(blockchain), target: Arc::new(target), archiver: Arc::new(archiver),
            continue_blocks,
            shutdown,
            notifications,
        })
    }

    async fn copy_block(&self, height: Height) -> Result<()> {
        tracing::info!("Archive block: {} {:?}", height.height, height.hash);
        let archiver = self.archiver.clone();
        let range = Range::Single(height.height);
        let block_file = self.target.create(DataKind::Blocks, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let block_file_url = block_file.get_url();
        let block_ref = if let Some(hash) = &height.hash {
            BlockReference::Hash(
                B::BlockHash::from_str(hash).map_err(|_| anyhow!("Not a valid hash"))?
            )
        } else {
            BlockReference::height(height.height)
        };
        let (record, block, txes) = archiver.fetch_block(&block_ref).await?;
        let _ = match block_file.append(record).await {
            Ok(_) => block_file.close().await?,
            Err(e) => return Err(e)
        };
        let notification_block = Notification {
            // common fields
            version: Notification::version(),
            ts: Utc::now(),
            blockchain: self.archiver.blockchain_id(),
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
        for tx_index in 0..txes.len() {
            let data = archiver.fetch_tx(&block, tx_index).await?;
            let _ = tx_file.append(data).await?;
        }
        let _ = tx_file.close().await?;
        let notification_tx = Notification {
            file_type: DataKind::Transactions,
            location: tx_file_url,

            ..notification_block
        };
        let _ = self.notifications.send(notification_tx).await;

        tracing::info!("Block {} is archived", height.height);
        Ok(())
    }

    async fn ensure_all(&self, blocks: Range) -> Result<()> {
        tracing::info!("Check if blocks are fully archived: {}..{}", blocks.start(), blocks.end());
        let mut existing = self.target.list(blocks.clone())?;
        let mut archived = HashMap::new();
        let shutdown = self.shutdown.clone();
        while let Some(file) = existing.recv().await {
            let range = file.range;
            let kind = file.kind;

            for height in range.iter() {
                let entry = archived.entry(height).or_insert(Vec::new());
                entry.push(kind.clone());
            }
        }
        let mut missing = Vec::new();
        for height in blocks.iter() {
            if let Some(entry) = archived.get(&height) {
                if entry.contains(&DataKind::Blocks) && entry.contains(&DataKind::Transactions) {
                    continue;
                }
            }
            missing.push(height);
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
                jobs.push(self.copy_block(Height { height: *height, hash: None }));
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
}

#[async_trait]
impl<B: BlockchainTypes> CommandExecutor for StreamCommand<B> {

    async fn execute(&self) -> Result<()> {
        let mut heights = self.blockchain.subscribe_blocks().await?;
        let mut stop = false;
        let mut cotinued = self.continue_blocks.is_none();
        while !stop {
            tokio::select! {
                _ = self.shutdown.signalled() => {
                    tracing::info!("Shutdown signal received");
                    stop = true;
                }
                next = heights.recv()  => {
                    if let Some(height) = next {
                        if !cotinued {
                            self.ensure_all(
                                Range::up_to(self.continue_blocks.unwrap(), &Range::Single(height.height))
                            ).await?;
                            cotinued = true;
                        }
                        self.copy_block(height).await?;
                    } else {
                        stop = true;
                    }
                }
            }
        }


        Ok(())
    }
}
