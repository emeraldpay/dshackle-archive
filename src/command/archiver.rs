use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc::Sender;
use crate::{
    blockchain::{BlockReference, BlockchainData, BlockchainTypes, MultiBlockReference},
    blockchain::connection::{Height},
    datakind::{DataKind, DataOptions, TraceOptions, TxOptions},
    notify::{Notification, Notifier, RunMode},
    notify::empty::EmptyNotifier,
    range::Range,
    storage::{
        TargetStorage,
        TargetFile,
        TargetFileWriter
    }
};

#[derive(Clone)]
pub struct Archiver<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    pub target: Arc<TS>,
    pub data_provider: Arc<B::DataProvider>,
    pub notifications: Sender<Notification>,
}

#[allow(type_alias_bounds)]
type BlockTransactions<B: BlockchainTypes> = Vec<(B::BlockParsed, Vec<B::TxId>)>;

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

}

///
/// Archiver of a specific table `<T>` based on the type of options.
/// Ex. one for `TraceOptions`, another for `TxOptions`, etc.
trait ArchiveTable<T, B: BlockchainTypes> {
    ///
    /// @param range - range of blocks being processed (could be a single block)
    /// @param notification - a notification struct with base fields filled (run mode, blockchain, heights)
    /// @param blocks - which blocks and their transaction must be archived
    /// @param options - specific options for the table being archived
    async fn process_table(&self, range: Range, notification: Notification, blocks: &BlockTransactions<B>, options: &T) -> anyhow::Result<()>;
}

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

///
/// Processes blocks and stores them in the archive.
#[async_trait]
trait ArchiveBlock<B: BlockchainTypes> {
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
            let (_record, block, txes) = self.data_provider.fetch_block(&height).await?;
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

#[async_trait]
pub trait ArchiveAll<T> {
    async fn archive(&self, what: T, mode: RunMode, options: &DataOptions) -> anyhow::Result<()>;
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> ArchiveAll<Height> for Archiver<B, TS> {
    async fn archive(&self, what: Height, mode: RunMode, options: &DataOptions) -> anyhow::Result<()> {
        let start_time = Utc::now();

        let notification = Notification {
            // common fields
            version: Notification::version(),
            ts: Utc::now(),
            blockchain: self.data_provider.blockchain_id(),
            run: mode,
            height_start: what.height,
            height_end: what.height,

            // specific fields, should be overridden later
            file_type: DataKind::Blocks,
            location: "".to_string(),
        };

        let blocks = self.process_blocks(MultiBlockReference::Single(what.clone()), notification.clone()).await?;
        let range = Range::Single(what.height);

        if let Some(tx_options) = &options.tx {
            self.process_table(range.clone(), notification.clone(), &blocks, tx_options).await?;
        }

        if let Some(traces_options) = &options.trace {
            self.process_table(range.clone(), notification.clone(), &blocks, traces_options).await?;
        }

        let duration = Utc::now().signed_duration_since(start_time);
        tracing::info!("Blocks {} is archived in {}ms", what, duration.num_milliseconds());
        Ok(())
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> ArchiveAll<Range> for Archiver<B, TS> {
    async fn archive(&self, what: Range, mode: RunMode, options: &DataOptions) -> anyhow::Result<()> {
        let start_time = Utc::now();

        let notification = Notification {
            // common fields
            version: Notification::version(),
            ts: Utc::now(),
            blockchain: self.data_provider.blockchain_id(),
            run: mode,
            height_start: what.start(),
            height_end: what.end(),

            // specific fields, should be overridden later
            file_type: DataKind::Blocks,
            location: "".to_string(),
        };

        let blocks = self.process_blocks(MultiBlockReference::Range(what.clone()), notification.clone()).await?;

        if let Some(tx_options) = &options.tx {
            self.process_table(what.clone(), notification.clone(), &blocks, tx_options).await?;
        }

        if let Some(traces_options) = &options.trace {
            self.process_table(what.clone(), notification.clone(), &blocks, traces_options).await?;
        }

        let duration = Utc::now().signed_duration_since(start_time);
        if duration.num_seconds() > 2 {
            tracing::info!("Range {:?} is archived in {}sec", what, duration.num_seconds());
        } else {
            tracing::info!("Range {:?} is archived in {}ms", what, duration.num_milliseconds());
        }

        Ok(())
    }
}
