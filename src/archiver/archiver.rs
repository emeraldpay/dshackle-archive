use std::marker::PhantomData;
use std::sync::Arc;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc::Sender;
use crate::archiver::block::ArchiveBlock;
use crate::archiver::table::ArchiveTable;
use crate::blockchain::{BlockchainData, BlockchainTypes, MultiBlockReference};
use crate::blockchain::connection::Height;
use crate::archiver::datakind::{DataKind, DataOptions};
use crate::notify::empty::EmptyNotifier;
use crate::notify::{Maturity, Notification, Notifier, RunMode};
use crate::archiver::range::Range;
use crate::global;
use crate::storage::TargetStorage;

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

}

#[async_trait]
pub trait ArchiveAll<T> {
    async fn archive(&self, what: T, mode: RunMode, maturity: Option<Maturity>, options: &DataOptions) -> anyhow::Result<()>;
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> ArchiveAll<Height> for Archiver<B, TS> {
    async fn archive(&self, what: Height, mode: RunMode, maturity: Option<Maturity>, options: &DataOptions) -> anyhow::Result<()> {
        let start_time = Utc::now();

        let notification = Notification {
            // common fields
            version: Notification::version(),
            ts: Utc::now(),
            blockchain: self.data_provider.blockchain_id(),
            run: mode,
            height_start: what.height,
            height_end: what.height,
            maturity,

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
    async fn archive(&self, what: Range, mode: RunMode, maturity: Option<Maturity>, options: &DataOptions) -> anyhow::Result<()> {
        let start_time = Utc::now();
        tracing::debug!("Archiving range: {:?}", what);

        let notification = Notification {
            // common fields
            version: Notification::version(),
            ts: Utc::now(),
            blockchain: self.data_provider.blockchain_id(),
            run: mode,
            height_start: what.start(),
            height_end: what.end(),
            maturity,

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

        let shutdown = global::get_shutdown();
        if shutdown.is_signalled() {
            return Ok(());
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
