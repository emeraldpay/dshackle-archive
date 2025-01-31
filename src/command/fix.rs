use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use anyhow::anyhow;
use async_trait::async_trait;
use shutdown::Shutdown;
use crate::args::Args;
use crate::blockchain::{BlockchainData, BlockchainTypes};
use crate::command::archiver::Archiver;
use crate::command::CommandExecutor;
use crate::notify::{Notifier};
use crate::range::Range;
use crate::storage::TargetStorage;

///
/// Provides `fix` command.
/// It checks the archive for the specified range and add missing data
///
#[derive(Clone)]
pub struct FixCommand<B: BlockchainTypes> {
    b: PhantomData<B>,
    blocks: Blocks,
    archiver: Archiver<B>,
}

#[derive(Clone)]
enum Blocks {
    Tail(u64),
    Range(Range),
}

impl<B: BlockchainTypes> FixCommand<B> {
    pub fn new(config: &Args,
                     shutdown: Shutdown,
                     target: Box<dyn TargetStorage>,
                     data_provider: B::DataProvider,
                     notifier: Box<dyn Notifier>,
    ) -> anyhow::Result<Self> {
        let target = Arc::new(target);
        let data_provider = Arc::new(data_provider);

        let blocks = if let Some(tail) = config.tail {
            Blocks::Tail(tail)
        } else if let Some(range) = &config.range {
            Blocks::Range(Range::from_str(range)?)
        } else {
            return Err(anyhow!("Either `tail` or `range` should be specified"));
        };

        let notifications = notifier.start();
        let archiver = Archiver::new(
            shutdown.clone(), target, data_provider, notifications.clone()
        );
        Ok(Self {
            b: PhantomData,
            archiver,
            blocks,
        })
    }
}

#[async_trait]
impl<B: BlockchainTypes> CommandExecutor for FixCommand<B> {

    async fn execute(&self) -> anyhow::Result<()> {
        let range = match &self.blocks {
            Blocks::Tail(n) => {
                let height = self.archiver.data_provider.height().await?;
                let start = if height.0 > *n {
                    height.0 - n
                } else {
                    0
                };
                Range::new(start, height.0)
            }
            Blocks::Range(range) => range.clone()
        };

        tracing::info!("Fixing range: {}", range);
        self.archiver.ensure_all(range).await
    }
}
