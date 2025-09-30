use std::marker::PhantomData;
use async_trait::async_trait;
use crate::args::Args;
use crate::blockchain::{BlockchainTypes, TxOptions};
use crate::blocks_config::Blocks;
use crate::command::archiver::Archiver;
use crate::command::{CommandExecutor};
use crate::storage::TargetStorage;

///
/// Provides `fix` command.
/// It checks the archive for the specified range and add missing data
///
#[derive(Clone)]
pub struct FixCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    blocks: Blocks,
    archiver: Archiver<B, TS>,
    tx_options: TxOptions,
}

impl<B: BlockchainTypes, TS: TargetStorage> FixCommand<B, TS> {
    pub fn new(config: &Args,
               archiver: Archiver<B, TS>) -> anyhow::Result<Self> {

        let tx_options = TxOptions::from(config);

        Ok(Self {
            b: PhantomData,
            archiver,
            blocks: Blocks::try_from(config)?,
            tx_options,
        })
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> CommandExecutor for FixCommand<B, TS> {

    async fn execute(&self) -> anyhow::Result<()> {
        let range = self.blocks.to_range(self.archiver.data_provider.as_ref()).await?;
        tracing::info!("Fixing range: {}", range);
        self.archiver.ensure_all(range, &self.tx_options).await
    }
}
