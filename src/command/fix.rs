use std::marker::PhantomData;
use async_trait::async_trait;
use crate::{
    archiver::{ArchiveAll, Archiver},
    args::Args,
    blockchain::BlockchainTypes,
    command::CommandExecutor,
    notify::RunMode,
    storage::TargetStorage
};
use crate::archiver::blocks_config::Blocks;
use crate::archiver::datakind::DataOptions;

///
/// Provides `fix` command.
/// It checks the archive for the specified range and add missing data
///
#[derive(Clone)]
pub struct FixCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    blocks: Blocks,
    chunk_size: usize,
    archiver: Archiver<B, TS>,
    tx_options: DataOptions,
}

impl<B: BlockchainTypes, TS: TargetStorage> FixCommand<B, TS> {
    pub fn new(config: &Args,
               archiver: Archiver<B, TS>) -> anyhow::Result<Self> {

        let tx_options = DataOptions::from(config);

        Ok(Self {
            b: PhantomData,
            archiver,
            blocks: Blocks::try_from(config)?,
            chunk_size: config.get_chunk_size(),
            tx_options,
        })
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> CommandExecutor for FixCommand<B, TS> {

    async fn execute(&self) -> anyhow::Result<()> {
        let range = self.blocks.to_range(self.archiver.data_provider.as_ref()).await?;
        tracing::info!("Fixing range: {}", range);

        let options = self.tx_options.clone();
        let missing = self.archiver.target.find_incomplete_tables(range, &options).await?;
        for (range, kinds) in missing {
            tracing::info!("Found missing data in range {}: {:?}", range, kinds);
            let chunks = range.split_chunks(self.chunk_size, true);
            let options = self.tx_options.clone().only_include(&kinds);
            for chunk in chunks {
                self.archiver.archive(chunk, RunMode::Fix, &options).await?;
            }
        }
        Ok(())
    }
}
