use std::marker::PhantomData;
use std::str::FromStr;
use anyhow::anyhow;
use async_trait::async_trait;
use crate::{
    archiver::{ArchiveAll, Archiver},
    args::Args,
    blockchain::BlockchainTypes,
    command::CommandExecutor,
    global,
    notify::RunMode,
    storage::TargetStorage
};
use crate::archiver::datakind::DataOptions;
use crate::archiver::range::Range;

///
/// Provides `archive` command.
/// It builds a large archive from the blockchain by putting multiple blocks (ex., 1000) into one file
///
#[derive(Clone)]
pub struct ArchiveCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    archiver: Archiver<B, TS>,

    range: Range,
    chunk_size: usize,
    data_options: DataOptions,
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> CommandExecutor for ArchiveCommand<B, TS> {

    async fn execute(&self) -> anyhow::Result<()> {
        let shutdown = global::get_shutdown();
        let ranges = self.range.split_chunks(self.chunk_size, false);

        for subrange in ranges {
            if shutdown.is_signalled() {
                break;
            }
            self.archiver.archive(subrange, RunMode::Archive, None, &self.data_options).await?;
        }

        Ok(())
    }
}

impl<B: BlockchainTypes, TS: TargetStorage> ArchiveCommand<B, TS> {

    pub fn new(config: &Args,
                     archiver: Archiver<B, TS>
    ) -> anyhow::Result<Self> {
        let range = config.range.as_ref()
            .map(|s| Range::from_str(s.as_str()))
            .ok_or(anyhow!("Provide range to archive --range"))??;
        let chunk_size = config.range_chunk.unwrap_or(1000);

        let tx_options = DataOptions::from(config);

        Ok(Self {
            b: PhantomData,
            archiver,
            range,
            chunk_size,
            data_options: tx_options,
        })
    }

}
