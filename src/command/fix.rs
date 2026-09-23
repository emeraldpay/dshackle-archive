use std::marker::PhantomData;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use crate::{archiver::{ArchiveAll, Archiver}, args::Args, blockchain::BlockchainTypes, command::CommandExecutor, global, notify::RunMode, storage::ScanTarget};
use crate::archiver::blocks_config::Blocks;
use crate::archiver::datakind::DataOptions;

///
/// Provides `fix` command.
/// It checks the archive for the specified range and add missing data
///
#[derive(Clone)]
pub struct FixCommand<B: BlockchainTypes, TS: ScanTarget> {
    b: PhantomData<B>,
    blocks: Blocks,
    chunk_size: usize,
    archiver: Archiver<B, TS>,
    tx_options: DataOptions,
}

impl<B: BlockchainTypes, TS: ScanTarget> FixCommand<B, TS> {
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
impl<B: BlockchainTypes, TS: ScanTarget> CommandExecutor for FixCommand<B, TS> {

    async fn execute(&self) -> anyhow::Result<()> {
        let shutdown = global::get_shutdown();
        let range = self.blocks.to_range(self.archiver.data_provider.as_ref()).await?;
        // A tail starts at an arbitrary height that moves with every run. If it's left mid-chunk, the existing chunk file
        // is not found for it, and the partial head of that chunk would be re-archived on each run.
        let range = if self.blocks.is_tail() {
            match range.trim_to_chunk_start(self.chunk_size) {
                Some(range) => range,
                None => {
                    tracing::info!("Tail {} doesn't reach a chunk boundary. Nothing to fix", range);
                    return Ok(());
                }
            }
        } else {
            range
        };
        let dry_run = global::is_dry_run();
        tracing::info!("Fixing range: {}", range);

        let options = DataOptions {
            // we always keep existing files in Fix
            overwrite: false,
            ..self.tx_options.clone()
        };
        let missing = self.archiver.target.find_incomplete_tables(range, &options).await?;
        // `fix` runs against settled archive state — no re-org signal applies.
        let cancel = CancellationToken::new();
        for (range, kinds) in missing {
            if shutdown.is_signalled() {
                break;
            }
            tracing::info!(range = %range, "Found missing data: {:?}", kinds);
            let chunks = range.split_chunks(self.chunk_size, false);
            let options = self.tx_options.clone().only_include(&kinds);
            for chunk in chunks {
                if shutdown.is_signalled() {
                    break;
                }
                tracing::info!(range = %chunk, "Fixing chunk");
                if !dry_run {
                    self.archiver.archive(chunk, RunMode::Fix, None, &options, &cancel).await?;
                }
            }
        }
        Ok(())
    }
}
