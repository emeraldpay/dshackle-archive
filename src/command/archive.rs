use std::marker::PhantomData;
use std::str::FromStr;
use anyhow::anyhow;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use crate::{
    archiver::{ArchiveAll, Archiver},
    args::Args,
    blockchain::BlockchainTypes,
    command::CommandExecutor,
    global,
    notify::RunMode,
    storage::WriteTarget
};
use crate::archiver::datakind::DataOptions;
use crate::archiver::range::Range;

///
/// Provides `archive` command.
/// It builds a large archive from the blockchain by putting multiple blocks (ex., 1000) into one file
///
#[derive(Clone)]
pub struct ArchiveCommand<B: BlockchainTypes, TS: WriteTarget> {
    b: PhantomData<B>,
    archiver: Archiver<B, TS>,

    range: Range,
    chunk_size: usize,
    data_options: DataOptions,
}

#[async_trait]
impl<B: BlockchainTypes, TS: WriteTarget> CommandExecutor for ArchiveCommand<B, TS> {

    async fn execute(&self) -> anyhow::Result<()> {
        let shutdown = global::get_shutdown();
        let ranges = self.range.split_chunks(self.chunk_size, false);

        // Historical archive runs against settled blocks; no re-org signal
        // ever fires here, so a never-cancelled token covers the trait
        // surface without any moving parts.
        let cancel = CancellationToken::new();
        // A failed table is never committed, so for files the subrange stays incomplete and the `fix` command finds it later.
        // An ordered stream cannot be fixed that way: the following subranges would be published after a gap.
        let skip_failed = !self.archiver.target.needs_ordering();
        let mut failed: Vec<Range> = vec![];
        for subrange in ranges {
            if shutdown.is_signalled() {
                break;
            }
            let result = self.archiver.archive(subrange.clone(), RunMode::Archive, None, &self.data_options, &cancel).await;
            if let Err(e) = result {
                if !skip_failed {
                    return Err(e);
                }
                tracing::error!(range = %subrange, "Failed to archive range: {:?}", e);
                failed.push(subrange);
            }
        }
        if !failed.is_empty() {
            let ranges = failed.iter().map(|r| r.to_string()).collect::<Vec<_>>().join(", ");
            return Err(anyhow!("Failed to archive {} range(s): {}. Use the `fix` command to complete them", failed.len(), ranges));
        }

        Ok(())
    }
}

impl<B: BlockchainTypes, TS: WriteTarget> ArchiveCommand<B, TS> {

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
