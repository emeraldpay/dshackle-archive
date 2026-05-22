use std::marker::PhantomData;
use std::sync::Arc;
use async_trait::async_trait;
use crate::{
    archiver::{ArchiveAll, Archiver, ScanResume, StreamResume},
    args::Args,
    blockchain::{
        connection::Blockchain,
        BlockchainTypes
    },
    command::CommandExecutor,
    global,
    notify::RunMode,
    storage::{ScanTarget, WriteTarget}
};
use anyhow::{anyhow, Result};
use crate::archiver::datakind::DataOptions;
use crate::args::Follow;
use crate::blockchain::BlockchainData;
use crate::notify::Maturity;

///
/// Provides `stream` command.
/// It appends fresh blocks one by one to the archive.
///
/// Generic over any [`WriteTarget`], so it serves both file-based targets
/// (Avro, JSON) and streaming targets (Pulsar). The optional [`StreamResume`]
/// strategy lets file-based callers opt into `--continue` behaviour while
/// streaming targets simply pass `None` — they don't have a tail-scan
/// capability in the v1 implementation.
pub struct StreamCommand<B: BlockchainTypes, TS: WriteTarget> {
    b: PhantomData<B>,
    blockchain: Arc<Blockchain>,
    archiver: Archiver<B, TS>,
    data_options: DataOptions,
    follow: Follow,
    /// `Some` when the user passed `--continue` *and* the target supports
    /// resume. Streaming targets always carry `None`; for those, `--continue`
    /// is rejected at construction time.
    resume: Option<Arc<dyn StreamResume>>,
}

/// Number of blocks the `--continue` tail scan rewinds before live streaming
/// resumes. Matches the original (pre-refactor) hardcoded value so the
/// observable behaviour of `stream --continue` is unchanged.
const CONTINUE_TAIL_BLOCKS: u64 = 100;

/// Build the [`DataOptions`] used for the stream command — same as
/// [`DataOptions::from(args)`] but with `overwrite: false` so simultaneous
/// streams (e.g., one Head + one Finalized) don't clobber each other's files.
fn stream_data_options(config: &Args) -> DataOptions {
    DataOptions {
        overwrite: false,
        ..DataOptions::from(config)
    }
}

impl<B: BlockchainTypes, TS: WriteTarget> StreamCommand<B, TS> {
    /// Build a stream command for any [`WriteTarget`], with no resume support.
    ///
    /// Used by streaming targets (Pulsar). Rejects `--continue` at startup
    /// because the target can't enumerate existing data. The Pulsar dispatch
    /// path in `main` already rejects `--continue` upfront with a clearer
    /// message; this check is the fallback that catches any future write-only
    /// target wired in the same way.
    pub async fn new(config: &Args, archiver: Archiver<B, TS>) -> Result<Self> {
        if config.continue_last {
            return Err(anyhow!(
                "--continue is not supported by the selected target (no tail-scan capability)"
            ));
        }
        Self::build(config, archiver, stream_data_options(config), None).await
    }

    async fn build(
        config: &Args,
        archiver: Archiver<B, TS>,
        data_options: DataOptions,
        resume: Option<Arc<dyn StreamResume>>,
    ) -> Result<Self> {
        let blockchain = Arc::new(
            Blockchain::new(
                &config.connection,
                config.as_dshackle_blockchain()?,
                config.get_blockchain()?.code(),
            )
            .await?,
        );
        let follow = config.follow.clone();
        Ok(Self {
            b: PhantomData,
            blockchain,
            archiver,
            data_options,
            follow,
            resume,
        })
    }
}

impl<B, TS> StreamCommand<B, TS>
where
    B: BlockchainTypes + 'static,
    TS: ScanTarget + 'static,
{
    /// Build a stream command that honours `--continue` against a target that
    /// can list existing data ([`ScanTarget`]). When `--continue` is not set
    /// the resume strategy is left empty and the behaviour matches
    /// [`StreamCommand::new`].
    pub async fn new_with_resume(config: &Args, archiver: Archiver<B, TS>) -> Result<Self> {
        let data_options = stream_data_options(config);
        // The resumer shares the live stream's DataOptions so re-archived
        // tail blocks land with the same `overwrite: false` semantics as
        // fresh ones.
        let resume: Option<Arc<dyn StreamResume>> = if config.continue_last {
            Some(ScanResume::boxed(
                archiver.clone(),
                CONTINUE_TAIL_BLOCKS,
                data_options.clone(),
            ))
        } else {
            None
        };
        Self::build(config, archiver, data_options, resume).await
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: WriteTarget> CommandExecutor for StreamCommand<B, TS> {

    async fn execute(&self) -> Result<()> {

        let maturity = match self.follow {
            Follow::Latest => Maturity::Head,
            Follow::Finalized => Maturity::Finalized,
        };

        let heights = match self.follow {
            Follow::Latest => {
                Box::new(self.blockchain.clone())
            }
            Follow::Finalized => {
                self.archiver.data_provider.next_finalized_blocks()?
            }
        };
        let heights = Arc::new(heights);
        let mut heights = heights.next_blocks().await?;

        let mut stop = false;
        let mut continued = self.resume.is_none();
        let shutdown = global::get_shutdown();
        while !stop {
            crate::progress::pause();
            tokio::select! {
                _ = shutdown.signalled() => {
                    crate::progress::resume();
                    tracing::info!("Shutdown signal received");
                    stop = true;
                }
                next = heights.recv()  => {
                    crate::progress::resume();
                    if let Some(height) = next {
                        // when we have learned the latest height, we ensure that the last N blocks are archived; but just once
                        if !continued {
                            if let Some(resume) = &self.resume {
                                let up_to_height = height.clone();
                                // we ignore the error here because the new blocks should be more important
                                // and if it failed here then the Fix command can fix it later
                                let _ = resume.ensure_continued(up_to_height).await;
                            }
                            continued = true;
                        }

                        tracing::info!("Archive block: {} {:?}", height.height, height.hash);
                        self.archiver.archive(height, RunMode::Stream, Some(maturity.clone()), &self.data_options).await?;
                    } else {
                        stop = true;
                    }
                }
            }
        }

        Ok(())
    }
}
