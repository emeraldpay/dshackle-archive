use std::marker::PhantomData;
use std::sync::Arc;
use async_trait::async_trait;
use crate::{
    archiver::{ArchiveAll, Archiver},
    args::Args,
    blockchain::{
        connection::Height,
        connection::Blockchain,
        BlockchainTypes
    },
    command::CommandExecutor,
    global,
    notify::RunMode,
    storage::TargetStorage
};
use anyhow::Result;
use crate::archiver::datakind::DataOptions;
use crate::archiver::range::Range;
use crate::args::Follow;
use crate::blockchain::BlockchainData;
use crate::notify::Maturity;

///
/// Provides `stream` command.
/// It appends fresh blocks one by one to the archive
///
#[derive(Clone)]
pub struct StreamCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    blockchain: Arc<Blockchain>,
    continue_blocks: Option<u64>,
    archiver: Archiver<B, TS>,
    tx_options: DataOptions,
    follow: Follow,
}

impl<B: BlockchainTypes, TS: TargetStorage> StreamCommand<B, TS> {
    pub async fn new(config: &Args,
                     archiver: Archiver<B, TS>
    ) -> Result<Self> {
        let blockchain = Arc::new(Blockchain::new(&config.connection, config.as_dshackle_blockchain()?).await?);

        let continue_blocks = if config.continue_last {
            Some(100)
        } else {
            None
        };

        let tx_options = DataOptions::from(config);
        let follow = config.follow.clone();

        Ok(Self {
            b: PhantomData,
            blockchain,
            continue_blocks,
            archiver,
            tx_options,
            follow,
        })
    }

    ///
    /// Ensures that the last N blocks are archived, where N is `self.continue_blocks`.
    async fn ensure_continued(&self, height: Height) -> Result<()> {
        if let Some(len) = self.continue_blocks {
            let range = Range::up_to(len, &Range::Single(height.height));
            let options = self.tx_options.clone();
            let missing = self.archiver.target.find_incomplete_tables(range, &options).await?;
            for (range, kinds) in missing {
                let range_opts= options.clone().only_include(&kinds);
                for height in range.iter().collect::<Vec<u64>>() {
                    self.archiver.archive(
                        Height::from(height),
                        RunMode::Stream,
                        None,
                        &range_opts
                    ).await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> CommandExecutor for StreamCommand<B, TS> {

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
        let mut continued = self.continue_blocks.is_none();
        let shutdown = global::get_shutdown();
        while !stop {
            tokio::select! {
                _ = shutdown.signalled() => {
                    tracing::info!("Shutdown signal received");
                    stop = true;
                }
                next = heights.recv()  => {
                    if let Some(height) = next {
                        // when we have learned the latest height, we ensure that the last N blocks are archived; but just once
                        if !continued {
                            let up_to_height = height.clone();
                            // we ignore the error here because the new blocks should be more important
                            // and if it failed here then the Fix command can fix it later
                            let _ = self.ensure_continued(up_to_height).await;
                            continued = true;
                        }

                        tracing::info!("Archive block: {} {:?}", height.height, height.hash);
                        self.archiver.archive(height, RunMode::Stream, Some(maturity.clone()), &self.tx_options).await?;
                    } else {
                        stop = true;
                    }
                }
            }
        }

        Ok(())
    }
}
