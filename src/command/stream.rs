use std::marker::PhantomData;
use std::sync::Arc;
use async_trait::async_trait;
use crate::{
    range::Range,
    command::CommandExecutor,
    args::Args,
    blockchain::{
        connection::{Blockchain}
    },
};
use anyhow::{Result};
use shutdown::Shutdown;
use crate::blockchain::BlockchainTypes;
use crate::command::archiver::Archiver;
use crate::storage::TargetStorage;

///
/// Provides `stream` command.
/// It appends fresh blocks one by one to the archive
///
#[derive(Clone)]
pub struct StreamCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    blockchain: Arc<Blockchain>,
    shutdown: Shutdown,
    continue_blocks: Option<u64>,
    archiver: Archiver<B, TS>,
}

impl<B: BlockchainTypes, TS: TargetStorage> StreamCommand<B, TS> {
    pub async fn new(config: &Args,
                     shutdown: Shutdown,
                     archiver: Archiver<B, TS>
    ) -> Result<Self> {
        let blockchain = Arc::new(Blockchain::new(&config.connection, config.as_dshackle_blockchain()?).await?);

        let continue_blocks = if config.continue_last {
            Some(100)
        } else {
            None
        };

        Ok(Self {
            b: PhantomData,
            blockchain,
            continue_blocks,
            shutdown,
            archiver
        })
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> CommandExecutor for StreamCommand<B, TS> {

    async fn execute(&self) -> Result<()> {
        let mut heights = self.blockchain.subscribe_blocks().await?;
        let mut stop = false;
        let mut cotinued = self.continue_blocks.is_none();
        while !stop {
            tokio::select! {
                _ = self.shutdown.signalled() => {
                    tracing::info!("Shutdown signal received");
                    stop = true;
                }
                next = heights.recv()  => {
                    if let Some(height) = next {
                        if !cotinued {
                            self.archiver.ensure_all(
                                Range::up_to(self.continue_blocks.unwrap(), &Range::Single(height.height))
                            ).await?;
                            cotinued = true;
                        }
                        self.archiver.copy_block(height).await?;
                    } else {
                        stop = true;
                    }
                }
            }
        }


        Ok(())
    }
}
