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
    storage::TargetStorage
};
use anyhow::{Result};
use shutdown::Shutdown;
use crate::blockchain::BlockchainTypes;
use crate::command::archiver::Archiver;
use crate::notify::{Notifier};

///
/// Provides `stream` command.
/// It appends fresh blocks one by one to the archive
///
#[derive(Clone)]
pub struct StreamCommand<B: BlockchainTypes> {
    b: PhantomData<B>,
    blockchain: Arc<Blockchain>,
    shutdown: Shutdown,
    continue_blocks: Option<u64>,
    archiver: Archiver<B>,
}

impl<B: BlockchainTypes> StreamCommand<B> {
    pub async fn new(config: &Args,
                     shutdown: Shutdown,
                     target: Box<dyn TargetStorage>,
                     data_provider: B::DataProvider,
                     notifier: Box<dyn Notifier>,
    ) -> Result<Self> {
        let blockchain = Arc::new(Blockchain::new(&config.connection, config.as_dshackle_blockchain()?).await?);
        let target = Arc::new(target);
        let data_provider = Arc::new(data_provider);

        let continue_blocks = if config.continue_last {
            Some(100)
        } else {
            None
        };
        let notifications = notifier.start();
        let archiver = Archiver::new(
            shutdown.clone(), target, data_provider, notifications.clone()
        );
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
impl<B: BlockchainTypes> CommandExecutor for StreamCommand<B> {

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
