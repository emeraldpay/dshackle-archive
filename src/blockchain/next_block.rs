use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use crate::blockchain::block_seq::BlockSequence;
use crate::blockchain::{BlockchainTypes, EthereumType};
use crate::blockchain::connection::{Blockchain, Height};
use crate::errors::BlockchainError;

///
/// Provides next blocks to archive (basically just for Stream archiving mode)
#[async_trait]
pub trait NextBlock: Send + Sync {
    async fn next_blocks(&self) -> Result<Receiver<Height>, BlockchainError>;
}

///
/// A default implementation that just subscribes to new blocks from the blockchain ("head" subscription)
/// Note that with Head subscription a block may be reorganized later, i.e., a block could be replaced
#[async_trait]
impl NextBlock for Arc<Blockchain> {
    async fn next_blocks(&self) -> Result<Receiver<Height>, BlockchainError> {
        self.subscribe_blocks().await
    }
}

///
/// Provides next finalized blocks for Ethereum-like blockchains.
/// Finalized Block is a block that agreed by the majority of the network and extremely unlikely to be replaced (reorganized)
/// On Ethereum Mainnet, a finalized block is about ~7 minutes behind the head block
pub struct NextFinalizedBlock<B: BlockchainTypes> {
    blockchain: Arc<Blockchain>,
    data_provider: Arc<B::DataProvider>
}

impl NextFinalizedBlock<EthereumType> {
    pub fn new(blockchain: Arc<Blockchain>, data_provider: Arc<<EthereumType as BlockchainTypes>::DataProvider>) -> Self {
        Self { blockchain, data_provider }
    }
}

#[async_trait]
impl NextBlock for NextFinalizedBlock<EthereumType> {
    async fn next_blocks(&self) -> Result<Receiver<Height>, BlockchainError> {
        let mut head = self.blockchain.subscribe_blocks().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let data_provider = self.data_provider.clone();
        tokio::spawn(async move {
            let mut last: Option<Height> = None;
            let mut block_seq = BlockSequence::<EthereumType>::new(10);

            while let Some(_height) = head.recv().await {
                let block = data_provider.get_finalized_block_data().await;
                if block.is_err() {
                    tracing::error!("Failed to get finalized height: {:?}", block.err());
                    continue;
                }
                let mut block = block.unwrap();

                // we can get the same block multiple times, so skip those
                if let Some(last_height) = &last {
                    if block.number() <= last_height.height {
                        continue;
                    }
                }

                // Sometimes we need to produce more than one block from this method.
                // It's because we ask for a block only periodically, and we don't know how many just got finalized, so a few could be finalized in between

                // first: remember the last fetched  block
                let mut next = vec![
                    Height { height: block.number(), hash: Some(format!("0x{:x}", block.header.hash)) }
                ];
                // second: add all the block between this and the one loaded the last time
                while let Some(missing) = block_seq.append(block.number(), block.header.parent_hash, block.header.hash) {
                    let missing = data_provider.get_block_data(&missing).await;
                    if missing.is_err() {
                        tracing::error!("Failed to get missing block data: {:?}", missing.err());
                        break;
                    }
                    block = missing.unwrap();
                    next.push(Height { height: block.number(), hash: Some(format!("0x{:x}", block.header.hash)) });
                }

                // third:
                // now produce the blocks, but make sure it's in the correct order
                // i.e., since we were adding missing (=older) blocks at the end, we need to go from the back to the front
                for h in next.iter().rev() {
                    tracing::debug!("Finalized Height: {}", h.height);
                    if let Err(e) = tx.send(h.clone()).await {
                        tracing::error!("Failed to send finalized height: {}", e);
                        break;
                    }
                    last = Some(h.clone());
                }
            }
        });
        Ok(rx)
    }
}
