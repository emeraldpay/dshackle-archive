pub mod ethereum;
pub mod connection;

use alloy::primitives::{BlockHash, TxHash};
use alloy::rpc::types::Block;
use apache_avro::types::Record;
use async_trait::async_trait;
use num_traits::cast::ToPrimitive;
use connection::TransactionId;
use anyhow::Result;

pub fn to_zero_hex<T: AsRef<[u8]>>(value: T) -> String {
    format!("0x{}", hex::encode(value))
}

pub fn to_zero_number<T: ToPrimitive>(value: T) -> String {
    format!("0x{}", hex::encode(value.to_u64().unwrap_or_default().to_be_bytes()))
}

pub enum BlockReference {
    Hash(BlockHash),
    Height(u64),
}

impl From<BlockHash> for BlockReference {
    fn from(value: BlockHash) -> Self {
        BlockReference::Hash(value)
    }
}

impl From<u64> for BlockReference {
    fn from(value: u64) -> Self {
        BlockReference::Height(value)
    }
}

#[async_trait]
pub trait BlockchainData: Send + Sync {

    fn blockchain_id(&self) -> String;

    async fn fetch_block(&self, height: &BlockReference) -> Result<(Record, Block<TxHash>, Vec<TransactionId>)>;

    async fn fetch_tx(&self, block: &Block<TxHash>, index: usize) -> Result<Record>;
}
