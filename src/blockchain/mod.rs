pub mod ethereum;
pub mod connection;
pub mod bitcoin;

use std::str::FromStr;
use apache_avro::types::Record;
use async_trait::async_trait;
use anyhow::{anyhow, Error, Result};
use crate::blockchain::bitcoin::BitcoinData;
use crate::blockchain::connection::Blockchain;
use crate::blockchain::ethereum::EthereumData;

///
/// Defined the data types for a blockchain
pub trait BlockchainTypes: Send + Sync + Sized {

    ///
    /// Type of the Block Hash / Block Identifier
    type BlockHash: FromStr + Send + Sync;

    ///
    /// Type of the Transaction Hash / Transaction Identifier
    type TxId: Send + Sync;

    ///
    /// Block details converted from the JSON response
    type BlockParsed: Send + Sync;

    ///
    /// Data provider for the blockchain
    type DataProvider: BlockchainData<Self> + Send + Sync + Sized;

    fn create_data_provider(blockchain: Blockchain, id: String) -> Self::DataProvider;
}

pub enum BlockchainType {
    Ethereum,
    Bitcoin,
}

pub struct EthereumType {}
impl BlockchainTypes for EthereumType {
    type BlockHash = alloy::primitives::BlockHash;
    type TxId = alloy::primitives::TxHash;
    type BlockParsed = alloy::rpc::types::Block<Self::TxId>;

    type DataProvider = EthereumData;

    fn create_data_provider(blockchain: Blockchain, id: String) -> Self::DataProvider {
        EthereumData::new(blockchain, id)
    }
}
pub struct BitcoinType {}
impl BlockchainTypes for BitcoinType {
    type BlockHash = bitcoin::BlockHash;
    type TxId = bitcoin::TxHash;
    type BlockParsed = bitcoin::BitcoinBlock;

    type DataProvider = BitcoinData;

    fn create_data_provider(blockchain: Blockchain, id: String) -> Self::DataProvider {
        BitcoinData::new(blockchain, id)
    }
}

///
/// Data provider for the blockchain
#[async_trait]
pub trait BlockchainData<T: BlockchainTypes>: Send + Sync {
    ///
    /// Actual blockchain id, as specified by the user, for the field of `blockchainId` in the output records.
    /// Ex for the blockchain "Ethereum Classic" the user may set the id as "ETC" and for "Ethereum" as "ETH",
    /// while the both blockchain are of the type "Ethereum" and share the same code/types/etc except this id
    fn blockchain_id(&self) -> String;

    ///
    /// Get the details for the block
    async fn fetch_block(&self, height: &BlockReference<T::BlockHash>) -> Result<(Record, T::BlockParsed, Vec<T::TxId>)>;

    ///
    /// Get the details for the transaction
    async fn fetch_tx(&self, block: &T::BlockParsed, index: usize) -> Result<Record>;
}

///
/// A reference to a block
pub enum BlockReference<T> where T: FromStr {
    /// By its hash / identifier
    Hash(T),
    /// Byt its height
    Height(u64),
}

impl<T> FromStr for BlockReference<T> where T: FromStr {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let inner = T::from_str(s)
            .map_err(|_| anyhow!("Failed to convert from string. Value {}", s))?;
        Ok(BlockReference::Hash(inner))
    }
}

impl<T> BlockReference<T> where T: FromStr {

    pub fn hash(h: T) -> Self {
        BlockReference::Hash(h)
    }

    pub fn height(h: u64) -> Self {
        BlockReference::Height(h)
    }

}

pub struct JsonString(String);

impl TryFrom<Vec<u8>> for JsonString {
    type Error = Error;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        if value == b"null" {
            return Err(anyhow!("Null value"));
        }
        let str = String::from_utf8_lossy(&value[1..(value.len() - 1)]).to_string();
        Ok(JsonString(str))
    }
}

pub enum OptionalJson<T> {
    Some(T),
    None,
}

impl<T> TryFrom<Vec<u8>> for OptionalJson<T> where T: TryFrom<Vec<u8>> {
    type Error = Error;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        if value == b"null" {
            return Ok(OptionalJson::None);
        }
        let value = T::try_from(value)
            .map_err(|_| anyhow!("Failed to convert from bytes"))?;
        Ok(OptionalJson::Some(value))
    }
}
