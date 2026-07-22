// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

pub mod ethereum;
pub mod connection;
pub mod bitcoin;
#[cfg(test)]
pub mod mock;
pub mod next_block;
pub mod block_seq;

use std::fmt::Debug;
use std::hash::Hash;
use std::str::FromStr;
use async_trait::async_trait;
use anyhow::{anyhow, Error, Result};
use serde::Deserialize;
use crate::{
    blockchain::{
        bitcoin::BitcoinData,
        connection::{Blockchain},
        ethereum::EthereumData,
        next_block::{NextBlock}
    },
    archiver::{
        datakind::TraceOptions,
    },
    record::{ArchiveRow, BlockchainType},
};
use crate::archiver::range::Height;
use crate::errors::BlockchainError;

/// Parse a JSON payload from a blockchain response, keeping the failure
/// diagnosable: `null` responses are retried away before parsing (see the
/// chain providers' `*_expected` fetchers), so a failure here means the
/// upstream returned genuinely malformed data — log the serde error and a
/// payload sample before collapsing it into the opaque
/// [`BlockchainError::InvalidResponse`].
pub fn parse_json_response<T: serde::de::DeserializeOwned>(raw: &[u8]) -> Result<T, BlockchainError> {
    serde_json::from_slice::<T>(raw)
        .map_err(|e| {
            let sample = String::from_utf8_lossy(&raw[..raw.len().min(1024)]);
            tracing::warn!("Invalid JSON response ({}): {}", e, sample);
            BlockchainError::InvalidResponse
        })
}

///
/// Defined the data types for a blockchain
pub trait BlockchainTypes: Send + Sync + Sized {

    ///
    /// Runtime discriminator for the blockchain family. Lets generic code
    /// (e.g. the Pulsar topic-creation path) branch on Bitcoin vs Ethereum
    /// without needing a `match` on the type-erased provider.
    const BLOCKCHAIN_TYPE: BlockchainType;

    ///
    /// Type of the Block Hash / Block Identifier
    type BlockHash: FromStr + PartialEq + Hash + Eq + Send + Sync + Debug + Clone + 'static;

    ///
    /// Type of the Transaction Hash / Transaction Identifier
    type TxId: FromStr + PartialEq + Hash + Eq + Send + Sync + Debug + Clone + 'static;

    ///
    /// Block details converted from the JSON response
    type BlockParsed: BlockDetails<Self> + for<'a> Deserialize<'a> + Send + Sync + Clone + 'static;

    ///
    /// Data provider for the blockchain
    type DataProvider: BlockchainData<Self> + Send + Sync + Sized + Clone + 'static;

    fn create_data_provider(blockchain: Blockchain, id: String) -> Self::DataProvider;
}


pub struct EthereumType {}
impl BlockchainTypes for EthereumType {
    const BLOCKCHAIN_TYPE: BlockchainType = BlockchainType::Ethereum;

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
    const BLOCKCHAIN_TYPE: BlockchainType = BlockchainType::Bitcoin;

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
    /// Get the details for the block. Returns the format-neutral [`ArchiveRow`] alongside
    /// the parsed block (used to enumerate transactions) and the list of transaction ids.
    async fn fetch_block(&self, height: &BlockReference<T::BlockHash>) -> Result<(ArchiveRow, T::BlockParsed, Vec<T::TxId>)>;

    ///
    /// Lightweight header-only fetch returning the block's `(height, hash, parent)`
    /// linkage. Used by the re-org-aware live follower to walk parent hashes
    /// without paying for the full `fetch_block` (which also fetches uncle
    /// JSON and builds an [`ArchiveRow`]).
    ///
    /// The default implementation just calls `fetch_block` and projects the
    /// linkage fields; concrete implementations may override with a cheaper
    /// path that skips uncle / row construction.
    async fn fetch_block_link(
        &self,
        reference: &BlockReference<T::BlockHash>,
    ) -> Result<BlockHeaderInfo> {
        let (row, _parsed, _txes) = self.fetch_block(reference).await?;
        Ok(BlockHeaderInfo {
            height: row.height,
            hash: row.block_id,
            parent: row.parent_id.unwrap_or_default(),
        })
    }

    ///
    /// Get the details for the transaction.
    async fn fetch_tx(&self, block: &T::BlockParsed, index: usize) -> Result<ArchiveRow>;

    ///
    /// Get the details for the transaction trace.
    async fn fetch_traces(&self, block: &T::BlockParsed, index: usize, options: &TraceOptions) -> Result<ArchiveRow>;

    ///
    /// Get the current height
    async fn height(&self) -> Result<(u64, T::BlockHash)>;

    ///
    /// Create the next Finalized blocks provider (applicable for Ethereum blockchain types)
    fn next_finalized_blocks(&self) -> Result<Box<dyn NextBlock>>;

}

///
/// A reference to a block on blockchain

pub enum BlockReference<T> where T: FromStr {
    /// By its hash / identifier, i.e. when the height is unknown
    Hash(T),
    /// Byt its height (and optionally hash)
    Height(Height),
}

impl Debug for BlockReference<String> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockReference::Hash(h) => write!(f, "Hash({})", h),
            BlockReference::Height(h) => write!(f, "Height({})", h.height),
        }
    }
}

impl <T> From<Height> for BlockReference<T> where T: FromStr {
    fn from(value: Height) -> Self {
        match &value.hash {
            Some(h) => match T::from_str(h.as_ref()) {
                Ok(hash) => BlockReference::Hash(hash),
                Err(_) => {
                    tracing::warn!("Failed to parse block hash from: {}", h);
                    BlockReference::Height(value)
                }
            },
            None => BlockReference::Height(value)
        }
    }
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
        BlockReference::Height(h.into())
    }

}

pub trait BlockDetails<T> where T: BlockchainTypes{
    fn txes(&self) -> Vec<T::TxId>;
    fn hash(&self) -> T::BlockHash;
    fn parent(&self) -> T::BlockHash;
}

///
/// Lightweight block-header linkage returned by
/// [`BlockchainData::fetch_block_link`]. The string fields use the same
/// formatting convention as [`crate::record::ArchiveRow::block_id`] and
/// [`crate::record::ArchiveRow::parent_id`] (chain-specific; e.g. `0x…` for
/// Ethereum) so the values round-trip through the
/// `From<Height> for BlockReference` impl.
#[derive(Debug, Clone)]
pub struct BlockHeaderInfo {
    pub height: u64,
    pub hash: String,
    pub parent: String,
}

pub struct JsonString(pub String);

impl Into<String> for JsonString {
    fn into(self) -> String {
        self.0
    }
}

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
