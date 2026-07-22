// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use std::fmt::{Formatter, LowerHex};
use std::str::FromStr;
use std::sync::Arc;
use anyhow::anyhow;
use crate::blockchain::connection::Blockchain;
use anyhow::{Result, Error};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use tokio_retry2::{Retry, RetryError};
use crate::blockchain::{parse_json_response, BitcoinType, BlockDetails, BlockHeaderInfo, BlockReference, BlockchainData, BlockchainTypes, JsonString};
use crate::archiver::datakind::{DataKind, TraceOptions};
use crate::blockchain::next_block::NextBlock;
use crate::errors::BlockchainError;
use crate::global::RETRY_MAX_DELAY_FAST_SECS;
use crate::record::{ArchiveRow, BlockchainType as ArchiveBlockchainType, Field};

#[derive(Clone)]
pub struct BitcoinData {
    blockchain: Arc<Blockchain>,
    blockchain_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Hex32([u8; 32]);
pub type BlockHash = Hex32;
pub type TxHash = Hex32;

impl FromStr for Hex32 {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.len() != 64 {
            return Err(anyhow!("Invalid length: {}", s.len()));
        }
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(s, &mut bytes)
            .map_err(|e| anyhow!("Not a hex: {}", e))?;
        Ok(Hex32(bytes))
    }
}

impl LowerHex for Hex32 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))?;
        Ok(())
    }
}

impl<'de> Deserialize<'de> for Hex32 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error> where D: Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        Hex32::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl BitcoinData {

    pub fn new(blockchain: Blockchain, blockchain_id: String) -> Self {
        Self {
            blockchain: Arc::new(blockchain),
            blockchain_id,
        }
    }

    async fn get_bet_block_hash(&self) -> Result<BlockHash> {
        tracing::debug!("Get best block hash");
        let data = self.blockchain.native_call("getbestblockhash", b"[]".to_vec()).await?;
        let hash = JsonString::try_from(data)?;
        BlockHash::from_str(&hash.0)
    }

    async fn get_block_hash(&self, height: u64) -> Result<BlockHash> {
        tracing::debug!(height = %height, "Get block hash at height");
        let params = format!("[{}]", height).as_bytes().to_vec();
        let data = self.blockchain.native_call("getblockhash", params).await?;
        let hash = JsonString::try_from(data)?;
        BlockHash::from_str(&hash.0)
    }

    async fn get_block_at(&self, height: u64) -> Result<Vec<u8>> {
        tracing::debug!(height = %height, "Get block at height");
        let hash = self.get_block_hash(height).await?;
        self.get_block(&hash).await
    }

    async fn get_block(&self, hash: &BlockHash) -> Result<Vec<u8>> {
        tracing::debug!(block_hash = %format!("{:x}", hash), "Get block by hash");
        let params = format!("[\"{:x}\", 1]", &hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("getblock", params).await?;
        Ok(data)
    }

    /// Fetch a block by hash, retrying transient failures.
    ///
    /// Right after a head event the load balancer can route the call to a
    /// node that has not seen the announced block yet. Unlike Ethereum,
    /// Bitcoin nodes report an unknown block as an error response, so any
    /// failure here is retried as transient rather than crashing the run.
    async fn get_block_expected(&self, hash: &BlockHash) -> Result<Vec<u8>> {
        let retry_strategy = crate::global::retry_strategy(RETRY_MAX_DELAY_FAST_SECS);
        Retry::spawn(retry_strategy, async || {
            self.get_block(hash).await
                .map_err(|e| RetryError::transient(e))
        }).await
    }

    /// Same as [`Self::get_block_expected`] but for a height-based lookup —
    /// covers both the `getblockhash` and `getblock` calls, since either can
    /// land on a node that is still behind that height.
    async fn get_block_at_expected(&self, height: u64) -> Result<Vec<u8>> {
        let retry_strategy = crate::global::retry_strategy(RETRY_MAX_DELAY_FAST_SECS);
        Retry::spawn(retry_strategy, async || {
            self.get_block_at(height).await
                .map_err(|e| RetryError::transient(e))
        }).await
    }

    async fn get_tx(&self, hash: &TxHash) -> Result<Vec<u8>> {
        tracing::debug!(tx_hash = %format!("{:x}", hash), "Get transaction by hash");
        let params = format!("[\"{:x}\", true]", &hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("getrawtransaction", params).await?;
        Ok(data)
    }

    async fn get_tx_raw(&self, hash: &TxHash) -> Result<Vec<u8>> {
        tracing::debug!(tx_hash = %format!("{:x}", hash), "Get raw transaction by hash");
        let params = format!("[\"{:x}\", false]", &hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("getrawtransaction", params).await?;
        let raw = JsonString::try_from(data)?;
        hex::decode(raw.0).map_err(|e| anyhow!("Invalid hex for a raw transaction: {}", e))
    }

}

#[derive(Debug, Clone, Deserialize)]
pub struct BitcoinBlock {
    hash: BlockHash,
    /// Absent in the node response for the genesis block only.
    #[serde(rename = "previousblockhash")]
    previous_block_hash: Option<BlockHash>,
    height: u64,
    #[serde(rename = "tx")]
    pub transactions: Vec<TxHash>,
    time: u64
}

impl BitcoinBlock {
    /// Parent hash with the genesis convention applied: the node omits
    /// `previousblockhash` for block 0, whose parent is all zeros by
    /// consensus definition.
    fn parent_hash(&self) -> BlockHash {
        self.previous_block_hash.clone().unwrap_or(Hex32([0u8; 32]))
    }
}

impl BlockDetails<BitcoinType> for BitcoinBlock {
    fn txes(&self) -> Vec<TxHash> {
        self.transactions.clone()
    }

    fn hash(&self) -> <BitcoinType as BlockchainTypes>::BlockHash {
        self.hash.clone()
    }

    fn parent(&self) -> <BitcoinType as BlockchainTypes>::BlockHash {
        self.parent_hash()
    }
}

/// Parse a block JSON payload; see [`parse_json_response`] for the error handling.
fn parse_block(raw: &[u8]) -> Result<BitcoinBlock, BlockchainError> {
    parse_json_response(raw)
}

#[async_trait]
impl BlockchainData<BitcoinType> for BitcoinData {

    fn blockchain_id(&self) -> String {
        self.blockchain_id.clone()
    }

    async fn fetch_block(&self, height: &BlockReference<BlockHash>) -> Result<(ArchiveRow, BitcoinBlock, Vec<TxHash>)> {
        let raw_block = match height {
            BlockReference::Hash(hash) => self.get_block_expected(hash).await?,
            BlockReference::Height(height) => self.get_block_at_expected(height.height).await?,
        };
        let parsed_block = parse_block(&raw_block)?;

        let row = ArchiveRow {
            kind: DataKind::Blocks,
            blockchain_type: ArchiveBlockchainType::Bitcoin,
            blockchain_id: self.blockchain_id(),
            archive_ts: Utc::now(),
            height: parsed_block.height,
            block_id: format!("{:x}", &parsed_block.hash),
            timestamp: block_timestamp(parsed_block.time),
            parent_id: Some(format!("{:x}", parsed_block.parent_hash())),
            tx_index: None,
            tx_id: None,
            tx_count: Some(parsed_block.transactions.len() as u64),
            fields: vec![Field::BlockJson(raw_block)],
        };

        let transactions = parsed_block.transactions.clone();
        Ok((row, parsed_block, transactions))
    }

    /// Header-only linkage fetch for the re-org follower.
    ///
    /// Overridden not for cost but for its retry shape: the default
    /// implementation delegates to `fetch_block`, whose retries follow the
    /// `--retry` policy and may run forever. The caller here is the re-org
    /// follower itself — the very component that cancels fetches of replaced
    /// blocks — so nothing can cancel *it* out of retrying a block that was
    /// re-orged away. Retries stay bounded, and on give-up the follower
    /// re-validates the chain on the next head event.
    async fn fetch_block_link(
        &self,
        reference: &BlockReference<BlockHash>,
    ) -> Result<BlockHeaderInfo> {
        let retry_strategy = crate::global::retry_strategy_bounded(RETRY_MAX_DELAY_FAST_SECS);
        let raw = Retry::spawn(retry_strategy, async || {
            let result = match reference {
                BlockReference::Hash(hash) => self.get_block(hash).await,
                BlockReference::Height(h) => self.get_block_at(h.height).await,
            };
            result.map_err(|e| RetryError::transient(e))
        }).await?;
        let parsed = parse_block(&raw)?;
        Ok(BlockHeaderInfo {
            height: parsed.height,
            hash: format!("{:x}", parsed.hash),
            parent: format!("{:x}", parsed.parent_hash()),
        })
    }

    async fn fetch_tx(&self, block: &BitcoinBlock, index: usize) -> Result<ArchiveRow> {
        let tx_hash = block.transactions.get(index).ok_or_else(|| anyhow!("Transaction not found"))?;

        let (tx, tx_raw) = tokio::join!(
            self.get_tx(tx_hash),
            self.get_tx_raw(tx_hash)
        );

        Ok(ArchiveRow {
            kind: DataKind::Transactions,
            blockchain_type: ArchiveBlockchainType::Bitcoin,
            blockchain_id: self.blockchain_id(),
            archive_ts: Utc::now(),
            height: block.height,
            block_id: format!("{:x}", &block.hash),
            timestamp: block_timestamp(block.time),
            parent_id: Some(format!("{:x}", block.parent_hash())),
            tx_index: Some(index as u64),
            tx_id: Some(format!("{:x}", tx_hash)),
            tx_count: Some(block.transactions.len() as u64),
            fields: vec![
                Field::TxJson(tx?),
                Field::TxRaw(tx_raw?),
            ],
        })
    }

    async fn fetch_traces(&self, _block: &BitcoinBlock, _index: usize, _options: &TraceOptions) -> Result<ArchiveRow> {
        Err(anyhow!("Traces are not supported for Bitcoin"))
    }

    async fn height(&self) -> Result<(u64, BlockHash)> {
        let best_block = self.get_bet_block_hash().await?;
        let raw_block = self.get_block_expected(&best_block).await?;
        let parsed_block = parse_block(&raw_block)?;
        Ok((parsed_block.height, best_block))
    }

    fn next_finalized_blocks(&self) -> Result<Box<dyn NextBlock>> {
        Err(anyhow!("Next finalized blocks are not supported for Bitcoin"))
    }
}

/// Convert the node's block timestamp (Unix seconds) into a UTC `DateTime`.
fn block_timestamp(secs: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(secs as i64, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
}
