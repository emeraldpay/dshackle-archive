use std::sync::{Arc};
use async_trait::async_trait;
use crate::errors::{BlockchainError};
use crate::blockchain::connection::{Blockchain};
use chrono::{DateTime, Utc};
use alloy::{
    primitives::{TxHash, BlockHash},
    rpc::types::{Transaction as TransactionJson, Block as BlockJson, Block, TransactionTrait}
};
use alloy::network::TransactionResponse;
use crate::blockchain::{BlockDetails, BlockHeaderInfo, BlockReference, BlockchainData, BlockchainTypes, EthereumType, JsonString};
use anyhow::{Result, anyhow};
use tokio_retry2::{Retry, RetryError};
use crate::archiver::datakind::{DataKind, TraceOptions};
use crate::blockchain::next_block::{NextBlock, NextFinalizedBlock};
use crate::record::{ArchiveRow, BlockchainType as ArchiveBlockchainType, Field};

#[derive(Clone)]
pub struct EthereumData {
    blockchain: Arc<Blockchain>,
    blockchain_id: String,
}

/// Default cap on the time between retry attempts. Higher than the typical
/// 95th-percentile RPC latency so a degraded node has space to recover, but
/// low enough that a transient blip doesn't stall a block for noticeably long.
const RETRY_MAX_DELAY_FAST_SECS: u64 = 2;

/// Cap used by the trace/state-diff helpers — these RPCs are heavier (full
/// `debug_traceTransaction` runs), so a longer backoff is appropriate.
const RETRY_MAX_DELAY_TRACE_SECS: u64 = 5;

impl EthereumData {

    pub fn new(blockchain: Blockchain, blockchain_id: String) -> Self {
        Self {
            blockchain: Arc::new(blockchain),
            blockchain_id,
        }
    }

    async fn get_block_at(&self, height: u64) -> Result<Vec<u8>> {
        tracing::debug!(height = %height, "Get block at height");
        let params = format!("[\"{:#01x}\", false]", height).as_bytes().to_vec();
        let data = self.blockchain.native_call("eth_getBlockByNumber", params).await?;
        Ok(data)
    }

    pub async fn get_block_data(&self, hash: &BlockHash) -> Result<BlockJson<TxHash>> {
        tracing::debug!(hash = %format!("0x{:x}", hash), "Get block JSON");
        let raw_block = self.get_block(hash).await?;
        serde_json::from_slice::<BlockJson<TxHash>>(raw_block.as_slice())
            .map_err(|_| anyhow!("Invalid block JSON response"))
    }

    async fn get_block(&self, hash: &BlockHash) -> Result<Vec<u8>> {
        tracing::debug!(hash = %format!("0x{:x}", hash), "Get block by hash");
        let params = format!("[\"0x{:x}\", false]", hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("eth_getBlockByHash", params).await?;
        Ok(data)
    }

    pub async fn get_finalized_block_data(&self) -> Result<BlockJson<TxHash>> {
        tracing::debug!("Get finalized block JSON");
        let params = "[\"finalized\", false]".as_bytes().to_vec();
        let raw_block = self.blockchain.native_call("eth_getBlockByNumber", params).await?;
        serde_json::from_slice::<BlockJson<TxHash>>(raw_block.as_slice())
            .map_err(|_| anyhow!("Invalid block JSON response"))
    }

    async fn get_uncle(&self, hash: &BlockHash, i: usize) -> Result<Vec<u8>> {
        tracing::debug!(hash = %format!("0x{:x}", hash), "Get uncle {}", i);
        let params = format!("[\"0x{:x}\", \"0x{:x}\"]", hash, i).as_bytes().to_vec();
        let data = self.blockchain.native_call("eth_getUncleByBlockHashAndIndex", params).await?;
        Ok(data)
    }

    async fn get_tx_at(&self, block: &BlockHash, i: usize) -> Result<Vec<u8>> {
        tracing::debug!(block_hash = %format!("0x{:x}", block), tx_index = %i, "Get transaction");
        let retry_strategy = crate::global::retry_strategy(RETRY_MAX_DELAY_FAST_SECS);
        Retry::spawn(retry_strategy, async || {
            let params = format!("[\"0x{:x}\", \"{:#01x}\"]", block, i).as_bytes().to_vec();
            self.blockchain.native_call("eth_getTransactionByBlockHashAndIndex", params).await
                .and_then(|value| if value == b"null" {
                    Err(BlockchainError::InvalidResponse)
                } else {
                    Ok(value)
                })
                .map_err(|e| RetryError::transient(e))
        }).await.map_err(|e| anyhow!("Failed to get transaction at block 0x{:x} index {}: {}", block, i, e))
    }

    async fn get_tx_receipt(&self, hash: &TxHash) -> Result<Vec<u8>> {
        tracing::debug!(tx_hash = %format!("0x{:x}", hash), "Get transaction receipt");
        let params = format!("[\"0x{:x}\"]", hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("eth_getTransactionReceipt", params).await?;
        Ok(data)
    }

    async fn get_tx_raw(&self, hash: &TxHash) -> Result<Vec<u8>> {
        tracing::debug!(tx_hash = %format!("0x{:x}", hash), "Get raw transaction");
        let params = format!("[\"0x{:x}\"]", hash).as_bytes().to_vec();
        let data_as_json = self.blockchain.native_call("eth_getRawTransactionByHash", params).await?;
        if data_as_json == b"null" {
            return Err(anyhow!("Transaction not found: 0x{:x}", hash));
        }
        // Wire format is a JSON string like "0xabcdef…"; strip the surrounding
        // quotes and the `0x` prefix, then hex-decode to bytes.
        let data_as_hex = String::from_utf8(
            data_as_json[3..(data_as_json.len() - 1)].to_vec()
        ).map_err(|_| anyhow!("Invalid hex"))?;
        hex::decode(data_as_hex).map_err(|_| anyhow!("Invalid hex"))
    }

    async fn get_tx_receipt_expected(&self, hash: &TxHash) -> Result<Vec<u8>> {
        let retry_strategy = crate::global::retry_strategy(RETRY_MAX_DELAY_FAST_SECS);
        Retry::spawn(retry_strategy, async || {
            self.get_tx_receipt(hash).await
                .and_then(|value| if value == b"null" {
                    Err(anyhow!("Transaction Receipt not found: 0x{:x}", hash))
                } else {
                    Ok(value)
                })
                .map_err(|e| RetryError::transient(e))
        }).await
    }

    async fn get_tx_raw_expected(&self, hash: &TxHash) -> Result<Vec<u8>> {
        let retry_strategy = crate::global::retry_strategy(RETRY_MAX_DELAY_FAST_SECS);
        Retry::spawn(retry_strategy, async || {
            self.get_tx_raw(hash).await
                .and_then(|value| if value.is_empty() {
                    Err(anyhow!("Transaction Raw not found: 0x{:x}", hash))
                } else {
                    Ok(value)
                })
                .map_err(|e| RetryError::transient(e))
        }).await
    }

    async fn get_tx_trace_expected(&self, hash: &TxHash) -> Result<Vec<u8>> {
        tracing::debug!(tx_hash = %format!("0x{:x}", hash), "Get transaction trace");
        // See https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers#call-tracer
        let tracer = r#"{
            "tracer": "callTracer"
        }"#;
        let params = format!("[\"0x{:x}\", {}]", hash, tracer).as_bytes().to_vec();
        let blockchain = self.blockchain.clone();
        let hash = hash.clone();

        let retry_strategy = crate::global::retry_strategy(RETRY_MAX_DELAY_TRACE_SECS);
        Retry::spawn(retry_strategy, async || {
            blockchain.native_call("debug_traceTransaction", params.clone()).await
                .map_err(|e| anyhow!("Failed to get transaction trace: {}", e))
                .and_then(|value| if value == b"null" {
                    Err(anyhow!("Transaction Raw not found: 0x{:x}", hash))
                } else {
                    Ok(value)
                })
                .map_err(|e| RetryError::transient(e))
        }).await
    }

    async fn get_tx_state_diff_expected(&self, hash: &TxHash) -> Result<Vec<u8>> {
        tracing::debug!(tx_hash = %format!("0x{:x}", hash), "Get transaction state diff");
        // See https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers#prestate-tracer
        let tracer = r#"{
            "tracer": "prestateTracer",
            "tracerConfig": {
                "diffMode": true
            }
        }"#;
        let params = format!("[\"0x{:x}\", {}]", hash, tracer).as_bytes().to_vec();
        let blockchain = self.blockchain.clone();
        let hash = hash.clone();

        let retry_strategy = crate::global::retry_strategy(RETRY_MAX_DELAY_TRACE_SECS);
        Retry::spawn(retry_strategy, async || {
            blockchain.native_call("debug_traceTransaction", params.clone()).await
                .map_err(|e| anyhow!("Failed to get transaction trace: {}", e))
                .and_then(|value| if value == b"null" {
                    Err(anyhow!("Transaction Raw not found: 0x{:x}", hash))
                } else {
                    Ok(value)
                })
                .map_err(|e| RetryError::transient(e))
        }).await
    }
}

/// Build an [`ArchiveRow`] skeleton for a per-tx row (Transactions or TransactionTraces),
/// populated with the common (block + tx) identification fields. Per-kind fields are
/// added by the caller.
fn tx_row(kind: DataKind, blockchain_id: String, block: &Block<TxHash>, index: usize, tx_hash: &TxHash) -> ArchiveRow {
    ArchiveRow {
        kind,
        blockchain_type: ArchiveBlockchainType::Ethereum,
        blockchain_id,
        archive_ts: Utc::now(),
        height: block.header.number,
        block_id: format!("0x{:x}", &block.header.hash),
        timestamp: block_timestamp(block.header.timestamp),
        parent_id: Some(format!("0x{:x}", &block.header.parent_hash)),
        tx_index: Some(index as u64),
        tx_id: Some(format!("0x{:x}", tx_hash)),
        fields: Vec::new(),
    }
}

/// Convert the node's block timestamp (Unix seconds) into a UTC `DateTime`.
fn block_timestamp(secs: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(secs as i64, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
}

#[async_trait]
impl BlockchainData<EthereumType> for EthereumData {

    fn blockchain_id(&self) -> String {
        self.blockchain_id.clone()
    }

    async fn fetch_block(&self, height: &BlockReference<BlockHash>) -> Result<(ArchiveRow, Block<TxHash>, Vec<TxHash>)> {
        let raw_block = match height {
            BlockReference::Hash(hash) => self.get_block(&hash).await?,
            BlockReference::Height(height) => self.get_block_at(height.height).await?,
        };
        let parsed_block = serde_json::from_slice::<BlockJson<TxHash>>(raw_block.as_slice())
            .map_err(|_| BlockchainError::InvalidResponse)?;

        let mut fields = vec![Field::BlockJson(raw_block)];
        for (i, _uncle) in parsed_block.uncles.iter().enumerate() {
            let uncle = self.get_uncle(&parsed_block.header.hash, i).await?;
            // TODO should it verify if it has the same hash as expected?
            fields.push(Field::Uncle { index: i as u8, json: uncle });
        }

        let row = ArchiveRow {
            kind: DataKind::Blocks,
            blockchain_type: ArchiveBlockchainType::Ethereum,
            blockchain_id: self.blockchain_id(),
            archive_ts: Utc::now(),
            height: parsed_block.header.number,
            block_id: format!("0x{:x}", &parsed_block.header.hash),
            timestamp: block_timestamp(parsed_block.header.timestamp),
            parent_id: Some(format!("0x{:x}", &parsed_block.header.parent_hash)),
            tx_index: None,
            tx_id: None,
            fields,
        };

        let transactions: Vec<TxHash> = parsed_block
            .transactions
            .txns()
            .cloned()
            .collect();

        Ok((row, parsed_block, transactions))
    }

    /// Cheap header-only path: pulls one `eth_getBlockBy{Hash,Number}` and
    /// projects just the three fields the re-org follower needs. Skips uncle
    /// RPCs and full row construction.
    async fn fetch_block_link(
        &self,
        reference: &BlockReference<BlockHash>,
    ) -> Result<BlockHeaderInfo> {
        let raw = match reference {
            BlockReference::Hash(hash) => self.get_block(hash).await?,
            BlockReference::Height(h) => self.get_block_at(h.height).await?,
        };
        let parsed = serde_json::from_slice::<BlockJson<TxHash>>(raw.as_slice())
            .map_err(|_| BlockchainError::InvalidResponse)?;
        Ok(BlockHeaderInfo {
            height: parsed.header.number,
            hash: format!("0x{:x}", parsed.header.hash),
            parent: format!("0x{:x}", parsed.header.parent_hash),
        })
    }

    async fn fetch_tx(&self, block: &Block<TxHash>, index: usize) -> Result<ArchiveRow> {
        let block_hash = block.header.hash.clone();
        let tx_hash = block.transactions.as_transactions().map(|txes| txes[index])
            .ok_or_else(|| anyhow!("Transaction not found"))?;


        // Fetch all transaction data in parallel
        let (tx_json_bytes, tx_raw, tx_receipt) = tokio::join!(
            self.get_tx_at(&block_hash, index),
            self.get_tx_raw_expected(&tx_hash),
            self.get_tx_receipt_expected(&tx_hash),
        );

        let tx_json_bytes = tx_json_bytes?;
        let parsed_tx = serde_json::from_slice::<TransactionJson>(tx_json_bytes.as_slice())
            .map_err(|e| anyhow!("Invalid Transaction JSON: {} from {}", e, String::from_utf8_lossy(tx_json_bytes.as_slice()).to_string()))?;

        let mut row = tx_row(DataKind::Transactions, self.blockchain_id(), block, index, &tx_hash);
        row.fields.push(Field::TxJson(tx_json_bytes));
        row.fields.push(Field::TxRaw(tx_raw?));
        row.fields.push(Field::From(format!("0x{:x}", parsed_tx.from())));
        if let Some(to) = parsed_tx.inner.to() {
            row.fields.push(Field::To(format!("0x{:x}", to)));
        }
        row.fields.push(Field::Receipt(tx_receipt?));

        Ok(row)
    }

    async fn fetch_traces(&self, block: &Block<TxHash>, index: usize, options: &TraceOptions) -> Result<ArchiveRow> {
        let tx_hash = block.transactions.as_transactions().map(|txes| txes[index])
            .ok_or_else(|| anyhow!("Transaction not found"))?;

        if !options.include_trace && !options.include_state_diff {
            return Err(anyhow!("At least one of include_trace or include_state_diff must be true"));
        }

        // Fetch all transaction data in parallel
        let (trace_data, state_diff_data) = tokio::join!(
            async {
                if options.include_trace {
                    Some(self.get_tx_trace_expected(&tx_hash).await)
                } else {
                    None
                }
            },
            async {
                if options.include_state_diff {
                    Some(self.get_tx_state_diff_expected(&tx_hash).await)
                } else {
                    None
                }
            }
        );

        let mut row = tx_row(DataKind::TransactionTraces, self.blockchain_id(), block, index, &tx_hash);

        if let Some(trace_result) = trace_data {
            let trace_bytes = trace_result?;
            if trace_bytes == b"null" {
                return Err(anyhow!("Trace data requested but got null for tx 0x{:x}", tx_hash));
            }
            row.fields.push(Field::Trace(trace_bytes));
        } else if options.include_trace {
            return Err(anyhow!("Trace data requested but not available for tx 0x{:x}", tx_hash));
        }

        if let Some(state_diff_result) = state_diff_data {
            let state_diff_bytes = state_diff_result?;
            if state_diff_bytes == b"null" {
                return Err(anyhow!("State Diff data requested but got null for tx 0x{:x}", tx_hash));
            }
            row.fields.push(Field::StateDiff(state_diff_bytes));
        } else if options.include_state_diff {
            return Err(anyhow!("State Diff data requested but not available for tx 0x{:x}", tx_hash));
        }

        Ok(row)
    }


    async fn height(&self) -> Result<(u64, BlockHash)> {
        let height = self.blockchain.native_call("eth_blockNumber", b"[]".to_vec())
            .await?;
        let height = parse_number(JsonString::try_from(height)?.into())?;
        let raw_block = self.get_block_at(height).await?;
        let parsed_block = serde_json::from_slice::<BlockJson<TxHash>>(raw_block.as_slice())
            .map_err(|_| BlockchainError::InvalidResponse)?;

        Ok((parsed_block.header.number, parsed_block.header.hash))
    }

    fn next_finalized_blocks(&self) -> Result<Box<dyn NextBlock>> {
        Ok(Box::new(NextFinalizedBlock::new(
            self.blockchain.clone(),
            Arc::new(self.clone()),
        )))
    }
}


fn parse_number(s: String) -> Result<u64> {
    let s = s.trim_start_matches("0x");
    u64::from_str_radix(s, 16).map_err(|e| anyhow!("Invalid number: {}", e))
}

impl BlockDetails<EthereumType> for Block<TxHash> {
    fn txes(&self) -> Vec<TxHash> {
        self.transactions.as_transactions()
            .map(|txes| txes.to_vec())
            .unwrap_or_default()
    }

    fn hash(&self) -> <EthereumType as BlockchainTypes>::BlockHash {
        self.header.hash
    }

    fn parent(&self) -> <EthereumType as BlockchainTypes>::BlockHash {
        self.header.parent_hash
    }
}
