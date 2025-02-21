use std::sync::{Arc};
use apache_avro::types::{Record, Value};
use async_trait::async_trait;
use crate::avros::{BLOCK_SCHEMA, TX_SCHEMA};
use crate::errors::{BlockchainError};
use crate::blockchain::connection::{Blockchain};
use chrono::Utc;
use alloy::{
    primitives::{TxHash, BlockHash},
    rpc::types::{Transaction as TransactionJson, Block as BlockJson, Block, TransactionTrait}
};
use crate::blockchain::{BlockDetails, BlockReference, BlockchainData, EthereumType, JsonString};
use anyhow::{Result, anyhow};

#[derive(Clone)]
pub struct EthereumData {
    blockchain: Arc<Blockchain>,
    blockchain_id: String,
}

impl EthereumData {

    pub fn new(blockchain: Blockchain, blockchain_id: String) -> Self {
        Self {
            blockchain: Arc::new(blockchain),
            blockchain_id,
        }
    }

    async fn get_block_at(&self, height: u64) -> Result<Vec<u8>> {
        let params = format!("[\"{:#01x}\", false]", height).as_bytes().to_vec();
        let data = self.blockchain.native_call("eth_getBlockByNumber", params).await?;
        Ok(data)
    }

    async fn get_block(&self, hash: &BlockHash) -> Result<Vec<u8>> {
        let params = format!("[\"0x{:x}\", false]", hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("eth_getBlockByHash", params).await?;
        Ok(data)
    }

    async fn get_uncle(&self, hash: &BlockHash, i: usize) -> Result<Vec<u8>> {
        let params = format!("[\"0x{:x}\", \"0x{:x}\"]", hash, i).as_bytes().to_vec();
        let data = self.blockchain.native_call("eth_getUncleByBlockHashAndIndex", params).await?;
        Ok(data)
    }

    async fn get_tx(&self, hash: &TxHash) -> Result<Vec<u8>> {
        let params = format!("[\"0x{:x}\"]", hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("eth_getTransactionByHash", params).await?;
        Ok(data)
    }

    async fn get_tx_receipt(&self, hash: &TxHash) -> Result<Vec<u8>> {
        let params = format!("[\"0x{:x}\"]", hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("eth_getTransactionReceipt", params).await?;
        Ok(data)
    }

    async fn get_tx_raw(&self, hash: &TxHash) -> Result<Vec<u8>> {
        let params = format!("[\"0x{:x}\"]", hash).as_bytes().to_vec();
        let data_as_json = self.blockchain.native_call("eth_getRawTransactionByHash", params).await?;
        if data_as_json == b"null" {
            return Err(anyhow!("Transaction not found: 0x{:x}", hash));
        }
        let data_as_hex = String::from_utf8(
            data_as_json[3..(data_as_json.len() - 1)].to_vec()
        ).map_err(|_| anyhow!("Invalid hex"))?;
        hex::decode(data_as_hex).map_err(|_| anyhow!("Invalid hex"))
    }

}

#[async_trait]
impl BlockchainData<EthereumType> for EthereumData {

    fn blockchain_id(&self) -> String {
        self.blockchain_id.clone()
    }



    async fn fetch_block(&self, height: &BlockReference<BlockHash>) -> Result<(Record, Block<TxHash>, Vec<TxHash>)> {
        let raw_block = match height {
            BlockReference::Hash(hash) => self.get_block(&hash).await?,
            BlockReference::Height(height) => self.get_block_at(*height).await?,
        };
        let parsed_block = serde_json::from_slice::<BlockJson<TxHash>>(raw_block.as_slice())
            .map_err(|_| BlockchainError::InvalidResponse)?;

        let mut transactions = vec![];

        let mut record = Record::new(&BLOCK_SCHEMA).unwrap();
        record.put("blockchainType", "ETHEREUM");
        record.put("blockchainId", self.blockchain_id());
        record.put("archiveTimestamp", Utc::now().timestamp_millis());
        record.put("height", parsed_block.header.number as i64);
        record.put("blockId", format!("0x{:x}", &parsed_block.header.hash));
        record.put("parentId", format!("0x{:x}", &parsed_block.header.parent_hash));
        record.put("timestamp", (parsed_block.header.timestamp * 1000) as i64);
        record.put("json", raw_block);
        record.put("unclesCount", parsed_block.uncles.len() as i32);

        for (i, _uncle) in parsed_block.uncles.iter().enumerate() {
            let uncle = self.get_uncle(&parsed_block.header.hash, i).await?;
            // TODO should it verify if it has the same hash as expected?
            record.put(format!("uncle{}Json", i).as_str(), uncle);
        }

        for transaction in parsed_block.transactions.txns() {
            transactions.push(transaction.clone());
        }

        Ok((record, parsed_block, transactions))
    }

    async fn fetch_tx(&self, block: &Block<TxHash>, index: usize) -> Result<Record> {
        let tx_hash = block.transactions.as_transactions().map(|txes| txes[index])
            .ok_or_else(|| anyhow!("Transaction not found"))?;

        let (tx_json_bytes, tx_raw, tx_receipt) = tokio::join!(
            self.get_tx(&tx_hash),
            self.get_tx_raw(&tx_hash),
            self.get_tx_receipt(&tx_hash),
        );
        let tx_json_bytes = tx_json_bytes?;

        let parsed_tx = serde_json::from_slice::<TransactionJson>(tx_json_bytes.as_slice())
            .map_err(|e| anyhow!("Invalid Transaction JSON: {}", e))?;

        let mut record = Record::new(&TX_SCHEMA).unwrap();
        record.put("blockchainType", "ETHEREUM");
        record.put("blockchainId", self.blockchain_id());
        record.put("archiveTimestamp", Utc::now().timestamp_millis());
        record.put("height", block.header.number as i64);
        record.put("blockId", format!("0x{:x}", &block.header.hash));
        record.put("timestamp", (block.header.timestamp * 1000) as i64);
        record.put("index", index as i64);
        record.put("txid", format!("0x{:x}", &tx_hash));
        record.put("json", tx_json_bytes);
        record.put("raw", tx_raw?);

        record.put("from",  Value::Union(1, Box::new(Value::String(format!("0x{:x}", parsed_tx.from)))));
        if let Some(to) = parsed_tx.inner.to() {
            record.put("to", Value::Union(1, Box::new(Value::String(format!("0x{:x}", to)))));
        } else {
            record.put("to", Value::Union(0, Box::new(Value::Null)));
        }
        record.put("receiptJson", Value::Union(1, Box::new(Value::Bytes(tx_receipt?))));

        Ok(record)
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
}


fn parse_number(s: String) -> Result<u64> {
    let s = s.trim_start_matches("0x");
    u64::from_str_radix(s, 16).map_err(|e| anyhow!("Invalid number: {}", e))
}

impl BlockDetails<TxHash> for Block<TxHash> {
    fn txes(&self) -> Vec<TxHash> {
        self.transactions.as_transactions()
            .map(|txes| txes.to_vec())
            .unwrap_or_default()
    }
}
