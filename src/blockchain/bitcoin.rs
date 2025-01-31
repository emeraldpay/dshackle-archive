use std::fmt::{Formatter, LowerHex};
use std::str::FromStr;
use std::sync::Arc;
use anyhow::anyhow;
use crate::blockchain::connection::Blockchain;
use anyhow::{Result, Error};
use apache_avro::types::Record;
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Deserializer};
use crate::avros::{BLOCK_SCHEMA, TX_SCHEMA};
use crate::blockchain::{BitcoinType, BlockReference, BlockchainData, JsonString};

#[derive(Clone)]
pub struct BitcoinData {
    blockchain: Arc<Blockchain>,
    blockchain_id: String,
}

#[derive(Debug, Clone, PartialEq)]
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
        let data = self.blockchain.native_call("getbestblockhash", b"[]".to_vec()).await?;
        let hash = JsonString::try_from(data)?;
        BlockHash::from_str(&hash.0)
    }

    async fn get_block_hash(&self, height: u64) -> Result<BlockHash> {
        let params = format!("[\"{}\"]", height).as_bytes().to_vec();
        let data = self.blockchain.native_call("getblockhash", params).await?;
        let hash = JsonString::try_from(data)?;
        BlockHash::from_str(&hash.0)
    }

    async fn get_block_at(&self, height: u64) -> Result<Vec<u8>> {
        let hash = self.get_block_hash(height).await?;
        self.get_block(&hash).await
    }

    async fn get_block(&self, hash: &BlockHash) -> Result<Vec<u8>> {
        let params = format!("[\"{:x}\", 1]", &hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("getblock", params).await?;
        Ok(data)
    }

    async fn get_tx(&self, hash: &TxHash) -> Result<Vec<u8>> {
        let params = format!("[\"{:x}\", true]", &hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("getrawtransaction", params).await?;
        Ok(data)
    }

    async fn get_tx_raw(&self, hash: &TxHash) -> Result<Vec<u8>> {
        let params = format!("[\"{:x}\", false]", &hash).as_bytes().to_vec();
        let data = self.blockchain.native_call("getrawtransaction", params).await?;
        let raw = JsonString::try_from(data)?;
        hex::decode(raw.0).map_err(|e| anyhow!("Invalid hex for a raw transaction: {}", e))
    }

}

#[derive(Debug, Deserialize)]
pub struct BitcoinBlock {
    hash: BlockHash,
    #[serde(rename = "previousblockhash")]
    previous_block_hash: BlockHash,
    height: u64,
    #[serde(rename = "tx")]
    pub transactions: Vec<TxHash>,
    time: u64
}

#[async_trait]
impl BlockchainData<BitcoinType> for BitcoinData {

    fn blockchain_id(&self) -> String {
        self.blockchain_id.clone()
    }

    async fn fetch_block(&self, height: &BlockReference<BlockHash>) -> Result<(Record, BitcoinBlock, Vec<TxHash>)> {
        let raw_block = match height {
            BlockReference::Hash(hash) => self.get_block(hash).await?,
            BlockReference::Height(height) => self.get_block_at(*height).await?,
        };
        let parsed_block = serde_json::from_slice::<BitcoinBlock>(&raw_block)?;

        let mut record = Record::new(&BLOCK_SCHEMA).unwrap();
        record.put("blockchainType", "BITCOIN");
        record.put("blockchainId", self.blockchain_id());
        record.put("archiveTimestamp", Utc::now().timestamp_millis());
        record.put("height", parsed_block.height as i64);
        record.put("blockId", format!("{:x}", &parsed_block.hash));
        record.put("parentId", format!("{:x}", &parsed_block.previous_block_hash));
        record.put("timestamp", (parsed_block.time * 1000) as i64);
        record.put("json", raw_block);
        record.put("unclesCount", 0);

        let transactions = parsed_block.transactions.clone();

        Ok((record, parsed_block, transactions))
    }

    async fn fetch_tx(&self, block: &BitcoinBlock, index: usize) -> Result<Record> {
        let tx_hash = block.transactions.get(index).ok_or_else(|| anyhow!("Transaction not found"))?;

        let (tx, tx_raw) = tokio::join!(
            self.get_tx(tx_hash),
            self.get_tx_raw(tx_hash)
        );

        let mut record = Record::new(&TX_SCHEMA).unwrap();
        record.put("blockchainType", "BITCOIN");
        record.put("blockchainId", self.blockchain_id());
        record.put("archiveTimestamp", Utc::now().timestamp_millis());
        record.put("height", block.height as i64);
        record.put("blockId", format!("{:x}", &block.hash));
        record.put("timestamp", (block.time * 1000) as i64);
        record.put("index", index as i64);
        record.put("txid", format!("{:x}", &tx_hash));
        record.put("json", tx?);
        record.put("raw", tx_raw?);

        Ok(record)
    }

    async fn height(&self) -> Result<(u64, BlockHash)> {
        let best_block = self.get_bet_block_hash().await?;
        let raw_block = self.get_block(&best_block).await?;
        let parsed_block = serde_json::from_slice::<BitcoinBlock>(&raw_block)?;
        Ok((parsed_block.height, best_block))
    }
}
