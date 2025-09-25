use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use apache_avro::types::Record;
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use crate::avros::{BLOCK_SCHEMA, TX_SCHEMA};
use crate::blockchain::{BlockDetails, BlockReference, BlockchainData, BlockchainTypes, TxOptions};
use crate::blockchain::connection::Blockchain;

pub struct MockType {}

impl BlockchainTypes for MockType {

    type BlockHash = String;
    type TxId = String;
    type BlockParsed = MockBlock;

    type DataProvider = MockData;

    fn create_data_provider(_blockchain: Blockchain, id: String) -> Self::DataProvider {
        MockData::new(id.as_str())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MockBlock {
    pub height: u64,
    pub hash: String,
    pub transactions: Vec<String>,
}

impl BlockDetails<String> for MockBlock {
    fn txes(&self) -> Vec<String> {
        self.transactions.clone()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MockTx {
    pub hash: String,
}

#[derive(Clone)]
pub struct MockData {
    pub id: String,
    pub blocks: Arc<Mutex<Vec<MockBlock>>>,
    pub txes: Arc<Mutex<Vec<MockTx>>>
}

impl MockData {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            blocks: Arc::new(Mutex::new(Vec::new())),
            txes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn find_block(&self, height: u64) -> Option<MockBlock> {
        let blocks = self.blocks.lock().unwrap();
        blocks.iter()
            .find(|b| b.height == height)
            .cloned()
    }

    pub fn add_block(&self, block: MockBlock) {
        let mut blocks = self.blocks.lock().unwrap();
        blocks.push(block);
    }

    pub fn add_tx(&self, tx: MockTx) {
        let mut txes = self.txes.lock().unwrap();
        txes.push(tx);
    }
}

#[async_trait]
impl BlockchainData<MockType> for MockData {
    fn blockchain_id(&self) -> String {
        self.id.clone()
    }

    async fn fetch_block(&self, height: &BlockReference<String>) -> anyhow::Result<(Record, MockBlock, Vec<String>)> {
        let blocks = self.blocks.lock().unwrap();
        let block = blocks.iter()
            .find(|b| match height {
                BlockReference::Height(h) => b.height == *h,
                BlockReference::Hash(h) => b.hash == *h,
            })
            .ok_or(anyhow!("Block not found"))?
            .clone();

        let mut record = Record::new(&BLOCK_SCHEMA).unwrap();
        record.put("blockchainType", "ETHEREUM");
        record.put("blockchainId", self.blockchain_id());
        record.put("archiveTimestamp", Utc::now().timestamp_millis());
        record.put("height", block.height as i64);
        record.put("blockId", block.hash.clone());
        record.put("parentId", "");
        record.put("timestamp", 1_i64);
        record.put("json", serde_json::to_vec(&block).unwrap());
        record.put("unclesCount", 0);

        let txes = block.transactions.clone();
        Ok((record, block, txes))
    }

    async fn fetch_tx(&self, block: &MockBlock, index: usize, _tx_options: &TxOptions) -> anyhow::Result<Record> {
        let txes = self.txes.lock().unwrap();
        let tx = txes.iter()
            .find(|t| t.hash == block.transactions[index])
            .ok_or(anyhow!("Tx not found"))?
            .clone();

        let mut record = Record::new(&TX_SCHEMA).unwrap();
        record.put("blockchainType", "ETHEREUM");
        record.put("blockchainId", self.blockchain_id());
        record.put("archiveTimestamp", Utc::now().timestamp_millis());
        record.put("height", block.height as i64);
        record.put("blockId", block.hash.clone());
        record.put("timestamp", 1_i64);
        record.put("index", index as i64);
        record.put("txid", tx.hash.clone());
        record.put("json", serde_json::to_vec(&tx).unwrap());
        record.put("raw", serde_json::to_vec(&tx).unwrap());

        Ok(record)
    }

    async fn height(&self) -> anyhow::Result<(u64, String)> {
        let blocks = self.blocks.lock().unwrap();
        blocks
            .iter().max_by(|a, b| a.height.cmp(&b.height))
            .map(|b| (b.height, b.hash.clone()))
            .ok_or_else(|| anyhow!("No blocks found"))
    }
}
