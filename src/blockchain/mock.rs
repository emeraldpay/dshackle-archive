use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use apache_avro::types::Record;
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use crate::avros::{BLOCK_SCHEMA, TX_SCHEMA, TX_TRACE_SCHEMA};
use crate::blockchain::{BlockDetails, BlockReference, BlockchainData, BlockchainTypes};
use crate::blockchain::connection::Blockchain;
use crate::archiver::datakind::TraceOptions;
use crate::blockchain::next_block::{NextBlock};

#[derive(Clone)]
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
    pub parent: String,
    pub transactions: Vec<String>,
}

impl BlockDetails<MockType> for MockBlock {
    fn txes(&self) -> Vec<String> {
        self.transactions.clone()
    }

    fn hash(&self) -> String {
        self.hash.clone()
    }

    fn parent(&self) -> String {
        self.parent.clone()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MockTx {
    pub hash: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MockTrace {
    pub tx_hash: String,
    pub trace_json: Option<Vec<u8>>,
    pub state_diff_json: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct MockData {
    pub id: String,
    pub blocks: Arc<Mutex<Vec<MockBlock>>>,
    pub txes: Arc<Mutex<Vec<MockTx>>>,
    pub traces: Arc<Mutex<Vec<MockTrace>>>,
}

impl MockData {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            blocks: Arc::new(Mutex::new(Vec::new())),
            txes: Arc::new(Mutex::new(Vec::new())),
            traces: Arc::new(Mutex::new(Vec::new())),
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

    pub fn add_trace(&self, trace: MockTrace) {
        let mut traces = self.traces.lock().unwrap();
        traces.push(trace);
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
                BlockReference::Height(h) => b.height == h.height,
                BlockReference::Hash(h) => b.hash == *h,
            })
            .ok_or(anyhow!("Block not found: {:?}", height))?
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

    async fn fetch_tx(&self, block: &MockBlock, index: usize) -> anyhow::Result<Record> {
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
    
    async fn fetch_traces(&self, block: &MockBlock, index: usize, options: &TraceOptions) -> anyhow::Result<Record> {
        let traces = self.traces.lock().unwrap();
        let tx_hash = &block.transactions[index];
        let trace = traces.iter()
            .find(|t| &t.tx_hash == tx_hash)
            .ok_or(anyhow!("Trace not found for tx: {}", tx_hash))?
            .clone();

        let mut record = Record::new(&TX_TRACE_SCHEMA).unwrap();
        record.put("blockchainType", "ETHEREUM");
        record.put("blockchainId", self.blockchain_id());
        record.put("archiveTimestamp", Utc::now().timestamp_millis());
        record.put("height", block.height as i64);
        record.put("blockId", block.hash.clone());
        record.put("timestamp", 1_i64);
        record.put("index", index as i64);
        record.put("txid", tx_hash.clone());

        if options.include_trace {
            if let Some(trace_json) = trace.trace_json {
                record.put("traceJson", apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Bytes(trace_json))));
            } else {
                record.put("traceJson", apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));
            }
        } else {
            record.put("traceJson", apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));
        }

        if options.include_state_diff {
            if let Some(state_diff_json) = trace.state_diff_json {
                record.put("stateDiffJson", apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Bytes(state_diff_json))));
            } else {
                record.put("stateDiffJson", apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));
            }
        } else {
            record.put("stateDiffJson", apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)));
        }

        Ok(record)
    }

    async fn height(&self) -> anyhow::Result<(u64, String)> {
        let blocks = self.blocks.lock().unwrap();
        blocks
            .iter().max_by(|a, b| a.height.cmp(&b.height))
            .map(|b| (b.height, b.hash.clone()))
            .ok_or_else(|| anyhow!("No blocks found"))
    }

    fn next_finalized_blocks(&self) -> anyhow::Result<Box<dyn NextBlock>> {
        Err(anyhow!("Not implemented"))
    }
}
