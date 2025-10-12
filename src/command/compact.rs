use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::str::FromStr;
use anyhow::{anyhow, Result};
use apache_avro::types::{Record, Value};
use async_trait::async_trait;
use shutdown::Shutdown;
use tokio::task::JoinSet;
use crate::{
    archiver::{
        Archiver,
        blocks_config::Blocks,
        datakind::{DataKind, DataOptions},
        range::Range,
        range_bag::RangeBag,
        range_group::{
            ArchiveGroup,
            ArchivesList
        }
    },
    args::Args,
    avros,
    blockchain::{BlockDetails, BlockchainTypes},
    command::{
        CommandExecutor
    },
    global,
    storage::{FileReference, TargetFileReader, TargetFileWriter, TargetStorage},
};

#[derive(Clone)]
pub struct CompactCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    archiver: Archiver<B, TS>,
    
    blocks: Blocks,
    chunk_size: usize,
    tx_options: DataOptions,
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage + 'static> CommandExecutor for CompactCommand<B, TS> {

    async fn execute(&self) -> Result<()> {
        let shutdown = global::get_shutdown();
        let range = self.blocks.to_range(self.archiver.data_provider.as_ref()).await?;
        tracing::info!(range = display(&range), "Compacting all in range");
        let chunks = range.split_chunks(self.chunk_size, self.blocks.is_tail());

        let mut files = self.archiver.target.list(range.clone())?;

        let mut current: Option<Range> = None;
        let mut current_files = vec![];
        while !shutdown.is_signalled() {
            tokio::select! {
                _ = shutdown.signalled() => break,
                f = files.recv() => {
                    match f {
                        Some(file) => {
                            let relevant_chunk = chunks.iter().find(|c| c.is_intersected_with(&file.range));
                            tracing::trace!("File {:?} is in chunk {:?}", file, relevant_chunk);
                            if relevant_chunk.is_none() {
                                continue;
                            }
                            let relevant_chunk = relevant_chunk.unwrap();

                            if current.is_none() {
                                current = Some(relevant_chunk.clone());
                            }
                            let current_chunk = current.as_ref().unwrap();

                            if !current_chunk.eq(relevant_chunk) {
                                // -----
                                // Process the current chunk as we got the new files
                                // -----

                                let compact = self.compact_range(shutdown.clone(), current_chunk, current_files.clone())
                                    .await.map_err(|e| anyhow!("Error compacting range {:?}: {}", current_chunk, e))
                                    .unwrap_or(false);

                                // keep feels that are not fully copied, for the next chunk
                                // i.e., if a file covers multiple chunks
                                let mut next_files = vec![];
                                let mut deleting = JoinSet::new();
                                for c in current_files.into_iter() {
                                    let is_fully_read = current_chunk.contains(&c.range) || c.range.end() < current_chunk.end();
                                    if !is_fully_read {
                                        next_files.push(c);
                                    } else if compact { // delete only if the file is fully copied
                                        let c = c.clone();
                                        let target = self.archiver.target.clone();
                                        deleting.spawn(async move {
                                            let _ = target.delete(&c).await;
                                        });
                                    }
                                }
                                let _ = deleting.join_all().await;
                                current_files = next_files;
                            }

                            // If it was just processed, it will be overwritten with the new.
                            // If it was not processed, then it didn't change anyway
                            current = Some(relevant_chunk.clone());
                            current_files.push(file);
                        },
                        None => break,
                    }
                }
            }
        }

        Ok(())
    }
}

impl<B: BlockchainTypes, TS: TargetStorage> CompactCommand<B, TS> {
    pub fn new(config: &Args,
               archiver: Archiver<B, TS>
    ) -> Result<Self> {
        Ok(Self {
            b: PhantomData,
            archiver,
            blocks: Blocks::try_from(config)?,
            chunk_size: config.range_chunk.unwrap_or(1000),
            tx_options: DataOptions::from(config),
        })
    }

    async fn compact_range(&self, shutdown: Shutdown, range: &Range, files: Vec<FileReference>) -> Result<bool> {
        tracing::info!(range = display(range), "Compacting range chunk");
        let complete = match Self::verify_files(range, files, &self.tx_options) {
            Err(e) => {
                tracing::warn!(range = display(range), "{}", e);
                return Ok(false);
            },
            Ok(c) => c,
        };

        let block_file = self.archiver.target.create(DataKind::Blocks, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let tx_file = self.archiver.target.create(DataKind::Transactions, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;

        let mut status = CopiedStatus::new();

        for group in complete.all() {
            self.compact_group(shutdown.clone(), range, &block_file, &tx_file, group, &mut status).await?;
        }

        let all_copied = status.validate(range);

        if all_copied {
            block_file.close().await?;
            tx_file.close().await?;
            tracing::debug!(range = display(range), "Range compacted");
        }

        Ok(all_copied)
    }

    async fn compact_group(&self, shutdown: Shutdown, range: &Range,
                           block_file: &TS::Writer, tx_file: &TS::Writer,
                           group: ArchiveGroup,
                           status: &mut CopiedStatus<B::TxId>) -> Result<()> {
        let mut source_blocks = self.archiver.target
            .open(group.blocks.as_ref().unwrap())
            .await?
            .read()?;
        let mut source_txes = self.archiver.target
            .open(group.txes.as_ref().unwrap())
            .await?
            .read()?;
        let mut blocks_read = false;
        let mut txes_read = false;
        while !shutdown.is_signalled() && (!blocks_read || !txes_read) {
            tokio::select! {
                    _ = shutdown.signalled() => break,
                    block = source_blocks.recv() => {
                        match block {
                            Some(record) => {
                                let height = avros::get_height(&record)?;
                                if range.contains(&height.into()) && !status.contains_height(height) {
                                    let txes = self.parse_block(&record)?.txes();
                                    let _ = block_file.append(record).await;
                                    status.on_copied_block(height, txes);
                                }
                            },
                            None => blocks_read = true,
                        }
                    }
                    tx = source_txes.recv() => {
                        match tx {
                            Some(record) => {
                                let height = avros::get_height(&record)?;
                                let txid = self.parse_tx_id(&record)?;
                                if range.contains(&height.into()) && !status.contains_tx(&txid)  {
                                    let _ = tx_file.append(record).await;
                                    status.on_copied_tx(txid);
                                }
                            },
                            None => txes_read = true,
                        }
                    }
                }
        }
        Ok(())
    }

    fn verify_files(range: &Range, files: Vec<FileReference>, tx_options: &DataOptions) -> Result<ArchivesList> {
        //
        // fist make sure it has complete data for every height in group
        //
        let mut complete = ArchivesList::new(tx_options.files());
        for file in files {
            complete.append(file)?;
        }
        let mut bag = RangeBag::new();
        for group in complete.iter() {
            if group.is_complete() {
                bag.append(group.range.clone());
            }
        }
        let bag = bag.compact();

        // must have all the data in the same range as requested
        if bag.len() != 1 || bag.as_single_range().unwrap() != range {
            return Err(anyhow!("Range is missing data. Please run Fix before Compaction. Skipping this range."));
        }
        Ok(complete)
    }

    fn parse_block(&self, record: &Record) -> Result<B::BlockParsed> {
        let json = record.fields
            .iter().find(|f| f.0 == "json")
            .ok_or(anyhow!("No json field in the record"))?;
        let json = match &json.1 {
            Value::Bytes(b) => b.as_slice(),
            Value::String(s) => s.as_bytes(),
            _ => return Err(anyhow!("Expected a bytes, got {:?}", json.1)),
        };
        serde_json::from_slice(json)
            .map_err(|e| anyhow!("Unable to parse block: {}", e))
    }

    fn parse_tx_id(&self, record: &Record) -> Result<B::TxId> {
        let txid = record.fields
            .iter().find(|f| f.0 == "txid")
            .ok_or(anyhow!("No json field in the record"))?;
        let txid = match &txid.1 {
            Value::String(s) => s.clone(),
            _ => return Err(anyhow!("Expected a string, got {:?}", txid.1)),
        };
        B::TxId::from_str(txid.as_str())
            .map_err(|_| anyhow!("Unable to parse txid"))
    }
}

struct CopiedStatus<TXID: Hash + Eq> {
    copied_blocks: HashSet<u64>,
    expected_txes: HashSet<TXID>,
    copied_txes: HashSet<TXID>,
}

impl<TXID: Hash + Eq + Debug> CopiedStatus<TXID> {
    fn new() -> Self {
        Self {
            copied_blocks: HashSet::new(),
            expected_txes: HashSet::new(),
            copied_txes: HashSet::new(),
        }
    }

    fn contains_height(&self, height: u64) -> bool {
        self.copied_blocks.contains(&height)
    }

    fn contains_tx(&self, tx: &TXID) -> bool {
        self.copied_txes.contains(tx)
    }

    fn on_copied_block(&mut self, height: u64, expected_txes: Vec<TXID>) {
        self.copied_blocks.insert(height);
        expected_txes.into_iter().for_each(|tx| {
            self.expected_txes.insert(tx);
        });
    }

    fn on_copied_tx(&mut self, tx: TXID) {
        self.copied_txes.insert(tx);
    }

    fn validate(self, range: &Range) -> bool {
        let copied_blocks = RangeBag::from(self.copied_blocks.into_iter().collect::<Vec<u64>>()).compact();
        if copied_blocks.len() != 1 || copied_blocks.as_single_range().unwrap() != range {
            tracing::warn!(range = display(range), "Missing blocks. Please run Fix before Compaction. Skipping this range.");
            return false;
        }

        if self.expected_txes != self.copied_txes {
            tracing::trace!("Copied transactions: {:?}", self.copied_txes);
            tracing::trace!("Missing transactions: {:?}", self.expected_txes.difference(&self.copied_txes).collect::<Vec<_>>());
            tracing::trace!("Extra transactions: {:?}", self.copied_txes.difference(&self.expected_txes).collect::<Vec<_>>());


            tracing::warn!(range = display(range), "Missing transactions. Please run Fix before Compaction. Skipping this range.");
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::blockchain::mock::*;
    use crate::archiver::Archiver;
    use crate::command::CommandExecutor;
    use crate::command::compact::CompactCommand;
    use crate::storage::objects::ObjectsStorage;
    use crate::archiver::filenames::Filenames;
    use object_store::memory::InMemory;
    use crate::args::Args;
    use crate::testing;

    #[tokio::test]
    async fn test_compact_command() {
        testing::start_test();

        let mem = Arc::new(InMemory::new());
        let data = MockData::new("TEST_DATA");
        let data_provider: Arc<MockData> = Arc::new(data);
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/test".to_string()));
        let archiver = Archiver::new_simple(Arc::new(storage), data_provider.clone());


        for i in 0..25 {
            let block_height = 100 + i as u64;
            let txs = vec![
                format!("TX{}-1", block_height),
                format!("TX{}-2", block_height),
            ];
            data_provider.add_block(MockBlock {
                height: block_height,
                hash: format!("B{}", block_height),
                transactions: txs.clone(),
            });
            for tx in &txs {
                data_provider.add_tx(MockTx { hash: tx.clone() });
            }

            testing::write_block_and_tx(&archiver, block_height, None).await.unwrap();
        }

        let args = Args {
            range: Some("100..125".to_string()),
            range_chunk: Some(10),
            ..Default::default()
        };
        let compact_cmd = CompactCommand::new(&args, archiver).unwrap();

        let _ = compact_cmd.execute().await.unwrap();

        let result = testing::list_mem_filenames(mem).await;

        assert_eq!(
            result,
            vec![
                "archive/test/000000000/000000000/000000120.block.avro",
                "archive/test/000000000/000000000/000000120.txes.avro",
                "archive/test/000000000/000000000/000000121.block.avro",
                "archive/test/000000000/000000000/000000121.txes.avro",
                "archive/test/000000000/000000000/000000122.block.avro",
                "archive/test/000000000/000000000/000000122.txes.avro",
                "archive/test/000000000/000000000/000000123.block.avro",
                "archive/test/000000000/000000000/000000123.txes.avro",
                "archive/test/000000000/000000000/000000124.block.avro",
                "archive/test/000000000/000000000/000000124.txes.avro",

                "archive/test/000000000/range-000000100_000000109.blocks.avro",
                "archive/test/000000000/range-000000100_000000109.txes.avro",
                "archive/test/000000000/range-000000110_000000119.blocks.avro",
                "archive/test/000000000/range-000000110_000000119.txes.avro",
            ]
        );
    }
}
