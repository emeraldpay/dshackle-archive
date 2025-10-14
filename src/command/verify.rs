use std::collections::HashSet;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use apache_avro::types::{Record, Value};
use async_trait::async_trait;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use crate::{
    archiver::{
        datakind::DataOptions,
        blocks_config::Blocks,
        Archiver,
        range::Range,
        range_group::{ArchiveGroup, ArchivesList}
    },
    avros,
    blockchain::{BlockDetails, BlockchainTypes},
    command::{CommandExecutor},
    global,
    storage::{
        TargetFileReader,
        TargetStorage
    },
};
use crate::archiver::datakind::{BlockOptions, TraceOptions, TxOptions};
use crate::storage::FileReference;

///
/// Provides `verify` command.
///
/// Checks the range of block in the current archive and deletes the files with missing or incorrect data (which is supposed to be reloaded back with `fix` command)
///
#[derive(Clone)]
pub struct VerifyCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    blocks: Blocks,
    archiver: Arc<Archiver<B, TS>>,
    data_options: DataOptions,
    delete_chunk: bool,
    chunk: usize,
}

impl<B: BlockchainTypes + 'static, TS: TargetStorage + 'static> VerifyCommand<B, TS> {
    pub fn new(config: &crate::args::Args,
               archiver: Archiver<B, TS>,
    ) -> Result<Self> {

        Ok(Self {
            b: PhantomData,
            blocks: Blocks::try_from(config)?,
            archiver: Arc::new(archiver),
            data_options: DataOptions::from(config),
            delete_chunk: config.fix_clean,
            chunk: config.get_chunk_size(),
        })
    }

    async fn verify_chunk(&self, archived: ArchivesList) -> Result<()> {
        let shutdown = global::get_shutdown();
        let delete_chunk = self.delete_chunk;
        let archiver = self.archiver.clone();
        let data_options = self.data_options.clone();
        let mut jobs = JoinSet::new();
        let parallel = Arc::new(Semaphore::new(4));
        for group in archived.all() {
            let parallel = parallel.clone();
            let archiver = archiver.clone();
            let data_options = data_options.clone();
            jobs.spawn(async move {
                let _permit = parallel.acquire().await;
                verify_table_group(delete_chunk, archiver, group, data_options).await
            });
        }
        while !shutdown.is_signalled() {
            tokio::select! {
            _ = shutdown.signalled() => {
                tracing::info!("Shutting down...");
                jobs.shutdown().await
            },
            next = jobs.join_next() => {
                if next.is_none() {
                    break;
                }
            }
        }
        }
        Ok(())
    }

}

#[async_trait]
impl<B: BlockchainTypes + 'static, FR: TargetStorage + 'static> CommandExecutor for VerifyCommand<B, FR> {

    async fn execute(&self) -> Result<()> {
        let full_range = self.blocks.to_range(self.archiver.data_provider.as_ref()).await?;
        tracing::info!(range = %full_range, "Verifying range");

        let ranges = full_range.split_chunks(self.chunk, false);
        let shutdown = global::get_shutdown();

        for range in ranges {
            tracing::info!(range = %range, "Verifying chunk");
            let mut existing = self.archiver.target.list(range)?;
            let mut archived = ArchivesList::new(self.data_options.files());

            while !shutdown.is_signalled() {
                tokio::select! {
                    _ = shutdown.signalled() => return Ok(()),
                    file = existing.recv() => {
                        if file.is_none() {
                            break;
                        }
                        let file = file.unwrap();
                        tracing::trace!("Received file: {:?}", file.path);
                        //TODO start processing once it's known the range is complete / incomplete
                        let _is_completed = archived.append(file);
                    }
                }
            }

            self.verify_chunk(archived).await?;
        }

        Ok(())
    }
}

async fn verify_table_group<B: BlockchainTypes, TS: TargetStorage>(delete_chunk: bool, archiver: Arc<Archiver<B, TS>>, group: ArchiveGroup, data_options: DataOptions) -> Result<()> {
    let shutdown = global::get_shutdown();
    let incomplete = !group.is_complete();
    let mut for_deletion: Vec<&FileReference> = vec![];

    if incomplete && delete_chunk {
        tracing::info!(range = %group.range, "Incomplete chunk, deleting all tables");
        for_deletion.extend(group.tables())
    } else {
        if let Ok(files) = verify_content(archiver.clone(), &group, data_options).await {
            for_deletion.extend(files);
        }
    }

    if shutdown.is_signalled() {
        // when it's shutting down there (1) is not time for deletion and (2) most likely it got some invalid data as other threads are stopping
        return Ok(());
    }

    if !for_deletion.is_empty() && delete_chunk {
        tracing::info!(range = %group.range, "Deleting all tables in the chunk due to --fix.clean");
        for_deletion = group.tables();
    }

    for f in for_deletion {
        archiver.target.delete(&f).await?;
    }
    Ok(())
}

async fn verify_content<B: BlockchainTypes, TS: TargetStorage>(archiver: Arc<Archiver<B, TS>>, group: &ArchiveGroup, data_options: DataOptions) -> Result<Vec<&FileReference>> {
    tracing::trace!(range = %group.range, "Verify table data");
    let shutdown = global::get_shutdown();
    let mut broken_files = vec![];

    let block_file = group.blocks.as_ref().unwrap();
    let block_verification = VerifyTable::<BlockOptions, B, TS>::verify_table(block_file, archiver.target.as_ref(), data_options.block.as_ref().unwrap(), &group.range, &()).await;
    if shutdown.is_signalled() {
        return Ok(vec![]);
    }
    match block_verification {
        Err(e) => {
            tracing::error!(range = %group.range, "Block data is corrupted: {:?}", e);
            if data_options.include_block() {
                broken_files.push(group.blocks.as_ref().unwrap());
                if data_options.include_tx() || data_options.include_trace() {
                    // we cannot verify txes if blocks are corrupted
                    tracing::warn!(range = %group.range, "Cannot verify txes without a valid block");
                }
            }
        },
        Ok(expected_txes) => {
            if let Some(tx_options) = &data_options.tx {
                if let Some(txes) = group.txes.as_ref() {
                    let ok = VerifyTable::<TxOptions, B, TS>::verify_table(txes, archiver.target.as_ref(), tx_options, &group.range, &expected_txes).await;
                    if ok.is_err() {
                        tracing::error!(range = %group.range, "Tx data is corrupted: {:?}", ok.err().unwrap());
                        broken_files.push(txes);
                    }
                }
            }
            if let Some(trace_options) = &data_options.trace {
                if let Some(traces) = group.traces.as_ref() {
                    let ok = VerifyTable::<TraceOptions, B, TS>::verify_table(traces, archiver.target.as_ref(), trace_options, &group.range, &expected_txes).await;
                    if ok.is_err() {
                        tracing::error!(range = %group.range, "Trace data is corrupted: {:?}", ok.err().unwrap());
                        broken_files.push(traces);
                    }
                }
            }
        }
    }

    Ok(broken_files)
}

fn verify_field_exist(record: &Record, field: &str) -> Result<()> {
    let value = record.fields.iter()
        .find(|f| f.0 == field)
        .ok_or(anyhow!("No {} in the record", field))?;
    match &value.1 {
        Value::Null => Err(anyhow!("Null {} in the record", field)),
        Value::Bytes(v) => if v.is_empty() {
            Err(anyhow!("Empty {} in the record", field))
        } else {
            Ok(())
        },
        Value::String(s) => if s.is_empty() {
            Err(anyhow!("Empty {} in the record", field))
        } else {
            Ok(())
        },
        _ => Ok(())
    }
}

#[async_trait]
trait VerifyTable<T, B: BlockchainTypes, TS: TargetStorage> {
    type Returns;
    type Params;

    async fn verify_table(&self, storage: &TS, table: &T, range: &Range, params: &Self::Params) -> Result<Self::Returns>;
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> VerifyTable<TxOptions, B, TS> for FileReference {
    type Returns = ();
    type Params = Vec<B::TxId>;

    async fn verify_table(&self, storage: &TS, _table: &TxOptions, range: &Range, params: &Self::Params) -> Result<Self::Returns> {
        tracing::trace!(range = %range, "Verify txes");
        let shutdown = global::get_shutdown();
        let expected_txes = params;
        let mut txes = storage.open(&self)
            .await?
            .read()?;
        let mut existing_txes = HashSet::new();
        while !shutdown.is_signalled() {
            tokio::select! {
                _ = shutdown.signalled() => {
                    tracing::info!("Shutting down...");
                    break;
                },
                record = txes.recv() => {
                    if record.is_none() {
                        break
                    }
                    let record = record.unwrap();
                    let txid = record.fields.iter()
                        .find(|f| f.0 == "txid")
                        .ok_or(anyhow!("No txid in the record"))?;
                    let txid_str = match &txid.1 {
                        Value::String(s) => s.clone(),
                        _ => return Err(anyhow!("Invalid txid type: {:?}", txid.1))
                    };

                    let txid = B::TxId::from_str(&txid_str)
                        .map_err(|_| anyhow!("Invalid txid: {}", txid_str))?;
                    if !expected_txes.contains(&txid) {
                        return Err(anyhow!("Unexpected txid: {}", txid_str));
                    }

                    verify_field_exist(&record, "json")?;
                    verify_field_exist(&record, "raw")?;
                    //TODO blockchain specific verification

                    let first = existing_txes.insert(txid);
                    if !first {
                        return Err(anyhow!("Duplicate txid: {}", txid_str));
                    }
                }
            }
        }
        if existing_txes.len() != expected_txes.len() {
            return Err(anyhow!("Missing txes in the table"));
        }
        Ok(())
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> VerifyTable<TraceOptions, B, TS> for FileReference {
    type Returns = ();
    type Params = Vec<B::TxId>;

    async fn verify_table(&self, storage: &TS, table: &TraceOptions, range: &Range, params: &Self::Params) -> Result<Self::Returns> {
        tracing::trace!(range = %range, "Verify traces");
        let shutdown = global::get_shutdown();
        let expected_txes = params;
        let mut traces = storage.open(&self)
            .await?
            .read()?;
        let mut existing_txes = HashSet::new();
        while !shutdown.is_signalled() {
            tokio::select! {
                _ = shutdown.signalled() => {
                    tracing::info!("Shutting down...");
                    break;
                },
                record = traces.recv() => {
                    if record.is_none() {
                        break
                    }
                    let record = record.unwrap();
                    let txid = record.fields.iter()
                        .find(|f| f.0 == "txid")
                        .ok_or(anyhow!("No txid in the record"))?;
                    let txid_str = match &txid.1 {
                        Value::String(s) => s.clone(),
                        _ => return Err(anyhow!("Invalid txid type: {:?}", txid.1))
                    };

                    let txid = B::TxId::from_str(&txid_str)
                        .map_err(|_| anyhow!("Invalid txid: {}", txid_str))?;
                    if !expected_txes.contains(&txid) {
                        return Err(anyhow!("Unexpected txid: {}", txid_str));
                    }

                    if table.include_trace {
                        verify_field_exist(&record, "traceJson")?;
                    }
                    if table.include_state_diff {
                        verify_field_exist(&record, "stateDiffJson")?;
                    }

                    let first = existing_txes.insert(txid);
                    if !first {
                        return Err(anyhow!("Duplicate txid: {}", txid_str));
                    }
                }
            }
        }
        if existing_txes.len() != expected_txes.len() {
            return Err(anyhow!("Missing txes in the table"));
        }
        Ok(())
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> VerifyTable<BlockOptions, B, TS> for FileReference {
    type Returns = Vec<B::TxId>;
    type Params = ();

    async fn verify_table(&self, storage: &TS, _table: &BlockOptions, range: &Range, _params: &Self::Params) -> Result<Self::Returns> {
        tracing::trace!(range = %range, "Verify blocks");
        let mut blocks = storage.open(&self)
            .await?
            .read()?;
        let shutdown = global::get_shutdown();

        let mut heights = HashSet::new();
        let mut expected_txes = Vec::new();
        while !shutdown.is_signalled() {
            tokio::select! {
            _ = shutdown.signalled() => {
                tracing::info!("Shutting down...");
                break;
            },

            record = blocks.recv() => {
                if record.is_none() {
                    break
                }
                let record = record.unwrap();

                let height = avros::get_height(&record)?;
                if !range.contains(&Range::Single(height)) {
                    tracing::error!(range = %range, "Height is not in range: {}", height);
                    return Err(anyhow!("Height is not in range: {}", height));
                }
                let first = heights.insert(height);
                if !first {
                    tracing::error!(range = %range, "Duplicate height: {}", height);
                    return Err(anyhow!("Duplicate height: {}", height));
                }

                let json = record.fields.iter()
                    .find(|f| f.0 == "json");
                if json.is_none() {
                    tracing::error!(range = %range, "No json in the record");
                    return Err(anyhow!("No json in the record"));
                }
                let json = json.unwrap();
                let json = match &json.1 {
                    Value::Bytes(b) => b.clone(),
                    _ => {
                        tracing::error!(range = %range, "Invalid json type: {:?}", json.1);
                        return Err(anyhow!("Invalid json type: {:?}", json.1));
                    }
                };
                let block = serde_json::from_slice::<B::BlockParsed>(json.as_slice());
                if block.is_err() {
                    tracing::error!(range = %range, "Invalid json data: {:?}", block.err().unwrap());
                    return Err(anyhow!("Invalid json data"));
                }
                let block = block.unwrap();
                expected_txes.extend(block.txes());
            },
        }
        }
        if heights.len() != range.len() {
            tracing::error!(range = %range, "Missing blocks in the table");
            return Err(anyhow!("Missing blocks in the table"));
        }
        Ok(expected_txes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use object_store::memory::InMemory;
    use object_store::{ObjectMeta, ObjectStore};
    use crate::args::Args;
    use crate::blockchain::mock::{MockBlock, MockData, MockTx, MockType};
    use crate::archiver::Archiver;
    use crate::command::CommandExecutor;
    use crate::command::verify::VerifyCommand;
    use crate::archiver::filenames::Filenames;
    use crate::storage::objects::ObjectsStorage;
    use futures_util::StreamExt;
    use crate::blockchain::{BlockReference, BlockchainData};
    use crate::archiver::datakind::DataKind;
    use crate::archiver::range::Range;
    use crate::storage::{TargetFileWriter, TargetStorage};
    use crate::testing;

    fn create_archiver(mem: Arc<InMemory>) -> Archiver<MockType, ObjectsStorage<InMemory>> {
        let storage = ObjectsStorage::new(mem, "test".to_string(), Filenames::with_dir("archive/eth".to_string()));

        Archiver::new_simple(
            Arc::new(storage),
            Arc::new(MockData::new("test")),
        )
    }

    #[tokio::test]
    async fn does_nothing_on_empty_archive() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let archiver = create_archiver(mem.clone());

        let args = Args {
            range: Some("100..110".to_string()),
            ..Default::default()
        };

        let command = VerifyCommand::new(
            &args,
            archiver,
        ).unwrap();

        let result = command.execute().await;
        if let Err(err) = result {
            panic!("Failed: {:?}", err);
        }
    }

    #[tokio::test]
    async fn does_nothing_with_full_group() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());

        let block101 = MockBlock {
            height: 101,
            hash: "B101".to_string(),
            transactions: vec!["TX001".to_string()],
        };
        let data = MockData::new("TEST");
        data.add_block(block101.clone());
        data.add_tx(MockTx {
            hash: "TX001".to_string(),
        });

        let archiver = create_archiver(mem.clone());

        let write = archiver.target.create(DataKind::Blocks, &Range::Single(101)).await.unwrap();
        let record = data.fetch_block(&BlockReference::Height(101)).await.unwrap();
        write.append(record.0).await.unwrap();
        write.close().await.unwrap();

        let write = archiver.target.create(DataKind::Transactions, &Range::Single(101)).await.unwrap();
        let record = data.fetch_tx(&block101, 0).await.unwrap();
        write.append(record).await.unwrap();
        write.close().await.unwrap();

        let args = Args {
            range: Some("100..110".to_string()),
            ..Default::default()
        };

        let command = VerifyCommand::new(
            &args,
            archiver,
        ).unwrap();

        let result = command.execute().await;
        if let Err(err) = result {
            panic!("Failed: {:?}", err);
        }

        let all: Vec<object_store::Result<ObjectMeta>> = mem.list(None).collect().await;

        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn deletes_incomplete_group() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());

        let block101 = MockBlock {
            height: 101,
            hash: "B101".to_string(),
            transactions: vec!["TX001".to_string()],
        };
        let block102 = MockBlock {
            height: 102,
            hash: "B102".to_string(),
            transactions: vec!["TX002".to_string()],
        };
        let block103 = MockBlock {
            height: 103,
            hash: "B103".to_string(),
            transactions: vec!["TX003".to_string()],
        };
        let data = MockData::new("TEST");
        data.add_block(block101.clone());
        data.add_block(block102.clone());
        data.add_block(block103.clone());
        data.add_tx(MockTx {
            hash: "TX001".to_string(),
        });
        data.add_tx(MockTx {
            hash: "TX002".to_string(),
        });
        data.add_tx(MockTx {
            hash: "TX003".to_string(),
        });

        let archiver = create_archiver(mem.clone());

        // should have:
        // block 101 + txes 101
        // txes 102
        // block 103

        let write = archiver.target.create(DataKind::Blocks, &Range::Single(101)).await.unwrap();
        let record = data.fetch_block(&BlockReference::Height(101)).await.unwrap();
        write.append(record.0).await.unwrap();
        write.close().await.unwrap();

        let write = archiver.target.create(DataKind::Blocks, &Range::Single(103)).await.unwrap();
        let record = data.fetch_block(&BlockReference::Height(103)).await.unwrap();
        write.append(record.0).await.unwrap();
        write.close().await.unwrap();

        let write = archiver.target.create(DataKind::Transactions, &Range::Single(101)).await.unwrap();
        let record = data.fetch_tx(&block101, 0).await.unwrap();
        write.append(record).await.unwrap();
        write.close().await.unwrap();

        let write = archiver.target.create(DataKind::Transactions, &Range::Single(102)).await.unwrap();
        let record = data.fetch_tx(&block102, 0).await.unwrap();
        write.append(record).await.unwrap();
        write.close().await.unwrap();

        let args = Args {
            range: Some("100..110".to_string()),
            range_chunk: Some(100),
            fix_clean: true,
            ..Default::default()
        };

        let command = VerifyCommand::new(
            &args,
            archiver,
        ).unwrap();

        let result = command.execute().await;
        if let Err(err) = result {
            panic!("Failed: {:?}", err);
        }

        let all = testing::list_mem_filenames(mem).await;

        assert_eq!(all.len(), 2);
        assert_eq!(
            all.get(0).unwrap(),
            "archive/eth/000000000/000000000/000000101.block.avro"
        );
        assert_eq!(
            all.get(1).unwrap(),
            "archive/eth/000000000/000000000/000000101.txes.avro"
        );
    }

    #[tokio::test]
    async fn deletes_empty_block() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let archiver = create_archiver(mem.clone());
        let data = MockData::new("TEST");

        let block100 = MockBlock {
            height: 100,
            hash: "B100".to_string(),
            transactions: vec!["TX001".to_string()],
        };
        data.add_block(block100.clone());
        data.add_tx(MockTx {
            hash: "TX001".to_string(),
        });

        let blocks = archiver.target
            .create(DataKind::Blocks, &Range::Single(100))
            .await.expect("Create block");
        // no date written
        blocks.close().await.unwrap();

        let txes = archiver.target
            .create(DataKind::Transactions, &Range::Single(100))
            .await.expect("Create txes");
        let record = data.fetch_tx(&block100, 0).await.unwrap();
        txes.append(record).await.unwrap();
        txes.close().await.unwrap();

        let files = testing::list_mem_filenames(mem.clone()).await;
        assert_eq!(files.len(), 2);

        let args = Args {
            range: Some("100..110".to_string()),
            fix_clean: true,
            ..Default::default()
        };

        let command = VerifyCommand::new(
            &args,
            archiver,
        ).unwrap();

        let result = command.execute().await;
        if let Err(err) = result {
            panic!("Failed: {:?}", err);
        }

        let files = testing::list_mem_filenames(mem).await;
        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    async fn deletes_missing_tx() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let archiver = create_archiver(mem.clone());
        let data = archiver.data_provider.clone();

        let block100 = MockBlock {
            height: 100,
            hash: "B100".to_string(),
            transactions: vec!["TX001".to_string(), "TX002".to_string()],
        };
        data.add_block(block100.clone());
        data.add_tx(MockTx {
            hash: "TX001".to_string(),
        });

        testing::write_block_and_tx(&archiver, 100, Some(vec![0])).await.unwrap();

        let files = testing::list_mem_filenames(mem.clone()).await;
        assert_eq!(files.len(), 2);

        let args = Args {
            range: Some("100..110".to_string()),
            fix_clean: true,
            ..Default::default()
        };

        let command = VerifyCommand::new(
            &args,
            archiver,
        ).unwrap();

        let result = command.execute().await;
        if let Err(err) = result {
            panic!("Failed: {:?}", err);
        }

        let files = testing::list_mem_filenames(mem).await;
        assert_eq!(files.len(), 0);
    }

}
