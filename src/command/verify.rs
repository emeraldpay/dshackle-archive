use std::collections::HashSet;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use apache_avro::types::{Record, Value};
use async_trait::async_trait;
use shutdown::Shutdown;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use crate::blockchain::{BlockDetails, BlockchainTypes};
use crate::command::archiver::Archiver;
use crate::command::{ArchiveGroup, ArchivesList, Blocks, CommandExecutor};
use crate::global;
use crate::range::Range;
use crate::storage::TargetStorage;
use crate::storage::TargetFileReader;

///
/// Provides `verify` command.
///
/// Checks the range of block in the current archive and deleted files with missing or incorrect data (which is supposed to be reloaded back with `fix` command)
///
#[derive(Clone)]
pub struct VerifyCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    blocks: Blocks,
    archiver: Arc<Archiver<B, TS>>,
}

impl<B: BlockchainTypes + 'static, TS: TargetStorage + 'static> VerifyCommand<B, TS> {
    pub fn new(config: &crate::args::Args,
               archiver: Archiver<B, TS>,
    ) -> anyhow::Result<Self> {

        let blocks = if let Some(tail) = config.tail {
            Blocks::Tail(tail)
        } else if let Some(range) = &config.range {
            Blocks::Range(Range::from_str(range)?)
        } else {
            return Err(anyhow!("Either `tail` or `range` should be specified"));
        };

        Ok(Self {
            b: PhantomData,
            blocks,
            archiver: Arc::new(archiver),
        })
    }

}

#[async_trait]
impl<B: BlockchainTypes + 'static, FR: TargetStorage + 'static> CommandExecutor for VerifyCommand<B, FR> {

    async fn execute(&self) -> anyhow::Result<()> {
        let range = self.archiver.get_range(&self.blocks).await?;
        tracing::info!(range = %range, "Verifying range");

        let mut existing = self.archiver.target.list(range)?;
        let mut archived = ArchivesList::new();
        let shutdown = global::get_shutdown();

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

        verify_list(self.archiver.clone(), archived, shutdown).await?;

        Ok(())
    }
}

async fn verify_list<B: BlockchainTypes + 'static, TS: TargetStorage + 'static>(archiver: Arc<Archiver<B, TS>>, archived: ArchivesList, shutdown: Shutdown) -> Result<()> {
    let mut jobs = JoinSet::new();
    let parallel = Arc::new(Semaphore::new(4));
    for group in archived.all() {
        let parallel = parallel.clone();
        let archiver = archiver.clone();
        let shutdown = shutdown.clone();
        jobs.spawn(async move {
            let _permit = parallel.acquire().await;
            verify_group(archiver, group, shutdown).await
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

async fn verify_group<B: BlockchainTypes, TS: TargetStorage>(archiver: Arc<Archiver<B, TS>>, group: ArchiveGroup, shutdown: Shutdown) -> Result<()> {
    let delete = if !group.is_complete() {
        tracing::info!(range = %group.range, "Incomplete group");
        true
    } else {
        if let Err(e) = verify_content(archiver.clone(), &group, shutdown.clone()).await {
            tracing::info!(range = %group.range, "Invalid data in group: {}", e);
            true
        } else if shutdown.is_signalled() {
            return Ok(());
        } else {
            tracing::info!(range = %group.range, "Confirmed group");
            false
        }
    };
    if shutdown.is_signalled() {
        // when it's shutting down there (1) is not time for deletion and (2) most likely it got some invalid data as other threads are stopping
        return Ok(());
    }
    if delete {
        tracing::info!(range = %group.range, "Deleting group");
        for f in group.files() {
            archiver.target.delete(f).await?;
        }
    }
    Ok(())
}

async fn verify_content<B: BlockchainTypes, TS: TargetStorage>(archiver: Arc<Archiver<B, TS>>, group: &ArchiveGroup, shutdown: Shutdown) -> Result<()> {
    tracing::trace!(range = %group.range, "Verify group");

    tracing::trace!(range = %group.range, "Verify blocks");
    let mut blocks = archiver.target
        .open(group.blocks.as_ref().unwrap())
        .await?
        .read()?;

    let mut heights = HashSet::new();
    let mut expected_txes = Vec::new();
    while !shutdown.is_signalled() {
        tokio::select! {
            _ = shutdown.signalled() => {
                tracing::info!("Shutting down...");
                return Ok(());
            },

            record = blocks.recv() => {
                if record.is_none() {
                    break
                }
                let record = record.unwrap();

                let height = record.fields.iter()
                    .find(|f| f.0 == "height")
                    .ok_or(anyhow!("No height in the record"))?;
                let height = match &height.1 {
                    Value::Long(h) => h.clone() as u64,
                    _ => return Err(anyhow!("Invalid height type: {:?}", height.1))
                };
                if !group.range.contains(height) {
                    return Err(anyhow!("Height is not in range: {}", height));
                }
                let first = heights.insert(height);
                if !first {
                    return Err(anyhow!("Duplicate height: {}", height));
                }

                let json = record.fields.iter()
                    .find(|f| f.0 == "json")
                    .ok_or(anyhow!("No block json in the record"))?;
                let json = match &json.1 {
                    Value::Bytes(b) => b.clone(),
                    _ => return Err(anyhow!("Invalid json type: {:?}", json.1))
                };
                let block = serde_json::from_slice::<B::BlockParsed>(json.as_slice())?;
                expected_txes.extend(block.txes());
            },
        }
    }

    if shutdown.is_signalled() {
        return Ok(());
    }
    if heights.len() != group.range.len() {
        return Err(anyhow!("Missing block"));
    }

    tracing::trace!(range = %group.range, "Verify txes");
    let mut txes = archiver.target.open(group.txes.as_ref().unwrap())
        .await?
        .read()?;
    let mut existing_txes = HashSet::new();
    while !shutdown.is_signalled() {
        tokio::select! {
            _ = shutdown.signalled() => {
                tracing::info!("Shutting down...");
                return Ok(());
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

    if shutdown.is_signalled() {
        return Ok(());
    }
    if existing_txes.len() != expected_txes.len() {
        return Err(anyhow!("Missing tx"));
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use object_store::memory::InMemory;
    use object_store::{ObjectMeta, ObjectStore};
    use crate::args::Args;
    use crate::blockchain::mock::{MockBlock, MockData, MockTx, MockType};
    use crate::command::archiver::Archiver;
    use crate::command::CommandExecutor;
    use crate::command::verify::VerifyCommand;
    use crate::filenames::Filenames;
    use crate::storage::objects::ObjectsStorage;
    use futures_util::StreamExt;
    use crate::blockchain::{BlockReference, BlockchainData};
    use crate::datakind::DataKind;
    use crate::range::Range;
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
        let data = MockData::new("TEST");

        let block100 = MockBlock {
            height: 100,
            hash: "B100".to_string(),
            transactions: vec!["TX001".to_string(), "TX002".to_string()],
        };
        data.add_block(block100.clone());
        data.add_tx(MockTx {
            hash: "TX001".to_string(),
        });

        let blocks = archiver.target
            .create(DataKind::Blocks, &Range::Single(100))
            .await.expect("Create block");
        let record = data.fetch_block(&BlockReference::Height(100)).await.unwrap();
        blocks.append(record.0).await.unwrap();
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
