use std::marker::PhantomData;
use std::str::FromStr;
use anyhow::anyhow;
use async_trait::async_trait;
use shutdown::Shutdown;
use crate::blockchain::BlockchainTypes;
use crate::command::archiver::Archiver;
use crate::command::{ArchivesList, Blocks, CommandExecutor};
use crate::range::Range;

///
/// Provides `verify` command.
///
/// Checks the range of block in the current archive and deleted files with missing or incorrect data (which is supposed to be reloaded back with `fix` command)
///
#[derive(Clone)]
pub struct VerifyCommand<B: BlockchainTypes> {
    b: PhantomData<B>,
    shutdown: Shutdown,
    blocks: Blocks,
    archiver: Archiver<B>,
}

impl<B: BlockchainTypes> VerifyCommand<B> {
    pub fn new(config: &crate::args::Args,
               shutdown: Shutdown,
               archiver: Archiver<B>,
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
            shutdown,
            blocks,
            archiver
        })
    }
}

#[async_trait]
impl<B: BlockchainTypes> CommandExecutor for VerifyCommand<B> {

    async fn execute(&self) -> anyhow::Result<()> {
        let range = self.archiver.get_range(&self.blocks).await?;
        tracing::info!("Verifying range: {}", range);

        let mut existing = self.archiver.target.list(range)?;
        let mut archived = ArchivesList::new();
        let shutdown = self.shutdown.clone();

        while !shutdown.is_signalled() {
            tokio::select! {
                _ = shutdown.signalled() => return Ok(()),
                file = existing.recv() => {
                    if file.is_none() {
                        break;
                    }
                    let file = file.unwrap();
                    tracing::trace!("Received file: {:?}", file.path);
                    //TODO start processing once it's known the range is not complete
                    let _is_completed = archived.append(file);
                }
            }
        }

        for group in archived.iter() {
            if !group.is_complete() {
                tracing::info!("Incomplete group: {:?}", group);
                for f in group.files() {
                    //TODO delete in parallel
                    tracing::debug!("Deleting file: {:?}", f.path);
                    let deleted = self.archiver.target.delete(f).await;
                    if let Err(err) = deleted {
                        tracing::warn!("Failed to delete file: {:?}", err);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::{ObjectMeta, ObjectStore};
    use object_store::path::Path;
    use shutdown::Shutdown;
    use crate::args::Args;
    use crate::blockchain::mock::{MockData, MockType};
    use crate::command::archiver::Archiver;
    use crate::command::CommandExecutor;
    use crate::command::verify::VerifyCommand;
    use crate::filenames::Filenames;
    use crate::storage::objects::ObjectsStorage;
    use futures_util::StreamExt;
    use crate::testing;

    #[tokio::test]
    async fn does_nothing_on_empty_archive() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let storage = ObjectsStorage::new(mem, "test".to_string(), Filenames::with_dir("archive/eth".to_string()));

        let archiver: Archiver<MockType> = Archiver::new_simple(
            Arc::new(Box::new(storage)),
            Arc::new(MockData::new("test")),
        );

        let args = Args {
            range: Some("100..110".to_string()),
            ..Default::default()
        };

        let command = VerifyCommand::new(
            &args,
            Shutdown::new().unwrap(),
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

        mem.put(
            &Path::from("archive/eth/000000000/000000000/000000101.block.avro"),
            Bytes::from_static(b"test").into(),
        ).await.expect("Put 1");
        mem.put(
            &Path::from("archive/eth/000000000/000000000/000000101.txes.avro"),
            Bytes::from_static(b"test").into(),
        ).await.expect("Put 2");

        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/eth".to_string()));

        let archiver: Archiver<MockType> = Archiver::new_simple(
            Arc::new(Box::new(storage)),
            Arc::new(MockData::new("test")),
        );

        let args = Args {
            range: Some("100..110".to_string()),
            ..Default::default()
        };

        let command = VerifyCommand::new(
            &args,
            Shutdown::new().unwrap(),
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

        mem.put(
            &Path::from("archive/eth/000000000/000000000/000000101.block.avro"),
            Bytes::from_static(b"test").into(),
        ).await.expect("Put 1");
        mem.put(
            &Path::from("archive/eth/000000000/000000000/000000101.txes.avro"),
            Bytes::from_static(b"test").into(),
        ).await.expect("Put 2");
        mem.put(
            &Path::from("archive/eth/000000000/000000000/000000102.txes.avro"),
            Bytes::from_static(b"test").into(),
        ).await.expect("Put 3");
        mem.put(
            &Path::from("archive/eth/000000000/000000000/000000103.block.avro"),
            Bytes::from_static(b"test").into(),
        ).await.expect("Put 4");

        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/eth".to_string()));

        let archiver: Archiver<MockType> = Archiver::new_simple(
            Arc::new(Box::new(storage)),
            Arc::new(MockData::new("test")),
        );

        let args = Args {
            range: Some("100..110".to_string()),
            ..Default::default()
        };

        let command = VerifyCommand::new(
            &args,
            Shutdown::new().unwrap(),
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


}
