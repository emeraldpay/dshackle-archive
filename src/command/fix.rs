// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use anyhow::anyhow;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use crate::{archiver::{ArchiveAll, Archiver}, args::{Args, Command}, blockchain::BlockchainTypes, command::{CommandExecutor, report::RunReport}, global, memory, notify::RunMode, storage::{MissingTables, ScanTarget}};
use crate::archiver::blocks_config::Blocks;
use crate::archiver::datakind::DataOptions;
use crate::archiver::range::Range;
use crate::archiver::range_bag::RangeBag;

///
/// Provides `fix` command.
/// It checks the archive for the specified range and add missing data
///
#[derive(Clone)]
pub struct FixCommand<B: BlockchainTypes, TS: ScanTarget> {
    b: PhantomData<B>,
    blocks: Blocks,
    chunk_size: usize,
    archiver: Archiver<B, TS>,
    tx_options: DataOptions,
}

///
/// What the Fix did with a part of the range
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Fixing {
    /// The missing tables are archived
    Fixed,
    /// The tables are missing, but nothing is archived because of the dry-run mode
    DryRun,
    /// Nothing is missing there, so the Fix didn't touch it
    AlreadyArchived,
}

impl Display for Fixing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Fixing::Fixed => write!(f, "fixed"),
            Fixing::DryRun => write!(f, "would be fixed (dry run)"),
            Fixing::AlreadyArchived => write!(f, "already archived"),
        }
    }
}

impl<B: BlockchainTypes, TS: ScanTarget> FixCommand<B, TS> {
    pub fn new(config: &Args,
               archiver: Archiver<B, TS>) -> anyhow::Result<Self> {

        let tx_options = DataOptions::from(config);

        Ok(Self {
            b: PhantomData,
            archiver,
            blocks: Blocks::try_from(config)?,
            chunk_size: config.get_chunk_size(),
            tx_options,
        })
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: ScanTarget> CommandExecutor for FixCommand<B, TS> {

    async fn execute(&self) -> anyhow::Result<()> {
        let shutdown = global::get_shutdown();
        let range = self.blocks.to_range(self.archiver.data_provider.as_ref()).await?;
        // A tail starts at an arbitrary height that moves with every run. If it's left mid-chunk, the existing chunk file
        // is not found for it, and the partial head of that chunk would be re-archived on each run.
        let range = if self.blocks.is_tail() {
            match range.trim_to_chunk_start(self.chunk_size) {
                Some(range) => range,
                None => {
                    tracing::info!("Tail {} doesn't reach a chunk boundary. Nothing to fix", range);
                    return Ok(());
                }
            }
        } else {
            range
        };
        let dry_run = global::is_dry_run();
        tracing::info!("Fixing range: {}", range);

        let options = DataOptions {
            // we always keep existing files in Fix
            overwrite: false,
            ..self.tx_options.clone()
        };
        let missing = self.archiver.target.find_incomplete_tables(range.clone(), &options).await?;

        let mut already_archived = RangeBag::new();
        already_archived.append(range.clone());
        let mut chunked: Vec<(Vec<Range>, DataOptions)> = vec![];
        let mut unfixable: Vec<MissingTables> = vec![];
        for missing in missing {
            already_archived.remove(&missing.range);
            tracing::info!(range = %missing.range, "Found missing data: {:?}", missing.kinds);
            match missing.chunks(self.chunk_size) {
                Some(chunks) => chunked.push((chunks, options.clone().only_include(&missing.kinds))),
                None => unfixable.push(missing),
            }
        }
        let planned = already_archived.len()
            + unfixable.len()
            + chunked.iter().map(|(chunks, _)| chunks.len()).sum::<usize>();
        let mut report = RunReport::new(Command::Fix, range, planned);
        // the scan stops at a shutdown and gives only what it found by then, so the rest is not known to be archived
        if !shutdown.is_signalled() {
            for part in &already_archived.ranges {
                report.on_range(Fixing::AlreadyArchived, part, None);
            }
        }

        let mut failed: Vec<Range> = vec![];
        // Archiving them would only add files next to the existing ones, under another name, and every following run
        // would find the same tables missing. It needs Verify to delete the existing files first.
        for missing in unfixable {
            let kinds = missing.kinds.iter().map(|k| k.to_string()).collect::<Vec<_>>().join("+");
            let reason = format!(
                "Range has an incomplete archive that Fix cannot complete with chunks of {} blocks (no {}). Please run Verify with --fix.clean before Fix.",
                self.chunk_size, kinds,
            );
            tracing::warn!(range = %missing.range, "{}", reason);
            report.on_failure(&missing.range, None, reason);
            failed.push(missing.range);
        }

        // `fix` runs against settled archive state — no re-org signal applies.
        let cancel = CancellationToken::new();
        for (chunks, options) in chunked {
            if shutdown.is_signalled() {
                break;
            }
            for chunk in chunks {
                if shutdown.is_signalled() {
                    break;
                }
                tracing::info!(range = %chunk, "Fixing chunk");
                if dry_run {
                    report.on_range(Fixing::DryRun, &chunk, None);
                    continue;
                }
                // A failed table is never committed, so the chunk stays incomplete and the next run finds it again.
                // It's safe to move on instead of leaving the rest of the range unfixed because of one failed block.
                match self.archiver.archive(chunk.clone(), RunMode::Fix, None, &options, &cancel).await {
                    Ok(archived) => report.on_range(Fixing::Fixed, &chunk, Some(archived.files)),
                    // a chunk interrupted by a shutdown is not a failure, it's just not processed
                    Err(_) if shutdown.is_signalled() => {},
                    Err(e) => {
                        tracing::error!(range = %chunk, "Failed to fix chunk: {:?}", e);
                        report.on_failure(&chunk, None, format!("{:#}", e));
                        failed.push(chunk);
                    }
                }
                let _ = memory::release().await;
            }
        }

        if shutdown.is_signalled() {
            report.on_interrupted();
        }
        report.log();

        if !failed.is_empty() {
            let ranges = failed.iter().map(|r| r.to_string()).collect::<Vec<_>>().join(", ");
            return Err(anyhow!("Failed to fix {} chunk(s): {}", failed.len(), ranges));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use futures_util::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
    use crate::archiver::Archiver;
    use crate::archiver::filenames::Filenames;
    use crate::args::Args;
    use crate::blockchain::mock::{MockData, MockType};
    use crate::command::CommandExecutor;
    use crate::command::fix::FixCommand;
    use crate::storage::objects::ObjectsStorage;
    use crate::testing;

    #[tokio::test(flavor = "multi_thread")]
    async fn fails_on_single_block_range_file_and_keeps_files() {
        testing::start_test();
        let files = [
            "archive/eth/000000000/000000000/000000105.block.avro",
            "archive/eth/000000000/000000000/000000105.txes.avro",
            "archive/eth/000000000/range-000000105_000000105.blocks.avro",
        ];
        let mem = Arc::new(InMemory::new());
        for path in files {
            mem.put(&Path::from(path), PutPayload::from_static(b"data")).await.unwrap();
        }
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/eth".to_string()));
        let archiver: Archiver<MockType, ObjectsStorage<InMemory>> = Archiver::new_simple(
            Arc::new(storage),
            Arc::new(MockData::new("test")),
        );
        let args = Args {
            range: Some("105..105".to_string()),
            range_chunk: Some(10),
            ..Default::default()
        };

        let result = FixCommand::new(&args, archiver).unwrap()
            .execute().await;

        assert!(result.is_err());
        let mut left: Vec<String> = mem.list(None)
            .map(|meta| meta.unwrap().location.to_string())
            .collect().await;
        left.sort();
        assert_eq!(left, vec![
            "archive/eth/000000000/000000000/000000105.block.avro",
            "archive/eth/000000000/000000000/000000105.txes.avro",
            "archive/eth/000000000/range-000000105_000000105.blocks.avro",
        ]);
    }
}
