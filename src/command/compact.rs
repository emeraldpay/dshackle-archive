use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use anyhow::{anyhow, Result};
use apache_avro::types::{Record, Value};
use async_trait::async_trait;
use tokio::task::JoinSet;
use crate::{
    archiver::{
        Archiver,
        blocks_config::Blocks,
        datakind::{DataKind, DataOptions},
        range::Range,
        range_bag::RangeBag,
        range_group::ArchivesList
    },
    args::Args,
    avros,
    blockchain::{BlockDetails, BlockchainTypes},
    command::{
        CommandExecutor
    },
    global,
    storage::{FileReference, TargetFileReader, TargetFileWriter, ReadTarget},
};
use crate::archiver::datakind::{BlockOptions, TraceOptions, TxOptions};

#[derive(Clone)]
pub struct CompactCommand<B: BlockchainTypes, TS: ReadTarget> {
    b: PhantomData<B>,
    archiver: Archiver<B, TS>,
    
    blocks: Blocks,
    chunk_size: usize,
    data_options: DataOptions,
}

#[async_trait]
impl<B: BlockchainTypes + 'static, TS: ReadTarget + 'static> CommandExecutor for CompactCommand<B, TS> {

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
                            if relevant_chunk.is_none() {
                                continue;
                            }
                            let relevant_chunk = relevant_chunk.unwrap();
                            tracing::trace!("File {:?} is in chunk {}", file, relevant_chunk);

                            if current.is_none() {
                                current = Some(relevant_chunk.clone());
                            }
                            let current_chunk = current.as_ref().unwrap();

                            if !current_chunk.eq(relevant_chunk) {
                                // -----
                                // Process the current chunk as we got the new files
                                // -----

                                let compact = self.compact_range(current_chunk, current_files.clone())
                                    .await;

                                // no need to check the result if shutting down
                                if shutdown.is_signalled() {
                                    break;
                                }

                                let compact = match compact {
                                    Ok(compacted) => compacted,
                                    Err(e) => {
                                        tracing::error!(range = display(current_chunk), "Range is not compacted: {}", e);
                                        false
                                    }
                                };

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

impl<B: BlockchainTypes+ 'static, TS: ReadTarget + 'static> CompactCommand<B, TS> {
    pub fn new(config: &Args,
               archiver: Archiver<B, TS>
    ) -> Result<Self> {
        Ok(Self {
            b: PhantomData,
            archiver,
            blocks: Blocks::try_from(config)?,
            chunk_size: config.range_chunk.unwrap_or(1000),
            data_options: DataOptions::from(config),
        })
    }

    ///
    /// Compact data from the given tables into a single range table (per data kind). It only selects data that is in the given range.
    /// @range Range to compact
    /// @files List of files that can be used for compaction
    async fn compact_range(&self, range: &Range, files: Vec<FileReference>) -> Result<bool> {
        tracing::info!(range = display(range), "Compacting range chunk");
        let complete = match Self::verify_files(range, files, &self.data_options) {
            Err(e) => {
                tracing::warn!(range = display(range), "{}", e);
                return Ok(false);
            },
            Ok(c) => c,
        };
        let complete = Arc::new(complete);

        let data_options = self.data_options.clone();
        let mut compactions: JoinSet<Result<TS::Writer>> = JoinSet::new();
        let status = CopiedStatus::new(data_options);
        let status = Arc::new(Mutex::new(status));


        if let Some(_opts) = &self.data_options.block {
            let job = <CompactCommand<B, TS> as TableCompaction<BlockOptions, B, TS>>::compact_table(
                self.archiver.target.clone(),
                range.clone(),
                complete.clone(),
                status.clone()
            );
            compactions.spawn(async move {
                job.await
            });
        };

        if let Some(_opts) = &self.data_options.tx {
            let job = <CompactCommand<B, TS> as TableCompaction<TxOptions, B, TS>>::compact_table(
                self.archiver.target.clone(),
                range.clone(),
                complete.clone(),
                status.clone()
            );
            compactions.spawn(async move {
                job.await
            });
        };

        if let Some(_opts) = &self.data_options.trace {
            let job = <CompactCommand<B, TS> as TableCompaction<TraceOptions, B, TS>>::compact_table(
                self.archiver.target.clone(),
                range.clone(),
                complete.clone(),
                status.clone()
            );
            compactions.spawn(async move {
                job.await
            });
        };


        let mut files = vec![];
        while let Some(next) = compactions.join_next().await {
            match next {
                Ok(Ok(file)) => files.push(file),
                Ok(Err(e)) => return Err(e),
                // a panicked compaction must not kill the whole run, the caller just skips this range
                Err(e) => return Err(anyhow!("Compaction of the range failed: {}", e)),
            }
        }

        let all_copied = {
            let status = status.lock().unwrap();
            status.validate(range)
        };

        if all_copied {
            for file in files.into_iter() {
                // an unclosed file is incomplete (and dropped from the archive), so the caller must not
                // treat the range as compacted and delete its sources
                file.close().await?;
            }
            tracing::debug!(range = display(range), "Range compacted");
        }

        Ok(all_copied)
    }

    fn verify_files(range: &Range, files: Vec<FileReference>, tx_options: &DataOptions) -> Result<ArchivesList> {
        //
        // fist make sure it has complete data for every height in group
        //
        let mut complete = ArchivesList::new(tx_options.files());
        for file in files {
            complete.append(file)?;
        }
        // a group missing a table cannot be used as a source, even when the other groups already
        // cover the whole range (ex., leftovers of an interrupted compaction of the same range)
        let incomplete = complete.list_incomplete();
        if !incomplete.is_empty() {
            let sample = incomplete.iter()
                .take(5)
                .map(|g| format!("{} (no {})", g.range, g.get_incomplete_kinds().iter().map(|k| k.to_string()).collect::<Vec<String>>().join("+")))
                .collect::<Vec<String>>()
                .join(", ");
            // Fix alone would only fill the missing table and leave the range covered twice, which the
            // copy validation then rejects as a broken range
            return Err(anyhow!("Range has {} incomplete archives (ex., {}). Please run Verify with --fix.clean before Compaction. Skipping this range.", incomplete.len(), sample));
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

}

struct CopiedStatus<TXID: Hash + Eq> {
    data_options: DataOptions,
    copied_blocks: Vec<u64>,
    expected_txes: HashSet<TXID>,
    copied_txes: Vec<TXID>,
    copied_traces: Vec<TXID>,
}

impl<TXID: Hash + Eq + Debug> CopiedStatus<TXID> {
    fn new(data_options: DataOptions) -> Self {
        Self {
            data_options,
            copied_blocks: Vec::new(),
            expected_txes: HashSet::new(),
            copied_txes: Vec::new(),
            copied_traces: Vec::new(),
        }
    }

    fn contains_height(&self, height: u64) -> bool {
        self.copied_blocks.contains(&height)
    }

    fn contains_tx(&self, tx: &TXID) -> bool {
        self.copied_txes.contains(tx)
    }

    fn on_copied_block(&mut self, height: u64, expected_txes: Vec<TXID>) {
        self.copied_blocks.push(height);
        expected_txes.into_iter().for_each(|tx| {
            self.expected_txes.insert(tx);
        });
    }

    fn on_copied_tx(&mut self, tx: TXID) {
        self.copied_txes.push(tx);
    }

    fn on_copied_trace(&mut self, tx: TXID) {
        self.copied_traces.push(tx);
    }

    fn validate(&self, range: &Range) -> bool {
        if let Some(_opts) = &self.data_options.block {
            if self.copied_blocks.len() != range.len() {
                tracing::warn!(range = display(range), "Missing blocks. Please run Fix before Compaction. Skipping this range.");
                return false;
            }
            // build the range from the copied blocks to see if it's whole range
            let resulting_range = RangeBag::from(
                self.copied_blocks.iter().map(|h| h.clone()).collect::<Vec<u64>>()
            ).compact();

            if resulting_range.len() != 1 || resulting_range.as_single_range().unwrap() != range {
                tracing::warn!(range = display(range), "Missing blocks. Please run Fix before Compaction. Skipping this range.");
                return false;
            }
        }

        if let Some(_opts) = &self.data_options.tx {
            if self.expected_txes.len() != self.copied_txes.len() {
                tracing::warn!(range = display(range), "Missing transactions. Please run Fix before Compaction. Skipping this range.");
                return false;
            }
        }

        if let Some(_opts) = &self.data_options.trace {
            if self.expected_txes.len() != self.copied_traces.len() {
                tracing::warn!(range = display(range), "Missing traces. Please run Fix before Compaction. Skipping this range.");
                return false;
            }
        }

        true
    }
}

trait TableCompaction<T, B: BlockchainTypes, TS: ReadTarget> {
    async fn compact_table(target: Arc<TS>,
                           range: Range,
                           files: Arc<ArchivesList>,
                           status: Arc<Mutex<CopiedStatus<B::TxId>>>) -> Result<TS::Writer>;
}

fn parse_block<B: BlockchainTypes>(record: &Record) -> Result<B::BlockParsed> {
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

impl<B: BlockchainTypes, TS: ReadTarget> TableCompaction<BlockOptions, B, TS> for CompactCommand<B, TS> {
    async fn compact_table(target: Arc<TS>,
                           range: Range,
                           files: Arc<ArchivesList>,
                           status: Arc<Mutex<CopiedStatus<B::TxId>>>) -> Result<TS::Writer> {
        let target_file = target.create(DataKind::Blocks, &range, true)
            .await
            .map_err(|e| anyhow!("Unable to create block file: {}", e))?
            .ok_or_else(|| anyhow!("Target file was not created"))?;
        let shutdown = global::get_shutdown();

        for source_group in files.iter() {
            if shutdown.is_signalled() {
                return Err(anyhow!("Shutdown signalled"));
            }

            let mut source = target
                .open(source_group.blocks.as_ref().unwrap())
                .await?
                .read()?;

            while !shutdown.is_signalled() {
                tokio::select! {
                    _ = shutdown.signalled() => return Err(anyhow!("Shutdown signalled")),
                    record = source.recv() => {
                        match record {
                            // The sources are deleted once the range is copied, so a file that stopped
                            // being readable must stop the whole compaction instead of copying a part of it
                            Some(Err(failure)) => return Err(failure.into()),
                            Some(Ok(record)) => {
                                let height = avros::get_height(&record)?;
                                if range.contains(&height.into()) {
                                    let txes = parse_block::<B>(&record)?.txes();
                                    // the sources are deleted once the copied status covers the range, so a
                                    // record that failed to be written must never be counted as copied
                                    target_file.append_avro_record(record).await?;
                                    {
                                        let mut status = status.lock().unwrap();
                                        status.on_copied_block(height, txes);
                                    }
                                }
                            },
                            None => break,
                        }
                    }
                }
            }

        }

        Ok(target_file)
    }


}

fn parse_tx_id<B: BlockchainTypes>(record: &Record) -> Result<B::TxId> {
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

impl<B: BlockchainTypes, TS: ReadTarget> TableCompaction<TxOptions, B, TS> for CompactCommand<B, TS> {
    async fn compact_table(target: Arc<TS>,
                           range: Range,
                           files: Arc<ArchivesList>,
                           status: Arc<Mutex<CopiedStatus<B::TxId>>>) -> Result<TS::Writer> {
        let target_file = target.create(DataKind::Transactions, &range, true)
            .await
            .map_err(|e| anyhow!("Unable to create tx file: {}", e))?
            .ok_or_else(|| anyhow!("Target file was not created"))?;
        let shutdown = global::get_shutdown();

        for source_group in files.iter() {
            if shutdown.is_signalled() {
                return Err(anyhow!("Shutdown signalled"));
            }
            let mut source = target
                .open(source_group.txes.as_ref().unwrap())
                .await?
                .read()?;

            while !shutdown.is_signalled() {
                tokio::select! {
                    _ = shutdown.signalled() => return Err(anyhow!("Shutdown signalled")),
                    record = source.recv() => {
                        match record {
                            // See the blocks compaction on why a failed read is not just an end of the file
                            Some(Err(failure)) => return Err(failure.into()),
                            Some(Ok(record)) => {
                                let height = avros::get_height(&record)?;
                                let txid = parse_tx_id::<B>(&record)?;
                                if range.contains(&height.into()) {
                                    target_file.append_avro_record(record).await?;
                                    {
                                        let mut status = status.lock().unwrap();
                                        status.on_copied_tx(txid);
                                    }
                                }
                            },
                            None => break,
                        }
                    }
                }
            }

        }

        Ok(target_file)
    }
}

impl<B: BlockchainTypes, TS: ReadTarget> TableCompaction<TraceOptions, B, TS> for CompactCommand<B, TS> {
    async fn compact_table(target: Arc<TS>,
                           range: Range,
                           files: Arc<ArchivesList>,
                           status: Arc<Mutex<CopiedStatus<B::TxId>>>) -> Result<TS::Writer> {
        let target_file = target.create(DataKind::TransactionTraces, &range, true)
            .await
            .map_err(|e| anyhow!("Unable to create trace file: {}", e))?
            .ok_or_else(|| anyhow!("Target file was not created"))?;
        let shutdown = global::get_shutdown();

        for source_group in files.iter() {
            if shutdown.is_signalled() {
                return Err(anyhow!("Shutdown signalled"));
            }
            let mut source = target
                .open(source_group.traces.as_ref().unwrap())
                .await?
                .read()?;

            while !shutdown.is_signalled() {
                tokio::select! {
                    _ = shutdown.signalled() => return Err(anyhow!("Shutdown signalled")),
                    record = source.recv() => {
                        match record {
                            // See the blocks compaction on why a failed read is not just an end of the file
                            Some(Err(failure)) => return Err(failure.into()),
                            Some(Ok(record)) => {
                                let height = avros::get_height(&record)?;
                                let txid = parse_tx_id::<B>(&record)?;
                                if range.contains(&height.into()) {
                                    target_file.append_avro_record(record).await?;
                                    {
                                        let mut status = status.lock().unwrap();
                                        status.on_copied_trace(txid);
                                    }
                                }
                            },
                            None => break,
                        }
                    }
                }
            }

        }

        Ok(target_file)
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
    use crate::archiver::datakind::{DataOptions, TraceOptions};
    use crate::archiver::range::Range;
    use crate::storage::FileReference;
    use anyhow::Result;
    use super::CopiedStatus;

    // ============================================================================
    // CopiedStatus Tests
    // ============================================================================

    #[test]
    fn copied_status_new_creates_empty_status() {
        let data_options = DataOptions::default();
        let status: CopiedStatus<String> = CopiedStatus::new(data_options);

        assert_eq!(status.copied_blocks.len(), 0);
        assert_eq!(status.copied_txes.len(), 0);
        assert_eq!(status.copied_traces.len(), 0);
        assert_eq!(status.expected_txes.len(), 0);
    }

    #[test]
    fn copied_status_on_copied_block_adds_height_and_txes() {
        let data_options = DataOptions::default();
        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        let txes = vec!["tx1".to_string(), "tx2".to_string(), "tx3".to_string()];
        status.on_copied_block(100, txes.clone());

        assert_eq!(status.copied_blocks.len(), 1);
        assert_eq!(status.copied_blocks[0], 100);
        assert_eq!(status.expected_txes.len(), 3);
        assert!(status.expected_txes.contains(&"tx1".to_string()));
        assert!(status.expected_txes.contains(&"tx2".to_string()));
        assert!(status.expected_txes.contains(&"tx3".to_string()));
    }

    #[test]
    fn copied_status_on_copied_block_handles_duplicate_txes() {
        let data_options = DataOptions::default();
        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add first block with txes
        status.on_copied_block(100, vec!["tx1".to_string(), "tx2".to_string()]);
        // Add second block with overlapping tx (tx2 appears again)
        status.on_copied_block(101, vec!["tx2".to_string(), "tx3".to_string()]);

        assert_eq!(status.copied_blocks.len(), 2);
        // HashSet should deduplicate tx2
        assert_eq!(status.expected_txes.len(), 3);
    }

    #[test]
    fn copied_status_on_copied_tx_adds_tx() {
        let data_options = DataOptions::default();
        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        status.on_copied_tx("tx1".to_string());
        status.on_copied_tx("tx2".to_string());

        assert_eq!(status.copied_txes.len(), 2);
        assert_eq!(status.copied_txes[0], "tx1");
        assert_eq!(status.copied_txes[1], "tx2");
    }

    #[test]
    fn copied_status_on_copied_trace_adds_trace() {
        let data_options = DataOptions::default();
        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        status.on_copied_trace("tx1".to_string());
        status.on_copied_trace("tx2".to_string());

        assert_eq!(status.copied_traces.len(), 2);
        assert_eq!(status.copied_traces[0], "tx1");
        assert_eq!(status.copied_traces[1], "tx2");
    }

    #[test]
    fn copied_status_contains_height_returns_true_when_present() {
        let data_options = DataOptions::default();
        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        status.on_copied_block(100, vec![]);

        assert!(status.contains_height(100));
        assert!(!status.contains_height(101));
    }

    #[test]
    fn copied_status_contains_tx_returns_true_when_present() {
        let data_options = DataOptions::default();
        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        status.on_copied_tx("tx1".to_string());

        assert!(status.contains_tx(&"tx1".to_string()));
        assert!(!status.contains_tx(&"tx2".to_string()));
    }

    #[test]
    fn copied_status_validate_succeeds_with_complete_blocks_only() {
        let mut data_options = DataOptions::default();
        data_options.block = Some(crate::archiver::datakind::BlockOptions::default());
        data_options.tx = None;
        data_options.trace = None;

        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add blocks 100-104 (5 blocks)
        for height in 100..105 {
            status.on_copied_block(height, vec![]);
        }

        let range = Range::new(100, 104);
        assert!(status.validate(&range));
    }

    #[test]
    fn copied_status_validate_fails_with_missing_blocks() {
        let mut data_options = DataOptions::default();
        data_options.block = Some(crate::archiver::datakind::BlockOptions::default());

        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add blocks 100-102 (missing 103-104)
        for height in 100..103 {
            status.on_copied_block(height, vec![]);
        }

        let range = Range::new(100, 104);
        assert!(!status.validate(&range));
    }

    #[test]
    fn copied_status_validate_fails_with_non_contiguous_blocks() {
        let mut data_options = DataOptions::default();
        data_options.block = Some(crate::archiver::datakind::BlockOptions::default());

        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add blocks 100, 101, 103, 104 (missing 102)
        status.on_copied_block(100, vec![]);
        status.on_copied_block(101, vec![]);
        status.on_copied_block(103, vec![]);
        status.on_copied_block(104, vec![]);

        let range = Range::new(100, 104);
        assert!(!status.validate(&range));
    }

    #[test]
    fn copied_status_validate_succeeds_with_complete_blocks_and_txes() {
        let mut data_options = DataOptions::default();
        data_options.block = Some(crate::archiver::datakind::BlockOptions::default());
        data_options.tx = Some(crate::archiver::datakind::TxOptions::default());

        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add blocks with txes
        status.on_copied_block(100, vec!["tx1".to_string(), "tx2".to_string()]);
        status.on_copied_block(101, vec!["tx3".to_string()]);

        // Add copied txes
        status.on_copied_tx("tx1".to_string());
        status.on_copied_tx("tx2".to_string());
        status.on_copied_tx("tx3".to_string());

        let range = Range::new(100, 101);
        assert!(status.validate(&range));
    }

    #[test]
    fn copied_status_validate_fails_with_missing_txes() {
        let mut data_options = DataOptions::default();
        data_options.block = Some(crate::archiver::datakind::BlockOptions::default());
        data_options.tx = Some(crate::archiver::datakind::TxOptions::default());

        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add blocks with txes
        status.on_copied_block(100, vec!["tx1".to_string(), "tx2".to_string()]);
        status.on_copied_block(101, vec!["tx3".to_string()]);

        // Add only partial txes
        status.on_copied_tx("tx1".to_string());
        // Missing tx2 and tx3

        let range = Range::new(100, 101);
        assert!(!status.validate(&range));
    }

    #[test]
    fn copied_status_validate_succeeds_with_complete_traces() {
        let data_options = DataOptions {
            overwrite: true,
            block: Some(crate::archiver::datakind::BlockOptions::default()),
            tx: None,  // Disable tx validation
            trace: Some(crate::archiver::datakind::TraceOptions::default()),
        };

        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add blocks with txes
        status.on_copied_block(100, vec!["tx1".to_string(), "tx2".to_string()]);

        // Add copied traces
        status.on_copied_trace("tx1".to_string());
        status.on_copied_trace("tx2".to_string());

        let range = Range::Single(100.into());
        assert!(status.validate(&range));
    }

    #[test]
    fn copied_status_validate_fails_with_missing_traces() {
        let mut data_options = DataOptions::default();
        data_options.block = Some(crate::archiver::datakind::BlockOptions::default());
        data_options.trace = Some(crate::archiver::datakind::TraceOptions::default());

        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add blocks with txes
        status.on_copied_block(100, vec!["tx1".to_string(), "tx2".to_string()]);

        // Add only partial traces
        status.on_copied_trace("tx1".to_string());
        // Missing tx2 trace

        let range = Range::new(100, 100);
        assert!(!status.validate(&range));
    }

    #[test]
    fn copied_status_validate_succeeds_with_all_data_types() {
        let mut data_options = DataOptions::default();
        data_options.block = Some(crate::archiver::datakind::BlockOptions::default());
        data_options.tx = Some(crate::archiver::datakind::TxOptions::default());
        data_options.trace = Some(crate::archiver::datakind::TraceOptions::default());

        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add blocks with txes
        status.on_copied_block(100, vec!["tx1".to_string(), "tx2".to_string()]);
        status.on_copied_block(101, vec!["tx3".to_string()]);

        // Add copied txes
        status.on_copied_tx("tx1".to_string());
        status.on_copied_tx("tx2".to_string());
        status.on_copied_tx("tx3".to_string());

        // Add copied traces
        status.on_copied_trace("tx1".to_string());
        status.on_copied_trace("tx2".to_string());
        status.on_copied_trace("tx3".to_string());

        let range = Range::new(100, 101);
        assert!(status.validate(&range));
    }

    #[test]
    fn copied_status_validate_skips_validation_when_data_type_not_enabled() {
        let data_options = DataOptions {
            overwrite: true,
            block: Some(crate::archiver::datakind::BlockOptions::default()),
            tx: None,  // Disable tx validation
            trace: None,  // Disable trace validation
        };

        let mut status: CopiedStatus<String> = CopiedStatus::new(data_options);

        // Add blocks with txes (but we won't validate txes/traces)
        for height in 100..102 {
            status.on_copied_block(height, vec!["tx1".to_string(), "tx2".to_string()]);
        }

        // Don't add txes or traces - but validation should still pass since they're not enabled

        let range = Range::new(100, 101);
        assert!(status.validate(&range));
    }

    // ============================================================================
    // CompactCommand Integration Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn compact_command() {
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
                parent: format!("B{}", block_height-1),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn compact_command_with_traces() {
        testing::start_test();

        let mem = Arc::new(InMemory::new());
        let data = MockData::new("TEST_TRACES");
        let data_provider: Arc<MockData> = Arc::new(data);
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/eth".to_string()));
        let archiver = Archiver::new_simple(Arc::new(storage), data_provider.clone());

        let trace_options = TraceOptions {
            include_trace: true,
            include_state_diff: true,
        };

        // Create 25 blocks with transactions and traces
        for i in 0..25 {
            let block_height = 100 + i as u64;
            let txs = vec![
                format!("TX{}-1", block_height),
                format!("TX{}-2", block_height),
            ];
            data_provider.add_block(MockBlock {
                height: block_height,
                hash: format!("B{}", block_height),
                parent: format!("B{}", block_height-1),
                transactions: txs.clone(),
            });
            for tx in &txs {
                data_provider.add_tx(MockTx { hash: tx.clone() });
                data_provider.add_trace(MockTrace {
                    tx_hash: tx.clone(),
                    trace_json: Some(format!(r#"{{"trace":"data for {}"}}"#, tx).into_bytes()),
                    state_diff_json: Some(format!(r#"{{"diff":"state for {}"}}"#, tx).into_bytes()),
                });
            }

            testing::write_block_tx_and_traces(&archiver, block_height, None, Some(trace_options.clone())).await.unwrap();
        }

        let args = Args {
            range: Some("100..125".to_string()),
            range_chunk: Some(10),
            tables: Some("blocks,txes,traces".to_string()),
            fields_trace: Some("calls,stateDiff".to_string()),
            ..Default::default()
        };
        let compact_cmd = CompactCommand::new(&args, archiver).unwrap();

        let _ = compact_cmd.execute().await.unwrap();

        let result = testing::list_mem_filenames(mem).await;

        // Should have:
        // - Last 5 blocks (120-124) as individual files (blocks + txes + traces)
        // - Two compacted ranges (100-109 and 110-119) with blocks, txes, and traces
        assert_eq!(
            result,
            vec![
                // Individual files for blocks 120-124
                "archive/eth/000000000/000000000/000000120.block.avro",
                "archive/eth/000000000/000000000/000000120.traces.avro",
                "archive/eth/000000000/000000000/000000120.txes.avro",
                "archive/eth/000000000/000000000/000000121.block.avro",
                "archive/eth/000000000/000000000/000000121.traces.avro",
                "archive/eth/000000000/000000000/000000121.txes.avro",
                "archive/eth/000000000/000000000/000000122.block.avro",
                "archive/eth/000000000/000000000/000000122.traces.avro",
                "archive/eth/000000000/000000000/000000122.txes.avro",
                "archive/eth/000000000/000000000/000000123.block.avro",
                "archive/eth/000000000/000000000/000000123.traces.avro",
                "archive/eth/000000000/000000000/000000123.txes.avro",
                "archive/eth/000000000/000000000/000000124.block.avro",
                "archive/eth/000000000/000000000/000000124.traces.avro",
                "archive/eth/000000000/000000000/000000124.txes.avro",

                // Compacted ranges
                "archive/eth/000000000/range-000000100_000000109.blocks.avro",
                "archive/eth/000000000/range-000000100_000000109.traces.avro",
                "archive/eth/000000000/range-000000100_000000109.txes.avro",
                "archive/eth/000000000/range-000000110_000000119.blocks.avro",
                "archive/eth/000000000/range-000000110_000000119.traces.avro",
                "archive/eth/000000000/range-000000110_000000119.txes.avro",
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compact_command_with_traces_only_calls() {
        testing::start_test();

        let mem = Arc::new(InMemory::new());
        let data = MockData::new("TEST_TRACE_CALLS");
        let data_provider: Arc<MockData> = Arc::new(data);
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/eth".to_string()));
        let archiver = Archiver::new_simple(Arc::new(storage), data_provider.clone());

        let trace_options = TraceOptions {
            include_trace: true,
            include_state_diff: false,
        };

        // Create 15 blocks with transactions and traces (only calls)
        for i in 0..15 {
            let block_height = 200 + i as u64;
            let txs = vec![
                format!("TX{}-1", block_height),
                format!("TX{}-2", block_height),
                format!("TX{}-3", block_height),
            ];
            data_provider.add_block(MockBlock {
                height: block_height,
                hash: format!("B{}", block_height),
                parent: format!("B{}", block_height-1),
                transactions: txs.clone(),
            });
            for tx in &txs {
                data_provider.add_tx(MockTx { hash: tx.clone() });
                data_provider.add_trace(MockTrace {
                    tx_hash: tx.clone(),
                    trace_json: Some(format!(r#"{{"calls":["call1","call2"]}}"#).into_bytes()),
                    state_diff_json: None,
                });
            }

            testing::write_block_tx_and_traces(&archiver, block_height, None, Some(trace_options.clone())).await.unwrap();
        }

        let args = Args {
            range: Some("200..215".to_string()),
            range_chunk: Some(10),
            tables: Some("blocks,txes,traces".to_string()),
            fields_trace: Some("calls".to_string()),
            ..Default::default()
        };
        let compact_cmd = CompactCommand::new(&args, archiver).unwrap();

        let _ = compact_cmd.execute().await.unwrap();

        let result = testing::list_mem_filenames(mem).await;

        // Should have:
        // - Last 5 blocks (210-214) as individual files
        // - One compacted range (200-209)
        assert_eq!(result.len(), 18); // 5*3 + 3 = 18 files

        // Check that compacted ranges exist
        assert!(result.contains(&"archive/eth/000000000/range-000000200_000000209.blocks.avro".to_string()));
        assert!(result.contains(&"archive/eth/000000000/range-000000200_000000209.txes.avro".to_string()));
        assert!(result.contains(&"archive/eth/000000000/range-000000200_000000209.traces.avro".to_string()));
    }

    fn archive_files(range: Range, kinds: &[crate::archiver::datakind::DataKind]) -> Vec<FileReference> {
        kinds.iter()
            .map(|kind| FileReference {
                range: range.clone(),
                kind: *kind,
                path: format!("{}-{}", range, kind),
            })
            .collect()
    }

    fn verify_all_tables(files: Vec<FileReference>) -> Result<()> {
        let mut options = DataOptions::default();
        options.trace = Some(TraceOptions::default());
        CompactCommand::<MockType, ObjectsStorage<InMemory>>::verify_files(&Range::new(100, 104), files, &options)
            .map(|_| ())
    }

    #[test]
    fn verify_files_accepts_complete_blocks() {
        use crate::archiver::datakind::DataKind::*;

        let files = (100..105)
            .flat_map(|h| archive_files(Range::single(h), &[Blocks, Transactions, TransactionTraces]))
            .collect();

        assert!(verify_all_tables(files).is_ok());
    }

    #[test]
    fn verify_files_rejects_group_missing_a_table() {
        use crate::archiver::datakind::DataKind::*;

        let mut files: Vec<FileReference> = (100..105)
            .flat_map(|h| archive_files(Range::single(h), &[Blocks, Transactions, TransactionTraces]))
            .collect();
        // the whole range is covered by the blocks above, but this one cannot be used as a source
        files.extend(archive_files(Range::new(100, 104), &[Blocks, Transactions]));

        let err = verify_all_tables(files).unwrap_err().to_string();
        assert!(err.contains("incomplete archives"), "{}", err);
    }

    /// Leftovers of an interrupted compaction (a range file for some of the tables) used to make the
    /// compaction unwrap a missing source file and panic
    #[tokio::test(flavor = "multi_thread")]
    async fn compact_command_skips_range_with_incomplete_leftovers() {
        use crate::storage::{TargetFileWriter, WriteTarget};

        testing::start_test();

        let mem = Arc::new(InMemory::new());
        let data = MockData::new("TEST_LEFTOVER");
        let data_provider: Arc<MockData> = Arc::new(data);
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/test".to_string()));
        let archiver = Archiver::new_simple(Arc::new(storage), data_provider.clone());

        let trace_options = TraceOptions {
            include_trace: true,
            include_state_diff: false,
        };

        // a chunk is compacted only once the listing moves on to the next one, so there are blocks after it
        for i in 0..15 {
            let block_height = 100 + i as u64;
            let txs = vec![format!("TX{}-1", block_height)];
            data_provider.add_block(MockBlock {
                height: block_height,
                hash: format!("B{}", block_height),
                parent: format!("B{}", block_height-1),
                transactions: txs.clone(),
            });
            for tx in &txs {
                data_provider.add_tx(MockTx { hash: tx.clone() });
                data_provider.add_trace(MockTrace {
                    tx_hash: tx.clone(),
                    trace_json: Some(format!(r#"{{"calls":["call1"]}}"#).into_bytes()),
                    state_diff_json: None,
                });
            }
            testing::write_block_tx_and_traces(&archiver, block_height, None, Some(trace_options.clone())).await.unwrap();
        }

        // as if a previous compaction wrote the range files for a part of the tables and then died
        let chunk = Range::new(100, 109);
        for kind in [crate::archiver::datakind::DataKind::Blocks, crate::archiver::datakind::DataKind::Transactions] {
            let file = archiver.target.create(kind, &chunk, true).await.unwrap().unwrap();
            file.close().await.unwrap();
        }

        let args = Args {
            range: Some("100..115".to_string()),
            range_chunk: Some(10),
            tables: Some("blocks,txes,traces".to_string()),
            fields_trace: Some("calls".to_string()),
            ..Default::default()
        };
        let compact_cmd = CompactCommand::new(&args, archiver).unwrap();

        compact_cmd.execute().await.unwrap();

        // the range is skipped, so nothing is deleted
        let result = testing::list_mem_filenames(mem).await;
        assert_eq!(result.len(), 15 * 3 + 2);
        assert!(result.contains(&"archive/test/000000000/000000000/000000100.traces.avro".to_string()));
        assert!(result.contains(&"archive/test/000000000/000000000/000000109.traces.avro".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compact_command_partial_chunk_not_deleted() {
        testing::start_test();

        let mem = Arc::new(InMemory::new());
        let data = MockData::new("TEST_PARTIAL");
        let data_provider: Arc<MockData> = Arc::new(data);
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/test".to_string()));
        let archiver = Archiver::new_simple(Arc::new(storage), data_provider.clone());

        // Create only 7 blocks (not a complete chunk of 10)
        for i in 0..7 {
            let block_height = 100 + i as u64;
            let txs = vec![
                format!("TX{}-1", block_height),
                format!("TX{}-2", block_height),
            ];
            data_provider.add_block(MockBlock {
                height: block_height,
                hash: format!("B{}", block_height),
                parent: format!("B{}", block_height-1),
                transactions: txs.clone(),
            });
            for tx in &txs {
                data_provider.add_tx(MockTx { hash: tx.clone() });
            }

            testing::write_block_and_tx(&archiver, block_height, None).await.unwrap();
        }

        let args = Args {
            range: Some("100..107".to_string()),
            range_chunk: Some(10),
            ..Default::default()
        };
        let compact_cmd = CompactCommand::new(&args, archiver).unwrap();

        let _ = compact_cmd.execute().await.unwrap();

        let result = testing::list_mem_filenames(mem).await;

        // All 7 blocks should remain as individual files (no compaction since incomplete chunk)
        assert_eq!(result.len(), 14); // 7 blocks * 2 files (block + txes)

        // Verify individual files exist
        for i in 0..7 {
            let height = 100 + i;
            assert!(result.contains(&format!("archive/test/000000000/000000000/{:09}.block.avro", height)));
            assert!(result.contains(&format!("archive/test/000000000/000000000/{:09}.txes.avro", height)));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compact_command_deletes_source_files() {
        testing::start_test();

        let mem = Arc::new(InMemory::new());
        let data = MockData::new("TEST_DELETE");
        let data_provider: Arc<MockData> = Arc::new(data);
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/test".to_string()));
        let archiver = Archiver::new_simple(Arc::new(storage), data_provider.clone());

        // Create 15 blocks - enough for one complete chunk (300-309) and partial chunk (310-314)
        for i in 0..15 {
            let block_height = 300 + i as u64;
            let txs = vec![format!("TX{}", block_height)];
            data_provider.add_block(MockBlock {
                height: block_height,
                hash: format!("B{}", block_height),
                parent: format!("B{}", block_height-1),
                transactions: txs.clone(),
            });
            for tx in &txs {
                data_provider.add_tx(MockTx { hash: tx.clone() });
            }

            testing::write_block_and_tx(&archiver, block_height, None).await.unwrap();
        }

        // Verify we have 30 files before compaction (15 blocks * 2)
        let before = testing::list_mem_filenames(mem.clone()).await;
        assert_eq!(before.len(), 30);

        let args = Args {
            range: Some("300..315".to_string()),  // 300-315 = 16 blocks total
            range_chunk: Some(10),
            ..Default::default()
        };
        let compact_cmd = CompactCommand::new(&args, archiver).unwrap();

        let _ = compact_cmd.execute().await.unwrap();

        let after = testing::list_mem_filenames(mem).await;

        // After compaction:
        // - First chunk (300-309) should be compacted into range files
        // - Individual files for 310-314 should remain (incomplete chunk)
        assert_eq!(after.len(), 12); // 2 compacted files + 5*2 individual files
        assert!(after.contains(&"archive/test/000000000/range-000000300_000000309.blocks.avro".to_string()));
        assert!(after.contains(&"archive/test/000000000/range-000000300_000000309.txes.avro".to_string()));

        // Verify individual files for incomplete chunk remain
        for height in 310..315 {
            assert!(after.contains(&format!("archive/test/000000000/000000000/{:09}.block.avro", height)));
            assert!(after.contains(&format!("archive/test/000000000/000000000/{:09}.txes.avro", height)));
        }
    }

    ///
    /// The source files are deleted once their content is copied into the range file, so a read
    /// that fails in the middle must stop the compaction instead of leaving a truncated copy.
    #[tokio::test(flavor = "multi_thread")]
    async fn keeps_source_files_that_cannot_be_read() {
        testing::start_test();

        let mem = Arc::new(InMemory::new());
        let data_provider: Arc<MockData> = Arc::new(MockData::new("TEST_UNREADABLE"));
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/test".to_string()));
        let archiver: Archiver<MockType, _> = Archiver::new_simple(Arc::new(storage), data_provider.clone());

        // as in `compact_command_deletes_source_files`: the chunk 300-309 is compacted only when
        // the files of the next chunk show up in the listing
        for i in 0..15 {
            let block_height = 300 + i as u64;
            let txs = vec![format!("TX{}", block_height)];
            data_provider.add_block(MockBlock {
                height: block_height,
                hash: format!("B{}", block_height),
                parent: format!("B{}", block_height - 1),
                transactions: txs.clone(),
            });
            for tx in &txs {
                data_provider.add_tx(MockTx { hash: tx.clone() });
            }
            testing::write_block_and_tx(&archiver, block_height, None).await.unwrap();
        }

        let before = testing::list_mem_filenames(mem.clone()).await;
        assert_eq!(before.len(), 30);

        // the same archive, but now every download from the storage breaks
        let storage = ObjectsStorage::new(
            Arc::new(testing::BrokenDownloads::new(mem.clone())),
            "test".to_string(),
            Filenames::with_dir("archive/test".to_string()),
        );
        let archiver: Archiver<MockType, _> = Archiver::new_simple(Arc::new(storage), data_provider);

        let args = Args {
            range: Some("300..315".to_string()),
            range_chunk: Some(10),
            ..Default::default()
        };
        CompactCommand::new(&args, archiver).unwrap().execute().await.unwrap();

        let after = testing::list_mem_filenames(mem).await;
        for file in &before {
            assert!(after.contains(file), "Deleted a file that was never read: {}", file);
        }
    }

    /// A write that fails only when the data goes to the storage (i.e., not on the append) used to be
    /// reported as a successfully compacted range, and the sources were deleted
    #[tokio::test(flavor = "multi_thread")]
    async fn keeps_source_files_when_the_target_is_not_written() {
        testing::start_test();

        let mem = Arc::new(InMemory::new());
        let data_provider: Arc<MockData> = Arc::new(MockData::new("TEST_UNWRITABLE"));
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/test".to_string()));
        let archiver: Archiver<MockType, _> = Archiver::new_simple(Arc::new(storage), data_provider.clone());

        // the chunk 300-309 is compacted only when the files of the next chunk show up in the listing
        for i in 0..15 {
            let block_height = 300 + i as u64;
            let txs = vec![format!("TX{}", block_height)];
            data_provider.add_block(MockBlock {
                height: block_height,
                hash: format!("B{}", block_height),
                parent: format!("B{}", block_height - 1),
                transactions: txs.clone(),
            });
            for tx in &txs {
                data_provider.add_tx(MockTx { hash: tx.clone() });
            }
            testing::write_block_and_tx(&archiver, block_height, None).await.unwrap();
        }

        let before = testing::list_mem_filenames(mem.clone()).await;
        assert_eq!(before.len(), 30);

        // the same archive, but now the compacted range files cannot be uploaded
        let storage = ObjectsStorage::new(
            Arc::new(testing::BrokenUploads::only(mem.clone(), "range-")),
            "test".to_string(),
            Filenames::with_dir("archive/test".to_string()),
        );
        let archiver: Archiver<MockType, _> = Archiver::new_simple(Arc::new(storage), data_provider);

        let args = Args {
            range: Some("300..315".to_string()),
            range_chunk: Some(10),
            ..Default::default()
        };
        CompactCommand::new(&args, archiver).unwrap().execute().await.unwrap();

        let after = testing::list_mem_filenames(mem).await;
        for file in &before {
            assert!(after.contains(file), "Deleted a source of a range that was not written: {}", file);
        }
    }
}
