use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
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
use crate::archiver::datakind::{BlockOptions, DataKind, TraceOptions, TxOptions};
use crate::archiver::range_bag::RangeBag;
use crate::archiver::range_group::RangeGroupError;
use crate::blockchain::block_seq::BlockSequence;
use crate::blockchain::{BlockReference, BlockchainData};
use crate::storage::FileReference;

///
/// Provides `verify` command.
///
/// Checks the range of block in the current archive and deletes the files with missing or incorrect data (which is supposed to be reloaded back with `fix` command).
///
/// How it works:
/// - gets the list of all the files and groups them by range (ArchiveGroup)
/// - if the group is incomplete (missing some tables), deletes all files in the group if --fix.clean is set, otherwise skips the group
/// - for each large group:
///   - if there are multiple groups in the same range, keeps only the largest one (by number of blocks covered), deletes the others
///   - verifies the blocks are in sequence and all blocks are present
///     - "in sequence" means each block reference the parent block
///   - verifies that the final block in the range is actually present on the blockchain
/// - for each small group:
///   - merge the connected ranges to form a large group (if possible)
///   - apply the logic for the large groups
///
/// Small groups: groups that cover less than 10 blocks
///
/// What it deleted:
///   - if blocks in the groups are not in sequence or missing, deletes all files in the group (i.e., including txes and traces)
///   - if blocks are ok, but some txes or traces are missing or corrupted, deletes only the group of files;
///     i.e., if a tx for just one of the blocks is missing then it deletes all files with txes in that group;
///
#[derive(Clone)]
pub struct VerifyCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    blocks: Blocks,
    archiver: Arc<Archiver<B, TS>>,
    data_options: DataOptions,
    /// if try then delete the whole group of files in any issue with any of other files in the group
    /// i.e., if a Tx is invalid (or missing, etc.) the delete the file with corresponding blocks as well even if it's verified as valid
    delete_whole_groups: bool,
    chunk: usize,
}

struct TableStat {
    processed: usize,
    deleted: usize,
}

impl Default for TableStat {
    fn default() -> Self {
        Self {
            processed: 0,
            deleted: 0,
        }
    }
}

struct VerificationStat {
    tables: HashMap<DataKind, TableStat>,
}

impl VerificationStat {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    fn on_processed(&mut self, kind: &DataKind, range: &Range) {
        let stat = self.tables.entry(kind.clone()).or_insert_with(|| TableStat::default());
        stat.processed += range.len();
    }

    fn on_delete(&mut self, kind: &DataKind, range: &Range) {
        let stat = self.tables.entry(kind.clone()).or_insert_with(|| TableStat::default());
        stat.deleted += range.len();
    }
}

///
/// Data for the verification based on the filenames only
struct Preprocess {
    input: Vec<ArchiveGroup>,
    for_deletion: Vec<FileReference>,
}

impl Preprocess {
    fn new(archive: ArchivesList) -> Self {
        Self {
            input: archive.all(),
            for_deletion: vec![],
        }
    }

    ///
    /// Mark all files in the group for deletion
    fn delete_all(&mut self, group: ArchiveGroup) {
        for f in group.tables() {
            self.for_deletion.push(f.clone());
        }
    }

    /// Get out the current list of input files.
    ///
    /// WARNING: this clears the internal list.
    ///
    fn inputs(&mut self) -> Vec<ArchiveGroup> {
        std::mem::take(&mut self.input)
    }
}

impl<B: BlockchainTypes + 'static, TS: TargetStorage + 'static> VerifyCommand<B, TS> {
    pub fn new(config: &crate::args::Args,
               archiver: Archiver<B, TS>,
    ) -> anyhow::Result<Self> {

        Ok(Self {
            b: PhantomData,
            blocks: Blocks::try_from(config)?,
            archiver: Arc::new(archiver),
            data_options: DataOptions::from(config),
            delete_whole_groups: config.fix_clean,
            chunk: config.get_chunk_size(),
        })
    }

    async fn verify_chunk(&self, archived: ArchivesList, stat: Arc<Mutex<VerificationStat>>) -> anyhow::Result<()> {
        let mut jobs = JoinSet::new();

        for g in archived.iter() {
            for table in g.tables() {
                stat.lock().unwrap().on_processed(&table.kind, &g.range);
            }
        }

        // ----
        // 1. Apply verification that can be done on the filename level
        // ----

        let mut preprocess = Preprocess::new(archived);
        // first we need to leave only uniq files
        if self.delete_whole_groups {
            // we know that incomplete groups of files are always invalid
            let _ = select_complete(&mut preprocess)?;
        }
        let _ = remove_forks::<B>(&mut preprocess, self.archiver.data_provider.clone()).await?;
        let _ = deduplicate(&mut preprocess)?;

        // ----
        // 2. now we can execute on that data.
        // ----

        // group small ranges together to process them in one go
        let grouped = merge_small(preprocess.input);

        // first, delete files that are marked for deletion
        for delete in &preprocess.for_deletion {
            stat.lock().unwrap().on_delete(&delete.kind, &delete.range);
        }
        let target = self.archiver.target.clone();
        jobs.spawn(async move {
            delete(preprocess.for_deletion, target).await
        });

        process_data(
            grouped,
            self.archiver.clone(), self.data_options.clone(), self.delete_whole_groups,
            &mut jobs,
            stat
        );

        let shutdown = global::get_shutdown();
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

///
/// verify data in each group
fn process_data<B: BlockchainTypes + 'static, TS: TargetStorage + 'static>(
    ranges: Vec<(Range, Vec<ArchiveGroup>)>,
    archiver: Arc<Archiver<B, TS>>,
    data_options: DataOptions,
    delete_whole_chunk: bool,
    jobs: &mut JoinSet<anyhow::Result<()>>,
    stat: Arc<Mutex<VerificationStat>>,
) {
    let parallel = Arc::new(Semaphore::new(4));
    for (range, group) in ranges {
        let parallel = parallel.clone();
        let archiver = archiver.clone();
        let data_options = data_options.clone();
        let stat = stat.clone();
        jobs.spawn(async move {
            let _permit = parallel.acquire().await;
            verify_table_group(delete_whole_chunk, archiver, range, group, data_options, stat).await
        });
    }
}

///
/// Merge all individual files that cover small ranges into larger list of groups
/// A small range is 10 or fewer blocks.
fn merge_small(groups: Vec<ArchiveGroup>) -> Vec<(Range, Vec<ArchiveGroup>)> {
    let limit = 10;
    let mut small = RangeBag::new();
    let mut result = vec![];
    let mut left = vec![];
    for group in groups {
        if group.range.len() > limit {
            result.push((group.range.clone(), vec![group]));
        } else {
            small.append(group.range.clone());
            left.push(group);
        }
    }

    let small = small.compact();
    for range in small.ranges {
        // TODO makes sense to split into smaller ranges, but we cannot just split by chunks as we need to align with the existing files (i.e., we cannot split them into different groups)
        let mut in_range = vec![];
        for group in &left {
            if range.contains(&group.range) {
                in_range.push(group.clone());
            }
        }
        result.push((range.clone(), in_range));
    }

    result
}

///
/// Delete the list of files
/// Does nothing is dry-run mode is enabled
async fn delete<TS: TargetStorage + 'static>(files: Vec<FileReference>, target: Arc<TS>) -> anyhow::Result<()> {
    let dry_run = global::is_dry_run();
    if dry_run {
        tracing::info!("Dry run mode, no files will be deleted");
    }
    let shutdown = global::get_shutdown();
    let semaphore = Arc::new(Semaphore::new(4));
    for f in &files {
        tracing::debug!(range = %f.range, dry_run = %dry_run, "Deleting file: {}", f.path);
        if shutdown.is_signalled() {
            break;
        }
        let shutdown = shutdown.branch();
        let semaphore = semaphore.clone();
        let target = target.clone();
        let f = f.clone();
        tokio::spawn(async move {
            if shutdown.is_signalled() {
                return;
            }
            let _permit = semaphore.acquire().await;
            let _ = target.delete(&f).await;
        });
    }
    Ok(())
}

///
/// Leaves only groups files where all the files are present (as specified by DataOptions in the group)
/// Marks all other files for deletion
fn select_complete(data: &mut Preprocess) -> anyhow::Result<()> {
    let mut result = vec![];
    let input = data.inputs();

    for group in input {
        if group.is_complete() {
            result.push(group);
        } else {
            tracing::debug!(range = %group.range, "Delete incomplete group");
            data.delete_all(group);
        }
    }
    data.input = result;
    Ok(())
}

///
/// Cleans up individual heights (as made by Stream command) when a height contains multiple blocks (i.e., a fork happened at that height)
/// In those cases, it uses the block hash from the filename, and checks the blockchain to see which block is
/// the actual one and removes other files from the archive
async fn remove_forks<B: BlockchainTypes + 'static>(data: &mut Preprocess, data_provider: Arc<B::DataProvider>) -> anyhow::Result<()> {
    let mut result = vec![];
    let mut left = data.inputs();

    while !left.is_empty() {
        let group = left.remove(0);
        if group.range.len() > 1 {
            tracing::trace!(ranage = %group.range, "Large range, no fork checks");
            result.push(group);
            continue;
        }
        let height = group.range.start();
        let mut forks: Vec<ArchiveGroup> = left.extract_if(.., |g| g.range.start() == group.range.start())
            .collect();
        if forks.is_empty() {
            tracing::trace!("Only one block at height {}, no forks", height);
            result.push(group);
            continue;
        }
        forks.push(group);
        let (_, actual, _) = data_provider.fetch_block(&BlockReference::height(height)).await?;
        let expected_hash = actual.hash();
        for fork in forks {
            if let Range::Single(h) = &fork.range {
                if let Some(fork_hash) = h.hash.as_ref() {
                    if let Ok(fork_hash) = B::BlockHash::from_str(fork_hash.as_str()) {
                        if fork_hash.eq(&expected_hash) {
                            result.push(fork.clone());
                            continue
                        }
                    }
                }

                tracing::debug!(range = %fork.range, "Delete forked blocks");
                data.delete_all(fork);
            }
        }
    }
    data.input = result;

    Ok(())
}

///
/// Remove all intersected ranges, by keeping only the groups that covers the largest range
fn deduplicate(data: &mut Preprocess) -> anyhow::Result<()> {
    let mut result = vec![];

    let mut left = data.inputs();

    while !left.is_empty() {
        let group = left.remove(0);
        let all_groups_in_range: Vec<ArchiveGroup> = left.extract_if(.., |g| g.range.is_intersected_with(&group.range))
            .collect();
        let group = if all_groups_in_range.len() > 1 {
            // we got few files in the range
            // select ib that it the largest
            let best = all_groups_in_range.iter()
                .max_by_key(|g| g.range.len())
                .ok_or_else(|| anyhow::anyhow!("Empty group"))?;
            // delete the other files
            for other in &all_groups_in_range {
                if best.ne(other) {
                    tracing::debug!(range = %group.range, "Delete duplicate group");
                    data.delete_all(other.clone());
                }
            }
            best.clone()
        } else {
            group
        };
        result.push(group);
    }
    data.input = result;

    Ok(())
}

#[async_trait]
impl<B: BlockchainTypes + 'static, FR: TargetStorage + 'static> CommandExecutor for VerifyCommand<B, FR> {

    async fn execute(&self) -> anyhow::Result<()> {
        let full_range = self.blocks.to_range(self.archiver.data_provider.as_ref()).await?;
        tracing::info!(range = %full_range, "Verifying range");

        let ranges = full_range.split_chunks(self.chunk, false);
        let shutdown = global::get_shutdown();
        let dry_run = global::is_dry_run();
        let stat = Arc::new(Mutex::new(VerificationStat::new()));

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
                        let is_completed = archived.append(file);
                        match is_completed {
                            Err(e) => {
                                // error means there is something wrong with the file. ex. a duplicate table
                                match e {
                                    RangeGroupError::Duplicate(f1, f2) => {
                                        {
                                            let mut stat = stat.lock().unwrap();
                                            stat.on_delete(&f1.kind, &f1.range);
                                            stat.on_delete(&f2.kind, &f2.range);
                                        }
                                        if !dry_run {
                                            let _ = tokio::join!(
                                                self.archiver.target.delete(&f1),
                                                self.archiver.target.delete(&f2)
                                            );
                                        }
                                    },
                                    _ => {
                                    }
                                }
                            },
                            Ok(_) => {}
                        }
                    }
                }
            }

            self.verify_chunk(archived, stat.clone()).await?;
        }

        {
            let stat = stat.lock().unwrap();
            for (kind, table_stat) in &stat.tables {
                tracing::info!(
                    "Verification stats {}: processed = {} blocks, deleted = {} blocks",
                    kind, table_stat.processed, table_stat.deleted
                );
            }
        }

        Ok(())
    }
}

async fn verify_table_group<B: BlockchainTypes + , TS: TargetStorage + 'static>(
    delete_whole_chunk: bool,
    archiver: Arc<Archiver<B, TS>>,
    range: Range,
    groups: Vec<ArchiveGroup>,
    data_options: DataOptions,
    stat: Arc<Mutex<VerificationStat>>
) -> anyhow::Result<()> {
    let shutdown = global::get_shutdown();
    let mut for_deletion: Vec<&FileReference> = vec![];

    if let Ok(files) = verify_content(archiver.clone(), &range, &groups, data_options).await {
        for_deletion.extend(files);
    }

    if shutdown.is_signalled() {
        // when it's shutting down there (1) is not time for deletion and (2) most likely it got some invalid data as other threads are stopping
        return Ok(());
    }

    if !for_deletion.is_empty() && delete_whole_chunk {
        tracing::info!(range = %range, "Deleting all tables in the chunk due to --fix.clean");
        for_deletion = groups.iter().flat_map(|g| g.tables()).collect();
    }

    {
        let mut stat = stat.lock().unwrap();
        for file in &for_deletion {
            stat.on_delete(&file.kind, &file.range);
        }
    }

    delete(for_deletion.into_iter().cloned().collect(), archiver.target.clone()).await?;

    Ok(())
}

async fn verify_content<'a, B: BlockchainTypes, TS: TargetStorage>(archiver: Arc<Archiver<B, TS>>, range: &Range, groups: &'a Vec<ArchiveGroup>, data_options: DataOptions) -> anyhow::Result<Vec<&'a FileReference>> {
    tracing::trace!(range = %range, "Verify table data");
    let shutdown = global::get_shutdown();
    let mut broken_files = vec![];

    let blocks: Vec<&FileReference> = groups.iter()
        .filter_map(|g| g.blocks.as_ref())
        .collect();

    let block_verification = if blocks.len() > 0 && data_options.block.is_some() {
        BlockVerify::<B, TS>::new().verify_table(data_options.block.as_ref().unwrap(), archiver.target.as_ref(), &range, &blocks, archiver.data_provider.as_ref()).await
    } else {
        tracing::error!(range = %range, "No block file. Skip verification and delete other tables in the range");
        return Ok(groups.iter().flat_map(|g| g.tables()).collect())
    };

    if shutdown.is_signalled() {
        return Ok(vec![]);
    }
    match block_verification {
        Err(e) => {
            tracing::error!(range = %range, "Block data is corrupted: {:?}", e);
            if data_options.include_block() {
                broken_files.extend_from_slice(blocks.as_slice());
                if data_options.include_tx() || data_options.include_trace() {
                    // we cannot verify txes if blocks are corrupted
                    tracing::warn!(range = %range, "Cannot verify txes without a valid block");
                }
            }
        },
        Ok(expected_txes) => {
            if let Some(tx_options) = &data_options.tx {
                let txes: Vec<&FileReference> = groups.iter()
                    .filter_map(|g| g.txes.as_ref())
                    .collect();

                if !txes.is_empty() {
                    let ok = TxVerify::<B, TS>::new().verify_table(tx_options, archiver.target.as_ref(), &range, &txes, &expected_txes).await;
                    if ok.is_err() {
                        tracing::error!(range = %range, "Tx data is corrupted: {:?}", ok.err().unwrap());
                        broken_files.extend_from_slice(txes.as_slice());
                    }
                }
            }
            if let Some(trace_options) = &data_options.trace {
                let traces: Vec<&FileReference> = groups.iter()
                    .filter_map(|g| g.traces.as_ref())
                    .collect();

                if !traces.is_empty() {
                    let ok = TraceVerify::<B, TS>::new().verify_table(trace_options, archiver.target.as_ref(), &range, &traces, &expected_txes).await;
                    if ok.is_err() {
                        tracing::error!(range = %range, "Trace data is corrupted: {:?}", ok.err().unwrap());
                        broken_files.extend_from_slice(traces.as_slice());
                    }
                }
            }
        }
    }

    Ok(broken_files)
}

fn verify_field_exist(record: &Record, field: &str) -> Result<(), String> {
    let value = record.fields.iter()
        .find(|f| f.0 == field)
        .ok_or(format!("No {} in the record", field))?;
    match &value.1 {
        Value::Null => Err(format!("Null {} in the record", field)),
        Value::Bytes(v) => if v.is_empty() {
            Err(format!("Empty {} in the record", field))
        } else {
            Ok(())
        },
        Value::String(s) => if s.is_empty() {
            Err(format!("Empty {} in the record", field))
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

    async fn verify_table(&self, options: &T, storage: &TS, range: &Range, files: &Vec<&FileReference>, params: &Self::Params) -> Result<Self::Returns, String>;
}


struct TxVerify<B, TS> where B: BlockchainTypes, TS: TargetStorage {
    _b: PhantomData<B>,
    _ts: PhantomData<TS>,
}

impl<B: BlockchainTypes, TS: TargetStorage> TxVerify<B, TS> {
    pub fn new() -> Self {
        Self { _b: Default::default(), _ts: Default::default() }
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> VerifyTable<TxOptions, B, TS> for TxVerify<B, TS> {
    type Returns = ();
    type Params = Vec<B::TxId>;

    async fn verify_table(&self, _options: &TxOptions, storage: &TS, range: &Range, files: &Vec<&FileReference>, params: &Self::Params) -> Result<Self::Returns, String> {
        tracing::trace!(range = %range, "Verify txes");
        let shutdown = global::get_shutdown();
        let expected_txes = params;
        let mut existing_txes = HashSet::new();

        for file in files {
            tracing::debug!(range = %file.range, "Processing tx file: {}", file.path);
            let mut txes = storage.open(file)
                .await.map_err(|e| format!("Failed to open txes storage: {}", e))?
                .read().map_err(|e| format!("Failed to read txes storage: {}", e))?;
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
                            .ok_or(format!("No txid in the record"))?;
                        let txid_str = match &txid.1 {
                            Value::String(s) => s.clone(),
                            _ => return Err(format!("Invalid txid type: {:?}", txid.1))
                        };

                        let txid = B::TxId::from_str(&txid_str)
                            .map_err(|_| format!("Invalid txid: {}", txid_str))?;
                        if !expected_txes.contains(&txid) {
                            return Err(format!("Unexpected txid: {}", txid_str));
                        }

                        verify_field_exist(&record, "json")?;
                        verify_field_exist(&record, "raw")?;
                        //TODO blockchain specific verification

                        let first = existing_txes.insert(txid);
                        if !first {
                            return Err(format!("Duplicate txid: {}", txid_str));
                        }
                    }
                }
            }
        }

        if existing_txes.len() != expected_txes.len() {
            return Err(format!("Missing txes in the table"));
        }
        Ok(())
    }
}

struct TraceVerify<B, TS> where B: BlockchainTypes, TS: TargetStorage {
    _b: PhantomData<B>,
    _ts: PhantomData<TS>,
}

impl<B: BlockchainTypes, TS: TargetStorage> TraceVerify<B, TS> {
    pub fn new() -> Self {
        Self { _b: Default::default(), _ts: Default::default() }
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> VerifyTable<TraceOptions, B, TS> for TraceVerify<B, TS> {
    type Returns = ();
    type Params = Vec<B::TxId>;

    async fn verify_table(&self, options: &TraceOptions, storage: &TS, range: &Range, files: &Vec<&FileReference>, params: &Self::Params) -> Result<Self::Returns, String> {
        tracing::trace!(range = %range, "Verify traces");
        let shutdown = global::get_shutdown();
        let expected_txes = params;
        let mut existing_txes = HashSet::new();

        for file in files {
            tracing::debug!(range = %file.range, "Processing trace file: {}", file.path);
            let mut traces = storage.open(file)
                .await.map_err(|e| format!("Failed to open traces storage: {}", e))?
                .read().map_err(|e| format!("Failed to read traces storage: {}", e))?;

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
                            .ok_or("No txid in the record".to_string())?;
                        let txid_str = match &txid.1 {
                            Value::String(s) => s.clone(),
                            _ => return Err(format!("Invalid txid type: {:?}", txid.1))
                        };

                        let txid = B::TxId::from_str(&txid_str)
                            .map_err(|_| format!("Invalid txid: {}", txid_str))?;
                        if !expected_txes.contains(&txid) {
                            return Err(format!("Unexpected txid: {}", txid_str));
                        }

                        if options.include_trace {
                            verify_field_exist(&record, "traceJson")?;
                        }
                        if options.include_state_diff {
                            verify_field_exist(&record, "stateDiffJson")?;
                        }

                        let first = existing_txes.insert(txid);
                        if !first {
                            return Err(format!("Duplicate txid: {}", txid_str));
                        }
                    }
                }
            }
        }

        if existing_txes.len() != expected_txes.len() {
            return Err("Missing txes in the table".to_string());
        }
        Ok(())
    }
}

struct BlockVerify<B, TS> where B: BlockchainTypes, TS: TargetStorage {
    _b: PhantomData<B>,
    _ts: PhantomData<TS>,
}

impl<B: BlockchainTypes, TS: TargetStorage> BlockVerify<B, TS> {
    pub fn new() -> Self {
        Self { _b: Default::default(), _ts: Default::default() }
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> VerifyTable<BlockOptions, B, TS> for BlockVerify<B, TS> {
    type Returns = Vec<B::TxId>;
    type Params = B::DataProvider;

    async fn verify_table(&self, _options: &BlockOptions, storage: &TS, range: &Range, files: &Vec<&FileReference>, data_provider: &Self::Params) -> Result<Self::Returns, String> {
        tracing::trace!(range = %range, "Verify blocks");

        let mut block_seq: BlockSequence<B> = BlockSequence::new(range.len());
        let mut heights = HashSet::new();
        let mut expected_txes = Vec::new();
        let shutdown = global::get_shutdown();

        for file in files {
            tracing::debug!(range = %file.range, "Processing block file: {}", file.path);
            let mut blocks = storage.open(file)
                .await.map_err(|e| format!("Failed to open blocks storage: {}", e))?
                .read().map_err(|e| format!("Failed to read blocks storage: {}", e))?;

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

                        let height = avros::get_height(&record).map_err(|e| format!("Failed to get height: {}", e))?;
                        if !range.contains(&Range::Single(height.into())) {
                            tracing::error!(range = %range, "Height is not in range: {}", height);
                            return Err(format!("Height is not in range: {}", height));
                        }
                        let first = heights.insert(height);
                        if !first {
                            tracing::error!(range = %range, "Duplicate height: {}", height);
                            return Err(format!("Duplicate height: {}", height));
                        }

                        let json = record.fields.iter()
                            .find(|f| f.0 == "json");
                        if json.is_none() {
                            tracing::error!(range = %range, "No json in the record");
                            return Err("No json in the record".to_string());
                        }
                        let json = json.unwrap();
                        let json = match &json.1 {
                            Value::Bytes(b) => b.clone(),
                            _ => {
                                tracing::error!(range = %range, "Invalid json type: {:?}", json.1);
                                return Err(format!("Invalid json type: {:?}", json.1));
                            }
                        };
                        let block = serde_json::from_slice::<B::BlockParsed>(json.as_slice());
                        if block.is_err() {
                            tracing::error!(range = %range, "Invalid json data: {:?}", block.err().unwrap());
                            return Err("Invalid json data".to_string());
                        }
                        let block = block.unwrap();
                        block_seq.append(height, block.parent(), block.hash());
                        expected_txes.extend(block.txes());
                    },
                }
            }
            if shutdown.is_signalled() {
                break;
            }
        }

        if heights.len() != range.len() {
            let mut heights_range = RangeBag::new();
            for h in &heights {
                heights_range.append(Range::Single((*h).into()));
            }
            let heights_range = heights_range.compact();
            tracing::error!(range = %range, "Missing blocks in the table. actual={}", heights_range);
            return Err("Missing blocks in the table".to_string());
        }

        {
            let top_block = block_seq.get_head();
            if top_block.is_none() {
                tracing::error!(range = %range, "No blocks in the table");
                return Err("No blocks in the table".to_string());
            }
            let top_block = top_block.unwrap();
            let chain = block_seq.up_to(top_block.0, top_block.1);
            let chain_range = Range::new(
                chain.first().unwrap().0,
                chain.last().unwrap().0
            );

            // not using Eq because the ranges may contain hash info, or may be None
            if chain_range.start() != range.start() || chain_range.end() != range.end() {
                tracing::error!(range = %range, "Table range is not consistent for blocks. Expected range: {}, actual range: {}", range, chain_range);
                return Err("Table range is not consistent for blocks".to_string());
            }

            let (_, actual, _) = data_provider.fetch_block(&BlockReference::height(range.end())).await
                .map_err(|e| format!("Failed to fetch block from blockchain: {}", e))?;
            if !actual.hash().eq(&top_block.1) {
                tracing::error!(range = %range, "Top block hash does not match blockchain: expected {:?}, in archive {:?}", actual.hash(), top_block.1);
                return Err("Top block hash does not match blockchain".to_string());
            }
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
    use crate::command::verify::{merge_small, VerifyCommand};
    use crate::archiver::filenames::Filenames;
    use crate::storage::objects::ObjectsStorage;
    use futures_util::StreamExt;
    use crate::blockchain::{BlockReference, BlockchainData};
    use crate::archiver::datakind::{DataKind, DataTables};
    use crate::archiver::range::Range;
    use crate::archiver::range_group::ArchiveGroup;
    use crate::storage::{TargetFileWriter, TargetStorage};
    use crate::testing;

    fn create_archiver(mem: Arc<InMemory>) -> Archiver<MockType, ObjectsStorage<InMemory>> {
        let storage = ObjectsStorage::new(mem, "test".to_string(), Filenames::with_dir("archive/eth".to_string()));

        Archiver::new_simple(
            Arc::new(storage),
            Arc::new(MockData::new("test")),
        )
    }

    fn create_archiver_with_data(mem: Arc<InMemory>, data: Arc<MockData>) -> Archiver<MockType, ObjectsStorage<InMemory>> {
        let storage = ObjectsStorage::new(mem, "test".to_string(), Filenames::with_dir("archive/eth".to_string()));

        Archiver::new_simple(
            Arc::new(storage),
            data,
        )
    }

    #[tokio::test(flavor = "multi_thread")]
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

    #[tokio::test(flavor = "multi_thread")]
    async fn does_nothing_with_full_group() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());

        let block101 = MockBlock {
            height: 101,
            hash: "B101".to_string(),
            parent: "B100".to_string(),
            transactions: vec!["TX001".to_string()],
        };
        let data = Arc::new(MockData::new("TEST"));
        data.add_block(block101.clone());
        data.add_tx(MockTx {
            hash: "TX001".to_string(),
        });

        let archiver = create_archiver_with_data(mem.clone(), data.clone());

        let write = archiver.target.create(DataKind::Blocks, &Range::Single(101.into())).await.unwrap();
        let record = data.fetch_block(&BlockReference::Height(101.into())).await.unwrap();
        write.append(record.0).await.unwrap();
        write.close().await.unwrap();

        let write = archiver.target.create(DataKind::Transactions, &Range::Single(101.into())).await.unwrap();
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

    #[tokio::test(flavor = "multi_thread")]
    async fn deletes_incomplete_group() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());

        let block101 = MockBlock {
            height: 101,
            hash: "B101".to_string(),
            parent: "B100".to_string(),
            transactions: vec!["TX001".to_string()],
        };
        let block102 = MockBlock {
            height: 102,
            hash: "B102".to_string(),
            parent: "B101".to_string(),
            transactions: vec!["TX002".to_string()],
        };
        let block103 = MockBlock {
            height: 103,
            hash: "B103".to_string(),
            parent: "B102".to_string(),
            transactions: vec!["TX003".to_string()],
        };
        let data = Arc::new(MockData::new("TEST"));
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

        let archiver = create_archiver_with_data(mem.clone(), data.clone());

        // should have:
        // block 101 + txes 101
        // txes 102
        // block 103

        let write = archiver.target.create(DataKind::Blocks, &Range::Single(101.into())).await.unwrap();
        let record = data.fetch_block(&BlockReference::Height(101.into())).await.unwrap();
        write.append(record.0).await.unwrap();
        write.close().await.unwrap();

        let write = archiver.target.create(DataKind::Blocks, &Range::Single(103.into())).await.unwrap();
        let record = data.fetch_block(&BlockReference::Height(103.into())).await.unwrap();
        write.append(record.0).await.unwrap();
        write.close().await.unwrap();

        let write = archiver.target.create(DataKind::Transactions, &Range::Single(101.into())).await.unwrap();
        let record = data.fetch_tx(&block101, 0).await.unwrap();
        write.append(record).await.unwrap();
        write.close().await.unwrap();

        let write = archiver.target.create(DataKind::Transactions, &Range::Single(102.into())).await.unwrap();
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

    #[tokio::test(flavor = "multi_thread")]
    async fn deletes_empty_block() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let data = Arc::new(MockData::new("TEST"));
        let archiver = create_archiver_with_data(mem.clone(), data.clone());

        let block100 = MockBlock {
            height: 100,
            hash: "B100".to_string(),
            parent: "B099".to_string(),
            transactions: vec!["TX001".to_string()],
        };
        data.add_block(block100.clone());
        data.add_tx(MockTx {
            hash: "TX001".to_string(),
        });

        let blocks = archiver.target
            .create(DataKind::Blocks, &Range::Single(100.into()))
            .await.expect("Create block");
        // no date written
        blocks.close().await.unwrap();

        let txes = archiver.target
            .create(DataKind::Transactions, &Range::Single(100.into()))
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

    #[tokio::test(flavor = "multi_thread")]
    async fn deletes_missing_tx() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let archiver = create_archiver(mem.clone());
        let data = archiver.data_provider.clone();

        let block100 = MockBlock {
            height: 100,
            hash: "B100".to_string(),
            parent: "B099".to_string(),
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

    #[test]
    fn test_merge_small_empty() {
        let groups = vec![];
        let result = merge_small(groups);
        assert!(result.is_empty());
    }

    #[test]
    fn test_merge_small_all_large() {
        let groups = vec![
            ArchiveGroup::new(Range::new(0, 20), DataTables::default()),
            ArchiveGroup::new(Range::new(21, 41), DataTables::default()),
        ];
        let result = merge_small(groups.clone());
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1[0].range, groups[0].range);
        assert_eq!(result[1].1[0].range, groups[1].range);
    }

    #[test]
    fn test_merge_small_all_small() {
        let groups = vec![
            ArchiveGroup::new(Range::single(5), DataTables::default()),
            ArchiveGroup::new(Range::new(0, 4), DataTables::default()),
            ArchiveGroup::new(Range::new(6, 10), DataTables::default()),

        ];
        let result = merge_small(groups.clone());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, Range::new(0, 10));
        assert_eq!(result[0].1.len(), 3);
    }

    #[test]
    fn test_merge_small_mixed() {
        let large = ArchiveGroup::new(Range::new(0, 20), DataTables::default());
        let small1 = ArchiveGroup::new(Range::new(21, 25), DataTables::default());
        let small2 = ArchiveGroup::new(Range::single(26), DataTables::default());
        let groups = vec![large.clone(), small1.clone(), small2.clone()];
        let result = merge_small(groups.clone());

        assert_eq!(result.len(), 2);

        let all_large: Vec<&(Range, Vec<ArchiveGroup>)> = result.iter().filter(|(r, _)| r.len() > 10).collect();
        let all_small: Vec<&(Range, Vec<ArchiveGroup>)> = result.iter().filter(|(r, _)| r.len() <= 10).collect();

        assert_eq!(all_large[0].0, Range::new(0, 20));
        assert_eq!(all_large[0].1.len(), 1);

        assert_eq!(all_small[0].0, Range::new(21, 26));
        assert_eq!(all_small[0].1.len(), 2);

    }
}
