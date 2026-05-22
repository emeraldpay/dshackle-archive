use std::path::PathBuf;
use std::sync::Arc;
use apache_avro::types::Record;
use async_trait::async_trait;
use crate::{
    storage::{
        fs::FsStorage,
        json_fs::JsonFsStorage,
        json_objects::JsonObjectsStorage,
        objects::ObjectsStorage
    },
    archiver::{
        range_bag::RangeBag,
        range::Range,
        filenames::Filenames,
        datakind::{DataKind, DataOptions},
        range_group::ArchivesList
    },
    args::Args,
    global,
    record::ArchiveRow,
};
use anyhow::{anyhow, Result};
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    ClientOptions
};
use tokio::sync::mpsc::Receiver;
use url::Url;
use itertools::Itertools;

pub mod fs;
pub mod json_fs;
pub mod json_objects;
pub mod objects;
pub mod pulsar;
mod avro_reader;
mod copy;
mod sorted_files;

pub fn is_s3(args: &Args) -> bool {
    args.aws.is_some()
}

pub fn is_fs(args: &Args) -> bool {
    args.dir.is_some() && !is_s3(args)
}

/// True when the user selected a streaming target whose URL is a Pulsar URL
/// (`pulsar://…`). Mutually exclusive with the file targets: when this is true
/// the storage layer ignores `--dir` and `--auth.aws.*` and routes records to
/// per-field topics instead.
pub fn is_pulsar(args: &Args) -> bool {
    args.stream
        .as_ref()
        .map(|s| s.is_pulsar())
        .unwrap_or(false)
}

/// Build a [`pulsar::PulsarStorage`] from the user-supplied `--stream.*` args.
///
/// Pre-creates one producer per [`crate::formats::stream::FieldLabel`] so the
/// first append doesn't pay startup cost. `--stream.topics` is taken
/// verbatim — callers are expected to include the Pulsar topic path up to and
/// including the blockchain segment (e.g.
/// `persistent://public/default/archive-eth`).
pub async fn create_pulsar(value: &Args) -> Result<pulsar::PulsarStorage> {
    let stream = value
        .stream
        .as_ref()
        .ok_or_else(|| anyhow!("No --stream.* options set"))?;
    let url = stream
        .stream_url
        .clone()
        .ok_or_else(|| anyhow!("--stream.url is required for a streaming target"))?;
    let prefix = stream
        .stream_topics
        .clone()
        .ok_or_else(|| anyhow!("--stream.topics is required for a streaming target"))?;
    tracing::info!("Using Pulsar streaming target at {} with topic prefix {}", url, prefix);
    pulsar::PulsarStorage::new(url, prefix).await
}

///
/// Compute the S3 key prefix (without bucket) under which the archive lives.
///
/// Joins the user-supplied `--dir` URL path with the lowercased blockchain code
/// (e.g. `eth`), trimming any leading/trailing slashes on the URL path so
/// `s3://bucket/`, `s3://bucket/archive`, and `s3://bucket/archive/` all
/// produce the expected `archive/eth` (or just `eth` for the bucket root).
fn s3_parent_dir(value: &Args) -> Result<String> {
    let blockchain_dir = value.get_blockchain()?.code().to_lowercase();
    if value.dir.is_none() {
        return Err(anyhow!("Please set target dir as a s3://bucket/path"));
    }
    let raw = url::Url::parse(value.dir.as_ref().unwrap())
        .map_err(|_| anyhow!("Please specify a --dir as s3://bucket/path"))?
        .path()
        .to_string();
    let trimmed = raw.trim_matches('/');
    Ok(if trimmed.is_empty() {
        blockchain_dir
    } else {
        format!("{}/{}", trimmed, blockchain_dir)
    })
}

pub fn create_aws(value: &Args) -> Result<ObjectsStorage<AmazonS3>> {
    let aws = value.aws.as_ref().unwrap();

    tracing::info!("Using S3 storage");

    // s3_parent_dir errors if --dir is missing, so subsequent unwraps are safe.
    let filenames = Filenames::with_dir(s3_parent_dir(value)?);

    let mut builder = AmazonS3Builder::new()
        .with_access_key_id(aws.access_key.clone())
        .with_secret_access_key(aws.secret_key.clone())
        .with_url(value.dir.clone().unwrap())
        .with_client_options(
            ClientOptions::default()
                .with_allow_http(true)
                .with_allow_invalid_certificates(aws.trust_tls)
        );
    if let Some(endpoint) = &aws.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    } else {
        builder = builder.with_endpoint("https://s3.amazonaws.com".to_string());
    }

    if let Some(region) = &aws.region {
        builder = builder.with_region(region.clone());
    }

    builder = builder
        .with_virtual_hosted_style_request(!aws.path_style);

    let bucket = {
        // AWS builder doesn't give access to the bucket name, so this is copied from its source code
        let parsed = Url::parse(value.dir.clone().unwrap().as_str())?;
        parsed.host_str().unwrap().to_string()
    };

    let os = builder.build()
        .map_err(|e| anyhow!("Invalid S3 connection options: {}", e))?;


    Ok(ObjectsStorage::new(Arc::new(os), bucket, filenames))
}

pub fn create_fs(value: &Args) -> Result<FsStorage> {
    let blockchain_dir = value.get_blockchain()?.code().to_lowercase();
    tracing::info!("Using Filesystem storage");
    if value.dir.is_none() {
        return Err(anyhow!("No target dir set for a Filesystem based storage"));
    }
    let dir = value.dir.as_ref().unwrap().clone();
    Ok(FsStorage::new(PathBuf::from(dir), Filenames::with_dir(blockchain_dir)))
}

/// Build a [`JsonFsStorage`] using the same root-dir and blockchain-subdirectory
/// conventions as [`create_fs`].
pub fn create_fs_json(value: &Args) -> Result<JsonFsStorage> {
    let blockchain_dir = value.get_blockchain()?.code().to_lowercase();
    tracing::info!("Using Filesystem storage (JSON per-field layout)");
    if value.dir.is_none() {
        return Err(anyhow!("No target dir set for a Filesystem based storage"));
    }
    let dir = value.dir.as_ref().unwrap().clone();
    Ok(JsonFsStorage::new(
        PathBuf::from(dir),
        Filenames::with_dir(blockchain_dir),
    ))
}

/// Build a [`JsonObjectsStorage`] backed by AWS S3 (or any S3-compatible service),
/// sharing the same authentication and URL-parsing logic as [`create_aws`].
pub fn create_aws_json(value: &Args) -> Result<JsonObjectsStorage<AmazonS3>> {
    let aws = value.aws.as_ref().unwrap();

    tracing::info!("Using S3 storage (JSON per-field layout)");

    // s3_parent_dir errors if --dir is missing, so subsequent unwraps are safe.
    let filenames = Filenames::with_dir(s3_parent_dir(value)?);

    let mut builder = AmazonS3Builder::new()
        .with_access_key_id(aws.access_key.clone())
        .with_secret_access_key(aws.secret_key.clone())
        .with_url(value.dir.clone().unwrap())
        .with_client_options(
            ClientOptions::default()
                .with_allow_http(true)
                .with_allow_invalid_certificates(aws.trust_tls),
        );
    if let Some(endpoint) = &aws.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    } else {
        builder = builder.with_endpoint("https://s3.amazonaws.com".to_string());
    }
    if let Some(region) = &aws.region {
        builder = builder.with_region(region.clone());
    }
    builder = builder.with_virtual_hosted_style_request(!aws.path_style);

    let bucket = {
        let parsed = Url::parse(value.dir.clone().unwrap().as_str())?;
        parsed.host_str().unwrap().to_string()
    };
    let os = builder
        .build()
        .map_err(|e| anyhow!("Invalid S3 connection options: {}", e))?;

    Ok(JsonObjectsStorage::new(Arc::new(os), bucket, filenames))
}

///
/// Capability trait for targets that can have new records appended.
///
/// Every target (Avro/JSON files, future Pulsar/Kafka topics) implements this.
/// The `archive` command requires only this capability.
#[async_trait]
pub trait WriteTarget: Send + Sync {
    ///
    /// A type used for writing new records.
    type Writer: TargetFileWriter + Send + Sync + 'static;

    ///
    /// Create a new file/session (or overwrite an existing one).
    ///
    /// Returns `None` when `overwrite` is false and the destination already exists.
    async fn create(&self, kind: DataKind, range: &Range, overwrite: bool) -> Result<Option<Self::Writer>>;
}

///
/// Capability trait for targets that can report which heights/kinds are not
/// yet fully archived. Required by the `stream --continue` and `fix` commands.
///
/// Both file-based targets (Avro, JSON) implement this; streaming targets
/// (Pulsar, Kafka) will get a separate resume mechanism in Phase 3 and
/// intentionally do not implement [`ScanTarget`].
#[async_trait]
pub trait ScanTarget: WriteTarget {
    async fn find_incomplete_tables(
        &self,
        blocks: Range,
        tx_options: &DataOptions,
    ) -> Result<Vec<(Range, Vec<DataKind>)>>;
}

///
/// Capability trait for targets whose existing data can be enumerated, opened,
/// and deleted file-by-file. Required by the `verify` and `compact` commands.
///
/// Implemented by the row-batched file backends (Avro on FS/S3, future Parquet)
/// but **not** by the per-field JSON layout (which has no row-batched files to
/// open or merge) or by streaming targets.
#[async_trait]
pub trait ReadTarget: ScanTarget {
    ///
    /// A type used for reading the existing records.
    type Reader: TargetFileReader + Send + Sync;

    ///
    /// Delete the file
    async fn delete(&self, path: &FileReference) -> Result<()>;

    ///
    /// Open the file for reading
    async fn open(&self, path: &FileReference) -> Result<Self::Reader>;

    ///
    /// List all files in the range
    fn list(&self, range: Range) -> Result<Receiver<FileReference>>;
}

///
/// Default [`ScanTarget::find_incomplete_tables`] implementation for row-batched
/// file-based targets — uses [`ReadTarget::list`] to enumerate existing files and
/// computes the missing kinds via [`ArchivesList`]. Called by [`fs::FsStorage`]
/// and [`objects::ObjectsStorage`]; per-field JSON backends override
/// `find_incomplete_tables` with their own heuristic and don't go through here.
pub async fn find_incomplete_by_listing<T>(
    target: &T,
    blocks: Range,
    tx_options: &DataOptions,
) -> Result<Vec<(Range, Vec<DataKind>)>>
where
    T: ReadTarget,
{
    tracing::info!("Check if blocks are fully archived in range: {}", blocks);
    let mut existing = target.list(blocks.clone())?;
    let mut archived = ArchivesList::new(tx_options.files().clone());

    // Track which ranges have no files at all
    let mut missing_ranges = RangeBag::new();
    missing_ranges.append(blocks.clone());

    let shutdown = global::get_shutdown();
    while let Some(file) = existing.recv().await {
        // Remove this file's range from the missing ranges
        missing_ranges.remove(&file.range);
        archived.append(file)?;
        if shutdown.is_signalled() {
            tracing::info!("Shutdown signal received");
            return Ok(vec![]);
        }
    }

    let incomplete = archived.list_incomplete();
    let mut result: Vec<(Range, Vec<DataKind>)> = Vec::new();

    // Add ranges that have incomplete files (some files present but not all)
    if !incomplete.is_empty() {
        // We do not process ranges here (like joining them) because we don't want to break any existing layout (i.e, chunk size, alignment, etc)
        let ranges = incomplete.iter()
            .map(|g| g.range.clone())
            .collect::<Vec<Range>>();

        tracing::debug!("Incomplete blocks {} (sample: {},...)",
            ranges.len(),
            ranges.iter().take(5).map(|r| r.to_string()).join(",")
        );

        let incomplete_by_range: Vec<(Range, Vec<DataKind>)> = ranges.iter()
            .filter_map(|r| {
                // we clone all file groups here b/c if it split by different ranges each range would download its part
                incomplete.iter()
                    .find(|g| g.range.eq(r))
                    .map(|g| (r.clone(), g.get_incomplete_kinds()))
            })
            .collect();
        result.extend(incomplete_by_range);
    }

    // Add completely missing ranges (no files at all)
    if !missing_ranges.is_empty() {
        let all_kinds = tx_options.files().files.clone();
        tracing::debug!("Missing blocks (no files at all) {} (sample: {},...)",
            missing_ranges.len(),
            missing_ranges.ranges.iter().take(5).map(|r| r.to_string()).join(",")
        );

        for range in missing_ranges.ranges {
            result.push((range, all_kinds.clone()));
        }
    }

    if result.is_empty() {
        tracing::info!("All blocks are fully archived");
    }

    Ok(result)
}

pub trait TargetFile {
    fn get_url(&self) -> String;
}

#[async_trait]
pub trait TargetFileWriter: TargetFile {
    ///
    /// Append a new row to the file/session.
    ///
    /// The row is format-neutral; backends encode it to their on-the-wire
    /// representation internally (Avro Record, JSON bytes, Kafka message, ...).
    async fn append(&self, row: ArchiveRow) -> Result<()>;

    ///
    /// Append a pre-encoded Avro [`Record`] without going through [`ArchiveRow`].
    ///
    /// Used by the `compact` command, which reads existing Avro files and copies
    /// records into new range files. Going through [`ArchiveRow`] would require
    /// decoding then re-encoding every record; this method short-circuits that.
    ///
    /// File-based Avro backends override this; future non-Avro backends (JSON
    /// files, Pulsar, Kafka) keep the default `Err` since they have no `Record`
    /// concept. Such backends are not [`ReadTarget`]s, so `compact` cannot run
    /// against them anyway.
    async fn append_avro_record(&self, _data: Record<'_>) -> Result<()> {
        Err(anyhow!(
            "append_avro_record is not supported by this target"
        ))
    }

    ///
    /// MUST BE called if everything is written ok. Otherwise, the file is deleted on Drop.
    async fn close(mut self: Self) -> Result<()>;
}

pub trait TargetFileReader: TargetFile {
    ///
    /// Read the content from the file
    fn read(self) -> Result<Receiver<Record<'static>>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileReference {
    pub path: String,
    pub kind: DataKind,
    pub range: Range,
}

impl FileReference {
    pub fn new<S: ToString>(path: S, kind: DataKind, range: Range) -> Self {
        Self {
            path: path.to_string(),
            kind,
            range,
        }
    }
}

impl Ord for FileReference {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.range.start().cmp(&other.range.start())
    }
}

impl PartialOrd for FileReference {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::archiver::datakind::{DataKind, DataOptions};
    use crate::archiver::filenames::Filenames;
    use crate::archiver::range::Range;
    use crate::storage::objects::ObjectsStorage;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    fn args_with_dir(dir: &str) -> Args {
        Args {
            blockchain: "ethereum".to_string(),
            dir: Some(dir.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn s3_parent_dir_trims_trailing_slash() {
        assert_eq!(
            s3_parent_dir(&args_with_dir("s3://bucket/archive/")).unwrap(),
            "archive/eth"
        );
    }

    #[test]
    fn s3_parent_dir_keeps_prefix_without_trailing_slash() {
        assert_eq!(
            s3_parent_dir(&args_with_dir("s3://bucket/archive")).unwrap(),
            "archive/eth"
        );
    }

    #[test]
    fn s3_parent_dir_handles_nested_prefix() {
        assert_eq!(
            s3_parent_dir(&args_with_dir("s3://bucket/foo/bar/")).unwrap(),
            "foo/bar/eth"
        );
    }

    #[test]
    fn s3_parent_dir_bucket_root() {
        assert_eq!(
            s3_parent_dir(&args_with_dir("s3://bucket/")).unwrap(),
            "eth"
        );
        assert_eq!(
            s3_parent_dir(&args_with_dir("s3://bucket")).unwrap(),
            "eth"
        );
    }

    #[test]
    fn s3_parent_dir_errors_without_dir() {
        let args = Args {
            blockchain: "ethereum".to_string(),
            dir: None,
            ..Default::default()
        };
        assert!(s3_parent_dir(&args).is_err());
    }

    /// Helper to create a test storage with specified files
    async fn create_test_storage(files: Vec<&str>) -> ObjectsStorage<InMemory> {
        let mem = Arc::new(InMemory::new());

        for path in files {
            mem.put(
                &Path::from(path),
                PutPayload::from_static(&[1]),
            ).await.unwrap();
        }

        ObjectsStorage::new(
            mem.clone(),
            "test".to_string(),
            Filenames::with_dir("archive/eth".to_string())
        )
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_all_complete() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/021596000/021596362.block.avro",
            "archive/eth/021000000/021596000/021596362.txes.avro",
            "archive/eth/021000000/021596000/021596363.block.avro",
            "archive/eth/021000000/021596000/021596363.txes.avro",
        ]).await;

        let range = Range::new(21596362, 21596363);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        assert_eq!(incomplete.len(), 0);
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_missing_blocks() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/021596000/021596362.txes.avro",
            "archive/eth/021000000/021596000/021596363.block.avro",
            "archive/eth/021000000/021596000/021596363.txes.avro",
        ]).await;

        let range = Range::new(21596362, 21596363);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::Single(21596362.into()));
        assert_eq!(incomplete[0].1, vec![DataKind::Blocks]);
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_missing_txes() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/021596000/021596362.block.avro",
            "archive/eth/021000000/021596000/021596363.block.avro",
            "archive/eth/021000000/021596000/021596363.txes.avro",
        ]).await;

        let range = Range::new(21596362, 21596363);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::Single(21596362.into()));
        assert_eq!(incomplete[0].1, vec![DataKind::Transactions]);
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_missing_both() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/021596000/021596363.block.avro",
            "archive/eth/021000000/021596000/021596363.txes.avro",
        ]).await;

        let range = Range::new(21596362, 21596363);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        // 21596362 has no files at all, so it's reported as completely missing
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::Single(21596362.into()));
        assert!(incomplete[0].1.contains(&DataKind::Blocks));
        assert!(incomplete[0].1.contains(&DataKind::Transactions));
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_multiple_ranges() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/021596000/021596362.block.avro",
            // missing 021596362.txes.avro
            "archive/eth/021000000/021596000/021596363.block.avro",
            "archive/eth/021000000/021596000/021596363.txes.avro",
            // missing 021596364.block.avro
            "archive/eth/021000000/021596000/021596364.txes.avro",
        ]).await;

        let range = Range::new(21596362, 21596364);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        assert_eq!(incomplete.len(), 2);

        // Sort for consistent testing
        let mut sorted_incomplete = incomplete.clone();
        sorted_incomplete.sort_by_key(|(r, _)| r.start());

        assert_eq!(sorted_incomplete[0].0, Range::Single(21596362.into()));
        assert_eq!(sorted_incomplete[0].1, vec![DataKind::Transactions]);

        assert_eq!(sorted_incomplete[1].0, Range::Single(21596364.into()));
        assert_eq!(sorted_incomplete[1].1, vec![DataKind::Blocks]);
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_with_traces_complete() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/021596000/021596362.block.avro",
            "archive/eth/021000000/021596000/021596362.txes.avro",
            "archive/eth/021000000/021596000/021596362.traces.avro",
        ]).await;

        let range = Range::new(21596362, 21596362);
        let tx_options = DataOptions {
            overwrite: true,
            block: Some(Default::default()),
            tx: Some(Default::default()),
            trace: Some(Default::default()),
        };

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        assert_eq!(incomplete.len(), 0);
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_with_traces_missing_traces() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/021596000/021596362.block.avro",
            "archive/eth/021000000/021596000/021596362.txes.avro",
            // missing traces
        ]).await;

        let range = Range::new(21596362, 21596362);
        let tx_options = DataOptions {
            overwrite: true,
            block: Some(Default::default()),
            tx: Some(Default::default()),
            trace: Some(Default::default()),
        };

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::Single(21596362.into()));
        assert_eq!(incomplete[0].1, vec![DataKind::TransactionTraces]);
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_only_blocks_requested() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/021596000/021596362.block.avro",
            // missing txes, but not requested
            "archive/eth/021000000/021596000/021596363.block.avro",
        ]).await;

        let range = Range::new(21596362, 21596363);
        let tx_options = DataOptions {
            overwrite: true,
            block: Some(Default::default()),
            tx: None,
            trace: None,
        };

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        assert_eq!(incomplete.len(), 0);
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_empty_range() {
        let storage = create_test_storage(vec![]).await;

        let range = Range::new(21596362, 21596363);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        // When there are no files at all, the entire range is reported as missing
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::new(21596362, 21596363));
        assert!(incomplete[0].1.contains(&DataKind::Blocks));
        assert!(incomplete[0].1.contains(&DataKind::Transactions));
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_with_range_files() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/range-021596000_021596999.blocks.avro",
            "archive/eth/021000000/range-021596000_021596999.txes.avro",
        ]).await;

        let range = Range::new(21596000, 21596999);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        assert_eq!(incomplete.len(), 0);
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_range_files_missing_txes() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/range-021596000_021596999.blocks.avro",
            // missing range txes
        ]).await;

        let range = Range::new(21596000, 21596999);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::new(21596000, 21596999));
        assert_eq!(incomplete[0].1, vec![DataKind::Transactions]);
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_mixed_single_and_range() {
        let storage = create_test_storage(vec![
            // Range files for some blocks
            "archive/eth/021000000/range-021596000_021596099.blocks.avro",
            "archive/eth/021000000/range-021596000_021596099.txes.avro",
            // Individual files for others
            "archive/eth/021000000/021596000/021596100.block.avro",
            "archive/eth/021000000/021596000/021596100.txes.avro",
            // Missing 021596101
        ]).await;

        let range = Range::new(21596000, 21596101);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        // 21596101 is completely missing, so it's reported
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::Single(21596101.into()));
        assert!(incomplete[0].1.contains(&DataKind::Blocks));
        assert!(incomplete[0].1.contains(&DataKind::Transactions));
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_gap_in_middle() {
        let storage = create_test_storage(vec![
            // Files at start
            "archive/eth/021000000/021596000/021596362.block.avro",
            "archive/eth/021000000/021596000/021596362.txes.avro",
            "archive/eth/021000000/021596000/021596363.block.avro",
            "archive/eth/021000000/021596000/021596363.txes.avro",
            // Gap: 021596364-021596366 missing
            // Files at end
            "archive/eth/021000000/021596000/021596367.block.avro",
            "archive/eth/021000000/021596000/021596367.txes.avro",
        ]).await;

        let range = Range::new(21596362, 21596367);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        // The gap should be reported as a missing range
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::new(21596364, 21596366));
        assert!(incomplete[0].1.contains(&DataKind::Blocks));
        assert!(incomplete[0].1.contains(&DataKind::Transactions));
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_mixed_incomplete_and_missing() {
        let storage = create_test_storage(vec![
            // Complete files
            "archive/eth/021000000/021596000/021596362.block.avro",
            "archive/eth/021000000/021596000/021596362.txes.avro",
            // Incomplete: missing txes
            "archive/eth/021000000/021596000/021596363.block.avro",
            // Completely missing: 021596364
            // Complete files again
            "archive/eth/021000000/021596000/021596365.block.avro",
            "archive/eth/021000000/021596000/021596365.txes.avro",
        ]).await;

        let range = Range::new(21596362, 21596365);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        // Should report both incomplete (363) and missing (364)
        assert_eq!(incomplete.len(), 2);

        // Sort for consistent testing
        let mut sorted = incomplete.clone();
        sorted.sort_by_key(|(r, _)| r.start());

        // 21596363 is incomplete (missing txes)
        assert_eq!(sorted[0].0, Range::Single(21596363.into()));
        assert_eq!(sorted[0].1, vec![DataKind::Transactions]);

        // 21596364 is completely missing
        assert_eq!(sorted[1].0, Range::Single(21596364.into()));
        assert!(sorted[1].1.contains(&DataKind::Blocks));
        assert!(sorted[1].1.contains(&DataKind::Transactions));
    }

    #[tokio::test]
    async fn test_find_incomplete_tables_large_missing_gap() {
        let storage = create_test_storage(vec![
            "archive/eth/021000000/021596000/021596362.block.avro",
            "archive/eth/021000000/021596000/021596362.txes.avro",
            // Large gap: 021596363-021596999 all missing
            "archive/eth/021000000/021597000/021597000.block.avro",
            "archive/eth/021000000/021597000/021597000.txes.avro",
        ]).await;

        let range = Range::new(21596362, 21597000);
        let tx_options = DataOptions::default();

        let incomplete = storage.find_incomplete_tables(range, &tx_options).await.unwrap();

        // Should report the large gap as a single missing range
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::new(21596363, 21596999));
        assert!(incomplete[0].1.contains(&DataKind::Blocks));
        assert!(incomplete[0].1.contains(&DataKind::Transactions));
    }
}
