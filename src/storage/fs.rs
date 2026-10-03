use std::{fs};
use std::fs::File;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex};
use apache_avro::types::Record;
use apache_avro::{Writer};
use async_trait::async_trait;
use crate::archiver::datakind::{DataKind, DataOptions};
use crate::archiver::filenames::{Filenames, Level, LevelDouble, LevelSingle};
use crate::archiver::range::Range;
use crate::formats::avro;
use crate::notify::Location;
use crate::record::ArchiveRow;
use crate::storage::{
    avro_reader, copy, find_incomplete_by_listing, sorted_files, FileReference, ReadTarget, RecordStream,
    ScanTarget, TargetFile, TargetFileReader, TargetFileWriter, WriteTarget,
};
use anyhow::{anyhow, Context, Result};
use tokio::sync::mpsc::{Receiver, Sender};
use crate::global;

pub struct FsStorage {
    parent_dir: PathBuf,
    filenames: Filenames,
}

impl FsStorage {
    pub fn new(dir: PathBuf, filenames: Filenames) -> Self {
        Self { parent_dir: dir, filenames }
    }
}

#[async_trait]
impl WriteTarget for FsStorage {

    type Writer = FsFileWriter<'static>;

    async fn create(&self, kind: DataKind, range: &Range, overwrite: bool) -> Result<Option<FsFileWriter<'static>>> {
        let filename = self.parent_dir.join(self.filenames.path(&kind, range));
        if !overwrite && filename.exists() {
            return Ok(None);
        }
        Ok(Some(FsFileWriter::new(filename.clone(), kind, range.clone()).context(format!("Path: {:?}", &filename))?))
    }
}

#[async_trait]
impl ScanTarget for FsStorage {
    async fn find_incomplete_tables(
        &self,
        blocks: Range,
        tx_options: &DataOptions,
    ) -> Result<Vec<(Range, Vec<DataKind>)>> {
        find_incomplete_by_listing(self, blocks, tx_options).await
    }
}

#[async_trait]
impl ReadTarget for FsStorage {

    type Reader = FsFileReader;

    async fn delete(&self, path: &FileReference) -> Result<()> {
        let path = PathBuf::from(&path.path);
        if !fs::exists(&path).map_err(|e| anyhow!("FS is not accessible: {}", e))? {
            return Ok(())
        }
        let removed = fs::remove_file(&path);
        if let Err(err) = removed {
            return Err(anyhow!("Failed to remove file: {:?}", err));
        }
        Ok(())
    }

    async fn open(&self, path: &FileReference) -> Result<FsFileReader> {
        let file = FsFileReader {
            path: PathBuf::from(&path.path),
            kind: path.kind.clone(),
            file: File::open(&path.path).context(format!("Path: {:?}", &path.path))?,
        };
        Ok(file)
    }

    fn list(&self, range: Range) -> Result<Receiver<FileReference>> {
        // The single block files and the range files are in different directories, see `Filenames::relative_path`.
        // Each kind is listed in its own order, and both are merged to get all the files ordered by their blocks.
        let (tx_single, rx_single) = tokio::sync::mpsc::channel(2);
        let filenames = self.filenames.clone();
        let parent_dir = self.parent_dir.clone();
        let range_single = range.clone();
        tokio::spawn(async move {
            let level = LevelDouble::new(&filenames, range_single.start());
            Self::list_by_steps(&parent_dir, &filenames, &range_single, level, tx_single).await;
        });

        let (tx_range, rx_range) = tokio::sync::mpsc::channel(2);
        let filenames = self.filenames.clone();
        let parent_dir = self.parent_dir.clone();
        tokio::spawn(async move {
            let level = LevelSingle::new(&filenames, range.start());
            Self::list_by_steps(&parent_dir, &filenames, &range, level, tx_range).await;
        });

        Ok(sorted_files::merge_sort(rx_single, rx_range))
    }
}

impl FsStorage {

    ///
    /// Send the archive files from the directories of the level, going from the start of the range to its end.
    ///
    /// It gives the same files as [`crate::storage::objects::ObjectsStorage`] does, i.e. the files that start within the range.
    /// So a range file that starts before the range is not listed, even if it covers some of its blocks.
    async fn list_by_steps<L: Level>(parent_dir: &Path, filenames: &Filenames, range: &Range, mut level: L, tx: Sender<FileReference>) {
        while level.height() <= range.end() && !tx.is_closed() {
            let dir = parent_dir.join(level.dir());
            level = level.next();

            let entries = match fs::read_dir(&dir) {
                Ok(entries) => entries,
                // Nothing is archived to this directory, which is not the end of the archive.
                // Ex., a range that was never archived by single blocks has no directory for them
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    tracing::trace!("Doesn't exist: {:?}. Skipping", dir);
                    continue
                },
                Err(e) => {
                    tracing::warn!("Cannot read dir {:?}: {}", dir, e);
                    continue
                }
            };

            let mut files: Vec<FileReference> = entries
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| {
                    let path = entry.path();
                    let (kind, file_range) = filenames.parse(path.file_name()?.to_str()?)?;
                    let starts_within = range.start() <= file_range.start() && file_range.start() <= range.end();
                    starts_within.then(|| FileReference {
                        range: file_range,
                        kind,
                        path: path.to_string_lossy().to_string(),
                        size: entry.metadata().ok().map(|m| m.len()),
                    })
                })
                .collect();
            // a directory is read in no particular order
            files.sort_by(|a, b| a.range.start().cmp(&b.range.start()).then_with(|| a.path.cmp(&b.path)));

            for file in files {
                if tx.send(file).await.is_err() {
                    return
                }
            }
        }
    }
}

pub struct FsFileWriter<'a> {
    path: PathBuf,
    pub writer: Option<Mutex<Writer<'a, File>>>,
    kind: DataKind,
    range: Range,
}

impl FsFileWriter<'_> {
    pub fn new(path: PathBuf, kind: DataKind, range: Range) -> Result<Self> {
        tracing::debug!("Create file: {:?}", path);
        let _ = fs::create_dir_all(path.parent().unwrap())?;
        let file = File::create(path.clone())?;
        let writer = Writer::with_codec(avro::schema_for(kind), file, global::get_avro_codec());
        let writer = Mutex::new(writer);
        Ok(Self { path, writer: Some(writer), kind, range })
    }

    ///
    /// Append a pre-encoded Avro [`Record`] to the file. Used by code paths that
    /// read existing Avro files and copy records (e.g., compaction) where converting
    /// through [`ArchiveRow`] would be redundant.
    pub(crate) fn append_record(&self, data: Record<'_>) -> Result<()> {
        match &self.writer {
            None => Err(anyhow!("Writer is already closed")),
            Some(writer) => {
                let mut writer = writer.lock().unwrap();
                let bytes = writer.append(data).map_err(|e| anyhow!("IO Error: {}. File: {:?}", e, self.path))?;
                crate::progress::on_bytes(bytes);
                crate::metrics::add_bytes(&self.kind, crate::metrics::Direction::Write, bytes);
                Ok(())
            }
        }
    }
}

pub struct FsFileReader {
    path: PathBuf,
    kind: DataKind,
    file: File,
}

impl TargetFile for FsFileWriter<'_> {
    fn get_url(&self) -> String {
        format!("file://{}", self.path.canonicalize().unwrap_or(self.path.clone()).to_str().unwrap_or("invalid"))
    }
}

impl TargetFile for FsFileReader {
    fn get_url(&self) -> String {
        format!("file://{}", self.path.canonicalize().unwrap_or(self.path.clone()).to_str().unwrap_or("invalid"))
    }
}

#[async_trait]
impl TargetFileWriter for FsFileWriter<'_> {

    async fn append(&self, row: ArchiveRow) -> Result<()> {
        let record = avro::encode_row(row)?;
        self.append_record(record)
    }

    async fn append_avro_record(&self, data: Record<'_>) -> Result<()> {
        self.append_record(data)
    }

    fn locations(&self) -> Vec<(Range, Location)> {
        vec![(self.range.clone(), Location::File { url: self.get_url() })]
    }

    async fn close(mut self: Self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            let mut writer = writer.lock().unwrap();
            let _ = writer.flush().map_err(|e| anyhow!("IO Error: {}. File: {:?}", e, self.path))?;
        }
        self.writer = None;
        Ok(())
    }
}

impl TargetFileReader for FsFileReader {
    fn read(self) -> Result<RecordStream> {
        let url = self.get_url();
        let rx_sync = avro_reader::consume_sync(self.kind, avro::schema_for(self.kind), url, self.file);
        let rx = copy::copy_from_sync(rx_sync);
        Ok(rx)
    }
}

impl Drop for FsFileWriter<'_> {
    fn drop(&mut self) {
        if self.writer.is_none() {
            return;
        }
        let writer = self.writer.take().unwrap();
        self.writer = None;
        let mut writer = writer.lock().unwrap();
        writer.flush().unwrap();
        drop(writer);
        let removed = fs::remove_file(&self.path);
        if let Err(err) = removed {
            tracing::error!("Failed to remove file that was not committed: {:?}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use tempfile::TempDir;
    use crate::archiver::datakind::{DataKind, DataOptions};
    use crate::archiver::filenames::Filenames;
    use crate::archiver::range::Range;
    use crate::storage::{ReadTarget, ScanTarget};
    use super::FsStorage;

    fn create_storage(files: &[&str]) -> (TempDir, FsStorage) {
        let dir = tempfile::tempdir().unwrap();
        for file in files {
            let path = dir.path().join(file);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"data").unwrap();
        }
        let storage = FsStorage::new(dir.path().to_path_buf(), Filenames::with_dir("eth".to_string()));
        (dir, storage)
    }

    /// Paths of the listed files, relative to the archive dir
    async fn list(dir: &TempDir, storage: &FsStorage, range: Range) -> Vec<String> {
        let mut listed = storage.list(range).unwrap();
        let mut result = vec![];
        while let Some(file) = listed.recv().await {
            let path = std::path::Path::new(&file.path).strip_prefix(dir.path()).unwrap();
            result.push(path.to_string_lossy().to_string());
        }
        result
    }

    #[tokio::test]
    async fn lists_nothing_in_empty_archive() {
        let (dir, storage) = create_storage(&[]);

        let files = list(&dir, &storage, Range::new(21_500_000, 21_600_000)).await;

        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn lists_single_block_files_after_missing_directories() {
        // there are no directories for the blocks before 21_596_000, and none for 21_597_000..21_597_999
        let (dir, storage) = create_storage(&[
            "eth/021000000/021596000/021596362.block.avro",
            "eth/021000000/021596000/021596362.txes.avro",
            "eth/021000000/021596000/021596363.block.avro",
            "eth/021000000/021596000/021596363.txes.avro",
            "eth/021000000/021598000/021598444.block.avro",
            "eth/021000000/021598000/021598444.txes.avro",
            "eth/022000000/022000000/022000001.block.avro",
        ]);

        let files = list(&dir, &storage, Range::new(21_500_000, 22_000_500)).await;

        assert_eq!(files, vec![
            "eth/021000000/021596000/021596362.block.avro",
            "eth/021000000/021596000/021596362.txes.avro",
            "eth/021000000/021596000/021596363.block.avro",
            "eth/021000000/021596000/021596363.txes.avro",
            "eth/021000000/021598000/021598444.block.avro",
            "eth/021000000/021598000/021598444.txes.avro",
            "eth/022000000/022000000/022000001.block.avro",
        ]);
    }

    #[tokio::test]
    async fn lists_only_files_in_range() {
        let (dir, storage) = create_storage(&[
            "eth/021000000/021596000/021596362.block.avro",
            "eth/021000000/021596000/021596363.block.avro",
            "eth/021000000/021596000/021596364.block.avro",
            "eth/021000000/021596000/021596365.block.avro",
        ]);

        let files = list(&dir, &storage, Range::new(21_596_363, 21_596_364)).await;

        assert_eq!(files, vec![
            "eth/021000000/021596000/021596363.block.avro",
            "eth/021000000/021596000/021596364.block.avro",
        ]);
    }

    #[tokio::test]
    async fn lists_range_files_with_single_block_files() {
        let (dir, storage) = create_storage(&[
            "eth/021000000/021596000/021596362.block.avro",
            "eth/021000000/021596000/021596362.txes.avro",
            "eth/021000000/range-021596000_021596999.blocks.avro",
            "eth/021000000/range-021596000_021596999.txes.avro",
            "eth/021000000/range-021597000_021597999.blocks.avro",
            "eth/021000000/range-021597000_021597999.txes.avro",
            "eth/021000000/range-021600000_021600999.blocks.avro",
            "eth/021000000/range-021600000_021600999.txes.avro",
        ]);

        let files = list(&dir, &storage, Range::new(21_500_000, 21_599_999)).await;

        // in the order of the blocks, i.e. the same as the listing of an object storage gives
        assert_eq!(files, vec![
            "eth/021000000/range-021596000_021596999.blocks.avro",
            "eth/021000000/range-021596000_021596999.txes.avro",
            "eth/021000000/021596000/021596362.block.avro",
            "eth/021000000/021596000/021596362.txes.avro",
            "eth/021000000/range-021597000_021597999.blocks.avro",
            "eth/021000000/range-021597000_021597999.txes.avro",
        ]);
    }

    #[tokio::test]
    async fn lists_range_files_of_different_directories() {
        let (dir, storage) = create_storage(&[
            "eth/021000000/range-021999000_021999999.blocks.avro",
            "eth/022000000/range-022000000_022000999.blocks.avro",
            "eth/023000000/range-023000000_023000999.blocks.avro",
        ]);

        let files = list(&dir, &storage, Range::new(21_999_000, 22_000_999)).await;

        assert_eq!(files, vec![
            "eth/021000000/range-021999000_021999999.blocks.avro",
            "eth/022000000/range-022000000_022000999.blocks.avro",
        ]);
    }

    #[tokio::test]
    async fn skips_range_file_starting_before_the_range() {
        let (dir, storage) = create_storage(&[
            "eth/021000000/range-021596000_021596999.blocks.avro",
            "eth/021000000/range-021597000_021597999.blocks.avro",
        ]);

        let files = list(&dir, &storage, Range::new(21_596_500, 21_597_999)).await;

        assert_eq!(files, vec![
            "eth/021000000/range-021597000_021597999.blocks.avro",
        ]);
    }

    #[tokio::test]
    async fn lists_single_block_at_the_end_of_range() {
        let (dir, storage) = create_storage(&[
            "eth/021000000/021596000/021596999.block.avro",
            "eth/021000000/021597000/021597000.block.avro",
            "eth/021000000/021597000/021597001.block.avro",
        ]);

        let files = list(&dir, &storage, Range::new(21_596_000, 21_597_000)).await;

        assert_eq!(files, vec![
            "eth/021000000/021596000/021596999.block.avro",
            "eth/021000000/021597000/021597000.block.avro",
        ]);
    }

    #[tokio::test]
    async fn lists_files_with_their_size() {
        let (_dir, storage) = create_storage(&[
            "eth/021000000/range-021596000_021596999.blocks.avro",
        ]);

        let mut listed = storage.list(Range::new(21_596_000, 21_596_999)).unwrap();
        let file = listed.recv().await.unwrap();

        assert_eq!(file.kind, DataKind::Blocks);
        assert_eq!(file.range, Range::new(21_596_000, 21_596_999));
        assert_eq!(file.size, Some(4));
    }

    #[tokio::test]
    async fn finds_no_gaps_in_archive_of_range_and_single_block_files() {
        let (_dir, storage) = create_storage(&[
            "eth/021000000/range-021596000_021596999.blocks.avro",
            "eth/021000000/range-021596000_021596999.txes.avro",
            "eth/021000000/021597000/021597000.block.avro",
            "eth/021000000/021597000/021597000.txes.avro",
            "eth/021000000/021597000/021597001.block.avro",
            "eth/021000000/021597000/021597001.txes.avro",
        ]);

        let incomplete = storage
            .find_incomplete_tables(Range::new(21_596_000, 21_597_001), &DataOptions::default()).await
            .unwrap();

        assert!(incomplete.is_empty(), "{:?}", incomplete);
    }

    #[tokio::test]
    async fn finds_gap_between_range_and_single_block_files() {
        let (_dir, storage) = create_storage(&[
            "eth/021000000/range-021596000_021596999.blocks.avro",
            "eth/021000000/range-021596000_021596999.txes.avro",
            "eth/021000000/021597000/021597002.block.avro",
            "eth/021000000/021597000/021597002.txes.avro",
        ]);

        let incomplete = storage
            .find_incomplete_tables(Range::new(21_596_000, 21_597_002), &DataOptions::default()).await
            .unwrap();

        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::new(21_597_000, 21_597_001));
    }
}
