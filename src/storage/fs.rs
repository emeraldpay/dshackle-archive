use std::{fs};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Mutex};
use apache_avro::types::Record;
use apache_avro::{Writer};
use async_trait::async_trait;
use crate::archiver::datakind::{DataKind, DataOptions};
use crate::archiver::filenames::{Filenames, Level, LevelDouble};
use crate::archiver::range::Range;
use crate::formats::avro;
use crate::notify::Location;
use crate::record::ArchiveRow;
use crate::storage::{
    avro_reader, copy, find_incomplete_by_listing, FileReference, ReadTarget, ScanTarget,
    TargetFile, TargetFileReader, TargetFileWriter, WriteTarget,
};
use anyhow::{anyhow, Context, Result};
use tokio::sync::mpsc::Receiver;
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
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let filenames = self.filenames.clone();
        let parent_dir = self.parent_dir.clone();

        tokio::spawn(async move {
            let mut level = LevelDouble::new(&filenames, range.start());
            let mut prev = PathBuf::new();
            while level.height() < range.end() {
                let dir = parent_dir.join(level.dir());
                if dir == prev {
                    tracing::error!("Checking the same dir twice");
                    return
                }
                prev = dir.clone();
                let exist = fs::exists(&dir);
                if exist.is_err() {
                    tracing::warn!("Cannot read dir: {:?}", exist.err().unwrap());
                    return
                }
                if !exist.unwrap() {
                    tracing::debug!("Doesn't exist: {:?}. Skipping", dir);
                    continue
                }
                let meta = fs::metadata(&dir);
                if meta.is_err() {
                    tracing::warn!("Cannot read dir: {:?}", meta.err().unwrap());
                    return
                }
                let meta = meta.unwrap();
                if !meta.is_dir() {
                    // skip the level, but continue with the next one
                    tracing::warn!("Is not a dir: {:?}. Skipping", dir);
                    continue
                }
                let files = fs::read_dir(dir);
                if files.is_err() {
                    tracing::warn!("Cannot read dir: {:?}", files.err().unwrap());
                    return
                }
                let files = files.unwrap();
                for file in files {
                    if tx.is_closed() {
                        return
                    }
                    if let Ok(file) = file {
                        let path = file.path();
                        let filename = path.file_name().unwrap().to_str().unwrap();
                        let is_archive = filenames.parse(filename);
                        if is_archive.is_none() {
                            tracing::debug!("Not an archive: {}", filename);
                            continue
                        }
                        let (kind, file_range) = is_archive.unwrap();
                        if file_range.is_intersected_with(&range) {
                            let r = FileReference {
                                range: file_range,
                                kind,
                                path: path.to_string_lossy().to_string(),
                            };
                            if tx.send(r).await.is_err() {
                                return
                            }
                        }
                    }
                }
                level = level.next();
            }
        });
        Ok(rx)
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
        let record = avro::encode_row(&row)?;
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
    fn read(self) -> Result<Receiver<Record<'static>>> {
        let rx_sync = avro_reader::consume_sync(self.kind, avro::schema_for(self.kind), self.file);
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
