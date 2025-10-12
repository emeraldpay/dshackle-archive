use std::{fs};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Mutex};
use apache_avro::types::Record;
use apache_avro::{Codec, Writer};
use async_trait::async_trait;
use crate::archiver::datakind::DataKind;
use crate::archiver::filenames::{Filenames, Level, LevelDouble};
use crate::archiver::range::Range;
use crate::storage::{avro_reader, copy, FileReference, TargetFile, TargetFileReader, TargetFileWriter, TargetStorage};
use anyhow::{anyhow, Context, Result};
use tokio::sync::mpsc::Receiver;

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
impl TargetStorage for FsStorage {

    type Writer = FsFileWriter<'static>;
    type Reader = FsFileReader;

    async fn create(&self, kind: DataKind, range: &Range) -> Result<FsFileWriter<'static>> {
        let filename = self.parent_dir.join(self.filenames.path(&kind, range));
        Ok(FsFileWriter::new(filename.clone(), kind).context(format!("Path: {:?}", &filename))?)
    }

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
                        let is_archive = Filenames::parse(filename.to_string());
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
}

impl FsFileWriter<'_> {
    pub fn new(path: PathBuf, kind: DataKind) -> Result<Self> {
        tracing::debug!("Create file: {:?}", path);
        let _ = fs::create_dir_all(path.parent().unwrap())?;
        let file = File::create(path.clone())?;
        let writer = Writer::with_codec(kind.schema(), file, Codec::Snappy);
        let writer = Mutex::new(writer);
        Ok(Self { path, writer: Some(writer) })
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

    async fn append(&self, data: Record<'_>) -> Result<()> {
        match &self.writer {
            None => Err(anyhow!("Writer is already closed")),
            Some(writer) => {
                let mut writer = writer.lock().unwrap();
                let _ = writer.append(data).map_err(|e| anyhow!("IO Error: {}. File: {:?}", e, self.path))?;
                Ok(())
            }
        }
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
        let rx_sync = avro_reader::consume_sync(self.kind.schema(), self.file);
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
