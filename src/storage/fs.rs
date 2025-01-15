use std::{fs};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Mutex};
use apache_avro::types::Record;
use apache_avro::{Codec, Writer};
use async_trait::async_trait;
use crate::datakind::DataKind;
use crate::filenames::Filenames;
use crate::range::Range;
use crate::storage::{FileReference, TargetFile, TargetStorage};
use anyhow::{anyhow, Context, Result};
use tokio::sync::mpsc::Receiver;

pub struct FsStorage {
    dir: PathBuf,
    filenames: Filenames,
}

impl FsStorage {
    pub fn new(dir: PathBuf, filenames: Filenames) -> Self {
        Self { dir, filenames }
    }
}

#[async_trait]
impl TargetStorage for FsStorage {
    // type Target = FsFile;
    async fn create(&self, kind: DataKind, range: &Range) -> Result<Box<dyn TargetFile + Sync + Send>> {
        let filename = self.dir.join(self.filenames.path(&kind, range));
        Ok(Box::new(FsFile::new(filename.clone(), kind).context(format!("Path: {:?}", &filename))?))
    }

    fn list(&self, range: Range) -> Result<Receiver<FileReference>> {
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let filenames = self.filenames.clone();
        let parent_dir = self.dir.clone();

        tokio::spawn(async move {
            let mut level = filenames.levels(range.start());
            let mut prev = PathBuf::new();
            while level.height < range.end() {
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
                        let (kind, range) = is_archive.unwrap();
                        if kind != kind || !range.contains(level.height) {
                            continue
                        }
                        let r = FileReference {
                            range,
                            kind,
                            path: path.to_string_lossy().to_string(),
                        };
                        if tx.send(r).await.is_err() {
                            return
                        }
                    }
                }
                level = level.next_l2();
            }
        });
        Ok(rx)
    }
}

pub struct FsFile<'a> {
    path: PathBuf,
    pub writer: Option<Mutex<Writer<'a, File>>>,
}

impl FsFile<'_> {
    pub fn new(path: PathBuf, kind: DataKind) -> Result<Self> {
        tracing::debug!("Create file: {:?}", path);
        let _ = fs::create_dir_all(path.parent().unwrap())?;
        let file = File::create(path.clone())?;
        let writer = Writer::with_codec(kind.schema(), file, Codec::Snappy);
        let writer = Mutex::new(writer);
        Ok(Self { path, writer: Some(writer) })
    }
}

#[async_trait]
impl TargetFile for FsFile<'_> {
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

    async fn close(mut self: Box<Self>) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            let mut writer = writer.lock().unwrap();
            let _ = writer.flush().map_err(|e| anyhow!("IO Error: {}. File: {:?}", e, self.path))?;
        }
        self.writer = None;
        Ok(())
    }
}

impl Drop for FsFile<'_> {
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
