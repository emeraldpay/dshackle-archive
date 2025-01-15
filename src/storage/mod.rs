use std::path::PathBuf;
use apache_avro::types::Record;
use async_trait::async_trait;
use crate::args::Args;
use crate::datakind::DataKind;
use crate::errors::{ConfigError};
use crate::filenames::Filenames;
use crate::range::Range;
use crate::storage::fs::FsStorage;
use anyhow::Result;
use tokio::sync::mpsc::Receiver;

mod fs;

pub fn from_args(value: &Args) -> Result<impl TargetStorage, ConfigError> {
    if value.dir.is_none() {
        return Err(ConfigError::NoTargetDir);
    }
    let dir = value.dir.as_ref().unwrap().clone();
    Ok(FsStorage::new(PathBuf::from(dir), Filenames::default()))
}

#[async_trait]
pub trait TargetStorage: Send + Sync {
    async fn create(&self, kind: DataKind, range: &Range) -> Result<Box<dyn TargetFile + Send + Sync>>;

    fn list(&self, range: Range) -> Result<Receiver<FileReference>>;
}

#[async_trait]
pub trait TargetFile {
    async fn append(&self, data: Record<'_>) -> Result<()>;

    ///
    /// MUST BE called if everything is written ok. Otherwise, the file is deleted on Drop.
    async fn close(mut self: Box<Self>) -> Result<()>;
}

pub struct FileReference {
    pub path: String,
    pub kind: DataKind,
    pub range: Range,
}
