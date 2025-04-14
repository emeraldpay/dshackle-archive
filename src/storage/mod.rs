use std::path::PathBuf;
use std::sync::Arc;
use apache_avro::types::Record;
use async_trait::async_trait;
use crate::args::Args;
use crate::datakind::DataKind;
use crate::filenames::Filenames;
use crate::range::Range;
use crate::storage::fs::FsStorage;
use anyhow::{anyhow, Result};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::ClientOptions;
use tokio::sync::mpsc::Receiver;
use url::Url;
use crate::storage::objects::ObjectsStorage;

pub mod fs;
pub mod objects;
mod avro_reader;
mod copy;
mod sorted_files;

pub fn is_s3(args: &Args) -> bool {
    args.aws.is_some()
}

pub fn is_fs(args: &Args) -> bool {
    args.dir.is_some() && !is_s3(args)
}

pub fn create_aws(value: &Args) -> Result<ObjectsStorage<AmazonS3>> {
    // inside the archive we create a subdirectory for each blockchain
    let blockchain_dir = value.get_blockchain()?.code().to_lowercase();
    let aws = value.aws.as_ref().unwrap();

    tracing::info!("Using S3 storage");

    let parent_dir = if let Some(dir) = &value.dir {
        url::Url::parse(dir)
            .map_err(|_| anyhow!("Please specify a --dir as s3://bucket/path"))?
            .path()
            .to_string()
    } else {
        "".to_string()
    };

    let parent_dir = if parent_dir.ends_with('/') {
        blockchain_dir
    } else {
        format!("{}/{}", parent_dir, blockchain_dir)
    };

    let filenames = Filenames::with_dir(parent_dir);

    if value.dir.is_none() {
        return Err(anyhow!("Please set target dir as a s3://bucket/path"));
    }

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

#[async_trait]
pub trait TargetStorage: Send + Sync {

    ///
    /// A type used for writing new records
    type Writer: TargetFileWriter + Send + Sync;

    ///
    /// A type used for reading the existing records
    type Reader: TargetFileReader + Send + Sync;

    ///
    /// Create a new file (or overwrite an existing one)
    async fn create(&self, kind: DataKind, range: &Range) -> Result<Self::Writer>;

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

pub trait TargetFile {
    fn get_url(&self) -> String;
}

#[async_trait]
pub trait TargetFileWriter: TargetFile {
    ///
    /// Append a new record to the file
    async fn append(&self, data: Record<'_>) -> Result<()>;

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
