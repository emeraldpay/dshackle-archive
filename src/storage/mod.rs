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
use object_store::aws::{AmazonS3Builder};
use object_store::ClientOptions;
use tokio::sync::mpsc::Receiver;
use url::Url;
use crate::storage::objects::ObjectsStorage;

pub mod fs;
pub mod objects;

pub fn from_args(value: &Args) -> Result<Box<dyn TargetStorage>> {

    // inside the archive we create a subdirectory for each blockchain
    let blockchain_dir = value.get_blockchain()?.code().to_lowercase();

    let target_storage: Box<dyn TargetStorage> = if let Some(aws) = &value.aws {
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


        Box::new(ObjectsStorage::new(Arc::new(os), bucket, filenames))
    } else {
        tracing::info!("Using Filesystem storage");
        if value.dir.is_none() {
            return Err(anyhow!("No target dir set for a Filesystem based storage"));
        }
        let dir = value.dir.as_ref().unwrap().clone();
        Box::new(FsStorage::new(PathBuf::from(dir), Filenames::with_dir(blockchain_dir)))
    };

    Ok(target_storage)
}

#[async_trait]
pub trait TargetStorage: Send + Sync {
    async fn create(&self, kind: DataKind, range: &Range) -> Result<Box<dyn TargetFile + Send + Sync>>;
    async fn delete(&self, path: &FileReference) -> Result<()>;

    fn list(&self, range: Range) -> Result<Receiver<FileReference>>;
}

#[async_trait]
pub trait TargetFile {
    fn get_url(&self) -> String;

    async fn append(&self, data: Record<'_>) -> Result<()>;

    ///
    /// MUST BE called if everything is written ok. Otherwise, the file is deleted on Drop.
    async fn close(mut self: Box<Self>) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileReference {
    pub path: String,
    pub kind: DataKind,
    pub range: Range,
}
