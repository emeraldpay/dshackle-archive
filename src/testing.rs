use std::sync::Arc;
use futures_util::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use crate::blockchain::{BlockReference, BlockchainData};
use crate::blockchain::mock::{MockType};
use crate::archiver::Archiver;
use crate::archiver::datakind::{DataKind, TraceOptions};
use crate::archiver::range::Range;
use crate::storage::{TargetFileWriter, ReadTarget};
use crate::kafka::BootstrapBrokers;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

static INIT: std::sync::Once = std::sync::Once::new();

/// Start a single-node Kafka broker in KRaft mode, and the address to reach it.
///
/// `partitions` becomes the broker's `num.partitions` — the shape it gives
/// every topic it creates — so pass more than one wherever the height-to-
/// partition mapping matters. `auto_create` toggles
/// `auto.create.topics.enable`; turn it off to make a test go through the
/// archive's own topic creation instead.
pub async fn start_kafka(
    partitions: u32,
    auto_create: bool,
) -> (ContainerAsync<GenericImage>, BootstrapBrokers) {
    // A Kafka broker tells clients the address to reach it at, and for a
    // container that has to be the mapped *host* port — which therefore has to
    // be known before the broker starts.
    let host_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();

    let container = GenericImage::new("apache/kafka", "3.9.0")
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_mapped_port(host_port, 9092.tcp())
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        // host left empty on purpose: it binds every interface, while a
        // literal 0.0.0.0 makes the broker refuse to start because it also
        // advertises the controller listener
        .with_env_var("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", format!("PLAINTEXT://127.0.0.1:{}", host_port))
        .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_NUM_PARTITIONS", partitions.to_string())
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", auto_create.to_string())
        .start()
        .await
        .unwrap();

    let brokers: BootstrapBrokers = format!("kafka://127.0.0.1:{}", host_port).parse().unwrap();
    (container, brokers)
}

pub fn start_test() {
    INIT.call_once(|| {
        init_tracing();
    });
}

fn init_tracing() {
    let filter = Targets::new()
        .with_target("dshackle_archive", tracing::level_filters::LevelFilter::TRACE)
        .with_default(tracing::level_filters::LevelFilter::INFO);
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_filter(filter.clone())
        ;
    let subscriber = tracing_subscriber::registry()
        .with(stdout_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");
}


pub async fn list_mem_files(mem: Arc<InMemory>) -> Vec<ObjectMeta> {
    let mut result = Vec::new();
    let mut stream = mem.list(None);
    while let Some(meta) = stream.next().await {
        if let Ok(meta) = meta {
            result.push(meta);
        }
    }
    result
}

pub async fn list_mem_filenames(mem: Arc<InMemory>) -> Vec<String> {
    list_mem_files(mem).await.into_iter()
        .map(|m| m.location.as_ref().to_string())
        .collect()
}

pub async fn write_block_and_tx<TS: ReadTarget>(
    archiver: &Archiver<MockType, TS>,
    height: u64, tx_index: Option<Vec<usize>>
) -> anyhow::Result<()> {
    let block = archiver.data_provider.find_block(height).unwrap();
    let file_block = archiver.target
        .create(DataKind::Blocks, &Range::Single(height.into()), true)
        .await.expect("Create block").unwrap();

    let record = archiver.data_provider.fetch_block(&BlockReference::Height(height.into())).await?;
    file_block.append(record.0).await?;
    file_block.close().await?;

    let file_txes = archiver.target
        .create(DataKind::Transactions, &Range::Single(height.into()), true)
        .await.expect("Create txes").unwrap();

    let txes = match tx_index {
        None => block.transactions.iter().enumerate().map(|(i, _)| i).collect(),
        Some(v) => v,
    };

    for i in txes {
        let record = archiver.data_provider.fetch_tx(&block, i).await?;
        file_txes.append(record).await?;
    }
    file_txes.close().await?;

    Ok(())
}

/// Write block, transactions, and traces for the given height
///
/// # Arguments
/// * `archiver` - The archiver instance
/// * `height` - Block height to write
/// * `tx_index` - Optional list of transaction indices to write (None = write all)
/// * `trace_options` - Optional trace options (None = no traces)
pub async fn write_block_tx_and_traces<TS: ReadTarget>(
    archiver: &Archiver<MockType, TS>,
    height: u64,
    tx_index: Option<Vec<usize>>,
    trace_options: Option<TraceOptions>,
) -> anyhow::Result<()> {
    let block = archiver.data_provider.find_block(height).unwrap();

    // Write block
    let file_block = archiver.target
        .create(DataKind::Blocks, &Range::Single(height.into()), true)
        .await.expect("Create block").unwrap();

    let record = archiver.data_provider.fetch_block(&BlockReference::Height(height.into())).await?;
    file_block.append(record.0).await?;
    file_block.close().await?;

    // Write transactions
    let file_txes = archiver.target
        .create(DataKind::Transactions, &Range::Single(height.into()), true)
        .await.expect("Create txes").unwrap();

    let txes = match tx_index {
        None => block.transactions.iter().enumerate().map(|(i, _)| i).collect(),
        Some(v) => v,
    };

    for i in &txes {
        let record = archiver.data_provider.fetch_tx(&block, *i).await?;
        file_txes.append(record).await?;
    }
    file_txes.close().await?;

    // Write traces if requested
    if let Some(options) = trace_options {
        let file_traces = archiver.target
            .create(DataKind::TransactionTraces, &Range::Single(height.into()), true)
            .await.expect("Create traces").unwrap();

        for i in &txes {
            let record = archiver.data_provider.fetch_traces(&block, *i, &options).await?;
            file_traces.append(record).await?;
        }
        file_traces.close().await?;
    }

    Ok(())
}

///
/// An object store where every download breaks after the first bytes
///
/// Everything else, including writing and listing, works as usual, so a test can fill the
/// storage first and then read it through this wrapper.
#[derive(Debug)]
pub struct BrokenDownloads {
    inner: Arc<InMemory>,
    /// Break only the paths containing it, or every path when `None`
    only: Option<String>,
}

impl BrokenDownloads {
    pub fn new(inner: Arc<InMemory>) -> Self {
        Self { inner, only: None }
    }

    ///
    /// Break the downloads of the files with the given text in the path, and serve the others as
    /// usual. For a test where a part of the archive is readable and a part is not.
    pub fn only<S: ToString>(inner: Arc<InMemory>, path_part: S) -> Self {
        Self { inner, only: Some(path_part.to_string()) }
    }

    fn breaks(&self, location: &Path) -> bool {
        match &self.only {
            None => true,
            Some(part) => location.as_ref().contains(part.as_str()),
        }
    }
}

impl std::fmt::Display for BrokenDownloads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BrokenDownloads({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for BrokenDownloads {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let response = self.inner.get_opts(location, options).await?;
        if !self.breaks(location) {
            return Ok(response);
        }
        let meta = response.meta.clone();
        let range = response.range.clone();
        let attributes = response.attributes.clone();
        let extensions = response.extensions.clone();
        let broken = futures_util::stream::once(async {
            Err(object_store::Error::Generic {
                store: "test",
                source: "connection reset".into(),
            })
        }).boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(broken),
            meta,
            range,
            attributes,
            extensions,
        })
    }

    fn delete_stream(&self, locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
