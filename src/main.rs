#[macro_use]
extern crate enum_display_derive;
extern crate serde;

use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use clap::Parser;
use tracing_subscriber::{
    layer::SubscriberExt,
    EnvFilter,
    Layer
};
use blockchain::connection::Blockchain;
use anyhow::{anyhow, Result};
use emerald_api::{
    common::blockchain_ref::BlockchainType,
    proto::common::ChainRef
};
use crate::{
    command::{
        archive::ArchiveCommand,
        stream::StreamCommand,
        CommandExecutor,
        compact::CompactCommand,
        fix::FixCommand,
        verify::VerifyCommand
    },
    args::{
        Command,
        Args,
        Format,
    },
    blockchain::{BitcoinType, BlockchainTypes, EthereumType},
    notify::Notifier,
    storage::{ReadTarget, ScanTarget, WriteTarget},
    archiver::Archiver,
};

#[cfg(test)]
pub mod testing;

pub mod args;
pub mod command;
pub mod errors;
pub mod blockchain;
pub mod storage;
pub mod avros;
pub mod formats;
pub mod notify;
mod global;
pub mod metrics;
pub(crate) mod progress;
pub mod archiver;
pub mod record;

fn init_tracing() {
    let filter = EnvFilter::builder()
        .with_default_directive("dshackle_archive=info".parse().unwrap())
        .from_env_lossy();
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_filter(filter);
    let subscriber = tracing_subscriber::registry()
        .with(stdout_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");
}

#[tokio::main]
async fn main() -> Result<()>{
    main_inner().await
}

async fn main_inner() -> Result<()> {
    init_tracing();
    args::print_banner();
    let args = Args::parse();
    tracing::info!("Run: {}", args.command);
    tracing::debug!("debug");
    tracing::trace!("trace");

    global::set_dry_run(&args);
    global::set_compression(&args);
    global::set_threads(&args);
    global::set_retry_policy(&args);
    progress::start();

    if let Some(ref addr) = args.metrics {
        let addr: std::net::SocketAddr = addr.parse()
            .expect("Invalid metrics address, expected HOST:PORT (e.g., 127.0.0.1:8080)");
        metrics::init(addr);
    }

    if global::is_dry_run() {
        tracing::info!("Dry run mode enabled, no changes will be made");
    }

    if args.format == Format::Json {
        match args.command {
            Command::Compact | Command::Verify => {
                return Err(anyhow!(
                    "{:?} is not supported with --format json (the per-height JSON layout has no ranges to compact or verify)",
                    args.command
                ));
            }
            _ => {}
        }
    }

    if storage::is_pulsar(&args) {
        if args.command != Command::Stream {
            return Err(anyhow!(
                "{:?} is not supported with the Pulsar streaming target (topics are append-only — only `stream` can publish to them)",
                args.command
            ));
        }
        if args.continue_last {
            return Err(anyhow!(
                "--continue is not supported by the Pulsar streaming target (no tail-scan capability in v1)"
            ));
        }
    }

    let chain_ref = ChainRef::from_str(&args.blockchain)
        .map_err(|_| anyhow!("Unsupported blockchain: {}", args.blockchain))?;
    let chain_type = BlockchainType::try_from(chain_ref)
        .map_err(|_| anyhow!("Unsupported blockchain type: {}", args.blockchain))?;
    match chain_type {
        BlockchainType::Ethereum => run(Builder::<EthereumType>::new(), &args).await?,
        BlockchainType::Bitcoin => run(Builder::<BitcoinType>::new(), &args).await?,
    };

    tracing::info!("Done: {}", args.command);

    if args.metrics.is_some() && args.metrics_await {
        metrics::await_last_scrape().await;
    }

    Ok(())
}

async fn run<B: BlockchainTypes + 'static>(builder: Builder<B>, args: &Args) -> Result<()> {
    if let Some(stream) = args.stream.as_ref() {
        if let Some(url) = stream.stream_url.as_deref() {
            if !storage::is_pulsar(args) {
                return Err(anyhow!(
                    "Unsupported --stream.url scheme: {} (only pulsar:// is supported today)",
                    url
                ));
            }
        }
    }
    if storage::is_pulsar(&args) {
        run_with_write_target(builder, storage::create_pulsar::<B>(&args).await?, args).await
    } else if storage::is_fs(&args) {
        match args.format {
            Format::Avro => run_with_read_target(builder, storage::create_fs(&args)?, args).await,
            Format::Json => run_with_scan_target(builder, storage::create_fs_json(&args)?, args).await,
        }
    } else if storage::is_s3(&args) {
        match args.format {
            Format::Avro => run_with_read_target(builder, storage::create_aws(&args)?, args).await,
            Format::Json => run_with_scan_target(builder, storage::create_aws_json(&args)?, args).await,
        }
    } else {
        return Err(anyhow!("Unsupported storage"));
    }
}

///
/// Dispatch path for targets that support full read access (Avro on FS/S3 today).
/// All five commands are valid.
async fn run_with_read_target<B: BlockchainTypes + 'static, TS: ReadTarget + 'static>(
    builder: Builder<B>,
    target: TS,
    args: &Args,
) -> Result<()> {
    let builder = build_with_target(builder, target, args).await?;
    match args.command {
        Command::Stream => builder.stream(args).await.execute().await,
        Command::Fix => builder.fix(args).execute().await,
        Command::Archive => builder.archive(args).execute().await,
        Command::Verify => builder.verify(args).execute().await,
        Command::Compact => builder.compact(args).execute().await,
    }
}

///
/// Dispatch path for scan-only targets (JSON-per-field today; streaming targets
/// in Phase 3 will use a separate write-only path). `verify`/`compact` are
/// rejected upfront in [`main_inner`], so the runtime path here is unreachable
/// for those commands; we still error defensively in case the upfront check is
/// ever loosened.
async fn run_with_scan_target<B: BlockchainTypes + 'static, TS: ScanTarget + 'static>(
    builder: Builder<B>,
    target: TS,
    args: &Args,
) -> Result<()> {
    let builder = build_with_target(builder, target, args).await?;
    match args.command {
        Command::Stream => builder.stream(args).await.execute().await,
        Command::Fix => builder.fix(args).execute().await,
        Command::Archive => builder.archive(args).execute().await,
        Command::Verify | Command::Compact => Err(anyhow!(
            "{:?} requires a read-capable target",
            args.command
        )),
    }
}

///
/// Dispatch path for write-only targets (today: Pulsar). Only `stream` is
/// available; everything else was already rejected in [`main_inner`] with a
/// clearer message, but we err here too in case the upfront check is ever
/// loosened.
async fn run_with_write_target<B: BlockchainTypes + 'static, TS: WriteTarget + 'static>(
    builder: Builder<B>,
    target: TS,
    args: &Args,
) -> Result<()> {
    let builder = build_with_target(builder, target, args).await?;
    match args.command {
        Command::Stream => builder.stream_write_only(args).await.execute().await,
        _ => Err(anyhow!(
            "{:?} is not supported by a write-only streaming target",
            args.command
        )),
    }
}

async fn build_with_target<B: BlockchainTypes + 'static, TS: WriteTarget + 'static>(
    builder: Builder<B>,
    target: TS,
    args: &Args,
) -> Result<BuilderWithData<B, TS>> {
    let chain_ref = ChainRef::from_str(&args.blockchain)
        .map_err(|_| anyhow!("Unsupported blockchain: {}", args.blockchain))?;
    let blockchain = Blockchain::new(&args.connection, args.as_dshackle_blockchain()?, chain_ref.code()).await?;

    Ok(builder
        .with_notifier(notify::create_notifier(&args).await?)
        .with_target(target)
        .with_data(blockchain, chain_ref.code()))
}

struct Builder<B: BlockchainTypes> {
    b: PhantomData<B>,
    notifier: Option<Box<dyn Notifier>>,
}

struct BuilderWithTarget<B: BlockchainTypes, TS: WriteTarget> {
    parent: Builder<B>,
    target: TS,
}

struct BuilderWithData<B: BlockchainTypes, TS: WriteTarget> {
    parent: BuilderWithTarget<B, TS>,
    data: B::DataProvider,
}

impl<B> Builder<B> where B: BlockchainTypes {
    fn new() -> Self {
        Self {
            b: PhantomData,
            notifier: None,
        }
    }

    fn with_notifier(self, notifier: Box<dyn Notifier>) -> Self {
        Self {
            notifier: Some(notifier),
            ..self
        }
    }

    fn with_target<TS>(self, target: TS) -> BuilderWithTarget<B, TS> where TS: WriteTarget {
        BuilderWithTarget {
            target,
            parent: self,
        }
    }
}

impl<B, TS> BuilderWithTarget<B, TS> where B: BlockchainTypes, TS: WriteTarget {
    fn with_data(self, blockchain: Blockchain, id: String) -> BuilderWithData<B, TS> {
        BuilderWithData {
            parent: self,
            data: B::create_data_provider(blockchain, id),
        }
    }
}

impl<B, TS> BuilderWithData<B, TS> where B: BlockchainTypes + 'static, TS: WriteTarget + 'static {

    /// `stream` with `--continue` support — requires [`ScanTarget`] so it can
    /// enumerate already-archived data before starting the live tail.
    async fn stream(self, args: &Args) -> StreamCommand<B, TS>
    where
        TS: ScanTarget,
    {
        let notifier = self.parent.parent.notifier.unwrap();
        let notifications = notifier.start();
        let archiver = Archiver::new(
            Arc::new(self.parent.target), Arc::new(self.data), notifications
        );
        let command = StreamCommand::new_with_resume(&args, archiver).await.unwrap();
        command
    }

    /// `stream` against a write-only target (Pulsar). `--continue` is rejected
    /// because there's no scan capability to compute a resume point from.
    async fn stream_write_only(self, args: &Args) -> StreamCommand<B, TS> {
        let notifier = self.parent.parent.notifier.unwrap();
        let notifications = notifier.start();
        let archiver = Archiver::new(
            Arc::new(self.parent.target), Arc::new(self.data), notifications
        );
        let command = StreamCommand::new(&args, archiver).await.unwrap();
        command
    }

    /// `fix` enumerates missing data via [`ScanTarget::find_incomplete_tables`].
    fn fix(self, args: &Args) -> FixCommand<B, TS>
    where
        TS: ScanTarget,
    {
        let notifier = self.parent.parent.notifier.unwrap();
        let notifications = notifier.start();
        let archiver = Archiver::new(
            Arc::new(self.parent.target), Arc::new(self.data), notifications
        );
        let command = FixCommand::new(&args, archiver).unwrap();
        command
    }

    /// `verify` opens existing files to inspect them — requires [`ReadTarget`].
    fn verify(self, args: &Args) -> VerifyCommand<B, TS>
    where
        TS: ReadTarget,
    {
        let notifier = self.parent.parent.notifier.unwrap();
        let notifications = notifier.start();
        let archiver = Archiver::new(
            Arc::new(self.parent.target), Arc::new(self.data), notifications
        );
        let command = VerifyCommand::new(&args, archiver).unwrap();
        command
    }

    /// `archive` only writes; [`WriteTarget`] is enough.
    fn archive(self, args: &Args) -> ArchiveCommand<B, TS> {
        let notifier = self.parent.parent.notifier.unwrap();
        let notifications = notifier.start();
        let archiver = Archiver::new(
            Arc::new(self.parent.target), Arc::new(self.data), notifications
        );
        let command = ArchiveCommand::new(&args, archiver).unwrap();
        command
    }

    /// `compact` reads existing range files and rewrites them — requires [`ReadTarget`].
    fn compact(self, args: &Args) -> CompactCommand<B, TS>
    where
        TS: ReadTarget,
    {
        let notifier = self.parent.parent.notifier.unwrap();
        let notifications = notifier.start();
        let archiver = Archiver::new(
            Arc::new(self.parent.target), Arc::new(self.data), notifications
        );
        let command = CompactCommand::new(&args, archiver).unwrap();
        command
    }
}
