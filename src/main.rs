#[macro_use]
extern crate enum_display_derive;
extern crate serde;

use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use clap::Parser;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use crate::{
    blockchain::{BitcoinType, BlockchainTypes, EthereumType},
    command::{
        stream::StreamCommand,
        CommandExecutor
    },
    args::{Args},
    notify::Notifier,
    storage::TargetStorage
};
use blockchain::connection::Blockchain;
use anyhow::{anyhow, Result};
use emerald_api::{
    proto::common::ChainRef,
    common::blockchain_ref::BlockchainType
};
use crate::args::Command;
use crate::command::archiver::Archiver;
use crate::command::fix::FixCommand;
use crate::command::verify::VerifyCommand;

pub mod args;
pub mod command;
pub mod errors;
pub mod blockchain;
pub mod storage;
pub mod range;
pub mod datakind;
pub mod filenames;
pub mod avros;
pub mod notify;
pub mod testing;

fn init_tracing() {
    let filter = Targets::new()
        .with_target("dshackle_archive", tracing::level_filters::LevelFilter::DEBUG)
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

#[tokio::main]
async fn main() -> Result<(), String>{
    main_inner().await.map_err(|e| e.to_string())
}

async fn main_inner() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    tracing::info!("Run: {}", args.command);

    let chain_ref = ChainRef::from_str(&args.blockchain)
        .map_err(|_| anyhow!("Unsupported blockchain: {}", args.blockchain))?;
    let chain_type = BlockchainType::try_from(chain_ref)
        .map_err(|_| anyhow!("Unsupported blockchain: {}", args.blockchain))?;
    match chain_type {
        BlockchainType::Ethereum => run(Builder::<EthereumType>::new(), &args).await?,
        BlockchainType::Bitcoin => run(Builder::<BitcoinType>::new(), &args).await?,
    };

    tracing::info!("Done: {}", args.command);
    Ok(())
}

async fn run<B: BlockchainTypes>(builder: Builder<B>, args: &Args) -> Result<()> {
    if storage::is_fs(&args) {
        let target = storage::create_fs(&args)?;
        run_with_target(builder, target, args).await
    } else if storage::is_s3(&args) {
        let target = storage::create_aws(&args)?;
        run_with_target(builder, target, args).await
    } else {
        return Err(anyhow!("Unsupported storage"));
    }
}

async fn run_with_target<B: BlockchainTypes, TS: TargetStorage>(builder: Builder<B>, target: TS, args: &Args) -> Result<()> {
    let blockchain = Blockchain::new(&args.connection, args.as_dshackle_blockchain()?).await?;
    let chain_ref = ChainRef::from_str(&args.blockchain)
        .map_err(|_| anyhow!("Unsupported blockchain: {}", args.blockchain))?;

    let builder = builder
        .with_notifier(notify::create_notifier(&args).await?)
        .with_target(target)
        .with_data(blockchain, chain_ref.code());

    match args.command {
        Command::Stream => {
            builder.stream(args).await
                .execute().await
        },
        Command::Fix => {
            builder.fix(args)
                .execute().await
        },
        Command::Verify => {
            builder.verify(args)
                .execute().await
        }
    }
}

struct Builder<B: BlockchainTypes> {
    b: PhantomData<B>,
    notifier: Option<Box<dyn Notifier>>,
}

struct BuilderWithTarget<B: BlockchainTypes, TS: TargetStorage> {
    parent: Builder<B>,
    target: TS,
}

struct BuilderWithData<B: BlockchainTypes, TS: TargetStorage> {
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

    fn with_target<TS>(self, target: TS) -> BuilderWithTarget<B, TS> where TS: TargetStorage {
        BuilderWithTarget {
            target,
            parent: self,
        }
    }
}

impl<B, TS> BuilderWithTarget<B, TS> where B: BlockchainTypes, TS: TargetStorage {
    fn with_data(self, blockchain: Blockchain, id: String) -> BuilderWithData<B, TS> {
        BuilderWithData {
            parent: self,
            data: B::create_data_provider(blockchain, id),
        }
    }
}

impl<B, TS> BuilderWithData<B, TS> where B: BlockchainTypes, TS: TargetStorage {

    async fn stream(self, args: &Args) -> StreamCommand<B, TS> {
        let shutdown = shutdown::Shutdown::new().unwrap();
        let notifier = self.parent.parent.notifier.unwrap();
        let notifications = notifier.start();
        let archiver = Archiver::new(
            shutdown.clone(), Arc::new(self.parent.target), Arc::new(self.data), notifications
        );
        let command = StreamCommand::new(&args, shutdown, archiver).await.unwrap();
        command
    }

    fn fix(self, args: &Args) -> FixCommand<B, TS> {
        let shutdown = shutdown::Shutdown::new().unwrap();
        let notifier = self.parent.parent.notifier.unwrap();
        let notifications = notifier.start();
        let archiver = Archiver::new(
            shutdown.clone(), Arc::new(self.parent.target), Arc::new(self.data), notifications
        );
        let command = FixCommand::new(&args, archiver).unwrap();
        command
    }

    fn verify(self, args: &Args) -> VerifyCommand<B, TS> {
        let shutdown = shutdown::Shutdown::new().unwrap();
        let notifier = self.parent.parent.notifier.unwrap();
        let notifications = notifier.start();
        let archiver = Archiver::new(
            shutdown.clone(), Arc::new(self.parent.target), Arc::new(self.data), notifications
        );
        let command = VerifyCommand::new(&args, shutdown, archiver).unwrap();
        command
    }
}
