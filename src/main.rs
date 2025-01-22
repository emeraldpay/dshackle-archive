#[macro_use]
extern crate enum_display_derive;
extern crate serde;

use std::marker::PhantomData;
use std::str::FromStr;
use crate::args::{Args};
use clap::Parser;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use crate::command::stream::StreamCommand;
use crate::blockchain::{BitcoinType, BlockchainTypes, EthereumType};
use blockchain::connection::Blockchain;
use anyhow::{anyhow, Result};
use emerald_api::proto::common::ChainRef;
use crate::command::CommandExecutor;
use crate::notify::Notifier;
use crate::storage::TargetStorage;

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
    match chain_ref {
        ChainRef::ChainEthereum | ChainRef::ChainEthereumClassic | ChainRef::ChainSepolia => run(Builder::<EthereumType>::new(), &args).await?,
        ChainRef::ChainBitcoin | ChainRef::ChainTestnetBitcoin => run(Builder::<BitcoinType>::new(), &args).await?,
        _ => return Err(anyhow!("Unsupported blockchain: {} / {:?}", args.blockchain, chain_ref)),
    };

    tracing::info!("Done: {}", args.command);
    Ok(())
}

async fn run<B: BlockchainTypes>(builder: Builder<B>, args: &Args) -> Result<()> {
    let target: Box<dyn TargetStorage> = storage::from_args(&args).unwrap();
    let blockchain = Blockchain::new(&args.connection, args.as_dshackle_chain()?).await?;
    builder.with_target(target)
        .with_notifier(notify::create_notifier(&args).await?)
        .with_data(blockchain, args.blockchain.clone())
        .stream(args)
        .await
        .execute()
        .await
}

struct Builder<B: BlockchainTypes> {
    b: PhantomData<B>,
    target: Option<Box<dyn TargetStorage>>,
    notifier: Option<Box<dyn Notifier>>,
}

struct BuilderWithData<B: BlockchainTypes> {
    parent: Builder<B>,
    data: B::DataProvider,
}

impl<B> Builder<B> where B: BlockchainTypes {
    fn new() -> Self {
        Self {
            b: PhantomData,
            target: None,
            notifier: None,
        }
    }

    fn with_notifier(self, notifier: Box<dyn Notifier>) -> Self {
        Self {
            notifier: Some(notifier),
            ..self
        }
    }

    fn with_target(self, target: Box<dyn TargetStorage>) -> Self {
        Self {
            target: Some(target),
            ..self
        }
    }

    fn with_data(self, blockchain: Blockchain, id: String) -> BuilderWithData<B> {
        BuilderWithData {
            parent: self,
            data: B::create_data_provider(blockchain, id),
        }
    }
}

impl<B> BuilderWithData<B> where B: BlockchainTypes {
    async fn stream(self, args: &Args) -> StreamCommand<B> {
        let shutdown = shutdown::Shutdown::new().unwrap();
        let notifier = self.parent.notifier.unwrap();
        let command = StreamCommand::new(&args, shutdown, self.parent.target.unwrap(), self.data, notifier).await.unwrap();
        command
    }
}
