#[macro_use]
extern crate enum_display_derive;
extern crate serde;

use crate::args::{Args, Command};
use clap::Parser;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use crate::command::stream::StreamCommand;
use crate::blockchain::BlockchainData;
use crate::blockchain::ethereum::EthereumData;
use blockchain::connection::Blockchain;
use anyhow::{anyhow, Result};
use crate::command::CommandExecutor;
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

    let shutdown = shutdown::Shutdown::new()?;

    let archiver: Box<dyn BlockchainData> = match args.blockchain.to_lowercase().as_str() {
        "ethereum" => {
            let blockchain = Blockchain::new(&args.connection, args.as_dshackle_chain()?).await?;
            Box::new(EthereumData::new(blockchain, args.blockchain.clone()))
        },
        _ => return Err(anyhow!("Unsupported blockchain: {}", args.blockchain)),
    };

    let notifier = notify::create_notifier(&args);

    let command: Box<dyn CommandExecutor> = match args.command {
        Command::Stream => {
            let target: Box<dyn TargetStorage> = storage::from_args(&args).unwrap();
            let command = StreamCommand::new(&args, shutdown, target, archiver, notifier.as_ref()).await?;
            Box::new(command)
        }
    };

    let _ok = command.execute().await?;

    tracing::info!("Done: {}", args.command);
    Ok(())
}
