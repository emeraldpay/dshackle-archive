pub mod pulsar;
pub mod kafka;
pub mod empty;
pub mod fs;
pub mod location;
pub mod target;

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::archiver::datakind::DataKind;
use crate::archiver::range::Range;
use tokio::sync::mpsc::{Sender};
use crate::args::Args;
use crate::kafka::BootstrapBrokers;
use anyhow::Result;

pub use location::{FileGroup, FileSlot, Location, MessageRef, RowFiles};
pub use target::NotifyTarget;

/// Notification represents the metadata for an archive event.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Notification {
    /// `version` id of the current JSON format
    pub version: String,
    /// `ts` timestamp of the archive event
    pub ts: chrono::DateTime<chrono::Utc>,
    /// `blockchain` blockchain
    pub blockchain: String,
    /// `type` type of data (transactions, blocks, or traces)
    #[serde(rename = "type")]
    pub file_type: DataKind,
    /// `run` mode in which the Dshackle Archive is run (`archive`, `stream`, `copy` or `compact`)
    pub run: RunMode,
    /// `heightStart` range of blocks in the archived files
    #[serde(rename = "heightStart")]
    pub height_start: u64,
    /// `heightEnd` range of blocks in the archived files
    #[serde(rename = "heightEnd")]
    pub height_end: u64,
    /// `location` where the data landed; the shape depends on the target type
    pub location: Location,
    /// `maturity` maturity level of the block in that archive (`finalized` or `head`)
    pub maturity: Option<Maturity>,
}

///
/// Everything known about a run before the target reports where the data
/// landed. The archiver builds one [`Notification`] per location the writer
/// reports — a single file for row-batched targets, one entry per height for
/// per-height targets (JSON files, streaming brokers).
#[derive(Clone)]
pub struct NotificationBuilder {
    pub blockchain: String,
    pub run: RunMode,
    pub maturity: Option<Maturity>,
}

impl NotificationBuilder {
    pub fn notification(&self, file_type: DataKind, range: &Range, location: Location) -> Notification {
        Notification {
            version: Notification::version(),
            ts: chrono::Utc::now(),
            blockchain: self.blockchain.clone(),
            file_type,
            run: self.run.clone(),
            height_start: range.start(),
            height_end: range.end(),
            location,
            maturity: self.maturity.clone(),
        }
    }
}

/// RunMode represents the mode in which the Dshackle Archive is run.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum RunMode {
    Archive,
    Stream,
    Copy,
    Compact,
    Fix,
}

/// Maturity represents the maturity level of the block.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Maturity {
    /// Block is finalized (i.e, on Ethereum)
    Finalized,
    /// Just a fresh block ath the top
    Head,
}

impl Notification {
    /// The `location` structure changed from a plain URL string to the typed
    /// [`Location`] object in v2 — consumers distinguish the formats by this id.
    pub fn version() -> String {
        "https://schema.emrld.io/dshackle-archive/notify/v2".to_string()
    }
}

#[async_trait]
pub trait Notifier: Send + Sync {
    fn start(&self) -> Sender<Notification>;

    ///
    /// Wait for the notifications already submitted to be delivered, and
    /// report a delivery failure that happened in the background.
    ///
    /// Called once the command is over and its senders are gone. Targets that
    /// deliver synchronously have nothing to wait for.
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub async fn create_notifier(args: &Args) -> Result<Arc<dyn Notifier>> {
    match NotifyTarget::from_args(args)? {
        NotifyTarget::Disabled => Ok(Arc::new(empty::EmptyNotifier::default())),
        NotifyTarget::Dir(dir) => {
            tracing::info!("Save updates to file in directory: {}", dir);
            Ok(Arc::new(fs::FsNotifier::new(dir)))
        }
        NotifyTarget::Pulsar { url, topic } => {
            tracing::info!("Send updates to Pulsar at {} topic {}", url, topic);
            Ok(Arc::new(pulsar::PulsarNotifier::new(url, topic).await?))
        }
        NotifyTarget::Kafka { url, topic } => {
            tracing::info!("Send updates to Kafka at {} topic {}", url, topic);
            let brokers = url.parse::<BootstrapBrokers>()?;
            Ok(Arc::new(kafka::KafkaNotifier::new(brokers, topic).await?))
        }
    }
}
