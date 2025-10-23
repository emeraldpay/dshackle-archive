pub mod pulsar;
pub mod empty;
pub mod fs;

use serde::{Deserialize, Serialize};
use crate::archiver::datakind::DataKind;
use tokio::sync::mpsc::{Sender};
use crate::args::Args;
use anyhow::Result;

/// Notification represents the metadata for an archive event.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Notification {
    /// `version` id of the current JSON format
    pub version: String,
    /// `ts` timestamp of the archive event
    pub ts: chrono::DateTime<chrono::Utc>,
    /// `blockchain` blockchain
    pub blockchain: String,
    /// `type` type of file (transactions or blocks)
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
    /// `location` a URL to the archived file
    pub location: String,
    /// `maturity` maturity level of the block in that archive (`finalized` or `head`)
    pub maturity: Option<Maturity>,
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
    pub fn version() -> String {
        "https://schema.emrld.io/dshackle-archive/notify".to_string()
    }
}

pub trait Notifier {
    fn start(&self) -> Sender<Notification>;
}

pub async fn create_notifier(args: &Args) -> Result<Box<dyn Notifier>> {
    if let Some(notify) = &args.notify {
        if let Some(dir) = &notify.notify_dir {
            tracing::info!("Save updates to file in directory: {:?}", dir);
            return Ok(Box::new(fs::FsNotifier::new(dir)));
        }
        if let Some(pulsar_url) = &notify.pulsar_url {
            if notify.pulsar_topic.is_none() {
                tracing::warn!("Pulsar topic is not set. Notifications will not be sent.");
                return Ok(Box::new(empty::EmptyNotifier::default()));
            }
            let topic = notify.pulsar_topic.clone().unwrap();
            tracing::info!("Send updates to Pulsar at {} topic {}", &pulsar_url, topic);
            return Ok(Box::new(pulsar::PulsarNotifier::new(pulsar_url.clone(), topic).await?));
        }
    }
    Ok(Box::new(empty::EmptyNotifier::default()))
}
