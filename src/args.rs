use clap::Parser;
use std::fmt::Display;
use std::str::FromStr;
use emerald_api::proto::common::ChainRef;
use serde::Deserialize;
use crate::errors::ConfigError;

#[derive(Parser, Debug, Clone)]
pub struct Args {
    #[arg(id = "COMMAND")]
    pub command: Command,

    /// Blockchain
    #[arg(long, short)]
    pub blockchain: String,

    /// Do not modify the storage
    #[arg(long = "dryRun")]
    pub dry_run: bool,

    #[command(flatten)]
    pub connection: Connection,

    #[command(flatten)]
    pub notify: Option<Notify>,

    #[command(flatten)]
    pub aws: Option<Aws>,

    /// Target directory
    #[arg(long = "dir", short)]
    pub dir: Option<String>,

    /// Continue from the last file if set
    #[arg(long = "continue")]
    pub continue_last: bool,

    /// Ensure that the latest T are archived (with `fix` command)
    #[arg(long = "tail")]
    pub tail: Option<u64>,

    /// Blocks Range (`N...M`)
    #[arg(long, short)]
    pub range: Option<String>,

    /// Range chunk size (default 1000)
    #[arg(long = "rangeChunk")]
    pub range_chunk: Option<usize>,

    /// (Ethereum Geth specific) Enable trace extraction (debug_traceTransaction with `callTracer` tracing)
    #[arg(long = "includeTrace")]
    pub include_trace: bool,

    /// (Ethereum Geth specific) Enable stateDiff extraction (debug_traceTransaction with `prestateTracer` tracing)
    #[arg(long = "includeStateDiff")]
    pub include_state_diff: bool,

    /// Put Transaction traces into a separate file (with `trace` suffix). Traces could be very large, sometimes gigabytes per transaction and in some cases it makes sense to store and process them separately.
    /// By default, traces are stored inline with the transaction data.
    /// If not trances are included, this flag has no effect.
    #[arg(long = "separateTraces")]
    pub tx_trace_separate: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            command: Command::Stream,
            blockchain: "ethereum".to_string(),
            dry_run: true,
            connection: Connection::default(),
            notify: None,
            aws: None,
            dir: None,
            continue_last: false,
            tail: None,
            range: None,
            range_chunk: Some(1000),
            include_trace: false,
            include_state_diff: false,
            tx_trace_separate: false,
        }
    }
}

impl Args {

    pub fn get_blockchain(&self) -> Result<ChainRef, ConfigError> {
        ChainRef::from_str(&self.blockchain)
            .map_err(|_| ConfigError::UnsupportedBlockchain(self.blockchain.clone()))
    }

    pub fn as_dshackle_blockchain(&self) -> Result<i32, ConfigError> {
        let chain_ref = self.get_blockchain()?;
        Ok(chain_ref as i32)
    }

}

#[derive(Deserialize, clap::ValueEnum, Debug, Display, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    Stream,
    Fix,
    Verify,
    Archive,
    Compact,
}

#[derive(Parser, Debug, Clone)]
pub struct Connection {
    /// Connection (host:port)
    #[arg(long, short, value_name = "HOST:PORT")]
    pub connection: String,

    /// Disable TLS
    #[arg(long = "connection.notls")]
    pub connection_no_tls: bool,

    /// How many requests to make in parallel. Range: 1..512. Default: 8
    #[arg(long = "parallel", default_value = "8")]
    pub parallel: usize,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            connection: "localhost:2448".to_string(),
            connection_no_tls: true,
            parallel: 8,
        }
    }
}

#[derive(Parser, Debug, Clone)]
pub struct Notify {

    /// Write notifications as JSON line to the specified dir in a file <dshackle-archive-%STARTTIME.jsonl>
    #[arg(long = "notify.dir", required = false)]
    pub notify_dir: Option<String>,

    /// Send notifications as JSON to the Pulsar to the specified topic (notify.pulsar.url must be specified)
    #[arg(long = "notify.pulsar.topic", required = false)]
    pub pulsar_topic: Option<String>,

    /// Send notifications as JSON to the Pulsar with specified URL (notify.pulsar.topic must be specified)
    #[arg(long = "notify.pulsar.url", required = false)]
    pub pulsar_url: Option<String>,
}

impl Default for Notify {
    fn default() -> Self {
        Self {
            pulsar_topic: None,
            pulsar_url: None,
            notify_dir: None,
        }
    }
}

#[derive(Parser, Debug, Clone)]
pub struct Aws {
    /// AWS / S3 Access Key
    #[arg(long = "auth.aws.accessKey", required = false)]
    pub access_key: String,

    /// AWS / S3 Secret Key
    #[arg(long = "auth.aws.secretKey", required = false)]
    pub secret_key: String,

    /// AWS / S3 endpoint url instead of the default one
    #[arg(long = "aws.endpoint", required = false)]
    pub endpoint: Option<String>,

    /// AWS / S3 region ID to use for requests
    #[arg(long = "aws.region", required = false)]
    pub region: Option<String>,

    /// Enable S3 Path Style access (default is false). Use this flag for a no-AWS service
    #[arg(long = "aws.s3.pathStyle")]
    pub path_style: bool,

    /// Trust any TLS certificate for AWS / S3 (default is false)
    #[arg(long = "aws.trustTls")]
    pub trust_tls: bool,
}

impl Default for Aws {
    fn default() -> Self {
        Self {
            access_key: "".to_string(),
            secret_key: "".to_string(),
            endpoint: None,
            region: None,
            path_style: false,
            trust_tls: false,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_as_dshackle_chain() {
        let args = Args {
            blockchain: "ethereum".to_string(),
            ..Args::default()
        };
        assert_eq!(args.as_dshackle_blockchain().unwrap(), 100);
    }

    #[test]
    fn test_chain_naming() {
        let args = Args {
            blockchain: "testnet-sepolia".to_string(),
            ..Args::default()
        };
        assert_eq!(args.get_blockchain().unwrap().code(), "SEPOLIA");

        let args = Args {
            blockchain: "etc".to_string(),
            ..Args::default()
        };
        assert_eq!(args.get_blockchain().unwrap().code(), "ETC");
    }
}
