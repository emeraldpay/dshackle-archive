use clap::Parser;
use std::fmt::Display;
use std::str::FromStr;
use emerald_api::proto::common::ChainRef;
use serde::Deserialize;
use crate::errors::ConfigError;
use shadow_rs::{shadow};

shadow!(build);

pub fn version() -> String {
    let date = chrono::DateTime::from_timestamp_secs(build::BUILD_TIMESTAMP)
        .expect("valid build timestamp")
        .format("%Y-%m-%dT%H:%M:%S UTC");
    format!(
        "{} built from {} on {} for {}",
        build::PKG_VERSION,
        build::SHORT_COMMIT,
        date,
        build::BUILD_OS,
    )
}

const BANNER: &str = r#"
                               _     _    __  _     _                _    _      _                _     _
  ___ _ __ ___   ___ _ __ __ _| | __| |  / /_| |___| |__   __ _  ___| | _| | ___| | __ _ _ __ ___| |__ (_)_   _____
 / _ \ '_ ` _ \ / _ \ '__/ _` | |/ _` | / / _` / __| '_ \ / _` |/ __| |/ / |/ _ \ |/ _` | '__/ __| '_ \| \ \ / / _ \
|  __/ | | | | |  __/ | | (_| | | (_| |/ / (_| \__ \ | | | (_| | (__|   <| |  __/ | (_| | | | (__| | | | |\ V /  __/
 \___|_| |_| |_|\___|_|  \__,_|_|\__,_/_/ \__,_|___/_| |_|\__,_|\___|_|\_\_|\___| |\__,_|_|  \___|_| |_|_| \_/ \___|
                                                                                |_|

  Emerald Dshackle Archive - Archiving tool for the blockchain data
  https://github.com/emeraldpay/dshackle-archive"#;

pub fn print_banner() {
    println!("{}", BANNER);
    println!("  v{}\n", version());
}

#[derive(Parser, Debug, Clone)]
#[command(version = version())]
pub struct Args {
    #[arg(id = "COMMAND")]
    pub command: Command,

    /// Blockchain
    #[arg(long, short)]
    pub blockchain: String,

    /// Do not modify the storage
    #[arg(long = "dry-run", aliases = vec!["dryRun", "dryrun"])]
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

    /// [Stream Command] Continue from the last archived block; i.e., not the latest in blockchain
    #[arg(long = "continue")]
    pub continue_last: bool,

    /// [Verify/Fix Commands] Use the latest T blocks instead of a range
    #[arg(long = "tail")]
    pub tail: Option<u64>,

    /// Blocks Range (`N...M`)
    #[arg(long, short)]
    pub range: Option<String>,

    /// Range chunk size (default 1000)
    #[arg(long = "range.chunk", aliases = vec!["rangeChunk", "range-chunk"])]
    pub range_chunk: Option<usize>,

    /// Types of tables to archive (comma-separated list of `blocks`, `txes`, `traces`). Default: blocks,txes
    #[arg(long = "tables", short = 't')]
    pub tables: Option<String>,

    /// List of data to include into tracing archive table (comma-separated list of `calls`, `stateDiff`). Default: calls,stateDiff;
    /// Used only if `traces` are included into the archived tables (see `--tables` option);
    /// Details:
    /// `calls` - debug_traceTransaction with `callTracer` tracing;
    /// `stateDiff` - debug_traceTransaction with `prestateTracer` tracing
    #[arg(long = "fields.trace", aliases = vec!["fieldsTrace", "fields-trace"])]
    pub fields_trace: Option<String>,

    ///
    /// [Fix Command] Set to remove any existing data in whole chunk if any of tables is missing a block in the chunk or has broken values.
    /// Default is `false`, which deleted only tables with missing / corrupted data.
    #[arg(long = "fix.clean", alias = "fix-clean")]
    pub fix_clean: bool,

    ///
    /// Compression algorithm to use when writing new Avro files. Default is `zstd`.
    #[arg(long = "compression")]
    pub compression: Option<Compression>,

    ///
    /// [Stream Command] Follow mode for new blocks: `latest` - follow the latest blocks (default); `finalized` - follow only finalized blocks
    #[arg(long = "follow", default_value = "latest")]
    pub follow: Follow,

    /// Start a Prometheus-compatible metrics server on the given address (e.g., 127.0.0.1:8080).
    /// Metrics are served at http://HOST:PORT/metrics
    #[arg(long = "metrics", value_name = "HOST:PORT")]
    pub metrics: Option<String>,

    /// After the main command finishes, keep the metrics server running until one final scrape
    /// completes (or 60 seconds elapse). Useful for short-lived commands (fix, verify, compact)
    /// to ensure Prometheus collects the final metrics before the process exits.
    #[arg(long = "metrics.await", alias = "metrics-await")]
    pub metrics_await: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            command: Command::Stream,
            blockchain: "ethereum".to_string(),
            dry_run: false,
            connection: Connection::default(),
            notify: None,
            aws: None,
            dir: None,
            continue_last: false,
            tail: None,
            range: None,
            range_chunk: Some(1000),
            tables: Some("blocks,txes".to_string()),
            fields_trace: Some("calls,stateDiff".to_string()),
            fix_clean: false,
            compression: None,
            follow: Follow::Latest,
            metrics: None,
            metrics_await: false,
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

    pub fn get_chunk_size(&self) -> usize {
        self.range_chunk.or(Args::default().range_chunk).unwrap()
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
    #[arg(long = "connection.notls", alias = "connection-notls")]
    pub connection_no_tls: bool,

    /// How many API requests to make in parallel. Range: 1..512. Default: 16
    #[arg(long = "parallel")]
    pub parallel: Option<usize>,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            connection: "localhost:2448".to_string(),
            connection_no_tls: true,
            parallel: None,
        }
    }
}

#[derive(Parser, Debug, Clone)]
pub struct Notify {

    /// Write notifications as JSON line to the specified dir in a file <dshackle-archive-%STARTTIME.jsonl>
    #[arg(long = "notify.dir", required = false, alias = "notify-dir")]
    pub notify_dir: Option<String>,

    /// Send notifications as JSON to the Pulsar to the specified topic (notify.pulsar.url must be specified)
    #[arg(long = "notify.pulsar.topic", required = false, alias = "notify-pulsar-topic")]
    pub pulsar_topic: Option<String>,

    /// Send notifications as JSON to the Pulsar with specified URL (notify.pulsar.topic must be specified)
    #[arg(long = "notify.pulsar.url", required = false, alias = "notify-pulsar-url")]
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
    #[arg(long = "auth.aws.access-key", required = false, aliases = vec!["auth.aws.accessKey", "auth.aws.accesskey", "auth-aws-access-key"])]
    pub access_key: String,

    /// AWS / S3 Secret Key
    #[arg(long = "auth.aws.secret-key", required = false, aliases = vec!["auth.aws.secretKey", "auth.aws.secretkey", "auth-aws-secret-key"])]
    pub secret_key: String,

    /// AWS / S3 endpoint url instead of the default one
    #[arg(long = "aws.endpoint", required = false, alias = "aws.endpoint")]
    pub endpoint: Option<String>,

    /// AWS / S3 region ID to use for requests
    #[arg(long = "aws.region", required = false, alias = "aws-region")]
    pub region: Option<String>,

    /// Enable S3 Path Style access (default is false). Use this flag for a no-AWS service
    #[arg(long = "aws.s3.path-style", aliases = vec!["aws.s3.pathStyle", "aws.s3.pathstyle", "aws-s3-path-style"])]
    pub path_style: bool,

    /// Trust any TLS certificate for AWS / S3 (default is false)
    #[arg(long = "aws.trust-tls", aliases = vec!["aws.trustTls", "aws.trusttls", "aws-trust-tls"])]
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

#[derive(clap::ValueEnum, Debug, Clone, PartialEq)]
pub enum Compression {
    Snappy,
    Zstd,
}

#[derive(clap::ValueEnum, Debug, Clone, PartialEq)]
pub enum Follow {
    Latest,
    Finalized
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
