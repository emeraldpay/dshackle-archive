use clap::Parser;
use std::fmt::Display;
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
        }
    }
}

impl Args {
    pub fn as_dshackle_chain(&self) -> Result<i32, ConfigError> {
        match self.blockchain.to_ascii_lowercase().as_str() {
            "ethereum" => Ok(100),
            "bitcoin" => Ok(1),
            _ => Err(ConfigError::UnsupportedBlockchain(self.blockchain.clone())),
        }
    }
}

#[derive(Deserialize, clap::ValueEnum, Debug, Display, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    Stream,
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

    /// Enable S3 Path Style access (default is false)
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
        assert_eq!(args.as_dshackle_chain().unwrap(), 100);
    }
}
