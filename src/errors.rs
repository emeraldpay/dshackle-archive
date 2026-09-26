#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum Error {
    #[error("Blockchain Error: {0}")]
    Blockchain(BlockchainError),
    #[error("Config Error: {0}")]
    Config(ConfigError),
    #[error("Write Error: {0}")]
    Write(WriteError),
}

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ConfigError {
    #[error("Unsupported blockchain: {0}")]
    UnsupportedBlockchain(String),
    #[error("No target dir is set")]
    NoTargetDir,
}

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum BlockchainError {
    #[error("Invalid connection to blockchain: {0}")]
    InvalidConnection(String),
    #[error("No connection to blockchain")]
    NoConnection,
    #[error("Timeout calling: {0}")]
    Timeout(String),
    #[error("Invalid response from blockchain upstream")]
    InvalidResponse,
    #[error("Blockchain Error: {0} -> {1}")]
    FailResponse(String, String),
    /// The call itself failed (connection, gRPC status, missing reply), as
    /// opposed to the upstream answering with an error. Carries the method
    /// and the cause, since this is what gets reported once retries give up.
    #[error("IO Error calling {0}: {1}")]
    IO(String, String),
}

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum WriteError {
    #[error("File is closed")]
    Closed,
    #[error("Failed to convert to Avro: {0}")]
    Avro(String),
}

impl From<BlockchainError> for Error {
    fn from(e: BlockchainError) -> Self {
        Error::Blockchain(e)
    }
}

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Self {
        Error::Config(e)
    }
}

impl From<WriteError> for Error {
    fn from(e: WriteError) -> Self {
        Error::Write(e)
    }
}

impl From<apache_avro::Error> for WriteError {
    fn from(value: apache_avro::Error) -> Self {
        WriteError::Avro(value.to_string())
    }
}
