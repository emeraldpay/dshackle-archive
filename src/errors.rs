use tonic;

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
    #[error("Invalid response from blockchain upstream")]
    InvalidResponse,
    #[error("Blockchain Error: {0}")]
    FailResponse(String),
    #[error("IO Error")]
    IO,
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

impl From<tonic::Status> for BlockchainError {
    fn from(e: tonic::Status) -> Self {
        BlockchainError::FailResponse(e.to_string())
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
