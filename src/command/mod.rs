use async_trait::async_trait;
use anyhow::Result;

pub mod stream;
pub mod fix;
pub mod archiver;

///
/// A base trait for Dshackle Archive commands (i.e., for `stream`, `archive`, `compact`, etc.)
#[async_trait]
pub trait CommandExecutor {

    ///
    /// Executes the command
    async fn execute(&self) -> Result<()>;
}
