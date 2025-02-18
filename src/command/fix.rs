use std::marker::PhantomData;
use std::str::FromStr;
use anyhow::anyhow;
use async_trait::async_trait;
use crate::args::Args;
use crate::blockchain::{BlockchainTypes};
use crate::command::archiver::Archiver;
use crate::command::{Blocks, CommandExecutor};
use crate::range::Range;

///
/// Provides `fix` command.
/// It checks the archive for the specified range and add missing data
///
#[derive(Clone)]
pub struct FixCommand<B: BlockchainTypes> {
    b: PhantomData<B>,
    blocks: Blocks,
    archiver: Archiver<B>,
}

impl<B: BlockchainTypes> FixCommand<B> {
    pub fn new(config: &Args,
               archiver: Archiver<B>) -> anyhow::Result<Self> {
        let blocks = if let Some(tail) = config.tail {
            Blocks::Tail(tail)
        } else if let Some(range) = &config.range {
            Blocks::Range(Range::from_str(range)?)
        } else {
            return Err(anyhow!("Either `tail` or `range` should be specified"));
        };

        Ok(Self {
            b: PhantomData,
            archiver,
            blocks,
        })
    }
}

#[async_trait]
impl<B: BlockchainTypes> CommandExecutor for FixCommand<B> {

    async fn execute(&self) -> anyhow::Result<()> {
        let range = self.archiver.get_range(&self.blocks).await?;
        tracing::info!("Fixing range: {}", range);
        self.archiver.ensure_all(range).await
    }
}
