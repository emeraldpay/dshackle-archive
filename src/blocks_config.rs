use std::str::FromStr;
use anyhow::anyhow;
use crate::args::Args;
use crate::blockchain::{BlockchainData, BlockchainTypes};
use crate::range::Range;

#[derive(Clone)]
pub enum Blocks {
    Tail(u64),
    Range(Range),
}

impl TryFrom<&Args> for Blocks {
    type Error = anyhow::Error;

    fn try_from(value: &Args) -> Result<Self, Self::Error> {
        if let Some(tail) = value.tail {
            Ok(Blocks::Tail(tail))
        } else if let Some(range) = &value.range {
            Ok(Blocks::Range(Range::from_str(range)?))
        } else {
            Err(anyhow!("Either `tail` or `range` should be specified"))
        }
    }
}

impl Blocks {
    pub async fn to_range<T: BlockchainTypes>(&self, data_provider: &dyn BlockchainData<T>) -> anyhow::Result<Range> {
        let range = match self {
            Blocks::Tail(n) => {
                // for the tail it's possibly that some data is still being written
                // and so for the tail range we don't touch the last 4 blocks
                // TODO blocks length should be configurable
                let height = data_provider.height().await?.0 - 4;
                let start = if height > *n {
                    height - n
                } else {
                    0
                };
                Range::new(start, height)
            }
            Blocks::Range(range) => range.clone()
        };
        Ok(range)
    }
}
