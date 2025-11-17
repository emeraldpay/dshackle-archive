mod archiver;
mod table;
mod block;
pub mod range;
pub mod datakind;
pub mod filenames;
pub mod range_bag;
pub mod blocks_config;
pub mod range_group;

pub use archiver::{ArchiveAll, Archiver};

use crate::blockchain::BlockchainTypes;

#[allow(type_alias_bounds)]
pub type BlockTransactions<B: BlockchainTypes> = Vec<(B::BlockParsed, Vec<B::TxId>)>;

pub type BlockHash = String;
