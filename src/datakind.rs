use std::str::FromStr;
use apache_avro::Schema;
use crate::avros;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataKind {
    Blocks,
    Transactions,
}

impl DataKind {
    pub fn schema(&self) -> &'static Schema {
        match self {
            DataKind::Blocks => &avros::BLOCK_SCHEMA,
            DataKind::Transactions => &avros::TX_SCHEMA,
        }
    }
}

impl FromStr for DataKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "blocks" | "block" => Ok(DataKind::Blocks),
            "txes" | "tx" | "transactions" | "transaction" => Ok(DataKind::Transactions),
            _ => Err(format!("Unknown data kind: {}", s)),
        }
    }
}
