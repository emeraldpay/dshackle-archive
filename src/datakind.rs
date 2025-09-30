use std::str::FromStr;
use apache_avro::Schema;
use serde::{Deserialize, Serialize};
use crate::avros;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataKind {
    Blocks,
    Transactions,
    TransactionTraces,
}

impl DataKind {
    pub fn schema(&self) -> &'static Schema {
        match self {
            DataKind::Blocks => &avros::BLOCK_SCHEMA,
            DataKind::Transactions => &avros::TX_SCHEMA,
            DataKind::TransactionTraces => &avros::TX_TRACE_SCHEMA,
        }
    }
}

impl FromStr for DataKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "blocks" | "block" => Ok(DataKind::Blocks),
            "txes" | "tx" | "transactions" | "transaction" => Ok(DataKind::Transactions),
            "traces" | "trace" => Ok(DataKind::TransactionTraces),
            _ => Err(format!("Unknown data kind: {}", s)),
        }
    }
}
