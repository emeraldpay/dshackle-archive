use std::str::FromStr;
use apache_avro::Schema;
use serde::{Deserialize, Serialize};
use crate::args::Args;
use crate::avros;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DataKind {
    #[serde(rename = "blocks")]
    Blocks,
    #[serde(rename = "transactions")]
    Transactions,
    #[serde(rename = "traces")]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataFile {
    Blocks,
    Transactions,
    TransactionTraces,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFiles {
    pub files: Vec<DataFile>,
}

impl DataFiles {
    pub fn new(files: Vec<DataFile>) -> Self {
        let mut files = files.clone();
        let _ = files.sort();
        let _ = files.dedup();
        Self { files }
    }
    pub fn include(&self, kind: DataFile) -> bool {
        self.files.contains(&kind)
    }
}

impl Default for DataFiles {
    fn default() -> Self {
        Self {
            files: vec![DataFile::Blocks, DataFile::Transactions],
        }
    }
}

impl FromStr for DataFiles {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut files = Vec::new();
        for part in s.to_lowercase().split(',') {
            match part.trim() {
                "blocks" | "block" => files.push(DataFile::Blocks),
                "txes" | "tx" | "transactions" | "transaction" => files.push(DataFile::Transactions),
                "traces" | "trace" => files.push(DataFile::TransactionTraces),
                _ => return Err(()),
            }
        }
        if files.is_empty() {
            return Err(());
        }
        let _ = files.sort();
        let _ = files.dedup();
        Ok(Self { files })
    }
}

impl From<&Args> for DataFiles {
    fn from(value: &Args) -> Self {
        if let Some(files_str) = &value.files {
            match DataFiles::from_str(files_str) {
                Ok(files) => {
                    return files;
                }
                Err(_) => {
                    tracing::warn!("Unable to parse files list '{}'. Using default (blocks,txes)", files_str);
                }
            }
        }
        DataFiles::default()
    }
}

#[derive(Debug, Clone)]
pub struct DataOptions {
    pub files: DataFiles,
    pub block: Option<()>,
    pub tx: Option<()>,
    pub trace: Option<TraceOptions>,
}

impl DataOptions {
    pub fn include_block(&self) -> bool {
        self.block.is_some()
    }
    pub fn include_tx(&self) -> bool {
        self.tx.is_some()
    }
    pub fn include_trace(&self) -> bool {
        self.trace.is_some()
    }
}

impl Default for DataOptions {
    fn default() -> Self {
        Self {
            files: DataFiles::default(),
            block: Some(()),
            tx: Some(()),
            trace: None,
        }
    }
}


impl From<&Args> for DataOptions {
    fn from(value: &Args) -> Self {
        let files = DataFiles::from(value);
        let include_block = files.include(DataFile::Blocks);
        let block = if include_block { Some(()) } else { None };
        let include_tx = files.include(DataFile::Transactions);
        let tx = if include_tx { Some(()) } else { None };
        let include_trace = files.include(DataFile::TransactionTraces);
        let trace = if include_trace {
            if let Some(trace_str) = &value.include_trace {
                Some(TraceOptions::from_str(trace_str).expect("Unable to parse include trace option"))
            } else {
                Some(TraceOptions::default())
            }
        } else {
            None
        };
        Self {
            files,
            block,
            tx,
            trace,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceOptions {
    /// if tx-archive should include callTracer JSON
    pub include_trace: bool,
    /// if tx-archive should include prestateTracer JSON
    pub include_state_diff: bool,
}

impl Default for TraceOptions {
    fn default() -> Self {
        Self {
            include_trace: true,
            include_state_diff: true,
        }
    }
}

impl FromStr for TraceOptions {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase().split(",").map(str::trim).map(String::from).collect::<Vec<_>>();
        let include_trace = s.contains(&"calls".to_string());
        let include_state_diff = s.contains(&"statediff".to_string());
        if !include_trace && !include_state_diff {
            return Err(anyhow::anyhow!("At least one of 'calls' or 'stateDiff' must be specified for trace options"));
        }
        Ok(Self {
            include_trace,
            include_state_diff,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datafiles_from_str_blocks() {
        let df = DataFiles::from_str("blocks").unwrap();
        assert_eq!(df.files, vec![DataFile::Blocks]);
    }

    #[test]
    fn test_datafiles_from_str_transactions() {
        let df = DataFiles::from_str("tx,transactions").unwrap();
        assert_eq!(df.files, vec![DataFile::Transactions]);
    }

    #[test]
    fn test_datafiles_from_str_traces() {
        let df = DataFiles::from_str("trace,traces").unwrap();
        assert_eq!(df.files, vec![DataFile::TransactionTraces]);
    }

    #[test]
    fn test_datafiles_from_str_mixed() {
        let df = DataFiles::from_str("blocks,tx,trace").unwrap();
        assert_eq!(
            df.files,
            vec![
                DataFile::Blocks,
                DataFile::Transactions,
                DataFile::TransactionTraces
            ]
        );
    }

    #[test]
    fn test_datafiles_from_str_duplicates_and_spaces() {
        let df = DataFiles::from_str("blocks, block, tx, txes, trace, traces").unwrap();
        assert_eq!(
            df.files,
            vec![
                DataFile::Blocks,
                DataFile::Transactions,
                DataFile::TransactionTraces
            ]
        );
    }

    #[test]
    fn test_datafiles_from_str_invalid() {
        assert!(DataFiles::from_str("foo,bar").is_err());
        assert!(DataFiles::from_str("").is_err());
    }

    #[test]
    fn test_traceoptions_from_str_calls() {
        let opts = TraceOptions::from_str("calls").unwrap();
        assert!(opts.include_trace);
        assert!(!opts.include_state_diff);
    }

    #[test]
    fn test_traceoptions_from_str_state_diff() {
        let opts = TraceOptions::from_str("stateDiff").unwrap();
        assert!(!opts.include_trace);
        assert!(opts.include_state_diff);
    }

    #[test]
    fn test_traceoptions_from_str_both() {
        let opts = TraceOptions::from_str("calls,stateDiff").unwrap();
        assert!(opts.include_trace);
        assert!(opts.include_state_diff);
    }

    #[test]
    fn test_traceoptions_from_str_invalid() {
        assert!(TraceOptions::from_str("foo").is_err());
        assert!(TraceOptions::from_str("").is_err());
    }

    #[test]
    fn test_dataoptions_from_args_default() {
        let args = Args {
            files: None,
            include_trace: None,
            ..Default::default()
        };
        let opts = DataOptions::from(&args);
        assert!(opts.include_block());
        assert!(opts.include_tx());
        assert!(!opts.include_trace());
    }

    #[test]
    fn test_dataoptions_from_args_blocks_and_tx() {
        let args = Args {
            files: Some("blocks,tx".to_string()),
            include_trace: None,
            ..Default::default()
        };
        let opts = DataOptions::from(&args);
        assert!(opts.include_block());
        assert!(opts.include_tx());
        assert!(!opts.include_trace());
    }

    #[test]
    fn test_dataoptions_from_args_with_trace_default() {
        let args = Args {
            files: Some("blocks,tx,trace".to_string()),
            include_trace: None,
            ..Default::default()
        };
        let opts = DataOptions::from(&args);
        assert!(opts.include_block());
        assert!(opts.include_tx());
        assert!(opts.include_trace());
        assert_eq!(opts.trace, Some(TraceOptions::default()));
    }

    #[test]
    fn test_dataoptions_from_args_with_trace_custom() {
        let args = Args {
            files: Some("trace".to_string()),
            include_trace: Some("calls".to_string()),
            ..Default::default()
        };
        let opts = DataOptions::from(&args);
        assert!(!opts.include_block());
        assert!(!opts.include_tx());
        assert!(opts.include_trace());
        let trace_opts = opts.trace.unwrap();
        assert!(trace_opts.include_trace);
        assert!(!trace_opts.include_state_diff);
    }

    #[test]
    fn test_dataoptions_from_args_invalid_files() {
        let args = Args {
            files: Some("foo,bar".to_string()),
            include_trace: None,
            ..Default::default()
        };
        let opts = DataOptions::from(&args);
        // Should fallback to default
        assert!(opts.include_block());
        assert!(opts.include_tx());
        assert!(!opts.include_trace());
    }
}
