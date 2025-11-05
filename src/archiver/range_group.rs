use std::collections::HashMap;
use thiserror::Error;
use crate::archiver::datakind::{DataKind, DataTables};
use crate::archiver::range::Range;
use crate::storage::FileReference;

///
/// A group of files for the same range
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveGroup {
    pub range: Range,
    expect_tables: DataTables,
    pub blocks: Option<FileReference>,
    pub txes: Option<FileReference>,
    pub traces: Option<FileReference>,
}

#[derive(Error, Debug)]
pub enum RangeGroupError {
    #[error("Table already exists for the range: {}", .0.path)]
    Duplicate(FileReference, FileReference),
    #[error("File range mismatch: expected {0}, got {1}")]
    DifferentRange(Range, Range),
}

impl ArchiveGroup {

    ///
    /// @expect_trace_files - if true, the group is considered complete only if trace files are present
    pub fn new(range: Range, expect_tables: DataTables) -> Self {
        Self {
            range,
            expect_tables,
            blocks: None,
            txes: None,
            traces: None,
        }
    }

    ///
    /// Add a file to the group
    ///
    pub fn with_file(self, file: FileReference) -> Result<Self, RangeGroupError> {
        if file.range != self.range {
            return Err(RangeGroupError::DifferentRange(self.range, file.range));
        }
        let merged = match file.kind {
            DataKind::Blocks => {
                if self.blocks.is_some() {
                    return Err(RangeGroupError::Duplicate(self.blocks.unwrap(), file));
                }
                Self {
                    blocks: Some(file),
                    ..self
                }
            },
            DataKind::Transactions => {
                if self.txes.is_some() {
                    return Err(RangeGroupError::Duplicate(self.txes.unwrap(), file));
                }
                Self {
                    txes: Some(file),
                    ..self
                }
            },
            DataKind::TransactionTraces => {
                if self.traces.is_some() {
                    return Err(RangeGroupError::Duplicate(self.traces.unwrap(), file));
                }

                Self {
                    traces: Some(file),
                    ..self
                }
            },
        };
        Ok(merged)
    }

    ///
    /// Checks if all the data kinds are present
    ///
    pub fn is_complete(&self) -> bool {
        if self.expect_tables.include(DataKind::Blocks) && self.blocks.is_none() {
            return false;
        }
        if self.expect_tables.include(DataKind::Transactions) && self.txes.is_none() {
            return false;
        }
        if self.expect_tables.include(DataKind::TransactionTraces) && self.traces.is_none() {
            return false;
        }
        true
    }

    ///
    /// List all missing data kinds
    ///
    pub fn get_incomplete_kinds(&self) -> Vec<DataKind> {
        let mut missing = Vec::new();
        if self.expect_tables.include(DataKind::Blocks) && self.blocks.is_none() {
            missing.push(DataKind::Blocks);
        }
        if self.expect_tables.include(DataKind::Transactions) && self.txes.is_none() {
            missing.push(DataKind::Transactions);
        }
        if self.expect_tables.include(DataKind::TransactionTraces) && self.traces.is_none() {
            missing.push(DataKind::TransactionTraces);
        }
        missing
    }

    ///
    /// List all files in the group
    pub fn tables(&self) -> Vec<&FileReference> {
        let mut files = Vec::new();
        if let Some(blocks) = &self.blocks {
            files.push(blocks);
        }
        if let Some(txes) = &self.txes {
            files.push(txes);
        }
        if let Some(traces) = &self.traces {
            files.push(traces);
        }
        files
    }
}

///
/// A list of archive groups. Supposed to be used for checking a range to find out what data is in the archive
pub struct ArchivesList {
    expect_tables: DataTables,
    current: HashMap<Range, ArchiveGroup>,
}


impl ArchivesList {

    ///
    /// expect_trace_files - if true, the groups are considered complete only if trace files are present
    pub fn new(expect_tables: DataTables) -> Self {
        Self {
            expect_tables,
            current: HashMap::new(),
        }
    }

    ///
    /// Append to the current list or update the current group. Returns `true` if the group is complete
    pub fn append(&mut self, file: FileReference) -> Result<bool, RangeGroupError> {
        let range = file.range.clone();

        let current = self.current.remove(&range)
            .unwrap_or(ArchiveGroup::new(range.clone(), self.expect_tables.clone()));

        let updated = current.with_file(file)?;
        let is_complete = updated.is_complete();
        self.current.insert(range, updated);
        Ok(is_complete)
    }

    pub fn remove_all(&mut self, range: &Range) -> Option<ArchiveGroup> {
        self.current.remove(range)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ArchiveGroup> {
        self.current.values()
    }

    pub fn all(self) -> Vec<ArchiveGroup> {
        self.current.into_iter().map(|(_, group)| group).collect()
    }

    ///
    /// Get all file groups that are missing one or more DataKinds in their range
    /// Ex., called after appending all the files in the ranges.
    pub fn list_incomplete(&self) -> Vec<&ArchiveGroup> {
        self.current.values()
            .filter(|g| !g.is_complete())
            .collect()
    }

}

impl Default for ArchivesList {
    fn default() -> Self {
        Self::new(DataTables::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::FileReference;
    use crate::archiver::datakind::DataKind;
    use crate::archiver::range::Range;

    #[test]
    fn test_append_new_file() {
        let mut archives_list = ArchivesList::default();
        let range = Range::new(0, 10);
        let file = FileReference {
            range: range.clone(),
            kind: DataKind::Blocks,
            path: "file1".to_string(),
        };

        let is_complete = archives_list.append(file).unwrap();
        assert!(!is_complete);
        assert_eq!(archives_list.current.len(), 1);
    }

    #[test]
    fn test_append_complete_group() {
        let mut archives_list = ArchivesList::default();
        let range = Range::new(0, 10);
        let file_blocks = FileReference {
            range: range.clone(),
            kind: DataKind::Blocks,
            path: "file1".to_string(),
        };
        let file_txes = FileReference {
            range: range.clone(),
            kind: DataKind::Transactions,
            path: "file2".to_string(),
        };

        archives_list.append(file_blocks).unwrap();
        let is_complete = archives_list.append(file_txes).unwrap();
        assert!(is_complete);
        assert_eq!(archives_list.current.len(), 1);
    }

    #[test]
    fn test_append_complete_with_traces_group() {
        let mut archives_list = ArchivesList::new(DataTables::new(vec![DataKind::Blocks, DataKind::Transactions, DataKind::TransactionTraces]));
        let range = Range::new(0, 10);
        let file_blocks = FileReference {
            range: range.clone(),
            kind: DataKind::Blocks,
            path: "file1".to_string(),
        };
        let file_txes = FileReference {
            range: range.clone(),
            kind: DataKind::Transactions,
            path: "file2".to_string(),
        };

        archives_list.append(file_blocks).unwrap();
        let is_complete = archives_list.append(file_txes).unwrap();
        assert!(!is_complete);

        let file_traces = FileReference {
            range: range.clone(),
            kind: DataKind::TransactionTraces,
            path: "file3".to_string(),
        };
        let is_complete = archives_list.append(file_traces).unwrap();
        assert!(is_complete);
        assert_eq!(archives_list.current.len(), 1);
    }

    #[test]
    fn test_remove_group() {
        let mut archives_list = ArchivesList::default();
        let range = Range::new(0, 10);
        let file = FileReference {
            range: range.clone(),
            kind: DataKind::Blocks,
            path: "file1".to_string(),
        };

        archives_list.append(file).unwrap();
        let removed = archives_list.remove_all(&range);
        assert!(removed.is_some());
        assert_eq!(archives_list.current.len(), 0);
    }

    #[test]
    fn test_iter() {
        let mut archives_list = ArchivesList::default();
        let range1 = Range::new(0, 10);
        let range2 = Range::new(11, 20);
        let file1 = FileReference {
            range: range1.clone(),
            kind: DataKind::Blocks,
            path: "file1".to_string(),
        };
        let file2 = FileReference {
            range: range2.clone(),
            kind: DataKind::Blocks,
            path: "file2".to_string(),
        };
        let file3 = FileReference {
            range: range2.clone(),
            kind: DataKind::Transactions,
            path: "file3".to_string(),
        };

        archives_list.append(file1).unwrap();
        archives_list.append(file2).unwrap();
        archives_list.append(file3).unwrap();

        let mut all: Vec<Range> = archives_list.iter()
            .map(|x| x.range.clone())
            .collect();
        all.sort();
        assert!(all.contains(&range1));
        assert!(all.contains(&range2));
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_list_incomplete_empty() {
        let archives_list = ArchivesList::default();
        let incomplete = archives_list.list_incomplete();
        assert!(incomplete.is_empty());
    }

    #[test]
    fn test_list_incomplete_all_complete() {
        let mut archives_list = ArchivesList::default();
        let range = Range::new(0, 10);
        let file_blocks = FileReference {
            range: range.clone(),
            kind: DataKind::Blocks,
            path: "file1".to_string(),
        };
        let file_txes = FileReference {
            range: range.clone(),
            kind: DataKind::Transactions,
            path: "file2".to_string(),
        };
        archives_list.append(file_blocks).unwrap();
        archives_list.append(file_txes).unwrap();
        let incomplete = archives_list.list_incomplete();
        assert!(incomplete.is_empty());
    }

    #[test]
    fn test_list_incomplete_some_incomplete() {
        let mut archives_list = ArchivesList::default();
        let range1 = Range::new(0, 10);
        let range2 = Range::new(11, 20);
        let file1 = FileReference {
            range: range1.clone(),
            kind: DataKind::Blocks,
            path: "file1".to_string(),
        };
        let file2 = FileReference {
            range: range2.clone(),
            kind: DataKind::Blocks,
            path: "file2".to_string(),
        };
        let file3 = FileReference {
            range: range2.clone(),
            kind: DataKind::Transactions,
            path: "file3".to_string(),
        };
        archives_list.append(file1).unwrap(); // incomplete
        archives_list.append(file2).unwrap();
        archives_list.append(file3).unwrap(); // complete
        let incomplete = archives_list.list_incomplete();
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].range, range1);
    }
}
