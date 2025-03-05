use std::collections::HashMap;
use async_trait::async_trait;
use anyhow::Result;
use crate::range::Range;
use crate::storage::FileReference;

pub mod stream;
pub mod fix;
pub mod archiver;
pub mod verify;
pub mod archive;

///
/// A base trait for Dshackle Archive commands (i.e., for `stream`, `archive`, `compact`, etc.)
#[async_trait]
pub trait CommandExecutor {

    ///
    /// Executes the command
    async fn execute(&self) -> Result<()>;
}

#[derive(Clone)]
pub enum Blocks {
    Tail(u64),
    Range(Range),
}

///
/// A group of files for the same range
///
#[derive(Clone, Debug)]
pub struct ArchiveGroup {
    pub range: Range,
    pub blocks: Option<FileReference>,
    pub txes: Option<FileReference>,
}

impl ArchiveGroup {
    pub fn new(range: Range) -> Self {
        Self {
            range,
            blocks: None,
            txes: None,
        }
    }

    ///
    /// Add a file to the group
    ///
    pub fn with_file(self, file: FileReference) -> Result<Self> {
        if file.range != self.range {
            return Err(anyhow::anyhow!("File range mismatch: expected {:?}, got {:?}", self.range, file.range));
        }
        let merged = match file.kind {
            crate::datakind::DataKind::Blocks => Self {
                blocks: Some(file),
                ..self
            },
            crate::datakind::DataKind::Transactions => Self {
                txes: Some(file),
                ..self
            },
        };
        Ok(merged)
    }

    ///
    /// Checks if all the data kinds are present
    ///
    pub fn is_complete(&self) -> bool {
        self.blocks.is_some() && self.txes.is_some()
    }

    ///
    /// List all files in the group
    pub fn files(&self) -> Vec<&FileReference> {
        let mut files = Vec::new();
        if let Some(blocks) = &self.blocks {
            files.push(blocks);
        }
        if let Some(txes) = &self.txes {
            files.push(txes);
        }
        files
    }
}

///
/// A list of archive groups. Supposed to be used for checking a range to find out what data is in the archive
pub struct ArchivesList {
    current: HashMap<Range, ArchiveGroup>,
}

impl ArchivesList {
    pub fn new() -> Self {
        Self {
            current: HashMap::new(),
        }
    }

    ///
    /// Append to the current list or update the current group. Returns `true` if the group is complete
    pub fn append(&mut self, file: FileReference) -> Result<bool> {
        let range = file.range.clone();

        let current = self.current.remove(&range)
            .unwrap_or(ArchiveGroup::new(range.clone()));

        let updated = current.with_file(file)?;
        let is_complete = updated.is_complete();
        self.current.insert(range, updated);
        Ok(is_complete)
    }

    pub fn remove(&mut self, range: &Range) -> Option<ArchiveGroup> {
        self.current.remove(range)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ArchiveGroup> {
        self.current.values()
    }

    pub fn all(self) -> Vec<ArchiveGroup> {
        self.current.into_iter().map(|(_, group)| group).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::FileReference;
    use crate::datakind::DataKind;
    use crate::range::Range;

    #[test]
    fn test_append_new_file() {
        let mut archives_list = ArchivesList::new();
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
        let mut archives_list = ArchivesList::new();
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
    fn test_remove_group() {
        let mut archives_list = ArchivesList::new();
        let range = Range::new(0, 10);
        let file = FileReference {
            range: range.clone(),
            kind: DataKind::Blocks,
            path: "file1".to_string(),
        };

        archives_list.append(file).unwrap();
        let removed = archives_list.remove(&range);
        assert!(removed.is_some());
        assert_eq!(archives_list.current.len(), 0);
    }

    #[test]
    fn test_iter() {
        let mut archives_list = ArchivesList::new();
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
}
