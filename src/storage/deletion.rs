// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0
//! Deleting many archive files at once, see [`super::ReadTarget::delete_all`].

use crate::storage::FileReference;

/// How many files are deleted at the same time by a storage that deletes them one by one, i.e., the filesystem.
/// More than that gives little, as each of them is a short operation.
pub(super) const DELETE_PARALLEL: usize = 32;

/// The longest reason kept for a failure. An error of a batch request may list all the files of the batch, which
/// makes it a line of tens of kilobytes in the log and in the report, while the files are listed separately anyway.
const MAX_REASON_LENGTH: usize = 300;

///
/// Some of the files are not deleted, i.e., they are still in the archive or nobody knows if they are
#[derive(thiserror::Error, Debug)]
#[error("{} files are not deleted: {reason}", files.len())]
pub struct DeleteFailure {
    /// The requested files that the storage didn't confirm as deleted
    pub files: Vec<FileReference>,
    /// The first error reported by the storage, shortened. It's an example and not always about a particular file,
    /// as a request that deletes a batch of files may fail as a whole.
    pub reason: String,
}

impl DeleteFailure {
    ///
    /// The outcome of a deletion that left the `not_deleted` files, with the first error the storage reported, if any.
    /// A file may be left without an error too, when the storage just didn't confirm it.
    pub(super) fn check(not_deleted: Vec<FileReference>, first_error: Option<String>) -> Result<(), DeleteFailure> {
        if not_deleted.is_empty() {
            return Ok(());
        }
        let reason = match first_error {
            Some(reason) if reason.chars().count() > MAX_REASON_LENGTH => {
                format!("{}...", reason.chars().take(MAX_REASON_LENGTH).collect::<String>())
            },
            Some(reason) => reason,
            None => "the storage didn't confirm the deletion".to_string(),
        };
        Err(DeleteFailure { files: not_deleted, reason })
    }
}

#[cfg(test)]
mod tests {
    use crate::archiver::datakind::DataKind;
    use crate::archiver::range::Range;
    use crate::storage::FileReference;
    use super::{DeleteFailure, MAX_REASON_LENGTH};

    fn file(height: u64) -> FileReference {
        FileReference::new(format!("{}.block.avro", height), DataKind::Blocks, Range::single(height))
    }

    #[test]
    fn ok_when_nothing_left() {
        assert!(DeleteFailure::check(vec![], None).is_ok());
    }

    #[test]
    fn lists_files_not_deleted_with_first_error() {
        let failure = DeleteFailure::check(vec![file(101), file(102)], Some("SlowDown".to_string()))
            .unwrap_err();

        assert_eq!(failure.files, vec![file(101), file(102)]);
        assert_eq!(failure.to_string(), "2 files are not deleted: SlowDown");
    }

    #[test]
    fn unconfirmed_file_is_not_deleted() {
        let failure = DeleteFailure::check(vec![file(100)], None)
            .unwrap_err();

        assert_eq!(failure.reason, "the storage didn't confirm the deletion");
    }

    #[test]
    fn shortens_long_reason() {
        let paths = (0..1000).map(|i| format!("archive/eth/{:09}.block.avro", i)).collect::<Vec<String>>().join(",");

        let failure = DeleteFailure::check(vec![file(100)], Some(format!("Access denied: {}", paths)))
            .unwrap_err();

        assert_eq!(failure.reason.chars().count(), MAX_REASON_LENGTH + 3);
        assert!(failure.reason.starts_with("Access denied: archive/eth/000000000.block.avro,"));
        assert!(failure.reason.ends_with("..."));
    }
}
