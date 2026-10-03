// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Quick verification for the `verify` command (`--quick`).
//!
//! Reading every table of a large archive takes hours, and most of that time goes to data that is fine.
//! The quick verification uses only what the listing of the storage tells: which tables exist for
//! a range and how large they are. It trusts that the range in a file name is the range of blocks
//! in that file, so it finds a lost or an empty table, but not a missing block or a broken record
//! inside of one.

use crate::archiver::datakind::DataOptions;
use crate::archiver::range::Range;
use crate::archiver::range_group::ArchiveGroup;
use super::RangeVerification;

impl<'a> RangeVerification<'a> {

    ///
    /// Verify the range by the listed files only, without opening them.
    ///
    /// A table is broken when its file is empty. A table of unknown size is kept, and makes the result
    /// incomplete, the same way as a table that could not be read by the full verification.
    pub(super) fn by_listing(range: &Range, groups: &'a [ArchiveGroup], data_options: &DataOptions) -> Self {
        let has_blocks = groups.iter().any(|g| g.blocks.is_some());
        if !has_blocks || !data_options.include_block() {
            return Self::missing_blocks(range, groups);
        }

        let expected = data_options.files();
        let mut broken = vec![];
        let mut incomplete = false;
        let tables = groups.iter()
            .flat_map(|g| g.tables())
            .filter(|table| expected.include(table.kind));
        for table in tables {
            match table.size {
                Some(0) => {
                    tracing::error!(range = %table.range, "Empty file: {}", table.path);
                    broken.push(table);
                },
                Some(_) => {},
                None => {
                    tracing::warn!(range = %table.range, "Unknown size of the file, keeping it as is: {}", table.path);
                    incomplete = true;
                }
            }
        }

        Self { broken, incomplete }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use futures_util::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
    use crate::archiver::Archiver;
    use crate::archiver::datakind::{DataKind, DataOptions, DataTables};
    use crate::archiver::filenames::Filenames;
    use crate::archiver::range::Range;
    use crate::archiver::range_group::ArchiveGroup;
    use crate::args::Args;
    use crate::blockchain::mock::{MockData, MockType};
    use crate::command::CommandExecutor;
    use crate::command::verify::{RangeVerification, VerifyCommand};
    use crate::storage::FileReference;
    use crate::storage::objects::ObjectsStorage;
    use crate::testing;

    fn table(kind: DataKind, range: &Range, size: Option<u64>) -> FileReference {
        FileReference {
            path: format!("{}-{}", range, kind),
            kind,
            range: range.clone(),
            size,
        }
    }

    fn group(range: &Range, tables: Vec<FileReference>) -> ArchiveGroup {
        let expect = DataTables::new(vec![DataKind::Blocks, DataKind::Transactions]);
        tables.into_iter()
            .fold(ArchiveGroup::new(range.clone(), expect), |group, file| group.with_file(file).unwrap())
    }

    #[test]
    fn keeps_non_empty_tables() {
        let range = Range::new(100, 109);
        let groups = vec![group(&range, vec![
            table(DataKind::Blocks, &range, Some(1024)),
            table(DataKind::Transactions, &range, Some(1)),
        ])];

        let verification = RangeVerification::by_listing(&range, &groups, &DataOptions::default());

        assert!(verification.broken.is_empty());
        assert!(!verification.incomplete);
    }

    #[test]
    fn finds_empty_table() {
        let range = Range::new(100, 109);
        let groups = vec![group(&range, vec![
            table(DataKind::Blocks, &range, Some(1024)),
            table(DataKind::Transactions, &range, Some(0)),
        ])];

        let verification = RangeVerification::by_listing(&range, &groups, &DataOptions::default());

        assert_eq!(verification.broken.len(), 1);
        assert_eq!(verification.broken[0].kind, DataKind::Transactions);
        assert!(!verification.incomplete);
    }

    #[test]
    fn finds_tables_without_blocks() {
        let range = Range::new(100, 109);
        let groups = vec![group(&range, vec![
            table(DataKind::Transactions, &range, Some(1024)),
        ])];

        let verification = RangeVerification::by_listing(&range, &groups, &DataOptions::default());

        assert_eq!(verification.broken.len(), 1);
        assert_eq!(verification.broken[0].kind, DataKind::Transactions);
    }

    #[test]
    fn keeps_table_of_unknown_size() {
        let range = Range::new(100, 109);
        let groups = vec![group(&range, vec![
            table(DataKind::Blocks, &range, Some(1024)),
            table(DataKind::Transactions, &range, None),
        ])];

        let verification = RangeVerification::by_listing(&range, &groups, &DataOptions::default());

        assert!(verification.broken.is_empty());
        assert!(verification.incomplete);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deletes_only_empty_file_without_reading_content() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());

        // not an Avro file, i.e. the full verification would delete all of them
        let unreadable = [
            "archive/eth/000000000/range-000000100_000000109.blocks.avro",
            "archive/eth/000000000/range-000000100_000000109.txes.avro",
            "archive/eth/000000000/range-000000110_000000119.blocks.avro",
        ];
        for path in unreadable {
            mem.put(&Path::from(path), PutPayload::from_static(b"not an avro")).await.unwrap();
        }
        let empty = "archive/eth/000000000/range-000000110_000000119.txes.avro";
        mem.put(&Path::from(empty), PutPayload::from_static(&[])).await.unwrap();

        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/eth".to_string()));
        let archiver: Archiver<MockType, ObjectsStorage<InMemory>> = Archiver::new_simple(
            Arc::new(storage),
            Arc::new(MockData::new("test")),
        );
        let args = Args {
            range: Some("100..119".to_string()),
            range_chunk: Some(10),
            quick: true,
            ..Default::default()
        };

        VerifyCommand::new(&args, archiver).unwrap()
            .execute().await.unwrap();

        let mut left: Vec<String> = mem.list(None)
            .map(|meta| meta.unwrap().location.to_string())
            .collect().await;
        left.sort();
        assert_eq!(left, unreadable.iter().map(|p| p.to_string()).collect::<Vec<_>>());
    }
}
