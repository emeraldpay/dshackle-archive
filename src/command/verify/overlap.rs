// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Cleaning of the overlapping ranges for the `verify` command.
//!
//! Two groups of files that cover the same block are the same records archived twice, and whoever reads the
//! archive gets them as duplicates. It's what stays after an interrupted compaction, or after archiving a range
//! again with another chunk size. Only one of such groups can stay, even when the data is valid in all of them.

use std::cmp::Ordering;
use crate::archiver::range::Range;
use crate::archiver::range_group::ArchiveGroup;
use super::Preprocess;

///
/// Order in which the groups claim their range, i.e. the first one stays and the ones overlapping with it are deleted.
fn by_preference(a: &ArchiveGroup, b: &ArchiveGroup) -> Ordering {
    // A group missing a table is not usable as it is, and it's going to be archived again anyway.
    // So it should not replace a complete group, even a smaller one.
    b.is_complete().cmp(&a.is_complete())
        .then(b.range.len().cmp(&a.range.len()))
        .then(a.range.start().cmp(&b.range.start()))
}

impl Preprocess {

    ///
    /// Leave only the groups that don't overlap with each other, and mark all others for deletion.
    ///
    /// Of the overlapping groups it keeps a complete one, then the one covering more blocks, then the one that starts earlier.
    /// The group reaching in from the previous chunks takes part on the same terms, so it can be deleted here too.
    pub(super) fn deduplicate(&mut self) {
        let earlier = self.reaching_in.take();
        let mut candidates = self.inputs();
        // A listing may return the files of the earlier group once more, and it's not a duplicate of itself.
        // The earlier one is what stays, because it's verified, and has no tables deleted by that verification
        candidates.retain(|g| earlier.as_ref().is_none_or(|earlier| earlier.range != g.range));
        candidates.extend(earlier.iter().cloned());
        candidates.sort_by(by_preference);

        let mut kept: Vec<Range> = vec![];
        for group in candidates {
            if let Some(other) = kept.iter().find(|range| range.is_intersected_with(&group.range)) {
                tracing::info!(range = %group.range, "Delete the group as a duplicate of {}", other);
                self.delete_all(group);
            } else {
                kept.push(group.range.clone());
                if earlier.as_ref() == Some(&group) {
                    self.reaching_in = Some(group);
                } else {
                    self.input.push(group);
                }
            }
        }
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
    use crate::archiver::datakind::{DataKind, DataTables};
    use crate::archiver::filenames::Filenames;
    use crate::archiver::range::Range;
    use crate::archiver::range_group::ArchiveGroup;
    use crate::args::Args;
    use crate::blockchain::mock::{MockData, MockType};
    use crate::command::CommandExecutor;
    use crate::command::verify::{Preprocess, VerifyCommand};
    use crate::storage::FileReference;
    use crate::storage::objects::ObjectsStorage;
    use crate::testing;

    fn group(range: Range, kinds: &[DataKind]) -> ArchiveGroup {
        let expect = DataTables::new(vec![DataKind::Blocks, DataKind::Transactions]);
        kinds.iter().fold(ArchiveGroup::new(range.clone(), expect), |group, kind| {
            let file = FileReference::new(format!("{}-{}", range, kind), *kind, range.clone());
            group.with_file(file).unwrap()
        })
    }

    fn complete(range: Range) -> ArchiveGroup {
        group(range, &[DataKind::Blocks, DataKind::Transactions])
    }

    fn deduplicate(input: Vec<ArchiveGroup>, reaching_in: Option<ArchiveGroup>) -> Preprocess {
        let mut data = Preprocess { input, reaching_in, for_deletion: vec![] };
        data.deduplicate();
        data
    }

    fn kept_ranges(data: &Preprocess) -> Vec<Range> {
        let mut ranges: Vec<Range> = data.input.iter().map(|g| g.range.clone()).collect();
        ranges.sort();
        ranges
    }

    fn deleted_ranges(data: &Preprocess) -> Vec<Range> {
        let mut ranges: Vec<Range> = data.for_deletion.iter().map(|f| f.range.clone()).collect();
        ranges.sort();
        ranges.dedup();
        ranges
    }

    #[test]
    fn keeps_groups_without_overlap() {
        let data = deduplicate(vec![
            complete(Range::new(100, 199)),
            complete(Range::new(200, 299)),
            complete(Range::single(300)),
        ], None);

        assert_eq!(kept_ranges(&data), vec![Range::new(100, 199), Range::new(200, 299), Range::single(300)]);
        assert!(data.for_deletion.is_empty());
    }

    #[test]
    fn deletes_smaller_of_two_overlapping() {
        let data = deduplicate(vec![
            complete(Range::new(100, 199)),
            complete(Range::new(150, 299)),
        ], None);

        assert_eq!(kept_ranges(&data), vec![Range::new(150, 299)]);
        assert_eq!(deleted_ranges(&data), vec![Range::new(100, 199)]);
        // both tables of the deleted group
        assert_eq!(data.for_deletion.len(), 2);
    }

    #[test]
    fn deletes_smaller_of_two_overlapping_in_any_order() {
        let data = deduplicate(vec![
            complete(Range::new(150, 299)),
            complete(Range::new(100, 199)),
        ], None);

        assert_eq!(kept_ranges(&data), vec![Range::new(150, 299)]);
        assert_eq!(deleted_ranges(&data), vec![Range::new(100, 199)]);
    }

    #[test]
    fn deletes_single_blocks_inside_range() {
        let data = deduplicate(vec![
            complete(Range::single(105)),
            complete(Range::new(100, 109)),
            complete(Range::single(109)),
            complete(Range::single(110)),
        ], None);

        assert_eq!(kept_ranges(&data), vec![Range::new(100, 109), Range::single(110)]);
        assert_eq!(deleted_ranges(&data), vec![Range::single(105), Range::single(109)]);
    }

    #[test]
    fn deletes_group_sharing_only_one_block() {
        let data = deduplicate(vec![
            complete(Range::new(100, 199)),
            complete(Range::new(199, 399)),
            complete(Range::new(399, 449)),
        ], None);

        assert_eq!(kept_ranges(&data), vec![Range::new(199, 399)]);
        assert_eq!(deleted_ranges(&data), vec![Range::new(100, 199), Range::new(399, 449)]);
    }

    #[test]
    fn keeps_earlier_of_equal_size() {
        let data = deduplicate(vec![
            complete(Range::new(150, 249)),
            complete(Range::new(100, 199)),
        ], None);

        assert_eq!(kept_ranges(&data), vec![Range::new(100, 199)]);
        assert_eq!(deleted_ranges(&data), vec![Range::new(150, 249)]);
    }

    #[test]
    fn keeps_complete_over_larger_incomplete() {
        let data = deduplicate(vec![
            group(Range::new(100, 199), &[DataKind::Blocks]),
            complete(Range::single(150)),
        ], None);

        assert_eq!(kept_ranges(&data), vec![Range::single(150)]);
        assert_eq!(deleted_ranges(&data), vec![Range::new(100, 199)]);
    }

    #[test]
    fn keeps_largest_of_chain() {
        // the first and the last don't overlap with each other, only with the one in the middle
        let data = deduplicate(vec![
            complete(Range::new(0, 99)),
            complete(Range::new(50, 249)),
            complete(Range::new(200, 299)),
        ], None);

        assert_eq!(kept_ranges(&data), vec![Range::new(50, 249)]);
        assert_eq!(deleted_ranges(&data), vec![Range::new(0, 99), Range::new(200, 299)]);
    }

    #[test]
    fn keeps_smaller_when_larger_is_deleted_by_another() {
        // 90..119 overlaps only with 50..99, which is deleted as a duplicate of 0..59
        let data = deduplicate(vec![
            complete(Range::new(0, 59)),
            complete(Range::new(50, 99)),
            complete(Range::new(90, 119)),
        ], None);

        assert_eq!(kept_ranges(&data), vec![Range::new(0, 59), Range::new(90, 119)]);
        assert_eq!(deleted_ranges(&data), vec![Range::new(50, 99)]);
    }

    #[test]
    fn deletes_duplicate_of_group_from_previous_chunk() {
        let earlier = complete(Range::new(100, 119));
        let data = deduplicate(vec![
            complete(Range::new(110, 119)),
        ], Some(earlier.clone()));

        assert!(data.input.is_empty());
        assert_eq!(data.reaching_in, Some(earlier));
        assert_eq!(deleted_ranges(&data), vec![Range::new(110, 119)]);
    }

    #[test]
    fn deletes_group_from_previous_chunk_as_duplicate_of_larger() {
        let data = deduplicate(vec![
            complete(Range::new(100, 199)),
        ], Some(complete(Range::new(95, 104))));

        assert_eq!(kept_ranges(&data), vec![Range::new(100, 199)]);
        assert_eq!(data.reaching_in, None);
        assert_eq!(deleted_ranges(&data), vec![Range::new(95, 104)]);
    }

    #[test]
    fn keeps_group_from_previous_chunk_out_of_verification() {
        let earlier = complete(Range::new(100, 119));
        let data = deduplicate(vec![
            complete(Range::new(120, 129)),
        ], Some(earlier.clone()));

        assert_eq!(kept_ranges(&data), vec![Range::new(120, 129)]);
        assert_eq!(data.reaching_in, Some(earlier));
        assert!(data.for_deletion.is_empty());
    }

    #[test]
    fn keeps_group_from_previous_chunk_when_listed_again() {
        let earlier = complete(Range::new(100, 119));
        let data = deduplicate(vec![
            earlier.clone(),
        ], Some(earlier.clone()));

        assert!(data.input.is_empty());
        assert_eq!(data.reaching_in, Some(earlier));
        assert!(data.for_deletion.is_empty());
    }

    // The files below are not Avro, so the tests run the quick verification which doesn't read them

    async fn verify_quick(files: &[(&str, &'static [u8])], range: &str) -> Vec<String> {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        for (path, content) in files {
            mem.put(&Path::from(*path), PutPayload::from_static(content)).await.unwrap();
        }
        let storage = ObjectsStorage::new(mem.clone(), "test".to_string(), Filenames::with_dir("archive/eth".to_string()));
        let archiver: Archiver<MockType, ObjectsStorage<InMemory>> = Archiver::new_simple(
            Arc::new(storage),
            Arc::new(MockData::new("test")),
        );
        let args = Args {
            range: Some(range.to_string()),
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
        left
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deletes_single_block_files_duplicating_range() {
        let left = verify_quick(&[
            ("archive/eth/000000000/range-000000100_000000109.blocks.avro", b"data"),
            ("archive/eth/000000000/range-000000100_000000109.txes.avro", b"data"),
            ("archive/eth/000000000/000000000/000000100.block.avro", b"data"),
            ("archive/eth/000000000/000000000/000000100.txes.avro", b"data"),
            ("archive/eth/000000000/000000000/000000105.block.avro", b"data"),
            ("archive/eth/000000000/000000000/000000105.txes.avro", b"data"),
        ], "100..109").await;

        assert_eq!(left, vec![
            "archive/eth/000000000/range-000000100_000000109.blocks.avro",
            "archive/eth/000000000/range-000000100_000000109.txes.avro",
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deletes_duplicate_of_range_from_previous_chunk() {
        let left = verify_quick(&[
            ("archive/eth/000000000/range-000000100_000000119.blocks.avro", b"data"),
            ("archive/eth/000000000/range-000000100_000000119.txes.avro", b"data"),
            ("archive/eth/000000000/range-000000110_000000119.blocks.avro", b"data"),
            ("archive/eth/000000000/range-000000110_000000119.txes.avro", b"data"),
        ], "100..119").await;

        assert_eq!(left, vec![
            "archive/eth/000000000/range-000000100_000000119.blocks.avro",
            "archive/eth/000000000/range-000000100_000000119.txes.avro",
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn keeps_range_listed_again_by_single_block_chunk() {
        // the last chunk is the block 110 alone, and listing for a single block gives all ranges that cover it
        let left = verify_quick(&[
            ("archive/eth/000000000/range-000000100_000000119.blocks.avro", b"data"),
            ("archive/eth/000000000/range-000000100_000000119.txes.avro", b"data"),
        ], "100..110").await;

        assert_eq!(left, vec![
            "archive/eth/000000000/range-000000100_000000119.blocks.avro",
            "archive/eth/000000000/range-000000100_000000119.txes.avro",
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn keeps_one_of_tables_with_different_names() {
        // `block` is how the blocks of a range were named before, and it's still read as the same table
        let left = verify_quick(&[
            ("archive/eth/000000000/range-000000100_000000109.block.avro", b"data"),
            ("archive/eth/000000000/range-000000100_000000109.blocks.avro", b"data"),
            ("archive/eth/000000000/range-000000100_000000109.traces.avro", b"data"),
            ("archive/eth/000000000/range-000000100_000000109.txes.avro", b"data"),
        ], "100..109").await;

        assert_eq!(left, vec![
            "archive/eth/000000000/range-000000100_000000109.blocks.avro",
            "archive/eth/000000000/range-000000100_000000109.traces.avro",
            "archive/eth/000000000/range-000000100_000000109.txes.avro",
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn keeps_non_empty_table_over_empty_with_current_name() {
        let left = verify_quick(&[
            ("archive/eth/000000000/range-000000100_000000109.block.avro", b"data"),
            ("archive/eth/000000000/range-000000100_000000109.blocks.avro", b""),
            ("archive/eth/000000000/range-000000100_000000109.txes.avro", b"data"),
        ], "100..109").await;

        assert_eq!(left, vec![
            "archive/eth/000000000/range-000000100_000000109.block.avro",
            "archive/eth/000000000/range-000000100_000000109.txes.avro",
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn keeps_first_of_tables_when_none_has_current_name() {
        let left = verify_quick(&[
            ("archive/eth/000000000/range-000000100_000000109.blocks.avro", b"data"),
            ("archive/eth/000000000/range-000000100_000000109.transactions.avro", b"data"),
            ("archive/eth/000000000/range-000000100_000000109.tx.avro", b"data"),
        ], "100..109").await;

        assert_eq!(left, vec![
            "archive/eth/000000000/range-000000100_000000109.blocks.avro",
            "archive/eth/000000000/range-000000100_000000109.transactions.avro",
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn keeps_single_block_files_listed_twice_by_single_block_chunk() {
        // the last chunk is the block 110 alone, and listing for a single block gives its files twice
        let left = verify_quick(&[
            ("archive/eth/000000000/000000000/000000110.block.avro", b"data"),
            ("archive/eth/000000000/000000000/000000110.txes.avro", b"data"),
        ], "100..110").await;

        assert_eq!(left, vec![
            "archive/eth/000000000/000000000/000000110.block.avro",
            "archive/eth/000000000/000000000/000000110.txes.avro",
        ]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn keeps_valid_range_when_one_from_previous_chunk_is_broken() {
        // the empty blocks are deleted by the verification, and the txes left without them should not replace a valid range
        let left = verify_quick(&[
            ("archive/eth/000000000/range-000000100_000000119.blocks.avro", b""),
            ("archive/eth/000000000/range-000000100_000000119.txes.avro", b"data"),
            ("archive/eth/000000000/range-000000110_000000119.blocks.avro", b"data"),
            ("archive/eth/000000000/range-000000110_000000119.txes.avro", b"data"),
        ], "100..119").await;

        assert_eq!(left, vec![
            "archive/eth/000000000/range-000000110_000000119.blocks.avro",
            "archive/eth/000000000/range-000000110_000000119.txes.avro",
        ]);
    }
}
