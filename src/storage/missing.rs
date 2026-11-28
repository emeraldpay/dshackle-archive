// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Tables missing from the archive, as found by [`super::ScanTarget::find_incomplete_tables`].

use crate::archiver::datakind::DataKind;
use crate::archiver::range::Range;

///
/// Tables that a range of blocks is missing in the archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingTables {
    /// Blocks missing the tables. For tables missing next to existing files, it's the range of those files
    pub range: Range,
    /// Tables to archive for the range
    pub kinds: Vec<DataKind>,
    /// What chunks can be used to archive the tables
    pub completion: Completion,
}

///
/// Which ranges the missing tables can be archived in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    /// Nothing is archived for the range yet, or the archive keeps each height on its own,
    /// so the tables can be archived in any chunks.
    AnyChunks,
    /// The other tables of the range are files named by that range. A missing table completes them only when it's
    /// archived for the same range, otherwise it's written next to them under another name and the range stays incomplete.
    SameRange,
}

impl MissingTables {
    /// Tables missing for the range, to be archived in chunks allowed by the `completion`
    pub fn new(range: Range, kinds: Vec<DataKind>, completion: Completion) -> Self {
        Self { range, kinds, completion }
    }

    ///
    /// Chunks to archive the missing tables in, or `None` when chunks of such size cannot complete the range.
    ///
    /// That's the case for a range file of a size other than the chunk, or for a single block in a range file
    /// (`range-N_N`), which is archived under a single block name instead.
    pub fn chunks(&self, chunk_size: usize) -> Option<Vec<Range>> {
        let chunks = self.range.split_chunks(chunk_size, false);
        match self.completion {
            Completion::AnyChunks => Some(chunks),
            Completion::SameRange if chunks == [self.range.clone()] => Some(chunks),
            Completion::SameRange => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_gap_into_chunks() {
        let missing = MissingTables::new(Range::new(1500, 2499), vec![DataKind::Blocks], Completion::AnyChunks);
        assert_eq!(missing.chunks(1000), Some(vec![Range::new(1500, 1999), Range::new(2000, 2499)]));
    }

    #[test]
    fn keeps_chunk_of_same_range() {
        let missing = MissingTables::new(Range::new(1000, 1999), vec![DataKind::Transactions], Completion::SameRange);
        assert_eq!(missing.chunks(1000), Some(vec![Range::new(1000, 1999)]));
    }

    #[test]
    fn keeps_single_block() {
        let missing = MissingTables::new(Range::single(1234), vec![DataKind::Transactions], Completion::SameRange);
        assert_eq!(missing.chunks(1000), Some(vec![Range::single(1234)]));
    }

    #[test]
    fn rejects_single_block_range_file() {
        // as parsed from `range-000001234_000001234.blocks.avro`
        let range = Range::Multiple(1234.into(), 1234.into());
        let missing = MissingTables::new(range, vec![DataKind::Transactions], Completion::SameRange);
        assert_eq!(missing.chunks(1000), None);
    }
}
