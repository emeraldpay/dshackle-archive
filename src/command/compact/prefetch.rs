// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0
//! Opens the source files of a table ahead of the one being copied.
//!
//! The sources of a chunk are usually per-block files left by the streaming, i.e., a thousand small files per table.
//! Opening each of them is a storage round trip, and when they're opened one after another, the compaction waits
//! for each of these round trips instead of copying.

use std::collections::VecDeque;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use tokio_util::task::AbortOnDropHandle;
use crate::storage::{FileReference, ReadTarget};

/// How many sources are opened ahead of the one being copied.
///
/// A source is only opened ahead, not decoded, so it holds an HTTP response (or a file handle) and not
/// the decoded records, i.e., the memory doesn't grow with the read-ahead of each opened file.
const OPEN_AHEAD: usize = 8;

/// How many bytes may be copied before the last source opened ahead is read.
///
/// The S3 client limits the whole request to 30 seconds by default, including the body, so a response that waits
/// for too long behind large files breaks, and it has to be requested again. With this limit, a source is opened
/// ahead only when the copying of the sources before it is expected to take seconds, and behind a large file,
/// such as an already compacted range, nothing is opened ahead.
const OPEN_AHEAD_BYTES: u64 = 4 * 1024 * 1024;

///
/// The sources of a table in the order they are copied, with the next few of them already being opened.
pub(super) struct PrefetchedSources<TS: ReadTarget> {
    target: Arc<TS>,
    waiting: std::vec::IntoIter<FileReference>,
    /// The opening runs as a separate task, because a future would progress only while it's polled,
    /// and it's not polled while the previous source is copied.
    /// Aborted on drop, so a compaction that stopped doesn't leave them running.
    opening: VecDeque<(u64, AbortOnDropHandle<Result<TS::Reader>>)>,
    /// Size of the source given last, which is being copied until the next one is asked for
    copying: u64,
}

impl<TS: ReadTarget + 'static> PrefetchedSources<TS> {
    pub(super) fn new(target: Arc<TS>, sources: Vec<FileReference>) -> Self {
        let mut prefetched = Self {
            target,
            waiting: sources.into_iter(),
            opening: VecDeque::with_capacity(OPEN_AHEAD),
            copying: 0,
        };
        prefetched.open_ahead();
        prefetched
    }

    fn open_ahead(&mut self) {
        while self.opening.len() < OPEN_AHEAD && self.bytes_ahead() < OPEN_AHEAD_BYTES {
            let Some(source) = self.waiting.next() else {
                return;
            };
            // without a known size the count is the only limit, which is fine for the per-block files
            let size = source.size.unwrap_or(0);
            let target = self.target.clone();
            let task = tokio::spawn(async move {
                target.open(&source).await
            });
            self.opening.push_back((size, AbortOnDropHandle::new(task)));
        }
    }

    /// Bytes to copy before the next source to open is read
    fn bytes_ahead(&self) -> u64 {
        self.copying + self.opening.iter().map(|(size, _)| size).sum::<u64>()
    }

    ///
    /// The next source to copy, or `None` when all of them are given.
    pub(super) async fn next(&mut self) -> Option<Result<TS::Reader>> {
        // the previous source is copied by now, so it doesn't hold the sources behind it anymore,
        // and when it was too large to open anything ahead of it, the next one is opened here
        self.copying = 0;
        self.open_ahead();
        let (size, opening) = self.opening.pop_front()?;
        self.copying = size;
        self.open_ahead();
        let opened = opening.await
            .unwrap_or_else(|e| Err(anyhow!("Opening the source failed: {}", e)));
        Some(opened)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use object_store::memory::InMemory;
    use crate::archiver::datakind::DataKind;
    use crate::archiver::filenames::Filenames;
    use crate::archiver::range::Range;
    use crate::storage::objects::ObjectsStorage;
    use crate::storage::{FileReference, ReadTarget, TargetFile, TargetFileWriter, WriteTarget};
    use crate::testing;
    use super::{PrefetchedSources, OPEN_AHEAD, OPEN_AHEAD_BYTES};

    async fn storage_with_files(count: u64) -> (Arc<ObjectsStorage<InMemory>>, Vec<FileReference>) {
        let storage = Arc::new(ObjectsStorage::new(Arc::new(InMemory::new()), "test".to_string(), Filenames::with_dir("archive/eth".to_string())));
        let mut files = vec![];
        for height in 0..count {
            let range = Range::new(height, height);
            let file = storage.create(DataKind::Blocks, &range, true).await.unwrap().unwrap();
            let path = storage.table_path(DataKind::Blocks, &range);
            file.close().await.unwrap();
            files.push(FileReference { path, kind: DataKind::Blocks, range, size: None });
        }
        (storage, files)
    }

    #[tokio::test]
    async fn gives_sources_in_order() {
        testing::start_test();
        let count = OPEN_AHEAD as u64 * 2 + 3;
        let (storage, files) = storage_with_files(count).await;

        let mut sources = PrefetchedSources::new(storage.clone(), files.clone());
        assert_eq!(sources.opening.len(), OPEN_AHEAD);
        let mut opened = vec![];
        while let Some(source) = sources.next().await {
            opened.push(source.unwrap().get_url());
        }

        let expected: Vec<String> = files.iter().map(|f| format!("s3://test/{}", f.path)).collect();
        assert_eq!(opened, expected);
    }

    #[tokio::test]
    async fn fails_on_missing_source_in_its_place() {
        testing::start_test();
        let (storage, mut files) = storage_with_files(3).await;
        files.insert(1, FileReference {
            path: "archive/eth/missing.blocks.avro".to_string(),
            kind: DataKind::Blocks,
            range: Range::new(100, 100),
            size: None,
        });

        let mut sources = PrefetchedSources::new(storage, files);

        assert!(sources.next().await.unwrap().is_ok());
        assert!(sources.next().await.unwrap().is_err());
        assert!(sources.next().await.unwrap().is_ok());
        assert!(sources.next().await.unwrap().is_ok());
        assert!(sources.next().await.is_none());
    }

    #[tokio::test]
    async fn opens_nothing_ahead_of_large_source() {
        testing::start_test();
        let (storage, files) = storage_with_files(3).await;
        let files: Vec<FileReference> = files.into_iter()
            .map(|f| FileReference { size: Some(OPEN_AHEAD_BYTES), ..f })
            .collect();

        let mut sources = PrefetchedSources::new(storage, files);
        assert_eq!(sources.opening.len(), 1);

        assert!(sources.next().await.unwrap().is_ok());
        assert_eq!(sources.opening.len(), 0);
        assert!(sources.next().await.unwrap().is_ok());
        assert!(sources.next().await.unwrap().is_ok());
        assert!(sources.next().await.is_none());
    }
}
