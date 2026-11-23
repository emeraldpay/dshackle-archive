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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::task::AbortOnDropHandle;
use crate::storage::{FileReference, ReadTarget, RecordStream, TargetFileReader};

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

/// How many sources all the chunks compacted at the same time may have open, i.e., the ones being copied and the
/// ones opened ahead. Each of them is a file descriptor or an HTTP connection, and without a common limit their
/// number would grow with the number of the chunks.
const MAX_OPEN_SOURCES: usize = 128;

///
/// The limit of open sources, shared by all the chunks compacted at the same time
#[derive(Clone)]
pub(super) struct OpenSources(Arc<Semaphore>);

impl Default for OpenSources {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(MAX_OPEN_SOURCES)))
    }
}

///
/// A source that is open, and counts towards [`OpenSources`] until it's read to the end
pub(super) struct OpenedSource<R> {
    reader: R,
    permit: OwnedSemaphorePermit,
}

impl<R: TargetFileReader> OpenedSource<R> {
    pub(super) fn read(self) -> Result<ReadingSource> {
        Ok(ReadingSource {
            records: self.reader.read()?,
            _permit: self.permit,
        })
    }
}

///
/// The records of an open source. The source stays counted as open until it's dropped, since that's when its
/// reader is stopped and the file is closed.
pub(super) struct ReadingSource {
    pub records: RecordStream,
    _permit: OwnedSemaphorePermit,
}

///
/// A source being opened ahead
struct Opening<R> {
    size: u64,
    path: String,
    permit: OwnedSemaphorePermit,
    /// The opening runs as a separate task, because a future would progress only while it's polled,
    /// and it's not polled while the previous source is copied.
    /// Aborted on drop, so a compaction that stopped doesn't leave them running.
    task: AbortOnDropHandle<Result<R>>,
}

///
/// The sources of a table in the order they are copied, with the next few of them already being opened.
pub(super) struct PrefetchedSources<TS: ReadTarget> {
    target: Arc<TS>,
    open_sources: OpenSources,
    waiting: std::vec::IntoIter<FileReference>,
    opening: VecDeque<Opening<TS::Reader>>,
    /// Size of the source given last, which is being copied until the next one is asked for
    copying: u64,
}

impl<TS: ReadTarget + 'static> PrefetchedSources<TS> {
    pub(super) fn new(target: Arc<TS>, sources: Vec<FileReference>, open_sources: OpenSources) -> Self {
        let mut prefetched = Self {
            target,
            open_sources,
            waiting: sources.into_iter(),
            opening: VecDeque::with_capacity(OPEN_AHEAD),
            copying: 0,
        };
        prefetched.open_ahead();
        prefetched
    }

    fn open_ahead(&mut self) {
        while self.opening.len() < OPEN_AHEAD && self.bytes_ahead() < OPEN_AHEAD_BYTES {
            // Opening ahead never waits for a place. A table that waits holding the places of its sources opened
            // ahead may block the other tables that hold the rest of them in the same way, so they'd all wait forever.
            let Ok(permit) = self.open_sources.0.clone().try_acquire_owned() else {
                return;
            };
            let Some(source) = self.waiting.next() else {
                return;
            };
            self.open(source, permit);
        }
    }

    fn open(&mut self, source: FileReference, permit: OwnedSemaphorePermit) {
        // without a known size the count is the only limit, which is fine for the per-block files
        let size = source.size.unwrap_or(0);
        let path = source.path.clone();
        let target = self.target.clone();
        let task = tokio::spawn(async move {
            target.open(&source).await
        });
        self.opening.push_back(Opening { size, path, permit, task: AbortOnDropHandle::new(task) });
    }

    /// Bytes to copy before the next source to open is read
    fn bytes_ahead(&self) -> u64 {
        self.copying + self.opening.iter().map(|opening| opening.size).sum::<u64>()
    }

    ///
    /// The next source to copy, or `None` when all of them are given.
    ///
    /// The previous source must be dropped by then, as its place may be needed to open the next one.
    pub(super) async fn next(&mut self) -> Option<Result<OpenedSource<TS::Reader>>> {
        // the previous source is copied by now, so it doesn't hold the sources behind it anymore,
        // and when it was too large to open anything ahead of it, the next one is opened here
        self.copying = 0;
        self.open_ahead();
        if self.opening.is_empty() {
            // Nothing is opened ahead when the other chunks hold all the places, so this table waits for one.
            // It holds no places by now, so it doesn't block the others.
            if self.waiting.len() == 0 {
                return None;
            }
            let permit = self.open_sources.0.clone().acquire_owned().await
                .expect("Open sources limit is never closed");
            // taken only after the waiting, so a caller that stops waiting doesn't lose the source
            let source = self.waiting.next()?;
            self.open(source, permit);
        }
        let Opening { size, path, permit, task } = self.opening.pop_front()?;
        self.copying = size;
        self.open_ahead();
        let opened = task.await
            .unwrap_or_else(|e| Err(anyhow!("Opening {} failed: {}", path, e)))
            .map(|reader| OpenedSource { reader, permit });
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
    use super::{OpenSources, PrefetchedSources, MAX_OPEN_SOURCES, OPEN_AHEAD, OPEN_AHEAD_BYTES};

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

        let mut sources = PrefetchedSources::new(storage.clone(), files.clone(), OpenSources::default());
        assert_eq!(sources.opening.len(), OPEN_AHEAD);
        let mut opened = vec![];
        while let Some(source) = sources.next().await {
            opened.push(source.unwrap().reader.get_url());
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

        let mut sources = PrefetchedSources::new(storage, files, OpenSources::default());

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

        let mut sources = PrefetchedSources::new(storage, files, OpenSources::default());
        assert_eq!(sources.opening.len(), 1);

        assert!(sources.next().await.unwrap().is_ok());
        assert_eq!(sources.opening.len(), 0);
        assert!(sources.next().await.unwrap().is_ok());
        assert!(sources.next().await.unwrap().is_ok());
        assert!(sources.next().await.is_none());
    }

    #[tokio::test]
    async fn opens_ahead_only_within_open_limit() {
        testing::start_test();
        let (storage, files) = storage_with_files(3).await;
        let open_sources = OpenSources::default();
        let held = open_sources.0.clone().acquire_many_owned(MAX_OPEN_SOURCES as u32 - 1).await.unwrap();

        let mut sources = PrefetchedSources::new(storage, files, open_sources.clone());
        assert_eq!(sources.opening.len(), 1);

        let first = sources.next().await.unwrap().unwrap();
        assert_eq!(sources.opening.len(), 0);
        let reading = first.read().unwrap();
        assert_eq!(open_sources.0.available_permits(), 0);

        drop(reading);
        drop(held);
        assert!(sources.next().await.unwrap().is_ok());
        assert_eq!(sources.opening.len(), 1);
    }

    #[tokio::test]
    async fn waits_for_a_place_when_all_are_held() {
        testing::start_test();
        let (storage, files) = storage_with_files(1).await;
        let open_sources = OpenSources::default();
        let held = open_sources.0.clone().acquire_many_owned(MAX_OPEN_SOURCES as u32).await.unwrap();

        let mut sources = PrefetchedSources::new(storage, files, open_sources);
        assert_eq!(sources.opening.len(), 0);

        let waiting = tokio::time::timeout(std::time::Duration::from_millis(100), sources.next()).await;
        assert!(waiting.is_err(), "Opened a source beyond the limit");

        drop(held);
        assert!(sources.next().await.unwrap().is_ok());
        assert!(sources.next().await.is_none());
    }
}
