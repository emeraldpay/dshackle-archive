// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0
//! Chunks being compacted at the same time.
//!
//! A single chunk spends most of its time waiting for the storage, so compacting a few of them at once is what makes
//! the compaction use the bandwidth it has.

use std::collections::HashMap;
use std::future::Future;
use anyhow::anyhow;
use tokio::task::{Id, JoinSet};
use crate::archiver::range::Range;
use crate::storage::FileReference;
use super::{Compaction, Failure};

///
/// A chunk given to the compaction, with the source files to delete once it's compacted
pub(super) struct StartedChunk {
    pub chunk: Range,
    pub copied: Vec<FileReference>,
}

///
/// A chunk that is done with the compaction, successfully or not
pub(super) struct FinishedChunk {
    pub chunk: Range,
    pub copied: Vec<FileReference>,
    pub outcome: Result<Compaction, Failure>,
}

///
/// Compactions of chunks that run at the same time, up to a limit
pub(super) struct RunningChunks {
    limit: usize,
    tasks: JoinSet<Result<Compaction, Failure>>,
    /// Kept apart from the task, because a panicked task gives back nothing but its id, and the chunk is still to be
    /// reported as failed
    chunks: HashMap<Id, StartedChunk>,
}

impl RunningChunks {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            tasks: JoinSet::new(),
            chunks: HashMap::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(super) fn is_full(&self) -> bool {
        self.tasks.len() >= self.limit
    }

    pub(super) fn start<F>(&mut self, chunk: StartedChunk, compaction: F)
    where F: Future<Output = Result<Compaction, Failure>> + Send + 'static {
        let task = self.tasks.spawn(compaction);
        self.chunks.insert(task.id(), chunk);
    }

    ///
    /// Wait for any of the running chunks to finish. `None` when nothing is running.
    pub(super) async fn next_finished(&mut self) -> Option<FinishedChunk> {
        let (id, outcome) = match self.tasks.join_next_with_id().await? {
            Ok((id, outcome)) => (id, outcome),
            Err(e) => (e.id(), Err(Failure::Error(anyhow!("Compaction of the range failed: {}", e)))),
        };
        let StartedChunk { chunk, copied } = self.chunks.remove(&id)
            .expect("Each task is started with its chunk");
        Some(FinishedChunk { chunk, copied, outcome })
    }
}

#[cfg(test)]
mod tests {
    use crate::archiver::range::Range;
    use super::{Compaction, Failure, RunningChunks, StartedChunk};

    fn started(start: u64) -> StartedChunk {
        StartedChunk { chunk: Range::new(start, start + 99), copied: vec![] }
    }

    #[tokio::test]
    async fn full_at_limit() {
        let mut running = RunningChunks::new(2);
        running.start(started(0), async { Ok(Compaction::Done) });
        assert!(!running.is_full());
        running.start(started(100), async { Ok(Compaction::Done) });
        assert!(running.is_full());

        running.next_finished().await.unwrap();
        assert!(!running.is_full());
    }

    #[tokio::test]
    async fn gives_each_chunk_its_outcome() {
        let mut running = RunningChunks::new(4);
        running.start(started(0), async { Ok(Compaction::Done) });
        running.start(started(100), async { Ok(Compaction::AlreadyCompacted) });

        let mut finished = vec![];
        while let Some(chunk) = running.next_finished().await {
            finished.push((chunk.chunk.start(), chunk.outcome.unwrap()));
        }
        finished.sort();
        assert_eq!(finished, vec![(0, Compaction::Done), (100, Compaction::AlreadyCompacted)]);
    }

    #[tokio::test]
    async fn panicked_compaction_fails_its_chunk() {
        let mut running = RunningChunks::new(1);
        running.start(started(100), async { panic!("test") });

        let finished = running.next_finished().await.unwrap();
        assert_eq!(finished.chunk, Range::new(100, 199));
        assert!(matches!(finished.outcome, Err(Failure::Error(_))));
        assert!(running.next_finished().await.is_none());
    }
}
