// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Holding fresh blocks back before archiving them (`--archive-delay`).
//!
//! Right after a new block the nodes are still processing it: Erigon fails `debug_traceTransaction`
//! until it prepares the traces, and other requests are slow. Starting the archival a few seconds
//! later is cheaper than retrying through that window.

use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use crate::blockchain::next_block::{BlockJob, NextBlock};
use crate::errors::BlockchainError;

/// A [`NextBlock`] that passes the jobs of another one on only after `delay` since each block was announced.
///
/// A job whose block got replaced by a re-org during the delay is dropped, since the follower
/// emits the replacement right after cancelling it.
pub struct DelayedNextBlock {
    inner: Box<dyn NextBlock>,
    delay: Duration,
}

impl DelayedNextBlock {
    /// Delay the jobs of `inner` by `delay` since each block's announcement.
    pub fn new(inner: Box<dyn NextBlock>, delay: Duration) -> Self {
        Self { inner, delay }
    }
}

#[async_trait]
impl NextBlock for DelayedNextBlock {
    async fn next_blocks(&self) -> Result<Receiver<BlockJob>, BlockchainError> {
        let mut jobs = self.inner.next_blocks().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let delay = self.delay;
        tokio::spawn(async move {
            while let Some(job) = jobs.recv().await {
                // Counted from the announcement, not from now: when the archival falls behind, a job that waited
                // in the queue has already had its delay, and waiting again would only grow the lag.
                tokio::time::sleep_until(job.announced_at + delay).await;
                if job.cancel.is_cancelled() {
                    tracing::debug!("Block {} replaced by a re-org while delayed", job.height.height);
                    continue;
                }
                if tx.send(job).await.is_err() {
                    break;
                }
            }
        });
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use tokio::sync::mpsc::Sender;
    use tokio::time::Instant;
    use super::*;
    use crate::archiver::range::Height;

    const DELAY: Duration = Duration::from_secs(5);

    /// Emits whatever the test sends into it.
    struct Given(Mutex<Option<Receiver<BlockJob>>>);

    #[async_trait]
    impl NextBlock for Given {
        async fn next_blocks(&self) -> Result<Receiver<BlockJob>, BlockchainError> {
            Ok(self.0.lock().unwrap().take().unwrap())
        }
    }

    async fn delayed() -> (Sender<BlockJob>, Receiver<BlockJob>) {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let delayed = DelayedNextBlock::new(Box::new(Given(Mutex::new(Some(rx)))), DELAY);
        (tx, delayed.next_blocks().await.unwrap())
    }

    fn job(height: u64) -> BlockJob {
        BlockJob::untracked(Height { height, hash: None })
    }

    #[tokio::test(start_paused = true)]
    async fn holds_fresh_block_for_delay() {
        let (tx, mut rx) = delayed().await;
        let start = Instant::now();
        tx.send(job(100)).await.unwrap();

        let got = rx.recv().await.unwrap();

        assert_eq!(got.height.height, 100);
        assert_eq!(Instant::now() - start, DELAY);
    }

    #[tokio::test(start_paused = true)]
    async fn doesnt_wait_again_for_block_announced_long_ago() {
        let (tx, mut rx) = delayed().await;
        let queued = job(100);
        tokio::time::advance(DELAY * 2).await;
        let sent_at = Instant::now();
        tx.send(queued).await.unwrap();

        rx.recv().await.unwrap();

        assert_eq!(Instant::now(), sent_at);
    }

    #[tokio::test(start_paused = true)]
    async fn drops_block_replaced_during_delay() {
        let (tx, mut rx) = delayed().await;
        let replaced = job(100);
        let cancel = replaced.cancel.clone();
        tx.send(replaced).await.unwrap();
        tx.send(job(100)).await.unwrap();
        cancel.cancel();

        let got = rx.recv().await.unwrap();

        assert!(!got.cancel.is_cancelled());
        drop(tx);
        assert!(rx.recv().await.is_none());
    }
}
