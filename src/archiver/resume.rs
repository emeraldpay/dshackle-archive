// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Stream-resume strategies.
//!
//! When the `stream` command starts with `--continue`, it asks the target to
//! report which heights/fields are already archived and re-archives the
//! missing tail. The mechanism is target-specific:
//!
//! - File-based backends ([`crate::storage::ScanTarget`]) can list existing
//!   files and compute the missing kinds per range. See [`ScanResume`].
//! - Streaming backends (Pulsar, future Kafka) will eventually grow a
//!   broker-side variant that tail-reads each topic; the v1 implementation
//!   ships without it.
//!
//! The trait lives in the archiver module rather than alongside the
//! `stream` command because it's an archival operation (it calls back into
//! [`crate::archiver::Archiver::archive`]); the command just dispatches it.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::archiver::archiver::Archiver;
use crate::archiver::datakind::DataOptions;
use crate::archiver::range::{Height, Range};
use crate::archiver::ArchiveAll;
use crate::blockchain::BlockchainTypes;
use crate::notify::RunMode;
use crate::storage::ScanTarget;

/// Resume strategy invoked once at stream startup, *before* the first new
/// block is published. Implementations re-archive the tail of the chain so
/// that gaps left by a previous run are filled in before live tailing begins.
#[async_trait]
pub trait StreamResume: Send + Sync {
    /// Make the archive complete up to (and including) `up_to`'s parent
    /// height, then return. Errors are the caller's to handle — typically
    /// callers log and continue so a transient resume failure doesn't block
    /// fresh blocks from being archived.
    async fn ensure_continued(&self, up_to: Height) -> Result<()>;
}

/// [`ScanTarget`]-backed [`StreamResume`] implementation that walks the last
/// `continue_blocks` heights of the archive and re-archives anything reported
/// as incomplete.
pub struct ScanResume<B: BlockchainTypes, TS: ScanTarget> {
    archiver: Archiver<B, TS>,
    continue_blocks: u64,
    data_options: DataOptions,
}

impl<B: BlockchainTypes, TS: ScanTarget> ScanResume<B, TS> {
    pub fn new(archiver: Archiver<B, TS>, continue_blocks: u64, data_options: DataOptions) -> Self {
        Self { archiver, continue_blocks, data_options }
    }

    /// Convenience constructor that hands back the boxed trait object the
    /// command layer wants to store. Saves callers from importing
    /// `Arc<dyn StreamResume>` and the trait at once.
    pub fn boxed(
        archiver: Archiver<B, TS>,
        continue_blocks: u64,
        data_options: DataOptions,
    ) -> Arc<dyn StreamResume>
    where
        B: 'static,
        TS: 'static,
    {
        Arc::new(Self::new(archiver, continue_blocks, data_options))
    }
}

#[async_trait]
impl<B, TS> StreamResume for ScanResume<B, TS>
where
    B: BlockchainTypes + 'static,
    TS: ScanTarget + 'static,
{
    async fn ensure_continued(&self, height: Height) -> Result<()> {
        let range = Range::up_to(self.continue_blocks, &Range::Single(height));
        let options = self.data_options.clone();
        let missing = self
            .archiver
            .target
            .find_incomplete_tables(range, &options)
            .await?;
        for (range, kinds) in missing {
            let range_opts = options.clone().only_include(&kinds);
            for height in range.iter().collect::<Vec<u64>>() {
                self.archiver
                    .archive(Height::from(height), RunMode::Stream, None, &range_opts)
                    .await?;
            }
        }
        Ok(())
    }
}
