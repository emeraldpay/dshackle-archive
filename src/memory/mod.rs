// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Process memory: the allocator and how it gives the freed memory back to the OS.
//!
//! The default glibc allocator never gives the memory back during a long compaction: RSS grew from ~150MB to ~1.7GB
//! over a few hours and stayed there. Avro blocks are decoded and zstd contexts are created on `spawn_blocking`
//! threads, but the buffers are dropped on the tokio workers, and glibc keeps the freed space cached in each thread's
//! own arena. Capping the arenas (`MALLOC_ARENA_MAX=2`) kept it at ~350MB, which proved it's a retention and not a
//! leak. jemalloc handles such cross-thread frees well, but by itself it returns the freed pages only when the app
//! allocates again, so the process stays at what it took at the peak. Here it returns them from its background
//! threads (Linux only), right after each chunk processed by a command, and whenever it holds more than
//! `--memory.limit`.
//!
//! On Linux jemalloc replaces the plain `malloc` too, because the zstd encoders are C code and would otherwise keep
//! the same glibc problem. On other platforms it's only the allocator of the Rust code. That and the background
//! threads are set in `Cargo.toml`; the background threads as a build-time default, so `MALLOC_CONF` can still turn
//! them off.
//!
//! The number of arenas is not configured here. jemalloc reads it on the first allocation, which happens before
//! `main`, so it cannot be a command line option, and a value compiled in would not fit every deployment. It's set
//! with `MALLOC_CONF=narenas:N` on Linux (`_RJEM_MALLOC_CONF` elsewhere, where the jemalloc symbols are renamed).

mod jemalloc;
mod size;
mod watch;

use anyhow::Context;
use crate::args::Args;

pub use jemalloc::MemoryStats;
pub use size::MemorySize;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Start the memory watch for the run, if it's needed. Must be called from within the tokio runtime.
pub fn init(args: &Args) {
    // the stats are needed only to enforce the limit or to report them
    if args.memory_limit.is_some() || args.metrics.is_some() {
        watch::start(args.memory_limit);
    }
}

/// Return all the freed memory to the OS now, without waiting for the background release.
///
/// The commands call it after each chunk of a range. A chunk is the peak of the memory use, and the next one starts
/// with waiting on IO (the storage or the blockchain API), so it's the moment when the memory is not needed and the
/// release doesn't slow anything down. Without it, the process keeps the chunk's memory for the decay time.
///
/// Returns the memory stats after it, or `None` if it failed or the stats are not available.
pub async fn release() -> Option<MemoryStats> {
    // it walks over all the arenas and calls `madvise` for each free extent, which may take a while
    let purged = tokio::task::spawn_blocking(jemalloc::purge).await
        .context("Release task failed")
        .and_then(|purged| purged);
    if let Err(e) = purged {
        tracing::warn!("Failed to release memory: {:#}", e);
        return None;
    }
    match MemoryStats::read() {
        Ok(stats) => {
            tracing::debug!(allocated = display(stats.allocated), resident = display(stats.resident), "Released free memory");
            Some(stats)
        }
        Err(e) => {
            tracing::debug!("Released free memory, but the stats are not available: {:#}", e);
            None
        }
    }
}
