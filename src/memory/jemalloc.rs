// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! The jemalloc controls used by the app.

use std::ffi::c_char;
use std::ptr::null_mut;
use anyhow::{anyhow, Result};
use tikv_jemalloc_ctl::{epoch, stats};
use super::size::MemorySize;

/// `arena.<MALLCTL_ARENAS_ALL>.purge`
const PURGE_ALL_ARENAS: &[u8] = b"arena.4096.purge\0";

/// Memory as the allocator sees it.
#[derive(Debug, Clone, Copy)]
pub struct MemoryStats {
    /// In live allocations, i.e., what the app actually uses
    pub allocated: MemorySize,
    /// Physically held by the allocator, including the free pages it hasn't returned to the OS yet
    pub resident: MemorySize,
}

impl MemoryStats {
    /// Read the current stats.
    ///
    /// jemalloc caches the stats and refreshes them only on a new epoch, so it advances the epoch first, which merges
    /// the counters of all arenas. It's cheap enough for a periodic check, but not for a hot path.
    pub fn read() -> Result<Self> {
        epoch::advance()?;
        Ok(Self {
            allocated: MemorySize::from(stats::allocated::read()? as u64),
            resident: MemorySize::from(stats::resident::read()? as u64),
        })
    }
}

/// Return all the free pages of all arenas to the OS.
pub fn purge() -> Result<()> {
    // It's a command and takes no value, and jemalloc rejects it if any pointer is passed. So it cannot go through
    // `tikv_jemalloc_ctl::raw::write`, which passes a pointer even for an empty value.
    let code = unsafe {
        tikv_jemalloc_sys::mallctl(PURGE_ALL_ARENAS.as_ptr() as *const c_char, null_mut(), null_mut(), null_mut(), 0)
    };
    if code != 0 {
        return Err(anyhow!("jemalloc purge failed with error code {}", code));
    }
    Ok(())
}
