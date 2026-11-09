// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Periodic check of the allocator memory: reports it to the metrics and enforces `--memory.limit`.

use std::time::Duration;
use tokio::time::MissedTickBehavior;
use crate::metrics;
use super::{release, MemoryStats, MemorySize};

const CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How much the memory has to grow, as a part of the limit, after a release that didn't bring it within the limit,
/// before it's released again. The memory above the limit is in use then, and releasing it on every check would only
/// take away the pages that the allocator is about to reuse.
const REPEAT_RELEASE_GROWTH_DIVIDER: u64 = 10;

/// Start the check in background. It stops only with the process.
pub(super) fn start(limit: Option<MemorySize>) {
    if let Some(limit) = limit {
        tracing::info!(limit = display(limit), "Memory limit");
    }
    tokio::spawn(async move {
        let mut watcher = Watcher { limit, after_release: None, stats_failed: false };
        let mut interval = tokio::time::interval(CHECK_INTERVAL);
        // a release may take long, and the checks missed during it are not needed after it
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            watcher.check().await;
        }
    });
}

struct Watcher {
    limit: Option<MemorySize>,
    /// The memory left after the last release, while it stays above the limit. It's also how the warning is logged
    /// once and not on every check.
    after_release: Option<MemorySize>,
    /// Whether the stats failed on the last check, so the error is logged once and not on every check
    stats_failed: bool,
}

impl Watcher {
    async fn check(&mut self) {
        let stats = match MemoryStats::read() {
            Ok(stats) => {
                self.stats_failed = false;
                stats
            }
            Err(e) => {
                if !self.stats_failed {
                    tracing::warn!("Memory stats are not available: {:#}", e);
                    self.stats_failed = true;
                }
                return;
            }
        };
        metrics::set_memory(&stats);

        let Some(limit) = self.limit else {
            return;
        };
        if stats.resident <= limit {
            if self.after_release.take().is_some() {
                tracing::info!(resident = display(stats.resident), limit = display(limit), "Memory is back within the limit");
            }
            return;
        }
        if let Some(after_release) = self.after_release {
            let growth = u64::from(limit) / REPEAT_RELEASE_GROWTH_DIVIDER;
            if u64::from(stats.resident) <= u64::from(after_release).saturating_add(growth) {
                return;
            }
        }

        let Some(released) = release().await else {
            return;
        };
        metrics::set_memory(&released);
        if released.resident <= limit {
            self.after_release = None;
            return;
        }
        // Only the free memory can be released; if the rest is still above, the app really uses that much
        if self.after_release.is_none() {
            tracing::warn!(
                allocated = display(released.allocated), resident = display(released.resident), limit = display(limit),
                "Memory in use is above the limit"
            );
        }
        self.after_release = Some(released.resident);
    }
}
