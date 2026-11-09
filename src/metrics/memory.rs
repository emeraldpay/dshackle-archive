// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use prometheus::{GaugeVec, Opts, Registry};
use crate::memory::MemoryStats;

/// Metrics for the process memory.
///
/// - `memory_bytes` — gauge of the allocator memory, by `type`: `allocated` is what the app uses, `resident` is what
///   the process holds. A large gap between them is the free memory not returned to the OS yet.
pub struct MemoryMetrics {
    /// Allocator memory in bytes, labeled by `type`
    pub bytes: GaugeVec,
}

impl MemoryMetrics {
    /// Create the metrics with names prefixed by `app_name`. They are not registered yet.
    pub fn new(app_name: &str) -> Self {
        Self {
            bytes: GaugeVec::new(
                Opts::new(
                    format!("{}_memory_bytes", app_name),
                    "Memory of the allocator, in bytes",
                ),
                &["type"],
            )
            .unwrap(),
        }
    }

    /// Add the metrics to the registry, so they are served on `/metrics`.
    pub fn register(&self, registry: &Registry) {
        registry.register(Box::new(self.bytes.clone())).unwrap();
    }

    /// Update the gauges with the current stats.
    pub fn set(&self, stats: &MemoryStats) {
        self.bytes.with_label_values(&["allocated"]).set(u64::from(stats.allocated) as f64);
        self.bytes.with_label_values(&["resident"]).set(u64::from(stats.resident) as f64);
    }
}
