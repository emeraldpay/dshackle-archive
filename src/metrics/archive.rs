// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use prometheus::{CounterVec, Histogram, HistogramOpts, Opts, Registry};
use crate::archiver::datakind::DataKind;
use super::Direction;

/// Metrics for the archive processing zone.
///
/// - `items` — counter of items (blocks, transactions, traces) processed, by type and direction
/// - `bytes` — counter of bytes transferred, by type and direction
/// - `block_archive_duration` — histogram of single-block archival time (block + txes + traces)
pub struct ArchiveMetrics {
    /// Total items processed, labeled by type and direction
    pub items: CounterVec,
    /// Total bytes transferred, labeled by type and direction
    pub bytes: CounterVec,
    /// Time to archive a single block with all its tables
    pub block_archive_duration: Histogram,
}

impl ArchiveMetrics {
    pub fn new(app_name: &str) -> Self {
        // Buckets tuned for the expected 500ms–2s range, with tails for slow blocks
        let buckets = vec![
            0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
            1.25, 1.5, 1.75, 2.0, 2.5,
            3.0, 4.0, 5.0, 7.5, 10.0, 12.5, 15.0,
            20.0, 25.0, 30.0,
        ];
        Self {
            items: CounterVec::new(
                Opts::new(
                    format!("{}_archive_items_total", app_name),
                    "Total number of items processed",
                ),
                &["type", "direction"],
            )
            .unwrap(),
            bytes: CounterVec::new(
                Opts::new(
                    format!("{}_archive_bytes_total", app_name),
                    "Total number of bytes transferred",
                ),
                &["type", "direction"],
            )
            .unwrap(),
            block_archive_duration: Histogram::with_opts(
                HistogramOpts::new(
                    format!("{}_archive_blockTime_seconds", app_name),
                    "Time to archive a single block with all its tables (block, txes, traces)",
                )
                .buckets(buckets),
            )
            .unwrap(),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry.register(Box::new(self.items.clone())).unwrap();
        registry.register(Box::new(self.bytes.clone())).unwrap();
        registry.register(Box::new(self.block_archive_duration.clone())).unwrap();
    }

    /// Record that `n` items of the given data kind have been processed.
    pub fn add_items(&self, kind: &DataKind, direction: &Direction, n: usize) {
        self.items
            .with_label_values(&[kind.metrics_label(), direction.metrics_label()])
            .inc_by(n as f64);
    }

    /// Record that `n` bytes of the given data kind have been transferred.
    pub fn add_bytes(&self, kind: &DataKind, direction: &Direction, n: usize) {
        self.bytes
            .with_label_values(&[kind.metrics_label(), direction.metrics_label()])
            .inc_by(n as f64);
    }

    /// Record the time it took to archive a single block (including txes and traces).
    pub fn observe_block_archive(&self, duration_secs: f64) {
        self.block_archive_duration.observe(duration_secs);
    }
}
