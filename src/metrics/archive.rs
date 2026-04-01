// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use prometheus::{CounterVec, Opts, Registry};
use crate::archiver::datakind::DataKind;

/// Metrics for the archive processing zone.
///
/// Uses two `CounterVec` metrics with a `type` label (`"block"`, `"transaction"`, `"trace"`):
/// - `items` — total number of items (blocks, transactions, traces) processed
/// - `bytes` — total number of bytes written to storage
pub struct ArchiveMetrics {
    /// Total items processed, labeled by type
    pub items: CounterVec,
    /// Total bytes written to storage, labeled by type
    pub bytes: CounterVec,
}

impl ArchiveMetrics {
    pub fn new(app_name: &str) -> Self {
        Self {
            items: CounterVec::new(
                Opts::new(
                    format!("{}_archive_items_total", app_name),
                    "Total number of items processed",
                ),
                &["type"],
            )
            .unwrap(),
            bytes: CounterVec::new(
                Opts::new(
                    format!("{}_archive_bytes_total", app_name),
                    "Total number of bytes written to storage",
                ),
                &["type"],
            )
            .unwrap(),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry.register(Box::new(self.items.clone())).unwrap();
        registry.register(Box::new(self.bytes.clone())).unwrap();
    }

    /// Record that `n` items of the given data kind have been processed.
    pub fn add_items(&self, kind: &DataKind, n: usize) {
        self.items.with_label_values(&[kind.metrics_label()]).inc_by(n as f64);
    }

    /// Record that `n` bytes of the given data kind have been written to storage.
    pub fn add_bytes(&self, kind: &DataKind, n: usize) {
        self.bytes.with_label_values(&[kind.metrics_label()]).inc_by(n as f64);
    }
}
