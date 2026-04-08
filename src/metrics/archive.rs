// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use prometheus::{CounterVec, Opts, Registry};
use crate::archiver::datakind::DataKind;
use super::Direction;

/// Metrics for the archive processing zone.
///
/// Uses two `CounterVec` metrics with `type` and `direction` labels:
/// - `items` — total number of items (blocks, transactions, traces) processed
/// - `bytes` — total number of bytes transferred to/from storage
pub struct ArchiveMetrics {
    /// Total items processed, labeled by type and direction
    pub items: CounterVec,
    /// Total bytes transferred, labeled by type and direction
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
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry.register(Box::new(self.items.clone())).unwrap();
        registry.register(Box::new(self.bytes.clone())).unwrap();
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
}
