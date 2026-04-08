// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Prometheus-compatible metrics for the Dshackle Archive application.
//!
//! When enabled via the `--metrics HOST:PORT` CLI option, an HTTP server is started
//! that serves metrics in Prometheus text format at `/metrics`. When not enabled,
//! all metric operations are no-ops.
//!
//! Metrics are organized by application zone. Each metric name follows the format
//! `dshackleArchive_<zone>_<metric>`, with labels to distinguish data kinds or methods.

mod archive;
mod blockchain;
mod server;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};

use lazy_static::lazy_static;
use prometheus::Registry;

use crate::archiver::datakind::DataKind;

pub use archive::ArchiveMetrics;
pub use blockchain::BlockchainMetrics;

/// Whether data is being read from or written to storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Read,
    Write,
}

impl Direction {
    /// Label value used in Prometheus metrics (the `direction` tag).
    pub fn metrics_label(&self) -> &'static str {
        match self {
            Direction::Read => "read",
            Direction::Write => "write",
        }
    }
}

static ENABLED: AtomicBool = AtomicBool::new(false);

lazy_static! {
    static ref REGISTRY: Registry = Registry::new();
    static ref METRICS: Metrics = {
        let m = Metrics::new();
        m.archive.register(&REGISTRY);
        m.blockchain.register(&REGISTRY);
        m
    };
}

/// Global metrics organized by application zone.
struct Metrics {
    archive: ArchiveMetrics,
    blockchain: BlockchainMetrics,
}

impl Metrics {
    fn new() -> Self {
        Self {
            archive: ArchiveMetrics::new("dshackleArchive"),
            blockchain: BlockchainMetrics::new("dshackleArchive"),
        }
    }
}

/// Initialize and start the Prometheus metrics server.
///
/// Must be called from within a tokio runtime. Starts an HTTP server at the given
/// address that serves metrics at the `/metrics` endpoint.
pub fn init(addr: SocketAddr) {
    ENABLED.store(true, Ordering::SeqCst);
    lazy_static::initialize(&METRICS);
    server::start(addr, &REGISTRY);
}

/// Wait for one final metrics scrape before the process exits.
///
/// Keeps the metrics server alive until Prometheus (or another scraper) collects the
/// final values, a 60-second timeout elapses, or a termination signal arrives.
pub async fn await_last_scrape() {
    server::await_last_scrape().await;
}

/// Record that `n` items of the given data kind have been processed.
pub fn add_items(kind: &DataKind, direction: Direction, n: usize) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.archive.add_items(kind, &direction, n);
}

/// Record that `n` bytes of the given data kind have been transferred.
pub fn add_bytes(kind: &DataKind, direction: Direction, n: usize) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.archive.add_bytes(kind, &direction, n);
}

/// Observe the duration of a blockchain RPC request.
pub fn observe_request(method: &str, blockchain: &str, duration_secs: f64) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    METRICS.blockchain.observe_request(method, blockchain, duration_secs);
}
