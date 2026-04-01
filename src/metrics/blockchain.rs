// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use prometheus::{HistogramOpts, HistogramVec, Registry};

/// Metrics for the blockchain RPC zone.
///
/// Uses a histogram to track both the count and duration of `native_call` requests,
/// labeled by `method` (e.g., `"eth_getBlockByNumber"`) and `blockchain` (e.g., `"ETH"`).
pub struct BlockchainMetrics {
    /// Duration of blockchain RPC requests in seconds
    pub request_duration: HistogramVec,
}

impl BlockchainMetrics {
    pub fn new(app_name: &str) -> Self {
        Self {
            request_duration: HistogramVec::new(
                HistogramOpts::new(
                    format!("{}_blockchain_requestTime_seconds", app_name),
                    "Duration of blockchain RPC requests in seconds",
                ),
                &["method", "blockchain"],
            )
            .unwrap(),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.request_duration.clone()))
            .unwrap();
    }

    /// Observe the duration of a blockchain RPC request.
    pub fn observe_request(&self, method: &str, blockchain: &str, duration_secs: f64) {
        self.request_duration
            .with_label_values(&[method, blockchain])
            .observe(duration_secs);
    }
}
