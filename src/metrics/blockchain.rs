// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use prometheus::{HistogramOpts, HistogramVec, Registry};

/// Metrics for the blockchain RPC zone.
///
/// - `request_duration` — a single successful `native_call`, by `method` (e.g., `"eth_getBlockByNumber"`) and `blockchain` (e.g., `"ETH"`)
/// - `fetch_duration` — a successful fetch with all its retries and backoff delays, by `data` (see [`FetchedData`]) and `blockchain`
pub struct BlockchainMetrics {
    /// Duration of successful blockchain RPC requests in seconds
    pub request_duration: HistogramVec,
    /// Duration of successful blockchain data fetches, including retries, in seconds
    pub fetch_duration: HistogramVec,
}

/// What a retried fetch gets from the blockchain, for the `data` label of the fetch metric.
///
/// It's not the RPC method because one fetch may use different methods (a block by height on Bitcoin is
/// `getblockhash` + `getblock`), and different fetches may use the same one (a trace and a state diff are both
/// `debug_traceTransaction`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchedData {
    Block,
    BlockAtHeight,
    FinalizedBlock,
    /// A block header checked by the re-org follower.
    BlockLink,
    Uncle,
    Transaction,
    RawTransaction,
    Receipt,
    Trace,
    StateDiff,
}

impl FetchedData {
    /// Label value used in Prometheus metrics (the `data` tag).
    pub fn label(&self) -> &'static str {
        match self {
            FetchedData::Block => "block",
            FetchedData::BlockAtHeight => "blockAtHeight",
            FetchedData::FinalizedBlock => "finalizedBlock",
            FetchedData::BlockLink => "blockLink",
            FetchedData::Uncle => "uncle",
            FetchedData::Transaction => "transaction",
            FetchedData::RawTransaction => "rawTransaction",
            FetchedData::Receipt => "receipt",
            FetchedData::Trace => "trace",
            FetchedData::StateDiff => "stateDiff",
        }
    }
}

impl BlockchainMetrics {
    pub fn new(app_name: &str) -> Self {
        // A fetch waiting for a node to catch up with a fresh block retries for seconds,
        // which is past the default buckets' 10s end.
        let fetch_buckets = vec![
            0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            10.0, 15.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0,
        ];
        Self {
            request_duration: HistogramVec::new(
                HistogramOpts::new(
                    format!("{}_blockchain_requestTime_seconds", app_name),
                    "Duration of successful blockchain RPC requests in seconds",
                ),
                &["method", "blockchain"],
            )
            .unwrap(),
            fetch_duration: HistogramVec::new(
                HistogramOpts::new(
                    format!("{}_blockchain_fetchTime_seconds", app_name),
                    "Duration of successful blockchain data fetches, including retries, in seconds",
                )
                .buckets(fetch_buckets),
                &["data", "blockchain"],
            )
            .unwrap(),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.request_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(self.fetch_duration.clone()))
            .unwrap();
    }

    /// Observe the duration of a successful blockchain RPC request.
    pub fn observe_request(&self, method: &str, blockchain: &str, duration_secs: f64) {
        self.request_duration
            .with_label_values(&[method, blockchain])
            .observe(duration_secs);
    }

    /// Observe the duration of a successful fetch, from its first attempt to the one that succeeded.
    pub fn observe_fetch(&self, data: FetchedData, blockchain: &str, duration_secs: f64) {
        self.fetch_duration
            .with_label_values(&[data.label(), blockchain])
            .observe(duration_secs);
    }
}
