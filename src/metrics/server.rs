// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use prometheus::Registry;
use tokio::sync::Notify;
use warp::Filter;

/// Signals that the main work is done and the next scrape should trigger shutdown.
static FINISHED: AtomicBool = AtomicBool::new(false);

/// Woken after a scrape completes while `FINISHED` is true.
static SCRAPED_AFTER_FINISH: Notify = Notify::const_new();

const AWAIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Start the metrics HTTP server in a background tokio task.
pub fn start(addr: SocketAddr, registry: &Registry) {
    let registry = registry.clone();
    tokio::spawn(async move {
        serve(addr, registry).await;
    });
}

/// Wait for one final metrics scrape after the main work has completed.
///
/// Sets the "finished" flag so the next `/metrics` request will signal back,
/// then waits for that signal, a timeout (60 s), or a termination signal —
/// whichever comes first.
pub async fn await_last_scrape() {
    FINISHED.store(true, Ordering::SeqCst);
    tracing::info!("Waiting for a final metrics scrape (up to {}s)...", AWAIT_TIMEOUT.as_secs());

    let shutdown = crate::global::get_shutdown();
    tokio::select! {
        _ = SCRAPED_AFTER_FINISH.notified() => {
            tracing::info!("Final metrics scrape completed");
        }
        _ = tokio::time::sleep(AWAIT_TIMEOUT) => {
            tracing::warn!("Timed out waiting for the final metrics scrape");
        }
        _ = shutdown.signalled() => {
            tracing::info!("Shutdown signal received, skipping final metrics scrape");
        }
    }
}

async fn serve(addr: SocketAddr, registry: Registry) {
    tracing::info!("Metrics are available at http://{}/metrics", addr);
    let metrics_route = warp::path!("metrics").map(move || {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let mut buffer = Vec::new();
        if let Err(e) = encoder.encode(&registry.gather(), &mut buffer) {
            tracing::warn!("Could not encode metrics: {}", e);
        }
        if FINISHED.load(Ordering::Relaxed) {
            SCRAPED_AFTER_FINISH.notify_one();
        }
        String::from_utf8(buffer).unwrap_or_default()
    });
    let _ = warp::serve(metrics_route).run(addr).await;
}
