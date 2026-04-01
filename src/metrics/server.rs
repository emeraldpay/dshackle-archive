// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use std::net::SocketAddr;
use prometheus::Registry;
use warp::Filter;

/// Start the metrics HTTP server in a background tokio task.
pub fn start(addr: SocketAddr, registry: &Registry) {
    let registry = registry.clone();
    tokio::spawn(async move {
        serve(addr, registry).await;
    });
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
        String::from_utf8(buffer).unwrap_or_default()
    });
    let _ = warp::serve(metrics_route).run(addr).await;
}
