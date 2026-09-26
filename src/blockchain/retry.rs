// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Retrying a blockchain fetch while logging its failure once, not per attempt.
//!
//! Most failed attempts are recovered by the next one (a node that hasn't seen
//! a fresh block yet, a connection dropped by the load balancer), so logging
//! each of them buries the failures that matter under noise. The individual
//! attempts are logged at `debug` by the connection; the fetch as a whole is
//! reported here only when retrying didn't help.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_retry2::{Retry, RetryError};

/// With a strategy that retries forever there's no final attempt to report,
/// so a fetch that keeps failing is reported every this many attempts instead
/// of staying silent while the archive stalls on it.
const STILL_FAILING_EVERY: usize = crate::global::DEFAULT_RETRY_MAX_ATTEMPTS;

/// Run `fetch` under `strategy` (see [`crate::global::retry_strategy`]),
/// logging an error with the last failure when all attempts are exhausted.
/// `describe` names what's being fetched, for that log line.
pub async fn retry_fetch<T, F, Fut>(
    strategy: Box<dyn Iterator<Item = Duration> + Send>,
    describe: impl Fn() -> String,
    fetch: F,
) -> anyhow::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    // A bounded strategy is a `take(n)` of the backoff, which always reports
    // an upper bound; the unbounded backoff doesn't.
    let forever = strategy.size_hint().1.is_none();
    let attempts = AtomicUsize::new(0);
    let result = Retry::spawn(strategy, async || {
        let attempt = attempts.fetch_add(1, Ordering::Relaxed) + 1;
        fetch().await.map_err(|e| {
            if forever && attempt % STILL_FAILING_EVERY == 0 {
                tracing::warn!("{} still failing after {} attempts: {:#}", describe(), attempt, e);
            }
            RetryError::transient(e)
        })
    }).await;
    if let Err(e) = &result {
        tracing::error!("{} failed after {} attempts: {:#}", describe(), attempts.load(Ordering::Relaxed), e);
    }
    result
}
