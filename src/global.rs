// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use std::sync::Mutex;
use std::time::Duration;
use apache_avro::{Codec, ZstandardSettings};
use lazy_static::lazy_static;
use tokio_retry2::strategy::{jitter, ExponentialFactorBackoff};
use crate::args::{Args, Compression, RetryMode};

/// Configuration for parallelism limits across different archival operations.
///
/// Each field controls the maximum number of concurrent tasks for a specific operation type.
/// Values are resolved from CLI args (`--parallel`) and environment variables, with sensible
/// defaults derived from the API limit.
#[derive(Debug, Clone)]
pub struct ThreadsConfig {
    /// Max concurrent API requests to the blockchain node (set via `--parallel` or `EMERALD_DSHACKLE_THREADS_API`)
    pub api: usize,
    /// Max concurrent transaction fetch tasks (set via `EMERALD_DSHACKLE_THREADS_TX`, default: api / 2)
    pub tx: usize,
    /// Max concurrent trace fetch tasks (set via `EMERALD_DSHACKLE_THREADS_TRACE`, default: api / 4)
    pub trace: usize,
    /// Max concurrent block fetch tasks (set via `EMERALD_DSHACKLE_THREADS_BLOCK`, default: api / 2)
    pub blocks: usize,
}

lazy_static! {
    pub static ref SHUTDOWN: shutdown::Shutdown = shutdown::Shutdown::new().expect("Failed to create a shutdown hook");
    static ref COMPRESSION: Mutex<Compression> = Mutex::new(Compression::Zstd);
    static ref DRY_RUN: Mutex<bool> = Mutex::new(false);
    static ref THREADS: Mutex<ThreadsConfig> = Mutex::new(ThreadsConfig { api: 16, tx: 8, trace: 4, blocks: 8 });
    // Default to the bounded policy (matching pre-flag behaviour) until
    // `set_retry_policy` resolves the real value at startup.
    static ref RETRY_POLICY: Mutex<RetryPolicy> = Mutex::new(RetryPolicy::Bounded {
        max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
    });
}

pub fn get_shutdown() -> shutdown::Shutdown {
    SHUTDOWN.clone()
}

/// Avro codec for `--format=avro` writes.
///
/// Zstd level **9** is intentional: Avro files are long-lived archive
/// artifacts (held for months/years, scanned by downstream batch jobs), so we
/// pay one-time CPU at write time in exchange for the smaller storage
/// footprint that compounds across the whole archive. The level is high
/// enough to noticeably beat default (~level 3) on the JSON-heavy payloads
/// dshackle-archive writes, while still well below the diminishing-returns
/// zone above ~15.
pub fn get_avro_codec() -> Codec {
    let compression = COMPRESSION.lock().unwrap();
    match *compression {
        Compression::Snappy => Codec::Snappy,
        Compression::Zstd => Codec::Zstandard(ZstandardSettings::new(9)),
    }
}

/// Map the user-selected compression to a Pulsar producer compression option.
///
/// Honours the same `--compression` flag the Avro path uses, so a single
/// archive run uses a consistent codec choice across whichever target it
/// writes to. Pulsar's `compression` feature is enabled by default in the
/// upstream crate, so both `Zstd` and `Snappy` are always available here.
///
/// Note the **level asymmetry vs. [`get_avro_codec`]**: this returns
/// `CompressionZstd::default()` (≈ level 3), whereas the Avro path uses
/// level 9. The trade-off is intentional:
///
/// - Avro files are long-lived archive artifacts where write CPU amortizes
///   across years of cold storage — level 9 favours ratio.
/// - Broker topics are typically short-retention live streams. Producer-side
///   compression sits on the latency path of every published message, so a
///   faster, ratio-modest codec is the better default. If someone needs
///   tighter compression on a Pulsar topic they can negotiate it
///   broker-side; for our v1 we keep write latency low.
pub fn get_pulsar_compression() -> pulsar::compression::Compression {
    let compression = COMPRESSION.lock().unwrap();
    match *compression {
        Compression::Snappy => pulsar::compression::Compression::Snappy(
            pulsar::compression::CompressionSnappy::default(),
        ),
        Compression::Zstd => pulsar::compression::Compression::Zstd(
            pulsar::compression::CompressionZstd::default(),
        ),
    }
}

/// Map the user-selected compression to a Kafka produce-call compression.
///
/// Same trade-off as [`get_pulsar_compression`]: a broker topic is a live
/// stream, so the codec sits on the latency path of every published message.
/// Unlike Pulsar, rskafka takes the codec per `produce()` call rather than per
/// producer, so the target resolves this once and carries it to its writers.
pub fn get_kafka_compression() -> rskafka::client::partition::Compression {
    let compression = COMPRESSION.lock().unwrap();
    match *compression {
        Compression::Snappy => rskafka::client::partition::Compression::Snappy,
        Compression::Zstd => rskafka::client::partition::Compression::Zstd,
    }
}

pub fn set_compression(args: &Args) {
    let compression = args.compression.clone().unwrap_or(Compression::Zstd);
    let mut comp = COMPRESSION.lock().unwrap();
    *comp = compression.clone();
}

pub fn is_dry_run() -> bool {
    let dry_run = DRY_RUN.lock().unwrap();
    *dry_run
}

pub fn set_dry_run(args: &Args) {
    let dry_run = args.dry_run;
    let mut dr = DRY_RUN.lock().unwrap();
    *dr = dry_run;
}

const MAX_THREADS: usize = 512;
const DEFAULT_API: usize = 16;

/// Initialize the threads configuration from CLI args and environment variables.
///
/// Resolution order for the API limit:
/// 1. `--parallel` CLI arg
/// 2. `EMERALD_DSHACKLE_THREADS_API` env var
/// 3. Default value (16)
///
/// For tx/trace/blocks: env var takes priority, otherwise defaults are derived from the API limit
/// (tx = api/2, trace = api/4, blocks = api/2, each clamped to 1..max).
/// When set explicitly via env, only the global max of 512 is enforced.
pub fn set_threads(args: &Args) {
    let api = args.connection.parallel
        .or_else(|| read_env("EMERALD_DSHACKLE_THREADS_API"))
        .unwrap_or(DEFAULT_API)
        .clamp(1, MAX_THREADS);

    let tx = read_env("EMERALD_DSHACKLE_THREADS_TX")
        .map(|v| v.clamp(1, MAX_THREADS))
        .unwrap_or((api / 2).clamp(1, 64));

    let trace = read_env("EMERALD_DSHACKLE_THREADS_TRACE")
        .map(|v| v.clamp(1, MAX_THREADS))
        .unwrap_or((api / 4).clamp(1, 16));

    let blocks = read_env("EMERALD_DSHACKLE_THREADS_BLOCK")
        .map(|v| v.clamp(1, MAX_THREADS))
        .unwrap_or((api / 2).clamp(1, 64));

    let config = ThreadsConfig { api, tx, trace, blocks };
    tracing::info!(
        api = config.api, tx = config.tx, trace = config.trace, blocks = config.blocks,
        "Threads configuration"
    );
    let mut threads = THREADS.lock().unwrap();
    *threads = config;
}

/// Returns the current threads configuration.
pub fn get_threads() -> ThreadsConfig {
    THREADS.lock().unwrap().clone()
}

fn read_env(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

/// Number of attempts the `Bounded` retry policy uses. Matches the
/// hardcoded `.take(10)` the per-RPC helpers used to carry inline before this
/// became a global. Not exposed as a CLI knob yet — most callers either
/// accept the default or switch to `Forever`.
pub const DEFAULT_RETRY_MAX_ATTEMPTS: usize = 10;

/// Default cap on the time between retry attempts for blockchain fetches,
/// shared by all chain providers. Higher than the typical 95th-percentile RPC
/// latency so a degraded node has space to recover, but low enough that a
/// transient blip doesn't stall a block for noticeably long.
pub const RETRY_MAX_DELAY_FAST_SECS: u64 = 2;

/// Runtime retry policy resolved from CLI args (and the target type).
///
/// File targets default to [`RetryPolicy::Bounded`]: a transient node failure
/// leaves a gap that the `fix`/`verify` commands can repair later. Ordered
/// streaming targets default to [`RetryPolicy::Forever`]: a missing record
/// permanently breaks the topic-order contract, so the writer must wait the
/// node out instead. The user can override either default via `--retry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPolicy {
    Bounded { max_attempts: usize },
    Forever,
}

/// Initialise the retry policy from CLI args.
///
/// Explicit `--retry` wins; otherwise the default is derived from the target
/// type — streaming-ordered targets need [`RetryPolicy::Forever`] to keep
/// their order contract; everything else can fail fast and be repaired
/// later. Logs the resolved policy so operators can see what's in effect.
pub fn set_retry_policy(args: &Args) {
    let policy = resolve_retry_policy(args);
    tracing::info!("Retry policy: {:?}", policy);
    *RETRY_POLICY.lock().unwrap() = policy;
}

fn resolve_retry_policy(args: &Args) -> RetryPolicy {
    match args.retry {
        Some(RetryMode::Bounded) => RetryPolicy::Bounded {
            max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
        },
        Some(RetryMode::Forever) => RetryPolicy::Forever,
        None => {
            if crate::storage::is_streaming(args) {
                RetryPolicy::Forever
            } else {
                RetryPolicy::Bounded {
                    max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
                }
            }
        }
    }
}

/// Current retry policy. Cheap (one mutex lock); callers can call it once
/// per retry-strategy build.
pub fn get_retry_policy() -> RetryPolicy {
    *RETRY_POLICY.lock().unwrap()
}

/// Build the iterator passed to `tokio_retry2::Retry::spawn` for a single
/// fetch attempt sequence.
///
/// Same shape regardless of policy — exponential backoff with jitter, capped
/// at `max_delay_secs` between attempts — but the iterator is bounded or
/// unbounded based on [`get_retry_policy`]. Returned boxed so both branches
/// have the same type at the call site (the underlying iterator types
/// otherwise differ between `Take<…>` and the unbounded form).
pub fn retry_strategy(max_delay_secs: u64) -> Box<dyn Iterator<Item = Duration> + Send> {
    match get_retry_policy() {
        RetryPolicy::Bounded { max_attempts } => Box::new(backoff(max_delay_secs).take(max_attempts)),
        RetryPolicy::Forever => Box::new(backoff(max_delay_secs)),
    }
}

/// Always-bounded variant of [`retry_strategy`] that ignores the `--retry`
/// policy.
///
/// For callers that must not wait indefinitely even under `--retry=forever`.
/// Specifically the re-org follower's linkage walk: the follower is the
/// component that *detects* replaced blocks and fires the cancellation
/// tokens, so no signal can ever break it out of a retry on a block that was
/// re-orged away and will never appear. It has to give up on its own and let
/// the next head event re-validate the chain.
pub fn retry_strategy_bounded(max_delay_secs: u64) -> Box<dyn Iterator<Item = Duration> + Send> {
    Box::new(backoff(max_delay_secs).take(DEFAULT_RETRY_MAX_ATTEMPTS))
}

/// Backoff shape shared by every retry strategy: exponential with jitter,
/// capped at `max_delay_secs` between attempts.
fn backoff(max_delay_secs: u64) -> impl Iterator<Item = Duration> + Send {
    ExponentialFactorBackoff::from_millis(100, 1.75)
        .max_delay(Duration::from_secs(max_delay_secs))
        .map(jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_with(retry: Option<RetryMode>, stream_url: Option<&str>) -> Args {
        let stream = stream_url.map(|url| crate::args::Stream {
            stream_url: Some(url.to_string()),
            stream_topics: Some("archive-eth".to_string()),
        });
        Args {
            retry,
            stream,
            ..Args::default()
        }
    }

    const PULSAR: Option<&str> = Some("pulsar://localhost:6650");
    const KAFKA: Option<&str> = Some("kafka://localhost:9092");

    #[test]
    fn explicit_bounded_wins_over_streaming_default() {
        let policy = resolve_retry_policy(&args_with(Some(RetryMode::Bounded), PULSAR));
        assert!(matches!(policy, RetryPolicy::Bounded { .. }));
    }

    #[test]
    fn explicit_forever_wins_over_file_default() {
        let policy = resolve_retry_policy(&args_with(Some(RetryMode::Forever), None));
        assert!(matches!(policy, RetryPolicy::Forever));
    }

    #[test]
    fn pulsar_defaults_to_forever() {
        let policy = resolve_retry_policy(&args_with(None, PULSAR));
        assert!(matches!(policy, RetryPolicy::Forever));
    }

    #[test]
    fn kafka_defaults_to_forever() {
        let policy = resolve_retry_policy(&args_with(None, KAFKA));
        assert!(matches!(policy, RetryPolicy::Forever));
    }

    #[test]
    fn file_target_defaults_to_bounded() {
        let policy = resolve_retry_policy(&args_with(None, None));
        assert!(matches!(
            policy,
            RetryPolicy::Bounded {
                max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS
            }
        ));
    }
}
