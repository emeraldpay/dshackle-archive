use std::sync::Mutex;
use apache_avro::{Codec, ZstandardSettings};
use lazy_static::lazy_static;
use crate::args::{Args, Compression};

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
