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

pub fn get_avro_codec() -> Codec {
    let compression = COMPRESSION.lock().unwrap();
    match *compression {
        Compression::Snappy => Codec::Snappy,
        Compression::Zstd => Codec::Zstandard(ZstandardSettings::new(9)),
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
