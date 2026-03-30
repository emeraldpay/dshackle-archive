// Copyright 2025 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use lazy_static::lazy_static;

/// Number of past snapshots to keep. Each snapshot is taken once per minute,
/// so this controls how many minutes the sliding speed window covers.
const WINDOW_SIZE: usize = 5;

lazy_static! {
    static ref PROGRESS: Progress = Progress::new();
}

/// Tracks archiving progress and periodically reports statistics to the console.
///
/// Maintains global counters for blocks processed and bytes written, and spawns
/// a background task that logs throughput every 60 seconds. The reported speed is
/// a sliding average over the past [`WINDOW_SIZE`] minutes.
///
/// For streaming mode, the timer can be paused while waiting for new blocks so that
/// speed calculations reflect only active processing time.
struct Progress {
    /// Total number of blocks processed since start
    blocks: AtomicU64,
    /// Total number of bytes written to storage since start
    bytes: AtomicU64,
    /// Whether the background reporter has been started
    started: AtomicBool,
    /// Mutable timing state
    state: Mutex<ProgressState>,
}

struct ProgressState {
    /// Cumulative active processing time (excluding the current active period)
    total_active: Duration,
    /// When the current active period started (`None` if paused)
    active_since: Option<Instant>,
    /// Ring buffer of past snapshots for sliding window speed calculation.
    /// Seeded with a zero snapshot at creation so the first report has a baseline.
    snapshots: VecDeque<Snapshot>,
}

/// A point-in-time snapshot of the progress counters, used for sliding window calculation.
#[derive(Clone)]
struct Snapshot {
    blocks: u64,
    bytes: u64,
    /// Cumulative active time at the moment this snapshot was taken
    active: Duration,
}

impl Progress {
    fn new() -> Self {
        let initial = Snapshot { blocks: 0, bytes: 0, active: Duration::ZERO };
        Self {
            blocks: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            started: AtomicBool::new(false),
            state: Mutex::new(ProgressState {
                total_active: Duration::ZERO,
                active_since: Some(Instant::now()),
                snapshots: VecDeque::from([initial]),
            }),
        }
    }
}

/// Return cumulative active duration including the current (not yet paused) period.
fn current_active(state: &ProgressState) -> Duration {
    let mut active = state.total_active;
    if let Some(start) = state.active_since {
        active += start.elapsed();
    }
    active
}

/// Start the periodic progress reporter. Must be called from within a tokio runtime.
///
/// Spawns a background task that logs throughput every 60 seconds. The task stops
/// automatically on shutdown signal. Calling this more than once is a no-op.
pub fn start() {
    if PROGRESS.started.swap(true, Ordering::SeqCst) {
        return;
    }
    tokio::spawn(async {
        let shutdown = crate::global::get_shutdown();
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await; // skip the first immediate tick
        loop {
            tokio::select! {
                _ = shutdown.signalled() => break,
                _ = interval.tick() => {
                    report();
                }
            }
        }
    });
}

/// Record that one block has been processed.
pub fn add_block() {
    PROGRESS.blocks.fetch_add(1, Ordering::Relaxed);
}

/// Record that `n` bytes have been written to storage.
pub fn add_bytes(n: usize) {
    PROGRESS.bytes.fetch_add(n as u64, Ordering::Relaxed);
}

/// Pause the progress timer. Call this when waiting for new blocks (streaming mode).
///
/// While paused, elapsed time does not count toward speed calculations, so idle
/// wait time does not dilute the reported throughput.
pub fn pause() {
    let mut state = PROGRESS.state.lock().unwrap();
    if let Some(start) = state.active_since.take() {
        state.total_active += start.elapsed();
    }
}

/// Resume the progress timer after a pause.
pub fn resume() {
    let mut state = PROGRESS.state.lock().unwrap();
    if state.active_since.is_none() {
        state.active_since = Some(Instant::now());
    }
}

/// Take a snapshot and log the sliding-window speed report.
fn report() {
    let blocks = PROGRESS.blocks.load(Ordering::Relaxed);
    let bytes = PROGRESS.bytes.load(Ordering::Relaxed);

    if blocks == 0 {
        return;
    }

    let mut state = PROGRESS.state.lock().unwrap();
    let active = current_active(&state);

    let current = Snapshot { blocks, bytes, active };

    // The oldest snapshot is the baseline for the sliding window
    let oldest = state.snapshots.front().cloned();

    // Maintain the ring buffer: keep at most WINDOW_SIZE past snapshots
    state.snapshots.push_back(current.clone());
    if state.snapshots.len() > WINDOW_SIZE {
        state.snapshots.pop_front();
    }

    drop(state);

    let Some(oldest) = oldest else {
        return;
    };

    let delta_blocks = current.blocks - oldest.blocks;
    let delta_bytes = current.bytes - oldest.bytes;
    let delta_active = current.active.saturating_sub(oldest.active);

    let active_secs = delta_active.as_secs_f64();
    if active_secs < 0.001 {
        tracing::info!("Progress: {} blocks total (paused)", blocks);
        return;
    }

    let blocks_per_min = (delta_blocks as f64) / active_secs * 60.0;
    let bytes_per_sec = (delta_bytes as f64) / active_secs;
    let throughput = format_throughput(bytes_per_sec);

    tracing::info!(
        "Progress: {} blocks; {:.1} blocks/min; {}",
        blocks, blocks_per_min, throughput
    );
}

fn format_throughput(bytes_per_sec: f64) -> String {
    if bytes_per_sec < 1024.0 {
        format!("{:.0} bytes/sec", bytes_per_sec)
    } else if bytes_per_sec < 1024.0 * 1024.0 {
        format!("{:.1} kb/sec", bytes_per_sec / 1024.0)
    } else {
        format!("{:.1} mb/sec", bytes_per_sec / (1024.0 * 1024.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_throughput_bytes() {
        assert_eq!(format_throughput(0.0), "0 bytes/sec");
        assert_eq!(format_throughput(512.0), "512 bytes/sec");
        assert_eq!(format_throughput(1023.0), "1023 bytes/sec");
    }

    #[test]
    fn test_format_throughput_kb() {
        assert_eq!(format_throughput(1024.0), "1.0 kb/sec");
        assert_eq!(format_throughput(1536.0), "1.5 kb/sec");
        assert_eq!(format_throughput(500_000.0), "488.3 kb/sec");
    }

    #[test]
    fn test_format_throughput_mb() {
        assert_eq!(format_throughput(1_048_576.0), "1.0 mb/sec");
        assert_eq!(format_throughput(5_242_880.0), "5.0 mb/sec");
    }
}
