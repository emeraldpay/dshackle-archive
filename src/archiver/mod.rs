mod archiver;
mod table;
mod block;
pub mod range;
pub mod datakind;
pub mod filenames;
pub mod range_bag;
pub mod blocks_config;
pub mod range_group;
pub mod resume;
pub mod order;

pub use archiver::{ArchiveAll, Archiver};
pub use resume::{ScanResume, StreamResume};
pub use order::{AppendSink, OrderedSink};

use crate::blockchain::BlockchainTypes;
use crate::notify::Notification;

#[allow(type_alias_bounds)]
pub type BlockTransactions<B: BlockchainTypes> = Vec<(B::BlockParsed, Vec<B::TxId>)>;

pub type BlockHash = String;

/// Result of an archiver process step that can be cleanly cut short by
/// upstream cancellation (e.g., a re-org invalidating the block being
/// fetched).
///
/// `Completed { value, notifications }` is the normal outcome — work
/// finished and produced the inner value, plus the notifications the
/// caller should publish once the entire run is known to be uncancelled
/// (one per location the target reported; per-height targets produce one
/// per archived height). Notifications are NOT sent inside the process
/// step itself: otherwise a concurrent `process_txes` and
/// `process_traces` could race the cancellation signal and emit a torn
/// notification stream (one side publishes before observing the cancel,
/// the other observes it and stays silent), leaving downstream consumers
/// with partial-block state they can't distinguish from corruption.
///
/// `Cancelled` means the run was abandoned cooperatively; any
/// partially-written rows are dropped and the file is left to Drop (which
/// deletes it for file backends and is a no-op for streaming brokers
/// whose messages have already landed on the wire). Real errors continue
/// to flow through `Result::Err`, so callers' `?` still abort on genuine
/// fetch / storage failures.
#[derive(Debug)]
pub enum ProcessOutcome<T> {
    Completed {
        value: T,
        notifications: Vec<Notification>,
    },
    Cancelled,
}

impl<T> ProcessOutcome<T> {
    pub fn is_cancelled(&self) -> bool {
        matches!(self, ProcessOutcome::Cancelled)
    }
}
