use std::marker::PhantomData;
use std::sync::Arc;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use crate::blockchain::{BlockchainData, BlockchainTypes};
use crate::archiver::datakind::DataOptions;
use crate::archiver::ProcessOutcome;
use crate::notify::empty::EmptyNotifier;
use crate::notify::{Maturity, Notification, NotificationBuilder, Notifier, RunMode};
use crate::archiver::range::{Height, Range};
use crate::global;
use crate::storage::WriteTarget;

pub struct Archiver<B: BlockchainTypes, TS: WriteTarget> {
    b: PhantomData<B>,
    pub target: Arc<TS>,
    pub data_provider: Arc<B::DataProvider>,
    pub notifications: Sender<Notification>,
}

// Manual `Clone` impl: all fields are reference-counted, so cloning works
// without requiring `B: Clone` or `TS: Clone` (which derive(Clone) would
// otherwise demand).
impl<B: BlockchainTypes, TS: WriteTarget> Clone for Archiver<B, TS> {
    fn clone(&self) -> Self {
        Self {
            b: PhantomData,
            target: self.target.clone(),
            data_provider: self.data_provider.clone(),
            notifications: self.notifications.clone(),
        }
    }
}

impl<B: BlockchainTypes, TS: WriteTarget> Archiver<B, TS> {

    pub fn new_simple(target: Arc<TS>, data_provider: Arc<B::DataProvider>) -> Self {
        Self::new(
            target,
            data_provider,
            EmptyNotifier::default().start(),
        )
    }

    pub fn new(target: Arc<TS>,
               data_provider: Arc<B::DataProvider>,
               notifications: Sender<Notification>,
    ) -> Self {
        Self {
            b: PhantomData,
            target,
            data_provider,
            notifications,
        }
    }

}

/// Drives a full archive run for a single height or a range of heights.
///
/// The `cancel` token lets the caller abort cooperatively — it's wired into
/// every parallel fetcher and into the ordering layer, so a fire mid-run
/// drops in-flight RPCs and abandons partial sinks without raising a
/// "closed with gap" error. Non-cancellable callers (historical archive,
/// finalized streams) pass a fresh, never-fired token.
#[async_trait]
pub trait ArchiveAll<T> {
    async fn archive(
        &self,
        what: T,
        mode: RunMode,
        maturity: Option<Maturity>,
        options: &DataOptions,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()>;
}

#[async_trait]
impl<B: BlockchainTypes, TS: WriteTarget> ArchiveAll<Height> for Archiver<B, TS> {
    async fn archive(
        &self,
        what: Height,
        mode: RunMode,
        maturity: Option<Maturity>,
        options: &DataOptions,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let start_time = Utc::now();

        let template = NotificationBuilder {
            blockchain: self.data_provider.blockchain_id(),
            run: mode,
            maturity,
        };

        let (blocks, blocks_notifs) = match self
            .process_blocks(Range::Single(what.clone()), template.clone(), options, cancel)
            .await?
        {
            ProcessOutcome::Completed { value, notifications } => (value, notifications),
            ProcessOutcome::Cancelled => {
                tracing::info!(
                    "Block {} cancelled (re-org) — skipping tx/trace fetch",
                    what
                );
                return Ok(());
            }
        };
        let range = Range::Single(what.clone());

        // Run txes and traces concurrently. Each branch returns either:
        //   - Some(ProcessOutcome::Completed { .. } | ProcessOutcome::Cancelled)
        //     when the side was actually run
        //   - None when the side errored — we log inline, surface the error
        //     state to the joint, but don't abort the other side
        //   - Some(Completed { value: (), notification: None }) when the
        //     side is disabled by DataOptions (treated as success-with-nothing)
        let (tx_side, trace_side) = tokio::join! {
            async {
                if options.include_tx() {
                    match self.process_txes(range.clone(), template.clone(), &blocks, options, cancel).await {
                        Ok(outcome) => Some(outcome),
                        Err(e) => {
                            tracing::warn!("Failed to archive txes for block {}: {}", what, e);
                            None
                        }
                    }
                } else {
                    Some(ProcessOutcome::Completed { value: (), notifications: vec![] })
                }
            },
            async {
                if options.include_trace() {
                    match self.process_traces(range.clone(), template.clone(), &blocks, options, cancel).await {
                        Ok(outcome) => Some(outcome),
                        Err(e) => {
                            tracing::warn!("Failed to archive traces for block {}: {}", what, e);
                            None
                        }
                    }
                } else {
                    Some(ProcessOutcome::Completed { value: (), notifications: vec![] })
                }
            }
        };

        let duration = Utc::now().signed_duration_since(start_time);
        let duration_secs = duration.num_milliseconds() as f64 / 1000.0;
        crate::metrics::observe_block_archive(duration_secs);

        let cancelled = tx_side.as_ref().map_or(false, |o| o.is_cancelled())
            || trace_side.as_ref().map_or(false, |o| o.is_cancelled());
        let errored = tx_side.is_none() || trace_side.is_none();

        // Publish notifications only when neither side cancelled — otherwise
        // a torn notification stream (blocks notified, txes/traces silent)
        // would mislead consumers into thinking the archive is incomplete
        // due to corruption rather than a re-org. On cancel the partial
        // artifacts have already been cleaned up via writer Drop in the
        // process_* functions; the doomed block is invisible to consumers.
        if !cancelled && !global::get_shutdown().is_signalled() {
            self.publish_notifications(
                blocks_notifs
                    .into_iter()
                    .chain(extract_notifications(tx_side.as_ref()))
                    .chain(extract_notifications(trace_side.as_ref())),
            )
            .await;
        }

        if cancelled {
            tracing::info!(
                "Block {} cancelled (re-org) in {}ms",
                what,
                duration.num_milliseconds()
            );
        } else if errored {
            tracing::warn!(
                "Blocks {} is partially archived (with error) in {}ms",
                what,
                duration.num_milliseconds()
            );
        } else {
            tracing::info!(
                "Blocks {} is archived in {}ms",
                what,
                duration.num_milliseconds()
            );
        }
        Ok(())
    }
}

#[async_trait]
impl<B: BlockchainTypes, TS: WriteTarget> ArchiveAll<Range> for Archiver<B, TS> {
    async fn archive(
        &self,
        what: Range,
        mode: RunMode,
        maturity: Option<Maturity>,
        options: &DataOptions,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let start_time = Utc::now();
        tracing::debug!("Archiving range: {}", what);

        let template = NotificationBuilder {
            blockchain: self.data_provider.blockchain_id(),
            run: mode,
            maturity,
        };

        let (blocks, blocks_notifs) = match self
            .process_blocks(what.clone(), template.clone(), options, cancel)
            .await?
        {
            ProcessOutcome::Completed { value, notifications } => (value, notifications),
            ProcessOutcome::Cancelled => {
                tracing::info!(range = %what, "Range archive cancelled before tx/trace fetch");
                return Ok(());
            }
        };

        let (result_tx, result_trace) = tokio::join!(
            async {
                if options.include_tx() {
                    tracing::debug!(range = %what, "Process txes");
                    self.process_txes(what.clone(), template.clone(), &blocks, options, cancel).await
                } else {
                    Ok(ProcessOutcome::Completed { value: (), notifications: vec![] })
                }
            },
            async {
                if options.include_trace() {
                    tracing::debug!(range = %what, "Process traces");
                    self.process_traces(what.clone(), template.clone(), &blocks, options, cancel).await
                } else {
                    Ok(ProcessOutcome::Completed { value: (), notifications: vec![] })
                }
            }
        );
        // Surface both errors when both sides fail — `?` on the first would
        // silently drop the second. Operators investigating a stuck Range
        // archive need both messages.
        let (tx_outcome, trace_outcome) = match (result_tx, result_trace) {
            (Ok(tx), Ok(trace)) => (tx, trace),
            (Err(tx_err), Err(trace_err)) => {
                tracing::warn!(range = %what, "process_traces failed: {}", trace_err);
                return Err(tx_err);
            }
            (Err(tx_err), Ok(_)) => return Err(tx_err),
            (Ok(_), Err(trace_err)) => return Err(trace_err),
        };

        let shutdown = global::get_shutdown();
        if shutdown.is_signalled() {
            return Ok(());
        }

        let cancelled = tx_outcome.is_cancelled() || trace_outcome.is_cancelled();
        if !cancelled {
            self.publish_notifications(
                blocks_notifs
                    .into_iter()
                    .chain(extract_notifications(Some(&tx_outcome)))
                    .chain(extract_notifications(Some(&trace_outcome))),
            )
            .await;
        }

        let duration = Utc::now().signed_duration_since(start_time);
        if what.len() == 1 {
            let duration_secs = duration.num_milliseconds() as f64 / 1000.0;
            crate::metrics::observe_block_archive(duration_secs);
        }
        if cancelled {
            tracing::info!(range = %what, "Range archive cancelled mid-run");
        } else if duration.num_seconds() > 2 {
            tracing::info!("Range {} is archived in {}sec", what, duration.num_seconds());
        } else {
            tracing::info!("Range {} is archived in {}ms", what, duration.num_milliseconds());
        }

        Ok(())
    }
}

/// Extract the deferred [`Notification`]s from a [`ProcessOutcome`].
///
/// Returns nothing when the side errored (outer Option is None), was
/// cancelled, or completed without producing notifications (target skipped
/// an existing file). The archiver coordinator collects these across all
/// three process_* calls and publishes them as a batch — only when the
/// whole run is known to be uncancelled.
fn extract_notifications<T>(side: Option<&ProcessOutcome<T>>) -> Vec<Notification> {
    match side {
        Some(ProcessOutcome::Completed { notifications, .. }) => notifications.clone(),
        _ => vec![],
    }
}

impl<B: BlockchainTypes, TS: WriteTarget> Archiver<B, TS> {
    /// Publish the queued notifications from a completed run. Errors from
    /// the notification channel are logged but not propagated — the data
    /// itself has already landed in the archive; a failed notify shouldn't
    /// fail the whole run.
    async fn publish_notifications(&self, notifs: impl IntoIterator<Item = Notification>) {
        for notif in notifs {
            if let Err(e) = self.notifications.send(notif).await {
                tracing::warn!("Failed to publish notification: {}", e);
            }
        }
    }
}
