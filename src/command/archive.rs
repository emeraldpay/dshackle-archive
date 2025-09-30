use std::marker::PhantomData;
use std::str::FromStr;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::Utc;
use shutdown::Shutdown;
use crate::args::Args;
use crate::blockchain::{BlockchainData, BlockchainTypes, TxOptions};
use crate::command::archiver::Archiver;
use crate::command::{CommandExecutor};
use crate::datakind::DataKind;
use crate::global;
use crate::notify::{Notification, RunMode};
use crate::range::Range;
use crate::storage::{TargetFile, TargetFileWriter, TargetStorage};

///
/// Provides `archive` command.
/// It builds a large archive from the blockchain by putting multiple blocks (ex., 1000) into one file
///
#[derive(Clone)]
pub struct ArchiveCommand<B: BlockchainTypes, TS: TargetStorage> {
    b: PhantomData<B>,
    archiver: Archiver<B, TS>,

    range: Range,
    chunk_size: usize,
    tx_options: TxOptions,
}

#[async_trait]
impl<B: BlockchainTypes, TS: TargetStorage> CommandExecutor for ArchiveCommand<B, TS> {

    async fn execute(&self) -> anyhow::Result<()> {
        let shutdown = global::get_shutdown();
        let ranges = self.range.split_chunks(self.chunk_size, false);

        for subrange in ranges {
            if shutdown.is_signalled() {
                break;
            }
            self.archive_range(shutdown.clone(), subrange).await?;
        }

        Ok(())
    }
}

impl<B: BlockchainTypes, TS: TargetStorage> ArchiveCommand<B, TS> {

    pub fn new(config: &Args,
                     archiver: Archiver<B, TS>
    ) -> anyhow::Result<Self> {
        let range = config.range.as_ref()
            .map(|s| Range::from_str(s.as_str()))
            .ok_or(anyhow!("Provide range to archive --range"))??;
        let chunk_size = config.range_chunk.unwrap_or(1000);

        let tx_options = TxOptions::from(config);

        Ok(Self {
            b: PhantomData,
            archiver,
            range,
            chunk_size,
            tx_options,
        })
    }

    ///
    /// Archive a specific subrange into one file
    async fn archive_range(&self, shutdown: Shutdown, range: Range) -> anyhow::Result<()> {
        let start_time = Utc::now();
        tracing::info!("Archiving range: {:?}", range);
        let block_file = self.archiver.target.create(DataKind::Blocks, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let block_file_url = block_file.get_url();

        let tx_file = self.archiver.target.create(DataKind::Transactions, &range)
            .await
            .map_err(|e| anyhow!("Unable to create file: {}", e))?;
        let tx_file_url = tx_file.get_url();

        let tx_trace_file = if self.tx_options.separate_traces {
            let file = self.archiver.target.create(DataKind::TransactionTraces, &range)
                .await
                .map_err(|e| anyhow!("Unable to create file: {}", e))?;
            Some(file)
        } else {
            None
        };
        let tx_trace_file_url = tx_trace_file.as_ref().map(|f| f.get_url());

        let heights: Vec<u64> = range.iter().collect();

        for height in heights {
            if shutdown.is_signalled() {
                return Ok(());
            }
            let (block, txes) = self.archiver.append_block(&block_file, &height.into())
                .await.context(format!("Block at {}", height))?;
            let _ = self.archiver.append_txes(&tx_file, &block, &txes, &self.tx_options.for_record(DataKind::Transactions).unwrap())
                .await.context(format!("Txes at {}", height))?;
            if let Some(trace_file) = &tx_trace_file {
                let _ = self.archiver.append_txes(&trace_file, &block, &txes, &self.tx_options.for_record(DataKind::TransactionTraces).unwrap())
                    .await.context(format!("Txes at {}", height))?;
            }
        }
        let _ = block_file.close().await?;
        let _ = tx_file.close().await?;
        if let Some(trace_file) = tx_trace_file {
            let _ = trace_file.close().await?;
        }

        let notification_block = Notification {
            // common fields
            version: Notification::version(),
            ts: Utc::now(),
            blockchain: self.archiver.data_provider.blockchain_id(),
            run: RunMode::Stream,
            height_start: range.start(),
            height_end: range.end(),

            // specific fields
            file_type: DataKind::Blocks,
            location: block_file_url,
        };
        let _ = self.archiver.notifications.send(notification_block.clone()).await;

        let notification_tx = Notification {
            file_type: DataKind::Transactions,
            location: tx_file_url,

            ..notification_block.clone()
        };
        let _ = self.archiver.notifications.send(notification_tx).await;

        if let Some(url) = tx_trace_file_url {
            let notification_tx_trace = Notification {
                file_type: DataKind::TransactionTraces,
                location: url,

                ..notification_block
            };
            let _ = self.archiver.notifications.send(notification_tx_trace).await;
        }

        let duration = Utc::now().signed_duration_since(start_time);
        if duration.num_seconds() > 2 {
            tracing::info!("Range {:?} is archived in {}sec", range, duration.num_seconds());
        } else {
            tracing::info!("Range {:?} is archived in {}ms", range, duration.num_milliseconds());
        }

        Ok(())
    }
}
