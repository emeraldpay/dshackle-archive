use std::sync::Arc;
use futures_util::StreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectMeta, ObjectStore};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use crate::blockchain::{BlockReference, BlockchainData};
use crate::blockchain::mock::{MockType};
use crate::archiver::Archiver;
use crate::archiver::datakind::{DataKind, TraceOptions};
use crate::archiver::range::Range;
use crate::storage::{TargetFileWriter, TargetStorage};

static INIT: std::sync::Once = std::sync::Once::new();

pub fn start_test() {
    INIT.call_once(|| {
        init_tracing();
    });
}

fn init_tracing() {
    let filter = Targets::new()
        .with_target("dshackle_archive", tracing::level_filters::LevelFilter::TRACE)
        .with_default(tracing::level_filters::LevelFilter::INFO);
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_filter(filter.clone())
        ;
    let subscriber = tracing_subscriber::registry()
        .with(stdout_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");
}


pub async fn list_mem_files(mem: Arc<InMemory>) -> Vec<ObjectMeta> {
    let mut result = Vec::new();
    let mut stream = mem.list(None);
    while let Some(meta) = stream.next().await {
        if let Ok(meta) = meta {
            result.push(meta);
        }
    }
    result
}

pub async fn list_mem_filenames(mem: Arc<InMemory>) -> Vec<String> {
    list_mem_files(mem).await.into_iter()
        .map(|m| m.location.as_ref().to_string())
        .collect()
}

pub async fn write_block_and_tx<TS: TargetStorage>(
    archiver: &Archiver<MockType, TS>,
    height: u64, tx_index: Option<Vec<usize>>
) -> anyhow::Result<()> {
    let block = archiver.data_provider.find_block(height).unwrap();
    let file_block = archiver.target
        .create(DataKind::Blocks, &Range::Single(height.into()))
        .await.expect("Create block");

    let record = archiver.data_provider.fetch_block(&BlockReference::Height(height.into())).await?;
    file_block.append(record.0).await?;
    file_block.close().await?;

    let file_txes = archiver.target
        .create(DataKind::Transactions, &Range::Single(height.into()))
        .await.expect("Create txes");

    let txes = match tx_index {
        None => block.transactions.iter().enumerate().map(|(i, _)| i).collect(),
        Some(v) => v,
    };

    for i in txes {
        let record = archiver.data_provider.fetch_tx(&block, i).await?;
        file_txes.append(record).await?;
    }
    file_txes.close().await?;

    Ok(())
}

/// Write block, transactions, and traces for the given height
///
/// # Arguments
/// * `archiver` - The archiver instance
/// * `height` - Block height to write
/// * `tx_index` - Optional list of transaction indices to write (None = write all)
/// * `trace_options` - Optional trace options (None = no traces)
pub async fn write_block_tx_and_traces<TS: TargetStorage>(
    archiver: &Archiver<MockType, TS>,
    height: u64,
    tx_index: Option<Vec<usize>>,
    trace_options: Option<TraceOptions>,
) -> anyhow::Result<()> {
    let block = archiver.data_provider.find_block(height).unwrap();

    // Write block
    let file_block = archiver.target
        .create(DataKind::Blocks, &Range::Single(height.into()))
        .await.expect("Create block");

    let record = archiver.data_provider.fetch_block(&BlockReference::Height(height.into())).await?;
    file_block.append(record.0).await?;
    file_block.close().await?;

    // Write transactions
    let file_txes = archiver.target
        .create(DataKind::Transactions, &Range::Single(height.into()))
        .await.expect("Create txes");

    let txes = match tx_index {
        None => block.transactions.iter().enumerate().map(|(i, _)| i).collect(),
        Some(v) => v,
    };

    for i in &txes {
        let record = archiver.data_provider.fetch_tx(&block, *i).await?;
        file_txes.append(record).await?;
    }
    file_txes.close().await?;

    // Write traces if requested
    if let Some(options) = trace_options {
        let file_traces = archiver.target
            .create(DataKind::TransactionTraces, &Range::Single(height.into()))
            .await.expect("Create traces");

        for i in &txes {
            let record = archiver.data_provider.fetch_traces(&block, *i, &options).await?;
            file_traces.append(record).await?;
        }
        file_traces.close().await?;
    }

    Ok(())
}
