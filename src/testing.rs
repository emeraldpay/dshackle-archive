use std::sync::Arc;
use futures_util::StreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectMeta, ObjectStore};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

static INIT: std::sync::Once = std::sync::Once::new();

#[cfg(test)]
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
