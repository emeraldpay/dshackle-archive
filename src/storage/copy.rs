pub(super) fn copy_from_sync<T: Send + 'static>(source: std::sync::mpsc::Receiver<T>) -> tokio::sync::mpsc::Receiver<T> {
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        while let Ok(record) = source.recv() {
            if let Err(e) = tx.send(record).await {
                tracing::error!("Error sending record to channel: {:?}", e);
            }
        }
    });
    rx
}
