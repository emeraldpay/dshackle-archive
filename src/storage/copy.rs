use tokio::sync::mpsc::UnboundedReceiver;

pub(super) fn copy_from_sync<T: Send + 'static>(mut source: UnboundedReceiver<T>) -> tokio::sync::mpsc::Receiver<T> {
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        while let Some(record) = source.recv().await {
            if let Err(e) = tx.send(record).await {
                tracing::error!("Error sending record to channel: {:?}", e);
                break
            }
        }
    });
    rx
}
