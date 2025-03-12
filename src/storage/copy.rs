use tokio::sync::mpsc::UnboundedReceiver;
use crate::global;

pub(super) fn copy_from_sync<T: Send + 'static>(mut source: UnboundedReceiver<T>) -> tokio::sync::mpsc::Receiver<T> {
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    tokio::spawn(async move {
        let shutdown = global::get_shutdown();
        while !shutdown.is_signalled() {
            tokio::select! {
                _ = shutdown.signalled() => break,
                record = source.recv() => {
                    if let Some(record) = record {
                        if let Err(e) = tx.send(record).await {
                            tracing::error!("Error sending record to channel: {:?}", e);
                            break
                        }
                    } else {
                        break
                    }
                }
            }
        }
    });
    rx
}
