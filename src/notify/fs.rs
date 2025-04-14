use std::fs;
use std::path::PathBuf;
use tokio::sync::mpsc::Sender;
use tokio::io::AsyncWriteExt;
use tokio::fs::File;
use crate::notify::{Notification, Notifier};

pub struct FsNotifier {
    dir: PathBuf,
}

impl FsNotifier {
    pub fn new<P: Into<PathBuf>>(dir: P) -> Self {
        Self { dir: dir.into() }
    }
}

impl Notifier for FsNotifier {
    fn start(&self) -> Sender<Notification> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let dir = self.dir.clone();
        if let Err(e) = fs::create_dir_all(&dir) {
            tracing::error!("Error creating directory: {:?}", e);
        }
        tokio::spawn(async move {
            let file = dir.join(
                format!("dshackle-archive-{}.jsonl", chrono::Utc::now().format("%Y%m%d%H%M%S"))
            );
            let file = File::create(file).await;
            if let Err(e) = file {
                tracing::error!("Error creating file: {:?}", e);
                return
            }
            let mut file = file.unwrap();
            while let Some(notification) = rx.recv().await {
                let mut data = serde_json::to_vec(&notification).unwrap();
                data.extend_from_slice(b"\n");
                let write = file.write(data.as_slice()).await;
                if let Err(e) = write {
                    tracing::error!("Error writing notification to file: {:?}", e);
                }
            }
        });
        tx
    }
}
