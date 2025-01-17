use tokio::sync::mpsc::{Sender};
use crate::notify::{Notification, Notifier};

pub struct EmptyNotifier {}

impl Notifier for EmptyNotifier {
    fn start(&self) -> Sender<Notification> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        tokio::spawn(async move {
            loop {
                let _ = rx.recv().await;
            }
        });
        tx
    }
}

impl Default for EmptyNotifier {
    fn default() -> Self {
        Self {}
    }
}
