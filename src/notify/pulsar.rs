use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use pulsar::{producer, proto as pulsarProto, Pulsar, TokioExecutor};
use tokio::sync::mpsc::Sender;
use crate::notify::{Notification, Notifier};
use anyhow::{anyhow, Result};

pub struct PulsarNotifier {
    client: Arc<Pulsar<TokioExecutor>>,
    topic: String,
    messages_sent: Arc<AtomicUsize>
}

impl PulsarNotifier {
    pub async fn new(broker_url: String, topic: String) -> Result<Self> {
        let client = Pulsar::builder(broker_url, TokioExecutor).build().await
            .map_err(|e| anyhow!("Failed to connect to Pulsar: {:?}", e))?;
        Ok(Self {
            client: Arc::new(client),
            topic,
            messages_sent: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn get_messages_sent(&self) -> usize {
        self.messages_sent.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Notifier for PulsarNotifier {
    fn start(&self) -> Sender<Notification> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let client = self.client.clone();
        let topic = self.topic.clone();
        let counter = self.messages_sent.clone();
        tokio::spawn(async move {
            let mut producer = client.producer()
                .with_topic(topic)
                .with_options(producer::ProducerOptions {
                    schema: Some(pulsarProto::Schema {
                        r#type: pulsarProto::schema::Type::String as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .build()
                .await.unwrap();

            while let Some(notification) = rx.recv().await {
                let data = serde_json::to_string(&notification).unwrap();
                let sent = producer.send_non_blocking(data.as_bytes()).await;
                match sent {
                    Ok(sf) => {
                        if let Err(receipt) = sf.await {
                            tracing::error!("Error sending notification to Pulsar: {:?}", receipt);
                        } else {
                            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error sending notification to Pulsar: {:?}", e);
                    }
                }
            }
        });
        tx
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::{GenericImage, ImageExt};
    use testcontainers::runners::AsyncRunner;
    use tokio::time::Duration;
    use crate::datakind::DataKind;
    use crate::notify::{Notification, RunMode};
    use crate::notify::Notifier;

    #[tokio::test]
    async fn can_start() {
        let image = GenericImage::new("apachepulsar/pulsar", "3.3.3")
            .with_exposed_port(6650.tcp())
            .with_exposed_port(8080.tcp())
            .with_wait_for(WaitFor::message_on_stdout("became the leader"));

        let container = image
            .with_cmd(vec![
                "bin/pulsar".to_string(),
                "standalone".to_string(),
            ])
            .start()
            .await
            .unwrap();

        let uri = format!(
            "pulsar://{}:{}",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(6650).await.unwrap()
        );

        println!("Pulsar running on {}", uri);

        let notifier = super::PulsarNotifier::new(uri, "non-persistent://public/default/test_1".to_string()).await;

        if let Err(e) = notifier {
            tokio::time::sleep(Duration::from_secs(60)).await;
            panic!("Error creating Pulsar notifier: {:?}", e);
        }

        let notifier = notifier.unwrap();
        let sender = notifier.start();

        let _ = sender.send(Notification {
            version: Notification::version(),
            ts: Utc::now(),
            blockchain: "ETHEREUM".to_string(),
            file_type: DataKind::Blocks,
            run: RunMode::Archive,
            height_start: 100,
            height_end: 120,
            location: "file://archive/range-100_120.blocks.avro".to_string(),
        }).await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let messages_sent = notifier.get_messages_sent();
        assert_eq!(messages_sent, 1);
    }
}

