// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Which notification target the command line selects.
//!
//! Resolved before anything else runs, so an ambiguous or half-written
//! configuration stops the process with an error. Both mistakes used to end up
//! notifying nowhere, which is indistinguishable from a healthy run until a
//! downstream system is found to be missing data.

use anyhow::{anyhow, Result};
use crate::args::{Args, Notify};

///
/// The single place archive events are sent to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotifyTarget {
    /// No notifications requested.
    Disabled,
    /// A JSON-line file in the given directory.
    Dir(String),
    Pulsar { url: String, topic: String },
    Kafka { url: String, topic: String },
}

impl NotifyTarget {
    ///
    /// Resolve the target, rejecting a configuration that names more than one
    /// target or only half of a broker.
    pub fn from_args(args: &Args) -> Result<Self> {
        match &args.notify {
            Some(notify) => Self::from_notify(notify),
            None => Ok(Self::Disabled),
        }
    }

    fn from_notify(notify: &Notify) -> Result<Self> {
        let has_pulsar = notify.pulsar_url.is_some() || notify.pulsar_topic.is_some();
        let has_kafka = notify.kafka_url.is_some() || notify.kafka_topic.is_some();

        let mut selected = Vec::new();
        if notify.notify_dir.is_some() {
            selected.push("--notify.dir");
        }
        if has_pulsar {
            selected.push("--notify.pulsar.*");
        }
        if has_kafka {
            selected.push("--notify.kafka.*");
        }
        if selected.len() > 1 {
            return Err(anyhow!(
                "Notifications can go to only one target, but several are configured: {}",
                selected.join(", ")
            ));
        }

        if let Some(dir) = &notify.notify_dir {
            return Ok(Self::Dir(dir.clone()));
        }
        if has_pulsar {
            let (url, topic) = both_halves("pulsar", &notify.pulsar_url, &notify.pulsar_topic)?;
            return Ok(Self::Pulsar { url, topic });
        }
        if has_kafka {
            let (url, topic) = both_halves("kafka", &notify.kafka_url, &notify.kafka_topic)?;
            return Ok(Self::Kafka { url, topic });
        }
        Ok(Self::Disabled)
    }
}

/// URL and topic of a broker target, or an error naming the missing half.
fn both_halves(broker: &str, url: &Option<String>, topic: &Option<String>) -> Result<(String, String)> {
    match (url, topic) {
        (Some(url), Some(topic)) => Ok((url.clone(), topic.clone())),
        (Some(_), None) => Err(anyhow!(
            "--notify.{}.topic is required together with --notify.{}.url", broker, broker
        )),
        (None, Some(_)) => Err(anyhow!(
            "--notify.{}.url is required together with --notify.{}.topic", broker, broker
        )),
        (None, None) => Err(anyhow!(
            "--notify.{}.url and --notify.{}.topic are required", broker, broker
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notify(dir: Option<&str>, pulsar: (Option<&str>, Option<&str>), kafka: (Option<&str>, Option<&str>)) -> Notify {
        Notify {
            notify_dir: dir.map(String::from),
            pulsar_url: pulsar.0.map(String::from),
            pulsar_topic: pulsar.1.map(String::from),
            kafka_url: kafka.0.map(String::from),
            kafka_topic: kafka.1.map(String::from),
        }
    }

    #[test]
    fn nothing_configured_is_disabled() {
        let target = NotifyTarget::from_notify(&notify(None, (None, None), (None, None))).unwrap();
        assert_eq!(target, NotifyTarget::Disabled);
    }

    #[test]
    fn resolves_kafka() {
        let target = NotifyTarget::from_notify(
            &notify(None, (None, None), (Some("kafka://localhost:9092"), Some("events")))
        ).unwrap();
        assert_eq!(target, NotifyTarget::Kafka {
            url: "kafka://localhost:9092".to_string(),
            topic: "events".to_string(),
        });
    }

    #[test]
    fn rejects_two_brokers() {
        let err = NotifyTarget::from_notify(&notify(
            None,
            (Some("pulsar://localhost:6650"), Some("persistent://public/default/events")),
            (Some("localhost:9092"), Some("events")),
        )).unwrap_err();
        assert!(err.to_string().contains("only one target"), "{}", err);
    }

    #[test]
    fn rejects_dir_together_with_broker() {
        let err = NotifyTarget::from_notify(
            &notify(Some("/tmp/notify"), (None, None), (Some("localhost:9092"), Some("events")))
        ).unwrap_err();
        assert!(err.to_string().contains("only one target"), "{}", err);
    }

    #[test]
    fn rejects_url_without_topic() {
        let err = NotifyTarget::from_notify(
            &notify(None, (None, None), (Some("localhost:9092"), None))
        ).unwrap_err();
        assert!(err.to_string().contains("--notify.kafka.topic is required"), "{}", err);
    }

    #[test]
    fn rejects_topic_without_url() {
        let err = NotifyTarget::from_notify(
            &notify(None, (None, Some("persistent://public/default/events")), (None, None))
        ).unwrap_err();
        assert!(err.to_string().contains("--notify.pulsar.url is required"), "{}", err);
    }
}
