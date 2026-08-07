// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Apache Kafka plumbing shared by the notification producer and the
//! streaming target: how a URL becomes a connected client, how the topics it
//! needs are resolved (and created), and how a block height picks the
//! partition it is published to.
//!
//! rskafka ships no producer-side partitioner — a
//! [`PartitionClient`](rskafka::client::partition::PartitionClient) is bound to
//! a single partition and the caller decides which one. Both Kafka users need
//! the same rule, so it lives here.

use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Result};
use rskafka::BackoffConfig;
use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};

/// Reported to the broker, so a Kafka operator can tell our connections apart.
const CLIENT_ID: &str = "dshackle-archive";

/// Bounds the retries rskafka performs inside every broker call — connecting,
/// reading metadata, producing.
///
/// Without a deadline it retries connection and IO errors forever, so a broker
/// that goes away would leave the archive waiting silently instead of
/// reporting the failure. Generous enough to ride out a leader election, short
/// enough that an unreachable broker is reported rather than waited on.
const BROKER_DEADLINE: Duration = Duration::from_secs(30);

/// A broker that auto-creates topics does so only when it sees a metadata
/// request naming the topic, and answers that first request with "unknown
/// topic". Retrying the lookup covers that round trip — and, when the topic
/// had to be created outright instead, the propagation that follows it on a
/// multi-broker cluster.
const TOPIC_LOOKUP_ATTEMPTS: usize = 5;
const TOPIC_LOOKUP_DELAY: Duration = Duration::from_secs(1);

/// The partition a topic is probed on. Kafka numbers partitions from zero, so
/// this one exists for as long as the topic does.
const PROBE_PARTITION: i32 = 0;

/// Passed to `CreateTopics` as the partition count and the replication factor.
///
/// Kafka reads a negative value as "use the configured default" — the broker's
/// own `num.partitions` and `default.replication.factor` (KIP-464, brokers
/// 2.4 and later). Naming numbers here instead would mean second-guessing how
/// the operator sized their cluster, which is not ours to decide.
const BROKER_DEFAULT_PARTITIONS: i32 = -1;
const BROKER_DEFAULT_REPLICATION: i16 = -1;

/// How long a broker may spend on a topic creation before answering anyway.
/// A creation that outlives it isn't lost — the metadata is re-read either
/// way, so a later attempt picks the topic up.
const TOPIC_CREATE_TIMEOUT_MS: i32 = 5_000;

///
/// Addresses of the Kafka bootstrap brokers, as provided on the command line.
///
/// Accepts both a plain address list (`host:9092,host2:9092`) and the
/// scheme-prefixed form used by `--stream.url` (`kafka://host:9092`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapBrokers(Vec<String>);

impl FromStr for BootstrapBrokers {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        // per entry, not once for the whole string: a list is as likely to be
        // written `kafka://one:9092,kafka://two:9092` as with a single prefix
        let addresses: Vec<String> = value
            .split(',')
            .map(str::trim)
            .map(|address| address.strip_prefix("kafka://").unwrap_or(address))
            .filter(|address| !address.is_empty())
            .map(String::from)
            .collect();
        if addresses.is_empty() {
            return Err(anyhow!("No Kafka broker address specified: {}", value));
        }
        Ok(Self(addresses))
    }
}

impl Display for BootstrapBrokers {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join(","))
    }
}

impl BootstrapBrokers {
    /// Connect to the cluster. Fails when none of the bootstrap brokers can be
    /// reached within [`BROKER_DEADLINE`] — a misconfigured broker must stop
    /// the run, not degrade it.
    pub async fn connect(&self) -> Result<Client> {
        ClientBuilder::new(self.0.clone())
            .client_id(CLIENT_ID)
            .backoff_config(BackoffConfig {
                deadline: Some(BROKER_DEADLINE),
                ..BackoffConfig::default()
            })
            .build()
            .await
            .map_err(|e| anyhow!("Failed to connect to Kafka at {}: {}", self, e))
    }
}

///
/// Partitions of a single topic, and the rule that assigns a block height to
/// one of them.
#[derive(Clone, Debug)]
pub struct TopicPartitions {
    topic: String,
    /// Partition ids as reported by the broker, ascending. Never empty.
    partitions: Vec<i32>,
}

///
/// A topic resolved from the cluster metadata, together with the client that
/// resolved it.
pub struct ResolvedTopic {
    partitions: TopicPartitions,
    /// The client opened on [`PROBE_PARTITION`] to make the broker name the
    /// topic in a metadata request. Handed back so a caller that wants a
    /// client per partition reuses this one instead of opening a second for a
    /// partition it already has.
    probe: Option<(i32, PartitionClient)>,
}

impl ResolvedTopic {
    ///
    /// Resolve every topic in `topics` from the cluster metadata, creating
    /// whichever of them the cluster doesn't have yet.
    ///
    /// The whole set is resolved together: one metadata request covers all of
    /// them however many are asked for, and they share the retry budget, so a
    /// run waiting on seven topics waits as long as one rather than seven
    /// times as long.
    ///
    /// Errors when any topic still doesn't exist afterwards — silently
    /// producing nowhere would be worse than not starting.
    pub async fn discover(client: &Client, topics: &[String]) -> Result<Vec<Self>> {
        let mut resolved: Vec<Option<Self>> = topics.iter().map(|_| None).collect();
        let mut reason: Option<String> = None;
        let mut asked_to_create = false;

        for attempt in 0..TOPIC_LOOKUP_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(TOPIC_LOOKUP_DELAY).await;
            }
            // asking for a partition client is what puts the topic name into a
            // metadata request, which on a broker with
            // `auto.create.topics.enable` is already enough to create it. It's
            // also the only call that reports why a topic is unusable, which
            // `list_topics` below cannot tell apart from one that isn't there.
            let mut probes: Vec<Option<PartitionClient>> = Vec::with_capacity(topics.len());
            for (i, topic) in topics.iter().enumerate() {
                if resolved[i].is_some() {
                    probes.push(None);
                    continue;
                }
                match client
                    .partition_client(topic, PROBE_PARTITION, UnknownTopicHandling::Error)
                    .await
                {
                    Ok(probe) => probes.push(Some(probe)),
                    Err(e) => {
                        reason = Some(e.to_string());
                        probes.push(None);
                    }
                }
            }

            let known = client
                .list_topics()
                .await
                .map_err(|e| anyhow!("Failed to read Kafka topics: {}", e))?;
            for (i, topic) in topics.iter().enumerate() {
                if resolved[i].is_some() {
                    continue;
                }
                let found = known
                    .iter()
                    .find(|known| known.name == *topic)
                    .filter(|known| !known.partitions.is_empty());
                if let Some(found) = found {
                    resolved[i] = Some(Self {
                        partitions: TopicPartitions {
                            topic: found.name.clone(),
                            partitions: found.partitions.iter().copied().collect(),
                        },
                        probe: probes[i].take().map(|probe| (PROBE_PARTITION, probe)),
                    });
                }
            }
            if resolved.iter().all(Option::is_some) {
                return Ok(resolved.into_iter().flatten().collect());
            }

            if !asked_to_create {
                asked_to_create = true;
                // the probe alone didn't produce them: auto-creation is off,
                // or slower than one round trip. Ask for them outright.
                let missing: Vec<&str> = topics
                    .iter()
                    .zip(resolved.iter())
                    .filter(|(_, resolved)| resolved.is_none())
                    .map(|(topic, _)| topic.as_str())
                    .collect();
                if let Err(e) = Self::create(client, &missing).await {
                    reason = Some(e.to_string());
                }
            }
        }

        let missing = topics
            .iter()
            .zip(resolved.iter())
            .find(|(_, resolved)| resolved.is_none())
            .map(|(topic, _)| topic.as_str())
            .unwrap_or("");
        Err(match reason {
            Some(reason) => anyhow!(
                "Kafka topic {} is not available: {}. Create it on the broker, or grant this client permission to",
                missing, reason
            ),
            None => anyhow!(
                "Kafka topic {} doesn't exist and could not be created. Create it on the broker",
                missing
            ),
        })
    }

    ///
    /// Ask the cluster to create `topics`, leaving their shape entirely to the
    /// broker — see [`BROKER_DEFAULT_PARTITIONS`].
    ///
    /// Best-effort on purpose: the caller re-reads the metadata afterwards and
    /// treats that as the truth, so a topic that another process created first
    /// (or that this call created but the broker hasn't propagated yet) needs
    /// no special handling here.
    async fn create(client: &Client, topics: &[&str]) -> Result<()> {
        let controller = client
            .controller_client()
            .map_err(|e| anyhow!("Failed to reach the Kafka controller: {}", e))?;
        let mut failure = None;
        for topic in topics {
            tracing::info!("Creating Kafka topic {} with the broker's defaults", topic);
            if let Err(e) = controller
                .create_topic(
                    *topic,
                    BROKER_DEFAULT_PARTITIONS,
                    BROKER_DEFAULT_REPLICATION,
                    TOPIC_CREATE_TIMEOUT_MS,
                )
                .await
            {
                // keep going: one topic we may not create says nothing about
                // the rest, and the metadata re-read decides either way
                failure.get_or_insert_with(|| {
                    anyhow!("Failed to create Kafka topic {}: {}", topic, e)
                });
            }
        }
        match failure {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }

    ///
    /// Split into the partitions and the client already open on one of them.
    pub fn into_parts(self) -> (TopicPartitions, Option<(i32, PartitionClient)>) {
        (self.partitions, self.probe)
    }
}

impl TopicPartitions {
    ///
    /// Read the partitions of a single `topic`. See
    /// [`ResolvedTopic::discover`] for the resolution rules.
    pub async fn discover(client: &Client, topic: &str) -> Result<Self> {
        let topics = vec![topic.to_string()];
        let resolved = ResolvedTopic::discover(client, &topics).await?;
        Ok(resolved
            .into_iter()
            .next()
            .expect("discover returns one entry per requested topic")
            .partitions)
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn count(&self) -> usize {
        self.partitions.len()
    }

    /// Partition ids as reported by the broker. For a caller that needs a
    /// client per partition rather than one per height.
    pub fn ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.partitions.iter().copied()
    }

    /// True when `partition` is one of this topic's own partitions.
    pub fn contains(&self, partition: i32) -> bool {
        self.partitions.contains(&partition)
    }

    ///
    /// Partition that carries the data of the given block height.
    ///
    /// Kafka orders records within a partition only, so every message of one
    /// block — and any later re-org replacement for it — must land on the same
    /// partition. Selecting by height (instead of hashing the record key, which
    /// is what a standard Kafka producer would do) also spreads consecutive
    /// blocks evenly, and lets a consumer compute where a height went.
    pub fn for_height(&self, height: u64) -> i32 {
        self.partitions[(height % self.partitions.len() as u64) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_address() {
        let brokers: BootstrapBrokers = "localhost:9092".parse().unwrap();
        assert_eq!(brokers, BootstrapBrokers(vec!["localhost:9092".to_string()]));
    }

    #[test]
    fn parses_list_with_scheme() {
        let brokers: BootstrapBrokers = "kafka://one:9092, two:9092".parse().unwrap();
        assert_eq!(
            brokers,
            BootstrapBrokers(vec!["one:9092".to_string(), "two:9092".to_string()])
        );
        assert_eq!(brokers.to_string(), "one:9092,two:9092");
    }

    #[test]
    fn parses_list_with_scheme_on_each_entry() {
        let brokers: BootstrapBrokers = "kafka://one:9092,kafka://two:9092".parse().unwrap();
        assert_eq!(
            brokers,
            BootstrapBrokers(vec!["one:9092".to_string(), "two:9092".to_string()])
        );
    }

    #[test]
    fn fails_on_empty_address() {
        assert!("kafka://".parse::<BootstrapBrokers>().is_err());
    }

    #[test]
    fn spreads_heights_over_partitions() {
        let topic = TopicPartitions {
            topic: "archive-eth-blocks".to_string(),
            partitions: vec![0, 1, 2],
        };
        assert_eq!(topic.for_height(100), 1);
        assert_eq!(topic.for_height(101), 2);
        assert_eq!(topic.for_height(102), 0);
        // same height always goes to the same partition, incl. a re-org replacement
        assert_eq!(topic.for_height(100), 1);
    }

    #[test]
    fn uses_actual_partition_ids() {
        let topic = TopicPartitions {
            topic: "archive-eth-blocks".to_string(),
            partitions: vec![3, 7],
        };
        assert_eq!(topic.for_height(100), 3);
        assert_eq!(topic.for_height(101), 7);
    }
}
