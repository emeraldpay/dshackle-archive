// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Topic naming for the streaming targets.
//!
//! Every broker uses the same layout — one topic per field, named
//! `<prefix>-<field>`, where the prefix is what the user passed in
//! `--stream.topics` and the field labels depend on the blockchain and the
//! table selection. [`TopicSet`] owns that naming so a producer opened at
//! startup and a message routed at append time cannot disagree about which
//! topic a field belongs to.

use crate::archiver::datakind::DataOptions;
use crate::record::BlockchainType;

///
/// The per-field topics one streaming run publishes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSet {
    prefix: String,
    labels: Vec<&'static str>,
}

impl TopicSet {
    ///
    /// The topics needed for the given blockchain and table selection, under
    /// the user-supplied prefix.
    ///
    /// The prefix is taken verbatim — callers are expected to pass whatever
    /// their broker needs up to and including the blockchain segment (e.g.
    /// `persistent://public/default/archive-eth` for Pulsar, a plain
    /// `archive-eth` for Kafka).
    pub fn new(prefix: String, blockchain: BlockchainType, options: &DataOptions) -> Self {
        Self {
            prefix,
            labels: topic_labels_for(blockchain, options),
        }
    }

    ///
    /// Full topic name of one field label.
    pub fn name_for(&self, label: &str) -> String {
        format!("{}-{}", self.prefix, label)
    }

    ///
    /// The prefix as given in `--stream.topics`. Reported as the location of a
    /// streaming run, which has no single addressable artifact to point at.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    ///
    /// How many topics the run publishes to.
    pub fn count(&self) -> usize {
        self.labels.len()
    }

    ///
    /// Every `(label, full topic name)` pair, in [`topic_labels_for`] order.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, String)> + '_ {
        self.labels.iter().map(|label| (*label, self.name_for(label)))
    }
}

/// The set of topic labels actually published for the given
/// `(blockchain, data_options)` combination.
///
/// Two filters apply:
///
/// 1. **Blockchain shape.** Bitcoin has no receipts, no uncles, and no
///    traces, so those topics are never created for a Bitcoin run. Ethereum
///    can publish to every label.
/// 2. **User selection.** `--tables` controls whether block / tx / trace
///    topics are created at all; `--fields.trace` further narrows the trace
///    topic set to `calls` / `statediff`.
///
/// Returned labels follow a stable order (blocks → uncles → tx → traces),
/// so callers can rely on consistent iteration order for logging.
pub fn topic_labels_for(
    blockchain: BlockchainType,
    options: &DataOptions,
) -> Vec<&'static str> {
    let mut labels: Vec<&'static str> = Vec::new();

    // Block-kind topics.
    if options.include_block() {
        labels.push("blocks");
        if matches!(blockchain, BlockchainType::Ethereum) {
            labels.push("blocks-uncles");
        }
    }

    // Transaction-kind topics. Bitcoin produces tx-json + tx-raw; Ethereum
    // additionally produces tx-receipts.
    if options.include_tx() {
        labels.push("tx-json");
        labels.push("tx-raw");
        if matches!(blockchain, BlockchainType::Ethereum) {
            labels.push("tx-receipts");
        }
    }

    // Trace-kind topics. Bitcoin has no traces — even if the user passes
    // `--tables traces`, we suppress the topic creation here so a misconfig
    // doesn't create dead topics. For Ethereum, each sub-field (`calls` /
    // `stateDiff`) is created only when its corresponding flag is set on
    // `TraceOptions`.
    if options.include_trace() && matches!(blockchain, BlockchainType::Ethereum) {
        if let Some(trace) = options.trace.as_ref() {
            if trace.include_trace {
                labels.push("trace-calls");
            }
            if trace.include_state_diff {
                labels.push("trace-statediff");
            }
        }
    }

    labels
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archiver::datakind::{BlockOptions, TraceOptions, TxOptions};

    fn all_tables() -> DataOptions {
        DataOptions {
            overwrite: true,
            block: Some(BlockOptions::default()),
            tx: Some(TxOptions::default()),
            trace: Some(TraceOptions {
                include_trace: true,
                include_state_diff: true,
            }),
        }
    }

    #[test]
    fn names_topics_under_the_prefix() {
        let topics = TopicSet::new(
            "persistent://public/default/archive-eth".to_string(),
            BlockchainType::Ethereum,
            &all_tables(),
        );
        assert_eq!(
            topics.name_for("tx-json"),
            "persistent://public/default/archive-eth-tx-json"
        );
        assert_eq!(topics.prefix(), "persistent://public/default/archive-eth");
        assert_eq!(topics.count(), 7);
    }

    #[test]
    fn iterates_labels_paired_with_their_names() {
        let topics = TopicSet::new(
            "archive-btc".to_string(),
            BlockchainType::Bitcoin,
            &all_tables(),
        );
        let pairs: Vec<(&str, String)> = topics.iter().collect();
        assert_eq!(
            pairs,
            vec![
                ("blocks", "archive-btc-blocks".to_string()),
                ("tx-json", "archive-btc-tx-json".to_string()),
                ("tx-raw", "archive-btc-tx-raw".to_string()),
            ]
        );
    }

    /// `topic_labels_for` filters by blockchain shape: Bitcoin gets no
    /// uncles, no receipts, no traces — even if `--tables` includes traces
    /// (a misconfig).
    #[test]
    fn topic_labels_for_bitcoin_excludes_eth_only_topics() {
        let labels = topic_labels_for(BlockchainType::Bitcoin, &all_tables());
        assert_eq!(labels, vec!["blocks", "tx-json", "tx-raw"]);
    }

    /// `topic_labels_for` defaults for Ethereum + blocks/txes: receipts and
    /// uncles ARE included; trace topics are NOT (trace = None).
    #[test]
    fn topic_labels_for_ethereum_default_tables_excludes_traces() {
        let opts = DataOptions {
            trace: None,
            ..all_tables()
        };
        let labels = topic_labels_for(BlockchainType::Ethereum, &opts);
        assert_eq!(
            labels,
            vec!["blocks", "blocks-uncles", "tx-json", "tx-raw", "tx-receipts"]
        );
    }

    /// Ethereum with traces enabled: both trace topics surface when
    /// `TraceOptions` requests both fields.
    #[test]
    fn topic_labels_for_ethereum_with_full_traces() {
        let labels = topic_labels_for(BlockchainType::Ethereum, &all_tables());
        assert_eq!(
            labels,
            vec![
                "blocks",
                "blocks-uncles",
                "tx-json",
                "tx-raw",
                "tx-receipts",
                "trace-calls",
                "trace-statediff",
            ]
        );
    }

    /// Ethereum with `--fields.trace calls`: only the calls trace topic
    /// surfaces; statediff is suppressed.
    #[test]
    fn topic_labels_for_ethereum_trace_calls_only() {
        let opts = DataOptions {
            trace: Some(TraceOptions {
                include_trace: true,
                include_state_diff: false,
            }),
            ..all_tables()
        };
        let labels = topic_labels_for(BlockchainType::Ethereum, &opts);
        assert!(labels.contains(&"trace-calls"));
        assert!(!labels.contains(&"trace-statediff"));
    }

    /// `--tables blocks` only: tx-* and trace-* topics must all be absent.
    #[test]
    fn topic_labels_for_blocks_only_returns_just_block_topics() {
        let opts = DataOptions {
            tx: None,
            trace: None,
            ..all_tables()
        };
        let eth = topic_labels_for(BlockchainType::Ethereum, &opts);
        assert_eq!(eth, vec!["blocks", "blocks-uncles"]);
        let btc = topic_labels_for(BlockchainType::Bitcoin, &opts);
        assert_eq!(btc, vec!["blocks"]);
    }
}
