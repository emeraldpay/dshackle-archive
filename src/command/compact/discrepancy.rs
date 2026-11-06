// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Describes how the data copied from the sources of a chunk differs from what the chunk is expected to have.
//!
//! A chunk with such a difference is skipped by the compaction, and to repair it the user needs to know what
//! exactly is wrong: a few missing items, data from another block, or the same records archived twice.

use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;

/// How many items are listed as an example, as a broken chunk may miss thousands of them
const SAMPLE_SIZE: usize = 5;

///
/// The difference between the items (heights, transaction ids) a chunk is expected to have and the items copied into it
#[derive(Debug)]
pub(super) struct Discrepancy<T> {
    expected: usize,
    copied: usize,
    /// Expected, but not copied, in the order they're expected
    missing: Vec<T>,
    /// Copied, but not expected, i.e., they belong to another block
    unexpected: Vec<T>,
    /// How many copies are extra, i.e., copied more than once
    duplicates: usize,
}

impl<T: Hash + Eq + Clone + Debug> Discrepancy<T> {
    ///
    /// Compare the expected items with the copied ones. Returns `None` when each expected item is copied exactly once,
    /// and nothing else is copied.
    pub(super) fn find(expected: impl IntoIterator<Item = T>, copied: &[T]) -> Option<Self> {
        let copied_unique: HashSet<&T> = copied.iter().collect();

        let mut expected_unique = HashSet::new();
        let mut missing = Vec::new();
        for item in expected {
            if !copied_unique.contains(&item) {
                missing.push(item.clone());
            }
            expected_unique.insert(item);
        }

        let mut seen = HashSet::new();
        let unexpected: Vec<T> = copied.iter()
            .filter(|item| !expected_unique.contains(*item) && seen.insert(*item))
            .cloned()
            .collect();

        let duplicates = copied.len() - copied_unique.len();

        if missing.is_empty() && unexpected.is_empty() && duplicates == 0 {
            return None;
        }
        Some(Self {
            expected: expected_unique.len(),
            copied: copied.len(),
            missing,
            unexpected,
            duplicates,
        })
    }
}

fn sample<T: Debug>(items: &[T]) -> String {
    let mut sample = items.iter()
        .take(SAMPLE_SIZE)
        .map(|item| format!("{:?}", item))
        .collect::<Vec<String>>()
        .join(", ");
    if items.len() > SAMPLE_SIZE {
        sample.push_str(", ...");
    }
    sample
}

impl<T: Debug> Display for Discrepancy<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "expected {}, copied {}", self.expected, self.copied)?;
        if !self.missing.is_empty() {
            write!(f, "; missing {} ({})", self.missing.len(), sample(&self.missing))?;
        }
        if !self.unexpected.is_empty() {
            write!(f, "; unexpected {} ({})", self.unexpected.len(), sample(&self.unexpected))?;
        }
        if self.duplicates > 0 {
            write!(f, "; duplicates {}", self.duplicates)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Discrepancy;

    #[test]
    fn none_when_all_copied_once() {
        let discrepancy = Discrepancy::find(100..105u64, &[104, 100, 101, 103, 102]);
        assert!(discrepancy.is_none());
    }

    #[test]
    fn none_when_nothing_expected_and_nothing_copied() {
        let discrepancy = Discrepancy::<u64>::find(vec![], &[]);
        assert!(discrepancy.is_none());
    }

    #[test]
    fn lists_missing_in_expected_order() {
        let discrepancy = Discrepancy::find(100..105u64, &[100, 103]).unwrap();
        assert_eq!(discrepancy.missing, vec![101, 102, 104]);
        assert_eq!(
            discrepancy.to_string(),
            "expected 5, copied 2; missing 3 (101, 102, 104)"
        );
    }

    #[test]
    fn limits_the_sample() {
        let discrepancy = Discrepancy::find(100..110u64, &[]).unwrap();
        assert_eq!(discrepancy.missing.len(), 10);
        assert_eq!(
            discrepancy.to_string(),
            "expected 10, copied 0; missing 10 (100, 101, 102, 103, 104, ...)"
        );
    }

    #[test]
    fn finds_unexpected_even_when_counts_match() {
        let expected = vec!["tx1".to_string(), "tx2".to_string()];
        let copied = vec!["tx1".to_string(), "tx3".to_string()];
        let discrepancy = Discrepancy::find(expected, &copied).unwrap();
        assert_eq!(
            discrepancy.to_string(),
            r#"expected 2, copied 2; missing 1 ("tx2"); unexpected 1 ("tx3")"#
        );
    }

    #[test]
    fn lists_unexpected_once() {
        let expected = vec!["tx1".to_string()];
        let copied = vec!["tx1".to_string(), "tx9".to_string(), "tx9".to_string()];
        let discrepancy = Discrepancy::find(expected, &copied).unwrap();
        assert_eq!(discrepancy.unexpected, vec!["tx9".to_string()]);
        assert_eq!(discrepancy.duplicates, 1);
    }

    #[test]
    fn finds_duplicates() {
        let discrepancy = Discrepancy::find(100..102u64, &[100, 101, 101, 100]).unwrap();
        assert!(discrepancy.missing.is_empty());
        assert!(discrepancy.unexpected.is_empty());
        assert_eq!(
            discrepancy.to_string(),
            "expected 2, copied 4; duplicates 2"
        );
    }
}
