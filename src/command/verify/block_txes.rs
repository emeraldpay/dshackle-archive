// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Transactions expected in the per-transaction tables (txes and traces) of a range.
//!
//! Each transaction is known by its block height and its id together. The id alone is not
//! unique: on Bitcoin, blocks 91812 and 91842 (and 91722 and 91880) have coinbase transactions
//! with the same id (see BIP30), and a range with both of them would always look as if it had
//! a duplicate. The height also tells which file a transaction belongs to, so each file can be
//! verified on its own instead of the whole range at once.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use crate::archiver::range::Range;

///
/// Transactions of the blocks in a range, as read from the block table.
pub(super) struct BlockTxes<T> {
    by_height: BTreeMap<u64, Vec<T>>,
}

impl<T: Eq + Hash + Clone + Debug> BlockTxes<T> {
    pub(super) fn new() -> Self {
        Self { by_height: BTreeMap::new() }
    }

    pub(super) fn add_block(&mut self, height: u64, txes: Vec<T>) {
        self.by_height.insert(height, txes);
    }

    ///
    /// Start checking a file that covers the `range`, i.e., is supposed to contain exactly
    /// the transactions of the blocks in it.
    pub(super) fn expect_in(&self, range: &Range) -> FileTxes<T> {
        let found = self.by_height.range(range.start()..=range.end())
            .flat_map(|(height, txes)| txes.iter().map(move |txid| ((*height, txid.clone()), false)))
            .collect();
        FileTxes { found }
    }
}

///
/// Transactions of a single file, checked off one by one as the file is read.
pub(super) struct FileTxes<T> {
    /// All the transactions expected in the file, and whether each of them is already found
    found: HashMap<(u64, T), bool>,
}

impl<T: Eq + Hash + Debug> FileTxes<T> {

    ///
    /// Check off a transaction found in the file. Fails if the file is not supposed to have it,
    /// or already had it.
    pub(super) fn check_off(&mut self, height: u64, txid: T) -> Result<(), String> {
        let key = (height, txid);
        match self.found.get_mut(&key) {
            None => Err(format!("Unexpected tx {:?} at height {}", key.1, height)),
            Some(true) => Err(format!("Duplicate tx {:?} at height {}", key.1, height)),
            Some(found) => {
                *found = true;
                Ok(())
            }
        }
    }

    ///
    /// Fails if any of the expected transactions was not found in the file.
    pub(super) fn finish(&self) -> Result<(), String> {
        let mut missing = self.found.iter().filter(|(_, found)| !**found).map(|(key, _)| key);
        let Some((height, txid)) = missing.next() else {
            return Ok(());
        };
        Err(format!("Missing {} tx(es), e.g. {:?} at height {}", missing.count() + 1, txid, height))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn txes() -> BlockTxes<&'static str> {
        let mut txes = BlockTxes::new();
        txes.add_block(100, vec!["A", "B"]);
        txes.add_block(101, vec![]);
        txes.add_block(102, vec!["C"]);
        txes
    }

    #[test]
    fn accepts_all_txes_of_the_range() {
        let txes = txes();
        let mut file = txes.expect_in(&Range::new(100, 102));
        file.check_off(100, "B").unwrap();
        file.check_off(102, "C").unwrap();
        file.check_off(100, "A").unwrap();
        assert!(file.finish().is_ok());
    }

    #[test]
    fn accepts_a_block_without_txes() {
        let txes = txes();
        let file = txes.expect_in(&Range::single(101));
        assert!(file.finish().is_ok());
    }

    #[test]
    fn rejects_missing_tx() {
        let txes = txes();
        let mut file = txes.expect_in(&Range::single(100));
        file.check_off(100, "A").unwrap();
        let err = file.finish().unwrap_err();
        assert!(err.contains("Missing 1"), "{}", err);
    }

    #[test]
    fn rejects_tx_of_another_file() {
        let txes = txes();
        let mut file = txes.expect_in(&Range::single(100));
        assert!(file.check_off(102, "C").is_err());
    }

    #[test]
    fn rejects_tx_at_wrong_height() {
        let txes = txes();
        let mut file = txes.expect_in(&Range::new(100, 102));
        assert!(file.check_off(102, "A").is_err());
    }

    #[test]
    fn rejects_duplicate_tx() {
        let txes = txes();
        let mut file = txes.expect_in(&Range::single(100));
        file.check_off(100, "A").unwrap();
        assert!(file.check_off(100, "A").is_err());
    }

    #[test]
    fn accepts_same_txid_in_different_blocks() {
        let mut txes = BlockTxes::new();
        txes.add_block(91812, vec!["D5D2"]);
        txes.add_block(91842, vec!["D5D2"]);
        let mut file = txes.expect_in(&Range::new(91000, 91999));
        file.check_off(91812, "D5D2").unwrap();
        file.check_off(91842, "D5D2").unwrap();
        assert!(file.finish().is_ok());
    }
}
