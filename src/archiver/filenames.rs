use std::str::FromStr;
use lazy_static::lazy_static;
use regex::Regex;
use crate::archiver::datakind::DataKind;
use crate::archiver::range::{Height, Range};

lazy_static! {
    static ref RE_SINGLE: Regex = Regex::new(r"^(\d+)\.(([a-f0-9]{64})\.)?(\w+)\.(\w+\.)?avro$").unwrap();
    static ref RE_RANGE: Regex = Regex::new(r"^range-(\d+)_(\d+)\.(\w+)\.(\w+\.)?avro$").unwrap();
}

#[derive(Debug, Clone)]
pub struct Filenames {
    pub parent: String,
    pub padding: usize,
    pub dir_block_size_l1: u64,
    pub dir_block_size_l2: u64,
}

impl Filenames {

    pub fn with_dir(dir: String) -> Self {
        Filenames {
            parent: dir,
            ..Filenames::default()
        }
    }

    pub fn parse(filename: String) -> Option<(DataKind, Range)> {
        if let Some(cap) = RE_SINGLE.captures(filename.as_str()) {
            let height: u64 = cap.get(1).unwrap().as_str().parse().unwrap();
            let hash = cap.get(3).map(|x| x.as_str().to_string());
            let kind = DataKind::from_str(cap.get(4).unwrap().as_str());
            if kind.is_err() {
                return None;
            }
            return Some((kind.unwrap(), Range::Single(Height::new(height, hash))));
        }
        if let Some(cap) = RE_RANGE.captures(filename.as_str()) {
            let start: u64 = cap.get(1).unwrap().as_str().parse().unwrap();
            let end: u64 = cap.get(2).unwrap().as_str().parse().unwrap();
            let kind = DataKind::from_str(cap.get(3).unwrap().as_str());
            if kind.is_err() {
                return None;
            }
            return Some((kind.unwrap(), Range::Multiple(start.into(), end.into())));
        }
        None
    }

    pub fn filename(&self, kind: &DataKind, blocks: &Range) -> String {
        let suffix = match kind {
            DataKind::Blocks => if blocks.len() == 1 { "block" } else { "blocks" },
            DataKind::Transactions => "txes",
            DataKind::TransactionTraces => "traces"
        };

        if blocks.len() == 1 {
            let height = blocks.first_height();
            if let Some(hash) = &height.hash {
                format!("{}.{}.{}.avro", self.range_padded(blocks.start()), hash, suffix)
            } else {
                format!("{}.{}.avro", self.range_padded(blocks.start()), suffix)
            }
        } else {
            format!("range-{}_{}.{}.avro", self.range_padded(blocks.start()), self.range_padded(blocks.end()), suffix)
        }
    }

    pub fn relative_path(&self, kind: &DataKind, blocks: &Range) -> String {
        if blocks.len() == 1 {
            format!("{}/{}/{}",
                    self.level_1(blocks.start()),
                    self.level_2(blocks.start()),
                    self.filename(kind, blocks)
            )
        } else {
            format!("{}/{}",
                    self.level_1(blocks.start()),
                    self.filename(kind, blocks)
            )
        }
    }

    ///
    /// Offset is a position in a directory list. Since Dshackle Archive uses a S3-like storage, which sorts files by their name,
    /// this is where to start to look for files in a given range.
    pub fn offset(&self, range: &Range) -> String {
        match range {
            // For a single block, it's just the height
            Range::Single(start) => self.range_padded(start.height),
            // For a range we use a prefix `range-` only to distinguish them from the single files,
            // otherwise they would be mixed and single ranges listed twice, etc.
            Range::Multiple(start, _) => format!("range-{}", self.range_padded(start.height))
        }
    }

    pub fn full_path(&self, relative: String) -> String {
        if self.parent.is_empty() {
            relative
        } else {
            format!("{}/{}", self.parent, relative)
        }
    }

    pub fn path(&self, kind: &DataKind, range: &Range) -> String {
        self.full_path(self.relative_path(kind, range))
    }

    fn level_1(&self, value: u64) -> String {
        let number = value / self.dir_block_size_l1 * self.dir_block_size_l1;
        self.range_padded(number)
    }

    fn level_2(&self, value: u64) -> String {
        let number = (value / self.dir_block_size_l2) * self.dir_block_size_l2;
        self.range_padded(number)
    }

    fn range_padded(&self, value: u64) -> String {
        format!("{:0length$}", value, length = self.padding)
    }
}


impl Default for Filenames {
    fn default() -> Self {
        Filenames {
            parent: "".to_string(),
            padding: 9,
            dir_block_size_l1: 1_000_000,
            dir_block_size_l2: 1_000,
        }
    }
}

pub trait Level {
    fn dir(&self) -> String;
    fn next(self) -> Self;
    fn height(&self) -> u64;
}

pub struct LevelDouble<'a> {
    filenames: &'a Filenames,
    height: u64,
    step: u64
}

pub struct LevelSingle<'a> {
    filenames: &'a Filenames,
    height: u64,
    step: u64
}

impl<'a> LevelDouble<'a> {
    pub fn new(filenames: &'a Filenames, height: u64) -> Self {
        let start_height = (height / filenames.dir_block_size_l2) * filenames.dir_block_size_l2;
        LevelDouble {
            filenames,
            height: start_height,
            step: filenames.dir_block_size_l2
        }
    }
}

impl<'a> LevelSingle<'a> {
    pub fn new(filenames: &'a Filenames, height: u64) -> Self {
        let start_height = (height / filenames.dir_block_size_l1) * filenames.dir_block_size_l1;
        LevelSingle {
            filenames,
            height: start_height,
            step: filenames.dir_block_size_l1
        }
    }
}


impl<'a> Level for LevelDouble<'a> {

    fn dir(&self) -> String {
        self.filenames.full_path(
            format!("{}/{}", self.filenames.level_1(self.height), self.filenames.level_2(self.height))
        )
    }

    fn next(self) -> Self {
        LevelDouble {
            filenames: self.filenames,
            height: self.height + self.step,
            step: self.step
        }
    }

    fn height(&self) -> u64 {
        self.height
    }

}

impl<'a> Level for LevelSingle<'a> {

    fn dir(&self) -> String {
        self.filenames.full_path(
            format!("{}", self.filenames.level_1(self.height))
        )
    }

    fn next(self) -> Self {
        LevelSingle {
            filenames: self.filenames,
            height: self.height + self.step,
            step: self.step
        }
    }

    fn height(&self) -> u64 {
        self.height
    }

}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::archiver::range::Range;

    #[test]
    fn single_block_file() {
        let filenames = Filenames::default();
        let kind = DataKind::Blocks;
        let range = Range::Single(12000000.into());
        assert_eq!(filenames.filename(&kind, &range), "012000000.block.avro");
    }

    #[test]
    fn single_block_txes_file() {
        let filenames = Filenames::default();
        let kind = DataKind::Transactions;
        let range = Range::Single(12000000.into());
        assert_eq!(filenames.filename(&kind, &range), "012000000.txes.avro");
    }

    #[test]
    fn single_block_tx_traces_file() {
        let filenames = Filenames::default();
        let kind = DataKind::TransactionTraces;
        let range = Range::Single(12000000.into());
        assert_eq!(filenames.filename(&kind, &range), "012000000.traces.avro");
    }

    #[test]
    fn single_block_path() {
        let filenames = Filenames::default();
        let kind = DataKind::Blocks;
        assert_eq!(filenames.path(&kind, &Range::Single(12000005.into())), "012000000/012000000/012000005.block.avro");
        assert_eq!(filenames.path(&kind, &Range::Single(12004999.into())), "012000000/012004000/012004999.block.avro");
        assert_eq!(filenames.path(&kind, &Range::Single(12005000.into())), "012000000/012005000/012005000.block.avro");
        assert_eq!(filenames.path(&kind, &Range::Single(12005001.into())), "012000000/012005000/012005001.block.avro");
        assert_eq!(filenames.path(&kind, &Range::Single(12345678.into())), "012000000/012345000/012345678.block.avro");
    }

    #[test]
    fn multi_block_path() {
        let filenames = Filenames::default();
        let kind = DataKind::Blocks;
        assert_eq!(filenames.path(&kind, &Range::Multiple(12000000.into(), 12000999.into())), "012000000/range-012000000_012000999.blocks.avro");
    }

    #[test]
    fn multi_tx_path() {
        let filenames = Filenames::default();
        let kind = DataKind::Transactions;
        assert_eq!(filenames.path(&kind, &Range::Multiple(12000000.into(), 12000999.into())), "012000000/range-012000000_012000999.txes.avro");
    }

    #[test]
    fn multi_tx_traces_path() {
        let filenames = Filenames::default();
        let kind = DataKind::TransactionTraces;
        assert_eq!(filenames.path(&kind, &Range::Multiple(12000000.into(), 12000999.into())), "012000000/range-012000000_012000999.traces.avro");
    }

    #[test]
    fn correct_level_dir() {
        let filenames = Filenames::default();
        assert_eq!(LevelDouble::new(&filenames, 12000005).dir(), "012000000/012000000");
        assert_eq!(LevelDouble::new(&filenames, 12345678).dir(), "012000000/012345000");

        assert_eq!(LevelSingle::new(&filenames, 12000005).dir(), "012000000");
        assert_eq!(LevelSingle::new(&filenames, 12345678).dir(), "012000000");
    }

    #[test]
    fn go_next_level2() {
        let filenames = Filenames::default();
        let level = LevelDouble::new(&filenames, 12000005);
        assert_eq!(level.dir(), "012000000/012000000");

        let level = level.next();
        assert_eq!(level.dir(), "012000000/012001000");

        let level = level.next();
        assert_eq!(level.dir(), "012000000/012002000");
    }

    #[test]
    fn go_next_level2_start() {
        let filenames = Filenames::default();
        let level = LevelDouble::new(&filenames, 0);
        assert_eq!(level.dir(), "000000000/000000000");

        let level = level.next();
        assert_eq!(level.dir(), "000000000/000001000");

        let level = level.next();
        assert_eq!(level.dir(), "000000000/000002000");
    }

    #[test]
    fn go_next_level2_at_the_end() {
        let filenames = Filenames::default();
        let level = LevelDouble::new(&filenames, 12_998_005);
        assert_eq!(level.dir(), "012000000/012998000");

        let level = level.next();
        assert_eq!(level.dir(), "012000000/012999000");

        let level = level.next();
        assert_eq!(level.dir(), "013000000/013000000");

        let level = level.next();
        assert_eq!(level.dir(), "013000000/013001000");
    }

    #[test]
    fn go_next_level2_starts_at_edge() {
        let filenames = Filenames::default();
        let level = LevelDouble::new(&filenames, 21917490);
        assert_eq!(level.height, 21917000);
        let level = level.next();
        assert_eq!(level.height, 21918000);
    }

    #[test]
    fn parse_single_block_file() {
        let (kind, range) = Filenames::parse("021625120.block.avro".to_string()).unwrap();
        assert_eq!(kind, DataKind::Blocks);
        assert_eq!(range, Range::Single(21625120.into()));
        assert!(range.first_height().hash.is_none());
    }

    #[test]
    fn parse_single_block_file_with_hash() {
        let (kind, range) = Filenames::parse("021625120.b4e72a78fd2cb0e75768401a92e5258618eec892b7e4edb49b2a592e9a5de5c4.block.avro".to_string()).unwrap();
        assert_eq!(kind, DataKind::Blocks);
        assert_eq!(range, Range::Single(Height::new(21625120, Some("b4e72a78fd2cb0e75768401a92e5258618eec892b7e4edb49b2a592e9a5de5c4".to_string()))));
    }

    #[test]
    fn parse_single_tx_file() {
        let (kind, range) = Filenames::parse("021625139.txes.avro".to_string()).unwrap();
        assert_eq!(kind, DataKind::Transactions);
        assert_eq!(range, Range::Single(21625139.into()));
        assert!(range.first_height().hash.is_none());
    }

    #[test]
    fn parse_single_tx_file_with_hash() {
        let (kind, range) = Filenames::parse("021625139.b4e72a78fd2cb0e75768401a92e5258618eec892b7e4edb49b2a592e9a5de5c4.txes.avro".to_string()).unwrap();
        assert_eq!(kind, DataKind::Transactions);
        assert_eq!(range, Range::Single(Height::new(21625139, Some("b4e72a78fd2cb0e75768401a92e5258618eec892b7e4edb49b2a592e9a5de5c4".to_string()))));

        let (kind, range) = Filenames::parse("009651437.8c6afe53418e6c5265113c2d3dd3ca2541817f6471c42d27e802ebdf13540f8e.txes.avro".to_string()).unwrap();
        assert_eq!(kind, DataKind::Transactions);
        assert_eq!(range, Range::Single(Height::new(9651437, Some("8c6afe53418e6c5265113c2d3dd3ca2541817f6471c42d27e802ebdf13540f8e".to_string()))));
    }

    #[test]
    fn parse_single_tx_traces_file() {
        let (kind, range) = Filenames::parse("021625139.traces.avro".to_string()).unwrap();
        assert_eq!(kind, DataKind::TransactionTraces);
        assert_eq!(range, Range::Single(21625139.into()));
        assert!(range.first_height().hash.is_none());
    }

    #[test]
    fn offset_single_block() {
        let filenames = Filenames::default();
        let single = Range::Single(12000000.into());
        assert_eq!(filenames.offset(&single), "012000000");
    }

    #[test]
    fn offset_multi_block() {
        let filenames = Filenames::default();
        let multi = Range::Multiple(12000000.into(), 12000999.into());
        assert_eq!(filenames.offset(&multi), "range-012000000");
    }

    #[test]
    fn offset_different_for_ranges() {
        let filenames = Filenames::default();
        let multi = Range::Multiple(12000000.into(), 12000999.into());
        let single = multi.first();
        assert_ne!(filenames.offset(&single), filenames.offset(&multi));
    }
}
