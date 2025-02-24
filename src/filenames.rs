use std::str::FromStr;
use lazy_static::lazy_static;
use regex::Regex;
use crate::datakind::DataKind;
use crate::range::Range;

lazy_static! {
    static ref RE_SINGLE: Regex = Regex::new(r"^(\d+)\.(\w+)\.(\w+\.)?avro$").unwrap();
    static ref RE_RANGE: Regex = Regex::new(r"^range-(\d+)_(\d+)\.(\w+)\.(\w+\.)?avro$").unwrap();
}

#[derive(Debug, Clone)]
pub struct Filenames {
    parent: String,
    padding: usize,
    dir_block_size_l1: u64,
    dir_block_size_l2: u64,
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
            let height = cap.get(1).unwrap().as_str().parse().unwrap();
            let kind = DataKind::from_str(cap.get(2).unwrap().as_str());
            if kind.is_err() {
                return None;
            }
            return Some((kind.unwrap(), Range::Single(height)));
        }
        if let Some(cap) = RE_RANGE.captures(filename.as_str()) {
            let start = cap.get(1).unwrap().as_str().parse().unwrap();
            let end = cap.get(2).unwrap().as_str().parse().unwrap();
            let kind = DataKind::from_str(cap.get(3).unwrap().as_str());
            if kind.is_err() {
                return None;
            }
            return Some((kind.unwrap(), Range::Multiple(start, end)));
        }
        None
    }

    pub fn filename(&self, kind: &DataKind, range: &Range) -> String {
        let suffix = match kind {
            DataKind::Blocks => match range {
                Range::Single(_) => "block",
                Range::Multiple(_, _) => "blocks"
            },
            DataKind::Transactions => "txes",
        };

        match range {
            Range::Single(height) => {
                format!("{}.{}.avro", self.range_padded(*height), suffix)
            },
            Range::Multiple(start, end) => {
                format!("{}_{}.{}.avro", self.range_padded(*start), self.range_padded(*end), suffix)
            }
        }
    }

    pub fn relative_path(&self, kind: &DataKind, range: &Range) -> String {
        let start_height = range.start();
        format!("{}/{}/{}",
            self.level_1(start_height),
            self.level_2(start_height),
            self.filename(kind, range)
        )
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

    pub fn levels(&self, height: u64) -> Level {
        let level_height = (height / self.dir_block_size_l2) * self.dir_block_size_l2;
        Level {
            filenames: self,
            height: level_height,
        }
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

pub struct Level<'a> {
    pub filenames: &'a Filenames,
    pub height: u64,
}

impl <'a> Level<'a> {

    pub fn next_l2(&self) -> Level<'a> {
        let height = self.height + self.filenames.dir_block_size_l2;

        Level {
            filenames: self.filenames,
            height,
        }
    }

    pub fn dir(&self) -> String {
        self.filenames.full_path(
            format!("{}/{}", self.filenames.level_1(self.height), self.filenames.level_2(self.height))
        )
    }

}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::range::Range;

    #[test]
    fn single_block_file() {
        let filenames = Filenames::default();
        let kind = DataKind::Blocks;
        let range = Range::Single(12000000);
        assert_eq!(filenames.filename(&kind, &range), "012000000.block.avro");
    }

    #[test]
    fn single_block_txes_file() {
        let filenames = Filenames::default();
        let kind = DataKind::Transactions;
        let range = Range::Single(12000000);
        assert_eq!(filenames.filename(&kind, &range), "012000000.txes.avro");
    }

    #[test]
    fn single_block_path() {
        let filenames = Filenames::default();
        let kind = DataKind::Blocks;
        assert_eq!(filenames.path(&kind, &Range::Single(12000005)), "012000000/012000000/012000005.block.avro");
        assert_eq!(filenames.path(&kind, &Range::Single(12004999)), "012000000/012004000/012004999.block.avro");
        assert_eq!(filenames.path(&kind, &Range::Single(12005000)), "012000000/012005000/012005000.block.avro");
        assert_eq!(filenames.path(&kind, &Range::Single(12005001)), "012000000/012005000/012005001.block.avro");
        assert_eq!(filenames.path(&kind, &Range::Single(12345678)), "012000000/012345000/012345678.block.avro");
    }

    #[test]
    fn correct_level_dir() {
        let filenames = Filenames::default();
        assert_eq!(filenames.levels(12000005).dir(), "012000000/012000000");
        assert_eq!(filenames.levels(12345678).dir(), "012000000/012345000");
    }

    #[test]
    fn go_next_level2() {
        let filenames = Filenames::default();
        let level = filenames.levels(12000005);
        assert_eq!(level.dir(), "012000000/012000000");

        let level = level.next_l2();
        assert_eq!(level.dir(), "012000000/012001000");

        let level = level.next_l2();
        assert_eq!(level.dir(), "012000000/012002000");
    }

    #[test]
    fn go_next_level2_start() {
        let filenames = Filenames::default();
        let level = filenames.levels(0);
        assert_eq!(level.dir(), "000000000/000000000");

        let level = level.next_l2();
        assert_eq!(level.dir(), "000000000/000001000");

        let level = level.next_l2();
        assert_eq!(level.dir(), "000000000/000002000");
    }

    #[test]
    fn go_next_level2_at_the_end() {
        let filenames = Filenames::default();
        let level = filenames.levels(12_998_005);
        assert_eq!(level.dir(), "012000000/012998000");

        let level = level.next_l2();
        assert_eq!(level.dir(), "012000000/012999000");

        let level = level.next_l2();
        assert_eq!(level.dir(), "013000000/013000000");

        let level = level.next_l2();
        assert_eq!(level.dir(), "013000000/013001000");
    }

    #[test]
    fn go_next_level2_starts_at_edge() {
        let filenames = Filenames::default();
        let level = filenames.levels(21917490);
        assert_eq!(level.height, 21917000);
        let level = level.next_l2();
        assert_eq!(level.height, 21918000);
    }

    #[test]
    fn parse_single_block_file() {
        let (kind, range) = Filenames::parse("021625120.block.avro".to_string()).unwrap();
        assert_eq!(kind, DataKind::Blocks);
        assert_eq!(range, Range::Single(21625120));
    }

    #[test]
    fn parse_single_tx_file() {
        let (kind, range) = Filenames::parse("021625139.txes.avro".to_string()).unwrap();
        assert_eq!(kind, DataKind::Transactions);
        assert_eq!(range, Range::Single(21625139));
    }
}
