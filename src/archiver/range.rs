use std::fmt::{Display, Formatter};
use std::str::FromStr;
use anyhow::anyhow;
use crate::archiver::BlockHash;

///
/// A particular hash on blockchain
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Height {
    /// The height number
    pub height: u64,
    /// Optional block hash at this height
    /// If it's specified it has the higher priority on selecting blocks
    pub hash: Option<BlockHash>,
}

impl Height {
    pub fn new<S: Into<BlockHash>>(height: u64, hash: Option<S>) -> Self {
        Height { height, hash: hash.map(|h| h.into()) }
    }
}

impl From<u64> for Height {
    fn from(height: u64) -> Self {
        Height {
            height,
            hash: None,
        }
    }
}

impl Display for Height {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(hash) = &self.hash {
            write!(f, "{} ({})", self.height, hash)
        } else {
            write!(f, "{}", self.height)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Range {
    /// At a single height
    Single(Height),
    /// Sequence of blocks from start height to end (inclusive)
    Multiple(Height, Height),
}

impl PartialOrd for Range {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Range {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start().cmp(&other.start())
    }
}

impl Range {

    pub fn new(start: u64, end: u64) -> Self {
        if start > end {
            panic!("Invalid range: {}..{}", start, end);
        }
        if start == end {
            Range::Single(Height::from(start))
        } else {
            Range::Multiple(Height::from(start), Height::from(end))
        }
    }

    pub fn up_to(size: u64, target: &Self) -> Self {
        let target_start = target.start();
        let start = if size > target_start {
            0
        } else {
            target_start - size
        };
        Range::new(start, target_start - 1)
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = u64>> {
        match self {
            Range::Single(height) => Box::new(std::iter::once(height.height)),
            Range::Multiple(start, end) => Box::new((start.height..=end.height).into_iter()),
        }
    }

    pub fn iter_height(&self) -> Box<dyn Iterator<Item = Height>> {
        match self {
            Range::Single(height) => Box::new(std::iter::once(height.clone())),
            Range::Multiple(start, end) => Box::new(
                (start.height..=end.height).into_iter()
                    .map(|h| Height::from(h))
            ),
        }
    }

    pub fn start(&self) -> u64 {
        match self {
            Range::Single(height) => height.height,
            Range::Multiple(start, _) => start.height,
        }
    }

    pub fn end(&self) -> u64 {
        match self {
            Range::Single(height) => height.height,
            Range::Multiple(_, end) => end.height,
        }
    }

    ///
    /// Check if this range fully contains another range (i.e., inside the current or equal)
    pub fn contains(&self, other: &Range) -> bool {
        match self {
            Range::Single(h) => match other {
                Range::Single(ho) => h.height == ho.height,
                Range::Multiple(_, _) => false
            },
            Range::Multiple(start, end) => match other {
                Range::Single(h) => {
                    start.height <= h.height && h.height <= end.height
                }
                Range::Multiple(other_start, other_end) => {
                    start.height <= other_start.height && end.height >= other_end.height
                }
            }
        }
    }

    pub fn is_intersected_with(&self, other: &Self) -> bool {
        match self {
            Range::Single(h) => {
                match other {
                    Range::Single(other_h) => h.height == other_h.height,
                    Range::Multiple(other_start, other_end) => other_start.height <= h.height && h.height <= other_end.height
                }
            }
            Range::Multiple(start, end) => {
                match other {
                    Range::Single(h) => {
                        start.height <= h.height && h.height <= end.height
                    }
                    Range::Multiple(other_start, other_end) => {
                        if start.height < other_start.height {
                            end.height >= other_start.height
                        } else if start.height < other_end.height {
                            true
                        } else {
                            false
                        }
                    }
                }
            }
        }
    }

    pub fn is_connected_to(&self, another: &Self) -> bool {
        if another.start() < self.start() {
            return another.is_connected_to(self);
        }
        self.end() + 1 == another.start()
    }

    pub fn len(&self) -> usize {
        match self {
            Range::Single(_) => 1,
            Range::Multiple(start, end) => (end.height - start.height + 1) as usize,
        }
    }

    ///
    /// Join two ranges into one if they have any intersection or go next to each other.
    pub fn join(self, another: Range) -> anyhow::Result<Self> {
        if self.is_intersected_with(&another) || self.is_connected_to(&another) {
            let start = std::cmp::min(self.start(), another.start());
            let end = std::cmp::max(self.end(), another.end());
            Ok(Range::new(start, end))
        } else {
            Err(anyhow!("Ranges do not intersect"))
        }
    }

    ///
    /// Cut the current range by another range, returning 0, 1 or 2 ranges.
    /// Ex:
    /// - if another range covers the current - return empty
    /// - if it intersected anywhere - return the remaining parts (could head, tal, or both)
    pub fn cut(self, another: &Self) -> Vec<Self> {
        if !self.is_intersected_with(another) {
            return vec![self];
        }
        if another.contains(&self) {
            return vec![];
        }

        let mut result = Vec::new();
        if self.start() < another.start() {
            result.push(Range::new(self.start(), another.start() - 1));
        }
        if self.end() > another.end() {
            result.push(Range::new(another.end() + 1, self.end()));
        }
        result
    }

    ///
    /// Split this range into chunks of the given size.
    /// Note that the chunks start at 0, not from the start of the range.
    /// @param chunk_size The size of the chunks
    /// @param aligned If true, returns only chunks starting and ending at the chunk boundary. (i.e., if chunk size is 100, returns 100..199, for initial range 95..201)
    pub fn split_chunks(&self, chunk_size: usize, aligned: bool) -> Vec<Self> {

        match self {
            Range::Single(_) => {
                if aligned && chunk_size != 1{
                    return vec![];
                }
                vec![self.clone()]
            }
            Range::Multiple(start, end) => {
                let mut result = Vec::new();
                // Find the chunk boundary that contains the start
                let chunk_start = (start.height / chunk_size as u64) * chunk_size as u64;
                let mut current_start = chunk_start;

                if aligned {
                    let is_aligned = current_start == start.height;
                    if !is_aligned {
                        current_start = (start.height / chunk_size as u64 + 1) * chunk_size as u64;
                    }
                }

                while current_start <= end.height {
                    let current_end = std::cmp::min(current_start + chunk_size as u64 - 1, end.height);

                    if aligned {
                        let is_aligned = current_end % chunk_size as u64 == chunk_size as u64 - 1;
                        if !is_aligned {
                            // reached after the boundary of the initial range
                            break
                        }
                    }

                    let adjusted_start = std::cmp::max(current_start, start.height);
                    result.push(Range::new(adjusted_start, current_end));
                    current_start = current_end + 1;
                }
                result
            }
        }

    }

    pub fn first(&self) -> Self {
        match self {
            Range::Single(h) => Range::Single(h.clone()),
            Range::Multiple(start, _) => Range::Single(start.clone()),
        }
    }

    pub fn first_height(&self) -> Height {
        match self {
            Range::Single(h) => h.clone(),
            Range::Multiple(start, _) => start.clone(),
        }
    }
}

impl FromStr for Range {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("..").collect();
        if parts.len() == 1 {
            let h = parts[0].parse::<u64>()?;
            return Ok(Range::Single(Height::from(h)));
        }
        if parts.len() != 2 {
            return Err(anyhow!("Invalid range: {}", s));
        }
        let start = parts[0].parse::<u64>()?;
        let end = parts[1].parse::<u64>()?;
        Ok(Range::new(start, end))
    }
}

impl Display for Range {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Range::Single(height) => write!(f, "{}", height.height),
            Range::Multiple(start, end) => write!(f, "{}..{}", start.height, end.height),
        }
    }
}

impl From<u64> for Range {
    fn from(h: u64) -> Self {
        Range::Single(h.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_new() {
        assert_eq!(Range::new(5, 5), Range::Single(5.into()));
        assert_eq!(Range::new(3, 7), Range::Multiple(3.into(), 7.into()));
    }

    #[test]
    fn test_range_up_to() {
        let range = Range::new(10, 20);
        assert_eq!(Range::up_to(1, &range), Range::new(9, 9));
        assert_eq!(Range::up_to(5, &range), Range::new(5, 9));
        assert_eq!(Range::up_to(15, &range), Range::new(0, 9));
        assert_eq!(Range::up_to(25, &range), Range::new(0, 9));
    }

    #[test]
    fn test_range_iter_single() {
        let single = Range::Single(5.into());
        let values: Vec<u64> = single.iter().collect();
        assert_eq!(values, vec![5]);
    }

    #[test]
    fn test_range_iter_multiple() {
        let multiple = Range::Multiple(3.into(), 5.into());
        let values: Vec<u64> = multiple.iter().collect();
        assert_eq!(values, vec![3, 4, 5]);
    }

    #[test]
    fn test_range_start() {
        assert_eq!(Range::Single(5.into()).start(), 5);
        assert_eq!(Range::Multiple(3.into(), 7.into()).start(), 3);
    }

    #[test]
    fn test_range_end() {
        assert_eq!(Range::Single(5.into()).end(), 5);
        assert_eq!(Range::Multiple(3.into(), 7.into()).end(), 7);
    }

    #[test]
    fn test_range_contains_single() {
        let single = Range::Single(5.into());
        assert!(single.contains(&Range::Single(5.into())));
        assert!(!single.contains(&Range::Single(4.into())));
        assert!(!single.contains(&Range::Single(0.into())));
        assert!(!single.contains(&Range::Single(6.into())));
    }

    #[test]
    fn test_range_contains_multi() {
        let multiple = Range::Multiple(3.into(), 7.into());
        assert!(multiple.contains(&Range::Single(3.into())));
        assert!(multiple.contains(&Range::Single(4.into())));
        assert!(multiple.contains(&Range::Single(5.into())));
        assert!(multiple.contains(&Range::Single(6.into())));
        assert!(multiple.contains(&Range::Single(7.into())));
        assert!(!multiple.contains(&Range::Single(8.into())));
        assert!(!multiple.contains(&Range::Single(2.into())));
        assert!(!multiple.contains(&Range::Single(0.into())));
    }

    #[test]
    fn test_range_contains_parts() {
        let large = Range::Multiple(1.into(), 29.into());
        assert!(large.contains(&Range::Multiple(1.into(), 9.into())));
        assert!(large.contains(&Range::Multiple(10.into(), 19.into())));
        assert!(large.contains(&Range::Multiple(20.into(), 29.into())));
    }

    #[test]
    fn test_intersection() {
        let base = Range::new(20_000, 21_000);

        assert!(base.is_intersected_with(&Range::new(0, 100_000)));
        assert!(base.is_intersected_with(&Range::new(0, 21_000)));
        assert!(base.is_intersected_with(&Range::new(0, 20_500)));
        assert!(base.is_intersected_with(&Range::new(0, 20_001)));
        assert!(base.is_intersected_with(&Range::new(10_000, 21_000)));
        assert!(base.is_intersected_with(&Range::new(20_000, 21_000)));
        assert!(base.is_intersected_with(&Range::new(20_000, 25_000)));
        assert!(base.is_intersected_with(&Range::new(20_500, 21_000)));
        assert!(base.is_intersected_with(&Range::new(20_500, 20_510)));
        assert!(base.is_intersected_with(&Range::new(20_500, 21_500)));
        assert!(base.is_intersected_with(&Range::new(20_999, 21_099)));
        assert!(base.is_intersected_with(&Range::new(20_999, 21_000)));
    }

    #[test]
    fn test_no_intersect_next() {
        let base =  Range::new(1, 29);
        let other = Range::new(30, 39);

        assert!(!base.is_intersected_with(&other));
        assert!(!other.is_intersected_with(&base));

        let base =  Range::new(21_500_000, 21_599_999);
        let other = Range::new(21_600_000, 21_600_999);

        assert!(!base.is_intersected_with(&other));
        assert!(!other.is_intersected_with(&base));
    }

    #[test]
    fn test_no_intersection() {
        let base = Range::new(20_000, 21_000);

        assert!(!base.is_intersected_with(&Range::new(0, 10_000)));
        assert!(!base.is_intersected_with(&Range::new(21_001, 30_000)));
        assert!(!base.is_intersected_with(&Range::new(25_000, 30_000)));
    }

    #[test]
    fn test_connected_next() {
        let base =  Range::new(1, 29);
        let other = Range::new(30, 39);

        assert!(base.is_connected_to(&other));
        assert!(other.is_connected_to(&base));
    }

    #[test]
    fn test_not_connected_far() {
        let base =  Range::new(1, 29);
        let other = Range::new(40, 79);

        assert!(!base.is_connected_to(&other));
        assert!(!other.is_connected_to(&base));
    }

    #[test]
    fn test_split_chunks_large() {
        assert_eq!(
            Range::new(123, 345).split_chunks(100, false),
            vec![
                Range::new(123, 199),
                Range::new(200, 299),
                Range::new(300, 345),
            ]
        );
        assert_eq!(
            Range::new(153, 407).split_chunks(100, false),
            vec![
                Range::new(153, 199),
                Range::new(200, 299),
                Range::new(300, 399),
                Range::new(400, 407),
            ]
        );

        assert_eq!(
            Range::new(123, 345).split_chunks(100, true),
            vec![
                Range::new(200, 299),
            ]
        );
        assert_eq!(
            Range::new(153, 407).split_chunks(100, true),
            vec![
                Range::new(200, 299),
                Range::new(300, 399),
            ]
        );
    }

    #[test]
    fn test_split_chunks_already_at_boundaries() {
        assert_eq!(
            Range::new(200, 999).split_chunks(200, false),
            vec![
                Range::new(200, 399),
                Range::new(400, 599),
                Range::new(600, 799),
                Range::new(800, 999),
            ]
        );
        assert_eq!(
            Range::new(200, 999).split_chunks(200, true),
            vec![
                Range::new(200, 399),
                Range::new(400, 599),
                Range::new(600, 799),
                Range::new(800, 999),
            ]
        );
    }

    #[test]
    fn test_split_single() {
        assert_eq!(
            Range::Single(123.into()).split_chunks(100, false),
            vec![
                Range::Single(123.into()),
            ]
        );

        assert_eq!(
            Range::Single(123.into()).split_chunks(100, true),
            vec![]
        );
    }

    #[test]
    fn test_split_middle() {
        assert_eq!(
            Range::new(150, 250).split_chunks(100, false),
            vec![
                Range::new(150, 199),
                Range::new(200, 250),
            ]
        );
        assert_eq!(
            Range::new(150, 250).split_chunks(100, true),
            vec![]
        );
    }

    #[test]
    fn test_split_fits() {
        assert_eq!(
            Range::new(150, 250).split_chunks(1000, false),
            vec![
                Range::new(150, 250),
            ]
        );
        assert_eq!(
            Range::new(150, 250).split_chunks(1000, true),
            vec![]
        );
    }

    #[test]
    fn parse_range() {
        assert_eq!(
            "1..10".parse::<Range>().unwrap(),
            Range::new(1, 10)
        );
        assert_eq!(
            "1000..1999".parse::<Range>().unwrap(),
            Range::new(1000, 1999)
        );
    }

    #[test]
    fn parse_single() {
        assert_eq!(
            "1".parse::<Range>().unwrap(),
            Range::Single(1.into())
        );
        assert_eq!(
            "1999".parse::<Range>().unwrap(),
            Range::Single(1999.into())
        );
    }

    #[test]
    fn test_cut_no_intersection() {
        let base = Range::new(10, 20);
        let other = Range::new(21, 30);
        let result = base.cut(&other);
        assert_eq!(result, vec![Range::new(10, 20)]);
    }

    #[test]
    fn test_cut_fully_covered() {
        let base = Range::new(10, 20);
        let other = Range::new(10, 20);
        let result = base.cut(&other);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_cut_partial_head() {
        let base = Range::new(10, 20);
        let other = Range::new(10, 15);
        let result = base.cut(&other);
        assert_eq!(result, vec![Range::new(16, 20)]);
    }

    #[test]
    fn test_cut_partial_tail() {
        let base = Range::new(10, 20);
        let other = Range::new(16, 20);
        let result = base.cut(&other);
        assert_eq!(result, vec![Range::new(10, 15)]);
    }

    #[test]
    fn test_cut_middle() {
        let base = Range::new(10, 20);
        let other = Range::new(13, 17);
        let result = base.cut(&other);
        assert_eq!(result, vec![Range::new(10, 12), Range::new(18, 20)]);
    }

    #[test]
    fn test_cut_single_from_multiple() {
        let base = Range::new(10, 20);
        let other = Range::Single(15.into());
        let result = base.cut(&other);
        assert_eq!(result, vec![Range::new(10, 14), Range::new(16, 20)]);
    }

    #[test]
    fn test_cut_multiple_from_single() {
        let base = Range::Single(15.into());
        let other = Range::new(10, 20);
        let result = base.cut(&other);
        assert_eq!(result, vec![]);
    }
}
