use std::fmt::{Display, Formatter};
use std::str::FromStr;
use anyhow::anyhow;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Range {
    Single(u64),
    Multiple(u64, u64),
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
            Range::Single(start)
        } else {
            Range::Multiple(start, end)
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
            Range::Single(height) => Box::new(std::iter::once(*height)),
            Range::Multiple(start, end) => Box::new((*start..=*end).into_iter()),
        }
    }

    pub fn start(&self) -> u64 {
        match self {
            Range::Single(height) => *height,
            Range::Multiple(start, _) => *start,
        }
    }

    pub fn end(&self) -> u64 {
        match self {
            Range::Single(height) => *height,
            Range::Multiple(_, end) => *end,
        }
    }

    pub fn contains(&self, other: &Range) -> bool {
        match self {
            Range::Single(h) => match other {
                Range::Single(ho) => *h == *ho,
                Range::Multiple(_, _) => false
            },
            Range::Multiple(start, end) => match other {
                Range::Single(h) => {
                    *start <= *h && *h <= *end
                }
                Range::Multiple(other_start, other_end) => {
                    *start >= *other_start && *end <= *other_end
                }
            }
        }
    }

    pub fn intersect(&self, other: &Self) -> bool {
        match self {
            Range::Single(h) => {
                match other {
                    Range::Single(other_h) => *h == *other_h,
                    Range::Multiple(other_start, other_end) => other_start <= h && h <= other_end
                }
            }
            Range::Multiple(start, end) => {
                match other {
                    Range::Single(h) => {
                        *start <= *h && *h <= *end
                    }
                    Range::Multiple(other_start, other_end) => {
                        if *start < *other_start {
                            *end >= *other_start
                        } else if *start < *other_end {
                            true
                        } else {
                            false
                        }
                    }
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Range::Single(_) => 1,
            Range::Multiple(start, end) => (end - start + 1) as usize,
        }
    }

    pub fn join(self, another: Range) -> anyhow::Result<Self> {
        if self.intersect(&another) {
            let start = std::cmp::min(self.start(), another.start());
            let end = std::cmp::max(self.end(), another.end());
            Ok(Range::new(start, end))
        } else {
            Err(anyhow!("Ranges do not intersect"))
        }
    }

    ///
    /// Split this range into chunks of the given size.
    /// Note that the chunks start at 0, not from the start of the range.
    pub fn split_chunks(&self, chunk_size: usize) -> Vec<Self> {

        match self {
            Range::Single(_) => {
                vec![self.clone()]
            }
            Range::Multiple(start, end) => {
                let mut result = Vec::new();
                // Find the chunk boundary that contains the start
                let chunk_start = (start / chunk_size as u64) * chunk_size as u64;
                let mut current_start = chunk_start;

                while current_start <= *end {
                    let current_end = std::cmp::min(current_start + chunk_size as u64 - 1, *end);
                    let adjusted_start = std::cmp::max(current_start, *start);
                    result.push(Range::new(adjusted_start, current_end));
                    current_start = current_end + 1;
                }
                result
            }
        }

    }
}

impl FromStr for Range {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("..").collect();
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
            Range::Single(height) => write!(f, "{}", height),
            Range::Multiple(start, end) => write!(f, "{}..{}", start, end),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_new() {
        assert_eq!(Range::new(5, 5), Range::Single(5));
        assert_eq!(Range::new(3, 7), Range::Multiple(3, 7));
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
        let single = Range::Single(5);
        let values: Vec<u64> = single.iter().collect();
        assert_eq!(values, vec![5]);
    }

    #[test]
    fn test_range_iter_multiple() {
        let multiple = Range::Multiple(3, 5);
        let values: Vec<u64> = multiple.iter().collect();
        assert_eq!(values, vec![3, 4, 5]);
    }

    #[test]
    fn test_range_start() {
        assert_eq!(Range::Single(5).start(), 5);
        assert_eq!(Range::Multiple(3, 7).start(), 3);
    }

    #[test]
    fn test_range_end() {
        assert_eq!(Range::Single(5).end(), 5);
        assert_eq!(Range::Multiple(3, 7).end(), 7);
    }

    #[test]
    fn test_range_contains_single() {
        let single = Range::Single(5);
        assert!(single.contains(&Range::Single(5)));
        assert!(!single.contains(&Range::Single(4)));
        assert!(!single.contains(&Range::Single(0)));
        assert!(!single.contains(&Range::Single(6)));
    }

    #[test]
    fn test_range_contains_multi() {
        let multiple = Range::Multiple(3, 7);
        assert!(multiple.contains(&Range::Single(3)));
        assert!(multiple.contains(&Range::Single(4)));
        assert!(multiple.contains(&Range::Single(5)));
        assert!(multiple.contains(&Range::Single(6)));
        assert!(multiple.contains(&Range::Single(7)));
        assert!(!multiple.contains(&Range::Single(8)));
        assert!(!multiple.contains(&Range::Single(2)));
        assert!(!multiple.contains(&Range::Single(0)));
    }

    #[test]
    fn test_intersection() {
        let base = Range::new(20_000, 21_000);

        assert!(base.intersect(&Range::new(0, 100_000)));
        assert!(base.intersect(&Range::new(0, 21_000)));
        assert!(base.intersect(&Range::new(0, 20_500)));
        assert!(base.intersect(&Range::new(0, 20_001)));
        assert!(base.intersect(&Range::new(10_000, 21_000)));
        assert!(base.intersect(&Range::new(20_000, 21_000)));
        assert!(base.intersect(&Range::new(20_000, 25_000)));
        assert!(base.intersect(&Range::new(20_500, 21_000)));
        assert!(base.intersect(&Range::new(20_500, 20_510)));
        assert!(base.intersect(&Range::new(20_500, 21_500)));
        assert!(base.intersect(&Range::new(20_999, 21_099)));
        assert!(base.intersect(&Range::new(20_999, 21_000)));
    }

    #[test]
    fn test_no_intersection() {
        let base = Range::new(20_000, 21_000);

        assert!(!base.intersect(&Range::new(0, 10_000)));
        assert!(!base.intersect(&Range::new(21_001, 30_000)));
        assert!(!base.intersect(&Range::new(25_000, 30_000)));
    }

    #[test]
    fn test_split_chunks_large() {
        assert_eq!(
            Range::new(123, 345).split_chunks(100),
            vec![
                Range::new(123, 199),
                Range::new(200, 299),
                Range::new(300, 345),
            ]
        )
    }

    #[test]
    fn test_split_chunks_already_at_boundaries() {
        assert_eq!(
            Range::new(200, 999).split_chunks(200),
            vec![
                Range::new(200, 399),
                Range::new(400, 599),
                Range::new(600, 799),
                Range::new(800, 999),
            ]
        )
    }

    #[test]
    fn test_split_single() {
        assert_eq!(
            Range::Single(123).split_chunks(100),
            vec![
                Range::Single(123),
            ]
        )
    }

    #[test]
    fn test_split_middle() {
        assert_eq!(
            Range::new(150, 250).split_chunks(100),
            vec![
                Range::new(150, 199),
                Range::new(200, 250),
            ]
        )
    }

    #[test]
    fn test_split_fits() {
        assert_eq!(
            Range::new(150, 250).split_chunks(1000),
            vec![
                Range::new(150, 250),
            ]
        )
    }
}
