use std::fmt::{Display, Formatter};
use std::str::FromStr;
use anyhow::anyhow;

#[derive(Debug, Clone, PartialEq)]
pub enum Range {
    Single(u64),
    Multiple(u64, u64),
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

    pub fn contains(&self, height: u64) -> bool {
        match self {
            Range::Single(h) => *h == height,
            Range::Multiple(start, end) => *start <= height && height <= *end,
        }
    }

    pub fn intersect(&self, other: &Self) -> bool {
        match self {
            Range::Single(h) => {
                other.contains(*h)
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
        assert!(single.contains(5));
        assert!(!single.contains(4));
        assert!(!single.contains(0));
        assert!(!single.contains(6));
    }

    #[test]
    fn test_range_contains_multi() {
        let multiple = Range::Multiple(3, 7);
        assert!(multiple.contains(3));
        assert!(multiple.contains(4));
        assert!(multiple.contains(5));
        assert!(multiple.contains(6));
        assert!(multiple.contains(7));
        assert!(!multiple.contains(8));
        assert!(!multiple.contains(2));
        assert!(!multiple.contains(0));
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
}
