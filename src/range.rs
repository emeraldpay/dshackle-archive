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
}
