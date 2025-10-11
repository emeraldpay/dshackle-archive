use std::ops::Add;
use crate::range::Range;

///
/// Multiple ranges
pub struct RangeBag {
    pub ranges: Vec<Range>,
}

impl RangeBag {
    pub fn new() -> Self {
        Self {
            ranges: vec![],
        }
    }

    pub fn append(&mut self, range: Range) {
        self.ranges.push(range);
    }

    /// Total ranges in the bag (NOT the total of blocks)
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Check if the bag is empty
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Remove (cut out) a range from all overlapping ranges in the bag.
    /// This modifies the bag in place by removing the specified range from any ranges that contain it.
    pub fn remove(&mut self, to_remove: &Range) {
        let mut new_ranges = Vec::new();

        for range in self.ranges.drain(..) {
            if range.is_intersected_with(to_remove) {
                // Cut the range and add the remaining parts
                new_ranges.extend(range.cut(to_remove));
                // If cut fails, we skip this range (effectively removing it)
            } else {
                // Keep the range as is
                new_ranges.push(range);
            }
        }

        self.ranges = new_ranges;
    }

    ///
    /// Join connected and intersected ranges into a new bag
    pub fn compact(self) -> Self {
        if self.ranges.len() <= 1{
            return self
        }

        let mut result: Vec<Range> = self.ranges;

        let mut changed = true;
        while changed {
            changed = false;
            for i in 0..result.len()-1 {
                let mut current = result[i].clone();
                for j in i+1..result.len() {
                    let next = result[j].clone();
                    if current.is_intersected_with(&next) || current.is_connected_to(&next) {
                        current = current.join(next).unwrap();
                        changed = true;
                    }
                }
                if changed {
                    result.retain(|n| !current.contains(n));
                    result.push(current);
                    break
                }
            }
        }

        result.sort();

        Self {
            ranges: result,
        }

    }

    pub fn as_single_range(&self) -> Option<&Range> {
        if self.ranges.len() == 1 {
            Some(&self.ranges[0])
        } else {
            None
        }
    }
}

impl Add for RangeBag {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut result = self;
        for range in other.ranges {
            result.append(range);
        }
        result
    }
}

impl<R: Into<Range>> From<Vec<R>> for RangeBag {
    fn from(value: Vec<R>) -> Self {
        let mut result = RangeBag::new();
        for r in value {
            result.append(r.into());
        }
        result
    }
}


#[cfg(test)]
mod tests {
    use super::RangeBag;

    #[test]
    fn test_single_range() {
        let mut bag = RangeBag::new();
        bag.append("1..10".parse().unwrap());
        let compacted = bag.compact();
        assert_eq!(compacted.ranges.len(), 1);
        assert_eq!(compacted.ranges[0].to_string(), "1..10");
    }

    #[test]
    fn test_connected_ranges() {
        let mut bag = RangeBag::new();
        bag.append("1..9".parse().unwrap());
        bag.append("10..19".parse().unwrap());
        bag.append("30..39".parse().unwrap());
        bag.append("20..29".parse().unwrap());

        let compacted = bag.compact();
        assert_eq!(compacted.ranges.len(), 1);
        assert_eq!(compacted.ranges[0].to_string(), "1..39");
    }

    #[test]
    fn test_multiple_ranges() {
        let mut bag = RangeBag::new();

        bag.append("1..9".parse().unwrap());
        bag.append("10..19".parse().unwrap());
        bag.append("20..29".parse().unwrap());

        bag.append("50..59".parse().unwrap());
        bag.append("60..79".parse().unwrap());

        let compacted = bag.compact();
        assert_eq!(compacted.ranges.len(), 2);
        assert_eq!(compacted.ranges[0].to_string(), "1..29");
        assert_eq!(compacted.ranges[1].to_string(), "50..79");
    }

    #[test]
    fn test_duplicate_ranges() {
        let mut bag = RangeBag::new();

        bag.append("1..9".parse().unwrap());
        bag.append("10..19".parse().unwrap());
        bag.append("20..29".parse().unwrap());
        bag.append("5..19".parse().unwrap());

        bag.append("50..59".parse().unwrap());
        bag.append("60..69".parse().unwrap());
        bag.append("60".parse().unwrap());
        bag.append("60..79".parse().unwrap());

        let compacted = bag.compact();
        assert_eq!(compacted.ranges.len(), 2);
        assert_eq!(compacted.ranges[0].to_string(), "1..29");
        assert_eq!(compacted.ranges[1].to_string(), "50..79");
    }

    #[test]
    fn test_singles() {
        let mut bag = RangeBag::new();

        bag.append("1".parse().unwrap());
        bag.append("2".parse().unwrap());
        bag.append("3".parse().unwrap());
        bag.append("4".parse().unwrap());
        bag.append("5".parse().unwrap());

        bag.append("7".parse().unwrap());

        bag.append("9".parse().unwrap());
        bag.append("10".parse().unwrap());


        let compacted = bag.compact();
        assert_eq!(compacted.ranges.len(), 3);
        assert_eq!(compacted.ranges[0].to_string(), "1..5");
        assert_eq!(compacted.ranges[1].to_string(), "7");
        assert_eq!(compacted.ranges[2].to_string(), "9..10");
    }

    #[test]
    fn test_remove_from_middle() {
        let mut bag = RangeBag::new();
        bag.append("1..10".parse().unwrap());
        bag.remove(&"5..6".parse().unwrap());

        assert_eq!(bag.ranges.len(), 2);
        let mut sorted = bag.ranges.clone();
        sorted.sort();
        assert_eq!(sorted[0].to_string(), "1..4");
        assert_eq!(sorted[1].to_string(), "7..10");
    }

    #[test]
    fn test_remove_from_start() {
        let mut bag = RangeBag::new();
        bag.append("1..10".parse().unwrap());
        bag.remove(&"1..3".parse().unwrap());

        assert_eq!(bag.ranges.len(), 1);
        assert_eq!(bag.ranges[0].to_string(), "4..10");
    }

    #[test]
    fn test_remove_from_end() {
        let mut bag = RangeBag::new();
        bag.append("1..10".parse().unwrap());
        bag.remove(&"8..10".parse().unwrap());

        assert_eq!(bag.ranges.len(), 1);
        assert_eq!(bag.ranges[0].to_string(), "1..7");
    }

    #[test]
    fn test_remove_entire_range() {
        let mut bag = RangeBag::new();
        bag.append("1..10".parse().unwrap());
        bag.remove(&"1..10".parse().unwrap());

        assert_eq!(bag.ranges.len(), 0);
    }

    #[test]
    fn test_remove_no_intersection() {
        let mut bag = RangeBag::new();
        bag.append("1..10".parse().unwrap());
        bag.remove(&"20..30".parse().unwrap());

        assert_eq!(bag.ranges.len(), 1);
        assert_eq!(bag.ranges[0].to_string(), "1..10");
    }

    #[test]
    fn test_remove_single_block() {
        let mut bag = RangeBag::new();
        bag.append("1..10".parse().unwrap());
        bag.remove(&"5".parse().unwrap());

        assert_eq!(bag.ranges.len(), 2);
        let mut sorted = bag.ranges.clone();
        sorted.sort();
        assert_eq!(sorted[0].to_string(), "1..4");
        assert_eq!(sorted[1].to_string(), "6..10");
    }

    #[test]
    fn test_remove_from_multiple_ranges() {
        let mut bag = RangeBag::new();
        bag.append("1..10".parse().unwrap());
        bag.append("20..30".parse().unwrap());
        bag.append("40..50".parse().unwrap());

        bag.remove(&"5..25".parse().unwrap());

        assert_eq!(bag.ranges.len(), 3);
        let mut sorted = bag.ranges.clone();
        sorted.sort();
        assert_eq!(sorted[0].to_string(), "1..4");
        assert_eq!(sorted[1].to_string(), "26..30");
        assert_eq!(sorted[2].to_string(), "40..50");
    }

    #[test]
    fn test_remove_overlapping_ranges() {
        let mut bag = RangeBag::new();
        bag.append("1..100".parse().unwrap());

        bag.remove(&"10..20".parse().unwrap());
        bag.remove(&"30..40".parse().unwrap());
        bag.remove(&"50..60".parse().unwrap());

        assert_eq!(bag.ranges.len(), 4);
        let mut sorted = bag.ranges.clone();
        sorted.sort();
        assert_eq!(sorted[0].to_string(), "1..9");
        assert_eq!(sorted[1].to_string(), "21..29");
        assert_eq!(sorted[2].to_string(), "41..49");
        assert_eq!(sorted[3].to_string(), "61..100");
    }
}
