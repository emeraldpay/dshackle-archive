use std::ops::Add;
use crate::range::Range;

///
/// Multiple ranges
pub struct RangeBag {
    ranges: Vec<Range>,
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
}
