use crate::blockchain::BlockchainTypes;


#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockLink<T: BlockchainTypes> {
    pub parent: T::BlockHash,
    pub current: T::BlockHash,
}

impl<T: BlockchainTypes> BlockLink<T> {
    pub fn is_after(&self, other: &BlockLink<T>) -> bool {
        self.parent == other.current
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AtHeight<T: BlockchainTypes> {
    height: u64,
    blocks: Vec<BlockLink<T>>
}

impl<T: BlockchainTypes> AtHeight<T> {
    pub fn find_block(&self, hash: &T::BlockHash) -> Option<&BlockLink<T>> {
        self.blocks.iter().find(|b| &b.current == hash)
    }
}

///
/// Keeps blocks in order. Order is defined by block parent hashes, not only by heights.
/// That allows reorgs, forks, etc. But we always know the correct sequence to the specified block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSequence<T: BlockchainTypes> {
    size: usize,
    current: Vec<AtHeight<T>>,
}

impl<T: BlockchainTypes> BlockSequence<T> {

    ///
    /// @param size Maximum number of heights to keep in memory
    pub fn new(size: usize) -> Self {
        Self {
            size,
            current: Vec::new(),
        }
    }

    ///
    /// Appends a new block link at the given height.
    /// @return If the block is not linked to previous blocks, returns the hash of a missing block
    pub fn append(&mut self, height: u64, prev: T::BlockHash, current: T::BlockHash) -> Option<T::BlockHash> {
        if let Some(at_height) = self.current.iter_mut().find(|h| h.height == height) {
            at_height.blocks.push(BlockLink { parent: prev.clone(), current: current.clone() });
        } else {
            self.current.push(AtHeight {
                height,
                blocks: vec![BlockLink { parent: prev.clone(), current: current.clone() }],
            });
        }
        self.reorder();
        if !self.is_linked(height, &current) {
            Some(prev.clone())
        } else {
            None
        }
    }

    pub fn is_linked(&self, height: u64, hash: &T::BlockHash) -> bool {
        self.get_linked(height, hash).is_some()
    }

    pub fn get_block(&self, height: u64, hash: &T::BlockHash) -> Option<&BlockLink<T>> {
        match self.get_index(height) {
            None => None,
            Some(pos) => {
                self.current[pos].find_block(hash)
            }
        }
    }

    ///
    /// Check if the given block has the parent (or it's the first block in sequence)
    pub fn get_linked(&self, height: u64, hash: &T::BlockHash) -> Option<&BlockLink<T>> {
        match self.get_index(height) {
            None => None,
            Some(pos) => {
                let block = self.current[pos].find_block(hash);
                if pos == 0 {
                    block
                } else {
                    if block.is_none() {
                        return None;
                    }
                    let block = block.unwrap();
                    let prev_height = &self.current[pos - 1];
                    let has_prev = prev_height.blocks
                        .iter().any(|prev| {
                        block.is_after(prev)
                    });
                    if !has_prev {
                        return None;
                    }
                    Some(block)
                }
            }
        }
    }

    ///
    /// Get the sequence of linked blocks up to the specified.
    /// May be shorter that the whole contained sequence of height, if one of the blocks is
    /// missing and so break the chain
    pub fn up_to<'a>(&'a self, height: u64, hash: &'a T::BlockHash) -> Vec<(u64, &'a T::BlockHash)> {
        let mut linked = vec![];
        let top = self.get_index(height);
        if top.is_none() {
            return vec![];
        }
        let mut current = (height, hash);
        // linked.push(current);
        loop {
            let link = self.get_block(current.0, &current.1);
            if link.is_none() {
                return linked;
            }
            linked.push(current);
            let link = link.unwrap();
            // linked.push((height, &link.current));
            current = (current.0 - 1, &link.parent);
        }
    }

    pub fn get_head(&self) -> Option<(u64, &T::BlockHash)> {
        if self.current.is_empty() {
            return None;
        }
        let top = self.current.last().unwrap();
        if top.blocks.len() != 1 {
            return None;
        }
        Some((top.height, &top.blocks.first().unwrap().current))
    }

    ///
    /// Finds the index of the given height in the current heights, if any.
    fn get_index(&self, height: u64) -> Option<usize> {
        self.current.iter().position(|h| h.height == height)
    }

    ///
    /// Makes sure the current values are in order (by height) and there is no missing height.
    /// If it finds any missing height, it sets a value with empty blocks for that height.
    fn reorder(&mut self) {
        self.current.sort_by_key(|h| h.height);
        let mut expected_height = if let Some(first) = self.current.first() {
            first.height
        } else {
            return;
        };
        let mut i = 0;
        while i < self.current.len() {
            let height = self.current[i].height;
            if height > expected_height {
                for h in expected_height..height {
                    self.current.insert(i, AtHeight {
                        height: h,
                        blocks: Vec::new(),
                    });
                }
            }
            expected_height += 1;
            i += 1;
        }
        while self.current.len() > self.size {
            self.current.remove(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockchain::mock::MockType;

    #[test]
    fn is_after() {
        let a = BlockLink::<MockType> { parent: "parent_hash".to_string(), current: "a_hash".to_string() };
        let b = BlockLink::<MockType> { parent: "x".to_string(), current: "parent_hash".to_string() };
        assert!(a.is_after(&b));
    }

    #[test]
    fn is_not_after() {
        let a = BlockLink::<MockType> { parent: "p1".to_string(), current: "c1".to_string() };
        let b = BlockLink::<MockType> { parent: "p2".to_string(), current: "c2".to_string() };
        assert!(!a.is_after(&b));
    }

    #[test]
    fn find_block_exists() {
        let link1 = BlockLink::<MockType> { parent: "p1".to_string(), current: "c1".to_string() };
        let link2 = BlockLink::<MockType> { parent: "p2".to_string(), current: "c2".to_string() };
        let at = AtHeight::<MockType> { height: 42, blocks: vec![link1.clone(), link2.clone()] };
        let found = at.find_block(&"c2".to_string());
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.current, "c2");
        assert_eq!(found.parent, "p2");
    }

    #[test]
    fn find_block_none() {
        let link = BlockLink::<MockType> { parent: "p".to_string(), current: "c".to_string() };
        let at = AtHeight::<MockType> { height: 1, blocks: vec![link] };
        let found = at.find_block(&"missing".to_string());
        assert!(found.is_none());
    }

    #[test]
    fn append_and_find() {
        let mut seq = BlockSequence::<MockType>::new(3);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(2, "111".to_string(), "222".to_string());
        assert_eq!(seq.current.len(), 2);
        let found = seq.current[0].find_block(&"111".to_string());
        assert!(found.is_some());
        let found2 = seq.current[1].find_block(&"222".to_string());
        assert!(found2.is_some());
    }

    #[test]
    fn is_linked() {
        let mut seq = BlockSequence::<MockType>::new(3);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(2, "111".to_string(), "222".to_string());
        assert!(seq.is_linked(2, &"222".to_string()));
    }

    #[test]
    fn not_linked_on_missing_block() {
        let mut seq = BlockSequence::<MockType>::new(3);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(2, "111".to_string(), "222".to_string());
        assert!(!seq.is_linked(2, &"missing".to_string()));
    }

    #[test]
    fn is_linked_first_block() {
        let mut seq = BlockSequence::<MockType>::new(3);
        seq.append(1, "000".to_string(), "111".to_string());
        assert!(seq.is_linked(1, &"111".to_string()));
    }

    #[test]
    fn order() {
        let mut seq = BlockSequence::<MockType>::new(2);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(3, "222".to_string(), "333".to_string());
        seq.append(2, "111".to_string(), "222".to_string());
        seq.append(4, "333".to_string(), "444".to_string());
        seq.append(4, "333".to_string(), "444-2".to_string());
        // Should only keep last 2 heights
        assert_eq!(seq.current.len(), 2);
        assert_eq!(seq.current[0].height, 3);
        assert_eq!(seq.current[1].height, 4);
    }

    #[test]
    fn get_head_empty() {
        let seq = BlockSequence::<MockType>::new(3);
        assert_eq!(seq.get_head(), None);
    }

    #[test]
    fn get_head_multiple_blocks() {
        let mut seq = BlockSequence::<MockType>::new(3);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(1, "000".to_string(), "112".to_string()); // fork at height 1
        assert_eq!(seq.get_head(), None);
    }

    #[test]
    fn get_head_single_block() {
        let mut seq = BlockSequence::<MockType>::new(3);
        seq.append(1, "000".to_string(), "111".to_string());
        let head = seq.get_head();
        assert_eq!(head, Some((1, &"111".to_string())));
    }

    #[test]
    fn get_head_last_height_single_block() {
        let mut seq = BlockSequence::<MockType>::new(3);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(2, "111".to_string(), "222".to_string());
        let head = seq.get_head();
        assert_eq!(head, Some((2, &"222".to_string())));
    }

    #[test]
    fn get_head_last_height_multiple_blocks() {
        let mut seq = BlockSequence::<MockType>::new(3);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(2, "111".to_string(), "222".to_string());
        seq.append(3, "222".to_string(), "333".to_string());
        let head = seq.get_head();
        assert_eq!(head, Some((3, &"333".to_string())));
    }

    #[test]
    fn up_to_height_not_present() {
        let seq = BlockSequence::<MockType>::new(3);
        let hash = "111".to_string();
        let result = seq.up_to(1, &hash);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn up_to_first_block() {
        let mut seq = BlockSequence::<MockType>::new(3);
        seq.append(1, "000".to_string(), "111".to_string());
        let hash = "111".to_string();
        let result = seq.up_to(1, &hash);
        assert_eq!(result, vec![(1, &"111".to_string())]);
    }

    #[test]
    fn up_to_linear_chain() {
        let mut seq = BlockSequence::<MockType>::new(5);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(2, "111".to_string(), "222".to_string());
        seq.append(3, "222".to_string(), "333".to_string());
        let hash = "333".to_string();
        let result = seq.up_to(3, &hash);
        assert_eq!(result, vec![(3, &"333".to_string()), (2, &"222".to_string()), (1, &"111".to_string())]);
    }

    #[test]
    fn up_to_partial_chain_missing_link() {
        let mut seq = BlockSequence::<MockType>::new(5);
        seq.append(1, "000".to_string(), "111".to_string());
        // skip height 2
        seq.append(3, "222".to_string(), "333".to_string());
        let hash = "333".to_string();
        let result = seq.up_to(3, &hash);
        // Should only return the starting block, as the parent is missing
        assert_eq!(result, vec![(3, &"333".to_string())]);
    }

    #[test]
    fn up_to_fork_chain() {
        let mut seq = BlockSequence::<MockType>::new(5);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(2, "111".to_string(), "222a".to_string());
        seq.append(2, "111".to_string(), "222b".to_string());
        seq.append(3, "222b".to_string(), "333".to_string());
        let hash = "333".to_string();
        let result = seq.up_to(3, &hash);

        assert_eq!(result, vec![(3, &"333".to_string()), (2, &"222b".to_string()), (1, &"111".to_string())]);
    }

    #[test]
    fn up_to_missing_parent() {
        let mut seq = BlockSequence::<MockType>::new(5);
        seq.append(1, "000".to_string(), "111".to_string());
        seq.append(2, "111".to_string(), "222".to_string());
        seq.append(3, "999".to_string(), "333".to_string()); // wrong parent
        let hash = "333".to_string();
        let result = seq.up_to(3, &hash);
        // Should only return the starting block, as the parent is not linked
        assert_eq!(result, vec![(3, &"333".to_string())]);
    }

}
