use std::str::FromStr;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use crate::archiver::range::Height;
use crate::blockchain::block_seq::BlockSequence;
use crate::blockchain::{BlockReference, BlockchainData, BlockchainTypes, EthereumType};
use crate::blockchain::connection::{Blockchain};
use crate::errors::BlockchainError;

///
/// Provides next blocks to archive (basically just for Stream archiving mode)
#[async_trait]
pub trait NextBlock: Send + Sync {
    async fn next_blocks(&self) -> Result<Receiver<Height>, BlockchainError>;
}

///
/// A default implementation that just subscribes to new blocks from the blockchain ("head" subscription)
/// Note that with Head subscription a block may be reorganized later, i.e., a block could be replaced
#[async_trait]
impl NextBlock for Arc<Blockchain> {
    async fn next_blocks(&self) -> Result<Receiver<Height>, BlockchainError> {
        self.subscribe_blocks().await
    }
}

///
/// Re-org aware live follower used by `stream --follow=latest`.
///
/// Wraps a raw head subscription with a [`BlockSequence`] that validates
/// every incoming head event against its parent chain. When the new head's
/// parent isn't already in the sequence, the follower walks backwards by
/// hash (via [`BlockchainData::fetch_block_link`]) until it reconnects to a
/// known ancestor, and then emits the newly-linked heights in chain order
/// (oldest first).
///
/// This catches two cases the dumb follower misses:
///
/// - **Same-height re-org**: a new block arrives at a height we already
///   emitted, with a different hash. The walk-back terminates immediately
///   (its parent is the same as the old block's parent — already in the
///   sequence), and we emit the replacement block so the archiver re-publishes
///   it.
/// - **Deep re-org**: the new head's parent doesn't match anything we have.
///   The walk-back fetches ancestors one by one until it reconnects, and the
///   whole affected suffix gets re-emitted so the archiver overwrites the
///   stale heights.
///
/// Duplicate head events for blocks already in the sequence are silently
/// dropped (the head subscription can re-emit the current tip on
/// reconnections), keeping the live stream idempotent.
pub struct ReorgAwareFollower<B: BlockchainTypes> {
    blockchain: Arc<Blockchain>,
    data_provider: Arc<B::DataProvider>,
    /// Number of recent heights to remember when checking parent linkage.
    /// 64 covers any realistic re-org depth on production chains
    /// (Ethereum finality kicks in at ~64 slots).
    history: usize,
}

impl<B: BlockchainTypes> ReorgAwareFollower<B> {
    pub fn new(blockchain: Arc<Blockchain>, data_provider: Arc<B::DataProvider>) -> Self {
        Self {
            blockchain,
            data_provider,
            history: 64,
        }
    }
}

#[async_trait]
impl<B: BlockchainTypes + 'static> NextBlock for ReorgAwareFollower<B> {
    async fn next_blocks(&self) -> Result<Receiver<Height>, BlockchainError> {
        let mut head = self.blockchain.subscribe_blocks().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let data_provider = self.data_provider.clone();
        let history = self.history;
        tokio::spawn(async move {
            let mut seq = BlockSequence::<B>::new(history);
            while let Some(head_evt) = head.recv().await {
                if let Err(e) = handle_head_event::<B>(
                    &mut seq,
                    &data_provider,
                    head_evt,
                    &tx,
                )
                .await
                {
                    tracing::warn!("Re-org follower error on head event: {:?}", e);
                    // Transient errors shouldn't kill the follower — the next
                    // head event will re-validate the chain on its own.
                }
            }
            tracing::info!("Head subscription ended; re-org follower exiting");
        });
        Ok(rx)
    }
}

/// Process one head event: validate parent linkage, walk back if needed,
/// emit the newly-linked heights in chain order.
async fn handle_head_event<B: BlockchainTypes>(
    seq: &mut BlockSequence<B>,
    data_provider: &Arc<B::DataProvider>,
    head_evt: Height,
    tx: &tokio::sync::mpsc::Sender<Height>,
) -> anyhow::Result<()> {
    // First fetch the head's linkage to learn its parent hash. The head
    // subscription only gives us `(height, hash)`; the parent_hash comes from
    // the block header itself.
    let head_ref: BlockReference<B::BlockHash> = head_evt.clone().into();
    let head_link = data_provider.fetch_block_link(&head_ref).await?;

    // Idempotency: if we've already emitted this exact (height, hash) pair,
    // skip — head subscriptions can re-emit the current tip on reconnects.
    let head_hash_typed = parse_hash::<B>(&head_link.hash)?;
    if seq.get_block(head_link.height, &head_hash_typed).is_some() {
        tracing::trace!(
            "Head event already seen: height={} hash={}",
            head_link.height,
            head_link.hash
        );
        return Ok(());
    }

    // Collect the new chain segment, head-first. We'll reverse before emitting
    // so consumers see the oldest re-archive first.
    let mut to_emit: Vec<Height> = Vec::new();
    let mut cursor = head_link;

    loop {
        let cursor_hash = parse_hash::<B>(&cursor.hash)?;
        let cursor_parent = parse_hash::<B>(&cursor.parent)?;
        to_emit.push(Height {
            height: cursor.height,
            hash: Some(cursor.hash.clone()),
        });
        let missing = seq.append(cursor.height, cursor_parent.clone(), cursor_hash.clone());
        if missing.is_none() {
            // The new segment reconnects to a known ancestor (or this is the
            // first head we've ever seen). We're done.
            break;
        }
        // Parent isn't in the sequence yet. Either this is a deep re-org or a
        // skipped/initial sequence — fetch the parent and continue walking.
        if cursor.height == 0 {
            // Defensive: shouldn't happen with real chains, but stop walking
            // at genesis rather than overflowing.
            tracing::warn!("Re-org walk reached genesis without reconnecting");
            break;
        }
        let parent_ref = BlockReference::Hash(cursor_parent);
        cursor = data_provider.fetch_block_link(&parent_ref).await?;
    }

    // Emit oldest-first so the archiver re-publishes in chain order. Within
    // the broker, same-height re-orgs end up in the same partition (height is
    // the partition key) and consumers see the replacement after the original.
    for h in to_emit.into_iter().rev() {
        if tx.send(h).await.is_err() {
            return Err(anyhow::anyhow!("Receiver dropped; follower exiting"));
        }
    }
    Ok(())
}

fn parse_hash<B: BlockchainTypes>(s: &str) -> anyhow::Result<B::BlockHash> {
    B::BlockHash::from_str(s)
        .map_err(|_| anyhow::anyhow!("Failed to parse block hash: {}", s))
}

///
/// Provides next finalized blocks for Ethereum-like blockchains.
/// Finalized Block is a block that agreed by the majority of the network and extremely unlikely to be replaced (reorganized)
/// On Ethereum Mainnet, a finalized block is about ~7 minutes behind the head block
pub struct NextFinalizedBlock<B: BlockchainTypes> {
    blockchain: Arc<Blockchain>,
    data_provider: Arc<B::DataProvider>
}

impl NextFinalizedBlock<EthereumType> {
    pub fn new(blockchain: Arc<Blockchain>, data_provider: Arc<<EthereumType as BlockchainTypes>::DataProvider>) -> Self {
        Self { blockchain, data_provider }
    }
}

#[async_trait]
impl NextBlock for NextFinalizedBlock<EthereumType> {
    async fn next_blocks(&self) -> Result<Receiver<Height>, BlockchainError> {
        let mut head = self.blockchain.subscribe_blocks().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let data_provider = self.data_provider.clone();
        tokio::spawn(async move {
            let mut last: Option<Height> = None;
            let mut block_seq = BlockSequence::<EthereumType>::new(10);

            while let Some(_height) = head.recv().await {
                let block = data_provider.get_finalized_block_data().await;
                if block.is_err() {
                    tracing::error!("Failed to get finalized height: {:?}", block.err());
                    continue;
                }
                let mut block = block.unwrap();

                // we can get the same block multiple times, so skip those
                if let Some(last_height) = &last {
                    if block.number() <= last_height.height {
                        continue;
                    }
                }

                // Sometimes we need to produce more than one block from this method.
                // It's because we ask for a block only periodically, and we don't know how many just got finalized, so a few could be finalized in between

                // first: remember the last fetched  block
                let mut next = vec![
                    Height { height: block.number(), hash: Some(format!("0x{:x}", block.header.hash)) }
                ];
                // second: add all the block between this and the one loaded the last time
                while let Some(missing) = block_seq.append(block.number(), block.header.parent_hash, block.header.hash) {
                    let missing = data_provider.get_block_data(&missing).await;
                    if missing.is_err() {
                        tracing::error!("Failed to get missing block data: {:?}", missing.err());
                        break;
                    }
                    block = missing.unwrap();
                    next.push(Height { height: block.number(), hash: Some(format!("0x{:x}", block.header.hash)) });
                }

                // third:
                // now produce the blocks, but make sure it's in the correct order
                // i.e., since we were adding missing (=older) blocks at the end, we need to go from the back to the front
                for h in next.iter().rev() {
                    tracing::debug!("Finalized Height: {}", h.height);
                    if let Err(e) = tx.send(h.clone()).await {
                        tracing::error!("Failed to send finalized height: {}", e);
                        break;
                    }
                    last = Some(h.clone());
                }
            }
        });
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockchain::mock::{MockBlock, MockData, MockType};

    /// Build a chain of `count` blocks starting at `from_height` with the
    /// given `hash_prefix`. The first block's parent is `parent_of_first`.
    fn populate_chain(
        data: &MockData,
        from_height: u64,
        count: u64,
        hash_prefix: &str,
        parent_of_first: &str,
    ) {
        let mut parent = parent_of_first.to_string();
        for i in 0..count {
            let height = from_height + i;
            let hash = format!("{}{}", hash_prefix, height);
            data.add_block(MockBlock {
                height,
                hash: hash.clone(),
                parent: parent.clone(),
                transactions: vec![],
            });
            parent = hash;
        }
    }

    async fn drain<T>(rx: &mut tokio::sync::mpsc::Receiver<T>) -> Vec<T> {
        let mut out = Vec::new();
        while let Ok(item) = rx.try_recv() {
            out.push(item);
        }
        out
    }

    /// First head event seen: nothing in the sequence yet, so it should be
    /// emitted as-is without any walk-back.
    #[tokio::test]
    async fn first_head_event_is_emitted_as_is() {
        let data = Arc::new(MockData::new("MOCK"));
        populate_chain(&data, 100, 3, "0xA", "0xroot");
        let mut seq = BlockSequence::<MockType>::new(64);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        handle_head_event::<MockType>(
            &mut seq,
            &data,
            Height { height: 100, hash: Some("0xA100".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);
        let emitted = drain(&mut rx).await;
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].height, 100);
        assert_eq!(emitted[0].hash.as_deref(), Some("0xA100"));
    }

    /// Sequential head events: each new head's parent matches the previous
    /// head's hash. Each one is emitted on its own with no walk-back.
    #[tokio::test]
    async fn sequential_heads_emit_one_at_a_time() {
        let data = Arc::new(MockData::new("MOCK"));
        populate_chain(&data, 100, 3, "0xA", "0xroot");
        let mut seq = BlockSequence::<MockType>::new(64);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        for h in 100..103 {
            handle_head_event::<MockType>(
                &mut seq,
                &data,
                Height { height: h, hash: Some(format!("0xA{}", h)) },
                &tx,
            )
            .await
            .unwrap();
        }
        drop(tx);
        let emitted = drain(&mut rx).await;
        let names: Vec<_> = emitted
            .iter()
            .map(|h| (h.height, h.hash.clone().unwrap()))
            .collect();
        assert_eq!(
            names,
            vec![
                (100, "0xA100".to_string()),
                (101, "0xA101".to_string()),
                (102, "0xA102".to_string()),
            ]
        );
    }

    /// Same-height re-org: after seeing block A at height 102, a new block B
    /// arrives at the same height with the same parent. Only B should be
    /// emitted (no walk-back beyond the existing parent which is already in
    /// the sequence).
    #[tokio::test]
    async fn same_height_reorg_emits_replacement_only() {
        let data = Arc::new(MockData::new("MOCK"));
        // Original chain: 100 (A100) → 101 (A101) → 102 (A102).
        populate_chain(&data, 100, 3, "0xA", "0xroot");
        // Re-org variant of 102 with a different hash but same parent (A101).
        data.add_block(MockBlock {
            height: 102,
            hash: "0xB102".to_string(),
            parent: "0xA101".to_string(),
            transactions: vec![],
        });

        let mut seq = BlockSequence::<MockType>::new(64);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        for h in 100..103 {
            handle_head_event::<MockType>(
                &mut seq,
                &data,
                Height { height: h, hash: Some(format!("0xA{}", h)) },
                &tx,
            )
            .await
            .unwrap();
        }
        // Now the re-org head arrives.
        handle_head_event::<MockType>(
            &mut seq,
            &data,
            Height { height: 102, hash: Some("0xB102".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);

        let emitted = drain(&mut rx).await;
        let tail: Vec<_> = emitted
            .iter()
            .rev()
            .take(1)
            .map(|h| (h.height, h.hash.clone().unwrap()))
            .collect();
        assert_eq!(tail, vec![(102, "0xB102".to_string())]);
    }

    /// Deep re-org: after seeing chain A up to height 103, a new chain B
    /// arrives at height 105 whose parent (B104) isn't in the sequence. The
    /// follower must walk back via fetch_block_link until it reconnects to a
    /// common ancestor (here at height 101, where B102's parent A101 matches
    /// the existing block A101), then emit the new B-tail oldest-first.
    #[tokio::test]
    async fn deep_reorg_walks_back_and_emits_new_tail_in_order() {
        let data = Arc::new(MockData::new("MOCK"));
        // Original A-chain: 100..=103, all with prefix "0xA".
        populate_chain(&data, 100, 4, "0xA", "0xroot");
        // B-chain forks off A101: 102..=105 with prefix "0xB", parent of B102 = A101.
        populate_chain(&data, 102, 4, "0xB", "0xA101");

        let mut seq = BlockSequence::<MockType>::new(64);
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        // Replay the A-chain into the sequence.
        for h in 100..=103 {
            handle_head_event::<MockType>(
                &mut seq,
                &data,
                Height { height: h, hash: Some(format!("0xA{}", h)) },
                &tx,
            )
            .await
            .unwrap();
        }
        let _ = drain(&mut rx).await; // discard original emissions

        // Now the new head jumps straight to B105.
        handle_head_event::<MockType>(
            &mut seq,
            &data,
            Height { height: 105, hash: Some("0xB105".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);

        let emitted = drain(&mut rx).await;
        let names: Vec<_> = emitted
            .iter()
            .map(|h| (h.height, h.hash.clone().unwrap()))
            .collect();
        // Re-org tail: B102, B103, B104, B105 in chain order.
        assert_eq!(
            names,
            vec![
                (102, "0xB102".to_string()),
                (103, "0xB103".to_string()),
                (104, "0xB104".to_string()),
                (105, "0xB105".to_string()),
            ]
        );
    }

    /// Idempotency: a head event for a block already in the sequence (e.g.,
    /// the broker re-sending the current tip on reconnect) is silently
    /// dropped.
    #[tokio::test]
    async fn duplicate_head_event_is_silently_dropped() {
        let data = Arc::new(MockData::new("MOCK"));
        populate_chain(&data, 100, 2, "0xA", "0xroot");

        let mut seq = BlockSequence::<MockType>::new(64);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        handle_head_event::<MockType>(
            &mut seq,
            &data,
            Height { height: 100, hash: Some("0xA100".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        // Re-emit the same head.
        handle_head_event::<MockType>(
            &mut seq,
            &data,
            Height { height: 100, hash: Some("0xA100".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);

        let emitted = drain(&mut rx).await;
        assert_eq!(emitted.len(), 1, "duplicate head should not re-emit");
    }
}
