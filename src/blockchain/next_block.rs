// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use crate::archiver::range::Height;
use crate::blockchain::block_seq::BlockSequence;
use crate::blockchain::{BlockHeaderInfo, BlockReference, BlockchainData, BlockchainTypes, EthereumType};
use crate::blockchain::connection::{Blockchain};
use crate::errors::BlockchainError;

/// One unit of work emitted by a [`NextBlock`] pump: which block to archive,
/// plus a token the pump can fire to tell the archiver "abandon this one,
/// the chain moved on".
///
/// Pumps that operate on settled blocks ([`NextFinalizedBlock`], historical
/// archive) emit jobs with a fresh, never-fired [`CancellationToken`] — the
/// token field stays uniform across all pump kinds so the archiver loop
/// doesn't have to branch on the source. Only [`ReorgAwareFollower`]
/// actually fires tokens, when a same-height re-org or deep re-org
/// invalidates a previously-emitted height.
#[derive(Clone, Debug)]
pub struct BlockJob {
    pub height: Height,
    pub cancel: CancellationToken,
}

impl BlockJob {
    /// Convenience for non-cancelling pumps and tests: wrap a height with a
    /// fresh token that will never fire.
    pub fn untracked(height: Height) -> Self {
        Self {
            height,
            cancel: CancellationToken::new(),
        }
    }
}

///
/// Provides next blocks to archive (basically just for Stream archiving mode)
#[async_trait]
pub trait NextBlock: Send + Sync {
    async fn next_blocks(&self) -> Result<Receiver<BlockJob>, BlockchainError>;
}

///
/// A default implementation that just subscribes to new blocks from the blockchain ("head" subscription).
/// Note that with a Head subscription a block may be reorganized later, i.e., a block could be replaced;
/// this pump does not detect that — every emission carries an untracked token. For re-org awareness,
/// wrap this with [`ReorgAwareFollower`].
#[async_trait]
impl NextBlock for Arc<Blockchain> {
    async fn next_blocks(&self) -> Result<Receiver<BlockJob>, BlockchainError> {
        let mut heights = self.subscribe_blocks().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(h) = heights.recv().await {
                if tx.send(BlockJob::untracked(h)).await.is_err() {
                    break;
                }
            }
        });
        Ok(rx)
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
/// Per-height [`CancellationToken`]s are tracked alongside the live block
/// sequence: when a height is re-emitted with a different hash the prior
/// token is cancelled before the replacement [`BlockJob`] goes out, so the
/// archiver task that's still fetching the doomed block can drop its
/// in-flight RPCs and abandon any partially-written ordered sink.
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
    async fn next_blocks(&self) -> Result<Receiver<BlockJob>, BlockchainError> {
        let mut head = self.blockchain.subscribe_blocks().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let data_provider = self.data_provider.clone();
        let history = self.history;
        tokio::spawn(async move {
            let mut seq = BlockSequence::<B>::new(history);
            // Per-height token bookkeeping. Bounded to `history` entries; oldest
            // evicted on overflow. Storing the hash alongside the token lets us
            // distinguish "same block, retry" from "different block, re-org".
            let mut live_tokens: BTreeMap<u64, (String, CancellationToken)> = BTreeMap::new();
            while let Some(head_evt) = head.recv().await {
                if let Err(e) = handle_head_event::<B>(
                    &mut seq,
                    &mut live_tokens,
                    history,
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

/// Upper bound on a single linkage fetch during the re-org walk.
///
/// The follower must never block indefinitely: it is the component that
/// detects re-orgs and fires the cancellation tokens, so nothing can cancel
/// *it*, and a hung fetch would also block the head event that announces the
/// replacement. The chain providers keep their `fetch_block_link` retries
/// bounded for the same reason; this timeout enforces the invariant for any
/// provider that inherits the trait's default, policy-driven implementation.
const LINK_FETCH_TIMEOUT: Duration = Duration::from_secs(60);

/// [`BlockchainData::fetch_block_link`] wrapped in [`LINK_FETCH_TIMEOUT`].
async fn fetch_link_bounded<B: BlockchainTypes>(
    data_provider: &Arc<B::DataProvider>,
    reference: &BlockReference<B::BlockHash>,
) -> anyhow::Result<BlockHeaderInfo> {
    tokio::time::timeout(LINK_FETCH_TIMEOUT, data_provider.fetch_block_link(reference))
        .await
        .map_err(|_| {
            let what = match reference {
                BlockReference::Hash(hash) => format!("{:?}", hash),
                BlockReference::Height(h) => format!("height {}", h.height),
            };
            anyhow::anyhow!("Timed out fetching block linkage for {}", what)
        })?
}

/// Process one head event: validate parent linkage, walk back if needed,
/// emit the newly-linked heights in chain order — cancelling any prior
/// token at a height that's being replaced.
///
/// The walk-back is *atomic with respect to the sequence*: parents are
/// fetched into a local buffer and the sequence is only mutated after the
/// chain has been fully linked. A mid-walk `fetch_block_link` failure
/// returns Err with both `seq` and `live_tokens` unchanged, so the next
/// head event can re-validate from scratch instead of inheriting a
/// partial state.
async fn handle_head_event<B: BlockchainTypes>(
    seq: &mut BlockSequence<B>,
    live_tokens: &mut BTreeMap<u64, (String, CancellationToken)>,
    history: usize,
    data_provider: &Arc<B::DataProvider>,
    head_evt: Height,
    tx: &tokio::sync::mpsc::Sender<BlockJob>,
) -> anyhow::Result<()> {
    // First fetch the head's linkage to learn its parent hash. The head
    // subscription only gives us `(height, hash)`; the parent_hash comes from
    // the block header itself.
    let head_ref: BlockReference<B::BlockHash> = head_evt.clone().into();
    let head_link = fetch_link_bounded::<B>(data_provider, &head_ref).await?;

    // Idempotency: skip only when the latest *live* hash at this height
    // matches the incoming one — i.e., this is the broker re-emitting a
    // block we already acted on. A different hash at the same height —
    // including a revert back to a previously-canonical block — must NOT
    // short-circuit; `seq.get_block` is unsafe for this check because
    // `AtHeight.blocks` accumulates and matches any prior emission at this
    // height, silently dropping an A→B→A reversion.
    if live_tokens
        .get(&head_link.height)
        .map(|(existing_hash, _)| existing_hash == &head_link.hash)
        .unwrap_or(false)
    {
        tracing::trace!(
            "Head event already seen: height={} hash={}",
            head_link.height,
            head_link.hash
        );
        return Ok(());
    }

    // Walk parents into a local buffer without touching `seq`. The walk
    // stops when:
    //   - the parent is already in `seq` at `height - 1` (we've reconnected
    //     to the known chain),
    //   - `seq` is empty (this is the first head we've ever seen — there
    //     are no ancestors to walk back to),
    //   - or we hit genesis (defensive — shouldn't happen on real chains).
    let mut walked: Vec<BlockHeaderInfo> = Vec::new();
    let mut cursor = head_link;
    loop {
        // Parse both hashes eagerly so we don't fail later during commit
        // (which would leave the sequence partially mutated).
        let cursor_parent_typed = parse_hash::<B>(&cursor.parent)?;
        let _ = parse_hash::<B>(&cursor.hash)?;
        let parent_height = cursor.height.saturating_sub(1);
        let reconnected = cursor.height == 0
            || seq.is_empty()
            || seq
                .get_block(parent_height, &cursor_parent_typed)
                .is_some();
        let cursor_height = cursor.height;
        walked.push(cursor);
        if reconnected {
            if cursor_height == 0 {
                tracing::warn!("Re-org walk reached genesis without reconnecting");
            }
            break;
        }
        let parent_ref = BlockReference::Hash(cursor_parent_typed);
        cursor = fetch_link_bounded::<B>(data_provider, &parent_ref).await?;
    }

    // Walk fully succeeded — now commit. Trim BEFORE inserts so a deep
    // walk-back never evicts entries we're about to add this round. We use
    // `pop_first` for one tree descent per eviction instead of
    // `keys().next() + remove`.
    while live_tokens.len() > history {
        if live_tokens.pop_first().is_none() {
            break;
        }
    }

    // Commit chain segments to `seq` in chain order (oldest-first), so the
    // linkage check inside `seq.append` always sees parents before children.
    for link in walked.iter().rev() {
        let parent_h = parse_hash::<B>(&link.parent)?;
        let hash_h = parse_hash::<B>(&link.hash)?;
        seq.append(link.height, parent_h, hash_h);
    }

    // Emit oldest-first so the archiver re-publishes in chain order. For each
    // height, decide whether this is a fresh emission (no prior token) or a
    // re-org replacement (prior token at this height with a different hash —
    // cancel it before issuing the new job).
    for link in walked.into_iter().rev() {
        let token = match live_tokens.entry(link.height) {
            Entry::Occupied(mut e) => {
                let (existing_hash, existing_token) = e.get();
                if existing_hash == &link.hash {
                    // Unreachable in practice — the idempotency check at the
                    // top returns Ok for identical (height, hash). If we ever
                    // get here, reuse the existing token rather than minting
                    // a new one that would orphan the in-flight archive.
                    existing_token.clone()
                } else {
                    tracing::info!(
                        height = link.height,
                        previous_hash = %existing_hash,
                        new_hash = %link.hash,
                        "Re-org: cancelling prior block at this height"
                    );
                    existing_token.cancel();
                    let new_token = CancellationToken::new();
                    *e.get_mut() = (link.hash.clone(), new_token.clone());
                    new_token
                }
            }
            Entry::Vacant(e) => {
                let new_token = CancellationToken::new();
                e.insert((link.hash.clone(), new_token.clone()));
                new_token
            }
        };
        let job = BlockJob {
            height: Height {
                height: link.height,
                hash: Some(link.hash),
            },
            cancel: token,
        };
        if tx.send(job).await.is_err() {
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
    async fn next_blocks(&self) -> Result<Receiver<BlockJob>, BlockchainError> {
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
                    // Finalized blocks are settled by definition — no re-org
                    // signal applies, so every job carries an untracked token.
                    if let Err(e) = tx.send(BlockJob::untracked(h.clone())).await {
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
        let mut live_tokens = BTreeMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        handle_head_event::<MockType>(
            &mut seq,
            &mut live_tokens,
            64,
            &data,
            Height { height: 100, hash: Some("0xA100".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);
        let emitted = drain(&mut rx).await;
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].height.height, 100);
        assert_eq!(emitted[0].height.hash.as_deref(), Some("0xA100"));
        assert!(!emitted[0].cancel.is_cancelled());
    }

    /// Sequential head events: each new head's parent matches the previous
    /// head's hash. Each one is emitted on its own with no walk-back.
    #[tokio::test]
    async fn sequential_heads_emit_one_at_a_time() {
        let data = Arc::new(MockData::new("MOCK"));
        populate_chain(&data, 100, 3, "0xA", "0xroot");
        let mut seq = BlockSequence::<MockType>::new(64);
        let mut live_tokens = BTreeMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        for h in 100..103 {
            handle_head_event::<MockType>(
                &mut seq,
                &mut live_tokens,
                64,
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
            .map(|j| (j.height.height, j.height.hash.clone().unwrap()))
            .collect();
        assert_eq!(
            names,
            vec![
                (100, "0xA100".to_string()),
                (101, "0xA101".to_string()),
                (102, "0xA102".to_string()),
            ]
        );
        // None of the unique heights should have cancelled tokens.
        assert!(emitted.iter().all(|j| !j.cancel.is_cancelled()));
    }

    /// Same-height re-org: after seeing block A at height 102, a new block B
    /// arrives at the same height with the same parent. The original A's
    /// token must be cancelled before B is emitted with its own fresh token.
    #[tokio::test]
    async fn same_height_reorg_cancels_prior_token_and_emits_replacement() {
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
        let mut live_tokens = BTreeMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        for h in 100..103 {
            handle_head_event::<MockType>(
                &mut seq,
                &mut live_tokens,
                64,
                &data,
                Height { height: h, hash: Some(format!("0xA{}", h)) },
                &tx,
            )
            .await
            .unwrap();
        }
        // Capture the token issued for A102 — this is the one we expect the
        // re-org to fire.
        let a102_token = live_tokens.get(&102).unwrap().1.clone();
        assert!(!a102_token.is_cancelled());

        // Now the re-org head arrives.
        handle_head_event::<MockType>(
            &mut seq,
            &mut live_tokens,
            64,
            &data,
            Height { height: 102, hash: Some("0xB102".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);

        // The original A102 token must now be cancelled.
        assert!(a102_token.is_cancelled(), "prior block token at height 102 must be cancelled on re-org");

        // Last emission should be B102 with a fresh, uncancelled token.
        let emitted = drain(&mut rx).await;
        let last = emitted.last().unwrap();
        assert_eq!(last.height.height, 102);
        assert_eq!(last.height.hash.as_deref(), Some("0xB102"));
        assert!(!last.cancel.is_cancelled());
    }

    /// Deep re-org: heights 102..105 of the A-chain are replaced by a new
    /// B-chain. The follower must cancel the prior token at each replaced
    /// height (here only 102 and 103 had prior tokens — 104/105 are new).
    #[tokio::test]
    async fn deep_reorg_walks_back_and_emits_new_tail_in_order() {
        let data = Arc::new(MockData::new("MOCK"));
        // Original A-chain: 100..=103, all with prefix "0xA".
        populate_chain(&data, 100, 4, "0xA", "0xroot");
        // B-chain forks off A101: 102..=105 with prefix "0xB", parent of B102 = A101.
        populate_chain(&data, 102, 4, "0xB", "0xA101");

        let mut seq = BlockSequence::<MockType>::new(64);
        let mut live_tokens = BTreeMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        // Replay the A-chain into the sequence.
        for h in 100..=103 {
            handle_head_event::<MockType>(
                &mut seq,
                &mut live_tokens,
                64,
                &data,
                Height { height: h, hash: Some(format!("0xA{}", h)) },
                &tx,
            )
            .await
            .unwrap();
        }
        let a102_token = live_tokens.get(&102).unwrap().1.clone();
        let a103_token = live_tokens.get(&103).unwrap().1.clone();
        let _ = drain(&mut rx).await; // discard original emissions

        // Now the new head jumps straight to B105.
        handle_head_event::<MockType>(
            &mut seq,
            &mut live_tokens,
            64,
            &data,
            Height { height: 105, hash: Some("0xB105".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);

        // Both pre-existing tokens at the re-orged heights must be cancelled.
        assert!(a102_token.is_cancelled(), "A102 token must be cancelled on deep re-org");
        assert!(a103_token.is_cancelled(), "A103 token must be cancelled on deep re-org");

        let emitted = drain(&mut rx).await;
        let names: Vec<_> = emitted
            .iter()
            .map(|j| (j.height.height, j.height.hash.clone().unwrap()))
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
        // The replacements should all have fresh, uncancelled tokens.
        assert!(emitted.iter().all(|j| !j.cancel.is_cancelled()));
    }

    /// A→B→A revert: a chain that re-orgs from A to B and then back to A
    /// must re-emit A so consumers learn the chain reverted. Previously the
    /// `seq.get_block` idempotency check matched any historical hash at the
    /// height, silently swallowing the reversion.
    #[tokio::test]
    async fn revert_back_to_previous_block_is_re_emitted() {
        let data = Arc::new(MockData::new("MOCK"));
        // Both A102 and B102 share parent A101 — A and B are siblings at
        // height 102.
        populate_chain(&data, 100, 3, "0xA", "0xroot");
        data.add_block(MockBlock {
            height: 102,
            hash: "0xB102".to_string(),
            parent: "0xA101".to_string(),
            transactions: vec![],
        });

        let mut seq = BlockSequence::<MockType>::new(64);
        let mut live_tokens = BTreeMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);

        // Emit A100..A102, then re-org to B102, then revert back to A102.
        for h in 100..103 {
            handle_head_event::<MockType>(
                &mut seq,
                &mut live_tokens,
                64,
                &data,
                Height { height: h, hash: Some(format!("0xA{}", h)) },
                &tx,
            )
            .await
            .unwrap();
        }
        handle_head_event::<MockType>(
            &mut seq,
            &mut live_tokens,
            64,
            &data,
            Height { height: 102, hash: Some("0xB102".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        // Capture B102's token — when A returns it should be cancelled.
        let b102_token = live_tokens.get(&102).unwrap().1.clone();
        assert!(!b102_token.is_cancelled());

        // Chain reverts to A102.
        handle_head_event::<MockType>(
            &mut seq,
            &mut live_tokens,
            64,
            &data,
            Height { height: 102, hash: Some("0xA102".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);

        assert!(
            b102_token.is_cancelled(),
            "B102 token must be cancelled when the chain reverts back to A"
        );

        let emitted = drain(&mut rx).await;
        let last = emitted.last().unwrap();
        assert_eq!(last.height.height, 102);
        assert_eq!(
            last.height.hash.as_deref(),
            Some("0xA102"),
            "A102 must be re-emitted on reversion"
        );
        assert!(!last.cancel.is_cancelled());
        // The latest live hash at height 102 must reflect A again.
        assert_eq!(live_tokens.get(&102).unwrap().0, "0xA102");
    }

    /// Walk-back atomicity: if fetching an ancestor fails partway through a
    /// deep re-org walk, neither `seq` nor `live_tokens` should be mutated.
    /// The next head event must be able to re-validate from scratch.
    #[tokio::test]
    async fn walk_back_fetch_failure_leaves_seq_untouched() {
        let data = Arc::new(MockData::new("MOCK"));
        // Seed the original chain so the follower has something to re-org
        // against.
        populate_chain(&data, 100, 2, "0xA", "0xroot");

        let mut seq = BlockSequence::<MockType>::new(64);
        let mut live_tokens = BTreeMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);

        for h in 100..102 {
            handle_head_event::<MockType>(
                &mut seq,
                &mut live_tokens,
                64,
                &data,
                Height { height: h, hash: Some(format!("0xA{}", h)) },
                &tx,
            )
            .await
            .unwrap();
        }
        // Snapshot bookkeeping BEFORE the doomed walk. CancellationToken has
        // no PartialEq, so we compare the (height, hash) pairs that drive
        // the bookkeeping correctness invariant.
        let snapshot = |t: &BTreeMap<u64, (String, CancellationToken)>| -> Vec<(u64, String)> {
            t.iter().map(|(h, (hash, _))| (*h, hash.clone())).collect()
        };
        let live_tokens_before = snapshot(&live_tokens);
        // Discard the original emissions.
        let _ = drain(&mut rx).await;

        // Add only the TOP of a B-chain to MockData: the walk-back from B105
        // will succeed for B105 but fail at B104 (never added). The middle
        // ancestors don't exist as far as the data provider is concerned.
        data.add_block(MockBlock {
            height: 105,
            hash: "0xB105".to_string(),
            parent: "0xB104".to_string(),
            transactions: vec![],
        });
        let result = handle_head_event::<MockType>(
            &mut seq,
            &mut live_tokens,
            64,
            &data,
            Height { height: 105, hash: Some("0xB105".to_string()) },
            &tx,
        )
        .await;
        assert!(result.is_err(), "walk-back must fail when an ancestor is unfetchable");

        // `seq` and `live_tokens` must reflect ONLY the pre-walk A-chain
        // state — none of the doomed B-chain heights leaked in.
        assert!(
            seq.get_block(105, &"0xB105".to_string()).is_none(),
            "B105 must not be committed to seq after a failed walk-back"
        );
        assert_eq!(
            snapshot(&live_tokens),
            live_tokens_before,
            "live_tokens must be unchanged after a failed walk-back"
        );
        drop(tx);
        let emitted = drain(&mut rx).await;
        assert!(
            emitted.is_empty(),
            "no BlockJob should be emitted from a failed walk-back"
        );
    }

    /// Deep walk-back trim: when the segment to emit exceeds `history`, the
    /// trim must NOT evict tokens for jobs we're about to install in the same
    /// call. Previously the trim ran AFTER the emit loop and could drop the
    /// oldest entries even though they were just added.
    #[tokio::test]
    async fn deep_walk_back_does_not_evict_just_inserted_tokens() {
        let data = Arc::new(MockData::new("MOCK"));
        // 30 sequential A-blocks; we'll pre-warm with 100..104 then trigger
        // a walk-back from height 129 that walks 105..129 (25 heights).
        populate_chain(&data, 100, 30, "0xA", "0xroot");

        let mut seq = BlockSequence::<MockType>::new(64);
        let mut live_tokens = BTreeMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        // Pre-warm: emit 100..104 as separate events so seq isn't empty when
        // the deep event arrives (otherwise the walk-back short-circuits on
        // the first iteration because there's nothing to reconnect to).
        for h in 100..=104 {
            handle_head_event::<MockType>(
                &mut seq,
                &mut live_tokens,
                8,
                &data,
                Height { height: h, hash: Some(format!("0xA{}", h)) },
                &tx,
            )
            .await
            .unwrap();
        }
        let _ = drain(&mut rx).await; // discard the warmup emissions

        // Jump to height 129. Walk-back must traverse 128..105 by parent
        // hash, reconnecting at A104 (the youngest entry in seq). That's a
        // 25-height segment with `history=8` — a post-emit trim would have
        // silently dropped the oldest 22 entries (heights 105..126). The
        // new pre-emit trim must NOT.
        handle_head_event::<MockType>(
            &mut seq,
            &mut live_tokens,
            8,
            &data,
            Height { height: 129, hash: Some("0xA129".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);

        let emitted = drain(&mut rx).await;
        assert_eq!(emitted.len(), 25, "all 25 walked-back heights must emit");
        for job in &emitted {
            let h = job.height.height;
            let entry = live_tokens
                .get(&h)
                .unwrap_or_else(|| panic!("missing live_tokens entry for emitted height {}", h));
            assert_eq!(
                entry.0,
                job.height.hash.clone().unwrap(),
                "live_tokens hash must match emitted hash at height {}",
                h
            );
            assert!(
                !job.cancel.is_cancelled(),
                "freshly-emitted job at height {} must not carry a cancelled token",
                h
            );
        }
    }

    /// Idempotency: a head event for a block already in the sequence (e.g.,
    /// the broker re-sending the current tip on reconnect) is silently
    /// dropped — and the existing token is not cancelled.
    #[tokio::test]
    async fn duplicate_head_event_is_silently_dropped() {
        let data = Arc::new(MockData::new("MOCK"));
        populate_chain(&data, 100, 2, "0xA", "0xroot");

        let mut seq = BlockSequence::<MockType>::new(64);
        let mut live_tokens = BTreeMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        handle_head_event::<MockType>(
            &mut seq,
            &mut live_tokens,
            64,
            &data,
            Height { height: 100, hash: Some("0xA100".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        let a100_token = live_tokens.get(&100).unwrap().1.clone();
        // Re-emit the same head.
        handle_head_event::<MockType>(
            &mut seq,
            &mut live_tokens,
            64,
            &data,
            Height { height: 100, hash: Some("0xA100".to_string()) },
            &tx,
        )
        .await
        .unwrap();
        drop(tx);

        let emitted = drain(&mut rx).await;
        assert_eq!(emitted.len(), 1, "duplicate head should not re-emit");
        // No cancellation should happen on a duplicate.
        assert!(!a100_token.is_cancelled());
    }
}
