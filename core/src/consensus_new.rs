use crate::replay_stage::DUPLICATE_THRESHOLD;

pub mod vote_history_storage;

use {
    self::vote_history_storage::{SavedVoteHistory, SavedVoteHistoryVersions, VoteHistoryStorage},
    crate::consensus::{
        heaviest_subtree_fork_choice::HeaviestSubtreeForkChoice,
        latest_validator_votes_for_frozen_banks::LatestValidatorVotesForFrozenBanks,
        progress_map::{LockoutIntervals, ProgressMap},
    },
    chrono::prelude::*,
    solana_ledger::{ancestor_iterator::AncestorIterator, blockstore::Blockstore, blockstore_db},
    solana_runtime::{bank::Bank, bank_forks::BankForks, commitment::VOTE_THRESHOLD_SIZE},
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        hash::Hash,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::Keypair,
        slot_history::{Check, SlotHistory},
    },
    solana_vote_new::vote_account::VoteAccountsHashMap,
    solana_vote_new_program::{
        vote_instruction,
        vote_state_new::{
            process_vote_unchecked, BlockTimestamp, Vote, VoteRange, VoteState, VoteStateVersions,
            VoteTransaction,
        },
    },
    std::{
        cmp::Ordering,
        collections::{HashMap, HashSet, VecDeque},
        ops::{
            Bound::{Included, Unbounded},
            Deref,
        },
    },
    thiserror::Error,
};

pub const MAX_TRACKED_VOTES_PER_VALIDATOR: usize = 4;
pub type ValidatorVoteHistory = HashMap<Pubkey, VecDeque<VoteRange>>;

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum SwitchForkDecision {
    SwitchProof(Hash),
    SameFork,
    FailedSwitchThreshold(
        /* Switch proof stake */ u64,
        /* Total stake */ u64,
    ),
    FailedSwitchDuplicateRollback(Slot),
}

impl SwitchForkDecision {
    pub fn to_vote_instruction(
        &self,
        vote: VoteTransaction,
        vote_account_pubkey: &Pubkey,
        authorized_voter_pubkey: &Pubkey,
    ) -> Option<Instruction> {
        match (self, vote) {
            (SwitchForkDecision::FailedSwitchThreshold(_, total_stake), _) => {
                assert_ne!(*total_stake, 0);
                None
            }
            (SwitchForkDecision::FailedSwitchDuplicateRollback(_), _) => None,
            (SwitchForkDecision::SameFork, VoteTransaction::Vote(v)) => Some(
                vote_instruction::vote(vote_account_pubkey, authorized_voter_pubkey, v),
            ),
            (SwitchForkDecision::SwitchProof(switch_proof_hash), VoteTransaction::Vote(v)) => {
                Some(vote_instruction::vote_switch(
                    vote_account_pubkey,
                    authorized_voter_pubkey,
                    v,
                    *switch_proof_hash,
                ))
            }
        }
    }

    pub fn can_vote(&self) -> bool {
        match self {
            SwitchForkDecision::FailedSwitchThreshold(_, _) => false,
            SwitchForkDecision::FailedSwitchDuplicateRollback(_) => false,
            SwitchForkDecision::SameFork => true,
            SwitchForkDecision::SwitchProof(_) => true,
        }
    }
}

pub const SWITCH_FORK_THRESHOLD: f64 = 0.38;

pub type Result<T> = std::result::Result<T, VoteHistoryError>;
pub type Stake = u64;
pub type VotedStakes = HashMap<Slot, Stake>;
pub type PubkeyVotes = Vec<(Pubkey, Slot)>;

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct ComputedBankState {
    pub voted_stakes: VotedStakes,
    pub total_stake: Stake,
    // Tree of intervals of lockouts of the form [slot, slot + slot.lockout],
    // keyed by end of the range
    // pub lockout_intervals: LockoutIntervals,
    pub my_latest_landed_vote: Option<Slot>,
    pub votes_per_validator: ValidatorVoteHistory,
    pub last_quorum_vote: Option<VoteRange>,
    pub last_quorum_commit: Option<Slot>,
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(PartialEq, Eq, Debug, Default, Clone, Copy)]
pub(crate) enum BlockhashStatus {
    /// No vote since restart
    #[default]
    Uninitialized,
    /// Non voting validator
    NonVoting,
    /// Hot spare validator
    HotSpare,
    /// Successfully generated vote tx with blockhash
    Blockhash(Hash),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum VoteHistoryVersions {
    Current(VoteHistory),
}

impl VoteHistoryVersions {
    pub fn new_current(vote_history: VoteHistory) -> Self {
        Self::Current(vote_history)
    }

    pub fn convert_to_current(self) -> VoteHistory {
        match self {
            VoteHistoryVersions::Current(vote_history) => vote_history,
        }
    }
}
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "8ziHa1vA7WG5RCvXiE3g1f2qjSTNa47FB7e2czo7en7a")
)]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct VoteHistory {
    pub node_pubkey: Pubkey,
    threshold_size: f64,
    last_vote: Option<VoteTransaction>,
    last_vote_quorum: Slot,
    pub root: Slot,
    #[serde(skip)]
    // The blockhash used in the last vote transaction, may or may not equal the
    // blockhash of the voted block itself, depending if the vote slot was refreshed.
    // For instance, a vote for slot 5, may be refreshed/resubmitted for inclusion in
    //  block 10, in  which case `last_vote_tx_blockhash` equals the blockhash of 10, not 5.
    // For non voting validators this is NonVoting
    last_vote_tx_blockhash: BlockhashStatus,
    last_timestamp: BlockTimestamp,
    /*#[serde(skip)]
    pub last_switch_threshold_check: Option<(Slot, SwitchForkDecision)>,*/
}

impl Default for VoteHistory {
    fn default() -> Self {
        Self {
            node_pubkey: Pubkey::default(),
            threshold_size: VOTE_THRESHOLD_SIZE,
            last_vote: None,
            last_vote_quorum: 0,
            last_vote_tx_blockhash: BlockhashStatus::default(),
            last_timestamp: BlockTimestamp::default(),
            root: 0,
            //last_switch_threshold_check: Option::default(),
        }
    }
}

pub(crate) fn collect_vote_lockouts(
    vote_account_pubkey: &Pubkey,
    vote_accounts: &VoteAccountsHashMap,
    ancestors: &HashMap<Slot, HashSet<Slot>>,
    get_frozen_hash: impl Fn(Slot) -> Option<Hash>,
    latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
    mut votes_per_validator: ValidatorVoteHistory,
    threshold_size: f64,
    get_last_slot_vote_quorum: impl Fn(Slot) -> Option<VoteRange>,
) -> ComputedBankState {
    let mut voted_stakes = HashMap::new();
    let mut total_stake = 0;
    let mut my_latest_landed_vote = None;

    // This will store events with the associated validator Pubkey
    let mut events: Vec<(Slot, Pubkey, i64)> = Vec::new();

    for (&key, (voted_stake, account)) in vote_accounts.iter() {
        let voted_stake = *voted_stake;
        if voted_stake == 0 {
            continue;
        }
        total_stake += voted_stake;
        trace!("{} {} with stake {}", vote_account_pubkey, key, voted_stake);
        let vote_state = account.vote_state().clone();
        let vote_range = vote_state.vote_range;
        let reference_slot = vote_range.reference_slot;
        let vote_slot = vote_range.slot;

        if vote_slot == 0 {
            // This is the empty vote
            continue;
        }

        let mut validator_prev_votes = votes_per_validator.entry(key).or_default();
        if let Some(latest_vote_range) = validator_prev_votes.back() {
            if *latest_vote_range != vote_range {
                assert!(
                    vote_range.reference_slot > latest_vote_range.slot
                        || vote_range.reference_slot == latest_vote_range.reference_slot
                );
                // new vote has been made, insert into tracker
                validator_prev_votes.push_back(vote_range);
                if validator_prev_votes.len() > MAX_TRACKED_VOTES_PER_VALIDATOR {
                    validator_prev_votes.pop_front();
                }
            }
        }

        // Add events with the validator's Pubkey:
        // 1. Start of the range (add stake)
        // 2. End of the range (remove stake after the vote_slot)
        events.push((reference_slot, key, voted_stake as i64)); // Add stake at reference slot
        events.push((vote_slot, key, -(voted_stake as i64))); // Remove stake after vote slot

        if key == *vote_account_pubkey {
            my_latest_landed_vote = Some(vote_state.slot());
            datapoint_info!("vote-history-observed", ("slot", vote_state.slot(), i64));
        }

        // Add the last vote to update the `heaviest_subtree_fork_choice`
        latest_validator_votes_for_frozen_banks.check_add_vote(
            key,
            vote_state.slot(),
            get_frozen_hash(vote_state.slot()),
            true,
        );

        update_ancestor_voted_stakes(&mut voted_stakes, vote_slot, voted_stake, ancestors);
    }

    // Calculate threshold stake for quorum
    let quorum_stake = (total_stake as f64 * threshold_size).ceil() as u64;

    // Try to find a quorum commit
    let mut vote_quorums = find_vote_quorums(events, quorum_stake);
    let last_quorum_commit = find_commit_quorum(&vote_quorums, get_last_slot_vote_quorum);

    ComputedBankState {
        total_stake,
        my_latest_landed_vote,
        voted_stakes,
        last_quorum_vote: vote_quorums.pop(),
        last_quorum_commit,
        votes_per_validator,
    }
}

/// This function performs a sweep line algorithm to find the latest vote slot
/// with >quorum_stake of the total stake.
/// This function prevents double counting by ensuring that each validator contributes its stake only once
fn find_vote_quorums(
    events: Vec<(Slot, Pubkey, i64)>, // Events with Pubkey to allow tracking
    // must be > 2/3 of the total stake or this function might break
    quorum_stake: u64,
) -> Vec<VoteRange> {
    let mut quorums = vec![];
    let mut running_stake: i64 = 0;
    let mut quorum_start: Option<Slot> = None;

    // Track active validators for single and double quorums
    let mut active_validators: HashMap<Pubkey, usize> = HashMap::new(); // Count occurrences for double quorum

    let mut sorted_events = events;
    sorted_events.sort_by_key(|(slot, _, _)| *slot);

    // Sweep through the events
    for (slot, pubkey, stake_change) in sorted_events {
        if stake_change > 0 {
            // Add stake for the single quorum if not already counted
            let count = active_validators.entry(pubkey).or_insert(0);
            // For single quorum tracking
            if *count == 0 {
                running_stake += stake_change;
            }
            *count += 1;
        } else {
            let count = active_validators.get_mut(&pubkey).unwrap();
            *count -= 1;
            // Remove stake for the single quorum
            if *count == 0 {
                running_stake -= stake_change.abs();
                active_validators.remove(&pubkey);
            }
        }

        // Check for single validator quorum
        if running_stake >= quorum_stake as i64 {
            if quorum_start.is_none() {
                quorum_start = Some(slot);
            }
        } else {
            // Store the quorum range
            if let Some(quorum_start) = quorum_start {
                println!("pushing vote range: {} {}", quorum_start, slot);
                quorums.push(VoteRange::new(quorum_start, slot));
            }
            quorum_start = None; // Reset if it doesn't meet quorum
        }
    }

    quorums
}

fn find_commit_quorum(
    latest_vote_quorums: &[VoteRange],
    get_last_slot_vote_quorum: impl Fn(Slot) -> Option<VoteRange>,
) -> Option<Slot> {
    // TODO: decide if it's worth to check the other vote quorums
    // other than the last one as well
    if let Some(last_vote_quorum) = latest_vote_quorums.last() {
        let last_quorom_slot = last_vote_quorum.slot;
        if let Some(slot_quorum) = get_last_slot_vote_quorum(last_quorom_slot) {
            info!("slot: {}, quorum: {:?}", last_quorom_slot, slot_quorum);
            // check if `last_vote_quorum`` and `quorom` have any overlaps
            if slot_quorum.slot >= last_vote_quorum.reference_slot {
                // must be true because votes that landed in last_quorom_slot
                // cannot include `last_quorom_slot`
                assert!(slot_quorum.slot < last_quorom_slot);
                return Some(slot_quorum.slot);
            }
        }
    }

    None
}

/// Update stake for all the ancestors.
/// Note, stake is the same for all the ancestor.
fn update_ancestor_voted_stakes(
    voted_stakes: &mut VotedStakes,
    voted_slot: Slot,
    voted_stake: u64,
    ancestors: &HashMap<Slot, HashSet<Slot>>,
) {
    // If there's no ancestors, that means this slot must be from
    // before the current root, so ignore this slot
    if let Some(vote_slot_ancestors) = ancestors.get(&voted_slot) {
        *voted_stakes.entry(voted_slot).or_default() += voted_stake;
        for slot in vote_slot_ancestors {
            *voted_stakes.entry(*slot).or_default() += voted_stake;
        }
    }
}

pub(crate) fn is_slot_duplicate_confirmed(
    slot: Slot,
    voted_stakes: &VotedStakes,
    total_stake: Stake,
) -> bool {
    voted_stakes
        .get(&slot)
        .map(|stake| (*stake as f64 / total_stake as f64) > DUPLICATE_THRESHOLD)
        .unwrap_or(false)
}

impl VoteHistory {
    pub(crate) fn new(node_pubkey: Pubkey, root: Slot) -> Self {
        Self {
            node_pubkey,
            root,
            ..Self::default()
        }
    }

    pub(crate) fn last_vote_tx_blockhash(&self) -> BlockhashStatus {
        self.last_vote_tx_blockhash
    }

    pub(crate) fn last_vote_quorum(&self) -> Slot {
        self.last_vote_quorum
    }

    pub fn refresh_last_vote_timestamp(&mut self, heaviest_slot_on_same_fork: Slot) {
        let timestamp = if let Some(last_vote_timestamp) =
            self.last_vote.as_ref().and_then(|last_vote| {
                // To avoid a refreshed vote tx getting caught in deduplication filters,
                // we need to update timestamp. Increment by smallest amount to avoid skewing
                // the Timestamp Oracle.
                last_vote.timestamp()
            }) {
            last_vote_timestamp.saturating_add(1)
        } else {
            // If the previous vote did not send a timestamp due to clock error,
            // use the last good timestamp + 1
            datapoint_info!(
                "refresh-timestamp-missing",
                ("heaviest-slot", heaviest_slot_on_same_fork, i64),
                ("last-timestamp", self.last_timestamp.timestamp, i64),
                ("last-slot", self.last_timestamp.slot, i64),
            );
            self.last_timestamp.timestamp.saturating_add(1)
        };

        if let Some(ref mut last_vote) = self.last_vote {
            let last_voted_slot = last_vote.slot();
            if heaviest_slot_on_same_fork <= last_voted_slot {
                warn!(
                    "Trying to refresh timestamp for vote on {last_voted_slot}
                     using smaller heaviest bank {heaviest_slot_on_same_fork}"
                );
                return;
            }
            self.last_timestamp = BlockTimestamp {
                slot: last_voted_slot,
                timestamp,
            };
            last_vote.set_timestamp(Some(timestamp));
        } else {
            warn!(
                "Trying to refresh timestamp for last vote on heaviest bank on same fork
                   {heaviest_slot_on_same_fork}, but there is no vote to refresh"
            );
        }
    }

    pub fn refresh_last_vote_tx_blockhash(&mut self, new_vote_tx_blockhash: Hash) {
        self.last_vote_tx_blockhash = BlockhashStatus::Blockhash(new_vote_tx_blockhash);
    }

    pub(crate) fn mark_last_vote_tx_blockhash_non_voting(&mut self) {
        self.last_vote_tx_blockhash = BlockhashStatus::NonVoting;
    }

    pub(crate) fn mark_last_vote_tx_blockhash_hot_spare(&mut self) {
        self.last_vote_tx_blockhash = BlockhashStatus::HotSpare;
    }

    /*pub fn last_voted_slot_in_bank(bank: &Bank, vote_account_pubkey: &Pubkey) -> Option<Slot> {
        let vote_account = bank.get_vote_account(vote_account_pubkey)?;
        let vote_state = vote_account.vote_state();
        Some(vote_state.slot())
    }*/

    fn update_latest_vote(
        &mut self,
        reference_slot: Slot,
        vote_slot: Slot,
        vote_hash: Hash,
        last_quorum_slot: Slot,
    ) {
        let vote_transaction = VoteTransaction::from(Vote::new(
            VoteRange::new(reference_slot, vote_slot),
            vote_hash,
        ));
        self.last_vote = Some(vote_transaction);
        self.last_vote_quorum = last_quorum_slot;
    }

    pub fn record_bank_vote(&mut self, bank: &Bank, reference_slot: Slot, last_quorum_slot: Slot) {
        // Returns the new root if one is made after applying a vote for the given bank to
        // `self.vote_state`
        self.update_latest_vote(reference_slot, bank.slot(), bank.hash(), last_quorum_slot)
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn record_vote(
        &mut self,
        reference_slot: Slot,
        slot: Slot,
        hash: Hash,
        last_quorum_slot: Slot,
    ) {
        self.update_latest_vote(reference_slot, slot, hash, last_quorum_slot)
    }

    pub fn last_voted_slot(&self) -> Option<Slot> {
        self.last_vote.as_ref().map(|vote| vote.slot())
    }

    pub fn last_voted_slot_hash(&self) -> Option<(Slot, Hash)> {
        self.last_vote
            .as_ref()
            .map(|vote| (vote.slot(), vote.hash()))
    }

    pub fn last_vote(&self) -> Option<VoteTransaction> {
        self.last_vote.clone()
    }

    fn maybe_timestamp(&mut self, current_slot: Slot) -> Option<UnixTimestamp> {
        if current_slot > self.last_timestamp.slot
            || self.last_timestamp.slot == 0 && current_slot == self.last_timestamp.slot
        {
            let timestamp = Utc::now().timestamp();
            if timestamp >= self.last_timestamp.timestamp {
                self.last_timestamp = BlockTimestamp {
                    slot: current_slot,
                    timestamp,
                };
                return Some(timestamp);
            } else {
                datapoint_info!(
                    "backwards-timestamp",
                    ("slot", current_slot, i64),
                    ("timestamp", timestamp, i64),
                    ("last-timestamp", self.last_timestamp.timestamp, i64),
                )
            }
        }
        None
    }

    // a slot is recent if it's newer than the last vote we have. If we haven't voted yet
    // but have a root (hard forks situation) then compare it to the root
    pub fn is_recent(&self, slot: Slot) -> bool {
        if let Some(last_vote) = &self.last_vote {
            if slot <= last_vote.slot() {
                return false;
            }
        } else if slot <= self.root {
            return false;
        }
        true
    }

    pub fn is_locked_out(&self, slot: Slot, ancestors: &HashSet<Slot>) -> bool {
        if !self.is_recent(slot) {
            return true;
        }
        false
    }

    /*
    /// Checks if a vote for `candidate_slot` is usable in a switching proof
    /// from `last_voted_slot` to `switch_slot`.
    /// We assume `candidate_slot` is not an ancestor of `last_voted_slot`.
    ///
    /// Returns None if `candidate_slot` or `switch_slot` is not present in `ancestors`
    fn is_valid_switching_proof_vote(
        &self,
        candidate_slot: Slot,
        last_voted_slot: Slot,
        switch_slot: Slot,
        ancestors: &HashMap<Slot, HashSet<Slot>>,
        last_vote_ancestors: &HashSet<Slot>,
    ) -> Option<bool> {
        trace!(
            "Checking if {candidate_slot} is a valid switching proof vote from {last_voted_slot} \
             to {switch_slot}"
        );
        // Ignore if the `candidate_slot` is a descendant of the `last_voted_slot`, since we do not
        // want to count votes on the same fork.
        if Self::is_descendant_slot(candidate_slot, last_voted_slot, ancestors)? {
            return Some(false);
        }

        if last_vote_ancestors.is_empty() {
            // If `last_vote_ancestors` is empty, this means we must have a last vote that is stray. If the `last_voted_slot`
            // is stray, it must be descended from some earlier root than the latest root (the anchor at startup).
            // The above check also guarentees that the candidate slot is not a descendant of this stray last vote.
            //
            // This gives us a fork graph:
            //     / ------------- stray `last_voted_slot`
            // old root
            //     \- latest root (anchor) - ... - candidate slot
            //                                \- switch slot
            //
            // Thus the common acnestor of `last_voted_slot` and `candidate_slot` is `old_root`, which the `switch_slot`
            // descends from. Thus it is safe to use `candidate_slot` in the switching proof.
            //
            // Note: the calling function should have already panicked if we do not have ancestors and the last vote is not stray.
            assert!(self.is_stray_last_vote());
            return Some(true);
        }

        // Only consider forks that split at the common_ancestor of `switch_slot` and `last_voted_slot` or earlier.
        // This is to prevent situations like this from being included in the switching proof:
        //
        //         /-- `last_voted_slot`
        //     /--Y
        //    X    \-- `candidate_slot`
        //     \-- `switch_slot`
        //
        // The common ancestor of `last_voted_slot` and `switch_slot` is `X`. Votes for the `candidate_slot`
        // should not count towards the switch proof since `candidate_slot` is "on the same fork" as `last_voted_slot`
        // in relation to `switch_slot`.
        // However these candidate slots should be allowed:
        //
        //             /-- Y -- `last_voted_slot`
        //    V - W - X
        //        \    \-- `candidate_slot` -- `switch_slot`
        //         \    \-- `candidate_slot`
        //          \-- `candidate_slot`
        //
        // As the `candidate_slot`s forked off from `X` or earlier.
        //
        // To differentiate, we check the common ancestor of `last_voted_slot` and `candidate_slot`.
        // If the `switch_slot` descends from this ancestor, then the vote for `candidate_slot` can be included.
        Self::greatest_common_ancestor(ancestors, candidate_slot, last_voted_slot)
            .and_then(|ancestor| Self::is_descendant_slot(switch_slot, ancestor, ancestors))
    }

    /// Checks if `maybe_descendant` is a descendant of `slot`.
    ///
    /// Returns None if `maybe_descendant` is not present in `ancestors`
    fn is_descendant_slot(
        maybe_descendant: Slot,
        slot: Slot,
        ancestors: &HashMap<Slot, HashSet<u64>>,
    ) -> Option<bool> {
        ancestors
            .get(&maybe_descendant)
            .map(|candidate_slot_ancestors| candidate_slot_ancestors.contains(&slot))
    }

    /// Returns `Some(gca)` where `gca` is the greatest (by slot number)
    /// common ancestor of both `slot_a` and `slot_b`.
    ///
    /// Returns `None` if:
    /// * `slot_a` is not in `ancestors`
    /// * `slot_b` is not in `ancestors`
    /// * There is no common ancestor of slot_a and slot_b in `ancestors`
    fn greatest_common_ancestor(
        ancestors: &HashMap<Slot, HashSet<Slot>>,
        slot_a: Slot,
        slot_b: Slot,
    ) -> Option<Slot> {
        (ancestors.get(&slot_a)?)
            .intersection(ancestors.get(&slot_b)?)
            .max()
            .copied()
    }

    #[allow(clippy::too_many_arguments)]
    fn make_check_switch_threshold_decision(
        &self,
        switch_slot: Slot,
        ancestors: &HashMap<Slot, HashSet<u64>>,
        descendants: &HashMap<Slot, HashSet<u64>>,
        progress: &ProgressMap,
        total_stake: u64,
        epoch_vote_accounts: &VoteAccountsHashMap,
        latest_validator_votes_for_frozen_banks: &LatestValidatorVotesForFrozenBanks,
        heaviest_subtree_fork_choice: &HeaviestSubtreeForkChoice,
    ) -> SwitchForkDecision {
        let Some((last_voted_slot, last_voted_hash)) = self.last_voted_slot_hash() else {
            return SwitchForkDecision::SameFork;
        };
        let root = self.root();
        let empty_ancestors = HashSet::default();
        let empty_ancestors_due_to_minor_unsynced_ledger = || {
            // This condition (stale stray last vote) shouldn't occur under normal validator
            // operation, indicating something unusual happened.
            // This condition could be introduced by manual ledger mishandling,
            // validator SEGV, OS/HW crash, or plain No Free Space FS error.

            // However, returning empty ancestors as a fallback here shouldn't result in
            // slashing by itself (Note that we couldn't fully preclude any kind of slashing if
            // the failure was OS or HW level).

            // Firstly, lockout is ensured elsewhere.

            // Also, there is no risk of optimistic conf. violation. Although empty ancestors
            // could result in incorrect (= more than actual) locked_out_stake and
            // false-positive SwitchProof later in this function, there should be no such a
            // heavier fork candidate, first of all, if the last vote (or any of its
            // unavailable ancestors) were already optimistically confirmed.
            // The only exception is that other validator is already violating it...
            if self.is_first_switch_check() && switch_slot < last_voted_slot {
                // `switch < last` is needed not to warn! this message just because of using
                // newer snapshots on validator restart
                let message = format!(
                    "bank_forks doesn't have corresponding data for the stray restored last \
                     vote({last_voted_slot}), meaning some inconsistency between saved tower and \
                     ledger."
                );
                warn!("{}", message);
                datapoint_warn!("vote_history_warn", ("warn", message, String));
            }
            &empty_ancestors
        };

        let suspended_decision_due_to_major_unsynced_ledger = || {
            // This peculiar corner handling is needed mainly for a tower which is newer than
            // blockstore. (Yeah, we tolerate it for ease of maintaining validator by operators)
            // This condition could be introduced by manual ledger mishandling,
            // validator SEGV, OS/HW crash, or plain No Free Space FS error.

            // When we're in this clause, it basically means validator is badly running
            // with a future tower while replaying past slots, especially problematic is
            // last_voted_slot.
            // So, don't re-vote on it by returning pseudo FailedSwitchThreshold, otherwise
            // there would be slashing because of double vote on one of last_vote_ancestors.
            // (Well, needless to say, re-creating the duplicate block must be handled properly
            // at the banking stage: https://github.com/solana-labs/solana/issues/8232)
            //
            // To be specific, the replay stage is tricked into a false perception where
            // last_vote_ancestors is AVAILABLE for descendant-of-`switch_slot`,  stale, and
            // stray slots (which should always be empty_ancestors).
            //
            // This is covered by test_future_tower_* in local_cluster
            SwitchForkDecision::FailedSwitchThreshold(0, total_stake)
        };

        let rollback_due_to_duplicate_ancestor = |latest_duplicate_ancestor| {
            SwitchForkDecision::FailedSwitchDuplicateRollback(latest_duplicate_ancestor)
        };

        // `heaviest_subtree_fork_choice` entries are not cleaned by duplicate block purging/rollback logic,
        // so this is safe to check here. We return here if the last voted slot was rolled back/purged due to
        // being a duplicate because `ancestors`/`descendants`/`progress` structures may be missing this slot due
        // to duplicate purging. This would cause many of the `unwrap()` checks below to fail.
        //
        // TODO: Handle if the last vote is on a dupe, and then we restart. The dupe won't be in
        // heaviest_subtree_fork_choice, so `heaviest_subtree_fork_choice.latest_invalid_ancestor()` will return
        // None, but the last vote will be persisted in tower.
        let switch_hash = progress
            .get_hash(switch_slot)
            .expect("Slot we're trying to switch to must exist AND be frozen in progress map");
        if let Some(latest_duplicate_ancestor) = heaviest_subtree_fork_choice
            .latest_invalid_ancestor(&(last_voted_slot, last_voted_hash))
        {
            // We're rolling back because one of the ancestors of the last vote was a duplicate. In this
            // case, it's acceptable if the switch candidate is one of ancestors of the previous vote,
            // just fail the switch check because there's no point in voting on an ancestor. ReplayStage
            // should then have a special case continue building an alternate fork from this ancestor, NOT
            // the `last_voted_slot`. This is in contrast to usual SwitchFailure where ReplayStage continues to build blocks
            // on latest vote. See `ReplayStage::select_vote_and_reset_forks()` for more details.
            if heaviest_subtree_fork_choice.is_strict_ancestor(
                &(switch_slot, switch_hash),
                &(last_voted_slot, last_voted_hash),
            ) {
                return rollback_due_to_duplicate_ancestor(latest_duplicate_ancestor);
            } else if progress
                .get_hash(last_voted_slot)
                .map(|current_slot_hash| current_slot_hash != last_voted_hash)
                .unwrap_or(true)
            {
                // Our last vote slot was purged because it was on a duplicate fork, don't continue below
                // where checks may panic. We allow a freebie vote here that may violate switching
                // thresholds
                // TODO: Properly handle this case
                info!(
                    "Allowing switch vote on {:?} because last vote {:?} was rolled back",
                    (switch_slot, switch_hash),
                    (last_voted_slot, last_voted_hash)
                );
                return SwitchForkDecision::SwitchProof(Hash::default());
            }
        }

        let last_vote_ancestors = ancestors.get(&last_voted_slot).unwrap_or_else(|| {
            if self.is_stray_last_vote() {
                // Unless last vote is stray and stale, ancestors.get(last_voted_slot) must
                // return Some(_), justifying to panic! here.
                // Also, adjust_lockouts_after_replay() correctly makes last_voted_slot None,
                // if all saved votes are ancestors of replayed_root_slot. So this code shouldn't be
                // touched in that case as well.
                // In other words, except being stray, all other slots have been voted on while
                // this validator has been running, so we must be able to fetch ancestors for
                // all of them.
                empty_ancestors_due_to_minor_unsynced_ledger()
            } else {
                panic!("no ancestors found with slot: {last_voted_slot}");
            }
        });

        let switch_slot_ancestors = ancestors.get(&switch_slot).unwrap();

        if switch_slot == last_voted_slot || switch_slot_ancestors.contains(&last_voted_slot) {
            // If the `switch_slot is a descendant of the last vote,
            // no switching proof is necessary
            return SwitchForkDecision::SameFork;
        }

        if last_vote_ancestors.contains(&switch_slot) {
            if self.is_stray_last_vote() {
                return suspended_decision_due_to_major_unsynced_ledger();
            } else {
                panic!(
                    "Should never consider switching to ancestor ({switch_slot}) of last vote: \
                     {last_voted_slot}, ancestors({last_vote_ancestors:?})",
                );
            }
        }

        // By this point, we know the `switch_slot` is on a different fork
        // (is neither an ancestor nor descendant of `last_vote`), so a
        // switching proof is necessary
        let switch_proof = Hash::default();
        let mut locked_out_stake = 0;
        let mut locked_out_vote_accounts = HashSet::new();
        for (candidate_slot, descendants) in descendants.iter() {
            // 1) Don't consider any banks that haven't been frozen yet
            //    because the needed stats are unavailable
            // 2) Only consider lockouts at the latest `frozen` bank
            //    on each fork, as that bank will contain all the
            //    lockout intervals for ancestors on that fork as well.
            // 3) Don't consider lockouts on the `last_vote` itself
            // 4) Don't consider lockouts on any descendants of
            //    `last_vote`
            // 5) Don't consider any banks before the root because
            //    all lockouts must be ancestors of `last_vote`
            if !progress
                .get_fork_stats(*candidate_slot)
                .map(|stats| stats.computed)
                .unwrap_or(false)
                || {
                    // If any of the descendants have the `computed` flag set, then there must be a more
                    // recent frozen bank on this fork to use, so we can ignore this one. Otherwise,
                    // even if this bank has descendants, if they have not yet been frozen / stats computed,
                    // then use this bank as a representative for the fork.
                    descendants.iter().any(|d| {
                        progress
                            .get_fork_stats(*d)
                            .map(|stats| stats.computed)
                            .unwrap_or(false)
                    })
                }
                || *candidate_slot == last_voted_slot
                || *candidate_slot <= root
                || {
                    !self
                        .is_valid_switching_proof_vote(
                            *candidate_slot,
                            last_voted_slot,
                            switch_slot,
                            ancestors,
                            last_vote_ancestors,
                        )
                        .expect(
                            "candidate_slot and switch_slot exist in descendants map,
                        so they must exist in ancestors map",
                        )
                }
            {
                continue;
            }

            // By the time we reach here, any ancestors of the `last_vote`,
            // should have been filtered out, as they all have a descendant,
            // namely the `last_vote` itself.
            assert!(!last_vote_ancestors.contains(candidate_slot));

            // Evaluate which vote accounts in the bank are locked out
            // in the interval candidate_slot..last_vote, which means
            // finding any lockout intervals in the `lockout_intervals` tree
            // for this bank that contain `last_vote`.
            let lockout_intervals = &progress
                .get(candidate_slot)
                .unwrap()
                .fork_stats
                .lockout_intervals;
            // Find any locked out intervals for vote accounts in this bank with
            // `lockout_interval_end` >= `last_vote`, which implies they are locked out at
            // `last_vote` on another fork.
            for (_lockout_interval_end, intervals_keyed_by_end) in
                lockout_intervals.range((Included(last_voted_slot), Unbounded))
            {
                for (lockout_interval_start, vote_account_pubkey) in intervals_keyed_by_end {
                    if locked_out_vote_accounts.contains(vote_account_pubkey) {
                        continue;
                    }

                    // Only count lockouts on slots that are:
                    // 1) Not ancestors of `last_vote`, meaning being on different fork
                    // 2) Not from before the current root as we can't determine if
                    // anything before the root was an ancestor of `last_vote` or not
                    if !last_vote_ancestors.contains(lockout_interval_start) && {
                        // Given a `lockout_interval_start` < root that appears in a
                        // bank for a `candidate_slot`, it must be that `lockout_interval_start`
                        // is an ancestor of the current root, because `candidate_slot` is a
                        // descendant of the current root
                        *lockout_interval_start > root
                    } {
                        let stake = epoch_vote_accounts
                            .get(vote_account_pubkey)
                            .map(|(stake, _)| *stake)
                            .unwrap_or(0);
                        locked_out_stake += stake;
                        if (locked_out_stake as f64 / total_stake as f64) > SWITCH_FORK_THRESHOLD {
                            return SwitchForkDecision::SwitchProof(switch_proof);
                        }
                        locked_out_vote_accounts.insert(vote_account_pubkey);
                    }
                }
            }
        }

        // Check the latest votes for potentially gossip votes that haven't landed yet
        for (
            vote_account_pubkey,
            (candidate_latest_frozen_vote, _candidate_latest_frozen_vote_hash),
        ) in latest_validator_votes_for_frozen_banks.max_gossip_frozen_votes()
        {
            if locked_out_vote_accounts.contains(&vote_account_pubkey) {
                continue;
            }

            if *candidate_latest_frozen_vote > last_voted_slot && {
                // Because `candidate_latest_frozen_vote` is the last vote made by some validator
                // in the cluster for a frozen bank `B` observed through gossip, we may have cleared
                // that frozen bank `B` because we `set_root(root)` for a `root` on a different fork,
                // like so:
                //
                //    |----------X ------candidate_latest_frozen_vote (frozen)
                // old root
                //    |----------new root ----last_voted_slot
                //
                // In most cases, because `last_voted_slot` must be a descendant of `root`, then
                // if `candidate_latest_frozen_vote` is not found in the ancestors/descendants map (recall these
                // directly reflect the state of BankForks), this implies that `B` was pruned from BankForks
                // because it was on a different fork than `last_voted_slot`, and thus this vote for `candidate_latest_frozen_vote`
                // should be safe to count towards the switching proof:
                //
                // However, there is also the possibility that `last_voted_slot` is a stray, in which
                // case we cannot make this conclusion as we do not know the ancestors/descendants
                // of strays. Hence we err on the side of caution here and ignore this vote. This
                // is ok because validators voting on different unrooted forks should eventually vote
                // on some descendant of the root, at which time they can be included in switching proofs.
                self.is_valid_switching_proof_vote(
                    *candidate_latest_frozen_vote,
                    last_voted_slot,
                    switch_slot,
                    ancestors,
                    last_vote_ancestors,
                )
                .unwrap_or(false)
            } {
                let stake = epoch_vote_accounts
                    .get(vote_account_pubkey)
                    .map(|(stake, _)| *stake)
                    .unwrap_or(0);
                locked_out_stake += stake;
                if (locked_out_stake as f64 / total_stake as f64) > SWITCH_FORK_THRESHOLD {
                    return SwitchForkDecision::SwitchProof(switch_proof);
                }
                locked_out_vote_accounts.insert(vote_account_pubkey);
            }
        }

        // We have not detected sufficient lockout past the last voted slot to generate
        // a switching proof
        SwitchForkDecision::FailedSwitchThreshold(locked_out_stake, total_stake)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn check_switch_threshold(
        &mut self,
        switch_slot: Slot,
        ancestors: &HashMap<Slot, HashSet<u64>>,
        descendants: &HashMap<Slot, HashSet<u64>>,
        progress: &ProgressMap,
        total_stake: u64,
        epoch_vote_accounts: &VoteAccountsHashMap,
        latest_validator_votes_for_frozen_banks: &LatestValidatorVotesForFrozenBanks,
        heaviest_subtree_fork_choice: &HeaviestSubtreeForkChoice,
    ) -> SwitchForkDecision {
        let decision = self.make_check_switch_threshold_decision(
            switch_slot,
            ancestors,
            descendants,
            progress,
            total_stake,
            epoch_vote_accounts,
            latest_validator_votes_for_frozen_banks,
            heaviest_subtree_fork_choice,
        );
        let new_check = Some((switch_slot, decision.clone()));
        if new_check != self.last_switch_threshold_check {
            trace!(
                "new switch threshold check: slot {}: {:?}",
                switch_slot,
                decision,
            );
            self.last_switch_threshold_check = new_check;
        }
        decision
    }

    fn is_first_switch_check(&self) -> bool {
        self.last_switch_threshold_check.is_none()
    }*/

    pub fn save(
        &self,
        vote_history_storage: &dyn VoteHistoryStorage,
        node_keypair: &Keypair,
    ) -> Result<()> {
        let saved_vote_history = SavedVoteHistory::new(self, node_keypair)?;
        vote_history_storage.store(&SavedVoteHistoryVersions::from(saved_vote_history))?;
        Ok(())
    }

    pub fn restore(
        vote_history_storage: &dyn VoteHistoryStorage,
        node_pubkey: &Pubkey,
    ) -> Result<Self> {
        vote_history_storage.load(node_pubkey)
    }
}

#[derive(Error, Debug)]
pub enum VoteHistoryError {
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization Error: {0}")]
    SerializeError(#[from] bincode::Error),

    #[error("The signature on the saved vote history is invalid")]
    InvalidSignature,

    #[error("The vote history does not match this validator: {0}")]
    WrongVoteHistory(String),

    #[error(
        "The vote history is too old: newest slot in vote history ({0}) << oldest slot in available history \
         ({1})"
    )]
    TooOldTower(Slot, Slot),

    #[error("The vote history is fatally inconsistent with blockstore: {0}")]
    FatallyInconsistent(&'static str),

    #[error("The vote history is useless because of new hard fork: {0}")]
    HardFork(Slot),
}

impl VoteHistoryError {
    pub fn is_file_missing(&self) -> bool {
        if let VoteHistoryError::IoError(io_err) = &self {
            io_err.kind() == std::io::ErrorKind::NotFound
        } else {
            false
        }
    }
}

// Given an untimely crash, vote history may have roots that are not reflected in blockstore,
// or the reverse of this.
// That's because we don't impose any ordering guarantee or any kind of write barriers
// between vote history (plain old POSIX fs calls) and blockstore (through RocksDB), when
// `ReplayState::handle_votable_bank()` saves vote history before setting blockstore roots.
/*pub fn reconcile_blockstore_roots_with_external_source(
    external_source: ExternalRootSource,
    blockstore: &Blockstore,
    // blockstore.max_root() might have been updated already.
    // so take a &mut param both to input (and output iff we update root)
    last_blockstore_root: &mut Slot,
) -> blockstore_db::Result<()> {
    let external_root = external_source.root();
    if *last_blockstore_root < external_root {
        // Ensure external_root itself to exist and be marked as rooted in the blockstore
        // in addition to its ancestors.
        let new_roots: Vec<_> = AncestorIterator::new_inclusive(external_root, blockstore)
            .take_while(|current| match current.cmp(last_blockstore_root) {
                Ordering::Greater => true,
                Ordering::Equal => false,
                Ordering::Less => panic!(
                    "last_blockstore_root({last_blockstore_root}) is skipped while traversing \
                     blockstore (currently at {current}) from external root \
                     ({external_source:?})!?",
                ),
            })
            .collect();
        if !new_roots.is_empty() {
            info!(
                "Reconciling slots as root based on external root: {:?} (external: {:?}, \
                 blockstore: {})",
                new_roots, external_source, last_blockstore_root
            );

            // Unfortunately, we can't supply duplicate-confirmed hashes,
            // because it can't be guaranteed to be able to replay these slots
            // under this code-path's limited condition (i.e.  those shreds
            // might not be available, etc...) also correctly overcoming this
            // limitation is hard...
            blockstore.mark_slots_as_if_rooted_normally_at_startup(
                new_roots.into_iter().map(|root| (root, None)).collect(),
                false,
            )?;

            // Update the caller-managed state of last root in blockstore.
            // Repeated calls of this function should result in a no-op for
            // the range of `new_roots`.
            *last_blockstore_root = blockstore.max_root();
        } else {
            // This indicates we're in bad state; but still don't panic here.
            // That's because we might have a chance of recovering properly with
            // newer snapshot.
            warn!(
                "Couldn't find any ancestor slots from external source ({:?}) towards blockstore \
                 root ({}); blockstore pruned or only tower moved into new ledger or just hard \
                 fork?",
                external_source, last_blockstore_root,
            );
        }
    }
    Ok(())
}*/

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Clone)]
    struct VoteData {
        start_slot: Slot,
        end_slot: Slot,
        pubkey: Pubkey,
        stake: i64,
    }

    #[derive(Clone)]
    struct LandedVoteData {
        vote_data: VoteData,
        landed_slot: Slot,
    }

    // Generates the events from the vote data
    fn generate_events(vote_data: &[VoteData]) -> Vec<(Slot, Pubkey, i64)> {
        let mut events: Vec<(Slot, Pubkey, i64)> = Vec::new();

        for vote in vote_data {
            let VoteData {
                start_slot,
                end_slot,
                pubkey,
                stake,
            } = vote.clone();

            // Add events for vote start and end
            events.push((start_slot, pubkey, stake)); // Add stake at start_slot
            events.push((end_slot, pubkey, -stake)); // Remove stake after end_slot
        }

        events
    }

    // Generates the landed_votes_map from vote data
    fn generate_landed_votes_map(vote_data: &[LandedVoteData]) -> HashMap<Slot, Vec<VoteData>> {
        let mut landed_votes_map: HashMap<Slot, Vec<VoteData>> = HashMap::new();

        for landed_vote in vote_data {
            let LandedVoteData {
                vote_data:
                    VoteData {
                        start_slot,
                        end_slot,
                        pubkey,
                        stake,
                    },
                landed_slot,
            } = landed_vote.clone();
            // Can't land votes in future slots
            assert!(end_slot < landed_slot);

            // Create a new VoteData instance for the landed vote
            let vote_data = VoteData {
                start_slot,
                end_slot,
                pubkey,
                stake,
            };

            // Add the vote range, pubkey, and stake to the landed_votes_map
            landed_votes_map
                .entry(landed_slot)
                .or_insert_with(Vec::new)
                .push(vote_data);
        }

        landed_votes_map
    }

    fn generate_slot_quorums(
        landed_vote_data: &[LandedVoteData],
        quorum_stake: u64,
    ) -> HashMap<Slot, Vec<VoteRange>> {
        let mut latest_slot_quorum: HashMap<Slot, Vec<VoteRange>> = HashMap::new();

        // Generate the landed votes map from landed_vote_data
        let landed_votes_map = generate_landed_votes_map(landed_vote_data);

        // Iterate through landed_votes_map to process each landed slot's vote ranges
        for (landed_slot, vote_datas) in landed_votes_map {
            // Generate events from the vote data
            let slot_events = generate_events(&vote_datas);

            // Run find_vote_quorums for this landed slot
            let quorum_votes = find_vote_quorums(slot_events.clone(), quorum_stake);

            // If we have a quorum, add it to the latest_slot_quorum map
            latest_slot_quorum.insert(landed_slot, quorum_votes);
        }

        latest_slot_quorum
    }

    #[test]
    fn test_find_vote_quorums_with_single_validator() {
        let pubkey = Pubkey::new_unique();
        let vote_data = vec![
            VoteData {
                start_slot: 1,
                end_slot: 3,
                pubkey,
                stake: 1,
            },
            VoteData {
                start_slot: 1,
                end_slot: 4,
                pubkey,
                stake: 1,
            },
            VoteData {
                start_slot: 1,
                end_slot: 7,
                pubkey,
                stake: 1,
            },
        ];
        let events = generate_events(&vote_data);
        let threshold_stake = 1;
        let quorum_vote = find_vote_quorums(events.clone(), threshold_stake);
        assert_eq!(quorum_vote, vec![VoteRange::new(1, 7)]);
    }

    #[test]
    fn test_find_vote_quorums_with_single_validator_2() {
        let pubkey = Pubkey::new_unique();
        let vote_data = vec![
            VoteData {
                start_slot: 1,
                end_slot: 3,
                pubkey,
                stake: 1,
            },
            VoteData {
                start_slot: 4,
                end_slot: 6,
                pubkey,
                stake: 1,
            },
            VoteData {
                start_slot: 7,
                end_slot: 8,
                pubkey,
                stake: 1,
            },
        ];
        let events = generate_events(&vote_data);
        let threshold_stake = 2;
        let quorum_vote = find_vote_quorums(events.clone(), threshold_stake);
        assert_eq!(quorum_vote, vec![]);
    }

    #[test]
    fn test_find_vote_quorums_with_single_validator_3() {
        let pubkey = Pubkey::new_unique();
        let vote_data = vec![
            VoteData {
                start_slot: 1,
                end_slot: 3,
                pubkey,
                stake: 1,
            },
            VoteData {
                start_slot: 4,
                end_slot: 6,
                pubkey,
                stake: 1,
            },
            VoteData {
                start_slot: 7,
                end_slot: 8,
                pubkey,
                stake: 1,
            },
        ];
        let events = generate_events(&vote_data);
        let threshold_stake = 1;
        let quorum_vote = find_vote_quorums(events.clone(), threshold_stake);
        assert_eq!(
            quorum_vote,
            vec![
                VoteRange::new(1, 3),
                VoteRange::new(4, 6),
                VoteRange::new(7, 8)
            ]
        );
    }

    #[test]
    fn test_find_vote_quorums_with_single_validator_4() {
        let pubkey = Pubkey::new_unique();
        let vote_data = vec![
            VoteData {
                start_slot: 1,
                end_slot: 3,
                pubkey,
                stake: 1,
            },
            VoteData {
                start_slot: 4,
                end_slot: 4,
                pubkey,
                stake: 1,
            },
        ];
        let events = generate_events(&vote_data);
        let threshold_stake = 1;
        let quorum_vote = find_vote_quorums(events.clone(), threshold_stake);
        assert_eq!(
            quorum_vote,
            vec![VoteRange::new(1, 3), VoteRange::new(4, 4)]
        );
    }

    #[test]
    fn test_find_vote_quorums_overlapping_ranges() {
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        let pubkey3 = Pubkey::new_unique();

        let vote_data = vec![
            VoteData {
                start_slot: 1,
                end_slot: 4,
                pubkey: pubkey1,
                stake: 1,
            },
            VoteData {
                start_slot: 2,
                end_slot: 5,
                pubkey: pubkey2,
                stake: 1,
            },
            VoteData {
                start_slot: 3,
                end_slot: 6,
                pubkey: pubkey3,
                stake: 1,
            },
        ];
        let events = generate_events(&vote_data);
        let threshold_stake = 2;

        let quorum_vote = find_vote_quorums(events.clone(), threshold_stake);
        assert_eq!(quorum_vote, vec![VoteRange::new(2, 5)]);
    }

    #[test]
    fn test_find_vote_quorums_multiple_commits() {
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();

        let vote_data = vec![
            VoteData {
                start_slot: 1,
                end_slot: 3,
                pubkey: pubkey1,
                stake: 1,
            },
            VoteData {
                start_slot: 1,
                end_slot: 4,
                pubkey: pubkey2,
                stake: 1,
            },
            VoteData {
                start_slot: 1,
                end_slot: 7,
                pubkey: pubkey1,
                stake: 1,
            },
            VoteData {
                start_slot: 1,
                end_slot: 7,
                pubkey: pubkey2,
                stake: 1,
            },
            VoteData {
                start_slot: 1,
                end_slot: 8,
                pubkey: pubkey1,
                stake: 1,
            },
            VoteData {
                start_slot: 1,
                end_slot: 8,
                pubkey: pubkey2,
                stake: 1,
            },
        ];
        let events = generate_events(&vote_data);
        let threshold_stake = 2;
        let quorum_vote = find_vote_quorums(events.clone(), threshold_stake);
        assert_eq!(quorum_vote, vec![VoteRange::new(1, 8)]);
    }

    #[test]
    fn test_find_vote_quorums_commit_overlapping_ranges() {
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        let pubkey3 = Pubkey::new_unique();
        let pubkey4 = Pubkey::new_unique();
        let pubkey5 = Pubkey::new_unique();

        let vote_data = vec![
            VoteData {
                start_slot: 1,
                end_slot: 10,
                pubkey: pubkey1,
                stake: 4,
            },
            VoteData {
                start_slot: 1,
                end_slot: 10,
                pubkey: pubkey2,
                stake: 4,
            },
            VoteData {
                start_slot: 1,
                end_slot: 16,
                pubkey: pubkey1,
                stake: 4,
            },
            VoteData {
                start_slot: 1,
                end_slot: 4,
                pubkey: pubkey3,
                stake: 1,
            },
            VoteData {
                start_slot: 2,
                end_slot: 5,
                pubkey: pubkey4,
                stake: 1,
            },
            VoteData {
                start_slot: 3,
                end_slot: 6,
                pubkey: pubkey5,
                stake: 2,
            },
        ];

        let events = generate_events(&vote_data);
        let threshold_stake = 8;

        let quorum_vote = find_vote_quorums(events.clone(), threshold_stake);
        assert_eq!(quorum_vote, vec![VoteRange::new(1, 10)]);
    }

    #[test]
    fn test_find_vote_quorums_majority_stake_votes_twice() {
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();

        let vote_data = vec![
            VoteData {
                start_slot: 1,
                end_slot: 10,
                pubkey: pubkey1,
                stake: 6,
            },
            VoteData {
                start_slot: 1,
                end_slot: 10,
                pubkey: pubkey2,
                stake: 4,
            },
            VoteData {
                start_slot: 1,
                end_slot: 16,
                pubkey: pubkey1,
                stake: 6,
            },
        ];

        let events = generate_events(&vote_data);
        let threshold_stake = 7;

        let quorum_vote = find_vote_quorums(events.clone(), threshold_stake);
        assert_eq!(quorum_vote, vec![VoteRange::new(1, 10)]);
    }

    #[test]
    fn test_find_commit_quorum() {
        // Create LandedVoteData instances for the test directly
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();

        let landed_vote_data: Vec<LandedVoteData> = vec![
            LandedVoteData {
                vote_data: VoteData {
                    start_slot: 5,
                    end_slot: 10,
                    pubkey: pubkey2,
                    stake: 2,
                },
                landed_slot: 11,
            },
            LandedVoteData {
                vote_data: VoteData {
                    start_slot: 5,
                    end_slot: 10,
                    pubkey: pubkey1,
                    stake: 1,
                },
                landed_slot: 12,
            },
            LandedVoteData {
                vote_data: VoteData {
                    start_slot: 6,
                    end_slot: 11,
                    pubkey: pubkey2,
                    stake: 2,
                },
                landed_slot: 13,
            },
        ];

        // Generate slot quorums from the landed vote data
        let slot_quorums = generate_slot_quorums(&landed_vote_data, 2); // Assuming quorum_stake is 2

        // Define the closure to retrieve vote ranges by landed slot
        let get_last_slot_vote_quorum = |slot: Slot| -> Option<VoteRange> {
            slot_quorums
                .get(&slot)
                .and_then(|quorums| quorums.last().cloned())
        };

        // Run the `find_commit_quorum` function
        let result = find_commit_quorum(slot_quorums.get(&13).unwrap(), get_last_slot_vote_quorum);

        // Assert that the result matches the expected landed slot quorum
        assert_eq!(result, Some(10)); // Adjust as necessary based on expected behavior
    }

    #[test]
    fn test_find_commit_quorum_2() {
        // Create LandedVoteData instances for the test directly
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();

        let landed_vote_data: Vec<LandedVoteData> = vec![
            LandedVoteData {
                vote_data: VoteData {
                    start_slot: 1,
                    end_slot: 3,
                    pubkey: pubkey2,
                    stake: 2,
                },
                landed_slot: 11,
            },
            LandedVoteData {
                vote_data: VoteData {
                    start_slot: 5,
                    end_slot: 10,
                    pubkey: pubkey1,
                    stake: 1,
                },
                landed_slot: 12,
            },
            LandedVoteData {
                vote_data: VoteData {
                    start_slot: 6,
                    end_slot: 11,
                    pubkey: pubkey2,
                    stake: 2,
                },
                landed_slot: 13,
            },
        ];

        // Generate slot quorums from the landed vote data
        let slot_quorums = generate_slot_quorums(&landed_vote_data, 2); // Assuming quorum_stake is 2

        // Define the closure to retrieve vote ranges by landed slot
        let get_last_slot_vote_quorum = |slot: Slot| -> Option<VoteRange> {
            slot_quorums
                .get(&slot)
                .and_then(|quorums| quorums.last().cloned())
        };

        // Run the `find_commit_quorum` function
        let result = find_commit_quorum(slot_quorums.get(&13).unwrap(), get_last_slot_vote_quorum);

        // Assert that the result matches the expected landed slot quorum
        assert_eq!(result, None); // Adjust as necessary based on expected behavior
    }

    #[test]
    fn test_find_commit_quorum_3() {
        // Create LandedVoteData instances for the test directly
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();

        let landed_vote_data: Vec<LandedVoteData> = vec![
            LandedVoteData {
                vote_data: VoteData {
                    start_slot: 1,
                    end_slot: 6,
                    pubkey: pubkey2,
                    stake: 2,
                },
                landed_slot: 11,
            },
            LandedVoteData {
                vote_data: VoteData {
                    start_slot: 5,
                    end_slot: 10,
                    pubkey: pubkey1,
                    stake: 1,
                },
                landed_slot: 12,
            },
            LandedVoteData {
                vote_data: VoteData {
                    start_slot: 6,
                    end_slot: 11,
                    pubkey: pubkey2,
                    stake: 2,
                },
                landed_slot: 13,
            },
        ];

        // Generate slot quorums from the landed vote data
        let slot_quorums = generate_slot_quorums(&landed_vote_data, 2); // Assuming quorum_stake is 2

        // Define the closure to retrieve vote ranges by landed slot
        let get_last_slot_vote_quorum = |slot: Slot| -> Option<VoteRange> {
            slot_quorums
                .get(&slot)
                .and_then(|quorums| quorums.last().cloned())
        };

        // Run the `find_commit_quorum` function
        let result = find_commit_quorum(slot_quorums.get(&13).unwrap(), get_last_slot_vote_quorum);

        // Assert that the result matches the expected landed slot quorum
        assert_eq!(result, Some(6)); // Adjust as necessary based on expected behavior
    }
}
