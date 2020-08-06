use crate::bank::Bank;
use solana_sdk::{
    account::Account, clock::Slot, contains::Contains, pubkey::Pubkey, signature::Signature,
};
use solana_vote_program::vote_state::{Vote, VoteState, MAX_LOCKOUT_HISTORY};
use std::{
    collections::{BTreeMap, HashMap},
    ops::{Deref, DerefMut},
};

#[derive(Debug)]
struct IncreasedConfirmationVote {
    transaction_signature: Signature,
    representative_slot: Slot,
    vote_landed_slot: Slot,
    // Number of votes from when a vote for a slot first landed, to when
    // the vote confirmation reached some `X` represented by this struct
    num_total_votes: usize,
}

#[derive(Debug)]
struct ValidatorVote {
    slot: Slot,
    landed_slot: Slot,
    transaction_signature: Signature,
    // Map from `num_confirmations` to a list of different votes on different forks
    // that increased the confirmation to `num_confirmations` on `self.slot`.
    increased_vote_lockout_transactions: BTreeMap<u32, Vec<IncreasedConfirmationVote>>,
    // Tuple of (vote_slot, vote_landed_slot) where `vote_slot` is the immediate next vote
    // on some fork made by this validator that landed in some slot `vote_landed_slot`.
    next_votes: Vec<(Slot, Slot)>,
    // Last slot in the vote transaction that introduced this slot. For instance if the vote
    // was [0, 1, 2], 2 would be the representatiive for 0 and 1.
    representative_slot: Slot,
    height: usize,
}

#[derive(Default)]
struct ValidatorLandedVotes(
    // Maps a slot in which a vote `V` landed to metadata about that vote
    // Tracking the slot a vote landed in helps to uniquely identify which
    // votes have landed on which forks.
    HashMap<Slot, ValidatorVote>,
);

pub struct ValidatorVoteHistory {
    // Maps a slot for which validators voted to a map of information about those votes for
    // each such validator
    slot_to_votes: HashMap<Slot, HashMap<Pubkey, ValidatorLandedVotes>>,
    root: Slot,
    initial_slots: HashMap<Pubkey, Slot>,
}

impl Deref for ValidatorLandedVotes {
    type Target = HashMap<Slot, ValidatorVote>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ValidatorLandedVotes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ValidatorVoteHistory {
    pub fn new_from_root_bank(root_bank: &Bank) -> Self {
        let mut validator_vote_history = ValidatorVoteHistory::new(root_bank.slot());
        for (key, (_, account)) in root_bank.vote_accounts().into_iter() {
            let vote_state = VoteState::from(&account).expect("Vote state must be readable");
            if let Some(starting_slot) = vote_state.last_voted_slot() {
                validator_vote_history
                    .initial_slots
                    .insert(key, starting_slot);
            }
        }
        validator_vote_history
    }

    pub fn new(root: Slot) -> Self {
        Self {
            root,
            slot_to_votes: HashMap::new(),
            initial_slots: HashMap::new(),
        }
    }

    pub fn get_slot_history(
        &self,
        vote_slot: Slot,
        // Some later slot on the fork that includes a vote for `vote_slot`,
        // where the `num_confirmations` on `vote_slot` >= `num_confirmations`.
        reference_slot: Slot,
        vote_pubkey: &Pubkey,
        num_confirmations: u32,
        // ancestors of `reference_slot`
        ancestors: &dyn Contains<Slot>,
    ) -> Option<(Vec<Signature>, u32)> {
        assert!(num_confirmations > 0 && num_confirmations <= MAX_LOCKOUT_HISTORY as u32 + 1);

        // Find the first vote for slot `vote_slot` on this fork
        let validator_vote =
            self.get_validator_vote(vote_slot, reference_slot, vote_pubkey, ancestors);

        if validator_vote.is_none() {
            error!(
                "Validator vote {} must exist if history is being queried",
                vote_slot
            );
            return None;
        }

        let validator_vote = validator_vote.unwrap();

        // Find the vote that first increased the confirmations on `vote_slot` to
        // `>= num_confirmations`
        let confirmation_info = self.get_confirmation_info(
            &validator_vote,
            num_confirmations,
            reference_slot,
            ancestors,
        );

        if confirmation_info.is_none() {
            error!(
                "Must have a confirmation info for slot {} on fork {} with >= num_confirmations {}",
                vote_slot, reference_slot, num_confirmations
            );
            return None;
        }

        let (actual_num_confirmations, increased_confirmation_vote) = confirmation_info.unwrap();
        let expected_last_transaction_signature = increased_confirmation_vote.transaction_signature;

        // The number of votes in range [validator_vote.transa  ction_signature, last_transaction_signature]
        let num_votes = increased_confirmation_vote.num_total_votes;

        // Find all votes up to `last_transaction_signature`, starting at `validator_vote.transaction_signature`
        let mut all_signatures = vec![validator_vote.transaction_signature];
        let mut current_vote =
            self.get_next_vote(&validator_vote, vote_pubkey, reference_slot, ancestors);
        while all_signatures.len() < num_votes {
            all_signatures.push(current_vote.as_ref().unwrap().transaction_signature);
            current_vote = self.get_next_vote(
                &current_vote.unwrap(),
                vote_pubkey,
                reference_slot,
                ancestors,
            );
        }

        assert_eq!(
            *all_signatures
                .last()
                .expect("must be at least a vote for `vote_slot`"),
            expected_last_transaction_signature
        );
        Some((all_signatures, actual_num_confirmations))
    }

    pub fn insert_vote(
        &mut self,
        vote: &Vote,
        vote_account: &Account,
        // The slot in which the vote transaction landed
        vote_landed_slot: Slot,
        transaction_signature: Signature,
        vote_pubkey: &Pubkey,
        // Ancestors used to find in which slot the previous vote
        // transaction for this validator, on this fork, landed
        ancestors: &dyn Contains<Slot>,
    ) {
        // Vote state from before the input `vote` was applied
        let mut old_vote_state = VoteState::from(&vote_account).unwrap_or_else(|| {
            panic!(
                "Vote state from slot {} could not be parsed",
                vote_landed_slot
            )
        });

        // Get the last vote made by the validator on this fork, which should be the
        // last vote slot in the vote state before the input `vote` to this function
        // was applied
        let prev_vote_slot = old_vote_state.last_voted_slot();

        // Apply `vote` and see which slots' confirmations increased
        let confirmation_increased_slots = old_vote_state
            .process_vote_unchecked(vote)
            .expect("Must be successful since bank processed this successfully");
        if confirmation_increased_slots.is_empty() {
            // If this vote didn't introduce any new slots, return
            return;
        }

        // In order to increase confirmations, there must have been at least one vote slot
        // in the `Vote`.
        let representative_slot = vote.last_voted_slot().unwrap();

        let prev_height = prev_vote_slot
            .map(|prev_vote_slot| {
                // If the `prev_vote_slot` == the starting slot from the
                // first root bank/snapshot, that means no vote has landed for
                // this validator on this fork since booting, which means we expect
                // the previous vote to not be tracked in the history.
                if let Some(initial_slot) = self.initial_slots.get(&vote_pubkey) {
                    if prev_vote_slot == *initial_slot {
                        assert!(self
                            .get_validator_vote_mut(
                                prev_vote_slot,
                                vote_landed_slot,
                                &vote_pubkey,
                                ancestors,
                            )
                            .is_none());
                        return 0;
                    }
                }

                if prev_vote_slot < self.root {
                    return 0;
                }

                // Otherwise, we expect the previous vote to exist in the history.
                let validator_vote = self
                    .get_validator_vote_mut(
                        prev_vote_slot,
                        vote_landed_slot,
                        &vote_pubkey,
                        ancestors,
                    )
                    .expect("previous validator vote must exist");
                validator_vote
                    .next_votes
                    .push((representative_slot, vote_landed_slot));
                validator_vote.height
            })
            .unwrap_or(0);

        let cur_height = prev_height + 1;

        // Update any slots that have increased their confirmation levels
        for (confirmation_increased_slot, new_num_confirmations) in
            confirmation_increased_slots.into_iter()
        {
            if confirmation_increased_slot >= self.root {
                let validator_vote = self.get_validator_vote_mut_or_create(
                    confirmation_increased_slot,
                    representative_slot,
                    vote_landed_slot,
                    transaction_signature,
                    *vote_pubkey,
                    ancestors,
                    cur_height,
                );
                validator_vote
                    .increased_vote_lockout_transactions
                    .entry(new_num_confirmations)
                    .or_default()
                    .push(IncreasedConfirmationVote {
                        representative_slot,
                        vote_landed_slot,
                        transaction_signature,
                        num_total_votes: cur_height - validator_vote.height + 1,
                    });
            }
        }
    }

    pub fn set_root(&mut self, root: Slot) {
        assert!(root >= self.root);
        self.slot_to_votes.retain(|slot, _| *slot >= root);
        self.root = root;
    }

    fn get_representative_vote<'a>(
        &'a self,
        mut validator_vote: &'a ValidatorVote,
        vote_pubkey: &Pubkey,
    ) -> &'a ValidatorVote {
        if validator_vote.representative_slot != validator_vote.slot {
            assert!(validator_vote.next_votes.is_empty());
            validator_vote = self.get_validator_vote_exact(
                validator_vote.representative_slot,
                validator_vote.landed_slot,
                vote_pubkey,
            );
        }
        validator_vote
    }

    // Returns metadata about the vote that made the slot representedd by `validator_vote`
    // have confirmations == num_confirmations
    fn get_confirmation_info<'a>(
        &'a self,
        validator_vote: &'a ValidatorVote,
        num_confirmations: u32,
        reference_slot: Slot,
        ancestors: &dyn Contains<Slot>,
    ) -> Option<(u32, &'a IncreasedConfirmationVote)> {
        validator_vote
            .increased_vote_lockout_transactions
            .range(num_confirmations..)
            .next()
            .and_then(|(actual_num_confirmations, increased_confirmation_votes)| {
                increased_confirmation_votes
                    .iter()
                    .find_map(|increased_confirmation_vote| {
                        if ancestors.contains(&increased_confirmation_vote.vote_landed_slot)
                            || increased_confirmation_vote.vote_landed_slot == reference_slot
                        {
                            Some((*actual_num_confirmations, increased_confirmation_vote))
                        } else {
                            None
                        }
                    })
            })
    }

    fn get_next_vote<'a>(
        &'a self,
        validator_vote: &'a ValidatorVote,
        vote_pubkey: &Pubkey,
        reference_slot: Slot,
        ancestors: &dyn Contains<Slot>,
    ) -> Option<&'a ValidatorVote> {
        self.get_representative_vote(validator_vote, vote_pubkey)
            .next_votes
            .iter()
            .find_map(|(next_vote_slot, next_vote_landed_slot)| {
                if ancestors.contains(next_vote_landed_slot)
                    || *next_vote_landed_slot == reference_slot
                {
                    Some(self.get_validator_vote_exact(
                        *next_vote_slot,
                        *next_vote_landed_slot,
                        vote_pubkey,
                    ))
                } else {
                    None
                }
            })
    }

    fn find_ancestor_vote(
        slots_in_which_slot_vote_landed: &ValidatorLandedVotes,
        reference_slot: Slot,
        ancestors: &dyn Contains<Slot>,
    ) -> Option<Slot> {
        // `slots_in_which_vote_landed` are slots in which `slot_vote`
        // landed, on any forks
        for (slot, _) in slots_in_which_slot_vote_landed.iter() {
            // Find the transaction that landed in a slot in `ancestors`
            if ancestors.contains(slot) || *slot == reference_slot {
                return Some(*slot);
            }
        }

        None
    }

    fn get_validator_vote_mut(
        &mut self,
        slot_vote: Slot,
        reference_slot: Slot,
        vote_pubkey: &Pubkey,
        ancestors: &dyn Contains<Slot>,
    ) -> Option<&mut ValidatorVote> {
        let slot_votes = self.slot_to_votes.get_mut(&slot_vote);

        if slot_votes.is_none() {
            error!(
                "get_validator_vote_mut(): Votes for slot {} must exist",
                slot_vote
            );
            return None;
        }

        let slots_in_which_slot_vote_landed = slot_votes.unwrap().get_mut(vote_pubkey);
        if slots_in_which_slot_vote_landed.is_none() {
            error!(
                "get_validator_vote_mut(): Votes for pubkey {} must exist",
                vote_pubkey
            );
            return None;
        }

        let slots_in_which_slot_vote_landed = slots_in_which_slot_vote_landed.unwrap();
        // `slots_in_which_vote_landed` are slots in which `slot_vote`
        // landed, on any forks
        if let Some(ancestor_slot) =
            Self::find_ancestor_vote(&slots_in_which_slot_vote_landed, reference_slot, ancestors)
        {
            Some(
                slots_in_which_slot_vote_landed
                    .get_mut(&ancestor_slot)
                    .unwrap(),
            )
        } else {
            error!(
                "get_validator_vote_mut(): No ancestor of {} contained vote for slot {} for pubkey {}",
                reference_slot,
                slot_vote,
                vote_pubkey
            );
            None
        }
    }

    fn get_validator_vote_exact(
        &self,
        slot_vote: Slot,
        slot_landed: Slot,
        vote_pubkey: &Pubkey,
    ) -> &ValidatorVote {
        self.slot_to_votes
            .get(&slot_vote)
            .unwrap()
            .get(vote_pubkey)
            .unwrap()
            .get(&slot_landed)
            .unwrap()
    }

    fn get_validator_vote(
        &self,
        slot_vote: Slot,
        reference_slot: Slot,
        vote_pubkey: &Pubkey,
        ancestors: &dyn Contains<Slot>,
    ) -> Option<&ValidatorVote> {
        let slot_votes = self.slot_to_votes.get(&slot_vote);

        if slot_votes.is_none() {
            error!(
                "get_validator_vote(): Votes for slot {} must exist",
                slot_vote
            );
            return None;
        }

        let slots_in_which_slot_vote_landed = slot_votes.unwrap().get(vote_pubkey);
        if slots_in_which_slot_vote_landed.is_none() {
            error!(
                "get_validator_vote_mut(): Votes for pubkey {} must exist",
                vote_pubkey
            );
            return None;
        }

        let slots_in_which_slot_vote_landed = slots_in_which_slot_vote_landed.unwrap();
        // `slots_in_which_vote_landed` are slots in which `slot_vote`
        // landed, on any forks
        if let Some(ancestor_slot) =
            Self::find_ancestor_vote(&slots_in_which_slot_vote_landed, reference_slot, ancestors)
        {
            Some(slots_in_which_slot_vote_landed.get(&ancestor_slot).unwrap())
        } else {
            error!(
                "get_validator_vote_mut(): No ancestor of {} contained vote for slot {} for pubkey {}",
                reference_slot,
                slot_vote,
                vote_pubkey
            );
            None
        }
    }

    fn get_validator_vote_mut_or_create(
        &mut self,
        slot_vote: Slot,
        representative_slot: Slot,
        reference_slot: Slot,
        transaction_signature: Signature,
        vote_pubkey: Pubkey,
        ancestors: &dyn Contains<Slot>,
        height: usize,
    ) -> &mut ValidatorVote {
        let slots_in_which_slot_vote_landed = self
            .slot_to_votes
            .entry(slot_vote)
            .or_default()
            .entry(vote_pubkey)
            .or_default();

        let mut ancestor_slot =
            Self::find_ancestor_vote(&slots_in_which_slot_vote_landed, reference_slot, ancestors);

        if ancestor_slot.is_none() {
            slots_in_which_slot_vote_landed.insert(
                reference_slot,
                ValidatorVote {
                    slot: slot_vote,
                    landed_slot: reference_slot,
                    transaction_signature,
                    increased_vote_lockout_transactions: BTreeMap::new(),
                    next_votes: vec![],
                    height,
                    representative_slot,
                },
            );
            ancestor_slot = Some(reference_slot);
        }

        slots_in_which_slot_vote_landed
            .get_mut(&ancestor_slot.unwrap())
            .unwrap()
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::bank_forks::BankForks;
    use solana_sdk::{
        account_utils::StateMut,
        hash::Hash,
        signature::{Keypair, Signer},
    };
    use solana_vote_program::vote_state::VoteStateVersions;
    use std::collections::HashSet;
    use trees::tr;

    struct TestSetup {
        ancestors: HashMap<Slot, HashSet<Slot>>,
        descendants: HashMap<Slot, HashSet<Slot>>,
        validator_vote_history: ValidatorVoteHistory,
        vote_pubkey: Pubkey,
        vote_account: Account,
    }

    impl TestSetup {
        pub fn new() -> Self {
            /*
                Build fork structure:
                    slot 0
                      |
                    slot 1
                    /    \
                slot 2    |
                |       slot 3
                slot 4    |
                        slot 5
                          |
                        slot 6
                          |
                        slot 7
            */
            let forks = tr(0) / (tr(1) / (tr(2) / (tr(4))) / (tr(3) / (tr(5) / (tr(6) / (tr(7))))));
            let bank_forks = BankForks::new_from_tree(forks);
            let ancestors = bank_forks.ancestors();
            let descendants = bank_forks.descendants();

            // Initialize an arbitrary validator vote pubkey
            let vote_keypair = Keypair::new();
            let vote_pubkey = vote_keypair.pubkey();

            // Create ValidatorVoteHistory
            let validator_vote_history = ValidatorVoteHistory::new(bank_forks.root());

            // Create a vote account
            let vote_account = Account::new(100, VoteState::size_of(), &solana_vote_program::id());

            TestSetup {
                ancestors,
                descendants,
                validator_vote_history,
                vote_pubkey,
                vote_account,
            }
        }
    }

    fn simulate_vote_on_account(vote_account: &mut Account, vote: &Vote) {
        let mut vote_state = VoteState::from(&vote_account).unwrap();
        vote_state.process_vote_unchecked(&vote).unwrap();
        vote_account
            .set_state(&VoteStateVersions::Current(Box::new(vote_state)))
            .unwrap();
    }

    #[test]
    fn test_get_slot_history() {
        let TestSetup {
            ancestors,
            descendants,
            mut validator_vote_history,
            vote_pubkey,
            vote_account,
            ..
        } = TestSetup::new();

        let vote_landed_slot = 5;
        let transaction_signature =
            Signature::new(&(0..64).map(|_| rand::random::<u8>()).collect::<Vec<_>>());

        let vote_slots = vec![0, 1, 3];
        let vote = Vote::new(vote_slots.clone(), Hash::default());

        // Insert the vote for tracking
        validator_vote_history.insert_vote(
            &vote,
            &vote_account,
            // The slot in which the vote transaction landed
            vote_landed_slot,
            transaction_signature,
            &vote_pubkey,
            ancestors.get(&vote_landed_slot).unwrap(),
        );

        // Using any reference slot of that is `vote_landed_slot` or a descendant of `vote_landed_slot`
        // should work
        let vote_landed_slot_descendants = descendants.get(&vote_landed_slot).unwrap();
        assert!(!vote_landed_slot_descendants.is_empty());
        for reference_slot in vote_landed_slot_descendants
            .iter()
            .chain(std::iter::once(&vote_landed_slot))
        {
            let ancestors = ancestors.get(reference_slot).unwrap();
            for (i, slot) in vote_slots.iter().enumerate() {
                // A single transaction increased all the confirmation levels of each of the slots,
                // so for each slot `S` in `vote_slots`, asking for any confirmation level less than
                // the number of confirmations on `S` should return the same transaction, and asking for
                // any number of confirmations greater than this will panic.
                for num_confirmations in 1..vote_slots.len() - i {
                    assert_eq!(
                        validator_vote_history
                            .get_slot_history(
                                *slot,
                                *reference_slot,
                                &vote_pubkey,
                                num_confirmations as u32,
                                ancestors
                            )
                            .unwrap(),
                        (vec![transaction_signature], (vote_slots.len() - i) as u32)
                    );
                }
            }
        }
    }

    #[test]
    fn test_get_slot_history_same_lockout_multiple_forks() {
        let TestSetup {
            ancestors,
            mut validator_vote_history,
            vote_pubkey,
            mut vote_account,
            ..
        } = TestSetup::new();

        // Create a vote for slot 0, and lnd vote in slot 1
        let transaction_signature0 =
            Signature::new(&(0..64).map(|_| rand::random::<u8>()).collect::<Vec<_>>());
        let vote_slots = vec![0];
        let vote0 = Vote::new(vote_slots, Hash::default());
        let vote_landed_slot = 1;
        validator_vote_history.insert_vote(
            &vote0,
            &vote_account,
            vote_landed_slot,
            transaction_signature0,
            &vote_pubkey.clone(),
            ancestors.get(&vote_landed_slot).unwrap(),
        );

        // Commit the vote landing in the vote account
        simulate_vote_on_account(&mut vote_account, &vote0);

        // Now create a vote for slot 1, and land that vote in two
        // different forks. Both forks should increase the number of
        // confirmations on slot 0 to 2.
        let transaction_signature1 =
            Signature::new(&(0..64).map(|_| rand::random::<u8>()).collect::<Vec<_>>());
        let vote_slots = vec![1];
        let vote1 = Vote::new(vote_slots, Hash::default());
        for vote_landed_slot in &[2, 3] {
            validator_vote_history.insert_vote(
                &vote1,
                &vote_account,
                *vote_landed_slot,
                transaction_signature1,
                &vote_pubkey.clone(),
                ancestors.get(vote_landed_slot).unwrap(),
            );
        }

        // Check both forks have the correct history for slot 0
        for vote_landed_slot in &[2, 3] {
            let num_confirmations = 2;
            assert_eq!(
                validator_vote_history
                    .get_slot_history(
                        0,
                        *vote_landed_slot,
                        &vote_pubkey,
                        num_confirmations,
                        ancestors.get(&vote_landed_slot).unwrap(),
                    )
                    .unwrap(),
                (vec![transaction_signature0, transaction_signature1], 2)
            );
        }
    }

    #[test]
    fn test_get_slot_history_same_tx_multiple_forks() {
        let TestSetup {
            ancestors,
            mut validator_vote_history,
            vote_pubkey,
            mut vote_account,
            ..
        } = TestSetup::new();

        // Create a vote for slot 1
        let transaction_signature1 =
            Signature::new(&(0..64).map(|_| rand::random::<u8>()).collect::<Vec<_>>());
        let vote_slots = vec![0, 1];
        let vote1 = Vote::new(vote_slots.clone(), Hash::default());

        // Land vote on two different forks
        for vote_landed_fork in &[2, 3] {
            validator_vote_history.insert_vote(
                &vote1,
                &vote_account,
                // The slot in which the vote transaction landed
                *vote_landed_fork,
                transaction_signature1,
                &vote_pubkey,
                ancestors.get(&vote_landed_fork).unwrap(),
            );
        }

        // Try to find the vote history for confirmation level of 1 on the vote slots [0, 1],
        // but from later slots than the slots in which the vote landed on both forks.
        let check_first_confirmation = |validator_vote_history: &ValidatorVoteHistory| {
            for later_slot_on_fork in &[4, 6] {
                for (i, slot) in vote_slots.iter().enumerate() {
                    assert_eq!(
                        validator_vote_history
                            .get_slot_history(
                                *slot,
                                *later_slot_on_fork,
                                &vote_pubkey,
                                1,
                                ancestors.get(&later_slot_on_fork).unwrap(),
                            )
                            .unwrap(),
                        (vec![transaction_signature1], (vote_slots.len() - i) as u32)
                    );
                }
            }
        };

        check_first_confirmation(&validator_vote_history);

        // Commit the vote landing in the vote account
        simulate_vote_on_account(&mut vote_account, &vote1);

        // Simulate vote landing on fork with slot 2, increasing confirmation on slot 1
        // to 2, but this should not be visible on other fork
        let vote2 = Vote::new(vec![2], Hash::default());
        let vote_landed_slot = 4;
        let transaction_signature2 =
            Signature::new(&(0..64).map(|_| rand::random::<u8>()).collect::<Vec<_>>());
        validator_vote_history.insert_vote(
            &vote2,
            &vote_account,
            // The slot in which the vote transaction landed
            vote_landed_slot,
            transaction_signature2,
            &vote_pubkey,
            ancestors.get(&vote_landed_slot).unwrap(),
        );

        // Earlier confirmations should not be affected
        check_first_confirmation(&validator_vote_history);

        // Should be able to get chain of votes that made confirmation level == 3 on
        // fork with slot 4
        assert_eq!(
            validator_vote_history
                .get_slot_history(
                    0,
                    vote_landed_slot,
                    &vote_pubkey,
                    3,
                    ancestors.get(&vote_landed_slot).unwrap(),
                )
                .unwrap(),
            (vec![transaction_signature1, transaction_signature2], 3)
        );
    }

    #[test]
    fn test_insert_less_than_root() {
        let TestSetup {
            ancestors,
            mut validator_vote_history,
            vote_pubkey,
            vote_account,
            ..
        } = TestSetup::new();

        let root = 3;
        validator_vote_history.set_root(root);
        let vote_slots = vec![0, 1, 3, 5];
        let vote = Vote::new(vote_slots.clone(), Hash::default());
        let transaction_signature =
            Signature::new(&(0..64).map(|_| rand::random::<u8>()).collect::<Vec<_>>());
        let vote_landed_slot = 6;

        // Insert the votes for tracking
        validator_vote_history.insert_vote(
            &vote,
            &vote_account,
            // The slot in which the vote transaction landed
            6,
            transaction_signature,
            &vote_pubkey,
            ancestors.get(&vote_landed_slot).unwrap(),
        );

        // Only 5 > root 3 should be found
        for slot in vote_slots {
            if slot < root {
                assert!(validator_vote_history
                    .get_validator_vote(
                        slot,
                        vote_landed_slot,
                        &vote_pubkey,
                        ancestors.get(&vote_landed_slot).unwrap()
                    )
                    .is_none());
            } else {
                assert!(validator_vote_history
                    .get_validator_vote(
                        slot,
                        vote_landed_slot,
                        &vote_pubkey,
                        ancestors.get(&6).unwrap()
                    )
                    .is_some());
            }
        }

        // Set another root at 5, should purge 3
        validator_vote_history.set_root(5);
        assert!(validator_vote_history
            .get_validator_vote(
                3,
                vote_landed_slot,
                &vote_pubkey,
                ancestors.get(&vote_landed_slot).unwrap()
            )
            .is_none());
    }

    #[test]
    fn test_insert_vote() {
        let TestSetup {
            ancestors,
            mut validator_vote_history,
            vote_pubkey,
            mut vote_account,
            ..
        } = TestSetup::new();

        // Create votes that landed in slot 6
        let vote_landed_slot = 6;
        let vote_slots = vec![0, 3, 5];
        let vote = Vote::new(vote_slots.clone(), Hash::default());
        let transaction_signature1 =
            Signature::new(&(0..64).map(|_| rand::random::<u8>()).collect::<Vec<_>>());

        // Insert the vote for tracking
        validator_vote_history.insert_vote(
            &vote,
            &vote_account,
            // The slot in which the vote transaction landed
            vote_landed_slot,
            transaction_signature1,
            &vote_pubkey,
            ancestors.get(&vote_landed_slot).unwrap(),
        );

        // Commit the vote landing in the vote account
        simulate_vote_on_account(&mut vote_account, &vote);

        // Verify the vote slots were inserted correctly
        for &slot in &vote_slots {
            if slot == 0 {
                // Slot 0 will be popped off from the vote state so it won't exist
                assert!(validator_vote_history
                    .get_validator_vote(
                        slot,
                        vote_landed_slot,
                        &vote_pubkey,
                        ancestors.get(&vote_landed_slot).unwrap()
                    )
                    .is_none());
                continue;
            }
            let validator_vote = validator_vote_history
                .get_validator_vote(
                    slot,
                    vote_landed_slot,
                    &vote_pubkey,
                    ancestors.get(&vote_landed_slot).unwrap(),
                )
                .unwrap();
            assert_eq!(validator_vote.transaction_signature, transaction_signature1);
            assert!(validator_vote.next_votes.is_empty());
            assert_eq!(validator_vote.height, 1);
            // Only one vote transaction so far has increased lockout on this slot
            assert_eq!(validator_vote.increased_vote_lockout_transactions.len(), 1);
            let increased_lockout_vote = if slot == 3 {
                // Slot 3 should have 2 confirmations
                let (num_confirmations, increased_lockout_vote) = validator_vote_history
                    .get_confirmation_info(
                        &validator_vote,
                        2,
                        vote_landed_slot,
                        ancestors.get(&vote_landed_slot).unwrap(),
                    )
                    .unwrap();
                assert_eq!(num_confirmations, 2);
                increased_lockout_vote
            } else if slot == 5 {
                // Slot 5 should have 1 confirmation
                let (num_confirmations, increased_lockout_vote) = validator_vote_history
                    .get_confirmation_info(
                        &validator_vote,
                        1,
                        vote_landed_slot,
                        ancestors.get(&vote_landed_slot).unwrap(),
                    )
                    .unwrap();
                assert_eq!(num_confirmations, 1);
                increased_lockout_vote
            } else {
                panic!("No other option")
            };

            assert_eq!(
                increased_lockout_vote.transaction_signature,
                transaction_signature1
            );

            // [0, 3, 5] was the first vote, so should set all slots 0, 3, 5 should
            // reference 5 as the vote that initially increased the confirmations
            assert_eq!(increased_lockout_vote.representative_slot, 5,);
            assert_eq!(increased_lockout_vote.vote_landed_slot, vote_landed_slot);
            assert_eq!(increased_lockout_vote.num_total_votes, 1);
        }

        // Create votes that landed in slot 7, but with some votes that overlap previous vote
        let vote_slots = vec![0, 1, 3, 5, 6];
        let vote = Vote::new(vote_slots.clone(), Hash::default());
        let vote_landed_slot = 7;
        let transaction_signature2 =
            Signature::new(&(0..64).map(|_| rand::random::<u8>()).collect::<Vec<_>>());

        // Insert the vote for tracking
        validator_vote_history.insert_vote(
            &vote,
            &vote_account,
            // The slot in which the vote transaction landed
            vote_landed_slot,
            transaction_signature2,
            &vote_pubkey,
            ancestors.get(&vote_landed_slot).unwrap(),
        );

        // Verify the vote slots were inserted correctly
        for &slot in &vote_slots {
            if slot == 0 || slot == 1 {
                // Slot 0 will be popped off from the vote state so it won't exist
                // Slot 1 was earlier than last slot in previous vote transaction, 5,
                // so is ignored
                assert!(validator_vote_history
                    .get_slot_history(
                        slot,
                        vote_landed_slot,
                        &vote_pubkey,
                        1,
                        ancestors.get(&vote_landed_slot).unwrap()
                    )
                    .is_none());
                continue;
            }
            let max_confirmations = if slot == 3 {
                // Slot 3 should have 3 confirmations
                3
            } else if slot == 5 {
                // Slot 5 should have 2 confirmations
                2
            } else if slot == 6 {
                1
            } else {
                panic!("No other option")
            };

            for num_confirmations in 1..=max_confirmations + 1 {
                // Going beyond the `max_confirmations` should return `None`
                if num_confirmations == max_confirmations + 1 {
                    assert!(validator_vote_history
                        .get_slot_history(
                            slot,
                            vote_landed_slot,
                            &vote_pubkey,
                            num_confirmations,
                            ancestors.get(&vote_landed_slot).unwrap()
                        )
                        .is_none());
                    continue;
                }
                let expected = if slot == 3 {
                    // Slot 3 has confirmation levels: {2, 3}
                    if num_confirmations > 2 {
                        (vec![transaction_signature1, transaction_signature2], 3)
                    } else {
                        (vec![transaction_signature1], 2)
                    }
                } else if slot == 5 {
                    // Slot 5 has confirmations levels: {1, 2}
                    if num_confirmations > 1 {
                        (vec![transaction_signature1, transaction_signature2], 2)
                    } else {
                        (vec![transaction_signature1], 1)
                    }
                } else if slot == 6 {
                    // Slot 6 only has confirmation level: {1}
                    (vec![transaction_signature2], 1)
                } else {
                    panic!("No other option")
                };
                assert_eq!(
                    validator_vote_history
                        .get_slot_history(
                            slot,
                            vote_landed_slot,
                            &vote_pubkey,
                            num_confirmations,
                            ancestors.get(&vote_landed_slot).unwrap()
                        )
                        .unwrap(),
                    expected
                );
            }
        }
    }
}
