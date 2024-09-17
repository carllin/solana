//! Vote state, vote program
//! Receive and processes votes from validators
pub use solana_program::vote_new::state::{vote_state_versions::*, *};
use {
    log::*,
    serde_derive::{Deserialize, Serialize},
    solana_feature_set::{self as feature_set, FeatureSet},
    solana_program::vote_new::{error::VoteError, program::id},
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount, WritableAccount},
        clock::{Epoch, Slot, UnixTimestamp},
        epoch_schedule::EpochSchedule,
        hash::Hash,
        instruction::InstructionError,
        pubkey::Pubkey,
        rent::Rent,
        slot_hashes::SlotHash,
        sysvar::clock::Clock,
        transaction_context::{
            BorrowedAccount, IndexOfAccount, InstructionContext, TransactionContext,
        },
    },
    std::{
        cmp::Ordering,
        collections::{HashSet, VecDeque},
        fmt::Debug,
    },
};

#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample, AbiEnumVisitor),
    frozen_abi(digest = "4dbyMxwfCN43orGKa5YiyY1EqN2K97pTicNhKYTZSUQH")
)]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum VoteTransaction {
    Vote(Vote),
}

impl VoteTransaction {
    pub fn hash(&self) -> Hash {
        match self {
            VoteTransaction::Vote(vote) => vote.hash,
        }
    }

    pub fn timestamp(&self) -> Option<UnixTimestamp> {
        match self {
            VoteTransaction::Vote(vote) => vote.timestamp,
        }
    }

    pub fn set_timestamp(&mut self, ts: Option<UnixTimestamp>) {
        match self {
            VoteTransaction::Vote(vote) => vote.timestamp = ts,
        }
    }

    pub fn slot(&self) -> Slot {
        match self {
            VoteTransaction::Vote(vote) => vote.slot(),
        }
    }

    pub fn slot_hash(&self) -> (Slot, Hash) {
        (self.slot(), self.hash())
    }
}

impl From<Vote> for VoteTransaction {
    fn from(vote: Vote) -> Self {
        VoteTransaction::Vote(vote)
    }
}

// utility function, used by Stakes, tests
pub fn from<T: ReadableAccount>(account: &T) -> Option<VoteState> {
    VoteState::deserialize(account.data()).ok()
}

// utility function, used by Stakes, tests
pub fn to<T: WritableAccount>(versioned: &VoteStateVersions, account: &mut T) -> Option<()> {
    VoteState::serialize(versioned, account.data_as_mut_slice()).ok()
}

// Updates the vote account state with a new VoteState instance.  This is required temporarily during the
// upgrade of vote account state from V1_14_11 to Current.
fn set_vote_account_state(
    vote_account: &mut BorrowedAccount,
    vote_state: VoteState,
) -> Result<(), InstructionError> {
    vote_account.set_state(&VoteStateVersions::new_current(vote_state))
}

/// Checks the proposed vote state with the current and
/// slot hashes, making adjustments to the root / filtering
/// votes as needed.
fn check_vote_is_valid(
    vote_state: &VoteState,
    proposed_range: &VoteRange,
    //proposed_root: &mut Option<Slot>,
    proposed_hash: Hash,
    slot_hashes: &[(Slot, Hash)],
) -> Result<(), VoteError> {
    let last_proposed_slot = proposed_range.slot;
    let last_vote_slot = vote_state.slot();
    let last_proposed_reference_slot = proposed_range.reference_slot;
    let last_vote_reference_slot = vote_state.vote_range.reference_slot;

    if last_proposed_slot < last_proposed_reference_slot {
        return Err(VoteError::SlotsNotOrdered);
    }

    // If the proposed state is not new enough, return
    if last_proposed_slot <= last_vote_slot {
        return Err(VoteError::VoteTooOld);
    }

    // If the reference slot goes backwards, return
    if last_proposed_reference_slot < last_vote_reference_slot {
        return Err(VoteError::OverlappingVote);
    }

    // If the vote overlaps a previous vote, return
    if last_proposed_reference_slot <= last_vote_slot
        && last_proposed_reference_slot != last_vote_reference_slot
    {
        return Err(VoteError::OverlappingVote);
    }

    if slot_hashes.is_empty() {
        return Err(VoteError::SlotsMismatch);
    }
    let earliest_slot_hash_in_history = slot_hashes.last().unwrap().0;

    // Check if the proposed vote state is too old to be in the SlotHash history
    if last_proposed_slot < earliest_slot_hash_in_history
        || (last_proposed_reference_slot != last_vote_reference_slot
            && last_proposed_reference_slot < earliest_slot_hash_in_history)
    {
        // If this is the last slot in the vote update, it must be in SlotHashes,
        // otherwise we have no way of confirming if the hash matches
        return Err(VoteError::VoteTooOld);
    }

    // Overwrite the proposed root if it is too old to be in the SlotHash history
    /*if let Some(root) = *proposed_root {
        // If the new proposed root `R` is less than the earliest slot hash in the history
        // such that we cannot verify whether the slot was actually was on this fork, set
        // the root to the latest vote in the vote state that's less than R. If no
        // votes from the vote state are less than R, use its root instead.
        if root < earliest_slot_hash_in_history {
            // First overwrite the proposed root with the vote state's root
            *proposed_root = vote_state.root_slot;
        }
    }*/

    // Check the proposed root, slot and reference slot exist in the slot history
    let search_result =
        slot_hashes.binary_search_by(|probe| probe.0.cmp(&last_proposed_slot).reverse() /*reverse necessary here because SlotHasehs is sorted from greatest to least*/);
    if search_result.is_err() {
        return Err(VoteError::SlotsMismatch);
    }

    if slot_hashes[search_result.unwrap()].1 != proposed_hash {
        return Err(VoteError::SlotHashMismatch);
    }

    /*if !slot_hashes.contains(proposed_root) {
        return Err(VoteError::RootOnDifferentFork);
    }*/

    Ok(())
}

pub fn process_vote(
    vote_state: &mut VoteState,
    vote: &Vote,
    slot_hashes: &[SlotHash],
    epoch: Epoch,
    current_slot: Slot,
    timely_vote_credits: bool,
) -> Result<(), VoteError> {
    check_vote_is_valid(vote_state, &vote.vote_range, vote.hash, slot_hashes)?;
    vote_state.process_vote(&vote.vote_range, epoch, current_slot, timely_vote_credits);
    Ok(())
}

/// "unchecked" functions used by tests and Tower
pub fn process_vote_unchecked(vote_state: &mut VoteState, vote: Vote) -> Result<(), VoteError> {
    let slots = vec![vote.vote_range.reference_slot, vote.slot()];
    let slot_hashes: Vec<_> = slots.iter().rev().map(|x| (*x, vote.hash)).collect();
    process_vote(
        vote_state,
        &vote,
        &slot_hashes,
        vote_state.current_epoch(),
        0,
        true,
    )
}

#[cfg(test)]
pub fn process_slot_votes_unchecked(vote_state: &mut VoteState, slots: &[Slot]) {
    for slot in slots {
        process_slot_vote_unchecked(vote_state, 0, *slot);
    }
}

pub fn process_slot_vote_unchecked(vote_state: &mut VoteState, reference_slot: Slot, slot: Slot) {
    let _ = process_vote_unchecked(
        vote_state,
        Vote::new(VoteRange::new(reference_slot, slot), Hash::default()),
    );
}

/// Authorize the given pubkey to withdraw or sign votes. This may be called multiple times,
/// but will implicitly withdraw authorization from the previously authorized
/// key
pub fn authorize<S: std::hash::BuildHasher>(
    vote_account: &mut BorrowedAccount,
    authorized: &Pubkey,
    vote_authorize: VoteAuthorize,
    signers: &HashSet<Pubkey, S>,
    clock: &Clock,
) -> Result<(), InstructionError> {
    let mut vote_state: VoteState = vote_account
        .get_state::<VoteStateVersions>()?
        .convert_to_current();

    match vote_authorize {
        VoteAuthorize::Voter => {
            let authorized_withdrawer_signer =
                verify_authorized_signer(&vote_state.authorized_withdrawer, signers).is_ok();
            vote_state.set_new_authorized_voter(
                authorized,
                clock.epoch,
                clock
                    .leader_schedule_epoch
                    .checked_add(1)
                    .ok_or(InstructionError::InvalidAccountData)?,
                |epoch_authorized_voter| {
                    // current authorized withdrawer or authorized voter must say "yay"
                    if authorized_withdrawer_signer {
                        Ok(())
                    } else {
                        verify_authorized_signer(&epoch_authorized_voter, signers)
                    }
                },
            )?;
        }
        VoteAuthorize::Withdrawer => {
            // current authorized withdrawer must say "yay"
            verify_authorized_signer(&vote_state.authorized_withdrawer, signers)?;
            vote_state.authorized_withdrawer = *authorized;
        }
    }

    set_vote_account_state(vote_account, vote_state)
}

/// Update the node_pubkey, requires signature of the authorized voter
pub fn update_validator_identity<S: std::hash::BuildHasher>(
    vote_account: &mut BorrowedAccount,
    node_pubkey: &Pubkey,
    signers: &HashSet<Pubkey, S>,
) -> Result<(), InstructionError> {
    let mut vote_state: VoteState = vote_account
        .get_state::<VoteStateVersions>()?
        .convert_to_current();

    // current authorized withdrawer must say "yay"
    verify_authorized_signer(&vote_state.authorized_withdrawer, signers)?;

    // new node must say "yay"
    verify_authorized_signer(node_pubkey, signers)?;

    vote_state.node_pubkey = *node_pubkey;

    set_vote_account_state(vote_account, vote_state)
}

/// Update the vote account's commission
pub fn update_commission<S: std::hash::BuildHasher>(
    vote_account: &mut BorrowedAccount,
    commission: u8,
    signers: &HashSet<Pubkey, S>,
    epoch_schedule: &EpochSchedule,
    clock: &Clock,
    feature_set: &FeatureSet,
) -> Result<(), InstructionError> {
    // Decode vote state only once, and only if needed
    let mut vote_state = None;

    let enforce_commission_update_rule =
        if feature_set.is_active(&feature_set::allow_commission_decrease_at_any_time::id()) {
            if let Ok(decoded_vote_state) = vote_account.get_state::<VoteStateVersions>() {
                vote_state = Some(decoded_vote_state.convert_to_current());
                is_commission_increase(vote_state.as_ref().unwrap(), commission)
            } else {
                true
            }
        } else {
            true
        };

    #[allow(clippy::collapsible_if)]
    if enforce_commission_update_rule
        && feature_set
            .is_active(&feature_set::commission_updates_only_allowed_in_first_half_of_epoch::id())
    {
        if !is_commission_update_allowed(clock.slot, epoch_schedule) {
            return Err(VoteError::CommissionUpdateTooLate.into());
        }
    }

    let mut vote_state = match vote_state {
        Some(vote_state) => vote_state,
        None => vote_account
            .get_state::<VoteStateVersions>()?
            .convert_to_current(),
    };

    // current authorized withdrawer must say "yay"
    verify_authorized_signer(&vote_state.authorized_withdrawer, signers)?;

    vote_state.commission = commission;

    set_vote_account_state(vote_account, vote_state)
}

/// Given a proposed new commission, returns true if this would be a commission increase, false otherwise
pub fn is_commission_increase(vote_state: &VoteState, commission: u8) -> bool {
    commission > vote_state.commission
}

/// Given the current slot and epoch schedule, determine if a commission change
/// is allowed
pub fn is_commission_update_allowed(slot: Slot, epoch_schedule: &EpochSchedule) -> bool {
    // always allowed during warmup epochs
    if let Some(relative_slot) = slot
        .saturating_sub(epoch_schedule.first_normal_slot)
        .checked_rem(epoch_schedule.slots_per_epoch)
    {
        // allowed up to the midpoint of the epoch
        relative_slot.saturating_mul(2) <= epoch_schedule.slots_per_epoch
    } else {
        // no slots per epoch, just allow it, even though this should never happen
        true
    }
}

fn verify_authorized_signer<S: std::hash::BuildHasher>(
    authorized: &Pubkey,
    signers: &HashSet<Pubkey, S>,
) -> Result<(), InstructionError> {
    if signers.contains(authorized) {
        Ok(())
    } else {
        Err(InstructionError::MissingRequiredSignature)
    }
}

/// Withdraw funds from the vote account
pub fn withdraw<S: std::hash::BuildHasher>(
    transaction_context: &TransactionContext,
    instruction_context: &InstructionContext,
    vote_account_index: IndexOfAccount,
    lamports: u64,
    to_account_index: IndexOfAccount,
    signers: &HashSet<Pubkey, S>,
    rent_sysvar: &Rent,
    clock: &Clock,
) -> Result<(), InstructionError> {
    let mut vote_account = instruction_context
        .try_borrow_instruction_account(transaction_context, vote_account_index)?;
    let vote_state: VoteState = vote_account
        .get_state::<VoteStateVersions>()?
        .convert_to_current();

    verify_authorized_signer(&vote_state.authorized_withdrawer, signers)?;

    let remaining_balance = vote_account
        .get_lamports()
        .checked_sub(lamports)
        .ok_or(InstructionError::InsufficientFunds)?;

    if remaining_balance == 0 {
        let reject_active_vote_account_close = vote_state
            .epoch_credits
            .last()
            .map(|(last_epoch_with_credits, _, _)| {
                let current_epoch = clock.epoch;
                // if current_epoch - last_epoch_with_credits < 2 then the validator has received credits
                // either in the current epoch or the previous epoch. If it's >= 2 then it has been at least
                // one full epoch since the validator has received credits.
                current_epoch.saturating_sub(*last_epoch_with_credits) < 2
            })
            .unwrap_or(false);

        if reject_active_vote_account_close {
            return Err(VoteError::ActiveVoteAccountClose.into());
        } else {
            // Deinitialize upon zero-balance
            set_vote_account_state(&mut vote_account, VoteState::default())?;
        }
    } else {
        let min_rent_exempt_balance = rent_sysvar.minimum_balance(vote_account.get_data().len());
        if remaining_balance < min_rent_exempt_balance {
            return Err(InstructionError::InsufficientFunds);
        }
    }

    vote_account.checked_sub_lamports(lamports)?;
    drop(vote_account);
    let mut to_account = instruction_context
        .try_borrow_instruction_account(transaction_context, to_account_index)?;
    to_account.checked_add_lamports(lamports)?;
    Ok(())
}

/// Initialize the vote_state for a vote account
/// Assumes that the account is being init as part of a account creation or balance transfer and
/// that the transaction must be signed by the staker's keys
pub fn initialize_account<S: std::hash::BuildHasher>(
    vote_account: &mut BorrowedAccount,
    vote_init: &VoteInit,
    signers: &HashSet<Pubkey, S>,
    clock: &Clock,
) -> Result<(), InstructionError> {
    if vote_account.get_data().len() != VoteStateVersions::vote_state_size_of() {
        return Err(InstructionError::InvalidAccountData);
    }
    let versioned = vote_account.get_state::<VoteStateVersions>()?;

    if !versioned.is_uninitialized() {
        return Err(InstructionError::AccountAlreadyInitialized);
    }

    // node must agree to accept this vote account
    verify_authorized_signer(&vote_init.node_pubkey, signers)?;

    set_vote_account_state(vote_account, VoteState::new(vote_init, clock))
}

fn verify_and_get_vote_state<S: std::hash::BuildHasher>(
    vote_account: &BorrowedAccount,
    clock: &Clock,
    signers: &HashSet<Pubkey, S>,
) -> Result<VoteState, InstructionError> {
    let versioned = vote_account.get_state::<VoteStateVersions>()?;

    if versioned.is_uninitialized() {
        return Err(InstructionError::UninitializedAccount);
    }

    let mut vote_state = versioned.convert_to_current();
    let authorized_voter = vote_state.get_and_update_authorized_voter(clock.epoch)?;
    verify_authorized_signer(&authorized_voter, signers)?;

    Ok(vote_state)
}

pub fn process_vote_with_account<S: std::hash::BuildHasher>(
    vote_account: &mut BorrowedAccount,
    slot_hashes: &[SlotHash],
    clock: &Clock,
    vote: &Vote,
    signers: &HashSet<Pubkey, S>,
    feature_set: &FeatureSet,
) -> Result<(), InstructionError> {
    let mut vote_state = verify_and_get_vote_state(vote_account, clock, signers)?;
    let timely_vote_credits = feature_set.is_active(&feature_set::timely_vote_credits::id());
    process_vote(
        &mut vote_state,
        vote,
        slot_hashes,
        clock.epoch,
        clock.slot,
        timely_vote_credits,
    )?;
    if let Some(timestamp) = vote.timestamp {
        vote_state.process_timestamp(vote.slot(), timestamp);
    }
    set_vote_account_state(vote_account, vote_state)
}

// This function is used:
// a. In many tests.
// b. In the genesis tool that initializes a cluster to create the bootstrap validator.
// c. In the ledger tool when creating bootstrap vote accounts.
pub fn create_account_with_authorized(
    node_pubkey: &Pubkey,
    authorized_voter: &Pubkey,
    authorized_withdrawer: &Pubkey,
    commission: u8,
    lamports: u64,
) -> AccountSharedData {
    let mut vote_account = AccountSharedData::new(lamports, VoteState::size_of(), &id());

    let vote_state = VoteState::new(
        &VoteInit {
            node_pubkey: *node_pubkey,
            authorized_voter: *authorized_voter,
            authorized_withdrawer: *authorized_withdrawer,
            commission,
        },
        &Clock::default(),
    );

    VoteState::serialize(
        &VoteStateVersions::Current(Box::new(vote_state)),
        vote_account.data_as_mut_slice(),
    )
    .unwrap();

    vote_account
}

// create_account() should be removed, use create_account_with_authorized() instead
pub fn create_account(
    vote_pubkey: &Pubkey,
    node_pubkey: &Pubkey,
    commission: u8,
    lamports: u64,
) -> AccountSharedData {
    create_account_with_authorized(node_pubkey, vote_pubkey, vote_pubkey, commission, lamports)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::vote_state_new,
        assert_matches::assert_matches,
        solana_sdk::{
            account::AccountSharedData, account_utils::StateMut, clock::DEFAULT_SLOTS_PER_EPOCH,
            hash::hash, transaction_context::InstructionAccount, vote_new::*,
        },
        std::cell::RefCell,
        test_case::test_case,
    };

    const MAX_RECENT_VOTES: usize = 16;

    fn vote_state_new_for_test(auth_pubkey: &Pubkey) -> VoteState {
        VoteState::new(
            &VoteInit {
                node_pubkey: solana_sdk::pubkey::new_rand(),
                authorized_voter: *auth_pubkey,
                authorized_withdrawer: *auth_pubkey,
                commission: 0,
            },
            &Clock::default(),
        )
    }

    fn create_test_account() -> (Pubkey, RefCell<AccountSharedData>) {
        let rent = Rent::default();
        let balance = VoteState::get_rent_exempt_reserve(&rent);
        let vote_pubkey = solana_sdk::pubkey::new_rand();
        (
            vote_pubkey,
            RefCell::new(vote_state_new::create_account_with_authorized(
                &solana_sdk::pubkey::new_rand(),
                &vote_pubkey,
                &vote_pubkey,
                0,
                balance,
            )),
        )
    }

    #[test]
    fn test_update_commission() {
        let node_pubkey = Pubkey::new_unique();
        let withdrawer_pubkey = Pubkey::new_unique();
        let clock = Clock::default();
        let vote_state = VoteState::new(
            &VoteInit {
                node_pubkey,
                authorized_voter: withdrawer_pubkey,
                authorized_withdrawer: withdrawer_pubkey,
                commission: 10,
            },
            &clock,
        );

        let serialized =
            bincode::serialize(&VoteStateVersions::Current(Box::new(vote_state.clone()))).unwrap();
        let serialized_len = serialized.len();
        let rent = Rent::default();
        let lamports = rent.minimum_balance(serialized_len);
        let mut vote_account = AccountSharedData::new(lamports, serialized_len, &id());
        vote_account.set_data_from_slice(&serialized);

        // Create a fake TransactionContext with a fake InstructionContext with a single account which is the
        // vote account that was just created
        let processor_account = AccountSharedData::new(0, 0, &solana_sdk::native_loader::id());
        let transaction_context = TransactionContext::new(
            vec![(id(), processor_account), (node_pubkey, vote_account)],
            rent,
            0,
            0,
        );
        let mut instruction_context = InstructionContext::default();
        instruction_context.configure(
            &[0],
            &[InstructionAccount {
                index_in_transaction: 1,
                index_in_caller: 1,
                index_in_callee: 0,
                is_signer: false,
                is_writable: true,
            }],
            &[],
        );

        // Get the BorrowedAccount from the InstructionContext which is what is used to manipulate and inspect account
        // state
        let mut borrowed_account = instruction_context
            .try_borrow_instruction_account(&transaction_context, 0)
            .unwrap();

        let epoch_schedule = std::sync::Arc::new(EpochSchedule::without_warmup());

        let first_half_clock = std::sync::Arc::new(Clock {
            slot: epoch_schedule.slots_per_epoch / 4,
            ..Clock::default()
        });

        let second_half_clock = std::sync::Arc::new(Clock {
            slot: (epoch_schedule.slots_per_epoch * 3) / 4,
            ..Clock::default()
        });

        let mut feature_set = FeatureSet::default();
        feature_set.activate(
            &feature_set::commission_updates_only_allowed_in_first_half_of_epoch::id(),
            1,
        );

        let signers: HashSet<Pubkey> = vec![withdrawer_pubkey].into_iter().collect();

        // Increase commission in first half of epoch -- allowed
        assert_eq!(
            borrowed_account
                .get_state::<VoteStateVersions>()
                .unwrap()
                .convert_to_current()
                .commission,
            10
        );
        assert_matches!(
            update_commission(
                &mut borrowed_account,
                11,
                &signers,
                &epoch_schedule,
                &first_half_clock,
                &feature_set
            ),
            Ok(())
        );
        assert_eq!(
            borrowed_account
                .get_state::<VoteStateVersions>()
                .unwrap()
                .convert_to_current()
                .commission,
            11
        );

        // Increase commission in second half of epoch -- disallowed
        assert_matches!(
            update_commission(
                &mut borrowed_account,
                12,
                &signers,
                &epoch_schedule,
                &second_half_clock,
                &feature_set
            ),
            Err(_)
        );
        assert_eq!(
            borrowed_account
                .get_state::<VoteStateVersions>()
                .unwrap()
                .convert_to_current()
                .commission,
            11
        );

        // Decrease commission in first half of epoch -- allowed
        assert_matches!(
            update_commission(
                &mut borrowed_account,
                10,
                &signers,
                &epoch_schedule,
                &first_half_clock,
                &feature_set
            ),
            Ok(())
        );
        assert_eq!(
            borrowed_account
                .get_state::<VoteStateVersions>()
                .unwrap()
                .convert_to_current()
                .commission,
            10
        );

        // Decrease commission in second half of epoch -- disallowed because feature_set does not allow it
        assert_matches!(
            update_commission(
                &mut borrowed_account,
                9,
                &signers,
                &epoch_schedule,
                &second_half_clock,
                &feature_set
            ),
            Err(_)
        );
        assert_eq!(
            borrowed_account
                .get_state::<VoteStateVersions>()
                .unwrap()
                .convert_to_current()
                .commission,
            10
        );

        // Decrease commission in second half of epoch -- allowed because feature_set allows it
        feature_set.activate(&feature_set::allow_commission_decrease_at_any_time::id(), 1);
        assert_matches!(
            update_commission(
                &mut borrowed_account,
                9,
                &signers,
                &epoch_schedule,
                &second_half_clock,
                &feature_set
            ),
            Ok(())
        );
        assert_eq!(
            borrowed_account
                .get_state::<VoteStateVersions>()
                .unwrap()
                .convert_to_current()
                .commission,
            9
        );
    }

    #[test]
    fn test_duplicate_vote() {
        let voter_pubkey = solana_sdk::pubkey::new_rand();
        let mut vote_state = vote_state_new_for_test(&voter_pubkey);
        process_slot_vote_unchecked(&mut vote_state, 0, 0);
        process_slot_vote_unchecked(&mut vote_state, 0, 1);
        process_slot_vote_unchecked(&mut vote_state, 0, 0);
        assert_eq!(vote_state.slot(), 1);
    }

    #[test]
    fn test_check_vote_is_valid_new_vote() {
        let vote_state = VoteState::default();
        let vote = Vote::new(VoteRange::new(0, 1), Hash::default());
        let slot_hashes: Vec<_> = vec![(vote.slot(), vote.hash)];
        assert_eq!(
            check_vote_is_valid(&vote_state, &vote.vote_range, vote.hash, &slot_hashes),
            Ok(())
        );
    }

    #[test]
    fn test_check_vote_is_valid_bad_hash() {
        let vote_state = VoteState::default();
        let vote = Vote::new(VoteRange::new(0, 1), Hash::default());
        let slot_hashes: Vec<_> = vec![(vote.slot(), hash(vote.hash.as_ref()))];
        assert_eq!(
            check_vote_is_valid(&vote_state, &vote.vote_range, vote.hash, &slot_hashes),
            Err(VoteError::SlotHashMismatch)
        );
    }

    #[test]
    fn test_check_vote_is_valid_bad_slot() {
        let vote_state = VoteState::default();

        let vote = Vote::new(VoteRange::new(0, 1), Hash::default());
        let slot_hashes: Vec<_> = vec![(0, vote.hash)];
        assert_eq!(
            check_vote_is_valid(&vote_state, &vote.vote_range, vote.hash, &slot_hashes),
            Err(VoteError::SlotsMismatch)
        );
    }

    #[test]
    fn test_check_vote_is_valid_duplicate_vote() {
        let mut vote_state = VoteState::default();
        let vote = Vote::new(VoteRange::new(0, 1), Hash::default());
        // Doesn't need to include 0 in slot hashes because the default
        // vote already has the reference slot set to 0, so that slot
        // will skip the SlotHashes verification
        let slot_hashes: Vec<_> = vec![(vote.slot(), vote.hash)];
        assert_eq!(
            process_vote(&mut vote_state, &vote, &slot_hashes, 0, 0, true),
            Ok(())
        );
        assert_eq!(
            check_vote_is_valid(&vote_state, &vote.vote_range, vote.hash, &slot_hashes),
            Err(VoteError::VoteTooOld)
        );
    }

    #[test]
    fn test_check_vote_is_valid_next_vote() {
        let mut vote_state = VoteState::default();
        let vote = Vote::new(VoteRange::new(0, 1), Hash::default());
        let slot_hashes: Vec<_> = vec![(vote.slot(), vote.hash)];
        assert_eq!(
            process_vote(&mut vote_state, &vote, &slot_hashes, 0, 0, true),
            Ok(())
        );

        let vote = Vote::new(VoteRange::new(0, 2), Hash::default());
        let slot_hashes: Vec<_> = vec![(vote.slot(), vote.hash)];
        assert_eq!(
            check_vote_is_valid(&vote_state, &vote.vote_range, vote.hash, &slot_hashes),
            Ok(())
        );
    }

    /*#[test]
    fn test_check_vote_is_valid_slot_hash_mismatch() {
        let slot_hashes = build_slot_hashes(vec![2, 4, 6, 8]);
        let vote_state = build_vote_state(vec![2, 4, 6], &slot_hashes);

        // Test with a `TowerSync` where the hash is mismatched

        // Have to vote for a slot greater than the last vote in the vote state to avoid VoteTooOld
        // errors
        let vote_slot = vote_state.votes.back().unwrap().slot() + 2;
        let vote_slot_hash = Hash::new_unique();
        let mut tower_sync = TowerSync::from(vec![(2, 4), (4, 3), (6, 2), (vote_slot, 1)]);
        tower_sync.hash = vote_slot_hash;
        assert_eq!(
            check_vote_is_valid(
                &vote_state,
                &mut tower_sync.lockouts,
                &mut tower_sync.root,
                tower_sync.hash,
                &slot_hashes,
            ),
            Err(VoteError::SlotHashMismatch),
        );
    }*/

    #[test_case(0, true; "first slot")]
    #[test_case(DEFAULT_SLOTS_PER_EPOCH / 2, true; "halfway through epoch")]
    #[test_case((DEFAULT_SLOTS_PER_EPOCH / 2).saturating_add(1), false; "halfway through epoch plus one")]
    #[test_case(DEFAULT_SLOTS_PER_EPOCH.saturating_sub(1), false; "last slot in epoch")]
    #[test_case(DEFAULT_SLOTS_PER_EPOCH, true; "first slot in second epoch")]
    fn test_epoch_half_check(slot: Slot, expected_allowed: bool) {
        let epoch_schedule = EpochSchedule::without_warmup();
        assert_eq!(
            is_commission_update_allowed(slot, &epoch_schedule),
            expected_allowed
        );
    }

    #[test]
    fn test_warmup_epoch_half_check_with_warmup() {
        let epoch_schedule = EpochSchedule::default();
        let first_normal_slot = epoch_schedule.first_normal_slot;
        // first slot works
        assert!(is_commission_update_allowed(0, &epoch_schedule));
        // right before first normal slot works, since all warmup slots allow
        // commission updates
        assert!(is_commission_update_allowed(
            first_normal_slot - 1,
            &epoch_schedule
        ));
    }

    #[test_case(0, true; "first slot")]
    #[test_case(DEFAULT_SLOTS_PER_EPOCH / 2, true; "halfway through epoch")]
    #[test_case((DEFAULT_SLOTS_PER_EPOCH / 2).saturating_add(1), false; "halfway through epoch plus one")]
    #[test_case(DEFAULT_SLOTS_PER_EPOCH.saturating_sub(1), false; "last slot in epoch")]
    #[test_case(DEFAULT_SLOTS_PER_EPOCH, true; "first slot in second epoch")]
    fn test_epoch_half_check_with_warmup(slot: Slot, expected_allowed: bool) {
        let epoch_schedule = EpochSchedule::default();
        let first_normal_slot = epoch_schedule.first_normal_slot;
        assert_eq!(
            is_commission_update_allowed(first_normal_slot.saturating_add(slot), &epoch_schedule),
            expected_allowed
        );
    }
}
