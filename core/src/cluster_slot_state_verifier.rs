use crate::{
    fork_choice::ForkChoice, heaviest_subtree_fork_choice::HeaviestSubtreeForkChoice,
    progress_map::ProgressMap,
};
use solana_ledger::blockstore::Blockstore;
use solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub(crate) type DuplicateSlotsTracker = BTreeSet<Slot>;
pub(crate) type DuplicateSlotsToRepair = HashMap<Slot, Hash>;
pub(crate) type GossipDuplicateConfirmedSlots = BTreeMap<Slot, Hash>;
pub(crate) type EpochSlotsFrozenSlots = BTreeMap<Slot, Hash>;
type SlotStateHandler = fn(SlotStateHandlerArgs) -> Vec<ResultingStateChange>;

#[derive(Clone)]
struct SlotStateHandlerArgs<'a> {
    pubkey: Pubkey,
    slot: Slot,
    bank_frozen_hash: Hash,
    cluster_duplicate_confirmed_hash: Option<&'a Hash>,
    is_slot_duplicate: bool,
    is_dead: bool,
    epoch_slots_frozen_hash: Option<&'a Hash>,
    slot_state_update: SlotStateUpdate,
}

impl<'a> SlotStateHandlerArgs<'a> {
    #[cfg(test)]
    fn new_dead_slot_args(
        slot: Slot,
        cluster_duplicate_confirmed_hash: Option<&'a Hash>,
        is_slot_duplicate: bool,
        epoch_slots_frozen_hash: Option<&'a Hash>,
    ) -> Self {
        Self {
            pubkey: Pubkey::default(),
            slot,
            // Dead slot cannot have a non-default frozen hash because
            // it was never frozen
            bank_frozen_hash: Hash::default(),
            cluster_duplicate_confirmed_hash,
            is_slot_duplicate,
            is_dead: true,
            epoch_slots_frozen_hash,
            slot_state_update: SlotStateUpdate::Dead,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SlotStateUpdate {
    BankFrozen,
    DuplicateConfirmed,
    Dead,
    Duplicate,
    EpochSlotsFrozen(Hash),
}

#[derive(PartialEq, Debug)]
pub enum ResultingStateChange {
    // Bank was frozen
    BankFrozen(Hash),
    // Hash of our current frozen version of the slot
    MarkSlotDuplicate(Hash),
    // Hash of the either:
    // 1) Cluster duplicate confirmed slot
    // 2) Epoch Slots frozen sampled slot
    // that is not equivalent to our frozen version of the slot
    RepairClusterVersion(Hash),
    // Hash of our current frozen version of the slot
    DuplicateConfirmedSlotMatchesCluster(Hash),
    // Hash returned from EpochSlots does not match the duplicate
    // confirmed hash detected from replay or gossip.
    EpochSlotsMismatchedHash,
}

impl SlotStateUpdate {
    fn to_handler(&self) -> SlotStateHandler {
        match self {
            SlotStateUpdate::Dead => on_dead_slot,
            SlotStateUpdate::BankFrozen => on_frozen_slot,
            SlotStateUpdate::DuplicateConfirmed => on_cluster_update,
            SlotStateUpdate::Duplicate => on_cluster_update,
            SlotStateUpdate::EpochSlotsFrozen(_) => on_cluster_update,
        }
    }
}

fn on_dead_slot(args: SlotStateHandlerArgs) -> Vec<ResultingStateChange> {
    let SlotStateHandlerArgs {
        pubkey,
        slot,
        bank_frozen_hash,
        cluster_duplicate_confirmed_hash,
        is_dead,
        epoch_slots_frozen_hash,
        slot_state_update,
        ..
    } = args;

    match slot_state_update {
        SlotStateUpdate::DuplicateConfirmed
        | SlotStateUpdate::Dead
        | SlotStateUpdate::EpochSlotsFrozen(_)
        | SlotStateUpdate::Duplicate => (),
        SlotStateUpdate::BankFrozen => panic!("Invalid transition!"),
    }

    assert!(is_dead);
    // Bank should not have been frozen if the slot was marked dead
    assert_eq!(bank_frozen_hash, Hash::default());
    if let Some(cluster_duplicate_confirmed_hash) = cluster_duplicate_confirmed_hash {
        match slot_state_update {
            SlotStateUpdate::DuplicateConfirmed | SlotStateUpdate::Dead => {
                // If the cluster duplicate confirmed some version of this slot, then
                // we know there's another version, and our version is incorrect.
                warn!(
                    "{} Cluster duplicate_confirmed slot {} with hash {}, but we marked slot dead",
                    pubkey, slot, cluster_duplicate_confirmed_hash
                );
                // No need to check `is_slot_duplicate` and modify fork choice as dead slots
                // are never frozen, and thus never added to fork choice. The state change for
                // `MarkSlotDuplicate` will try to modify fork choice, but won't find the slot
                // in the fork choice tree, so is equivalent to a no-op
                vec![
                    ResultingStateChange::MarkSlotDuplicate(Hash::default()),
                    ResultingStateChange::RepairClusterVersion(*cluster_duplicate_confirmed_hash),
                ]
            }
            SlotStateUpdate::EpochSlotsFrozen(_) | SlotStateUpdate::Duplicate => vec![],
            SlotStateUpdate::BankFrozen => panic!("Invalid transition!"),
        }
    } else if let Some(epoch_slots_frozen_hash) = epoch_slots_frozen_hash {
        match slot_state_update {
            SlotStateUpdate::Dead | SlotStateUpdate::EpochSlotsFrozen(_) => {
                // Lower priority than having seen an actual confirmed hash in the `if` case above
                warn!(
                    "EpochSlots sample returned slot {} with hash {}, but we marked slot dead",
                    slot, epoch_slots_frozen_hash
                );
                vec![ResultingStateChange::RepairClusterVersion(
                    *epoch_slots_frozen_hash,
                )]
            }
            SlotStateUpdate::DuplicateConfirmed | SlotStateUpdate::Duplicate => vec![],
            SlotStateUpdate::BankFrozen => panic!("Invalid transition!"),
        }
    } else {
        vec![]
    }
}

fn on_frozen_slot(args: SlotStateHandlerArgs) -> Vec<ResultingStateChange> {
    let SlotStateHandlerArgs {
        pubkey,
        slot,
        bank_frozen_hash,
        cluster_duplicate_confirmed_hash,
        is_slot_duplicate,
        is_dead,
        epoch_slots_frozen_hash,
        slot_state_update,
    } = args;
    // If a slot is marked frozen, the bank hash should not be default,
    // and the slot should not be dead
    assert!(bank_frozen_hash != Hash::default());
    assert!(!is_dead);

    match slot_state_update {
        SlotStateUpdate::DuplicateConfirmed
        | SlotStateUpdate::BankFrozen
        | SlotStateUpdate::EpochSlotsFrozen(_)
        | SlotStateUpdate::Duplicate => (),
        SlotStateUpdate::Dead => panic!("Invalid transition!"),
    }

    let mut state_changes = if slot_state_update == SlotStateUpdate::BankFrozen {
        vec![ResultingStateChange::BankFrozen(bank_frozen_hash)]
    } else {
        vec![]
    };

    if let Some(cluster_duplicate_confirmed_hash) = cluster_duplicate_confirmed_hash {
        // If the cluster duplicate_confirmed some version of this slot, then
        // confirm our version agrees with the cluster
        match slot_state_update {
            SlotStateUpdate::BankFrozen | SlotStateUpdate::DuplicateConfirmed => {
                if *cluster_duplicate_confirmed_hash != bank_frozen_hash {
                    // If the versions do not match, modify fork choice rule
                    // to exclude our version from being voted on and also
                    // repair correct version
                    warn!(
                        "{} Cluster duplicate_confirmed slot {} with hash {}, but we froze slot with hash {}",
                        pubkey, slot, cluster_duplicate_confirmed_hash, bank_frozen_hash
                    );
                    state_changes.push(ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash));
                    state_changes.push(ResultingStateChange::RepairClusterVersion(
                        *cluster_duplicate_confirmed_hash,
                    ));
                } else {
                    // If the versions match, then add the slot to the candidate
                    // set to account for the case where it was removed earlier
                    // by the `on_duplicate_slot()` handler
                    state_changes.push(ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(
                        bank_frozen_hash,
                    ));
                }
            }
            SlotStateUpdate::EpochSlotsFrozen(_) | SlotStateUpdate::Duplicate => {}
            SlotStateUpdate::Dead => panic!("Invalid transition!"),
        }
    } else if let Some(epoch_slots_frozen_hash) = epoch_slots_frozen_hash {
        match slot_state_update {
            SlotStateUpdate::BankFrozen | SlotStateUpdate::EpochSlotsFrozen(_) => {
                if *epoch_slots_frozen_hash != bank_frozen_hash {
                    warn!(
                        "EpochSlots sample returned slot {} with hash {}, but we froze slot
                        with hash {}",
                        slot, epoch_slots_frozen_hash, bank_frozen_hash
                    );
                    state_changes.push(ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash));
                    state_changes.push(ResultingStateChange::RepairClusterVersion(
                        *epoch_slots_frozen_hash,
                    ));
                }
            }
            SlotStateUpdate::DuplicateConfirmed | SlotStateUpdate::Duplicate => (),
            SlotStateUpdate::Dead => panic!("Invalid transition!"),
        }
    } else if is_slot_duplicate {
        // If we detected a duplicate, but have not yet seen any version
        // of the slot duplicate_confirmed (i.e. block above did not execute), then
        // remove the slot from fork choice until we get confirmation.

        // If we get here, we either detected duplicate from
        // 1) WindowService
        // 2) A gossip duplicate_confirmed version that didn't match our frozen
        // version.
        // In both cases, mark the progress map for this slot as duplicate
        state_changes.push(ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash));
    }

    state_changes
}

// Called when we receive either:
// 1) A duplicate slot signal from WindowStage,
// 2) Confirmation of a slot by observing votes from replay or gossip.
//
// This signals external information about this slot, which affects
// this validator's understanding of the validity of this slot
fn on_cluster_update(mut args: SlotStateHandlerArgs) -> Vec<ResultingStateChange> {
    let mut state_changes = vec![];
    if !check_epoch_slots_hash_matches_duplicate_confirmed_hash(
        args.slot,
        args.epoch_slots_frozen_hash,
        args.cluster_duplicate_confirmed_hash,
    ) {
        args.epoch_slots_frozen_hash = None;
        state_changes.push(ResultingStateChange::EpochSlotsMismatchedHash);
    }

    state_changes.extend(if args.is_dead {
        on_dead_slot(args)
    } else if args.bank_frozen_hash != Hash::default() {
        // This case is mutually exclusive with is_dead case above because if a slot is dead,
        // it cannot have  been frozen, and thus cannot have a non-default bank hash.
        on_frozen_slot(args)
    } else {
        vec![]
    });

    state_changes
}

fn get_cluster_duplicate_confirmed_hash<'a>(
    slot: Slot,
    gossip_duplicate_confirmed_hash: Option<&'a Hash>,
    local_frozen_hash: &'a Hash,
    is_local_replay_duplicate_confirmed: bool,
) -> Option<&'a Hash> {
    let local_duplicate_confirmed_hash = if is_local_replay_duplicate_confirmed {
        // If local replay has duplicate_confirmed this slot, this slot must have
        // descendants with votes for this slot, hence this slot must be
        // frozen.
        assert!(*local_frozen_hash != Hash::default());
        Some(local_frozen_hash)
    } else {
        None
    };

    match (
        local_duplicate_confirmed_hash,
        gossip_duplicate_confirmed_hash,
    ) {
        (Some(local_duplicate_confirmed_hash), Some(gossip_duplicate_confirmed_hash)) => {
            if local_duplicate_confirmed_hash != gossip_duplicate_confirmed_hash {
                error!(
                    "For slot {}, the gossip duplicate confirmed hash {}, is not equal
                to the confirmed hash we replayed: {}",
                    slot, gossip_duplicate_confirmed_hash, local_duplicate_confirmed_hash
                );
            }
            Some(local_frozen_hash)
        }
        (Some(local_frozen_hash), None) => Some(local_frozen_hash),
        _ => gossip_duplicate_confirmed_hash,
    }
}

fn apply_state_changes(
    slot: Slot,
    fork_choice: &mut HeaviestSubtreeForkChoice,
    duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
    blockstore: &Blockstore,
    epoch_slots_frozen_slots: &mut EpochSlotsFrozenSlots,
    state_changes: Vec<ResultingStateChange>,
) {
    // Handle cases where the bank is frozen, but not duplicate confirmed
    // yet.
    let mut not_duplicate_confirmed_frozen_hash = None;
    for state_change in state_changes {
        match state_change {
            ResultingStateChange::BankFrozen(bank_frozen_hash) => {
                if !fork_choice
                    .is_duplicate_confirmed(&(slot, bank_frozen_hash))
                    .expect("frozen bank must exist in fork choice")
                {
                    not_duplicate_confirmed_frozen_hash = Some(bank_frozen_hash);
                }
            }
            ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash) => {
                fork_choice.mark_fork_invalid_candidate(&(slot, bank_frozen_hash));
            }
            ResultingStateChange::RepairClusterVersion(cluster_duplicate_confirmed_hash) => {
                duplicate_slots_to_repair.insert(slot, cluster_duplicate_confirmed_hash);
            }
            ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(bank_frozen_hash) => {
                not_duplicate_confirmed_frozen_hash = None;
                // When we detect that our frozen slot matches the cluster version (note this
                // will catch both bank frozen first -> confirmation, or confirmation first ->
                // bank frozen), mark all the newly duplicate confirmed slots in blockstore
                let new_duplicate_confirmed_slot_hashes =
                    fork_choice.mark_fork_valid_candidate(&(slot, bank_frozen_hash));
                blockstore
                    .set_duplicate_confirmed_slots_and_hashes(
                        new_duplicate_confirmed_slot_hashes.into_iter(),
                    )
                    .unwrap();
                duplicate_slots_to_repair.remove(&slot);
            }
            ResultingStateChange::EpochSlotsMismatchedHash => {
                // Even if the order of events is:
                // 1) DuplicateConfirmed(hash1), mismatched deteced, add hash1 to
                //    `duplicate_slots_to_repair`
                // 2) Dump + Repair in Replay based on `duplicate_slots_to_repair`
                // 3) EpochSlotsFrozen(hash2) before replay of new version of the
                //    slot finishes replaying
                // 4) Ran the `state_handler`.
                // We won't re-add `hash1` to `duplicate_slots_to_repair` and
                // trigger another dump + repair because the unfrozen bank hash is still
                // default, and all the state handlers take no action when the bank
                // hash is still default.
                epoch_slots_frozen_slots
                    .remove(&slot)
                    .expect("epoch_slots_frozen_slots must be some at this point");
            }
        }
    }

    if let Some(frozen_hash) = not_duplicate_confirmed_frozen_hash {
        blockstore.insert_bank_hash(slot, frozen_hash, false);
    }
}

fn check_epoch_slots_hash_matches_duplicate_confirmed_hash(
    slot: Slot,
    epoch_slots_frozen_hash: Option<&Hash>,
    gossip_duplicate_confirmed_hash: Option<&Hash>,
) -> bool {
    match (epoch_slots_frozen_hash, gossip_duplicate_confirmed_hash) {
        (Some(epoch_slots_frozen_hash), Some(gossip_duplicate_confirmed_hash))
            if epoch_slots_frozen_hash != gossip_duplicate_confirmed_hash =>
        {
            warn!(
                "EpochSlots sample for slot {} returned hash {}, 
                  but hash {} was duplicate confirmed. Removing bad hash 
                  from EpochSlots sample!",
                slot, epoch_slots_frozen_hash, gossip_duplicate_confirmed_hash
            );
            false
        }

        _ => true,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn check_slot_agrees_with_cluster(
    pubkey: &Pubkey,
    slot: Slot,
    root: Slot,
    blockstore: &Blockstore,
    bank_frozen_hash: Option<Hash>,
    duplicate_slots_tracker: &mut DuplicateSlotsTracker,
    gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
    epoch_slots_frozen_slots: &mut EpochSlotsFrozenSlots,
    progress: &ProgressMap,
    fork_choice: &mut HeaviestSubtreeForkChoice,
    duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
    slot_state_update: SlotStateUpdate,
) {
    info!(
        "{} check_slot_agrees_with_cluster()
        slot: {},
        root: {},
        bank_frozen_hash: {:?},
        update: {:?}",
        pubkey, slot, root, bank_frozen_hash, slot_state_update
    );

    if slot <= root {
        return;
    }

    // Needs to happen before the bank_frozen_hash.is_none() check below to account for duplicate
    // signals arriving before the bank is constructed in replay.
    if matches!(slot_state_update, SlotStateUpdate::Duplicate) {
        // If this slot has already been processed before, return
        if !duplicate_slots_tracker.insert(slot) {
            return;
        }
    }

    if let SlotStateUpdate::EpochSlotsFrozen(new_epoch_slots_frozen_hash) = slot_state_update {
        if let Some(old_epoch_slots_frozen_hash) =
            epoch_slots_frozen_slots.insert(slot, new_epoch_slots_frozen_hash)
        {
            if old_epoch_slots_frozen_hash == new_epoch_slots_frozen_hash {
                // If EpochSlots has already told us this same hash was frozen, return
                return;
            }
        }
    }

    if bank_frozen_hash.is_none() {
        // If the bank doesn't even exist in BankForks yet,
        // then there's nothing to do as replay of the slot
        // hasn't even started
        return;
    }

    let bank_frozen_hash = bank_frozen_hash.unwrap();
    let gossip_duplicate_confirmed_hash = gossip_duplicate_confirmed_slots.get(&slot);

    // If the bank hasn't been frozen yet, then we haven't duplicate confirmed a local version
    // this slot through replay yet.
    let is_local_replay_duplicate_confirmed = fork_choice
        .is_duplicate_confirmed(&(slot, bank_frozen_hash))
        .unwrap_or(false);
    let cluster_duplicate_confirmed_hash = get_cluster_duplicate_confirmed_hash(
        slot,
        gossip_duplicate_confirmed_hash,
        &bank_frozen_hash,
        is_local_replay_duplicate_confirmed,
    );
    let is_slot_duplicate = duplicate_slots_tracker.contains(&slot);
    let is_dead = progress.is_dead(slot).expect("If the frozen hash exists, then the slot must exist in bank forks and thus in progress map");
    let epoch_slots_frozen_hash: Option<&Hash> = epoch_slots_frozen_slots.get(&slot);

    info!(
        "check_slot_agrees_with_cluster() state
        is_local_replay_duplicate_confirmed: {:?},
        cluster_duplicate_confirmed_hash: {:?},
        is_slot_duplicate: {:?},
        is_dead: {:?},
        epoch_slots_frozen_hash: {:?}",
        is_local_replay_duplicate_confirmed,
        cluster_duplicate_confirmed_hash,
        is_slot_duplicate,
        is_dead,
        epoch_slots_frozen_hash,
    );

    let state_handler = slot_state_update.to_handler();
    let args = SlotStateHandlerArgs {
        pubkey: *pubkey,
        slot,
        bank_frozen_hash,
        cluster_duplicate_confirmed_hash,
        is_slot_duplicate,
        is_dead,
        epoch_slots_frozen_hash,
        slot_state_update,
    };
    let state_changes = state_handler(args);
    apply_state_changes(
        slot,
        fork_choice,
        duplicate_slots_to_repair,
        blockstore,
        epoch_slots_frozen_slots,
        state_changes,
    );
}

/*#[cfg(test)]
mod test {
    use super::*;
    use crate::replay_stage::tests::{setup_forks_from_tree, GenerateVotes};
    use solana_runtime::bank_forks::BankForks;
    use std::{
        collections::{HashMap, HashSet},
        sync::{Arc, RwLock},
    };
    use trees::tr;

    struct InitialState {
        heaviest_subtree_fork_choice: HeaviestSubtreeForkChoice,
        progress: ProgressMap,
        descendants: HashMap<Slot, HashSet<Slot>>,
        bank_forks: Arc<RwLock<BankForks>>,
        blockstore: Blockstore,
    }

    fn setup() -> InitialState {
        // Create simple fork 0 -> 1 -> 2 -> 3
        let forks = tr(0) / (tr(1) / (tr(2) / tr(3)));
        let (vote_simulator, blockstore) = setup_forks_from_tree(forks, 1, None::<GenerateVotes>);

        let descendants = vote_simulator
            .bank_forks
            .read()
            .unwrap()
            .descendants()
            .clone();

        InitialState {
            heaviest_subtree_fork_choice: vote_simulator.heaviest_subtree_fork_choice,
            progress: vote_simulator.progress,
            descendants,
            bank_forks: vote_simulator.bank_forks,
            blockstore,
        }
    }

    #[test]
    fn test_frozen_duplicate() {
        // Common state
        let slot = 0;
        let cluster_duplicate_confirmed_hash = None;
        let epoch_slots_frozen_hash = None;
        let is_dead = false;

        // Slot is not detected as duplicate yet
        let mut is_slot_duplicate = false;

        // Simulate freezing the bank, add a
        // new non-default hash, should return
        // just a frozen state transition
        let bank_frozen_hash = Hash::new_unique();
        assert_eq!(
            on_frozen_slot(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::BankFrozen
            }),
            vec![ResultingStateChange::BankFrozen(bank_frozen_hash)]
        );

        // Now mark the slot as duplicate, should
        // trigger marking the slot as a duplicate
        is_slot_duplicate = true;
        assert_eq!(
            on_cluster_update(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::Duplicate
            }),
            vec![ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash)]
        );
    }

    #[test]
    fn test_frozen_duplicate_confirmed() {
        // Common state
        let slot = 0;
        let is_slot_duplicate = false;
        let is_dead = false;

        // No cluster duplicate_confirmed hash or EpochSlots
        // sampled slot yet
        let mut cluster_duplicate_confirmed_hash = None;
        let epoch_slots_frozen_hash = None;

        // Simulate freezing the bank, add a
        // new non-default hash, should return
        // no actionable state changes
        let bank_frozen_hash = Hash::new_unique();
        assert_eq!(
            on_frozen_slot(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::BankFrozen
            }),
            vec![ResultingStateChange::BankFrozen(bank_frozen_hash)]
        );

        // Now mark the same frozen slot hash as duplicate_confirmed by the cluster,
        // should just confirm the slot
        cluster_duplicate_confirmed_hash = Some(&bank_frozen_hash);
        assert_eq!(
            on_cluster_update(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::DuplicateConfirmed,
            }),
            vec![ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(
                bank_frozen_hash
            ),]
        );

        // If we freeze the bank after we get a cluster duplicate confirmed hash
        // and both hashes match, then we should see both BankFrozen and
        // DuplicateConfirmedSlotMatchesCluster state transitions
        cluster_duplicate_confirmed_hash = Some(&bank_frozen_hash);
        assert_eq!(
            on_cluster_update(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::BankFrozen,
            }),
            vec![
                ResultingStateChange::BankFrozen(bank_frozen_hash),
                ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(bank_frozen_hash)
            ]
        );

        // If the cluster_duplicate_confirmed_hash does not match, then we
        // should trigger marking the slot as a duplicate, and also
        // try to repair correct version
        let mismatched_hash = Hash::new_unique();
        cluster_duplicate_confirmed_hash = Some(&mismatched_hash);
        assert_eq!(
            on_cluster_update(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::DuplicateConfirmed,
            }),
            vec![
                ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash),
                ResultingStateChange::RepairClusterVersion(mismatched_hash),
            ]
        );
    }

    #[test]
    fn test_duplicate_frozen_duplicate_confirmed() {
        // Common state
        let slot = 0;
        let is_dead = false;
        let is_slot_duplicate = true;

        // Bank is not frozen yet
        let mut cluster_duplicate_confirmed_hash = None;
        let epoch_slots_frozen_hash = None;
        let mut bank_frozen_hash = Hash::default();

        // Mark the slot as duplicate. Because our version of the slot is not
        // frozen yet, we don't know which version we have, so no action is
        // taken.
        assert!(on_cluster_update(SlotStateHandlerArgs {
            slot,
            bank_frozen_hash,
            cluster_duplicate_confirmed_hash,
            is_slot_duplicate,
            is_dead,
            epoch_slots_frozen_hash,
            slot_state_update: SlotStateUpdate::Duplicate,
        })
        .is_empty());

        // Freeze the bank, should now mark the slot as duplicate since we have
        // not seen confirmation yet.
        bank_frozen_hash = Hash::new_unique();
        assert_eq!(
            on_cluster_update(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::BankFrozen,
            }),
            vec![
                ResultingStateChange::BankFrozen(bank_frozen_hash),
                ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash)
            ]
        );

        // If the cluster_duplicate_confirmed_hash matches, we just confirm
        // the slot
        cluster_duplicate_confirmed_hash = Some(&bank_frozen_hash);
        assert_eq!(
            on_cluster_update(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::DuplicateConfirmed,
            }),
            vec![ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(
                bank_frozen_hash
            ),]
        );

        // If the cluster_duplicate_confirmed_hash does not match, then we
        // should trigger marking the slot as a duplicate, and also
        // try to repair correct version
        let mismatched_hash = Hash::new_unique();
        cluster_duplicate_confirmed_hash = Some(&mismatched_hash);
        assert_eq!(
            on_cluster_update(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::DuplicateConfirmed,
            }),
            vec![
                ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash),
                ResultingStateChange::RepairClusterVersion(mismatched_hash),
            ]
        );
    }

    #[test]
    fn test_duplicate_duplicate_confirmed() {
        let slot = 0;
        let correct_hash = Hash::new_unique();
        let cluster_duplicate_confirmed_hash = Some(&correct_hash);
        let epoch_slots_frozen_hash = None;
        let is_dead = false;
        // Bank is not frozen yet
        let bank_frozen_hash = Hash::default();

        // Because our version of the slot is not frozen yet, then even though
        // the cluster has duplicate_confirmed a hash, we don't know which version we
        // have, so no action is taken.
        let is_slot_duplicate = true;
        assert!(on_cluster_update(SlotStateHandlerArgs {
            slot,
            bank_frozen_hash,
            cluster_duplicate_confirmed_hash,
            is_slot_duplicate,
            is_dead,
            epoch_slots_frozen_hash,
            slot_state_update: SlotStateUpdate::DuplicateConfirmed,
        })
        .is_empty());
    }

    #[test]
    fn test_duplicate_dead() {
        let slot = 0;
        let cluster_duplicate_confirmed_hash = None;
        // Bank is not frozen yet
        let bank_frozen_hash = Hash::default();

        // Simulate marking slot dead
        let mut epoch_slots_frozen_hash = None;
        let mut is_slot_duplicate = false;
        let is_dead = true;
        assert!(on_cluster_update(SlotStateHandlerArgs {
            slot,
            bank_frozen_hash,
            cluster_duplicate_confirmed_hash,
            is_slot_duplicate,
            is_dead,
            epoch_slots_frozen_hash,
            slot_state_update: SlotStateUpdate::Dead,
        })
        .is_empty());

        // Simulate detecting another duplicate version of this slot.
        // Even though our version of the slot is dead, the cluster has not
        // duplicate_confirmed a hash, we don't know which version we have, so no action
        // is taken.
        is_slot_duplicate = true;
        assert!(on_cluster_update(SlotStateHandlerArgs {
            slot,
            bank_frozen_hash,
            cluster_duplicate_confirmed_hash,
            is_slot_duplicate,
            is_dead,
            epoch_slots_frozen_hash,
            slot_state_update: SlotStateUpdate::Duplicate,
        })
        .is_empty());

        // If the EpochSlots tell us a version of the slot has been frozen,
        // we should try to repair that slot
        let correct_hash = Hash::new_unique();
        epoch_slots_frozen_hash = Some(&correct_hash);
        assert_eq!(
            on_cluster_update(SlotStateHandlerArgs {
                slot,
                bank_frozen_hash,
                cluster_duplicate_confirmed_hash,
                is_slot_duplicate,
                is_dead,
                epoch_slots_frozen_hash,
                slot_state_update: SlotStateUpdate::EpochSlotsFrozen(correct_hash),
            }),
            vec![ResultingStateChange::RepairClusterVersion(correct_hash)]
        );
    }

    fn create_default_dead_slot_args<'a>(slot: Slot) -> SlotStateHandlerArgs<'a> {
        let cluster_duplicate_confirmed_hash = None;
        let is_slot_duplicate = false;
        let epoch_slots_frozen_hash = None;
        let dead_slot_args = SlotStateHandlerArgs::new_dead_slot_args(
            slot,
            cluster_duplicate_confirmed_hash,
            is_slot_duplicate,
            epoch_slots_frozen_hash,
        );
        assert!(on_cluster_update(dead_slot_args.clone()).is_empty());
        dead_slot_args
    }

    fn simulate_duplicate_confirmed_on_args<'a>(
        args: &mut SlotStateHandlerArgs<'a>,
        cluster_duplicate_confirmed_hash: &'a Hash,
    ) {
        args.cluster_duplicate_confirmed_hash = Some(cluster_duplicate_confirmed_hash);
        args.slot_state_update = SlotStateUpdate::DuplicateConfirmed;
    }

    fn simulate_duplicate_on_args(args: &mut SlotStateHandlerArgs) {
        args.is_slot_duplicate = true;
        args.slot_state_update = SlotStateUpdate::Duplicate;
    }

    fn simulate_bank_frozen_on_args(args: &mut SlotStateHandlerArgs, bank_frozen_hash: Hash) {
        args.bank_frozen_hash = bank_frozen_hash;
        args.is_dead = false;
        args.slot_state_update = SlotStateUpdate::BankFrozen;
    }

    fn simulate_unreplayed_bank_on_args(args: &mut SlotStateHandlerArgs) {
        args.bank_frozen_hash = Hash::default();
        args.is_dead = false;
    }

    fn simulate_epoch_slots_frozen_on_args<'a>(
        args: &mut SlotStateHandlerArgs<'a>,
        epoch_slots_frozen_hash: &'a Hash,
    ) {
        args.epoch_slots_frozen_hash = Some(epoch_slots_frozen_hash);
        args.slot_state_update = SlotStateUpdate::EpochSlotsFrozen(*epoch_slots_frozen_hash);
    }

    #[test]
    fn test_dead_duplicate_confirmed_before_duplicate() {
        let slot = 0;

        // 1) Simulate marking our version of the slot as dead,
        // cluster has not duplicate confirmed a slot yet.
        let mut args = create_default_dead_slot_args(slot);

        // 2) Now simulate that the cluster has duplicate confirmed some version of the slot
        // Even if the duplicate signal hasn't come in yet,
        // we can deduce the slot is duplicate AND we have,
        // the wrong version, so should mark the slot as duplicate,
        // and repair the correct version
        let cluster_duplicate_confirmed_hash = Hash::new_unique();
        simulate_duplicate_confirmed_on_args(&mut args, &cluster_duplicate_confirmed_hash);
        assert_eq!(
            on_cluster_update(args.clone()),
            vec![
                ResultingStateChange::MarkSlotDuplicate(args.bank_frozen_hash),
                ResultingStateChange::RepairClusterVersion(cluster_duplicate_confirmed_hash),
            ]
        );

        // 3) When the SlotStateUpdate::Duplicate signal comes in, it should not trigger
        // another series of repair requests, even if the `cluster_duplicate_confirmed_hash`
        // is known.
        simulate_duplicate_on_args(&mut args);
        assert!(on_cluster_update(args.clone()).is_empty());

        // 4) After we dump and repair and replay another version of the slot, if that version
        // still mismatches, we should trigger another repair
        let new_bad_bank_frozen_hash = Hash::new_unique();
        simulate_bank_frozen_on_args(&mut args, new_bad_bank_frozen_hash);
        assert_eq!(
            on_frozen_slot(args),
            vec![
                ResultingStateChange::BankFrozen(new_bad_bank_frozen_hash),
                ResultingStateChange::MarkSlotDuplicate(new_bad_bank_frozen_hash),
                ResultingStateChange::RepairClusterVersion(cluster_duplicate_confirmed_hash),
            ]
        );
    }

    #[test]
    fn test_dead_then_epoch_slots_frozen() {
        let slot = 0;

        // Simulate marking our version of the slot as dead,
        // cluster has not duplicate confirmed a slot yet.
        let mut args = create_default_dead_slot_args(slot);

        // If there is no cluster_duplicate_confirmed_hash yet, but we do have
        // a version of the slot from EpochSlots sampling, then we should also
        // should trigger marking the slot as a duplicate, and also try to repair
        // the correct version
        assert!(args.cluster_duplicate_confirmed_hash.is_none());
        let epoch_slots_frozen_hash = Hash::new_unique();
        simulate_epoch_slots_frozen_on_args(&mut args, &epoch_slots_frozen_hash);
        assert_eq!(
            on_cluster_update(args.clone()),
            vec![ResultingStateChange::RepairClusterVersion(
                epoch_slots_frozen_hash
            ),]
        );
    }

    #[test]
    fn test_mismatched_bank_frozen_then_epoch_slots_frozen() {
        let slot = 0;

        // Simulate marking our version of the slot as dead,
        // cluster has not duplicate confirmed a slot yet.
        let mut args = create_default_dead_slot_args(slot);

        // If there is no cluster_duplicate_confirmed_hash yet, and our
        // bank frozen hash does not match the version of the slot from EpochSlots
        // sampling, then we should also should trigger marking the slot as a duplicate,
        // and also try to repair the correct version.
        assert!(args.cluster_duplicate_confirmed_hash.is_none());
        let bank_frozen_hash = Hash::new_unique();
        let epoch_slots_frozen_hash = Hash::new_unique();
        simulate_bank_frozen_on_args(&mut args, bank_frozen_hash);
        simulate_epoch_slots_frozen_on_args(&mut args, &epoch_slots_frozen_hash);
        assert_eq!(
            on_cluster_update(args.clone()),
            vec![
                ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash),
                ResultingStateChange::RepairClusterVersion(epoch_slots_frozen_hash),
            ]
        );
    }

    #[test]
    fn test_matching_bank_frozen_then_epoch_slots_frozen() {
        let slot = 0;

        // Simulate marking our version of the slot as dead,
        // cluster has not duplicate confirmed a slot yet.
        let mut args = create_default_dead_slot_args(slot);

        // If there is no cluster_duplicate_confirmed_hash yet, and our
        // bank frozen hash does not match the version of the slot from EpochSlots
        // sampling, then we should also should trigger marking the slot as a duplicate,
        // and also try to repair the correct version.
        assert!(args.cluster_duplicate_confirmed_hash.is_none());
        let epoch_slots_frozen_hash = Hash::new_unique();
        simulate_bank_frozen_on_args(&mut args, epoch_slots_frozen_hash);
        simulate_epoch_slots_frozen_on_args(&mut args, &epoch_slots_frozen_hash);
        assert!(on_cluster_update(args.clone()).is_empty());
    }

    #[test]
    fn test_unreplayed_bank_then_epoch_slots_frozen() {
        let slot = 0;

        // Simulate marking our version of the slot as dead,
        // cluster has not duplicate confirmed a slot yet.
        let mut args = create_default_dead_slot_args(slot);

        // If there is no cluster_duplicate_confirmed_hash yet, and our
        // bank frozen hash does not match the version of the slot from EpochSlots
        // sampling, then we should also should trigger marking the slot as a duplicate,
        // and also try to repair the correct version.
        assert!(args.cluster_duplicate_confirmed_hash.is_none());
        let epoch_slots_frozen_hash = Hash::new_unique();
        simulate_unreplayed_bank_on_args(&mut args);
        simulate_epoch_slots_frozen_on_args(&mut args, &epoch_slots_frozen_hash);
    }

    #[test]
    fn test_duplicate_confirmed_before_epoch_slots_frozen() {
        let slot = 0;

        // 1) Simulate marking our version of the slot as dead,
        // cluster has not duplicate confirmed a slot yet.
        let mut args = create_default_dead_slot_args(slot);

        // 2) Now simulate that the cluster has duplicate confirmed some version of the slot
        let cluster_duplicate_confirmed_hash = Hash::new_unique();
        simulate_duplicate_confirmed_on_args(&mut args, &cluster_duplicate_confirmed_hash);

        // 3) Simulate an EpochSlots frozen signal. Since we know the duplicate
        // confirmed hash, we should not send a repair signal even if EpochSlots
        // signals the matching hash has been frozen, since the duplicate confirmed
        // signal or bank frozen signal should have already triggered that repair.
        let epoch_slots_frozen_hash = cluster_duplicate_confirmed_hash;
        simulate_epoch_slots_frozen_on_args(&mut args, &epoch_slots_frozen_hash);
        assert!(on_cluster_update(args.clone()).is_empty());

        // 4) If EpochSlots gives a different result than the duplicate confirmed hash, result
        // should prioritize the duplicate confirmed version, and not send another repair
        // request.
        let epoch_slots_frozen_hash = Hash::new_unique();
        simulate_epoch_slots_frozen_on_args(&mut args, &epoch_slots_frozen_hash);
        assert_eq!(
            on_cluster_update(args.clone()),
            vec![ResultingStateChange::EpochSlotsMismatchedHash]
        );

        // 5) If we have already sent a repair request to the network after duplicate confirmation
        // was detected (i.e. SlotStateUpdate::DuplicateConfirmed above), the `bank_frozen_hash`
        // would be default after Replay dumps the slot, so we should not generate another
        // repair request here when we get the SlotStateUpdate::Duplicate update.
        simulate_unreplayed_bank_on_args(&mut args);
        assert_eq!(
            on_cluster_update(args.clone()),
            vec![ResultingStateChange::EpochSlotsMismatchedHash]
        );
    }

    #[test]
    fn test_apply_state_changes() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            descendants,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();

        // MarkSlotDuplicate should mark progress map and remove
        // the slot from fork choice
        let duplicate_slot = bank_forks.read().unwrap().root() + 1;
        let duplicate_slot_hash = bank_forks
            .read()
            .unwrap()
            .get(duplicate_slot)
            .unwrap()
            .hash();
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &mut epoch_slots_frozen_slots,
            vec![ResultingStateChange::MarkSlotDuplicate(duplicate_slot_hash)],
        );
        assert!(!heaviest_subtree_fork_choice
            .is_candidate(&(duplicate_slot, duplicate_slot_hash))
            .unwrap());
        for child_slot in descendants
            .get(&duplicate_slot)
            .unwrap()
            .iter()
            .chain(std::iter::once(&duplicate_slot))
        {
            assert_eq!(
                heaviest_subtree_fork_choice
                    .latest_invalid_ancestor(&(
                        *child_slot,
                        bank_forks.read().unwrap().get(*child_slot).unwrap().hash()
                    ))
                    .unwrap(),
                duplicate_slot
            );
        }

        // Simulate detecting another hash that is the correct version,
        // RepairClusterVersion should add the slot to repair
        // to `duplicate_slots_to_repair`
        assert!(duplicate_slots_to_repair.is_empty());
        let correct_hash = Hash::new_unique();
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &mut epoch_slots_frozen_slots,
            vec![ResultingStateChange::RepairClusterVersion(correct_hash)],
        );
        assert_eq!(duplicate_slots_to_repair.len(), 1);
        assert_eq!(
            *duplicate_slots_to_repair.get(&duplicate_slot).unwrap(),
            correct_hash
        );

        // Simulate EpochSlots giving us a frozen hash that doesn't match
        // the cluster duplicate confirmed hash.
        //
        // EpochSlotsMismatchedHash should remove the mismatched
        // hash from `epoch_slots_frozen_slots`
        let mismatched_hash = Hash::new_unique();
        epoch_slots_frozen_slots.insert(duplicate_slot, mismatched_hash);
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &mut epoch_slots_frozen_slots,
            vec![ResultingStateChange::EpochSlotsMismatchedHash],
        );
        assert!(epoch_slots_frozen_slots.is_empty());
    }

    #[test]
    fn test_apply_state_changes_bank_frozen() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();

        let duplicate_slot = bank_forks.read().unwrap().root() + 1;
        let duplicate_slot_hash = bank_forks
            .read()
            .unwrap()
            .get(duplicate_slot)
            .unwrap()
            .hash();

        // Simulate ReplayStage freezing a Bank with the given hash.
        // BankFrozen should mark it down in Blockstore.
        assert!(blockstore.get_bank_hash(duplicate_slot).is_none());
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &mut epoch_slots_frozen_slots,
            vec![ResultingStateChange::BankFrozen(duplicate_slot_hash)],
        );
        assert_eq!(
            blockstore.get_bank_hash(duplicate_slot).unwrap(),
            duplicate_slot_hash
        );
        assert!(!blockstore.is_duplicate_confirmed(duplicate_slot));

        // If we freeze another version of the bank, it should overwrite the first
        // version in blockstore.
        let new_bank_hash = Hash::new_unique();
        let root_slot_hash = {
            let root_bank = bank_forks.read().unwrap().root_bank();
            (root_bank.slot(), root_bank.hash())
        };
        heaviest_subtree_fork_choice
            .add_new_leaf_slot((duplicate_slot, new_bank_hash), Some(root_slot_hash));
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &mut epoch_slots_frozen_slots,
            vec![ResultingStateChange::BankFrozen(new_bank_hash)],
        );
        assert_eq!(
            blockstore.get_bank_hash(duplicate_slot).unwrap(),
            new_bank_hash
        );
        assert!(!blockstore.is_duplicate_confirmed(duplicate_slot));
    }

    fn run_test_apply_state_changes_duplicate_confirmed_matches_frozen(
        modify_state_changes: impl Fn(Hash, &mut Vec<ResultingStateChange>),
    ) {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            descendants,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();

        let duplicate_slot = bank_forks.read().unwrap().root() + 1;
        let our_duplicate_slot_hash = bank_forks
            .read()
            .unwrap()
            .get(duplicate_slot)
            .unwrap()
            .hash();

        // Setup and check the state that is about to change.
        duplicate_slots_to_repair.insert(duplicate_slot, Hash::new_unique());
        assert!(blockstore.get_bank_hash(duplicate_slot).is_none());
        assert!(!blockstore.is_duplicate_confirmed(duplicate_slot));

        // DuplicateConfirmedSlotMatchesCluster should:
        // 1) Re-enable fork choice
        // 2) Clear any pending repairs from `duplicate_slots_to_repair` since we have the
        //    right version now
        // 3) Set the status to duplicate confirmed in Blockstore
        let mut state_changes = vec![ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(
            our_duplicate_slot_hash,
        )];
        modify_state_changes(our_duplicate_slot_hash, &mut state_changes);
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &mut epoch_slots_frozen_slots,
            state_changes,
        );
        for child_slot in descendants
            .get(&duplicate_slot)
            .unwrap()
            .iter()
            .chain(std::iter::once(&duplicate_slot))
        {
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(
                    *child_slot,
                    bank_forks.read().unwrap().get(*child_slot).unwrap().hash()
                ))
                .is_none());
        }
        assert!(heaviest_subtree_fork_choice
            .is_candidate(&(duplicate_slot, our_duplicate_slot_hash))
            .unwrap());
        assert!(duplicate_slots_to_repair.is_empty());
        assert_eq!(
            blockstore.get_bank_hash(duplicate_slot).unwrap(),
            our_duplicate_slot_hash
        );
        assert!(blockstore.is_duplicate_confirmed(duplicate_slot));
    }

    #[test]
    fn test_apply_state_changes_duplicate_confirmed_matches_frozen() {
        run_test_apply_state_changes_duplicate_confirmed_matches_frozen(
            |_our_duplicate_slot_hash, _state_changes: &mut Vec<ResultingStateChange>| {},
        );
    }

    #[test]
    fn test_apply_state_changes_bank_frozen_and_duplicate_confirmed_matches_frozen() {
        run_test_apply_state_changes_duplicate_confirmed_matches_frozen(
            |our_duplicate_slot_hash, state_changes: &mut Vec<ResultingStateChange>| {
                state_changes.push(ResultingStateChange::BankFrozen(our_duplicate_slot_hash));
            },
        );
    }

    fn run_test_state_duplicate_then_bank_frozen(initial_bank_hash: Option<Hash>) {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        // Setup a duplicate slot state transition with the initial bank state of the duplicate slot
        // determined by `initial_bank_hash`, which can be:
        // 1) A default hash (unfrozen bank),
        // 2) None (a slot that hasn't even started replay yet).
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        let duplicate_slot = 2;
        check_slot_agrees_with_cluster(
            duplicate_slot,
            root,
            &blockstore,
            initial_bank_hash,
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut epoch_slots_frozen_slots,
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::Duplicate,
        );
        assert!(duplicate_slots_tracker.contains(&duplicate_slot));
        // Nothing should be applied yet to fork choice, since bank was not yet frozen
        for slot in 2..=3 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(slot, slot_hash))
                .is_none());
        }

        // Now freeze the bank
        let frozen_duplicate_slot_hash = bank_forks
            .read()
            .unwrap()
            .get(duplicate_slot)
            .unwrap()
            .hash();
        check_slot_agrees_with_cluster(
            duplicate_slot,
            root,
            &blockstore,
            Some(frozen_duplicate_slot_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut epoch_slots_frozen_slots,
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::BankFrozen,
        );

        // Progress map should have the correct updates, fork choice should mark duplicate
        // as unvotable
        assert!(heaviest_subtree_fork_choice
            .is_unconfirmed_duplicate(&(duplicate_slot, frozen_duplicate_slot_hash))
            .unwrap());

        // The ancestor of the duplicate slot should be the best slot now
        let (duplicate_ancestor, duplicate_parent_hash) = {
            let r_bank_forks = bank_forks.read().unwrap();
            let parent_bank = r_bank_forks.get(duplicate_slot).unwrap().parent().unwrap();
            (parent_bank.slot(), parent_bank.hash())
        };
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (duplicate_ancestor, duplicate_parent_hash)
        );
    }

    #[test]
    fn test_state_unfrozen_bank_duplicate_then_bank_frozen() {
        run_test_state_duplicate_then_bank_frozen(Some(Hash::default()));
    }

    #[test]
    fn test_state_unreplayed_bank_duplicate_then_bank_frozen() {
        run_test_state_duplicate_then_bank_frozen(None);
    }

    #[test]
    fn test_state_ancestor_confirmed_descendant_duplicate() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();

        // Mark slot 2 as duplicate confirmed
        let slot2_hash = bank_forks.read().unwrap().get(2).unwrap().hash();
        gossip_duplicate_confirmed_slots.insert(2, slot2_hash);
        check_slot_agrees_with_cluster(
            2,
            root,
            &blockstore,
            Some(slot2_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut EpochSlotsFrozenSlots::default(),
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            SlotStateUpdate::DuplicateConfirmed,
        );
        assert!(heaviest_subtree_fork_choice
            .is_duplicate_confirmed(&(2, slot2_hash))
            .unwrap());
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        for slot in 0..=2 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            assert!(heaviest_subtree_fork_choice
                .is_duplicate_confirmed(&(slot, slot_hash))
                .unwrap());
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(slot, slot_hash))
                .is_none());
        }

        // Mark 3 as duplicate, should not remove the duplicate confirmed slot 2 from
        // fork choice
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            Some(slot3_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut EpochSlotsFrozenSlots::default(),
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            SlotStateUpdate::Duplicate,
        );
        assert!(duplicate_slots_tracker.contains(&3));
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (2, slot2_hash)
        );
        for slot in 0..=3 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            if slot <= 2 {
                assert!(heaviest_subtree_fork_choice
                    .is_duplicate_confirmed(&(slot, slot_hash))
                    .unwrap());
                assert!(heaviest_subtree_fork_choice
                    .latest_invalid_ancestor(&(slot, slot_hash))
                    .is_none());
            } else {
                assert!(!heaviest_subtree_fork_choice
                    .is_duplicate_confirmed(&(slot, slot_hash))
                    .unwrap());
                assert_eq!(
                    heaviest_subtree_fork_choice
                        .latest_invalid_ancestor(&(slot, slot_hash))
                        .unwrap(),
                    3
                );
            }
        }
    }

    #[test]
    fn test_state_ancestor_duplicate_descendant_confirmed() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();

        // Mark 2 as duplicate
        check_slot_agrees_with_cluster(
            2,
            root,
            &blockstore,
            Some(bank_forks.read().unwrap().get(2).unwrap().hash()),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut EpochSlotsFrozenSlots::default(),
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            SlotStateUpdate::Duplicate,
        );
        assert!(duplicate_slots_tracker.contains(&2));
        for slot in 2..=3 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            assert_eq!(
                heaviest_subtree_fork_choice
                    .latest_invalid_ancestor(&(slot, slot_hash))
                    .unwrap(),
                2
            );
        }

        let slot1_hash = bank_forks.read().unwrap().get(1).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (1, slot1_hash)
        );

        // Mark slot 3 as duplicate confirmed, should mark slot 2 as duplicate confirmed as well
        gossip_duplicate_confirmed_slots.insert(3, slot3_hash);
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            Some(slot3_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut EpochSlotsFrozenSlots::default(),
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            SlotStateUpdate::DuplicateConfirmed,
        );
        for slot in 0..=3 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            assert!(heaviest_subtree_fork_choice
                .is_duplicate_confirmed(&(slot, slot_hash))
                .unwrap());
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(slot, slot_hash))
                .is_none());
        }
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
    }

    fn verify_all_slots_duplicate_confirmed(
        bank_forks: &RwLock<BankForks>,
        heaviest_subtree_fork_choice: &HeaviestSubtreeForkChoice,
        upper_bound: Slot,
        expected_is_duplicate_confirmed: bool,
    ) {
        for slot in 0..upper_bound {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            let expected_is_duplicate_confirmed = expected_is_duplicate_confirmed ||
            // root is always duplicate confirmed
            slot == 0;
            assert_eq!(
                heaviest_subtree_fork_choice
                    .is_duplicate_confirmed(&(slot, slot_hash))
                    .unwrap(),
                expected_is_duplicate_confirmed
            );
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(slot, slot_hash))
                .is_none());
        }
    }

    #[test]
    fn test_state_descendant_confirmed_ancestor_duplicate() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        // Mark 3 as duplicate confirmed
        gossip_duplicate_confirmed_slots.insert(3, slot3_hash);
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            Some(slot3_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut epoch_slots_frozen_slots,
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::DuplicateConfirmed,
        );
        verify_all_slots_duplicate_confirmed(&bank_forks, &heaviest_subtree_fork_choice, 3, true);
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );

        // Mark ancestor 1 as duplicate, fork choice should be unaffected since
        // slot 1 was duplicate confirmed by the confirmation on its
        // descendant, 3.
        let slot1_hash = bank_forks.read().unwrap().get(1).unwrap().hash();
        check_slot_agrees_with_cluster(
            1,
            root,
            &blockstore,
            Some(slot1_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut epoch_slots_frozen_slots,
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::Duplicate,
        );
        assert!(duplicate_slots_tracker.contains(&1));
        verify_all_slots_duplicate_confirmed(&bank_forks, &heaviest_subtree_fork_choice, 3, true);
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
    }

    #[test]
    fn test_duplicate_confirmed_and_epoch_slots_frozen() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        // Mark 3 as only epoch slots frozen, matching our `slot3_hash`, should not duplicate
        // confirm the slot
        let mut expected_is_duplicate_confirmed = false;
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            Some(slot3_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut epoch_slots_frozen_slots,
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::EpochSlotsFrozen(slot3_hash),
        );
        verify_all_slots_duplicate_confirmed(
            &bank_forks,
            &heaviest_subtree_fork_choice,
            3,
            expected_is_duplicate_confirmed,
        );
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            Some(slot3_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut epoch_slots_frozen_slots,
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::DuplicateConfirmed,
        );
        verify_all_slots_duplicate_confirmed(
            &bank_forks,
            &heaviest_subtree_fork_choice,
            3,
            expected_is_duplicate_confirmed,
        );

        // Mark 3 as duplicate confirmed and epoch slots frozen with the same hash. Should
        // duplicate confirm all descendants of 3
        gossip_duplicate_confirmed_slots.insert(3, slot3_hash);
        epoch_slots_frozen_slots.insert(3, slot3_hash);
        expected_is_duplicate_confirmed = true;
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            Some(slot3_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut epoch_slots_frozen_slots,
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::DuplicateConfirmed,
        );
        verify_all_slots_duplicate_confirmed(
            &bank_forks,
            &heaviest_subtree_fork_choice,
            3,
            expected_is_duplicate_confirmed,
        );
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
    }

    #[test]
    fn test_duplicate_confirmed_and_epoch_slots_frozen_mismatched() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        // Mark 3 as only epoch slots frozen with different hash than the our
        // locally replayed `slot3_hash`. This should not duplicate confirm the slot,
        // but should add the epoch slots frozen hash to the repair set
        let mismatched_hash = Hash::new_unique();
        let mut expected_is_duplicate_confirmed = false;
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            Some(slot3_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut epoch_slots_frozen_slots,
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::EpochSlotsFrozen(mismatched_hash),
        );
        assert_eq!(*duplicate_slots_to_repair.get(&3).unwrap(), mismatched_hash);
        verify_all_slots_duplicate_confirmed(
            &bank_forks,
            &heaviest_subtree_fork_choice,
            3,
            expected_is_duplicate_confirmed,
        );

        // Mark our version of slot 3 as duplicate confirmed with a hash different than
        // the epoch slots frozen hash above. Should duplicate confirm all descendants of
        // 3 and remove the mismatched hash from both:
        // 1) epoch_slots_frozen_slots
        // 2) duplicate_slots_to_repair, since we have the right version now, no need to repair
        gossip_duplicate_confirmed_slots.insert(3, slot3_hash);
        expected_is_duplicate_confirmed = true;
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            Some(slot3_hash),
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut epoch_slots_frozen_slots,
            &progress,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            SlotStateUpdate::DuplicateConfirmed,
        );
        assert!(duplicate_slots_to_repair.is_empty());
        assert!(epoch_slots_frozen_slots.is_empty());
        verify_all_slots_duplicate_confirmed(
            &bank_forks,
            &heaviest_subtree_fork_choice,
            3,
            expected_is_duplicate_confirmed,
        );
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
    }
}*/
