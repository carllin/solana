use crate::{
    fork_choice::ForkChoice, heaviest_subtree_fork_choice::HeaviestSubtreeForkChoice,
    progress_map::ProgressMap,
};
use solana_sdk::{clock::Slot, hash::Hash};
use std::collections::{BTreeMap, HashMap, HashSet};

pub type GossipConfirmedSlots = BTreeMap<Slot, Hash>;
type SlotStateHandler = fn(Slot, &Hash, Option<&Hash>, bool, bool) -> Vec<ResultingStateChange>;

#[derive(PartialEq, Debug)]
pub enum SlotStateUpdate {
    Frozen,
    Confirmed,
    Dead,
    Duplicate,
}

#[derive(PartialEq, Debug)]
pub enum ResultingStateChange {
    // Hash of our current frozen version of the slot
    MarkSlotDuplicate(Hash),
    // Hash of the cluster confirmed slot that is not equivalent
    // to our frozen version of the slot
    RepairConfirmedVersion(Hash),
    // Hash of our current frozen version of the slot
    ConfirmedSlotMatchesCluster(Hash),
}

impl SlotStateUpdate {
    fn to_handler(&self) -> SlotStateHandler {
        match self {
            SlotStateUpdate::Dead => on_dead_slot,
            SlotStateUpdate::Frozen => on_frozen_slot,
            SlotStateUpdate::Confirmed => on_cluster_update,
            SlotStateUpdate::Duplicate => on_cluster_update,
        }
    }
}

fn on_dead_slot(
    slot: Slot,
    bank_frozen_hash: &Hash,
    cluster_confirmed_hash: Option<&Hash>,
    _is_slot_duplicate: bool,
    is_dead: bool,
) -> Vec<ResultingStateChange> {
    assert!(is_dead);
    // Bank should not have been frozen if the slot was marked dead
    assert_eq!(*bank_frozen_hash, Hash::default());
    if let Some(cluster_confirmed_hash) = cluster_confirmed_hash {
        // If the cluster confirmed some version of this slot, then
        // there's another version
        warn!(
            "Cluster confirmed slot {} with hash {}, but we marked slot dead",
            slot, cluster_confirmed_hash
        );
        // No need to check `is_slot_duplicate` and modify fork choice as dead slots
        // are never frozen, and thus never added to fork choice. The state change for
        // `MarkSlotDuplicate` will try to modify fork choice, but won't find the slot
        // in the fork choice tree, so is equivalent to a no-op
        return vec![
            ResultingStateChange::MarkSlotDuplicate(Hash::default()),
            ResultingStateChange::RepairConfirmedVersion(*cluster_confirmed_hash),
        ];
    }

    vec![]
}

fn on_frozen_slot(
    slot: Slot,
    bank_frozen_hash: &Hash,
    cluster_confirmed_hash: Option<&Hash>,
    is_slot_duplicate: bool,
    is_dead: bool,
) -> Vec<ResultingStateChange> {
    // If a slot is marked frozen, the bank hash should not be default,
    // and the slot should not be dead
    assert!(*bank_frozen_hash != Hash::default());
    assert!(!is_dead);

    if let Some(cluster_confirmed_hash) = cluster_confirmed_hash {
        // If the cluster confirmed some version of this slot, then
        // confirm our version agrees with the cluster,
        if cluster_confirmed_hash != bank_frozen_hash {
            // If the versions do not match, modify fork choice rule
            // to exclude our version from being voted on and also
            // repair correct version
            warn!(
                "Cluster confirmed slot {} with hash {}, but we froze slot with hash {}",
                slot, cluster_confirmed_hash, bank_frozen_hash
            );
            return vec![
                ResultingStateChange::MarkSlotDuplicate(*bank_frozen_hash),
                ResultingStateChange::RepairConfirmedVersion(*cluster_confirmed_hash),
            ];
        } else {
            // If the versions match, then add the slot to the candidate
            // set to account for the case where it was removed earlier
            // by the `on_duplicate_slot()` handler
            return vec![ResultingStateChange::ConfirmedSlotMatchesCluster(
                *bank_frozen_hash,
            )];
        }
    }

    if is_slot_duplicate {
        // If we detected a duplicate, but have not yet seen any version
        // of the slot confirmed (i.e. block above did not execute), then
        // remove the slot from fork choice until we get confirmation.

        // If we get here, we either detected duplicate from
        // 1) WindowService
        // 2) A gossip confirmed version that didn't match our frozen
        // version.
        // In both cases, mark the progress map for this slot as duplicate
        return vec![ResultingStateChange::MarkSlotDuplicate(*bank_frozen_hash)];
    }

    vec![]
}

// Called when we receive either:
// 1) A duplicate slot signal from WindowStage,
// 2) Confirmation of a slot by observing votes from replay or gossip.
//
// This signals external information about this slot, which affects
// this validator's understanding of the validity of this slot
fn on_cluster_update(
    slot: Slot,
    bank_frozen_hash: &Hash,
    cluster_confirmed_hash: Option<&Hash>,
    is_slot_duplicate: bool,
    is_dead: bool,
) -> Vec<ResultingStateChange> {
    if is_dead {
        on_dead_slot(
            slot,
            bank_frozen_hash,
            cluster_confirmed_hash,
            is_slot_duplicate,
            is_dead,
        )
    } else if *bank_frozen_hash != Hash::default() {
        // This case is mutually exclusive with is_dead case above because if a slot is dead,
        // it cannot have  been frozen, and thus cannot have a non-default bank hash.
        on_frozen_slot(
            slot,
            bank_frozen_hash,
            cluster_confirmed_hash,
            is_slot_duplicate,
            is_dead,
        )
    } else {
        vec![]
    }
}

fn get_cluster_confirmed_hash<'a>(
    gossip_confirmed_hash: Option<&'a Hash>,
    local_frozen_hash: &'a Hash,
    is_local_replay_confirmed: bool,
) -> Option<&'a Hash> {
    let local_confirmed_hash = if is_local_replay_confirmed {
        // If local replay has confirmed this slot, this slot must have
        // descendants with votes for this slot, hence this slot must be
        // frozen.
        assert!(*local_frozen_hash != Hash::default());
        Some(local_frozen_hash)
    } else {
        None
    };

    match (local_confirmed_hash, gossip_confirmed_hash) {
        (Some(local_frozen_hash), Some(gossip_confirmed_hash)) => {
            assert_eq!(local_frozen_hash, gossip_confirmed_hash);
            Some(&local_frozen_hash)
        }
        (Some(local_frozen_hash), None) => Some(local_frozen_hash),
        _ => gossip_confirmed_hash,
    }
}

fn apply_state_changes(
    slot: Slot,
    progress: &mut ProgressMap,
    fork_choice: &mut HeaviestSubtreeForkChoice,
    descendants: &HashMap<Slot, HashSet<Slot>>,
    state_changes: Vec<ResultingStateChange>,
    duplicate_slots_to_repair: &mut HashSet<(Slot, Hash)>,
) {
    for state_change in state_changes {
        match state_change {
            ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash) => {
                progress.set_unconfirmed_duplicate_slot(
                    slot,
                    descendants.get(&slot).unwrap_or(&HashSet::default()),
                );
                fork_choice.mark_fork_invalid_candidate(&(slot, bank_frozen_hash));
            }
            ResultingStateChange::RepairConfirmedVersion(cluster_confirmed_hash) => {
                duplicate_slots_to_repair.insert((slot, cluster_confirmed_hash));
            }
            ResultingStateChange::ConfirmedSlotMatchesCluster(bank_frozen_hash) => {
                progress.set_confirmed_duplicate_slot(
                    slot,
                    descendants.get(&slot).unwrap_or(&HashSet::default()),
                );
                fork_choice.mark_fork_valid_candidate(&(slot, bank_frozen_hash));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn check_slot_agrees_with_cluster(
    slot: Slot,
    root: Slot,
    frozen_hash: Option<Hash>,
    gossip_confirmed_slots: &GossipConfirmedSlots,
    descendants: &HashMap<Slot, HashSet<Slot>>,
    progress: &mut ProgressMap,
    fork_choice: &mut HeaviestSubtreeForkChoice,
    duplicate_slots_to_repair: &mut HashSet<(Slot, Hash)>,
    slot_state_update: SlotStateUpdate,
) {
    if slot <= root {
        return;
    }

    if frozen_hash.is_none() {
        // If the bank doesn't even exist in BankForks yet,
        // then there's nothing to do as replay of the slot
        // hasn't even started
        return;
    }

    let frozen_hash = frozen_hash.unwrap();
    let gossip_confirmed_hash = gossip_confirmed_slots.get(&slot);
    let is_local_replay_confirmed = progress.is_confirmed(slot).expect("If the frozen hash exists, then the slot must exist in bank forks and thus in progress map");
    let cluster_confirmed_hash = get_cluster_confirmed_hash(
        gossip_confirmed_hash,
        &frozen_hash,
        is_local_replay_confirmed,
    );
    let mut is_slot_duplicate =
        progress.is_unconfirmed_duplicate(slot).expect("If the frozen hash exists, then the slot must exist in bank forks and thus in progress map");
    if matches!(slot_state_update, SlotStateUpdate::Duplicate) {
        if is_slot_duplicate {
            // Already processed duplicate signal for this slot, no need to continue
            return;
        } else {
            // Otherwise, mark the slot as duplicate so the appropriate state changes
            // will trigger
            is_slot_duplicate = true;
        }
    }
    let is_dead = progress.is_dead(slot).expect("If the frozen hash exists, then the slot must exist in bank forks and thus in progress map");

    let state_handler = slot_state_update.to_handler();
    let state_changes = state_handler(
        slot,
        &frozen_hash,
        cluster_confirmed_hash,
        is_slot_duplicate,
        is_dead,
    );
    apply_state_changes(
        slot,
        progress,
        fork_choice,
        descendants,
        state_changes,
        duplicate_slots_to_repair,
    );
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::consensus::test::VoteSimulator;
    use trees::tr;

    struct InitialState {
        heaviest_subtree_fork_choice: HeaviestSubtreeForkChoice,
        progress: ProgressMap,
        descendants: HashMap<Slot, HashSet<Slot>>,
        slot: Slot,
    }

    fn setup() -> InitialState {
        // Create simple fork 0 -> 1 -> 2 -> 3
        let forks = tr(0) / (tr(1) / (tr(2) / tr(3)));
        let mut vote_simulator = VoteSimulator::new(1);
        vote_simulator.fill_bank_forks(forks, &HashMap::new());
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
            slot: 0,
        }
    }

    #[test]
    fn test_frozen_duplicate() {
        // Common state
        let slot = 0;
        let cluster_confirmed_hash = None;
        let is_dead = false;

        // Slot is not detected as duplicate yet
        let mut is_slot_duplicate = false;

        // Simulate freezing the bank, add a
        // new non-default hash, should return
        // no actionable state changes yet
        let bank_frozen_hash = Hash::new_unique();
        assert!(on_frozen_slot(
            slot,
            &bank_frozen_hash,
            cluster_confirmed_hash,
            is_slot_duplicate,
            is_dead
        )
        .is_empty());

        // Now mark the slot as duplicate, should
        // trigger marking the slot as a duplicate
        is_slot_duplicate = true;
        assert_eq!(
            on_cluster_update(
                slot,
                &bank_frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
                is_dead
            ),
            vec![ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash)]
        );
    }

    #[test]
    fn test_frozen_confirmed() {
        // Common state
        let slot = 0;
        let is_slot_duplicate = false;
        let is_dead = false;

        // No cluster confirmed hash yet
        let mut cluster_confirmed_hash = None;

        // Simulate freezing the bank, add a
        // new non-default hash, should return
        // no actionable state changes
        let bank_frozen_hash = Hash::new_unique();
        assert!(on_frozen_slot(
            slot,
            &bank_frozen_hash,
            cluster_confirmed_hash,
            is_slot_duplicate,
            is_dead
        )
        .is_empty());

        // Now mark the same frozen slot hash as confirmed by the cluster,
        // should just confirm the slot
        cluster_confirmed_hash = Some(&bank_frozen_hash);
        assert_eq!(
            on_cluster_update(
                slot,
                &bank_frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
                is_dead
            ),
            vec![ResultingStateChange::ConfirmedSlotMatchesCluster(
                bank_frozen_hash
            ),]
        );

        // If the cluster_confirmed_hash does not match, then we
        // should trigger marking the slot as a duplicate, and also
        // try to repair correct version
        let mismatched_hash = Hash::new_unique();
        cluster_confirmed_hash = Some(&mismatched_hash);
        assert_eq!(
            on_cluster_update(
                slot,
                &bank_frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
                is_dead
            ),
            vec![
                ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash),
                ResultingStateChange::RepairConfirmedVersion(mismatched_hash),
            ]
        );
    }

    #[test]
    fn test_duplicate_frozen_confirmed() {
        // Common state
        let slot = 0;
        let is_dead = false;
        let is_slot_duplicate = true;

        // Bank is not frozen yet
        let mut cluster_confirmed_hash = None;
        let mut bank_frozen_hash = Hash::default();

        // Mark the slot as duplicate. Because our version of the slot is not
        // frozen yet, we don't know which version we have, so no action is
        // taken.
        assert!(on_cluster_update(
            slot,
            &bank_frozen_hash,
            cluster_confirmed_hash,
            is_slot_duplicate,
            is_dead
        )
        .is_empty());

        // Freeze the bank, should now mark the slot as duplicate since we have
        // not seen confirmation yet.
        bank_frozen_hash = Hash::new_unique();
        assert_eq!(
            on_cluster_update(
                slot,
                &bank_frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
                is_dead
            ),
            vec![ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash),]
        );

        // If the cluster_confirmed_hash matches, we just confirm
        // the slot
        cluster_confirmed_hash = Some(&bank_frozen_hash);
        assert_eq!(
            on_cluster_update(
                slot,
                &bank_frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
                is_dead
            ),
            vec![ResultingStateChange::ConfirmedSlotMatchesCluster(
                bank_frozen_hash
            ),]
        );

        // If the cluster_confirmed_hash does not match, then we
        // should trigger marking the slot as a duplicate, and also
        // try to repair correct version
        let mismatched_hash = Hash::new_unique();
        cluster_confirmed_hash = Some(&mismatched_hash);
        assert_eq!(
            on_cluster_update(
                slot,
                &bank_frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
                is_dead
            ),
            vec![
                ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash),
                ResultingStateChange::RepairConfirmedVersion(mismatched_hash),
            ]
        );
    }

    #[test]
    fn test_duplicate_confirmed() {
        let slot = 0;
        let correct_hash = Hash::new_unique();
        let cluster_confirmed_hash = Some(&correct_hash);
        let is_dead = false;
        // Bank is not frozen yet
        let bank_frozen_hash = Hash::default();

        // Because our version of the slot is not frozen yet, then even though
        // the cluster has confirmed a hash, we don't know which version we
        // have, so no action is taken.
        let is_slot_duplicate = true;
        assert!(on_cluster_update(
            slot,
            &bank_frozen_hash,
            cluster_confirmed_hash,
            is_slot_duplicate,
            is_dead
        )
        .is_empty());
    }

    #[test]
    fn test_duplicate_dead() {
        let slot = 0;
        let cluster_confirmed_hash = None;
        let is_dead = true;
        // Bank is not frozen yet
        let bank_frozen_hash = Hash::default();

        // Even though our version of the slot is dead, the cluster has not
        // confirmed a hash, we don't know which version we have, so no action
        // is taken.
        let is_slot_duplicate = true;
        assert!(on_cluster_update(
            slot,
            &bank_frozen_hash,
            cluster_confirmed_hash,
            is_slot_duplicate,
            is_dead
        )
        .is_empty());
    }

    #[test]
    fn test_confirmed_dead_duplicate() {
        let slot = 0;
        let correct_hash = Hash::new_unique();
        // Cluster has confirmed some version of the slot
        let cluster_confirmed_hash = Some(&correct_hash);
        // Our version of the slot is dead
        let is_dead = true;
        let bank_frozen_hash = Hash::default();

        // Even if the duplicate signal hasn't come in yet,
        // we can deduce the slot is duplicate AND we have,
        // the wrong version, so should mark the slot as duplicate,
        // and repair the correct version
        let mut is_slot_duplicate = false;
        assert_eq!(
            on_cluster_update(
                slot,
                &bank_frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
                is_dead
            ),
            vec![
                ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash),
                ResultingStateChange::RepairConfirmedVersion(correct_hash),
            ]
        );

        // If the duplicate signal comes in, nothing should change
        is_slot_duplicate = true;
        assert_eq!(
            on_cluster_update(
                slot,
                &bank_frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
                is_dead
            ),
            vec![
                ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash),
                ResultingStateChange::RepairConfirmedVersion(correct_hash),
            ]
        );
    }

    #[test]
    fn test_apply_state_changes() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            mut progress,
            descendants,
            slot,
        } = setup();

        // MarkSlotDuplicate should mark progress map and remove
        // the slot from fork choice
        apply_state_changes(
            slot,
            &mut progress,
            &mut heaviest_subtree_fork_choice,
            &descendants,
            vec![ResultingStateChange::MarkSlotDuplicate(Hash::default())],
            &mut HashSet::new(),
        );
        assert!(!heaviest_subtree_fork_choice
            .is_candidate_slot(&(slot, Hash::default()))
            .unwrap());
        for child_slot in descendants
            .get(&slot)
            .unwrap()
            .iter()
            .chain(std::iter::once(&slot))
        {
            assert_eq!(
                progress
                    .latest_unconfirmed_duplicate_ancestor(*child_slot)
                    .unwrap(),
                slot
            );
        }

        // ConfirmedSlotMatchesCluster should re-enable fork choice
        apply_state_changes(
            slot,
            &mut progress,
            &mut heaviest_subtree_fork_choice,
            &descendants,
            vec![ResultingStateChange::ConfirmedSlotMatchesCluster(
                Hash::default(),
            )],
            &mut HashSet::new(),
        );
        for child_slot in descendants
            .get(&slot)
            .unwrap()
            .iter()
            .chain(std::iter::once(&slot))
        {
            assert!(progress
                .latest_unconfirmed_duplicate_ancestor(*child_slot)
                .is_none());
        }
        assert!(heaviest_subtree_fork_choice
            .is_candidate_slot(&(slot, Hash::default()))
            .unwrap());
    }
}
