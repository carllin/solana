use crate::bank_forks::BankForks;
use crate::staking_utils;
use hashbrown::{HashMap, HashSet};
use solana_metrics::influxdb;
use solana_runtime::bank::Bank;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use solana_vote_api::vote_instruction::Vote;
use solana_vote_api::vote_state::{Lockout, VoteState, MAX_LOCKOUT_HISTORY};
use std::sync::Arc;

pub const VOTE_THRESHOLD_DEPTH: usize = 8;
pub const VOTE_THRESHOLD_SIZE: f64 = 2f64 / 3f64;

#[derive(Default)]
pub struct EpochStakes {
    slot: u64,
    stakes: HashMap<Pubkey, u64>,
    self_staked: u64,
    total_staked: u64,
    delegate_id: Pubkey,
}

#[derive(Default, Debug)]
pub struct StakeLockout {
    lockout: u64,
    stake: u64,
}

#[derive(Default)]
pub struct Locktower {
    epoch_stakes: EpochStakes,
    threshold_depth: usize,
    threshold_size: f64,
    lockouts: VoteState,
}

impl EpochStakes {
    pub fn new(slot: u64, stakes: HashMap<Pubkey, u64>, delegate_id: &Pubkey) -> Self {
        let total_staked = stakes.values().sum();
        let self_staked = *stakes.get(&delegate_id).unwrap_or(&0);
        Self {
            slot,
            stakes,
            total_staked,
            self_staked,
            delegate_id: *delegate_id,
        }
    }
    pub fn new_for_tests(lamports: u64) -> Self {
        Self::new(
            0,
            vec![(Pubkey::default(), lamports)].into_iter().collect(),
            &Pubkey::default(),
        )
    }
    pub fn new_from_stake_accounts(slot: u64, accounts: &[(Pubkey, Account)]) -> Self {
        let stakes = accounts.iter().map(|(k, v)| (*k, v.lamports)).collect();
        Self::new(slot, stakes, &accounts[0].0)
    }
    pub fn new_from_bank(bank: &Bank, my_id: &Pubkey) -> Self {
        let bank_epoch = bank.get_epoch_and_slot_index(bank.slot()).0;
        let stakes = staking_utils::vote_account_balances_at_epoch(bank, bank_epoch)
            .expect("voting require a bank with stakes");
        Self::new(bank_epoch, stakes, my_id)
    }
}

impl Locktower {
    pub fn new_from_forks(bank_forks: &BankForks, my_id: &Pubkey) -> Self {
        let mut frozen_banks: Vec<_> = bank_forks.frozen_banks().values().cloned().collect();
        frozen_banks.sort_by_key(|b| (b.parents().len(), b.slot()));
        let epoch_stakes = {
            if let Some(bank) = frozen_banks.last() {
                EpochStakes::new_from_bank(bank, my_id)
            } else {
                return Self::default();
            }
        };

        let mut locktower = Self {
            epoch_stakes,
            threshold_depth: VOTE_THRESHOLD_DEPTH,
            threshold_size: VOTE_THRESHOLD_SIZE,
            lockouts: VoteState::default(),
        };

        let bank = locktower.find_heaviest_bank(bank_forks).unwrap();
        locktower.lockouts =
            Self::initialize_lockouts_from_bank(&bank, locktower.epoch_stakes.slot);
        locktower
    }
    pub fn new(epoch_stakes: EpochStakes, threshold_depth: usize, threshold_size: f64) -> Self {
        Self {
            epoch_stakes,
            threshold_depth,
            threshold_size,
            lockouts: VoteState::default(),
        }
    }
    pub fn collect_vote_lockouts<F>(
        &self,
        bank_slot: u64,
        vote_accounts: F,
        ancestors: &HashMap<u64, HashSet<u64>>,
    ) -> HashMap<u64, StakeLockout>
    where
        F: Iterator<Item = (Pubkey, Account)>,
    {
        let mut stake_lockouts = HashMap::new();
        for (key, account) in vote_accounts {
            let lamports: u64 = *self.epoch_stakes.stakes.get(&key).unwrap_or(&0);
            if lamports == 0 {
                continue;
            }
            let mut vote_state: VoteState = VoteState::deserialize(&account.data)
                .expect("bank should always have valid VoteState data");

            if key == self.epoch_stakes.delegate_id
                || vote_state.delegate_id == self.epoch_stakes.delegate_id
            {
                debug!("vote state {:?}", vote_state);
                debug!(
                    "observed slot {}",
                    vote_state.nth_recent_vote(0).map(|v| v.slot).unwrap_or(0) as i64
                );
                debug!("observed root {}", vote_state.root_slot.unwrap_or(0) as i64);
                solana_metrics::submit(
                    influxdb::Point::new("counter-locktower-observed")
                        .add_field(
                            "slot",
                            influxdb::Value::Integer(
                                vote_state.nth_recent_vote(0).map(|v| v.slot).unwrap_or(0) as i64,
                            ),
                        )
                        .add_field(
                            "root",
                            influxdb::Value::Integer(vote_state.root_slot.unwrap_or(0) as i64),
                        )
                        .to_owned(),
                );
            }
            let start_root = vote_state.root_slot;
            vote_state.process_vote(Vote { slot: bank_slot });
            for vote in &vote_state.votes {
                Self::update_ancestor_lockouts(&mut stake_lockouts, &vote, ancestors);
            }
            if start_root != vote_state.root_slot {
                if let Some(root) = start_root {
                    let vote = Lockout {
                        confirmation_count: MAX_LOCKOUT_HISTORY as u32,
                        slot: root,
                    };
                    trace!("ROOT: {}", vote.slot);
                    Self::update_ancestor_lockouts(&mut stake_lockouts, &vote, ancestors);
                }
            }
            if let Some(root) = vote_state.root_slot {
                let vote = Lockout {
                    confirmation_count: MAX_LOCKOUT_HISTORY as u32,
                    slot: root,
                };
                Self::update_ancestor_lockouts(&mut stake_lockouts, &vote, ancestors);
            }
            // each account hash a stake for all the forks in the active tree for this bank
            Self::update_ancestor_stakes(&mut stake_lockouts, bank_slot, lamports, ancestors);
        }
        stake_lockouts
    }

    pub fn is_slot_confirmed(&self, slot: u64, lockouts: &HashMap<u64, StakeLockout>) -> bool {
        lockouts
            .get(&slot)
            .map(|lockout| {
                (lockout.stake as f64 / self.epoch_stakes.total_staked as f64) > self.threshold_size
            })
            .unwrap_or(false)
    }

    pub fn is_recent_epoch(&self, bank: &Bank) -> bool {
        let bank_epoch = bank.get_epoch_and_slot_index(bank.slot()).0;
        bank_epoch >= self.epoch_stakes.slot
    }

    pub fn update_epoch(&mut self, bank: &Bank) {
        trace!(
            "updating bank epoch {} {}",
            bank.slot(),
            self.epoch_stakes.slot
        );
        let bank_epoch = bank.get_epoch_and_slot_index(bank.slot()).0;
        if bank_epoch != self.epoch_stakes.slot {
            assert!(
                self.is_recent_epoch(bank),
                "epoch_stakes cannot move backwards"
            );
            info!(
                "Locktower updated epoch bank {} {}",
                bank.slot(),
                self.epoch_stakes.slot
            );
            self.epoch_stakes = EpochStakes::new_from_bank(bank, &self.epoch_stakes.delegate_id);
            solana_metrics::submit(
                influxdb::Point::new("counter-locktower-epoch")
                    .add_field(
                        "slot",
                        influxdb::Value::Integer(self.epoch_stakes.slot as i64),
                    )
                    .add_field(
                        "self_staked",
                        influxdb::Value::Integer(self.epoch_stakes.self_staked as i64),
                    )
                    .add_field(
                        "total_staked",
                        influxdb::Value::Integer(self.epoch_stakes.total_staked as i64),
                    )
                    .to_owned(),
            );
        }
    }

    pub fn record_vote(&mut self, slot: u64) -> Option<u64> {
        let root_slot = self.lockouts.root_slot;
        self.lockouts.process_vote(Vote { slot });
        solana_metrics::submit(
            influxdb::Point::new("counter-locktower-vote")
                .add_field("latest", influxdb::Value::Integer(slot as i64))
                .add_field(
                    "root",
                    influxdb::Value::Integer(self.lockouts.root_slot.unwrap_or(0) as i64),
                )
                .to_owned(),
        );
        if root_slot != self.lockouts.root_slot {
            Some(self.lockouts.root_slot.unwrap())
        } else {
            None
        }
    }

    pub fn root(&self) -> Option<u64> {
        self.lockouts.root_slot
    }

    pub fn calculate_weight(&self, stake_lockouts: &HashMap<u64, StakeLockout>) -> u128 {
        let mut sum = 0u128;
        let root_slot = self.lockouts.root_slot.unwrap_or(0);
        for (slot, stake_lockout) in stake_lockouts {
            if self.lockouts.root_slot.is_some() && *slot <= root_slot {
                continue;
            }
            sum += u128::from(stake_lockout.lockout) * u128::from(stake_lockout.stake)
        }
        sum
    }

    pub fn has_voted(&self, slot: u64) -> bool {
        for vote in &self.lockouts.votes {
            if vote.slot == slot {
                return true;
            }
        }
        false
    }

    pub fn is_locked_out(&self, slot: u64, descendants: &HashMap<u64, HashSet<u64>>) -> bool {
        let mut lockouts = self.lockouts.clone();
        lockouts.process_vote(Vote { slot });
        for vote in &lockouts.votes {
            if vote.slot == slot {
                continue;
            }
            if !descendants[&vote.slot].contains(&slot) {
                return true;
            }
        }
        if let Some(root) = lockouts.root_slot {
            !descendants[&root].contains(&slot)
        } else {
            false
        }
    }

    pub fn check_vote_stake_threshold(
        &self,
        slot: u64,
        stake_lockouts: &HashMap<u64, StakeLockout>,
    ) -> bool {
        let mut lockouts = self.lockouts.clone();
        lockouts.process_vote(Vote { slot });
        let vote = lockouts.nth_recent_vote(self.threshold_depth);
        if let Some(vote) = vote {
            if let Some(fork_stake) = stake_lockouts.get(&vote.slot) {
                (fork_stake.stake as f64 / self.epoch_stakes.total_staked as f64)
                    > self.threshold_size
            } else {
                false
            }
        } else {
            true
        }
    }

    /// Update lockouts for all the ancestors
    fn update_ancestor_lockouts(
        stake_lockouts: &mut HashMap<u64, StakeLockout>,
        vote: &Lockout,
        ancestors: &HashMap<u64, HashSet<u64>>,
    ) {
        let mut slot_with_ancestors = vec![vote.slot];
        slot_with_ancestors.extend(ancestors.get(&vote.slot).unwrap_or(&HashSet::new()));
        for slot in slot_with_ancestors {
            let entry = &mut stake_lockouts.entry(slot).or_default();
            entry.lockout += vote.lockout();
        }
    }

    /// Update stake for all the ancestors.
    /// Note, stake is the same for all the ancestor.
    fn update_ancestor_stakes(
        stake_lockouts: &mut HashMap<u64, StakeLockout>,
        slot: u64,
        lamports: u64,
        ancestors: &HashMap<u64, HashSet<u64>>,
    ) {
        let mut slot_with_ancestors = vec![slot];
        slot_with_ancestors.extend(ancestors.get(&slot).unwrap_or(&HashSet::new()));
        for slot in slot_with_ancestors {
            let entry = &mut stake_lockouts.entry(slot).or_default();
            entry.stake += lamports;
        }
    }

    fn bank_weight(&self, bank: &Bank, ancestors: &HashMap<u64, HashSet<u64>>) -> u128 {
        let stake_lockouts =
            self.collect_vote_lockouts(bank.slot(), bank.vote_accounts(), ancestors);
        self.calculate_weight(&stake_lockouts)
    }

    fn find_heaviest_bank(&self, bank_forks: &BankForks) -> Option<Arc<Bank>> {
        let ancestors = bank_forks.ancestors();
        let mut bank_weights: Vec<_> = bank_forks
            .frozen_banks()
            .values()
            .map(|b| {
                (
                    self.bank_weight(b, &ancestors),
                    b.parents().len(),
                    b.clone(),
                )
            })
            .collect();
        bank_weights.sort_by_key(|b| (b.0, b.1));
        bank_weights.pop().map(|b| b.2)
    }

    fn initialize_lockouts_from_bank(bank: &Bank, current_epoch: u64) -> VoteState {
        let mut lockouts = VoteState::default();
        if let Some(iter) = staking_utils::node_staked_accounts_at_epoch(&bank, current_epoch) {
            for (delegate_id, _, account) in iter {
                if *delegate_id == bank.collector_id() {
                    let state = VoteState::deserialize(&account.data).expect("votes");
                    if lockouts.votes.len() < state.votes.len() {
                        lockouts = state;
                    }
                }
            }
        };
        lockouts
    }
}
