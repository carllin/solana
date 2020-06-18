use crate::{
    consensus::{ComputedBankState, SwitchForkDecision, Tower},
    progress_map::ProgressMap,
    replay_stage::HeaviestForkFailures,
};
use solana_ledger::bank_forks::BankForks;
use solana_runtime::bank::Bank;
use solana_sdk::clock::Slot;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, RwLock},
};

pub(crate) struct SelectVoteAndResetForkResult {
    pub vote_bank: Option<(Arc<Bank>, SwitchForkDecision)>,
    pub reset_bank: Option<Arc<Bank>>,
    pub heaviest_fork_failures: Vec<HeaviestForkFailures>,
}

pub(crate) trait ForkChoice {
    fn compute_bank_stats(
        &mut self,
        bank: &Bank,
        tower: &Tower,
        progress: &mut ProgressMap,
        computed_bank_stats: &ComputedBankState,
    );

    // Returns:
    // 1) The heaviest overall bbank
    // 2) The heavest bank on the same fork as the last vote (doesn't require a
    // switching proof to vote for)
    fn select_forks(
        &mut self,
        frozen_banks: &[Arc<Bank>],
        tower: &Tower,
        progress: &ProgressMap,
        ancestors: &HashMap<u64, HashSet<u64>>,
        bank_forks: &RwLock<BankForks>,
        vote_slots: &mut VecDeque<Slot>,
    ) -> (Arc<Bank>, Option<Arc<Bank>>);
}
