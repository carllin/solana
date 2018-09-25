//! The `leader_scheduler` module implements a structure and functions for tracking and
//! managing the schedule for leader rotation

use bank::Bank;
use bincode::serialize;
use byteorder::{LittleEndian, ReadBytesExt};
use hash::hash;
use signature::Pubkey;
use std::collections::HashMap;
use std::io::Cursor;

pub const ACTIVE_WINDOW: u64 = 5000;

struct ActiveValidators {
    // Map from validator id to the last entry height at which they voted,
    // TODO: could optimize range queries by combining this existing active_validators hashmap
    // with a BTreeMap<u64, HashMap<PubKey>> from entry height to list of validators
    // who voted at that height,
    active_validators: HashMap<Pubkey, u64>,
}

impl ActiveValidators {
    pub fn new() -> Self {
        ActiveValidators {
            active_validators: HashMap::new(),
        }
    }

    pub fn get_active_set(&mut self, entry_height: u64) -> Vec<Pubkey> {
        let lower_bound = entry_height.saturating_sub(ACTIVE_WINDOW);
        self.active_validators
            .retain(|_, entry_height| *entry_height > lower_bound);
        self.active_validators.keys().cloned().collect()
    }

    // Push a vote for a validator with id == "id" who voted at entry height == "entry_height"
    pub fn push_vote(&mut self, id: Pubkey, entry_height: u64) -> () {
        let old_height = self.active_validators.entry(id).or_insert(entry_height);
        if entry_height > *old_height {
            *old_height = entry_height;
        }
    }
}

pub struct LeaderScheduler {
    // The interval at which to rotate the leader, should be much less than
    // seed_rotation_interval
    pub leader_rotation_interval: u64,

    // The interval at which to generate the seed used for ranking the validators
    pub seed_rotation_interval: u64,

    // The first leader who will bootstrap the network
    pub bootstrap_leader: Pubkey,

    // The last entry height at which the bootstrap_leader will be in power before
    // the leader rotation process begins to pick future leaders
    pub bootstrap_entry_height: u64,

    // Maintain the set of active validators
    active_validators: ActiveValidators,

    // Round-robin ordering for the validators
    pub validator_rankings: Vec<Pubkey>,

    // The last entry height at which the seed was generated
    last_seed_entry_height: u64,

    // The index into the validator_rankings vector at which the round-robin leader selection
    // process begins
    start_ranking_index: usize,

    // The seed used to determine the round robin order of leaders
    seed: u64,
}

impl LeaderScheduler {
    pub fn new(
        bootstrap_leader: Pubkey,
        bootstrap_entry_height_option: Option<u64>,
        leader_rotation_interval_option: Option<u64>,
        seed_rotation_interval_option: Option<u64>,
    ) -> Self {
        let mut bootstrap_entry_height = 1000;
        if let Some(input) = bootstrap_entry_height_option {
            bootstrap_entry_height = input;
        }

        let mut leader_rotation_interval = 100;
        if let Some(input) = leader_rotation_interval_option {
            leader_rotation_interval = input;
        }

        let mut seed_rotation_interval = 1000;
        if let Some(input) = seed_rotation_interval_option {
            seed_rotation_interval = input;
        }

        LeaderScheduler {
            active_validators: ActiveValidators::new(),
            leader_rotation_interval,
            seed_rotation_interval,
            validator_rankings: Vec::new(),
            last_seed_entry_height: 0,
            start_ranking_index: 0,
            bootstrap_leader,
            bootstrap_entry_height,
            seed: 0,
        }
    }

    pub fn push_vote(&mut self, id: Pubkey, entry_height: u64) {
        self.active_validators.push_vote(id, entry_height);
    }

    fn get_active_set(&mut self, entry_height: u64) -> Vec<Pubkey> {
        self.active_validators.get_active_set(entry_height)
    }

    pub fn update_entry_height(&mut self, entry_height: u64, bank: &Bank) {
        if entry_height % self.seed_rotation_interval == 0 {
            self.generate_schedule(entry_height, bank);
        }
    }

    // Uses the schedule generated by the last call to generate_schedule() to return the
    // leader for a given entry height in round-robin fashion
    pub fn get_scheduled_leader(&self, entry_height: u64) -> Option<Pubkey> {
        if entry_height < self.bootstrap_entry_height {
            return Some(self.bootstrap_leader);
        }

        // Leaders for entry heights outside of the last seed_rotation_interval range
        // are undefined
        if entry_height >= self.last_seed_entry_height + self.seed_rotation_interval
            || entry_height < self.last_seed_entry_height
        {
            return None;
        }

        // Round down to nearest multiple of leader_rotation_interval
        assert!(!self.validator_rankings.is_empty());
        let index = entry_height / self.leader_rotation_interval;
        let validator_index =
            (self.start_ranking_index + index as usize) as usize % self.validator_rankings.len();
        Some(self.validator_rankings[validator_index])
    }

    // Called every seed_rotation_interval entries, generates the leader schedule
    // for the range of entries: [entry_height, entry_height + seed_rotation_interval)
    fn generate_schedule(&mut self, entry_height: u64, bank: &Bank) {
        let seed = Self::calculate_seed(entry_height);
        self.seed = seed;
        let active_set = self.get_active_set(entry_height);
        let ranked_active_set = Self::rank_active_set(bank, active_set);
        let (validator_rankings, total_tokens) = ranked_active_set.iter().fold(
            (Vec::with_capacity(ranked_active_set.len()), 0),
            |(mut ids, total_tokens), (pk, tokens)| {
                ids.push(*pk);
                (ids, total_tokens + *tokens)
            },
        );
        self.validator_rankings = validator_rankings;
        let ranked_accounts = ranked_active_set.into_iter().map(|(_, tokens)| tokens);
        self.start_ranking_index = Self::choose_account(ranked_accounts, self.seed, total_tokens);
        self.last_seed_entry_height = entry_height;
    }

    // TODO: Bank needs to pause while this is happening so we can accurately rank the
    // validators
    fn rank_active_set(bank: &Bank, active: Vec<Pubkey>) -> Vec<(Pubkey, u64)> {
        let bank_accounts = bank.accounts.read().unwrap();
        let mut active_accounts: Vec<(Pubkey, u64)> = active
            .into_iter()
            .filter_map(|pk| {
                bank_accounts
                    .get(&pk)
                    .filter(|account| account.tokens > 0)
                    .map(|account| (pk, account.tokens as u64))
            }).collect();

        active_accounts.sort_by(|(_, t1), (_, t2)| t1.cmp(&t2));
        active_accounts
    }

    fn calculate_seed(entry_height: u64) -> u64 {
        let bytes = hash(&serialize(&entry_height).unwrap()).0;
        let mut rdr = Cursor::new(bytes);
        rdr.read_u64::<LittleEndian>().unwrap()
    }

    fn choose_account<I>(tokens: I, seed: u64, total_tokens: u64) -> usize
    where
        I: Iterator<Item = u64>,
    {
        let mut total = 0;
        let mut chosen_account = 0;
        let seed = seed % total_tokens;
        for (i, tokens) in tokens.enumerate() {
            total += tokens;
            if total > seed {
                chosen_account = i;
                break;
            }
        }

        chosen_account
    }
}
