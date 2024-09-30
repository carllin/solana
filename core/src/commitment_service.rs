use {
    crate::consensus::Stake,
    crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender},
    solana_measure::measure::Measure,
    solana_metrics::datapoint_info,
    solana_rpc::rpc_subscriptions::RpcSubscriptions,
    solana_runtime::{
        bank::Bank,
        commitment::{BlockCommitment, BlockCommitmentCache, CommitmentSlots, VOTE_THRESHOLD_SIZE},
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    solana_vote_new_program::vote_state_new::VoteState,
    std::{
        cmp::max,
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

pub struct CommitmentAggregationData {
    bank: Arc<Bank>,
    root: Slot,
    total_stake: Stake,
    // The latest local vote state of the node running this service.
    // Used for commitment aggregation if the node's vote account is staked.
    node_vote_state: (Pubkey, VoteState),
}

impl CommitmentAggregationData {
    pub fn new(
        bank: Arc<Bank>,
        root: Slot,
        total_stake: Stake,
        node_vote_state: (Pubkey, VoteState),
    ) -> Self {
        Self {
            bank,
            root,
            total_stake,
            node_vote_state,
        }
    }
}

fn get_highest_super_majority_root(mut rooted_stake: Vec<(Slot, u64)>, total_stake: u64) -> Slot {
    rooted_stake.sort_by(|a, b| a.0.cmp(&b.0).reverse());
    let mut stake_sum = 0;
    for (root, stake) in rooted_stake {
        stake_sum += stake;
        if (stake_sum as f64 / total_stake as f64) > VOTE_THRESHOLD_SIZE {
            return root;
        }
    }
    0
}

pub struct AggregateCommitmentService {
    t_commitment: JoinHandle<()>,
}

impl AggregateCommitmentService {
    pub fn new(
        exit: Arc<AtomicBool>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        subscriptions: Arc<RpcSubscriptions>,
    ) -> (Sender<CommitmentAggregationData>, Self) {
        let (sender, receiver): (
            Sender<CommitmentAggregationData>,
            Receiver<CommitmentAggregationData>,
        ) = unbounded();
        (
            sender,
            Self {
                t_commitment: Builder::new()
                    .name("solAggCommitSvc".to_string())
                    .spawn(move || loop {
                        if exit.load(Ordering::Relaxed) {
                            break;
                        }

                        if let Err(RecvTimeoutError::Disconnected) =
                            Self::run(&receiver, &block_commitment_cache, &subscriptions, &exit)
                        {
                            break;
                        }
                    })
                    .unwrap(),
            },
        )
    }

    fn run(
        receiver: &Receiver<CommitmentAggregationData>,
        block_commitment_cache: &RwLock<BlockCommitmentCache>,
        subscriptions: &Arc<RpcSubscriptions>,
        exit: &AtomicBool,
    ) -> Result<(), RecvTimeoutError> {
        /*loop {
            if exit.load(Ordering::Relaxed) {
                return Ok(());
            }

            let mut aggregate_commitment_time = Measure::start("aggregate-commitment-ms");
            // TODO: just send the highest root here
            aggregate_commitment_time.stop();

            datapoint_info!(
                "block-commitment-cache",
                (
                    "aggregate-commitment-ms",
                    aggregate_commitment_time.as_ms() as i64,
                    i64
                ),
            );

            // Triggers rpc_subscription notifications as soon as new commitment data is available,
            // sending just the commitment cache slot information that the notifications thread
            // needs
            subscriptions.notify_subscribers(update_commitment_slots);
        }*/
        Ok(())
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_commitment.join()
    }
}
