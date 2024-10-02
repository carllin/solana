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
    root: Slot,
}

impl CommitmentAggregationData {
    pub fn new(root: Slot) -> Self {
        CommitmentAggregationData { root }
    }
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
        loop {
            if exit.load(Ordering::Relaxed) {
                return Ok(());
            }

            let aggregation_data = receiver.recv_timeout(Duration::from_secs(1))?;
            let aggregation_data = receiver.try_iter().last().unwrap_or(aggregation_data);
            let root = aggregation_data.root;
            let mut aggregate_commitment_time = Measure::start("aggregate-commitment-ms");
            let update_commitment_slots =
                Self::update_commitment_cache(block_commitment_cache, aggregation_data);
            aggregate_commitment_time.stop();

            // Triggers rpc_subscription notifications as soon as new commitment data is available,
            // sending just the commitment cache slot information that the notifications thread
            // needs
            subscriptions.notify_subscribers(CommitmentSlots {
                root,
                slot: root,
                highest_confirmed_slot: root,
                highest_super_majority_root: root,
            });
        }
        Ok(())
    }

    fn update_commitment_cache(
        block_commitment_cache: &RwLock<BlockCommitmentCache>,
        aggregation_data: CommitmentAggregationData,
    ) -> CommitmentSlots {
        let mut new_block_commitment = BlockCommitmentCache::new(
            HashMap::default(),
            0,
            CommitmentSlots {
                slot: aggregation_data.root,
                root: aggregation_data.root,
                highest_confirmed_slot: aggregation_data.root,
                highest_super_majority_root: aggregation_data.root,
            },
        );
        new_block_commitment.set_highest_confirmed_slot(aggregation_data.root);
        let mut w_block_commitment_cache = block_commitment_cache.write().unwrap();
        new_block_commitment.set_highest_super_majority_root(aggregation_data.root);
        *w_block_commitment_cache = new_block_commitment;
        w_block_commitment_cache.commitment_slots()
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_commitment.join()
    }
}
