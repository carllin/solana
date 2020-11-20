// Service to clean up dead slots in accounts_db
//
// This can be expensive since we have to walk the append vecs being cleaned up.

use crate::{
    bank::{Bank, BankSlotDelta},
    bank_forks::{BankForks, SnapshotConfig},
    snapshot_package::AccountsPackageSender,
    snapshot_utils,
};
use crossbeam_channel::{Receiver, SendError, Sender};
use log::*;
use rand::{thread_rng, Rng};
use solana_measure::measure::Measure;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{self, sleep, Builder, JoinHandle},
    time::Duration,
};

const INTERVAL_MS: u64 = 100;
const SHRUNKEN_ACCOUNT_PER_SEC: usize = 250;
const SHRUNKEN_ACCOUNT_PER_INTERVAL: usize =
    SHRUNKEN_ACCOUNT_PER_SEC / (1000 / INTERVAL_MS as usize);
const CLEAN_INTERVAL_BLOCKS: u64 = 100;

pub type SnapshotRequestSender = Sender<SnapshotRequest>;
pub type SnapshotRequestReceiver = Receiver<SnapshotRequest>;
pub type PrunedBanksSender = Sender<Vec<Arc<Bank>>>;
pub type PrunedBanksReceiver = Receiver<Vec<Arc<Bank>>>;

pub struct SnapshotRequest {
    pub snapshot_root_bank: Arc<Bank>,
    pub status_cache_slot_deltas: Vec<BankSlotDelta>,
}

pub struct SnapshotRequestHandler {
    pub snapshot_config: SnapshotConfig,
    pub snapshot_request_receiver: SnapshotRequestReceiver,
    pub accounts_package_sender: AccountsPackageSender,
}

impl SnapshotRequestHandler {
    // Returns the latest requested snapshot slot, if one exists
    pub fn handle_snapshot_requests(&self) -> Option<u64> {
        self.snapshot_request_receiver
            .try_iter()
            .last()
            .map(|snapshot_request| {
                let SnapshotRequest {
                    snapshot_root_bank,
                    status_cache_slot_deltas,
                } = snapshot_request;

                let mut hash_time = Measure::start("hash_time");
                snapshot_root_bank.update_accounts_hash();
                hash_time.stop();

                let mut shrink_time = Measure::start("shrink_time");
                snapshot_root_bank.process_stale_slot_with_budget(0, SHRUNKEN_ACCOUNT_PER_INTERVAL);
                shrink_time.stop();

                let mut clean_time = Measure::start("clean_time");
                // Don't clean the slot we're snapshotting because it may have zero-lamport
                // accounts that were included in the bank delta hash when the bank was frozen,
                // and if we clean them here, the newly created snapshot's hash may not match
                // the frozen hash.
                snapshot_root_bank.clean_accounts(true);
                clean_time.stop();

                // Generate an accounts package
                let mut snapshot_time = Measure::start("snapshot_time");
                let r = snapshot_utils::snapshot_bank(
                    &snapshot_root_bank,
                    status_cache_slot_deltas,
                    &self.accounts_package_sender,
                    &self.snapshot_config.snapshot_path,
                    &self.snapshot_config.snapshot_package_output_path,
                    self.snapshot_config.snapshot_version,
                    &self.snapshot_config.compression,
                );
                if r.is_err() {
                    warn!(
                        "Error generating snapshot for bank: {}, err: {:?}",
                        snapshot_root_bank.slot(),
                        r
                    );
                }
                snapshot_time.stop();

                // Cleanup outdated snapshots
                let mut purge_old_snapshots_time = Measure::start("purge_old_snapshots_time");
                snapshot_utils::purge_old_snapshots(&self.snapshot_config.snapshot_path);
                purge_old_snapshots_time.stop();

                datapoint_info!(
                    "handle_snapshot_requests-timing",
                    ("shrink_time", shrink_time.as_us(), i64),
                    ("clean_time", clean_time.as_us(), i64),
                    ("snapshot_time", snapshot_time.as_us(), i64),
                    (
                        "purge_old_snapshots_time",
                        purge_old_snapshots_time.as_us(),
                        i64
                    ),
                    ("hash_time", hash_time.as_us(), i64),
                );
                snapshot_root_bank.block_height()
            })
    }
}

#[derive(Default)]
pub struct ABSRequestSender {
    snapshot_request_sender: Option<SnapshotRequestSender>,
    pruned_banks_sender: Option<PrunedBanksSender>,
}

impl ABSRequestSender {
    pub fn new(
        snapshot_request_sender: Option<SnapshotRequestSender>,
        pruned_banks_sender: Option<PrunedBanksSender>,
    ) -> Self {
        ABSRequestSender {
            snapshot_request_sender,
            pruned_banks_sender,
        }
    }

    pub fn is_snapshot_creation_enabled(&self) -> bool {
        self.snapshot_request_sender.is_some()
    }

    pub fn send_snapshot_request(
        &self,
        snapshot_request: SnapshotRequest,
    ) -> Result<(), SendError<SnapshotRequest>> {
        if let Some(ref snapshot_request_sender) = self.snapshot_request_sender {
            snapshot_request_sender.send(snapshot_request)
        } else {
            Ok(())
        }
    }

    pub fn send_pruned_banks(
        &self,
        pruned_banks: Vec<Arc<Bank>>,
    ) -> Result<(), SendError<Vec<Arc<Bank>>>> {
        if let Some(ref pruned_banks_sender) = self.pruned_banks_sender {
            pruned_banks_sender.send(pruned_banks)
        } else {
            // If there is no sender, the banks are dropped immediately, which
            // is not recommended for anything other than tests.
            Ok(())
        }
    }
}

pub struct ABSRequestHandler {
    pub snapshot_request_handler: Option<SnapshotRequestHandler>,
    pub pruned_banks_receiver: PrunedBanksReceiver,
}

impl ABSRequestHandler {
    // Returns the latest requested snapshot block height, if one exists
    pub fn handle_snapshot_requests(&self) -> Option<u64> {
        self.snapshot_request_handler
            .as_ref()
            .and_then(|snapshot_request_handler| {
                snapshot_request_handler.handle_snapshot_requests()
            })
    }

    pub fn handle_pruned_banks<'a>(&'a self) -> impl Iterator<Item = Arc<Bank>> + 'a {
        self.pruned_banks_receiver.try_iter().flatten()
    }
}

pub struct AccountsBackgroundService {
    t_background: JoinHandle<()>,
}

impl AccountsBackgroundService {
    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        exit: &Arc<AtomicBool>,
        request_handler: ABSRequestHandler,
    ) -> Self {
        info!("AccountsBackgroundService active");
        let exit = exit.clone();
        let mut consumed_budget = 0;
        let mut last_cleaned_block_height = 0;
        let mut pending_drop_banks = vec![];
        let mut dropped_count = 0;
        let mut scan_time = 0;
        let mut drop_time = 0;
        let t_background = Builder::new()
            .name("solana-accounts-background".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }

                // Grab the current root bank
                let bank = bank_forks.read().unwrap().root_bank().clone();

                // Check to see if there were any requests for snapshotting banks
                // < the current root bank `bank` above.

                // Claim: Any snapshot request for slot `N` found here implies that the last cleanup
                // slot `M` satisfies `M < N`
                //
                // Proof: Assume for contradiction that we find a snapshot request for slot `N` here,
                // but cleanup has already happened on some slot `M >= N`. Because the call to
                // `bank.clean_accounts(true)` (in the code below) implies we only clean slots `<= bank - 1`,
                // then that means in some *previous* iteration of this loop, we must have gotten a root
                // bank for slot some slot `R` where `R > N`, but did not see the snapshot for `N` in the
                // snapshot request channel.
                //
                // However, this is impossible because BankForks.set_root() will always flush the snapshot
                // request for `N` to the snapshot request channel before setting a root `R > N`, and
                // snapshot_request_handler.handle_requests() will always look for the latest
                // available snapshot in the channel.
                let snapshot_block_height = request_handler.handle_snapshot_requests();

                if let Some(snapshot_block_height) = snapshot_block_height {
                    // Safe, see proof above
                    assert!(last_cleaned_block_height <= snapshot_block_height);
                    last_cleaned_block_height = snapshot_block_height;
                } else {
                    consumed_budget = bank.process_stale_slot_with_budget(
                        consumed_budget,
                        SHRUNKEN_ACCOUNT_PER_INTERVAL,
                    );

                    if bank.block_height() - last_cleaned_block_height
                        > (CLEAN_INTERVAL_BLOCKS + thread_rng().gen_range(0, 10))
                    {
                        bank.clean_accounts(true);
                        last_cleaned_block_height = bank.block_height();
                    }
                }

                pending_drop_banks.extend(request_handler.handle_pruned_banks());
                let mut scan_start = Measure::start("scan_time");

                // Should use `drain_filter()`, but not available in stable Rust yet.
                let (to_drop, to_keep) = pending_drop_banks
                    .into_iter()
                    .partition(|bank| Arc::strong_count(bank) == 1);
                scan_start.stop();
                scan_time += scan_start.as_us();

                pending_drop_banks = to_keep;
                dropped_count += to_drop.len();

                // Drop the banks that are dead
                let mut drop_start = Measure::start("drop_time");
                drop(to_drop);
                drop_start.stop();
                drop_time += drop_start.as_us();

                if dropped_count >= 100 {
                    datapoint_info!(
                        "drop-banks-timing",
                        ("scan_time", scan_time, i64),
                        ("drop_time", drop_time, i64),
                        ("dropped_count", dropped_count, i64),
                    );
                    scan_time = 0;
                    drop_time = 0;
                    dropped_count = 0;
                }

                sleep(Duration::from_millis(INTERVAL_MS));
            })
            .unwrap();
        Self { t_background }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_background.join()
    }
}
