//! The `replay_stage` replays transactions broadcast by the leader.

use crate::bank::Bank;
use crate::cluster_info::ClusterInfo;
use crate::counter::Counter;
use crate::db_ledger::DbLedger;
use crate::entry::{Entry, EntryReceiver, EntrySender, EntrySlice};
#[cfg(not(test))]
use crate::entry_stream::EntryStream;
use crate::entry_stream::EntryStreamHandler;
#[cfg(test)]
use crate::entry_stream::MockEntryStream as EntryStream;
use crate::fullnode::TvuRotationSender;
use crate::leader_scheduler::TICKS_PER_BLOCK;
use crate::packet::BlobError;
use crate::result::{Error, Result};
use crate::service::Service;
use crate::streamer::{responder, BlobSender};
use crate::tvu::TvuReturnType;
use crate::vote_signer_proxy::VoteSignerProxy;
use log::Level;
use solana_metrics::{influxdb, submit};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::timing::duration_as_ms;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, SyncSender};
use std::sync::{Arc, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::Instant;

pub const MAX_ENTRY_RECV_PER_ITER: usize = 512;

// Implement a destructor for the ReplayStage thread to signal it exited
// even on panics
struct Finalizer {
    exit_sender: Arc<AtomicBool>,
}

impl Finalizer {
    fn new(exit_sender: Arc<AtomicBool>) -> Self {
        Finalizer { exit_sender }
    }
}
// Implement a destructor for Finalizer.
impl Drop for Finalizer {
    fn drop(&mut self) {
        self.exit_sender.clone().store(true, Ordering::Relaxed);
    }
}

pub struct ReplayStage {
    t_responder: JoinHandle<()>,
    t_replay: JoinHandle<()>,
    exit: Arc<AtomicBool>,
    ledger_signal_sender: SyncSender<bool>,
}

impl ReplayStage {
    /// Process entry blobs, already in order
    #[allow(clippy::too_many_arguments)]
    fn process_entries(
        mut entries: Vec<Entry>,
        bank: &Arc<Bank>,
        cluster_info: &Arc<RwLock<ClusterInfo>>,
        my_id: Pubkey,
        vote_signer_proxy: Option<&Arc<VoteSignerProxy>>,
        vote_blob_sender: Option<&BlobSender>,
        ledger_entry_sender: &EntrySender,
        entry_height: &Arc<RwLock<u64>>,
        last_entry_id: &Arc<RwLock<Hash>>,
        entry_stream: Option<&mut EntryStream>,
    ) -> Result<()> {
        if let Some(stream) = entry_stream {
            stream.stream_entries(&entries).unwrap_or_else(|e| {
                error!("Entry Stream error: {:?}, {:?}", e, stream.socket);
            });
        }
        //coalesce all the available entries into a single vote
        submit(
            influxdb::Point::new("replicate-stage")
                .add_field("count", influxdb::Value::Integer(entries.len() as i64))
                .to_owned(),
        );

        let mut res = Ok(());
        let mut num_entries_to_write = entries.len();
        let now = Instant::now();
        if !entries.as_slice().verify(&last_entry_id.read().unwrap()) {
            inc_new_counter_info!("replicate_stage-verify-fail", entries.len());
            return Err(Error::BlobError(BlobError::VerificationFailed));
        }
        inc_new_counter_info!(
            "replicate_stage-verify-duration",
            duration_as_ms(&now.elapsed()) as usize
        );

        let (mut current_leader, _) = bank
            .get_current_leader()
            .expect("Scheduled leader should be calculated by this point");
        let already_leader = my_id == current_leader;
        let mut did_rotate = false;

        // Next vote tick is ceiling of (current tick/ticks per block)
        let mut num_ticks_to_next_vote = TICKS_PER_BLOCK - (bank.tick_height() % TICKS_PER_BLOCK);
        let mut start_entry_index = 0;
        for (i, entry) in entries.iter().enumerate() {
            inc_new_counter_info!("replicate-stage_bank-tick", bank.tick_height() as usize);
            if entry.is_tick() {
                num_ticks_to_next_vote -= 1;
            }
            inc_new_counter_info!(
                "replicate-stage_tick-to-vote",
                num_ticks_to_next_vote as usize
            );
            // If it's the last entry in the vector, i will be vec len - 1.
            // If we don't process the entry now, the for loop will exit and the entry
            // will be dropped.
            if 0 == num_ticks_to_next_vote || (i + 1) == entries.len() {
                res = bank.process_entries(&entries[start_entry_index..=i]);

                if res.is_err() {
                    // TODO: This will return early from the first entry that has an erroneous
                    // transaction, instead of processing the rest of the entries in the vector
                    // of received entries. This is in line with previous behavior when
                    // bank.process_entries() was used to process the entries, but doesn't solve the
                    // issue that the bank state was still changed, leading to inconsistencies with the
                    // leader as the leader currently should not be publishing erroneous transactions
                    inc_new_counter_info!(
                        "replicate-stage_failed_process_entries",
                        (i - start_entry_index)
                    );

                    break;
                }

                if 0 == num_ticks_to_next_vote {
                    if let Some(signer) = vote_signer_proxy {
                        if let Some(sender) = vote_blob_sender {
                            signer
                                .send_validator_vote(bank, &cluster_info, sender)
                                .unwrap();
                        }
                    }
                }
                let (scheduled_leader, _) = bank
                    .get_current_leader()
                    .expect("Scheduled leader should be calculated by this point");

                // TODO: Remove this soon once we boot the leader from ClusterInfo
                if scheduled_leader != current_leader {
                    did_rotate = true;
                    cluster_info.write().unwrap().set_leader(scheduled_leader);
                    current_leader = scheduled_leader
                }

                if !already_leader && my_id == scheduled_leader && did_rotate {
                    num_entries_to_write = i + 1;
                    break;
                }
                start_entry_index = i + 1;
                num_ticks_to_next_vote = TICKS_PER_BLOCK;
            }
        }

        // If leader rotation happened, only write the entries up to leader rotation.
        entries.truncate(num_entries_to_write);
        *last_entry_id.write().unwrap() = entries
            .last()
            .expect("Entries cannot be empty at this point")
            .id;

        inc_new_counter_info!(
            "replicate-transactions",
            entries.iter().map(|x| x.transactions.len()).sum()
        );

        let entries_len = entries.len() as u64;
        // TODO: In line with previous behavior, this will write all the entries even if
        // an error occurred processing one of the entries (causing the rest of the entries to
        // not be processed).
        if entries_len != 0 {
            ledger_entry_sender.send(entries)?;
        }

        *entry_height.write().unwrap() += entries_len;

        res?;
        inc_new_counter_info!(
            "replicate_stage-duration",
            duration_as_ms(&now.elapsed()) as usize
        );
        Ok(())
    }

    #[allow(clippy::new_ret_no_self, clippy::too_many_arguments)]
    pub fn new(
        my_id: Pubkey,
        vote_signer_proxy: Option<Arc<VoteSignerProxy>>,
        db_ledger: Arc<DbLedger>,
        bank: Arc<Bank>,
        cluster_info: Arc<RwLock<ClusterInfo>>,
        exit: Arc<AtomicBool>,
        entry_height: Arc<RwLock<u64>>,
        last_entry_id: Arc<RwLock<Hash>>,
        to_leader_sender: TvuRotationSender,
        entry_stream: Option<String>,
        ledger_signal_sender: SyncSender<bool>,
        ledger_signal_receiver: Receiver<bool>,
    ) -> (Self, EntryReceiver) {
        let (vote_blob_sender, vote_blob_receiver) = channel();
        let (ledger_entry_sender, ledger_entry_receiver) = channel();
        let send = UdpSocket::bind("0.0.0.0:0").expect("bind");
        let t_responder = responder("replay_stage", Arc::new(send), vote_blob_receiver);

        let (_, mut current_slot) = bank
            .get_current_leader()
            .expect("Scheduled leader should be calculated by this point");

        let mut max_tick_height_for_slot = bank
            .leader_scheduler
            .read()
            .unwrap()
            .max_tick_height_for_slot(current_slot);

        let exit_ = exit.clone();
        let t_replay = Builder::new()
            .name("solana-replay-stage".to_string())
            .spawn(move || {
                let _exit = Finalizer::new(exit_.clone());
                let (mut last_leader_id, _) = bank
                    .get_current_leader()
                    .expect("Scheduled leader should be calculated by this point");
                let mut entry_stream = entry_stream.map(EntryStream::new);
                'outer: loop {
                    // Loop through db_ledger MAX_ENTRY_RECV_PER_ITER entries at a time for each
                    // relevant slot to see if there are any available updates
                    loop {
                        // Stop getting entries if we get exit signal
                        if exit_.load(Ordering::Relaxed) {
                            break 'outer;
                        }

                        let current_entry_height = *entry_height.read().unwrap();
                        // Fetch the next entries from the database
                        if let Ok(entries) = db_ledger.get_slot_entries(
                            current_slot,
                            current_entry_height,
                            Some(MAX_ENTRY_RECV_PER_ITER as u64),
                        ) {
                            if entries.is_empty() {
                                break;
                            }
                            if let Err(e) = Self::process_entries(
                                entries,
                                &bank,
                                &cluster_info,
                                my_id,
                                vote_signer_proxy.as_ref(),
                                Some(&vote_blob_sender),
                                &ledger_entry_sender,
                                &entry_height,
                                &last_entry_id,
                                entry_stream.as_mut(),
                            ) {
                                error!("{:?}", e);
                            }
                        } else {
                            break;
                        }

                        let current_tick_height = bank.tick_height();

                        // We've reached the end of a slot, reset our state and check
                        // for leader rotation
                        if max_tick_height_for_slot == current_tick_height {
                            // Check for leader rotation
                            let leader_id = Self::get_leader(&bank, &cluster_info);
                            if leader_id != last_leader_id && my_id == leader_id {
                                to_leader_sender
                                    .send(TvuReturnType::LeaderRotation(
                                        bank.tick_height(),
                                        *entry_height.read().unwrap(),
                                        *last_entry_id.read().unwrap(),
                                    ))
                                    .unwrap();
                            }

                            current_slot += 1;
                            max_tick_height_for_slot = bank
                                .leader_scheduler
                                .read()
                                .unwrap()
                                .max_tick_height_for_slot(current_slot);
                            last_leader_id = leader_id;
                            continue;
                        }
                    }

                    // Block until there are updates again
                    {
                        if ledger_signal_receiver.recv().is_err() {
                            // Update disconnected, exit
                            break 'outer;
                        }
                    }
                }
            })
            .unwrap();

        (
            Self {
                t_responder,
                t_replay,
                exit,
                ledger_signal_sender,
            },
            ledger_entry_receiver,
        )
    }

    pub fn close(self) -> thread::Result<()> {
        self.exit();
        self.join()
    }

    pub fn exit(&self) {
        self.exit.store(true, Ordering::Relaxed);
        let _ = self.ledger_signal_sender.send(true);
    }

    fn get_leader(bank: &Bank, cluster_info: &Arc<RwLock<ClusterInfo>>) -> Pubkey {
        let (scheduled_leader, _) = bank
            .get_current_leader()
            .expect("Scheduled leader should be calculated by this point");

        // TODO: Remove this soon once we boot the leader from ClusterInfo
        cluster_info.write().unwrap().set_leader(scheduled_leader);

        scheduled_leader
    }
}

impl Service for ReplayStage {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.t_responder.join()?;
        self.t_replay.join()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::bank::Bank;
    use crate::cluster_info::{ClusterInfo, Node};
    use crate::db_ledger::create_tmp_sample_ledger;
    use crate::db_ledger::{DbLedger, DEFAULT_SLOT_HEIGHT};
    use crate::entry::create_ticks;
    use crate::entry::Entry;
    use crate::fullnode::Fullnode;
    use crate::genesis_block::GenesisBlock;
    use crate::leader_scheduler::{
        make_active_set_entries, LeaderScheduler, LeaderSchedulerConfig,
    };
    use crate::replay_stage::ReplayStage;
    use crate::tvu::TvuReturnType;
    use crate::vote_signer_proxy::VoteSignerProxy;
    use solana_sdk::hash::Hash;
    use solana_sdk::signature::{Keypair, KeypairUtil};
    use std::fs::remove_dir_all;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::{Arc, RwLock};
}
