//! The `write_stage` module implements the TPU's write stage. It
//! writes entries to the given writer, which is typically a file or
//! stdout, and then sends the Entry to its output channel.

use bank::Bank;
use counter::Counter;
use crdt::{Crdt, LEADER_ROTATION_INTERVAL};
use entry::Entry;
use ledger::{Block, LedgerWriter};
use log::Level;
use packet::BlobRecycler;
use result::{Error, Result};
use service::Service;
use signature::Keypair;
use std::cmp;
use std::net::UdpSocket;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use streamer::{responder, BlobReceiver, BlobSender};
use vote_stage::send_leader_vote;

pub struct WriteStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl WriteStage {
    /// Process any Entry items that have been published by the RecordStage.
    /// continuosly broadcast blobs of entries out
    pub fn write_and_send_entries(
        crdt: &Arc<RwLock<Crdt>>,
        bank: &Arc<Bank>,
        ledger_writer: &mut LedgerWriter,
        blob_sender: &BlobSender,
        blob_recycler: &BlobRecycler,
        entry_receiver: &Receiver<Vec<Entry>>,
        entry_height: &mut u64,
    ) -> Result<()> {
        let mut entries = entry_receiver.recv_timeout(Duration::new(1, 0))?;

        // Find out how many more entries we can squeeze in. Note if the leader stays in power
        // for the next round as well, then there will be a point at which
        // *entry_height % LEADER_ROTATION_INTERVAL == 0, which is ok b/c
        // that will set LEADER_ROTATION_INTERVAL - (*entry_height % LEADER_ROTATION_INTERVAL)
        // equal to LEADER_ROTATION_INTERVAL (represents LEADER_ROTATION_INTERVAL open slots),
        // which is what we want.
        let num_entries_to_take = cmp::min(
            LEADER_ROTATION_INTERVAL - (*entry_height % LEADER_ROTATION_INTERVAL),
            entries.len() as u64,
        ) as usize;

        *entry_height += num_entries_to_take as u64;
        entries.truncate(num_entries_to_take);

        let votes = &entries.votes();
        crdt.write().unwrap().insert_votes(&votes);

        ledger_writer.write_entries(entries.clone())?;

        for entry in &entries {
            if !entry.has_more {
                bank.register_entry_id(&entry.id);
            }
        }

        //TODO(anatoly): real stake based voting needs to change this
        //leader simply votes if the current set of validators have voted
        //on a valid last id

        trace!("New blobs? {}", entries.len());
        let blobs = entries.to_blobs(blob_recycler);

        if !blobs.is_empty() {
            inc_new_counter_info!("write_stage-recv_vote", votes.len());
            inc_new_counter_info!("write_stage-broadcast_blobs", blobs.len());
            trace!("broadcasting {}", blobs.len());
            blob_sender.send(blobs)?;
        }
        Ok(())
    }

    /// Create a new WriteStage for writing and broadcasting entries.
    pub fn new(
        keypair: Keypair,
        bank: Arc<Bank>,
        crdt: Arc<RwLock<Crdt>>,
        blob_recycler: BlobRecycler,
        ledger_path: &str,
        entry_receiver: Receiver<Vec<Entry>>,
        entry_height: u64,
    ) -> (Self, BlobReceiver, Sender<bool>) {
        let (vote_blob_sender, vote_blob_receiver) = channel();
        let send = UdpSocket::bind("0.0.0.0:0").expect("bind");
        let t_responder = responder(
            "write_stage_vote_sender",
            Arc::new(send),
            blob_recycler.clone(),
            vote_blob_receiver,
        );
        let (blob_sender, blob_receiver) = channel();
        let (exit_sender, exit_receiver) = channel();
        let mut ledger_writer = LedgerWriter::recover(ledger_path).unwrap();

        let thread_hdl = Builder::new()
            .name("solana-writer".to_string())
            .spawn(move || {
                let mut last_vote = 0;
                let mut last_valid_validator_timestamp = 0;
                let id = crdt.read().unwrap().id;
                let mut entry_height = entry_height;
                loop {
                    // Note that entry height is not zero indexed, it starts at 1, so the
                    // old leader is in power up to and including entry height
                    // n * LEADER_ROTATION_INTERVAL, so once we've forwarded that last block,
                    // check for the next leader. If we happen to exit due to leader rotation
                    // before the send_leader_vote() below, the next leader will have to
                    // take care of voting
                    if entry_height % (LEADER_ROTATION_INTERVAL as u64) == 0 {
                        let rcrdt = crdt.read().unwrap();
                        let my_id = rcrdt.my_data().id;
                        let a = rcrdt.get_scheduled_leader(entry_height);
                        match a {
                            Some(id) if id == my_id => (),
                            // If the leader stays in power for the next
                            // round as well, then we don't exit. Otherwise, exit.
                            _ => {
                                // Make sure broadcast stage has received the last blob sent
                                // before we exit and shut down the channel
                                let _ = exit_receiver.recv();
                                return;
                            }
                        }
                    }

                    if let Err(e) = Self::write_and_send_entries(
                        &crdt,
                        &bank,
                        &mut ledger_writer,
                        &blob_sender,
                        &blob_recycler,
                        &entry_receiver,
                        &mut entry_height,
                    ) {
                        match e {
                            Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => {
                                break;
                            }
                            Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                            _ => {
                                inc_new_counter_info!(
                                    "write_stage-write_and_send_entries-error",
                                    1
                                );
                                error!("{:?}", e);
                            }
                        }
                    };
                    if let Err(e) = send_leader_vote(
                        &id,
                        &keypair,
                        &bank,
                        &crdt,
                        &blob_recycler,
                        &vote_blob_sender,
                        &mut last_vote,
                        &mut last_valid_validator_timestamp,
                    ) {
                        inc_new_counter_info!("write_stage-leader_vote-error", 1);
                        error!("{:?}", e);
                    }
                }
            }).unwrap();

        let thread_hdls = vec![t_responder, thread_hdl];
        (WriteStage { thread_hdls }, blob_receiver, exit_sender)
    }
}

impl Service for WriteStage {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bank::Bank;
    use crdt::{Crdt, Node, LEADER_ROTATION_INTERVAL};
    use entry::Entry;
    use ledger::{genesis, read_ledger};
    use packet::BlobRecycler;
    use recorder::Recorder;
    use service::Service;
    use signature::{Keypair, KeypairUtil, Pubkey};
    use std::fs::remove_dir_all;
    use std::sync::mpsc::{channel, Sender};
    use std::sync::{Arc, RwLock};
    use std::thread::sleep;
    use std::time::Duration;
    use write_stage::WriteStage;

    fn process_ledger(ledger_path: &str, bank: &Bank) -> (u64, Vec<Entry>) {
        let entries = read_ledger(ledger_path, true).expect("opening ledger");

        let entries = entries
            .map(|e| e.unwrap_or_else(|err| panic!("failed to parse entry. error: {}", err)));

        info!("processing ledger...");
        bank.process_ledger(entries).expect("process_ledger")
    }

    fn setup_dummy_write_stage() -> (
        Pubkey,
        WriteStage,
        Sender<bool>,
        Sender<Vec<Entry>>,
        Arc<RwLock<Crdt>>,
        Arc<Bank>,
        String,
        Vec<Entry>,
    ) {
        // Setup leader info
        let leader_keypair = Keypair::new();
        let id = leader_keypair.pubkey();
        let leader_info = Node::new_localhost_with_pubkey(leader_keypair.pubkey());

        let crdt = Arc::new(RwLock::new(Crdt::new(leader_info.info).expect("Crdt::new")));
        let bank = Bank::new_default(true);
        let bank = Arc::new(bank);
        let blob_recycler = BlobRecycler::default();

        // Make a ledger
        let (_, leader_ledger_path) = genesis("test_leader_rotation_exit", 10_000);

        let (entry_height, ledger_tail) = process_ledger(&leader_ledger_path, &bank);

        // Make a dummy pipe
        let (entry_sender, entry_receiver) = channel();

        // Start up the write stage
        let (write_stage, _, exit_sender) = WriteStage::new(
            leader_keypair,
            bank.clone(),
            crdt.clone(),
            blob_recycler,
            &leader_ledger_path,
            entry_receiver,
            entry_height,
        );

        (
            id,
            write_stage,
            exit_sender,
            entry_sender,
            crdt,
            bank,
            leader_ledger_path,
            ledger_tail,
        )
    }

    #[test]
    fn test_write_stage_leader_rotation_exit() {
        let (
            id,
            write_stage,
            exit_sender,
            entry_sender,
            crdt,
            bank,
            leader_ledger_path,
            ledger_tail,
        ) = setup_dummy_write_stage();

        crdt.write()
            .unwrap()
            .set_scheduled_leader(LEADER_ROTATION_INTERVAL, id);

        let last_entry_hash = ledger_tail.last().expect("Ledger should not be empty").id;

        let genesis_entry_height = ledger_tail.len() as u64;

        // Input enough entries to make exactly LEADER_ROTATION_INTERVAL entries, which will
        // trigger a check for leader rotation. Because the next scheduled leader
        // is ourselves, we won't exit
        let mut recorder = Recorder::new(last_entry_hash);
        let mut new_entries = vec![];
        for _ in genesis_entry_height..LEADER_ROTATION_INTERVAL {
            let new_entry = recorder.record(vec![]);
            new_entries.extend(new_entry);
        }

        entry_sender.send(new_entries).unwrap();

        // Wait until at least LEADER_ROTATION_INTERVAL have been written to the ledger
        loop {
            sleep(Duration::from_secs(1));
            let (current_entry_height, _) = process_ledger(&leader_ledger_path, &bank);

            if current_entry_height == LEADER_ROTATION_INTERVAL {
                break;
            }
        }

        // Set the scheduled next leader in the crdt with to some other node
        let leader2_keypair = Keypair::new();
        let leader2_info = Node::new_localhost_with_pubkey(leader2_keypair.pubkey());

        {
            let mut wcrdt = crdt.write().unwrap();
            wcrdt.insert(&leader2_info.info);
            wcrdt.set_scheduled_leader(2 * LEADER_ROTATION_INTERVAL, leader2_keypair.pubkey());
        }

        // Input another LEADER_ROTATION_INTERVAL dummy entries one at a time,
        // which will take us past the point of the leader rotation.
        // The write_stage will see that it's no longer the leader after
        // checking the crdt, and exit
        for _ in 0..LEADER_ROTATION_INTERVAL {
            let new_entry = recorder.record(vec![]);
            entry_sender.send(new_entry).unwrap();
        }

        // Make sure the threads closed cleanly
        exit_sender.send(true).unwrap();
        write_stage.join().unwrap();

        // Make sure the ledger contains exactly LEADER_ROTATION_INTERVAL entries
        let (entry_height, _) = process_ledger(&leader_ledger_path, &bank);
        remove_dir_all(leader_ledger_path).unwrap();
        assert_eq!(entry_height, 2 * LEADER_ROTATION_INTERVAL);
    }
}
