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
use std::process::exit;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::sync::{Arc, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use streamer::{responder, BlobReceiver, BlobSender};
use vote_stage::send_leader_vote;

const LEADER_SWITCH_EXIT_CODE: i32 = 3;

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
        // Note that entry height is not zero indexed, it starts at 1, so the
        // old leader is in power up to and including entry height
        // n * LEADER_ROTATION_INTERVAL, so once we've forwarded that last block,
        // check for the next leader. 
        if *entry_height % (LEADER_ROTATION_INTERVAL as u64) == 0 {
            let rcrdt = crdt.read().unwrap();
            let my_id = rcrdt.my_data().id;
            match rcrdt.get_scheduled_leader(*entry_height) {
                Some(id) if id == my_id => {}
            // If the leader stays in power for the next 
            // round as well, then we don't exit. Otherwise, exit.
                _ => exit(LEADER_SWITCH_EXIT_CODE),
            }
        }

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
    ) -> (Self, BlobReceiver) {
        let (vote_blob_sender, vote_blob_receiver) = channel();
        let send = UdpSocket::bind("0.0.0.0:0").expect("bind");
        let t_responder = responder(
            "write_stage_vote_sender",
            Arc::new(send),
            blob_recycler.clone(),
            vote_blob_receiver,
        );
        let (blob_sender, blob_receiver) = channel();
        let mut ledger_writer = LedgerWriter::recover(ledger_path).unwrap();

        let thread_hdl = Builder::new()
            .name("solana-writer".to_string())
            .spawn(move || {
                let mut last_vote = 0;
                let mut last_valid_validator_timestamp = 0;
                let id = crdt.read().unwrap().id;
                let mut entry_height = entry_height;
                loop {
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
                            Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
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
        (WriteStage { thread_hdls }, blob_receiver)
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
