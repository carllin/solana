//! The `vote_stage` constructs votes to broadcast to the network

use ledger;
use entry::Entry;
use result::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use streamer::BlobReceiver;

/// The number of entries between submitting votes
pub const VOTE_INTERVAL: u32 = 1000;

/// The number of entries waiting for supermajority, otherwise trigger rollback
pub const ROLLBACK_TIMEOUT: u32 = 5 * VOTE_INTERVAL;

pub struct VoteStage {
    pub thread_hdl: JoinHandle<()>,
}

impl VoteStage {
    fn entry_to_vote(entry: Entry, keypair: &KeyPair) {
        let sign_data = entry.id;
        Signature::clone_from_slice(keypair.sign(&sign_data).as_ref());
    }

    /// Process entry blobs, already in order
    fn generate_vote(
        crdt: &Arc<RwLock<Crdt>>,
        entry_receiver: &Receiver<Entry>,
        entry_height,
    ) -> Result<()>
    {
        let timer = Duration::new(1, 0);
        let entries = entry_receiver.recv_timeout(timer)?;
        let entries_len = entries.len();

        let next_vote_gap = VOTE_INTERVAL - (entry_height % VOTE_INTERVAL);

        if next_vote_gap > num_entries {
            return Ok(entry_height + entries_len);
        }

        entry_height += next_vote_gap;

        for i in next_vote_gap...entries_len {
            let vote = entry_to_vote(entries[i - 1])
            i += VOTE_INTERVAL;
        }

        Ok(entry_height)
    }

    pub fn new(
        crdt: Arc<RwLock<Crdt>>,
        entry_receiver: Receiver<Entry>,
        entry_height: u64,
        exit: Arc<AtomicBool>,
    ) -> Self 
    {
        let thread_hdl = Builder::new()
            .name("solana-vote-stage".to_string())
            .spawn(move || loop {
                let entry_height = 
                    Self::generate_vote(&crdt, &entry_receiver, &entry_receiver, entry_height);
                if e.is_err() && exit.load(Ordering::Relaxed) {
                    break;
                }
            })
            .unwrap();
        VoteStage { thread_hdl }
    }
}
