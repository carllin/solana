//! The `replicate_stage` replicates transactions broadcast by the leader.

use bank::Bank;
use entry::Entry;
use ledger;
use result::Result;
use service::Service;
use signature::KeyPair;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use streamer::BlobReceiver;
use transaction::Transaction;
use vote_manager::VoteManager;

pub struct ReplicateStage {
    thread_hdl: JoinHandle<()>,
}

impl ReplicateStage {
    /// Process entry blobs, already in order
    fn replicate_requests(
        bank: &Arc<Bank>,
        blob_receiver: &BlobReceiver,
    ) -> Result<Vec<(u64, Entry)>>
    {
        let timer = Duration::new(1, 0);
        let blobs = blob_receiver.recv_timeout(timer)?;
        let blobs_len = blobs.len();
        let entries = ledger::reconstruct_entries_from_blobs(blobs)?;

        let valid_entries = Vec::new();
        let old_height = bank.entry_height();
        let (num_valid_entries, res) = bank.process_entries(entries);
        
        if res.is_err() {
            error!("process_entries {} {:?}", blobs_len, res);
        }

        let result = Vec::new();

        for i in 0..num_valid_entries {
            result.push(old_height + i + 1, entries[i]);
        }

        Ok(result)
    }

    pub fn new(
        keypair: KeyPair,
        bank: Arc<Bank>,
        exit: Arc<AtomicBool>,
        window_receiver: BlobReceiver,
        transaction_sender: Sender<Vec<Transaction>>,
    ) -> Self 
    {
        let consistency_manager = ConsistencyManager::new(
            bank,
            keypair,
            transaction_sender
        );

        let thread_hdl = Builder::new()
            .name("solana-replicate-stage".to_string())
            .spawn(move || loop {
                let e = Self::replicate_requests(&bank, &window_receiver);

                if e.is_ok() {
                    consistency_manager.process_entries(e.unwrap());
                }
                
                if e.is_err() && exit.load(Ordering::Relaxed) {
                    break;
                }
            })
            .unwrap();
        ReplicateStage { thread_hdl }
    }
}

impl Service for ReplicateStage {
    fn thread_hdls(self) -> Vec<JoinHandle<()>> {
        vec![self.thread_hdl]
    }

    fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}