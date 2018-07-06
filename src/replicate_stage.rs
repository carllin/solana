//! The `replicate_stage` replicates transactions broadcast by the leader.

use bank::Bank;
use consistency_manager::ConsistencyManager;
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

pub struct ReplicateStage {
    thread_hdl: JoinHandle<()>,
}

impl ReplicateStage {
    /// Process entry blobs, already in order
    fn replicate_requests(
        consistency_manager: &ConsistencyManager,
        blob_receiver: &BlobReceiver,
    ) -> Result<()> {
        let timer = Duration::new(1, 0);
        let blobs = blob_receiver.recv_timeout(timer)?;
        let blobs_len = blobs.len();
        let entries = ledger::reconstruct_entries_from_blobs(blobs)?;
        let res = consistency_manager.process_entries(entries);
        if res.is_err() {
            error!("process_entries {} {:?}", blobs_len, res);
        }
        res?;
        Ok(())
    }

    pub fn new(
        keypair: KeyPair,
        consistency_manager: ConsistencyManager,
        exit: Arc<AtomicBool>,
        window_receiver: BlobReceiver,
    ) -> Self 
    {
        let thread_hdl = Builder::new()
            .name("solana-replicate-stage".to_string())
            .spawn(move || loop {
                let e = Self::replicate_requests(&consistency_manager, &window_receiver);
                
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