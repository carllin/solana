//! The `replicate_stage` replicates transactions broadcast by the leader.

use bank::Bank;
use ledger;
use result::Result;
use service::Service;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use streamer::BlobReceiver;

pub struct ReplicateStage {
<<<<<<< 9c456b2fb04ea73a7dc9447f311daef1f977b2e4
    thread_hdl: JoinHandle<()>,
=======
    pub thread_hdl: JoinHandle<()>,
    pub entry_height: u64,
>>>>>>> add validation voting
}

impl ReplicateStage {
    /// Process entry blobs, already in order
    fn replicate_requests(bank: &Arc<Bank>, blob_receiver: &BlobReceiver) -> Result<()> {
        let timer = Duration::new(1, 0);
        let blobs = blob_receiver.recv_timeout(timer)?;
        let blobs_len = blobs.len();
        let entries = ledger::reconstruct_entries_from_blobs(blobs)?;
        let res = bank.process_entries_return_state(entries);
        if res.is_err() {
            error!("process_entries {} {:?}", blobs_len, res);
        }
        res?;
        Ok(())
    }

    pub fn new(
        bank: Arc<Bank>,
        exit: Arc<AtomicBool>,
        window_receiver: BlobReceiver,
        entry_height: u64,
    ) -> Self 
    {
        let thread_hdl = Builder::new()
            .name("solana-replicate-stage".to_string())
            .spawn(move || loop {
                let e = Self::replicate_requests(&bank, &window_receiver);
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
