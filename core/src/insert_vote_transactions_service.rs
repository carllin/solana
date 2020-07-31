use crossbeam_channel::RecvTimeoutError;
use solana_ledger::{blockstore::Blockstore, blockstore_processor::ReplayTransactionReceiver};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, Builder, JoinHandle},
    time::Duration,
};

pub struct InsertVoteTransactionsService {
    thread_hdl: JoinHandle<()>,
}

impl InsertVoteTransactionsService {
    pub fn new(
        vote_transactions_receiver: ReplayTransactionReceiver,
        blockstore: Arc<Blockstore>,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let exit = exit.clone();
        let thread_hdl = Builder::new()
            .name("insert-vote-transaction-service".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(RecvTimeoutError::Disconnected) =
                    Self::recv_insert_vote_transactions(&vote_transactions_receiver, &blockstore)
                {
                    break;
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    fn recv_insert_vote_transactions(
        vote_transactions_receiver: &ReplayTransactionReceiver,
        blockstore: &Blockstore,
    ) -> Result<(), RecvTimeoutError> {
        let vote_transaction = vote_transactions_receiver.recv_timeout(Duration::from_secs(1))?;
        for (slot, tx) in
            std::iter::once(vote_transaction).chain(vote_transactions_receiver.try_iter())
        {
            blockstore
                .insert_vote_transaction(slot, &tx)
                .expect("Couldn't insert vote transaction in blockstore");
        }

        Ok(())
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
