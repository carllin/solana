//! The `consistency_manager` constructs votes and manages the voting status
//! for entries seen on the network

use bank::{Bank, VOTE_INTERVAL, ROLLBACK_INTERVAL};
use entry::Entry;
use signature::{KeyPair, KeyPairUtil, Signature};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use transaction::Transaction;

pub struct ConsistencyManager {
    bank: Arc<Bank>,
    keypair: KeyPair,
    transaction_sender: Sender<Vec<Transaction>>
}

impl ConsistencyManager {
    pub fn new(
        bank: Arc<Bank>,
        keypair: KeyPair,
        transaction_sender: Sender<Vec<Transaction>>,
    ) -> Self 
    {
        ConsistencyManager{bank, keypair, transaction_sender}
    }

    pub fn process_entries(&mut self, new_entries: Vec<(u64, Entry)>) {
        self.generate_and_send_votes(new_entries);
        self.check_rollback(new_entries);
    }

    fn generate_and_send_votes(&mut self, new_entries: Vec<(u64, Entry)>) {
        let votes = self.generate_votes(new_entries);
        self.transaction_sender.send(votes);
    }

    fn entry_to_vote(&self, entry: Entry) -> Signature {
        let sign_data = entry.id;
        Signature::clone_from_slice(self.keypair.sign(&sign_data).as_ref())
    }

    fn generate_votes(
        &mut self,
        new_entries: Vec<(u64, Entry)>,
    ) -> Vec<Transaction>
    {
        let votes = Vec::new();

        for (entry_height, entry) in new_entries {
            if entry_height % VOTE_INTERVAL != 0 {
                continue;
            }

            let sig = self.entry_to_vote(entry);
            votes.push(Transaction::new_validation_vote(
                &self.keypair,
                entry.id,
                sig,
                self.bank.last_id(),
            ));
        }

        votes
    }

    fn check_rollback(&self, new_entries: Vec<(u64, Entry)>) {

    }
}
