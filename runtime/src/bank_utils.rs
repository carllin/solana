use crate::{
    bank::{Bank, TransactionResults},
    genesis_utils::{self, GenesisConfigInfo, ValidatorVoteKeypairs},
    validator_vote_history::ValidatorVoteHistory,
    vote_sender_types::*,
};
use solana_sdk::{
    clock::Slot, contains::Contains, pubkey::Pubkey, signature::Signer, transaction::Transaction,
};
use solana_vote_program::vote_transaction;
use std::sync::RwLock;

pub fn setup_bank_and_vote_pubkeys(num_vote_accounts: usize, stake: u64) -> (Bank, Vec<Pubkey>) {
    // Create some voters at genesis
    let validator_voting_keypairs: Vec<_> = (0..num_vote_accounts)
        .map(|_| ValidatorVoteKeypairs::new_rand())
        .collect();

    let vote_pubkeys: Vec<_> = validator_voting_keypairs
        .iter()
        .map(|k| k.vote_keypair.pubkey())
        .collect();
    let GenesisConfigInfo { genesis_config, .. } =
        genesis_utils::create_genesis_config_with_vote_accounts(
            10_000,
            &validator_voting_keypairs,
            vec![stake; validator_voting_keypairs.len()],
        );
    let bank = Bank::new(&genesis_config);
    (bank, vote_pubkeys)
}

pub fn find_and_send_votes(
    bank_slot: Slot,
    txs: &[Transaction],
    tx_results: &TransactionResults,
    vote_sender: Option<&ReplayVoteSender>,
    vote_transaction_sender: Option<&ReplayVoteTransactionSender>,
    vote_history: &RwLock<ValidatorVoteHistory>,
    ancestors: &dyn Contains<Slot>,
) {
    let TransactionResults {
        processing_results,
        overwritten_vote_accounts,
        ..
    } = tx_results;
    for old_account in overwritten_vote_accounts {
        assert!(processing_results[old_account.transaction_result_index]
            .0
            .is_ok());
        let transaction = &txs[old_account.transaction_index];
        if let Some(parsed_vote) = vote_transaction::parse_vote_transaction(transaction) {
            vote_history.write().unwrap().insert_vote(
                &parsed_vote.1,
                &old_account.account,
                // The slot in which the vote transaction landed
                bank_slot,
                transaction.signatures[0],
                &parsed_vote.0,
                // Ancestors used to find in which slot the previous vote
                // transaction for this validator, on this fork, landed
                ancestors,
            );
            if let Some(voted_slot) = parsed_vote.1.slots.last().copied() {
                if let Some(vote_sender) = vote_sender {
                    let _ = vote_sender.send(parsed_vote);
                }
                if let Some(vote_transaction_sender) = vote_transaction_sender {
                    let _ = vote_transaction_sender.send((voted_slot, transaction.clone()));
                }
            }
        }
    }
}
