use {
    solana_program::vote_new::{
        self,
        state::{Vote, VoteRange},
    },
    solana_sdk::{
        clock::Slot,
        hash::Hash,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
};

pub fn new_vote_transaction(
    vote: Vote,
    blockhash: Hash,
    node_keypair: &Keypair,
    vote_keypair: &Keypair,
    authorized_voter_keypair: &Keypair,
    switch_proof_hash: Option<Hash>,
) -> Transaction {
    let vote_ix = if let Some(switch_proof_hash) = switch_proof_hash {
        vote_new::instruction::vote_switch(
            &vote_keypair.pubkey(),
            &authorized_voter_keypair.pubkey(),
            vote,
            switch_proof_hash,
        )
    } else {
        vote_new::instruction::vote(
            &vote_keypair.pubkey(),
            &authorized_voter_keypair.pubkey(),
            vote,
        )
    };

    let mut vote_tx = Transaction::new_with_payer(&[vote_ix], Some(&node_keypair.pubkey()));

    vote_tx.partial_sign(&[node_keypair], blockhash);
    vote_tx.partial_sign(&[authorized_voter_keypair], blockhash);
    vote_tx
}
