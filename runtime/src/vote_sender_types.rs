use crossbeam_channel::{Receiver, Sender};
use solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey, transaction::Transaction};
use solana_vote_program::vote_state::Vote;

pub type ReplayedVote = (Pubkey, Vote, Option<Hash>);
pub type ReplayVoteSender = Sender<ReplayedVote>;
pub type ReplayVoteReceiver = Receiver<ReplayedVote>;
pub type ReplayVoteTransactionSender = Sender<(Slot, Transaction)>;
pub type ReplayVoteTransactionReceiver = Receiver<(Slot, Transaction)>;
