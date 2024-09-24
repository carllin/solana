use solana_sdk::{
    clock::{Slot, UnixTimestamp},
    hash::Hash,
    vote_new::state::Vote,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum VoteTransaction {
    Vote(Vote),
}

impl VoteTransaction {
    pub fn hash(&self) -> Hash {
        match self {
            VoteTransaction::Vote(vote) => vote.hash,
        }
    }

    pub fn timestamp(&self) -> Option<UnixTimestamp> {
        match self {
            VoteTransaction::Vote(vote) => vote.timestamp,
        }
    }

    pub fn last_voted_slot(&self) -> Slot {
        match self {
            VoteTransaction::Vote(vote) => vote.slot(),
        }
    }

    pub fn last_voted_slot_hash(&self) -> (Slot, Hash) {
        (self.last_voted_slot(), self.hash())
    }
}

impl From<Vote> for VoteTransaction {
    fn from(vote: Vote) -> Self {
        VoteTransaction::Vote(vote)
    }
}
