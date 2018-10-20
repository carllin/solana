//! The `vote_stage` votes on the `last_id` of the bank at a regular cadence

use bank::Bank;
use bincode::serialize;
use cluster_info::ClusterInfo;
use counter::Counter;
use hash::Hash;
use log::Level;
use packet::SharedBlob;
use result::{Error, Result};
use signature::{Keypair, KeypairUtil};
use solana_program_interface::pubkey::Pubkey;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use streamer::BlobSender;
use transaction::Transaction;
use vote_program::{Vote, VoteProgram};
use vote_transaction::VoteTransaction;

pub const VOTE_TIMEOUT_MS: u64 = 1000;

#[derive(Debug, PartialEq, Eq)]
pub enum VoteError {
    NoValidLastIdsToVoteOn,
    NoLeader,
    LeaderInfoNotFound,
}

// TODO: Change voting to be on fixed tick intervals based on bank state
pub fn create_new_signed_vote_blob(
    last_id: &Hash,
    keypair: &Keypair,
    vote_account: &Pubkey,
    bank: &Arc<Bank>,
    cluster_info: &Arc<RwLock<ClusterInfo>>,
) -> Result<SharedBlob> {
    let shared_blob = SharedBlob::default();
    let tick_height = bank.get_tick_height();
    let leader_id = {
        if let Some(leader_id) = bank.get_current_leader() {
            leader_id
        } else {
            return Err(Error::VoteError(VoteError::NoLeader));
        }
    };

    let leader_tpu = {
        let rcluster_info = cluster_info.read().unwrap();
        rcluster_info
            .table
            .get(&leader_id)
            .map(|leader| leader.contact_info.tpu)
    };

    if let Some(leader_tpu) = leader_tpu {
        //TODO: doesn't seem like there is a synchronous call to get height and id
        debug!("voting on {:?}", &last_id.as_ref()[..8]);
        let vote = Vote { tick_height };
        let tx = Transaction::vote_new(&keypair, *vote_account, vote, *last_id, 0);
        {
            let mut blob = shared_blob.write().unwrap();
            let bytes = serialize(&tx)?;
            let len = bytes.len();
            blob.data[..len].copy_from_slice(&bytes);
            blob.meta.set_addr(&leader_tpu);
            blob.meta.size = len;
        };
    } else {
        return Err(Error::VoteError(VoteError::LeaderInfoNotFound));
    }

    Ok(shared_blob)
}

// Find the account key for this validator in the Vote Contract.
// TODO: Better way to do this is to to pass in the vote account
// as a configurable value at startup.
pub fn find_vote_account(bank: &Arc<Bank>, keypair: &Arc<Keypair>) -> Pubkey {
    let bank_accounts = bank.accounts.read().unwrap();
    for (k, account) in bank_accounts.iter() {
        if VoteProgram::check_id(&account.program_id) {
            if let Ok(vote_state) = VoteProgram::deserialize(&account.userdata) {
                if vote_state.node_id == keypair.pubkey() {
                    return *k;
                }
            }
        }
    }

    Keypair::new().pubkey()
}

pub fn send_validator_vote(
    bank: &Arc<Bank>,
    keypair: &Arc<Keypair>,
    vote_account: &mut Pubkey,
    cluster_info: &Arc<RwLock<ClusterInfo>>,
    vote_blob_sender: &BlobSender,
) -> Result<()> {
    if *vote_account == Pubkey::default() {
        *vote_account = find_vote_account(bank, keypair);
    }

    let last_id = bank.last_id();
    let shared_blob =
        create_new_signed_vote_blob(&last_id, keypair, vote_account, bank, cluster_info)?;
    inc_new_counter_info!("replicate-vote_sent", 1);
    vote_blob_sender.send(vec![shared_blob])?;

    Ok(())
}
