//! Crds Gossip Pull overlay
//! This module implements the anti-entropy protocol for the network.
//!
//! The basic strategy is as follows:
//! 1. Construct a bloom filter of the local data set
//! 2. Randomly ask a node on the network for data that is not contained in the bloom filter.
//!
//! Bloom filters have a false positive rate.  Each requests uses a different bloom filter
//! with random hash functions.  So each subsequent request will have a different distribution
//! of false positives.

use crate::bloom::Bloom;
use crate::crds::Crds;
use crate::crds_gossip::CRDS_GOSSIP_BLOOM_SIZE;
use crate::crds_gossip_error::CrdsGossipError;
use crate::crds_value::{CrdsValue, CrdsValueLabel};
use crate::packet::BLOB_DATA_SIZE;
use bincode::serialized_size;
use hashbrown::HashMap;
use rand;
use rand::distributions::{Distribution, WeightedIndex};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use std::cmp;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

pub const CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS: u64 = 15000;

pub struct CrdsGossipPull {
    /// timestamp of last request
    pub pull_request_time: HashMap<Pubkey, u64>,
    /// hash and insert time
    purged_values: VecDeque<(Hash, u64)>,
    /// max bytes per message
    pub max_bytes: usize,
    pub crds_timeout: u64,
}

impl Default for CrdsGossipPull {
    fn default() -> Self {
        Self {
            purged_values: VecDeque::new(),
            pull_request_time: HashMap::new(),
            max_bytes: BLOB_DATA_SIZE,
            crds_timeout: CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS,
        }
    }
}
impl CrdsGossipPull {
    /// generate a random request
    pub fn new_pull_request(
        &self,
        crds: &Crds,
        self_id: Pubkey,
        now: u64,
    ) -> Result<(Pubkey, Bloom<Hash>, CrdsValue), CrdsGossipError> {
        let options: Vec<_> = crds
            .table
            .values()
            .filter_map(|v| v.value.contact_info())
            .filter(|v| {
                v.id != self_id && !v.gossip.ip().is_unspecified() && !v.gossip.ip().is_multicast()
            })
            .map(|item| {
                let req_time: u64 = *self.pull_request_time.get(&item.id).unwrap_or(&0);
                let weight = cmp::max(
                    1,
                    cmp::min(u64::from(u16::max_value()) - 1, (now - req_time) / 1024) as u32,
                );
                (weight, item)
            })
            .collect();
        if options.is_empty() {
            return Err(CrdsGossipError::NoPeers);
        }
        let filter = self.build_crds_filter(crds);
        let index = WeightedIndex::new(options.iter().map(|weighted| weighted.0)).unwrap();
        let random = index.sample(&mut rand::thread_rng());
        let self_info = crds
            .lookup(&CrdsValueLabel::ContactInfo(self_id))
            .unwrap_or_else(|| panic!("self_id invalid {}", self_id));
        Ok((options[random].1.id, filter, self_info.clone()))
    }

    /// time when a request to `from` was initiated
    /// This is used for weighted random selection during `new_pull_request`
    /// It's important to use the local nodes request creation time as the weight
    /// instead of the response received time otherwise failed nodes will increase their weight.
    pub fn mark_pull_request_creation_time(&mut self, from: Pubkey, now: u64) {
        self.pull_request_time.insert(from, now);
    }

    /// Store an old hash in the purged values set
    pub fn record_old_hash(&mut self, hash: Hash, timestamp: u64) {
        self.purged_values.push_back((hash, timestamp))
    }

    /// process a pull request and create a response
    pub fn process_pull_request(
        &mut self,
        crds: &mut Crds,
        caller: CrdsValue,
        mut filter: Bloom<Hash>,
        now: u64,
    ) -> Vec<CrdsValue> {
        println!("PROCESSING PULL REQUEST");
        let rv = self.filter_crds_values(crds, &mut filter);
        let key = caller.label().pubkey();
        let old = crds.insert(caller, now);
        if let Some(val) = old.ok().and_then(|opt| opt) {
            self.purged_values
                .push_back((val.value_hash, val.local_timestamp))
        }
        crds.update_record_timestamp(key, now);
        rv
    }
    /// process a pull response
    pub fn process_pull_response(
        &mut self,
        crds: &mut Crds,
        from: Pubkey,
        response: Vec<CrdsValue>,
        now: u64,
    ) -> usize {
        println!("PROCESSING PULL RESPONSE");
        let mut failed = 0;
        for r in response {
            let owner = r.label().pubkey();
            let old = crds.insert(r, now);
            failed += old.is_err() as usize;
            old.ok().map(|opt| {
                crds.update_record_timestamp(owner, now);
                opt.map(|val| {
                    self.purged_values
                        .push_back((val.value_hash, val.local_timestamp))
                })
            });
        }
        crds.update_record_timestamp(from, now);
        failed
    }
    /// build a filter of the current crds table
    fn build_crds_filter(&self, crds: &Crds) -> Bloom<Hash> {
        let num = cmp::max(
            CRDS_GOSSIP_BLOOM_SIZE,
            crds.table.values().count() + self.purged_values.len(),
        );
        let mut bloom = Bloom::random(num, 0.1, 4 * 1024 * 8 - 1);
        for v in crds.table.values() {
            bloom.add(&v.value_hash);
        }
        for (value_hash, _insert_timestamp) in &self.purged_values {
            bloom.add(value_hash);
        }
        bloom
    }
    /// filter values that fail the bloom filter up to max_bytes
    fn filter_crds_values(&self, crds: &Crds, filter: &mut Bloom<Hash>) -> Vec<CrdsValue> {
        let mut max_bytes = self.max_bytes as isize;
        let mut ret = vec![];
        for v in crds.table.values() {
            if filter.contains(&v.value_hash) {
                continue;
            }
            max_bytes -= serialized_size(&v.value).unwrap() as isize;
            if max_bytes < 0 {
                break;
            }
            ret.push(v.value.clone());
        }
        ret
    }
    /// Purge values from the crds that are older then `active_timeout`
    /// The value_hash of an active item is put into self.purged_values queue
    pub fn purge_active(&mut self, crds: &mut Crds, self_id: Pubkey, min_ts: u64) {
        let old = crds.find_old_labels(min_ts);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("create timestamp in timing");

        let mut purged: VecDeque<_> = old
            .iter()
            .filter(|label| label.pubkey() != self_id)
            .filter_map(|label| {
                println!("{:?}, Removing label: {:?}", now, label);
                let rv = crds.lookup_versioned(label).map(|val| {
                    println!("{:?}, Removing label: {:?}, value: {:?}", now, label, val);
                    (val.value_hash, val.local_timestamp)
                });
                crds.remove(label);
                rv
            })
            .collect();
        println!("{:?}, After purge remaining len: {}", now, crds.table.len());
        self.purged_values.append(&mut purged);
    }
    /// Purge values from the `self.purged_values` queue that are older then purge_timeout
    pub fn purge_purged(&mut self, min_ts: u64) {
        let cnt = self
            .purged_values
            .iter()
            .take_while(|v| v.1 < min_ts)
            .count();
        self.purged_values.drain(..cnt);
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::contact_info::ContactInfo;
    use crate::crds_value::LeaderId;
    use solana_sdk::signature::{Keypair, KeypairUtil};

    #[test]
    fn test_new_pull_request() {
        let mut crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        let id = entry.label().pubkey();
        let node = CrdsGossipPull::default();
        assert_eq!(
            node.new_pull_request(&crds, id, 0),
            Err(CrdsGossipError::NoPeers)
        );

        crds.insert(entry.clone(), 0).unwrap();
        assert_eq!(
            node.new_pull_request(&crds, id, 0),
            Err(CrdsGossipError::NoPeers)
        );

        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        crds.insert(new.clone(), 0).unwrap();
        let req = node.new_pull_request(&crds, id, 0);
        let (to, _, self_info) = req.unwrap();
        assert_eq!(to, new.label().pubkey());
        assert_eq!(self_info, entry);
    }

    #[test]
    fn test_new_mark_creation_time() {
        let mut crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        let node_id = entry.label().pubkey();
        let mut node = CrdsGossipPull::default();
        crds.insert(entry.clone(), 0).unwrap();
        let old = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        crds.insert(old.clone(), 0).unwrap();
        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        crds.insert(new.clone(), 0).unwrap();

        // set request creation time to max_value
        node.mark_pull_request_creation_time(new.label().pubkey(), u64::max_value());

        // odds of getting the other request should be 1 in u64::max_value()
        for _ in 0..10 {
            let req = node.new_pull_request(&crds, node_id, u64::max_value());
            let (to, _, self_info) = req.unwrap();
            assert_eq!(to, old.label().pubkey());
            assert_eq!(self_info, entry);
        }
    }

    #[test]
    fn test_process_pull_request() {
        let mut node_crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        let node_id = entry.label().pubkey();
        let node = CrdsGossipPull::default();
        node_crds.insert(entry.clone(), 0).unwrap();
        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        node_crds.insert(new.clone(), 0).unwrap();
        let req = node.new_pull_request(&node_crds, node_id, 0);

        let mut dest_crds = Crds::default();
        let mut dest = CrdsGossipPull::default();
        let (_, filter, caller) = req.unwrap();
        let rsp = dest.process_pull_request(&mut dest_crds, caller.clone(), filter, 1);
        assert!(rsp.is_empty());
        assert!(dest_crds.lookup(&caller.label()).is_some());
        assert_eq!(
            dest_crds
                .lookup_versioned(&caller.label())
                .unwrap()
                .insert_timestamp,
            1
        );
        assert_eq!(
            dest_crds
                .lookup_versioned(&caller.label())
                .unwrap()
                .local_timestamp,
            1
        );
    }
    #[test]
    fn test_process_pull_request_response() {
        let mut node_crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        let node_id = entry.label().pubkey();
        let mut node = CrdsGossipPull::default();
        node_crds.insert(entry.clone(), 0).unwrap();
        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        node_crds.insert(new.clone(), 0).unwrap();

        let mut dest = CrdsGossipPull::default();
        let mut dest_crds = Crds::default();
        let new = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        dest_crds.insert(new.clone(), 0).unwrap();

        // node contains a key from the dest node, but at an older local timestamp
        let dest_id = new.label().pubkey();
        let same_key = CrdsValue::LeaderId(LeaderId::new(dest_id, dest_id, 1));
        node_crds.insert(same_key.clone(), 0).unwrap();
        assert_eq!(
            node_crds
                .lookup_versioned(&same_key.label())
                .unwrap()
                .local_timestamp,
            0
        );
        let mut done = false;
        for _ in 0..30 {
            // there is a chance of a false positive with bloom filters
            let req = node.new_pull_request(&node_crds, node_id, 0);
            let (_, filter, caller) = req.unwrap();
            let rsp = dest.process_pull_request(&mut dest_crds, caller, filter, 0);
            // if there is a false positive this is empty
            // prob should be around 0.1 per iteration
            if rsp.is_empty() {
                continue;
            }

            assert_eq!(rsp.len(), 1);
            let failed = node.process_pull_response(&mut node_crds, node_id, rsp, 1);
            assert_eq!(failed, 0);
            assert_eq!(
                node_crds
                    .lookup_versioned(&new.label())
                    .unwrap()
                    .local_timestamp,
                1
            );
            // verify that the whole record was updated for dest since this is a response from dest
            assert_eq!(
                node_crds
                    .lookup_versioned(&same_key.label())
                    .unwrap()
                    .local_timestamp,
                1
            );
            done = true;
            break;
        }
        assert!(done);
    }
    #[test]
    fn test_gossip_purge() {
        let mut node_crds = Crds::default();
        let entry = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        let node_label = entry.label();
        let node_id = node_label.pubkey();
        let mut node = CrdsGossipPull::default();
        node_crds.insert(entry.clone(), 0).unwrap();
        let old = CrdsValue::ContactInfo(ContactInfo::new_localhost(Keypair::new().pubkey(), 0));
        node_crds.insert(old.clone(), 0).unwrap();
        let value_hash = node_crds.lookup_versioned(&old.label()).unwrap().value_hash;

        //verify self is valid
        assert_eq!(node_crds.lookup(&node_label).unwrap().label(), node_label);

        // purge
        node.purge_active(&mut node_crds, node_id, 1);

        //verify self is still valid after purge
        assert_eq!(node_crds.lookup(&node_label).unwrap().label(), node_label);

        assert_eq!(node_crds.lookup_versioned(&old.label()), None);
        assert_eq!(node.purged_values.len(), 1);
        for _ in 0..30 {
            // there is a chance of a false positive with bloom filters
            // assert that purged value is still in the set
            // chance of 30 consecutive false positives is 0.1^30
            let mut filter = node.build_crds_filter(&node_crds);
            assert!(filter.contains(&value_hash));
        }

        // purge the value
        node.purge_purged(1);
        assert_eq!(node.purged_values.len(), 0);
    }
}
