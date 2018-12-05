use db_ledger::{DataCf, DbLedger, LedgerColumnFamilyRaw, SlotMeta};
use db_window::find_missing_data_indexes;
use std::collections::HashSet;

pub const MAX_REPAIR_LENGTH: usize = 128;

#[derive(Default)]
pub struct RepairCache {
    last_missing_index: u64,
    last_missing_slot: u64,
    pub missing_indexes: HashSet<u64>,
}

impl RepairCache {
    pub fn new(ledger_meta: &SlotMeta) -> Self {
        let mut repair_cache = Self::default();
        repair_cache.last_missing_index = ledger_meta.consumed;
        repair_cache.last_missing_slot = ledger_meta.consumed_slot;
        repair_cache.missing_indexes.insert(ledger_meta.consumed);
        repair_cache
    }

    pub fn update_cache(
        &mut self,
        db_ledger: &DbLedger,
        ledger_meta: &SlotMeta,
        max_repair_entry_height: u64,
    ) {
        println!("IN UPDATE CACHE, ledger_meta: {:?}", ledger_meta);
        if ledger_meta.consumed > self.last_missing_index {
            println!("CLEARING CACHE");
            // Clear everything because we have received everything that was in the missing
            // cache, update the last_missing_index and last_missing_slot to the latest
            // updated versions based on consumed, because consumed might have jumped far ahead
            // once the missing holes were patched
            self.missing_indexes.clear();
            self.last_missing_index = ledger_meta.consumed;
            self.last_missing_slot = ledger_meta.consumed_slot;
            self.missing_indexes.insert(ledger_meta.consumed);
        }

        assert!(self.missing_indexes.len() <= MAX_REPAIR_LENGTH);

        if self.missing_indexes.len() == MAX_REPAIR_LENGTH {
            return;
        }

        let mut current_slot = self.last_missing_slot;
        while self.missing_indexes.len() != MAX_REPAIR_LENGTH
            && current_slot <= ledger_meta.received_slot
        {
            let result = find_missing_data_indexes(
                current_slot,
                db_ledger,
                self.last_missing_index + 1,
                max_repair_entry_height,
                MAX_REPAIR_LENGTH - self.missing_indexes.len(),
            );

            println!("find_missing_data_indexes() result: {:?}", result);
            if !result.is_empty() {
                self.last_missing_slot = current_slot;
                self.last_missing_index = *result.last().unwrap();
                for idx in result {
                    self.missing_indexes.insert(idx);
                }
            }

            // Check if this slot contains the upper bound, max_repair_entry_height - 1 (recall
            // find_missing_data_indexes finds everything missing in the in the range [self.last_missing_index + 1, max_repair_entry_height)
            // where the ending bound is exclusive). If so, then this is the last slot we should check
            if Self::is_index_within_slot_limit(
                db_ledger,
                current_slot,
                max_repair_entry_height - 1,
            ) {
                break;
            }

            current_slot += 1;
        }
    }

    pub fn record_received(&mut self, received_indexes: &[u64]) {
        for index in received_indexes {
            self.missing_indexes.remove(index);
        }
    }

    // Check if a slot contains any blobs with index >= index
    fn is_index_within_slot_limit(db_ledger: &DbLedger, slot_height: u64, index: u64) -> bool {
        let mut db_iterator = db_ledger
            .db
            .raw_iterator_cf(db_ledger.data_cf.handle(&db_ledger.db))
            .expect("Expected to be able to open database iterator");
        db_iterator.seek(&DataCf::key(slot_height, index));
        if db_iterator.valid() {
            let key = &db_iterator.key().expect("Expected valid key");
            let slot =
                DataCf::slot_height_from_key(&key).expect("Expect valid slot from blob in ledger");
            return slot == slot_height;
        }

        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use db_ledger::{LedgerColumnFamily, MetaCf, DEFAULT_SLOT_HEIGHT};
    use ledger::{get_tmp_ledger_path, make_tiny_test_entries, Block};
    use packet::Blob;

    #[test]
    fn test_is_index_within_slot_limit() {
        let shared_blobs = make_tiny_test_entries(20).to_blobs();
        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();
        let slot = DEFAULT_SLOT_HEIGHT;

        let ledger_path = get_tmp_ledger_path("test_is_index_within_slot_limit");
        let mut ledger = DbLedger::open(&ledger_path).unwrap();

        ledger.write_blobs(slot, &blobs[0..5]).unwrap();
        ledger.write_blobs(slot + 1, &blobs[5..10]).unwrap();

        for i in 0..5 {
            assert!(RepairCache::is_index_within_slot_limit(
                &ledger, slot, i as u64
            ));
        }

        for i in 5..10 {
            assert!(RepairCache::is_index_within_slot_limit(
                &ledger,
                slot + 1,
                i as u64
            ));
            assert!(!RepairCache::is_index_within_slot_limit(
                &ledger, slot, i as u64
            ));
        }

        for (i, b) in blobs[10..20].iter().enumerate() {
            ledger.write_blobs(slot + 2 + i as u64, vec![b]).unwrap();
        }

        for i in 0..10 as u64 {
            assert!(RepairCache::is_index_within_slot_limit(
                &ledger,
                slot + 2 + i,
                i + 10
            ));
            assert!(!RepairCache::is_index_within_slot_limit(
                &ledger,
                slot + 2 + i,
                i + 1 + 10
            ));
        }

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        DbLedger::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_update_cache() {
        let interval: usize = 4;
        let num_intervals: usize = 5;
        assert!(interval > 1);
        assert!(num_intervals > 1);

        let total_blobs = interval * num_intervals;
        let shared_blobs = make_tiny_test_entries(total_blobs).to_blobs();
        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();
        let slot = DEFAULT_SLOT_HEIGHT;

        let ledger_path = get_tmp_ledger_path("test_update_cache");
        let mut ledger = DbLedger::open(&ledger_path).unwrap();

        // Skip inserting every index where index % interval == interval - 1.
        for i in 0..num_intervals {
            ledger
                .write_blobs(
                    slot + i as u64,
                    &blobs[i * interval..(i + 1) * interval - 1],
                ).unwrap();
        }

        let meta = ledger
            .meta_cf
            .get(&ledger.db, &MetaCf::key(DEFAULT_SLOT_HEIGHT))
            .expect("expect to be able to access ledger")
            .expect("expect valid metadata to exist");

        let mut repair_cache = RepairCache::new(&meta);

        // Only update up to the index: (num_intervals - 1) * update
        repair_cache.update_cache(&ledger, &meta, ((num_intervals - 1) * interval) as u64);
        let expected: HashSet<u64> = (1..num_intervals)
            .map(|i| (i * interval - 1) as u64)
            .collect();
        assert_eq!(repair_cache.missing_indexes, expected);
        assert_eq!(
            repair_cache.last_missing_index,
            ((num_intervals - 1) * interval - 1) as u64
        );
        assert_eq!(repair_cache.last_missing_slot, (num_intervals - 1) as u64);

        // Simulate receiving blobs, should clear the cache
        let received_vec: Vec<u64> = expected.into_iter().collect();
        repair_cache.record_received(&received_vec);
        assert!(repair_cache.missing_indexes.is_empty());

        // Now update up to the index: (num_intervals) * update
        repair_cache.update_cache(&ledger, &meta, (num_intervals * interval) as u64);
        let mut expected: HashSet<u64> = HashSet::new();
        expected.insert((num_intervals * interval - 1) as u64);
        assert_eq!(repair_cache.missing_indexes, expected);
        assert_eq!(
            repair_cache.last_missing_index,
            ((num_intervals) * interval - 1) as u64
        );
        assert_eq!(repair_cache.last_missing_slot, num_intervals as u64);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        DbLedger::destroy(&ledger_path).expect("Expected successful database destruction");
    }
}
