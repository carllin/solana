use db_ledger::{DataCf, DbLedger, LedgerColumnFamilyRaw, SlotMeta};
use db_window::find_missing_data_indexes;
use std::collections::HashSet;

pub const MAX_REPAIR_LENGTH: usize = 128;

#[derive(Default)]
pub struct RepairCache {
    last_index: u64,
    last_slot: u64,
    pub missing_indexes: HashSet<u64>,
}

impl RepairCache {
    pub fn new(ledger_meta: &SlotMeta) -> Self {
        let mut repair_cache = Self::default();
        repair_cache.last_index = ledger_meta.consumed;
        repair_cache.last_slot = ledger_meta.consumed_slot;
        repair_cache
    }

    pub fn update_cache(
        &mut self,
        db_ledger: &DbLedger,
        ledger_meta: &SlotMeta,
        max_repair_entry_height: u64,
    ) {
        println!("IN UPDATE CACHE, ledger_meta: {:?}", ledger_meta);
        if ledger_meta.consumed > self.last_index {
            println!("CLEARING CACHE");
            // Clear everything because we have received everything that was in the missing
            // cache, update the last_index and last_slot to the latest
            // updated versions based on consumed, because consumed might have jumped far ahead
            // once the missing holes were patched
            self.missing_indexes.clear();
            self.last_index = ledger_meta.consumed;
            self.last_slot = ledger_meta.consumed_slot;
        }

        assert!(self.missing_indexes.len() <= MAX_REPAIR_LENGTH);

        if self.missing_indexes.len() == MAX_REPAIR_LENGTH {
            return;
        }

        let (result, last_slot, last_index) = find_missing_data_indexes(
            self.last_slot,
            db_ledger,
            self.last_index,
            max_repair_entry_height,
            MAX_REPAIR_LENGTH - self.missing_indexes.len(),
        );

        println!("find_missing_data_indexes() result: {:?}", result);
        if !result.is_empty() {
            self.last_slot = last_slot;
            self.last_index = last_index;
            for idx in result {
                self.missing_indexes.insert(idx);
            }
        }
    }

    pub fn record_received(&mut self, received_indexes: &[u64]) {
        for index in received_indexes {
            self.missing_indexes.remove(index);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use db_ledger::{LedgerColumnFamily, MetaCf, DEFAULT_SLOT_HEIGHT};
    use ledger::{get_tmp_ledger_path, make_tiny_test_entries, Block};
    use packet::Blob;

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
            repair_cache.last_index,
            ((num_intervals - 1) * interval) as u64
        );
        assert_eq!(repair_cache.last_slot, (num_intervals - 2) as u64);

        // Simulate receiving blobs, should clear the cache
        let received_vec: Vec<u64> = expected.into_iter().collect();
        repair_cache.record_received(&received_vec);
        assert!(repair_cache.missing_indexes.is_empty());

        // Now update up to the index: (num_intervals) * update
        repair_cache.update_cache(&ledger, &meta, (num_intervals * interval) as u64);
        let mut expected: HashSet<u64> = HashSet::new();
        expected.insert((num_intervals * interval - 1) as u64);
        assert_eq!(repair_cache.missing_indexes, expected);
        assert_eq!(repair_cache.last_index, ((num_intervals) * interval) as u64);
        assert_eq!(repair_cache.last_slot, (num_intervals - 1) as u64);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        DbLedger::destroy(&ledger_path).expect("Expected successful database destruction");
    }
}
