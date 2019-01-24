//! The `repair_service` module implements the tools necessary to generate a thread which
//! regularly finds missing blobs in the ledger and sends repair requests for those blobs

use crate::cluster_info::ClusterInfo;
use crate::db_ledger::{DbLedger, SlotMeta};
use crate::result::Result;
use crate::service::Service;
use solana_sdk::pubkey::Pubkey;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;

pub const MAX_REPAIR_LENGTH: usize = 16;
pub const REPAIR_MS: u64 = 100;

pub struct RepairService {
    t_repair: JoinHandle<()>,
}

impl RepairService {
    pub fn new(
        db_ledger: Arc<DbLedger>,
        exit: Arc<AtomicBool>,
        repair_socket: Arc<UdpSocket>,
        cluster_info: Arc<RwLock<ClusterInfo>>,
    ) -> Self {
        let id = cluster_info.read().unwrap().id();
        let t_repair = Builder::new()
            .name("solana-repair-service".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }

                let rcluster_info = cluster_info.read().unwrap();
                let repairs = Self::generate_repairs(&id, &db_ledger, MAX_REPAIR_LENGTH);

                if let Ok(repairs) = repairs {
                    let reqs: Vec<_> = repairs
                        .into_iter()
                        .filter_map(|(slot_height, blob_index)| {
                            rcluster_info
                                .window_index_request(slot_height, blob_index)
                                .ok()
                        })
                        .collect();

                    for (to, req) in reqs {
                        repair_socket.send_to(&req, to).unwrap_or_else(|e| {
                            println!("{} repair req send_to({}) error {:?}", id, to, e);
                            0
                        });
                    }
                }
                sleep(Duration::from_millis(REPAIR_MS));
            })
            .unwrap();

        RepairService { t_repair }
    }

    fn process_slot(
        id: &Pubkey,
        db_ledger: &DbLedger,
        slot_height: u64,
        slot: &SlotMeta,
        max_repairs: usize,
    ) -> Result<Vec<(u64, u64)>> {
        println!(
            "{} Repair, slot_height: {}, consumed: {}, received: {}, consumed_tick_height: {}",
            id, slot_height, slot.consumed, slot.received, slot.consumed_ticks,
        );
        if slot.contains_all_ticks(slot_height, db_ledger) {
            println!("contains all ticks");
            Ok(vec![])
        } else {
            let num_unreceived_ticks = {
                if slot.consumed == slot.received {
                    let num_expected_ticks = slot.num_expected_ticks(slot_height, db_ledger);
                    if num_expected_ticks == 0 {
                        // This signals that we have received nothing for this slot, try to get at least the
                        // first entry
                        1
                    }
                    // This signals that we will never use other slots (leader rotation is
                    // off)
                    else if num_expected_ticks == std::u64::MAX
                        || num_expected_ticks <= slot.consumed_ticks
                    {
                        0
                    } else {
                        num_expected_ticks - slot.consumed_ticks
                    }
                } else {
                    0
                }
            };

            let upper = slot.received + num_unreceived_ticks;

            let reqs =
                db_ledger.find_missing_data_indexes(slot_height, slot.consumed, upper, max_repairs);

            Ok(reqs.into_iter().map(|i| (slot_height, i)).collect())
        }
    }

    fn generate_repairs(
        id: &Pubkey,
        db_ledger: &DbLedger,
        max_repairs: usize,
    ) -> Result<Vec<(u64, u64)>> {
        // Slot height and blob indexes for blobs we want to repair
        let mut repairs: Vec<(u64, u64)> = vec![];
        let mut current_slot_height = Some(0);
        while repairs.len() < max_repairs && current_slot_height.is_some() {
            let slot = db_ledger.meta(current_slot_height.unwrap())?;
            if slot.is_none() {
                current_slot_height = db_ledger.get_next_slot(current_slot_height.unwrap())?;
                continue;
            }
            let slot = slot.unwrap();
            let new_repairs = Self::process_slot(
                id,
                db_ledger,
                current_slot_height.unwrap(),
                &slot,
                max_repairs - repairs.len(),
            )?;
            repairs.extend(new_repairs);
            current_slot_height = db_ledger.get_next_slot(current_slot_height.unwrap())?;
        }

        println!("{}: repairs: {:?}", id, repairs);
        Ok(repairs)
    }
}

impl Service for RepairService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.t_repair.join()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db_ledger::{get_tmp_ledger_path, DbLedger, DbLedgerConfig};
    use crate::entry::{make_tiny_test_entries, EntrySlice};
    /*
    #[test]
    pub fn test_generate_repairs() {
        let db_ledger_path = get_tmp_ledger_path("test_generate_repairs");
        {
            let num_ticks_per_block = 10;
            let db_ledger_config = DbLedgerConfig::new(num_ticks_per_block, num_ticks_per_block, 1);
            let db_ledger = DbLedger::open_config(&db_ledger_path, &db_ledger_config).unwrap();

            let num_entries_per_slot = 10;
            let num_slots = 2;
            let mut blobs = make_tiny_test_entries(num_slots * num_entries_per_slot).to_blobs();

            // Insert every nth entry for each slot
            let nth = 3;
            for (i, b) in blobs.iter_mut().enumerate() {
                b.set_index(((i % num_entries_per_slot) * nth) as u64)
                    .unwrap();
                b.set_slot((i / num_entries_per_slot) as u64).unwrap();
            }

            db_ledger.write_blobs(&blobs).unwrap();

            let missing_indexes_per_slot: Vec<u64> = (0..num_entries_per_slot - 1)
                .flat_map(|x| ((nth * x + 1) as u64..(nth * x + nth) as u64))
                .collect();

            let expected: Vec<(u64, u64)> = (0..num_slots)
                .flat_map(|slot_height| {
                    missing_indexes_per_slot
                        .iter()
                        .map(move |blob_index| (slot_height as u64, *blob_index))
                })
                .collect();

            // Across all slots, find all missing indexes in the range [0, num_entries_per_slot * nth]
            assert_eq!(
                RepairService::generate_repairs(&db_ledger, std::usize::MAX).unwrap(),
                expected
            );

            assert_eq!(
                RepairService::generate_repairs(&db_ledger, expected.len() - 2).unwrap()[..],
                expected[0..expected.len() - 2]
            );

            // Now fill in all the holes for each slot such that for each slot, consumed == received.
            // Because none of the slots contain ticks, we should see that the repair requests
            // ask for ticks, starting from the last received index for that slot
            for (slot_height, blob_index) in expected {
                let mut b = make_tiny_test_entries(1).to_blobs().pop().unwrap();
                b.set_index(blob_index).unwrap();
                b.set_slot(slot_height).unwrap();
                db_ledger.write_blobs(&vec![b]).unwrap();
            }

            let last_index_per_slot = ((num_entries_per_slot - 1) * nth) as u64;
            let missing_indexes_per_slot: Vec<u64> =
                (last_index_per_slot + 1..last_index_per_slot + 1 + num_ticks_per_block).collect();
            let expected: Vec<(u64, u64)> = (0..num_slots)
                .flat_map(|slot_height| {
                    missing_indexes_per_slot
                        .iter()
                        .map(move |blob_index| (slot_height as u64, *blob_index))
                })
                .collect();
            assert_eq!(
                RepairService::generate_repairs(&db_ledger, std::usize::MAX).unwrap(),
                expected
            );
            assert_eq!(
                RepairService::generate_repairs(&db_ledger, expected.len() - 2).unwrap()[..],
                expected[0..expected.len() - 2]
            );
        }

        let last_index_per_slot = ((num_entries_per_slot - 1) * nth) as u64;
        let missing_indexes_per_slot: Vec<u64> =
            (last_index_per_slot + 1..last_index_per_slot + 1 + num_ticks_per_block).collect();
        let expected: Vec<(u64, u64)> = (0..num_slots)
            .flat_map(|slot_height| {
                missing_indexes_per_slot
                    .iter()
                    .map(move |blob_index| (slot_height as u64, *blob_index))
            })
            .collect();
        assert_eq!(
            RepairService::generate_repairs(&db_ledger, std::usize::MAX).unwrap(),
            expected
        );
        assert_eq!(
            RepairService::generate_repairs(&db_ledger, expected.len() - 2).unwrap()[..],
            expected[0..expected.len() - 2]
        );
    }*/
}
