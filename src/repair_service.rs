//! The `repair_service` module implements the tools necessary to generate a thread which
//! regularly finds missing blobs in the ledger and sends repair requests for those blobs

use crate::cluster_info::ClusterInfo;
use crate::db_ledger::{DbLedger, SlotMeta};
use crate::result::Result;
use crate::service::Service;
use solana_metrics::{influxdb, submit};
use solana_sdk::pubkey::Pubkey;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;

pub const MAX_REPAIR_LENGTH: usize = 32;
pub const REPAIR_MS: u64 = 100;
pub const MAX_REPAIR_TRIES: u64 = 128;

struct RepairInfo {
    max_slot: u64,
    repair_tries: u64,
}

impl RepairInfo {
    fn new() -> Self {
        RepairInfo {
            max_slot: 0,
            repair_tries: 0,
        }
    }
}

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
        let mut repair_info = RepairInfo::new();
        let id = cluster_info.read().unwrap().id();
        let t_repair = Builder::new()
            .name("solana-repair-service".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }

                let repairs =
                    Self::generate_repairs(&id, &db_ledger, MAX_REPAIR_LENGTH, &mut repair_info);

                if let Ok(repairs) = repairs {
                    let reqs: Vec<_> = repairs
                        .into_iter()
                        .filter_map(|(slot_height, blob_index)| {
                            cluster_info
                                .read()
                                .unwrap()
                                .window_index_request(slot_height, blob_index)
                                .map(|result| (result, slot_height, blob_index))
                                .ok()
                        })
                        .collect();

                    for ((to, req), slot_height, blob_index) in reqs {
                        if let Ok(local_addr) = repair_socket.local_addr() {
                            submit(
                                influxdb::Point::new("repair_service")
                                    .add_field(
                                        "repair_slot",
                                        influxdb::Value::Integer(slot_height as i64),
                                    )
                                    .to_owned()
                                    .add_field(
                                        "repair_blob",
                                        influxdb::Value::Integer(blob_index as i64),
                                    )
                                    .to_owned()
                                    .add_field("to", influxdb::Value::String(to.to_string()))
                                    .to_owned()
                                    .add_field(
                                        "from",
                                        influxdb::Value::String(local_addr.to_string()),
                                    )
                                    .to_owned()
                                    .add_field("id", influxdb::Value::String(id.to_string()))
                                    .to_owned(),
                            );
                        }

                        repair_socket.send_to(&req, to).unwrap_or_else(|e| {
                            info!("{} repair req send_to({}) error {:?}", id, to, e);
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
        repair_info: &mut RepairInfo,
    ) -> Result<Vec<(u64, u64)>> {
        // Slot height and blob indexes for blobs we want to repair
        let mut repairs: Vec<(u64, u64)> = vec![];
        let mut current_slot_height = Some(0);
        while repairs.len() < max_repairs && current_slot_height.is_some() {
            if current_slot_height.unwrap() > repair_info.max_slot {
                repair_info.repair_tries = 0;
                repair_info.max_slot = current_slot_height.unwrap();
            }

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
        // Only increment repair_tries if the ledger contains every blob for every slot
        if repairs.is_empty() {
            repair_info.repair_tries += 1;
        }

        // Optimistically try the next slot if we haven't gotten any repairs
        // for a while
        if repair_info.repair_tries >= MAX_REPAIR_TRIES {
            repairs.push((repair_info.max_slot + 1, 0))
        }

        Ok(repairs)
    }
}

impl Service for RepairService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.t_repair.join()
    }
}
