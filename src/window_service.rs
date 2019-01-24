//! The `window_service` provides a thread for maintaining a window (tail of the ledger).
//!
use crate::cluster_info::ClusterInfo;
use crate::counter::Counter;
use crate::db_ledger::DbLedger;
use crate::db_window::*;
use crate::leader_scheduler::LeaderScheduler;
use crate::repair_service::RepairService;
use crate::result::{Error, Result};
use crate::service::Service;
use crate::streamer::{BlobReceiver, BlobSender};
use log::Level;
use solana_metrics::{influxdb, submit};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::timing::duration_as_ms;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::{Duration, Instant};

pub const MAX_REPAIR_BACKOFF: usize = 128;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WindowServiceReturnType {
    LeaderRotation(u64),
}

#[allow(clippy::too_many_arguments)]
fn recv_window(
    db_ledger: &Arc<DbLedger>,
    id: &Pubkey,
    leader_scheduler: &Arc<RwLock<LeaderScheduler>>,
    tick_height: &mut u64,
    max_ix: u64,
    r: &BlobReceiver,
    retransmit: &BlobSender,
    done: &Arc<AtomicBool>,
) -> Result<()> {
    let timer = Duration::from_millis(200);
    let mut dq = r.recv_timeout(timer)?;

    while let Ok(mut nq) = r.try_recv() {
        dq.append(&mut nq)
    }
    let now = Instant::now();
    inc_new_counter_info!("streamer-recv_window-recv", dq.len(), 100);

    submit(
        influxdb::Point::new("recv-window")
            .add_field("count", influxdb::Value::Integer(dq.len() as i64))
            .to_owned(),
    );

    retransmit_all_leader_blocks(&dq, leader_scheduler, retransmit)?;

    //send a contiguous set of blocks
    let mut consume_queue = Vec::new();

    trace!("{} num blobs received: {}", id, dq.len());

    for b in dq {
        let (slot, pix, meta_size) = {
            let p = b.read().unwrap();
            (p.slot()?, p.index()?, p.meta.size)
        };

        submit(
            influxdb::Point::new("recv-window-blob")
                .add_field("slot", influxdb::Value::Integer(slot as i64))
                .to_owned()
                .add_field("index", influxdb::Value::Integer(pix as i64))
                .to_owned()
                .add_field("id", influxdb::Value::String(id.to_string()))
                .to_owned(),
        );

        trace!("{} window pix: {} size: {}", id, pix, meta_size);

        let _ = process_blob(
            leader_scheduler,
            db_ledger,
            &b,
            max_ix,
            &mut consume_queue,
            tick_height,
            done,
        );
    }

    trace!(
        "Elapsed processing time in recv_window(): {}",
        duration_as_ms(&now.elapsed())
    );

    Ok(())
}

// Implement a destructor for the window_service thread to signal it exited
// even on panics
struct Finalizer {
    exit_sender: Arc<AtomicBool>,
}

impl Finalizer {
    fn new(exit_sender: Arc<AtomicBool>) -> Self {
        Finalizer { exit_sender }
    }
}
// Implement a destructor for Finalizer.
impl Drop for Finalizer {
    fn drop(&mut self) {
        self.exit_sender.clone().store(true, Ordering::Relaxed);
    }
}

pub struct WindowService {
    t_window: JoinHandle<()>,
    repair_service: RepairService,
}

impl WindowService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db_ledger: Arc<DbLedger>,
        cluster_info: Arc<RwLock<ClusterInfo>>,
        tick_height: u64,
        max_entry_height: u64,
        r: BlobReceiver,
        retransmit: BlobSender,
        repair_socket: Arc<UdpSocket>,
        leader_scheduler: Arc<RwLock<LeaderScheduler>>,
        done: Arc<AtomicBool>,
    ) -> WindowService {
        let exit = Arc::new(AtomicBool::new(false));
        let exit_ = exit.clone();
        let repair_service =
            RepairService::new(db_ledger.clone(), exit, repair_socket, cluster_info.clone());
        let t_window = Builder::new()
            .name("solana-window".to_string())
            .spawn(move || {
                let _exit = Finalizer::new(exit_);
                let mut tick_height_ = tick_height;
                let id = cluster_info.read().unwrap().id();
                trace!("{}: RECV_WINDOW started", id);
                loop {
                    if let Err(e) = recv_window(
                        &db_ledger,
                        &id,
                        &leader_scheduler,
                        &mut tick_height_,
                        max_entry_height,
                        &r,
                        &retransmit,
                        &done,
                    ) {
                        match e {
                            Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                            Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                            _ => {
                                inc_new_counter_info!("streamer-window-error", 1, 1);
                                error!("window error: {:?}", e);
                            }
                        }
                    }
                }
            })
            .unwrap();

        WindowService {
            t_window,
            repair_service,
        }
    }
}

impl Service for WindowService {
    type JoinReturnType = ();

    fn join(self) -> thread::Result<()> {
        self.t_window.join()?;
        self.repair_service.join()
    }
}

#[cfg(test)]
mod test {
    use crate::cluster_info::{ClusterInfo, Node};
    use crate::db_ledger::get_tmp_ledger_path;
    use crate::db_ledger::DbLedger;
    use crate::entry::make_consecutive_blobs;
    use crate::leader_scheduler::LeaderScheduler;
    use crate::service::Service;

    use crate::packet::{SharedBlob, PACKET_DATA_SIZE};
    use crate::streamer::{blob_receiver, responder};
    use crate::window_service::WindowService;
    use solana_sdk::hash::Hash;
    use std::fs::remove_dir_all;
    use std::net::UdpSocket;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    #[test]
    pub fn window_send_test() {
        solana_logger::setup();
        let tn = Node::new_localhost();
        let exit = Arc::new(AtomicBool::new(false));
        let mut cluster_info_me = ClusterInfo::new(tn.info.clone());
        let me_id = cluster_info_me.my_data().id;
        cluster_info_me.set_leader(me_id);
        let subs = Arc::new(RwLock::new(cluster_info_me));

        let (s_reader, r_reader) = channel();
        let t_receiver = blob_receiver(Arc::new(tn.sockets.gossip), exit.clone(), s_reader);
        let (s_retransmit, r_retransmit) = channel();
        let done = Arc::new(AtomicBool::new(false));
        let db_ledger_path = get_tmp_ledger_path("window_send_test");
        let db_ledger = Arc::new(
            DbLedger::open(&db_ledger_path).expect("Expected to be able to open database ledger"),
        );
        let window_service = WindowService::new(
            db_ledger,
            subs,
            0,
            0,
            r_reader,
            s_retransmit,
            Arc::new(tn.sockets.repair),
            Arc::new(RwLock::new(LeaderScheduler::from_bootstrap_leader(me_id))),
            done,
        );
        let t_responder = {
            let (s_responder, r_responder) = channel();
            let blob_sockets: Vec<Arc<UdpSocket>> =
                tn.sockets.tvu.into_iter().map(Arc::new).collect();

            let t_responder = responder("window_send_test", blob_sockets[0].clone(), r_responder);
            let num_blobs_to_make = 10;
            let gossip_address = &tn.info.gossip;
            let msgs = make_consecutive_blobs(
                &me_id,
                num_blobs_to_make,
                0,
                Hash::default(),
                &gossip_address,
            )
            .into_iter()
            .rev()
            .collect();;
            s_responder.send(msgs).expect("send");
            t_responder
        };

        let mut q = r_retransmit.recv().unwrap();
        while let Ok(mut nq) = r_retransmit.try_recv() {
            q.append(&mut nq);
        }
        assert_eq!(q.len(), 10);
        exit.store(true, Ordering::Relaxed);
        t_receiver.join().expect("join");
        t_responder.join().expect("join");
        window_service.join().expect("join");
        DbLedger::destroy(&db_ledger_path).expect("Expected successful database destruction");
        let _ignored = remove_dir_all(&db_ledger_path);
    }

    #[test]
    pub fn window_send_leader_test2() {
        solana_logger::setup();
        let tn = Node::new_localhost();
        let exit = Arc::new(AtomicBool::new(false));
        let cluster_info_me = ClusterInfo::new(tn.info.clone());
        let me_id = cluster_info_me.my_data().id;
        let subs = Arc::new(RwLock::new(cluster_info_me));

        let (s_reader, r_reader) = channel();
        let t_receiver = blob_receiver(Arc::new(tn.sockets.gossip), exit.clone(), s_reader);
        let (s_retransmit, r_retransmit) = channel();
        let done = Arc::new(AtomicBool::new(false));
        let db_ledger_path = get_tmp_ledger_path("window_send_late_leader_test");
        let db_ledger = Arc::new(
            DbLedger::open(&db_ledger_path).expect("Expected to be able to open database ledger"),
        );
        let window_service = WindowService::new(
            db_ledger,
            subs.clone(),
            0,
            0,
            r_reader,
            s_retransmit,
            Arc::new(tn.sockets.repair),
            Arc::new(RwLock::new(LeaderScheduler::from_bootstrap_leader(me_id))),
            done,
        );
        let t_responder = {
            let (s_responder, r_responder) = channel();
            let blob_sockets: Vec<Arc<UdpSocket>> =
                tn.sockets.tvu.into_iter().map(Arc::new).collect();
            let t_responder = responder("window_send_test", blob_sockets[0].clone(), r_responder);
            let mut msgs = Vec::new();
            for v in 0..10 {
                let i = 9 - v;
                let b = SharedBlob::default();
                {
                    let mut w = b.write().unwrap();
                    w.set_index(i).unwrap();
                    w.set_id(&me_id).unwrap();
                    assert_eq!(i, w.index().unwrap());
                    w.meta.size = PACKET_DATA_SIZE;
                    w.meta.set_addr(&tn.info.gossip);
                }
                msgs.push(b);
            }
            s_responder.send(msgs).expect("send");

            subs.write().unwrap().set_leader(me_id);
            let mut msgs1 = Vec::new();
            for v in 1..5 {
                let i = 9 + v;
                let b = SharedBlob::default();
                {
                    let mut w = b.write().unwrap();
                    w.set_index(i).unwrap();
                    w.set_id(&me_id).unwrap();
                    assert_eq!(i, w.index().unwrap());
                    w.meta.size = PACKET_DATA_SIZE;
                    w.meta.set_addr(&tn.info.gossip);
                }
                msgs1.push(b);
            }
            s_responder.send(msgs1).expect("send");
            t_responder
        };
        let mut q = r_retransmit.recv().unwrap();
        while let Ok(mut nq) = r_retransmit.recv_timeout(Duration::from_millis(100)) {
            q.append(&mut nq);
        }
        assert!(q.len() > 10);
        exit.store(true, Ordering::Relaxed);
        t_receiver.join().expect("join");
        t_responder.join().expect("join");
        window_service.join().expect("join");
        DbLedger::destroy(&db_ledger_path).expect("Expected successful database destruction");
        let _ignored = remove_dir_all(&db_ledger_path);
    }
}
