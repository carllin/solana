//! The `broadcast_stage` broadcasts data from a leader node to validators
//!
use counter::Counter;
use crdt::{Crdt, CrdtError, NodeInfo, LEADER_ROTATION_INTERVAL};
#[cfg(feature = "erasure")]
use erasure;
use log::Level;
use packet::BlobRecycler;
use result::{Error, Result};
use service::Service;
use std::net::UdpSocket;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{RecvTimeoutError, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use streamer::BlobReceiver;
use window::{self, SharedWindow, WindowIndex, WindowUtil, WINDOW_SIZE};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BroadcastStageReturnType {
    LeaderRotation,
    ChannelDisconnected,
}

fn broadcast(
    node_info: &NodeInfo,
    broadcast_table: &[NodeInfo],
    window: &SharedWindow,
    recycler: &BlobRecycler,
    receiver: &BlobReceiver,
    sock: &UdpSocket,
    transmit_index: &mut WindowIndex,
    receive_index: &mut u64,
) -> Result<()> {
    let id = node_info.id;
    let timer = Duration::new(1, 0);
    let mut dq = receiver.recv_timeout(timer)?;
    while let Ok(mut nq) = receiver.try_recv() {
        dq.append(&mut nq);
    }

    // flatten deque to vec
    let blobs_vec: Vec<_> = dq.into_iter().collect();

    // We could receive more blobs than window slots so
    // break them up into window-sized chunks to process
    let blobs_chunked = blobs_vec.chunks(WINDOW_SIZE as usize).map(|x| x.to_vec());

    trace!("{}", window.read().unwrap().print(&id, *receive_index));

    for mut blobs in blobs_chunked {
        let blobs_len = blobs.len();
        trace!("{}: broadcast blobs.len: {}", id, blobs_len);

        // Index the blobs
        window::index_blobs(node_info, &blobs, receive_index)
            .expect("index blobs for initial window");

        // keep the cache of blobs that are broadcast
        inc_new_counter_info!("streamer-broadcast-sent", blobs.len());
        {
            let mut win = window.write().unwrap();
            assert!(blobs.len() <= win.len());
            for b in &blobs {
                let ix = b.read().unwrap().get_index().expect("blob index");
                let pos = (ix % WINDOW_SIZE) as usize;
                if let Some(x) = win[pos].data.take() {
                    trace!(
                        "{} popped {} at {}",
                        id,
                        x.read().unwrap().get_index().unwrap(),
                        pos
                    );
                    recycler.recycle(x, "broadcast-data");
                }
                if let Some(x) = win[pos].coding.take() {
                    trace!(
                        "{} popped {} at {}",
                        id,
                        x.read().unwrap().get_index().unwrap(),
                        pos
                    );
                    recycler.recycle(x, "broadcast-coding");
                }

                trace!("{} null {}", id, pos);
            }
            while let Some(b) = blobs.pop() {
                let ix = b.read().unwrap().get_index().expect("blob index");
                let pos = (ix % WINDOW_SIZE) as usize;
                trace!("{} caching {} at {}", id, ix, pos);
                assert!(win[pos].data.is_none());
                win[pos].data = Some(b);
            }
        }

        // Fill in the coding blob data from the window data blobs
        #[cfg(feature = "erasure")]
        {
            erasure::generate_coding(
                &id,
                &mut window.write().unwrap(),
                recycler,
                *receive_index,
                blobs_len,
                &mut transmit_index.coding,
            )?;
        }

        *receive_index += blobs_len as u64;

        // Send blobs out from the window
        Crdt::broadcast(
            &node_info,
            &broadcast_table,
            &window,
            &sock,
            transmit_index,
            *receive_index,
        )?;
    }
    Ok(())
}

pub struct BroadcastStage {
    thread_hdl: JoinHandle<BroadcastStageReturnType>,
}

impl BroadcastStage {
    fn run(
        sock: &UdpSocket,
        crdt: &Arc<RwLock<Crdt>>,
        window: &SharedWindow,
        entry_height: u64,
        recycler: &BlobRecycler,
        receiver: &BlobReceiver,
        exit_sender: Sender<bool>,
    ) -> BroadcastStageReturnType {
        let mut transmit_index = WindowIndex {
            data: entry_height,
            coding: entry_height,
        };
        let mut receive_index = entry_height;
        let me = crdt.read().unwrap().my_data().clone();
        loop {
            if transmit_index.data % (LEADER_ROTATION_INTERVAL as u64) == 0 {
                let rcrdt = crdt.read().unwrap();
                let my_id = rcrdt.my_data().id;
                match rcrdt.get_scheduled_leader(entry_height) {
                    Some(id) if id == my_id => (),
                    // If the leader stays in power for the next
                    // round as well, then we don't exit. Otherwise, exit.
                    _ => {
                        let _ = exit_sender.send(true);
                        return BroadcastStageReturnType::LeaderRotation;
                    }
                }
            }

            let broadcast_table = crdt.read().unwrap().compute_broadcast_table();
            if let Err(e) = broadcast(
                &me,
                &broadcast_table,
                &window,
                &recycler,
                &receiver,
                &sock,
                &mut transmit_index,
                &mut receive_index,
            ) {
                match e {
                    Error::RecvTimeoutError(RecvTimeoutError::Disconnected) => {
                        return BroadcastStageReturnType::ChannelDisconnected
                    }
                    Error::RecvTimeoutError(RecvTimeoutError::Timeout) => (),
                    Error::CrdtError(CrdtError::NoPeers) => (), // TODO: Why are the unit-tests throwing hundreds of these?
                    _ => {
                        inc_new_counter_info!("streamer-broadcaster-error", 1, 1);
                        error!("broadcaster error: {:?}", e);
                    }
                }
            }
        }
    }

    /// Service to broadcast messages from the leader to layer 1 nodes.
    /// See `crdt` for network layer definitions.
    /// # Arguments
    /// * `sock` - Socket to send from.
    /// * `exit` - Boolean to signal system exit.
    /// * `crdt` - CRDT structure
    /// * `window` - Cache of blobs that we have broadcast
    /// * `recycler` - Blob recycler.
    /// * `receiver` - Receive channel for blobs to be retransmitted to all the layer 1 nodes.
    pub fn new(
        sock: UdpSocket,
        crdt: Arc<RwLock<Crdt>>,
        window: SharedWindow,
        entry_height: u64,
        recycler: BlobRecycler,
        receiver: BlobReceiver,
        exit_sender: Sender<bool>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("solana-broadcaster".to_string())
            .spawn(move || {
                Self::run(
                    &sock,
                    &crdt,
                    &window,
                    entry_height,
                    &recycler,
                    &receiver,
                    exit_sender,
                )
            })
            .unwrap();

        BroadcastStage { thread_hdl }
    }
}

impl Service for BroadcastStage {
    type JoinReturnType = BroadcastStageReturnType;

    fn join(self) -> thread::Result<BroadcastStageReturnType> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use broadcast_stage::{BroadcastStage, BroadcastStageReturnType};
    use crdt::{Crdt, Node, LEADER_ROTATION_INTERVAL};
    use entry::Entry;
    use ledger::Block;
    use mint::Mint;
    use packet::BlobRecycler;
    use recorder::Recorder;
    use service::Service;
    use signature::{Keypair, KeypairUtil, Pubkey};
    use std::cmp;
    use std::sync::mpsc::{channel, Receiver};
    use std::sync::{Arc, RwLock};
    use std::thread::sleep;
    use std::time::Duration;
    use streamer::BlobSender;
    use window::{new_window_from_entries, SharedWindow};

    fn setup_dummy_broadcast_stage() -> (
        Pubkey,
        Pubkey,
        BroadcastStage,
        SharedWindow,
        BlobSender,
        BlobRecycler,
        Arc<RwLock<Crdt>>,
        Vec<Entry>,
        Receiver<bool>,
    ) {
        // Setup dummy leader info
        let leader_keypair = Keypair::new();
        let id = leader_keypair.pubkey();
        let leader_info = Node::new_localhost_with_pubkey(leader_keypair.pubkey());

        // Give the leader somebody to broadcast to so he isn't lonely
        let buddy_keypair = Keypair::new();
        let buddy_id = buddy_keypair.pubkey();
        let broadcast_buddy = Node::new_localhost_with_pubkey(buddy_keypair.pubkey());

        // Fill the crdt with the buddy's info
        let mut crdt = Crdt::new(leader_info.info.clone()).expect("Crdt::new");
        crdt.insert(&broadcast_buddy.info);
        let crdt = Arc::new(RwLock::new(crdt));
        let blob_recycler = BlobRecycler::default();

        // Make dummy initial entries
        let mint = Mint::new(10000);
        let entries = mint.create_entries();
        let entry_height = entries.len() as u64;

        // Setup a window
        let window =
            new_window_from_entries(&entries, entry_height, &leader_info.info, &blob_recycler);

        let shared_window = Arc::new(RwLock::new(window));

        let (blob_sender, blob_receiver) = channel();
        let (exit_sender, exit_receiver) = channel();

        // Start up the broadcast stage
        let broadcast_stage = BroadcastStage::new(
            leader_info.sockets.broadcast,
            crdt.clone(),
            shared_window.clone(),
            entry_height,
            blob_recycler.clone(),
            blob_receiver,
            exit_sender,
        );

        (
            id,
            buddy_id,
            broadcast_stage,
            shared_window,
            blob_sender,
            blob_recycler,
            crdt,
            entries,
            exit_receiver,
        )
    }

    fn find_highest_window_index(shared_window: &SharedWindow) -> u64 {
        let window = shared_window.read().unwrap();
        window.iter().fold(0, |m, w_slot| {
            if let Some(ref blob) = w_slot.data {
                cmp::max(m, blob.read().unwrap().get_index().unwrap())
            } else {
                m
            }
        })
    }

    #[test]
    fn test_broadcast_stage_leader_rotation_exit() {
        let (
            id,
            buddy_id,
            broadcast_stage,
            shared_window,
            blob_sender,
            blob_recycler,
            crdt,
            entries,
            exit_receiver,
        ) = setup_dummy_broadcast_stage();

        crdt.write()
            .unwrap()
            .set_scheduled_leader(LEADER_ROTATION_INTERVAL, id);

        let genesis_len = entries.len() as u64;
        let last_entry_hash = entries.last().expect("Ledger should not be empty").id;

        // Input enough entries to make exactly LEADER_ROTATION_INTERVAL entries, which will
        // trigger a check for leader rotation. Because the next scheduled leader
        // is ourselves, we won't exit
        let mut recorder = Recorder::new(last_entry_hash);

        for _ in genesis_len..LEADER_ROTATION_INTERVAL {
            let new_entry = recorder.record(vec![]);
            let blob = new_entry.to_blobs(&blob_recycler);
            blob_sender.send(blob).unwrap();
        }

        loop {
            sleep(Duration::from_secs(1));
            let index = find_highest_window_index(&shared_window);
            // The window is one behind the entry height b/c the window
            // index is zero indexed, while the entry height starts at one.
            if index == LEADER_ROTATION_INTERVAL - 1 {
                break;
            }
        }

        // Set the scheduled next leader in the crdt to the buddy
        crdt.write()
            .unwrap()
            .set_scheduled_leader(2 * LEADER_ROTATION_INTERVAL, buddy_id);

        // Input another LEADER_ROTATION_INTERVAL dummy entries, which will take us
        // past the point of the leader rotation. The write_stage will see that
        // it's no longer the leader after checking the crdt, and exit
        for _ in 0..LEADER_ROTATION_INTERVAL {
            let new_entry = recorder.record(vec![]);
            let blob = new_entry.to_blobs(&blob_recycler);
            match blob_sender.send(blob) {
                // We disconnected, break out of loop and check the results
                Err(_) => break,
                _ => (),
            };
        }

        match exit_receiver.recv() {
            Ok(x) if x == false => panic!("Unexpected value on exit channel for Broadcast stage"),
            _ => (),
        }

        let highest_index = find_highest_window_index(&shared_window);

        // TODO: 2 * LEADER_ROTATION_INTERVAL - 1 due to the same bug in
        // index_blobs() as mentioned above
        assert_eq!(highest_index, 2 * LEADER_ROTATION_INTERVAL - 1);
        // Make sure the threads closed cleanly
        assert_eq!(
            broadcast_stage.join().unwrap(),
            BroadcastStageReturnType::LeaderRotation
        );
    }
}
