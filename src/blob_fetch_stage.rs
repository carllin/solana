//! The `blob_fetch_stage` pulls blobs from UDP sockets and sends it to a channel.

use packet::BlobRecycler;
use service::Service;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use streamer::{self, BlobReceiver, BooleanCondvar};

pub struct BlobFetchStage {
    exit: Arc<AtomicBool>,
    thread_hdls: Vec<JoinHandle<()>>,
    block: Arc<BooleanCondvar>,
}

impl BlobFetchStage {
    pub fn new(
        socket: Arc<UdpSocket>,
        exit: Arc<AtomicBool>,
        block: Arc<BooleanCondvar>,
        recycler: &BlobRecycler,
    ) -> (Self, BlobReceiver) {
        Self::new_multi_socket(vec![socket], exit, block, recycler)
    }
    pub fn new_multi_socket(
        sockets: Vec<Arc<UdpSocket>>,
        exit: Arc<AtomicBool>,
        block: Arc<BooleanCondvar>,
        recycler: &BlobRecycler,
    ) -> (Self, BlobReceiver) {
        let (sender, receiver) = channel();
        let thread_hdls: Vec<_> = sockets
            .into_iter()
            .map(|socket| {
                streamer::blob_receiver(
                    socket,
                    exit.clone(),
                    Some(block.clone()),
                    recycler.clone(),
                    sender.clone(),
                )
            })
            .collect();

        (
            BlobFetchStage {
                exit,
                thread_hdls,
                block,
            },
            receiver,
        )
    }

    pub fn block(&self) {
        self.block.set_no_signal(true);
    }

    pub fn unblock(&self) {
        self.block.set_signal_all(false);
    }

    pub fn close(&self) {
        self.block.set_signal_all(false);
        self.exit.store(true, Ordering::Relaxed);
    }
}

impl Service for BlobFetchStage {
    fn thread_hdls(self) -> Vec<JoinHandle<()>> {
        self.thread_hdls
    }

    fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls() {
            thread_hdl.join()?;
        }
        Ok(())
    }
}
