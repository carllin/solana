//! The `fetch_stage` batches input from a UDP socket and sends it to a channel.

use packet::PacketRecycler;
use service::Service;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use streamer::{self, BooleanCondvar, PacketReceiver};

pub struct FetchStage {
    exit: Arc<AtomicBool>,
    thread_hdls: Vec<JoinHandle<()>>,
    block: Arc<BooleanCondvar>,
}

impl FetchStage {
    pub fn new(
        sockets: Vec<UdpSocket>,
        exit: Arc<AtomicBool>,
        block: Arc<BooleanCondvar>,
        recycler: &PacketRecycler,
    ) -> (Self, PacketReceiver) {
        let tx_sockets = sockets.into_iter().map(Arc::new).collect();
        Self::new_multi_socket(tx_sockets, exit, block, recycler)
    }
    pub fn new_multi_socket(
        sockets: Vec<Arc<UdpSocket>>,
        exit: Arc<AtomicBool>,
        block: Arc<BooleanCondvar>,
        recycler: &PacketRecycler,
    ) -> (Self, PacketReceiver) {
        let (sender, receiver) = channel();
        let thread_hdls: Vec<_> = sockets
            .into_iter()
            .map(|socket| {
                streamer::receiver(
                    socket,
                    exit.clone(),
                    Some(block.clone()),
                    recycler.clone(),
                    sender.clone(),
                )
            }).collect();

        (
            FetchStage {
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

impl Service for FetchStage {
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
