use crdt::Crdt;
use packet::{to_packets, PacketRecycler};
use result::Result;
use std::net::UdpSocket;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, Sender};
use std::thread::JoinHandle;
use std::time::Duration;
use transaction::Transaction;

fn send_transaction(
    crdt: Arc<RwLock<Crdt>>,
    transactions: Vec<Transaction>,
    sock: UdpSocket,
    r: &PacketRecycler,
)
{
    let leader_info = crdt.read().unwrap().leader_data();
    let p = to_packets(r, transactions, leader_info.transactions_addr);
    p.send_to(sock).unwrap();
}

fn receive_transactions(r: Receiver<Vec<Transaction>>) -> Result<Vec<Transaction>> {
    let timer = Duration::new(1, 0);
    let mut transactions = r.recv_timeout(timer)?;
    while let Ok(mut nq) = r.try_recv() {
        transactions.append(&mut nq);
    }
    transactions
}

pub fn transaction_recv_and_send(
    sock: UdpSocket,
    exit: Arc<AtomicBool>,
    crdt: Arc<RwLock<Crdt>>,
    recycler: PacketRecycler,
) -> (JoinHandle<()>, Sender<Vec<Transaction>>)
{
    let (transaction_sender, transaction_receiver) = channel();

    let t = Builder::new()
        .name("solana-transaction_recv_and_send".to_string())
        .spawn(move || {
            let debug_id = crdt.read().unwrap().debug_id();
            loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                let transactions = receive_transactions(transaction_receiver);
                let _ = send_transaction(
                    crdt,
                    transactions,
                    sock,
                    recycler,
                );
            }
        })
        .unwrap();

    (t, transaction_sender);
}