use crdt::Crdt;
use packet::{to_packets, PacketRecycler};
use std::net::UdpSocket;
use std::sync::{Arc, RwLock};
use transaction::Transaction;

pub fn send_transaction(
    crdt: Arc<RwLock<Crdt>>,
    transactions: Vec<Transaction>,
    sock: UdpSocket,
    r: &PacketRecycler,
)
{
    let leader_info = crdt.read().unwrap().leader_data().unwrap();
    let pv = to_packets(r, transactions, leader_info.transactions_addr);
    for p in pv {
        p.read().unwrap().send_to(&sock).unwrap();
    }
}