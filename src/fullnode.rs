//! The `fullnode` module hosts all the fullnode microservices.

use bank::Bank;
use broadcast_stage::BroadcastStage;
use crdt::{Crdt, Node, NodeInfo};
use drone::DRONE_PORT;
use entry::Entry;
use ledger::read_ledger;
use ncp::Ncp;
use packet::BlobRecycler;
use rpc::{JsonRpcService, RPC_PORT};
use rpu::Rpu;
use service::Service;
use signature::{Keypair, KeypairUtil, Pubkey};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{JoinHandle, Result};
use tpu::Tpu;
use tvu::Tvu;
use untrusted::Input;
use window;

#[derive(Copy, Clone, Debug, PartialEq)]
enum NodeRole {
    Leader,
    Validator,
}

pub struct Fullnode {
    pub id: Pubkey,
    exit: Arc<AtomicBool>,
    thread_hdls: Vec<JoinHandle<()>>,
    node_role: NodeRole,
    crdt: Arc<RwLock<Crdt>>,
    bank: Arc<Bank>,
    tvu: Tvu,
    tpu: Option<Tpu>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
/// Fullnode configuration to be stored in file
pub struct Config {
    pub node_info: NodeInfo,
    pkcs8: Vec<u8>,
}

/// Structure to be replicated by the network
impl Config {
    pub fn new(bind_addr: &SocketAddr, pkcs8: Vec<u8>) -> Self {
        let keypair =
            Keypair::from_pkcs8(Input::from(&pkcs8)).expect("from_pkcs8 in fullnode::Config new");
        let pubkey = keypair.pubkey();
        let node_info = NodeInfo::new_with_pubkey_socketaddr(pubkey, bind_addr);
        Config { node_info, pkcs8 }
    }
    pub fn keypair(&self) -> Keypair {
        Keypair::from_pkcs8(Input::from(&self.pkcs8))
            .expect("from_pkcs8 in fullnode::Config keypair")
    }
}

impl Fullnode {
    pub fn new(
        node: Node,
        ledger_path: &str,
        keypair: Keypair,
        leader_addr: Option<SocketAddr>,
        sigverify_disabled: bool,
    ) -> Self {
        info!("creating bank...");
        let bank = Bank::new_default(leader_addr.is_none());

        let entries = read_ledger(ledger_path, true).expect("opening ledger");

        let entries = entries.map(|e| e.expect("failed to parse entry"));

        info!("processing ledger...");
        let (entry_height, ledger_tail) = bank.process_ledger(entries).expect("process_ledger");
        // entry_height is the network-wide agreed height of the ledger.
        //  initialize it from the input ledger
        info!("processed {} ledger...", entry_height);

        info!("creating networking stack...");

        let local_gossip_addr = node.sockets.gossip.local_addr().unwrap();
        info!(
            "starting... local gossip address: {} (advertising {})",
            local_gossip_addr, node.info.contact_info.ncp
        );
        let exit = Arc::new(AtomicBool::new(false));
        let local_requests_addr = node.sockets.requests.local_addr().unwrap();
        let requests_addr = node.info.contact_info.rpu;
        let leader_info = leader_addr.map(|i| NodeInfo::new_entry_point(&i));
        let server = Self::new_with_bank(
            keypair,
            bank,
            entry_height,
            &ledger_tail,
            node,
            leader_info.as_ref(),
            exit,
            Some(ledger_path),
            sigverify_disabled,
        );

        match leader_addr {
            Some(leader_addr) => {
                info!(
                "validator ready... local request address: {} (advertising {}) connected to: {}",
                local_requests_addr, requests_addr, leader_addr
            );
            }
            None => {
                info!(
                    "leader ready... local request address: {} (advertising {})",
                    local_requests_addr, requests_addr
                );
            }
        }

        server
    }

    /// Create a fullnode instance acting as a leader or validator.
    ///
    /// ```text
    ///              .---------------------.
    ///              |  Leader             |
    ///              |                     |
    ///  .--------.  |  .-----.            |
    ///  |        |---->|     |            |
    ///  | Client |  |  | RPU |            |
    ///  |        |<----|     |            |
    ///  `----+---`  |  `-----`            |
    ///       |      |     ^               |
    ///       |      |     |               |
    ///       |      |  .--+---.           |
    ///       |      |  | Bank |           |
    ///       |      |  `------`           |
    ///       |      |     ^               |
    ///       |      |     |               |    .------------.
    ///       |      |  .--+--.   .-----.  |    |            |
    ///       `-------->| TPU +-->| NCP +------>| Validators |
    ///              |  `-----`   `-----`  |    |            |
    ///              |                     |    `------------`
    ///              `---------------------`
    ///
    ///               .-------------------------------.
    ///               | Validator                     |
    ///               |                               |
    ///   .--------.  |            .-----.            |
    ///   |        |-------------->|     |            |
    ///   | Client |  |            | RPU |            |
    ///   |        |<--------------|     |            |
    ///   `--------`  |            `-----`            |
    ///               |               ^               |
    ///               |               |               |
    ///               |            .--+---.           |
    ///               |            | Bank |           |
    ///               |            `------`           |
    ///               |               ^               |
    ///   .--------.  |               |               |    .------------.
    ///   |        |  |            .--+--.            |    |            |
    ///   | Leader |<------------->| TVU +<--------------->|            |
    ///   |        |  |            `-----`            |    | Validators |
    ///   |        |  |               ^               |    |            |
    ///   |        |  |               |               |    |            |
    ///   |        |  |            .--+--.            |    |            |
    ///   |        |<------------->| NCP +<--------------->|            |
    ///   |        |  |            `-----`            |    |            |
    ///   `--------`  |                               |    `------------`
    ///               `-------------------------------`
    /// ```
    pub fn new_with_bank(
        keypair: Keypair,
        bank: Bank,
        entry_height: u64,
        ledger_tail: &[Entry],
        mut node: Node,
        leader_info: Option<&NodeInfo>,
        exit: Arc<AtomicBool>,
        ledger_path: Option<&str>,
        sigverify_disabled: bool,
    ) -> Self {
        if leader_info.is_none() {
            node.info.leader_id = node.info.id;
        }

        let bank = Arc::new(bank);
        let mut thread_hdls = vec![];

        let rpu = Rpu::new(
            &bank,
            node.sockets.requests,
            node.sockets.respond,
            exit.clone(),
        );
        thread_hdls.extend(rpu.thread_hdls());

        // TODO: this code assumes this node is the leader
        let mut drone_addr = node.info.contact_info.tpu;
        drone_addr.set_port(DRONE_PORT);
        let rpc_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(0)), RPC_PORT);
        let rpc_service = JsonRpcService::new(
            &bank,
            node.info.contact_info.tpu,
            drone_addr,
            rpc_addr,
            exit.clone(),
        );
        thread_hdls.extend(rpc_service.thread_hdls());

        let blob_recycler = BlobRecycler::default();
        let window =
            window::new_window_from_entries(ledger_tail, entry_height, &node.info, &blob_recycler);

        let crdt = Arc::new(RwLock::new(Crdt::new(node.info).expect("Crdt::new")));

        let ncp = Ncp::new(
            &crdt,
            window.clone(),
            ledger_path,
            node.sockets.gossip,
            exit.clone(),
        );
        thread_hdls.extend(ncp.thread_hdls());
        let node_role;
        match leader_info {
            Some(leader_info) => {
                // Start in validator mode.
                // TODO: let Crdt get that data from the network?
                crdt.write().unwrap().insert(leader_info);
                node_role = NodeRole::Validator;
            }
            None => {
                node_role = NodeRole::Leader;
            }
        }

        let keypair = Arc::new(keypair);
        let tvu = Tvu::new(
            keypair.clone(),
            &bank,
            entry_height,
            crdt.clone(),
            window.clone(),
            node.sockets.replicate,
            node.sockets.repair,
            node.sockets.retransmit,
            ledger_path,
            exit.clone(),
            node_role == NodeRole::Leader,
        );

        let tick_duration = None;
        // TODO: To light up PoH, uncomment the following line:
        //let tick_duration = Some(Duration::from_millis(1000));

        let tpu_option;
        if let Some(ledger_path) = ledger_path {
            let (tpu, blob_receiver) = Tpu::new(
                keypair.clone(),
                &bank,
                &crdt,
                tick_duration,
                node.sockets.transaction,
                &blob_recycler,
                exit.clone(),
                ledger_path,
                sigverify_disabled,
                node_role == NodeRole::Validator,
            );

            let broadcast_stage = BroadcastStage::new(
                node.sockets.broadcast,
                crdt.clone(),
                window,
                entry_height,
                blob_recycler.clone(),
                blob_receiver,
            );

            thread_hdls.extend(broadcast_stage.thread_hdls());
            tpu_option = Some(tpu);
        } else {
            tpu_option = None;
        }

        Fullnode {
            exit,
            thread_hdls,
            node_role,
            id: keypair.pubkey(),
            crdt,
            tpu: tpu_option,
            tvu,
            bank,
        }
    }

    pub fn handle_new_leader(&mut self, new_leader_id: Pubkey) {
        self.crdt
            .write()
            .unwrap()
            .update_leader(Some(new_leader_id));
        if self.node_role == NodeRole::Leader && new_leader_id != self.id {
            self.transition_role(NodeRole::Validator)
        }

        if self.node_role == NodeRole::Validator && new_leader_id == self.id {
            self.transition_role(NodeRole::Leader)
        }
    }

    fn transition_role(&mut self, new_role: NodeRole) {
        match (self.node_role, new_role) {
            (NodeRole::Leader, NodeRole::Validator) => {
                self.bank.is_leader.store(false, Ordering::Relaxed);
                self.start_validator();
                self.node_role = NodeRole::Validator;
            }
            (NodeRole::Validator, NodeRole::Leader) => {
                // If this is a validator only node (didn't supply a ledger path), 
                // don't transition
                if self.tpu.is_none() {
                    return;
                }

                self.bank.is_leader.store(true, Ordering::Relaxed);
                self.start_leader();
                self.node_role = NodeRole::Leader;
            }
            _ => {
                panic!(format!(
                    "Invalid role transition from {:?} to {:?}",
                    self.node_role, new_role
                ));
            }
        }

        self.node_role = new_role;
    }

    fn start_leader(&self) {
        self.tvu.block();
        if let Some(ref tpu) = self.tpu {
            tpu.unblock();
        }
    }

    fn start_validator(&self) {
        if let Some(ref tpu) = self.tpu {
            tpu.block();
        }
        self.tvu.unblock();
    }

    //used for notifying many nodes in parallel to exit
    pub fn exit(&self) {
        self.exit.store(true, Ordering::Relaxed);
        self.tvu.unblock();
        if let Some(ref tpu) = self.tpu {
            tpu.unblock();
        }
    }
    
    pub fn close(self) -> Result<()> {
        self.exit();
        self.join()
    }
}

impl Service for Fullnode {
    fn thread_hdls(self) -> Vec<JoinHandle<()>> {
        let mut threads = self.thread_hdls;
        threads.extend(self.tvu.thread_hdls());
        if let Some(tpu) = self.tpu {
            threads.extend(tpu.thread_hdls());
        }
        threads
    }

    fn join(self) -> Result<()> {
        for thread_hdl in self.thread_hdls() {
            thread_hdl.join()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    extern crate utilities;
    use bank::Bank;
    use crdt::Node;
    use fullnode::{Fullnode, NodeRole};
    use mint::Mint;
    use service::Service;
    use signature::{Keypair, KeypairUtil};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use self::utilities::node_test_helpers::genesis;

    #[test]
    fn validator_exit() {
        let keypair = Keypair::new();
        let tn = Node::new_localhost_with_pubkey(keypair.pubkey());
        let alice = Mint::new(10_000);
        let bank = Bank::new(&alice);
        let exit = Arc::new(AtomicBool::new(false));
        let entry = tn.info.clone();
        let v = Fullnode::new_with_bank(keypair, bank, 0, &[], tn, Some(&entry), exit, None, false);
        v.exit();
        v.join().unwrap();
    }
    #[test]
    fn validator_parallel_exit() {
        let vals: Vec<Fullnode> = (0..2)
            .map(|_| {
                let keypair = Keypair::new();
                let tn = Node::new_localhost_with_pubkey(keypair.pubkey());
                let alice = Mint::new(10_000);
                let bank = Bank::new(&alice);
                let exit = Arc::new(AtomicBool::new(false));
                let entry = tn.info.clone();
                Fullnode::new_with_bank(keypair, bank, 0, &[], tn, Some(&entry), exit, None, false)
            })
            .collect();
        //each validator can exit in parallel to speed many sequential calls to `join`
        vals.iter().for_each(|v| v.exit());
        //while join is called sequentially, the above exit call notified all the
        //validators to exit from all their threads
        vals.into_iter().for_each(|v| {
            v.join().unwrap();
        });
    }

    #[test]
    fn test_transition_exit() {
        // Make a mint and a genesis entry in the leader ledger
        let (_, leader_ledger_path) = genesis("test_transition_exit", 10_000);

        // Initialize the leader ledger
        let mut ledger_paths = Vec::new();
        ledger_paths.push(leader_ledger_path.clone());

        // Start the leader node
        let leader_keypair = Keypair::new();
        let leader_info = Node::new_localhost_with_pubkey(leader_keypair.pubkey());
        let mut leader_node = Fullnode::new(leader_info, &leader_ledger_path, leader_keypair, None, false);

        // Demote the leader to a validator, promote back to leader, then test exit
        leader_node.transition_role(NodeRole::Validator);
        leader_node.transition_role(NodeRole::Leader);
        leader_node.close().unwrap();
    }
}
