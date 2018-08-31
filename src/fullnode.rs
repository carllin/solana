//! The `fullnode` module hosts all the fullnode microservices.

use bank::Bank;
use broadcast_stage::BroadcastStage;
use crdt::{Crdt, Node, NodeInfo, Sockets};
use drone::DRONE_PORT;
use entry::Entry;
use ledger::read_ledger;
use ncp::Ncp;
use packet::BlobRecycler;
use rpc::{JsonRpcService, RPC_PORT};
use rpu::Rpu;
use service::Service;
use signature::{Keypair, KeypairUtil};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{JoinHandle, Result};
use tpu::Tpu;
use tvu::Tvu;
use untrusted::Input;
use window;

#[derive(Copy, Clone, Debug)]
pub enum NodeEvent {
    /// State Change from Validator to Leader
    ValidatorToLeader,
    /// State Change from Leader to Validator
    LeaderToValidator,
}

pub struct CommonState {
    keypair: Arc<Keypair>,
    bank: Arc<Bank>,
    blob_recycler: BlobRecycler,
    window: window::SharedWindow,
    crdt: Arc<RwLock<Crdt>>,
    sockets: Sockets,
}

#[derive(Copy, Clone, Debug)]
pub enum NodeRole {
    Leader,
    Validator,
}

struct ServiceManager {
    thread_hdls: Vec<JoinHandle<()>>,
    exit: Arc<AtomicBool>,
}

impl ServiceManager {
    fn new() -> Self {
        let exit = Arc::new(AtomicBool::new(false));
        let thread_hdls = vec![];
        ServiceManager { exit, thread_hdls }
    }

    fn thread_hdls(self) -> Vec<JoinHandle<()>> {
        self.thread_hdls
    }

    fn exit(&self) -> () {
        self.exit.store(true, Ordering::Relaxed);
    }

    fn join(self) -> Result<()> {
        let mut results = vec![];
        for thread_hdl in self.thread_hdls {
            let result = thread_hdl.join();
            if let Err(ref err) = &result {
                println!("Thread panicked with error: {:?}", err);
            }

            results.push(result);
        }

        for r in results {
            r?;
        }

        Ok(())
    }

    fn close(self) -> Result<()> {
        self.exit();
        self.join()
    }
}

trait RoleServices {
    fn close_services(self: Box<Self>) -> Result<()>;
    fn service_manager_ref(&self) -> &ServiceManager;
    fn service_manager_owned(self: Box<Self>) -> ServiceManager;
}

struct LeaderRole {
    service_manager: ServiceManager,
}

impl LeaderRole {
    fn new(
        entry_height: u64,
        common_state: &CommonState,
        sigverify_disabled: bool,
        ledger_path: &str,
    ) -> Self {
        let mut service_manager = ServiceManager::new();
        let service_handles = Self::leader_services(
            entry_height,
            common_state,
            sigverify_disabled,
            ledger_path,
            service_manager.exit.clone(),
        );

        service_manager.thread_hdls = service_handles;
        LeaderRole { service_manager }
    }

    fn leader_services(
        entry_height: u64,
        common_state: &CommonState,
        sigverify_disabled: bool,
        ledger_path: &str,
        exit: Arc<AtomicBool>,
    ) -> Vec<JoinHandle<()>> {
        let mut thread_hdls = vec![];
        let tick_duration = None;
        // TODO: To light up PoH, uncomment the following line:
        //let tick_duration = Some(Duration::from_millis(1000));
        let (tpu, blob_receiver) = Tpu::new(
            &common_state.keypair,
            &common_state.bank,
            &common_state.crdt,
            tick_duration,
            common_state.sockets.transaction.clone(),
            &common_state.blob_recycler,
            exit,
            ledger_path,
            sigverify_disabled,
        );

        thread_hdls.extend(tpu.thread_hdls());

        let broadcast_stage = BroadcastStage::new(
            common_state.sockets.broadcast.clone(),
            common_state.crdt.clone(),
            common_state.window.clone(),
            entry_height,
            common_state.blob_recycler.clone(),
            blob_receiver,
        );

        thread_hdls.extend(broadcast_stage.thread_hdls());
        thread_hdls
    }
}

impl RoleServices for LeaderRole {
    fn close_services(self: Box<Self>) -> Result<()> {
        self.service_manager_owned().close()
    }

    fn service_manager_ref(&self) -> &ServiceManager {
        &self.service_manager
    }

    fn service_manager_owned(self: Box<Self>) -> ServiceManager {
        self.service_manager
    }
}

struct ValidatorRole {
    service_manager: ServiceManager,
}

impl ValidatorRole {
    fn new(entry_height: u64, common_state: &CommonState, ledger_path: Option<&str>) -> Self {
        let mut service_manager = ServiceManager::new();
        let service_handles = Self::validator_services(
            entry_height,
            common_state,
            ledger_path,
            service_manager.exit.clone(),
        );

        service_manager.thread_hdls = service_handles;
        ValidatorRole { service_manager }
    }

    fn validator_services(
        entry_height: u64,
        common_state: &CommonState,
        ledger_path: Option<&str>,
        exit: Arc<AtomicBool>,
    ) -> Vec<JoinHandle<()>> {
        let mut thread_hdls = vec![];
        let tvu = Tvu::new(
            &common_state.keypair,
            &common_state.bank,
            entry_height,
            common_state.crdt.clone(),
            common_state.window.clone(),
            common_state.sockets.replicate.clone(),
            common_state.sockets.repair.clone(),
            common_state.sockets.retransmit.clone(),
            ledger_path,
            exit,
        );

        thread_hdls.extend(tvu.thread_hdls());
        thread_hdls
    }
}

impl RoleServices for ValidatorRole {
    fn close_services(self: Box<Self>) -> Result<()> {
        self.service_manager_owned().close()
    }

    fn service_manager_ref(&self) -> &ServiceManager {
        &self.service_manager
    }

    fn service_manager_owned(self: Box<Self>) -> ServiceManager {
        self.service_manager
    }
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

pub struct Fullnode {
    exit: Arc<AtomicBool>,
    thread_hdls: Vec<JoinHandle<()>>,
    node_role: NodeRole,
    role_services: Option<Box<RoleServices + Send>>,
    common_state: CommonState,
    ledger_path: Option<String>,
    sigverify_disabled: bool,
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
            node.sockets.requests.clone(),
            node.sockets.respond.clone(),
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
            node.sockets.gossip.clone(),
            exit.clone(),
        );
        thread_hdls.extend(ncp.thread_hdls());

        let keypair = Arc::new(keypair);
        // Make the common state info
        let common_state = CommonState {
            keypair,
            bank,
            blob_recycler,
            window,
            crdt,
            sockets: node.sockets,
        };

        let node_role;
        let role_services: Box<RoleServices + Send>;
        match leader_info {
            Some(leader_info) => {
                // Start in validator mode.
                // TODO: let Crdt get that data from the network?
                common_state.crdt.write().unwrap().insert(leader_info);
                let validator_role = ValidatorRole::new(entry_height, &common_state, ledger_path);

                role_services = Box::new(validator_role);
                node_role = NodeRole::Validator;
            }
            None => {
                // Start in leader mode.
                let ledger_path = ledger_path.expect("ledger path");
                let leader_role =
                    LeaderRole::new(entry_height, &common_state, sigverify_disabled, ledger_path);
                role_services = Box::new(leader_role);
                node_role = NodeRole::Leader;
            }
        }

        Fullnode {
            exit,
            thread_hdls,
            node_role,
            role_services: Some(role_services),
            sigverify_disabled,
            common_state,
            // Take ownership of the ledger path passed in because we store the value
            ledger_path: ledger_path.map(|path| path.to_string()),
        }
    }

    pub fn handle_event(&mut self, event: NodeEvent) -> Result<()> {
        let role_services_option = self.role_services.take();
        let role_services = role_services_option.unwrap();
        let ref_ledger_path = self.ledger_path.as_ref().map(String::as_ref);

        match (self.node_role, event) {
            (NodeRole::Leader, NodeEvent::LeaderToValidator) => {
                // TODO (carlin): If error occurs on closing the other services we
                // still try to continue opening the other services? Should we return
                // Ok or Err? Right now on join failures, we still try to spin up the
                // new services and return an Error after.
                let close_result = role_services.close_services();
                let validator_role = ValidatorRole::new(
                    0, //TODO (carllin): fill in the actual entry_height
                    &self.common_state,
                    ref_ledger_path,
                );

                self.role_services = Some(Box::new(validator_role));
                self.node_role = NodeRole::Validator;
                close_result?
            }

            (NodeRole::Validator, NodeEvent::ValidatorToLeader) => {
                let ledger_path =
                    ref_ledger_path.expect("ledger path expected for transition to leader role");
                let close_result = role_services.close_services();
                let leader_role = LeaderRole::new(
                    0, //TODO (carllin): fill in the actual entry_height
                    &self.common_state,
                    self.sigverify_disabled,
                    ledger_path,
                );
                self.role_services = Some(Box::new(leader_role));
                self.node_role = NodeRole::Leader;
                close_result?
            }

            _ => panic!(
                format!(
                    "Invalid node role transition : {:#?} -> {:#?}",
                    self.node_role, event,
                ).to_string()
            ),
        }

        Ok(())
    }

    //used for notifying many nodes in parallel to exit
    pub fn exit(&self) {
        if let Some(ref role_services) = self.role_services {
            role_services.service_manager_ref().exit();
        }
        self.exit.store(true, Ordering::Relaxed);
    }

    pub fn close(self) -> Result<()> {
        self.exit();
        self.join()
    }
}

impl Service for Fullnode {
    fn thread_hdls(self) -> Vec<JoinHandle<()>> {
        let mut thread_hdls = self.thread_hdls;
        if let Some(role_services) = self.role_services {
            thread_hdls.extend(role_services.service_manager_owned().thread_hdls())
        }

        thread_hdls
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
    use bank::Bank;
    use crdt::Node;
    use fullnode::Fullnode;
    use mint::Mint;
    use service::Service;
    use signature::{Keypair, KeypairUtil};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

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
}
