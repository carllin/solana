use clap::{crate_description, crate_name, value_t, value_t_or_exit, App, Arg};
use log::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use solana_client::rpc_client::RpcClient;
use solana_core::gossip_service::discover;
use solana_faucet::faucet::{request_airdrop_transaction, FAUCET_PORT};
use solana_measure::measure::Measure;
use solana_sdk::rpc_port::DEFAULT_RPC_PORT;
use solana_sdk::signature::{read_keypair_file, Keypair, Signature, Signer};
use solana_sdk::timing::timestamp;
use solana_sdk::{message::Message, transaction::Transaction};
use solana_sdk::{system_instruction, system_program};
use std::net::SocketAddr;
use std::process::exit;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{sleep, Builder, JoinHandle};
use std::time::Duration;
use std::time::Instant;

fn run_accounts_bench(
    entrypoint_addr: SocketAddr,
    faucet_addr: SocketAddr,
    keypair: &Keypair,
    iterations: usize,
    maybe_space: Option<u64>,
    batch_size: usize,
    maybe_lamports: Option<u64>,
    num_instructions: usize,
) {
    assert!(num_instructions > 0);
    let client = RpcClient::new_socket(entrypoint_addr);

    info!("Targetting {}", entrypoint_addr);

    let mut last_blockhash = Instant::now();
    let mut last_log = Instant::now();
    let mut count = 0;
    let mut recent_blockhash = client.get_recent_blockhash().expect("blockhash");
    let mut tx_sent_count = 0;
    let mut total_account_count = 0;

    info!("Starting balance: {}", balance);

    let executor = TransactionExecutor::new(entrypoint_addr);

    loop {
        if last_blockhash.elapsed().as_millis() > 10_000 {
            recent_blockhash = client.get_recent_blockhash().expect("blockhash");
            last_blockhash = Instant::now();
        }

        // Craft the openorders accounts
        system_instruction::create_account(
            &keypair.pubkey(),
            &new_keypair.pubkey(),
            balance,
            space,
            &system_program::id(),
        );

        // Craft transactions
        match self.client.send_transaction(&tx) {
            Ok(sig) => {
                return Some((sig, timestamp(), id));
            }
            Err(e) => {
                info!("error: {:#?}", e);
            }
        }
    }
    executor.close();
}

fn main() {
    solana_logger::setup_with_default("solana=info");
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("entrypoint")
                .long("entrypoint")
                .takes_value(true)
                .value_name("HOST:PORT")
                .help("RPC entrypoint address. Usually <ip>:8899"),
        )
        .get_matches();

    let mut entrypoint_addr = SocketAddr::from(([127, 0, 0, 1], port));
    if let Some(addr) = matches.value_of("entrypoint") {
        entrypoint_addr = solana_net_utils::parse_host_port(addr).unwrap_or_else(|e| {
            eprintln!("failed to parse entrypoint address: {}", e);
            exit(1)
        });
    }
    let keypair =
        read_keypair_file(&value_t_or_exit!(matches, "identity", String)).expect("bad keypair");
    let rpc_addr = if !skip_gossip {
        info!("Finding cluster entry: {:?}", entrypoint_addr);
        let (gossip_nodes, _validators) = discover(
            None,
            Some(&entrypoint_addr),
            None,
            Some(60),
            None,
            Some(&entrypoint_addr),
            None,
            0,
        )
        .unwrap_or_else(|err| {
            eprintln!("Failed to discover {} node: {:?}", entrypoint_addr, err);
            exit(1);
        });

        info!("done found {} nodes", gossip_nodes.len());
        gossip_nodes[0].rpc
    } else {
        info!("Using {:?} as the RPC address", entrypoint_addr);
        entrypoint_addr
    };

    run_accounts_bench(rpc_addr, &keypair);
}

#[cfg(test)]
pub mod test {
    use super::*;
    use solana_core::validator::ValidatorConfig;
    use solana_local_cluster::local_cluster::{ClusterConfig, LocalCluster};
    use solana_sdk::poh_config::PohConfig;

    #[test]
    fn test_accounts_cluster_bench() {
        solana_logger::setup();
        let validator_config = ValidatorConfig::default();
        let num_nodes = 1;
        let mut config = ClusterConfig {
            cluster_lamports: 10_000_000,
            poh_config: PohConfig::new_sleep(Duration::from_millis(50)),
            node_stakes: vec![100; num_nodes],
            validator_configs: vec![validator_config; num_nodes],
            ..ClusterConfig::default()
        };

        let faucet_addr = SocketAddr::from(([127, 0, 0, 1], 9900));
        let cluster = LocalCluster::new(&mut config);
        let iterations = 10;
        let maybe_space = None;
        let batch_size = 100;
        let maybe_lamports = None;
        let num_instructions = 2;
        let mut start = Measure::start("total accounts run");
        run_accounts_bench(
            cluster.entry_point_info.rpc,
            faucet_addr,
            &cluster.funding_keypair,
            iterations,
            maybe_space,
            batch_size,
            maybe_lamports,
            num_instructions,
        );
        start.stop();
        info!("{}", start);
    }
}
