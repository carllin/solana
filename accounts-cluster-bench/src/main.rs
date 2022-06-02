extern crate log;
use {
    clap::{crate_description, crate_name, value_t, App, Arg},
    rand::thread_rng,
    solana_client::thin_client::ThinClient,
    solana_sdk::{
        client::AsyncClient, hash::Hash, pubkey::Pubkey, signature::Keypair,
        transaction::Transaction,
    },
    std::{
        net::UdpSocket,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread::{sleep, spawn},
        time::Duration,
    },
};

fn main() {
    solana_logger::setup();

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("tpu")
                .short("n")
                .long("tpu")
                .value_name("HOST:PORT")
                .required(true)
                .help("Rendezvous with the cluster at this gossip entrypoint"),
        )
        .arg(
            Arg::with_name("tps")
                .long("tps")
                .takes_value(true)
                .value_name("TPS")
                .help("Transactions per second!")
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

    let tpu = matches
        .value_of("tpu")
        .map(|address| {
            solana_net_utils::parse_host_port(address).expect("failed to parse faucet address")
        })
        .unwrap();

    let tps = value_t!(matches, "tps", usize).unwrap();
    let keypair = Arc::new(Keypair::generate(&mut thread_rng()));
    let key = Pubkey::new_unique();

    let counter = Arc::new(AtomicUsize::new(0));
    let thread_count = value_t!(matches, "num_threads", u32).unwrap();
    let mut threads = Vec::new();

    for i in 0..thread_count {
        let counter = Arc::clone(&counter);
        let keypair = keypair.clone();
        let client = ThinClient::new(
            tpu,
            tpu,
            UdpSocket::bind(format!("0.0.0.0:{}", 50000 + i)).unwrap(),
        );
        threads.push(spawn(move || loop {
            for _ in 0..tps / thread_count as usize{
                let transaction = Transaction::new_with_compiled_instructions(
                    &[keypair.as_ref()],
                    &[],
                    Hash::new_unique(),
                    vec![key],
                    vec![],
                );
                let _ = client.async_send_transaction(transaction).unwrap();
                if counter.fetch_add(1, Ordering::Relaxed) % 0x1000 == 0 {
                    println!("sent {}", counter.load(Ordering::Relaxed));
                }
            }
            sleep(Duration::from_millis(1000));
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
