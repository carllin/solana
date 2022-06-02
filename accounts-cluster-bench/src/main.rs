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
        thread::sleep,
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

    let mut counter = 0;
    let client = ThinClient::new(tpu, tpu, UdpSocket::bind(format!("0.0.0.0:{}", 50000)).unwrap());

    loop {
        for _ in 0..tps / 10 {
            let transaction = Transaction::new_with_compiled_instructions(
                &[keypair.as_ref()],
                &[],
                Hash::new(&[0; 32]),
                vec![key],
                vec![],
            );
            let _ = client.async_send_transaction(transaction).unwrap();
            counter += 1;
            if counter % 1000 == 0 {
                println!("sent {}", counter);
            }
        }
        sleep(Duration::from_millis(100));
    }
}
