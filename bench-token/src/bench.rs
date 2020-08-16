use crate::cli::Config;
use log::*;
use rayon::prelude::*;
use solana_client::{
    perf_utils::{sample_txs, SampleStats},
    thin_client::ThinClient,
};
use solana_core::gen_keys::GenKeys;
use solana_faucet::faucet::request_airdrop_transaction;
use solana_measure::measure::Measure;
use solana_metrics::{self, datapoint_info};
use solana_sdk::{
    client::{AsyncClient, Client, SyncClient},
    clock::{DEFAULT_TICKS_PER_SECOND, DEFAULT_TICKS_PER_SLOT, MAX_PROCESSING_AGE},
    commitment_config::CommitmentConfig,
    fee_calculator::FeeCalculator,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::Message,
    program_error::ProgramError,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    timing::{duration_as_ms, duration_as_s, duration_as_us, timestamp},
    transaction::Transaction,
};
use spl_token_v1_0::{
    self,
    instruction::*,
    state::{Account, Mint},
};
use std::mem::size_of;
use std::{
    collections::{HashSet, VecDeque},
    net::SocketAddr,
    process::exit,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

const NUM_DECIMALS: u8 = 0;
// The point at which transactions become "too old", in seconds.
const MAX_TX_QUEUE_AGE: u64 =
    MAX_PROCESSING_AGE as u64 * DEFAULT_TICKS_PER_SLOT / DEFAULT_TICKS_PER_SECOND;

pub const MAX_SPENDS_PER_TX: u64 = 4;

#[derive(Debug)]
pub enum BenchTokenError {
    AirdropFailure,
}

pub type Result<T> = std::result::Result<T, BenchTokenError>;

pub type SharedTransactions = Arc<RwLock<VecDeque<Vec<(Transaction, u64)>>>>;

fn get_recent_blockhash(client: &ThinClient) -> (Hash, FeeCalculator) {
    loop {
        match client.get_recent_blockhash_with_commitment(CommitmentConfig::recent()) {
            Ok((blockhash, fee_calculator, _last_valid_slot)) => {
                return (blockhash, fee_calculator)
            }
            Err(err) => {
                info!("Couldn't get recent blockhash: {:?}", err);
                sleep(Duration::from_secs(1));
            }
        };
    }
}

fn wait_for_target_slots_per_epoch(target_slots_per_epoch: u64, client: &Arc<ThinClient>) {
    if target_slots_per_epoch != 0 {
        info!(
            "Waiting until epochs are {} slots long..",
            target_slots_per_epoch
        );
        loop {
            if let Ok(epoch_info) = client.get_epoch_info() {
                if epoch_info.slots_in_epoch >= target_slots_per_epoch {
                    info!("Done epoch_info: {:?}", epoch_info);
                    break;
                }
                info!(
                    "Waiting for epoch: {} now: {}",
                    target_slots_per_epoch, epoch_info.slots_in_epoch
                );
            }
            sleep(Duration::from_secs(3));
        }
    }
}

fn create_sampler_thread(
    client: &Arc<ThinClient>,
    exit_signal: &Arc<AtomicBool>,
    sample_period: u64,
    maxes: &Arc<RwLock<Vec<(String, SampleStats)>>>,
) -> JoinHandle<()> {
    info!("Sampling TPS every {} second...", sample_period);
    let exit_signal = exit_signal.clone();
    let maxes = maxes.clone();
    let client = client.clone();
    Builder::new()
        .name("solana-client-sample".to_string())
        .spawn(move || {
            sample_txs(&exit_signal, &maxes, sample_period, &client);
        })
        .unwrap()
}

fn generate_chunked_transfers(
    recent_blockhash: Arc<RwLock<Hash>>,
    shared_txs: &SharedTransactions,
    shared_tx_active_thread_count: Arc<AtomicIsize>,
    source_keypair_chunks: Vec<Vec<&Keypair>>,
    dest_keypair_chunks: &mut Vec<VecDeque<&Keypair>>,
    threads: usize,
    duration: Duration,
    sustained: bool,
) {
    // generate and send transactions for the specified duration
    let start = Instant::now();
    let keypair_chunks = source_keypair_chunks.len();
    let mut reclaim_lamports_back_to_source_account = false;
    let mut chunk_index = 0;
    while start.elapsed() < duration {
        generate_txs(
            shared_txs,
            &recent_blockhash,
            &source_keypair_chunks[chunk_index],
            &dest_keypair_chunks[chunk_index],
            threads,
            reclaim_lamports_back_to_source_account,
        );

        // In sustained mode, overlap the transfers with generation. This has higher average
        // performance but lower peak performance in tested environments.
        if sustained {
            // Ensure that we don't generate more transactions than we can handle.
            while shared_txs.read().unwrap().len() > 2 * threads {
                sleep(Duration::from_millis(1));
            }
        } else {
            while !shared_txs.read().unwrap().is_empty()
                || shared_tx_active_thread_count.load(Ordering::Relaxed) > 0
            {
                sleep(Duration::from_millis(1));
            }
        }

        // Rotate destination keypairs so that the next round of transactions will have different
        // transaction signatures even when blockhash is reused.
        dest_keypair_chunks[chunk_index].rotate_left(1);

        // Move on to next chunk
        chunk_index = (chunk_index + 1) % keypair_chunks;

        // Switch directions after transfering for each "chunk"
        if chunk_index == 0 {
            reclaim_lamports_back_to_source_account = !reclaim_lamports_back_to_source_account;
        }
    }
}

fn create_sender_threads(
    client: &Arc<ThinClient>,
    shared_txs: &SharedTransactions,
    thread_batch_sleep_ms: usize,
    total_tx_sent_count: &Arc<AtomicUsize>,
    threads: usize,
    exit_signal: &Arc<AtomicBool>,
    shared_tx_active_thread_count: &Arc<AtomicIsize>,
) -> Vec<JoinHandle<()>> {
    (0..threads)
        .map(|_| {
            let exit_signal = exit_signal.clone();
            let shared_txs = shared_txs.clone();
            let shared_tx_active_thread_count = shared_tx_active_thread_count.clone();
            let total_tx_sent_count = total_tx_sent_count.clone();
            let client = client.clone();
            Builder::new()
                .name("solana-client-sender".to_string())
                .spawn(move || {
                    do_tx_transfers(
                        &exit_signal,
                        &shared_txs,
                        &shared_tx_active_thread_count,
                        &total_tx_sent_count,
                        thread_batch_sleep_ms,
                        &client,
                    );
                })
                .unwrap()
        })
        .collect()
}

pub fn do_bench_token(client: Arc<ThinClient>, config: Config, gen_keypairs: Vec<Keypair>) -> u64 {
    let Config {
        id,
        threads,
        thread_batch_sleep_ms,
        duration,
        tx_count,
        sustained,
        target_slots_per_epoch,
        ..
    } = config;

    let mut source_keypair_chunks: Vec<Vec<&Keypair>> = Vec::new();
    let mut dest_keypair_chunks: Vec<VecDeque<&Keypair>> = Vec::new();
    assert!(gen_keypairs.len() >= 2 * tx_count);
    for chunk in gen_keypairs.chunks_exact(2 * tx_count) {
        source_keypair_chunks.push(chunk[..tx_count].iter().collect());
        dest_keypair_chunks.push(chunk[tx_count..].iter().collect());
    }

    let first_tx_count = loop {
        match client.get_transaction_count() {
            Ok(count) => break count,
            Err(err) => {
                info!("Couldn't get transaction count: {:?}", err);
                sleep(Duration::from_secs(1));
            }
        }
    };
    info!("Initial transaction count {}", first_tx_count);

    let exit_signal = Arc::new(AtomicBool::new(false));

    // Setup a thread per validator to sample every period
    // collect the max transaction rate and total tx count seen
    let maxes = Arc::new(RwLock::new(Vec::new()));
    let sample_period = 1; // in seconds
    let sample_thread = create_sampler_thread(&client, &exit_signal, sample_period, &maxes);

    let shared_txs: SharedTransactions = Arc::new(RwLock::new(VecDeque::new()));

    let recent_blockhash = Arc::new(RwLock::new(get_recent_blockhash(client.as_ref()).0));
    let shared_tx_active_thread_count = Arc::new(AtomicIsize::new(0));
    let total_tx_sent_count = Arc::new(AtomicUsize::new(0));

    let blockhash_thread = {
        let exit_signal = exit_signal.clone();
        let recent_blockhash = recent_blockhash.clone();
        let client = client.clone();
        let id = id.pubkey();
        Builder::new()
            .name("solana-blockhash-poller".to_string())
            .spawn(move || {
                poll_blockhash(&exit_signal, &recent_blockhash, &client, &id);
            })
            .unwrap()
    };

    let s_threads = create_sender_threads(
        &client,
        &shared_txs,
        thread_batch_sleep_ms,
        &total_tx_sent_count,
        threads,
        &exit_signal,
        &shared_tx_active_thread_count,
    );

    wait_for_target_slots_per_epoch(target_slots_per_epoch, &client);

    let start = Instant::now();

    generate_chunked_transfers(
        recent_blockhash,
        &shared_txs,
        shared_tx_active_thread_count,
        source_keypair_chunks,
        &mut dest_keypair_chunks,
        threads,
        duration,
        sustained,
    );

    // Stop the sampling threads so it will collect the stats
    exit_signal.store(true, Ordering::Relaxed);

    info!("Waiting for sampler threads...");
    if let Err(err) = sample_thread.join() {
        info!("  join() failed with: {:?}", err);
    }

    // join the tx send threads
    info!("Waiting for transmit threads...");
    for t in s_threads {
        if let Err(err) = t.join() {
            info!("  join() failed with: {:?}", err);
        }
    }

    info!("Waiting for blockhash thread...");
    if let Err(err) = blockhash_thread.join() {
        info!("  join() failed with: {:?}", err);
    }

    let balance = client.get_balance(&id.pubkey()).unwrap_or(0);
    metrics_submit_lamport_balance(balance);

    compute_and_report_stats(
        &maxes,
        sample_period,
        &start.elapsed(),
        total_tx_sent_count.load(Ordering::Relaxed),
    );

    let r_maxes = maxes.read().unwrap();
    r_maxes.first().unwrap().1.txs
}

fn metrics_submit_lamport_balance(lamport_balance: u64) {
    info!("Token balance: {}", lamport_balance);
    datapoint_info!(
        "bench-token-lamport_balance",
        ("balance", lamport_balance, i64)
    );
}

fn generate_system_txs(
    source: &[&Keypair],
    dest: &VecDeque<&Keypair>,
    reclaim: bool,
    blockhash: &Hash,
) -> Vec<(Transaction, u64)> {
    let pairs: Vec<_> = if !reclaim {
        source.iter().zip(dest.iter()).collect()
    } else {
        dest.iter().zip(source.iter()).collect()
    };

    pairs
        .par_iter()
        .map(|(from, to)| {
            (
                transfer_tokens_transaction(from, &to.pubkey(), 1, *blockhash),
                timestamp(),
            )
        })
        .collect()
}

fn generate_txs(
    shared_txs: &SharedTransactions,
    blockhash: &Arc<RwLock<Hash>>,
    source: &[&Keypair],
    dest: &VecDeque<&Keypair>,
    threads: usize,
    reclaim: bool,
) {
    let blockhash = *blockhash.read().unwrap();
    let tx_count = source.len();
    info!(
        "Signing transactions... {} (reclaim={}, blockhash={})",
        tx_count, reclaim, &blockhash
    );
    let signing_start = Instant::now();

    let transactions = generate_system_txs(source, dest, reclaim, &blockhash);

    let duration = signing_start.elapsed();
    let ns = duration.as_secs() * 1_000_000_000 + u64::from(duration.subsec_nanos());
    let bsps = (tx_count) as f64 / ns as f64;
    let nsps = ns as f64 / (tx_count) as f64;
    info!(
        "Done. {:.2} thousand signatures per second, {:.2} us per signature, {} ms total time, {}",
        bsps * 1_000_000_f64,
        nsps / 1_000_f64,
        duration_as_ms(&duration),
        blockhash,
    );
    datapoint_info!(
        "bench-token-generate_txs",
        ("duration", duration_as_us(&duration), i64)
    );

    let sz = transactions.len() / threads;
    let chunks: Vec<_> = transactions.chunks(sz).collect();
    {
        let mut shared_txs_wl = shared_txs.write().unwrap();
        for chunk in chunks {
            shared_txs_wl.push_back(chunk.to_vec());
        }
    }
}

fn poll_blockhash(
    exit_signal: &Arc<AtomicBool>,
    blockhash: &Arc<RwLock<Hash>>,
    client: &Arc<ThinClient>,
    id: &Pubkey,
) {
    let mut blockhash_last_updated = Instant::now();
    let mut last_error_log = Instant::now();
    loop {
        let blockhash_updated = {
            let old_blockhash = *blockhash.read().unwrap();
            if let Ok((new_blockhash, _fee)) = client.get_new_blockhash(&old_blockhash) {
                *blockhash.write().unwrap() = new_blockhash;
                blockhash_last_updated = Instant::now();
                true
            } else {
                if blockhash_last_updated.elapsed().as_secs() > 120 {
                    eprintln!("Blockhash is stuck");
                    exit(1)
                } else if blockhash_last_updated.elapsed().as_secs() > 30
                    && last_error_log.elapsed().as_secs() >= 1
                {
                    last_error_log = Instant::now();
                    error!("Blockhash is not updating");
                }
                false
            }
        };

        if blockhash_updated {
            let balance = client.get_balance(id).unwrap_or(0);
            metrics_submit_lamport_balance(balance);
        }

        if exit_signal.load(Ordering::Relaxed) {
            break;
        }

        sleep(Duration::from_millis(50));
    }
}

fn do_tx_transfers(
    exit_signal: &Arc<AtomicBool>,
    shared_txs: &SharedTransactions,
    shared_tx_thread_count: &Arc<AtomicIsize>,
    total_tx_sent_count: &Arc<AtomicUsize>,
    thread_batch_sleep_ms: usize,
    client: &Arc<ThinClient>,
) {
    loop {
        if thread_batch_sleep_ms > 0 {
            sleep(Duration::from_millis(thread_batch_sleep_ms as u64));
        }
        let txs = {
            let mut shared_txs_wl = shared_txs.write().expect("write lock in do_tx_transfers");
            shared_txs_wl.pop_front()
        };
        if let Some(txs0) = txs {
            shared_tx_thread_count.fetch_add(1, Ordering::Relaxed);
            info!(
                "Transferring 1 unit {} times... to {}",
                txs0.len(),
                client.as_ref().tpu_addr(),
            );
            let tx_len = txs0.len();
            let transfer_start = Instant::now();
            let mut old_transactions = false;
            for tx in txs0 {
                let now = timestamp();
                // Transactions that are too old will be rejected by the cluster Don't bother
                // sending them.
                if now > tx.1 && now - tx.1 > 1000 * MAX_TX_QUEUE_AGE {
                    old_transactions = true;
                    continue;
                }
                client
                    .async_send_transaction(tx.0)
                    .expect("async_send_transaction in do_tx_transfers");
            }
            if old_transactions {
                let mut shared_txs_wl = shared_txs.write().expect("write lock in do_tx_transfers");
                shared_txs_wl.clear();
            }
            shared_tx_thread_count.fetch_add(-1, Ordering::Relaxed);
            total_tx_sent_count.fetch_add(tx_len, Ordering::Relaxed);
            info!(
                "Tx send done. {} ms {} tps",
                duration_as_ms(&transfer_start.elapsed()),
                tx_len as f32 / duration_as_s(&transfer_start.elapsed()),
            );
            datapoint_info!(
                "bench-token-do_tx_transfers",
                ("duration", duration_as_us(&transfer_start.elapsed()), i64),
                ("count", tx_len, i64)
            );
        }
        if exit_signal.load(Ordering::Relaxed) {
            break;
        }
    }
}

fn verify_token_balance(client: &Arc<ThinClient>, key: &Pubkey, amount: u64) -> Option<bool> {
    match client.get_token_account_balance_with_commitment(&key, CommitmentConfig::recent()) {
        Ok(token_balance) => {
            info!(
                "verifying token balance: {} {:?} {}",
                key, token_balance, amount
            );
            return Some(token_balance.ui_amount as u64 == amount);
        }
        Err(err) => error!("failed to get token balance {:?}", err),
    }
    None
}

fn verify_balance(client: &Arc<ThinClient>, key: &Pubkey, amount: u64) -> Option<bool> {
    match client.get_balance_with_commitment(key, CommitmentConfig::recent()) {
        Ok(balance) => {
            info!("verifying balance: {} {} {}", key, balance, amount);
            return Some(balance >= amount);
        }
        Err(err) => error!("failed to get balance {:?}", err),
    }
    None
}

fn verify_funding_transfer(client: &Arc<ThinClient>, tx: &Transaction, amount: u64) -> bool {
    for a in &tx.message().account_keys[1..] {
        if let Some(res) = verify_balance(client, a, amount) {
            return res;
        }
    }
    false
}

trait FundingTransactions<'a> {
    fn fund(
        &mut self,
        client: &Arc<ThinClient>,
        to_fund: &[(&'a Keypair, Vec<(&'a Keypair, u64)>)],
        to_lamports: u64,
        mint_pubkey: &Pubkey,
        minimum_balance_for_rent_exemption: u64,
    );
    fn make(
        &mut self,
        to_fund: &[(&'a Keypair, Vec<(&'a Keypair, u64)>)],
        mint_pubkey: &Pubkey,
        minimum_balance_for_rent_exemption: u64,
    );
    fn sign(&mut self, blockhash: Hash);
    fn send<T: Client>(&self, client: &Arc<T>);
    fn verify(&mut self, client: &Arc<ThinClient>, to_lamports: u64);
}

impl<'a> FundingTransactions<'a> for Vec<(&'a Keypair, &'a Keypair, Transaction)> {
    fn fund(
        &mut self,
        client: &Arc<ThinClient>,
        to_fund: &[(&'a Keypair, Vec<(&'a Keypair, u64)>)],
        to_lamports: u64,
        mint_pubkey: &Pubkey,
        minimum_balance_for_rent_exemption: u64,
    ) {
        self.make(to_fund, mint_pubkey, minimum_balance_for_rent_exemption);

        let mut tries = 0;
        while !self.is_empty() {
            info!(
                "{} {} each to {} accounts in {} txs",
                if tries == 0 {
                    "transferring"
                } else {
                    " retrying"
                },
                to_lamports,
                self.len() * MAX_SPENDS_PER_TX as usize,
                self.len(),
            );

            let (blockhash, _fee_calculator) = get_recent_blockhash(client.as_ref());

            // re-sign retained to_fund_txes with updated blockhash
            self.sign(blockhash);
            self.send(&client);

            // Sleep a few slots to allow transactions to process
            sleep(Duration::from_secs(1));

            self.verify(&client, to_lamports - minimum_balance_for_rent_exemption);

            // retry anything that seems to have dropped through cracks
            //  again since these txs are all or nothing, they're fine to
            //  retry
            tries += 1;
        }
        info!("transferred");
    }

    fn make(
        &mut self,
        to_fund: &[(&'a Keypair, Vec<(&'a Keypair, u64)>)],
        mint_pubkey: &Pubkey,
        minimum_balance_for_rent_exemption: u64,
    ) {
        let mut make_txs = Measure::start("make_txs");
        let to_fund_txs: Vec<(&Keypair, &Keypair, Transaction)> = to_fund
            .par_iter()
            .flat_map(|(k, t)| {
                t.iter()
                    .map(|(new_keypair, num_lamports)| {
                        (
                            *k,
                            *new_keypair,
                            create_system_and_token_account_tx(
                                &k.pubkey(),
                                mint_pubkey,
                                &new_keypair.pubkey(),
                                *num_lamports,
                                minimum_balance_for_rent_exemption,
                            ),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        make_txs.stop();
        debug!(
            "make {} unsigned txs: {}us",
            to_fund_txs.len(),
            make_txs.as_us()
        );
        self.extend(to_fund_txs);
    }

    fn sign(&mut self, blockhash: Hash) {
        let mut sign_txs = Measure::start("sign_txs");
        self.par_iter_mut().for_each(|(k, new_keypair, tx)| {
            tx.sign(&[*k, new_keypair], blockhash);
        });
        sign_txs.stop();
        debug!("sign {} txs: {}us", self.len(), sign_txs.as_us());
    }

    fn send<T: Client>(&self, client: &Arc<T>) {
        let mut send_txs = Measure::start("send_txs");
        self.iter().for_each(|(_, _, tx)| {
            client.async_send_transaction(tx.clone()).expect("transfer");
        });
        send_txs.stop();
        debug!("send {} txs: {}us", self.len(), send_txs.as_us());
    }

    fn verify(&mut self, client: &Arc<ThinClient>, to_lamports: u64) {
        let starting_txs = self.len();
        let verified_txs = Arc::new(AtomicUsize::new(0));
        let too_many_failures = Arc::new(AtomicBool::new(false));
        let loops = if starting_txs < 1000 { 3 } else { 1 };
        // Only loop multiple times for small (quick) transaction batches
        let time = Arc::new(Mutex::new(Instant::now()));
        for _ in 0..loops {
            let time = time.clone();
            let failed_verify = Arc::new(AtomicUsize::new(0));
            let client = client.clone();
            let verified_txs = &verified_txs;
            let failed_verify = &failed_verify;
            let too_many_failures = &too_many_failures;
            let verified_set: HashSet<Pubkey> = self
                .par_iter()
                .filter_map(move |(k, _, tx)| {
                    if too_many_failures.load(Ordering::Relaxed) {
                        return None;
                    }

                    let verified = if verify_funding_transfer(&client, &tx, to_lamports) {
                        verified_txs.fetch_add(1, Ordering::Relaxed);
                        Some(k.pubkey())
                    } else {
                        failed_verify.fetch_add(1, Ordering::Relaxed);
                        None
                    };

                    let verified_txs = verified_txs.load(Ordering::Relaxed);
                    let failed_verify = failed_verify.load(Ordering::Relaxed);
                    let remaining_count = starting_txs.saturating_sub(verified_txs + failed_verify);
                    if failed_verify > 100 && failed_verify > verified_txs {
                        too_many_failures.store(true, Ordering::Relaxed);
                        warn!(
                            "Too many failed transfers... {} remaining, {} verified, {} failures",
                            remaining_count, verified_txs, failed_verify
                        );
                    }
                    if remaining_count > 0 {
                        let mut time_l = time.lock().unwrap();
                        if time_l.elapsed().as_secs() > 2 {
                            info!(
                                "Verifying transfers... {} remaining, {} verified, {} failures",
                                remaining_count, verified_txs, failed_verify
                            );
                            *time_l = Instant::now();
                        }
                    }

                    verified
                })
                .collect();

            self.retain(|(k, _, _)| !verified_set.contains(&k.pubkey()));
            if self.is_empty() {
                break;
            }
            info!("Looping verifications");

            let verified_txs = verified_txs.load(Ordering::Relaxed);
            let failed_verify = failed_verify.load(Ordering::Relaxed);
            let remaining_count = starting_txs.saturating_sub(verified_txs + failed_verify);
            info!(
                "Verifying transfers... {} remaining, {} verified, {} failures",
                remaining_count, verified_txs, failed_verify
            );
            sleep(Duration::from_millis(100));
        }
    }
}

/// fund the dests keys by spending all of the source keys into MAX_SPENDS_PER_TX
/// on every iteration.  This allows us to replay the transfers because the source is either empty,
/// or full
pub fn fund_keys(
    client: Arc<ThinClient>,
    source: &Keypair,
    dests: &[Keypair],
    total: u64,
    max_fee: u64,
    lamports_per_account: u64,
    mint_pubkey: &Pubkey,
    minimum_balance_for_rent_exemption: u64,
) {
    let mut funded: Vec<&Keypair> = vec![source];
    let mut funded_funds = total;
    let mut not_funded: Vec<&Keypair> = dests.iter().collect();
    while !not_funded.is_empty() {
        // Build to fund list and prepare funding sources for next iteration
        let mut new_funded: Vec<&Keypair> = vec![];
        let mut to_fund: Vec<(&Keypair, Vec<(&Keypair, u64)>)> = vec![];
        let to_lamports = (funded_funds - lamports_per_account - max_fee) / MAX_SPENDS_PER_TX;
        for f in funded {
            let start = not_funded.len() - MAX_SPENDS_PER_TX as usize;
            let dests: Vec<_> = not_funded.drain(start..).collect();
            let spends: Vec<_> = dests.iter().map(|k| (*k, to_lamports)).collect();
            to_fund.push((f, spends));
            new_funded.extend(dests.into_iter());
        }

        // try to transfer a "few" at a time with recent blockhassh
        //  assume 4MB network buffers, and 512 byte packets
        const FUND_CHUNK_LEN: usize = 4 * 1024 * 1024 / 512;

        to_fund.chunks(FUND_CHUNK_LEN).for_each(|chunk| {
            Vec::<(&Keypair, &Keypair, Transaction)>::with_capacity(chunk.len()).fund(
                &client,
                chunk,
                to_lamports,
                mint_pubkey,
                minimum_balance_for_rent_exemption,
            );
        });

        info!("funded: {} left: {}", new_funded.len(), not_funded.len());
        funded = new_funded;
        funded_funds = to_lamports;
    }
}

pub fn airdrop_lamports(
    client: &ThinClient,
    faucet_addr: &SocketAddr,
    id: &Keypair,
    desired_balance: u64,
) -> Result<()> {
    let starting_balance = client.get_balance(&id.pubkey()).unwrap_or(0);
    metrics_submit_lamport_balance(starting_balance);
    info!("starting balance {}", starting_balance);

    if starting_balance < desired_balance {
        let airdrop_amount = desired_balance - starting_balance;
        info!(
            "Airdropping {:?} lamports from {} for {}",
            airdrop_amount,
            faucet_addr,
            id.pubkey(),
        );

        let (blockhash, _fee_calculator) = get_recent_blockhash(client);
        match request_airdrop_transaction(&faucet_addr, &id.pubkey(), airdrop_amount, blockhash) {
            Ok(transaction) => {
                let mut tries = 0;
                loop {
                    tries += 1;
                    let signature = client.async_send_transaction(transaction.clone()).unwrap();
                    let result = client.poll_for_signature_confirmation(&signature, 1);

                    if result.is_ok() {
                        break;
                    }
                    if tries >= 5 {
                        panic!(
                            "Error requesting airdrop: to addr: {:?} amount: {} {:?}",
                            faucet_addr, airdrop_amount, result
                        )
                    }
                }
            }
            Err(err) => {
                panic!(
                    "Error requesting airdrop: {:?} to addr: {:?} amount: {}",
                    err, faucet_addr, airdrop_amount
                );
            }
        };

        let current_balance = client
            .get_balance_with_commitment(&id.pubkey(), CommitmentConfig::recent())
            .unwrap_or_else(|e| {
                info!("airdrop error {}", e);
                starting_balance
            });
        info!("current balance {}...", current_balance);

        metrics_submit_lamport_balance(current_balance);
        if current_balance - starting_balance != airdrop_amount {
            info!(
                "Airdrop failed! {} {} {}",
                id.pubkey(),
                current_balance,
                starting_balance
            );
            return Err(BenchTokenError::AirdropFailure);
        }
    }
    Ok(())
}

fn compute_and_report_stats(
    maxes: &Arc<RwLock<Vec<(String, SampleStats)>>>,
    sample_period: u64,
    tx_send_elapsed: &Duration,
    total_tx_send_count: usize,
) {
    // Compute/report stats
    let mut max_of_maxes = 0.0;
    let mut max_tx_count = 0;
    let mut nodes_with_zero_tps = 0;
    let mut total_maxes = 0.0;
    info!(" Node address        |       Max TPS | Total Transactions");
    info!("---------------------+---------------+--------------------");

    for (sock, stats) in maxes.read().unwrap().iter() {
        let maybe_flag = match stats.txs {
            0 => "!!!!!",
            _ => "",
        };

        info!(
            "{:20} | {:13.2} | {} {}",
            sock, stats.tps, stats.txs, maybe_flag
        );

        if stats.tps == 0.0 {
            nodes_with_zero_tps += 1;
        }
        total_maxes += stats.tps;

        if stats.tps > max_of_maxes {
            max_of_maxes = stats.tps;
        }
        if stats.txs > max_tx_count {
            max_tx_count = stats.txs;
        }
    }

    if total_maxes > 0.0 {
        let num_nodes_with_tps = maxes.read().unwrap().len() - nodes_with_zero_tps;
        let average_max = total_maxes / num_nodes_with_tps as f32;
        info!(
            "\nAverage max TPS: {:.2}, {} nodes had 0 TPS",
            average_max, nodes_with_zero_tps
        );
    }

    let total_tx_send_count = total_tx_send_count as u64;
    let drop_rate = if total_tx_send_count > max_tx_count {
        (total_tx_send_count - max_tx_count) as f64 / total_tx_send_count as f64
    } else {
        0.0
    };
    info!(
        "\nHighest TPS: {:.2} sampling period {}s max transactions: {} clients: {} drop rate: {:.2}",
        max_of_maxes,
        sample_period,
        max_tx_count,
        maxes.read().unwrap().len(),
        drop_rate,
    );
    info!(
        "\tAverage TPS: {}",
        max_tx_count as f32 / duration_as_s(tx_send_elapsed)
    );
}

pub fn generate_keypairs(seed_keypair: &Keypair, count: u64) -> (Vec<Keypair>, u64) {
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_keypair.to_bytes()[..32]);
    let mut rnd = GenKeys::new(seed);

    let mut total_keys = 0;
    let mut extra = 0; // This variable tracks the number of keypairs needing extra transaction fees funded
    let mut delta = 1;
    while total_keys < count {
        extra += delta;
        delta *= MAX_SPENDS_PER_TX;
        total_keys += delta;
    }
    (rnd.gen_n_keypairs(total_keys), extra)
}

pub fn generate_and_fund_keypairs(
    client: Arc<ThinClient>,
    faucet_addr: Option<SocketAddr>,
    funding_key: &Keypair,
    keypair_count: usize,
    lamports_per_account: u64,
) -> Result<Vec<Keypair>> {
    info!("Creating {} keypairs...", keypair_count);
    let (mut keypairs, extra) = generate_keypairs(funding_key, keypair_count as u64);
    info!("Get lamports...");

    // Sample the first keypair, to prevent lamport loss on repeated solana-bench-token executions
    let first_key = keypairs[0].pubkey();
    let first_keypair_balance = client.get_balance(&first_key).unwrap_or(0);

    // Sample the last keypair, to check if funding was already completed
    let last_key = keypairs[keypair_count - 1].pubkey();
    let last_keypair_balance = client.get_balance(&last_key).unwrap_or(0);

    // Repeated runs will eat up keypair balances from transaction fees. In order to quickly
    //   start another bench-token run without re-funding all of the keypairs, check if the
    //   keypairs still have at least 80% of the expected funds. That should be enough to
    //   pay for the transaction fees in a new run.
    let enough_lamports = 8 * lamports_per_account / 10;
    if first_keypair_balance < enough_lamports || last_keypair_balance < enough_lamports {
        let mint_minimum_balance_for_rent_exemption = client
            .get_minimum_balance_for_rent_exemption(size_of::<Mint>())
            .unwrap();
        let token_account_minimum_balance_for_rent_exemption = client
            .get_minimum_balance_for_rent_exemption(size_of::<Account>())
            .unwrap();

        info!(
            "minimum for mint: {} account: {}",
            mint_minimum_balance_for_rent_exemption,
            token_account_minimum_balance_for_rent_exemption
        );

        let fee_rate_governor = client.get_fee_rate_governor().unwrap();
        let max_fee = fee_rate_governor.max_lamports_per_signature;
        let extra_fees = extra * max_fee * 2;
        let total_keypairs = keypairs.len() as u64 + 1; // Add one for funding keypair
        let total = (lamports_per_account + token_account_minimum_balance_for_rent_exemption)
            * total_keypairs
            + extra_fees
            + mint_minimum_balance_for_rent_exemption;

        let funding_key_balance = client.get_balance(&funding_key.pubkey()).unwrap_or(0);
        info!(
            "Funding keypair balance: {} max_fee: {} lamports_per_account: {} extra: {} total: {}",
            funding_key_balance, max_fee, lamports_per_account, extra, total
        );

        if client.get_balance(&funding_key.pubkey()).unwrap_or(0) < total {
            airdrop_lamports(client.as_ref(), &faucet_addr.unwrap(), funding_key, total)?;
        }

        // Make a token account for holding all the tokens
        let new_mint_keypair = Keypair::new();
        let mut create_account_tx = create_token_account_transaction(
            &funding_key,
            &new_mint_keypair,
            token_account_minimum_balance_for_rent_exemption,
        );
        info!("Initializing first token account");
        client
            .send_and_confirm_transaction(&[funding_key], &mut create_account_tx, 5, 0)
            .unwrap();
        if !verify_balance(
            &client,
            &token_account_from_system_account(&funding_key.pubkey()),
            token_account_minimum_balance_for_rent_exemption,
        )
        .unwrap()
        {
            panic!("Could not create first token account");
        }
        info!("First token account successfully created!");

        // Deploy new token mint, deposit tokens into token account with key == funding_key
        info!("Creating mint");
        let mut create_token_tx = create_token_transaction(
            &*client,
            &new_mint_keypair,
            &token_account_from_system_account(&funding_key.pubkey()),
            &funding_key,
            mint_minimum_balance_for_rent_exemption,
            total,
            NUM_DECIMALS,
        )
        .unwrap()
        .unwrap();
        info!("Sending create mint transaction");
        client
            .send_and_confirm_transaction(&[funding_key], &mut create_token_tx, 5, 0)
            .unwrap();
        if !verify_balance(
            &client,
            &new_mint_keypair.pubkey(),
            mint_minimum_balance_for_rent_exemption,
        )
        .unwrap()
        {
            panic!("Could not create mint");
        }

        info!("New token mint successfully created!");

        if !verify_token_balance(
            &client,
            &token_account_from_system_account(&funding_key.pubkey()),
            total,
        )
        .unwrap()
        {
            panic!("Mint issued wrong balance");
        }
        info!("Issued token balance verified!");

        fund_keys(
            client,
            funding_key,
            &keypairs,
            total,
            max_fee,
            lamports_per_account,
            &new_mint_keypair.pubkey(),
            token_account_minimum_balance_for_rent_exemption,
        );
    }

    // 'generate_keypairs' generates extra keys to be able to have size-aligned funding batches for fund_keys.
    keypairs.truncate(keypair_count);

    Ok(keypairs)
}

type Error = Box<dyn std::error::Error>;
type CommmandResult = std::result::Result<Option<Transaction>, Error>;

// A helper function to convert spl_token_v1_0::id() as spl_sdk::pubkey::Pubkey to
// solana_sdk::pubkey::Pubkey
pub fn to_this_pubkey(pubkey: &spl_token_v1_0::solana_sdk::pubkey::Pubkey) -> Pubkey {
    Pubkey::from_str(&pubkey.to_string()).unwrap()
}

fn initialize_mint(
    token_program_id: &Pubkey,
    mint_pubkey: &Pubkey,
    account_pubkey: Option<&Pubkey>,
    amount: u64,
    decimals: u8,
) -> std::result::Result<Instruction, ProgramError> {
    let data = TokenInstruction::InitializeMint { amount, decimals }
        .pack()
        .unwrap();

    let mut accounts = vec![AccountMeta::new(*mint_pubkey, false)];
    if amount != 0 {
        match account_pubkey {
            Some(pubkey) => accounts.push(AccountMeta::new(*pubkey, false)),
            None => {
                return Err(ProgramError::NotEnoughAccountKeys);
            }
        }
    }

    Ok(Instruction {
        program_id: *token_program_id,
        accounts,
        data,
    })
}

fn create_token_transaction<'a>(
    client: &ThinClient,
    new_mint: &'a Keypair,
    owner: &'a Pubkey,
    fee_payer: &'a Keypair,
    num_lamports: u64,
    num_tokens: u64,
    decimals: u8,
) -> CommmandResult {
    info!(
        "Creating token mint {}, owner: {}, fee_payer: {}",
        new_mint.pubkey(),
        owner,
        fee_payer.pubkey()
    );

    let mut transaction = Transaction::new_with_payer(
        &[
            system_instruction::create_account(
                &fee_payer.pubkey(),
                &new_mint.pubkey(),
                num_lamports,
                size_of::<Mint>() as u64,
                &to_this_pubkey(&spl_token_v1_0::id()),
            ),
            initialize_mint(
                &to_this_pubkey(&spl_token_v1_0::id()),
                &new_mint.pubkey(),
                // Deposit all the tokens with the owner
                Some(owner),
                num_tokens,
                decimals,
            )
            .unwrap(),
        ],
        Some(&fee_payer.pubkey()),
    );

    let (recent_blockhash, _) = client.get_recent_blockhash()?;
    transaction.sign(&[fee_payer, new_mint], recent_blockhash);
    Ok(Some(transaction))
}

fn transfer_tokens_ix(
    source_system_account: &Pubkey,
    destination_system_account: &Pubkey,
    amount: u64,
) -> Instruction {
    let data = TokenInstruction::Transfer { amount }.pack().unwrap();

    let mut accounts = Vec::with_capacity(3);
    accounts.push(AccountMeta::new(
        token_account_from_system_account(source_system_account),
        false,
    ));
    accounts.push(AccountMeta::new(
        token_account_from_system_account(destination_system_account),
        false,
    ));
    accounts.push(AccountMeta::new_readonly(*source_system_account, true));

    Instruction {
        program_id: to_this_pubkey(&spl_token_v1_0::id()),
        accounts,
        data,
    }
}

fn transfer_tokens_transaction(
    source_system_keypair: &Keypair,
    destination_system_pubkey: &Pubkey,
    amount: u64,
    recent_blockhash: Hash,
) -> Transaction {
    let message = Message::new(
        &[transfer_tokens_ix(
            &source_system_keypair.pubkey(),
            destination_system_pubkey,
            amount,
        )],
        Some(&source_system_keypair.pubkey()),
    );
    Transaction::new(&[source_system_keypair], message, recent_blockhash)
}

fn initialize_token_account_ix(
    token_program_id: &Pubkey,
    account_pubkey: &Pubkey,
    mint_pubkey: &Pubkey,
    owner_pubkey: &Pubkey,
) -> std::result::Result<Instruction, ProgramError> {
    let data = TokenInstruction::InitializeAccount.pack().unwrap();

    let accounts = vec![
        AccountMeta::new(*account_pubkey, false),
        AccountMeta::new_readonly(*mint_pubkey, false),
        AccountMeta::new_readonly(*owner_pubkey, false),
    ];

    Ok(Instruction {
        program_id: *token_program_id,
        accounts,
        data,
    })
}

fn create_token_account_ix(
    // fee payer system account
    fee_payer: &Pubkey,
    mint_pubkey: &Pubkey,
    minimum_balance_for_rent_exemption: u64,
) -> Vec<Instruction> {
    println!("Creating token account {}", fee_payer);
    let spl_token_id = to_this_pubkey(&spl_token_v1_0::id());
    let seed = "token";
    let derived_key = token_account_from_system_account(fee_payer);
    vec![
        system_instruction::create_account_with_seed(
            fee_payer,
            &derived_key, // must match create_address_with_seed(base, seed, program_id)
            fee_payer,
            &seed,
            minimum_balance_for_rent_exemption,
            size_of::<Account>() as u64,
            &spl_token_id,
        ),
        initialize_token_account_ix(&spl_token_id, &derived_key, &mint_pubkey, &fee_payer).unwrap(),
    ]
}

fn create_token_account_transaction<'a>(
    fee_payer_keypair: &'a Keypair,
    mint: &'a Keypair,
    minimum_balance_for_rent_exemption: u64,
) -> Transaction {
    let instructions = create_token_account_ix(
        &fee_payer_keypair.pubkey(),
        &mint.pubkey(),
        minimum_balance_for_rent_exemption,
    );
    let message = Message::new(&instructions, Some(&fee_payer_keypair.pubkey()));
    Transaction::new_unsigned(message)
}

fn create_system_and_token_account_tx(
    fee_payer: &Pubkey,
    mint_pubkey: &Pubkey,
    new_account_pubkey: &Pubkey,
    num_lamports: u64,
    minimum_balance_for_rent_exemption: u64,
) -> Transaction {
    assert!(num_lamports > minimum_balance_for_rent_exemption);
    let mut ixs = vec![
        // Create a system account with `num_lamports`
        system_instruction::transfer(fee_payer, new_account_pubkey, num_lamports),
    ];
    ixs.extend(
        // Now make a new token account, paid for and owned by `new_account_pubkey`
        create_token_account_ix(
            new_account_pubkey,
            mint_pubkey,
            minimum_balance_for_rent_exemption,
        ),
    );
    ixs.push(transfer_tokens_ix(
        &fee_payer,
        &new_account_pubkey,
        num_lamports,
    ));
    let message = Message::new(&ixs, Some(&fee_payer));
    Transaction::new_unsigned(message)
}

fn token_account_from_system_account(system_account: &Pubkey) -> Pubkey {
    let seed = "token";
    let spl_token_id = to_this_pubkey(&spl_token_v1_0::id());
    Pubkey::create_with_seed(system_account, seed, &spl_token_id).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_runtime::bank::Bank;
    use solana_runtime::bank_client::BankClient;
    use solana_sdk::client::SyncClient;
    use solana_sdk::fee_calculator::FeeRateGovernor;
    use solana_sdk::genesis_config::create_genesis_config;

    #[test]
    fn test_create_token() {
        let (genesis_config, genesis_keypair) = create_genesis_config(10_000);
        let bank = Bank::new(&genesis_config);
        let client = BankClient::new(bank);
        create_token_transaction(
            &client,
            &Keypair::new(),
            &Keypair::new().pubkey(),
            &genesis_keypair,
            10,
            10000,
            NUM_DECIMALS,
        )
        .unwrap();
    }

    #[test]
    fn test_bench_token_bank_client() {
        let (genesis_config, id) = create_genesis_config(10_000);
        let bank = Bank::new(&genesis_config);
        let client = Arc::new(BankClient::new(bank));

        let mut config = Config::default();
        config.id = id;
        config.tx_count = 10;
        config.duration = Duration::from_secs(5);

        let keypair_count = config.tx_count * config.keypair_multiplier;
        let keypairs =
            generate_and_fund_keypairs(client.clone(), None, &config.id, keypair_count, 20)
                .unwrap();

        do_bench_token(client, config, keypairs);
    }

    #[test]
    fn test_bench_token_fund_keys() {
        let (genesis_config, id) = create_genesis_config(10_000);
        let bank = Bank::new(&genesis_config);
        let client = Arc::new(BankClient::new(bank));
        let keypair_count = 20;
        let lamports = 20;

        let keypairs =
            generate_and_fund_keypairs(client.clone(), None, &id, keypair_count, lamports).unwrap();

        for kp in &keypairs {
            assert_eq!(
                client
                    .get_balance_with_commitment(&kp.pubkey(), CommitmentConfig::recent())
                    .unwrap(),
                lamports
            );
        }
    }

    #[test]
    fn test_bench_token_fund_keys_with_fees() {
        let (mut genesis_config, id) = create_genesis_config(10_000);
        let fee_rate_governor = FeeRateGovernor::new(11, 0);
        genesis_config.fee_rate_governor = fee_rate_governor;
        let bank = Bank::new(&genesis_config);
        let client = Arc::new(BankClient::new(bank));
        let keypair_count = 20;
        let lamports = 20;

        let keypairs =
            generate_and_fund_keypairs(client.clone(), None, &id, keypair_count, lamports).unwrap();

        for kp in &keypairs {
            assert_eq!(client.get_balance(&kp.pubkey()).unwrap(), lamports);
        }
    }
}
