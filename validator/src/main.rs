use bzip2::bufread::BzDecoder;
use clap::{
    crate_description, crate_name, value_t, value_t_or_exit, values_t_or_exit, App, Arg, ArgMatches,
};
use console::{style, Emoji};
use indicatif::{ProgressBar, ProgressStyle};
use log::*;
use rand::{thread_rng, Rng};
use solana_clap_utils::{
    input_parsers::pubkey_of,
    input_validators::{is_keypair, is_pubkey_or_keypair},
    keypair::{
        self, keypair_input, KeypairWithSource, ASK_SEED_PHRASE_ARG,
        SKIP_SEED_PHRASE_VALIDATION_ARG,
    },
};
use solana_client::rpc_client::RpcClient;
use solana_core::ledger_cleanup_service::DEFAULT_MAX_LEDGER_SLOTS;
use solana_core::{
    cluster_info::{ClusterInfo, Node, VALIDATOR_PORT_RANGE},
    contact_info::ContactInfo,
    gossip_service::GossipService,
    rpc::JsonRpcConfig,
    validator::{Validator, ValidatorConfig},
};
use solana_ledger::bank_forks::SnapshotConfig;
use solana_perf::recycler::enable_recycler_warming;
use solana_sdk::{
    clock::Slot,
    hash::Hash,
    pubkey::Pubkey,
    signature::{Keypair, KeypairUtil},
};
use std::{
    fs::{self, File},
    io::{self, Read},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::sleep,
    time::{Duration, Instant},
};

fn port_validator(port: String) -> Result<(), String> {
    port.parse::<u16>()
        .map(|_| ())
        .map_err(|e| format!("{:?}", e))
}

fn port_range_validator(port_range: String) -> Result<(), String> {
    if solana_net_utils::parse_port_range(&port_range).is_some() {
        Ok(())
    } else {
        Err("Invalid port range".to_string())
    }
}

fn hash_validator(hash: String) -> Result<(), String> {
    Hash::from_str(&hash)
        .map(|_| ())
        .map_err(|e| format!("{:?}", e))
}

static TRUCK: Emoji = Emoji("🚚 ", "");
static SPARKLE: Emoji = Emoji("✨ ", "");

/// Creates a new process bar for processing that will take an unknown amount of time
fn new_spinner_progress_bar() -> ProgressBar {
    let progress_bar = ProgressBar::new(42);
    progress_bar
        .set_style(ProgressStyle::default_spinner().template("{spinner:.green} {wide_msg}"));
    progress_bar.enable_steady_tick(100);
    progress_bar
}

fn download_tar_bz2(
    rpc_addr: &SocketAddr,
    archive_name: &str,
    download_path: &Path,
    is_snapshot: bool,
) -> Result<(), String> {
    let archive_path = download_path.join(archive_name);
    if archive_path.is_file() {
        return Ok(());
    }
    fs::create_dir_all(download_path).map_err(|err| err.to_string())?;

    let temp_archive_path = {
        let mut p = archive_path.clone();
        p.set_extension(".tmp");
        p
    };

    let url = format!("http://{}/{}", rpc_addr, archive_name);
    let download_start = Instant::now();

    let progress_bar = new_spinner_progress_bar();
    progress_bar.set_message(&format!("{}Downloading {}...", TRUCK, url));

    let client = reqwest::Client::new();
    let response = client
        .get(url.as_str())
        .send()
        .map_err(|err| err.to_string())?;

    if is_snapshot && response.status() == reqwest::StatusCode::NOT_FOUND {
        progress_bar.finish_and_clear();
        warn!("Snapshot not found at {}", url);
        return Ok(());
    }

    let response = response
        .error_for_status()
        .map_err(|err| format!("Unable to get: {:?}", err))?;
    let download_size = {
        response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|content_length| content_length.to_str().ok())
            .and_then(|content_length| content_length.parse().ok())
            .unwrap_or(0)
    };
    progress_bar.set_length(download_size);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template(&format!(
                "{}{}Downloading {} {}",
                "{spinner:.green} ",
                TRUCK,
                url,
                "[{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})"
            ))
            .progress_chars("=> "),
    );

    struct DownloadProgress<R> {
        progress_bar: ProgressBar,
        response: R,
    }

    impl<R: Read> Read for DownloadProgress<R> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.response.read(buf).map(|n| {
                self.progress_bar.inc(n as u64);
                n
            })
        }
    }

    let mut source = DownloadProgress {
        progress_bar,
        response,
    };

    let mut file = File::create(&temp_archive_path)
        .map_err(|err| format!("Unable to create {:?}: {:?}", temp_archive_path, err))?;
    std::io::copy(&mut source, &mut file)
        .map_err(|err| format!("Unable to write {:?}: {:?}", temp_archive_path, err))?;

    source.progress_bar.finish_and_clear();
    info!(
        "  {}{}",
        SPARKLE,
        format!(
            "Downloaded {} ({} bytes) in {:?}",
            url,
            download_size,
            Instant::now().duration_since(download_start),
        )
    );

    if !is_snapshot {
        info!("Extracting {:?}...", archive_path);
        let extract_start = Instant::now();
        let tar_bz2 = File::open(&temp_archive_path)
            .map_err(|err| format!("Unable to open {}: {:?}", archive_name, err))?;
        let tar = BzDecoder::new(std::io::BufReader::new(tar_bz2));
        let mut archive = tar::Archive::new(tar);
        archive
            .unpack(download_path)
            .map_err(|err| format!("Unable to unpack {}: {:?}", archive_name, err))?;
        info!(
            "Extracted {} in {:?}",
            archive_name,
            Instant::now().duration_since(extract_start)
        );
    }
    std::fs::rename(temp_archive_path, archive_path)
        .map_err(|err| format!("Unable to rename: {:?}", err))?;

    Ok(())
}

fn get_rpc_addr(
    node: &Node,
    identity_keypair: &Arc<Keypair>,
    entrypoint_gossip: &SocketAddr,
    expected_shred_version: Option<u16>,
) -> (RpcClient, SocketAddr) {
    let mut cluster_info = ClusterInfo::new(
        ClusterInfo::spy_contact_info(&identity_keypair.pubkey()),
        identity_keypair.clone(),
    );
    cluster_info.set_entrypoint(ContactInfo::new_gossip_entry_point(entrypoint_gossip));
    let cluster_info = Arc::new(RwLock::new(cluster_info));

    let gossip_exit_flag = Arc::new(AtomicBool::new(false));
    let gossip_service = GossipService::new(
        &cluster_info.clone(),
        None,
        None,
        node.sockets.gossip.try_clone().unwrap(),
        &gossip_exit_flag,
    );

    let (rpc_client, rpc_addr) = loop {
        info!(
            "Searching for RPC service, shred version={:?}...\n{}",
            expected_shred_version,
            cluster_info.read().unwrap().contact_info_trace()
        );

        let mut rpc_peers = cluster_info.read().unwrap().all_rpc_peers();

        let shred_version_required = !rpc_peers
            .iter()
            .all(|contact_info| contact_info.shred_version == rpc_peers[0].shred_version);

        if let Some(expected_shred_version) = expected_shred_version {
            // Filter out rpc peers that don't match the expected shred version
            rpc_peers = rpc_peers
                .into_iter()
                .filter(|contact_info| contact_info.shred_version == expected_shred_version)
                .collect::<Vec<_>>();
        }

        if !rpc_peers.is_empty() {
            // Prefer the entrypoint's RPC service if present, otherwise pick a node at random
            let contact_info = if let Some(contact_info) = rpc_peers
                .iter()
                .find(|contact_info| contact_info.gossip == *entrypoint_gossip)
            {
                Some(contact_info.clone())
            } else if shred_version_required && expected_shred_version.is_none() {
                // Require the user supply a shred version if there are conflicting shred version in
                // gossip to reduce the chance of human error
                warn!("Multiple shred versions in gossip.  Restart with --expected-shred-version");
                None
            } else {
                // Pick a node at random
                Some(rpc_peers[thread_rng().gen_range(0, rpc_peers.len())].clone())
            };

            if let Some(ContactInfo { id, rpc, .. }) = contact_info {
                info!("Contacting RPC port of node {}: {:?}", id, rpc);
                let rpc_client = RpcClient::new_socket(rpc);
                match rpc_client.get_version() {
                    Ok(rpc_version) => {
                        info!("RPC node version: {}", rpc_version.solana_core);
                        break (rpc_client, rpc);
                    }
                    Err(err) => {
                        warn!("Failed to get RPC version: {}", err);
                    }
                }
            }
        } else {
            info!("No RPC service found");
        }

        sleep(Duration::from_secs(1));
    };

    gossip_exit_flag.store(true, Ordering::Relaxed);
    gossip_service.join().unwrap();

    (rpc_client, rpc_addr)
}

fn check_vote_account(
    rpc_client: &RpcClient,
    vote_pubkey: &Pubkey,
    voting_pubkey: &Pubkey,
    node_pubkey: &Pubkey,
) -> Result<(), String> {
    let found_vote_account = rpc_client
        .get_account(vote_pubkey)
        .map_err(|err| format!("Failed to get vote account: {}", err.to_string()))?;

    if found_vote_account.owner != solana_vote_program::id() {
        return Err(format!(
            "not a vote account (owned by {}): {}",
            found_vote_account.owner, vote_pubkey
        ));
    }

    let found_node_account = rpc_client
        .get_account(node_pubkey)
        .map_err(|err| format!("Failed to get identity account: {}", err.to_string()))?;

    let found_vote_account = solana_vote_program::vote_state::VoteState::from(&found_vote_account);
    if let Some(found_vote_account) = found_vote_account {
        if found_vote_account.authorized_voter != *voting_pubkey {
            return Err(format!(
                "account's authorized voter ({}) does not match to the given voting keypair ({}).",
                found_vote_account.authorized_voter, voting_pubkey
            ));
        }
        if found_vote_account.node_pubkey != *node_pubkey {
            return Err(format!(
                "account's node pubkey ({}) does not match to the given identity keypair ({}).",
                found_vote_account.node_pubkey, node_pubkey
            ));
        }
    } else {
        return Err(format!("invalid vote account data: {}", vote_pubkey));
    }

    // Maybe we can calculate minimum voting fee; rather than 1 lamport
    if found_node_account.lamports <= 1 {
        return Err(format!(
            "unfunded identity account ({}): only {} lamports (needs more fund to vote)",
            node_pubkey, found_node_account.lamports
        ));
    }

    Ok(())
}

fn download_ledger(
    rpc_addr: &SocketAddr,
    ledger_path: &Path,
    no_snapshot_fetch: bool,
) -> Result<(), String> {
    download_tar_bz2(rpc_addr, "genesis.tar.bz2", ledger_path, false)?;

    if !no_snapshot_fetch {
        let snapshot_package =
            solana_ledger::snapshot_utils::get_snapshot_archive_path(ledger_path);
        if snapshot_package.exists() {
            fs::remove_file(&snapshot_package)
                .map_err(|err| format!("error removing {:?}: {}", snapshot_package, err))?;
        }
        download_tar_bz2(
            rpc_addr,
            snapshot_package.file_name().unwrap().to_str().unwrap(),
            snapshot_package.parent().unwrap(),
            true,
        )
        .map_err(|err| format!("Failed to fetch snapshot: {:?}", err))?;
    }

    Ok(())
}

// This function is duplicated in ledger-tool/src/main.rs...
fn hardforks_of(matches: &ArgMatches<'_>, name: &str) -> Option<Vec<Slot>> {
    if matches.is_present(name) {
        Some(values_t_or_exit!(matches, name, Slot))
    } else {
        None
    }
}

#[allow(clippy::cognitive_complexity)]
pub fn main() {
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_clap_utils::version!())
        .arg(
            Arg::with_name("ledger_path")
                .short("l")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .help("Use DIR as persistent ledger location"),
        );

    let ledger_path = PathBuf::from(matches.value_of("ledger_path").unwrap());
    // Canonicalize ledger path to avoid issues with symlink creation
    let _ = fs::create_dir_all(&ledger_path);
    let ledger_path = fs::canonicalize(&ledger_path).unwrap_or_else(|err| {
        eprintln!("Unable to access ledger path: {:?}", err);
        exit(1);
    });

    // Create and canonicalize account paths to avoid issues with symlink creation
    let validator_config = &ValidatorConfig::default();
    let account_paths = vec![ledger_path.join("accounts")];

    validator_config.account_paths = account_paths
        .into_iter()
        .map(|account_path| {
            match fs::create_dir_all(&account_path).and_then(|_| fs::canonicalize(&account_path)) {
                Ok(account_path) => account_path,
                Err(err) => {
                    eprintln!(
                        "Unable to access account path: {:?}, err: {:?}",
                        account_path, err
                    );
                    exit(1);
                }
            }
        })
        .collect();
    if !ledger_path.is_dir() {
        error!(
            "ledger directory does not exist or is not accessible: {:?}",
            ledger_path
        );
        exit(1);
    }

    let validator = Validator::new(
        Node::new_localhost(),
        &Arc::new(Keypair::new()),
        &ledger_path,
        &Pubkey::default(),
        &Arc::new(Keypair::new()),
        &Arc::new(Keypair::new()),
        None,
        true,
        &validator_config,
    );
}
