use clap::{
    crate_description, crate_name, value_t, value_t_or_exit, values_t, values_t_or_exit, App, Arg,
    ArgMatches,
};
use log::*;
use rand::{thread_rng, Rng};
use solana_clap_utils::{
    input_parsers::{keypair_of, keypairs_of, pubkey_of},
    input_validators::{is_keypair_or_ask_keyword, is_pubkey, is_pubkey_or_keypair, is_slot},
    keypair::SKIP_SEED_PHRASE_VALIDATION_ARG,
};
use solana_client::rpc_client::RpcClient;
use solana_core::ledger_cleanup_service::{
    DEFAULT_MAX_LEDGER_SHREDS, DEFAULT_MIN_MAX_LEDGER_SHREDS,
};
use solana_core::{
    cluster_info::{ClusterInfo, Node, MINIMUM_VALIDATOR_PORT_RANGE_WIDTH, VALIDATOR_PORT_RANGE},
    contact_info::ContactInfo,
    gossip_service::GossipService,
    rpc::JsonRpcConfig,
    validator::{Validator, ValidatorConfig},
};
use solana_download_utils::{download_genesis_if_missing, download_snapshot};
use solana_ledger::{
    bank_forks::{CompressionType, SnapshotConfig},
    hardened_unpack::{unpack_genesis_archive, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE},
};
use solana_perf::recycler::enable_recycler_warming;
use solana_sdk::{
    clock::Slot,
    commitment_config::CommitmentConfig,
    genesis_config::GenesisConfig,
    hash::Hash,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use std::{
    collections::HashSet,
    env,
    fs::{self, File},
    net::{SocketAddr, TcpListener, UdpSocket},
    path::PathBuf,
    process::exit,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{sleep, JoinHandle},
    time::{Duration, Instant},
};

fn port_validator(port: String) -> Result<(), String> {
    port.parse::<u16>()
        .map(|_| ())
        .map_err(|e| format!("{:?}", e))
}

fn port_range_validator(port_range: String) -> Result<(), String> {
    if let Some((start, end)) = solana_net_utils::parse_port_range(&port_range) {
        if end - start < MINIMUM_VALIDATOR_PORT_RANGE_WIDTH {
            Err(format!(
                "Port range is too small.  Try --dynamic-port-range {}-{}",
                start,
                start + MINIMUM_VALIDATOR_PORT_RANGE_WIDTH
            ))
        } else {
            Ok(())
        }
    } else {
        Err("Invalid port range".to_string())
    }
}

fn hash_validator(hash: String) -> Result<(), String> {
    Hash::from_str(&hash)
        .map(|_| ())
        .map_err(|e| format!("{:?}", e))
}

fn get_shred_rpc_peers(
    cluster_info: &ClusterInfo,
    expected_shred_version: Option<u16>,
) -> Vec<ContactInfo> {
    let rpc_peers = cluster_info.all_rpc_peers();
    match expected_shred_version {
        Some(expected_shred_version) => {
            // Filter out rpc peers that don't match the expected shred version
            rpc_peers
                .into_iter()
                .filter(|contact_info| contact_info.shred_version == expected_shred_version)
                .collect::<Vec<_>>()
        }
        None => {
            if !rpc_peers
                .iter()
                .all(|contact_info| contact_info.shred_version == rpc_peers[0].shred_version)
            {
                eprintln!(
                        "Multiple shred versions observed in gossip.  Restart with --expected-shred-version"
                    );
                exit(1);
            }
            rpc_peers
        }
    }
}

fn is_trusted_validator(id: &Pubkey, trusted_validators: &Option<HashSet<Pubkey>>) -> bool {
    if let Some(trusted_validators) = trusted_validators {
        trusted_validators.contains(id)
    } else {
        false
    }
}

fn get_trusted_snapshot_hashes(
    cluster_info: &ClusterInfo,
    trusted_validators: &Option<HashSet<Pubkey>>,
) -> Option<HashSet<(Slot, Hash)>> {
    if let Some(trusted_validators) = trusted_validators {
        let mut trusted_snapshot_hashes = HashSet::new();
        for trusted_validator in trusted_validators {
            cluster_info.get_snapshot_hash_for_node(trusted_validator, |snapshot_hashes| {
                for snapshot_hash in snapshot_hashes {
                    trusted_snapshot_hashes.insert(*snapshot_hash);
                }
            });
        }
        Some(trusted_snapshot_hashes)
    } else {
        None
    }
}

fn start_gossip_node(
    identity_keypair: &Arc<Keypair>,
    entrypoint_gossip: &SocketAddr,
    gossip_addr: &SocketAddr,
    gossip_socket: UdpSocket,
    expected_shred_version: Option<u16>,
) -> (Arc<ClusterInfo>, Arc<AtomicBool>, GossipService) {
    let cluster_info = ClusterInfo::new(
        ClusterInfo::gossip_contact_info(
            &identity_keypair.pubkey(),
            *gossip_addr,
            expected_shred_version.unwrap_or(0),
        ),
        identity_keypair.clone(),
    );
    cluster_info.set_entrypoint(ContactInfo::new_gossip_entry_point(entrypoint_gossip));
    let cluster_info = Arc::new(cluster_info);

    let gossip_exit_flag = Arc::new(AtomicBool::new(false));
    let gossip_service = GossipService::new(&cluster_info, None, gossip_socket, &gossip_exit_flag);
    (cluster_info, gossip_exit_flag, gossip_service)
}

fn get_rpc_node(
    cluster_info: &ClusterInfo,
    validator_config: &ValidatorConfig,
    blacklisted_rpc_nodes: &mut HashSet<Pubkey>,
    snapshot_not_required: bool,
    no_untrusted_rpc: bool,
) -> (ContactInfo, Option<(Slot, Hash)>) {
    let mut blacklist_timeout = Instant::now();
    loop {
        info!(
            "Searching for an RPC service, shred version={:?}...",
            validator_config.expected_shred_version
        );
        sleep(Duration::from_secs(1));
        info!("\n{}", cluster_info.contact_info_trace());

        let rpc_peers = get_shred_rpc_peers(&cluster_info, validator_config.expected_shred_version);
        let rpc_peers_total = rpc_peers.len();

        // Filter out blacklisted nodes
        let rpc_peers: Vec<_> = rpc_peers
            .into_iter()
            .filter(|rpc_peer| !blacklisted_rpc_nodes.contains(&rpc_peer.id))
            .collect();
        let rpc_peers_blacklisted = rpc_peers_total - rpc_peers.len();
        let rpc_peers_trusted = rpc_peers
            .iter()
            .filter(|rpc_peer| {
                is_trusted_validator(&rpc_peer.id, &validator_config.trusted_validators)
            })
            .count();

        info!(
            "Total {} RPC nodes found. {} trusted, {} blacklisted ",
            rpc_peers_total, rpc_peers_trusted, rpc_peers_blacklisted
        );

        if rpc_peers_blacklisted == rpc_peers_total {
            // If all nodes are blacklisted and no additional nodes are discovered after 60 seconds,
            // remove the blacklist and try them all again
            if blacklist_timeout.elapsed().as_secs() > 60 {
                info!("Blacklist timeout expired");
                blacklisted_rpc_nodes.clear();
            }
            continue;
        }
        blacklist_timeout = Instant::now();

        let mut highest_snapshot_hash: Option<(Slot, Hash)> = None;
        let eligible_rpc_peers = if snapshot_not_required {
            rpc_peers
        } else {
            let trusted_snapshot_hashes =
                get_trusted_snapshot_hashes(&cluster_info, &validator_config.trusted_validators);

            let mut eligible_rpc_peers = vec![];

            for rpc_peer in rpc_peers.iter() {
                if no_untrusted_rpc
                    && !is_trusted_validator(&rpc_peer.id, &validator_config.trusted_validators)
                {
                    continue;
                }
                cluster_info.get_snapshot_hash_for_node(&rpc_peer.id, |snapshot_hashes| {
                    for snapshot_hash in snapshot_hashes {
                        if let Some(ref trusted_snapshot_hashes) = trusted_snapshot_hashes {
                            if !trusted_snapshot_hashes.contains(snapshot_hash) {
                                // Ignore all untrusted snapshot hashes
                                continue;
                            }
                        }

                        if highest_snapshot_hash.is_none()
                            || snapshot_hash.0 > highest_snapshot_hash.unwrap().0
                        {
                            // Found a higher snapshot, remove all nodes with a lower snapshot
                            eligible_rpc_peers.clear();
                            highest_snapshot_hash = Some(*snapshot_hash)
                        }

                        if Some(*snapshot_hash) == highest_snapshot_hash {
                            eligible_rpc_peers.push(rpc_peer.clone());
                        }
                    }
                });
            }

            match highest_snapshot_hash {
                None => {
                    assert!(eligible_rpc_peers.is_empty());
                    info!("No snapshots available");
                }
                Some(highest_snapshot_hash) => {
                    info!(
                        "Highest available snapshot slot is {}, available from {} node{}: {:?}",
                        highest_snapshot_hash.0,
                        eligible_rpc_peers.len(),
                        if eligible_rpc_peers.len() > 1 {
                            "s"
                        } else {
                            ""
                        },
                        eligible_rpc_peers
                            .iter()
                            .map(|contact_info| contact_info.id)
                            .collect::<Vec<_>>()
                    );
                }
            }
            eligible_rpc_peers
        };

        if !eligible_rpc_peers.is_empty() {
            let contact_info =
                &eligible_rpc_peers[thread_rng().gen_range(0, eligible_rpc_peers.len())];
            return (contact_info.clone(), highest_snapshot_hash);
        }
    }
}

fn check_vote_account(
    rpc_client: &RpcClient,
    identity_pubkey: &Pubkey,
    vote_account_address: &Pubkey,
    authorized_voter_pubkeys: &[Pubkey],
) -> Result<(), String> {
    let vote_account = rpc_client
        .get_account_with_commitment(vote_account_address, CommitmentConfig::root())
        .map_err(|err| format!("failed to fetch vote account: {}", err.to_string()))?
        .value
        .ok_or_else(|| format!("vote account does not exist: {}", vote_account_address))?;

    if vote_account.owner != solana_vote_program::id() {
        return Err(format!(
            "not a vote account (owned by {}): {}",
            vote_account.owner, vote_account_address
        ));
    }

    let identity_account = rpc_client
        .get_account_with_commitment(identity_pubkey, CommitmentConfig::root())
        .map_err(|err| format!("failed to fetch identity account: {}", err.to_string()))?
        .value
        .ok_or_else(|| format!("identity account does not exist: {}", identity_pubkey))?;

    let vote_state = solana_vote_program::vote_state::VoteState::from(&vote_account);
    if let Some(vote_state) = vote_state {
        if vote_state.authorized_voters().is_empty() {
            return Err("Vote account not yet initialized".to_string());
        }

        if vote_state.node_pubkey != *identity_pubkey {
            return Err(format!(
                "vote account's identity ({}) does not match the validator's identity {}).",
                vote_state.node_pubkey, identity_pubkey
            ));
        }

        for (_, vote_account_authorized_voter_pubkey) in vote_state.authorized_voters().iter() {
            if !authorized_voter_pubkeys.contains(&vote_account_authorized_voter_pubkey) {
                return Err(format!(
                    "authorized voter {} not available",
                    vote_account_authorized_voter_pubkey
                ));
            }
        }
    } else {
        return Err(format!(
            "invalid vote account data for {}",
            vote_account_address
        ));
    }

    // Maybe we can calculate minimum voting fee; rather than 1 lamport
    if identity_account.lamports <= 1 {
        return Err(format!(
            "underfunded identity account ({}): only {} lamports available",
            identity_pubkey, identity_account.lamports
        ));
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

fn check_genesis_hash(
    genesis_config: &GenesisConfig,
    expected_genesis_hash: Option<Hash>,
) -> Result<(), String> {
    let genesis_hash = genesis_config.hash();

    if let Some(expected_genesis_hash) = expected_genesis_hash {
        if expected_genesis_hash != genesis_hash {
            return Err(format!(
                "Genesis hash mismatch: expected {} but downloaded genesis hash is {}",
                expected_genesis_hash, genesis_hash,
            ));
        }
    }

    Ok(())
}

fn download_then_check_genesis_hash(
    rpc_addr: &SocketAddr,
    ledger_path: &std::path::Path,
    expected_genesis_hash: Option<Hash>,
    max_genesis_archive_unpacked_size: u64,
) -> Result<Hash, String> {
    let genesis_package = ledger_path.join("genesis.tar.bz2");
    let genesis_config =
        if let Ok(tmp_genesis_package) = download_genesis_if_missing(rpc_addr, &genesis_package) {
            unpack_genesis_archive(
                &tmp_genesis_package,
                &ledger_path,
                max_genesis_archive_unpacked_size,
            )
            .map_err(|err| format!("Failed to unpack downloaded genesis config: {}", err))?;

            let downloaded_genesis = GenesisConfig::load(&ledger_path)
                .map_err(|err| format!("Failed to load downloaded genesis config: {}", err))?;

            check_genesis_hash(&downloaded_genesis, expected_genesis_hash)?;
            std::fs::rename(tmp_genesis_package, genesis_package)
                .map_err(|err| format!("Unable to rename: {:?}", err))?;

            downloaded_genesis
        } else {
            let existing_genesis = GenesisConfig::load(&ledger_path)
                .map_err(|err| format!("Failed to load genesis config: {}", err))?;
            check_genesis_hash(&existing_genesis, expected_genesis_hash)?;

            existing_genesis
        };

    Ok(genesis_config.hash())
}

fn is_snapshot_config_invalid(
    snapshot_interval_slots: u64,
    accounts_hash_interval_slots: u64,
) -> bool {
    snapshot_interval_slots != 0
        && (snapshot_interval_slots < accounts_hash_interval_slots
            || snapshot_interval_slots % accounts_hash_interval_slots != 0)
}

#[cfg(unix)]
fn redirect_stderr(filename: &str) {
    use std::{fs::OpenOptions, os::unix::io::AsRawFd};
    match OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(filename)
    {
        Ok(file) => unsafe {
            libc::dup2(file.as_raw_fd(), libc::STDERR_FILENO);
        },
        Err(err) => eprintln!("Unable to open {}: {}", filename, err),
    }
}

fn start_logger(logfile: Option<String>) -> Option<JoinHandle<()>> {
    let logger_thread = match logfile {
        None => None,
        Some(logfile) => {
            #[cfg(unix)]
            {
                let signals = signal_hook::iterator::Signals::new(&[signal_hook::SIGUSR1])
                    .unwrap_or_else(|err| {
                        eprintln!("Unable to register SIGUSR1 handler: {:?}", err);
                        exit(1);
                    });

                redirect_stderr(&logfile);
                Some(std::thread::spawn(move || {
                    for signal in signals.forever() {
                        info!(
                            "received SIGUSR1 ({}), reopening log file: {:?}",
                            signal, logfile
                        );
                        redirect_stderr(&logfile);
                    }
                }))
            }
            #[cfg(not(unix))]
            {
                println!("logging to a file is not supported on this platform");
                ()
            }
        }
    };

    solana_logger::setup_with_default(
        &[
            "solana=info", /* info logging for all solana modules */
            "rpc=trace",   /* json_rpc request/response logging */
        ]
        .join(","),
    );

    logger_thread
}

#[allow(clippy::cognitive_complexity)]
pub fn main() {
    let default_dynamic_port_range =
        &format!("{}-{}", VALIDATOR_PORT_RANGE.0, VALIDATOR_PORT_RANGE.1);
    let default_limit_ledger_size = &DEFAULT_MAX_LEDGER_SHREDS.to_string();
    let default_genesis_archive_unpacked_size = &MAX_GENESIS_ARCHIVE_UNPACKED_SIZE.to_string();

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .arg(
            Arg::with_name("ledger_path")
                .short("l")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .help("Use DIR as persistent ledger location"),
        )
        .arg(
            Arg::with_name("votes_file")
                .short("v")
                .long("votes")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .help("Use DIR as persistent ledger location"),
        )
        .get_matches();

    let ledger_path = PathBuf::from(matches.value_of("ledger_path").unwrap());
    let votes_file = PathBuf::from(matches.value_of("votes_file").unwrap());
    // Canonicalize ledger path to avoid issues with symlink creation
    let _ = fs::create_dir_all(&ledger_path);
    let ledger_path = fs::canonicalize(&ledger_path).unwrap_or_else(|err| {
        eprintln!("Unable to access ledger path: {:?}", err);
        exit(1);
    });

    // Create and canonicalize account paths to avoid issues with symlink creation
    println!("Make accounts");
    let mut validator_config = ValidatorConfig::default();
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

    let snapshot_path = ledger_path.join("snapshot");
    fs::create_dir_all(&snapshot_path).unwrap_or_else(|err| {
        eprintln!(
            "Failed to create snapshots directory {:?}: {}",
            snapshot_path, err
        );
        exit(1);
    });

    validator_config.snapshot_config = Some(SnapshotConfig {
        snapshot_interval_slots: std::u64::MAX,
        snapshot_path,
        snapshot_package_output_path: ledger_path.clone(),
        compression: CompressionType::NoCompression,
    });

    // Default to RUST_BACKTRACE=1 for more informative validator logs
    if env::var_os("RUST_BACKTRACE").is_none() {
        env::set_var("RUST_BACKTRACE", "1")
    }

    if !ledger_path.is_dir() {
        error!(
            "ledger directory does not exist or is not accessible: {:?}",
            ledger_path
        );
        exit(1);
    }

    println!("New validator");
    let validator = Validator::new(
        votes_file,
        Node::new_localhost(),
        &Arc::new(Keypair::new()),
        &ledger_path,
        &Pubkey::default(),
        vec![Arc::new(Keypair::new())],
        None,
        false,
        &validator_config,
    );
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_interval_check() {
        assert!(!is_snapshot_config_invalid(0, 100));
        assert!(is_snapshot_config_invalid(1, 100));
        assert!(is_snapshot_config_invalid(230, 100));
        assert!(!is_snapshot_config_invalid(500, 100));
        assert!(!is_snapshot_config_invalid(5, 5));
    }
}
