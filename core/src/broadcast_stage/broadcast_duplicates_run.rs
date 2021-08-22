use {
    super::*,
    crate::cluster_nodes::ClusterNodesCache,
    itertools::Itertools,
    solana_entry::entry::Entry,
    solana_ledger::shred::Shredder,
    solana_runtime::blockhash_queue::BlockhashQueue,
    solana_sdk::{
        hash::Hash,
        signature::{Keypair, Signature, Signer},
        system_transaction,
    },
    std::collections::HashSet,
};

pub const MINIMUM_DUPLICATE_SLOT: Slot = 20;
pub const DUPLICATE_RATE: usize = 10;

#[derive(PartialEq, Clone, Debug)]
pub struct BroadcastDuplicatesConfig {
    /// Amount of stake (excluding the leader) to send different version of slots to.
    /// Note this is sampled from a list of stakes sorted least to greatest.
    pub stake_partition: u64,
}

#[derive(Clone)]
pub(super) struct BroadcastDuplicatesRun {
    config: BroadcastDuplicatesConfig,
    // Local queue for broadcast to track which duplicate blockhashes we've sent
    duplicate_queue: BlockhashQueue,
    // Buffer for duplicate entries
    duplicate_entries_buffer: Vec<Entry>,
    last_duplicate_entry_hash: Hash,
    current_slot: Slot,
    next_shred_index: u32,
    shred_version: u16,
    recent_blockhash: Option<Hash>,
    prev_entry_hash: Option<Hash>,
    num_slots_broadcasted: usize,
    cluster_nodes_cache: Arc<ClusterNodesCache<BroadcastStage>>,
    original_last_data_shreds: Arc<RwLock<HashSet<Signature>>>,
    partition_last_data_shreds: Arc<RwLock<HashSet<Signature>>>,
}

impl BroadcastDuplicatesRun {
    pub(super) fn new(shred_version: u16, config: BroadcastDuplicatesConfig) -> Self {
        let cluster_nodes_cache = Arc::new(ClusterNodesCache::<BroadcastStage>::new(
            CLUSTER_NODES_CACHE_NUM_EPOCH_CAP,
            CLUSTER_NODES_CACHE_TTL,
        ));
        Self {
            config,
            duplicate_queue: BlockhashQueue::default(),
            duplicate_entries_buffer: vec![],
            next_shred_index: u32::MAX,
            last_duplicate_entry_hash: Hash::default(),
            shred_version,
            current_slot: 0,
            recent_blockhash: None,
            prev_entry_hash: None,
            num_slots_broadcasted: 0,
            cluster_nodes_cache,
            original_last_data_shreds: Arc::new(RwLock::new((HashSet::default()))),
            partition_last_data_shreds: Arc::new(RwLock::new((HashSet::default()))),
        }
    }
}

impl BroadcastRun for BroadcastDuplicatesRun {
    fn run(
        &mut self,
        keypair: &Keypair,
        _blockstore: &Arc<Blockstore>,
        receiver: &Receiver<WorkingBankEntry>,
        socket_sender: &Sender<(TransmitShreds, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
    ) -> Result<()> {
        // 1) Pull entries from banking stage
        let mut receive_results = broadcast_utils::recv_slot_entries(receiver)?;
        let bank = receive_results.bank.clone();
        let last_tick_height = receive_results.last_tick_height;

        if bank.slot() != self.current_slot {
            self.next_shred_index = 0;
            self.current_slot = bank.slot();
            self.prev_entry_hash = None;
            self.num_slots_broadcasted += 1;
        }

        if receive_results.entries.is_empty() {
            return Ok(());
        }

        // Update the recent blockhash based on transactions in the entries
        for entry in &receive_results.entries {
            if !entry.transactions.is_empty() {
                self.recent_blockhash = Some(entry.transactions[0].message.recent_blockhash);
                break;
            }
        }

        // 2) Convert entries to shreds + generate coding shreds. Set a garbage PoH on the last entry
        // in the slot to make verification fail on validators
        let last_entries = {
            if last_tick_height == bank.max_tick_height()
                && bank.slot() > MINIMUM_DUPLICATE_SLOT
                && self.num_slots_broadcasted % DUPLICATE_RATE == 0
                && self.recent_blockhash.is_some()
            {
                let entry_batch_len = receive_results.entries.len();
                let prev_entry_hash =
                    // Try to get second-to-last entry before last tick
                    if entry_batch_len > 1 {
                        Some(receive_results.entries[entry_batch_len - 2].hash)
                    } else {
                        self.prev_entry_hash
                    };

                if let Some(prev_entry_hash) = prev_entry_hash {
                    let original_last_entry = receive_results.entries.pop().unwrap();

                    // Last entry has to be a tick
                    assert!(original_last_entry.is_tick());

                    // Inject an extra entry before the last tick
                    let extra_tx = system_transaction::transfer(
                        keypair,
                        &Pubkey::new_unique(),
                        1,
                        self.recent_blockhash.unwrap(),
                    );
                    let new_extra_entry = Entry::new(&prev_entry_hash, 1, vec![extra_tx]);

                    // This will only work with sleepy tick producer where the hashing
                    // checks in replay are turned off, because we're introducing an extra
                    // hash for the last tick in the `new_extra_entry`.
                    let new_last_entry = Entry::new(
                        &new_extra_entry.hash,
                        original_last_entry.num_hashes,
                        vec![],
                    );

                    Some((original_last_entry, vec![new_extra_entry, new_last_entry]))
                } else {
                    None
                }
            } else {
                None
            }
        };

        self.prev_entry_hash = last_entries
            .as_ref()
            .map(|(original_last_entry, _)| original_last_entry.hash)
            .or_else(|| Some(receive_results.entries.last().unwrap().hash));

        let shredder = Shredder::new(
            bank.slot(),
            bank.parent().unwrap().slot(),
            (bank.tick_height() % bank.ticks_per_slot()) as u8,
            self.shred_version,
        )
        .expect("Expected to create a new shredder");

        let (data_shreds, _, _) = shredder.entries_to_shreds(
            keypair,
            &receive_results.entries,
            last_tick_height == bank.max_tick_height() && last_entries.is_none(),
            self.next_shred_index,
        );

        self.next_shred_index += data_shreds.len() as u32;
        let last_shreds = last_entries.map(|(original_last_entry, duplicate_extra_last_entries)| {
            let (original_last_data_shred, _, _) =
                shredder.entries_to_shreds(keypair, &[original_last_entry], true, self.next_shred_index);

            let (partition_last_data_shred, _, _) =
                // Don't mark the last shred as last so that validators won't know that
                // they've gotten all the shreds, and will continue trying to repair
                shredder.entries_to_shreds(keypair, &duplicate_extra_last_entries, true, self.next_shred_index);

                let sigs: Vec<_> = partition_last_data_shred.iter().map(|s| (s.signature(), s.index())).collect();
                info!(
                    "duplicate signatures for slot {}, sigs: {:?}",
                    bank.slot(),
                    sigs,
                );

            self.next_shred_index += 1;
            (original_last_data_shred, partition_last_data_shred)
        });

        let data_shreds = Arc::new(data_shreds);
        blockstore_sender.send((data_shreds.clone(), None))?;

        // 3) Start broadcast step
        let transmit_shreds = (bank.slot(), data_shreds.clone());
        socket_sender.send((transmit_shreds, None))?;
        info!(
            "{} Sending good shreds for slot {} to network",
            keypair.pubkey(),
            data_shreds.first().unwrap().slot()
        );

        // Special handling of last shred to cause partition
        if let Some((original_last_data_shred, partition_last_data_shred)) = last_shreds {
            let pubkey = keypair.pubkey();
            self.original_last_data_shreds.write().unwrap().extend(
                original_last_data_shred.iter().map(|shred| {
                    assert!(shred.verify(&pubkey));
                    shred.signature()
                }),
            );
            self.partition_last_data_shreds.write().unwrap().extend(
                partition_last_data_shred.iter().map(|shred| {
                    info!("adding {} to partition set", shred.signature());
                    assert!(shred.verify(&pubkey));
                    shred.signature()
                }),
            );
            let original_last_data_shred = Arc::new(original_last_data_shred);
            let partition_last_data_shred = Arc::new(partition_last_data_shred);

            // Store the original shreds that this node replayed
            blockstore_sender.send((original_last_data_shred.clone(), None))?;

            let original_transmit_shreds = (bank.slot(), original_last_data_shred);
            let partition_transmit_shreds = (bank.slot(), partition_last_data_shred);

            info!("sending original transmit shreds");
            socket_sender.send((original_transmit_shreds, None))?;
            info!("sending partition transmit shreds");
            socket_sender.send((partition_transmit_shreds, None))?;
        }
        Ok(())
    }

    fn transmit(
        &mut self,
        receiver: &Arc<Mutex<TransmitReceiver>>,
        cluster_info: &ClusterInfo,
        sock: &UdpSocket,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<()> {
        let ((slot, shreds), _) = receiver.lock().unwrap().recv()?;
        let (root_bank, working_bank) = {
            let bank_forks = bank_forks.read().unwrap();
            (bank_forks.root_bank(), bank_forks.working_bank())
        };
        let self_pubkey = cluster_info.id();

        // Creat cluster partition.
        let cluster_partition: HashSet<Pubkey> = {
            let mut cumilative_stake = 0;
            let epoch = root_bank.get_leader_schedule_epoch(slot);
            root_bank
                .epoch_staked_nodes(epoch)
                .unwrap()
                .iter()
                .filter(|(pubkey, _)| **pubkey != self_pubkey)
                .sorted_by_key(|(pubkey, stake)| (**stake, **pubkey))
                .take_while(|(_, stake)| {
                    cumilative_stake += *stake;
                    cumilative_stake <= self.config.stake_partition
                })
                .map(|(pubkey, _)| *pubkey)
                .collect()
        };

        info!(
            "cluster partition: {:?}, shreds len {}",
            cluster_partition,
            shreds.len()
        );

        // Broadcast data
        let cluster_nodes =
            self.cluster_nodes_cache
                .get(slot, &root_bank, &working_bank, cluster_info);
        let socket_addr_space = cluster_info.socket_addr_space();
        let packets: Vec<_> = shreds
            .iter()
            .flat_map(|shred| {
                info!(
                    "shred with signature {} made it through transmit",
                    shred.signature()
                );
                let seed = shred.seed(Some(self_pubkey), &root_bank);
                let node = cluster_nodes.get_broadcast_peer(seed);
                if node.is_none() {
                    return vec![];
                }
                let node = node.unwrap();
                if !socket_addr_space.check(&node.tvu) {
                    return vec![];
                }
                if self
                    .original_last_data_shreds
                    .write()
                    .unwrap()
                    .remove(&shred.signature())
                {
                    if cluster_partition.contains(&node.id) {
                        info!(
                            "skipping node {} for original shred index {}, slot {}",
                            node.id,
                            shred.index(),
                            shred.slot()
                        );
                        return vec![];
                    }
                } else if self
                    .partition_last_data_shreds
                    .write()
                    .unwrap()
                    .remove(&shred.signature())
                {
                    // If the shred is part of the partition, broadcast it directly to
                    // the partition node
                    return cluster_partition
                        .iter()
                        .filter_map(|pubkey| {
                            let tvu = cluster_info
                                .lookup_contact_info(pubkey, |contact_info| contact_info.tvu)?;
                            Some((&shred.payload, tvu))
                        })
                        .collect();
                }

                vec![(&shred.payload, node.tvu)]
            })
            .collect();
        if let Err(SendPktsError::IoError(ioerr, _)) = batch_send(sock, &packets) {
            return Err(Error::Io(ioerr));
        }
        Ok(())
    }

    fn record(
        &mut self,
        receiver: &Arc<Mutex<RecordReceiver>>,
        blockstore: &Arc<Blockstore>,
    ) -> Result<()> {
        let (all_shreds, _) = receiver.lock().unwrap().recv()?;
        blockstore
            .insert_shreds(all_shreds.to_vec(), None, true)
            .expect("Failed to insert shreds in blockstore");
        Ok(())
    }
}
