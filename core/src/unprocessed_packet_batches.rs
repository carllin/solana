use {
    min_max_heap::MinMaxHeap,
    solana_perf::packet::{limited_deserialize, Packet, PacketBatch},
    solana_runtime::bank::Bank,
    solana_sdk::{
        feature_set::tx_wide_compute_cap,
        hash::Hash,
        message::{
            v0::{self},
            Message, SanitizedMessage, VersionedMessage,
        },
        sanitize::Sanitize,
        short_vec::decode_shortu16_len,
        signature::Signature,
        transaction::{AddressLoader, VersionedTransaction},
    },
    std::{cmp::Ordering, collections::HashMap, mem::size_of, sync::Arc},
};

/// Holds deserialized messages, as well as computed message_hash and other things needed to create
/// SanitizedTransaction
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeserializedPacket {
    pub original_packet: Packet,

    #[allow(dead_code)]
    pub versioned_transaction: VersionedTransaction,

    #[allow(dead_code)]
    pub message_hash: Hash,

    #[allow(dead_code)]
    pub is_simple_vote: bool,

    pub fee_per_cu: u64,

    pub forwarded: bool,
}

impl DeserializedPacket {
    fn new(packet: Packet, bank: &Option<Arc<Bank>>) -> Option<Self> {
        let versioned_transaction: VersionedTransaction =
            match limited_deserialize(&packet.data[0..packet.meta.size]) {
                Ok(tx) => tx,
                Err(_) => return None,
            };

        if let Some(message_bytes) = packet_message(&packet) {
            let message_hash = Message::hash_raw_message(message_bytes);
            let is_simple_vote = packet.meta.is_simple_vote_tx();
            let fee_per_cu = bank
                .as_ref()
                .and_then(|bank| compute_fee_per_cu(&versioned_transaction.message, &*bank))
                .unwrap_or(0);
            Some(Self {
                original_packet: packet,
                versioned_transaction,
                message_hash,
                is_simple_vote,
                forwarded: false,
                fee_per_cu,
            })
        } else {
            None
        }
    }
}

impl PartialOrd for DeserializedPacket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeserializedPacket {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.fee_per_cu.cmp(&other.fee_per_cu) {
            Ordering::Equal => self
                .original_packet
                .meta
                .sender_stake
                .cmp(&other.original_packet.meta.sender_stake),
            ordering => ordering,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct PacketPriorityQueueEntry {
    fee_per_cu: u64,
    sender_stake: u64,
    message_hash: Hash,
}

impl PacketPriorityQueueEntry {
    fn from_packet(deserialized_packet: &DeserializedPacket) -> Self {
        Self {
            fee_per_cu: deserialized_packet.fee_per_cu,
            sender_stake: deserialized_packet.original_packet.meta.sender_stake,
            message_hash: deserialized_packet.message_hash,
        }
    }
}

impl PartialOrd for PacketPriorityQueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PacketPriorityQueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.fee_per_cu.cmp(&other.fee_per_cu) {
            Ordering::Equal => self.sender_stake.cmp(&other.sender_stake),
            ordering => ordering,
        }
    }
}

/// Currently each banking_stage thread has a `UnprocessedPacketBatches` buffer to store
/// PacketBatch's received from sigverify. Banking thread continuously scans the buffer
/// to pick proper packets to add to the block.
#[derive(Default)]
pub struct UnprocessedPacketBatches {
    packet_priority_queue: MinMaxHeap<PacketPriorityQueueEntry>,
    message_hash_to_transaction: HashMap<Hash, DeserializedPacket>,
    batch_limit: usize,
}

impl UnprocessedPacketBatches {
    pub fn with_capacity(capacity: usize) -> Self {
        UnprocessedPacketBatches {
            packet_priority_queue: MinMaxHeap::with_capacity(capacity),
            message_hash_to_transaction: HashMap::with_capacity(capacity),
            batch_limit: capacity,
        }
    }

    pub fn clear(&mut self) {
        self.packet_priority_queue.clear();
        self.message_hash_to_transaction.clear();
    }

    /// Insert new `deserizlized_packet_batch` into inner `MinMaxHeap<DeserializedPacket>`,
    /// weighted first by the fee-per-cu, then the stake of the sender.
    /// If buffer is at the max limit, the lowest weighted packet is dropped
    ///
    /// Returns tuple of number of packets dropped
    pub fn insert_batch(
        &mut self,
        deserialized_packets: impl Iterator<Item = DeserializedPacket>,
    ) -> usize {
        let mut num_dropped_packets = 0;
        for deserialized_packet in deserialized_packets {
            if self.push(deserialized_packet).is_some() {
                num_dropped_packets += 1;
            }
        }
        num_dropped_packets
    }

    pub fn push(&mut self, deserialized_packet: DeserializedPacket) -> Option<DeserializedPacket> {
        if self.len() >= self.batch_limit {
            // Optimized to not allocate by calling `MinMaxHeap::push_pop_min()`
            Some(self.push_pop_min(deserialized_packet))
        } else {
            self.push_internal(deserialized_packet);
            None
        }
    }

    pub fn iter(&mut self) -> impl Iterator<Item = &DeserializedPacket> {
        self.message_hash_to_transaction.values()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut DeserializedPacket> {
        self.message_hash_to_transaction.iter_mut().map(|(k, v)| v)
    }

    pub fn into_iter(self) -> impl Iterator<Item = DeserializedPacket> {
        self.message_hash_to_transaction.into_iter().map(|(k, v)| v)
    }

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut DeserializedPacket) -> bool,
    {
        // TODO: optimize this only when number of packets
        // with oudated blockhash is high
        self.packet_priority_queue.clear();
        self.message_hash_to_transaction
            .retain(|_k, deserialized_packet| {
                let should_retain = f(deserialized_packet);
                if should_retain {
                    let priority_queue_entry =
                        PacketPriorityQueueEntry::from_packet(deserialized_packet);
                    self.packet_priority_queue.push(priority_queue_entry);
                }
                should_retain
            })
    }

    pub fn len(&self) -> usize {
        self.packet_priority_queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.packet_priority_queue.is_empty()
    }

    fn push_internal(&mut self, deserialized_packet: DeserializedPacket) {
        let priority_queue_entry = PacketPriorityQueueEntry::from_packet(&deserialized_packet);

        // Push into the priority queue
        self.packet_priority_queue.push(priority_queue_entry);

        // Keep track of the original packet in the tracking hashmap
        self.message_hash_to_transaction
            .insert(deserialized_packet.message_hash, deserialized_packet);
    }

    /// Returns the popped minimum packet from the priority queue.
    fn push_pop_min(&mut self, deserialized_packet: DeserializedPacket) -> DeserializedPacket {
        let priority_queue_entry = PacketPriorityQueueEntry::from_packet(&deserialized_packet);

        // Push into the priority queue
        let popped_priority_queue_entry = self
            .packet_priority_queue
            .push_pop_min(priority_queue_entry);

        // Remove the popped entry from the tracking hashmap. Unwrap call is safe
        // because the priority queue and hashmap are kept consistent at all times.
        let popped_packet = self
            .message_hash_to_transaction
            .remove(&popped_priority_queue_entry.message_hash)
            .unwrap();

        // Keep track of the original packet in the tracking hashmap
        self.message_hash_to_transaction
            .insert(deserialized_packet.message_hash, deserialized_packet);

        popped_packet
    }

    pub fn pop_max(&mut self) -> Option<DeserializedPacket> {
        self.packet_priority_queue
            .pop_max()
            .map(|priority_queue_entry| {
                self.message_hash_to_transaction
                    .remove(&priority_queue_entry.message_hash)
                    .unwrap()
            })
    }

    /// Pop the next `n` highest priority transactions from the queue.
    /// Returns `None` if the queue is empty
    pub fn pop_max_n(&mut self, n: usize) -> Option<Vec<DeserializedPacket>> {
        let current_len = self.len();
        if current_len == 0 {
            None
        } else {
            let num_to_pop = std::cmp::min(current_len, n);
            Some(
                std::iter::repeat_with(|| self.pop_max().unwrap())
                    .take(num_to_pop)
                    .collect(),
            )
        }
    }

    pub fn capacity(&self) -> usize {
        self.packet_priority_queue.capacity()
    }
}

pub fn deserialize_packets<'a>(
    packet_batch: &'a PacketBatch,
    packet_indexes: &'a [usize],
    bank: &'a Option<Arc<Bank>>,
) -> impl Iterator<Item = DeserializedPacket> + 'a {
    packet_indexes.iter().filter_map(|packet_index| {
        DeserializedPacket::new(packet_batch.packets[*packet_index].clone(), bank)
    })
}

/// Read the transaction message from packet data
pub fn packet_message(packet: &Packet) -> Option<&[u8]> {
    let (sig_len, sig_size) = decode_shortu16_len(&packet.data).ok()?;
    let msg_start = sig_len
        .checked_mul(size_of::<Signature>())
        .and_then(|v| v.checked_add(sig_size))?;
    let msg_end = packet.meta.size;
    Some(&packet.data[msg_start..msg_end])
}

fn sanitize_message(
    versioned_message: &VersionedMessage,
    address_loader: impl AddressLoader,
) -> Option<SanitizedMessage> {
    versioned_message.sanitize().ok()?;

    match versioned_message {
        VersionedMessage::Legacy(message) => Some(SanitizedMessage::Legacy(message.clone())),
        VersionedMessage::V0(message) => {
            let loaded_addresses = address_loader
                .load_addresses(&message.address_table_lookups)
                .ok()?;
            Some(SanitizedMessage::V0(v0::LoadedMessage::new(
                message.clone(),
                loaded_addresses,
            )))
        }
    }
}

/// Computes `(addition_fee + base_fee / requested_cu)` for `deserialized_packet`
fn compute_fee_per_cu(message: &VersionedMessage, bank: &Bank) -> Option<u64> {
    let sanitized_message = sanitize_message(message, bank)?;
    let (total_fee, max_units) = Bank::calculate_fee(
        &sanitized_message,
        bank.get_lamports_per_signature(),
        &bank.fee_structure,
        bank.feature_set.is_active(&tx_wide_compute_cap::id()),
    );
    Some(total_fee / max_units)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_runtime::{
            bank::goto_end_of_slot,
            genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo},
        },
        solana_sdk::{
            compute_budget::ComputeBudgetInstruction,
            signature::{Keypair, Signer},
            system_instruction::{self},
            system_transaction,
            transaction::Transaction,
        },
        std::net::IpAddr,
    };

    fn packet_with_sender_stake(sender_stake: u64, ip: Option<IpAddr>) -> Packet {
        let tx = system_transaction::transfer(
            &Keypair::new(),
            &solana_sdk::pubkey::new_rand(),
            1,
            Hash::new_unique(),
        );
        let mut packet = Packet::from_data(None, &tx).unwrap();
        packet.meta.sender_stake = sender_stake;
        if let Some(ip) = ip {
            packet.meta.addr = ip;
        }
        packet
    }

    #[test]
    fn test_packet_message() {
        let keypair = Keypair::new();
        let pubkey = solana_sdk::pubkey::new_rand();
        let blockhash = Hash::new_unique();
        let transaction = system_transaction::transfer(&keypair, &pubkey, 1, blockhash);
        let packet = Packet::from_data(None, &transaction).unwrap();
        assert_eq!(
            DeserializedPacketBatch::packet_message(&packet)
                .unwrap()
                .to_vec(),
            transaction.message_data()
        );
    }

    #[test]
    fn test_get_stakes_and_locators_from_empty_buffer() {
        let unprocessed_packet_batches = UnprocessedPacketBatches::default();
        let (stakes, locators) = unprocessed_packet_batches.get_stakes_and_locators();

        assert!(stakes.is_empty());
        assert!(locators.is_empty());
    }

    #[test]
    fn test_get_stakes_and_locators() {
        solana_logger::setup();

        // setup senders' address and stake
        let senders: Vec<(IpAddr, u64)> = vec![
            (IpAddr::from([127, 0, 0, 1]), 1),
            (IpAddr::from([127, 0, 0, 2]), 2),
            (IpAddr::from([127, 0, 0, 3]), 3),
        ];
        // create a buffer with 3 batches, each has 2 packet from above sender.
        // buffer looks like:
        // [127.0.0.1, 127.0.0.2]
        // [127.0.0.3, 127.0.0.1]
        // [127.0.0.2, 127.0.0.3]
        let batch_size = 2usize;
        let batch_count = 3usize;
        let unprocessed_packet_batches: UnprocessedPacketBatches = (0..batch_count)
            .map(|batch_index| {
                DeserializedPacketBatch::new(
                    PacketBatch::new(
                        (0..batch_size)
                            .map(|packet_index| {
                                let n = (batch_index * batch_size + packet_index) % senders.len();
                                packet_with_sender_stake(senders[n].1, Some(senders[n].0))
                            })
                            .collect(),
                    ),
                    (0..batch_size).collect(),
                    false,
                )
            })
            .collect();

        let (stakes, locators) = unprocessed_packet_batches.get_stakes_and_locators();

        // Produced stakes and locators should both have "batch_size * batch_count" entries;
        assert_eq!(batch_size * batch_count, stakes.len());
        assert_eq!(batch_size * batch_count, locators.len());
        // Assert stakes and locators are in good order
        locators.iter().enumerate().for_each(|(index, locator)| {
            assert_eq!(
                stakes[index],
                senders[(locator.batch_index * batch_size + locator.packet_index) % senders.len()]
                    .1
            );
        });
    }

    #[test]
    fn test_replace_packet_by_priority() {
        solana_logger::setup();

        let batch = DeserializedPacketBatch::new(
            PacketBatch::new(vec![
                packet_with_sender_stake(200, None),
                packet_with_sender_stake(210, None),
            ]),
            vec![0, 1],
            false,
        );
        let mut unprocessed_packets: UnprocessedPacketBatches = vec![batch].into_iter().collect();

        // try to insert one with weight lesser than anything in buffer.
        // the new one should be rejected, and buffer should be unchanged
        {
            let sender_stake = 0u64;
            let new_batch = DeserializedPacketBatch::new(
                PacketBatch::new(vec![packet_with_sender_stake(sender_stake, None)]),
                vec![0],
                false,
            );
            let (dropped_batch, _, _) = unprocessed_packets.replace_packet_by_priority(new_batch);
            // dropped batch should be the one made from new packet:
            let dropped_packets = dropped_batch.unwrap();
            assert_eq!(1, dropped_packets.packet_batch.packets.len());
            assert_eq!(
                sender_stake,
                dropped_packets.packet_batch.packets[0].meta.sender_stake
            );
            // buffer should be unchanged
            assert_eq!(1, unprocessed_packets.len());
        }

        // try to insert one with sender_stake higher than anything in buffer.
        // the lest sender_stake batch should be dropped, new one will take its palce.
        {
            let sender_stake = 50_000u64;
            let new_batch = DeserializedPacketBatch::new(
                PacketBatch::new(vec![packet_with_sender_stake(sender_stake, None)]),
                vec![0],
                false,
            );
            let (dropped_batch, _, _) = unprocessed_packets.replace_packet_by_priority(new_batch);
            // dropped batch should be the one with lest sender_stake in buffer (the 3rd batch):
            let dropped_packets = dropped_batch.unwrap();
            assert_eq!(2, dropped_packets.packet_batch.packets.len());
            assert_eq!(
                200,
                dropped_packets.packet_batch.packets[0].meta.sender_stake
            );
            assert_eq!(
                210,
                dropped_packets.packet_batch.packets[1].meta.sender_stake
            );
            // buffer should still have 1 batches
            assert_eq!(1, unprocessed_packets.len());
            // ... which should be the new batch with one packet
            assert_eq!(1, unprocessed_packets[0].packet_batch.packets.len());
            assert_eq!(
                sender_stake,
                unprocessed_packets[0].packet_batch.packets[0]
                    .meta
                    .sender_stake
            );
            assert_eq!(1, unprocessed_packets[0].unprocessed_packets.len());
        }
    }

    // build a buffer of four batches, each contains packet with following stake:
    // 0: [ 10, 300]
    // 1: [100, 200, 300]
    // 2: [ 20,  30,  40]
    // 3: [500,  30, 200]
    fn build_unprocessed_packets_buffer() -> UnprocessedPacketBatches {
        vec![
            DeserializedPacketBatch::new(
                PacketBatch::new(vec![
                    packet_with_sender_stake(10, None),
                    packet_with_sender_stake(300, None),
                    packet_with_sender_stake(200, None),
                ]),
                vec![0, 1],
                false,
            ),
            DeserializedPacketBatch::new(
                PacketBatch::new(vec![
                    packet_with_sender_stake(100, None),
                    packet_with_sender_stake(200, None),
                    packet_with_sender_stake(300, None),
                ]),
                vec![0, 1, 2],
                false,
            ),
            DeserializedPacketBatch::new(
                PacketBatch::new(vec![
                    packet_with_sender_stake(20, None),
                    packet_with_sender_stake(30, None),
                    packet_with_sender_stake(40, None),
                ]),
                vec![0, 1, 2],
                false,
            ),
            DeserializedPacketBatch::new(
                PacketBatch::new(vec![
                    packet_with_sender_stake(500, None),
                    packet_with_sender_stake(30, None),
                    packet_with_sender_stake(200, None),
                ]),
                vec![0, 1, 2],
                false,
            ),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn test_prioritize_by_fee_per_cu() {
        solana_logger::setup();

        let leader = solana_sdk::pubkey::new_rand();
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config_with_leader(1_000_000, &leader, 3);
        genesis_config
            .fee_rate_governor
            .target_lamports_per_signature = 1;
        genesis_config.fee_rate_governor.target_signatures_per_slot = 1;

        let bank = Bank::new_for_tests(&genesis_config);
        let mut bank = Bank::new_from_parent(&Arc::new(bank), &leader, 1);
        goto_end_of_slot(&mut bank);
        // build a "packet" with higher fee-pr-cu with compute_budget instruction.
        let key0 = Keypair::new();
        let key1 = Keypair::new();
        let ix0 = system_instruction::transfer(&key0.pubkey(), &key1.pubkey(), 1);
        let ix1 = system_instruction::transfer(&key1.pubkey(), &key0.pubkey(), 1);
        let ix_cb = ComputeBudgetInstruction::request_units(1000, 20000);
        let message = Message::new(&[ix0, ix1, ix_cb], Some(&key0.pubkey()));
        let tx = Transaction::new(&[&key0, &key1], message, bank.last_blockhash());
        let packet = Packet::from_data(None, &tx).unwrap();

        // build a buffer with 4 batches
        let mut unprocessed_packets = build_unprocessed_packets_buffer();
        // add "packet" with higher fee-per-cu to buffer
        unprocessed_packets.push_back(DeserializedPacketBatch::new(
            PacketBatch::new(vec![packet]),
            vec![0],
            false,
        ));
        // randomly select 4 packets plus the higher fee/cu packets to feed into
        // prioritize_by_fee_per_cu function.
        let locators = vec![
            PacketLocator {
                batch_index: 2,
                packet_index: 2,
            },
            PacketLocator {
                batch_index: 1,
                packet_index: 2,
            },
            PacketLocator {
                batch_index: 3,
                packet_index: 0,
            },
            PacketLocator {
                batch_index: 3,
                packet_index: 2,
            },
            PacketLocator {
                batch_index: 4,
                packet_index: 0,
            },
        ];

        // If no bank is given, fee-per-cu won't calculate, should expect output is same as input
        {
            let prioritized_locators =
                unprocessed_packets.prioritize_by_fee_per_cu(&locators, None);
            assert_eq!(locators, prioritized_locators);
        }

        // If bank is given, fee-per-cu is calculated, should expect higher fee-per-cu come
        // out first
        {
            let expected_locators = vec![
                PacketLocator {
                    batch_index: 4,
                    packet_index: 0,
                },
                PacketLocator {
                    batch_index: 2,
                    packet_index: 2,
                },
                PacketLocator {
                    batch_index: 1,
                    packet_index: 2,
                },
                PacketLocator {
                    batch_index: 3,
                    packet_index: 0,
                },
                PacketLocator {
                    batch_index: 3,
                    packet_index: 2,
                },
            ];

            let prioritized_locators =
                unprocessed_packets.prioritize_by_fee_per_cu(&locators, Some(Arc::new(bank)));
            assert_eq!(expected_locators, prioritized_locators);
        }
    }

    #[test]
    fn test_get_cached_fee_per_cu() {
        let mut deserialized_packet = DeserializedPacket::default();
        let slot: Slot = 100;

        // assert default deserialized_packet has no cached fee-per-cu
        assert!(
            UnprocessedPacketBatches::get_cached_fee_per_cu(&deserialized_packet, &slot).is_none()
        );

        // cache fee-per-cu with slot 100
        let fee_per_cu = 1_000u64;
        deserialized_packet.fee_per_cu = Some(FeePerCu { fee_per_cu, slot });

        // assert cache fee-per-cu is available for same slot
        assert_eq!(
            fee_per_cu,
            UnprocessedPacketBatches::get_cached_fee_per_cu(&deserialized_packet, &slot).unwrap()
        );

        // assert cached value became too old
        assert!(
            UnprocessedPacketBatches::get_cached_fee_per_cu(&deserialized_packet, &(slot + 1))
                .is_none()
        );
    }
}
