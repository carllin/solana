use {
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender},
    solana_measure::measure::Measure,
    solana_perf::packet::{limited_deserialize, Packet, PacketBatch, PACKETS_PER_BATCH},
    solana_sdk::{
        hash::Hash,
        message::Message,
        pubkey::Pubkey,
        short_vec::decode_shortu16_len,
        signature::Signature,
        transaction::{SanitizedTransaction, VersionedTransaction},
    },
    std::{
        cell::RefCell,
        cmp::Ordering,
        collections::{BinaryHeap, HashMap},
        mem::size_of,
        rc::Rc,
        time::Duration,
    },
};

pub type TransactionReceiver = Receiver<DeserializedPacket>;
pub type TransactionSender = Sender<DeserializedPacket>;
pub type CompletedTransactionReceiver = Receiver<Vec<DeserializedPacket>>;
pub type CompletedTransactionSender = Sender<Vec<DeserializedPacket>>;

// TODO: enforce this limit across `BankingScheduler::all_transaction_queues`.
const MAX_QUEUE_LEN: usize = 500_000;

#[derive(Hash, PartialEq, Eq)]
enum LockedAccount {
    Read(Pubkey),
    Write(Pubkey),
}

#[derive(Default)]
struct LockedAccountManager {
    locked_accounts: HashMap<LockedAccount, usize>,
}

impl LockedAccountManager {
    fn is_account_lockable(&self, requested_lock_account: &LockedAccount) -> bool {
        match requested_lock_account {
            LockedAccount::Read(account_key) => !self
                .locked_accounts
                .contains_key(&LockedAccount::Write(*account_key)),

            LockedAccount::Write(account_key) => {
                !self
                    .locked_accounts
                    .contains_key(&LockedAccount::Read(*account_key))
                    && !self
                        .locked_accounts
                        .contains_key(&LockedAccount::Write(*account_key))
            }
        }
    }
}

// TODO: use common struct from `unprocessed_packet_batches.rs` once Tao checks
// it in
#[derive(Default, PartialEq, Eq)]
pub struct DeserializedPacket {
    #[allow(dead_code)]
    versioned_transaction: VersionedTransaction,

    #[allow(dead_code)]
    message_hash: Hash,

    #[allow(dead_code)]
    is_simple_vote: bool,

    fee: usize,
}

impl PartialOrd for DeserializedPacket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.fee.cmp(&other.fee))
    }
}

impl Ord for DeserializedPacket {
    fn cmp(&self, other: &Self) -> Ordering {
        self.fee.cmp(&other.fee)
    }
}

struct TransactionProcessingChannels {
    transaction_sender: TransactionSender,
    completed_transaction_receiver: CompletedTransactionReceiver,
}

/// Represents a heap of transactions that cannot be scheduled because they
/// would take locks on accounts needed by a higher paying transaction
struct BlockedTransactionsQueue {
    // The higher priority transaction blocking all the other transactions in
    // `blocked_transactions` below
    highest_priority_blocked_transaction: DeserializedPacket,
    other_blocked_transactions: BinaryHeap<DeserializedPacket>,
}

/// Manages scheduling highest priority transactions to BankingStage threads
struct BankingScheduler {
    /// A list of all the transaction queues, with the highest priority ones
    /// at the back of the list
    all_transaction_queues: Vec<Rc<RefCell<BinaryHeap<DeserializedPacket>>>>,
    /// Where all unprocessed transactions arriving to the scheduler first
    /// get queued
    default_transaction_queue: Rc<RefCell<BinaryHeap<DeserializedPacket>>>,
    /// Set of accounts currently locked for transactions that were scheduled
    /// to banking threads. These will be freed once the transactions are
    /// finished processing
    locked_accounts: LockedAccountManager,
    /// Keyed by transaction signature for a transaction `t`. This maps to
    /// a `BlockedTransactionsQueue`, `B`, where
    /// `B.highest_priority_blocked_transaction = t`
    blocked_transactions: HashMap<Signature, Rc<RefCell<BlockedTransactionsQueue>>>,
    /// keyed by account key `A`, maps to a `BlockedTransactionsQueue`, `B`, where
    /// `B.highest_priority_blocked_transaction` requires a lock on account `A`
    blocked_transaction_queues_by_accounts: HashMap<Pubkey, Rc<RefCell<BlockedTransactionsQueue>>>,
    /// Channels for communication with transaction processing threads
    transaction_processing_channels: Vec<TransactionProcessingChannels>,
    /// Receiver for all new, unprocessed packets
    unprocessed_packet_batch_receiver: Receiver<Vec<PacketBatch>>,
}

impl BankingScheduler {
    fn new(unprocessed_packet_batch_receiver: Receiver<Vec<PacketBatch>>) -> Self {
        let mut banking_scheduler = BankingScheduler {
            all_transaction_queues: vec![],
            default_transaction_queue: Rc::new(RefCell::new(BinaryHeap::default())),
            locked_accounts: LockedAccountManager::default(),
            blocked_transactions: HashMap::default(),
            blocked_transaction_queues_by_accounts: HashMap::default(),
            transaction_processing_channels: vec![],
            unprocessed_packet_batch_receiver,
        };
        banking_scheduler
            .all_transaction_queues
            .push(banking_scheduler.default_transaction_queue.clone());
        banking_scheduler
    }

    fn deserialize_packet_batch(packet_batch: &PacketBatch) -> Vec<DeserializedPacket> {
        packet_batch
            .packets
            .iter()
            .filter_map(|packet| {
                if packet.meta.discard() {
                    None
                } else {
                    Self::deserialize_packet(packet)
                }
            })
            .collect()
    }

    /// Read the transaction message from packet data
    fn packet_message(packet: &Packet) -> Option<&[u8]> {
        let (sig_len, sig_size) = decode_shortu16_len(&packet.data).ok()?;
        let msg_start = sig_len
            .checked_mul(size_of::<Signature>())
            .and_then(|v| v.checked_add(sig_size))?;
        let msg_end = packet.meta.size;
        Some(&packet.data[msg_start..msg_end])
    }

    // TODO: use common method from `unprocessed_packet_batches.rs` once Tao checks
    // it in
    fn deserialize_packet(packet: &Packet) -> Option<DeserializedPacket> {
        let versioned_transaction: VersionedTransaction =
            match limited_deserialize(&packet.data[0..packet.meta.size]) {
                Ok(tx) => tx,
                Err(_) => return None,
            };

        if let Some(message_bytes) = Self::packet_message(packet) {
            let message_hash = Message::hash_raw_message(message_bytes);
            let is_simple_vote = packet.meta.is_simple_vote_tx();
            Some(DeserializedPacket {
                versioned_transaction,
                message_hash,
                is_simple_vote,
                // TODO: insert a real weight based on fee
                fee: 1,
            })
        } else {
            None
        }
    }

    #[allow(clippy::too_many_arguments)]
    /// Receive incoming packets, push into `self.default_transaction_queue`
    /// TODO: introduce timing/packet metrics
    fn receive_and_buffer_packets(
        &mut self,
        verified_receiver: &Receiver<Vec<PacketBatch>>,
        recv_timeout: Duration,
    ) -> Result<(), RecvTimeoutError> {
        let mut recv_time = Measure::start("receive_and_buffer_packets_recv");
        let packet_batches = verified_receiver.recv_timeout(recv_timeout)?;
        recv_time.stop();

        let mut proc_start = Measure::start("receive_and_buffer_packets_transactions_process");
        let packet_batch_iter = packet_batches.into_iter();
        for packet_batch in packet_batch_iter {
            let deserialized_packets = Self::deserialize_packet_batch(&packet_batch);
            for deserialized_packet in deserialized_packets {
                // TODO: insert a real weight based on fee
                self.default_transaction_queue
                    .borrow_mut()
                    .push(deserialized_packet);
            }
        }
        proc_start.stop();

        Ok(())
    }
}
