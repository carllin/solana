use {
    crate::mcp,
    solana_clock::Slot,
    solana_hash::{Hash, HASH_BYTES},
    solana_packet::PACKET_DATA_SIZE,
    solana_pubkey::Pubkey,
    solana_signature::{Signature, SIGNATURE_BYTES},
    solana_signer::Signer,
    std::{
        collections::HashMap,
        time::{Duration, Instant},
    },
};

pub const CONSENSUS_BLOCK_V1: u8 = 1;
pub const CONSENSUS_META_V1: u8 = 1;

/// Wire size of ConsensusMeta::V1: version (1) + block_id (32) + delayed_slot (8)
pub const CONSENSUS_META_V1_WIRE_BYTES: usize = 1 + HASH_BYTES + 8;

/// Typed consensus metadata that replaces the opaque `consensus_meta` bytes.
/// This struct provides explicit versioning and carries fields that were
/// previously hardcoded (like delayed_slot).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsensusMeta {
    V1 {
        /// The authoritative block_id for this slot, as defined by consensus.
        block_id: Hash,
        /// The slot whose bank hash is referenced by `delayed_bankhash`.
        /// Previously hardcoded as `slot - 1`.
        delayed_slot: Slot,
    },
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ConsensusMetaError {
    #[error("unknown consensus meta version: {0}")]
    UnknownVersion(u8),
    #[error("consensus meta is truncated")]
    Truncated,
    #[error("consensus meta has trailing bytes")]
    TrailingBytes,
}

impl ConsensusMeta {
    /// Create a new V1 consensus meta with the given block_id and delayed_slot.
    pub fn new_v1(block_id: Hash, delayed_slot: Slot) -> Self {
        ConsensusMeta::V1 {
            block_id,
            delayed_slot,
        }
    }

    /// Serialize to wire bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            ConsensusMeta::V1 {
                block_id,
                delayed_slot,
            } => {
                let mut out = Vec::with_capacity(CONSENSUS_META_V1_WIRE_BYTES);
                out.push(CONSENSUS_META_V1);
                out.extend_from_slice(block_id.as_ref());
                out.extend_from_slice(&delayed_slot.to_le_bytes());
                out
            }
        }
    }

    /// Deserialize from wire bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConsensusMetaError> {
        if bytes.is_empty() {
            return Err(ConsensusMetaError::Truncated);
        }

        let version = bytes[0];
        match version {
            CONSENSUS_META_V1 => {
                if bytes.len() < CONSENSUS_META_V1_WIRE_BYTES {
                    return Err(ConsensusMetaError::Truncated);
                }
                if bytes.len() > CONSENSUS_META_V1_WIRE_BYTES {
                    return Err(ConsensusMetaError::TrailingBytes);
                }

                let mut block_id_bytes = [0u8; HASH_BYTES];
                block_id_bytes.copy_from_slice(&bytes[1..1 + HASH_BYTES]);
                let block_id = Hash::new_from_array(block_id_bytes);

                let mut delayed_slot_bytes = [0u8; 8];
                delayed_slot_bytes.copy_from_slice(&bytes[1 + HASH_BYTES..]);
                let delayed_slot = Slot::from_le_bytes(delayed_slot_bytes);

                Ok(ConsensusMeta::V1 {
                    block_id,
                    delayed_slot,
                })
            }
            _ => Err(ConsensusMetaError::UnknownVersion(version)),
        }
    }

    /// Get the block_id from this consensus meta.
    pub fn block_id(&self) -> Hash {
        match self {
            ConsensusMeta::V1 { block_id, .. } => *block_id,
        }
    }

    /// Get the delayed_slot from this consensus meta.
    pub fn delayed_slot(&self) -> Slot {
        match self {
            ConsensusMeta::V1 { delayed_slot, .. } => *delayed_slot,
        }
    }
}

const HEADER_LEN: usize = 1 + 8 + 4 + 4 + 4;
const TRAILER_LEN: usize = HASH_BYTES + SIGNATURE_BYTES;
const RELAY_ENTRY_HEADER_LEN: usize = 4 + 1;
const PROPOSER_ENTRY_LEN: usize = 4 + HASH_BYTES + SIGNATURE_BYTES;
const MAX_AGGREGATE_ATTESTATION_BYTES: usize = 1
    + 8
    + 4
    + 2
    + (mcp::NUM_RELAYS
        * (RELAY_ENTRY_HEADER_LEN + (mcp::NUM_PROPOSERS * PROPOSER_ENTRY_LEN) + SIGNATURE_BYTES));
// Keep consensus metadata bounded to a small sidecar payload while preserving
// room for attestation bytes and the leader signature under the QUIC cap.
const MAX_CONSENSUS_META_BYTES: usize = 64 * 1024;
const MAX_CONSENSUS_BLOCK_PROTOCOL_BYTES: usize =
    HEADER_LEN + MAX_AGGREGATE_ATTESTATION_BYTES + MAX_CONSENSUS_META_BYTES + TRAILER_LEN;
const MAX_CONSENSUS_BLOCK_WIRE_BYTES: usize =
    if MAX_CONSENSUS_BLOCK_PROTOCOL_BYTES < mcp::MAX_QUIC_CONTROL_PAYLOAD_BYTES {
        MAX_CONSENSUS_BLOCK_PROTOCOL_BYTES
    } else {
        mcp::MAX_QUIC_CONTROL_PAYLOAD_BYTES
    };

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusBlock {
    pub version: u8,
    pub slot: Slot,
    pub leader_index: u32,
    pub aggregate_bytes: Vec<u8>,
    /// Serialized ConsensusMeta bytes. Use `consensus_meta_parsed()` to access
    /// the typed version which includes block_id and delayed_slot.
    pub consensus_meta: Vec<u8>,
    pub delayed_bankhash: Hash,
    pub leader_signature: Signature,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ConsensusBlockError {
    #[error("unknown consensus block version: {0}")]
    UnknownVersion(u8),
    #[error("aggregate bytes length exceeds u32::MAX: {0}")]
    AggregateLengthOverflow(usize),
    #[error("consensus_meta length exceeds u32::MAX: {0}")]
    ConsensusMetaLengthOverflow(usize),
    #[error("aggregate attestation exceeds protocol maximum: {actual} > {max}")]
    AggregateLengthTooLarge { actual: usize, max: usize },
    #[error("consensus_meta exceeds protocol maximum: {actual} > {max}")]
    ConsensusMetaTooLarge { actual: usize, max: usize },
    #[error("consensus block is truncated")]
    Truncated,
    #[error("consensus block has trailing bytes")]
    TrailingBytes,
    #[error("consensus block exceeds protocol maximum: {actual} > {max}")]
    WireBytesTooLarge { actual: usize, max: usize },
    #[error("invalid consensus_meta: {0}")]
    InvalidConsensusMeta(#[from] ConsensusMetaError),
}

impl ConsensusBlock {
    pub fn new_unsigned(
        slot: Slot,
        leader_index: u32,
        aggregate_bytes: Vec<u8>,
        consensus_meta: Vec<u8>,
        delayed_bankhash: Hash,
    ) -> Result<Self, ConsensusBlockError> {
        if aggregate_bytes.len() > u32::MAX as usize {
            return Err(ConsensusBlockError::AggregateLengthOverflow(
                aggregate_bytes.len(),
            ));
        }
        if aggregate_bytes.len() > MAX_AGGREGATE_ATTESTATION_BYTES {
            return Err(ConsensusBlockError::AggregateLengthTooLarge {
                actual: aggregate_bytes.len(),
                max: MAX_AGGREGATE_ATTESTATION_BYTES,
            });
        }
        if consensus_meta.len() > u32::MAX as usize {
            return Err(ConsensusBlockError::ConsensusMetaLengthOverflow(
                consensus_meta.len(),
            ));
        }
        if consensus_meta.len() > MAX_CONSENSUS_META_BYTES {
            return Err(ConsensusBlockError::ConsensusMetaTooLarge {
                actual: consensus_meta.len(),
                max: MAX_CONSENSUS_META_BYTES,
            });
        }

        Ok(Self {
            version: CONSENSUS_BLOCK_V1,
            slot,
            leader_index,
            aggregate_bytes,
            consensus_meta,
            delayed_bankhash,
            leader_signature: Signature::default(),
        })
    }

    /// Create a new unsigned ConsensusBlock with typed ConsensusMeta.
    /// This is the preferred constructor as it ensures consensus_meta is properly formatted.
    pub fn new_unsigned_with_meta(
        slot: Slot,
        leader_index: u32,
        aggregate_bytes: Vec<u8>,
        consensus_meta: ConsensusMeta,
        delayed_bankhash: Hash,
    ) -> Result<Self, ConsensusBlockError> {
        Self::new_unsigned(
            slot,
            leader_index,
            aggregate_bytes,
            consensus_meta.to_bytes(),
            delayed_bankhash,
        )
    }

    /// Parse the consensus_meta bytes into a typed ConsensusMeta.
    /// Returns an error if the bytes are malformed or use an unknown version.
    pub fn consensus_meta_parsed(&self) -> Result<ConsensusMeta, ConsensusBlockError> {
        ConsensusMeta::from_bytes(&self.consensus_meta).map_err(ConsensusBlockError::from)
    }

    fn wire_body_bytes(&self) -> Result<Vec<u8>, ConsensusBlockError> {
        if self.aggregate_bytes.len() > u32::MAX as usize {
            return Err(ConsensusBlockError::AggregateLengthOverflow(
                self.aggregate_bytes.len(),
            ));
        }
        if self.aggregate_bytes.len() > MAX_AGGREGATE_ATTESTATION_BYTES {
            return Err(ConsensusBlockError::AggregateLengthTooLarge {
                actual: self.aggregate_bytes.len(),
                max: MAX_AGGREGATE_ATTESTATION_BYTES,
            });
        }
        if self.consensus_meta.len() > u32::MAX as usize {
            return Err(ConsensusBlockError::ConsensusMetaLengthOverflow(
                self.consensus_meta.len(),
            ));
        }
        if self.consensus_meta.len() > MAX_CONSENSUS_META_BYTES {
            return Err(ConsensusBlockError::ConsensusMetaTooLarge {
                actual: self.consensus_meta.len(),
                max: MAX_CONSENSUS_META_BYTES,
            });
        }

        let mut out = Vec::with_capacity(
            HEADER_LEN + self.aggregate_bytes.len() + self.consensus_meta.len() + HASH_BYTES,
        );
        out.push(self.version);
        out.extend_from_slice(&self.slot.to_le_bytes());
        out.extend_from_slice(&self.leader_index.to_le_bytes());
        out.extend_from_slice(&(self.aggregate_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.aggregate_bytes);
        out.extend_from_slice(&(self.consensus_meta.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.consensus_meta);
        out.extend_from_slice(self.delayed_bankhash.as_ref());
        Ok(out)
    }

    pub fn signing_bytes(&self) -> Result<Vec<u8>, ConsensusBlockError> {
        self.wire_body_bytes()
    }

    pub fn to_wire_bytes(&self) -> Result<Vec<u8>, ConsensusBlockError> {
        let mut bytes = self.wire_body_bytes()?;
        bytes.extend_from_slice(self.leader_signature.as_ref());
        if bytes.len() > MAX_CONSENSUS_BLOCK_WIRE_BYTES {
            return Err(ConsensusBlockError::WireBytesTooLarge {
                actual: bytes.len(),
                max: MAX_CONSENSUS_BLOCK_WIRE_BYTES,
            });
        }
        Ok(bytes)
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self, ConsensusBlockError> {
        if bytes.len() > MAX_CONSENSUS_BLOCK_WIRE_BYTES {
            return Err(ConsensusBlockError::WireBytesTooLarge {
                actual: bytes.len(),
                max: MAX_CONSENSUS_BLOCK_WIRE_BYTES,
            });
        }
        if bytes.len() < HEADER_LEN + TRAILER_LEN {
            return Err(ConsensusBlockError::Truncated);
        }

        let mut cursor = 0usize;
        let version = read_u8(bytes, &mut cursor)?;
        if version != CONSENSUS_BLOCK_V1 {
            return Err(ConsensusBlockError::UnknownVersion(version));
        }

        let slot = read_u64_le(bytes, &mut cursor)?;
        let leader_index = read_u32_le(bytes, &mut cursor)?;
        let aggregate_len = read_u32_le(bytes, &mut cursor)? as usize;
        if aggregate_len > MAX_AGGREGATE_ATTESTATION_BYTES {
            return Err(ConsensusBlockError::AggregateLengthTooLarge {
                actual: aggregate_len,
                max: MAX_AGGREGATE_ATTESTATION_BYTES,
            });
        }
        let aggregate_bytes = read_vec(bytes, &mut cursor, aggregate_len)?;
        let consensus_meta_len = read_u32_le(bytes, &mut cursor)? as usize;
        if consensus_meta_len > MAX_CONSENSUS_META_BYTES {
            return Err(ConsensusBlockError::ConsensusMetaTooLarge {
                actual: consensus_meta_len,
                max: MAX_CONSENSUS_META_BYTES,
            });
        }
        let consensus_meta = read_vec(bytes, &mut cursor, consensus_meta_len)?;
        let delayed_bankhash = Hash::new_from_array(read_array::<HASH_BYTES>(bytes, &mut cursor)?);
        let leader_signature = Signature::from(read_array::<SIGNATURE_BYTES>(bytes, &mut cursor)?);

        if cursor != bytes.len() {
            return Err(ConsensusBlockError::TrailingBytes);
        }

        Ok(Self {
            version,
            slot,
            leader_index,
            aggregate_bytes,
            consensus_meta,
            delayed_bankhash,
            leader_signature,
        })
    }

    pub fn sign_leader<T: Signer>(&mut self, signer: &T) -> Result<(), ConsensusBlockError> {
        let signing_bytes = self.signing_bytes()?;
        self.leader_signature = signer.sign_message(&signing_bytes);
        Ok(())
    }

    pub fn verify_leader_signature(&self, leader_pubkey: &Pubkey) -> bool {
        let Ok(signing_bytes) = self.signing_bytes() else {
            return false;
        };
        self.leader_signature
            .verify(leader_pubkey.as_ref(), &signing_bytes)
    }
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, ConsensusBlockError> {
    let Some(end) = cursor.checked_add(1) else {
        return Err(ConsensusBlockError::Truncated);
    };
    if end > bytes.len() {
        return Err(ConsensusBlockError::Truncated);
    }
    let value = bytes[*cursor];
    *cursor = end;
    Ok(value)
}

fn read_u32_le(bytes: &[u8], cursor: &mut usize) -> Result<u32, ConsensusBlockError> {
    Ok(u32::from_le_bytes(read_array::<4>(bytes, cursor)?))
}

fn read_u64_le(bytes: &[u8], cursor: &mut usize) -> Result<u64, ConsensusBlockError> {
    Ok(u64::from_le_bytes(read_array::<8>(bytes, cursor)?))
}

fn read_vec(bytes: &[u8], cursor: &mut usize, len: usize) -> Result<Vec<u8>, ConsensusBlockError> {
    let Some(end) = cursor.checked_add(len) else {
        return Err(ConsensusBlockError::Truncated);
    };
    if end > bytes.len() {
        return Err(ConsensusBlockError::Truncated);
    }
    let out = bytes[*cursor..end].to_vec();
    *cursor = end;
    Ok(out)
}

fn read_array<const N: usize>(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<[u8; N], ConsensusBlockError> {
    let Some(end) = cursor.checked_add(N) else {
        return Err(ConsensusBlockError::Truncated);
    };
    if end > bytes.len() {
        return Err(ConsensusBlockError::Truncated);
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(out)
}

// ── ConsensusBlock fragment protocol ──────────────────────────────────────────
//
// ConsensusBlocks (20KB–400KB) exceed the ~1232-byte QUIC datagram MTU on
// production networks.  We split them into MTU-sized fragments with a thin
// framing header so each fragment fits in a single datagram.
//
// Fragment wire format (max PACKET_DATA_SIZE = 1232 bytes):
//   type_tag  : u8       = 0x03   (1 byte)
//   slot      : u64               (8 bytes)
//   frag_idx  : u16               (2 bytes, 0-based)
//   total     : u16               (2 bytes, total fragment count)
//   cb_hash   : [u8; 32]          (32 bytes, SHA-256 of full ConsensusBlock wire bytes)
//   data      : [u8; N]           (remaining, up to MAX_FRAGMENT_DATA)

pub const MCP_CONTROL_MSG_CONSENSUS_BLOCK_FRAGMENT: u8 = 0x03;
/// Fragment header overhead: type_tag(1) + slot(8) + frag_idx(2) + total(2) + cb_hash(32).
pub const FRAGMENT_OVERHEAD: usize = 1 + 8 + 2 + 2 + 32;
/// Maximum data payload per fragment.
pub const MAX_FRAGMENT_DATA: usize = PACKET_DATA_SIZE - FRAGMENT_OVERHEAD;
/// Maximum number of fragments a single ConsensusBlock can produce.
/// Derived from the protocol wire-size bound so `total` in a fragment header
/// can be validated without trusting the sender.
pub const MAX_FRAGMENT_COUNT: usize = MAX_CONSENSUS_BLOCK_WIRE_BYTES.div_ceil(MAX_FRAGMENT_DATA);

const MAX_PENDING_REASSEMBLIES: usize = 64;
/// Maximum number of distinct cb_hash reassemblies tracked per slot.
/// One leader produces one ConsensusBlock per slot, so >2 is adversarial.
const MAX_PENDING_PER_SLOT: usize = 2;

/// Split ConsensusBlock wire bytes into MTU-sized fragments.
pub fn fragment_consensus_block(slot: Slot, consensus_wire_bytes: &[u8]) -> Vec<Vec<u8>> {
    use solana_sha256_hasher::hash;

    let cb_hash = hash(consensus_wire_bytes);
    let total_fragments = consensus_wire_bytes
        .len()
        .div_ceil(MAX_FRAGMENT_DATA)
        .max(1);
    let total = total_fragments as u16;

    let mut fragments = Vec::with_capacity(total_fragments);
    for (frag_idx, chunk) in consensus_wire_bytes
        .chunks(MAX_FRAGMENT_DATA)
        .enumerate()
    {
        let mut buf = Vec::with_capacity(FRAGMENT_OVERHEAD + chunk.len());
        buf.push(MCP_CONTROL_MSG_CONSENSUS_BLOCK_FRAGMENT);
        buf.extend_from_slice(&slot.to_le_bytes());
        buf.extend_from_slice(&(frag_idx as u16).to_le_bytes());
        buf.extend_from_slice(&total.to_le_bytes());
        buf.extend_from_slice(cb_hash.as_ref());
        buf.extend_from_slice(chunk);
        fragments.push(buf);
    }
    fragments
}

/// Parsed fragment header: `(slot, frag_idx, total, cb_hash)`.
pub type FragmentHeader = (Slot, u16, u16, [u8; 32]);

/// Parse the header of a consensus block fragment.  Returns
/// `(header, data_slice)` on success where header is `(slot, frag_idx, total, cb_hash)`.
pub fn parse_fragment_header(bytes: &[u8]) -> Option<(FragmentHeader, &[u8])> {
    if bytes.len() <= FRAGMENT_OVERHEAD || bytes.len() > PACKET_DATA_SIZE {
        return None;
    }
    if bytes[0] != MCP_CONTROL_MSG_CONSENSUS_BLOCK_FRAGMENT {
        return None;
    }
    let slot = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
    let frag_idx = u16::from_le_bytes(bytes[9..11].try_into().ok()?);
    let total = u16::from_le_bytes(bytes[11..13].try_into().ok()?);
    if total == 0 || total as usize > MAX_FRAGMENT_COUNT || frag_idx >= total {
        return None;
    }
    let mut cb_hash = [0u8; 32];
    cb_hash.copy_from_slice(&bytes[13..45]);
    let data = &bytes[FRAGMENT_OVERHEAD..];
    Some(((slot, frag_idx, total, cb_hash), data))
}

struct FragmentState {
    total: u16,
    received: Vec<Option<Vec<u8>>>,
    received_count: u16,
    first_seen: Instant,
}

/// Collects consensus block fragments and reassembles complete blocks.
#[derive(Default)]
pub struct ConsensusBlockFragmentCollector {
    pending: HashMap<(Slot, [u8; 32]), FragmentState>,
}

impl ConsensusBlockFragmentCollector {
    /// Ingest a raw fragment datagram.  Returns `(slot, reassembled_consensus_bytes)`
    /// when all fragments for a ConsensusBlock have arrived.
    pub fn ingest(&mut self, fragment_bytes: &[u8]) -> Option<(Slot, Vec<u8>)> {
        let ((slot, frag_idx, total, cb_hash), data) = parse_fragment_header(fragment_bytes)?;
        let key = (slot, cb_hash);
        let frag_idx = frag_idx as usize;

        // Before creating a new reassembly entry, enforce the per-slot cap so a
        // single attacker-controlled slot cannot monopolize the pending buffer.
        if !self.pending.contains_key(&key) {
            let slot_count = self.pending.keys().filter(|(s, _)| *s == slot).count();
            if slot_count >= MAX_PENDING_PER_SLOT {
                return None;
            }
        }

        let state = self.pending.entry(key).or_insert_with(|| FragmentState {
            total,
            received: vec![None; total as usize],
            received_count: 0,
            first_seen: Instant::now(),
        });

        // Reject mismatched total for same key.
        if state.total != total {
            return None;
        }

        // Deduplicate: ignore if already received.
        if state.received[frag_idx].is_some() {
            return None;
        }

        state.received[frag_idx] = Some(data.to_vec());
        state.received_count += 1;

        if state.received_count == state.total {
            let state = self.pending.remove(&key)?;
            let mut assembled = Vec::new();
            for part in state.received {
                assembled.extend_from_slice(part.as_ref()?);
            }
            // Verify hash matches.
            let actual_hash = solana_sha256_hasher::hash(&assembled);
            if actual_hash.as_ref() != cb_hash {
                return None;
            }
            Some((slot, assembled))
        } else {
            // Enforce max pending reassemblies after insert.
            self.enforce_max_pending();
            None
        }
    }

    /// Remove reassembly entries older than `max_age`.
    pub fn evict_stale(&mut self, max_age: Duration) {
        self.pending
            .retain(|_, state| state.first_seen.elapsed() < max_age);
    }

    fn enforce_max_pending(&mut self) {
        while self.pending.len() > MAX_PENDING_REASSEMBLIES {
            // Evict the oldest entry.
            let oldest_key = self
                .pending
                .iter()
                .min_by_key(|(_, state)| state.first_seen)
                .map(|(key, _)| *key);
            if let Some(key) = oldest_key {
                self.pending.remove(&key);
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_keypair::Keypair};

    #[test]
    fn test_roundtrip_and_signature_verification() {
        let leader = Keypair::new();
        let mut block = ConsensusBlock::new_unsigned(
            88,
            7,
            vec![1, 2, 3, 4],
            vec![9, 8, 7],
            Hash::new_unique(),
        )
        .unwrap();
        block.sign_leader(&leader).unwrap();

        let bytes = block.to_wire_bytes().unwrap();
        let decoded = ConsensusBlock::from_wire_bytes(&bytes).unwrap();

        assert_eq!(decoded, block);
        assert!(decoded.verify_leader_signature(&leader.pubkey()));
    }

    #[test]
    fn test_roundtrip_with_empty_aggregate_and_meta() {
        let leader = Keypair::new();
        let mut block =
            ConsensusBlock::new_unsigned(21, 3, vec![], vec![], Hash::new_unique()).unwrap();
        block.sign_leader(&leader).unwrap();

        let bytes = block.to_wire_bytes().unwrap();
        let decoded = ConsensusBlock::from_wire_bytes(&bytes).unwrap();

        assert_eq!(decoded, block);
        assert!(decoded.verify_leader_signature(&leader.pubkey()));
    }

    #[test]
    fn test_signature_verification_fails_after_tamper() {
        let leader = Keypair::new();
        let mut block =
            ConsensusBlock::new_unsigned(1, 2, vec![5, 6, 7], vec![10, 11], Hash::new_unique())
                .unwrap();
        block.sign_leader(&leader).unwrap();
        assert!(block.verify_leader_signature(&leader.pubkey()));

        block.consensus_meta.push(99);
        assert!(!block.verify_leader_signature(&leader.pubkey()));
    }

    #[test]
    fn test_signature_verification_fails_with_wrong_key() {
        let leader = Keypair::new();
        let wrong_leader = Keypair::new();
        let mut block =
            ConsensusBlock::new_unsigned(9, 3, vec![1, 2], vec![3], Hash::new_unique()).unwrap();
        block.sign_leader(&leader).unwrap();
        assert!(!block.verify_leader_signature(&wrong_leader.pubkey()));
    }

    #[test]
    fn test_unknown_version_rejected() {
        let bytes = vec![2u8; HEADER_LEN + TRAILER_LEN];
        assert_eq!(
            ConsensusBlock::from_wire_bytes(&bytes).unwrap_err(),
            ConsensusBlockError::UnknownVersion(2)
        );
    }

    #[test]
    fn test_trailing_bytes_rejected() {
        let leader = Keypair::new();
        let mut block =
            ConsensusBlock::new_unsigned(1, 2, vec![5, 6, 7], vec![10, 11], Hash::new_unique())
                .unwrap();
        block.sign_leader(&leader).unwrap();

        let mut bytes = block.to_wire_bytes().unwrap();
        bytes.push(0);
        assert_eq!(
            ConsensusBlock::from_wire_bytes(&bytes).unwrap_err(),
            ConsensusBlockError::TrailingBytes
        );
    }

    #[test]
    fn test_truncated_payload_rejected() {
        let leader = Keypair::new();
        let mut block =
            ConsensusBlock::new_unsigned(2, 4, vec![7, 8], vec![9], Hash::new_unique()).unwrap();
        block.sign_leader(&leader).unwrap();

        let mut bytes = block.to_wire_bytes().unwrap();
        bytes.pop();
        assert_eq!(
            ConsensusBlock::from_wire_bytes(&bytes).unwrap_err(),
            ConsensusBlockError::Truncated
        );
    }

    #[test]
    fn test_oversized_aggregate_len_rejected() {
        let mut bytes = Vec::new();
        bytes.push(CONSENSUS_BLOCK_V1);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&((MAX_AGGREGATE_ATTESTATION_BYTES as u32) + 1).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(Hash::new_unique().as_ref());
        bytes.extend_from_slice(Signature::default().as_ref());

        assert_eq!(
            ConsensusBlock::from_wire_bytes(&bytes).unwrap_err(),
            ConsensusBlockError::AggregateLengthTooLarge {
                actual: MAX_AGGREGATE_ATTESTATION_BYTES + 1,
                max: MAX_AGGREGATE_ATTESTATION_BYTES,
            }
        );
    }

    #[test]
    fn test_oversized_consensus_meta_rejected() {
        let err = ConsensusBlock::new_unsigned(
            3,
            4,
            vec![],
            vec![0u8; MAX_CONSENSUS_META_BYTES + 1],
            Hash::new_unique(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            ConsensusBlockError::ConsensusMetaTooLarge {
                actual: MAX_CONSENSUS_META_BYTES + 1,
                max: MAX_CONSENSUS_META_BYTES,
            }
        );
    }

    #[test]
    fn test_from_wire_bytes_rejects_oversized_wire() {
        let bytes = vec![0u8; MAX_CONSENSUS_BLOCK_WIRE_BYTES + 1];
        assert_eq!(
            ConsensusBlock::from_wire_bytes(&bytes).unwrap_err(),
            ConsensusBlockError::WireBytesTooLarge {
                actual: MAX_CONSENSUS_BLOCK_WIRE_BYTES + 1,
                max: MAX_CONSENSUS_BLOCK_WIRE_BYTES,
            }
        );
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_max_wire_fits_quic_control_payload_bound() {
        assert!(MAX_CONSENSUS_BLOCK_WIRE_BYTES <= mcp::MAX_QUIC_CONTROL_PAYLOAD_BYTES);
    }

    #[test]
    fn test_sign_leader_rejects_oversized_aggregate() {
        let mut block = ConsensusBlock {
            version: CONSENSUS_BLOCK_V1,
            slot: 1,
            leader_index: 0,
            aggregate_bytes: vec![0u8; MAX_AGGREGATE_ATTESTATION_BYTES + 1],
            consensus_meta: Vec::new(),
            delayed_bankhash: Hash::new_unique(),
            leader_signature: Signature::default(),
        };
        let leader = Keypair::new();
        assert_eq!(
            block.sign_leader(&leader).unwrap_err(),
            ConsensusBlockError::AggregateLengthTooLarge {
                actual: MAX_AGGREGATE_ATTESTATION_BYTES + 1,
                max: MAX_AGGREGATE_ATTESTATION_BYTES,
            }
        );
    }

    #[test]
    fn test_roundtrip_accepts_large_leader_index() {
        let leader = Keypair::new();
        let leader_index = u32::MAX;
        let mut block =
            ConsensusBlock::new_unsigned(1, leader_index, vec![], vec![], Hash::new_unique())
                .unwrap();
        block.sign_leader(&leader).unwrap();

        let bytes = block.to_wire_bytes().unwrap();
        let decoded = ConsensusBlock::from_wire_bytes(&bytes).unwrap();
        assert_eq!(decoded.leader_index, leader_index);
    }

    #[test]
    fn test_consensus_meta_v1_roundtrip() {
        let block_id = Hash::new_unique();
        let delayed_slot = 12345u64;
        let meta = ConsensusMeta::new_v1(block_id, delayed_slot);

        let bytes = meta.to_bytes();
        assert_eq!(bytes.len(), CONSENSUS_META_V1_WIRE_BYTES);
        assert_eq!(bytes[0], CONSENSUS_META_V1);

        let parsed = ConsensusMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, meta);
        assert_eq!(parsed.block_id(), block_id);
        assert_eq!(parsed.delayed_slot(), delayed_slot);
    }

    #[test]
    fn test_consensus_meta_unknown_version_rejected() {
        let mut bytes = vec![0u8; CONSENSUS_META_V1_WIRE_BYTES];
        bytes[0] = 99; // Unknown version
        assert_eq!(
            ConsensusMeta::from_bytes(&bytes).unwrap_err(),
            ConsensusMetaError::UnknownVersion(99)
        );
    }

    #[test]
    fn test_consensus_meta_truncated_rejected() {
        let bytes = vec![CONSENSUS_META_V1; 10]; // Too short
        assert_eq!(
            ConsensusMeta::from_bytes(&bytes).unwrap_err(),
            ConsensusMetaError::Truncated
        );
    }

    #[test]
    fn test_consensus_meta_trailing_bytes_rejected() {
        let mut bytes = ConsensusMeta::new_v1(Hash::new_unique(), 100).to_bytes();
        bytes.push(0); // Extra byte
        assert_eq!(
            ConsensusMeta::from_bytes(&bytes).unwrap_err(),
            ConsensusMetaError::TrailingBytes
        );
    }

    #[test]
    fn test_consensus_block_with_typed_meta_roundtrip() {
        let leader = Keypair::new();
        let block_id = Hash::new_unique();
        let delayed_slot = 99u64;
        let delayed_bankhash = Hash::new_unique();
        let consensus_meta = ConsensusMeta::new_v1(block_id, delayed_slot);

        let mut block = ConsensusBlock::new_unsigned_with_meta(
            100,
            7,
            vec![1, 2, 3],
            consensus_meta.clone(),
            delayed_bankhash,
        )
        .unwrap();
        block.sign_leader(&leader).unwrap();

        let bytes = block.to_wire_bytes().unwrap();
        let decoded = ConsensusBlock::from_wire_bytes(&bytes).unwrap();

        assert!(decoded.verify_leader_signature(&leader.pubkey()));

        let parsed_meta = decoded.consensus_meta_parsed().unwrap();
        assert_eq!(parsed_meta, consensus_meta);
        assert_eq!(parsed_meta.block_id(), block_id);
        assert_eq!(parsed_meta.delayed_slot(), delayed_slot);
    }

    #[test]
    fn test_consensus_block_invalid_meta_returns_error() {
        let leader = Keypair::new();
        let mut block = ConsensusBlock::new_unsigned(
            100,
            7,
            vec![],
            vec![99, 1, 2, 3], // Invalid: unknown version 99
            Hash::new_unique(),
        )
        .unwrap();
        block.sign_leader(&leader).unwrap();

        assert!(matches!(
            block.consensus_meta_parsed().unwrap_err(),
            ConsensusBlockError::InvalidConsensusMeta(_)
        ));
    }
    
    // ── Fragment tests ───────────────────────────────────────────────────

    #[test]
    fn test_fragment_roundtrip() {
        let leader = Keypair::new();
        let mut block = ConsensusBlock::new_unsigned(
            42,
            5,
            vec![1u8; 5000],
            vec![2u8; 32],
            Hash::new_unique(),
        )
        .unwrap();
        block.sign_leader(&leader).unwrap();
        let wire_bytes = block.to_wire_bytes().unwrap();

        let fragments = fragment_consensus_block(42, &wire_bytes);
        assert!(fragments.len() > 1);
        for frag in &fragments {
            assert!(frag.len() <= PACKET_DATA_SIZE);
        }

        let mut collector = ConsensusBlockFragmentCollector::default();
        let mut result = None;
        for frag in &fragments {
            result = collector.ingest(frag);
        }
        let (slot, reassembled) = result.expect("should reassemble");
        assert_eq!(slot, 42);
        assert_eq!(reassembled, wire_bytes);

        let decoded = ConsensusBlock::from_wire_bytes(&reassembled).unwrap();
        assert_eq!(decoded, block);
    }

    #[test]
    fn test_fragment_out_of_order_delivery() {
        let wire_bytes = vec![0xABu8; 5000];
        let fragments = fragment_consensus_block(99, &wire_bytes);
        assert!(fragments.len() > 1);

        // Deliver in reverse order.
        let mut collector = ConsensusBlockFragmentCollector::default();
        let mut result = None;
        for frag in fragments.iter().rev() {
            result = collector.ingest(frag);
        }
        let (slot, reassembled) = result.expect("should reassemble");
        assert_eq!(slot, 99);
        assert_eq!(reassembled, wire_bytes);
    }

    #[test]
    fn test_fragment_duplicate_is_idempotent() {
        let wire_bytes = vec![0xCDu8; 3000];
        let fragments = fragment_consensus_block(10, &wire_bytes);

        let mut collector = ConsensusBlockFragmentCollector::default();
        // Send first fragment twice.
        assert!(collector.ingest(&fragments[0]).is_none());
        assert!(collector.ingest(&fragments[0]).is_none());
        // Complete the rest.
        let mut result = None;
        for frag in &fragments[1..] {
            result = collector.ingest(frag);
        }
        let (slot, reassembled) = result.expect("should reassemble");
        assert_eq!(slot, 10);
        assert_eq!(reassembled, wire_bytes);
    }

    #[test]
    fn test_fragment_stale_eviction() {
        let wire_bytes = vec![0xEFu8; 2000];
        let fragments = fragment_consensus_block(50, &wire_bytes);

        let mut collector = ConsensusBlockFragmentCollector::default();
        // Ingest only first fragment.
        collector.ingest(&fragments[0]);
        assert_eq!(collector.pending.len(), 1);

        // Evict with zero duration => everything is stale.
        collector.evict_stale(Duration::from_secs(0));
        assert!(collector.pending.is_empty());
    }

    #[test]
    fn test_fragment_small_payload() {
        // Payload that fits in a single fragment.
        let wire_bytes = vec![0x11u8; 100];
        let fragments = fragment_consensus_block(1, &wire_bytes);
        assert_eq!(fragments.len(), 1);
        assert!(fragments[0].len() <= PACKET_DATA_SIZE);

        let mut collector = ConsensusBlockFragmentCollector::default();
        let (slot, reassembled) = collector
            .ingest(&fragments[0])
            .expect("single fragment should complete");
        assert_eq!(slot, 1);
        assert_eq!(reassembled, wire_bytes);
    }

    #[test]
    fn test_fragment_max_size_consensus_block() {
        // Use a large payload close to protocol max.
        let wire_bytes = vec![0x77u8; MAX_CONSENSUS_BLOCK_WIRE_BYTES];
        let fragments = fragment_consensus_block(200, &wire_bytes);
        let expected_fragments = MAX_CONSENSUS_BLOCK_WIRE_BYTES.div_ceil(MAX_FRAGMENT_DATA);
        assert_eq!(fragments.len(), expected_fragments);

        let mut collector = ConsensusBlockFragmentCollector::default();
        let mut result = None;
        for frag in &fragments {
            result = collector.ingest(frag);
        }
        let (slot, reassembled) = result.expect("should reassemble");
        assert_eq!(slot, 200);
        assert_eq!(reassembled, wire_bytes);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_fragment_constants() {
        assert_eq!(FRAGMENT_OVERHEAD, 45);
        assert_eq!(MAX_FRAGMENT_DATA, PACKET_DATA_SIZE - 45);
        assert!(MAX_FRAGMENT_DATA > 0);
    }

    #[test]
    fn test_parse_fragment_header_rejects_invalid() {
        // Too short.
        assert!(parse_fragment_header(&[0x03; FRAGMENT_OVERHEAD]).is_none());
        // Wrong type tag.
        let mut frag = vec![0x01; FRAGMENT_OVERHEAD + 1];
        assert!(parse_fragment_header(&frag).is_none());
        // total == 0.
        frag[0] = 0x03;
        frag[11..13].copy_from_slice(&0u16.to_le_bytes());
        assert!(parse_fragment_header(&frag).is_none());
    }

    #[test]
    fn test_fragment_hash_mismatch_rejected() {
        let wire_bytes = vec![0xAAu8; 2000];
        let mut fragments = fragment_consensus_block(5, &wire_bytes);
        // Corrupt data in one fragment.
        let last = fragments.last_mut().unwrap();
        let data_start = FRAGMENT_OVERHEAD;
        if last.len() > data_start {
            last[data_start] ^= 0xFF;
        }
        let mut collector = ConsensusBlockFragmentCollector::default();
        let mut result = None;
        for frag in &fragments {
            result = collector.ingest(frag);
        }
        // Hash mismatch should prevent reassembly.
        assert!(result.is_none());
    }

    // ── Adversarial fragment header tests ────────────────────────────────

    fn make_fragment(slot: Slot, frag_idx: u16, total: u16, cb_hash: [u8; 32]) -> Vec<u8> {
        let data = vec![0u8; 64]; // arbitrary payload, 1 byte past FRAGMENT_OVERHEAD
        let mut buf = Vec::with_capacity(FRAGMENT_OVERHEAD + data.len());
        buf.push(MCP_CONTROL_MSG_CONSENSUS_BLOCK_FRAGMENT);
        buf.extend_from_slice(&slot.to_le_bytes());
        buf.extend_from_slice(&frag_idx.to_le_bytes());
        buf.extend_from_slice(&total.to_le_bytes());
        buf.extend_from_slice(&cb_hash);
        buf.extend_from_slice(&data);
        buf
    }

    #[test]
    fn test_parse_fragment_rejects_total_exceeds_max() {
        let cb_hash = [1u8; 32];
        // MAX_FRAGMENT_COUNT is the legitimate ceiling; one above must be rejected.
        let oversized = (MAX_FRAGMENT_COUNT + 1) as u16;
        let frag = make_fragment(1, 0, oversized, cb_hash);
        assert!(parse_fragment_header(&frag).is_none());

        // MAX_FRAGMENT_COUNT itself should parse fine.
        let max = MAX_FRAGMENT_COUNT as u16;
        let frag = make_fragment(1, 0, max, cb_hash);
        assert!(parse_fragment_header(&frag).is_some());
    }

    #[test]
    fn test_parse_fragment_rejects_frag_idx_out_of_range() {
        let cb_hash = [2u8; 32];
        // frag_idx equal to total is out of range (valid range is 0..total-1).
        let frag = make_fragment(1, 3, 3, cb_hash);
        assert!(parse_fragment_header(&frag).is_none());

        // frag_idx strictly less than total is valid.
        let frag = make_fragment(1, 2, 3, cb_hash);
        assert!(parse_fragment_header(&frag).is_some());
    }

    #[test]
    fn test_collector_per_slot_cap_blocks_third_hash() {
        let mut collector = ConsensusBlockFragmentCollector::default();
        let slot = 99u64;

        // First two distinct cb_hashes for the same slot are accepted.
        let frag1 = make_fragment(slot, 0, 2, [0xAAu8; 32]);
        let frag2 = make_fragment(slot, 0, 2, [0xBBu8; 32]);
        let frag3 = make_fragment(slot, 0, 2, [0xCCu8; 32]);

        assert!(collector.ingest(&frag1).is_none()); // pending: 1
        assert!(collector.ingest(&frag2).is_none()); // pending: 2
        assert_eq!(collector.pending.len(), 2);

        // Third distinct cb_hash for the same slot must be silently dropped.
        assert!(collector.ingest(&frag3).is_none());
        assert_eq!(collector.pending.len(), 2, "third hash should not create a new entry");

        // A fragment for a different slot is unaffected by the per-slot cap.
        let frag_other = make_fragment(slot + 1, 0, 2, [0xCCu8; 32]);
        assert!(collector.ingest(&frag_other).is_none());
        assert_eq!(collector.pending.len(), 3);
    }

    #[test]
    fn test_collector_rejects_mismatched_total_for_same_key() {
        let mut collector = ConsensusBlockFragmentCollector::default();
        let slot = 7u64;
        let cb_hash = [0x42u8; 32];

        // Establish the entry with total=3.
        let frag_a = make_fragment(slot, 0, 3, cb_hash);
        assert!(collector.ingest(&frag_a).is_none());
        assert_eq!(collector.pending.len(), 1);

        // Same (slot, cb_hash) but different total must be rejected.
        let frag_b = make_fragment(slot, 1, 5, cb_hash);
        assert!(collector.ingest(&frag_b).is_none());
        // The mismatched fragment must not alter the stored total.
        let state = collector.pending.get(&(slot, cb_hash)).unwrap();
        assert_eq!(state.total, 3);
        // Nor should it have been stored as received.
        assert_eq!(state.received_count, 1);
    }
}
