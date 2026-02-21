#[cfg(test)]
use solana_sha256_hasher::hashv;
use {
    crate::{mcp, mcp_merkle},
    solana_clock::Slot,
    solana_packet::PACKET_DATA_SIZE,
    solana_perf::packet::{Packet, PacketRef},
    solana_pubkey::Pubkey,
    solana_signature::{Signature, SIGNATURE_BYTES},
    static_assertions::const_assert_eq,
    thiserror::Error,
};

pub const MCP_NUM_RELAYS: usize = mcp::NUM_RELAYS;
pub const MCP_NUM_PROPOSERS: usize = mcp::NUM_PROPOSERS;
pub const MCP_SHRED_DATA_BYTES: usize = mcp::SHRED_DATA_BYTES;
pub const MCP_WITNESS_LEN: usize = mcp_witness_len(MCP_NUM_RELAYS);

/// MCP shred discriminator byte placed at offset 64 (where ShredVariant lives in legacy shreds).
/// Value 0x03 is in the range 0x00-0x3F, which is disjoint from valid ShredVariant bytes (0x40-0xBF).
pub const MCP_SHRED_DISCRIMINATOR: u8 = 0x03;

/// Offset of the discriminator byte in the wire format.
/// This is byte 64, immediately after the 64-byte proposer signature.
pub const OFFSET_DISCRIMINATOR: usize = SIGNATURE_BYTES; // 64

/// Wire format (1232 bytes total):
/// ```text
/// proposer_sig:[u8;64] + discriminator:u8(0x03) + slot:u64 + proposer_index:u32 +
/// shred_index:u32 + commitment:[u8;32] + shred_data:[u8;862] + witness_len:u8 + witness:[u8;256]
/// ```
pub const MCP_SHRED_WIRE_SIZE: usize = SIGNATURE_BYTES // proposer_sig (64)
    + 1 // discriminator
    + std::mem::size_of::<Slot>() // slot (8)
    + std::mem::size_of::<u32>() // proposer_index (4)
    + std::mem::size_of::<u32>() // shred_index (4)
    + 32 // commitment
    + MCP_SHRED_DATA_BYTES // shred_data (862)
    + 1 // witness_len
    + (32 * MCP_WITNESS_LEN); // witness (256)

// Ensure MCP shred wire size matches UDP packet data size.
const_assert_eq!(MCP_SHRED_WIRE_SIZE, PACKET_DATA_SIZE);

// Field offsets in the new wire format (public for testing)
pub const OFFSET_SLOT: usize = OFFSET_DISCRIMINATOR + 1; // 65
pub const OFFSET_PROPOSER_INDEX: usize = OFFSET_SLOT + std::mem::size_of::<Slot>(); // 73
pub const OFFSET_SHRED_INDEX: usize = OFFSET_PROPOSER_INDEX + std::mem::size_of::<u32>(); // 77
const OFFSET_COMMITMENT: usize = OFFSET_SHRED_INDEX + std::mem::size_of::<u32>(); // 81
const OFFSET_SHRED_DATA: usize = OFFSET_COMMITMENT + 32; // 113
pub const OFFSET_WITNESS_LEN: usize = OFFSET_SHRED_DATA + MCP_SHRED_DATA_BYTES; // 975
const OFFSET_WITNESS: usize = OFFSET_WITNESS_LEN + 1; // 976

#[cfg(test)]
const LEAF_DOMAIN: [u8; 1] = [0x00];
#[cfg(test)]
const NODE_DOMAIN: [u8; 1] = [0x01];

#[derive(Debug, Error, Eq, PartialEq)]
pub enum McpShredError {
    #[error("invalid MCP shred size: expected {expected}, got {actual}")]
    InvalidSize { expected: usize, actual: usize },
    #[error("invalid MCP witness length: expected {expected}, got {actual}")]
    InvalidWitnessLength { expected: usize, actual: usize },
    #[error("invalid MCP proposer index: {0}")]
    InvalidProposerIndex(u32),
    #[error("invalid MCP shred index: {0}")]
    InvalidShredIndex(u32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpShred {
    pub slot: Slot,
    pub proposer_index: u32,
    pub shred_index: u32,
    pub commitment: [u8; 32],
    pub shred_data: [u8; MCP_SHRED_DATA_BYTES],
    pub witness: [[u8; 32]; MCP_WITNESS_LEN],
    pub proposer_signature: Signature,
}

const fn mcp_witness_len(num_relays: usize) -> usize {
    let mut width = 1usize;
    let mut depth = 0usize;
    while width < num_relays {
        width <<= 1;
        depth += 1;
    }
    depth
}

pub fn is_mcp_shred_bytes(data: &[u8]) -> bool {
    // Fast path: check discriminator at byte 64 first (zero collision with ShredVariant).
    // MCP discriminator 0x03 is in range 0x00-0x3F, disjoint from ShredVariant (0x40-0xBF).
    if data.len() != MCP_SHRED_WIRE_SIZE
        || data.get(OFFSET_DISCRIMINATOR) != Some(&MCP_SHRED_DISCRIMINATOR)
    {
        return false;
    }

    // Additional validation: check witness_len and bounded indices.
    if data
        .get(OFFSET_WITNESS_LEN)
        .is_none_or(|len| *len as usize != MCP_WITNESS_LEN)
    {
        return false;
    }

    // Keep classifier cost low: only check fixed layout and bounded indices.
    // Full signature and witness verification happens in MCP-specific verify paths.
    let proposer_index = u32::from_le_bytes(
        data[OFFSET_PROPOSER_INDEX..OFFSET_PROPOSER_INDEX + std::mem::size_of::<u32>()]
            .try_into()
            .unwrap(),
    );
    let shred_index = u32::from_le_bytes(
        data[OFFSET_SHRED_INDEX..OFFSET_SHRED_INDEX + std::mem::size_of::<u32>()]
            .try_into()
            .unwrap(),
    );
    (proposer_index as usize) < MCP_NUM_PROPOSERS && (shred_index as usize) < MCP_NUM_RELAYS
}

pub fn is_mcp_shred_packet(packet: &Packet) -> bool {
    packet.data(..).is_some_and(is_mcp_shred_bytes)
}

pub fn is_mcp_shred_packet_ref(packet: PacketRef<'_>) -> bool {
    packet.data(..).is_some_and(is_mcp_shred_bytes)
}

impl McpShred {
    pub fn from_bytes(data: &[u8]) -> Result<Self, McpShredError> {
        if data.len() != MCP_SHRED_WIRE_SIZE {
            return Err(McpShredError::InvalidSize {
                expected: MCP_SHRED_WIRE_SIZE,
                actual: data.len(),
            });
        }

        // Check discriminator at byte 64
        if data[OFFSET_DISCRIMINATOR] != MCP_SHRED_DISCRIMINATOR {
            return Err(McpShredError::InvalidSize {
                expected: MCP_SHRED_WIRE_SIZE,
                actual: data.len(),
            });
        }

        let witness_len = data[OFFSET_WITNESS_LEN] as usize;
        if witness_len != MCP_WITNESS_LEN {
            return Err(McpShredError::InvalidWitnessLength {
                expected: MCP_WITNESS_LEN,
                actual: witness_len,
            });
        }

        // Parse signature (bytes 0-63)
        let proposer_signature = Signature::from(
            <[u8; SIGNATURE_BYTES]>::try_from(&data[0..SIGNATURE_BYTES]).unwrap(),
        );

        // Parse slot (bytes 65-72)
        let slot = Slot::from_le_bytes(
            data[OFFSET_SLOT..OFFSET_SLOT + std::mem::size_of::<Slot>()]
                .try_into()
                .unwrap(),
        );

        // Parse proposer_index (bytes 73-76)
        let proposer_index = u32::from_le_bytes(
            data[OFFSET_PROPOSER_INDEX..OFFSET_PROPOSER_INDEX + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        );
        if (proposer_index as usize) >= MCP_NUM_PROPOSERS {
            return Err(McpShredError::InvalidProposerIndex(proposer_index));
        }

        // Parse shred_index (bytes 77-80)
        let shred_index = u32::from_le_bytes(
            data[OFFSET_SHRED_INDEX..OFFSET_SHRED_INDEX + std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        );
        if (shred_index as usize) >= MCP_NUM_RELAYS {
            return Err(McpShredError::InvalidShredIndex(shred_index));
        }

        // Parse commitment (bytes 81-112)
        let mut commitment = [0u8; 32];
        commitment.copy_from_slice(&data[OFFSET_COMMITMENT..OFFSET_COMMITMENT + 32]);

        // Parse shred_data (bytes 113-974)
        let mut shred_data = [0u8; MCP_SHRED_DATA_BYTES];
        shred_data.copy_from_slice(&data[OFFSET_SHRED_DATA..OFFSET_SHRED_DATA + MCP_SHRED_DATA_BYTES]);

        // Parse witness (bytes 976-1231)
        let mut witness = [[0u8; 32]; MCP_WITNESS_LEN];
        let mut offset = OFFSET_WITNESS;
        for sibling in &mut witness {
            sibling.copy_from_slice(&data[offset..offset + 32]);
            offset += 32;
        }

        Ok(Self {
            slot,
            proposer_index,
            shred_index,
            commitment,
            shred_data,
            witness,
            proposer_signature,
        })
    }

    pub fn to_bytes(&self) -> [u8; MCP_SHRED_WIRE_SIZE] {
        let mut data = [0u8; MCP_SHRED_WIRE_SIZE];

        // Write signature (bytes 0-63)
        data[0..SIGNATURE_BYTES].copy_from_slice(self.proposer_signature.as_ref());

        // Write discriminator (byte 64)
        data[OFFSET_DISCRIMINATOR] = MCP_SHRED_DISCRIMINATOR;

        // Write slot (bytes 65-72)
        data[OFFSET_SLOT..OFFSET_SLOT + std::mem::size_of::<Slot>()]
            .copy_from_slice(&self.slot.to_le_bytes());

        // Write proposer_index (bytes 73-76)
        data[OFFSET_PROPOSER_INDEX..OFFSET_PROPOSER_INDEX + std::mem::size_of::<u32>()]
            .copy_from_slice(&self.proposer_index.to_le_bytes());

        // Write shred_index (bytes 77-80)
        data[OFFSET_SHRED_INDEX..OFFSET_SHRED_INDEX + std::mem::size_of::<u32>()]
            .copy_from_slice(&self.shred_index.to_le_bytes());

        // Write commitment (bytes 81-112)
        data[OFFSET_COMMITMENT..OFFSET_COMMITMENT + 32].copy_from_slice(&self.commitment);

        // Write shred_data (bytes 113-974)
        data[OFFSET_SHRED_DATA..OFFSET_SHRED_DATA + MCP_SHRED_DATA_BYTES].copy_from_slice(&self.shred_data);

        // Write witness_len (byte 975)
        data[OFFSET_WITNESS_LEN] = MCP_WITNESS_LEN as u8;

        // Write witness (bytes 976-1231)
        let mut offset = OFFSET_WITNESS;
        for sibling in &self.witness {
            data[offset..offset + 32].copy_from_slice(sibling);
            offset += 32;
        }

        data
    }

    pub fn verify_signature(&self, proposer_pubkey: &Pubkey) -> bool {
        self.proposer_signature
            .verify(proposer_pubkey.as_ref(), &self.commitment)
    }

    pub fn verify(&self, proposer_pubkey: &Pubkey) -> bool {
        self.verify_signature(proposer_pubkey) && self.verify_witness()
    }

    pub fn verify_witness(&self) -> bool {
        mcp_merkle::verify_witness(
            self.slot,
            self.proposer_index,
            self.shred_index,
            &self.shred_data,
            &self.witness,
            &self.commitment,
            MCP_NUM_RELAYS,
        )
        .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::shred::{ProcessShredsStats, ReedSolomonCache, Shredder},
        solana_entry::entry::create_ticks,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_signer::Signer,
    };

    fn build_merkle_witness(
        slot: Slot,
        proposer_index: u32,
        leaves: &[[u8; MCP_SHRED_DATA_BYTES]],
        leaf_index: usize,
    ) -> ([u8; 32], [[u8; 32]; MCP_WITNESS_LEN]) {
        assert_eq!(leaves.len(), MCP_NUM_RELAYS);
        let mut level: Vec<[u8; 32]> = leaves
            .iter()
            .enumerate()
            .map(|(i, shred_data)| {
                hashv(&[
                    &LEAF_DOMAIN,
                    &slot.to_le_bytes(),
                    &proposer_index.to_le_bytes(),
                    &(i as u32).to_le_bytes(),
                    shred_data,
                ])
                .to_bytes()
            })
            .collect();

        let mut witness = [[0u8; 32]; MCP_WITNESS_LEN];
        let mut index = leaf_index;
        for sibling_out in &mut witness {
            let sibling_index = index ^ 1;
            let sibling = level
                .get(sibling_index)
                .copied()
                .unwrap_or_else(|| level[index]);
            *sibling_out = sibling;

            let mut next = Vec::with_capacity(level.len().div_ceil(2));
            let mut i = 0usize;
            while i < level.len() {
                let left = level[i];
                let right = level.get(i + 1).copied().unwrap_or(left);
                next.push(hashv(&[&NODE_DOMAIN, &left, &right]).to_bytes());
                i += 2;
            }
            level = next;
            index >>= 1;
        }

        (level[0], witness)
    }

    fn make_leaf(seed: u8) -> [u8; MCP_SHRED_DATA_BYTES] {
        let mut bytes = [0u8; MCP_SHRED_DATA_BYTES];
        bytes.fill(seed);
        bytes
    }

    #[test]
    fn test_mcp_shred_roundtrip_and_classifier() {
        let slot = 4242;
        let proposer_index = 7;
        let shred_index = 55usize;
        let leaves: Vec<[u8; MCP_SHRED_DATA_BYTES]> =
            (0..MCP_NUM_RELAYS).map(|i| make_leaf(i as u8)).collect();
        let (commitment, witness) =
            build_merkle_witness(slot, proposer_index, &leaves, shred_index);
        let keypair = Keypair::new();
        let proposer_signature = keypair.sign_message(&commitment);

        let shred = McpShred {
            slot,
            proposer_index,
            shred_index: shred_index as u32,
            commitment,
            shred_data: leaves[shred_index],
            witness,
            proposer_signature,
        };

        let bytes = shred.to_bytes();
        assert!(is_mcp_shred_bytes(&bytes));

        let mut packet = Packet::default();
        packet.buffer_mut()[..bytes.len()].copy_from_slice(&bytes);
        packet.meta_mut().size = bytes.len();
        assert!(is_mcp_shred_packet(&packet));
        assert!(is_mcp_shred_packet_ref(PacketRef::from(&packet)));

        let decoded = McpShred::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, shred);
        assert!(decoded.verify_signature(&keypair.pubkey()));
        assert!(decoded.verify_witness());
        assert!(decoded.verify(&keypair.pubkey()));
    }

    #[test]
    fn test_mcp_shred_rejects_wrong_witness_len() {
        let mut bytes = [0u8; MCP_SHRED_WIRE_SIZE];
        // Set discriminator so parsing proceeds past that check
        bytes[OFFSET_DISCRIMINATOR] = MCP_SHRED_DISCRIMINATOR;
        bytes[OFFSET_WITNESS_LEN] = (MCP_WITNESS_LEN as u8).saturating_sub(1);
        let err = McpShred::from_bytes(&bytes).unwrap_err();
        assert_eq!(
            err,
            McpShredError::InvalidWitnessLength {
                expected: MCP_WITNESS_LEN,
                actual: MCP_WITNESS_LEN.saturating_sub(1),
            }
        );
        assert!(!is_mcp_shred_bytes(&bytes));
    }

    #[test]
    fn test_mcp_shred_rejects_out_of_range_proposer_index_in_parser() {
        let slot = 42;
        let proposer_index = (MCP_NUM_PROPOSERS + 1) as u32;
        let shred_index = 0usize;
        let leaves: Vec<[u8; MCP_SHRED_DATA_BYTES]> =
            (0..MCP_NUM_RELAYS).map(|i| make_leaf(i as u8)).collect();
        let (commitment, witness) =
            build_merkle_witness(slot, proposer_index, &leaves, shred_index);
        let keypair = Keypair::new();
        let proposer_signature = keypair.sign_message(&commitment);
        let shred = McpShred {
            slot,
            proposer_index,
            shred_index: shred_index as u32,
            commitment,
            shred_data: leaves[shred_index],
            witness,
            proposer_signature,
        };
        let err = McpShred::from_bytes(&shred.to_bytes()).unwrap_err();
        assert_eq!(err, McpShredError::InvalidProposerIndex(proposer_index));
    }

    #[test]
    fn test_mcp_shred_rejects_out_of_range_shred_index_in_parser() {
        let slot = 42;
        let proposer_index = 0u32;
        let shred_index = 0usize;
        let leaves: Vec<[u8; MCP_SHRED_DATA_BYTES]> =
            (0..MCP_NUM_RELAYS).map(|i| make_leaf(i as u8)).collect();
        let (commitment, witness) =
            build_merkle_witness(slot, proposer_index, &leaves, shred_index);
        let keypair = Keypair::new();
        let proposer_signature = keypair.sign_message(&commitment);
        let shred = McpShred {
            slot,
            proposer_index,
            shred_index: shred_index as u32,
            commitment,
            shred_data: leaves[shred_index],
            witness,
            proposer_signature,
        };
        let mut bytes = shred.to_bytes();
        // Modify shred_index at its new offset (OFFSET_SHRED_INDEX = 77)
        bytes[OFFSET_SHRED_INDEX..OFFSET_SHRED_INDEX + std::mem::size_of::<u32>()]
            .copy_from_slice(&(MCP_NUM_RELAYS as u32).to_le_bytes());
        let err = McpShred::from_bytes(&bytes).unwrap_err();
        assert_eq!(err, McpShredError::InvalidShredIndex(MCP_NUM_RELAYS as u32));
    }

    #[test]
    fn test_mcp_shred_signature_and_witness_fail_when_mutated() {
        let slot = 88;
        let proposer_index = 1;
        let shred_index = 3usize;
        let leaves: Vec<[u8; MCP_SHRED_DATA_BYTES]> = (0..MCP_NUM_RELAYS)
            .map(|i| make_leaf((i * 3) as u8))
            .collect();
        let (commitment, witness) =
            build_merkle_witness(slot, proposer_index, &leaves, shred_index);
        let keypair = Keypair::new();
        let proposer_signature = keypair.sign_message(&commitment);

        let mut shred = McpShred {
            slot,
            proposer_index,
            shred_index: shred_index as u32,
            commitment,
            shred_data: leaves[shred_index],
            witness,
            proposer_signature,
        };

        assert!(shred.verify_signature(&keypair.pubkey()));
        assert!(shred.verify_witness());

        shred.shred_data[0] ^= 1;
        assert!(!shred.verify_witness());
        assert!(shred.verify_signature(&keypair.pubkey()));

        shred.shred_data[0] ^= 1;
        shred.commitment[0] ^= 1;
        assert!(!shred.verify_signature(&keypair.pubkey()));
    }

    #[test]
    fn test_classifier_rejects_out_of_range_indices() {
        let slot = 42;
        let proposer_index = (MCP_NUM_PROPOSERS + 1) as u32;
        let shred_index = 0usize;
        let leaves: Vec<[u8; MCP_SHRED_DATA_BYTES]> =
            (0..MCP_NUM_RELAYS).map(|i| make_leaf(i as u8)).collect();
        let (commitment, witness) =
            build_merkle_witness(slot, proposer_index, &leaves, shred_index);
        let keypair = Keypair::new();
        let proposer_signature = keypair.sign_message(&commitment);
        let shred = McpShred {
            slot,
            proposer_index,
            shred_index: shred_index as u32,
            commitment,
            shred_data: leaves[shred_index],
            witness,
            proposer_signature,
        };
        assert!(!is_mcp_shred_bytes(&shred.to_bytes()));
    }

    #[test]
    fn test_classifier_accepts_max_in_range_indices() {
        let slot = 42;
        let proposer_index = (MCP_NUM_PROPOSERS - 1) as u32;
        let shred_index = MCP_NUM_RELAYS - 1;
        let leaves: Vec<[u8; MCP_SHRED_DATA_BYTES]> =
            (0..MCP_NUM_RELAYS).map(|i| make_leaf(i as u8)).collect();
        let (commitment, witness) =
            build_merkle_witness(slot, proposer_index, &leaves, shred_index);
        let keypair = Keypair::new();
        let proposer_signature = keypair.sign_message(&commitment);
        let shred = McpShred {
            slot,
            proposer_index,
            shred_index: shred_index as u32,
            commitment,
            shred_data: leaves[shred_index],
            witness,
            proposer_signature,
        };
        assert!(is_mcp_shred_bytes(&shred.to_bytes()));
        assert!(shred.verify(&keypair.pubkey()));
    }

    #[test]
    fn test_classifier_rejects_shred_index_at_upper_bound() {
        let slot = 42;
        let proposer_index = 0u32;
        let shred_index = 0usize;
        let leaves: Vec<[u8; MCP_SHRED_DATA_BYTES]> =
            (0..MCP_NUM_RELAYS).map(|i| make_leaf(i as u8)).collect();
        let (commitment, witness) =
            build_merkle_witness(slot, proposer_index, &leaves, shred_index);
        let keypair = Keypair::new();
        let proposer_signature = keypair.sign_message(&commitment);
        let shred = McpShred {
            slot,
            proposer_index,
            shred_index: shred_index as u32,
            commitment,
            shred_data: leaves[shred_index],
            witness,
            proposer_signature,
        };
        let mut bytes = shred.to_bytes();
        // Modify shred_index at its new offset (OFFSET_SHRED_INDEX = 77)
        bytes[OFFSET_SHRED_INDEX..OFFSET_SHRED_INDEX + std::mem::size_of::<u32>()]
            .copy_from_slice(&(MCP_NUM_RELAYS as u32).to_le_bytes());
        assert!(!is_mcp_shred_bytes(&bytes));
    }

    #[test]
    fn test_classifier_rejects_legacy_merkle_shreds() {
        let keypair = Keypair::new();
        let entries = create_ticks(1, 0, Hash::new_unique());
        let shredder = Shredder::new(1, 0, 1, 0).unwrap();
        let (data_shreds, coding_shreds) = shredder.entries_to_merkle_shreds_for_tests(
            &keypair,
            &entries,
            true,
            Some(Hash::new_unique()),
            0,
            0,
            &ReedSolomonCache::default(),
            &mut ProcessShredsStats::default(),
        );
        for shred in data_shreds.into_iter().chain(coding_shreds) {
            assert!(!is_mcp_shred_bytes(shred.payload()));
        }
    }

    #[test]
    fn test_parser_and_classifier_accept_edge_slot_values() {
        let proposer_index = 0u32;
        let shred_index = 0usize;
        let leaves: Vec<[u8; MCP_SHRED_DATA_BYTES]> =
            (0..MCP_NUM_RELAYS).map(|i| make_leaf(i as u8)).collect();
        let keypair = Keypair::new();

        for slot in [0u64, u64::MAX] {
            let (commitment, witness) =
                build_merkle_witness(slot, proposer_index, &leaves, shred_index);
            let proposer_signature = keypair.sign_message(&commitment);
            let shred = McpShred {
                slot,
                proposer_index,
                shred_index: shred_index as u32,
                commitment,
                shred_data: leaves[shred_index],
                witness,
                proposer_signature,
            };
            let bytes = shred.to_bytes();
            assert!(is_mcp_shred_bytes(&bytes));
            let parsed = McpShred::from_bytes(&bytes).unwrap();
            assert_eq!(parsed.slot, slot);
            assert!(parsed.verify(&keypair.pubkey()));
        }
    }
}
