# MCP Implementation Plan (Agave, Minimal-Diff v2)

Spec: `docs/src/proposals/mcp-protocol-spec.md`

## Constraints (must hold)

- Reuse existing TPU/TVU pipelines, sigverify, `window_service`, blockstore, replay stage, and execution paths.
- No new stage types. Only add small targeted modules where no equivalent exists.
- All MCP behavior is gated by `feature_set::mcp_protocol_v1::id()`.
- Do **not** modify `ShredVariant`, `ShredCommonHeader`, or existing Agave shred pipeline types.
- MCP shreds use a separate wire format and dedicated column families.
- Invalid MCP messages are dropped and MUST NOT advance protocol state.

## Release Blockers (status)

- Vote-gate input producer wiring: `RESOLVED`
  - Per-slot `VoteGateInput` is populated from ingested `ConsensusBlock` state and refreshed in replay before MCP vote-gate evaluation.
  - Wiring points: `core/src/window_service.rs`, `core/src/mcp_replay.rs`, `core/src/replay_stage.rs`.
- Bankless recording caller wiring: `RESOLVED`
  - `PohRecorder::record_bankless` is called from production block recording when no working bank is installed.
  - Wiring point: `core/src/block_creation_loop.rs`.
- Deterministic pending-slot retry + delayed bankhash sourcing: `RESOLVED`
  - Leader finalization keeps retrying pending slots when prerequisites are temporarily unavailable (especially delayed bankhash).
  - Replay keeps retrying pending consensus slots independently of fork-choice one-shot behavior.
  - Delayed-slot bankhash source order is: `BankForks` delayed slot, then blockstore frozen bank hash.
- Reconstruction-to-execution bridge reader: `PARTIAL`
  - `blockstore_processor` now has a production reader for `McpExecutionOutput` and strict decode/verification of framed transaction bytes.
  - Reader accepts both bincode `VersionedTransaction` bytes and MCP latest/legacy wire bytes (converted to `VersionedTransaction`) before bank verification.
  - Current limitation: missing `McpExecutionOutput` still falls back to legacy entry-derived execution because output production is currently post-execution in replay; strict "must exist pre-execution" enforcement is blocked by pipeline ordering.

## Audit Follow-Ups (from `audit.md`)

- `A1` No silent lock-drop on critical MCP maps (`ConsensusBlock` cache, vote-gate input, included proposers):
  - Required behavior: no `try_write`/silent `if let Ok(..)` on critical path; use explicit `match` and warn on poisoned locks.
- `A2` Reconstruction-state poison semantics:
  - Commitment mismatch must not permanently brick a slot/proposer reconstruction state; next shard insert must reset state.
- `A3` Local-cluster MCP integration strictness:
  - Issue-20 test must fail on timeout by default (no env-gated soft pass).
  - Must read from live validator blockstore handles during runtime.
- `A4` Coverage gap to close before final sign-off:
  - Add multi-node MCP integration coverage for forwarding + two-pass replay-fee path (`block_verification=true` path).
  - Test should enforce non-leader execution-output observation when 2+ validators are live, and degrade to artifact-only checks in port-constrained environments.
- `A5` Rollout invariant:
  - MCP CF additions require coordinated node upgrade before feature activation.
- `A6` MCP control-message backpressure:
  - TVU MCP control channel must be bounded and ingress must be non-blocking with explicit drop counters.

## Resolved Policy Decisions

- `B1` Transaction payload compatibility:
  - MCP payload decoder accepts both:
    - latest MCP §7.1 transaction format
    - legacy Solana wire-format transaction bytes
  - Producer behavior:
    - produce latest §7.1 format whenever the transaction representation supports it
    - otherwise pass through legacy bytes
  - Fee/ordering extraction:
    - latest format:
      - `inclusion_fee`: read §7.1 config field when present, else default `0`
      - `ordering_fee`: read §7.1 config field when present, else derive from `compute_unit_price` (default `0` if absent)
    - legacy format: derive ordering key from `compute_unit_price` (default `0` if absent)
  - Note: this dual-format behavior requires explicit spec compatibility text.
- `B2` `ordering_fee` direction:
  - Execute highest-paying transactions first.
  - Ordering rule is `ordering_fee` descending.
  - Stable tie-break remains concatenated position (after proposer-index concatenation).
- `B3` `block_id` authority:
  - `block_id` is defined by Alpenglow consensus.
  - MCP MUST treat the Alpenglow-provided `block_id` as authoritative and MUST NOT derive a local substitute hash.
  - `consensus_meta` carries the consensus-defined data needed to recover/verify that `block_id`.
- `B4` delayed bankhash availability:
  - There must always be a delayed-slot bankhash before MCP progression.
  - Nodes MUST NOT proceed (leader finalization or validator voting) until delayed bankhash is locally available for the consensus-defined delayed slot.
  - No fallback hash value is permitted.

---

## Reuse Map (Agave)

| MCP area | Reuse target | Integration point |
|---|---|---|
| Schedules | Existing leader-schedule algorithm and cache pattern | `ledger/src/leader_schedule.rs`, `ledger/src/leader_schedule_cache.rs`, `ledger/src/leader_schedule_utils.rs` |
| Shred ingestion | Existing sigverify + window_service flow | `turbine/src/sigverify_shreds.rs`, `core/src/window_service.rs` |
| Storage | Existing blockstore column registration + purge plumbing | `ledger/src/blockstore/column.rs`, `ledger/src/blockstore_db.rs`, `ledger/src/blockstore.rs`, `ledger/src/blockstore/blockstore_purge.rs` |
| Replay integration | Existing replay main loop and receivers | `core/src/replay_stage.rs` |
| Transaction forwarding | Existing forwarding stage and clients | `core/src/forwarding_stage.rs`, `core/src/next_leader.rs` |
| QUIC patterns | Existing `quinn` endpoint patterns (not streamer packet path) | `turbine/src/quic_endpoint.rs`, `core/src/repair/quic_endpoint.rs` |
| Fee deduction primitives | Existing fee-payer and withdrawal behavior | `svm/src/account_loader.rs`, `runtime/src/bank.rs` |

Non-reusable for MCP wire correctness:
- Agave shred Merkle implementation uses different domain separation and proof entry width than MCP (`ledger/src/shred/merkle_tree.rs`).

---

## Pass 1 — Feature Gate + Constants + Wire Types

**Goal:** MCP types compile, serialize, and validate strictly. No behavior changes.

### 1.1 Feature gate

- `feature-set/src/lib.rs`:
  - Add `pub mod mcp_protocol_v1 { declare_id!("..."); }`.
  - Register in `FEATURE_NAMES`.
- Slot-effective feature check pattern (referenced as "slot feature gate" throughout plan):
  ```rust
  if let Some(activated_slot) = bank.feature_set.activated_slot(&mcp_protocol_v1::id()) {
      if slot >= activated_slot {
          // MCP active for this slot
      }
  }
  ```

### 1.2 MCP constants and wire types

- Add `ledger/src/mcp.rs`:
  - Constants:
    - `NUM_PROPOSERS = 16`
    - `NUM_RELAYS = 200`
    - `DATA_SHREDS_PER_FEC_BLOCK = 40`
    - `CODING_SHREDS_PER_FEC_BLOCK = 160`
    - `WITNESS_LEN = 8` (ceil(log2(NUM_RELAYS)))
    - `ATTESTATION_THRESHOLD = 0.60` (`ceil -> 120`)
    - `INCLUSION_THRESHOLD = 0.40` (`ceil -> 80`)
    - `RECONSTRUCTION_THRESHOLD = 0.20` (`ceil -> 40`)
    - `REQUIRED_ATTESTATIONS = ceil(ATTESTATION_THRESHOLD * NUM_RELAYS) = 120`
    - `REQUIRED_INCLUSIONS = ceil(INCLUSION_THRESHOLD * NUM_RELAYS) = 80`
    - `REQUIRED_RECONSTRUCTION = ceil(RECONSTRUCTION_THRESHOLD * NUM_RELAYS) = 40`
    - `MAX_PROPOSER_PAYLOAD = DATA_SHREDS_PER_FEC_BLOCK * SHRED_DATA_BYTES = 34_520` (this is always `<= NUM_RELAYS * SHRED_DATA_BYTES` because `DATA_SHREDS_PER_FEC_BLOCK <= NUM_RELAYS`)
  - Types:
    - `RelayAttestation`
    - `AggregateAttestation`
    - `ConsensusBlock`
- Add `transaction-view/src/mcp_payload.rs`:
  - `McpPayload` parser behavior:
    - decode `tx_count + [tx_len, tx_bytes]` framing
    - for each `tx_bytes`, accept latest MCP §7.1 tx or legacy layout without version prefix
    - carry per-tx format tag for later ordering-key extraction and possible re-encoding
    - after decoding `tx_count` entries, ignore trailing zero bytes and reject trailing non-zero bytes
  - Common parser invariants:
    - reject unknown version
    - reject out-of-range indices
    - enforce sortedness/uniqueness per spec for attestation entries

### 1.3 MCP Merkle

- Add `ledger/src/mcp_merkle.rs`:
  - leaf hash: `SHA-256(0x00 || slot || proposer_index || shred_index || shred_data)`
  - node hash: `SHA-256(0x01 || left || right)`
  - witness entries are 32 bytes
  - odd-node rule pairs with self
  - shared by `mcp_shred`, `mcp_erasure`, and `mcp_reconstruction` (no duplicated Merkle implementations)

### 1.4 MCP shred wire format

- Add `ledger/src/shred/mcp_shred.rs`:
  - Wire size constants:
    - `SIZE_OF_SLOT = 8` (u64)
    - `SIZE_OF_PROPOSER_INDEX = 4` (u32)
    - `SIZE_OF_SHRED_INDEX = 4` (u32)
    - `SIZE_OF_COMMITMENT = 32`
    - `SIZE_OF_WITNESS_LEN = 1` (u8)
    - `SIZE_OF_WITNESS = 32 * WITNESS_LEN = 256`
    - `SIZE_OF_PROPOSER_SIG = 64`
    - `MCP_SHRED_OVERHEAD = 8 + 4 + 4 + 32 + 1 + 256 + 64 = 369`
    - `SHRED_DATA_BYTES = solana_packet::PACKET_DATA_SIZE - MCP_SHRED_OVERHEAD = 1232 - 369 = 863`
    - `MCP_SHRED_WIRE_SIZE = solana_packet::PACKET_DATA_SIZE = 1232`
  - Format: `slot:u64 + proposer_index:u32 + shred_index:u32 + commitment:[u8;32] + shred_data + witness_len:u8 + witness + proposer_sig:[u8;64]`
  - Data and coding shreds use the same wire format. Unlike Agave where RS encodes entire data shreds (headers + payload) into coding shreds, MCP RS-encodes only the payload bytes. Headers are added after RS encoding, so all 200 shreds share the same structure.
    - `shred_index` 0 to DATA_SHREDS_PER_FEC_BLOCK-1 (0-39) = data shreds (original payload bytes)
    - `shred_index` DATA_SHREDS_PER_FEC_BLOCK to NUM_RELAYS-1 (40-199) = coding shreds (RS parity bytes)
    - The `shred_data` field contains either original or parity depending on index
    - Headers are added after RS encoding, not encoded by RS (see section on SHRED_DATA_BYTES derivation)
  - `is_mcp_shred_packet(packet)` classifier
  - strict parse + verify helpers
  - enforce `witness_len == ceil(log2(NUM_RELAYS))`

### 1.5 Reed-Solomon and shredding

- Add `ledger/src/mcp_shredder.rs` mirroring `ledger/src/shredder.rs` for MCP:
  - RS encode: payload bytes → 40 data shards + 160 coding shards
  - RS decode/reconstruct: recover payload from any 40 shards
  - Merkle tree construction and witness generation (using `mcp_merkle.rs`)
  - Build complete `McpShred` structs with signatures
- Follow the same pattern as `shredder.rs`: wrap RS encoding/decoding so `core` uses the wrapper, not the RS library directly.

### 1.6 Tests

- Round-trip serde for all MCP wire types.
- Merkle construction/proof vectors.
- Unknown version rejection.
- Attestation sorting/duplicate rejection.
- `witness_len` enforcement.
- MCP shred packet size and classifier tests.
- Constants verification:
  - Size constants match actual serialized sizes.
  - `MCP_SHRED_WIRE_SIZE == MCP_SHRED_OVERHEAD + SHRED_DATA_BYTES`.
- Wire layout verification:
  - Field offsets match expected positions.
  - Getters extract correct bytes at correct offsets.
- RS encode/decode round-trip:
  - Encode payload into 40+160 shards, decode back.
  - Recover from exactly `REQUIRED_RECONSTRUCTION` (40) shreds.
  - Fail recovery from 39 shreds.
  - Recovery with various erasure patterns (all data lost, all coding lost, mixed).
- Invalid field rejection:
  - Invalid proposer_index (>= NUM_PROPOSERS).
  - Invalid shred_index (>= NUM_RELAYS).
  - Truncated packet (< MCP_SHRED_WIRE_SIZE).
  - Oversized packet (> MCP_SHRED_WIRE_SIZE).
- Signature/witness mutation:
  - Mutated commitment fails signature verification.
  - Mutated shred_data fails witness verification.
  - Wrong shred_index fails witness verification.
- Commitment recomputation:
  - Given all 200 shards, recompute Merkle root and verify it matches commitment.
- Boundary values:
  - Edge slot values (0, u64::MAX).
  - Edge indices (0, NUM_PROPOSERS-1, 0, NUM_RELAYS-1).
  - Max payload size (exactly MAX_PROPOSER_PAYLOAD bytes).

---

## Pass 2 — Schedules

**Goal:** deterministic `Proposers[s]` and `Relays[s]`, including duplicate identities.

### 2.1 Domain-separated schedules

- `ledger/src/leader_schedule.rs`:
  - Add helper that reuses stake-weighted leader sampling with domain-separated seed.
  - `NUM_PROPOSERS`/`NUM_RELAYS` must be imported from `ledger::mcp` (single source of truth), not duplicated locally.
  - Seed construction (32 bytes):
    - `seed[0..8] = epoch.to_le_bytes()`
    - `seed[8..8+domain.len()] = domain`
    - remaining bytes = 0
  - Domains:
    - proposer: `b"mcp:proposer"` (12 bytes)
    - relay: `b"mcp:relay"` (9 bytes)
  - Key differences from leader schedule:
    - Multiple samples per slot: NUM_PROPOSERS (16) for proposers, NUM_RELAYS (200) for relays
    - No repeat/consecutive slots: each position is an independent sample (leader schedule repeats same leader for 4 consecutive slots)
    - Duplicates allowed: same validator can appear multiple times in a slot's list
  - Sampling pattern (contrast with leader schedule's `if i % repeat == 0` logic):
    ```rust
    (0..slots_in_epoch * count)
        .map(|_| keys[weighted_index.sample(rng)])
        .collect()
    ```
  - This ensures proposer, relay, and leader schedules produce independent random sequences from the same stake set.

### 2.2 Stake source parity with leader schedule

- `ledger/src/leader_schedule_utils.rs`:
  - Add `mcp_proposer_schedule(epoch, bank)`.
  - Add `mcp_relay_schedule(epoch, bank)`.
  - Mirror existing vote-keyed vs identity-keyed leader-schedule feature behavior.

### 2.3 Cache and lookup APIs

- `ledger/src/leader_schedule_cache.rs`:
  - Add proposer/relay schedule caches.
  - Add:
    - `proposers_at_slot(slot, bank) -> Option<Vec<Pubkey>>` (len=16)
    - `relays_at_slot(slot, bank) -> Option<Vec<Pubkey>>` (len=200)
    - `proposer_indices_at_slot(slot, pubkey, bank) -> Vec<u32>`
    - `relay_indices_at_slot(slot, pubkey, bank) -> Vec<u32>`
  - Feature gate check: all helpers must check if `mcp_protocol_v1` is active for the slot by checking inside the given bank; if not, return `None` or empty `Vec`. This ensures proposer/relay logic (e.g., section 5.1 proposer activation) does not activate when MCP is disabled.
  - Duplicate identities: if a validator appears multiple times in a schedule (e.g., at proposer indices 3, 7, 15), the index lookup returns all positions (spec §5). This applies to both proposer and relay schedules.

### 2.4 Tests

- Deterministic derivation for same epoch/stake set.
- Domain separation vs leader schedule.
- Window wrap-around across epoch boundary.
- Duplicate-identity index lookup returns all indices.

---

## Pass 3 — Storage + Sigverify Partition

**Goal:** MCP shreds are preserved through sigverify and stored in MCP CFs.

### 3.1 MCP column families

- `ledger/src/blockstore/column.rs`:
  - Add `McpShredData` with index `(Slot, u32 proposer_index, u32 shred_index)`.
  - Add `McpRelayAttestation` with index `(Slot, u32 relay_index)`.
- `ledger/src/blockstore_db.rs`:
  - Register CF descriptors.
  - Add names to `columns()` list.
- `ledger/src/blockstore/blockstore_purge.rs`:
  - Add both MCP CFs to `purge_range()` and `purge_files_in_range()`.

### 3.2 Blockstore APIs

- `ledger/src/blockstore.rs`:
  - Add CF handles to `Blockstore`.
  - Add MCP put/get APIs.
  - Use a dedicated MCP write lock for MCP CF read-modify-write paths (do not serialize on `insert_shreds_lock`).
  - Conflict rule for same key with different bytes:
    - do not silently overwrite
    - surface deterministic conflict marker (equivocation evidence) for replay logic

### 3.3 Sigverify integration (critical partition)

- `turbine/src/sigverify_shreds.rs`:
  - Partition MCP packets before Agave dedup/GPU/resign code paths.
  - MCP partition classifier MUST be strict (`McpShred` wire-size/layout + witness-length/path checks), not a loose size-only check.
  - Agave packets: unchanged existing path.
  - MCP packets:
    - apply slot feature gate (see §1.1) using the `working_bank` that `run_shred_sigverify()` already uses
    - feature is the only filter (no proposer/relay role check) because all validators need MCP shreds for vote gate (spec §3.5: count locally stored shreds >= RECONSTRUCTION_THRESHOLD) and reconstruction
    - bypass Agave shred-id/leader-sigverify assumptions
    - forward through existing `verified_sender` channel for MCP-specific verification in `window_service`

### 3.4 Tests

- MCP shreds survive partition and are not dropped by Agave layout assumptions.
- classifier rejects valid Agave Merkle shreds.
- Valid MCP shred passes, bad signature/proof fails.
- MCP CF put/get + purge behavior.
- Feature flag can toggle between MCP/non MCP pipelines starting from
packet ingestion, into dedup and sigverify

---

## Pass 4 — Window Service + Relay Attestations + MCP Transport

**Goal:** MCP shreds flow through `window_service`, relay attestations are emitted correctly, and MCP control messages are transported reliably.

### 4.1 Window-service MCP partition

- `core/src/window_service.rs` (`run_insert`):
  - Partition on raw payload bytes before `Shred::new_from_serialized_shred`.
  - MCP payload path:
    - parse + validate MCP shred
    - verify proposer signature + witness against slot schedule
    - store via MCP blockstore APIs
    - feed relay-attestation tracker
    - accept/store valid relay-broadcast MCP shreds on all validators (no local relay-index storage filter)
  - Non-MCP path remains unchanged.

### 4.2 Relay attestation semantics

- Track attestation state by `(slot, relay_index, proposer_index)`.
- Relay self-check:
  - relay-index ownership (`relay_indices_at_slot`) applies only to relay-attestation emission.
  - MCP shred storage/retransmit ingestion remains index-agnostic for validator reconstruction.
- Equivocation:
  - conflicting commitments for same `(slot, relay_index, proposer_index)` => no attestation entry for that tuple.
- Cardinality:
  - at most one `RelayAttestation` per `(slot, relay_index)`.
  - if a validator owns multiple relay indices, it may emit multiple attestations (one per index).
- Attestation encoding:
  - `RelayAttestation.entries` MUST be sorted by `proposer_index` and deduplicated.
  - Empty `RelayAttestation.entries` is invalid and MUST be rejected.
  - relay-signing domain is `version || slot || relay_index || entries_len || entries` and intentionally excludes `leader_index` (leader-agnostic within slot).

### 4.3 MCP QUIC transport

- Reuse existing QUIC endpoint patterns (no new gossip socket enums unless proven necessary).
- MCP control messages use `1-byte type + payload` framing:
  - `0x01` RelayAttestation
  - `0x02` ConsensusBlock
- Leader resolution reuses existing leader-schedule lookup and TVU QUIC contact info.
- Dispatch retries are bounded and instrumented; dropped sends must increment explicit counters.
- TVU MCP control ingress channel is bounded; fetch-stage enqueue is non-blocking with explicit full/disconnected drop counters.
- Reject unknown type and oversize frame.

### 4.4 Relay shred broadcast

- Relay retransmits verified MCP shreds to validators over existing TVU fetch UDP sockets.
- Extend retransmit addressing to derive slot/shred-id from MCP wire format when Agave shred-id parsing fails.

### 4.5 Tests

- Window-service partition before Agave shred deserialize.
- Duplicate relay-index behavior (one attestation per relay index).
- Equivocation suppression.
- QUIC payload >1232B accepted by MCP transport path.

---

## Pass 5 — Proposer Pipeline + Forwarding

**Goal:** proposer nodes build MCP shred sets from produced entry batches using existing broadcast flow; non-proposers forward txs to proposers.

### 5.1 Proposer dispatch in broadcast run

- `turbine/src/broadcast_stage/standard_broadcast_run.rs`:
  - use existing broadcast entry-batch flow (no new TPU stage/worker).
  - collect per-slot transaction wire bytes and enforce `MAX_PROPOSER_PAYLOAD` with framing overhead.
  - on slot completion, encode MCP shreds and dispatch to relay schedule over existing QUIC endpoint sender.
  - proposer activation: local pubkey must appear in `proposer_indices_at_slot`.
  - duplicate proposer indices are valid and dispatch once per owned index.

### 5.2 Bankless recording guardrails

- Bankless recording guardrails:
  - record path is explicit opt-in from replay-stage call sites
  - reject if a working bank is installed
  - reject slot-mismatch vs PoH recorder start slot
  - reject malformed inputs (`mixins.len() != transaction_batches.len()` or any empty transaction batch)

### 5.4 TPU proposer worker

**Implementation: `core/src/mcp_proposer.rs` + `core/src/tpu.rs`**

The MCP proposer is **bankless** (no execution, no PoH) but needs to validate transactions and extract ordering fees. This is achieved via `McpProposerContext`, a lightweight struct that extracts only what's needed from a frozen parent bank:

```rust
pub struct McpProposerContext {
    feature_set: Arc<FeatureSet>,
    fee_structure: FeeStructure,
    rent: Rent,
    lamports_per_signature: u64,
    accounts: Arc<Accounts>,        // For loading fee payer balances
    ancestors: Ancestors,
    cost_tracker: RwLock<CostTracker>,  // 1/16th limits
}
```

**Key methods (reuse patterns from consumer.rs:460-493):**
- `validate_fee_payer()` — Extracts compute budget limits, calculates fee, loads fee payer account, validates balance
- `try_add_cost()` — Enforces per-proposer 1/16th CU limits via CostTracker
- `extract_ordering_fee()` — Returns `compute_unit_price` from transaction's compute budget instructions


**Thread flow (If feature is active, `tpu.rs` spawns "solMcpProposer" thread):**
1. Wait for slot where this validator is a proposer (via `leader_schedule_cache.proposer_indices_at_slot()`)
2. Create `McpProposerContext::new(&parent_bank)` — cost tracker initialized with 1/16th limits
3. Receive cloned packets from sigverify (via `mcp_proposer_receiver`)
4. For each packet:
   - Deserialize via `ImmutableDeserializedPacket::new()` pattern
   - Build `RuntimeTransaction` via `build_sanitized_transaction()`
   - Validate fee payer: `context.validate_fee_payer(&tx)` → returns `ordering_fee`
   - Validate cost budget: `context.try_add_cost(&tx)` → rejects if exceeds 1/16th
   - Collect valid transactions as `ValidatedTransaction { transaction, ordering_fee }`
5. Sort by ordering_fee descending (stable sort preserves insertion order for ties)
6. Serialize to `McpPayload` (max `MAX_PROPOSER_PAYLOAD` = `DATA_SHREDS * SHRED_DATA_BYTES`)
7. RS encode via `reed_solomon_erasure::ReedSolomon::new(40, 160)` directly
8. Compute Merkle commitment per spec section 6 using `mcp_merkle_tree()` from `mcp_merkle.rs`
9. Build `McpShred` for each relay index (0..199) with witness + proposer_signature
   - Relay `i` receives shred with `shred_index = i`
   - Relays 0-39 receive data shreds (original payload), relays 40-199 receive coding shreds (RS parity)
10. Look up relay addresses via `relays_at_slot()` + `ClusterInfo::lookup_contact_info()`
11. Send one shred per relay to their TVU address

**MCP transaction format note (PENDING SPEC AMENDMENT):** See SPEC AMENDMENT REQUIREMENT in section 1.2. McpPayload carries standard Solana wire-format transactions. The `ordering_fee` is derived from the existing `compute_unit_price` set via `SetComputeUnitPrice` instruction. Transactions without `SetComputeUnitPrice` get `ordering_fee = 0`.

### 5.5 Forwarding stage changes

- `core/src/forwarding_stage.rs` + `core/src/next_leader.rs`:
  - MCP mode resolves proposer forward addresses using schedule cache and same lookahead slot-offset policy used today.
  - implement fanout behavior for both forwarding clients:
    - `ConnectionCacheClient`: `ForwardAddressGetter::get_non_vote_forwarding_addresses` resolves proposer addresses via `mcp_leader_schedule_cache`
    - `TpuClientNext`: `get_forward_addresses_from_tpu_info` must also resolve MCP proposer addresses when `mcp_protocol_v1` is active, using the same schedule cache lookup
  - preserve non-MCP behavior unchanged.

### 5.6 Explicit non-change

- Do not globally divide BankingStage QoS limits in `core/src/banking_stage/qos_service.rs` in v1.
- Per-proposer limits are handled by proposer admission and replay validation paths.

### 5.7 Per-proposer CU budgets

**Handled directly in `McpProposerContext::new()`:**

The `CostTracker` is initialized with 1/16th of block limits:
```rust
cost_tracker.set_limits(
    account_cost_limit / NUM_PROPOSERS,
    block_cost_limit / NUM_PROPOSERS,
    vote_cost_limit / NUM_PROPOSERS,
);
```

This ensures each proposer's payload doesn't exceed its share. No changes needed to `qos_service.rs` — the per-proposer tracking is local to the MCP proposer thread.

### 5.8 Tests

- MCP dispatch removes completed slot state and emits one shred per relay index per owned proposer index.

- Context created with 1/16th limits
- Rejects tx when payer has no balance
- Correctly extracts compute_unit_price (0 if no SetComputeUnitPrice)
- Transactions sorted descending, ties preserve insertion order
- Proposer produces 200 shreds (one per relay)
- Payload size within `DATA_SHREDS * SHRED_DATA_BYTES` = 34,520 bytes
- RS encode -> decode round-trip
- TransactionSigVerifier clones to MCP proposer channel
- Proposer worker emits exactly one shred per relay index.
- Payload bound enforcement.
- Forwarding routes to proposer addresses in MCP mode for both forwarding clients.

## Pass 6 — Leader Aggregation + ConsensusBlock

**Goal:** aggregate relay attestations and distribute leader-signed `ConsensusBlock`.

### 6.1 Control-path ingestion in window service

- `core/src/shred_fetch_stage.rs`:
  - classify MCP control frames (`0x01` relay attestation, `0x02` consensus block) and forward to TVU control channel.
- `core/src/window_service.rs`:
  - ingest and validate control frames with slot-effective feature gating.
  - store validated relay attestations in MCP CF.
  - store validated consensus blocks in shared in-memory slot map for replay consumption.

Validation rules:
- invalid relay signature => drop relay message
- invalid proposer signature inside relay entry => drop that entry, keep other valid entries
- canonical aggregate ordering by `relay_index`
- enforce aggregate and consensus wire-size upper bounds before decode (including QUIC control payload cap)
- threshold counting rule => count distinct `relay_index` entries that pass relay-signature/index validation and retain at least one valid proposer entry after proposer filtering
- relay entries that become empty after proposer-signature filtering are dropped and MUST NOT count toward attestation thresholds

### 6.2 Leader finalization

When this node is leader for slot `s` and attestation quorum is present:
1. filter invalid entries as above
2. if valid relay entries < `REQUIRED_ATTESTATIONS` -> do not finalize/broadcast a consensus block
3. build `AggregateAttestation`
4. construct/sign `ConsensusBlock` with:
   - `consensus_meta` and authoritative `block_id` from Alpenglow consensus (`B3`)
   - `delayed_bankhash` for the consensus-defined delayed slot
   - delayed-bankhash lookup order: delayed-slot `BankForks` entry, then blockstore frozen bank hash
   - if delayed-bankhash is unavailable, keep slot in a pending-finalization set and retry each insert loop until available or rooted-out

Implementation point:
- `core/src/window_service.rs::maybe_finalize_and_broadcast_mcp_consensus_block`

### 6.3 Distribution

- Leader broadcasts `ConsensusBlock` (`0x02` control frame) to TVU QUIC peers using existing turbine QUIC endpoint sender.

### 6.4 Replay consumption

- `core/src/replay_stage.rs` + `core/src/mcp_replay.rs`:
  - refresh per-slot `VoteGateInput` from validated consensus-block bytes before MCP vote-gate evaluation.
  - retain and retry pending MCP consensus slots in replay loop so evaluation does not depend on one-shot heaviest-fork selection timing.

### 6.5 Tests

- threshold behavior (`< REQUIRED_ATTESTATIONS` => empty)
- relay/proposer filtering semantics
- canonical aggregate ordering
- direct QUIC block distribution + frame typing

---

## Pass 7 — Vote Gate + Reconstruct + Replay + Fees

**Goal:** deterministic vote gate, reconstruction, and two-phase fee-safe execution.

### 7.1 Vote gate on `ConsensusBlock`

- `core/src/mcp_replay.rs` (called from replay loop):
  1. verify leader signature and leader index for slot
  2. verify delayed bankhash by consensus-defined delayed slot
     - if consensus block for slot is missing, keep slot pending and retry
     - if local delayed bankhash is unavailable, keep slot pending and retry
  3. verify relay/proposer signatures and ignore invalid entries
     - drop relay entries that become empty after proposer-signature filtering
  4. enforce global relay threshold using the same relay-count rule from Pass 6 (`>= REQUIRED_ATTESTATIONS`)
  5. implied proposer rules:
     - multiple commitments => exclude
     - one commitment with `>= REQUIRED_INCLUSIONS` relay attestations => include
  6. local availability check:
     - count only locally stored shreds whose witness validates against included commitment
     - require `>= REQUIRED_RECONSTRUCTION` per included proposer

Staged rollout guard:
- replay-stage vote-gate lookups must be non-consuming; repeated evaluations for the same slot must see identical gate input.
- critical-path map updates must never silently fail on lock contention/poisoning; all failures are explicit and logged.

### 7.2 Reconstruction

- For each included proposer:
  - load shreds from MCP CF
  - decode + re-encode + recompute commitment via ledger helper
  - discard proposer batch on commitment mismatch
  - commitment mismatch may poison current attempt, but next shard insert must reset state (no permanent poison for slot/proposer)
  - decode payload tx list with dual-format `B1` parser (latest + legacy)

### 7.3 Ordering and execution

- Deterministic order:
  - concat by proposer index
  - apply `ordering_fee` descending (highest fee first)
  - stable tie-break by concatenated position
  - duplicate transactions are not deduplicated in v1; each occurrence executes independently in deterministic order

- Replay execution wiring:
  - `ledger/src/blockstore_processor.rs::execute_batch` uses MCP two-pass path only when both are true:
    - block-verification execution path
    - `mcp_protocol_v1` is slot-effective active
  - Phase A calls `Bank::collect_fees_only_for_transactions`.
  - Phase B calls `Bank::load_execute_and_commit_transactions_skip_fee_collection_with_pre_commit_callback`.

Two-phase fee handling (spec §8):
- Phase A (fee commitment):
  - per-transaction checks for signature/basic validity
  - per-slot cumulative payer map in memory
  - required debit:
    - standard tx: `base_fee * NUM_PROPOSERS`
    - nonce tx: `base_fee * NUM_PROPOSERS + nonce_min_rent`
  - debit implementation:
    - standard tx: `Bank::withdraw()`
    - nonce tx: dedicated MCP nonce helper to debit the precomputed amount exactly once
  - drop only failing transaction; continue others
- Phase B (execution):
  - execute filtered tx list with fee deduction disabled
  - no second fee charge

Implementation wiring:
- keep default non-MCP execution path unchanged
- use explicit fee-mode control already plumbed through SVM (`skip_fee_collection`)
- apply phase-A fee failures as execution filters for phase B
- add explicit multi-node integration coverage for this path (single-node leader-only tests do not exercise `block_verification=true`)

Reconstruction-to-execution bridge:
- `McpExecutionOutput` stored via `put_mcp_execution_output()` MUST have a production reader
- during block-verification replay (`block_verification=true`) when `mcp_protocol_v1` is active:
  - read `McpExecutionOutput` for the slot from blockstore
  - deserialize the MCP-ordered transaction list
  - if present and valid, use it as canonical replay transaction input (replacing legacy entry-derived transaction entries)
  - malformed `McpExecutionOutput` is a hard replay error for the slot
  - if missing, fall back to legacy entry-derived replay input in v1 (temporary compatibility mode)
- during leader execution (`block_verification=false`):
  - legacy banking-stage path continues unchanged; MCP reconstruction does not apply to the leader's own execution
  - the leader's MCP execution output is still persisted for verification by other nodes
- strict no-fallback mode requires moving `McpExecutionOutput` production to a pre-execution point; current replay pipeline produces it post-execution

### 7.4 Bank/block ID and vote

**DEFERRED — requires Alpenglow consensus integration.**

- Per policy B3, `block_id` is defined by Alpenglow consensus and MCP MUST treat it as authoritative.
- `ConsensusBlock` currently carries `delayed_bankhash` and `consensus_meta` but does NOT carry an explicit `block_id` field, because `block_id` derivation depends on Alpenglow consensus logic that is not yet integrated.
- Prerequisites before this section can be implemented:
  1. Alpenglow consensus provides a `block_id` value (or the derivation rule) in `ConsensusBlock.consensus_meta`
  2. `Bank` exposes a setter for `block_id` that can be called after vote-gate validation
  3. Vote submission path uses `block_id` instead of (or in addition to) legacy bank hash
- Until prerequisites are met:
  - validators vote on the legacy bank hash (derived from Pipeline A entry execution)
  - `ConsensusBlock.consensus_meta` is opaque bytes, verified for presence but not interpreted
  - MCP acts as a consensus overlay that gates voting, not as the authority for block identity
- underlying vote wire format remains unchanged.

### 7.5 Empty result

- if consensus outputs empty result for slot, record empty MCP execution output for the slot.
- if a local slot bank exists with a different `block_id`, treat it as fork mismatch and do not record empty output for that slot.

### 7.6 Tests

- vote-gate rejections: bad leader sig/index, bad delayed bankhash, equivocation, insufficient local valid shreds, global threshold failure
- delayed bankhash availability gate: leader does not finalize and validator does not vote until delayed-slot bankhash is available locally
- reconstruction mismatch rejection
- deterministic ordering behavior
- execution-output reader behavior: strict decode for malformed payloads; MCP-wire (latest/legacy) and standard-wire transactions decode into replay input
- dual-format payload acceptance: mixed latest + legacy tx bytes decode correctly
- producer serialization preference: latest format emitted when available, legacy retained otherwise
- two-phase fees:
  - per-transaction granularity
  - cross-proposer cumulative payer accounting
  - nonce minimum-rent edge case
  - phase-B no re-deduction
- issue-20 single-node integration:
  - strict timeout failure by default (no soft-pass env toggle)
  - use live validator blockstore handles during runtime
  - assert all three artifacts are observed: relay attestation, MCP shreds, MCP execution output
- additional multi-node integration:
  - verify proposer forwarding fanout and non-leader replay path for MCP two-pass fees when 2+ validators are live
  - in constrained environments that cannot allocate enough validator UDP ports, skip only the non-leader assertion and keep artifact checks strict

---

## Modified Files (expected)

New files:
- `ledger/src/mcp.rs`
- `ledger/src/mcp_merkle.rs`
- `transaction-view/src/mcp_payload.rs`
- `ledger/src/shred/mcp_shred.rs`
- `core/src/mcp_replay.rs`

Modified files:
- `feature-set/src/lib.rs`
- `ledger/src/leader_schedule.rs`
- `ledger/src/leader_schedule_cache.rs`
- `ledger/src/leader_schedule_utils.rs`
- `ledger/src/blockstore/column.rs`
- `ledger/src/blockstore_db.rs`
- `ledger/src/blockstore.rs`
- `ledger/src/blockstore/blockstore_purge.rs`
- `turbine/src/sigverify_shreds.rs`
- `turbine/src/retransmit_stage.rs`
- `turbine/src/quic_endpoint.rs`
- `turbine/src/broadcast_stage/standard_broadcast_run.rs`
- `core/src/shred_fetch_stage.rs`
- `core/src/window_service.rs`
- `core/src/forwarding_stage.rs`
- `core/src/next_leader.rs`
- `core/src/tvu.rs`
- `core/src/replay_stage.rs`
- `core/src/block_creation_loop.rs`
- `core/src/mcp_vote_gate.rs`
- `core/src/mcp_relay.rs`
- `core/src/mcp_relay_submit.rs`
- `ledger/src/blockstore_processor.rs`
- `runtime/src/bank.rs` (MCP-specific execution entrypoint plumbing)
- `transaction-view/src/lib.rs`
- `transaction-view/src/mcp_transaction.rs`
- `svm/src/transaction_processor.rs` (fee-mode plumbing)
- `local-cluster/tests/local_cluster.rs`

---

## Dependency Order

- Passes 1, 2, and 3.1/3.2 can proceed in parallel.
- Pass 3.3 depends on 1 + 2.
- Pass 4 depends on 1 + 2 + 3.
- Pass 5 depends on 1 + 2 + 4.
- Pass 6 depends on 1 + 2 + 4.
- Pass 7 depends on 1 + 2 + 3 + 6.

## Acceptance Invariants

- No MCP packet is parsed by Agave shred wire parsers.
- No MCP state transition occurs without feature gate active.
- All threshold checks use ceil-threshold logic with `NUM_RELAYS=200`.
- Replay and vote gate use the same attestation filtering rules.
- Unknown versions, unsorted entries, duplicate entries, invalid signatures/proofs are rejected deterministically.
- Delayed-bankhash gating is strict: no leader finalization or validator vote progression without local delayed-slot bankhash availability.
- Pending-slot retry paths are deterministic in both window-service finalization and replay vote-gate evaluation.
- Critical MCP shared-map lock failures are never silently ignored; failures are explicit and logged.
- MCP control ingress is bounded; enqueue drops are explicit and counted.
- `McpExecutionOutput` decode failures are explicit replay errors (never silently ignored).
- Cross-crate MCP constants remain aligned via dedicated consistency tests.