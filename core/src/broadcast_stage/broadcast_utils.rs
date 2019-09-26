use crate::entry::Entry;
use crate::poh_recorder::WorkingBankEntry;
use crate::result::Result;
use crate::shred::{Shred, Shredder, RECOMMENDED_FEC_RATE};
use solana_runtime::bank::Bank;
use solana_sdk::signature::Keypair;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) struct ReceiveResults {
    pub entries: Vec<Entry>,
    pub time_elapsed: Duration,
    pub bank: Arc<Bank>,
    pub last_tick: u64,
}

/// Theis parameter tunes how many entries are received in one iteration of recv loop
/// This will prevent broadcast stage from consuming more entries, that could have led
/// to delays in shredding, and broadcasting shreds to peer validators
const RECEIVE_ENTRY_COUNT_THRESHOLD: usize = 8;

pub(super) fn recv_slot_entries(receiver: &Receiver<WorkingBankEntry>) -> Result<ReceiveResults> {
    let timer = Duration::new(1, 0);
    let (mut current_bank, (entry, mut last_tick)) = receiver.recv_timeout(timer)?;
    let recv_start = Instant::now();

    let mut entries = vec![entry];
    let mut slot = current_bank.slot();
    let mut max_tick_height = current_bank.max_tick_height();

    assert!(last_tick <= max_tick_height);

    if last_tick != max_tick_height {
        while let Ok((bank, (entry, tick_height))) = receiver.try_recv() {
            // If the bank changed, that implies the previous slot was interrupted and we do not have to
            // broadcast its entries.
            if bank.slot() != slot {
                info!("Abandoning bank: {}", slot);
                entries.clear();
                slot = bank.slot();
                max_tick_height = bank.max_tick_height();
                current_bank = bank;
            }
            last_tick = tick_height;
            entries.push(entry);

            if entries.len() >= RECEIVE_ENTRY_COUNT_THRESHOLD {
                break;
            }

            assert!(last_tick <= max_tick_height);
            if last_tick == max_tick_height {
                break;
            }
        }
    }

    let time_elapsed = recv_start.elapsed();
    Ok(ReceiveResults {
        entries,
        time_elapsed,
        bank: current_bank,
        last_tick,
    })
}

pub(super) fn entries_to_shreds(
    entries: Vec<Entry>,
    last_tick: u64,
    slot: u64,
    bank_max_tick: u64,
    keypair: &Arc<Keypair>,
    latest_shred_index: u64,
    parent_slot: u64,
) -> (Vec<Shred>, u64) {
    let mut shredder = Shredder::new(
        slot,
        parent_slot,
        RECOMMENDED_FEC_RATE,
        keypair,
        latest_shred_index as u32,
    )
    .expect("Expected to create a new shredder");

    bincode::serialize_into(&mut shredder, &entries)
        .expect("Expect to write all entries to shreds");

    if last_tick == bank_max_tick {
        shredder.finalize_slot();
    } else {
        shredder.finalize_data();
    }

    let shred_infos: Vec<Shred> = shredder.shreds.drain(..).collect();

    trace!("Inserting {:?} shreds in blocktree", shred_infos.len());

    (shred_infos, u64::from(shredder.index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::genesis_utils::{create_genesis_block, GenesisBlockInfo};
    use solana_sdk::genesis_block::GenesisBlock;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::system_transaction;
    use solana_sdk::transaction::Transaction;
    use std::sync::mpsc::channel;

    fn setup_test() -> (GenesisBlock, Arc<Bank>, Transaction) {
        let GenesisBlockInfo {
            genesis_block,
            mint_keypair,
            ..
        } = create_genesis_block(2);
        let bank0 = Arc::new(Bank::new(&genesis_block));
        let tx = system_transaction::create_user_account(
            &mint_keypair,
            &Pubkey::new_rand(),
            1,
            genesis_block.hash(),
        );

        (genesis_block, bank0, tx)
    }

    #[test]
    fn test_recv_slot_entries_1() {
        let (genesis_block, bank0, tx) = setup_test();

        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &Pubkey::default(), 1));
        let (s, r) = channel();
        let mut last_hash = genesis_block.hash();

        assert!(bank1.max_tick_height() > 1);
        let entries: Vec<_> = (0..bank1.max_tick_height() + 1)
            .map(|i| {
                let entry = Entry::new(&last_hash, 1, vec![tx.clone()]);
                last_hash = entry.hash;
                s.send((bank1.clone(), (entry.clone(), i))).unwrap();
                entry
            })
            .collect();

        let result = recv_slot_entries(&r).unwrap();

        assert_eq!(result.bank.slot(), bank1.slot());
        assert_eq!(result.last_tick, bank1.max_tick_height());
        assert_eq!(result.entries, entries);
    }

    #[test]
    fn test_recv_slot_entries_2() {
        let (genesis_block, bank0, tx) = setup_test();

        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &Pubkey::default(), 1));
        let bank2 = Arc::new(Bank::new_from_parent(&bank1, &Pubkey::default(), 2));
        let (s, r) = channel();

        let mut last_hash = genesis_block.hash();
        assert!(bank1.max_tick_height() > 1);
        // Simulate slot 2 interrupting slot 1's transmission
        let expected_last_index = bank1.max_tick_height() - 1;
        let last_entry = (0..bank1.max_tick_height())
            .map(|i| {
                let entry = Entry::new(&last_hash, 1, vec![tx.clone()]);
                last_hash = entry.hash;
                // Interrupt slot 1 right before the last tick
                if i == expected_last_index {
                    s.send((bank2.clone(), (entry.clone(), i))).unwrap();
                    Some(entry)
                } else {
                    s.send((bank1.clone(), (entry, i))).unwrap();
                    None
                }
            })
            .last()
            .unwrap()
            .unwrap();

        let result = recv_slot_entries(&r).unwrap();
        assert_eq!(result.bank.slot(), bank2.slot());
        assert_eq!(result.last_tick, expected_last_index);
        assert_eq!(result.entries, vec![last_entry]);
    }
}
