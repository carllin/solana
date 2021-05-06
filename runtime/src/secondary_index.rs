use dashmap::{mapref::entry::Entry::Occupied, DashMap};
use solana_sdk::pubkey::Pubkey;
use std::{boxed::Box, collections::HashSet, fmt::Debug, sync::RwLock};

// The only cases where an inner key should map to a different outer key is
// if the key had different account data for the indexed key across different
// slots. As this is rare, it should be ok to use a Vec here over a HashSet, even
// though we are running some key existence checks.
pub type SecondaryReverseIndexEntry = RwLock<Vec<Box<Pubkey>>>;

pub trait SecondaryIndexEntry: Debug {
    fn insert_if_not_exists(&self, key: &Box<Pubkey>);
    // Removes a value from the set. Returns whether the value was present in the set.
    fn remove_inner_key(&self, key: &Pubkey) -> bool;
    fn is_empty(&self) -> bool;
    fn keys(&self) -> Vec<Pubkey>;
    fn len(&self) -> usize;
}

#[derive(Debug, Default)]
pub struct DashMapSecondaryIndexEntry {
    account_keys: DashMap<Box<Pubkey>, ()>,
}

impl SecondaryIndexEntry for DashMapSecondaryIndexEntry {
    fn insert_if_not_exists(&self, key: &Box<Pubkey>) {
        self.account_keys.get(key).unwrap_or_else(|| {
            self.account_keys
                .entry(key.clone())
                .or_default()
                .downgrade()
        });
    }

    fn remove_inner_key(&self, key: &Pubkey) -> bool {
        self.account_keys.remove(key).is_some()
    }

    fn is_empty(&self) -> bool {
        self.account_keys.is_empty()
    }

    fn keys(&self) -> Vec<Pubkey> {
        self.account_keys
            .iter()
            .map(|entry_ref| **entry_ref.key())
            .collect()
    }

    fn len(&self) -> usize {
        self.account_keys.len()
    }
}

#[derive(Debug, Default)]
pub struct RwLockSecondaryIndexEntry {
    account_keys: RwLock<HashSet<Box<Pubkey>>>,
}

impl SecondaryIndexEntry for RwLockSecondaryIndexEntry {
    fn insert_if_not_exists(&self, key: &Box<Pubkey>) {
        let exists = self.account_keys.read().unwrap().contains(key);
        if !exists {
            self.account_keys.write().unwrap().insert(key.clone());
        };
    }

    fn remove_inner_key(&self, key: &Pubkey) -> bool {
        self.account_keys.write().unwrap().remove(key)
    }

    fn is_empty(&self) -> bool {
        self.account_keys.read().unwrap().is_empty()
    }

    fn keys(&self) -> Vec<Pubkey> {
        self.account_keys
            .read()
            .unwrap()
            .iter()
            .map(|k| **k)
            .collect()
    }

    fn len(&self) -> usize {
        self.account_keys.read().unwrap().len()
    }
}

#[derive(Debug, Default)]
pub struct SecondaryIndex<SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send> {
    // Map from index keys to index values
    pub index: DashMap<Box<Pubkey>, SecondaryIndexEntryType>,
    pub reverse_index: DashMap<Box<Pubkey>, SecondaryReverseIndexEntry>,
}

impl<SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send>
    SecondaryIndex<SecondaryIndexEntryType>
{
    pub fn print_size(&self) {
        let primary_index_num_keys = self.index.len();
        let reverse_index_num_keys = self.reverse_index.len();

        println!(
            "Secondary Index size: primary_index_num_keys: {}
            reverse_index_num_keys: {}",
            primary_index_num_keys, reverse_index_num_keys,
        );
    }

    pub fn insert(&self, key: &Pubkey, inner_key: &Pubkey) {
        let (boxed_inner_key, should_insert_into_reverse_index): (Box<Pubkey>, bool) = {
            let locked_ref = self.reverse_index.get(inner_key).unwrap_or_else(|| {
                self.reverse_index
                    .entry(Box::new(*inner_key))
                    .or_insert(RwLock::new(Vec::with_capacity(1)))
                    .downgrade()
            });
            let should_insert_into_reverse_index =
                !locked_ref.read().unwrap().iter().any(|k| **k == *key);
            (locked_ref.key().clone(), should_insert_into_reverse_index)
        };

        let boxed_key = {
            let locked_ref = self.index.get(key).unwrap_or_else(|| {
                self.index
                    .entry(Box::new(*key))
                    .or_insert(SecondaryIndexEntryType::default())
                    .downgrade()
            });

            locked_ref.insert_if_not_exists(&boxed_inner_key);
            locked_ref.key().clone()
        };

        let outer_keys_list = self.reverse_index.get(&boxed_inner_key).unwrap_or_else(|| {
            self.reverse_index
                .entry(Box::new(*inner_key))
                .or_insert(RwLock::new(Vec::with_capacity(1)))
                .downgrade()
        });

        if should_insert_into_reverse_index {
            let mut w_outer_keys_list = outer_keys_list.write().unwrap();
            let exists = w_outer_keys_list.iter().any(|k| **k == *key);
            if !exists {
                w_outer_keys_list.push(boxed_key);
            }
        }
    }

    // Only safe to call from `remove_by_inner_key()` due to asserts
    fn remove_index_entries(&self, outer_key: Box<Pubkey>, removed_inner_key: &Pubkey) {
        let is_outer_key_empty = {
            let inner_key_map = self
                .index
                .get_mut(&outer_key)
                .expect("If we're removing a key, then it must have an entry in the map");
            // If we deleted a pubkey from the reverse_index, then the corresponding entry
            // better exist in this index as well or the two indexes are out of sync!
            assert!(inner_key_map.value().remove_inner_key(removed_inner_key));
            inner_key_map.is_empty()
        };

        // Delete the `key` if the set of inner keys is empty
        if is_outer_key_empty {
            // Other threads may have interleaved writes to this `key`,
            // so double-check again for its emptiness
            if let Occupied(key_entry) = self.index.entry(outer_key) {
                if key_entry.get().is_empty() {
                    key_entry.remove();
                }
            }
        }
    }

    pub fn remove_by_inner_key(&self, inner_key: &Pubkey) {
        // Save off which keys in `self.index` had slots removed so we can remove them
        // after we purge the reverse index
        let mut removed_outer_keys: HashSet<Box<Pubkey>> = HashSet::new();

        // Check if the entry for `inner_key` in the reverse index is empty
        // and can be removed
        if let Some((_, outer_keys_set)) = self.reverse_index.remove(inner_key) {
            for removed_outer_key in outer_keys_set.into_inner().unwrap().into_iter() {
                removed_outer_keys.insert(removed_outer_key);
            }
        }

        // Remove this value from those keys
        for outer_key in removed_outer_keys {
            self.remove_index_entries(outer_key, inner_key);
        }
    }

    pub fn get(&self, key: &Pubkey) -> Vec<Pubkey> {
        if let Some(inner_keys_map) = self.index.get(key) {
            inner_keys_map.keys()
        } else {
            vec![]
        }
    }
}
