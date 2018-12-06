//! The `db_ledger` module provides functions for parallel verification of the
//! Proof of History ledger as well as iterative read, append write, and random
//! access read to a persistent file-based ledger.

use crate::entry::Entry;
use crate::packet::{Blob, SharedBlob, BLOB_HEADER_SIZE};
use crate::result::{Error, Result};
use bincode::{deserialize, serialize};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use rocksdb::{ColumnFamily, DBRawIterator, Options, WriteBatch, DB};
use serde::de::DeserializeOwned;
use serde::Serialize;
use solana_sdk::signature::{Keypair, KeypairUtil};
use std::borrow::Borrow;
use std::io;
use std::path::Path;

pub const DB_LEDGER_DIRECTORY: &str = "rocksdb";

#[derive(Debug, PartialEq, Eq)]
pub enum DbLedgerError {
    BlobForIndexExists,
    InvalidBlobData,
}

pub trait LedgerColumnFamily {
    type ValueType: DeserializeOwned + Serialize;

    fn get(&self, db: &DB, key: &[u8]) -> Result<Option<Self::ValueType>> {
        let data_bytes = db.get_cf(self.handle(db), key)?;

        if let Some(raw) = data_bytes {
            let result: Self::ValueType = deserialize(&raw)?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    fn get_bytes(&self, db: &DB, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let data_bytes = db.get_cf(self.handle(db), key)?;
        Ok(data_bytes.map(|x| x.to_vec()))
    }

    fn put_bytes(&self, db: &DB, key: &[u8], serialized_value: &[u8]) -> Result<()> {
        db.put_cf(self.handle(db), &key, &serialized_value)?;
        Ok(())
    }

    fn put(&self, db: &DB, key: &[u8], value: &Self::ValueType) -> Result<()> {
        let serialized = serialize(value)?;
        db.put_cf(self.handle(db), &key, &serialized)?;
        Ok(())
    }

    fn delete(&self, db: &DB, key: &[u8]) -> Result<()> {
        db.delete_cf(self.handle(db), &key)?;
        Ok(())
    }

    fn handle(&self, db: &DB) -> ColumnFamily;
}

pub trait LedgerColumnFamilyRaw {
    fn get(&self, db: &DB, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let data_bytes = db.get_cf(self.handle(db), key)?;
        Ok(data_bytes.map(|x| x.to_vec()))
    }

    fn put(&self, db: &DB, key: &[u8], serialized_value: &[u8]) -> Result<()> {
        db.put_cf(self.handle(db), &key, &serialized_value)?;
        Ok(())
    }

    fn delete(&self, db: &DB, key: &[u8]) -> Result<()> {
        db.delete_cf(self.handle(db), &key)?;
        Ok(())
    }

    fn handle(&self, db: &DB) -> ColumnFamily;
}

#[derive(Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
// The Meta column family
pub struct SlotMeta {
    // The total number of consecutive blob starting from index 0
    // we have received for this slot.
    pub consumed: u64,
    // The entry height of the highest blob received for this slot.
    pub received: u64,
    // The slot the blob with index == "consumed" is in
    pub consumed_slot: u64,
    // The slot the blob with index == "received" is in
    pub received_slot: u64,
}

impl SlotMeta {
    fn new() -> Self {
        SlotMeta {
            consumed: 0,
            received: 0,
            consumed_slot: 0,
            received_slot: 0,
        }
    }
}

#[derive(Default)]
pub struct MetaCf {}

impl MetaCf {
    pub fn key(slot_height: u64) -> Vec<u8> {
        let mut key = vec![0u8; 8];
        BigEndian::write_u64(&mut key[0..8], slot_height);
        key
    }
}

impl LedgerColumnFamily for MetaCf {
    type ValueType = SlotMeta;

    fn handle(&self, db: &DB) -> ColumnFamily {
        db.cf_handle(META_CF).unwrap()
    }
}

// The data column family
#[derive(Default)]
pub struct DataCf {}

impl DataCf {
    pub fn get_by_slot_index(
        &self,
        db: &DB,
        slot_height: u64,
        index: u64,
    ) -> Result<Option<Vec<u8>>> {
        let key = Self::key(slot_height, index);
        self.get(db, &key)
    }

    pub fn put_by_slot_index(
        &self,
        db: &DB,
        slot_height: u64,
        index: u64,
        serialized_value: &[u8],
    ) -> Result<()> {
        let key = Self::key(slot_height, index);
        self.put(db, &key, serialized_value)
    }

    pub fn key(slot_height: u64, index: u64) -> Vec<u8> {
        let mut key = vec![0u8; 16];
        BigEndian::write_u64(&mut key[0..8], slot_height);
        BigEndian::write_u64(&mut key[8..16], index);
        key
    }

    pub fn slot_height_from_key(key: &[u8]) -> Result<u64> {
        let mut rdr = io::Cursor::new(&key[0..8]);
        let height = rdr.read_u64::<BigEndian>()?;
        Ok(height)
    }

    pub fn index_from_key(key: &[u8]) -> Result<u64> {
        let mut rdr = io::Cursor::new(&key[8..16]);
        let index = rdr.read_u64::<BigEndian>()?;
        Ok(index)
    }
}

impl LedgerColumnFamilyRaw for DataCf {
    fn handle(&self, db: &DB) -> ColumnFamily {
        db.cf_handle(DATA_CF).unwrap()
    }
}

// The erasure column family
#[derive(Default)]
pub struct ErasureCf {}

impl ErasureCf {
    pub fn delete_by_slot_index(&self, db: &DB, slot_height: u64, index: u64) -> Result<()> {
        let key = Self::key(slot_height, index);
        self.delete(db, &key)
    }

    pub fn get_by_slot_index(
        &self,
        db: &DB,
        slot_height: u64,
        index: u64,
    ) -> Result<Option<Vec<u8>>> {
        let key = Self::key(slot_height, index);
        self.get(db, &key)
    }

    pub fn put_by_slot_index(
        &self,
        db: &DB,
        slot_height: u64,
        index: u64,
        serialized_value: &[u8],
    ) -> Result<()> {
        let key = Self::key(slot_height, index);
        self.put(db, &key, serialized_value)
    }

    pub fn key(slot_height: u64, index: u64) -> Vec<u8> {
        DataCf::key(slot_height, index)
    }

    pub fn slot_height_from_key(key: &[u8]) -> Result<u64> {
        DataCf::slot_height_from_key(key)
    }

    pub fn index_from_key(key: &[u8]) -> Result<u64> {
        DataCf::index_from_key(key)
    }
}

impl LedgerColumnFamilyRaw for ErasureCf {
    fn handle(&self, db: &DB) -> ColumnFamily {
        db.cf_handle(ERASURE_CF).unwrap()
    }
}

// ledger window
pub struct DbLedger {
    // Underlying database is automatically closed in the Drop implementation of DB
    pub db: DB,
    pub meta_cf: MetaCf,
    pub data_cf: DataCf,
    pub erasure_cf: ErasureCf,
}

// TODO: Once we support a window that knows about different leader
// slots, change functions where this is used to take slot height
// as a variable argument
pub const DEFAULT_SLOT_HEIGHT: u64 = 0;
// Column family for metadata about a leader slot
pub const META_CF: &str = "meta";
// Column family for the data in a leader slot
pub const DATA_CF: &str = "data";
// Column family for erasure data
pub const ERASURE_CF: &str = "erasure";

impl DbLedger {
    // Opens a Ledger in directory, provides "infinite" window of blobs
    pub fn open(ledger_path: &str) -> Result<Self> {
        let ledger_path = Path::new(ledger_path).join(DB_LEDGER_DIRECTORY);

        // Use default database options
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Column family names
        let cfs = vec![META_CF, DATA_CF, ERASURE_CF];

        // Open the database
        let db = DB::open_cf(&options, ledger_path, &cfs)?;

        // Create the metadata column family
        let meta_cf = MetaCf::default();

        // Create the data column family
        let data_cf = DataCf::default();

        // Create the erasure column family
        let erasure_cf = ErasureCf::default();

        Ok(DbLedger {
            db,
            meta_cf,
            data_cf,
            erasure_cf,
        })
    }

    pub fn destroy(ledger_path: &str) -> Result<()> {
        let ledger_path = Path::new(ledger_path).join(DB_LEDGER_DIRECTORY);
        DB::destroy(&Options::default(), &ledger_path)?;
        Ok(())
    }

    pub fn write_shared_blobs<I>(&mut self, slot: u64, shared_blobs: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<SharedBlob>,
    {
        let mut entries = vec![];
        for b in shared_blobs {
            let bl = b.borrow().read().unwrap();
            let index = bl.index()?;
            let key = DataCf::key(slot, index);
            let new_entries = self.insert_data_blob(&key, &*bl)?;
            entries.extend(new_entries);
        }
        Ok(entries)
    }

    pub fn write_blobs<'a, I>(&mut self, slot: u64, blobs: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator<Item = &'a &'a Blob>,
    {
        let mut entries = vec![];
        for blob in blobs.into_iter() {
            let index = blob.index()?;
            let key = DataCf::key(slot, index);
            let new_entries = self.insert_data_blob(&key, blob)?;
            entries.extend(new_entries);
        }
        Ok(entries)
    }

    pub fn write_entries<I>(&mut self, slot: u64, entries: I) -> Result<Vec<Entry>>
    where
        I: IntoIterator,
        I::Item: Borrow<Entry>,
    {
        let shared_blobs = entries
            .into_iter()
            .enumerate()
            .map(|(idx, entry)| entry.borrow().to_blob(Some(idx as u64), None, None));
        self.write_shared_blobs(slot, shared_blobs)
    }

    pub fn insert_data_blob(&self, key: &[u8], new_blob: &Blob) -> Result<Vec<Entry>> {
        let slot_height = DataCf::slot_height_from_key(key)?;
        let meta_key = MetaCf::key(DEFAULT_SLOT_HEIGHT);

        let mut should_write_meta = false;

        let mut meta = {
            if let Some(meta) = self.db.get_cf(self.meta_cf.handle(&self.db), &meta_key)? {
                deserialize(&meta)?
            } else {
                should_write_meta = true;
                SlotMeta::new()
            }
        };

        let mut index = DataCf::index_from_key(key)?;

        // TODO: Handle if leader sends different blob for same index when the index > consumed
        // The old window implementation would just replace that index.
        if index < meta.consumed {
            return Err(Error::DbLedgerError(DbLedgerError::BlobForIndexExists));
        }

        // Index is zero-indexed, while the "received" height starts from 1,
        // so received = index + 1 for the same blob.
        if index >= meta.received {
            meta.received = index + 1;
            meta.received_slot = slot_height;
            should_write_meta = true;
        }

        let mut consumed_queue = vec![];

        if meta.consumed == index {
            // Add the new blob to the consumed queue
            let serialized_entry_data =
                &new_blob.data[BLOB_HEADER_SIZE..BLOB_HEADER_SIZE + new_blob.size()?];
            // Verify entries can actually be reconstructed
            let entry: Entry = deserialize(serialized_entry_data)
                .expect("Blob made it past validation, so must be deserializable at this point");
            should_write_meta = true;
            meta.consumed += 1;
            consumed_queue.push(entry);
            // Find the next consecutive block of blobs.
            // TODO: account for consecutive blocks that
            // span multiple slots

            let mut current_slot = slot_height;
            loop {
                index += 1;
                let key = DataCf::key(current_slot, index);
                let blob_data = {
                    if let Some(blob_data) = self.data_cf.get(&self.db, &key)? {
                        blob_data
                    } else if meta.consumed < meta.received {
                        let key = DataCf::key(current_slot + 1, index);
                        if let Some(blob_data) = self.data_cf.get(&self.db, &key)? {
                            current_slot += 1;
                            meta.consumed_slot = current_slot;
                            blob_data
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                };

                let serialized_entry_data = &blob_data[BLOB_HEADER_SIZE..];
                let entry: Entry = deserialize(serialized_entry_data)
                    .expect("Ledger should only contain well formed data");
                consumed_queue.push(entry);
                meta.consumed += 1;
            }
        }

        // Commit Step: Atomic write both the metadata and the data
        let mut batch = WriteBatch::default();
        if should_write_meta {
            batch.put_cf(self.meta_cf.handle(&self.db), &meta_key, &serialize(&meta)?)?;
        }

        let serialized_blob_data = &new_blob.data[..BLOB_HEADER_SIZE + new_blob.size()?];
        batch.put_cf(self.data_cf.handle(&self.db), key, serialized_blob_data)?;
        self.db.write(batch)?;
        Ok(consumed_queue)
    }

    // Fill 'buf' with num_blobs or most number of consecutive
    // whole blobs that fit into buf.len()
    //
    // Return tuple of (number of blob read, total size of blobs read)
    pub fn get_blob_bytes(
        &mut self,
        start_index: u64,
        num_blobs: u64,
        buf: &mut [u8],
        slot_height: u64,
    ) -> Result<(u64, u64)> {
        let start_key = DataCf::key(slot_height, start_index);
        let mut db_iterator = self.db.raw_iterator_cf(self.data_cf.handle(&self.db))?;
        db_iterator.seek(&start_key);
        let mut total_blobs = 0;
        let mut total_current_size = 0;
        for expected_index in start_index..start_index + num_blobs {
            if !db_iterator.valid() {
                if expected_index == start_index {
                    return Err(Error::IO(io::Error::new(
                        io::ErrorKind::NotFound,
                        "Blob at start_index not found",
                    )));
                } else {
                    break;
                }
            }

            // Check key is the next sequential key based on
            // blob index
            let key = &db_iterator.key().expect("Expected valid key");
            let index = DataCf::index_from_key(key)?;
            if index != expected_index {
                break;
            }

            // Get the blob data
            let value = &db_iterator.value();

            if value.is_none() {
                break;
            }

            let value = value.as_ref().unwrap();
            let blob_data_len = value.len();

            if total_current_size + blob_data_len > buf.len() {
                break;
            }

            buf[total_current_size..total_current_size + value.len()].copy_from_slice(value);
            total_current_size += blob_data_len;
            total_blobs += 1;

            // TODO: Change this logic to support looking for data
            // that spans multiple leader slots, once we support
            // a window that knows about different leader slots
            db_iterator.next();
        }

        Ok((total_blobs, total_current_size as u64))
    }

    /// Return an iterator for all the entries in the given file.
    pub fn read_ledger(&self) -> Result<impl Iterator<Item = Entry>> {
        let mut db_iterator = self.db.raw_iterator_cf(self.data_cf.handle(&self.db))?;

        db_iterator.seek_to_first();
        Ok(EntryIterator { db_iterator })
    }
}

struct EntryIterator {
    db_iterator: DBRawIterator,
    // https://github.com/rust-rocksdb/rust-rocksdb/issues/234
    //   rocksdb issue: the _db_ledger member must be lower in the struct to prevent a crash
    //   when the db_iterator member above is dropped.
    //   _db_ledger is unused, but dropping _db_ledger results in a broken db_iterator
    //   you have to hold the database open in order to iterate over it, and in order
    //   for db_iterator to be able to run Drop
    //    _db_ledger: DbLedger,
}

impl Iterator for EntryIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        if self.db_iterator.valid() {
            if let Some(value) = self.db_iterator.value() {
                self.db_iterator.next();

                match deserialize(&value[BLOB_HEADER_SIZE..]) {
                    Ok(entry) => Some(entry),
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

pub fn write_entries_to_ledger<I>(ledger_paths: &[&str], entries: I, slot_height: u64)
where
    I: IntoIterator,
    I::Item: Borrow<Entry>,
{
    let mut entries = entries.into_iter();
    for ledger_path in ledger_paths {
        let mut db_ledger =
            DbLedger::open(ledger_path).expect("Expected to be able to open database ledger");
        db_ledger
            .write_entries(slot_height, entries.by_ref())
            .expect("Expected successful write of genesis entries");
    }
}

pub fn genesis<'a, I>(ledger_path: &str, keypair: Option<&Keypair>, entries: I) -> Result<()>
where
    I: IntoIterator<Item = &'a Entry>,
{
    let mut db_ledger = DbLedger::open(ledger_path)?;

    let pubkey = keypair.map(|k| k.pubkey());

    // TODO sign these blobs with keypair
    let blobs = entries
        .into_iter()
        .enumerate()
        .map(|(idx, entry)| entry.borrow().to_blob(Some(idx as u64), pubkey, None));

    db_ledger.write_shared_blobs(DEFAULT_SLOT_HEIGHT, blobs)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{get_tmp_ledger_path, make_tiny_test_entries, Block};

    #[test]
    fn test_put_get_simple() {
        let ledger_path = get_tmp_ledger_path("test_put_get_simple");
        let ledger = DbLedger::open(&ledger_path).unwrap();

        // Test meta column family
        let meta = SlotMeta::new();
        let meta_key = MetaCf::key(DEFAULT_SLOT_HEIGHT);
        ledger.meta_cf.put(&ledger.db, &meta_key, &meta).unwrap();
        let result = ledger
            .meta_cf
            .get(&ledger.db, &meta_key)
            .unwrap()
            .expect("Expected meta object to exist");

        assert_eq!(result, meta);

        // Test erasure column family
        let erasure = vec![1u8; 16];
        let erasure_key = ErasureCf::key(DEFAULT_SLOT_HEIGHT, 0);
        ledger
            .erasure_cf
            .put(&ledger.db, &erasure_key, &erasure)
            .unwrap();

        let result = ledger
            .erasure_cf
            .get(&ledger.db, &erasure_key)
            .unwrap()
            .expect("Expected erasure object to exist");

        assert_eq!(result, erasure);

        // Test data column family
        let data = vec![2u8; 16];
        let data_key = DataCf::key(DEFAULT_SLOT_HEIGHT, 0);
        ledger.data_cf.put(&ledger.db, &data_key, &data).unwrap();

        let result = ledger
            .data_cf
            .get(&ledger.db, &data_key)
            .unwrap()
            .expect("Expected data object to exist");

        assert_eq!(result, data);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        DbLedger::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_get_blobs_bytes() {
        let shared_blobs = make_tiny_test_entries(10).to_blobs();
        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();
        let slot = DEFAULT_SLOT_HEIGHT;

        let ledger_path = get_tmp_ledger_path("test_get_blobs_bytes");
        let mut ledger = DbLedger::open(&ledger_path).unwrap();
        ledger.write_blobs(slot, &blobs).unwrap();

        let mut buf = [0; 1024];
        let (num_blobs, bytes) = ledger.get_blob_bytes(0, 1, &mut buf, slot).unwrap();
        let bytes = bytes as usize;
        assert_eq!(num_blobs, 1);
        {
            let blob_data = &buf[..bytes];
            assert_eq!(blob_data, &blobs[0].data[..bytes]);
        }

        let (num_blobs, bytes2) = ledger.get_blob_bytes(0, 2, &mut buf, slot).unwrap();
        let bytes2 = bytes2 as usize;
        assert_eq!(num_blobs, 2);
        assert!(bytes2 > bytes);
        {
            let blob_data_1 = &buf[..bytes];
            assert_eq!(blob_data_1, &blobs[0].data[..bytes]);

            let blob_data_2 = &buf[bytes..bytes2];
            assert_eq!(blob_data_2, &blobs[1].data[..bytes2 - bytes]);
        }

        // buf size part-way into blob[1], should just return blob[0]
        let mut buf = vec![0; bytes + 1];
        let (num_blobs, bytes3) = ledger.get_blob_bytes(0, 2, &mut buf, slot).unwrap();
        assert_eq!(num_blobs, 1);
        let bytes3 = bytes3 as usize;
        assert_eq!(bytes3, bytes);

        let mut buf = vec![0; bytes2 - 1];
        let (num_blobs, bytes4) = ledger.get_blob_bytes(0, 2, &mut buf, slot).unwrap();
        assert_eq!(num_blobs, 1);
        let bytes4 = bytes4 as usize;
        assert_eq!(bytes4, bytes);

        let mut buf = vec![0; bytes * 2];
        let (num_blobs, bytes6) = ledger.get_blob_bytes(9, 1, &mut buf, slot).unwrap();
        assert_eq!(num_blobs, 1);
        let bytes6 = bytes6 as usize;

        {
            let blob_data = &buf[..bytes6];
            assert_eq!(blob_data, &blobs[9].data[..bytes6]);
        }

        // Read out of range
        assert!(ledger.get_blob_bytes(20, 2, &mut buf, slot).is_err());

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        DbLedger::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_data_blobs_basic() {
        let entries = make_tiny_test_entries(2);
        let shared_blobs = entries.to_blobs();
        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();

        let ledger_path = get_tmp_ledger_path("test_insert_data_blobs_basic");
        let ledger = DbLedger::open(&ledger_path).unwrap();

        // Insert second blob, we're missing the first blob, so should return nothing
        let result = ledger
            .insert_data_blob(&DataCf::key(DEFAULT_SLOT_HEIGHT, 1), blobs[1])
            .unwrap();

        assert!(result.len() == 0);
        let meta = ledger
            .meta_cf
            .get(&ledger.db, &MetaCf::key(DEFAULT_SLOT_HEIGHT))
            .unwrap()
            .expect("Expected new metadata object to be created");
        assert!(meta.consumed == 0 && meta.received == 2);

        // Insert first blob, check for consecutive returned entries
        let result = ledger
            .insert_data_blob(&DataCf::key(DEFAULT_SLOT_HEIGHT, 0), blobs[0])
            .unwrap();

        assert_eq!(result, entries);

        let meta = ledger
            .meta_cf
            .get(&ledger.db, &MetaCf::key(DEFAULT_SLOT_HEIGHT))
            .unwrap()
            .expect("Expected new metadata object to exist");
        assert!(meta.consumed == 2 && meta.received == 2);

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        DbLedger::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_data_blobs_multiple() {
        let num_blobs = 10;
        let entries = make_tiny_test_entries(num_blobs);
        let shared_blobs = entries.to_blobs();
        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();

        let ledger_path = get_tmp_ledger_path("test_insert_data_blobs_multiple");
        let ledger = DbLedger::open(&ledger_path).unwrap();

        // Insert blobs in reverse, check for consecutive returned blobs
        for i in (0..num_blobs).rev() {
            let result = ledger
                .insert_data_blob(&DataCf::key(DEFAULT_SLOT_HEIGHT, i as u64), blobs[i])
                .unwrap();

            let meta = ledger
                .meta_cf
                .get(&ledger.db, &MetaCf::key(DEFAULT_SLOT_HEIGHT))
                .unwrap()
                .expect("Expected metadata object to exist");
            if i != 0 {
                assert_eq!(result.len(), 0);
                assert!(meta.consumed == 0 && meta.received == num_blobs as u64);
            } else {
                assert_eq!(result, entries);
                assert!(meta.consumed == num_blobs as u64 && meta.received == num_blobs as u64);
            }
        }

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        DbLedger::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_insert_data_blobs_slots() {
        let num_blobs = 10;
        let entries = make_tiny_test_entries(num_blobs);
        let shared_blobs = entries.to_blobs();
        let blob_locks: Vec<_> = shared_blobs.iter().map(|b| b.read().unwrap()).collect();
        let blobs: Vec<&Blob> = blob_locks.iter().map(|b| &**b).collect();

        let ledger_path = get_tmp_ledger_path("test_insert_data_blobs_slots");
        let ledger = DbLedger::open(&ledger_path).unwrap();

        // Insert last blob into next slot
        let result = ledger
            .insert_data_blob(
                &DataCf::key(DEFAULT_SLOT_HEIGHT + 1, (num_blobs - 1) as u64),
                blobs.last().unwrap(),
            ).unwrap();
        assert_eq!(result.len(), 0);

        // Insert blobs into first slot, check for consecutive blobs
        for i in (0..num_blobs - 1).rev() {
            let result = ledger
                .insert_data_blob(&DataCf::key(DEFAULT_SLOT_HEIGHT, i as u64), blobs[i])
                .unwrap();
            let meta = ledger
                .meta_cf
                .get(&ledger.db, &MetaCf::key(DEFAULT_SLOT_HEIGHT))
                .unwrap()
                .expect("Expected metadata object to exist");
            if i != 0 {
                assert_eq!(result.len(), 0);
                assert!(meta.consumed == 0 && meta.received == num_blobs as u64);
            } else {
                assert_eq!(result, entries);
                assert!(meta.consumed == num_blobs as u64 && meta.received == num_blobs as u64);
            }
        }

        // Destroying database without closing it first is undefined behavior
        drop(ledger);
        DbLedger::destroy(&ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_iteration_order() {
        let slot = 0;
        // Create RocksDb ledger
        let db_ledger_path = get_tmp_ledger_path("test_iteration_order");
        {
            let mut db_ledger = DbLedger::open(&db_ledger_path).unwrap();

            // Write entries
            let num_entries = 8;
            let shared_blobs = make_tiny_test_entries(num_entries).to_blobs();

            for (i, b) in shared_blobs.iter().enumerate() {
                b.write().unwrap().set_index(1 << (i * 8)).unwrap();
            }

            db_ledger
                .write_shared_blobs(DEFAULT_SLOT_HEIGHT, &shared_blobs)
                .expect("Expected successful write of blobs");
            let mut db_iterator = db_ledger
                .db
                .raw_iterator_cf(db_ledger.data_cf.handle(&db_ledger.db))
                .expect("Expected to be able to open database iterator");

            db_iterator.seek(&DataCf::key(slot, 1));

            // Iterate through ledger
            for i in 0..num_entries {
                assert!(db_iterator.valid());
                let current_key = db_iterator.key().expect("Expected a valid key");
                let current_index = DataCf::index_from_key(&current_key)
                    .expect("Expect to be able to parse index from valid key");
                assert_eq!(current_index, (1 as u64) << (i * 8));
                db_iterator.next();
            }
        }
        DbLedger::destroy(&db_ledger_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_genesis_and_entry_iterator() {
        // Create RocksDb ledger
        let entries = make_tiny_test_entries(100);
        let ledger_path = get_tmp_ledger_path("test_entry_iterator");
        {
            assert!(genesis(&ledger_path, None, &entries).is_ok());

            let ledger = DbLedger::open(&ledger_path).expect("open failed");

            let read_entries: Vec<Entry> =
                ledger.read_ledger().expect("read_ledger failed").collect();
            assert_eq!(entries, read_entries);
        }

        DbLedger::destroy(&ledger_path).expect("Expected successful database destruction");
    }

}
