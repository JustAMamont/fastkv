//! Key-Value Store Implementation
//!
//! This module provides a high-performance hash table for storing
//! key-value pairs. We implement three versions:
//! 1. KvStoreSimple - HashMap wrapper (for understanding)
//! 2. KvStoreCustom - Custom hash table (single-threaded)
//! 3. KvStoreLockFree - Lock-free hash table (thread-safe!)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU32, AtomicU8, AtomicUsize, Ordering};

/// Default number of buckets in the hash table
/// 1 million buckets ≈ 8MB of memory
const DEFAULT_CAPACITY: usize = 1_000_000;

/// Maximum key size (256 bytes)
const MAX_KEY_SIZE: usize = 256;

/// Maximum value size (4KB)
const MAX_VALUE_SIZE: usize = 4096;

// ============================================================================
// VERSION 1: Simple HashMap wrapper (for understanding the interface)
// ============================================================================

/// Simple KV Store using std::HashMap
pub struct KvStoreSimple {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl KvStoreSimple {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.data.insert(key.to_vec(), value.to_vec());
    }

    pub fn del(&mut self, key: &[u8]) -> bool {
        self.data.remove(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Default for KvStoreSimple {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// VERSION 2: Custom Hash Table with Linear Probing (single-threaded)
// ============================================================================

/// A single entry in the hash table
#[derive(Clone)]
struct Entry {
    /// Hash of the key (0 means empty)
    hash: u64,
    /// The key bytes
    key: Vec<u8>,
    /// The value bytes
    value: Vec<u8>,
}

impl Entry {
    fn empty() -> Self {
        Self {
            hash: 0,
            key: Vec::new(),
            value: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.hash == 0
    }
}

/// Custom Hash Table with Linear Probing (single-threaded)
pub struct KvStoreCustom {
    buckets: Vec<Entry>,
    count: usize,
    capacity: usize,
}

impl KvStoreCustom {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut buckets = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buckets.push(Entry::empty());
        }

        Self {
            buckets,
            count: 0,
            capacity,
        }
    }

    fn hash_key(key: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in key {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    fn find_slot(&self, key: &[u8], hash: u64) -> (usize, bool) {
        let start = (hash as usize) % self.capacity;

        for i in 0..self.capacity {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];

            if entry.is_empty() {
                return (idx, false);
            }

            if entry.hash == hash && entry.key == key {
                return (idx, true);
            }
        }

        (0, false)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let hash = Self::hash_key(key);
        let (idx, found) = self.find_slot(key, hash);

        if found {
            Some(self.buckets[idx].value.clone())
        } else {
            None
        }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> bool {
        if key.len() > MAX_KEY_SIZE || value.len() > MAX_VALUE_SIZE {
            return false;
        }

        let hash = Self::hash_key(key);
        let (idx, found) = self.find_slot(key, hash);

        if found {
            self.buckets[idx].value = value.to_vec();
        } else {
            self.buckets[idx] = Entry {
                hash,
                key: key.to_vec(),
                value: value.to_vec(),
            };
            self.count += 1;
        }

        true
    }

    pub fn del(&mut self, key: &[u8]) -> bool {
        let hash = Self::hash_key(key);
        let (idx, found) = self.find_slot(key, hash);

        if found {
            self.buckets[idx] = Entry::empty();
            self.count -= 1;
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl Default for KvStoreCustom {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// VERSION 3: Lock-Free Hash Table (thread-safe!)
// ============================================================================

/// Inline storage size for keys and values
/// Small keys/values stored inline, larger ones use heap
const INLINE_SIZE: usize = 64;

/// A single entry in the lock-free hash table
#[repr(C, align(64))]  // Cache-line aligned
struct LockFreeEntry {
    /// Hash of the key. 0 = empty slot, used for CAS insertion
    hash: AtomicU64,
    /// Length of the key
    key_len: AtomicU32,
    /// Length of the value
    value_len: AtomicU32,
    /// Version counter for optimistic reads
    /// Odd = write in progress, Even = stable state
    version: AtomicU64,
    /// Inline data: first INLINE_SIZE bytes = key, next INLINE_SIZE bytes = value
    data: [AtomicU8; INLINE_SIZE * 2],
}

impl LockFreeEntry {
    /// Create a new empty entry
    fn new() -> Self {
        Self {
            hash: AtomicU64::new(0),
            key_len: AtomicU32::new(0),
            value_len: AtomicU32::new(0),
            version: AtomicU64::new(0),
            data: std::array::from_fn(|_| AtomicU8::new(0)),
        }
    }

    // Check if this entry is empty (hash == 0)
    //#[inline]
    //fn is_empty(&self) -> bool {
    //    self.hash.load(Ordering::Acquire) == 0
    //}
}

/// Lock-Free Hash Table
/// 
/// Thread-safe without any locks!
pub struct KvStoreLockFree {
    buckets: Box<[LockFreeEntry]>,
    count: AtomicUsize,
    capacity: usize,
}

// Safety: All fields are atomic or immutable
unsafe impl Send for KvStoreLockFree {}
unsafe impl Sync for KvStoreLockFree {}

impl KvStoreLockFree {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut buckets = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buckets.push(LockFreeEntry::new());
        }

        Self {
            buckets: buckets.into_boxed_slice(),
            count: AtomicUsize::new(0),
            capacity,
        }
    }

    #[inline]
    fn hash_key(key: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in key {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// GET - Lock-free read using optimistic reading
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if key.len() > INLINE_SIZE {
            return None;
        }

        let hash = Self::hash_key(key);
        let start = (hash as usize) % self.capacity;

        for _ in 0..3 {
            for i in 0..self.capacity {
                let idx = (start + i) % self.capacity;
                let entry = &self.buckets[idx];

                let v1 = entry.version.load(Ordering::Acquire);
                if v1 % 2 == 1 { continue; }

                let entry_hash = entry.hash.load(Ordering::Acquire);
                if entry_hash == 0 { return None; }
                if entry_hash != hash { continue; }

                let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                if key_len != key.len() { continue; }

                let mut entry_key = vec![0u8; key_len];
                for j in 0..key_len {
                    entry_key[j] = entry.data[j].load(Ordering::Acquire);
                }

                if entry_key != key { continue; }

                let value_len = entry.value_len.load(Ordering::Acquire) as usize;
                let mut value = vec![0u8; value_len];
                for j in 0..value_len {
                    value[j] = entry.data[INLINE_SIZE + j].load(Ordering::Acquire);
                }

                let v2 = entry.version.load(Ordering::Acquire);
                if v1 == v2 {
                    return Some(value);
                }
                break;
            }
        }
        None
    }

    /// SET - Lock-free write using CAS
    pub fn set(&self, key: &[u8], value: &[u8]) -> bool {
        if key.len() > INLINE_SIZE || value.len() > INLINE_SIZE {
            return false;
        }

        let hash = Self::hash_key(key);
        let start = (hash as usize) % self.capacity;

        for i in 0..self.capacity {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);

            if entry_hash == 0 {
                match entry.hash.compare_exchange(
                    0, hash, Ordering::AcqRel, Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.write_entry(entry, key, value);
                        self.count.fetch_add(1, Ordering::Relaxed);
                        return true;
                    }
                    Err(actual) if actual != hash => continue,
                    _ => {}
                }
            }

            if entry_hash == hash {
                let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                if key_len == key.len() {
                    let mut entry_key = vec![0u8; key_len];
                    for j in 0..key_len {
                        entry_key[j] = entry.data[j].load(Ordering::Acquire);
                    }
                    if entry_key == key {
                        self.write_entry(entry, key, value);
                        return true;
                    }
                }
            }
        }
        false
    }

    #[inline]
    fn write_entry(&self, entry: &LockFreeEntry, key: &[u8], value: &[u8]) {
        let v = entry.version.fetch_add(1, Ordering::AcqRel);
        entry.key_len.store(key.len() as u32, Ordering::Release);
        for (j, &byte) in key.iter().enumerate() {
            entry.data[j].store(byte, Ordering::Release);
        }
        entry.value_len.store(value.len() as u32, Ordering::Release);
        for (j, &byte) in value.iter().enumerate() {
            entry.data[INLINE_SIZE + j].store(byte, Ordering::Release);
        }
        entry.version.store(v + 2, Ordering::Release);
    }

    /// DEL - Lock-free delete
    pub fn del(&self, key: &[u8]) -> bool {
        if key.len() > INLINE_SIZE {
            return false;
        }

        let hash = Self::hash_key(key);
        let start = (hash as usize) % self.capacity;

        for i in 0..self.capacity {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);
            
            if entry_hash == 0 { return false; }
            if entry_hash == hash {
                let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                if key_len == key.len() {
                    let mut entry_key = vec![0u8; key_len];
                    for j in 0..key_len {
                        entry_key[j] = entry.data[j].load(Ordering::Acquire);
                    }
                    if entry_key == key {
                        let v = entry.version.fetch_add(1, Ordering::AcqRel);
                        entry.hash.store(0, Ordering::Release);
                        entry.version.store(v + 2, Ordering::Release);
                        self.count.fetch_sub(1, Ordering::Relaxed);
                        return true;
                    }
                }
            }
        }
        false
    }

    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for KvStoreLockFree {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// TYPE ALIAS: Now using Lock-Free version!
// ============================================================================

/// The main KV Store - thread-safe without locks!
pub type KvStore = KvStoreLockFree;
pub type KvStoreSingleThreaded = KvStoreCustom;

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_lockfree_basic() {
        let store = KvStoreLockFree::with_capacity(100);
        assert!(store.set(b"hello", b"world"));
        assert_eq!(store.get(b"hello"), Some(b"world".to_vec()));
        assert!(store.set(b"hello", b"rust"));
        assert_eq!(store.get(b"hello"), Some(b"rust".to_vec()));
        assert!(store.del(b"hello"));
        assert_eq!(store.get(b"hello"), None);
    }

    #[test]
    fn test_lockfree_multithreaded() {
        let store = Arc::new(KvStoreLockFree::with_capacity(10000));
        let mut handles = vec![];

        for t in 0..4 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("t{}_key_{}", t, i);
                    let value = format!("t{}_value_{}", t, i);
                    store.set(key.as_bytes(), value.as_bytes());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for t in 0..4 {
            for i in 0..100 {
                let key = format!("t{}_key_{}", t, i);
                let expected = format!("t{}_value_{}", t, i);
                assert_eq!(store.get(key.as_bytes()), Some(expected.as_bytes().to_vec()));
            }
        }
    }
}