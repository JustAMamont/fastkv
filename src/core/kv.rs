//! Key-Value Store Implementation
//!
//! This module provides a high-performance hash table for storing
//! key-value pairs. We implement three versions:
//! 1. `KvStoreSimple` — `HashMap` wrapper (reference baseline)
//! 2. `KvStoreCustom` — custom hash table with linear probing (single-threaded)
//! 3. `KvStoreLockFree` — lock-free hash table (thread-safe, cache-line aligned)
//!
//! The production type alias [`KvStore`] points to `KvStoreLockFree`.
//!
//! ## Extended operations (Phase 3)
//!
//! In addition to the core `GET` / `SET` / `DEL` the lock-free store now
//! supports atomic `INCR` / `DECR` (optimistic CAS loop), `MGET` / `MSET`,
//! `EXISTS`, `APPEND`, `STRLEN`, `GETRANGE`, and `SETRANGE`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU32, AtomicU8, AtomicUsize, Ordering};

/// Default number of buckets in the hash table.
///
/// 1 million buckets × 192 bytes ≈ 192 MB of memory.
const DEFAULT_CAPACITY: usize = 1_000_000;

/// Maximum number of probe steps before giving up.
///
/// This caps the worst-case scan to `MAX_PROBE_STEPS` entries per
/// lookup/insert, preventing O(n) behaviour when the table is nearly
/// full. 256 steps with a load factor < 50 % gives a collision
/// probability < 10⁻⁶ (birthday paradox).
const MAX_PROBE_STEPS: usize = 256;

/// Maximum key size (256 bytes) — only enforced by `KvStoreCustom`.
const MAX_KEY_SIZE: usize = 256;

/// Maximum value size (4 KB) — only enforced by `KvStoreCustom`.
const MAX_VALUE_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// Error types for extended operations
// ---------------------------------------------------------------------------

/// Errors returned by [`KvStoreLockFree::incr`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrError {
    /// The key does not exist in the store.
    KeyNotFound,
    /// The current value is not a valid 64-bit integer.
    NotInteger,
    /// The result overflows `i64`.
    Overflow,
    /// The key exceeds inline storage.
    KeyTooLong,
    /// The resulting string representation exceeds inline storage.
    ValueTooLong,
}

impl std::fmt::Display for IncrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncrError::KeyNotFound => write!(f, "key not found"),
            IncrError::NotInteger => write!(f, "value is not an integer"),
            IncrError::Overflow => write!(f, "increment would overflow"),
            IncrError::KeyTooLong => write!(f, "key exceeds maximum size"),
            IncrError::ValueTooLong => write!(f, "resulting value exceeds maximum size"),
        }
    }
}

impl std::error::Error for IncrError {}

// ============================================================================
// VERSION 1: Simple HashMap wrapper (reference baseline)
// ============================================================================

/// Simple KV Store using `std::HashMap`.
///
/// Not thread-safe; useful as a correctness reference.
pub struct KvStoreSimple {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl KvStoreSimple {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Get a clone of the value associated with *key*.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }

    /// Insert or update *key* with *value*.
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.data.insert(key.to_vec(), value.to_vec());
    }

    /// Remove *key*. Returns `true` if it existed.
    pub fn del(&mut self, key: &[u8]) -> bool {
        self.data.remove(key).is_some()
    }

    /// Number of entries in the store.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the store is empty.
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

/// A single entry in the hash table. Stores the wyhash-inspired hash alongside
/// owned key and value byte vectors.
#[derive(Clone)]
struct Entry {
    /// Hash of the key (0 means empty).
    hash: u64,
    /// The key bytes.
    key: Vec<u8>,
    /// The value bytes.
    value: Vec<u8>,
}

impl Entry {
    /// Create an empty entry (hash = 0 signals vacant).
    fn empty() -> Self {
        Self {
            hash: 0,
            key: Vec::new(),
            value: Vec::new(),
        }
    }

    /// Returns `true` if this slot is vacant (`hash == 0`).
    fn is_empty(&self) -> bool {
        self.hash == 0
    }
}

/// Custom Hash Table with Linear Probing (single-threaded).
pub struct KvStoreCustom {
    buckets: Vec<Entry>,
    count: usize,
    capacity: usize,
}

impl KvStoreCustom {
    /// Create an empty store with the default capacity (1 000 000 buckets).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create an empty store with the given number of buckets.
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

    /// Compute a wyhash-inspired 64-bit hash of *key*.
    ///
    /// Processes 8 bytes at a time for better throughput compared to
    /// the per-byte FNV-1a approach, while still being pure-Rust with
    /// no external dependencies.
    #[inline]
    fn hash_key(key: &[u8]) -> u64 {
        const SECRET0: u64 = 0xa0761d6478bd642f;
        const SECRET1: u64 = 0xe7037ed1a0b428db;
        let mut hash = 0xcbf29ce484222325;
        let chunks = key.chunks_exact(8);
        let remainder = chunks.remainder();
        for chunk in chunks {
            let v = u64::from_le_bytes(chunk.try_into().unwrap());
            hash ^= v.wrapping_mul(SECRET0);
            hash = hash.rotate_left(31).wrapping_mul(SECRET1);
        }
        let mut v = hash;
        for &byte in remainder {
            v ^= (byte as u64).wrapping_mul(SECRET0);
            v = v.rotate_left(31).wrapping_mul(SECRET1);
        }
        v ^ (v >> 31)
    }

    /// Probe the bucket array for *key* using linear probing.
    ///
    /// Returns `(slot_index, key_found)`. If the key is not present and a
    /// vacant slot exists, returns the first vacant slot index.
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

    /// Return a clone of the value for *key*, or `None`.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let hash = Self::hash_key(key);
        let (idx, found) = self.find_slot(key, hash);

        if found {
            Some(self.buckets[idx].value.clone())
        } else {
            None
        }
    }

    /// Insert or update *key* with *value*.
    ///
    /// Returns `false` if the key or value exceeds the size limits.
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

    /// Remove *key* from the store.
    ///
    /// Returns `true` if the key existed and was removed.
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

    /// Return the number of entries currently stored.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Return `true` if the store contains no entries.
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

/// Inline storage size for keys and values.
///
/// Keys up to 64 bytes and values up to 64 bytes are stored directly in the
/// cache-line-aligned entry structure, avoiding heap allocation entirely.
/// Larger keys/values will be rejected by `set` / `incr`.
const INLINE_SIZE: usize = 64;

/// A single entry in the lock-free hash table.
///
/// **Memory layout** (192 bytes, cache-line aligned):
///
/// | Field      | Size      | Offset | Description                        |
/// |------------|-----------|--------|------------------------------------|
/// | `hash`     | 8 B       | 0      | wyhash-inspired hash; 0 = empty     |
/// | `key_len`  | 4 B       | 8      | Length of the key                   |
/// | `value_len`| 4 B       | 12     | Length of the value                 |
/// | `version`  | 8 B       | 16     | Optimistic-read version counter     |
/// | `data`     | 128 B     | 24     | `key[0..64]` + `value[0..64]`       |
/// | *padding*  | 64 B      | 152    | Pad to 192 bytes (3 cache lines)    |
#[repr(C, align(64))]
struct LockFreeEntry {
    /// Hash of the key. `0` = empty slot, used for CAS insertion.
    hash: AtomicU64,
    /// Length of the key in bytes.
    key_len: AtomicU32,
    /// Length of the value in bytes.
    value_len: AtomicU32,
    /// Version counter for optimistic concurrency control.
    ///
    /// Even values indicate a stable state; odd values mean a write is in
    /// progress. Readers snapshot `v1`, read the data, then verify `v2 == v1`.
    version: AtomicU64,
    /// Inline key + value storage.
    ///
    /// Bytes `0..INLINE_SIZE` hold the key; bytes `INLINE_SIZE..2*INLINE_SIZE`
    /// hold the value. Each byte is an `AtomicU8` to support individual byte
    /// reads without locking.
    data: [AtomicU8; INLINE_SIZE * 2],
}

impl LockFreeEntry {
    /// Create a new empty entry (all zeros).
    fn new() -> Self {
        Self {
            hash: AtomicU64::new(0),
            key_len: AtomicU32::new(0),
            value_len: AtomicU32::new(0),
            version: AtomicU64::new(0),
            data: std::array::from_fn(|_| AtomicU8::new(0)),
        }
    }
}

/// Lock-free, thread-safe hash table.
///
/// # Concurrency model
///
/// * **SET** — CAS on the `hash` field to claim an empty slot; re-check for
///   key match if CAS fails due to a competing insert.
/// * **GET** — Optimistic read: snapshot the version, read key + value,
///   verify version hasn't changed. Retry up to 3 times on version mismatch.
/// * **DEL** — Find the entry, mark version as odd, clear hash to 0, mark
///   version as even again.
/// * **INCR** — Optimistic CAS loop: snapshot version → read value → parse
///   → compute new value → CAS version from even → odd → write → set even.
///
/// No `Mutex`, `RwLock`, or spin-lock is ever acquired.
pub struct KvStoreLockFree {
    buckets: Box<[LockFreeEntry]>,
    count: AtomicUsize,
    capacity: usize,
}

// SAFETY: all fields are either atomic or immutable.
unsafe impl Send for KvStoreLockFree {}
unsafe impl Sync for KvStoreLockFree {}

impl KvStoreLockFree {
    /// Create a store with the default capacity (1 000 000 entries).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a store with a custom number of pre-allocated buckets.
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

    /// Compute a wyhash-inspired 64-bit hash of *key*.
    ///
    /// Processes 8 bytes at a time for better throughput compared to
    /// the per-byte FNV-1a approach, while still being pure-Rust with
    /// no external dependencies.
    #[inline]
    fn hash_key(key: &[u8]) -> u64 {
        const SECRET0: u64 = 0xa0761d6478bd642f;
        const SECRET1: u64 = 0xe7037ed1a0b428db;
        let mut hash = 0xcbf29ce484222325;
        let chunks = key.chunks_exact(8);
        let remainder = chunks.remainder();
        for chunk in chunks {
            let v = u64::from_le_bytes(chunk.try_into().unwrap());
            hash ^= v.wrapping_mul(SECRET0);
            hash = hash.rotate_left(31).wrapping_mul(SECRET1);
        }
        let mut v = hash;
        for &byte in remainder {
            v ^= (byte as u64).wrapping_mul(SECRET0);
            v = v.rotate_left(31).wrapping_mul(SECRET1);
        }
        v ^ (v >> 31)
    }

    // -----------------------------------------------------------------------
    // Core: GET
    // -----------------------------------------------------------------------

    /// Read the value for *key*.
    ///
    /// Returns `None` if the key does not exist or if key length exceeds
    /// [`INLINE_SIZE`].
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if key.len() > INLINE_SIZE {
            return None;
        }

        let hash = Self::hash_key(key);
        let start = (hash as usize) % self.capacity;
        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        for _ in 0..3 {
            for i in 0..probe_limit {
                let idx = (start + i) % self.capacity;
                let entry = &self.buckets[idx];

                let v1 = entry.version.load(Ordering::Acquire);
                if v1 % 2 == 1 { continue; }

                let entry_hash = entry.hash.load(Ordering::Acquire);
                if entry_hash == 0 { return None; }
                if entry_hash != hash { continue; }

                let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                if key_len != key.len() { continue; }

                // Stack-allocated key buffer — no heap allocation.
                let mut entry_key = [0u8; INLINE_SIZE];
                for j in 0..key_len {
                    entry_key[j] = entry.data[j].load(Ordering::Relaxed);
                }

                if &entry_key[..key_len] != key { continue; }

                let value_len = entry.value_len.load(Ordering::Acquire) as usize;
                let mut value = vec![0u8; value_len];
                for j in 0..value_len {
                    value[j] = entry.data[INLINE_SIZE + j].load(Ordering::Relaxed);
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

    // -----------------------------------------------------------------------
    // Core: SET
    // -----------------------------------------------------------------------

    /// Insert or update *key* with *value*.
    ///
    /// Both key and value must be at most [`INLINE_SIZE`] bytes.
    /// Returns `false` if either exceeds the limit or if the table is full.
    pub fn set(&self, key: &[u8], value: &[u8]) -> bool {
        if key.len() > INLINE_SIZE || value.len() > INLINE_SIZE {
            return false;
        }

        let hash = Self::hash_key(key);
        let start = (hash as usize) % self.capacity;

        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        for i in 0..probe_limit {
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
                    // Stack-allocated key buffer — no heap allocation.
                    let mut entry_key = [0u8; INLINE_SIZE];
                    for j in 0..key_len {
                        entry_key[j] = entry.data[j].load(Ordering::Relaxed);
                    }
                    if &entry_key[..key_len] == key {
                        self.write_entry(entry, key, value);
                        return true;
                    }
                }
            }
        }
        false
    }

    // -----------------------------------------------------------------------
    // Core: DEL
    // -----------------------------------------------------------------------

    /// Remove *key* from the store.
    ///
    /// Returns `true` if the key existed and was removed.
    pub fn del(&self, key: &[u8]) -> bool {
        if key.len() > INLINE_SIZE {
            return false;
        }

        let hash = Self::hash_key(key);
        let start = (hash as usize) % self.capacity;
        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        for i in 0..probe_limit {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);

            if entry_hash == 0 { return false; }
            if entry_hash == hash {
                let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                if key_len == key.len() {
                    let mut entry_key = [0u8; INLINE_SIZE];
                    for j in 0..key_len {
                        entry_key[j] = entry.data[j].load(Ordering::Relaxed);
                    }
                    if &entry_key[..key_len] == key {
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

    // -----------------------------------------------------------------------
    // Extended: EXISTS
    // -----------------------------------------------------------------------

    /// Check whether *key* exists in the store.
    ///
    /// This is more efficient than `get(key).is_some()` because it does not
    /// copy the value bytes.
    pub fn exists(&self, key: &[u8]) -> bool {
        if key.len() > INLINE_SIZE {
            return false;
        }

        let hash = Self::hash_key(key);
        let start = (hash as usize) % self.capacity;
        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        for i in 0..probe_limit {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);

            if entry_hash == 0 { return false; }
            if entry_hash != hash { continue; }

            let key_len = entry.key_len.load(Ordering::Acquire) as usize;
            if key_len != key.len() { continue; }

            let mut entry_key = [0u8; INLINE_SIZE];
            for j in 0..key_len {
                entry_key[j] = entry.data[j].load(Ordering::Relaxed);
            }
            if &entry_key[..key_len] == key { return true; }
        }
        false
    }

    // -----------------------------------------------------------------------
    // Extended: INCR / DECR
    // -----------------------------------------------------------------------

    /// Atomically increment the integer value stored at *key* by *delta*.
    ///
    /// Uses an optimistic CAS loop on the entry version counter to provide
    /// linearizable semantics. If the key does not exist or the value is
    /// not a valid `i64`, an error is returned.
    ///
    /// # Errors
    ///
    /// * [`IncrError::KeyNotFound`] — the key does not exist.
    /// * [`IncrError::NotInteger`] — the current value cannot be parsed as `i64`.
    /// * [`IncrError::Overflow`] — the result would overflow `i64`.
    pub fn incr(&self, key: &[u8], delta: i64) -> Result<i64, IncrError> {
        if key.len() > INLINE_SIZE {
            return Err(IncrError::KeyTooLong);
        }

        let hash = Self::hash_key(key);
        let start = (hash as usize) % self.capacity;

        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        for i in 0..probe_limit {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);

            if entry_hash == 0 {
                return Err(IncrError::KeyNotFound);
            }
            if entry_hash != hash { continue; }

            // Verify key match.
            let key_len = entry.key_len.load(Ordering::Acquire) as usize;
            if key_len != key.len() { continue; }
            let mut entry_key = [0u8; INLINE_SIZE];
            for j in 0..key_len {
                entry_key[j] = entry.data[j].load(Ordering::Relaxed);
            }
            if &entry_key[..key_len] != key { continue; }

            // Key found — optimistic read-modify-write loop.
            loop {
                // 1. Snapshot version (must be even).
                let v1 = entry.version.load(Ordering::Acquire);
                if v1 % 2 == 1 { continue; }

                // 2. Read current value (stack-allocated, Relaxed reads).
                let value_len = entry.value_len.load(Ordering::Acquire) as usize;
                let mut value_bytes = [0u8; INLINE_SIZE];
                for j in 0..value_len {
                    value_bytes[j] = entry.data[INLINE_SIZE + j].load(Ordering::Relaxed);
                }

                // 3. Verify version hasn't changed.
                let v2 = entry.version.load(Ordering::Acquire);
                if v1 != v2 { continue; }

                // 4. Parse as i64.
                let current = std::str::from_utf8(&value_bytes[..value_len])
                    .map_err(|_| IncrError::NotInteger)?
                    .parse::<i64>()
                    .map_err(|_| IncrError::NotInteger)?;

                // 5. Compute new value.
                let new_value = current.checked_add(delta)
                    .ok_or(IncrError::Overflow)?;

                let new_str = new_value.to_string();
                if new_str.len() > INLINE_SIZE {
                    return Err(IncrError::ValueTooLong);
                }

                // 6. CAS version from even → odd to claim exclusive write.
                if entry.version.compare_exchange(
                    v1, v1 + 1, Ordering::AcqRel, Ordering::Acquire,
                ).is_err() {
                    continue; // Someone else is writing; retry.
                }

                // 7. We hold exclusive access — write new value.
                entry.value_len.store(new_str.len() as u32, Ordering::Release);
                for (j, &byte) in new_str.as_bytes().iter().enumerate() {
                    entry.data[INLINE_SIZE + j].store(byte, Ordering::Relaxed);
                }
                entry.version.store(v1 + 2, Ordering::Release);

                return Ok(new_value);
            }
        }
        Err(IncrError::KeyNotFound)
    }

    /// Convenience: increment by 1.
    pub fn incr_by_one(&self, key: &[u8]) -> Result<i64, IncrError> {
        self.incr(key, 1)
    }

    /// Convenience: decrement by 1.
    pub fn decr(&self, key: &[u8]) -> Result<i64, IncrError> {
        self.incr(key, -1)
    }

    // -----------------------------------------------------------------------
    // Extended: MGET
    // -----------------------------------------------------------------------

    /// Get values for multiple keys. Returns a vector with one entry per key;
    /// `None` means the key was not found.
    pub fn mget(&self, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    // -----------------------------------------------------------------------
    // Extended: MSET
    // -----------------------------------------------------------------------

    /// Set multiple key-value pairs. Returns `true` if **all** sets succeeded.
    ///
    /// Note: individual set operations are independent; a partial failure
    /// leaves some keys set and others not. In that case `false` is returned.
    pub fn mset(&self, pairs: &[(&[u8], &[u8])]) -> bool {
        pairs.iter().all(|(k, v)| self.set(k, v))
    }

    // -----------------------------------------------------------------------
    // Extended: APPEND
    // -----------------------------------------------------------------------

    /// Append *suffix* to the current string value of *key*.
    ///
    /// If the key does not exist it is created with *suffix* as the initial
    /// value (equivalent to `SET key suffix`).
    ///
    /// Returns the new length of the string value, or `None` if the result
    /// would exceed [`INLINE_SIZE`].
    pub fn append(&self, key: &[u8], suffix: &[u8]) -> Option<usize> {
        if key.len() > INLINE_SIZE { return None; }

        if let Some(current) = self.get(key) {
            let mut new_val = current;
            new_val.extend_from_slice(suffix);
            if new_val.len() > INLINE_SIZE { return None; }
            if self.set(key, &new_val) {
                Some(new_val.len())
            } else {
                None
            }
        } else {
            if suffix.len() > INLINE_SIZE { return None; }
            if self.set(key, suffix) {
                Some(suffix.len())
            } else {
                None
            }
        }
    }

    // -----------------------------------------------------------------------
    // Extended: STRLEN
    // -----------------------------------------------------------------------

    /// Return the length in bytes of the string value stored at *key*.
    ///
    /// Returns `0` if the key does not exist (matches Redis semantics).
    pub fn strlen(&self, key: &[u8]) -> usize {
        self.get(key).map_or(0, |v| v.len())
    }

    // -----------------------------------------------------------------------
    // Extended: GETRANGE
    // -----------------------------------------------------------------------

    /// Return a substring of the string value at *key*.
    ///
    /// * `start` / `end` are **inclusive** zero-based byte offsets
    ///   (matches Redis `GETRANGE` semantics).
    /// * Negative indices count from the end of the string.
    /// * Out-of-range indices are clamped to the actual string length.
    /// * Returns `None` if the key does not exist.
    pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> Option<Vec<u8>> {
        let value = self.get(key)?;

        let len = value.len() as i64;
        let start = normalize_index(start, len);
        let end = normalize_index(end, len);

        if start > end {
            return Some(Vec::new());
        }

        let s = start as usize;
        let e = ((end + 1) as usize).min(value.len());
        Some(value[s..e].to_vec())
    }

    // -----------------------------------------------------------------------
    // Extended: SETRANGE
    // -----------------------------------------------------------------------

    /// Overwrite part of the string value at *key* starting at *offset*.
    ///
    /// If *offset* exceeds the current length the string is zero-padded.
    /// If the key does not exist it is created (again with zero-padding).
    ///
    /// Returns the new length of the string, or `None` if the result would
    /// exceed [`INLINE_SIZE`].
    pub fn setrange(&self, key: &[u8], offset: usize, replacement: &[u8]) -> Option<usize> {
        let offset = offset as i64;
        let current = self.get(key).unwrap_or_default();
        let mut value = current;
        let new_len = (offset as usize + replacement.len()).max(value.len());
        if new_len > INLINE_SIZE { return None; }

        // Extend with zero bytes if needed.
        if new_len > value.len() {
            value.resize(new_len, 0);
        }

        // Write replacement.
        let start = offset as usize;
        value[start..start + replacement.len()].copy_from_slice(replacement);

        if self.set(key, &value) {
            Some(new_len)
        } else {
            None
        }
    }

    // -----------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------

    /// Approximate number of entries.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Whether the store has no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total capacity (number of buckets).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Write *key* and *value* into *entry* using the version protocol.
    ///
    /// Caller must have already "claimed" the entry (via CAS on `hash` or
    /// confirmed key match).
    #[inline]
    fn write_entry(&self, entry: &LockFreeEntry, key: &[u8], value: &[u8]) {
        let v = entry.version.fetch_add(1, Ordering::AcqRel);
        entry.key_len.store(key.len() as u32, Ordering::Release);
        for (j, &byte) in key.iter().enumerate() {
            entry.data[j].store(byte, Ordering::Relaxed);
        }
        entry.value_len.store(value.len() as u32, Ordering::Release);
        for (j, &byte) in value.iter().enumerate() {
            entry.data[INLINE_SIZE + j].store(byte, Ordering::Relaxed);
        }
        entry.version.store(v + 2, Ordering::Release);
    }
}

impl Default for KvStoreLockFree {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helper: normalize a possibly-negative index
// ---------------------------------------------------------------------------

/// Convert a (possibly negative) Redis-style index to a non-negative offset.
///
/// * `idx` < 0 → counts from the end (`len + idx`).
/// * `idx` >= `len` → clamped to `len`.
/// * `idx` < `-len` → clamped to 0.
#[inline]
fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx.min(len)
    }
}

// ============================================================================
// TYPE ALIASES
// ============================================================================

/// The production KV store — lock-free and thread-safe.
pub type KvStore = KvStoreLockFree;

/// Single-threaded KV store (for testing / comparison).
pub type KvStoreSingleThreaded = KvStoreCustom;

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // ----- Core operations -----

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

    // ----- EXISTS -----

    #[test]
    fn test_exists() {
        let store = KvStoreLockFree::with_capacity(100);
        assert!(!store.exists(b"missing"));
        store.set(b"present", b"yes");
        assert!(store.exists(b"present"));
        store.del(b"present");
        assert!(!store.exists(b"present"));
    }

    // ----- INCR / DECR -----

    #[test]
    fn test_incr() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"counter", b"10");

        assert_eq!(store.incr(b"counter", 1).unwrap(), 11);
        assert_eq!(store.get(b"counter"), Some(b"11".to_vec()));

        assert_eq!(store.incr(b"counter", 5).unwrap(), 16);
        assert_eq!(store.get(b"counter"), Some(b"16".to_vec()));

        assert_eq!(store.incr(b"counter", -3).unwrap(), 13);
        assert_eq!(store.incr_by_one(b"counter").unwrap(), 14);
        assert_eq!(store.decr(b"counter").unwrap(), 13);
    }

    #[test]
    fn test_incr_not_integer() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"str", b"hello");
        assert_eq!(store.incr(b"str", 1), Err(IncrError::NotInteger));
    }

    #[test]
    fn test_incr_key_not_found() {
        let store = KvStoreLockFree::with_capacity(100);
        assert_eq!(store.incr(b"missing", 1), Err(IncrError::KeyNotFound));
    }

    #[test]
    fn test_incr_overflow() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"big", i64::MAX.to_string().as_bytes());
        assert_eq!(store.incr(b"big", 1), Err(IncrError::Overflow));
    }

    #[test]
    fn test_incr_multithreaded() {
        let store = Arc::new(KvStoreLockFree::with_capacity(1000));
        store.set(b"race", b"0");

        let mut handles = vec![];
        for _ in 0..8 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _ = store.incr(b"race", 1);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(store.get(b"race"), Some(b"800".to_vec()));
    }

    // ----- MGET / MSET -----

    #[test]
    fn test_mget() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"a", b"1");
        store.set(b"c", b"3");

        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let vals = store.mget(&keys);
        assert_eq!(vals[0], Some(b"1".to_vec()));
        assert_eq!(vals[1], None);
        assert_eq!(vals[2], Some(b"3".to_vec()));
    }

    #[test]
    fn test_mset() {
        let store = KvStoreLockFree::with_capacity(100);
        let pairs: Vec<(&[u8], &[u8])> = vec![(b"x", b"1"), (b"y", b"2")];
        assert!(store.mset(&pairs));
        assert_eq!(store.get(b"x"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"y"), Some(b"2".to_vec()));
    }

    // ----- APPEND -----

    #[test]
    fn test_append() {
        let store = KvStoreLockFree::with_capacity(100);

        // Append to nonexistent key → create.
        assert_eq!(store.append(b"k", b"hello"), Some(5));
        assert_eq!(store.get(b"k"), Some(b"hello".to_vec()));

        // Append to existing key.
        assert_eq!(store.append(b"k", b" world"), Some(11));
        assert_eq!(store.get(b"k"), Some(b"hello world".to_vec()));
    }

    // ----- STRLEN -----

    #[test]
    fn test_strlen() {
        let store = KvStoreLockFree::with_capacity(100);
        assert_eq!(store.strlen(b"missing"), 0);
        store.set(b"txt", b"hello");
        assert_eq!(store.strlen(b"txt"), 5);
    }

    // ----- GETRANGE -----

    #[test]
    fn test_getrange() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"s", b"Hello, World!");

        assert_eq!(store.getrange(b"s", 0, 4), Some(b"Hello".to_vec()));
        assert_eq!(store.getrange(b"s", 7, 11), Some(b"World".to_vec()));
        assert_eq!(store.getrange(b"s", -6, -1), Some(b"World!".to_vec()));
        assert_eq!(store.getrange(b"s", 100, 200), Some(Vec::new()));
        assert_eq!(store.getrange(b"s", 5, 3), Some(Vec::new()));
    }

    // ----- SETRANGE -----

    #[test]
    fn test_setrange() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"s", b"Hello World");

        assert_eq!(store.setrange(b"s", 6, b"Redis"), Some(11));
        assert_eq!(store.get(b"s"), Some(b"Hello Redis".to_vec()));

        // Beyond end → zero-pad.
        assert_eq!(store.setrange(b"s", 15, b"!"), Some(16));
        let val = store.get(b"s").unwrap();
        assert_eq!(&val[11..16], b"\x00\x00\x00\x00!");
    }

    // ----- Helper -----

    #[test]
    fn test_normalize_index() {
        assert_eq!(normalize_index(0, 5), 0);
        assert_eq!(normalize_index(4, 5), 4);
        assert_eq!(normalize_index(10, 5), 5);
        assert_eq!(normalize_index(-1, 5), 4);
        assert_eq!(normalize_index(-5, 5), 0);
        assert_eq!(normalize_index(-10, 5), 0);
    }

    // ----- Edge cases -----

    #[test]
    fn test_lockfree_inline_size_limit() {
        let store = KvStoreLockFree::with_capacity(100);
        let long_key = vec![b'k'; 65]; // > INLINE_SIZE (64)
        assert!(!store.set(&long_key, b"v")); // key too long
        assert!(store.get(&long_key).is_none());

        let long_val = vec![b'v'; 65];
        assert!(!store.set(b"key", &long_val)); // value too long
        assert!(store.get(b"key").is_none());
    }

    #[test]
    fn test_lockfree_capacity_exhaustion() {
        let store = KvStoreLockFree::with_capacity(10);
        for i in 0..10 {
            let key = format!("k{}", i);
            assert!(store.set(key.as_bytes(), b"v"));
        }
        // 11th key should fail (table full, linear probing exhausted).
        assert!(!store.set(b"overflow", b"v"));
        assert_eq!(store.len(), 10);
    }

    #[test]
    fn test_lockfree_overwrite() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"k", b"v1");
        store.set(b"k", b"v2");
        store.set(b"k", b"v3");
        assert_eq!(store.get(b"k"), Some(b"v3".to_vec()));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_lockfree_del_nonexistent() {
        let store = KvStoreLockFree::with_capacity(100);
        assert!(!store.del(b"nope"));
    }

    #[test]
    fn test_decr() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"c", b"10");
        assert_eq!(store.decr(b"c").unwrap(), 9);
        assert_eq!(store.decr(b"c").unwrap(), 8);
    }

    #[test]
    fn test_incr_by_zero() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"c", b"42");
        assert_eq!(store.incr(b"c", 0).unwrap(), 42);
    }

    #[test]
    fn test_incr_negative() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"c", b"5");
        assert_eq!(store.incr(b"c", -3).unwrap(), 2);
    }

    #[test]
    fn test_mget_empty() {
        let store = KvStoreLockFree::with_capacity(100);
        let vals = store.mget(&[]);
        assert!(vals.is_empty());
    }

    #[test]
    fn test_mset_empty() {
        let store = KvStoreLockFree::with_capacity(100);
        assert!(store.mset(&[])); // no pairs → vacuously true
    }

    #[test]
    fn test_append_too_long() {
        let store = KvStoreLockFree::with_capacity(100);
        store.set(b"k", b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); // 64 bytes
        // One more byte → 65 > INLINE_SIZE → should fail.
        assert!(store.append(b"k", b"x").is_none());
    }

    #[test]
    fn test_getrange_missing_key() {
        let store = KvStoreLockFree::with_capacity(100);
        assert_eq!(store.getrange(b"nope", 0, 10), None);
    }

    #[test]
    fn test_setrange_missing_key() {
        let store = KvStoreLockFree::with_capacity(100);
        // Key doesn't exist → created with zero-padding.
        assert_eq!(store.setrange(b"k", 5, b"hello"), Some(10));
        let val = store.get(b"k").unwrap();
        assert_eq!(&val[0..5], b"\x00\x00\x00\x00\x00");
        assert_eq!(&val[5..10], b"hello");
    }

    #[test]
    fn test_lockfree_simple_operations() {
        let mut store = KvStoreSimple::new();
        store.set(b"a", b"1");
        store.set(b"b", b"2");
        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.len(), 2);
        assert!(store.del(b"a"));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_lockfree_custom_operations() {
        let mut store = KvStoreCustom::with_capacity(100);
        store.set(b"x", b"y");
        assert_eq!(store.get(b"x"), Some(b"y".to_vec()));
        assert!(!store.set(&[0u8; 257], b"v")); // key too long
        assert!(store.del(b"x"));
        assert_eq!(store.get(b"x"), None);
    }
}
