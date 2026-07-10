//! Key-Value Store Implementation
//!
//! This module provides a high-performance hash table for storing
//! key-value pairs. We implement three versions:
//! 1. `KvStoreSimple` — `HashMap` wrapper (reference baseline)
//! 2. `KvStoreCustom` — custom hash table with linear probing (single-threaded)
//! 3. `KvStoreLockFree` — lock-free hash table (thread-safe, cache-line aligned)
//!
//! The production type alias [`KvStore`] points to `KvStoreLockFree<DEFAULT_INLINE_SIZE>`.
//!
//! ## Const generic inline size
//!
//! `KvStoreLockFree<const N: usize>` allows the inline storage size to be
//! configured at compile time.  `N` is the maximum size for **each** of the
//! key and the value independently (not combined).  The default is
//! [`DEFAULT_INLINE_SIZE`] = 64 bytes.
//!
//! The internal `LockFreeEntry` stores `[[AtomicU8; N]; 2]` — two separate
//! N-byte banks — so that no arithmetic on the const generic is required
//! (stable Rust forbids `N * 2` in type positions).
//!
//! ## Extended operations (Phase 3)
//!
//! In addition to the core `GET` / `SET` / `DEL` the lock-free store now
//! supports atomic `INCR` / `DECR` (optimistic CAS loop), `MGET` / `MSET`,
//! `EXISTS`, `APPEND`, `STRLEN`, `GETRANGE`, and `SETRANGE`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::alloc::{alloc_zeroed, Layout};

/// Default number of buckets in the hash table.
///
/// 100 000 buckets × ~192 bytes (N=64) ≈ 19 MB of memory.
/// For N=256: 100 000 × ~576 bytes ≈ 55 MB.
///
/// Previous default was 1M buckets (192 MB for N=64, 576 MB for N=256)
/// which caused OOM on embedded use cases and machines with <8 GB RAM.
const DEFAULT_CAPACITY: usize = 100_000;

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

/// Default inline storage size for keys and values (per-side).
///
/// Keys up to 64 bytes and values up to 64 bytes are stored directly in the
/// cache-line-aligned entry structure, avoiding heap allocation entirely.
/// Larger keys/values will be rejected by `set` / `incr`.
///
/// Consumers that need a different inline size can use
/// `KvStoreLockFree<128>`, `KvStoreLockFree<256>`, etc.
pub const DEFAULT_INLINE_SIZE: usize = 64;

/// Sentinel hash value marking a deleted slot in the lock-free hash table.
///
/// `0` means "never used" (empty), while `TOMBSTONE` means "was used, then
/// deleted". The distinction is critical for linear probing: a tombstone does
/// **not** terminate a probe chain, whereas an empty slot does.
const TOMBSTONE: u64 = u64::MAX; // 0xFFFF_FFFF_FFFF_FFFF

/// Map a hash of 0 (or TOMBSTONE) to a safe value so it never collides with
/// the "empty slot" sentinel (0) or the tombstone marker (TOMBSTONE).
#[inline]
fn normalize_hash(hash: u64) -> u64 {
    if hash == 0 || hash == TOMBSTONE { 1 } else { hash }
}

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

    /// Returns `true` if this slot is a tombstone (deleted).
    fn is_tombstone(&self) -> bool {
        self.hash == TOMBSTONE
    }
}

/// Custom Hash Table with Linear Probing (single-threaded).
pub struct KvStoreCustom {
    buckets: Vec<Entry>,
    count: usize,
    capacity: usize,
}

impl KvStoreCustom {
    /// Create an empty store with the default capacity (100 000 buckets).
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
    /// vacant slot exists, returns the first vacant slot index (preferring
    /// tombstone slots for reuse over truly empty slots).
    fn find_slot(&self, key: &[u8], hash: u64) -> (usize, bool) {
        let start = (hash as usize) % self.capacity;
        let mut first_tombstone: Option<usize> = None;

        for i in 0..self.capacity {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];

            if entry.is_empty() {
                // Truly empty slot — key not found.
                // Return first tombstone for insertion reuse, or this empty slot.
                return (first_tombstone.unwrap_or(idx), false);
            }

            if entry.is_tombstone() {
                if first_tombstone.is_none() {
                    first_tombstone = Some(idx);
                }
                continue;
            }

            if entry.hash == hash && entry.key == key {
                return (idx, true);
            }
        }

        // Table is full of occupied/tombstone entries.
        (first_tombstone.unwrap_or(0), false)
    }

    /// Return a clone of the value for *key*, or `None`.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let hash = normalize_hash(Self::hash_key(key));
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

        let hash = normalize_hash(Self::hash_key(key));
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
    /// Uses backward-shift deletion to maintain probe chain integrity
    /// (no tombstones left behind).
    pub fn del(&mut self, key: &[u8]) -> bool {
        let hash = normalize_hash(Self::hash_key(key));
        let (idx, found) = self.find_slot(key, hash);

        if !found {
            return false;
        }

        self.count -= 1;

        // Backward-shift deletion: shift subsequent entries backward
        // to fill the gap, maintaining probe chain integrity.
        let mut gap = idx;

        for i in 1..self.capacity {
            let j = (idx + i) % self.capacity;
            if self.buckets[j].is_empty() || self.buckets[j].is_tombstone() {
                break;
            }
            let home = (self.buckets[j].hash as usize) % self.capacity;
            // Distance from home to gap and from home to j (circular).
            let dist_home_gap = (gap as isize - home as isize)
                .rem_euclid(self.capacity as isize) as usize;
            let dist_home_j = (j as isize - home as isize)
                .rem_euclid(self.capacity as isize) as usize;

            if dist_home_gap < dist_home_j {
                self.buckets[gap] = self.buckets[j].clone();
                gap = j;
            }
        }

        self.buckets[gap] = Entry::empty();
        true
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

/// A single entry in the lock-free hash table.
///
/// **Memory layout** (cache-line aligned):
///
/// | Field      | Size          | Description                        |
/// |------------|---------------|------------------------------------|
/// | `hash`     | 8 B           | wyhash-inspired hash; 0 = empty    |
/// | `key_len`  | 4 B           | Length of the key                  |
/// | `value_len`| 4 B           | Length of the value                |
/// | `version`  | 8 B           | Optimistic-read version counter    |
/// | `data`     | `N * 2` bytes | `data[0]` = key, `data[1]` = value |
///
/// The `data` field uses `[[AtomicU8; N]; 2]` — an array of two N-byte
/// banks — so that no arithmetic on the const generic `N` is required
/// (stable Rust forbids `N * 2` in type positions).
///
/// - `data[0][0..key_len]` holds the key bytes.
/// - `data[1][0..value_len]` holds the value bytes.
///
/// Each byte is an `AtomicU8` to support individual byte reads without locking.
#[repr(C, align(64))]
struct LockFreeEntry<const N: usize> {
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
    /// Inline key + value storage: `data[0]` = key, `data[1]` = value.
    data: [[AtomicU8; N]; 2],
}

// SAFETY: all fields are atomic.
unsafe impl<const N: usize> Send for LockFreeEntry<N> {}
unsafe impl<const N: usize> Sync for LockFreeEntry<N> {}

impl<const N: usize> LockFreeEntry<N> {
    /// Create a new empty entry (all zeros).
    ///
    /// Kept for direct use in tests; production code uses
    /// `KvStoreLockFree::allocate_zeroed_buckets` which is ~10× faster.
    #[allow(dead_code)]
    fn new() -> Self {
        Self {
            hash: AtomicU64::new(0),
            key_len: AtomicU32::new(0),
            value_len: AtomicU32::new(0),
            version: AtomicU64::new(0),
            data: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU8::new(0))),
        }
    }
}

/// Lock-free, thread-safe hash table.
///
/// # Const generic `N`
///
/// `N` is the per-side inline storage limit in bytes — both keys and values
/// may be up to `N` bytes.  The default is [`DEFAULT_INLINE_SIZE`] (64).
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
pub struct KvStoreLockFree<const N: usize = DEFAULT_INLINE_SIZE> {
    buckets: Box<[LockFreeEntry<N>]>,
    count: AtomicUsize,
    capacity: usize,
}

// SAFETY: all fields are either atomic or immutable.
unsafe impl<const N: usize> Send for KvStoreLockFree<N> {}
unsafe impl<const N: usize> Sync for KvStoreLockFree<N> {}

impl<const N: usize> KvStoreLockFree<N> {
    /// Create a store with the default capacity (100 000 entries).
    ///
    /// Memory usage: ~19 MB for N=64, ~55 MB for N=256.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a store with a custom number of pre-allocated buckets.
    ///
    /// Uses zeroed allocation: since `LockFreeEntry` with all-zero fields
    /// represents an empty slot (hash=0), we can use `alloc_zeroed` instead
    /// of initializing each entry individually. This is ~10× faster for
    /// large capacities.
    ///
    /// # Memory usage
    ///
    /// | Capacity | N=64  | N=128 | N=256 |
    /// |----------|-------|-------|-------|
    /// | 1K       | 0.2 MB| 0.3 MB| 0.5 MB|
    /// | 10K      | 1.8 MB| 2.7 MB| 5.5 MB|
    /// | 100K     | 19 MB | 27 MB | 55 MB |
    /// | 1M       | 183 MB| 275 MB| 550 MB|
    pub fn with_capacity(capacity: usize) -> Self {
        let buckets = Self::allocate_zeroed_buckets(capacity);

        Self {
            buckets,
            count: AtomicUsize::new(0),
            capacity,
        }
    }

    /// Allocates a zeroed bucket slice.
    ///
    /// SAFETY: `LockFreeEntry<N>` is `#[repr(C, align(64))]` and all-zero
    /// bytes represent a valid empty entry (hash=0, key_len=0, value_len=0,
    /// version=0, data=[0; N*2]). This is equivalent to `LockFreeEntry::new()`
    /// but avoids the per-element constructor overhead.
    fn allocate_zeroed_buckets(capacity: usize) -> Box<[LockFreeEntry<N>]> {
        if capacity == 0 {
            return Vec::new().into_boxed_slice();
        }

        let entry_size = std::mem::size_of::<LockFreeEntry<N>>();
        let entry_align = std::mem::align_of::<LockFreeEntry<N>>();
        let total_size = capacity.checked_mul(entry_size).expect("capacity overflow");

        let layout = Layout::from_size_align(total_size, entry_align)
            .expect("invalid layout");

        // SAFETY: layout.size() > 0 (capacity > 0), layout is valid.
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        // SAFETY: ptr points to `capacity` contiguous zeroed bytes,
        // which is a valid representation of `capacity` empty LockFreeEntry<N>.
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut LockFreeEntry<N>, capacity) };

        // Transfer ownership to Box<[LockFreeEntry<N>]>.
        // When the Box is dropped, dealloc uses Layout::for_value(&*box)
        // which computes size = capacity * size_of::<LockFreeEntry<N>>()
        // and align = align_of::<LockFreeEntry<N>>() = 64, matching our layout.
        unsafe { Box::from_raw(slice as *mut [LockFreeEntry<N>]) }
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
    /// Returns `None` if the key does not exist or if key length exceeds `N`.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if key.len() > N {
            return None;
        }

        let hash = normalize_hash(Self::hash_key(key));
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
                if entry_hash == TOMBSTONE { continue; }
                if entry_hash != hash { continue; }

                let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                if key_len != key.len() { continue; }

                // Stack-allocated key buffer — no heap allocation.
                let mut entry_key = [0u8; N];
                for j in 0..key_len {
                    entry_key[j] = entry.data[0][j].load(Ordering::Relaxed);
                }

                if &entry_key[..key_len] != key { continue; }

                let value_len = entry.value_len.load(Ordering::Acquire) as usize;
                let mut value = vec![0u8; value_len];
                for j in 0..value_len {
                    value[j] = entry.data[1][j].load(Ordering::Relaxed);
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
    /// Both key and value must be at most `N` bytes.
    /// Returns `false` if either exceeds the limit or if the table is full.
    pub fn set(&self, key: &[u8], value: &[u8]) -> bool {
        if key.len() > N || value.len() > N {
            return false;
        }

        let hash = normalize_hash(Self::hash_key(key));
        let start = (hash as usize) % self.capacity;

        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);
        let mut first_tombstone: Option<usize> = None;

        for i in 0..probe_limit {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);

            if entry_hash == 0 {
                // Empty slot — key not found, try to insert.
                // Prefer reusing a tombstone slot if we saw one earlier.
                if let Some(ts_idx) = first_tombstone {
                    let ts_entry = &self.buckets[ts_idx];
                    if ts_entry.hash.compare_exchange(
                        TOMBSTONE, hash, Ordering::AcqRel, Ordering::Acquire,
                    ).is_ok() {
                        self.write_entry(ts_entry, key, value);
                        self.count.fetch_add(1, Ordering::Relaxed);
                        return true;
                    }
                    // Tombstone CAS failed; fall through to try empty slot.
                }

                match entry.hash.compare_exchange(
                    0, hash, Ordering::AcqRel, Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.write_entry(entry, key, value);
                        self.count.fetch_add(1, Ordering::Relaxed);
                        return true;
                    }
                    Err(actual) if actual != hash => continue,
                    _ => {} // CAS failed but hash matches, fall through to key check
                }
            }

            if entry_hash == TOMBSTONE {
                if first_tombstone.is_none() {
                    first_tombstone = Some(idx);
                }
                continue;
            }

            if entry_hash == hash {
                let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                if key_len == key.len() {
                    // Stack-allocated key buffer — no heap allocation.
                    let mut entry_key = [0u8; N];
                    for j in 0..key_len {
                        entry_key[j] = entry.data[0][j].load(Ordering::Relaxed);
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
        if key.len() > N {
            return false;
        }

        let hash = normalize_hash(Self::hash_key(key));
        let start = (hash as usize) % self.capacity;
        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        // Retry loop: if a concurrent writer changes the version between
        // our hash check and our tombstone store, we retry.
        // Fixes C9: del without retry could silently fail under contention.
        for _attempt in 0..3 {
            for i in 0..probe_limit {
                let idx = (start + i) % self.capacity;
                let entry = &self.buckets[idx];
                let entry_hash = entry.hash.load(Ordering::Acquire);

                if entry_hash == 0 { return false; }
                if entry_hash == TOMBSTONE { continue; }
                if entry_hash == hash {
                    let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                    if key_len == key.len() {
                        let mut entry_key = [0u8; N];
                        for j in 0..key_len {
                            entry_key[j] = entry.data[0][j].load(Ordering::Relaxed);
                        }
                        if &entry_key[..key_len] == key {
                            let v = entry.version.fetch_add(1, Ordering::AcqRel);
                            entry.hash.store(TOMBSTONE, Ordering::Release);
                            entry.version.store(v + 2, Ordering::Release);
                            self.count.fetch_sub(1, Ordering::Relaxed);
                            return true;
                        }
                    }
                }
            }
            // If we didn't find it, it may have been temporarily in flux.
            // Retry once more before giving up.
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
        if key.len() > N {
            return false;
        }

        let hash = normalize_hash(Self::hash_key(key));
        let start = (hash as usize) % self.capacity;
        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        for i in 0..probe_limit {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);

            if entry_hash == 0 { return false; }
            if entry_hash == TOMBSTONE { continue; }
            if entry_hash != hash { continue; }

            let key_len = entry.key_len.load(Ordering::Acquire) as usize;
            if key_len != key.len() { continue; }

            let mut entry_key = [0u8; N];
            for j in 0..key_len {
                entry_key[j] = entry.data[0][j].load(Ordering::Relaxed);
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
        if key.len() > N {
            return Err(IncrError::KeyTooLong);
        }

        let hash = normalize_hash(Self::hash_key(key));
        let start = (hash as usize) % self.capacity;

        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        for i in 0..probe_limit {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);

            if entry_hash == 0 {
                return Err(IncrError::KeyNotFound);
            }
            if entry_hash == TOMBSTONE { continue; }
            if entry_hash != hash { continue; }

            // Verify key match.
            let key_len = entry.key_len.load(Ordering::Acquire) as usize;
            if key_len != key.len() { continue; }
            let mut entry_key = [0u8; N];
            for j in 0..key_len {
                entry_key[j] = entry.data[0][j].load(Ordering::Relaxed);
            }
            if &entry_key[..key_len] != key { continue; }

            // Key found — optimistic read-modify-write loop.
            loop {
                // 1. Snapshot version (must be even).
                let v1 = entry.version.load(Ordering::Acquire);
                if v1 % 2 == 1 { continue; }

                // 2. Read current value (stack-allocated, Relaxed reads).
                let value_len = entry.value_len.load(Ordering::Acquire) as usize;
                let mut value_bytes = [0u8; N];
                for j in 0..value_len {
                    value_bytes[j] = entry.data[1][j].load(Ordering::Relaxed);
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
                if new_str.len() > N {
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
                    entry.data[1][j].store(byte, Ordering::Relaxed);
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
    /// Uses an optimistic CAS loop (same protocol as [`incr`]) so the
    /// read-modify-write is **linearizable** under concurrency.
    ///
    /// Returns the new length of the string value, or `None` if the result
    /// would exceed `N`.
    pub fn append(&self, key: &[u8], suffix: &[u8]) -> Option<usize> {
        if key.len() > N || suffix.len() > N { return None; }

        let hash = normalize_hash(Self::hash_key(key));
        let start = (hash as usize) % self.capacity;
        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        // Try to find existing key and atomically append.
        for i in 0..probe_limit {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);

            if entry_hash == 0 {
                // Key not found — fall through to create.
                break;
            }
            if entry_hash == TOMBSTONE { continue; }
            if entry_hash != hash { continue; }

            // Verify key match.
            let key_len = entry.key_len.load(Ordering::Acquire) as usize;
            if key_len != key.len() { continue; }

            let mut entry_key = [0u8; N];
            for j in 0..key_len {
                entry_key[j] = entry.data[0][j].load(Ordering::Relaxed);
            }
            if &entry_key[..key_len] != key { continue; }

            // Key found — optimistic read-modify-write loop (same as INCR).
            loop {
                // 1. Snapshot version (must be even).
                let v1 = entry.version.load(Ordering::Acquire);
                if v1 % 2 == 1 { continue; }

                // 2. Read current value (stack-allocated, Relaxed reads).
                let cur_len = entry.value_len.load(Ordering::Acquire) as usize;
                let mut cur_val = [0u8; N];
                for j in 0..cur_len {
                    cur_val[j] = entry.data[1][j].load(Ordering::Relaxed);
                }

                // 3. Verify version hasn't changed.
                let v2 = entry.version.load(Ordering::Acquire);
                if v1 != v2 { continue; }

                // 4. Compute new value.
                let new_len = cur_len + suffix.len();
                if new_len > N { return None; }
                let mut new_val = [0u8; N];
                new_val[..cur_len].copy_from_slice(&cur_val[..cur_len]);
                new_val[cur_len..new_len].copy_from_slice(suffix);

                // 5. CAS version from even → odd to claim exclusive write.
                if entry.version.compare_exchange(
                    v1, v1 + 1, Ordering::AcqRel, Ordering::Acquire,
                ).is_err() {
                    continue; // Someone else is writing; retry.
                }

                // 6. We hold exclusive access — write new value.
                entry.value_len.store(new_len as u32, Ordering::Release);
                for j in 0..new_len {
                    entry.data[1][j].store(new_val[j], Ordering::Relaxed);
                }
                entry.version.store(v1 + 2, Ordering::Release);

                return Some(new_len);
            }
        }

        // Key not found — create with suffix as initial value.
        if self.set(key, suffix) {
            Some(suffix.len())
        } else {
            None
        }
    }

    // -----------------------------------------------------------------------
    // Extended: STRLEN
    // -----------------------------------------------------------------------

    /// Return the length in bytes of the string value stored at *key*.
    ///
    /// Optimised over `get(key).map_or(0, |v| v.len())` — reads only the
    /// `value_len` atomic field from the entry instead of copying the
    /// entire value into a heap buffer.
    ///
    /// Returns `0` if the key does not exist (matches Redis semantics).
    pub fn strlen(&self, key: &[u8]) -> usize {
        if key.len() > N { return 0; }

        let hash = normalize_hash(Self::hash_key(key));
        let start = (hash as usize) % self.capacity;
        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        for _ in 0..3 {
            for i in 0..probe_limit {
                let idx = (start + i) % self.capacity;
                let entry = &self.buckets[idx];

                let v1 = entry.version.load(Ordering::Acquire);
                if v1 % 2 == 1 { continue; }

                let entry_hash = entry.hash.load(Ordering::Acquire);
                if entry_hash == 0 { return 0; }
                if entry_hash == TOMBSTONE { continue; }
                if entry_hash != hash { continue; }

                let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                if key_len != key.len() { continue; }

                let mut entry_key = [0u8; N];
                for j in 0..key_len {
                    entry_key[j] = entry.data[0][j].load(Ordering::Relaxed);
                }
                if &entry_key[..key_len] != key { continue; }

                // Key matched — read only value_len, skip value bytes.
                let value_len = entry.value_len.load(Ordering::Acquire) as usize;

                let v2 = entry.version.load(Ordering::Acquire);
                if v1 == v2 {
                    return value_len;
                }
                break; // Version changed — retry outer loop.
            }
        }
        0
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
    /// Uses an optimistic CAS loop so the read-modify-write is
    /// **linearizable** under concurrency.
    ///
    /// Returns the new length of the string, or `None` if the result would
    /// exceed `N`.
    pub fn setrange(&self, key: &[u8], offset: usize, replacement: &[u8]) -> Option<usize> {
        if key.len() > N { return None; }
        if replacement.is_empty() {
            return Some(self.strlen(key));
        }
        let end = offset.checked_add(replacement.len())?;

        let hash = normalize_hash(Self::hash_key(key));
        let start = (hash as usize) % self.capacity;
        let probe_limit = self.capacity.min(MAX_PROBE_STEPS);

        // Try to find existing key.
        for i in 0..probe_limit {
            let idx = (start + i) % self.capacity;
            let entry = &self.buckets[idx];
            let entry_hash = entry.hash.load(Ordering::Acquire);

            if entry_hash == 0 { break; }
            if entry_hash == TOMBSTONE { continue; }
            if entry_hash != hash { continue; }

            let key_len = entry.key_len.load(Ordering::Acquire) as usize;
            if key_len != key.len() { continue; }

            let mut entry_key = [0u8; N];
            for j in 0..key_len {
                entry_key[j] = entry.data[0][j].load(Ordering::Relaxed);
            }
            if &entry_key[..key_len] != key { continue; }

            // Key found — optimistic read-modify-write loop.
            loop {
                let v1 = entry.version.load(Ordering::Acquire);
                if v1 % 2 == 1 { continue; }

                let cur_len = entry.value_len.load(Ordering::Acquire) as usize;
                let mut cur_val = [0u8; N];
                for j in 0..cur_len {
                    cur_val[j] = entry.data[1][j].load(Ordering::Relaxed);
                }

                let v2 = entry.version.load(Ordering::Acquire);
                if v1 != v2 { continue; }

                let target_len = end.max(cur_len);
                if target_len > N { return None; }

                // Build new value: copy original, overwrite at offset.
                let mut new_val = [0u8; N];
                new_val[..cur_len].copy_from_slice(&cur_val[..cur_len]);
                new_val[offset..end].copy_from_slice(replacement);

                // CAS version from even → odd to claim exclusive write.
                if entry.version.compare_exchange(
                    v1, v1 + 1, Ordering::AcqRel, Ordering::Acquire,
                ).is_err() {
                    continue;
                }

                entry.value_len.store(target_len as u32, Ordering::Release);
                for j in 0..target_len {
                    entry.data[1][j].store(new_val[j], Ordering::Relaxed);
                }
                entry.version.store(v1 + 2, Ordering::Release);

                return Some(target_len);
            }
        }

        // Key not found — create with zero-padded value.
        let target_len = end;
        if target_len > N { return None; }
        let mut new_val = vec![0u8; target_len];
        new_val[offset..end].copy_from_slice(replacement);
        if self.set(key, &new_val) {
            Some(target_len)
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
    // SCAN — cursor-based key iteration
    // -----------------------------------------------------------------------

    /// Iterate over keys in the hash table using a cursor.
    ///
    /// Returns `(next_cursor, keys)`. When `next_cursor == 0`, the iteration
    /// is complete. The caller should call `scan` again with the returned
    /// cursor until it receives `0`.
    ///
    /// * `cursor` — starting bucket index (`0` to start from the beginning).
    /// * `count` — hint for how many keys to return per call (default 10).
    ///   The actual number may be slightly different.
    /// * `pattern` — optional glob pattern (`session:*`, `profile:ru-*`).
    ///   Only keys matching the pattern are returned.
    ///
    /// The implementation does a linear scan of buckets starting at `cursor`,
    /// collecting up to `count` matching keys. Lock-free: uses the same
    /// optimistic version-check protocol as `get`.
    pub fn scan(&self, cursor: usize, count: usize, pattern: Option<&[u8]>) -> (usize, Vec<Vec<u8>>) {
        let count = if count == 0 { 10 } else { count };
        let mut keys = Vec::with_capacity(count);
        let cap = self.capacity;

        if cap == 0 {
            return (0, keys);
        }

        let mut idx = cursor.min(cap - 1);

        // Scan through all buckets once.
        for _ in 0..cap {
            let entry = &self.buckets[idx];

            // Optimistic read: check version, read key, verify version.
            let v1 = entry.version.load(Ordering::Acquire);
            if v1 % 2 == 1 {
                // Write in progress — skip this entry.
                idx = (idx + 1) % cap;
                continue;
            }

            let entry_hash = entry.hash.load(Ordering::Acquire);
            if entry_hash != 0 && entry_hash != TOMBSTONE {
                // Occupied slot — read the key.
                let key_len = entry.key_len.load(Ordering::Acquire) as usize;
                if key_len > 0 && key_len <= N {
                    // Stack-allocated key buffer — avoids heap allocation per key.
                    let mut key_buf = [0u8; N];
                    for j in 0..key_len {
                        key_buf[j] = entry.data[0][j].load(Ordering::Relaxed);
                    }

                    let v2 = entry.version.load(Ordering::Acquire);
                    if v1 == v2 {
                        // Consistent read — check pattern.
                        let matches = match pattern {
                            Some(pat) => glob_match(&key_buf[..key_len], pat),
                            None => true,
                        };
                        if matches {
                            keys.push(key_buf[..key_len].to_vec());
                            if keys.len() >= count {
                                let next = (idx + 1) % cap;
                                return (next, keys);
                            }
                        }
                    }
                    // Version changed — skip this entry (don't risk stale data).
                }
            }

            idx = (idx + 1) % cap;
        }

        // We've scanned the entire table.
        (0, keys)
    }

    // -----------------------------------------------------------------------
    // DBSTATS — aggregate store statistics
    // -----------------------------------------------------------------------

    /// Return aggregate statistics about the key-value store.
    ///
    /// The returned struct contains counts and memory estimates that can be
    /// serialized to RESP or JSON for monitoring dashboards.
    pub fn dbstats(&self) -> DbStats {
        let total_keys = self.count.load(Ordering::Relaxed);
        let total_buckets = self.capacity;
        let load_factor = if total_buckets > 0 {
            total_keys as f64 / total_buckets as f64
        } else {
            0.0
        };

        // Estimate memory: each entry is cache-line aligned to 64 bytes +
        // N*2 bytes for inline data (key + value banks).
        let entry_size = std::mem::size_of::<LockFreeEntry<N>>();
        let total_memory = total_buckets * entry_size;

        // Count blob refs (0xFD flag in value[0]).
        let mut blob_count = 0usize;
        for i in 0..total_buckets {
            let entry = &self.buckets[i];
            let h = entry.hash.load(Ordering::Relaxed);
            if h == 0 || h == TOMBSTONE { continue; }
            let vl = entry.value_len.load(Ordering::Relaxed) as usize;
            if vl > 0 && vl <= N {
                let first = entry.data[1][0].load(Ordering::Relaxed);
                if first == 0xFD {
                    blob_count += 1;
                }
            }
        }

        DbStats {
            total_keys,
            total_buckets,
            load_factor,
            entry_size,
            total_memory,
            blob_count,
            inline_size: N,
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Write *key* and *value* into *entry* using the version protocol.
    ///
    /// Caller must have already "claimed" the entry (via CAS on `hash` or
    /// confirmed key match).
    ///
    /// Uses a CAS loop on the version counter to ensure **exclusive** write
    /// access.  This prevents a critical race where two concurrent `SET`
    /// operations on the same key could both enter this function and:
    ///
    /// 1. Leave the version counter in an odd (writing) state permanently,
    ///    causing all subsequent readers/writers to spin forever.
    /// 2. Produce a torn write where the final value is a mix of both
    ///    concurrent writes.
    ///
    /// The CAS guarantees that only one thread transitions the version from
    /// even → odd at a time; the loser retries until it observes a stable
    /// even version again.
    #[inline]
    fn write_entry(&self, entry: &LockFreeEntry<N>, key: &[u8], value: &[u8]) {
        loop {
            let v1 = entry.version.load(Ordering::Acquire);
            if v1 % 2 == 1 {
                // Another write is in progress — spin-wait.
                std::hint::spin_loop();
                continue;
            }
            // CAS: even → odd to claim exclusive write access.
            if entry.version.compare_exchange(
                v1, v1 + 1, Ordering::AcqRel, Ordering::Acquire,
            ).is_err() {
                continue; // Someone else modified the version; retry.
            }
            // We hold exclusive access — write the data.
            entry.key_len.store(key.len() as u32, Ordering::Release);
            for (j, &byte) in key.iter().enumerate() {
                entry.data[0][j].store(byte, Ordering::Relaxed);
            }
            entry.value_len.store(value.len() as u32, Ordering::Release);
            for (j, &byte) in value.iter().enumerate() {
                entry.data[1][j].store(byte, Ordering::Relaxed);
            }
            // Release: odd → even + 2 (preserving the version increment).
            entry.version.store(v1 + 2, Ordering::Release);
            return;
        }
    }
}

impl<const N: usize> Default for KvStoreLockFree<N> {
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
// DbStats — aggregate store statistics
// ============================================================================

/// Aggregate statistics about the key-value store.
///
/// Returned by [`KvStoreLockFree::dbstats`].
#[derive(Debug, Clone)]
pub struct DbStats {
    /// Number of keys currently stored.
    pub total_keys: usize,
    /// Total number of hash table buckets (capacity).
    pub total_buckets: usize,
    /// Load factor: `total_keys / total_buckets`.
    pub load_factor: f64,
    /// Size of a single `LockFreeEntry<N>` in bytes.
    pub entry_size: usize,
    /// Total memory used by the hash table in bytes (`total_buckets * entry_size`).
    pub total_memory: usize,
    /// Number of keys whose value is a blob ref (0xFD prefix).
    pub blob_count: usize,
    /// Per-side inline storage size (compile-time const generic `N`).
    pub inline_size: usize,
}

// ============================================================================
// Glob pattern matching
// ============================================================================

/// Simple glob pattern matching supporting `*` (any chars) and `?` (one char).
///
/// Pattern examples: `session:*`, `profile:ru-*`, `user:??`, `*`
///
/// No character class `[...]` support — keeping it simple for performance.
/// Pattern matching is case-sensitive.
pub fn glob_match(text: &[u8], pattern: &[u8]) -> bool {
    // Iterative glob matching with backtracking — O(n*m) worst case,
    // no exponential blowup. Fixes C8 (DoS via nested * patterns).
    let mut ti = 0usize;
    let mut pi = 0usize;
    let mut star_pi: Option<usize> = None;
    let mut match_ti = 0usize;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = Some(pi);
            match_ti = ti;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            match_ti += 1;
            ti = match_ti;
        } else {
            return false;
        }
    }

    // Skip trailing * in pattern
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// The production KV store — lock-free and thread-safe with the default
/// inline size ([`DEFAULT_INLINE_SIZE`] = 64 bytes per side).
pub type KvStore = KvStoreLockFree<DEFAULT_INLINE_SIZE>;

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
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        assert!(store.set(b"hello", b"world"));
        assert_eq!(store.get(b"hello"), Some(b"world".to_vec()));
        assert!(store.set(b"hello", b"rust"));
        assert_eq!(store.get(b"hello"), Some(b"rust".to_vec()));
        assert!(store.del(b"hello"));
        assert_eq!(store.get(b"hello"), None);
    }

    #[test]
    fn test_lockfree_multithreaded() {
        let store = Arc::new(KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(10000));
        let mut handles = vec![];

        for t in 0..4 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("t{}:k{}", t, i);
                    let value = format!("t{}:v{}", t, i);
                    assert!(store.set(key.as_bytes(), value.as_bytes()));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all writes
        for t in 0..4 {
            for i in 0..100 {
                let key = format!("t{}:k{}", t, i);
                let expected = format!("t{}:v{}", t, i);
                assert_eq!(store.get(key.as_bytes()), Some(expected.into_bytes()));
            }
        }
    }

    #[test]
    fn test_lockfree_incr() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        assert!(store.set(b"counter", b"0"));
        assert_eq!(store.incr(b"counter", 1), Ok(1));
        assert_eq!(store.incr(b"counter", 5), Ok(6));
        assert_eq!(store.decr(b"counter"), Ok(5));
        assert_eq!(store.get(b"counter"), Some(b"5".to_vec()));
    }

    #[test]
    fn test_lockfree_exists() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        assert!(!store.exists(b"missing"));
        assert!(store.set(b"present", b"yes"));
        assert!(store.exists(b"present"));
    }

    #[test]
    fn test_lockfree_too_large_key() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        let big_key = vec![0u8; DEFAULT_INLINE_SIZE + 1];
        assert!(!store.set(&big_key, b"val"));
        assert_eq!(store.get(&big_key), None);
    }

    #[test]
    fn test_lockfree_custom_inline_size() {
        // Use a larger inline size (256 bytes per side).
        let store = KvStoreLockFree::<256>::with_capacity(100);
        let big_key = vec![b'k'; 200];
        let big_val = vec![b'v'; 200];
        assert!(store.set(&big_key, &big_val));
        assert_eq!(store.get(&big_key), Some(big_val.clone()));
        // Key of 257 bytes should fail.
        assert!(!store.set(&vec![0u8; 257], b"val"));
    }

    // ----- MGET / MSET -----

    #[test]
    fn test_lockfree_mget_mset() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        let pairs: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        assert!(store.mset(&pairs));
        let vals = store.mget(&[b"a", b"b", b"c", b"missing"]);
        assert_eq!(vals[0], Some(b"1".to_vec()));
        assert_eq!(vals[1], Some(b"2".to_vec()));
        assert_eq!(vals[2], Some(b"3".to_vec()));
        assert_eq!(vals[3], None);
    }

    // ----- APPEND / STRLEN -----

    #[test]
    fn test_lockfree_append_strlen() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        assert!(store.set(b"s", b"hello"));
        assert_eq!(store.append(b"s", b" world"), Some(11));
        assert_eq!(store.strlen(b"s"), 11);
        assert_eq!(store.get(b"s"), Some(b"hello world".to_vec()));
    }

    // ----- GETRANGE / SETRANGE -----

    #[test]
    fn test_lockfree_getrange_setrange() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        assert!(store.set(b"g", b"Hello World"));
        assert_eq!(store.getrange(b"g", 0, 4), Some(b"Hello".to_vec()));
        assert_eq!(store.getrange(b"g", -5, -1), Some(b"World".to_vec()));
        assert_eq!(store.setrange(b"g", 6, b"Redis"), Some(11));
        assert_eq!(store.get(b"g"), Some(b"Hello Redis".to_vec()));
    }

    // ----- INCR edge cases -----

    #[test]
    fn test_lockfree_incr_not_found() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        assert_eq!(store.incr(b"missing", 1), Err(IncrError::KeyNotFound));
    }

    #[test]
    fn test_lockfree_incr_not_integer() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        assert!(store.set(b"str", b"hello"));
        assert_eq!(store.incr(b"str", 1), Err(IncrError::NotInteger));
    }

    #[test]
    fn test_lockfree_incr_overflow() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        assert!(store.set(b"big", i64::MAX.to_string().as_bytes()));
        assert_eq!(store.incr(b"big", 1), Err(IncrError::Overflow));
    }

    // ----- KvStore alias -----

    #[test]
    fn test_kvstore_alias() {
        let store = KvStore::with_capacity(100);
        assert!(store.set(b"alias", b"works"));
        assert_eq!(store.get(b"alias"), Some(b"works".to_vec()));
    }

    // ----- SCAN -----

    #[test]
    fn test_scan_empty_store() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
        let (cursor, keys) = store.scan(0, 10, None);
        assert_eq!(cursor, 0);
        assert!(keys.is_empty());
    }

    #[test]
    fn test_scan_all_keys() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(200);
        for i in 0..20 {
            let key = format!("key:{}", i);
            store.set(key.as_bytes(), b"val");
        }
        let mut all_keys = std::collections::HashSet::new();
        let mut cursor = 0usize;
        let mut iterations = 0;
        loop {
            let (next, keys) = store.scan(cursor, 5, None);
            for k in keys {
                all_keys.insert(k);
            }
            cursor = next;
            iterations += 1;
            if cursor == 0 || iterations > 100 { break; }
        }
        assert_eq!(all_keys.len(), 20);
    }

    #[test]
    fn test_scan_with_match() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(200);
        store.set(b"session:1", b"v1");
        store.set(b"session:2", b"v2");
        store.set(b"profile:1", b"p1");
        store.set(b"other", b"o1");

        let mut all_session = Vec::new();
        let mut cursor = 0usize;
        let mut iterations = 0;
        loop {
            let (next, keys) = store.scan(cursor, 100, Some(b"session:*"));
            all_session.extend(keys);
            cursor = next;
            iterations += 1;
            if cursor == 0 || iterations > 100 { break; }
        }
        assert_eq!(all_session.len(), 2);
        for k in &all_session {
            assert!(std::str::from_utf8(k).unwrap().starts_with("session:"));
        }
    }

    #[test]
    fn test_scan_no_match() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(200);
        store.set(b"hello", b"world");
        let (cursor, keys) = store.scan(0, 100, Some(b"zzz*"));
        assert_eq!(cursor, 0);
        assert!(keys.is_empty());
    }

    #[test]
    fn test_glob_match_star() {
        assert!(glob_match(b"session:1", b"session:*"));
        assert!(glob_match(b"session:12345", b"session:*"));
        assert!(!glob_match(b"profile:1", b"session:*"));
        assert!(glob_match(b"anything", b"*"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match(b"ab", b"a?"));
        assert!(!glob_match(b"abc", b"a?"));
        assert!(glob_match(b"abc", b"a?c"));
    }

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match(b"hello", b"hello"));
        assert!(!glob_match(b"hello", b"world"));
    }

    #[test]
    fn test_glob_match_prefix() {
        assert!(glob_match(b"hello world", b"hello*"));
        assert!(glob_match(b"hello", b"hello*"));
    }

    #[test]
    fn test_glob_match_multiple_stars() {
        assert!(glob_match(b"abcdefgh", b"a*c*f*h"));
        assert!(!glob_match(b"abcdefgh", b"a*c*z*h"));
    }

    // ----- DBSTATS -----

    #[test]
    fn test_dbstats_empty() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(200);
        let stats = store.dbstats();
        assert_eq!(stats.total_keys, 0);
        assert_eq!(stats.total_buckets, 200);
        assert_eq!(stats.blob_count, 0);
        assert_eq!(stats.inline_size, DEFAULT_INLINE_SIZE);
    }

    #[test]
    fn test_dbstats_with_data() {
        let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(200);
        for i in 0..50 {
            let key = format!("k{}", i);
            store.set(key.as_bytes(), b"val");
        }
        let stats = store.dbstats();
        assert_eq!(stats.total_keys, 50);
        assert!(stats.load_factor > 0.0);
        assert!(stats.total_memory > 0);
    }
}
