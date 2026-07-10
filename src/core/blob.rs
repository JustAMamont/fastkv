//! Blob Arena — compressed large-value storage.
//!
//! The blob arena stores compressed values that exceed the inline size of the
//! hash table. A small 33-byte reference ([`BlobRef`]) is stored in the
//! existing hash table value field, pointing to the compressed data in the
//! arena.
//!
//! ## BlobRef format (stored in the inline value field)
//!
//! ```text
//! Byte 0:    flag = 0xFD (blob reference marker)
//! Bytes 1-8: offset (u64 LE) — position in blob arena
//! Bytes 9-12: comp_len (u32 LE) — compressed data length
//! Bytes 13-16: orig_len (u32 LE) — original data length
//! Bytes 17-32: data_hash ([u8; 16]) — dual-crc32c hash
//! Total: 33 bytes — fits even in N=64 inline size
//! ```
//!
//! ## Architecture
//!
//! - **Chunked allocation**: 64 MB chunks, new chunks allocated on demand.
//! - **Lock-free write**: CAS on `write_offset` to atomically claim space.
//! - **Lock-free read**: data is immutable after write.
//! - **Free list**: sorted best-fit free list with binary search for O(log n) reuse.
//! - **Compression**: zstd (always compiled in; runtime-toggleable via `--no-blob-store`).
//! - **Hashing**: dual crc32c (two seeds) for 16-byte integrity check.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::cell::UnsafeCell;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic byte prefix that identifies a blob-reference value in the KV store.
pub const BLOB_REF_FLAG: u8 = 0xFD;

/// Size of a serialized [`BlobRef`] in bytes.
pub const BLOB_REF_SIZE: usize = 33;

/// Size of each chunk in the arena (64 MiB).
const CHUNK_SIZE: u64 = 64 * 1024 * 1024;

/// zstd compression level (1 = fast, 22 = max; 3 is a good default).
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Maximum number of free-list nodes. Beyond this, the capacity is dynamically
/// doubled to avoid discarding freed slots (which would cause a memory leak).
static FREE_LIST_CAPACITY: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(10_000);

// ---------------------------------------------------------------------------
// BlobRef — the 33-byte inline reference
// ---------------------------------------------------------------------------

/// A reference to compressed blob data stored in the arena.
///
/// Serialized as 33 bytes and stored directly in the hash table's inline
/// value field. See the [module-level documentation](self) for the binary
/// layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRef {
    /// Byte offset of the compressed data within the arena.
    pub offset: u64,
    /// Length of the compressed data in bytes.
    pub comp_len: u32,
    /// Length of the original (uncompressed) data in bytes.
    pub orig_len: u32,
    /// 16-byte integrity hash (dual crc32c).
    pub data_hash: [u8; 16],
}

impl BlobRef {
    /// Serialize this reference into a 33-byte array.
    pub fn encode(&self) -> [u8; BLOB_REF_SIZE] {
        let mut buf = [0u8; BLOB_REF_SIZE];
        buf[0] = BLOB_REF_FLAG;
        buf[1..9].copy_from_slice(&self.offset.to_le_bytes());
        buf[9..13].copy_from_slice(&self.comp_len.to_le_bytes());
        buf[13..17].copy_from_slice(&self.orig_len.to_le_bytes());
        buf[17..33].copy_from_slice(&self.data_hash);
        buf
    }

    /// Deserialize a blob reference from a byte slice.
    ///
    /// Returns `None` if the data is too short or does not start with the
    /// `0xFD` marker byte.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < BLOB_REF_SIZE || data[0] != BLOB_REF_FLAG {
            return None;
        }
        let offset = u64::from_le_bytes(data[1..9].try_into().ok()?);
        let comp_len = u32::from_le_bytes(data[9..13].try_into().ok()?);
        let orig_len = u32::from_le_bytes(data[13..17].try_into().ok()?);
        let mut data_hash = [0u8; 16];
        data_hash.copy_from_slice(&data[17..33]);
        Some(Self {
            offset,
            comp_len,
            orig_len,
            data_hash,
        })
    }
}

// ---------------------------------------------------------------------------
// Hashing — dual crc32c for 16 bytes
// ---------------------------------------------------------------------------

/// Compute a 16-byte hash of *data* using two crc32c passes with different
/// seeds.
///
/// The first 8 bytes are crc32c with seed 0x_5AFE_KV00, extended to u64
/// with a mixing step. The second 8 bytes use seed 0x_B10B_DA7A.
fn compute_hash(data: &[u8]) -> [u8; 16] {
    let h1 = dual_crc32c_half(data, 0x5AFE_B700);
    let h2 = dual_crc32c_half(data, 0xB10B_DA7A);
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&h1.to_le_bytes());
    out[8..16].copy_from_slice(&h2.to_le_bytes());
    out
}

/// Compute one half of the dual crc32c hash.
///
/// We compute crc32c with the given seed, then mix the result with the
/// seed to produce a u64. This gives us two independent 64-bit values
/// (from two different seeds) for a total of 128 bits.
fn dual_crc32c_half(data: &[u8], seed: u64) -> u64 {
    let crc = crc32c::crc32c_append(seed as u32, data);
    // Mix: combine crc with the seed to produce a u64.
    let lo = crc as u64;
    let hi = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15); // golden-ratio constant
    lo ^ hi ^ (lo << 32)
}

// ---------------------------------------------------------------------------
// Chunk — 64 MB allocation unit
// ---------------------------------------------------------------------------

/// A single 64 MB chunk of storage in the arena.
struct Chunk {
    /// The raw data buffer. UnsafeCell allows writing through shared references
    /// (safe because data is written once and then only read — the hash table
    /// entry storing the BlobRef is only published after the write completes).
    data: UnsafeCell<Box<[u8]>>,
    /// Number of bytes used in this chunk.
    used: AtomicU32,
}

// SAFETY: The Chunk data is written before the BlobRef is stored in the
// hash table, which provides a happens-before relationship. Reads after
// that point are safe because data is immutable.
unsafe impl Sync for Chunk {}

impl Chunk {
    /// Allocate a new zeroed 64 MB chunk.
    fn new() -> Self {
        let data = vec![0u8; CHUNK_SIZE as usize].into_boxed_slice();
        Self {
            data: UnsafeCell::new(data),
            used: AtomicU32::new(0),
        }
    }

    /// Remaining capacity in this chunk.
    #[allow(dead_code)]
    fn remaining(&self) -> u64 {
        CHUNK_SIZE - self.used.load(Ordering::Acquire) as u64
    }

    /// Write data at the given offset within this chunk.
    ///
    /// # Safety
    ///
    /// The caller must ensure that no concurrent reads occur at the same
    /// offset. In practice, this is guaranteed because the BlobRef is only
    /// stored in the hash table after the write completes.
    fn write_at(&self, offset: usize, data: &[u8]) {
        // SAFETY: We have exclusive access to this region because it was
        // just allocated via CAS on write_offset, and the BlobRef hasn't
        // been published yet.
        let buf = unsafe { &mut *self.data.get() };
        let end = offset + data.len();
        if end <= buf.len() {
            buf[offset..end].copy_from_slice(data);
        }
    }

    /// Read data from the given offset within this chunk.
    fn read_at(&self, offset: usize, len: usize) -> Option<Vec<u8>> {
        let buf = unsafe { &*self.data.get() };
        let end = offset.checked_add(len)?;
        if end > buf.len() {
            return None;
        }
        Some(buf[offset..end].to_vec())
    }
}

// ---------------------------------------------------------------------------
// BlobSlot — free-list entry
// ---------------------------------------------------------------------------

/// A freed region in the arena that can potentially be reused.
struct BlobSlot {
    /// Byte offset of the freed region.
    offset: u64,
    /// Capacity of the freed region (the original compressed data length).
    len: u32,
}

// ---------------------------------------------------------------------------
// Sorted free list (best-fit, Mutex-guarded — cold path)
// ---------------------------------------------------------------------------

/// A sorted free-list for reclaimed blob slots.
///
/// The Vec is kept sorted by `len` in ascending order so that
/// [`find_fit`] can use binary search for O(log n) best-fit lookup.
/// Push is O(n) to maintain sort order, but this is the cold path (deletions
/// are rare) and the list is typically short (<100 entries).
///
/// A capacity cap ([`FREE_LIST_CAPACITY`]) prevents unbounded growth.
struct FreeList {
    /// Sorted slots (ascending by `len`).
    slots: std::sync::Mutex<Vec<BlobSlot>>,
    /// Approximate count (Relaxed — for stats and capacity guard).
    count: AtomicUsize,
}

impl FreeList {
    fn new() -> Self {
        Self {
            slots: std::sync::Mutex::new(Vec::new()),
            count: AtomicUsize::new(0),
        }
    }

    /// Insert a freed slot, maintaining sort order by `len`.
    /// Returns `false` if the capacity limit has been reached (after attempting
    /// to double capacity). When false is returned, the slot was discarded.
    fn push(&self, slot: BlobSlot) -> bool {
        let capacity = FREE_LIST_CAPACITY.load(Ordering::Relaxed);

        // Capacity guard — if at limit, try to double the capacity.
        if self.count.load(Ordering::Relaxed) >= capacity {
            // Attempt to grow the capacity limit by 2×.
            let new_capacity = capacity.saturating_mul(2);
            if FREE_LIST_CAPACITY.compare_exchange(
                capacity, new_capacity, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                eprintln!(
                    "[BlobArena] Free list capacity increased from {} to {} slots",
                    capacity, new_capacity
                );
            } else {
                // Another thread already changed the capacity; reload.
                // If still at capacity after the change, discard the slot
                // with a warning.
                let current_capacity = FREE_LIST_CAPACITY.load(Ordering::Relaxed);
                if self.count.load(Ordering::Relaxed) >= current_capacity {
                    eprintln!(
                        "[BlobArena] Warning: free list at capacity ({} slots), discarding freed slot at offset {} ({} bytes). Memory may not be reused.",
                        current_capacity, slot.offset, slot.len
                    );
                    return false;
                }
            }
        }

        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        // Binary search for insertion point (sorted by len).
        let pos = slots.binary_search_by(|s| s.len.cmp(&slot.len));
        let idx = match pos {
            Ok(i) => i,       // duplicate len — insert before
            Err(i) => i,      // insertion point
        };
        slots.insert(idx, slot);
        self.count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Find and remove the smallest slot that fits `min_len` (best-fit).
    /// Uses binary search for O(log n) lookup, then removes the entry.
    fn find_fit(&self, min_len: u32) -> Option<BlobSlot> {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        // Binary search for the first slot with len >= min_len.
        let pos = slots.binary_search_by(|s| s.len.cmp(&min_len));
        let idx = match pos {
            Ok(i) => i,
            Err(i) => {
                // i is the insertion point — the first slot with len > min_len.
                // If i == slots.len(), no slot fits.
                if i >= slots.len() {
                    return None;
                }
                i
            }
        };
        // slots[idx] is the best fit (smallest len >= min_len).
        self.count.fetch_sub(1, Ordering::Relaxed);
        Some(slots.remove(idx))
    }

    /// Approximate number of entries in the free list.
    fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// BlobArena
// ---------------------------------------------------------------------------

/// Compressed large-value storage arena.
///
/// Values are compressed with zstd and stored in 64 MB chunks. A small
/// [`BlobRef`] (33 bytes) is returned and can be stored in the existing
/// hash table's inline value field.
///
/// # Concurrency
///
/// - **Write** (`store`): lock-free via CAS on `write_offset`.
/// - **Read** (`retrieve`): lock-free (data is immutable after write).
/// - **Free** (`free`): acquires the free-list mutex (cold path).
/// - **New chunk**: acquires the chunks `RwLock` (rare operation).
pub struct BlobArena {
    /// All allocated chunks.
    chunks: RwLock<Vec<Arc<Chunk>>>,
    /// Index of the current (writeable) chunk.
    current_chunk: AtomicUsize,
    /// Global write offset — CAS to atomically claim space.
    write_offset: AtomicU64,
    /// Total bytes used (compressed data stored).
    total_used: AtomicU64,
    /// Total bytes of compressed data ever stored.
    total_compressed: AtomicU64,
    /// Total bytes of original (uncompressed) data ever stored.
    total_original: AtomicU64,
    /// Sorted free list for reclaimed blob slots (best-fit via binary search).
    free_list: FreeList,
}

// SAFETY: all fields are thread-safe (atomic, RwLock, Mutex, Arc).
unsafe impl Send for BlobArena {}
unsafe impl Sync for BlobArena {}

impl BlobArena {
    /// Create a new empty blob arena with one pre-allocated 64 MB chunk.
    pub fn new() -> Self {
        let initial_chunk = Arc::new(Chunk::new());
        Self {
            chunks: RwLock::new(vec![initial_chunk]),
            current_chunk: AtomicUsize::new(0),
            write_offset: AtomicU64::new(0),
            total_used: AtomicU64::new(0),
            total_compressed: AtomicU64::new(0),
            total_original: AtomicU64::new(0),
            free_list: FreeList::new(),
        }
    }

    /// Store compressed data in the arena.
    ///
    /// Compresses *original_data* with zstd, writes it to a chunk, and
    /// returns a [`BlobRef`] that can be used to retrieve the data later.
    /// The caller should store `BlobRef::encode()` in the hash table value.
    ///
    /// Returns `None` if compression fails or the data cannot be stored
    /// (extremely unlikely — would require running out of memory).
    pub fn store(&self, original_data: &[u8]) -> Option<BlobRef> {
        let orig_len = original_data.len() as u32;

        // Compress with zstd.
        let compressed = zstd::bulk::compress(original_data, ZSTD_COMPRESSION_LEVEL)
            .ok()?;
        let comp_len = compressed.len() as u32;

        // Compute integrity hash.
        let data_hash = compute_hash(original_data);

        // Try to find space in the free list first (cold path).
        // Use best-fit (smallest slot >= comp_len) via binary search.
        if let Some(slot) = self.free_list.find_fit(comp_len) {
            // Write data at the freed slot's offset.
            self.write_at(slot.offset, &compressed);
            // Update stats.
            self.total_compressed.fetch_add(comp_len as u64, Ordering::Relaxed);
            self.total_original.fetch_add(orig_len as u64, Ordering::Relaxed);

            return Some(BlobRef {
                offset: slot.offset,
                comp_len,
                orig_len,
                data_hash,
            });
        }

        // No free slot found — allocate new space via CAS.
        let comp_len_usize = compressed.len();
        let (offset, _chunk_idx) = self.allocate_space(comp_len_usize)?;

        // Write data to the chunk.
        self.write_at(offset, &compressed);

        self.total_used.fetch_add(comp_len as u64, Ordering::Relaxed);
        self.total_compressed.fetch_add(comp_len as u64, Ordering::Relaxed);
        self.total_original.fetch_add(orig_len as u64, Ordering::Relaxed);

        Some(BlobRef {
            offset,
            comp_len,
            orig_len,
            data_hash,
        })
    }

    /// Retrieve and decompress data from the arena.
    ///
    /// Returns the original uncompressed data, or `None` if the hash check
    /// fails or decompression fails.
    pub fn retrieve(&self, blob_ref: &BlobRef) -> Option<Vec<u8>> {
        let compressed = self.read_at(blob_ref.offset, blob_ref.comp_len as usize)?;

        // Decompress with zstd.
        let decompressed = zstd::bulk::decompress(&compressed, blob_ref.orig_len as usize)
            .ok()?;

        // Verify integrity hash.
        let actual_hash = compute_hash(&decompressed);
        if actual_hash != blob_ref.data_hash {
            return None;
        }

        Some(decompressed)
    }

    /// Retrieve the raw compressed bytes from the arena (no decompression).
    ///
    /// Useful for `BGETRAW` which returns compressed data as-is.
    pub fn retrieve_raw(&self, blob_ref: &BlobRef) -> Option<Vec<u8>> {
        self.read_at(blob_ref.offset, blob_ref.comp_len as usize)
    }

    /// Mark space as freed when a blob key is deleted.
    ///
    /// The space is added to a sorted free list for potential reuse. This is
    /// the cold path and acquires the free-list mutex.
    ///
    /// If the free list is at capacity, the capacity is dynamically doubled.
    /// If doubling fails (e.g., due to extreme concurrency), a warning is
    /// logged and the slot is discarded — but `total_used` is still decremented
    /// so that stats remain correct.
    pub fn free(&self, blob_ref: &BlobRef) {
        // Add to sorted free list (best-fit for future reuse).
        let pushed = self.free_list.push(BlobSlot {
            offset: blob_ref.offset,
            len: blob_ref.comp_len,
        });

        if !pushed {
            // Slot was discarded (free list still at capacity after doubling attempt).
            // Log a warning — the memory at this offset cannot be reused,
            // but the stats are still updated correctly below.
            eprintln!(
                "[BlobArena] Warning: freed blob slot at offset {} ({} bytes) was discarded — free list full. Memory will not be reused.",
                blob_ref.offset, blob_ref.comp_len
            );
        }

        // Update stats — always decrement total_used, even if the slot
        // was discarded, so that stats stay correct.
        self.total_used.fetch_sub(blob_ref.comp_len as u64, Ordering::Relaxed);
    }

    /// Get arena statistics.
    pub fn stats(&self) -> BlobStats {
        let total_used = self.total_used.load(Ordering::Relaxed);
        let total_compressed = self.total_compressed.load(Ordering::Relaxed);
        let total_original = self.total_original.load(Ordering::Relaxed);
        let free_slots = self.free_list.len();

        let compression_ratio = if total_original > 0 {
            total_compressed as f64 / total_original as f64
        } else {
            0.0
        };

        BlobStats {
            total_used,
            total_compressed,
            total_original,
            compression_ratio,
            free_slots,
        }
    }

    /// Check if an inline value is a blob reference.
    ///
    /// A value is a blob reference if it starts with the `0xFD` marker byte
    /// and is at least [`BLOB_REF_SIZE`] bytes long.
    pub fn is_blob_ref(value: &[u8]) -> bool {
        value.len() >= BLOB_REF_SIZE && value[0] == BLOB_REF_FLAG
    }
}

impl Default for BlobArena {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// BlobArena — internal helpers
// ---------------------------------------------------------------------------

impl BlobArena {
    /// Allocate *len* bytes of space in the arena via CAS on write_offset.
    ///
    /// Returns `(offset, chunk_index)` or `None` if allocation fails.
    fn allocate_space(&self, len: usize) -> Option<(u64, usize)> {
        let len_u64 = len as u64;

        loop {
            let current_offset = self.write_offset.load(Ordering::Acquire);
            let current_chunk_idx = self.current_chunk.load(Ordering::Acquire);

            // Check if the current chunk has space.
            let chunks = self.chunks.read().unwrap_or_else(|e| e.into_inner());
            if current_chunk_idx >= chunks.len() {
                // Shouldn't happen, but handle gracefully.
                drop(chunks);
                self.ensure_chunk_capacity(current_chunk_idx);
                continue;
            }

            let chunk = &chunks[current_chunk_idx];
            let chunk_start = current_chunk_idx as u64 * CHUNK_SIZE;
            let chunk_used = current_offset - chunk_start;

            if chunk_used + len_u64 <= CHUNK_SIZE {
                // Current chunk has space. Try to claim via CAS.
                match self.write_offset.compare_exchange(
                    current_offset,
                    current_offset + len_u64,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Claimed! Update chunk used counter.
                        chunk.used.fetch_add(len as u32, Ordering::Relaxed);
                        return Some((current_offset, current_chunk_idx));
                    }
                    Err(_) => continue, // Another thread claimed first; retry.
                }
            }

            // Current chunk is full — need a new chunk.
            drop(chunks);
            self.ensure_chunk_capacity(current_chunk_idx + 1);

            // Try to advance current_chunk via CAS.
            let _ = self.current_chunk.compare_exchange(
                current_chunk_idx,
                current_chunk_idx + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            // Also reset write_offset to the start of the new chunk.
            let new_offset = (current_chunk_idx as u64 + 1) * CHUNK_SIZE;
            let _ = self.write_offset.compare_exchange(
                current_offset,
                new_offset,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    /// Ensure that chunk *index* exists (allocate if needed).
    fn ensure_chunk_capacity(&self, index: usize) {
        let chunks = self.chunks.read().unwrap_or_else(|e| e.into_inner());
        if index < chunks.len() {
            return;
        }
        drop(chunks);

        // Need write lock to add a chunk.
        let mut chunks = self.chunks.write().unwrap_or_else(|e| e.into_inner());
        // Double-check under write lock.
        while index >= chunks.len() {
            chunks.push(Arc::new(Chunk::new()));
        }
    }

    /// Write *data* at the given *offset* in the arena.
    fn write_at(&self, offset: u64, data: &[u8]) {
        let chunks = self.chunks.read().unwrap_or_else(|e| e.into_inner());
        let chunk_idx = (offset / CHUNK_SIZE) as usize;
        let offset_in_chunk = (offset % CHUNK_SIZE) as usize;

        if chunk_idx >= chunks.len() {
            return;
        }

        let chunk = &chunks[chunk_idx];
        chunk.write_at(offset_in_chunk, data);
    }

    /// Read *len* bytes starting at *offset* from the arena.
    fn read_at(&self, offset: u64, len: usize) -> Option<Vec<u8>> {
        let chunks = self.chunks.read().unwrap_or_else(|e| e.into_inner());
        let chunk_idx = (offset / CHUNK_SIZE) as usize;
        let offset_in_chunk = (offset % CHUNK_SIZE) as usize;

        let chunk = chunks.get(chunk_idx)?;
        chunk.read_at(offset_in_chunk, len)
    }
}

// ---------------------------------------------------------------------------
// BlobStats
// ---------------------------------------------------------------------------

/// Statistics about the blob arena's usage.
#[derive(Debug, Clone)]
pub struct BlobStats {
    /// Total bytes currently used in the arena.
    pub total_used: u64,
    /// Total bytes of compressed data ever stored.
    pub total_compressed: u64,
    /// Total bytes of original (uncompressed) data ever stored.
    pub total_original: u64,
    /// Compression ratio (compressed / original). Lower is better.
    pub compression_ratio: f64,
    /// Number of entries in the free list.
    pub free_slots: usize,
}

impl std::fmt::Display for BlobStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "# Blob Arena\r\n\
             total_used:{}\r\n\
             total_compressed:{}\r\n\
             total_original:{}\r\n\
             compression_ratio:{:.4}\r\n\
             free_slots:{}\r\n",
            self.total_used,
            self.total_compressed,
            self.total_original,
            self.compression_ratio,
            self.free_slots,
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_arena() -> BlobArena {
        BlobArena::new()
    }

    // ----- BlobRef encode/decode -----

    #[test]
    fn test_blob_ref_roundtrip() {
        let blob_ref = BlobRef {
            offset: 0x1234_5678_9ABC_DEF0,
            comp_len: 1024,
            orig_len: 4096,
            data_hash: [0xAB; 16],
        };

        let encoded = blob_ref.encode();
        assert_eq!(encoded.len(), BLOB_REF_SIZE);
        assert_eq!(encoded[0], BLOB_REF_FLAG);

        let decoded = BlobRef::decode(&encoded).unwrap();
        assert_eq!(decoded.offset, blob_ref.offset);
        assert_eq!(decoded.comp_len, blob_ref.comp_len);
        assert_eq!(decoded.orig_len, blob_ref.orig_len);
        assert_eq!(decoded.data_hash, blob_ref.data_hash);
    }

    #[test]
    fn test_blob_ref_decode_invalid() {
        // Too short.
        assert!(BlobRef::decode(&[0xFD; 10]).is_none());
        // Wrong flag.
        assert!(BlobRef::decode(&[0xFE; 33]).is_none());
        // Empty.
        assert!(BlobRef::decode(&[]).is_none());
    }

    #[test]
    fn test_blob_ref_size_fits_inline() {
        // 33 bytes should fit in the default inline size (64 bytes).
        const { assert!(BLOB_REF_SIZE <= 64); }
    }

    // ----- is_blob_ref -----

    #[test]
    fn test_is_blob_ref() {
        let mut blob_data = vec![0u8; BLOB_REF_SIZE];
        blob_data[0] = BLOB_REF_FLAG;
        assert!(BlobArena::is_blob_ref(&blob_data));

        // Plain string — not a blob ref.
        assert!(!BlobArena::is_blob_ref(b"hello world"));

        // Hash value — not a blob ref.
        assert!(!BlobArena::is_blob_ref(&[0xFF, 0x00, 0x00]));

        // List value — not a blob ref.
        assert!(!BlobArena::is_blob_ref(&[0xFE]));

        // Too short — not a blob ref.
        assert!(!BlobArena::is_blob_ref(&[0xFD]));

        // Empty — not a blob ref.
        assert!(!BlobArena::is_blob_ref(b""));
    }

    // ----- Store and retrieve -----

    #[test]
    fn test_store_and_retrieve() {
        let arena = make_arena();
        let data = b"Hello, this is a test value for the blob arena! It should be compressible.";

        let blob_ref = arena.store(data).expect("store should succeed");
        assert!(blob_ref.comp_len > 0);
        assert_eq!(blob_ref.orig_len, data.len() as u32);

        let retrieved = arena.retrieve(&blob_ref).expect("retrieve should succeed");
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_store_retrieve_large() {
        let arena = make_arena();
        // Create a large, highly compressible payload.
        let data: Vec<u8> = (0u32..100_000).flat_map(|i| (i % 256).to_be_bytes()).collect();

        let blob_ref = arena.store(&data).expect("store should succeed");
        assert!(blob_ref.comp_len < blob_ref.orig_len, "should be compressed");

        let retrieved = arena.retrieve(&blob_ref).expect("retrieve should succeed");
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_store_multiple() {
        let arena = make_arena();

        let data1 = b"First blob value";
        let data2 = b"Second blob value -- a bit longer for variety";
        let data3 = b"Third value in the arena";

        let ref1 = arena.store(data1).unwrap();
        let ref2 = arena.store(data2).unwrap();
        let ref3 = arena.store(data3).unwrap();

        // Offsets should be different.
        assert_ne!(ref1.offset, ref2.offset);
        assert_ne!(ref2.offset, ref3.offset);

        // Each should be retrievable.
        assert_eq!(arena.retrieve(&ref1).unwrap(), data1);
        assert_eq!(arena.retrieve(&ref2).unwrap(), data2);
        assert_eq!(arena.retrieve(&ref3).unwrap(), data3);
    }

    // ----- Free -----

    #[test]
    fn test_free_and_reuse() {
        let arena = make_arena();
        let data = b"Some data to store and then free";

        let blob_ref = arena.store(data).unwrap();
        let offset = blob_ref.offset;

        arena.free(&blob_ref);

        let stats = arena.stats();
        assert_eq!(stats.free_slots, 1);

        // Store again — should reuse the freed slot.
        let new_ref = arena.store(b"Replacement data that fits").unwrap();
        // The offset should be reused from the free list.
        assert_eq!(new_ref.offset, offset);
    }

    // ----- Stats -----

    #[test]
    fn test_stats() {
        let arena = make_arena();

        let stats = arena.stats();
        assert_eq!(stats.total_used, 0);
        assert_eq!(stats.total_compressed, 0);
        assert_eq!(stats.total_original, 0);
        assert_eq!(stats.free_slots, 0);

        let data = b"Some data for stats testing";
        let blob_ref = arena.store(data).unwrap();

        let stats = arena.stats();
        assert!(stats.total_used > 0);
        assert!(stats.total_compressed > 0);
        assert_eq!(stats.total_original, data.len() as u64);
        // Compression ratio may be > 1.0 for very small data (zstd header overhead).
        assert!(stats.compression_ratio > 0.0);
        assert_eq!(stats.free_slots, 0);

        arena.free(&blob_ref);
        let stats = arena.stats();
        assert_eq!(stats.free_slots, 1);
    }

    // ----- Hash integrity -----

    #[test]
    fn test_hash_integrity_check() {
        let arena = make_arena();
        let data = b"Data with integrity check";

        let blob_ref = arena.store(data).unwrap();

        // Tamper with the hash — retrieve should fail.
        let mut bad_ref = blob_ref.clone();
        bad_ref.data_hash[0] ^= 0xFF;
        assert!(arena.retrieve(&bad_ref).is_none());
    }

    // ----- retrieve_raw -----

    #[test]
    fn test_retrieve_raw() {
        let arena = make_arena();
        let data = b"Raw compressed data test";

        let blob_ref = arena.store(data).unwrap();
        let raw = arena.retrieve_raw(&blob_ref).unwrap();

        // Raw data should be the compressed form (different from original).
        assert_ne!(raw.as_slice(), data);
        assert_eq!(raw.len(), blob_ref.comp_len as usize);

        // Should be decompressible.
        let decompressed = zstd::bulk::decompress(&raw, blob_ref.orig_len as usize).unwrap();
        assert_eq!(decompressed, data);
    }

    // ----- Display -----

    #[test]
    fn test_blob_stats_display() {
        let stats = BlobStats {
            total_used: 1024,
            total_compressed: 1024,
            total_original: 4096,
            compression_ratio: 0.25,
            free_slots: 3,
        };
        let s = stats.to_string();
        assert!(s.contains("total_used:1024"));
        assert!(s.contains("compression_ratio:0.2500"));
        assert!(s.contains("free_slots:3"));
    }

    // ----- Edge cases -----

    #[test]
    fn test_store_empty_data() {
        let arena = make_arena();
        // Empty data should still work (zstd can compress empty input).
        let blob_ref = arena.store(b"");
        // zstd should be able to compress empty data.
        assert!(blob_ref.is_some());
        if let Some(br) = blob_ref {
            assert_eq!(arena.retrieve(&br).unwrap(), b"");
        }
    }

    #[test]
    fn test_blob_ref_distinct_from_other_markers() {
        // 0xFD should not conflict with 0xFF (hash) or 0xFE (list).
        assert_ne!(BLOB_REF_FLAG, 0xFF); // hash magic
        assert_ne!(BLOB_REF_FLAG, 0xFE); // list magic
    }
}
