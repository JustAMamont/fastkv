//! WAL Compaction / Checkpoint (BGSAVE).
//!
//! # Overview
//!
//! The WAL only appends — over time it accumulates stale entries (overwritten
//! keys, deleted keys) that waste disk space. The checkpoint mechanism writes
//! a fresh, compact WAL containing **only** the current state (one SET per
//! live key + EXPIRE entries for keys with TTLs) and atomically replaces the
//! old WAL.
//!
//! # Atomic replacement protocol
//!
//! ```text
//! 1. Write compact WAL → fastkv.wal.new
//! 2. fsync fastkv.wal.new
//! 3. Rename fastkv.wal → fastkv.wal.bak
//! 4. Rename fastkv.wal.new → fastkv.wal
//! 5. Delete fastkv.wal.bak
//! ```
//!
//! Steps 3–4 are atomic on POSIX (rename is atomic on the same filesystem).
//! If the process crashes between steps 3 and 4, the backup file remains and
//! can be manually recovered. If it crashes before step 3, the original WAL
//! is untouched.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::kv::KvStoreLockFree;
use crate::core::wal::{FsyncPolicy, Wal};
use crate::core::expiration::ExpirationManager;
use crate::core::list::{ListManager, LIST_MAGIC, ListSubOp};

use crate::core::blob::{BlobArena, BlobRef};

// ---------------------------------------------------------------------------
// Checkpoint errors
// ---------------------------------------------------------------------------

/// Errors that can occur during checkpoint.
#[derive(Debug)]
pub enum CheckpointError {
    /// I/O error during file operations.
    Io(std::io::Error),
    /// WAL error during write.
    Wal(crate::core::wal::WalError),
    /// No WAL path available (WAL is disabled).
    NoWalPath,
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointError::Io(e) => write!(f, "checkpoint I/O error: {}", e),
            CheckpointError::Wal(e) => write!(f, "checkpoint WAL error: {}", e),
            CheckpointError::NoWalPath => write!(f, "checkpoint failed: no WAL path (WAL disabled)"),
        }
    }
}

impl std::error::Error for CheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CheckpointError::Io(e) => Some(e),
            CheckpointError::Wal(e) => Some(e),
            CheckpointError::NoWalPath => None,
        }
    }
}

impl From<std::io::Error> for CheckpointError {
    fn from(e: std::io::Error) -> Self {
        CheckpointError::Io(e)
    }
}

impl From<crate::core::wal::WalError> for CheckpointError {
    fn from(e: crate::core::wal::WalError) -> Self {
        CheckpointError::Wal(e)
    }
}

// ---------------------------------------------------------------------------
// Checkpoint implementation
// ---------------------------------------------------------------------------

/// Perform a checkpoint (BGSAVE): write a compact WAL with only the current
/// state and atomically replace the old WAL.
///
/// # Arguments
///
/// * `store` — The KV store to iterate.
/// * `expiry` — Optional expiration manager (for TTL entries).
/// * `wal_path` — Path to the current WAL file.
/// * `blob_arena` — Optional blob arena. Required to correctly persist blob
///   keys across restarts when the `blob-store` feature is enabled. If `None`
///   and blob keys are present, they will be written as plain `SET` entries
///   (with the 33-byte `BlobRef` as the value) and **will not be readable
///   after recovery** — a warning is logged for each such key.
///
/// # Returns
///
/// The number of keys written to the compact WAL, or an error.
pub fn checkpoint<const N: usize>(
    store: &KvStoreLockFree<N>,
    expiry: Option<&ExpirationManager<N>>,
    wal_path: &Path,
    lists: Option<&ListManager<N>>,
     blob_arena: Option<&BlobArena>,
) -> Result<usize, CheckpointError> {
    let wal_dir = wal_path.parent().unwrap_or_else(|| Path::new("."));
    let wal_file_name = wal_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "fastkv.wal".to_string());

    let new_path = wal_dir.join(format!("{}.new", wal_file_name));
    let bak_path = wal_dir.join(format!("{}.bak", wal_file_name));

    // Phase 2 (collect TTL entries first — small, safe to hold in memory).
    let ttl_entries: Vec<(Vec<u8>, u64)> = if let Some(exp) = expiry {
        exp.get_all_deadlines_ms()
    } else {
        Vec::new()
    };

    // Phase 3: Write the compact WAL.
    // Remove leftover .new file if it exists from a previous failed attempt.
    let _ = fs::remove_file(&new_path);

    let new_wal = Wal::open(&new_path, FsyncPolicy::Always)?;
    let mut written = 0usize;
    let mut total_keys_seen = 0usize;

    // Counters for the final log line.
    
    let mut blob_keys_written = 0usize;
    
    let mut blob_keys_degraded = 0usize;

    // STREAM keys in batches of 1000, writing each to the new WAL immediately.
    // This avoids holding all keys + all decompressed blob payloads in memory
    // at once (which previously caused OOM with ~18000 blob keys × ~100KB
    // decompressed each = ~1.8GB peak RSS for the checkpoint thread).
    let mut cursor = 0usize;
    loop {
        let (next_cursor, batch) = store.scan(cursor, 1000, None);
        if batch.is_empty() {
            break;
        }

        // Process this batch — values are dropped at the end of the iteration.
        for key in &batch {
            let value = match store.get(key) {
                Some(v) => v,
                None => continue,
            };

            // List key: write SET(magic) to mark the key as a list, then
            // write a single RPUSH LIST_OP entry containing all current
            // elements. On recovery, replay_list_op() rebuilds the list
            // contents. Without this, list keys would survive restart
            // (the magic byte is restored) but their contents would be
            // empty.
            if value.len() == 1 && value[0] == LIST_MAGIC
                && let Some(lists) = lists {
                    // Write SET(magic) first so the key exists in the hash
                    // table when the LIST_OP entry is replayed.
                    new_wal.set(key, &value)?;
                    written += 1;
                    // Read all elements and write as a single RPUSH.
                    let elements = lists.lrange(key, 0, -1);
                    if !elements.is_empty() {
                        let elem_refs: Vec<&[u8]> = elements.iter().map(|e| e.as_slice()).collect();
                        let payload = crate::core::list::encode_list_push(ListSubOp::RPush, &elem_refs);
                        new_wal.list_op(key, &payload)?;
                        written += 1;
                    }
                    total_keys_seen += 1;
                    continue;
                }
                // No ListManager — fall through to write as plain SET
                // (key survives, but list contents will be empty after recovery).

            
            {
                if BlobArena::is_blob_ref(&value) {
                    if let Some(arena) = blob_arena
                        && let Some(blob_ref) = BlobRef::decode(&value) {
                            // Retrieve the original (decompressed) payload,
                            // write BSET to the compact WAL, then drop it.
                            if let Some(original) = arena.retrieve(&blob_ref) {
                                new_wal.bset(key, &original)?;
                                blob_keys_written += 1;
                                written += 1;
                                total_keys_seen += 1;
                                // `original` dropped here
                                continue;
                            }
                        }
                    // Degraded path: write bare BlobRef as SET.
                    new_wal.set(key, &value)?;
                    blob_keys_degraded += 1;
                    written += 1;
                    total_keys_seen += 1;
                    continue;
                }
            }
            // Regular key — write as SET.
            new_wal.set(key, &value)?;
            written += 1;
            total_keys_seen += 1;
            // `value` dropped here
        }

        if next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }
    // Drop the cursor loop — keys_values no longer exists.

    // Write EXPIRE entries.
    for (key, deadline_ms) in &ttl_entries {
        new_wal.expire(key, *deadline_ms)?;
        written += 1;
    }

    // Sync the new WAL to ensure durability before swapping.
    new_wal.sync_now()?;
    // Drop the WAL to release the file handle (required for rename on some OSes).
    drop(new_wal);

    // Phase 4: Atomic swap.
    // Step 1: Remove old backup if it exists.
    let _ = fs::remove_file(&bak_path);

    // Step 2: Rename old WAL to backup.
    if wal_path.exists() {
        fs::rename(wal_path, &bak_path)?;
    }

    // Step 3: Rename new WAL to the live WAL path.
    fs::rename(&new_path, wal_path)?;

    // Step 4: Remove backup.
    let _ = fs::remove_file(&bak_path);

    
    {
        if blob_keys_written > 0 || blob_keys_degraded > 0 {
            eprintln!(
                "[CHECKPOINT] Compacted WAL: {} keys ({} blob, {} TTL), {} total entries written | blob keys: {} preserved, {} degraded",
                total_keys_seen,
                blob_keys_written + blob_keys_degraded,
                ttl_entries.len(),
                written,
                blob_keys_written,
                blob_keys_degraded
            );
        } else {
            eprintln!(
                "[CHECKPOINT] Compacted WAL: {} keys, {} TTL entries ({} total entries written)",
                total_keys_seen,
                ttl_entries.len(),
                written
            );
        }
    }

    Ok(written)
}

/// Run checkpoint in a background thread. Returns the JoinHandle.
///
/// The spawned thread captures Arc references to the store, expiry manager,
/// and (when `blob-store` is enabled) the blob arena.
pub fn spawn_checkpoint_thread<const N: usize>(
    store: Arc<KvStoreLockFree<N>>,
    expiry: Option<Arc<ExpirationManager<N>>>,
    wal_path: PathBuf,
    interval_secs: u64,
    lists: Option<Arc<ListManager<N>>>,
     blob_arena: Option<Arc<BlobArena>>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("fastkv-checkpoint".to_string())
        .spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(interval_secs));
                let res = {
                    checkpoint(
                        &store,
                        expiry.as_deref(),
                        &wal_path,
                        lists.as_deref(),
                        blob_arena.as_deref(),
                    )
                };
                match res {
                    Ok(count) => {
                        eprintln!("[CHECKPOINT] Periodic checkpoint completed: {} entries written", count);
                    }
                    Err(e) => {
                        eprintln!("[CHECKPOINT] Periodic checkpoint failed: {}", e);
                    }
                }
            }
        })
        .expect("failed to spawn fastkv-checkpoint thread")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::kv::DEFAULT_INLINE_SIZE;
    use crate::core::wal::WalOp;
    use std::sync::Arc;
    use std::time::Duration;

    type Store = KvStoreLockFree<DEFAULT_INLINE_SIZE>;

    #[test]
    fn test_checkpoint_basic() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("fastkv.wal");

        let store = Arc::new(Store::with_capacity(1000));
        let expiry = Arc::new(ExpirationManager::new(Arc::clone(&store)));

        // Populate store.
        store.set(b"key1", b"value1");
        store.set(b"key2", b"value2");
        store.set(b"key3", b"value3");

        // Delete one key — it should not appear in the compact WAL.
        store.del(b"key2");

        // Set a TTL.
        expiry.expire(b"key1", Duration::from_secs(600));

        // Write the initial WAL.
        let wal = Wal::open(&wal_path, FsyncPolicy::Never).unwrap();
        wal.set(b"key1", b"value1").unwrap();
        wal.set(b"key2", b"value2").unwrap(); // will be compacted away
        wal.set(b"key3", b"value3").unwrap();
        wal.del(b"key2").unwrap();
        let deadline_ms = expiry.expire_with_deadline(b"key1", Duration::from_secs(600)).unwrap();
        wal.expire(b"key1", deadline_ms).unwrap();
        drop(wal);

        // Run checkpoint.
        
        let count = checkpoint(&store, Some(&expiry), &wal_path, None, None).unwrap();
        // Should have 2 SET + 1 EXPIRE = 3 entries.
        assert_eq!(count, 3);

        // Verify the compact WAL can be recovered.
        let entries = Wal::recover(&wal_path).unwrap();
        assert_eq!(entries.len(), 3);

        // Verify key2 is gone.
        let set_keys: Vec<Vec<u8>> = entries.iter()
            .filter(|e| e.op == WalOp::Set)
            .map(|e| e.key.clone())
            .collect();
        assert!(set_keys.iter().any(|k| k == b"key1"));
        assert!(set_keys.iter().any(|k| k == b"key3"));
        assert!(!set_keys.iter().any(|k| k == b"key2"));

        // Verify EXPIRE entry exists.
        let expire_entries: Vec<_> = entries.iter()
            .filter(|e| e.op == WalOp::Expire)
            .collect();
        assert_eq!(expire_entries.len(), 1);
        assert_eq!(expire_entries[0].key, b"key1");
    }

    #[test]
    fn test_checkpoint_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("fastkv.wal");

        let store = Arc::new(Store::with_capacity(100));

        // Write an empty WAL.
        let wal = Wal::open(&wal_path, FsyncPolicy::Never).unwrap();
        drop(wal);

        
        let count = checkpoint(&store, None, &wal_path, None, None).unwrap();
        assert_eq!(count, 0);

        let entries = Wal::recover(&wal_path).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_checkpoint_no_wal_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("fastkv.wal");

        let store = Arc::new(Store::with_capacity(100));
        store.set(b"key", b"value");

        // WAL file doesn't exist yet — checkpoint should still work.
        
        let count = checkpoint(&store, None, &wal_path, None, None).unwrap();
        assert_eq!(count, 1);

        let entries = Wal::recover(&wal_path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"key");
    }

    /// Regression test for the bug where checkpoint wrote blob keys as plain
    /// SET entries (with the 33-byte BlobRef as the value) instead of BSET
    /// entries with the original uncompressed data. After recovery, BGET
    /// would return None because the blob arena was empty.
    ///
    /// With the fix, checkpoint writes a proper BSET entry to the compact
    /// WAL, so recovery can rebuild the blob arena correctly.
    
    #[test]
    fn test_checkpoint_preserves_blob_keys() {
        use crate::core::blob::BlobArena;

        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("fastkv.wal");

        let store = Arc::new(Store::with_capacity(1000));
        let arena = Arc::new(BlobArena::new());

        // Store two blob values via the arena (simulating BSET).
        let blob_ref_1 = arena.store(b"first blob value - large enough to be worth compressing").unwrap();
        let blob_ref_2 = arena.store(b"second blob value - also reasonably large for zstd").unwrap();
        store.set(b"blob:1", &blob_ref_1.encode());
        store.set(b"blob:2", &blob_ref_2.encode());

        // Run checkpoint WITH the arena — should write BSET entries.
        let count = checkpoint(&store, None, &wal_path, None, Some(&arena)).unwrap();
        assert_eq!(count, 2, "both blob keys should be written");

        // Recover the compact WAL.
        let entries = Wal::recover(&wal_path).unwrap();
        assert_eq!(entries.len(), 2, "compact WAL should have 2 entries");

        // All entries should be BSET (not SET).
        for entry in &entries {
            assert_eq!(
                entry.op,
                crate::core::wal::WalOp::BSet,
                "expected BSET entry for key {:?}, got {:?}",
                String::from_utf8_lossy(&entry.key),
                entry.op
            );
        }

        // Verify the original uncompressed data is in the WAL.
        let vals: std::collections::HashMap<Vec<u8>, Vec<u8>> = entries
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        assert_eq!(vals.get(b"blob:1".as_slice()).unwrap(), b"first blob value - large enough to be worth compressing");
        assert_eq!(vals.get(b"blob:2".as_slice()).unwrap(), b"second blob value - also reasonably large for zstd");
    }

    /// Verify that even after checkpoint compacts the WAL, a fresh server
    /// doing recovery correctly rebuilds the blob arena and BGET works.
    
    #[test]
    fn test_checkpoint_then_recover_rebuilds_arena() {
        use crate::core::blob::BlobArena;

        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("fastkv.wal");

        // --- Phase 1: write some BSET entries directly to WAL (pre-checkpoint state) ---
        {
            let wal = Wal::open(&wal_path, FsyncPolicy::Never).unwrap();
            wal.bset(b"session:1", b"large cookie data for session 1").unwrap();
            wal.bset(b"session:2", b"large cookie data for session 2").unwrap();
            wal.bset(b"session:3", b"large cookie data for session 3").unwrap();
            drop(wal);
        }

        // --- Phase 2: simulate server startup — replay WAL into store + arena ---
        let store = Arc::new(Store::with_capacity(1000));
        let arena = Arc::new(BlobArena::new());
        let entries = Wal::recover(&wal_path).unwrap();
        for entry in &entries {
            assert_eq!(entry.op, crate::core::wal::WalOp::BSet);
            let blob_ref = arena.store(&entry.value).expect("arena store must succeed");
            store.set(&entry.key, &blob_ref.encode());
        }
        assert_eq!(store.len(), 3);

        // Sanity: BGET should work via the arena + store.
        let v = store.get(b"session:1").unwrap();
        let br = BlobRef::decode(&v).unwrap();
        assert_eq!(arena.retrieve(&br).unwrap(), b"large cookie data for session 1");

        // --- Phase 3: run checkpoint (the previously buggy code path) ---
        let _ = checkpoint(&store, None, &wal_path, None, Some(&arena)).unwrap();

        // The compact WAL should contain BSET entries (not SET with bare BlobRef).
        let compact_entries = Wal::recover(&wal_path).unwrap();
        assert_eq!(compact_entries.len(), 3);
        for e in &compact_entries {
            assert_eq!(e.op, crate::core::wal::WalOp::BSet,
                "compact WAL must contain BSET entries, found {:?} for key {:?}",
                e.op, String::from_utf8_lossy(&e.key));
        }

        // --- Phase 4: simulate a fresh server restart ---
        // Empty store + fresh arena, then replay the compact WAL.
        let store2 = Arc::new(Store::with_capacity(1000));
        let arena2 = Arc::new(BlobArena::new());
        for entry in &compact_entries {
            let blob_ref = arena2.store(&entry.value).expect("arena store must succeed");
            store2.set(&entry.key, &blob_ref.encode());
        }

        // BGET on the rebuilt arena MUST return the original data.
        for (key, expected) in [
            (b"session:1", b"large cookie data for session 1" as &[u8]),
            (b"session:2", b"large cookie data for session 2" as &[u8]),
            (b"session:3", b"large cookie data for session 3" as &[u8]),
        ] {
            let v = store2.get(key).expect("key must exist after recovery");
            let br = BlobRef::decode(&v).expect("value must be a valid BlobRef");
            let data = arena2.retrieve(&br).expect("BGET must succeed after recovery");
            assert_eq!(data, expected, "BGET data mismatch for key {:?}", String::from_utf8_lossy(key));
        }
    }
}
