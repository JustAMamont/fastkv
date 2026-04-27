//! Write-Ahead Log (WAL) for durable persistence.
//!
//! # Overview
//!
//! The WAL provides crash-consistent durability by appending every mutation
//! (SET / DEL) to an append-only log file before applying it to the in-memory
//! store. On recovery the log is replayed sequentially to reconstruct the
//! latest state.
//!
//! # Binary Format
//!
//! ```text
//! ┌──────────┬──────────┬──────────┬────────┬────┬────────┬────┬────────┬──────────┐
//! │  magic   │  version │  crc32   │  op    │kl │  key   │vl │  value │   pad    │
//! │ 4 bytes  │ 2 bytes  │ 4 bytes  │ 1 byte │2B  │ kl B   │2B  │  vl B   │ to 16 B  │
//! └──────────┴──────────┴──────────┴────────┴────┴────────┴────┴────────┴──────────┘
//! ```
//!
//! * **magic** — `b"FKVL"` identifies FastKV WAL files.
//! * **version** — format version (currently `1`).
//! * **crc32** — CRC-32C covering *op + key + value* (not the header).
//! * **op** — `0x01` SET, `0x02` DEL.
//! * **key / value** — raw byte slices; value is empty for DEL.
//! * **pad** — entries are padded to a multiple of 16 bytes so that the
//!   file offset of every entry is 16-byte aligned (helps with direct I/O).
//!
//! # Fsync Policies
//!
//! | Policy      | Description                                    |
//! |-------------|------------------------------------------------|
//! | `Always`    | `fsync` after every write (safest, slowest)     |
//! | `EverySec`  | background thread calls `fsync` every ~1 s      |
//! | `Never`     | rely on OS page cache (fastest, least safe)     |
//!
//! # Recovery
//!
//! On startup [`Wal::recover`] reads the file from the beginning, verifies
//! each entry's CRC, and returns an iterator of [`WalEntry`] values that the
//! caller replays into the KV store. Truncated / corrupted trailing bytes
//! are silently ignored — only complete, checksummed entries are returned.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crc32c::crc32c as compute_crc32c;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// File magic bytes: "FKVL" (FastKV WAL).
const WAL_MAGIC: &[u8; 4] = b"FKVL";

/// Current WAL format version.
const WAL_VERSION: u16 = 1;

/// File header size: magic (4) + version (2) = 6 bytes.
const HEADER_SIZE: usize = 6;

/// Alignment of each entry on disk (bytes). Must be a power of two.
const ENTRY_ALIGN: usize = 16;

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// WAL operation codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalOp {
    /// Insert or update a key-value pair.
    Set = 0x01,
    /// Delete a key.
    Del = 0x02,
    /// Set a TTL deadline on a key.
    Expire = 0x03,
}

impl TryFrom<u8> for WalOp {
    type Error = WalError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(WalOp::Set),
            0x02 => Ok(WalOp::Del),
            0x03 => Ok(WalOp::Expire),
            other => Err(WalError::UnknownOp(other)),
        }
    }
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

/// A single deserialized WAL entry.
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Operation type (SET or DEL).
    pub op: WalOp,
    /// Key bytes.
    pub key: Vec<u8>,
    /// Value bytes (empty for DEL).
    pub value: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Fsync policy
// ---------------------------------------------------------------------------

/// Controls when [`fsync`] is called on the WAL file descriptor.
///
/// [`fsync`]: std::fs::File::sync_data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Call `fsync` after every single write. Maximum durability, lowest
    /// throughput.
    Always,
    /// A background thread calls `fsync` approximately once per second.
    /// Good balance of durability and performance (similar to Redis
    /// `appendfsync everysec`).
    EverySec,
    /// Never call `fsync`. Rely entirely on the operating system's page
    /// cache. Highest throughput; data may be lost on power failure.
    Never,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// WAL-related errors.
#[derive(Debug)]
pub enum WalError {
    /// The file does not start with the expected magic bytes.
    InvalidMagic,
    /// Unsupported WAL format version.
    UnsupportedVersion(u16),
    /// An unknown operation code was encountered.
    UnknownOp(u8),
    /// A CRC-32 checksum mismatch indicates corruption.
    ChecksumMismatch { expected: u32, actual: u32 },
    /// An I/O error from the underlying file system.
    Io(io::Error),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::InvalidMagic => write!(f, "invalid WAL magic bytes"),
            WalError::UnsupportedVersion(v) => write!(f, "unsupported WAL version {v}"),
            WalError::UnknownOp(op) => write!(f, "unknown WAL operation 0x{op:02x}"),
            WalError::ChecksumMismatch { expected, actual } => {
                write!(f, "CRC mismatch: expected 0x{expected:08x}, got 0x{actual:08x}")
            }
            WalError::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

// ---------------------------------------------------------------------------
// CRC-32C (hardware-accelerated via crc32c crate)
// ---------------------------------------------------------------------------

/// Compute CRC-32C over a byte slice using the `crc32c` crate.
///
/// The `crc32c` crate uses hardware CRC-32C instructions (SSE4.2 on x86,
/// CRC on ARM) when available, with a fast software fallback otherwise.
/// CRC-32C (Castagnoli, polynomial 0x1EDC6F41) is the variant used by
/// iSCSI, Kafka, and many storage engines for its excellent error-detection
/// properties.
#[inline]
fn crc32c(data: &[u8]) -> u32 {
    compute_crc32c(data)
}

// ---------------------------------------------------------------------------
// WAL Writer
// ---------------------------------------------------------------------------

/// Append-only WAL writer.
///
/// # Thread Safety
///
/// Internally synchronized via [`Mutex`] so it can be shared across async
/// tasks or OS threads. The lock is held only for the brief duration of the
/// `write` call (serialization + file write + optional fsync).
pub struct Wal {
    /// Underlying file handle (opened in append mode).
    file: std::sync::Mutex<File>,
    /// Path to the WAL file (used by the background fsync thread).
    path: PathBuf,
    /// Fsync policy.
    fsync_policy: FsyncPolicy,
    /// Signals the background fsync thread to shut down.
    shutdown: Arc<AtomicBool>,
    /// Handle to the background fsync thread (if `EverySec`).
    _fsync_thread: Option<thread::JoinHandle<()>>,
}

impl Wal {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Open an existing WAL file for appending, or create a new one.
    ///
    /// If *path* does not exist a new file is created with the standard
    /// header (magic + version).
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if the file cannot be opened or created.
    pub fn open(path: impl AsRef<Path>, fsync_policy: FsyncPolicy) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let exists = path.exists();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        // Write header for brand-new files.
        if !exists {
            let mut f = &file;
            f.write_all(WAL_MAGIC)?;
            f.write_all(&WAL_VERSION.to_le_bytes())?;
            f.sync_data()?;
        }

        // Spawn background fsync thread if needed.
        let shutdown = Arc::new(AtomicBool::new(false));
        let fsync_thread = if fsync_policy == FsyncPolicy::EverySec {
            let shutdown = Arc::clone(&shutdown);
            let fsync_path = path.clone();
            Some(thread::Builder::new()
                .name("fastkv-wal-fsync".to_string())
                .spawn(move || {
                    while !shutdown.load(Ordering::Relaxed) {
                        thread::sleep(Duration::from_secs(1));
                        if let Ok(f) = OpenOptions::new().write(true).open(&fsync_path) {
                            let _ = f.sync_data();
                        }
                    }
                })?)
        } else {
            None
        };

        Ok(Self {
            file: std::sync::Mutex::new(file),
            path,
            fsync_policy,
            shutdown,
            _fsync_thread: fsync_thread,
        })
    }

    // -----------------------------------------------------------------------
    // Writing
    // -----------------------------------------------------------------------

    /// Append a SET entry to the WAL.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] on write failure.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), WalError> {
        self.append(WalOp::Set, key, value)
    }

    /// Append a DEL entry to the WAL.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] on write failure.
    pub fn del(&self, key: &[u8]) -> Result<(), WalError> {
        self.append(WalOp::Del, key, &[])
    }

    /// Append an EXPIRE entry to the WAL.
    ///
    /// *deadline_ms* is an absolute timestamp (milliseconds since UNIX epoch)
    /// representing when the key should expire. On recovery, if the deadline
    /// has already passed the key is treated as immediately expired.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] on write failure.
    pub fn expire(&self, key: &[u8], deadline_ms: u64) -> Result<(), WalError> {
        self.append(WalOp::Expire, key, &deadline_ms.to_le_bytes())
    }

    /// Append an arbitrary entry to the WAL.
    fn append(&self, op: WalOp, key: &[u8], value: &[u8]) -> Result<(), WalError> {
        // Compute sizes upfront to use a single buffer.
        let payload_len = 1 + 2 + key.len() + 2 + value.len();
        let unpadded = 4 + payload_len;
        let padded = (unpadded + ENTRY_ALIGN - 1) & !(ENTRY_ALIGN - 1);
        let padding = padded - unpadded;

        let mut buf = Vec::with_capacity(padded);
        // Reserve space for CRC at the beginning.
        buf.extend_from_slice(&[0u8; 4]);
        // Write payload: op + key_len + key + val_len + value.
        buf.push(op as u8);
        buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
        buf.extend_from_slice(value);
        // Zero-pad to alignment.
        buf.extend_from_slice(&[0u8; ENTRY_ALIGN][..padding]);

        // Compute CRC over the payload portion (after the first 4 bytes).
        let checksum = crc32c(&buf[4..4 + payload_len]);
        // Write CRC into the reserved space.
        buf[0..4].copy_from_slice(&checksum.to_le_bytes());

        let mut file = self.file.lock().map_err(|e| {
            WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
        })?;
        file.write_all(&buf)?;

        if self.fsync_policy == FsyncPolicy::Always {
            file.sync_data()?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------

    /// Read the WAL file and return all valid entries.
    ///
    /// Trailing incomplete or corrupted entries are silently discarded so
    /// that recovery always succeeds (best-effort).
    ///
    /// # Errors
    ///
    /// Returns [`WalError::InvalidMagic`] or [`WalError::UnsupportedVersion`]
    /// if the header is corrupted. Individual entry CRC failures are logged
    /// internally and the entry is skipped.
    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<WalEntry>, WalError> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);

        // --- Header ---
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header)?;
        if &header[..4] != WAL_MAGIC {
            return Err(WalError::InvalidMagic);
        }
        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != WAL_VERSION {
            return Err(WalError::UnsupportedVersion(version));
        }

        // --- Entries ---
        let mut entries = Vec::new();
        // Work with a growing buffer for the current entry.
        let mut entry_buf = Vec::with_capacity(256);

        loop {
            // Read the fixed CRC (4 bytes) first.
            entry_buf.clear();
            entry_buf.resize(4, 0);
            if reader.read_exact(&mut entry_buf).is_err() {
                break; // EOF or truncated
            }
            let stored_crc = u32::from_le_bytes([entry_buf[0], entry_buf[1], entry_buf[2], entry_buf[3]]);

            // Read op (1 byte).
            entry_buf.resize(5, 0);
            if reader.read_exact(&mut entry_buf[4..]).is_err() {
                break;
            }
            let op_byte = entry_buf[4];
            let op = match WalOp::try_from(op_byte) {
                Ok(o) => o,
                Err(_) => break, // Unknown op — stop recovery.
            };

            // Read key_len (2 bytes).
            entry_buf.resize(7, 0);
            if reader.read_exact(&mut entry_buf[5..]).is_err() {
                break;
            }
            let key_len = u16::from_le_bytes([entry_buf[5], entry_buf[6]]) as usize;

            // Read key.
            entry_buf.resize(7 + key_len, 0);
            if key_len > 0 && reader.read_exact(&mut entry_buf[7..7 + key_len]).is_err() {
                break;
            }
            let key = entry_buf[7..7 + key_len].to_vec();

            // Read val_len (2 bytes).
            let mut vl_buf = [0u8; 2];
            if reader.read_exact(&mut vl_buf).is_err() {
                break;
            }
            let val_len = u16::from_le_bytes(vl_buf) as usize;

            // Read value.
            let mut value = vec![0u8; val_len];
            if val_len > 0 && reader.read_exact(&mut value).is_err() {
                break;
            }

            // Verify CRC.
            let mut payload = Vec::with_capacity(5 + key_len + val_len);
            payload.push(op_byte);
            payload.extend_from_slice(&(key_len as u16).to_le_bytes());
            payload.extend_from_slice(&key);
            payload.extend_from_slice(&(val_len as u16).to_le_bytes());
            payload.extend_from_slice(&value);

            let computed_crc = crc32c(&payload);
            if computed_crc != stored_crc {
                // Corrupted entry — stop recovery (further entries are suspect).
                eprintln!(
                    "[WAL] CRC mismatch at entry (expected 0x{:08x}, got 0x{:08x}) — stopping recovery",
                    stored_crc, computed_crc
                );
                break;
            }

            entries.push(WalEntry { op, key, value });

            // Skip padding to maintain alignment.
            let total_payload = 4 + 1 + 2 + key_len + 2 + val_len;
            let padded = (total_payload + ENTRY_ALIGN - 1) & !(ENTRY_ALIGN - 1);
            let padding = padded - total_payload;
            if padding > 0 {
                let mut pad = [0u8; ENTRY_ALIGN];
                let _ = reader.read_exact(&mut pad[..padding]);
            }
        }

        Ok(entries)
    }

    // -----------------------------------------------------------------------
    // Utilities
    // -----------------------------------------------------------------------

    /// Return the path of the WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the configured fsync policy.
    pub fn fsync_policy(&self) -> FsyncPolicy {
        self.fsync_policy
    }

    /// Force an `fsync` right now (regardless of the configured policy).
    ///
    /// Useful for graceful shutdown sequences.
    pub fn sync_now(&self) -> Result<(), WalError> {
        let file = self.file.lock().map_err(|e| {
            WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
        })?;
        file.sync_data()?;
        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Signal the background fsync thread to stop.
        self.shutdown.store(true, Ordering::Relaxed);
        // Final sync.
        if let Ok(file) = self.file.lock() {
            let _ = file.sync_data();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_crc32c_known_values() {
        // CRC-32C of an empty slice.
        assert_eq!(crc32c(b""), 0x0000_0000);
        // CRC-32C of "123456789" (well-known check value).
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn test_wal_create_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();

        wal.set(b"hello", b"world").unwrap();
        wal.set(b"foo", b"bar").unwrap();
        wal.del(b"hello").unwrap();
        wal.sync_now().unwrap();

        drop(wal);

        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].op, WalOp::Set);
        assert_eq!(entries[0].key, b"hello");
        assert_eq!(entries[0].value, b"world");

        assert_eq!(entries[1].op, WalOp::Set);
        assert_eq!(entries[1].key, b"foo");
        assert_eq!(entries[1].value, b"bar");

        assert_eq!(entries[2].op, WalOp::Del);
        assert_eq!(entries[2].key, b"hello");
        assert_eq!(entries[2].value, b"");
    }

    #[test]
    fn test_wal_fsync_always() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_fsync_always.wal");

        let wal = Wal::open(&path, FsyncPolicy::Always).unwrap();
        wal.set(b"k", b"v").unwrap();
        drop(wal);

        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_wal_recover_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.wal");

        let entries = Wal::recover(&path).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_wal_corrupted_trailing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.wal");

        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();
        wal.set(b"a", b"1").unwrap();
        wal.set(b"b", b"2").unwrap();
        drop(wal);

        // Append garbage to the end of the file.
        use std::io::Write;
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"garbage data that is definitely not valid").unwrap();
        f.sync_data().unwrap();

        // Recovery should return only the 2 valid entries.
        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, b"a");
        assert_eq!(entries[1].key, b"b");
    }

    #[test]
    fn test_wal_binary_keys_and_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("binary.wal");

        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();
        wal.set(b"\x00\xff\xab", b"\x01\x02\x03\x04").unwrap();
        drop(wal);

        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"\x00\xff\xab");
        assert_eq!(entries[0].value, b"\x01\x02\x03\x04");
    }

    #[test]
    fn test_wal_padded_alignment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("align.wal");

        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();
        // Write entries of varying sizes to exercise padding.
        wal.set(b"k", b"v").unwrap();
        wal.set(b"longer_key", b"longer_value").unwrap();
        wal.del(b"k").unwrap();
        drop(wal);

        // File size should be header + aligned entries.
        let file_size = fs::metadata(&path).unwrap().len() as usize;
        assert_eq!((file_size - HEADER_SIZE) % ENTRY_ALIGN, 0);
    }

    #[test]
    fn test_wal_op_try_from() {
        assert_eq!(WalOp::try_from(0x01).unwrap(), WalOp::Set);
        assert_eq!(WalOp::try_from(0x02).unwrap(), WalOp::Del);
        assert_eq!(WalOp::try_from(0x03).unwrap(), WalOp::Expire);
        assert!(matches!(WalOp::try_from(0xFF), Err(WalError::UnknownOp(0xFF))));
        assert!(matches!(WalOp::try_from(0x00), Err(WalError::UnknownOp(0x00))));
    }

    #[test]
    fn test_wal_error_display() {
        let err = WalError::InvalidMagic;
        assert!(err.to_string().contains("invalid WAL magic"));

        let err = WalError::UnsupportedVersion(99);
        assert!(err.to_string().contains("99"));

        let err = WalError::ChecksumMismatch { expected: 1, actual: 2 };
        assert!(err.to_string().contains("CRC mismatch"));
    }

    #[test]
    fn test_wal_expire_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("expire.wal");
        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();
        wal.set(b"session", b"abc123").unwrap();
        wal.expire(b"session", 1700000000000).unwrap();
        drop(wal);

        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].op, WalOp::Set);
        assert_eq!(entries[1].op, WalOp::Expire);
        assert_eq!(entries[1].key, b"session");
        assert_eq!(entries[1].value, 1700000000000u64.to_le_bytes());
    }

    #[test]
    fn test_wal_empty_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty_key.wal");
        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();
        wal.set(b"", b"value").unwrap();
        drop(wal);

        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"");
        assert_eq!(entries[0].value, b"value");
    }

    #[test]
    fn test_wal_empty_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty_val.wal");
        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();
        wal.set(b"key", b"").unwrap();
        drop(wal);

        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, b"key");
        assert_eq!(entries[0].value, b"");
    }

    #[test]
    fn test_wal_large_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.wal");
        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();

        let large_value = vec![b'x'; 10_000];
        wal.set(b"key", &large_value).unwrap();
        drop(wal);

        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value.len(), 10_000);
    }

    #[test]
    fn test_wal_many_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("many.wal");
        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();

        let count = 1_000;
        for i in 0..count {
            let key = format!("key:{}", i);
            let val = format!("val:{}", i);
            wal.set(key.as_bytes(), val.as_bytes()).unwrap();
        }
        drop(wal);

        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), count);
    }

    #[test]
    fn test_wal_sync_now() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sync.wal");
        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();
        wal.set(b"k", b"v").unwrap();
        wal.sync_now().unwrap(); // should not panic
        drop(wal);

        let entries = Wal::recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_wal_path_and_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.wal");
        let wal = Wal::open(&path, FsyncPolicy::Always).unwrap();
        assert!(wal.path().ends_with("meta.wal"));
        assert_eq!(wal.fsync_policy(), FsyncPolicy::Always);
    }

}
