//! Compressed Segment-based WAL for high-volume workloads.
//!
//! # Overview
//!
//! When the server handles millions of sessions, the plain WAL grows very
//! quickly because every entry is 16-byte aligned without any compression.
//! This module provides a **segment-based WAL** that:
//!
//! 1. Writes the first *N* entries **raw** (for fast fsync and immediate
//!    recovery of the most recent data).
//! 2. Batches subsequent entries in groups of *batch_size*, compresses each
//!    batch with zstd, and appends it to the current segment.
//! 3. When a segment reaches `max_segment_size` bytes, it is **rotated** —
//!    the current segment is closed and a new one is opened.
//! 4. A **footer** at the end of each segment contains an index of all
//!    compressed batches (offset + compressed size), allowing O(1) seek
//!    to any batch during recovery.
//!
//! # Binary Format (per segment)
//!
//! ```text
//! Segment Header:
//! ┌──────────┬──────────┬─────────────┬───────────┬─────────────┐
//! │ magic    │ version  │ segment_id  │ raw_count │ batch_size  │
//! │ 4 bytes  │ 2 bytes  │ u64 8 bytes │ u32 4B    │ u32 4B      │
//! └──────────┴──────────┴─────────────┴───────────┴─────────────┘
//!
//! Raw Entries (re-use the v1 WAL entry format, 16-byte aligned):
//!   [ entry₁ | entry₂ | … | entry_raw_count ]
//!
//! Compressed Batch:
//! ┌────────────┬──────────────┬─────────────────────┬──────────────┐
//! │ batch_crc  │ uncomp_len  │ comp_len            │ zstd_data    │
//! │ u32 4B     │ u32 4B      │ u32 4B              │ comp_len B   │
//! └────────────┴──────────────┴─────────────────────┴──────────────┘
//!
//! Footer:
//! ┌──────────────┬──────────────┬───────┬──────────────┬──────────────┬──────────────┐
//! │ num_batches  │ batch_index  │ …     │ footer_crc   │ footer_magic │
//! │ u32 4B       │ [offset+size]│       │ u32 4B       │ 4 bytes      │
//! └──────────────┴──────────────┴───────┴──────────────┴──────────────┘
//!   batch_index entry = { offset: u64, comp_len: u32, uncomp_len: u32, entry_count: u32 }
//! ```
//!
//! # Migration
//!
//! On recovery, if a file starts with the v1 WAL magic (`FKVL`), the
//! existing [`Wal::recover`] path is used transparently. Old WAL files are
//! **never modified** — a new compressed segment is simply started alongside
//! them. The user can delete the old WAL file after confirming successful
//! recovery.
//!
//! # Feature Flag
//!
//! Compressed WAL requires the `blob-store` feature (which pulls in `zstd`).
//! When `blob-store` is not enabled, this module is not compiled and the
//! server falls back to the plain v1 WAL.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crc32c::crc32c as compute_crc32c;
use zstd::bulk::compress as zstd_compress;
use zstd::bulk::decompress as zstd_decompress;

use super::wal::{
    Wal, WalEntry, WalError, WalOp, FsyncPolicy, WalWriter,
    ENTRY_ALIGN,
};

// ---------------------------------------------------------------------------
// Segment constants
// ---------------------------------------------------------------------------

/// Segment file magic: "FKVS" (FastKV Segment).
const SEGMENT_MAGIC: &[u8; 4] = b"FKVS";

/// Segment format version.
const SEGMENT_VERSION: u16 = 2;

/// Segment header size: magic(4) + version(2) + segment_id(8) + raw_count(4) + batch_size(4) = 22.
const SEGMENT_HEADER_SIZE: usize = 22;

/// Footer magic: "FKVF" (FastKV Footer).
const FOOTER_MAGIC: &[u8; 4] = b"FKVF";

/// Default number of raw (uncompressed) entries per segment.
const DEFAULT_RAW_COUNT: u32 = 1000;

/// Default number of entries per compressed batch.
const DEFAULT_BATCH_SIZE: u32 = 1000;

/// Default maximum segment size before rotation (64 MB).
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Zstd compression level (1 = fastest, 22 = best; 3 is a good balance).
const ZSTD_LEVEL: i32 = 3;

/// Each batch index entry in the footer: offset(u64) + comp_len(u32) + uncomp_len(u32) + entry_count(u32) = 20 bytes.
const BATCH_INDEX_ENTRY_SIZE: usize = 20;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for compressed segment-based WAL.
#[derive(Debug, Clone)]
pub struct SegmentConfig {
    /// Directory where segment files are stored.
    pub data_dir: PathBuf,
    /// Fsync policy.
    pub fsync_policy: FsyncPolicy,
    /// Number of raw (uncompressed) entries at the start of each segment.
    pub raw_count: u32,
    /// Number of entries per compressed batch.
    pub batch_size: u32,
    /// Maximum segment file size before rotation.
    pub max_segment_size: u64,
}

impl SegmentConfig {
    /// Create a config with the given data directory and defaults.
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            fsync_policy: FsyncPolicy::EverySec,
            raw_count: DEFAULT_RAW_COUNT,
            batch_size: DEFAULT_BATCH_SIZE,
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
        }
    }
}

// ---------------------------------------------------------------------------
// Batch index entry (stored in footer)
// ---------------------------------------------------------------------------

/// An entry in the segment footer's batch index.
#[derive(Debug, Clone)]
struct BatchIndexEntry {
    /// Byte offset of the compressed batch from the start of the file.
    offset: u64,
    /// Compressed size in bytes.
    comp_len: u32,
    /// Uncompressed size in bytes.
    uncomp_len: u32,
    /// Number of WAL entries in this batch.
    entry_count: u32,
}

impl BatchIndexEntry {
    fn encode(&self) -> [u8; BATCH_INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; BATCH_INDEX_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.comp_len.to_le_bytes());
        buf[12..16].copy_from_slice(&self.uncomp_len.to_le_bytes());
        buf[16..20].copy_from_slice(&self.entry_count.to_le_bytes());
        buf
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < BATCH_INDEX_ENTRY_SIZE {
            return None;
        }
        Some(Self {
            offset: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            comp_len: u32::from_le_bytes(buf[8..12].try_into().ok()?),
            uncomp_len: u32::from_le_bytes(buf[12..16].try_into().ok()?),
            entry_count: u32::from_le_bytes(buf[16..20].try_into().ok()?),
        })
    }
}

// ---------------------------------------------------------------------------
// CRC helper
// ---------------------------------------------------------------------------

#[inline]
fn crc32c(data: &[u8]) -> u32 {
    compute_crc32c(data)
}

// ---------------------------------------------------------------------------
// Segment WAL Writer
// ---------------------------------------------------------------------------

/// Compressed segment-based WAL writer.
///
/// Provides the same write API as [`Wal`] but internally batches entries
/// and compresses them with zstd. The first `raw_count` entries are written
/// raw (for fast recovery of recent data); subsequent entries are collected
/// into batches of `batch_size` and compressed.
pub struct WalSegment {
    /// Configuration.
    config: SegmentConfig,
    /// Current segment file.
    file: std::sync::Mutex<SegmentFile>,
    /// Global segment counter (AtomicU64 for thread-safe ID assignment).
    next_segment_id: AtomicU64,
    /// Signals the background fsync thread to shut down.
    shutdown: Arc<AtomicBool>,
    /// Handle to the background fsync thread.
    _fsync_thread: Option<thread::JoinHandle<()>>,
}

/// State for the currently open segment file.
struct SegmentFile {
    /// File handle.
    file: File,
    /// Current segment ID.
    segment_id: u64,
    /// File path.
    #[allow(dead_code)]
    path: PathBuf,
    /// Number of entries written so far (raw + batched).
    total_entries: u64,
    /// Number of raw entries written so far.
    raw_entries: u32,
    /// Buffered entries waiting to form a batch (after raw_count is reached).
    pending_batch: Vec<WalEntry>,
    /// Batch index (for footer).
    batch_index: Vec<BatchIndexEntry>,
    /// Current file size (tracked to avoid stat() on every write).
    file_size: u64,
}

impl WalSegment {
    /// Open or create a compressed segment WAL.
    ///
    /// On startup, any existing segment files in `config.data_dir` are
    /// detected and the segment ID counter is set to the maximum found + 1
    /// so that new segments don't overwrite old ones.
    pub fn open(config: SegmentConfig) -> Result<Self, WalError> {
        fs::create_dir_all(&config.data_dir)?;

        // Find the highest existing segment ID to continue from.
        let mut max_id: u64 = 0;
        for entry in fs::read_dir(&config.data_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Segment files: seg_0000000000000001.wal
            if let Some(id_str) = name_str.strip_prefix("seg_").and_then(|s| s.strip_suffix(".wal")) {
                if let Ok(id) = u64::from_str_radix(id_str, 16) {
                    max_id = max_id.max(id);
                }
            }
        }

        let next_segment_id = AtomicU64::new(max_id + 2);

        // Create the first new segment.
        let seg_file = Self::create_segment(&config, max_id + 1)?;

        // Spawn background fsync thread if needed.
        let shutdown = Arc::new(AtomicBool::new(false));
        let fsync_thread = if config.fsync_policy == FsyncPolicy::EverySec {
            let shutdown = Arc::clone(&shutdown);
            let dir = config.data_dir.clone();
            Some(thread::Builder::new()
                .name("fastkv-seg-fsync".to_string())
                .spawn(move || {
                    while !shutdown.load(Ordering::Relaxed) {
                        thread::sleep(Duration::from_secs(1));
                        // Fsync all segment files.
                        if let Ok(entries) = fs::read_dir(&dir) {
                            for entry in entries.flatten() {
                                if let Ok(f) = OpenOptions::new().write(true).open(entry.path()) {
                                    let _ = f.sync_data();
                                }
                            }
                        }
                    }
                })?)
        } else {
            None
        };

        Ok(Self {
            config,
            file: std::sync::Mutex::new(seg_file),
            next_segment_id,
            shutdown,
            _fsync_thread: fsync_thread,
        })
    }

    /// Create a new segment file.
    fn create_segment(config: &SegmentConfig, segment_id: u64) -> Result<SegmentFile, WalError> {
        let filename = format!("seg_{:016x}.wal", segment_id);
        let path = config.data_dir.join(&filename);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&path)?;

        // Write segment header.
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        header[0..4].copy_from_slice(SEGMENT_MAGIC);
        header[4..6].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
        header[6..14].copy_from_slice(&segment_id.to_le_bytes());
        header[14..18].copy_from_slice(&config.raw_count.to_le_bytes());
        header[18..22].copy_from_slice(&config.batch_size.to_le_bytes());

        // We'll write the header when the first entry comes in, or at close.
        // For now, just track the file.
        Ok(SegmentFile {
            file,
            segment_id,
            path,
            total_entries: 0,
            raw_entries: 0,
            pending_batch: Vec::with_capacity(config.batch_size as usize),
            batch_index: Vec::new(),
            file_size: 0,
        })
    }

    // -------------------------------------------------------------------
    // Writing API (same as Wal)
    // -------------------------------------------------------------------

    /// Append a SET entry.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), WalError> {
        self.append_entry(WalOp::Set, key, value)
    }

    /// Append a DEL entry.
    pub fn del(&self, key: &[u8]) -> Result<(), WalError> {
        self.append_entry(WalOp::Del, key, &[])
    }

    /// Append an EXPIRE entry.
    pub fn expire(&self, key: &[u8], deadline_ms: u64) -> Result<(), WalError> {
        self.append_entry(WalOp::Expire, key, &deadline_ms.to_le_bytes())
    }

    /// Append a BSET entry.
    pub fn bset(&self, key: &[u8], original_value: &[u8]) -> Result<(), WalError> {
        self.append_entry(WalOp::BSet, key, original_value)
    }

    /// Append a BDEL entry.
    pub fn bdel(&self, key: &[u8]) -> Result<(), WalError> {
        self.append_entry(WalOp::BDel, key, &[])
    }

    /// Append a LIST_OP entry.
    pub fn list_op(&self, key: &[u8], payload: &[u8]) -> Result<(), WalError> {
        self.append_entry(WalOp::ListOp, key, payload)
    }

    /// Internal: append an entry to the current segment.
    fn append_entry(&self, op: WalOp, key: &[u8], value: &[u8]) -> Result<(), WalError> {
        let entry = WalEntry {
            op,
            key: key.to_vec(),
            value: value.to_vec(),
        };

        let mut seg = self.file.lock().map_err(|e| {
            WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
        })?;

        // Write header if this is the first entry in the segment.
        if seg.total_entries == 0 {
            let mut header = [0u8; SEGMENT_HEADER_SIZE];
            header[0..4].copy_from_slice(SEGMENT_MAGIC);
            header[4..6].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
            header[6..14].copy_from_slice(&seg.segment_id.to_le_bytes());
            header[14..18].copy_from_slice(&self.config.raw_count.to_le_bytes());
            header[18..22].copy_from_slice(&self.config.batch_size.to_le_bytes());
            seg.file.write_all(&header)?;
            seg.file_size = SEGMENT_HEADER_SIZE as u64;
        }

        // If we haven't reached raw_count yet, write raw.
        if seg.raw_entries < self.config.raw_count {
            Self::write_raw_entry(&mut seg, &entry)?;
            seg.raw_entries += 1;
        } else {
            // Buffer for batch compression.
            seg.pending_batch.push(entry);
            if seg.pending_batch.len() >= self.config.batch_size as usize {
                Self::flush_batch(&mut seg)?;
            }
        }

        seg.total_entries += 1;

        // Check if we need to rotate.
        if seg.file_size >= self.config.max_segment_size {
            self.rotate_segment(&mut seg)?;
        }

        // Fsync policy.
        if self.config.fsync_policy == FsyncPolicy::Always {
            seg.file.sync_data()?;
        }

        Ok(())
    }

    /// Write a single raw entry to the segment file (v1 format, 16-byte aligned).
    fn write_raw_entry(seg: &mut SegmentFile, entry: &WalEntry) -> Result<(), WalError> {
        let payload_len = 1 + 2 + entry.key.len() + 2 + entry.value.len();
        let unpadded = 4 + payload_len;
        let padded = (unpadded + ENTRY_ALIGN - 1) & !(ENTRY_ALIGN - 1);
        let padding = padded - unpadded;

        let mut buf = Vec::with_capacity(padded);
        // CRC placeholder.
        buf.extend_from_slice(&[0u8; 4]);
        // Payload.
        buf.push(entry.op as u8);
        buf.extend_from_slice(&(entry.key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&entry.key);
        buf.extend_from_slice(&(entry.value.len() as u16).to_le_bytes());
        buf.extend_from_slice(&entry.value);
        // Padding.
        buf.extend_from_slice(&[0u8; ENTRY_ALIGN][..padding]);

        // Compute CRC over payload.
        let checksum = crc32c(&buf[4..4 + payload_len]);
        buf[0..4].copy_from_slice(&checksum.to_le_bytes());

        seg.file.write_all(&buf)?;
        seg.file_size += padded as u64;

        Ok(())
    }

    /// Flush the pending batch: serialize entries, compress, write to file.
    fn flush_batch(seg: &mut SegmentFile) -> Result<(), WalError> {
        if seg.pending_batch.is_empty() {
            return Ok(());
        }

        let entry_count = seg.pending_batch.len() as u32;

        // Serialize all entries in the batch into one buffer.
        let mut raw_buf = Vec::with_capacity(seg.pending_batch.len() * 64);
        for entry in &seg.pending_batch {
            // Each entry: op(1) + key_len(2) + key + val_len(2) + value
            raw_buf.push(entry.op as u8);
            raw_buf.extend_from_slice(&(entry.key.len() as u16).to_le_bytes());
            raw_buf.extend_from_slice(&entry.key);
            raw_buf.extend_from_slice(&(entry.value.len() as u16).to_le_bytes());
            raw_buf.extend_from_slice(&entry.value);
        }

        let uncomp_len = raw_buf.len() as u32;
        let batch_offset = seg.file_size;

        // Compress with zstd.
        let compressed = zstd_compress(&raw_buf, ZSTD_LEVEL)
            .map_err(|e| WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?;
        let comp_len = compressed.len() as u32;

        // Write batch header: batch_crc(4) + uncomp_len(4) + comp_len(4) + zstd_data.
        let mut batch_header = [0u8; 12];
        // CRC placeholder.
        batch_header[0..4].copy_from_slice(&[0u8; 4]);
        batch_header[4..8].copy_from_slice(&uncomp_len.to_le_bytes());
        batch_header[8..12].copy_from_slice(&comp_len.to_le_bytes());

        // Compute CRC over uncomp_len + comp_len + compressed_data.
        let mut crc_buf = Vec::with_capacity(8 + compressed.len());
        crc_buf.extend_from_slice(&uncomp_len.to_le_bytes());
        crc_buf.extend_from_slice(&comp_len.to_le_bytes());
        crc_buf.extend_from_slice(&compressed);
        let batch_crc = crc32c(&crc_buf);
        batch_header[0..4].copy_from_slice(&batch_crc.to_le_bytes());

        seg.file.write_all(&batch_header)?;
        seg.file.write_all(&compressed)?;
        seg.file_size += 12 + comp_len as u64;

        // Record in batch index.
        seg.batch_index.push(BatchIndexEntry {
            offset: batch_offset,
            comp_len,
            uncomp_len,
            entry_count,
        });

        seg.pending_batch.clear();

        Ok(())
    }

    /// Rotate: write footer to current segment, close it, open a new one.
    fn rotate_segment(&self, seg: &mut SegmentFile) -> Result<(), WalError> {
        // Flush any pending batch first.
        Self::flush_batch(seg)?;

        // Write footer.
        Self::write_footer(seg)?;

        // Sync current segment.
        seg.file.sync_data()?;

        // Create a new segment.
        let new_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
        *seg = Self::create_segment(&self.config, new_id)?;

        Ok(())
    }

    /// Write the footer to the current segment file.
    fn write_footer(seg: &mut SegmentFile) -> Result<(), WalError> {
        let num_batches = seg.batch_index.len() as u32;

        // Footer: num_batches(4) + batch_index_entries + footer_crc(4) + FOOTER_MAGIC(4).
        let mut footer = Vec::with_capacity(4 + seg.batch_index.len() * BATCH_INDEX_ENTRY_SIZE + 8);
        footer.extend_from_slice(&num_batches.to_le_bytes());
        for entry in &seg.batch_index {
            footer.extend_from_slice(&entry.encode());
        }

        // Compute footer CRC (over everything except the CRC itself and magic).
        let footer_crc = crc32c(&footer);
        footer.extend_from_slice(&footer_crc.to_le_bytes());
        footer.extend_from_slice(FOOTER_MAGIC);

        seg.file.write_all(&footer)?;
        seg.file_size += footer.len() as u64;

        Ok(())
    }

    /// Force an fsync (regardless of policy).
    pub fn sync_now(&self) -> Result<(), WalError> {
        let seg = self.file.lock().map_err(|e| {
            WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
        })?;
        seg.file.sync_data()?;
        Ok(())
    }

    /// Close the current segment gracefully (write footer + flush).
    pub fn close(&self) -> Result<(), WalError> {
        let mut seg = self.file.lock().map_err(|e| {
            WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
        })?;

        if seg.total_entries > 0 {
            Self::flush_batch(&mut seg)?;
            Self::write_footer(&mut seg)?;
            seg.file.sync_data()?;
        }
        Ok(())
    }

    /// Return the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }
}

impl Drop for WalSegment {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Best-effort close.
        let _ = self.close();
    }
}

// ---------------------------------------------------------------------------
// Segment WAL Recovery
// ---------------------------------------------------------------------------

/// Recover all entries from compressed segment WAL files in a directory.
///
/// This function:
/// 1. Looks for a v1 WAL file (`fastkv.wal`) and recovers from it if present.
/// 2. Finds all segment files (`seg_*.wal`) sorted by segment ID.
/// 3. For each segment, reads raw entries, decompresses batches, and
///    returns all valid entries.
///
/// # Migration
///
/// If a v1 WAL file exists alongside segment files, both are recovered.
/// The v1 entries come first (they are older), followed by segment entries
/// (newer). This ensures correct ordering during migration.
pub fn recover_segments(data_dir: &Path) -> Result<Vec<WalEntry>, WalError> {
    let mut all_entries = Vec::new();

    // Phase 1: recover from v1 WAL if present (migration).
    let v1_path = data_dir.join("fastkv.wal");
    if v1_path.exists() {
        match Wal::recover(&v1_path) {
            Ok(entries) => {
                if !entries.is_empty() {
                    eprintln!("[WAL] Recovered {} entries from v1 WAL (migration)", entries.len());
                    all_entries.extend(entries);
                }
            }
            Err(e) => {
                eprintln!("[WAL] Warning: v1 WAL recovery failed: {}", e);
            }
        }
    }

    // Phase 2: find and recover segment files.
    let mut segment_files: Vec<(u64, PathBuf)> = Vec::new();
    if let Ok(entries) = fs::read_dir(data_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(id_str) = name_str.strip_prefix("seg_").and_then(|s| s.strip_suffix(".wal")) {
                if let Ok(id) = u64::from_str_radix(id_str, 16) {
                    segment_files.push((id, entry.path()));
                }
            }
        }
    }

    // Sort by segment ID for correct ordering.
    segment_files.sort_by_key(|(id, _)| *id);

    for (seg_id, path) in &segment_files {
        match recover_single_segment(path) {
            Ok(entries) => {
                if !entries.is_empty() {
                    eprintln!("[WAL] Recovered {} entries from segment {}", entries.len(), seg_id);
                    all_entries.extend(entries);
                }
            }
            Err(e) => {
                eprintln!("[WAL] Warning: segment {} recovery failed: {}", seg_id, e);
            }
        }
    }

    Ok(all_entries)
}

/// Recover entries from a single segment file.
fn recover_single_segment(path: &Path) -> Result<Vec<WalEntry>, WalError> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();

    if file_size < SEGMENT_HEADER_SIZE as u64 {
        // Empty or too small — no entries.
        return Ok(Vec::new());
    }

    let mut reader = BufReader::with_capacity(256 * 1024, file);

    // --- Read header ---
    let mut header = [0u8; SEGMENT_HEADER_SIZE];
    reader.read_exact(&mut header)?;

    // Verify magic.
    if &header[0..4] != SEGMENT_MAGIC {
        // Not a segment file — could be a v1 WAL, skip.
        return Ok(Vec::new());
    }

    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != SEGMENT_VERSION {
        return Err(WalError::UnsupportedVersion(version));
    }

    let _segment_id = u64::from_le_bytes(header[6..14].try_into().map_err(|_| WalError::InvalidMagic)?);
    let raw_count = u32::from_le_bytes(header[14..18].try_into().map_err(|_| WalError::InvalidMagic)?);
    let _batch_size = u32::from_le_bytes(header[18..22].try_into().map_err(|_| WalError::InvalidMagic)?);

    let mut entries = Vec::new();

    // --- Read raw entries ---
    for _ in 0..raw_count {
        match read_raw_entry(&mut reader) {
            Ok(Some(entry)) => entries.push(entry),
            Ok(None) => break, // EOF
            Err(_) => break,   // Corrupted — stop
        }
    }

    // --- Try to read footer first to get batch index ---
    // Footer is at the end of the file. We'll try to read it.
    // Footer format: num_batches(4) + [batch_index_entries] + footer_crc(4) + FOOTER_MAGIC(4)
    // We search backward for FOOTER_MAGIC.

    let batch_index = read_footer_batch_index(path, file_size)?;

    if !batch_index.is_empty() {
        // Seek to each batch and decompress.
        for bix in &batch_index {
            reader.seek(SeekFrom::Start(bix.offset))?;
            match read_compressed_batch(&mut reader, bix.comp_len, bix.uncomp_len) {
                Ok(batch_entries) => entries.extend(batch_entries),
                Err(e) => {
                    eprintln!("[WAL] Warning: failed to decompress batch at offset {}: {}", bix.offset, e);
                }
            }
        }
    } else {
        // No footer or empty footer — try sequential reading of compressed batches.
        // This handles the case where the segment wasn't closed properly.
        loop {
            let pos = reader.stream_position()?;
            if pos >= file_size {
                break;
            }
            match read_compressed_batch_sequential(&mut reader) {
                Ok(batch_entries) => entries.extend(batch_entries),
                Err(_) => break,
            }
        }
    }

    Ok(entries)
}

/// Read a single raw entry from the reader (v1 format).
fn read_raw_entry(reader: &mut impl Read) -> Result<Option<WalEntry>, WalError> {
    // Read CRC (4 bytes).
    let mut crc_buf = [0u8; 4];
    if reader.read_exact(&mut crc_buf).is_err() {
        return Ok(None);
    }
    let stored_crc = u32::from_le_bytes(crc_buf);

    // Read op (1 byte).
    let mut op_buf = [0u8; 1];
    if reader.read_exact(&mut op_buf).is_err() {
        return Ok(None);
    }
    let op = match WalOp::try_from(op_buf[0]) {
        Ok(o) => o,
        Err(_) => return Ok(None),
    };

    // Read key_len (2 bytes).
    let mut kl_buf = [0u8; 2];
    if reader.read_exact(&mut kl_buf).is_err() {
        return Ok(None);
    }
    let key_len = u16::from_le_bytes(kl_buf) as usize;

    // Read key.
    let mut key = vec![0u8; key_len];
    if key_len > 0 && reader.read_exact(&mut key).is_err() {
        return Ok(None);
    }

    // Read val_len (2 bytes).
    let mut vl_buf = [0u8; 2];
    if reader.read_exact(&mut vl_buf).is_err() {
        return Ok(None);
    }
    let val_len = u16::from_le_bytes(vl_buf) as usize;

    // Read value.
    let mut value = vec![0u8; val_len];
    if val_len > 0 && reader.read_exact(&mut value).is_err() {
        return Ok(None);
    }

    // Verify CRC.
    let mut payload = Vec::with_capacity(5 + key_len + val_len);
    payload.push(op_buf[0]);
    payload.extend_from_slice(&(key_len as u16).to_le_bytes());
    payload.extend_from_slice(&key);
    payload.extend_from_slice(&(val_len as u16).to_le_bytes());
    payload.extend_from_slice(&value);

    let computed_crc = crc32c(&payload);
    if computed_crc != stored_crc {
        return Ok(None); // Corrupted — skip
    }

    // Skip padding.
    let total_payload = 4 + 1 + 2 + key_len + 2 + val_len;
    let padded = (total_payload + ENTRY_ALIGN - 1) & !(ENTRY_ALIGN - 1);
    let padding = padded - total_payload;
    if padding > 0 {
        let mut pad = [0u8; ENTRY_ALIGN];
        let _ = reader.read_exact(&mut pad[..padding]);
    }

    Ok(Some(WalEntry { op, key, value }))
}

/// Read a compressed batch at a known offset with known sizes.
fn read_compressed_batch(
    reader: &mut (impl Read + Seek),
    comp_len: u32,
    uncomp_len: u32,
) -> Result<Vec<WalEntry>, WalError> {
    // Read batch header: batch_crc(4) + uncomp_len(4) + comp_len(4).
    let mut batch_header = [0u8; 12];
    reader.read_exact(&mut batch_header)?;
    let stored_crc = u32::from_le_bytes([batch_header[0], batch_header[1], batch_header[2], batch_header[3]]);
    let _stored_uncomp = u32::from_le_bytes([batch_header[4], batch_header[5], batch_header[6], batch_header[7]]);
    let _stored_comp = u32::from_le_bytes([batch_header[8], batch_header[9], batch_header[10], batch_header[11]]);

    // Read compressed data.
    let mut compressed = vec![0u8; comp_len as usize];
    reader.read_exact(&mut compressed)?;

    // Verify batch CRC.
    let mut crc_buf = Vec::with_capacity(8 + comp_len as usize);
    crc_buf.extend_from_slice(&uncomp_len.to_le_bytes());
    crc_buf.extend_from_slice(&comp_len.to_le_bytes());
    crc_buf.extend_from_slice(&compressed);
    let computed_crc = crc32c(&crc_buf);
    if computed_crc != stored_crc {
        return Err(WalError::ChecksumMismatch {
            expected: stored_crc,
            actual: computed_crc,
        });
    }

    // Decompress.
    let raw = zstd_decompress(&compressed, uncomp_len as usize)
        .map_err(|e| WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?;

    // Parse entries from raw buffer.
    parse_entries_from_buffer(&raw)
}

/// Read a compressed batch sequentially (without knowing sizes in advance).
fn read_compressed_batch_sequential(reader: &mut impl Read) -> Result<Vec<WalEntry>, WalError> {
    // Read batch header: batch_crc(4) + uncomp_len(4) + comp_len(4).
    let mut batch_header = [0u8; 12];
    if reader.read_exact(&mut batch_header).is_err() {
        return Err(WalError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of compressed batch")));
    }

    let stored_crc = u32::from_le_bytes([batch_header[0], batch_header[1], batch_header[2], batch_header[3]]);
    let uncomp_len = u32::from_le_bytes([batch_header[4], batch_header[5], batch_header[6], batch_header[7]]);
    let comp_len = u32::from_le_bytes([batch_header[8], batch_header[9], batch_header[10], batch_header[11]]);

    // Sanity check: compressed size should be reasonable.
    if comp_len > 100_000_000 || uncomp_len > 1_000_000_000 {
        return Err(WalError::Io(io::Error::new(io::ErrorKind::InvalidData, "compressed batch size too large")));
    }

    // Read compressed data.
    let mut compressed = vec![0u8; comp_len as usize];
    if reader.read_exact(&mut compressed).is_err() {
        return Err(WalError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of compressed batch")));
    }

    // Verify batch CRC.
    let mut crc_buf = Vec::with_capacity(8 + comp_len as usize);
    crc_buf.extend_from_slice(&uncomp_len.to_le_bytes());
    crc_buf.extend_from_slice(&comp_len.to_le_bytes());
    crc_buf.extend_from_slice(&compressed);
    let computed_crc = crc32c(&crc_buf);
    if computed_crc != stored_crc {
        return Err(WalError::ChecksumMismatch {
            expected: stored_crc,
            actual: computed_crc,
        });
    }

    // Decompress.
    let raw = zstd_decompress(&compressed, uncomp_len as usize)
        .map_err(|e| WalError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?;

    parse_entries_from_buffer(&raw)
}

/// Parse WAL entries from a raw (decompressed) buffer.
fn parse_entries_from_buffer(buf: &[u8]) -> Result<Vec<WalEntry>, WalError> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos < buf.len() {
        // op(1)
        if pos + 1 > buf.len() { break; }
        let op_byte = buf[pos];
        let op = match WalOp::try_from(op_byte) {
            Ok(o) => o,
            Err(_) => break,
        };
        pos += 1;

        // key_len(2)
        if pos + 2 > buf.len() { break; }
        let key_len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;

        // key
        if pos + key_len > buf.len() { break; }
        let key = buf[pos..pos + key_len].to_vec();
        pos += key_len;

        // val_len(2)
        if pos + 2 > buf.len() { break; }
        let val_len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;

        // value
        if pos + val_len > buf.len() { break; }
        let value = buf[pos..pos + val_len].to_vec();
        pos += val_len;

        entries.push(WalEntry { op, key, value });
    }

    Ok(entries)
}

/// Read the footer from a segment file and return the batch index.
fn read_footer_batch_index(path: &Path, file_size: u64) -> Result<Vec<BatchIndexEntry>, WalError> {
    // Minimum footer size: num_batches(4) + footer_crc(4) + FOOTER_MAGIC(4) = 12
    if file_size < (SEGMENT_HEADER_SIZE as u64 + 12) {
        return Ok(Vec::new());
    }

    let mut file = File::open(path)?;

    // Read the last 8 bytes to check for FOOTER_MAGIC.
    let mut end_buf = [0u8; 8]; // footer_crc(4) + FOOTER_MAGIC(4)
    file.seek(SeekFrom::End(-8))?;
    file.read_exact(&mut end_buf)?;

    if &end_buf[4..8] != FOOTER_MAGIC {
        return Ok(Vec::new());
    }

    let footer_crc_stored = u32::from_le_bytes([end_buf[0], end_buf[1], end_buf[2], end_buf[3]]);

    // The footer body is everything between the segment data and footer_crc+FOOTER_MAGIC.
    // Footer layout: [num_batches(4)] [batch_index_entries × N] [footer_crc(4)] [FOOTER_MAGIC(4)]
    // Total footer size = 4 + num_batches * 20 + 4 + 4
    // We need to find num_batches. Strategy: try different values, check CRC.

    // Read up to 256KB before footer_crc to search for the footer body.
    let max_footer_body_size: u64 = 256 * 1024;
    let crc_pos = file_size - 8; // position of footer_crc
    let read_size = max_footer_body_size.min(crc_pos);
    let read_start = crc_pos - read_size;

    file.seek(SeekFrom::Start(read_start))?;
    let mut data = vec![0u8; read_size as usize];
    file.read_exact(&mut data)?;

    // Try different num_batches values (0..10_000), check CRC match.
    for num_batches in 0..=10_000u32 {
        let body_size = 4 + num_batches as usize * BATCH_INDEX_ENTRY_SIZE;
        if body_size > data.len() {
            break;
        }
        // The body is at the end of `data` (just before footer_crc).
        let body = &data[data.len() - body_size..];
        if crc32c(body) == footer_crc_stored {
            // Found it!
            let mut batch_index = Vec::with_capacity(num_batches as usize);
            for i in 0..num_batches as usize {
                let start = 4 + i * BATCH_INDEX_ENTRY_SIZE;
                if let Some(entry) = BatchIndexEntry::decode(&body[start..]) {
                    batch_index.push(entry);
                } else {
                    break;
                }
            }
            return Ok(batch_index);
        }
    }

    Ok(Vec::new())
}

// ---------------------------------------------------------------------------
// WalWriter trait implementation
// ---------------------------------------------------------------------------

impl WalWriter for WalSegment {
    fn wal_set(&self, key: &[u8], value: &[u8]) -> Result<(), WalError> { self.set(key, value) }
    fn wal_del(&self, key: &[u8]) -> Result<(), WalError> { self.del(key) }
    fn wal_expire(&self, key: &[u8], deadline_ms: u64) -> Result<(), WalError> { self.expire(key, deadline_ms) }
    fn wal_bset(&self, key: &[u8], original_value: &[u8]) -> Result<(), WalError> { self.bset(key, original_value) }
    fn wal_bdel(&self, key: &[u8]) -> Result<(), WalError> { self.bdel(key) }
    fn wal_list_op(&self, key: &[u8], payload: &[u8]) -> Result<(), WalError> { self.list_op(key, payload) }
    fn wal_sync_now(&self) -> Result<(), WalError> { self.sync_now() }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_create_and_recover_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = SegmentConfig::new(dir.path());
        let seg = WalSegment::open(config).unwrap();
        seg.close().unwrap();
        drop(seg);

        let entries = recover_segments(dir.path()).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_segment_raw_entries_only() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.raw_count = 5;    // Only 5 raw entries
        config.batch_size = 10;  // Won't fill a batch
        config.fsync_policy = FsyncPolicy::Never;

        let seg = WalSegment::open(config).unwrap();
        for i in 0..5 {
            let key = format!("key:{}", i);
            let val = format!("val:{}", i);
            seg.set(key.as_bytes(), val.as_bytes()).unwrap();
        }
        seg.close().unwrap();
        drop(seg);

        let entries = recover_segments(dir.path()).unwrap();
        assert_eq!(entries.len(), 5);
        for i in 0..5 {
            assert_eq!(entries[i].op, WalOp::Set);
            assert_eq!(entries[i].key, format!("key:{}", i).as_bytes());
        }
    }

    #[test]
    fn test_segment_compressed_batches() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.raw_count = 2;     // Only 2 raw entries
        config.batch_size = 3;    // 3 entries per batch
        config.fsync_policy = FsyncPolicy::Never;

        let seg = WalSegment::open(config).unwrap();

        // Write 8 entries total: 2 raw + 6 compressed (2 batches of 3)
        for i in 0..8 {
            let key = format!("key:{}", i);
            let val = format!("val:{}", i);
            seg.set(key.as_bytes(), val.as_bytes()).unwrap();
        }
        seg.close().unwrap();
        drop(seg);

        let entries = recover_segments(dir.path()).unwrap();
        assert_eq!(entries.len(), 8);
        for i in 0..8 {
            assert_eq!(entries[i].key, format!("key:{}", i).as_bytes());
            assert_eq!(entries[i].value, format!("val:{}", i).as_bytes());
        }
    }

    #[test]
    fn test_segment_all_op_types() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.raw_count = 1;
        config.batch_size = 5;
        config.fsync_policy = FsyncPolicy::Never;

        let seg = WalSegment::open(config).unwrap();
        seg.set(b"key1", b"value1").unwrap();
        seg.del(b"key1").unwrap();
        seg.expire(b"key2", 1700000000000).unwrap();
        seg.bset(b"blob1", b"large_data_here").unwrap();
        seg.bdel(b"blob1").unwrap();
        seg.list_op(b"mylist", b"\x01push_data").unwrap();
        seg.close().unwrap();
        drop(seg);

        let entries = recover_segments(dir.path()).unwrap();
        assert_eq!(entries.len(), 6);

        assert_eq!(entries[0].op, WalOp::Set);
        assert_eq!(entries[1].op, WalOp::Del);
        assert_eq!(entries[2].op, WalOp::Expire);
        assert_eq!(entries[3].op, WalOp::BSet);
        assert_eq!(entries[4].op, WalOp::BDel);
        assert_eq!(entries[5].op, WalOp::ListOp);
    }

    #[test]
    fn test_segment_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.raw_count = 2;
        config.batch_size = 5;
        config.max_segment_size = 512;  // Very small to trigger rotation
        config.fsync_policy = FsyncPolicy::Never;

        let seg = WalSegment::open(config).unwrap();

        // Write many entries to trigger rotation.
        for i in 0..50 {
            let key = format!("key:{:04}", i);
            let val = format!("value:{:04}", i);
            seg.set(key.as_bytes(), val.as_bytes()).unwrap();
        }
        seg.close().unwrap();
        drop(seg);

        let entries = recover_segments(dir.path()).unwrap();
        assert_eq!(entries.len(), 50);

        // Verify order is preserved across segments.
        for i in 0..50 {
            assert_eq!(entries[i].key, format!("key:{:04}", i).as_bytes());
        }
    }

    #[test]
    fn test_segment_migration_from_v1() {
        let dir = tempfile::tempdir().unwrap();

        // First, write a v1 WAL.
        let v1_path = dir.path().join("fastkv.wal");
        let wal = Wal::open(&v1_path, FsyncPolicy::Never).unwrap();
        wal.set(b"old_key", b"old_value").unwrap();
        wal.set(b"old_key2", b"old_value2").unwrap();
        drop(wal);

        // Then, write segment files.
        let mut config = SegmentConfig::new(dir.path());
        config.raw_count = 1;
        config.batch_size = 2;
        config.fsync_policy = FsyncPolicy::Never;

        let seg = WalSegment::open(config).unwrap();
        seg.set(b"new_key", b"new_value").unwrap();
        seg.del(b"old_key").unwrap();
        seg.set(b"another", b"entry").unwrap();
        seg.close().unwrap();
        drop(seg);

        // Recovery should get both v1 and segment entries.
        let entries = recover_segments(dir.path()).unwrap();
        // v1 entries come first.
        assert!(entries.len() >= 5);
        assert_eq!(entries[0].key, b"old_key");
        assert_eq!(entries[1].key, b"old_key2");
        // Segment entries follow.
        assert_eq!(entries[2].op, WalOp::Set);
        assert_eq!(entries[2].key, b"new_key");
    }

    #[test]
    fn test_segment_large_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.raw_count = 10;
        config.batch_size = 100;
        config.fsync_policy = FsyncPolicy::Never;

        let seg = WalSegment::open(config).unwrap();
        for i in 0..310 {
            let key = format!("k:{}", i);
            let val = format!("v:{}", i);
            seg.set(key.as_bytes(), val.as_bytes()).unwrap();
        }
        seg.close().unwrap();
        drop(seg);

        let entries = recover_segments(dir.path()).unwrap();
        assert_eq!(entries.len(), 310);
    }

    #[test]
    fn test_segment_crash_recovery_no_footer() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.raw_count = 2;
        config.batch_size = 5;
        config.fsync_policy = FsyncPolicy::Never;

        let seg = WalSegment::open(config).unwrap();
        for i in 0..7 {
            seg.set(format!("k:{}", i).as_bytes(), format!("v:{}", i).as_bytes()).unwrap();
        }
        // Don't call close() — simulate crash (pending batch not flushed, no footer).
        // The drop() will still try to close, but let's simulate it more aggressively.
        // We'll manually drop the inner file without writing footer.
        {
            let mut sf = seg.file.lock().unwrap();
            // Force flush the pending batch but don't write footer.
            WalSegment::flush_batch(&mut sf).ok();
            sf.file.sync_data().ok();
        }
        drop(seg);

        let entries = recover_segments(dir.path()).unwrap();
        // Should recover at least the raw entries + whatever batches were flushed.
        assert!(entries.len() >= 2);
    }

    #[test]
    fn test_segment_config_default() {
        let config = SegmentConfig::new("/tmp/test");
        assert_eq!(config.raw_count, DEFAULT_RAW_COUNT);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.max_segment_size, DEFAULT_MAX_SEGMENT_SIZE);
    }

    #[test]
    fn test_batch_index_entry_encode_decode() {
        let entry = BatchIndexEntry {
            offset: 12345,
            comp_len: 678,
            uncomp_len: 2048,
            entry_count: 42,
        };
        let encoded = entry.encode();
        let decoded = BatchIndexEntry::decode(&encoded).unwrap();
        assert_eq!(decoded.offset, 12345);
        assert_eq!(decoded.comp_len, 678);
        assert_eq!(decoded.uncomp_len, 2048);
        assert_eq!(decoded.entry_count, 42);
    }

    #[test]
    fn test_segment_binary_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.raw_count = 1;
        config.batch_size = 2;
        config.fsync_policy = FsyncPolicy::Never;

        let seg = WalSegment::open(config).unwrap();
        seg.set(b"\x00\xff\xab", b"\x01\x02\x03\x04").unwrap();
        seg.bset(b"blob\x00key", b"\xde\xad\xbe\xef\x00").unwrap();
        seg.del(b"\x00\xff\xab").unwrap();
        seg.close().unwrap();
        drop(seg);

        let entries = recover_segments(dir.path()).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"\x00\xff\xab");
        assert_eq!(entries[0].value, b"\x01\x02\x03\x04");
        assert_eq!(entries[1].op, WalOp::BSet);
        assert_eq!(entries[2].op, WalOp::Del);
    }
}
