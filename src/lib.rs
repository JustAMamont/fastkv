//! # FastKV
//!
//! High-performance, Redis-compatible key-value store.
//!
//! All features (Blob Arena, Similarity Search, WAL segments) are compiled
//! unconditionally. Runtime control via CLI flags.

// Index-based loops are intentional in the lock-free hot paths (kv.rs,
// minhash.rs, simhash.rs, etc.) where we read/write parallel AtomicU8
// arrays. The iterator-based rewrite clippy suggests would still need the
// index, hurting readability without any perf gain.
#![allow(clippy::needless_range_loop)]

pub mod core;

pub use core::kv::{KvStore, KvStoreLockFree, DEFAULT_INLINE_SIZE, IncrError};
pub use core::resp::{Command, RespEncoder, RespParser};
pub use core::wal::{Wal, WalEntry, WalOp, FsyncPolicy, WalWriter};
pub use core::wal_segment::{WalSegment, SegmentConfig, recover_segments};
pub use core::expiration::ExpirationManager;
pub use core::hash::{HashError, HashDelResult, WRONGTYPE_ERR};
pub use core::list::ListManager;
pub use core::blob::{BlobArena, BlobRef, BlobStats, BLOB_REF_FLAG, BLOB_REF_SIZE};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
