//! # FastKV
//!
//! High-performance, Redis-compatible key-value store.
//!
//! ## Features
//!
//! - **Lock-free hash table** — thread-safe without mutexes; uses atomic
//!   CAS and optimistic version reads.
//! - **Const-generic inline size** — compile-time configurable key/value
//!   storage size (default 64 bytes per side).
//! - **Redis-compatible** — supports RESP protocol; works with `redis-cli`.
//! - **Pipeline support** — batch multiple commands for higher throughput.
//! - **Persistence** — optional WAL with configurable fsync policy.
//! - **TTL / Expiration** — lazy + active key expiration.
//! - **io_uring (Linux)** — optional kernel bypass for maximum performance.
//! - **Cross-platform** — works on Linux, macOS, Windows.
//!
//! ## Example
//!
//! ```rust
//! use fast_kv::KvStore;
//!
//! // Single-threaded (default 64-byte inline size per side)
//! let store = KvStore::new();
//! store.set(b"hello", b"world");
//! let value = store.get(b"hello");
//! assert_eq!(value, Some(b"world".to_vec()));
//!
//! // Custom inline size (128 bytes per side)
//! use fast_kv::KvStoreLockFree;
//! let store: KvStoreLockFree<128> = KvStoreLockFree::new();
//! store.set(&[0u8; 100], &[0u8; 100]);
//!
//! // Atomic increment (INCR)
//! store.set(b"counter", b"0");
//! store.incr(b"counter", 1).unwrap(); // -> 1
//! ```

pub mod core;

pub use core::kv::{KvStore, KvStoreLockFree, DEFAULT_INLINE_SIZE, IncrError};
pub use core::resp::{Command, RespEncoder, RespParser};
pub use core::wal::{Wal, WalEntry, WalOp, FsyncPolicy};
pub use core::expiration::ExpirationManager;
pub use core::hash::{HashError, HashDelResult, WRONGTYPE_ERR};
pub use core::list::ListManager;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
