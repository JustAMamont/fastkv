//! # FastKV
//!
//! High-performance, Redis-compatible key-value store.
//!
//! ## Features
//!
//! - **Lock-free hash table** — thread-safe without mutexes; uses atomic
//!   CAS and optimistic version reads.
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
//! // Single-threaded
//! let store = KvStore::new();
//! store.set(b"hello", b"world");
//! let value = store.get(b"hello");
//! assert_eq!(value, Some(b"world".to_vec()));
//!
//! // Atomic increment (INCR)
//! store.set(b"counter", b"0");
//! store.incr(b"counter", 1).unwrap(); // → 1
//!
//! // Multi-threaded (lock-free!)
//! use std::sync::Arc;
//! use std::thread;
//!
//! let store = Arc::new(KvStore::new());
//! let store_clone = store.clone();
//!
//! let handle = thread::spawn(move || {
//!     store_clone.set(b"thread_key", b"thread_value");
//! });
//!
//! handle.join().unwrap();
//! assert_eq!(store.get(b"thread_key"), Some(b"thread_value".to_vec()));
//! ```

pub mod core;

pub use core::kv::KvStore;
pub use core::resp::{Command, RespEncoder, RespParser};
pub use core::wal::{Wal, WalEntry, WalOp, FsyncPolicy};
pub use core::expiration::ExpirationManager;
pub use core::hash::{HashError, HashDelResult, WRONGTYPE_ERR};
pub use core::list::ListManager;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
