//! # FastKV
//!
//! High-performance, Redis-compatible key-value store.
//!
//! ## Features
//! - Lock-free hash table for maximum concurrency
//! - Thread-safe (Send + Sync)
//! - Redis-compatible RESP protocol
//! - Linear scaling with CPU cores
//!
//! ## Example
//! ```rust
//! use fast_kv::KvStore;
//!
//! // Single-threaded
//! let store = KvStore::new();
//! store.set(b"hello", b"world");
//! let value = store.get(b"hello");
//! assert_eq!(value, Some(b"world".to_vec()));
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

pub use core::kv::{KvStore, KvStoreSingleThreaded, KvStoreSimple};
pub use core::resp::{Command, RespEncoder, RespParser};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
