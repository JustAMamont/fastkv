//! Core components — platform-independent.
//!
//! This module contains the core logic that works on any platform:
//!
//! * [`kv`] — lock-free hash table (the data engine)
//! * [`resp`] — RESP protocol parser and encoder
//! * [`wal`] — write-ahead log for crash-consistent persistence
//! * [`expiration`] — TTL / key-expiration management
//! * [`hash`] — Redis-compatible hash data type (HGET/HSET/…)
//! * [`blob`] — compressed large-value storage (BSET/BGET/…)
//! * [`simhash`] — SimHash locality-sensitive hashing for near-duplicate detection
//! * [`minhash`] — MinHash signature for Jaccard similarity estimation
//! * [`lsh`] — Locality-Sensitive Hashing (LSH) for O(1) approximate nearest neighbor search
//! * [`server`] — TCP server implementations (Tokio, io_uring)

pub mod kv;
pub mod resp;
pub mod wal;
pub mod expiration;
pub mod hash;
pub mod list;
pub mod checkpoint;
#[cfg(feature = "similarity")]
pub mod simhash;
#[cfg(feature = "similarity")]
pub mod minhash;
#[cfg(feature = "similarity")]
pub mod lsh;
pub mod server;

#[cfg(feature = "blob-store")]
pub mod blob;
#[cfg(feature = "blob-store")]
pub mod wal_segment;
