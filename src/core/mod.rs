//! Core components — platform-independent.
//!
//! All modules are always compiled — no feature gates.
//! Blob Arena, Similarity Search, and io_uring are controlled at runtime,
//! not compile time.

pub mod kv;
pub mod resp;
pub mod wal;
pub mod expiration;
pub mod hash;
pub mod list;
pub mod checkpoint;
pub mod pubsub;
pub mod sortedset;
pub mod simhash;
pub mod minhash;
pub mod lsh;
pub mod blob;
pub mod wal_segment;
pub mod server;
