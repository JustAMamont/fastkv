//! Core components — platform-independent.
//!
//! This module contains the core logic that works on any platform:
//!
//! * [`kv`] — lock-free hash table (the data engine)
//! * [`resp`] — RESP protocol parser and encoder
//! * [`wal`] — write-ahead log for crash-consistent persistence
//! * [`expiration`] — TTL / key-expiration management
//! * [`hash`] — Redis-compatible hash data type (HGET/HSET/…)
//! * [`server`] — TCP server implementations (Tokio, io_uring)

pub mod kv;
pub mod resp;
pub mod wal;
pub mod expiration;
pub mod hash;
pub mod server;
