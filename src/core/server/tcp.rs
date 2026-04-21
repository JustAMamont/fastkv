//! Optimized Tokio TCP server.
//!
//! Performance techniques:
//! - Reusable read/write buffers (zero per-request allocation).
//! - Batched writes — all responses for a single `read()` are flushed at
//!   once, reducing syscalls.
//! - Pipeline support — multiple commands in a single TCP read are all
//!   processed before a single `write_all`.
//!
//! The server accepts optional [`Wal`] and [`ExpirationManager`] handles
//! so that every mutation can be persisted and every GET can be checked
//! for expiration.

use crate::core::hash::{self, HashDelResult, WRONGTYPE_ERR};
use crate::core::kv::{IncrError, KvStore};
use crate::core::expiration::ExpirationManager;
use crate::core::resp::{write_usize_buf, RespEncoder, RespParser};
use crate::core::wal::Wal;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Default port (same as Redis).
pub const DEFAULT_PORT: u16 = 6379;

/// Maximum per-connection read buffer (1 MiB).
const MAX_BUFFER_SIZE: usize = 1024 * 1024;

/// Initial response buffer capacity.
const INITIAL_RESPONSE_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Tokio-based async TCP server.
///
/// Accepts Redis clients (RESP protocol) on the configured port.
pub struct TokioServer {
    /// Shared lock-free KV store.
    #[allow(dead_code)]
    store: Arc<KvStore>,
    /// Optional WAL for crash-consistent persistence.
    wal: Option<Arc<Wal>>,
    /// Optional expiration manager.
    expiry: Option<Arc<ExpirationManager>>,
    /// Port to listen on.
    pub port: u16,
}

impl TokioServer {
    /// Create a server with default settings (port 6379, no WAL, no TTL).
    pub fn new() -> Self {
        Self {
            store: Arc::new(KvStore::new()),
            wal: None,
            expiry: None,
            port: DEFAULT_PORT,
        }
    }

    /// Create a server that listens on *port*.
    pub fn with_port(port: u16) -> Self {
        Self {
            store: Arc::new(KvStore::new()),
            wal: None,
            expiry: None,
            port,
        }
    }

    /// Create a fully configured server.
    pub fn with_components(
        port: u16,
        store: Arc<KvStore>,
        wal: Option<Arc<Wal>>,
        expiry: Option<Arc<ExpirationManager>>,
    ) -> Self {
        Self { store, wal, expiry, port }
    }

    /// Clone the shared store reference.
    pub fn store(&self) -> Arc<KvStore> {
        Arc::clone(&self.store)
    }

    /// Run the accept loop (async — call inside a Tokio runtime).
    pub async fn run(&self) {
        let addr = format!("0.0.0.0:{}", self.port);

        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Failed to bind to {}: {}", addr, e);
                return;
            }
        };

        println!("FastKV server listening on {}", addr);
        println!("Ready to accept Redis clients!");
        println!();

        let wal = self.wal.clone();
        let expiry = self.expiry.clone();

        loop {
            let (socket, peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                    continue;
                }
            };

            let store = Arc::clone(&self.store);
            let wal = wal.clone();
            let expiry = expiry.clone();

            tokio::spawn(async move {
                handle_client(socket, store, wal, expiry, peer_addr).await;
            });
        }
    }
}

impl Default for TokioServer {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Client handler
// ---------------------------------------------------------------------------

/// Per-connection state and main read/write loop.
async fn handle_client(
    mut socket: TcpStream,
    store: Arc<KvStore>,
    wal: Option<Arc<Wal>>,
    expiry: Option<Arc<ExpirationManager>>,
    addr: std::net::SocketAddr,
) {
    // Reusable buffers — allocated once, cleared between iterations.
    let mut read_buf = vec![0u8; MAX_BUFFER_SIZE];
    let mut leftover: Vec<u8> = Vec::with_capacity(4096);
    let mut resp_buf = Vec::with_capacity(INITIAL_RESPONSE_SIZE);

    loop {
        let n = match socket.read(&mut read_buf).await {
            Ok(0) => break,  // client disconnected
            Ok(n) => n,
            Err(e) => {
                eprintln!("[{}] Read error: {}", addr, e);
                break;
            }
        };

        leftover.extend_from_slice(&read_buf[..n]);
        resp_buf.clear();

        let mut consumed = 0;
        let mut should_close = false;
        while consumed < leftover.len() {
            match parse_command_bounds(&leftover[consumed..]) {
                Some((len, is_inline)) => {
                    let cmd_data = &leftover[consumed..consumed + len];
                    should_close = process_command_into(cmd_data, &store, wal.as_deref(), expiry.as_deref(), &mut resp_buf, is_inline);
                    consumed += len;
                    if should_close { break; }
                }
                None => break,
            }
        }
        leftover.drain(..consumed);

        if !resp_buf.is_empty() {
            if let Err(e) = socket.write_all(&resp_buf).await {
                eprintln!("[{}] Write error: {}", addr, e);
                break;
            }
        }
        if should_close { break; }
    }
}

// ---------------------------------------------------------------------------
// Command-boundary detection
// ---------------------------------------------------------------------------

/// Determine the byte-length of the next complete command inside *data*.
///
/// Returns `Some((bytes_consumed, is_inline))` or `None` if the data
/// does not yet contain a full command.
pub fn parse_command_bounds(data: &[u8]) -> Option<(usize, bool)> {
    if data.is_empty() {
        return None;
    }

    match data[0] {
        b'*' => find_resp_array_end(data).map(|end| (end, false)),

        // Inline commands — recognized by their first letter.
        b'G' | b'S' | b'D' | b'P' | b'I' | b'H' | b'L' | b'E' | b'C'
        | b'Q' | b'F' | b'K' | b'M' | b'T' | b'X' | b'R' | b'O' | b'U'
        | b'Z' | b'N' => {
            for i in 0..data.len() {
                if data[i] == b'\n' {
                    return Some((i + 1, true));
                }
            }
            None
        }
        _ => None,
    }
}

/// Find the byte-length of a complete RESP array.
fn find_resp_array_end(data: &[u8]) -> Option<usize> {
    if data.len() < 4 || data[0] != b'*' {
        return None;
    }

    let mut pos = 1;
    // Find first \r\n.
    while pos + 1 < data.len() {
        if data[pos] == b'\r' && data[pos + 1] == b'\n' { break; }
        pos += 1;
    }
    if pos + 1 >= data.len() { return None; }

    let count: usize = std::str::from_utf8(&data[1..pos]).ok()?.parse().ok()?;
    pos += 2;

    for _ in 0..count {
        if pos >= data.len() || data[pos] != b'$' { return None; }
        pos += 1;

        let len_start = pos;
        while pos + 1 < data.len() {
            if data[pos] == b'\r' && data[pos + 1] == b'\n' { break; }
            pos += 1;
        }
        if pos + 1 >= data.len() { return None; }

        let len: usize = std::str::from_utf8(&data[len_start..pos]).ok()?.parse().ok()?;
        pos += 2;

        if pos + len + 2 > data.len() { return None; }
        pos += len + 2;
    }

    Some(pos)
}

// ---------------------------------------------------------------------------
// Command processing (shared between Tokio and io_uring servers)
// ---------------------------------------------------------------------------

/// Parse a single command and append the RESP-encoded response to *out*.
///
/// Returns `true` if the connection should be closed (e.g. QUIT command).
///
/// This is the **central command dispatcher**. Both the Tokio server and
/// the io_uring server delegate to this function so that behaviour is
/// identical regardless of the I/O backend.
pub fn process_command_into(
    data: &[u8],
    store: &KvStore,
    wal: Option<&Wal>,
    expiry: Option<&ExpirationManager>,
    out: &mut Vec<u8>,
    is_inline: bool,
) -> bool {
    // ---- inline command handling ----
    if is_inline {
        return handle_inline(data, store, wal, expiry, out);
    }

    // ---- RESP array command handling ----
    let command = match RespParser::parse(data) {
        Ok(cmd) => cmd,
        Err(e) => {
            let msg = format!("ERR parse error: {}", e);
            RespEncoder::write_error(out, &msg);
            return false;
        }
    };

    dispatch_command(&command, store, wal, expiry, out)
}

/// Dispatch an already-parsed [`Command`] and append the response.
///
/// Returns `true` if the connection should be closed (e.g. QUIT command).
fn dispatch_command(
    cmd: &crate::core::resp::Command,
    store: &KvStore,
    wal: Option<&Wal>,
    expiry: Option<&ExpirationManager>,
    out: &mut Vec<u8>,
) -> bool {
    match cmd.name.as_str() {
        // ----- Core -----

        "GET" => { cmd_get(cmd, store, expiry, out); false }
        "SET" => { cmd_set(cmd, store, wal, expiry, out); false }
        "DEL" => { cmd_del(cmd, store, wal, expiry, out); false }
        "PING" => { cmd_ping(cmd, out); false }
        "ECHO" => { cmd_echo(cmd, out); false }
        "COMMAND" => { out.extend_from_slice(b"+OK\r\n"); false }
        "INFO"  => { cmd_info(store, out); false }
        "DBSIZE" => { RespEncoder::write_integer(out, store.len() as i64); false }
        "QUIT"  => { out.extend_from_slice(b"+OK\r\n"); true }

        // ----- String operations -----

        "INCR"    => { cmd_incr(cmd, store, wal, expiry, out, 1); false }
        "INCRBY"  => { cmd_incrby(cmd, store, wal, expiry, out); false }
        "DECR"    => { cmd_incr(cmd, store, wal, expiry, out, -1); false }
        "DECRBY"  => { cmd_decrby(cmd, store, wal, expiry, out); false }
        "APPEND"  => { cmd_append(cmd, store, wal, expiry, out); false }
        "STRLEN"  => { cmd_strlen(cmd, store, expiry, out); false }
        "GETRANGE"  => { cmd_getrange(cmd, store, expiry, out); false }
        "SETRANGE"  => { cmd_setrange(cmd, store, wal, expiry, out); false }
        "MGET"    => { cmd_mget(cmd, store, expiry, out); false }
        "MSET"    => { cmd_mset(cmd, store, wal, out); false }
        "EXISTS"  => { cmd_exists(cmd, store, expiry, out); false }

        // ----- Expiration -----

        "EXPIRE"  => { cmd_expire(cmd, store, expiry, out); false }
        "TTL"     => { cmd_ttl(cmd, expiry, out); false }
        "PTTL"    => { cmd_pttl(cmd, expiry, out); false }
        "PERSIST" => { cmd_persist(cmd, expiry, out); false }

        // ----- Hash -----

        "HSET"    => { cmd_hset(cmd, store, wal, expiry, out); false }
        "HGET"    => { cmd_hget(cmd, store, expiry, out); false }
        "HDEL"    => { cmd_hdel(cmd, store, wal, expiry, out); false }
        "HGETALL" => { cmd_hgetall(cmd, store, expiry, out); false }
        "HEXISTS" => { cmd_hexists(cmd, store, expiry, out); false }
        "HLEN"    => { cmd_hlen(cmd, store, expiry, out); false }
        "HKEYS"   => { cmd_hkeys(cmd, store, expiry, out); false }
        "HVALS"   => { cmd_hvals(cmd, store, expiry, out); false }
        "HMGET"   => { cmd_hmget(cmd, store, expiry, out); false }
        "HMSET"   => { cmd_hmset(cmd, store, wal, expiry, out); false }

        _ => {
            let msg = format!("ERR unknown command '{}'", cmd.name);
            RespEncoder::write_error(out, &msg);
            false
        }
    }
}

/// Shortcut for a "wrong number of arguments" error response.
#[inline]
fn err_wrong_args(out: &mut Vec<u8>) {
    out.extend_from_slice(b"-ERR wrong number of arguments\r\n");
}

// ---------------------------------------------------------------------------
// Inline command handler
// ---------------------------------------------------------------------------

/// Parse and dispatch a plain-text inline command (e.g. `GET key\r\n`).
///
/// Returns `true` if the connection should be closed (e.g. QUIT command).
fn handle_inline(
    data: &[u8],
    store: &KvStore,
    wal: Option<&Wal>,
    expiry: Option<&ExpirationManager>,
    out: &mut Vec<u8>,
) -> bool {
    let line_end = data.iter().position(|&b| b == b'\r' || b == b'\n').unwrap_or(data.len());
    let line = &data[..line_end];
    let parts: Vec<&[u8]> = line.split(|&b| b == b' ').collect();
    if parts.is_empty() {
        out.extend_from_slice(b"-ERR empty command\r\n");
        return false;
    }

    let name = std::str::from_utf8(parts[0]).unwrap_or("").to_uppercase();
    let args: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();

    let cmd = crate::core::resp::Command { name, args };
    dispatch_command(&cmd, store, wal, expiry, out)
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

/// Handle `GET key` — return the value or null bulk string.
///
/// Returns a WRONGTYPE error if the key holds a hash.
fn cmd_get(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Lazy expiration check.
    if let Some(exp) = expiry {
        if exp.is_expired(key) {
            exp.purge_if_expired(key);
            RespEncoder::write_null(out);
            return;
        }
    }

    match store.get(key) {
        Some(v) if hash::is_hash_value(&v) => {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
        }
        Some(v) => RespEncoder::write_bulk_string(out, &v),
        None => RespEncoder::write_null(out),
    }
}

/// Handle `SET key value` — insert/update and persist to WAL.
fn cmd_set(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let (Some(key), Some(value)) = (cmd.key(), cmd.value()) else { return err_wrong_args(out); };

    // Remove old TTL if overwriting.
    if let Some(exp) = expiry {
        exp.remove(key);
    }

    if store.set(key, value) {
        if let Some(w) = wal {
            if w.set(key, value).is_err() {
                out.extend_from_slice(b"-ERR WAL write failed\r\n");
                return;
            }
        }
        out.extend_from_slice(b"+OK\r\n");
    } else {
        out.extend_from_slice(b"-ERR value too large\r\n");
    }
}

/// Handle `DEL key` — remove key, persist to WAL, clean up TTL.
fn cmd_del(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    let deleted = store.del(key);
    if deleted {
        if let Some(w) = wal { let _ = w.del(key); }
        if let Some(exp) = expiry { exp.remove(key); }
    }
    out.extend_from_slice(if deleted { b":1\r\n" } else { b":0\r\n" });
}

/// Handle `PING [message]` — return PONG or echo the message.
fn cmd_ping(cmd: &crate::core::resp::Command, out: &mut Vec<u8>) {
    if let Some(arg) = cmd.arg(1) {
        RespEncoder::write_bulk_string(out, arg);
    } else {
        RespEncoder::write_simple_string(out, "PONG");
    }
}

/// Handle `ECHO message` — return the message as a bulk string.
fn cmd_echo(cmd: &crate::core::resp::Command, out: &mut Vec<u8>) {
    let Some(msg) = cmd.arg(1) else { return err_wrong_args(out); };
    RespEncoder::write_bulk_string(out, msg);
}

/// Handle `INFO` — return server info as a bulk string.
///
/// Writes directly into the response buffer to avoid `format!()` allocation.
fn cmd_info(store: &KvStore, out: &mut Vec<u8>) {
    out.extend_from_slice(b"# Server\r\nfastkv_version:");
    out.extend_from_slice(crate::VERSION.as_bytes());
    out.extend_from_slice(b"\r\n# Memory\r\nused_memory_approx:");
    write_usize_buf(out, store.len() * 200);
    out.extend_from_slice(b"\r\n# Keys\r\ndb_size:");
    write_usize_buf(out, store.len());
    out.extend_from_slice(b"\r\n");
}

// ---------------------------------------------------------------------------
// String operations
// ---------------------------------------------------------------------------

/// Handle `INCR`/`DECR` — atomically increment/decrement by *delta*.
fn cmd_incr(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, _expiry: Option<&ExpirationManager>, out: &mut Vec<u8>, delta: i64) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    match store.incr(key, delta) {
        Ok(new_val) => {
            if let Some(w) = wal {
                if w.set(key, new_val.to_string().as_bytes()).is_err() {
                    RespEncoder::write_error(out, "ERR WAL write failed");
                    return;
                }
            }
            RespEncoder::write_integer(out, new_val);
        }
        Err(IncrError::KeyNotFound) => {
            RespEncoder::write_error(out, "ERR no such key");
        }
        Err(IncrError::NotInteger) => {
            RespEncoder::write_error(out, "ERR value is not an integer or out of range");
        }
        Err(IncrError::Overflow) => {
            RespEncoder::write_error(out, "ERR increment or decrement would overflow");
        }
        Err(_) => {
            RespEncoder::write_error(out, "ERR increment failed");
        }
    }
}

/// Handle `INCRBY key delta` — increment by a specified amount.
fn cmd_incrby(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let (Some(_key), Some(delta_bytes)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    let delta: i64 = match std::str::from_utf8(delta_bytes).ok().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => { RespEncoder::write_error(out, "ERR value is not an integer"); return; }
    };

    cmd_incr(cmd, store, wal, expiry, out, delta);
}

/// Handle `DECRBY key delta` — decrement by a specified (positive) amount.
fn cmd_decrby(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let (Some(_key), Some(delta_bytes)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    let delta: i64 = match std::str::from_utf8(delta_bytes).ok().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => { RespEncoder::write_error(out, "ERR value is not an integer"); return; }
    };

    // Negate for decrement.
    cmd_incr(cmd, store, wal, expiry, out, -delta);
}

/// Handle `APPEND key suffix` — append to string value, persist result.
///
/// Returns a WRONGTYPE error if the key holds a hash.
fn cmd_append(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, _expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let (Some(key), Some(suffix)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    // WRONGTYPE check: cannot APPEND to a hash.
    if let Some(current) = store.get(key) {
        if hash::is_hash_value(&current) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    match store.append(key, suffix) {
        Some(new_len) => {
            if let Some(w) = wal {
                if let Some(val) = store.get(key) {
                    if w.set(key, &val).is_err() {
                        RespEncoder::write_error(out, "ERR WAL write failed");
                        return;
                    }
                }
            }
            RespEncoder::write_integer(out, new_len as i64);
        }
        None => {
            RespEncoder::write_error(out, "ERR append failed (value too large)");
        }
    }
}

/// Handle `STRLEN key` — return byte length of the string value.
///
/// Returns a WRONGTYPE error if the key holds a hash.
fn cmd_strlen(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    if let Some(exp) = expiry {
        if exp.is_expired(key) {
            exp.purge_if_expired(key);
            out.extend_from_slice(b":0\r\n");
            return;
        }
    }

    // WRONGTYPE check.
    if let Some(v) = store.get(key) {
        if hash::is_hash_value(&v) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    RespEncoder::write_integer(out, store.strlen(key) as i64);
}

/// Handle `GETRANGE key start end` — return substring (inclusive bounds).
///
/// Returns a WRONGTYPE error if the key holds a hash.
fn cmd_getrange(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let (Some(key), Some(start_s), Some(end_s)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    if let Some(exp) = expiry {
        if exp.is_expired(key) {
            exp.purge_if_expired(key);
            out.extend_from_slice(b"$0\r\n\r\n");
            return;
        }
    }

    // WRONGTYPE check.
    if let Some(v) = store.get(key) {
        if hash::is_hash_value(&v) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    let start: i64 = std::str::from_utf8(start_s).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let end: i64 = std::str::from_utf8(end_s).ok().and_then(|s| s.parse().ok()).unwrap_or(-1);

    let value = store.getrange(key, start, end).unwrap_or_default();
    RespEncoder::write_bulk_string(out, &value);
}

/// Handle `SETRANGE key offset value` — overwrite part of a string, persist result.
///
/// Returns a WRONGTYPE error if the key holds a hash.
fn cmd_setrange(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, _expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let (Some(key), Some(offset_s), Some(replacement)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    // WRONGTYPE check.
    if let Some(v) = store.get(key) {
        if hash::is_hash_value(&v) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    let offset: usize = match std::str::from_utf8(offset_s).ok().and_then(|s| s.parse().ok()) {
        Some(o) => o,
        None => { RespEncoder::write_error(out, "ERR offset is not an integer"); return; }
    };

    match store.setrange(key, offset, replacement) {
        Some(new_len) => {
            if let Some(w) = wal {
                if let Some(val) = store.get(key) {
                    if w.set(key, &val).is_err() {
                        RespEncoder::write_error(out, "ERR WAL write failed");
                        return;
                    }
                }
            }
            RespEncoder::write_integer(out, new_len as i64);
        }
        None => {
            RespEncoder::write_error(out, "ERR setrange failed (result too large)");
        }
    }
}

/// Handle `MGET key1 key2 ...` — return an array of values (nil for missing).
fn cmd_mget(cmd: &crate::core::resp::Command, store: &KvStore, _expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    if cmd.argc() < 2 {
        return err_wrong_args(out);
    }
    let keys: Vec<&[u8]> = (1..cmd.argc()).filter_map(|i| cmd.arg(i)).collect();
    let values = store.mget(&keys);

    out.push(b'*');
    write_usize_buf(out, values.len());
    out.extend_from_slice(b"\r\n");
    for val in &values {
        match val {
            Some(v) => RespEncoder::write_bulk_string(out, v),
            None => RespEncoder::write_null(out),
        }
    }
}

/// Handle `MSET k1 v1 k2 v2 ...` — set multiple key-value pairs atomically.
fn cmd_mset(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, out: &mut Vec<u8>) {
    let argc = cmd.argc();
    if argc < 3 || argc % 2 == 0 {
        return err_wrong_args(out);
    }

    let pairs: Vec<(&[u8], &[u8])> = (1..argc)
        .step_by(2)
        .filter_map(|i| Some((cmd.arg(i)?, cmd.arg(i + 1)?)))
        .collect();

    if store.mset(&pairs) {
        if let Some(w) = wal {
            for (k, v) in &pairs {
                if w.set(k, v).is_err() {
                    RespEncoder::write_error(out, "ERR WAL write failed");
                    return;
                }
            }
        }
        RespEncoder::write_simple_string(out, "OK");
    } else {
        RespEncoder::write_error(out, "ERR MSET failed");
    }
}

/// Handle `EXISTS key` — return 1 if key exists, 0 otherwise.
fn cmd_exists(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    if let Some(exp) = expiry {
        if exp.is_expired(key) {
            exp.purge_if_expired(key);
            out.extend_from_slice(b":0\r\n");
            return;
        }
    }

    out.extend_from_slice(if store.exists(key) { b":1\r\n" } else { b":0\r\n" });
}

// ---------------------------------------------------------------------------
// Expiration commands
// ---------------------------------------------------------------------------

/// Handle `EXPIRE key seconds` — set a TTL in seconds.
fn cmd_expire(cmd: &crate::core::resp::Command, _store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let (Some(key), Some(secs_s)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };
    let Some(exp) = expiry else {
        out.extend_from_slice(b"-ERR expiration not enabled\r\n");
        return;
    };

    let secs: u64 = match std::str::from_utf8(secs_s).ok().and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => { out.extend_from_slice(b"-ERR value is not an integer\r\n"); return; }
    };

    if exp.expire(key, std::time::Duration::from_secs(secs)) {
        out.extend_from_slice(b":1\r\n");
    } else {
        out.extend_from_slice(b":0\r\n");
    }
}

/// Handle `TTL key` — return remaining TTL in seconds, or -2 if no TTL.
fn cmd_ttl(cmd: &crate::core::resp::Command, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    let Some(exp) = expiry else {
        out.extend_from_slice(b"-2\r\n");  // no TTL support → behave like missing key
        return;
    };

    match exp.ttl(key) {
        Some(dur) => {
            let secs = dur.as_secs();
            RespEncoder::write_integer(out, secs as i64);
        }
        None => out.extend_from_slice(b"-2\r\n"),
    }
}

/// Handle `PTTL key` — return remaining TTL in milliseconds, or -2 if no TTL.
fn cmd_pttl(cmd: &crate::core::resp::Command, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    let Some(exp) = expiry else {
        out.extend_from_slice(b"-2\r\n");
        return;
    };

    match exp.ttl(key) {
        Some(dur) => {
            let ms = dur.as_millis() as i64;
            RespEncoder::write_integer(out, ms);
        }
        None => out.extend_from_slice(b"-2\r\n"),
    }
}

/// Handle `PERSIST key` — remove the TTL, making the key persistent.
fn cmd_persist(cmd: &crate::core::resp::Command, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    let Some(exp) = expiry else {
        out.extend_from_slice(b":0\r\n");
        return;
    };

    out.extend_from_slice(if exp.persist(key) { b":1\r\n" } else { b":0\r\n" });
}

// ---------------------------------------------------------------------------
// Hash commands
// ---------------------------------------------------------------------------

/// Retrieve the current value for a key, respecting expiration.
///
/// Returns the raw value bytes, or an empty `Vec` if the key does not exist
/// or has expired.
#[inline]
fn get_current_value(key: &[u8], store: &KvStore, expiry: Option<&ExpirationManager>) -> Vec<u8> {
    if let Some(exp) = expiry {
        if exp.is_expired(key) {
            exp.purge_if_expired(key);
            return Vec::new();
        }
    }
    store.get(key).unwrap_or_default()
}

/// Handle `HSET key field value [field value ...]` — set one or more hash fields.
///
/// Returns the number of fields that were **added** (i.e. did not previously
/// exist). Updating an existing field does not count.
fn cmd_hset(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    // Need at least key + field + value = 3 args.
    if cmd.argc() < 4 || (cmd.argc() - 2) % 2 != 0 {
        return err_wrong_args(out);
    }

    let mut current = get_current_value(key, store, expiry);
    let mut new_fields = 0i64;

    // Process field-value pairs sequentially.
    let mut i = 2usize;
    while i + 1 < cmd.argc() {
        let Some(field) = cmd.arg(i) else { break; };
        let Some(value) = cmd.arg(i + 1) else { break; };

        let was_new = hash::hash_get(&current, field).is_none();

        match hash::hash_set(&current, field, value) {
            Ok(new_data) => {
                current = new_data;
                if was_new {
                    new_fields += 1;
                }
            }
            Err(hash::HashError::NotAHash) => {
                RespEncoder::write_error(out, WRONGTYPE_ERR);
                return;
            }
            Err(e) => {
                RespEncoder::write_error(out, &format!("ERR {}", e));
                return;
            }
        }
        i += 2;
    }

    // Persist the final state.
    if store.set(key, &current) {
        if let Some(w) = wal {
            if w.set(key, &current).is_err() {
                RespEncoder::write_error(out, "ERR WAL write failed");
                return;
            }
        }
        RespEncoder::write_integer(out, new_fields);
    } else {
        RespEncoder::write_error(out, "ERR hash value too large for inline storage");
    }
}

/// Handle `HGET key field` — return the value of a hash field, or nil.
fn cmd_hget(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let (Some(key), Some(field)) = (cmd.key(), cmd.arg(2)) else {
        return err_wrong_args(out);
    };

    let current = get_current_value(key, store, expiry);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    match hash::hash_get(&current, field) {
        Some(v) => RespEncoder::write_bulk_string(out, &v),
        None => RespEncoder::write_null(out),
    }
}

/// Handle `HDEL key field [field ...]` — delete one or more hash fields.
///
/// Returns the number of fields that were removed.
fn cmd_hdel(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    if cmd.argc() < 3 {
        return err_wrong_args(out);
    }

    let mut current = get_current_value(key, store, expiry);

    // WRONGTYPE check.
    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    let mut deleted = 0i64;

    for i in 2..cmd.argc() {
        let Some(field) = cmd.arg(i) else { continue; };
        if current.is_empty() { break; }

        match hash::hash_del(&current, field) {
            HashDelResult::Updated(new_data) => {
                current = new_data;
                deleted += 1;
            }
            HashDelResult::HashEmpty => {
                deleted += 1;
                // Delete the key from the store.
                store.del(key);
                if let Some(w) = wal { let _ = w.del(key); }
                if let Some(exp) = expiry { exp.remove(key); }
                current = Vec::new();
                break;
            }
            HashDelResult::FieldNotFound => {
                // Field not in hash — nothing to do.
            }
        }
    }

    // Persist if the hash still has fields.
    if !current.is_empty() {
        store.set(key, &current);
        if let Some(w) = wal { let _ = w.set(key, &current); }
    }

    RespEncoder::write_integer(out, deleted);
}

/// Handle `HGETALL key` — return all fields and values of a hash.
///
/// Returns a flat RESP array with alternating field / value elements
/// (matches Redis behaviour).
fn cmd_hgetall(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let current = get_current_value(key, store, expiry);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    let fields = hash::decode_hash(&current).unwrap_or_default();

    // *<2 * num_fields>\r\n alternating field / value bulk strings.
    out.push(b'*');
    write_usize_buf(out, fields.len() * 2);
    out.extend_from_slice(b"\r\n");
    for (f, v) in &fields {
        RespEncoder::write_bulk_string(out, f);
        RespEncoder::write_bulk_string(out, v);
    }
}

/// Handle `HEXISTS key field` — return 1 if field exists, 0 otherwise.
fn cmd_hexists(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let (Some(key), Some(field)) = (cmd.key(), cmd.arg(2)) else {
        return err_wrong_args(out);
    };

    let current = get_current_value(key, store, expiry);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    RespEncoder::write_integer(out, if hash::hash_exists(&current, field) { 1 } else { 0 });
}

/// Handle `HLEN key` — return the number of fields in the hash.
fn cmd_hlen(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let current = get_current_value(key, store, expiry);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    RespEncoder::write_integer(out, hash::hash_len(&current) as i64);
}

/// Handle `HKEYS key` — return all field names in the hash.
fn cmd_hkeys(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let current = get_current_value(key, store, expiry);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    let keys = hash::hash_keys(&current);
    out.push(b'*');
    write_usize_buf(out, keys.len());
    out.extend_from_slice(b"\r\n");
    for k in &keys {
        RespEncoder::write_bulk_string(out, k);
    }
}

/// Handle `HVALS key` — return all values in the hash.
fn cmd_hvals(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let current = get_current_value(key, store, expiry);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    let vals = hash::hash_values(&current);
    out.push(b'*');
    write_usize_buf(out, vals.len());
    out.extend_from_slice(b"\r\n");
    for v in &vals {
        RespEncoder::write_bulk_string(out, v);
    }
}

/// Handle `HMGET key field [field ...]` — return values for multiple fields.
///
/// Returns an array with one element per requested field; nil for missing.
fn cmd_hmget(cmd: &crate::core::resp::Command, store: &KvStore, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    if cmd.argc() < 3 {
        return err_wrong_args(out);
    }

    let current = get_current_value(key, store, expiry);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    let num_fields = cmd.argc() - 2;
    out.push(b'*');
    write_usize_buf(out, num_fields);
    out.extend_from_slice(b"\r\n");
    for i in 2..cmd.argc() {
        let field = cmd.arg(i).unwrap_or(b"");
        match hash::hash_get(&current, field) {
            Some(v) => RespEncoder::write_bulk_string(out, &v),
            None => RespEncoder::write_null(out),
        }
    }
}

/// Handle `HMSET key field value [field value ...]` — set multiple hash fields.
///
/// HMSET is deprecated in Redis in favour of HSET with multiple pairs, but
/// is kept for compatibility. Always returns OK.
fn cmd_hmset(cmd: &crate::core::resp::Command, store: &KvStore, wal: Option<&Wal>, expiry: Option<&ExpirationManager>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    if cmd.argc() < 4 || (cmd.argc() - 2) % 2 != 0 {
        return err_wrong_args(out);
    }

    let mut current = get_current_value(key, store, expiry);
    let mut i = 2usize;

    while i + 1 < cmd.argc() {
        let Some(field) = cmd.arg(i) else { break; };
        let Some(value) = cmd.arg(i + 1) else { break; };

        match hash::hash_set(&current, field, value) {
            Ok(new_data) => { current = new_data; }
            Err(hash::HashError::NotAHash) => {
                RespEncoder::write_error(out, WRONGTYPE_ERR);
                return;
            }
            Err(e) => {
                RespEncoder::write_error(out, &format!("ERR {}", e));
                return;
            }
        }
        i += 2;
    }

    if store.set(key, &current) {
        if let Some(w) = wal {
            if w.set(key, &current).is_err() {
                RespEncoder::write_error(out, "ERR WAL write failed");
                return;
            }
        }
        RespEncoder::write_simple_string(out, "OK");
    } else {
        RespEncoder::write_error(out, "ERR hash value too large for inline storage");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_creation() {
        let server = TokioServer::new();
        assert_eq!(server.port, DEFAULT_PORT);
    }

    #[test]
    fn test_find_resp_array_end() {
        let data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let end = find_resp_array_end(data).unwrap();
        assert_eq!(end, data.len());
    }

    #[test]
    fn test_parse_command_bounds() {
        let data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let (consumed, is_inline) = parse_command_bounds(data).unwrap();
        assert_eq!(consumed, data.len());
        assert!(!is_inline);
    }

    #[test]
    fn test_process_get_set_del() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b"+OK\r\n"));

        out.clear();
        process_command_into(
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b"$5\r\nvalue\r\n"));

        out.clear();
        process_command_into(
            b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b":1\r\n"));

        out.clear();
        process_command_into(
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b"$-1\r\n"));
    }

    #[test]
    fn test_process_incr() {
        let store = KvStore::new();
        let mut out = Vec::new();

        // Set counter to 10.
        process_command_into(
            b"*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$2\r\n10\r\n",
            &store, None, None, &mut out, false,
        );
        out.clear();

        // INCR
        process_command_into(
            b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b":11\r\n"));

        out.clear();
        // DECR
        process_command_into(
            b"*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b":10\r\n"));
    }

    #[test]
    fn test_process_mget_mset() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(
            b"*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b"+OK\r\n"));

        out.clear();
        process_command_into(
            b"*3\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nb\r\n",
            &store, None, None, &mut out, false,
        );
        // Should be an array of 2 elements.
        assert!(out.starts_with(b"*2\r\n"));
    }

    #[test]
    fn test_process_exists() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
            &store, None, None, &mut out, false,
        );
        out.clear();

        process_command_into(
            b"*2\r\n$6\r\nEXISTS\r\n$1\r\nk\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b":1\r\n"));
    }

    #[test]
    fn test_process_append_strlen() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(
            b"*3\r\n$3\r\nSET\r\n$1\r\ns\r\n$5\r\nhello\r\n",
            &store, None, None, &mut out, false,
        );
        out.clear();

        process_command_into(
            b"*3\r\n$6\r\nAPPEND\r\n$1\r\ns\r\n$6\r\n world\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b":11\r\n"));

        out.clear();
        process_command_into(
            b"*2\r\n$6\r\nSTRLEN\r\n$1\r\ns\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b":11\r\n"));
    }

    #[test]
    fn test_process_getrange_setrange() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(
            b"*3\r\n$3\r\nSET\r\n$1\r\ns\r\n$11\r\nHello World\r\n",
            &store, None, None, &mut out, false,
        );
        out.clear();

        // GETRANGE 0 4 → "Hello"
        process_command_into(
            b"*4\r\n$8\r\nGETRANGE\r\n$1\r\ns\r\n$1\r\n0\r\n$1\r\n4\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.windows(5).any(|w| w == b"Hello"));

        out.clear();
        // SETRANGE 6 Redis → "Hello Redis"
        process_command_into(
            b"*4\r\n$8\r\nSETRANGE\r\n$1\r\ns\r\n$1\r\n6\r\n$5\r\nRedis\r\n",
            &store, None, None, &mut out, false,
        );
        assert!(out.starts_with(b":11\r\n"));
    }

    #[test]
    fn test_process_ping_pong() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"*1\r\n$4\r\nPING\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b"+PONG\r\n"));

        out.clear();
        process_command_into(b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n", &store, None, None, &mut out, false);
        assert!(out.windows(5).any(|w| w == b"hello"));
    }

    #[test]
    fn test_process_echo() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", &store, None, None, &mut out, false);
        assert!(out.windows(5).any(|w| w == b"hello"));
    }

    #[test]
    fn test_process_dbsize() {
        let store = KvStore::new();
        store.set(b"a", b"1");
        store.set(b"b", b"2");
        let mut out = Vec::new();

        process_command_into(b"*1\r\n$6\r\nDBSIZE\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b":2\r\n"));
    }

    #[test]
    fn test_process_info() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"*1\r\n$4\r\nINFO\r\n", &store, None, None, &mut out, false);
        assert!(out.windows(6).any(|w| w == b"Server"));
    }

    #[test]
    fn test_process_unknown_command() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"*2\r\n$5\r\nBOGUS\r\n$1\r\nx\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b"-ERR unknown command"));
    }

    #[test]
    fn test_process_inline_set_get() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"SET mykey myvalue\r\n", &store, None, None, &mut out, true);
        assert!(out.starts_with(b"+OK\r\n"));

        out.clear();
        process_command_into(b"GET mykey\r\n", &store, None, None, &mut out, true);
        assert!(out.windows(7).any(|w| w == b"myvalue"));
    }

    #[test]
    fn test_process_incrby_decrby() {
        let store = KvStore::new();
        let mut out = Vec::new();

        // SET counter 10
        process_command_into(b"*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$2\r\n10\r\n", &store, None, None, &mut out, false);
        out.clear();

        // INCRBY counter 5
        process_command_into(b"*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$1\r\n5\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b":15\r\n"));

        out.clear();

        // DECRBY counter 3
        process_command_into(b"*3\r\n$6\r\nDECRBY\r\n$7\r\ncounter\r\n$1\r\n3\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b":12\r\n"));
    }

    #[test]
    fn test_process_expire_ttl_persist() {
        use crate::core::expiration::ExpirationManager;
        let store = Arc::new(KvStore::new());
        let exp = Arc::new(ExpirationManager::new(Arc::clone(&store)));
        let mut out = Vec::new();

        // SET session abc123
        process_command_into(
            b"*3\r\n$3\r\nSET\r\n$7\r\nsession\r\n$6\r\nabc123\r\n",
            &store, None, Some(&exp), &mut out, false,
        );
        out.clear();

        // EXPIRE session 3600
        process_command_into(
            b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nsession\r\n$4\r\n3600\r\n",
            &store, None, Some(&exp), &mut out, false,
        );
        assert!(out.starts_with(b":1\r\n"));

        out.clear();

        // TTL session
        process_command_into(
            b"*2\r\n$3\r\nTTL\r\n$7\r\nsession\r\n",
            &store, None, Some(&exp), &mut out, false,
        );
        // Should be a number close to 3600.
        let ttl_str = String::from_utf8_lossy(&out);
        assert!(ttl_str.contains("3600") || ttl_str.contains("3599"));

        out.clear();

        // PERSIST session
        process_command_into(
            b"*2\r\n$7\r\nPERSIST\r\n$7\r\nsession\r\n",
            &store, None, Some(&exp), &mut out, false,
        );
        assert!(out.starts_with(b":1\r\n"));
    }

    #[test]
    fn test_parse_command_bounds_inline() {
        let data = b"GET mykey\r\n";
        let (consumed, is_inline) = parse_command_bounds(data).unwrap();
        assert_eq!(consumed, data.len());
        assert!(is_inline);
    }

    #[test]
    fn test_parse_command_bounds_empty() {
        assert!(parse_command_bounds(b"").is_none());
    }

    // ----- Hash command integration tests -----

    #[test]
    fn test_process_hset_hget() {
        let store = KvStore::new();
        let mut out = Vec::new();

        // HSET myhash f1 v1
        let cmd = b"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$2\r\nf1\r\n$2\r\nv1\r\n";
        process_command_into(cmd, &store, None, None, &mut out, false);
        assert!(out.starts_with(b":1\r\n")); // new field

        out.clear();
        // HGET myhash f1
        process_command_into(b"*3\r\n$4\r\nHGET\r\n$6\r\nmyhash\r\n$2\r\nf1\r\n", &store, None, None, &mut out, false);
        assert!(out.windows(2).any(|w| w == b"v1"));
    }

    #[test]
    fn test_process_hset_update() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\n1\r\n", &store, None, None, &mut out, false);
        out.clear();
        // Update existing field
        process_command_into(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\n2\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b":0\r\n")); // updated, not new
    }

    #[test]
    fn test_process_hdel() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nv\r\n", &store, None, None, &mut out, false);
        out.clear();
        process_command_into(b"*3\r\n$4\r\nHDEL\r\n$1\r\nh\r\n$1\r\nf\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b":1\r\n"));
    }

    #[test]
    fn test_process_hgetall() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\na\r\n$1\r\n1\r\n", &store, None, None, &mut out, false);
        out.clear();
        process_command_into(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nb\r\n$1\r\n2\r\n", &store, None, None, &mut out, false);
        out.clear();
        process_command_into(b"*2\r\n$7\r\nHGETALL\r\n$1\r\nh\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b"*4\r\n")); // 2 fields × 2 = 4 elements
    }

    #[test]
    fn test_process_hash_wrongtype() {
        let store = KvStore::new();
        let mut out = Vec::new();

        // SET key as string
        process_command_into(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", &store, None, None, &mut out, false);
        out.clear();
        // HGET on string key → WRONGTYPE
        process_command_into(b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$1\r\nx\r\n", &store, None, None, &mut out, false);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("WRONGTYPE"), "expected WRONGTYPE, got: {:?}", out);
    }

    #[test]
    fn test_process_hlen_hexists() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nv\r\n", &store, None, None, &mut out, false);
        out.clear();

        // HLEN h → 1
        process_command_into(b"*2\r\n$4\r\nHLEN\r\n$1\r\nh\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b":1\r\n"));

        out.clear();
        // HEXISTS h f → 1
        process_command_into(b"*3\r\n$7\r\nHEXISTS\r\n$1\r\nh\r\n$1\r\nf\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b":1\r\n"));

        out.clear();
        // HEXISTS h missing → 0
        process_command_into(b"*3\r\n$7\r\nHEXISTS\r\n$1\r\nh\r\n$7\r\nmissing\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b":0\r\n"));
    }

    #[test]
    fn test_process_hkeys_hvals() {
        let store = KvStore::new();
        let mut out = Vec::new();

        process_command_into(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\na\r\n$1\r\n1\r\n", &store, None, None, &mut out, false);
        out.clear();
        process_command_into(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nb\r\n$1\r\n2\r\n", &store, None, None, &mut out, false);
        out.clear();

        // HKEYS h → *2
        process_command_into(b"*2\r\n$5\r\nHKEYS\r\n$1\r\nh\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b"*2\r\n"));

        out.clear();
        // HVALS h → *2
        process_command_into(b"*2\r\n$5\r\nHVALS\r\n$1\r\nh\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b"*2\r\n"));
    }

    #[test]
    fn test_process_hmset_hmget() {
        let store = KvStore::new();
        let mut out = Vec::new();

        // HMSET h f1 v1 f2 v2
        process_command_into(b"*6\r\n$5\r\nHMSET\r\n$1\r\nh\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b"+OK\r\n"));

        out.clear();
        // HMGET h f1 f2
        process_command_into(b"*4\r\n$5\r\nHMGET\r\n$1\r\nh\r\n$2\r\nf1\r\n$2\r\nf2\r\n", &store, None, None, &mut out, false);
        assert!(out.starts_with(b"*2\r\n"));
    }

    #[test]
    fn test_process_hset_on_string_wrongtype() {
        let store = KvStore::new();
        let mut out = Vec::new();

        // SET key as string
        process_command_into(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", &store, None, None, &mut out, false);
        out.clear();
        // HSET on string key → WRONGTYPE
        process_command_into(b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$1\r\nf\r\n$1\r\nv\r\n", &store, None, None, &mut out, false);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("WRONGTYPE"), "expected WRONGTYPE, got: {:?}", out);
    }
}
