//! Optimized Tokio TCP server.
//!
//! Performance techniques:
//! - Reusable read/write buffers (zero per-request allocation).
//! - Batched writes — all responses for a single `read()` are flushed at
//!   once, reducing syscalls.
//! - Pipeline support — multiple commands in a single TCP read are all
//!   processed before a single `write_all`.
//!
//! The server accepts optional [`Wal`], [`ExpirationManager`] and
//! [`ListManager`] handles so that every mutation can be persisted and
//! every GET can be checked for expiration.

use crate::core::hash::{self, HashDelResult, WRONGTYPE_ERR};
use crate::core::kv::{IncrError, KvStore};
use crate::core::expiration::ExpirationManager;
use crate::core::list::ListManager;
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
// Server context — shared by all command handlers
// ---------------------------------------------------------------------------

/// Bundles the shared components that command handlers need access to.
///
/// Using a single context struct avoids parameter explosion when new
/// components (lists, sets, sorted sets, …) are added over time.
pub struct ServerContext<'a> {
    pub store: &'a KvStore,
    pub wal: Option<&'a Wal>,
    pub expiry: Option<&'a ExpirationManager>,
    pub lists: Option<&'a ListManager>,
}

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
    /// Optional list manager.
    lists: Option<Arc<ListManager>>,
    /// Port to listen on.
    pub port: u16,
}

impl TokioServer {
    /// Create a server with default settings (port 6379, no WAL, no TTL, no lists).
    pub fn new() -> Self {
        Self {
            store: Arc::new(KvStore::new()),
            wal: None,
            expiry: None,
            lists: None,
            port: DEFAULT_PORT,
        }
    }

    /// Create a server that listens on *port*.
    pub fn with_port(port: u16) -> Self {
        Self {
            store: Arc::new(KvStore::new()),
            wal: None,
            expiry: None,
            lists: None,
            port,
        }
    }

    /// Create a fully configured server.
    pub fn with_components(
        port: u16,
        store: Arc<KvStore>,
        wal: Option<Arc<Wal>>,
        expiry: Option<Arc<ExpirationManager>>,
        lists: Option<Arc<ListManager>>,
    ) -> Self {
        Self { store, wal, expiry, lists, port }
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
        let lists = self.lists.clone();

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
            let lists = lists.clone();

            tokio::spawn(async move {
                handle_client(socket, store, wal, expiry, lists, peer_addr).await;
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
    lists: Option<Arc<ListManager>>,
    addr: std::net::SocketAddr,
) {
    // Reusable buffers — allocated once, cleared between iterations.
    let mut read_buf = vec![0u8; MAX_BUFFER_SIZE];
    let mut leftover: Vec<u8> = Vec::with_capacity(4096);
    let mut resp_buf = Vec::with_capacity(INITIAL_RESPONSE_SIZE);

    let ctx = ServerContext {
        store: &store,
        wal: wal.as_deref(),
        expiry: expiry.as_deref(),
        lists: lists.as_deref(),
    };

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
                    should_close = process_command_into(cmd_data, &ctx, &mut resp_buf, is_inline);
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
    ctx: &ServerContext,
    out: &mut Vec<u8>,
    is_inline: bool,
) -> bool {
    // ---- inline command handling ----
    if is_inline {
        return handle_inline(data, ctx, out);
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

    dispatch_command(&command, ctx, out)
}

/// Dispatch an already-parsed [`Command`] and append the response.
///
/// Returns `true` if the connection should be closed (e.g. QUIT command).
fn dispatch_command(
    cmd: &crate::core::resp::Command,
    ctx: &ServerContext,
    out: &mut Vec<u8>,
) -> bool {
    match cmd.name.as_str() {
        // ----- Core -----
        "GET" => { cmd_get(cmd, ctx, out); false }
        "SET" => { cmd_set(cmd, ctx, out); false }
        "DEL" => { cmd_del(cmd, ctx, out); false }
        "PING" => { cmd_ping(cmd, out); false }
        "ECHO" => { cmd_echo(cmd, out); false }
        "COMMAND" => {
            // redis-py sends COMMAND DOCS on connect — return empty array.
            RespEncoder::write_array(out, &[]);
            false
        }
        "INFO"  => { cmd_info(ctx, out); false }
        "DBSIZE" => { RespEncoder::write_integer(out, ctx.store.len() as i64); false }
        "QUIT"  => { out.extend_from_slice(b"+OK\r\n"); true }

        // ----- String operations -----
        "INCR"    => { cmd_incr(cmd, ctx, out, 1); false }
        "INCRBY"  => { cmd_incrby(cmd, ctx, out); false }
        "DECR"    => { cmd_incr(cmd, ctx, out, -1); false }
        "DECRBY"  => { cmd_decrby(cmd, ctx, out); false }
        "APPEND"  => { cmd_append(cmd, ctx, out); false }
        "STRLEN"  => { cmd_strlen(cmd, ctx, out); false }
        "GETRANGE"  => { cmd_getrange(cmd, ctx, out); false }
        "SETRANGE"  => { cmd_setrange(cmd, ctx, out); false }
        "MGET"    => { cmd_mget(cmd, ctx, out); false }
        "MSET"    => { cmd_mset(cmd, ctx, out); false }
        "EXISTS"  => { cmd_exists(cmd, ctx, out); false }

        // ----- Expiration -----
        "EXPIRE"  => { cmd_expire(cmd, ctx, out); false }
        "TTL"     => { cmd_ttl(cmd, ctx, out); false }
        "PTTL"    => { cmd_pttl(cmd, ctx, out); false }
        "PERSIST" => { cmd_persist(cmd, ctx, out); false }

        // ----- Hash -----
        "HSET"    => { cmd_hset(cmd, ctx, out); false }
        "HGET"    => { cmd_hget(cmd, ctx, out); false }
        "HDEL"    => { cmd_hdel(cmd, ctx, out); false }
        "HGETALL" => { cmd_hgetall(cmd, ctx, out); false }
        "HEXISTS" => { cmd_hexists(cmd, ctx, out); false }
        "HLEN"    => { cmd_hlen(cmd, ctx, out); false }
        "HKEYS"   => { cmd_hkeys(cmd, ctx, out); false }
        "HVALS"   => { cmd_hvals(cmd, ctx, out); false }
        "HMGET"   => { cmd_hmget(cmd, ctx, out); false }
        "HMSET"   => { cmd_hmset(cmd, ctx, out); false }

        // ----- List -----
        "LPUSH"  => { cmd_lpush(cmd, ctx, out); false }
        "RPUSH"  => { cmd_rpush(cmd, ctx, out); false }
        "LPOP"   => { cmd_lpop(cmd, ctx, out); false }
        "RPOP"   => { cmd_rpop(cmd, ctx, out); false }
        "LRANGE" => { cmd_lrange(cmd, ctx, out); false }
        "LLEN"   => { cmd_llen(cmd, ctx, out); false }
        "LINDEX" => { cmd_lindex(cmd, ctx, out); false }
        "LREM"   => { cmd_lrem(cmd, ctx, out); false }
        "LTRIM"  => { cmd_ltrim(cmd, ctx, out); false }
        "LSET"   => { cmd_lset(cmd, ctx, out); false }

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
    ctx: &ServerContext,
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
    dispatch_command(&cmd, ctx, out)
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

/// Handle `GET key` — return the value or null bulk string.
///
/// Returns a WRONGTYPE error if the key holds a hash or list.
fn cmd_get(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Lazy expiration check.
    if let Some(exp) = ctx.expiry {
        if exp.is_expired(key) {
            exp.purge_if_expired(key);
            RespEncoder::write_null(out);
            return;
        }
    }

    match ctx.store.get(key) {
        Some(v) if hash::is_hash_value(&v) || ListManager::is_list_value(&v) => {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
        }
        Some(v) => RespEncoder::write_bulk_string(out, &v),
        None => RespEncoder::write_null(out),
    }
}

/// Handle `SET key value` — insert/update and persist to WAL.
fn cmd_set(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let (Some(key), Some(value)) = (cmd.key(), cmd.value()) else { return err_wrong_args(out); };

    // Parse optional flags: NX, XX, EX, PX
    let mut nx = false;
    let mut xx = false;
    let mut ex_secs: Option<u64> = None;
    let mut px_ms: Option<u64> = None;

    let mut i = 3;
    while i < cmd.argc() {
        let flag = cmd.arg(i).unwrap_or(b"");
        match flag {
            b"NX" => { nx = true; }
            b"XX" => { xx = true; }
            b"EX" => {
                i += 1;
                ex_secs = cmd.arg(i).and_then(|v| std::str::from_utf8(v).ok()).and_then(|s| s.parse().ok());
            }
            b"PX" => {
                i += 1;
                px_ms = cmd.arg(i).and_then(|v| std::str::from_utf8(v).ok()).and_then(|s| s.parse().ok());
            }
            other => {
                let msg = format!("ERR syntax error - unknown option '{}'",
                    std::str::from_utf8(other).unwrap_or("?"));
                RespEncoder::write_error(out, &msg);
                return;
            }
        }
        i += 1;
    }

    let exists = ctx.store.exists(key);

    // NX: only set if key does NOT exist
    if nx && exists {
        out.extend_from_slice(b"$-1\r\n");
        return;
    }
    // XX: only set if key DOES exist
    if xx && !exists {
        out.extend_from_slice(b"$-1\r\n");
        return;
    }

    // If the key held a list, clean up the list data.
    if let Some(lists) = ctx.lists {
        if ListManager::is_list_value(&ctx.store.get(key).unwrap_or_default()) {
            lists.remove_key(key);
        }
    }

    // Remove old TTL if overwriting.
    if let Some(exp) = ctx.expiry {
        exp.remove(key);
    }

    if ctx.store.set(key, value) {
        if let Some(w) = ctx.wal {
            if w.set(key, value).is_err() {
                out.extend_from_slice(b"-ERR WAL write failed\r\n");
                return;
            }
        }

        // Apply EX/PX TTL if specified.
        if let Some(exp) = ctx.expiry {
            let ttl = if let Some(secs) = ex_secs {
                Some(std::time::Duration::from_secs(secs))
            } else if let Some(ms) = px_ms {
                Some(std::time::Duration::from_millis(ms))
            } else {
                None
            };
            if let Some(dur) = ttl {
                if let Some(deadline_ms) = exp.expire_with_deadline(key, dur) {
                    if let Some(w) = ctx.wal {
                        let _ = w.expire(key, deadline_ms);
                    }
                }
            }
        }

        out.extend_from_slice(b"+OK\r\n");
    } else {
        out.extend_from_slice(b"-ERR value too large\r\n");
    }
}

/// Handle `DEL key [key ...]` — remove one or more keys, return count of deleted.
fn cmd_del(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    if cmd.argc() < 2 {
        return err_wrong_args(out);
    }
    let mut deleted: i64 = 0;
    for i in 1..cmd.argc() {
        let Some(key) = cmd.arg(i) else { continue };
        if ctx.store.del(key) {
            deleted += 1;
            if let Some(w) = ctx.wal { let _ = w.del(key); }
            if let Some(exp) = ctx.expiry { exp.remove(key); }
            if let Some(lists) = ctx.lists { lists.remove_key(key); }
        }
    }
    RespEncoder::write_integer(out, deleted);
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
fn cmd_info(ctx: &ServerContext, out: &mut Vec<u8>) {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(b"# Server\r\nfastkv_version:");
    buf.extend_from_slice(crate::VERSION.as_bytes());
    buf.extend_from_slice(b"\r\n# Memory\r\nused_memory_approx:");
    write_usize_buf(&mut buf, ctx.store.len() * 200);
    buf.extend_from_slice(b"\r\n# Keys\r\ndb_size:");
    write_usize_buf(&mut buf, ctx.store.len());
    buf.extend_from_slice(b"\r\n");
    RespEncoder::write_bulk_string(out, &buf);
}

// ---------------------------------------------------------------------------
// String operations
// ---------------------------------------------------------------------------

/// Handle `INCR`/`DECR` — atomically increment/decrement by *delta*.
fn cmd_incr(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>, delta: i64) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check type: INCR only works on plain string keys.
    if let Some(val) = ctx.store.get(key) {
        if hash::is_hash_value(&val) || ListManager::is_list_value(&val) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    match ctx.store.incr(key, delta) {
        Ok(new_val) => {
            if let Some(w) = ctx.wal {
                if w.set(key, new_val.to_string().as_bytes()).is_err() {
                    RespEncoder::write_error(out, "ERR WAL write failed");
                    return;
                }
            }
            RespEncoder::write_integer(out, new_val);
        }
        Err(IncrError::KeyNotFound) => {
            // Redis semantics: INCR on missing key → initialise to 0, then apply delta.
            let new_val = delta;
            ctx.store.set(key, new_val.to_string().as_bytes());
            if let Some(w) = ctx.wal {
                if w.set(key, new_val.to_string().as_bytes()).is_err() {
                    RespEncoder::write_error(out, "ERR WAL write failed");
                    return;
                }
            }
            RespEncoder::write_integer(out, new_val);
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
fn cmd_incrby(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let (Some(_key), Some(delta_bytes)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    let delta: i64 = match std::str::from_utf8(delta_bytes).ok().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => { RespEncoder::write_error(out, "ERR value is not an integer"); return; }
    };

    cmd_incr(cmd, ctx, out, delta);
}

/// Handle `DECRBY key delta` — decrement by a specified (positive) amount.
fn cmd_decrby(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let (Some(_key), Some(delta_bytes)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    let delta: i64 = match std::str::from_utf8(delta_bytes).ok().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => { RespEncoder::write_error(out, "ERR value is not an integer"); return; }
    };

    cmd_incr(cmd, ctx, out, -delta);
}

/// Handle `APPEND key suffix` — append to string value, persist result.
fn cmd_append(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let (Some(key), Some(suffix)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    if let Some(current) = ctx.store.get(key) {
        if hash::is_hash_value(&current) || ListManager::is_list_value(&current) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    match ctx.store.append(key, suffix) {
        Some(new_len) => {
            if let Some(w) = ctx.wal {
                if let Some(val) = ctx.store.get(key) {
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
fn cmd_strlen(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    if let Some(exp) = ctx.expiry {
        if exp.is_expired(key) {
            exp.purge_if_expired(key);
            out.extend_from_slice(b":0\r\n");
            return;
        }
    }

    if let Some(v) = ctx.store.get(key) {
        if hash::is_hash_value(&v) || ListManager::is_list_value(&v) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    RespEncoder::write_integer(out, ctx.store.strlen(key) as i64);
}

/// Handle `GETRANGE key start end` — return substring (inclusive bounds).
fn cmd_getrange(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let (Some(key), Some(start_s), Some(end_s)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    if let Some(exp) = ctx.expiry {
        if exp.is_expired(key) {
            exp.purge_if_expired(key);
            out.extend_from_slice(b"$0\r\n\r\n");
            return;
        }
    }

    if let Some(v) = ctx.store.get(key) {
        if hash::is_hash_value(&v) || ListManager::is_list_value(&v) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    let start: i64 = std::str::from_utf8(start_s).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let end: i64 = std::str::from_utf8(end_s).ok().and_then(|s| s.parse().ok()).unwrap_or(-1);

    let value = ctx.store.getrange(key, start, end).unwrap_or_default();
    RespEncoder::write_bulk_string(out, &value);
}

/// Handle `SETRANGE key offset value` — overwrite part of a string, persist result.
fn cmd_setrange(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let (Some(key), Some(offset_s), Some(replacement)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    if let Some(v) = ctx.store.get(key) {
        if hash::is_hash_value(&v) || ListManager::is_list_value(&v) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    let offset: usize = match std::str::from_utf8(offset_s).ok().and_then(|s| s.parse().ok()) {
        Some(o) => o,
        None => { RespEncoder::write_error(out, "ERR offset is not an integer"); return; }
    };

    match ctx.store.setrange(key, offset, replacement) {
        Some(new_len) => {
            if let Some(w) = ctx.wal {
                if let Some(val) = ctx.store.get(key) {
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
fn cmd_mget(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    if cmd.argc() < 2 {
        return err_wrong_args(out);
    }
    let keys: Vec<&[u8]> = (1..cmd.argc()).filter_map(|i| cmd.arg(i)).collect();
    let values = ctx.store.mget(&keys);

    out.push(b'*');
    write_usize_buf(out, values.len());
    out.extend_from_slice(b"\r\n");
    for val in &values {
        match val {
            Some(v) if hash::is_hash_value(v) || ListManager::is_list_value(v) => {
                // In Redis MGET, WRONGTYPE is not returned per-element; nil is returned.
                RespEncoder::write_null(out);
            }
            Some(v) => RespEncoder::write_bulk_string(out, v),
            None => RespEncoder::write_null(out),
        }
    }
}

/// Handle `MSET k1 v1 k2 v2 ...` — set multiple key-value pairs atomically.
fn cmd_mset(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let argc = cmd.argc();
    if argc < 3 || argc % 2 == 0 {
        return err_wrong_args(out);
    }

    let pairs: Vec<(&[u8], &[u8])> = (1..argc)
        .step_by(2)
        .filter_map(|i| Some((cmd.arg(i)?, cmd.arg(i + 1)?)))
        .collect();

    if ctx.store.mset(&pairs) {
        if let Some(w) = ctx.wal {
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

/// Handle `EXISTS key [key ...]` — return count of existing keys.
fn cmd_exists(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    if cmd.argc() < 2 {
        return err_wrong_args(out);
    }
    let mut count: i64 = 0;
    for i in 1..cmd.argc() {
        let Some(key) = cmd.arg(i) else { continue };
        if let Some(exp) = ctx.expiry {
            if exp.is_expired(key) {
                exp.purge_if_expired(key);
                continue;
            }
        }
        if ctx.store.exists(key) {
            count += 1;
        }
    }
    RespEncoder::write_integer(out, count);
}

// ---------------------------------------------------------------------------
// Expiration commands
// ---------------------------------------------------------------------------

/// Handle `EXPIRE key seconds` — set a TTL in seconds and persist to WAL.
fn cmd_expire(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let (Some(key), Some(secs_s)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };
    let Some(exp) = ctx.expiry else {
        out.extend_from_slice(b"-ERR expiration not enabled\r\n");
        return;
    };

    let secs: i64 = match std::str::from_utf8(secs_s).ok().and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => { out.extend_from_slice(b"-ERR value is not an integer\r\n"); return; }
    };

    // Negative or zero seconds → delete the key immediately (Redis semantics).
    if secs <= 0 {
        ctx.store.del(key);
        if let Some(exp) = ctx.expiry { exp.remove(key); }
        if let Some(lists) = ctx.lists { lists.remove_key(key); }
        if let Some(w) = ctx.wal { let _ = w.del(key); }
        RespEncoder::write_integer(out, 1);
        return;
    }

    let ttl = std::time::Duration::from_secs(secs as u64);

    match exp.expire_with_deadline(key, ttl) {
        Some(deadline_ms) => {
            // Persist to WAL.
            if let Some(w) = ctx.wal {
                if w.expire(key, deadline_ms).is_err() {
                    RespEncoder::write_error(out, "ERR WAL write failed");
                    return;
                }
            }
            out.extend_from_slice(b":1\r\n");
        }
        None => {
            out.extend_from_slice(b":0\r\n");
        }
    }
}

/// Handle `TTL key` — return remaining TTL in seconds, or -2 if no TTL.
fn cmd_ttl(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Redis semantics:
    //   -2 = key does not exist
    //   -1 = key exists but has no TTL
    //   >=0 = remaining seconds
    if !ctx.store.exists(key) {
        RespEncoder::write_integer(out, -2);
        return;
    }

    let Some(exp) = ctx.expiry else {
        RespEncoder::write_integer(out, -1);
        return;
    };

    match exp.ttl(key) {
        Some(dur) => {
            let secs = dur.as_secs();
            RespEncoder::write_integer(out, secs as i64);
        }
        None => RespEncoder::write_integer(out, -1),
    }
}

/// Handle `PTTL key` — return remaining TTL in milliseconds, or -2 if no TTL.
fn cmd_pttl(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    if !ctx.store.exists(key) {
        RespEncoder::write_integer(out, -2);
        return;
    }

    let Some(exp) = ctx.expiry else {
        RespEncoder::write_integer(out, -1);
        return;
    };

    match exp.ttl(key) {
        Some(dur) => {
            let ms = dur.as_millis() as i64;
            RespEncoder::write_integer(out, ms);
        }
        None => RespEncoder::write_integer(out, -1),
    }
}

/// Handle `PERSIST key` — remove the TTL, making the key persistent.
fn cmd_persist(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    let Some(exp) = ctx.expiry else {
        out.extend_from_slice(b":0\r\n");
        return;
    };

    out.extend_from_slice(if exp.persist(key) { b":1\r\n" } else { b":0\r\n" });
}

// ---------------------------------------------------------------------------
// Hash commands
// ---------------------------------------------------------------------------

/// Retrieve the current value for a key, respecting expiration.
#[inline]
fn get_current_value(key: &[u8], ctx: &ServerContext) -> Vec<u8> {
    if let Some(exp) = ctx.expiry {
        if exp.is_expired(key) {
            exp.purge_if_expired(key);
            return Vec::new();
        }
    }
    ctx.store.get(key).unwrap_or_default()
}

/// Handle `HSET key field value [field value ...]`.
fn cmd_hset(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    if cmd.argc() < 4 || (cmd.argc() - 2) % 2 != 0 {
        return err_wrong_args(out);
    }

    let mut current = get_current_value(key, ctx);
    let mut new_fields = 0i64;

    let mut i = 2usize;
    while i + 1 < cmd.argc() {
        let Some(field) = cmd.arg(i) else { break; };
        let Some(value) = cmd.arg(i + 1) else { break; };

        let was_new = hash::hash_get(&current, field).is_none();

        match hash::hash_set(&current, field, value) {
            Ok(new_data) => {
                current = new_data;
                if was_new { new_fields += 1; }
            }
            Err(hash::HashError::NotAHash) => {
                // Also reject if it's a list.
                if ListManager::is_list_value(&current) {
                    RespEncoder::write_error(out, WRONGTYPE_ERR);
                    return;
                }
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

    if ctx.store.set(key, &current) {
        if let Some(w) = ctx.wal {
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

/// Handle `HGET key field`.
fn cmd_hget(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let (Some(key), Some(field)) = (cmd.key(), cmd.arg(2)) else {
        return err_wrong_args(out);
    };

    let current = get_current_value(key, ctx);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    match hash::hash_get(&current, field) {
        Some(v) => RespEncoder::write_bulk_string(out, &v),
        None => RespEncoder::write_null(out),
    }
}

/// Handle `HDEL key field [field ...]`.
fn cmd_hdel(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    if cmd.argc() < 3 {
        return err_wrong_args(out);
    }

    let mut current = get_current_value(key, ctx);

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
                ctx.store.del(key);
                if let Some(w) = ctx.wal { let _ = w.del(key); }
                if let Some(exp) = ctx.expiry { exp.remove(key); }
                current = Vec::new();
                break;
            }
            HashDelResult::FieldNotFound => {}
        }
    }

    if !current.is_empty() {
        ctx.store.set(key, &current);
        if let Some(w) = ctx.wal { let _ = w.set(key, &current); }
    }

    RespEncoder::write_integer(out, deleted);
}

/// Handle `HGETALL key`.
fn cmd_hgetall(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let current = get_current_value(key, ctx);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    let fields = hash::decode_hash(&current).unwrap_or_default();

    out.push(b'*');
    write_usize_buf(out, fields.len() * 2);
    out.extend_from_slice(b"\r\n");
    for (f, v) in &fields {
        RespEncoder::write_bulk_string(out, f);
        RespEncoder::write_bulk_string(out, v);
    }
}

/// Handle `HEXISTS key field`.
fn cmd_hexists(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let (Some(key), Some(field)) = (cmd.key(), cmd.arg(2)) else {
        return err_wrong_args(out);
    };

    let current = get_current_value(key, ctx);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    RespEncoder::write_integer(out, if hash::hash_exists(&current, field) { 1 } else { 0 });
}

/// Handle `HLEN key`.
fn cmd_hlen(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let current = get_current_value(key, ctx);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    RespEncoder::write_integer(out, hash::hash_len(&current) as i64);
}

/// Handle `HKEYS key`.
fn cmd_hkeys(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let current = get_current_value(key, ctx);

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

/// Handle `HVALS key`.
fn cmd_hvals(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let current = get_current_value(key, ctx);

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

/// Handle `HMGET key field [field ...]`.
fn cmd_hmget(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    if cmd.argc() < 3 {
        return err_wrong_args(out);
    }

    let current = get_current_value(key, ctx);

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

/// Handle `HMSET key field value [field value ...]`.
fn cmd_hmset(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    if cmd.argc() < 4 || (cmd.argc() - 2) % 2 != 0 {
        return err_wrong_args(out);
    }

    let mut current = get_current_value(key, ctx);
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

    if ctx.store.set(key, &current) {
        if let Some(w) = ctx.wal {
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
// List commands
// ---------------------------------------------------------------------------

/// Handle `LPUSH key element [element ...]`.
fn cmd_lpush(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    if cmd.argc() < 3 {
        return err_wrong_args(out);
    }

    let elements: Vec<&[u8]> = (2..cmd.argc()).filter_map(|i| cmd.arg(i)).collect();

    match lists.lpush(key, &elements) {
        Ok(len) => RespEncoder::write_integer(out, len as i64),
        Err(_) => RespEncoder::write_error(out, WRONGTYPE_ERR),
    }
}

/// Handle `RPUSH key element [element ...]`.
fn cmd_rpush(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let Some(key) = cmd.key() else { return err_wrong_args(out); };
    if cmd.argc() < 3 {
        return err_wrong_args(out);
    }

    let elements: Vec<&[u8]> = (2..cmd.argc()).filter_map(|i| cmd.arg(i)).collect();

    match lists.rpush(key, &elements) {
        Ok(len) => RespEncoder::write_integer(out, len as i64),
        Err(_) => RespEncoder::write_error(out, WRONGTYPE_ERR),
    }
}

/// Handle `LPOP key [count]`.
fn cmd_lpop(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check type: LPOP only works on list keys (or missing keys → nil).
    if let Some(val) = ctx.store.get(key) {
        if !ListManager::is_list_value(&val) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    let count: usize = cmd.arg(2)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let elements = lists.lpop(key, count);

    if cmd.argc() >= 3 && count > 1 {
        // Multi-element reply.
        out.push(b'*');
        write_usize_buf(out, elements.len());
        out.extend_from_slice(b"\r\n");
        for elem in &elements {
            RespEncoder::write_bulk_string(out, elem);
        }
    } else {
        match elements.into_iter().next() {
            Some(elem) => RespEncoder::write_bulk_string(out, &elem),
            None => RespEncoder::write_null(out),
        }
    }
}

/// Handle `RPOP key [count]`.
fn cmd_rpop(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check type: RPOP only works on list keys (or missing keys → nil).
    if let Some(val) = ctx.store.get(key) {
        if !ListManager::is_list_value(&val) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    let count: usize = cmd.arg(2)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let elements = lists.rpop(key, count);

    if cmd.argc() >= 3 && count > 1 {
        out.push(b'*');
        write_usize_buf(out, elements.len());
        out.extend_from_slice(b"\r\n");
        for elem in &elements {
            RespEncoder::write_bulk_string(out, elem);
        }
    } else {
        match elements.into_iter().next() {
            Some(elem) => RespEncoder::write_bulk_string(out, &elem),
            None => RespEncoder::write_null(out),
        }
    }
}

/// Handle `LRANGE key start stop`.
fn cmd_lrange(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let (Some(key), Some(start_s), Some(stop_s)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    let start: i64 = std::str::from_utf8(start_s).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let stop: i64 = std::str::from_utf8(stop_s).ok().and_then(|s| s.parse().ok()).unwrap_or(-1);

    let elements = lists.lrange(key, start, stop);

    out.push(b'*');
    write_usize_buf(out, elements.len());
    out.extend_from_slice(b"\r\n");
    for elem in &elements {
        RespEncoder::write_bulk_string(out, elem);
    }
}

/// Handle `LLEN key`.
fn cmd_llen(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check type: LLEN only works on list keys (or missing keys → 0).
    if let Some(val) = ctx.store.get(key) {
        if !ListManager::is_list_value(&val) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    RespEncoder::write_integer(out, lists.llen(key) as i64);
}

/// Handle `LINDEX key index`.
fn cmd_lindex(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let (Some(key), Some(idx_s)) = (cmd.key(), cmd.arg(2)) else {
        return err_wrong_args(out);
    };

    let index: i64 = std::str::from_utf8(idx_s).ok().and_then(|s| s.parse().ok()).unwrap_or(0);

    match lists.lindex(key, index) {
        Some(v) => RespEncoder::write_bulk_string(out, &v),
        None => RespEncoder::write_null(out),
    }
}

/// Handle `LREM key count element`.
fn cmd_lrem(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let (Some(key), Some(count_s), Some(element)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    let count: i64 = std::str::from_utf8(count_s).ok().and_then(|s| s.parse().ok()).unwrap_or(0);

    let removed = lists.lrem(key, count, element);
    RespEncoder::write_integer(out, removed as i64);
}

/// Handle `LTRIM key start stop`.
fn cmd_ltrim(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let (Some(key), Some(start_s), Some(stop_s)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    let start: i64 = std::str::from_utf8(start_s).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let stop: i64 = std::str::from_utf8(stop_s).ok().and_then(|s| s.parse().ok()).unwrap_or(-1);

    lists.ltrim(key, start, stop);
    RespEncoder::write_simple_string(out, "OK");
}

/// Handle `LSET key index element`.
fn cmd_lset(cmd: &crate::core::resp::Command, ctx: &ServerContext, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let (Some(key), Some(idx_s), Some(element)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    let index: i64 = std::str::from_utf8(idx_s).ok().and_then(|s| s.parse().ok()).unwrap_or(0);

    if lists.lset(key, index, element) {
        RespEncoder::write_simple_string(out, "OK");
    } else {
        RespEncoder::write_error(out, "ERR index out of range");
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
    fn test_dispatch_get_set() {
        let store = KvStore::with_capacity(100);
        let ctx = ServerContext {
            store: &store,
            wal: None,
            expiry: None,
            lists: None,
        };
        let mut out = Vec::new();

        let cmd = crate::core::resp::Command {
            name: "SET".into(),
            args: vec![b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);
        assert!(out.ends_with(b"+OK\r\n"));

        out.clear();
        let cmd = crate::core::resp::Command {
            name: "GET".into(),
            args: vec![b"GET".to_vec(), b"key".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);
        assert!(out.windows(5).any(|w| w == b"value"));
    }

    #[test]
    fn test_dispatch_ping() {
        let store = KvStore::new();
        let ctx = ServerContext { store: &store, wal: None, expiry: None, lists: None };
        let mut out = Vec::new();

        let cmd = crate::core::resp::Command {
            name: "PING".into(),
            args: vec![b"PING".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);
        assert_eq!(&out, b"+PONG\r\n");
    }

    #[test]
    fn test_dispatch_lpush_lrange_lpop() {
        let store = Arc::new(KvStore::with_capacity(100));
        let lists = ListManager::new(Arc::clone(&store));
        let ctx = ServerContext {
            store: &store,
            wal: None,
            expiry: None,
            lists: Some(&lists),
        };
        let mut out = Vec::new();

        let cmd = crate::core::resp::Command {
            name: "LPUSH".into(),
            args: vec![b"LPUSH".to_vec(), b"q".to_vec(), b"3".to_vec(), b"2".to_vec(), b"1".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);
        assert_eq!(&out, b":3\r\n");

        out.clear();
        let cmd = crate::core::resp::Command {
            name: "LRANGE".into(),
            args: vec![b"LRANGE".to_vec(), b"q".to_vec(), b"0".to_vec(), b"-1".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);
        // Should be an array of 3 elements: 1, 2, 3
        assert!(out.starts_with(b"*3\r\n"));

        out.clear();
        let cmd = crate::core::resp::Command {
            name: "LLEN".into(),
            args: vec![b"LLEN".to_vec(), b"q".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);
        assert_eq!(&out, b":3\r\n");
    }

    #[test]
    fn test_dispatch_del_removes_list() {
        let store = Arc::new(KvStore::with_capacity(100));
        let lists = ListManager::new(Arc::clone(&store));
        let ctx = ServerContext {
            store: &store,
            wal: None,
            expiry: None,
            lists: Some(&lists),
        };
        let mut out = Vec::new();

        // Push an element.
        let cmd = crate::core::resp::Command {
            name: "LPUSH".into(),
            args: vec![b"LPUSH".to_vec(), b"tmp".to_vec(), b"x".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);

        // Delete.
        out.clear();
        let cmd = crate::core::resp::Command {
            name: "DEL".into(),
            args: vec![b"DEL".to_vec(), b"tmp".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);
        assert_eq!(&out, b":1\r\n");

        // Key should be gone from store.
        assert!(!store.exists(b"tmp"));
        assert_eq!(lists.llen(b"tmp"), 0);
    }

    #[test]
    fn test_dispatch_wrong_type_get_on_list() {
        let store = Arc::new(KvStore::with_capacity(100));
        let lists = ListManager::new(Arc::clone(&store));
        let ctx = ServerContext {
            store: &store,
            wal: None,
            expiry: None,
            lists: Some(&lists),
        };
        let mut out = Vec::new();

        // Create a list.
        let cmd = crate::core::resp::Command {
            name: "LPUSH".into(),
            args: vec![b"LPUSH".to_vec(), b"mylist".to_vec(), b"val".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);

        // GET on a list → WRONGTYPE.
        out.clear();
        let cmd = crate::core::resp::Command {
            name: "GET".into(),
            args: vec![b"GET".to_vec(), b"mylist".to_vec()],
        };
        dispatch_command(&cmd, &ctx, &mut out);
        assert!(out.starts_with(b"-"));
        assert!(std::str::from_utf8(&out).unwrap().contains("WRONGTYPE"));
    }
}
