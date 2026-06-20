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
use crate::core::kv::{IncrError, KvStoreLockFree, DEFAULT_INLINE_SIZE};
use crate::core::expiration::ExpirationManager;
use crate::core::list::{self, ListManager};
use crate::core::resp::{write_usize_buf, RespEncoder, RespParser};
use crate::core::wal::WalWriter;
use crate::core::checkpoint;
#[cfg(feature = "blob-store")]
use crate::core::blob::BlobArena;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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
pub struct ServerContext<'a, const N: usize = DEFAULT_INLINE_SIZE> {
    pub store: &'a KvStoreLockFree<N>,
    pub wal: Option<&'a dyn WalWriter>,
    pub expiry: Option<&'a ExpirationManager<N>>,
    pub lists: Option<&'a ListManager<N>>,
    #[cfg(feature = "blob-store")]
    pub blob: Option<&'a BlobArena>,
    pub wal_path: Option<&'a Path>,
    /// Server password for AUTH. If set, clients must authenticate before
    /// issuing other commands.
    pub password: Option<&'a String>,
    /// Whether this connection has successfully authenticated.
    ///
    /// Uses `Arc<AtomicBool>` so the authentication state can be shared
    /// safely across `spawn_blocking` invocations (each command batch may
    /// run on a different blocking thread).
    pub authenticated: &'a AtomicBool,
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Tokio-based async TCP server.
///
/// Accepts Redis clients (RESP protocol) on the configured port.
pub struct TokioServer<const N: usize = DEFAULT_INLINE_SIZE> {
    /// Shared lock-free KV store.
    #[allow(dead_code)]
    store: Arc<KvStoreLockFree<N>>,
    /// Optional WAL for crash-consistent persistence (trait object).
    wal: Option<Arc<dyn WalWriter>>,
    /// Optional expiration manager.
    expiry: Option<Arc<ExpirationManager<N>>>,
    /// Optional list manager.
    lists: Option<Arc<ListManager<N>>>,
    /// Optional blob arena for large-value storage.
    #[cfg(feature = "blob-store")]
    blob: Option<Arc<BlobArena>>,
    /// Path to the WAL file (for BGSAVE/checkpoint).
    wal_path: Option<std::path::PathBuf>,
    /// Authentication password (if set, clients must AUTH before other commands).
    password: Option<String>,
    /// Maximum number of concurrent connections.
    max_connections: u32,
    /// Active connection counter.
    active_connections: Arc<AtomicU32>,
    /// Graceful shutdown flag — set to true when SIGINT/SIGTERM received.
    shutting_down: Arc<AtomicBool>,
    /// Host to bind to.
    host: String,
    /// Port to listen on.
    pub port: u16,
}

impl<const N: usize> TokioServer<N> {
    /// Create a server with default settings (port 6379, no WAL, no TTL, no lists).
    ///
    /// Uses a small capacity (1K buckets) since no data is expected.
    /// For production use [`with_components`] to set `--capacity`.
    pub fn new() -> Self {
        Self {
            store: Arc::new(KvStoreLockFree::<N>::with_capacity(1000)),
            wal: None,
            expiry: None,
            lists: None,
            #[cfg(feature = "blob-store")]
            blob: None,
            wal_path: None,
            password: None,
            max_connections: 10000,
            active_connections: Arc::new(AtomicU32::new(0)),
            shutting_down: Arc::new(AtomicBool::new(false)),
            host: "0.0.0.0".into(),
            port: DEFAULT_PORT,
        }
    }

    /// Create a server that listens on *port*.
    ///
    /// Uses a small capacity (1K buckets) since no data is expected.
    /// For production use [`with_components`] to set `--capacity`.
    pub fn with_port(port: u16) -> Self {
        Self {
            store: Arc::new(KvStoreLockFree::<N>::with_capacity(1000)),
            wal: None,
            expiry: None,
            lists: None,
            #[cfg(feature = "blob-store")]
            blob: None,
            wal_path: None,
            password: None,
            max_connections: 10000,
            active_connections: Arc::new(AtomicU32::new(0)),
            shutting_down: Arc::new(AtomicBool::new(false)),
            host: "0.0.0.0".into(),
            port,
        }
    }

    /// Create a fully configured server.
    #[cfg(feature = "blob-store")]
    pub fn with_components(
        port: u16,
        host: String,
        store: Arc<KvStoreLockFree<N>>,
        wal: Option<Arc<dyn WalWriter>>,
        expiry: Option<Arc<ExpirationManager<N>>>,
        lists: Option<Arc<ListManager<N>>>,
        blob: Option<Arc<BlobArena>>,
        wal_path: Option<std::path::PathBuf>,
        password: Option<String>,
        max_connections: u32,
        shutting_down: Arc<AtomicBool>,
    ) -> Self {
        Self {
            store, wal, expiry, lists,
            blob,
            wal_path,
            password,
            max_connections,
            active_connections: Arc::new(AtomicU32::new(0)),
            shutting_down,
            host, port,
        }
    }

    /// Create a fully configured server (no blob-store).
    #[cfg(not(feature = "blob-store"))]
    pub fn with_components_no_blob(
        port: u16,
        host: String,
        store: Arc<KvStoreLockFree<N>>,
        wal: Option<Arc<dyn WalWriter>>,
        expiry: Option<Arc<ExpirationManager<N>>>,
        lists: Option<Arc<ListManager<N>>>,
        wal_path: Option<std::path::PathBuf>,
        password: Option<String>,
        max_connections: u32,
        shutting_down: Arc<AtomicBool>,
    ) -> Self {
        Self {
            store, wal, expiry, lists,
            wal_path,
            password,
            max_connections,
            active_connections: Arc::new(AtomicU32::new(0)),
            shutting_down,
            host, port,
        }
    }

    /// Clone the shared store reference.
    pub fn store(&self) -> Arc<KvStoreLockFree<N>> {
        Arc::clone(&self.store)
    }

    /// Run the accept loop (async — call inside a Tokio runtime).
    ///
    /// Handles SIGINT/SIGTERM for graceful shutdown: stops accepting new
    /// connections, waits for existing ones to finish (up to 30 s), then exits.
    pub async fn run(&self) -> Result<(), String> {
        let addr = format!("{}:{}", self.host, self.port);

        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                let msg = format!("Failed to bind to {}: {}", addr, e);
                eprintln!("{}", msg);
                return Err(msg);
            }
        };

        println!("FastKV server listening on {}", addr);
        println!("Ready to accept Redis clients!");
        println!();

        let wal = self.wal.clone();
        let expiry = self.expiry.clone();
        let lists = self.lists.clone();
        #[cfg(feature = "blob-store")]
        let blob = self.blob.clone();
        let wal_path = self.wal_path.clone();
        let password = self.password.clone();
        let max_connections = self.max_connections;
        let active_connections = Arc::clone(&self.active_connections);
        let shutting_down = Arc::clone(&self.shutting_down);

        // --- Spawn signal handler task ---
        {
            let shutting_down = Arc::clone(&shutting_down);
            let wal = wal.clone();
            tokio::spawn(async move {
                // Wait for SIGINT (Ctrl+C) or SIGTERM.
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{signal, SignalKind};
                    let mut sigterm = signal(SignalKind::terminate())
                        .expect("Failed to install SIGTERM handler");
                    let mut sigint = signal(SignalKind::interrupt())
                        .expect("Failed to install SIGINT handler");
                    tokio::select! {
                        _ = sigint.recv() => {},
                        _ = sigterm.recv() => {},
                    }
                }
                #[cfg(not(unix))]
                {
                    tokio::signal::ctrl_c().await.ok();
                }

                println!("\nReceived shutdown signal, shutting down gracefully...");
                shutting_down.store(true, Ordering::SeqCst);
                // Flush WAL to ensure durability.
                if let Some(w) = wal {
                    let _ = w.wal_sync_now();
                }
                println!("WAL flushed.");
            });
        }

        // --- Accept loop ---
        loop {
            if shutting_down.load(Ordering::SeqCst) {
                break;
            }

            let accept_result = tokio::select! {
                result = listener.accept() => result,
                _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                    // Periodic re-check of the shutting_down flag.
                    continue;
                }
            };

            let (mut socket, peer_addr) = match accept_result {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                    continue;
                }
            };

            // Check connection limit before spawning.
            if active_connections.load(Ordering::Relaxed) >= max_connections {
                let _ = socket.write_all(b"-ERR max number of clients reached\r\n").await;
                continue;
            }

            // Increment active connections; the spawned task will decrement via guard.
            active_connections.fetch_add(1, Ordering::Relaxed);

            let store = Arc::clone(&self.store);
            let wal_c = wal.clone();
            let expiry_c = expiry.clone();
            let lists_c = lists.clone();
            #[cfg(feature = "blob-store")]
            let blob_c = blob.clone();
            let wal_path_c = wal_path.clone();
            let password_c = password.clone();
            let active_conn = Arc::clone(&active_connections);
            let shutting_down_c = Arc::clone(&shutting_down);

            tokio::spawn(async move {
                // RAII guard: decrements active_connections when the task exits.
                struct ConnGuard { conn: Arc<AtomicU32> }
                impl Drop for ConnGuard {
                    fn drop(&mut self) { self.conn.fetch_sub(1, Ordering::Relaxed); }
                }
                let _guard = ConnGuard { conn: active_conn };

                #[cfg(feature = "blob-store")]
                handle_client(socket, store, wal_c, expiry_c, lists_c, blob_c, wal_path_c, peer_addr, password_c, shutting_down_c).await;
                #[cfg(not(feature = "blob-store"))]
                handle_client(socket, store, wal_c, expiry_c, lists_c, wal_path_c, peer_addr, password_c, shutting_down_c).await;
            });
        }

        // --- Graceful shutdown: wait for existing connections ---
        let active = active_connections.load(Ordering::SeqCst);
        if active > 0 {
            println!("Waiting for {} active connection(s) to finish...", active);
            let timeout = std::time::Duration::from_secs(30);
            let start = std::time::Instant::now();
            while active_connections.load(Ordering::SeqCst) > 0 && start.elapsed() < timeout {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            let remaining = active_connections.load(Ordering::SeqCst);
            if remaining > 0 {
                println!("Timeout reached, forcing shutdown with {} connection(s) still active.", remaining);
            }
        }
        println!("Server shut down gracefully.");
        Ok(())
    }
}

impl<const N: usize> Default for TokioServer<N> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Client handler
// ---------------------------------------------------------------------------

/// Owned bundle of shared components passed into `spawn_blocking` so that
/// command processing (which includes synchronous WAL writes) runs on a
/// blocking thread instead of an async worker thread.
///
/// This is the key fix for the concurrency issue where heavy write load
/// (e.g. from a farmer process making many BSET/SET calls) would starve
/// read commands (SCAN, GET) by blocking all tokio worker threads on WAL
/// `std::sync::Mutex` + synchronous `file.write_all()` / `fsync`.
#[cfg(feature = "blob-store")]
struct OwnedCtx<const N: usize> {
    store: Arc<KvStoreLockFree<N>>,
    wal: Option<Arc<dyn WalWriter>>,
    expiry: Option<Arc<ExpirationManager<N>>>,
    lists: Option<Arc<ListManager<N>>>,
    blob: Option<Arc<BlobArena>>,
    wal_path: Option<std::path::PathBuf>,
    password: Option<String>,
    authenticated: Arc<AtomicBool>,
}

/// Owned bundle (no blob-store variant).
#[cfg(not(feature = "blob-store"))]
struct OwnedCtx<const N: usize> {
    store: Arc<KvStoreLockFree<N>>,
    wal: Option<Arc<dyn WalWriter>>,
    expiry: Option<Arc<ExpirationManager<N>>>,
    lists: Option<Arc<ListManager<N>>>,
    wal_path: Option<std::path::PathBuf>,
    password: Option<String>,
    authenticated: Arc<AtomicBool>,
}

/// Process all complete commands in *buffer* on a blocking thread.
///
/// Returns `(response_bytes, should_close, remaining_bytes)`.
///
/// This function is designed to be called via `tokio::task::spawn_blocking`.
/// It builds a transient [`ServerContext`] from the owned [`OwnedCtx`] and
/// processes every complete command in *buffer*, accumulating responses.
/// Incomplete trailing bytes (partial command) are returned as `remaining_bytes`
/// so the caller can carry them over to the next read.
#[cfg(feature = "blob-store")]
fn process_buffer<const N: usize>(
    ctx_owned: OwnedCtx<N>,
    mut buffer: Vec<u8>,
) -> (Vec<u8>, bool, Vec<u8>) {
    let ctx = ServerContext::<N> {
        store: &ctx_owned.store,
        wal: ctx_owned.wal.as_deref(),
        expiry: ctx_owned.expiry.as_deref(),
        lists: ctx_owned.lists.as_deref(),
        blob: ctx_owned.blob.as_deref(),
        wal_path: ctx_owned.wal_path.as_deref(),
        password: ctx_owned.password.as_ref(),
        authenticated: &ctx_owned.authenticated,
    };

    let mut resp_buf = Vec::with_capacity(INITIAL_RESPONSE_SIZE);
    let mut consumed = 0;
    let mut should_close = false;

    while consumed < buffer.len() {
        match parse_command_bounds(&buffer[consumed..]) {
            Some((len, is_inline)) => {
                let cmd_data = &buffer[consumed..consumed + len];
                should_close = process_command_into(cmd_data, &ctx, &mut resp_buf, is_inline);
                consumed += len;
                if should_close { break; }
            }
            None => break,
        }
    }

    // Drain consumed bytes; keep the unconsumed tail (partial command).
    buffer.drain(..consumed);

    (resp_buf, should_close, buffer)
}

/// Process all complete commands in *buffer* on a blocking thread (no blob-store).
#[cfg(not(feature = "blob-store"))]
fn process_buffer<const N: usize>(
    ctx_owned: OwnedCtx<N>,
    mut buffer: Vec<u8>,
) -> (Vec<u8>, bool, Vec<u8>) {
    let ctx = ServerContext::<N> {
        store: &ctx_owned.store,
        wal: ctx_owned.wal.as_deref(),
        expiry: ctx_owned.expiry.as_deref(),
        lists: ctx_owned.lists.as_deref(),
        wal_path: ctx_owned.wal_path.as_deref(),
        password: ctx_owned.password.as_ref(),
        authenticated: &ctx_owned.authenticated,
    };

    let mut resp_buf = Vec::with_capacity(INITIAL_RESPONSE_SIZE);
    let mut consumed = 0;
    let mut should_close = false;

    while consumed < buffer.len() {
        match parse_command_bounds(&buffer[consumed..]) {
            Some((len, is_inline)) => {
                let cmd_data = &buffer[consumed..consumed + len];
                should_close = process_command_into(cmd_data, &ctx, &mut resp_buf, is_inline);
                consumed += len;
                if should_close { break; }
            }
            None => break,
        }
    }

    buffer.drain(..consumed);

    (resp_buf, should_close, buffer)
}

/// Per-connection state and main read/write loop (with blob-store).
///
/// **Concurrency model**: socket I/O (read/write) runs on the async worker
/// thread, but command processing (which may do synchronous WAL writes) is
/// dispatched to `tokio::task::spawn_blocking`. This ensures that heavy
/// write load from one connection cannot starve read commands (SCAN, GET)
/// on other connections by blocking all async worker threads on WAL I/O.
#[cfg(feature = "blob-store")]
async fn handle_client<const N: usize>(
    mut socket: TcpStream,
    store: Arc<KvStoreLockFree<N>>,
    wal: Option<Arc<dyn WalWriter>>,
    expiry: Option<Arc<ExpirationManager<N>>>,
    lists: Option<Arc<ListManager<N>>>,
    blob: Option<Arc<BlobArena>>,
    wal_path: Option<std::path::PathBuf>,
    addr: std::net::SocketAddr,
    password: Option<String>,
    shutting_down: Arc<AtomicBool>,
) {
    let mut read_buf = vec![0u8; MAX_BUFFER_SIZE];
    let mut leftover: Vec<u8> = Vec::with_capacity(4096);
    let authenticated = Arc::new(AtomicBool::new(false));

    loop {
        // Stop processing when the server is shutting down.
        if shutting_down.load(Ordering::SeqCst) {
            break;
        }

        let n = match socket.read(&mut read_buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => { eprintln!("[{}] Read error: {}", addr, e); break; }
        };

        leftover.extend_from_slice(&read_buf[..n]);

        // Skip the spawn_blocking round-trip if there is nothing to process yet.
        if leftover.is_empty() {
            continue;
        }

        // Build the owned context for the blocking closure.
        let ctx_owned = OwnedCtx::<N> {
            store: Arc::clone(&store),
            wal: wal.clone(),
            expiry: expiry.clone(),
            lists: lists.clone(),
            blob: blob.clone(),
            wal_path: wal_path.clone(),
            password: password.clone(),
            authenticated: Arc::clone(&authenticated),
        };

        // Move command processing to a blocking thread so that synchronous
        // WAL writes (std::sync::Mutex + file.write_all + optional fsync)
        // do not block the async worker thread. This is critical for
        // concurrent read commands (SCAN, GET) to make progress while
        // other connections are doing heavy writes.
        let buffer_to_process = std::mem::take(&mut leftover);
        let process_result = tokio::task::spawn_blocking(move || {
            process_buffer(ctx_owned, buffer_to_process)
        }).await;

        let (resp_buf, should_close, remaining) = match process_result {
            Ok(result) => result,
            Err(join_err) => {
                eprintln!("[{}] Command processing task panicked: {}", addr, join_err);
                break;
            }
        };

        // Carry over unconsumed bytes (partial command) to the next iteration.
        leftover = remaining;

        if !resp_buf.is_empty() {
            if let Err(e) = socket.write_all(&resp_buf).await {
                eprintln!("[{}] Write error: {}", addr, e); break;
            }
        }
        if should_close { break; }
    }
}

/// Per-connection state and main read/write loop (no blob-store).
#[cfg(not(feature = "blob-store"))]
async fn handle_client<const N: usize>(
    mut socket: TcpStream,
    store: Arc<KvStoreLockFree<N>>,
    wal: Option<Arc<dyn WalWriter>>,
    expiry: Option<Arc<ExpirationManager<N>>>,
    lists: Option<Arc<ListManager<N>>>,
    wal_path: Option<std::path::PathBuf>,
    addr: std::net::SocketAddr,
    password: Option<String>,
    shutting_down: Arc<AtomicBool>,
) {
    let mut read_buf = vec![0u8; MAX_BUFFER_SIZE];
    let mut leftover: Vec<u8> = Vec::with_capacity(4096);
    let authenticated = Arc::new(AtomicBool::new(false));

    loop {
        // Stop processing when the server is shutting down.
        if shutting_down.load(Ordering::SeqCst) {
            break;
        }

        let n = match socket.read(&mut read_buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => { eprintln!("[{}] Read error: {}", addr, e); break; }
        };

        leftover.extend_from_slice(&read_buf[..n]);

        if leftover.is_empty() {
            continue;
        }

        let ctx_owned = OwnedCtx::<N> {
            store: Arc::clone(&store),
            wal: wal.clone(),
            expiry: expiry.clone(),
            lists: lists.clone(),
            wal_path: wal_path.clone(),
            password: password.clone(),
            authenticated: Arc::clone(&authenticated),
        };

        let buffer_to_process = std::mem::take(&mut leftover);
        let process_result = tokio::task::spawn_blocking(move || {
            process_buffer(ctx_owned, buffer_to_process)
        }).await;

        let (resp_buf, should_close, remaining) = match process_result {
            Ok(result) => result,
            Err(join_err) => {
                eprintln!("[{}] Command processing task panicked: {}", addr, join_err);
                break;
            }
        };

        leftover = remaining;

        if !resp_buf.is_empty() {
            if let Err(e) = socket.write_all(&resp_buf).await {
                eprintln!("[{}] Write error: {}", addr, e); break;
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
        b'A' | b'G' | b'S' | b'D' | b'P' | b'I' | b'H' | b'L' | b'E' | b'C'
        | b'Q' | b'F' | b'K' | b'M' | b'T' | b'X' | b'R' | b'O' | b'U'
        | b'Z' | b'N' | b'B' => {
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
pub fn process_command_into<const N: usize>(
    data: &[u8],
    ctx: &ServerContext<N>,
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
fn dispatch_command<const N: usize>(
    cmd: &crate::core::resp::Command,
    ctx: &ServerContext<N>,
    out: &mut Vec<u8>,
) -> bool {
    let cmd_name = cmd.name.as_str();

    // ----- AUTH enforcement -----
    // If a password is set and the connection is not yet authenticated,
    // only allow AUTH, PING, and QUIT commands.
    if ctx.password.is_some() && !ctx.authenticated.load(Ordering::SeqCst) {
        match cmd_name {
            "AUTH" | "PING" | "QUIT" => {}, // allowed without auth
            _ => {
                out.extend_from_slice(b"-NOAUTH Authentication required\r\n");
                return false;
            }
        }
    }

    match cmd_name {
        // ----- Core -----
        "AUTH"   => { cmd_auth(cmd, ctx, out); false }
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
        "TYPE"    => { cmd_type(cmd, ctx, out); false }
        "RENAME"  => { cmd_rename(cmd, ctx, out); false }
        "GETSET"  => { cmd_getset(cmd, ctx, out); false }
        "GETDEL"  => { cmd_getdel(cmd, ctx, out); false }
        "SETNX"   => { cmd_setnx(cmd, ctx, out); false }
        "PSETEX"  => { cmd_psetex(cmd, ctx, out); false }
        "UNLINK"  => { cmd_del(cmd, ctx, out); false }
        "FLUSHALL" => { cmd_flushall(ctx, out); false }
        "FLUSHDB"  => { cmd_flushall(ctx, out); false }

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
        "HINCRBY" => { cmd_hincrby(cmd, ctx, out); false }
        "HSETNX"  => { cmd_hsetnx(cmd, ctx, out); false }

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

        // ----- Blob -----
        #[cfg(feature = "blob-store")]
        "BSET"    => { cmd_bset(cmd, ctx, out); false }
        #[cfg(feature = "blob-store")]
        "BGET"    => { cmd_bget(cmd, ctx, out); false }
        #[cfg(feature = "blob-store")]
        "BGETRAW" => { cmd_bgetraw(cmd, ctx, out); false }
        #[cfg(feature = "blob-store")]
        "BSTATS"  => { cmd_bstats(ctx, out); false }

        // ----- Similarity -----
        #[cfg(feature = "similarity")]
        "SIMHASH" => { cmd_simhash(cmd, ctx, out); false }
        #[cfg(feature = "similarity")]
        "FINDSIM" => { cmd_findsim(cmd, ctx, out); false }
        #[cfg(feature = "similarity")]
        "LSHADD"  => { cmd_lshadd(cmd, ctx, out); false }
        #[cfg(feature = "similarity")]
        "LSHREM"  => { cmd_lshrem(cmd, ctx, out); false }

        // ----- Scan / Stats -----
        "SCAN"    => { cmd_scan(cmd, ctx, out); false }
        "DBSTATS" => { cmd_dbstats(ctx, out); false }

        // ----- Checkpoint -----
        "BGSAVE"  => { cmd_bgsave(ctx, out); false }
        "SAVE"    => { cmd_bgsave(ctx, out); false }

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
// AUTH command
// ---------------------------------------------------------------------------

/// Handle `AUTH <password>` — authenticate the connection.
///
/// If no password is configured on the server, returns an error.
/// If the password matches, marks the connection as authenticated.
fn cmd_auth<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    // No password configured on the server.
    let Some(expected) = ctx.password else {
        out.extend_from_slice(b"-ERR Client sent AUTH, but no password is set\r\n");
        return;
    };

    let Some(provided) = cmd.arg(1) else {
        return err_wrong_args(out);
    };

    if provided == expected.as_bytes() {
        ctx.authenticated.store(true, Ordering::SeqCst);
        out.extend_from_slice(b"+OK\r\n");
    } else {
        out.extend_from_slice(b"-ERR invalid password\r\n");
    }
}

// ---------------------------------------------------------------------------
// Inline command handler
// ---------------------------------------------------------------------------

/// Parse and dispatch a plain-text inline command (e.g. `GET key\r\n`).
///
/// Returns `true` if the connection should be closed (e.g. QUIT command).
fn handle_inline<const N: usize>(
    data: &[u8],
    ctx: &ServerContext<N>,
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
fn cmd_get<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Lazy expiration check (single lock acquisition).
    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            RespEncoder::write_null(out);
            return;
        }
    }

    match ctx.store.get(key) {
        Some(v) if hash::is_hash_value(&v) || list::is_list_value(&v) => {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
        }
        Some(v) => {
            // Auto-decompress blob references for transparent GET.
            #[cfg(feature = "blob-store")]
            if crate::core::blob::BlobArena::is_blob_ref(&v) {
                if let Some(blob_arena) = ctx.blob {
                    if let Some(blob_ref) = crate::core::blob::BlobRef::decode(&v) {
                        if let Some(decompressed) = blob_arena.retrieve(&blob_ref) {
                            RespEncoder::write_bulk_string(out, &decompressed);
                            return;
                        }
                    }
                }
                // Blob ref but arena unavailable or decompression failed.
                RespEncoder::write_error(out, "ERR blob decompression failed");
                return;
            }
            RespEncoder::write_bulk_string(out, &v)
        }
        None => RespEncoder::write_null(out),
    }
}

/// Handle `SET key value` — insert/update and persist to WAL.
fn cmd_set<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
        if list::is_list_value(&ctx.store.get(key).unwrap_or_default()) {
            lists.remove_key(key);
        }
    }

    // Remove old TTL if overwriting.
    if let Some(exp) = ctx.expiry {
        exp.remove(key);
    }

    if ctx.store.set(key, value) {
        if let Some(w) = ctx.wal {
            if w.wal_set(key, value).is_err() {
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
                        let _ = w.wal_expire(key, deadline_ms);
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
fn cmd_del<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    if cmd.argc() < 2 {
        return err_wrong_args(out);
    }
    let mut deleted: i64 = 0;
    for i in 1..cmd.argc() {
        let Some(key) = cmd.arg(i) else { continue };
        // Before deleting, check if the value is a blob ref and free it.
        #[cfg(feature = "blob-store")]
        let mut is_blob_key = false;
        #[cfg(feature = "blob-store")]
        if let Some(v) = ctx.store.get(key) {
            if crate::core::blob::BlobArena::is_blob_ref(&v) {
                if let Some(blob_arena) = ctx.blob {
                    if let Some(blob_ref) = crate::core::blob::BlobRef::decode(&v) {
                        blob_arena.free(&blob_ref);
                    }
                }
                is_blob_key = true;
            }
        }
        if ctx.store.del(key) {
            deleted += 1;
            if let Some(w) = ctx.wal {
                // Use BDEL for blob keys so recovery knows to also free
                // the arena slot; regular DEL for non-blob keys.
                #[cfg(feature = "blob-store")]
                if is_blob_key {
                    let _ = w.wal_bdel(key);
                } else {
                    let _ = w.wal_del(key);
                }
                #[cfg(not(feature = "blob-store"))]
                let _ = w.wal_del(key);
            }
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
fn cmd_info<const N: usize>(ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_incr<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>, delta: i64) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check type: INCR only works on plain string keys.
    if let Some(val) = ctx.store.get(key) {
        if hash::is_hash_value(&val) || list::is_list_value(&val) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    match ctx.store.incr(key, delta) {
        Ok(new_val) => {
            if let Some(w) = ctx.wal {
                if w.wal_set(key, new_val.to_string().as_bytes()).is_err() {
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
                if w.wal_set(key, new_val.to_string().as_bytes()).is_err() {
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
fn cmd_incrby<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(_key), Some(delta_bytes)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    let delta: i64 = match std::str::from_utf8(delta_bytes).ok().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => { RespEncoder::write_error(out, "ERR value is not an integer"); return; }
    };

    cmd_incr(cmd, ctx, out, delta);
}

/// Handle `DECRBY key delta` — decrement by a specified (positive) amount.
fn cmd_decrby<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(_key), Some(delta_bytes)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    let delta: i64 = match std::str::from_utf8(delta_bytes).ok().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => { RespEncoder::write_error(out, "ERR value is not an integer"); return; }
    };

    cmd_incr(cmd, ctx, out, -delta);
}

/// Handle `APPEND key suffix` — append to string value, persist result.
fn cmd_append<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(suffix)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    if let Some(current) = ctx.store.get(key) {
        if hash::is_hash_value(&current) || list::is_list_value(&current) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    match ctx.store.append(key, suffix) {
        Some(new_len) => {
            if let Some(w) = ctx.wal {
                if let Some(val) = ctx.store.get(key) {
                    if w.wal_set(key, &val).is_err() {
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
fn cmd_strlen<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            out.extend_from_slice(b":0\r\n");
            return;
        }
    }

    if let Some(v) = ctx.store.get(key) {
        if hash::is_hash_value(&v) || list::is_list_value(&v) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    RespEncoder::write_integer(out, ctx.store.strlen(key) as i64);
}

/// Handle `GETRANGE key start end` — return substring (inclusive bounds).
fn cmd_getrange<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(start_s), Some(end_s)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            out.extend_from_slice(b"$0\r\n\r\n");
            return;
        }
    }

    if let Some(v) = ctx.store.get(key) {
        if hash::is_hash_value(&v) || list::is_list_value(&v) {
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
fn cmd_setrange<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(offset_s), Some(replacement)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    if let Some(v) = ctx.store.get(key) {
        if hash::is_hash_value(&v) || list::is_list_value(&v) {
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
                    if w.wal_set(key, &val).is_err() {
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
fn cmd_mget<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
            Some(v) if hash::is_hash_value(v) || list::is_list_value(v) => {
                // In Redis MGET, WRONGTYPE is not returned per-element; nil is returned.
                RespEncoder::write_null(out);
            }
            Some(v) => RespEncoder::write_bulk_string(out, v),
            None => RespEncoder::write_null(out),
        }
    }
}

/// Handle `MSET k1 v1 k2 v2 ...` — set multiple key-value pairs atomically.
fn cmd_mset<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
                if w.wal_set(k, v).is_err() {
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
fn cmd_exists<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    if cmd.argc() < 2 {
        return err_wrong_args(out);
    }
    let mut count: i64 = 0;
    for i in 1..cmd.argc() {
        let Some(key) = cmd.arg(i) else { continue };
        if let Some(exp) = ctx.expiry {
            if exp.check_and_purge_if_expired(key) {
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
// TYPE / RENAME / GETSET / GETDEL / SETNX / PSETEX / FLUSHALL
// ---------------------------------------------------------------------------

/// Handle `TYPE key` — return the type of value stored at key.
fn cmd_type<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check expiration first.
    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            out.extend_from_slice(b"+none\r\n");
            return;
        }
    }

    match ctx.store.get(key) {
        None => out.extend_from_slice(b"+none\r\n"),
        Some(v) if hash::is_hash_value(&v) => out.extend_from_slice(b"+hash\r\n"),
        Some(v) if list::is_list_value(&v) => out.extend_from_slice(b"+list\r\n"),
        Some(_) => out.extend_from_slice(b"+string\r\n"),
    }
}

/// Handle `RENAME key newkey` — atomically rename a key.
fn cmd_rename<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(newkey)) = (cmd.key(), cmd.arg(2)) else { return err_wrong_args(out); };

    // If oldkey == newkey, do nothing.
    if key == newkey {
        out.extend_from_slice(b"+OK\r\n");
        return;
    }

    // Check expiration on old key.
    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            out.extend_from_slice(b"-ERR no such key\r\n");
            return;
        }
    }

    // GET old key value.
    let Some(value) = ctx.store.get(key) else {
        out.extend_from_slice(b"-ERR no such key\r\n");
        return;
    };

    // If the newkey held a list, clean up the list data.
    if let Some(lists) = ctx.lists {
        if list::is_list_value(&ctx.store.get(newkey).unwrap_or_default()) {
            lists.remove_key(newkey);
        }
    }

    // Remove old TTL from newkey if it existed.
    if let Some(exp) = ctx.expiry {
        exp.remove(newkey);
    }

    // SET newkey with the value.
    if !ctx.store.set(newkey, &value) {
        out.extend_from_slice(b"-ERR value too large\r\n");
        return;
    }

    // Transfer TTL from old key to new key.
    let old_ttl = ctx.expiry.and_then(|exp| exp.ttl(key));
    if let Some(exp) = ctx.expiry {
        exp.remove(key);
        if let Some(ttl) = old_ttl {
            let _ = exp.expire_with_deadline(newkey, ttl);
        }
    }

    // DEL oldkey.
    ctx.store.del(key);

    // WAL: SET newkey, DEL oldkey.
    if let Some(w) = ctx.wal {
        // Check if the value is a blob ref for proper WAL entry.
        #[cfg(feature = "blob-store")]
        let is_blob = crate::core::blob::BlobArena::is_blob_ref(&value);
        #[cfg(feature = "blob-store")]
        if is_blob {
            let _ = w.wal_bset(newkey, &value);
        } else {
            let _ = w.wal_set(newkey, &value);
        }
        #[cfg(not(feature = "blob-store"))]
        let _ = w.wal_set(newkey, &value);
        let _ = w.wal_del(key);
    }

    // Write EXPIRE for newkey to WAL if TTL was transferred.
    if let Some(exp) = ctx.expiry {
        if let Some(ttl) = old_ttl {
            if let Some(deadline_ms) = exp.expire_with_deadline(newkey, ttl) {
                if let Some(w) = ctx.wal {
                    let _ = w.wal_expire(newkey, deadline_ms);
                }
            }
        }
    }

    out.extend_from_slice(b"+OK\r\n");
}

/// Handle `GETSET key value` — atomically set new value and return old value.
fn cmd_getset<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(value)) = (cmd.key(), cmd.value()) else { return err_wrong_args(out); };

    // GET old value (check expiration).
    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            // Key expired — return nil for old value, but still set new value.
            // Fall through to set.
        }
    }

    let old_value = ctx.store.get(key);

    // Check WRONGTYPE for old value.
    if let Some(ref v) = old_value {
        if hash::is_hash_value(v) || list::is_list_value(v) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    // If the key held a list, clean up the list data.
    if let Some(lists) = ctx.lists {
        if list::is_list_value(&ctx.store.get(key).unwrap_or_default()) {
            lists.remove_key(key);
        }
    }

    // Remove old TTL.
    if let Some(exp) = ctx.expiry {
        exp.remove(key);
    }

    // SET new value.
    if ctx.store.set(key, value) {
        if let Some(w) = ctx.wal {
            if w.wal_set(key, value).is_err() {
                out.extend_from_slice(b"-ERR WAL write failed\r\n");
                return;
            }
        }
    } else {
        out.extend_from_slice(b"-ERR value too large\r\n");
        return;
    }

    // Return old value.
    match old_value {
        Some(v) => {
            // Auto-decompress blob references for transparent GETSET.
            #[cfg(feature = "blob-store")]
            if crate::core::blob::BlobArena::is_blob_ref(&v) {
                if let Some(blob_arena) = ctx.blob {
                    if let Some(blob_ref) = crate::core::blob::BlobRef::decode(&v) {
                        if let Some(decompressed) = blob_arena.retrieve(&blob_ref) {
                            RespEncoder::write_bulk_string(out, &decompressed);
                            return;
                        }
                    }
                }
            }
            RespEncoder::write_bulk_string(out, &v)
        }
        None => RespEncoder::write_null(out),
    }
}

/// Handle `GETDEL key` — get value and delete the key atomically.
fn cmd_getdel<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check expiration.
    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            RespEncoder::write_null(out);
            return;
        }
    }

    let value = ctx.store.get(key);

    match value {
        Some(ref v) if hash::is_hash_value(v) || list::is_list_value(v) => {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
        _ => {}
    }

    if let Some(ref v) = value {
        // Free blob ref if applicable.
        #[cfg(feature = "blob-store")]
        if crate::core::blob::BlobArena::is_blob_ref(v) {
            if let Some(blob_arena) = ctx.blob {
                if let Some(blob_ref) = crate::core::blob::BlobRef::decode(v) {
                    blob_arena.free(&blob_ref);
                }
            }
        }

        // DEL the key.
        ctx.store.del(key);
        if let Some(w) = ctx.wal {
            #[cfg(feature = "blob-store")]
            if crate::core::blob::BlobArena::is_blob_ref(v) {
                let _ = w.wal_bdel(key);
            } else {
                let _ = w.wal_del(key);
            }
            #[cfg(not(feature = "blob-store"))]
            let _ = w.wal_del(key);
        }
        if let Some(exp) = ctx.expiry { exp.remove(key); }
        if let Some(lists) = ctx.lists { lists.remove_key(key); }

        // Auto-decompress blob references for transparent GETDEL.
        #[cfg(feature = "blob-store")]
        if crate::core::blob::BlobArena::is_blob_ref(v) {
            if let Some(blob_arena) = ctx.blob {
                if let Some(blob_ref) = crate::core::blob::BlobRef::decode(v) {
                    if let Some(decompressed) = blob_arena.retrieve(&blob_ref) {
                        RespEncoder::write_bulk_string(out, &decompressed);
                        return;
                    }
                }
            }
        }

        RespEncoder::write_bulk_string(out, v);
    } else {
        RespEncoder::write_null(out);
    }
}

/// Handle `SETNX key value` — set if not exists.
fn cmd_setnx<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(value)) = (cmd.key(), cmd.value()) else { return err_wrong_args(out); };

    // Check expiration.
    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            // Key expired — treat as not existing.
        }
    }

    if ctx.store.exists(key) {
        out.extend_from_slice(b":0\r\n");
        return;
    }

    // If the key held a list, clean up the list data.
    if let Some(lists) = ctx.lists {
        if list::is_list_value(&ctx.store.get(key).unwrap_or_default()) {
            lists.remove_key(key);
        }
    }

    if ctx.store.set(key, value) {
        if let Some(w) = ctx.wal {
            if w.wal_set(key, value).is_err() {
                out.extend_from_slice(b"-ERR WAL write failed\r\n");
                return;
            }
        }
        out.extend_from_slice(b":1\r\n");
    } else {
        out.extend_from_slice(b"-ERR value too large\r\n");
    }
}

/// Handle `PSETEX key ms value` — set with expiry in milliseconds.
fn cmd_psetex<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(ms_bytes), Some(value)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    let ms: u64 = match std::str::from_utf8(ms_bytes).ok().and_then(|s| s.parse().ok()) {
        Some(m) => m,
        None => {
            RespEncoder::write_error(out, "ERR value is not an integer or out of range");
            return;
        }
    };

    // If the key held a list, clean up the list data.
    if let Some(lists) = ctx.lists {
        if list::is_list_value(&ctx.store.get(key).unwrap_or_default()) {
            lists.remove_key(key);
        }
    }

    // Remove old TTL if overwriting.
    if let Some(exp) = ctx.expiry {
        exp.remove(key);
    }

    if ctx.store.set(key, value) {
        if let Some(w) = ctx.wal {
            if w.wal_set(key, value).is_err() {
                out.extend_from_slice(b"-ERR WAL write failed\r\n");
                return;
            }
        }

        // Apply TTL.
        if let Some(exp) = ctx.expiry {
            let ttl = std::time::Duration::from_millis(ms);
            if let Some(deadline_ms) = exp.expire_with_deadline(key, ttl) {
                if let Some(w) = ctx.wal {
                    let _ = w.wal_expire(key, deadline_ms);
                }
            }
        }

        out.extend_from_slice(b"+OK\r\n");
    } else {
        out.extend_from_slice(b"-ERR value too large\r\n");
    }
}

/// Handle `FLUSHALL` / `FLUSHDB` — clear all data.
fn cmd_flushall<const N: usize>(ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    // Collect all keys.
    let mut all_keys: Vec<Vec<u8>> = Vec::new();
    let mut cursor = 0usize;
    loop {
        let (next_cursor, batch) = ctx.store.scan(cursor, 1000, None);
        all_keys.extend(batch);
        if next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }

    // Delete each key.
    for key in &all_keys {
        // Free blob ref if applicable.
        #[cfg(feature = "blob-store")]
        if let Some(v) = ctx.store.get(key) {
            if crate::core::blob::BlobArena::is_blob_ref(&v) {
                if let Some(blob_arena) = ctx.blob {
                    if let Some(blob_ref) = crate::core::blob::BlobRef::decode(&v) {
                        blob_arena.free(&blob_ref);
                    }
                }
            }
        }

        ctx.store.del(key);
        if let Some(w) = ctx.wal {
            let _ = w.wal_del(key);
        }
        if let Some(lists) = ctx.lists { lists.remove_key(key); }
    }

    // Clear all TTL entries.
    if let Some(exp) = ctx.expiry {
        // Collect all keys with TTLs and remove them.
        let (mut cursor, mut done) = (0usize, false);
        while !done {
            let (next_cursor, batch) = ctx.store.scan(cursor, 1000, None);
            for key in &batch {
                exp.remove(key);
            }
            if next_cursor == 0 {
                done = true;
            } else {
                cursor = next_cursor;
            }
        }
    }

    // WAL checkpoint if available.
    if let Some(wal_path) = ctx.wal_path {
        #[cfg(feature = "blob-store")]
        let _ = checkpoint::checkpoint(ctx.store, ctx.expiry, wal_path, ctx.lists, ctx.blob);
        #[cfg(not(feature = "blob-store"))]
        let _ = checkpoint::checkpoint(ctx.store, ctx.expiry, wal_path, ctx.lists);
        if let Some(w) = ctx.wal {
            let _ = w.wal_reopen();
        }
    }

    out.extend_from_slice(b"+OK\r\n");
}

// ---------------------------------------------------------------------------
// Expiration commands
// ---------------------------------------------------------------------------

/// Handle `EXPIRE key seconds` — set a TTL in seconds and persist to WAL.
fn cmd_expire<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
        if let Some(w) = ctx.wal { let _ = w.wal_del(key); }
        RespEncoder::write_integer(out, 1);
        return;
    }

    let ttl = std::time::Duration::from_secs(secs as u64);

    match exp.expire_with_deadline(key, ttl) {
        Some(deadline_ms) => {
            // Persist to WAL.
            if let Some(w) = ctx.wal {
                if w.wal_expire(key, deadline_ms).is_err() {
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
fn cmd_ttl<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_pttl<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_persist<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn get_current_value<const N: usize>(key: &[u8], ctx: &ServerContext<N>) -> Vec<u8> {
    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            return Vec::new();
        }
    }
    ctx.store.get(key).unwrap_or_default()
}

/// Handle `HSET key field value [field value ...]`.
fn cmd_hset<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
                if list::is_list_value(&current) {
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
            if w.wal_set(key, &current).is_err() {
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
fn cmd_hget<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_hdel<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
                if let Some(w) = ctx.wal { let _ = w.wal_del(key); }
                if let Some(exp) = ctx.expiry { exp.remove(key); }
                current = Vec::new();
                break;
            }
            HashDelResult::FieldNotFound => {}
        }
    }

    if !current.is_empty() {
        ctx.store.set(key, &current);
        if let Some(w) = ctx.wal { let _ = w.wal_set(key, &current); }
    }

    RespEncoder::write_integer(out, deleted);
}

/// Handle `HGETALL key`.
fn cmd_hgetall<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_hexists<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_hlen<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let current = get_current_value(key, ctx);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    RespEncoder::write_integer(out, hash::hash_len(&current) as i64);
}

/// Handle `HKEYS key`.
fn cmd_hkeys<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_hvals<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_hmget<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_hmset<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
            if w.wal_set(key, &current).is_err() {
                RespEncoder::write_error(out, "ERR WAL write failed");
                return;
            }
        }
        RespEncoder::write_simple_string(out, "OK");
    } else {
        RespEncoder::write_error(out, "ERR hash value too large for inline storage");
    }
}

/// Handle `HINCRBY key field delta` — increment hash field by delta.
fn cmd_hincrby<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(field), Some(delta_bytes)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    let delta: i64 = match std::str::from_utf8(delta_bytes).ok().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => { RespEncoder::write_error(out, "ERR value is not an integer"); return; }
    };

    let mut current = get_current_value(key, ctx);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        if list::is_list_value(&current) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    // Get current field value (or "0" if not exists).
    let cur_val = hash::hash_get(&current, field).unwrap_or_else(|| b"0".to_vec());
    let cur_num: i64 = match std::str::from_utf8(&cur_val).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => {
            RespEncoder::write_error(out, "ERR hash value is not an integer");
            return;
        }
    };

    let new_val = match cur_num.checked_add(delta) {
        Some(v) => v,
        None => {
            RespEncoder::write_error(out, "ERR increment or decrement would overflow");
            return;
        }
    };

    let new_val_bytes = new_val.to_string().into_bytes();

    match hash::hash_set(&current, field, &new_val_bytes) {
        Ok(new_data) => {
            current = new_data;
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

    if ctx.store.set(key, &current) {
        if let Some(w) = ctx.wal {
            if w.wal_set(key, &current).is_err() {
                RespEncoder::write_error(out, "ERR WAL write failed");
                return;
            }
        }
        RespEncoder::write_integer(out, new_val);
    } else {
        RespEncoder::write_error(out, "ERR hash value too large for inline storage");
    }
}

/// Handle `HSETNX key field value` — set hash field only if field doesn't exist.
fn cmd_hsetnx<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(field), Some(value)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    let mut current = get_current_value(key, ctx);

    if !current.is_empty() && !hash::is_hash_value(&current) {
        if list::is_list_value(&current) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    // If field already exists, return 0.
    if hash::hash_exists(&current, field) {
        out.extend_from_slice(b":0\r\n");
        return;
    }

    // Field doesn't exist — set it.
    match hash::hash_set(&current, field, value) {
        Ok(new_data) => {
            current = new_data;
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

    if ctx.store.set(key, &current) {
        if let Some(w) = ctx.wal {
            if w.wal_set(key, &current).is_err() {
                RespEncoder::write_error(out, "ERR WAL write failed");
                return;
            }
        }
        out.extend_from_slice(b":1\r\n");
    } else {
        RespEncoder::write_error(out, "ERR hash value too large for inline storage");
    }
}

// ---------------------------------------------------------------------------
// List commands
// ---------------------------------------------------------------------------

/// Handle `LPUSH key element [element ...]`.
fn cmd_lpush<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
        Ok(len) => {
            // Persist to WAL so list state survives restarts.
            if let Some(w) = ctx.wal {
                let payload = list::encode_list_push(list::ListSubOp::LPush, &elements);
                let _ = w.wal_list_op(key, &payload);
            }
            RespEncoder::write_integer(out, len as i64);
        }
        Err(_) => RespEncoder::write_error(out, WRONGTYPE_ERR),
    }
}

/// Handle `RPUSH key element [element ...]`.
fn cmd_rpush<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
        Ok(len) => {
            // Persist to WAL so list state survives restarts.
            if let Some(w) = ctx.wal {
                let payload = list::encode_list_push(list::ListSubOp::RPush, &elements);
                let _ = w.wal_list_op(key, &payload);
            }
            RespEncoder::write_integer(out, len as i64);
        }
        Err(_) => RespEncoder::write_error(out, WRONGTYPE_ERR),
    }
}

/// Handle `LPOP key [count]`.
fn cmd_lpop<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check type: LPOP only works on list keys (or missing keys → nil).
    if let Some(val) = ctx.store.get(key) {
        if !list::is_list_value(&val) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    let count: usize = cmd.arg(2)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let elements = lists.lpop(key, count);

    // Persist to WAL only if we actually popped something.
    if !elements.is_empty() {
        if let Some(w) = ctx.wal {
            let payload = list::encode_list_pop(list::ListSubOp::LPop, elements.len());
            let _ = w.wal_list_op(key, &payload);
        }
    }

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
fn cmd_rpop<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check type: RPOP only works on list keys (or missing keys → nil).
    if let Some(val) = ctx.store.get(key) {
        if !list::is_list_value(&val) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    let count: usize = cmd.arg(2)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let elements = lists.rpop(key, count);

    // Persist to WAL only if we actually popped something.
    if !elements.is_empty() {
        if let Some(w) = ctx.wal {
            let payload = list::encode_list_pop(list::ListSubOp::RPop, elements.len());
            let _ = w.wal_list_op(key, &payload);
        }
    }

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
fn cmd_lrange<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_llen<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Check type: LLEN only works on list keys (or missing keys → 0).
    if let Some(val) = ctx.store.get(key) {
        if !list::is_list_value(&val) {
            RespEncoder::write_error(out, WRONGTYPE_ERR);
            return;
        }
    }

    RespEncoder::write_integer(out, lists.llen(key) as i64);
}

/// Handle `LINDEX key index`.
fn cmd_lindex<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
fn cmd_lrem<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let (Some(key), Some(count_s), Some(element)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    let count: i64 = std::str::from_utf8(count_s).ok().and_then(|s| s.parse().ok()).unwrap_or(0);

    let removed = lists.lrem(key, count, element);
    // Persist to WAL only if we actually removed something.
    if removed > 0 {
        if let Some(w) = ctx.wal {
            let payload = list::encode_list_rem(count, element);
            let _ = w.wal_list_op(key, &payload);
        }
    }
    RespEncoder::write_integer(out, removed as i64);
}

/// Handle `LTRIM key start stop`.
fn cmd_ltrim<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
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
    // Persist to WAL.
    if let Some(w) = ctx.wal {
        let payload = list::encode_list_trim(start, stop);
        let _ = w.wal_list_op(key, &payload);
    }
    RespEncoder::write_simple_string(out, "OK");
}

/// Handle `LSET key index element`.
fn cmd_lset<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(lists) = ctx.lists else {
        RespEncoder::write_error(out, "ERR lists not enabled");
        return;
    };
    let (Some(key), Some(idx_s), Some(element)) = (cmd.key(), cmd.arg(2), cmd.arg(3)) else {
        return err_wrong_args(out);
    };

    let index: i64 = std::str::from_utf8(idx_s).ok().and_then(|s| s.parse().ok()).unwrap_or(0);

    if lists.lset(key, index, element) {
        // Persist to WAL.
        if let Some(w) = ctx.wal {
            let payload = list::encode_list_set(index, element);
            let _ = w.wal_list_op(key, &payload);
        }
        RespEncoder::write_simple_string(out, "OK");
    } else {
        RespEncoder::write_error(out, "ERR index out of range");
    }
}

// ---------------------------------------------------------------------------
// Blob commands (feature-gated behind blob-store)
// ---------------------------------------------------------------------------

#[cfg(feature = "blob-store")]
/// Handle `BSET key value` — compress and store in blob arena, store ref in hash table.
fn cmd_bset<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let (Some(key), Some(value)) = (cmd.key(), cmd.value()) else { return err_wrong_args(out); };

    let Some(blob_arena) = ctx.blob else {
        RespEncoder::write_error(out, "ERR blob store not enabled");
        return;
    };

    // If the key currently holds a blob ref, free the old slot.
    if let Some(old_val) = ctx.store.get(key) {
        if crate::core::blob::BlobArena::is_blob_ref(&old_val) {
            if let Some(old_ref) = crate::core::blob::BlobRef::decode(&old_val) {
                blob_arena.free(&old_ref);
            }
        }
    }

    // If the key held a list, clean up the list data.
    if let Some(lists) = ctx.lists {
        if list::is_list_value(&ctx.store.get(key).unwrap_or_default()) {
            lists.remove_key(key);
        }
    }

    // Remove old TTL if overwriting.
    if let Some(exp) = ctx.expiry {
        exp.remove(key);
    }

    // Store in blob arena.
    let Some(blob_ref) = blob_arena.store(value) else {
        RespEncoder::write_error(out, "ERR blob store failed");
        return;
    };

    // Store the blob ref in the hash table.
    let encoded = blob_ref.encode();
    if ctx.store.set(key, &encoded) {
        if let Some(w) = ctx.wal {
            // Write the ORIGINAL uncompressed value to the WAL so that the
            // blob arena can be reconstructed on recovery.
            if w.wal_bset(key, value).is_err() {
                out.extend_from_slice(b"-ERR WAL write failed\r\n");
                return;
            }
        }
        out.extend_from_slice(b"+OK\r\n");
    } else {
        // Failed to store ref inline — free the blob slot.
        blob_arena.free(&blob_ref);
        RespEncoder::write_error(out, "ERR blob ref too large for inline storage");
    }
}

#[cfg(feature = "blob-store")]
/// Handle `BGET key` — retrieve and decompress a blob value.
fn cmd_bget<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Lazy expiration check.
    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            RespEncoder::write_null(out);
            return;
        }
    }

    let Some(blob_arena) = ctx.blob else {
        RespEncoder::write_error(out, "ERR blob store not enabled");
        return;
    };

    let Some(v) = ctx.store.get(key) else {
        RespEncoder::write_null(out);
        return;
    };

    // Reject hash/list values.
    if hash::is_hash_value(&v) || list::is_list_value(&v) {
        RespEncoder::write_error(out, WRONGTYPE_ERR);
        return;
    }

    if !crate::core::blob::BlobArena::is_blob_ref(&v) {
        // Not a blob ref — return as-is (plain string).
        RespEncoder::write_bulk_string(out, &v);
        return;
    }

    let Some(blob_ref) = crate::core::blob::BlobRef::decode(&v) else {
        RespEncoder::write_error(out, "ERR invalid blob reference");
        return;
    };

    match blob_arena.retrieve(&blob_ref) {
        Some(data) => RespEncoder::write_bulk_string(out, &data),
        None => RespEncoder::write_error(out, "ERR blob decompression failed"),
    }
}

#[cfg(feature = "blob-store")]
/// Handle `BGETRAW key` — return compressed bytes as-is.
fn cmd_bgetraw<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Lazy expiration check.
    if let Some(exp) = ctx.expiry {
        if exp.check_and_purge_if_expired(key) {
            RespEncoder::write_null(out);
            return;
        }
    }

    let Some(blob_arena) = ctx.blob else {
        RespEncoder::write_error(out, "ERR blob store not enabled");
        return;
    };

    let Some(v) = ctx.store.get(key) else {
        RespEncoder::write_null(out);
        return;
    };

    if !crate::core::blob::BlobArena::is_blob_ref(&v) {
        RespEncoder::write_error(out, "ERR not a blob reference");
        return;
    }

    let Some(blob_ref) = crate::core::blob::BlobRef::decode(&v) else {
        RespEncoder::write_error(out, "ERR invalid blob reference");
        return;
    };

    match blob_arena.retrieve_raw(&blob_ref) {
        Some(data) => RespEncoder::write_bulk_string(out, &data),
        None => RespEncoder::write_error(out, "ERR blob read failed"),
    }
}

#[cfg(feature = "blob-store")]
/// Handle `BSTATS` — return blob arena statistics.
fn cmd_bstats<const N: usize>(ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(blob_arena) = ctx.blob else {
        RespEncoder::write_error(out, "ERR blob store not enabled");
        return;
    };

    let stats = blob_arena.stats();
    RespEncoder::write_bulk_string(out, stats.to_string().as_bytes());
}

// ===========================================================================
// Scan / Stats commands (SCAN, DBSTATS)
// ===========================================================================

/// Handle `SCAN cursor [COUNT n] [MATCH pattern]` — cursor-based key iteration.
///
/// Returns a RESP array: `[next_cursor, [key1, key2, ...]]`.
/// When `next_cursor` is `"0"`, the iteration is complete.
fn cmd_scan<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    // SCAN cursor [COUNT count] [MATCH pattern]
    if cmd.argc() < 2 {
        return err_wrong_args(out);
    }

    // Parse cursor.
    let cursor = match cmd.arg(1) {
        Some(b"0") => 0,
        Some(bytes) => {
            match std::str::from_utf8(bytes)
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                Some(c) => c,
                None => {
                    RespEncoder::write_error(out, "ERR invalid cursor");
                    return;
                }
            }
        }
        None => 0,
    };

    // Parse optional COUNT and MATCH.
    let mut count: usize = 10;
    let mut pattern: Option<Vec<u8>> = None;

    let mut i = 2;
    while i < cmd.argc() {
        if let Some(arg) = cmd.arg(i) {
            if arg.eq_ignore_ascii_case(b"COUNT") {
                i += 1;
                if let Some(cnt_bytes) = cmd.arg(i) {
                    count = std::str::from_utf8(cnt_bytes)
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(10);
                }
            } else if arg.eq_ignore_ascii_case(b"MATCH") {
                i += 1;
                if let Some(pat) = cmd.arg(i) {
                    pattern = Some(pat.to_vec());
                }
            }
        }
        i += 1;
    }

    let pat_ref = pattern.as_deref();
    let (next_cursor, keys) = ctx.store.scan(cursor, count, pat_ref);

    // RESP response: array of [next_cursor, [key1, key2, ...]]
    RespEncoder::write_array_len(out, 2);
    let cursor_str = next_cursor.to_string();
    RespEncoder::write_bulk_string(out, cursor_str.as_bytes());
    RespEncoder::write_array_len(out, keys.len() as i64);
    for key in &keys {
        RespEncoder::write_bulk_string(out, key);
    }
}

/// Handle `DBSTATS` — return aggregate store statistics.
///
/// Returns a RESP array of key-value pairs with store metrics.
fn cmd_dbstats<const N: usize>(ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let stats = ctx.store.dbstats();

    // Pre-compute all string values to satisfy borrow checker.
    let total_keys = stats.total_keys.to_string();
    let total_buckets = stats.total_buckets.to_string();
    let load_factor = format!("{:.4}", stats.load_factor);
    let entry_size = stats.entry_size.to_string();
    let total_memory = stats.total_memory.to_string();
    let blob_count = stats.blob_count.to_string();
    let inline_size = stats.inline_size.to_string();

    let pairs: &[(&[u8], &[u8])] = &[
        (b"total_keys", total_keys.as_bytes()),
        (b"total_buckets", total_buckets.as_bytes()),
        (b"load_factor", load_factor.as_bytes()),
        (b"entry_size", entry_size.as_bytes()),
        (b"total_memory", total_memory.as_bytes()),
        (b"blob_count", blob_count.as_bytes()),
        (b"inline_size", inline_size.as_bytes()),
    ];

    RespEncoder::write_array_len(out, (pairs.len() * 2) as i64);
    for (k, v) in pairs {
        RespEncoder::write_bulk_string(out, k);
        RespEncoder::write_bulk_string(out, v);
    }
}

// ===========================================================================
// Checkpoint command (BGSAVE / SAVE)
// ===========================================================================

/// Handle `BGSAVE` / `SAVE` — perform a checkpoint (compact the WAL).
///
/// Writes a new compact WAL containing only the current state and atomically
/// replaces the old WAL. Also reopens the live WAL writer so that subsequent
/// writes go to the new compact file.
fn cmd_bgsave<const N: usize>(ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(wal_path) = ctx.wal_path else {
        RespEncoder::write_error(out, "ERR BGSAVE failed: WAL path not available (WAL disabled?)");
        return;
    };

    match {
        #[cfg(feature = "blob-store")]
        {
            checkpoint::checkpoint(ctx.store, ctx.expiry, wal_path, ctx.lists, ctx.blob)
        }
        #[cfg(not(feature = "blob-store"))]
        {
            checkpoint::checkpoint(ctx.store, ctx.expiry, wal_path, ctx.lists)
        }
    } {
        Ok(count) => {
            // Reopen the live WAL writer so it writes to the new compact file.
            if let Some(w) = ctx.wal {
                if let Err(e) = w.wal_reopen() {
                    eprintln!("[BGSAVE] Warning: WAL reopen failed after checkpoint: {}", e);
                    // The checkpoint file was still written successfully,
                    // but the live writer is still pointing to the old file.
                    // New writes may be lost on crash until server restart.
                }
            }
            let msg = format!("BGSAVE: {} entries written to compact WAL", count);
            RespEncoder::write_simple_string(out, &msg);
        }
        Err(e) => {
            RespEncoder::write_error(out, &format!("ERR BGSAVE failed: {}", e));
        }
    }
}

// ===========================================================================
// Similarity commands (SIMHASH, FINDSIM, LSHADD, LSHREM)
// ===========================================================================

#[cfg(feature = "similarity")]
/// Handle `SIMHASH key` — compute SimHash for a stored value.
///
/// Returns the 64-bit SimHash as a hex string, or an error if the key
/// doesn't exist.
fn cmd_simhash<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    #[cfg(feature = "blob-store")]
    {
        let result = crate::core::lsh::compute_simhash_for_key(ctx.store, ctx.blob, key);
        match result {
            Some(hash) => {
                let hex = format!("{:016x}", hash);
                RespEncoder::write_bulk_string(out, hex.as_bytes());
            }
            None => RespEncoder::write_null(out),
        }
    }

    #[cfg(not(feature = "blob-store"))]
    {
        let result = crate::core::lsh::compute_simhash_for_key(ctx.store, key);
        match result {
            Some(hash) => {
                let hex = format!("{:016x}", hash);
                RespEncoder::write_bulk_string(out, hex.as_bytes());
            }
            None => RespEncoder::write_null(out),
        }
    }
}

#[cfg(feature = "similarity")]
/// Handle `FINDSIM key [threshold]` — find similar keys via LSH.
///
/// Looks up the SimHash for *key*, then queries LSH buckets to find
/// candidate similar profiles. The optional *threshold* parameter
/// specifies the maximum Hamming distance (default 3).
///
/// Returns a RESP array of similar key names.
fn cmd_findsim<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    // Parse optional threshold.
    let threshold = if cmd.argc() > 2 {
        match cmd.arg(2) {
            Some(bytes) => std::str::from_utf8(bytes)
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(crate::core::simhash::DEFAULT_HAMMING_THRESHOLD),
            None => crate::core::simhash::DEFAULT_HAMMING_THRESHOLD,
        }
    } else {
        crate::core::simhash::DEFAULT_HAMMING_THRESHOLD
    };

    // Get the stored SimHash for this key.
    let simhash_val = crate::core::lsh::get_simhash_for_profile(ctx.store, key);
    let Some(simhash_val) = simhash_val else {
        // Key not indexed — compute on the fly.
        #[cfg(feature = "blob-store")]
        {
            let Some(hash) = crate::core::lsh::compute_simhash_for_key(ctx.store, ctx.blob, key) else {
                RespEncoder::write_empty_array(out);
                return;
            };
            let candidates = crate::core::lsh::find_similar_sim(ctx.store, hash, 4, threshold);
            write_candidate_array(&candidates, out);
            return;
        }
        #[cfg(not(feature = "blob-store"))]
        {
            let Some(hash) = crate::core::lsh::compute_simhash_for_key(ctx.store, key) else {
                RespEncoder::write_empty_array(out);
                return;
            };
            let candidates = crate::core::lsh::find_similar_sim(ctx.store, hash, 4, threshold);
            write_candidate_array(&candidates, out);
            return;
        }
    };

    let candidates = crate::core::lsh::find_similar_sim(ctx.store, simhash_val, 4, threshold);
    write_candidate_array(&candidates, out);
}

#[cfg(feature = "similarity")]
/// Handle `LSHADD key [simhash_hex]` — index a key in LSH buckets.
///
/// If *simhash_hex* is provided, it is used directly. Otherwise, the
/// SimHash is computed from the key's value.
///
/// Returns the number of band entries created.
fn cmd_lshadd<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let simhash_val = if cmd.argc() > 2 {
        // Explicit SimHash provided as hex string.
        match cmd.arg(2) {
            Some(hex_bytes) => {
                let hex_str = std::str::from_utf8(hex_bytes).unwrap_or("");
                u64::from_str_radix(hex_str, 16).unwrap_or(0)
            }
            None => 0,
        }
    } else {
        // Compute SimHash from the stored value.
        #[cfg(feature = "blob-store")]
        {
            match crate::core::lsh::compute_simhash_for_key(ctx.store, ctx.blob, key) {
                Some(h) => h,
                None => {
                    RespEncoder::write_error(out, "ERR key not found");
                    return;
                }
            }
        }
        #[cfg(not(feature = "blob-store"))]
        {
            match crate::core::lsh::compute_simhash_for_key(ctx.store, key) {
                Some(h) => h,
                None => {
                    RespEncoder::write_error(out, "ERR key not found");
                    return;
                }
            }
        }
    };

    // Store the SimHash value for later FINDSIM verification.
    crate::core::lsh::store_simhash_for_profile(ctx.store, key, simhash_val);

    // Add to LSH buckets.
    let count = crate::core::lsh::lsh_add_sim(ctx.store, key, simhash_val, 4);
    RespEncoder::write_integer(out, count as i64);
}

#[cfg(feature = "similarity")]
/// Handle `LSHREM key [simhash_hex]` — remove a key from LSH buckets.
///
/// If *simhash_hex* is provided, it is used directly. Otherwise, the
/// stored SimHash metadata is looked up.
///
/// Returns the number of band entries removed.
fn cmd_lshrem<const N: usize>(cmd: &crate::core::resp::Command, ctx: &ServerContext<N>, out: &mut Vec<u8>) {
    let Some(key) = cmd.key() else { return err_wrong_args(out); };

    let simhash_val = if cmd.argc() > 2 {
        match cmd.arg(2) {
            Some(hex_bytes) => {
                let hex_str = std::str::from_utf8(hex_bytes).unwrap_or("");
                u64::from_str_radix(hex_str, 16).unwrap_or(0)
            }
            None => 0,
        }
    } else {
        // Look up stored SimHash.
        match crate::core::lsh::get_simhash_for_profile(ctx.store, key) {
            Some(h) => h,
            None => {
                RespEncoder::write_error(out, "ERR key not indexed (no stored SimHash)");
                return;
            }
        }
    };

    let count = crate::core::lsh::lsh_rem_sim(ctx.store, key, simhash_val, 4);
    RespEncoder::write_integer(out, count as i64);
}

#[cfg(feature = "similarity")]
/// Write a list of candidate IDs as a RESP array.
fn write_candidate_array(candidates: &[Vec<u8>], out: &mut Vec<u8>) {
    RespEncoder::write_array_len(out, candidates.len() as i64);
    for id in candidates {
        RespEncoder::write_bulk_string(out, id);
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
        let server: TokioServer = TokioServer::new();
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
        let store: KvStoreLockFree = KvStoreLockFree::with_capacity(100);
        let authenticated = AtomicBool::new(false);
        let ctx = ServerContext {
            store: &store,
            wal: None,
            expiry: None,
            lists: None,
            #[cfg(feature = "blob-store")]
            blob: None,
            wal_path: None,
            password: None,
            authenticated: &authenticated,
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
        let store: KvStoreLockFree = KvStoreLockFree::with_capacity(100);
        let authenticated = AtomicBool::new(false);
        let ctx = ServerContext { store: &store, wal: None, expiry: None, lists: None,
            #[cfg(feature = "blob-store")]
            blob: None,
            wal_path: None,
            password: None,
            authenticated: &authenticated,
        };
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
        let store: Arc<KvStoreLockFree> = Arc::new(KvStoreLockFree::with_capacity(100));
        let lists = ListManager::new(Arc::clone(&store));
        let authenticated = AtomicBool::new(false);
        let ctx = ServerContext {
            store: &store,
            wal: None,
            expiry: None,
            lists: Some(&lists),
            #[cfg(feature = "blob-store")]
            blob: None,
            wal_path: None,
            password: None,
            authenticated: &authenticated,
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
        let store: Arc<KvStoreLockFree> = Arc::new(KvStoreLockFree::with_capacity(100));
        let lists = ListManager::new(Arc::clone(&store));
        let authenticated = AtomicBool::new(false);
        let ctx = ServerContext {
            store: &store,
            wal: None,
            expiry: None,
            lists: Some(&lists),
            #[cfg(feature = "blob-store")]
            blob: None,
            wal_path: None,
            password: None,
            authenticated: &authenticated,
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
        let store: Arc<KvStoreLockFree> = Arc::new(KvStoreLockFree::with_capacity(100));
        let lists = ListManager::new(Arc::clone(&store));
        let authenticated = AtomicBool::new(false);
        let ctx = ServerContext {
            store: &store,
            wal: None,
            expiry: None,
            lists: Some(&lists),
            #[cfg(feature = "blob-store")]
            blob: None,
            wal_path: None,
            password: None,
            authenticated: &authenticated,
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
