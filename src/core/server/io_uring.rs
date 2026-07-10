//! io_uring Server (Linux only).
//!
//! Uses `tokio-uring` for zero-copy, async I/O with io_uring.
//! Command processing is delegated to the same
//! [`process_command_into`] function as the Tokio server so that
//! behaviour is identical regardless of the I/O backend.
//!
//! Pub/Sub is fully supported on the io_uring backend via the same
//! writer-task + forwarder-task architecture as the Tokio backend:
//! the reader task parses commands and pushes replies into an mpsc
//! channel, a dedicated writer task drains that channel into the socket,
//! and each `SUBSCRIBE` spawns a forwarder that pumps `broadcast::Receiver`
//! events into the same mpsc channel. The socket is wrapped in `Arc` so
//! both tasks can share it (tokio-uring's `TcpStream::read` and
//! `write_all` take `&self`).
//!
//! Enable with: `cargo run --release --features io-uring -- server 6380 io_uring`

use crate::core::expiration::ExpirationManager;
use crate::core::kv::{KvStoreLockFree, DEFAULT_INLINE_SIZE};
use crate::core::list::ListManager;
use crate::core::pubsub::PubSubRegistry;
use crate::core::server::tcp::{
    build_ctx, handle_pubsub_command, is_pubsub_command, parse_command_bounds,
    process_buffer,
};
use crate::core::wal::WalWriter;
use crate::core::sortedset::SortedSetStore;

use crate::core::blob::BlobArena;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Default listen port.
const DEFAULT_PORT: u16 = 6379;

/// Per-connection read buffer size (1 MiB).
const MAX_BUFFER_SIZE: usize = 1024 * 1024;

/// io_uring-based TCP server.
pub struct IoUringServer<const N: usize = DEFAULT_INLINE_SIZE> {
    store: Arc<KvStoreLockFree<N>>,
    wal: Option<Arc<dyn WalWriter>>,
    expiry: Option<Arc<ExpirationManager<N>>>,
    lists: Option<Arc<ListManager<N>>>,
    sorted_sets: Option<Arc<SortedSetStore>>,
    pubsub: Option<Arc<PubSubRegistry>>,
    blob: Option<Arc<BlobArena>>,
    wal_path: Option<std::path::PathBuf>,
    password: Option<String>,
    max_connections: u32,
    active_connections: Arc<AtomicU32>,
    host: String,
    pub port: u16,
}

impl<const N: usize> IoUringServer<N> {
    /// Create a server with default settings (port 6379).
    pub fn new() -> Self {
        Self {
            store: Arc::new(KvStoreLockFree::<N>::with_capacity(1000)),
            wal: None,
            expiry: None,
            lists: None,
            sorted_sets: None,
            pubsub: None,
            blob: None,
            wal_path: None,
            password: None,
            max_connections: 10000,
            active_connections: Arc::new(AtomicU32::new(0)),
            host: "0.0.0.0".into(),
            port: DEFAULT_PORT,
        }
    }

    /// Create a server that listens on *port*.
    pub fn with_port(port: u16) -> Self {
        Self {
            store: Arc::new(KvStoreLockFree::<N>::with_capacity(1000)),
            wal: None,
            expiry: None,
            lists: None,
            sorted_sets: None,
            pubsub: None,
            blob: None,
            wal_path: None,
            password: None,
            max_connections: 10000,
            active_connections: Arc::new(AtomicU32::new(0)),
            host: "0.0.0.0".into(),
            port,
        }
    }

    /// Create a fully configured server with all subsystems wired in.
    #[allow(clippy::too_many_arguments)]
    pub fn with_components(
        port: u16,
        host: String,
        store: Arc<KvStoreLockFree<N>>,
        wal: Option<Arc<dyn WalWriter>>,
        expiry: Option<Arc<ExpirationManager<N>>>,
        lists: Option<Arc<ListManager<N>>>,
        sorted_sets: Option<Arc<SortedSetStore>>,
        pubsub: Option<Arc<PubSubRegistry>>,
        blob: Option<Arc<BlobArena>>,
        wal_path: Option<std::path::PathBuf>,
        password: Option<String>,
        max_connections: u32,
    ) -> Self {
        Self {
            store, wal, expiry, lists, sorted_sets, pubsub,
            blob, wal_path, password, max_connections,
            active_connections: Arc::new(AtomicU32::new(0)),
            host, port,
        }
    }

    /// Block the calling thread running the io_uring event loop.
    pub fn run(&self) -> Result<(), String> {
        tokio_uring::start(self.run_inner())
    }

    async fn run_inner(&self) -> Result<(), String> {
        use tokio_uring::net::TcpListener;

        let addr: SocketAddr = format!("{}:{}", self.host, self.port)
            .parse()
            .map_err(|e| format!("invalid address '{}:{}': {}", self.host, self.port, e))?;

        let listener = match TcpListener::bind(addr) {
            Ok(l) => l,
            Err(e) => {
                let msg = format!("Failed to bind to {}: {}", addr, e);
                eprintln!("{}", msg);
                return Err(msg);
            }
        };

        println!("FastKV io_uring server listening on {}", addr);
        println!("Using io_uring for maximum performance!");

        let wal = self.wal.clone();
        let expiry = self.expiry.clone();
        let lists = self.lists.clone();
        let sorted_sets = self.sorted_sets.clone();
        let pubsub = self.pubsub.clone();
        let blob = self.blob.clone();
        let wal_path = self.wal_path.clone();
        let password = self.password.clone();
        let max_connections = self.max_connections;
        let active_connections = Arc::clone(&self.active_connections);

        loop {
            let (stream, client_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                    continue;
                }
            };

            // Check connection limit before spawning.
            if active_connections.load(Ordering::Relaxed) >= max_connections {
                let _ = stream.write_all(&b"-ERR max number of clients reached\r\n"[..]).await;
                continue;
            }

            // Increment active connections; the spawned task will decrement via guard.
            active_connections.fetch_add(1, Ordering::Relaxed);

            let store = Arc::clone(&self.store);
            let wal = wal.clone();
            let expiry = expiry.clone();
            let lists = lists.clone();
            let sorted_sets = sorted_sets.clone();
            let pubsub = pubsub.clone();
            let blob = blob.clone();
            let wal_path = wal_path.clone();
            let password = password.clone();
            let active_conn = Arc::clone(&active_connections);

            tokio_uring::spawn(async move {
                // RAII guard: decrements active_connections when the task exits.
                struct ConnGuard { conn: Arc<AtomicU32> }
                impl Drop for ConnGuard {
                    fn drop(&mut self) { self.conn.fetch_sub(1, Ordering::Relaxed); }
                }
                let _guard = ConnGuard { conn: active_conn };

                handle_client(stream, store, wal, expiry, lists, sorted_sets, pubsub, blob, wal_path, client_addr, password).await;
            });
        }
        // loop is infinite (accept runs forever until shutdown)
        // Ok(()) unreachable but kept for type signature
        #[allow(unreachable_code)]
        Ok(())
    }
}

/// Per-connection state and main read/write loop (io_uring edition).
///
/// Architecture mirrors the Tokio backend:
/// - **Reader task** (this function): owns `Arc<TcpStream>`, reads from the
///   socket, dispatches commands. Non-pubsub commands are batched and
///   processed inline (io_uring runs everything on a single-threaded
///   runtime, so spawn_blocking is not used here — WAL writes are short
///   enough not to starve the loop). Pubsub commands are handled inline
///   via the shared [`handle_pubsub_command`] helper.
/// - **Writer task**: owns another `Arc<TcpStream>` clone, drains the
///   outbound mpsc channel and flushes bytes to the socket.
/// - **Forwarder tasks**: one per `SUBSCRIBE`; pumps `broadcast::Receiver`
///   events into the same outbound channel.
#[allow(clippy::too_many_arguments)]
async fn handle_client<const N: usize>(
    stream: tokio_uring::net::TcpStream,
    store: Arc<KvStoreLockFree<N>>,
    wal: Option<Arc<dyn WalWriter>>,
    expiry: Option<Arc<ExpirationManager<N>>>,
    lists: Option<Arc<ListManager<N>>>,
    sorted_sets: Option<Arc<SortedSetStore>>,
    pubsub: Option<Arc<PubSubRegistry>>,
    blob: Option<Arc<BlobArena>>,
    wal_path: Option<std::path::PathBuf>,
    addr: SocketAddr,
    password: Option<String>,
) {
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    // tokio-uring's TcpStream exposes read and write as &self methods, so
    // both the reader and writer tasks can share the same stream via Arc.
    let stream = Arc::new(stream);
    let writer_stream = Arc::clone(&stream);

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<u8>>(1024);
    let mut subscriptions: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    // Writer task: sole owner of socket.write_all. Aborts on write error.
    let writer_handle: tokio::task::JoinHandle<()> = tokio_uring::spawn(async move {
        while let Some(buf) = outbound_rx.recv().await {
            let (result, _buf) = writer_stream.write_all(buf).await;
            if result.is_err() {
                break;
            }
        }
    });

    let mut read_buf = vec![0u8; MAX_BUFFER_SIZE];
    let mut leftover: Vec<u8> = Vec::with_capacity(4096);
    let authenticated = Arc::new(AtomicBool::new(false));

    loop {
        let (result, buf) = stream.read(read_buf).await;
        read_buf = buf;

        let n = match result {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                eprintln!("[{}] Read error: {}", addr, e);
                break;
            }
        };

        leftover.extend_from_slice(&read_buf[..n]);
        if leftover.is_empty() {
            continue;
        }

        // Walk one command at a time. Pub/Sub commands go through the
        // async helper; everything else is batched and processed inline
        // (single-threaded io_uring runtime — no spawn_blocking needed).
        let mut batch: Vec<u8> = Vec::with_capacity(leftover.len());
        let mut consumed = 0usize;
        let mut should_close = false;

        while consumed < leftover.len() {
            let Some((len, is_inline)) = parse_command_bounds(&leftover[consumed..]) else {
                break;
            };
            let cmd_bytes = leftover[consumed..consumed + len].to_vec();

            if is_pubsub_command(&cmd_bytes, is_inline) {
                // Flush any accumulated batch first so order is preserved.
                if !batch.is_empty() {
                    let ctx_owned = build_ctx::<N>(
                        &store, &wal, &expiry, &lists, &sorted_sets, &blob,
                        &wal_path, &password, &authenticated,
                    );
                    let batch_buf = std::mem::take(&mut batch);
                    let (resp, close, rem) = process_buffer(ctx_owned, batch_buf);
                    if !resp.is_empty() {
                        let _ = outbound_tx.send(resp).await;
                    }
                    should_close = should_close || close;
                    leftover = rem;
                    consumed = 0;
                }

                let close = handle_pubsub_command::<N>(
                    &cmd_bytes, is_inline, &pubsub, &password, &authenticated,
                    &outbound_tx, &mut subscriptions,
                ).await;
                if close { should_close = true; }
                consumed += len;
                continue;
            }

            batch.extend_from_slice(&cmd_bytes);
            consumed += len;
        }

        leftover.drain(..consumed);

        if !batch.is_empty() {
            let ctx_owned = build_ctx::<N>(
                &store, &wal, &expiry, &lists, &sorted_sets, &blob,
                &wal_path, &password, &authenticated,
            );
            let (resp, close, rem) = process_buffer(ctx_owned, batch);
            if !resp.is_empty() {
                let _ = outbound_tx.send(resp).await;
            }
            should_close = should_close || close;
            leftover = rem;
        }

        if should_close { break; }
    }

    // Tear down: drop subscriptions, close outbound channel, wait for writer.
    for (_, handle) in subscriptions.drain() {
        handle.abort();
    }
    drop(outbound_tx);
    let _ = writer_handle.await;
}

impl<const N: usize> Default for IoUringServer<N> {
    fn default() -> Self {
        Self::new()
    }
}
