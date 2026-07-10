//! io_uring Server (Linux only).
//!
//! Uses `tokio-uring` for zero-copy, async I/O with io_uring.
//! Command processing is delegated to the same
//! [`process_command_into`] function as the Tokio server so that
//! behaviour is identical regardless of the I/O backend.
//!
//! Enable with: `cargo run --release --features io-uring -- server 6380 io_uring`

use crate::core::expiration::ExpirationManager;
use crate::core::kv::{KvStoreLockFree, DEFAULT_INLINE_SIZE};
use crate::core::list::ListManager;
use crate::core::server::tcp::{parse_command_bounds, process_command_into, ServerContext};
use crate::core::wal::WalWriter;

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
    ///
    /// Uses a small capacity (1K buckets) since no data is expected.
    /// For production use [`with_components`] to set `--capacity`.
    pub fn new() -> Self {
        Self {
            store: Arc::new(KvStoreLockFree::<N>::with_capacity(1000)),
            wal: None,
            expiry: None,
            lists: None,
            
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
    ///
    /// Uses a small capacity (1K buckets) since no data is expected.
    /// For production use [`with_components`] to set `--capacity`.
    pub fn with_port(port: u16) -> Self {
        Self {
            store: Arc::new(KvStoreLockFree::<N>::with_capacity(1000)),
            wal: None,
            expiry: None,
            lists: None,
            
            blob: None,
            wal_path: None,
            password: None,
            max_connections: 10000,
            active_connections: Arc::new(AtomicU32::new(0)),
            host: "0.0.0.0".into(),
            port,
        }
    }

    /// Create a fully configured server.
    #[allow(clippy::too_many_arguments)]
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
    ) -> Self {
        Self { store, wal, expiry, lists, blob, wal_path, password, max_connections, active_connections: Arc::new(AtomicU32::new(0)), host, port }
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

                
                handle_client(stream, store, wal, expiry, lists, blob, wal_path, client_addr, password).await;
            });
        }
        // loop is infinite (accept runs forever until shutdown)
        // Ok(()) unreachable but kept for type signature
        #[allow(unreachable_code)]
        Ok(())
    }
}

/// Per-connection state and main read/write loop (io_uring edition).
#[allow(clippy::too_many_arguments)]
async fn handle_client<const N: usize>(
    stream: tokio_uring::net::TcpStream,
    store: Arc<KvStoreLockFree<N>>,
    wal: Option<Arc<dyn WalWriter>>,
    expiry: Option<Arc<ExpirationManager<N>>>,
    lists: Option<Arc<ListManager<N>>>,
    blob: Option<Arc<BlobArena>>,
    wal_path: Option<std::path::PathBuf>,
    addr: SocketAddr,
    password: Option<String>,
) {
    let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
    let mut leftover: Vec<u8> = Vec::with_capacity(4096);
    let mut response: Vec<u8> = Vec::with_capacity(4096);
    let authenticated = Arc::new(AtomicBool::new(false));

    let ctx = ServerContext::<N> {
        store: &store,
        wal: wal.as_deref(),
        expiry: expiry.as_deref(),
        lists: lists.as_deref(),
        blob: blob.as_deref(),
        wal_path: wal_path.as_deref(),
        password: password.as_ref(),
        authenticated: &authenticated,
    };

    loop {
        let (result, buf) = stream.read(buffer).await;
        buffer = buf;

        let n = match result {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                eprintln!("[{}] Read error: {}", addr, e);
                break;
            }
        };

        leftover.extend_from_slice(&buffer[..n]);
        response.clear();

        let mut consumed = 0;
        let mut should_close = false;
        while consumed < leftover.len() {
            match parse_command_bounds(&leftover[consumed..]) {
                Some((len, is_inline)) => {
                    let cmd_data = &leftover[consumed..consumed + len];
                    should_close = process_command_into(
                        cmd_data,
                        &ctx,
                        &mut response,
                        is_inline,
                    );
                    consumed += len;
                    if should_close { break; }
                }
                None => break,
            }
        }
        leftover.drain(..consumed);

        if !response.is_empty() {
            let (result, resp) = stream.write_all(response).await;
            response = resp;
            if let Err(e) = result {
                eprintln!("[{}] Write error: {}", addr, e);
                break;
            }
        }
        if should_close { break; }
    }
}

impl<const N: usize> Default for IoUringServer<N> {
    fn default() -> Self {
        Self::new()
    }
}
