//! io_uring Server (Linux only).
//!
//! Uses `tokio-uring` for zero-copy, async I/O with io_uring.
//! Command processing is delegated to the same
//! [`process_command_into`] function as the Tokio server so that
//! behaviour is identical regardless of the I/O backend.
//!
//! Enable with: `cargo run --release --features io-uring -- server 6380 io_uring`

use crate::core::expiration::ExpirationManager;
use crate::core::kv::KvStore;
use crate::core::list::ListManager;
use crate::core::server::tcp::{parse_command_bounds, process_command_into, ServerContext};
use crate::core::wal::Wal;
use std::net::SocketAddr;
use std::sync::Arc;

/// Default listen port.
const DEFAULT_PORT: u16 = 6379;

/// Per-connection read buffer size (1 MiB).
const MAX_BUFFER_SIZE: usize = 1024 * 1024;

/// io_uring-based TCP server.
pub struct IoUringServer {
    store: Arc<KvStore>,
    wal: Option<Arc<Wal>>,
    expiry: Option<Arc<ExpirationManager>>,
    lists: Option<Arc<ListManager>>,
    host: String,
    pub port: u16,
}

impl IoUringServer {
    /// Create a server with default settings (port 6379).
    pub fn new() -> Self {
        Self {
            store: Arc::new(KvStore::new()),
            wal: None,
            expiry: None,
            lists: None,
            host: "0.0.0.0".into(),
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
            host: "0.0.0.0".into(),
            port,
        }
    }

    /// Create a fully configured server.
    pub fn with_components(
        port: u16,
        host: String,
        store: Arc<KvStore>,
        wal: Option<Arc<Wal>>,
        expiry: Option<Arc<ExpirationManager>>,
        lists: Option<Arc<ListManager>>,
    ) -> Self {
        Self { store, wal, expiry, lists, host, port }
    }

    /// Block the calling thread running the io_uring event loop.
    pub fn run(&self) {
        tokio_uring::start(self.run_inner());
    }

    async fn run_inner(&self) {
        use tokio_uring::net::TcpListener;

        let addr: SocketAddr = format!("{}:{}", self.host, self.port)
            .parse()
            .expect("invalid address");

        let listener = match TcpListener::bind(addr) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Failed to bind: {}", e);
                return;
            }
        };

        println!("FastKV io_uring server listening on {}", addr);
        println!("Using io_uring for maximum performance!");

        let wal = self.wal.clone();
        let expiry = self.expiry.clone();
        let lists = self.lists.clone();

        loop {
            let (stream, client_addr) = match listener.accept().await {
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

            tokio_uring::spawn(async move {
                handle_client(stream, store, wal, expiry, lists, client_addr).await;
            });
        }
    }
}

/// Per-connection state and main read/write loop (io_uring edition).
async fn handle_client(
    stream: tokio_uring::net::TcpStream,
    store: Arc<KvStore>,
    wal: Option<Arc<Wal>>,
    expiry: Option<Arc<ExpirationManager>>,
    lists: Option<Arc<ListManager>>,
    addr: SocketAddr,
) {
    let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
    let mut leftover: Vec<u8> = Vec::with_capacity(4096);
    let mut response: Vec<u8> = Vec::with_capacity(4096);

    let ctx = ServerContext {
        store: &store,
        wal: wal.as_deref(),
        expiry: expiry.as_deref(),
        lists: lists.as_deref(),
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

impl Default for IoUringServer {
    fn default() -> Self {
        Self::new()
    }
}
