//! io_uring Server (Linux only)
//!
//! Uses tokio-uring for zero-copy, async I/O with io_uring.
//! This provides significant performance improvement on Linux.
//!
//! Enable with: cargo run --release --features io-uring -- server 6380 io_uring

use crate::core::kv::KvStore;
use crate::core::server::tcp::{parse_command_bounds, process_command_into};
use std::net::SocketAddr;
use std::sync::Arc;

const DEFAULT_PORT: u16 = 6379;
const MAX_BUFFER_SIZE: usize = 1024 * 1024;

pub struct IoUringServer {
    store: Arc<KvStore>,
    pub port: u16,
}

impl IoUringServer {
    pub fn new() -> Self {
        Self {
            store: Arc::new(KvStore::new()),
            port: DEFAULT_PORT,
        }
    }

    pub fn with_port(port: u16) -> Self {
        Self {
            store: Arc::new(KvStore::new()),
            port,
        }
    }

    pub fn run(&self) {
        tokio_uring::start(self.run_inner());
    }

    async fn run_inner(&self) {
        use tokio_uring::net::TcpListener;

        let addr: SocketAddr = format!("0.0.0.0:{}", self.port)
            .parse()
            .expect("Invalid address");
            
        let listener = match TcpListener::bind(addr) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Failed to bind: {}", e);
                return;
            }
        };

        println!("FastKV io_uring server listening on {}", addr);
        println!("Using io_uring for maximum performance!");

        loop {
            let (stream, client_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                    continue;
                }
            };

            let store = Arc::clone(&self.store);
            
            tokio_uring::spawn(async move {
                handle_client(stream, store, client_addr).await;
            });
        }
    }
}

async fn handle_client(
    stream: tokio_uring::net::TcpStream,
    store: Arc<KvStore>,
    addr: SocketAddr,
) {
    println!("[{}] Client connected (io_uring)", addr);

    let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
    let mut leftover: Vec<u8> = Vec::with_capacity(4096);
    let mut response = Vec::with_capacity(4096);

    // Need mutable stream for read/write
    let stream = stream;

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

        let mut processed = 0;
        while processed < leftover.len() {
            match parse_command_bounds(&leftover[processed..]) {
                Some((consumed, is_inline)) => {
                    let cmd_data = &leftover[processed..processed + consumed];
                    process_command_into(cmd_data, &store, &mut response, is_inline);
                    processed += consumed;
                }
                None => break,
            }
        }

        leftover.drain(..processed);

        if !response.is_empty() {
            let (result, buf) = stream.write_all(response).await;
            response = buf;
            
            if let Err(e) = result {
                eprintln!("[{}] Write error: {}", addr, e);
                break;
            }
        }
    }

    println!("[{}] Client disconnected", addr);
}

impl Default for IoUringServer {
    fn default() -> Self {
        Self::new()
    }
}
