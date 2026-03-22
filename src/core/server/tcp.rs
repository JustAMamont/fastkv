//! Optimized TCP Server with buffer pooling and zero-copy optimizations
//!
//! Performance improvements:
//! - Buffer pool (reuse allocations)
//! - Batched writes (fewer syscalls)
//! - Zero-copy parsing where possible

use crate::core::kv::KvStore;
use crate::core::resp::RespParser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Default port (same as Redis)
pub const DEFAULT_PORT: u16 = 6379;

/// Maximum buffer size (1MB)
const MAX_BUFFER_SIZE: usize = 1024 * 1024;

/// Initial response buffer size (avoid reallocations)
const INITIAL_RESPONSE_SIZE: usize = 4096;

/// TCP Server using Tokio async runtime
pub struct TokioServer {
    /// Shared KV store (lock-free, thread-safe)
    #[allow(dead_code)]
    store: Arc<KvStore>,
    /// Port to listen on
    pub port: u16,
}

impl TokioServer {
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

    pub fn store(&self) -> Arc<KvStore> {
        Arc::clone(&self.store)
    }

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
        println!("Try: redis-cli -p {}", self.port);
        println!();

        loop {
            let (socket, addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                    continue;
                }
            };

            let store = Arc::clone(&self.store);

            tokio::spawn(async move {
                handle_client_optimized(socket, store, addr).await;
            });
        }
    }
}

impl Default for TokioServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Optimized client handler with buffer reuse
async fn handle_client_optimized(
    mut socket: tokio::net::TcpStream, 
    store: Arc<KvStore>, 
    addr: std::net::SocketAddr
) {
    println!("[{}] Client connected", addr);

    // Reusable buffers (avoid allocations per request)
    let mut read_buffer = vec![0u8; MAX_BUFFER_SIZE];
    let mut leftover: Vec<u8> = Vec::with_capacity(4096);
    let mut response_buffer = Vec::with_capacity(INITIAL_RESPONSE_SIZE);

    loop {
        let n = match socket.read(&mut read_buffer).await {
            Ok(0) => {
                println!("[{}] Client disconnected", addr);
                break;
            }
            Ok(n) => n,
            Err(e) => {
                eprintln!("[{}] Read error: {}", addr, e);
                break;
            }
        };

        // Extend leftover with new data
        leftover.extend_from_slice(&read_buffer[..n]);

        // Clear response buffer for reuse (keep capacity)
        response_buffer.clear();

        // Process all complete commands
        let mut processed = 0;
        while processed < leftover.len() {
            match parse_command_bounds(&leftover[processed..]) {
                Some((consumed, is_inline)) => {
                    let cmd_data = &leftover[processed..processed + consumed];
                    process_command_into(cmd_data, &store, &mut response_buffer, is_inline);
                    processed += consumed;
                }
                None => break,
            }
        }

        // Keep unprocessed data
        leftover.drain(..processed);

        // Send all responses at once
        if !response_buffer.is_empty() {
            if let Err(e) = socket.write_all(&response_buffer).await {
                eprintln!("[{}] Write error: {}", addr, e);
                break;
            }
        }
    }
}

/// Parse command bounds, returns (bytes_consumed, is_inline)
pub fn parse_command_bounds(data: &[u8]) -> Option<(usize, bool)> {
    if data.is_empty() {
        return None;
    }

    match data[0] {
        b'*' => {
            let end = find_resp_array_end(data)?;
            Some((end, false))
        }
        // Inline commands
        b'G' | b'S' | b'D' | b'P' | b'I' | b'H' | b'L' | b'E' | b'C' | b'Q' | b'F' | b'K' | b'M' | b'T' => {
            // Find \n
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

/// Find end of RESP array (returns byte offset)
fn find_resp_array_end(data: &[u8]) -> Option<usize> {
    if data.len() < 4 || data[0] != b'*' {
        return None;
    }

    // Find first \r\n
    let mut pos = 1;
    while pos + 1 < data.len() {
        if data[pos] == b'\r' && data[pos + 1] == b'\n' {
            break;
        }
        pos += 1;
    }
    if pos + 1 >= data.len() {
        return None;
    }

    let count_str = std::str::from_utf8(&data[1..pos]).ok()?;
    let count: usize = count_str.parse().ok()?;
    pos += 2; // Skip \r\n

    // Parse each bulk string
    for _ in 0..count {
        if pos >= data.len() || data[pos] != b'$' {
            return None;
        }
        pos += 1;

        // Find \r\n after length
        let len_start = pos;
        while pos + 1 < data.len() {
            if data[pos] == b'\r' && data[pos + 1] == b'\n' {
                break;
            }
            pos += 1;
        }
        if pos + 1 >= data.len() {
            return None;
        }

        let len_str = std::str::from_utf8(&data[len_start..pos]).ok()?;
        let len: usize = len_str.parse().ok()?;
        pos += 2; // Skip \r\n

        // Check if we have the full string
        if pos + len + 2 > data.len() {
            return None;
        }
        pos += len + 2; // string + \r\n
    }

    Some(pos)
}

/// Process command and append response to buffer (zero-copy output)
pub fn process_command_into(data: &[u8], store: &KvStore, response: &mut Vec<u8>, is_inline: bool) {
    // For inline commands, we need to handle them specially
    if is_inline {
        // Find the end (before \r\n or \n)
        let line_end = data.iter().position(|&b| b == b'\r' || b == b'\n').unwrap_or(data.len());
        let line = &data[..line_end];
        
        // Parse inline: COMMAND arg1 arg2
        let parts: Vec<&[u8]> = line.split(|&b| b == b' ').collect();
        if parts.is_empty() {
            response.extend_from_slice(b"-ERR empty command\r\n");
            return;
        }

        let cmd_name = std::str::from_utf8(parts[0]).unwrap_or("").to_uppercase();
        
        match cmd_name.as_str() {
            "PING" => {
                response.extend_from_slice(b"+PONG\r\n");
            }
            "GET" if parts.len() > 1 => {
                match store.get(parts[1]) {
                    Some(v) => {
                        response.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        response.extend_from_slice(&v);
                        response.extend_from_slice(b"\r\n");
                    }
                    None => response.extend_from_slice(b"$-1\r\n"),
                }
            }
            "SET" if parts.len() > 2 => {
                if store.set(parts[1], parts[2]) {
                    response.extend_from_slice(b"+OK\r\n");
                } else {
                    response.extend_from_slice(b"-ERR set failed\r\n");
                }
            }
            "DEL" if parts.len() > 1 => {
                if store.del(parts[1]) {
                    response.extend_from_slice(b":1\r\n");
                } else {
                    response.extend_from_slice(b":0\r\n");
                }
            }
            _ => {
                response.extend_from_slice(b"-ERR unknown command\r\n");
            }
        }
        return;
    }

    // RESP array format - use parser
    let command = match RespParser::parse(data) {
        Ok(cmd) => cmd,
        Err(e) => {
            response.extend_from_slice(format!("-ERR parse error: {}\r\n", e).as_bytes());
            return;
        }
    };

    match command.name.as_str() {
        "GET" => {
            match command.key() {
                Some(key) => {
                    match store.get(key) {
                        Some(value) => {
                            response.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
                            response.extend_from_slice(&value);
                            response.extend_from_slice(b"\r\n");
                        }
                        None => response.extend_from_slice(b"$-1\r\n"),
                    }
                }
                None => response.extend_from_slice(b"-ERR wrong number of arguments\r\n"),
            }
        }

        "SET" => {
            match (command.key(), command.value()) {
                (Some(key), Some(value)) => {
                    if store.set(key, value) {
                        response.extend_from_slice(b"+OK\r\n");
                    } else {
                        response.extend_from_slice(b"-ERR set failed\r\n");
                    }
                }
                _ => response.extend_from_slice(b"-ERR wrong number of arguments\r\n"),
            }
        }

        "DEL" => {
            match command.key() {
                Some(key) => {
                    if store.del(key) {
                        response.extend_from_slice(b":1\r\n");
                    } else {
                        response.extend_from_slice(b":0\r\n");
                    }
                }
                None => response.extend_from_slice(b"-ERR wrong number of arguments\r\n"),
            }
        }

        "PING" => {
            if let Some(arg) = command.arg(1) {
                response.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
                response.extend_from_slice(arg);
                response.extend_from_slice(b"\r\n");
            } else {
                response.extend_from_slice(b"+PONG\r\n");
            }
        }

        "ECHO" => {
            match command.arg(1) {
                Some(msg) => {
                    response.extend_from_slice(format!("${}\r\n", msg.len()).as_bytes());
                    response.extend_from_slice(msg);
                    response.extend_from_slice(b"\r\n");
                }
                None => response.extend_from_slice(b"-ERR wrong number of arguments\r\n"),
            }
        }

        "COMMAND" => {
            response.extend_from_slice(b"+OK\r\n");
        }

        "INFO" => {
            let info = format!("# Server\nfastkv_version:0.1.0\n# Memory\nused_memory:{}\n", store.len() * 200);
            response.extend_from_slice(format!("${}\r\n", info.len()).as_bytes());
            response.extend_from_slice(info.as_bytes());
            response.extend_from_slice(b"\r\n");
        }

        "DBSIZE" => {
            response.extend_from_slice(format!(":{}\r\n", store.len()).as_bytes());
        }

        "QUIT" => {
            response.extend_from_slice(b"+OK\r\n");
        }

        _ => {
            response.extend_from_slice(format!("-ERR unknown command '{}'\r\n", command.name).as_bytes());
        }
    }
}

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
}