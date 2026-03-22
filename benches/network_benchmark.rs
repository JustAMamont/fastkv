//! Network Benchmark for FastKV
//!
//! Compare FastKV vs Redis over TCP (fair comparison!)
//!
//! Usage:
//!   # Start Redis
//!   docker run -d --name redis -p 6379:6379 redis
//!
//!   # Start FastKV
//!   cargo run --release -- server 6380
//!
//!   # Run benchmark
//!   cargo run --release --example netbench -- [redis|fastkv|both] [ops]
//!
//!   # Or with criterion:
//!   cargo bench --bench network_benchmark

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::io::{Read, Write};
use std::net::TcpStream;
//use std::time::Instant;

// ============================================================================
// RESP HELPERS
// ============================================================================

/// Build a RESP SET command
fn resp_set(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n", key.len()).into_bytes();
    cmd.extend_from_slice(key);
    cmd.extend_from_slice(b"\r\n");
    cmd.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
    cmd.extend_from_slice(value);
    cmd.extend_from_slice(b"\r\n");
    cmd
}

/// Build a RESP GET command
fn resp_get(key: &[u8]) -> Vec<u8> {
    let mut cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n", key.len()).into_bytes();
    cmd.extend_from_slice(key);
    cmd.extend_from_slice(b"\r\n");
    cmd
}

/// Read RESP response (simple parser)
fn read_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).expect("Failed to read response");
    buf[..n].to_vec()
}

// ============================================================================
// BENCHMARK CLIENT
// ============================================================================

struct BenchmarkClient {
    stream: TcpStream,
}

impl BenchmarkClient {
    fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr)
            .unwrap_or_else(|_| panic!("Failed to connect to {}", addr));
        stream.set_nodelay(true).ok(); // Disable Nagle's algorithm
        Self { stream }
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        let cmd = resp_set(key, value);
        self.stream.write_all(&cmd).expect("Failed to write SET");
        let _ = read_response(&mut self.stream);
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        let cmd = resp_get(key);
        self.stream.write_all(&cmd).expect("Failed to write GET");
        read_response(&mut self.stream)
    }

    //fn ping(&mut self) -> bool {
    //    self.stream.write_all(b"PING\r\n").ok();
    //    let resp = read_response(&mut self.stream);
    //    Some(resp.starts_with(b"+PONG") || resp.starts_with(b"$4")).is_some()
    //}
}

// ============================================================================
// CRITERION BENCHMARKS
// ============================================================================

fn benchmark_redis_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis_network_set");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut client = BenchmarkClient::connect("127.0.0.1:6379");
            
            b.iter(|| {
                for i in 0..size {
                    let key = format!("bench_key_{}", i);
                    let value = format!("bench_value_{}", i);
                    client.set(key.as_bytes(), value.as_bytes());
                }
            });
        });
    }
    
    group.finish();
}

fn benchmark_redis_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis_network_get");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut client = BenchmarkClient::connect("127.0.0.1:6379");
            
            // Pre-populate
            for i in 0..size {
                let key = format!("bench_key_{}", i);
                let value = format!("bench_value_{}", i);
                client.set(key.as_bytes(), value.as_bytes());
            }
            
            b.iter(|| {
                for i in 0..size {
                    let key = format!("bench_key_{}", i);
                    black_box(client.get(key.as_bytes()));
                }
            });
        });
    }
    
    group.finish();
}

fn benchmark_fastkv_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("fastkv_network_set");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut client = match BenchmarkClient::connect("127.0.0.1:6380") {
                c if client_ping_check(&c) => c,
                _ => {
                    // FastKV not running, skip this benchmark
                    return;
                }
            };
            
            b.iter(|| {
                for i in 0..size {
                    let key = format!("bench_key_{}", i);
                    let value = format!("bench_value_{}", i);
                    client.set(key.as_bytes(), value.as_bytes());
                }
            });
        });
    }
    
    group.finish();
}

fn benchmark_fastkv_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("fastkv_network_get");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut client = match BenchmarkClient::connect("127.0.0.1:6380") {
                c if client_ping_check(&c) => c,
                _ => return,
            };
            
            // Pre-populate
            for i in 0..size {
                let key = format!("bench_key_{}", i);
                let value = format!("bench_value_{}", i);
                client.set(key.as_bytes(), value.as_bytes());
            }
            
            b.iter(|| {
                for i in 0..size {
                    let key = format!("bench_key_{}", i);
                    black_box(client.get(key.as_bytes()));
                }
            });
        });
    }
    
    group.finish();
}

fn client_ping_check(_client: &BenchmarkClient) -> bool {
    // Can't actually ping here since we need &mut
    true
}

criterion_group!(
    redis_benches,
    benchmark_redis_set,
    benchmark_redis_get,
);

criterion_group!(
    fastkv_benches,
    benchmark_fastkv_set,
    benchmark_fastkv_get,
);

criterion_main!(redis_benches, fastkv_benches);
