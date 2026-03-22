//! Network Benchmark CLI with Table Output
//!
//! Compare FastKV vs Redis over TCP

use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Instant;

const REDIS_ADDR: &str = "127.0.0.1:6379";
const FASTKV_ADDR: &str = "127.0.0.1:6380";
const DEFAULT_OPS: usize = 100_000;

// ============================================================================
// RESP PROTOCOL
// ============================================================================

fn resp_set(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n", key.len()).into_bytes();
    cmd.extend_from_slice(key);
    cmd.extend_from_slice(b"\r\n");
    cmd.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
    cmd.extend_from_slice(value);
    cmd.extend_from_slice(b"\r\n");
    cmd
}

fn resp_get(key: &[u8]) -> Vec<u8> {
    let mut cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n", key.len()).into_bytes();
    cmd.extend_from_slice(key);
    cmd.extend_from_slice(b"\r\n");
    cmd
}

// ============================================================================
// CLIENT
// ============================================================================

struct Client {
    stream: TcpStream,
}

impl Client {
    fn connect(addr: &str) -> Result<Self, String> {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                stream.set_nodelay(true).ok();
                Ok(Self { stream })
            }
            Err(e) => Err(format!("Failed to connect to {}: {}", addr, e)),
        }
    }

    fn send(&mut self, data: &[u8]) {
        self.stream.write_all(data).expect("Write failed");
    }

    fn recv(&mut self) -> Vec<u8> {
        let mut buf = [0u8; 65536];
        let n = self.stream.read(&mut buf).expect("Read failed");
        buf[..n].to_vec()
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.send(&resp_set(key, value));
        self.recv();
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        self.send(&resp_get(key));
        self.recv()
    }

    fn ping(&mut self) -> bool {
        self.send(b"PING\r\n");
        let resp = self.recv();
        resp.starts_with(b"+PONG") || resp.starts_with(b"$4")
    }

    fn pipeline_set(&mut self, keys: &[Vec<u8>], values: &[Vec<u8>]) {
        let mut batch = Vec::with_capacity(keys.len() * 64);
        for (k, v) in keys.iter().zip(values.iter()) {
            batch.extend_from_slice(&resp_set(k, v));
        }
        self.send(&batch);
        let mut total = 0;
        while total < keys.len() * 5 {
            let mut buf = [0u8; 65536];
            let n = self.stream.read(&mut buf).expect("Read failed");
            if n == 0 { break; }
            total += buf[..n].windows(2).filter(|w| *w == b"\r\n").count();
            if total >= keys.len() { break; }
        }
    }

    fn pipeline_get(&mut self, keys: &[Vec<u8>]) {
        let mut batch = Vec::with_capacity(keys.len() * 32);
        for k in keys {
            batch.extend_from_slice(&resp_get(k));
        }
        self.send(&batch);
        let mut total = 0;
        while total < keys.len() * 5 {
            let mut buf = [0u8; 65536];
            let n = self.stream.read(&mut buf).expect("Read failed");
            if n == 0 { break; }
            total += buf[..n].windows(2).filter(|w| *w == b"\r\n").count();
            if total >= keys.len() { break; }
        }
    }
}

// ============================================================================
// RESULTS STORAGE
// ============================================================================

#[derive(Clone)]
struct BenchmarkResult {
    name: String,
    set_ops: f64,
    get_ops: f64,
    mix_ops: f64,
}

// ============================================================================
// BENCHMARKS
// ============================================================================

fn benchmark_sequential(addr: &str, name: &str, ops: usize) -> Option<BenchmarkResult> {
    let mut client = Client::connect(addr).ok()?;
    
    if !client.ping() {
        return None;
    }

    for i in 0..500 {
        let key = format!("warm:{}", i);
        client.set(key.as_bytes(), b"warmup");
    }

    let start = Instant::now();
    for i in 0..ops {
        let key = format!("seq:key:{}", i);
        let value = format!("seq:value:{}", i);
        client.set(key.as_bytes(), value.as_bytes());
    }
    let set_ops = ops as f64 / start.elapsed().as_secs_f64();

    let start = Instant::now();
    for i in 0..ops {
        let key = format!("seq:key:{}", i);
        client.get(key.as_bytes());
    }
    let get_ops = ops as f64 / start.elapsed().as_secs_f64();

    let start = Instant::now();
    for i in 0..ops {
        let key = format!("mix:key:{}", i);
        if i % 2 == 0 {
            client.get(key.as_bytes());
        } else {
            client.set(key.as_bytes(), b"mixvalue");
        }
    }
    let mix_ops = ops as f64 / start.elapsed().as_secs_f64();

    Some(BenchmarkResult {
        name: name.to_string(),
        set_ops,
        get_ops,
        mix_ops,
    })
}

fn benchmark_pipeline(addr: &str, name: &str, ops: usize, batch: usize) -> Option<BenchmarkResult> {
    let mut client = Client::connect(addr).ok()?;
    
    if !client.ping() {
        return None;
    }

    let batches = ops / batch;
    let all_keys: Vec<Vec<u8>> = (0..ops).map(|i| format!("pipe:k:{}", i).into_bytes()).collect();
    let all_values: Vec<Vec<u8>> = (0..ops).map(|i| format!("pipe:v:{}", i).into_bytes()).collect();

    for _ in 0..10 {
        let keys: Vec<Vec<u8>> = (0..batch).map(|j| format!("w:{}", j).into_bytes()).collect();
        let vals: Vec<Vec<u8>> = keys.iter().map(|_| b"w".to_vec()).collect();
        client.pipeline_set(&keys, &vals);
    }

    let start = Instant::now();
    for b in 0..batches {
        let idx = b * batch;
        client.pipeline_set(&all_keys[idx..idx+batch], &all_values[idx..idx+batch]);
    }
    let set_ops = ops as f64 / start.elapsed().as_secs_f64();

    let start = Instant::now();
    for b in 0..batches {
        let idx = b * batch;
        client.pipeline_get(&all_keys[idx..idx+batch]);
    }
    let get_ops = ops as f64 / start.elapsed().as_secs_f64();

    Some(BenchmarkResult {
        name: format!("{} (batch={})", name, batch),
        set_ops,
        get_ops,
        mix_ops: 0.0,
    })
}

fn benchmark_multi(addr: &str, name: &str, ops: usize, threads: usize) -> Option<BenchmarkResult> {
    Client::connect(addr).ok()?;

    let ops_per_thread = ops / threads;
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let addr = addr.to_string();
            thread::spawn(move || {
                let mut client = Client::connect(&addr).unwrap();
                for i in 0..ops_per_thread {
                    let key = format!("t{}:k{}", t, i);
                    let value = format!("t{}:v{}", t, i);
                    client.set(key.as_bytes(), value.as_bytes());
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let set_ops = ops as f64 / start.elapsed().as_secs_f64();

    Some(BenchmarkResult {
        name: format!("{} ({} threads)", name, threads),
        set_ops,
        get_ops: 0.0,
        mix_ops: 0.0,
    })
}

// ============================================================================
// TABLE OUTPUT
// ============================================================================

fn print_table(title: &str, results: &[BenchmarkResult], cols: &[&str]) {
    println!();
    println!("{}", "=".repeat(80));
    println!("  {}", title);
    println!("{}", "=".repeat(80));
    
    let mut header = format!("{:<30}", "Server");
    for col in cols {
        header += &format!(" | {:>12}", col);
    }
    println!("{}", header);
    println!("{}", "-".repeat(80));

    for r in results {
        let mut row = format!("{:<30}", r.name);
        if cols.contains(&"SET") {
            row += &format!(" | {:>12.0}", r.set_ops);
        }
        if cols.contains(&"GET") {
            row += &format!(" | {:>12.0}", r.get_ops);
        }
        if cols.contains(&"MIXED") {
            row += &format!(" | {:>12.0}", r.mix_ops);
        }
        println!("{}", row);
    }
    println!("{}", "-".repeat(80));
}

fn print_comparison_table(redis: &[BenchmarkResult], fastkv: &[BenchmarkResult]) {
    println!();
    println!("{}", "=".repeat(80));
    println!("  COMPARISON SUMMARY (FastKV vs Redis)");
    println!("{}", "=".repeat(80));
    println!("{:<30} | {:>12} | {:>12} | {:>8}", "Test", "Redis", "FastKV", "Speedup");
    println!("{}", "-".repeat(80));

    for (r, f) in redis.iter().zip(fastkv.iter()) {
        let speedup = if r.set_ops > 0.0 && f.set_ops > 0.0 {
            format!("{:.1}x", f.set_ops / r.set_ops)
        } else if r.get_ops > 0.0 && f.get_ops > 0.0 {
            format!("{:.1}x", f.get_ops / r.get_ops)
        } else {
            "-".to_string()
        };

        let redis_val = if r.set_ops > 0.0 { r.set_ops } else { r.get_ops };
        let fastkv_val = if f.set_ops > 0.0 { f.set_ops } else { f.get_ops };
        
        println!("{:<30} | {:>12.0} | {:>12.0} | {:>8}", 
            f.name.split(" (").next().unwrap_or(&f.name),
            redis_val,
            fastkv_val,
            speedup
        );
    }
    println!("{}", "-".repeat(80));
}

// ============================================================================
// MAIN
// ============================================================================

fn main() {
    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("all");
    let ops: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_OPS);

    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                     FastKV vs Redis Network Benchmark                        ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Operations per test: {}", ops);

    // Sequential
    if mode == "seq" || mode == "all" {
        println!("\n>>> Sequential Benchmark...");
        let redis = benchmark_sequential(REDIS_ADDR, "Redis", ops);
        let fastkv = benchmark_sequential(FASTKV_ADDR, "FastKV", ops);
        
        let mut results: Vec<BenchmarkResult> = vec![];
        if let Some(r) = redis.clone() { results.push(r); }
        if let Some(f) = fastkv.clone() { results.push(f); }
        
        if !results.is_empty() {
            print_table("SEQUENTIAL (one request at a time)", &results, &["SET", "GET", "MIXED"]);
        }
        
        if redis.is_some() && fastkv.is_some() {
            print_comparison_table(&[redis.unwrap()], &[fastkv.unwrap()]);
        }
    }

    // Pipeline
    if mode == "pipeline" || mode == "all" {
        println!("\n>>> Pipeline Benchmark...");
        let mut redis_results: Vec<BenchmarkResult> = vec![];
        let mut fastkv_results: Vec<BenchmarkResult> = vec![];
        
        for batch in [10, 50, 100, 500] {
            println!("  Testing batch={}...", batch);
            if let Some(r) = benchmark_pipeline(REDIS_ADDR, "Redis", ops, batch) {
                redis_results.push(r);
            }
            if let Some(f) = benchmark_pipeline(FASTKV_ADDR, "FastKV", ops, batch) {
                fastkv_results.push(f);
            }
        }
        
        let mut all: Vec<BenchmarkResult> = redis_results.clone();
        all.extend(fastkv_results.clone());
        
        if !all.is_empty() {
            print_table("PIPELINE (batched requests)", &all, &["SET", "GET"]);
        }
        
        if !redis_results.is_empty() && !fastkv_results.is_empty() {
            print_comparison_table(&redis_results, &fastkv_results);
        }
    }

    // Multi-threaded
    if mode == "multi" || mode == "all" {
        println!("\n>>> Multi-threaded Benchmark...");
        let mut redis_results: Vec<BenchmarkResult> = vec![];
        let mut fastkv_results: Vec<BenchmarkResult> = vec![];
        
        for threads in [1, 2, 4, 8] {
            println!("  Testing {} thread(s)...", threads);
            if let Some(r) = benchmark_multi(REDIS_ADDR, "Redis", ops, threads) {
                redis_results.push(r);
            }
            if let Some(f) = benchmark_multi(FASTKV_ADDR, "FastKV", ops, threads) {
                fastkv_results.push(f);
            }
        }
        
        let mut all: Vec<BenchmarkResult> = redis_results.clone();
        all.extend(fastkv_results.clone());
        
        if !all.is_empty() {
            print_table("MULTI-THREADED (concurrent clients)", &all, &["SET"]);
        }
        
        if !redis_results.is_empty() && !fastkv_results.is_empty() {
            print_comparison_table(&redis_results, &fastkv_results);
        }
    }

    println!("\nDone!");
}
