//! FastKV - High-Performance Key-Value Store
//!
//! A Redis-compatible KV store written in Rust.

use fast_kv::{KvStore, VERSION};
use std::env;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();

    println!("FastKV v{} (Lock-Free Edition)", VERSION);
    println!("==================================\n");

    if args.len() > 1 {
        match args[1].as_str() {
            "server" => run_server(args.get(2), args.get(3)),
            "bench" => run_benchmark(),
            "bench-threads" => run_threaded_benchmark(),
            "test" => run_tests(),
            "help" => print_help(),
            _ => {
                eprintln!("Unknown command: {}", args[1]);
                print_help();
            }
        }
    } else {
        print_help();
    }
}

fn print_help() {
    println!("Usage: fast_kv <command> [args]");
    println!();
    println!("Commands:");
    println!("  server [port] [mode]    Start TCP server (default: 6379, tokio)");
    println!("  bench                   Run single-threaded benchmark");
    println!("  bench-threads           Run multi-threaded benchmark");
    println!("  test                    Run tests");
    println!("  help                    Show this help");
    println!();
    println!("Server modes:");
    println!("  tokio    - Cross-platform async (default)");
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    println!("  io_uring - Linux only, maximum performance");
    println!();
    println!("Examples:");
    println!("  fast_kv server 6379           # Tokio server");
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    println!("  fast_kv server 6379 io_uring  # io_uring server");
    println!();
    println!("Features:");
    println!("  + Lock-free hash table");
    println!("  + Thread-safe (Send + Sync)");
    println!("  + Redis-compatible protocol (RESP)");
    println!("  + Pipeline support");
}

fn run_server(port_arg: Option<&String>, mode_arg: Option<&String>) {
    let port: u16 = port_arg
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379);

    let mode = mode_arg.map(|s| s.as_str()).unwrap_or("tokio");

    match mode {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        "io_uring" => {
            use fast_kv::core::server::IoUringServer;
            println!("Starting FastKV io_uring server on port {}...", port);
            let server = IoUringServer::with_port(port);
            server.run();
        }
        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        "io_uring" => {
            eprintln!("io_uring support not compiled in!");
            eprintln!("Build with: cargo run --release --features io-uring -- server {} io_uring", port);
        }
        _ => {
            use fast_kv::core::server::TokioServer;
            println!("Starting FastKV Tokio server on port {}...", port);
            let server = TokioServer::with_port(port);
            
            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
            rt.block_on(server.run());
        }
    }
}

fn run_tests() {
    println!("Running tests...\n");

    println!("Test 1: Basic operations");
    let store = KvStore::new();
    
    store.set(b"hello", b"world");
    assert_eq!(store.get(b"hello"), Some(b"world".to_vec()));
    println!("  + SET and GET work");
    
    store.set(b"hello", b"rust");
    assert_eq!(store.get(b"hello"), Some(b"rust".to_vec()));
    println!("  + UPDATE works");
    
    assert!(store.del(b"hello"));
    assert_eq!(store.get(b"hello"), None);
    println!("  + DEL works");

    println!("\nTest 2: Many keys (1000)");
    let store = KvStore::new();
    
    for i in 0..1000 {
        let key = format!("key:{}", i);
        let value = format!("value:{}", i);
        store.set(key.as_bytes(), value.as_bytes());
    }
    println!("  + Inserted 1000 keys");

    println!("\nTest 3: Multi-threaded (4 threads)");
    let store = Arc::new(KvStore::new());
    let mut handles = vec![];

    for t in 0..4 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("t{}:k{}", t, i);
                let value = format!("t{}:v{}", t, i);
                store.set(key.as_bytes(), value.as_bytes());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    println!("  + 4 threads wrote 400 keys");

    println!("\nAll tests passed!");
}

fn run_benchmark() {
    println!("Running single-threaded benchmark...\n");

    let store = KvStore::new();
    let iterations = 100_000;

    println!("Warming up...");
    for i in 0..1000 {
        let key = format!("warm:{}", i);
        store.set(key.as_bytes(), b"warmup");
    }

    println!("\nBenchmark: SET");
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("bench:key:{}", i);
        let value = format!("bench:value:{}", i);
        store.set(key.as_bytes(), value.as_bytes());
    }
    let set_ops = iterations as f64 / start.elapsed().as_secs_f64();
    println!("  Throughput: {:.0} ops/sec", set_ops);

    println!("\nBenchmark: GET");
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("bench:key:{}", i);
        let _ = store.get(key.as_bytes());
    }
    let get_ops = iterations as f64 / start.elapsed().as_secs_f64();
    println!("  Throughput: {:.0} ops/sec", get_ops);

    println!("\nSummary: SET={:.0} GET={:.0}", set_ops, get_ops);
}

fn run_threaded_benchmark() {
    println!("Running multi-threaded benchmark...\n");

    let iterations = 100_000;

    for num_threads in [1, 2, 4] {
        println!("\n=== {} Thread(s) ===", num_threads);

        let store = Arc::new(KvStore::new());
        let ops_per_thread = iterations / num_threads;

        let start = Instant::now();
        let mut handles = vec![];
        
        for t in 0..num_threads {
            let store = Arc::clone(&store);
            let handle = thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("t{}:key:{}", t, i);
                    let value = format!("t{}:value:{}", t, i);
                    store.set(key.as_bytes(), value.as_bytes());
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let set_ops = iterations as f64 / start.elapsed().as_secs_f64();
        println!("  SET: {:.0} ops/sec", set_ops);
    }
}
