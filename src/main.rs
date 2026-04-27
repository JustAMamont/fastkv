//! FastKV — High-Performance Key-Value Store.
//!
//! CLI entry point. Supports running the server, in-memory benchmarks,
//! and quick tests.

use fast_kv::{
    ExpirationManager, FsyncPolicy, KvStore, ListManager, VERSION, Wal,
    core::expiration::spawn_active_expiration,
    WalOp,
};
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
            "server" => run_server(&args[2..]),
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
    println!("  server [port] [mode]       Start TCP server (default: 6379, tokio)");
    println!("  bench                      Run single-threaded benchmark");
    println!("  bench-threads              Run multi-threaded benchmark");
    println!("  test                       Run quick smoke tests");
    println!("  help                       Show this help");
    println!();
    println!("Server modes:");
    println!("  tokio    - Cross-platform async (default)");
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    println!("  io_uring - Linux only, maximum performance");
    println!();
    println!("Environment variables:");
    println!("  FASTKV_DIR       - Data directory for WAL (default: ./fastkv_data)");
    println!("  FASTKV_FSYNC     - fsync policy: always | everysec | never (default: everysec)");
    println!();
    println!("Examples:");
    println!("  fast_kv server 6379                  # Tokio server with WAL + TTL");
    println!("  FASTKV_FSYNC=always fast_kv server   # Strongest durability");
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    println!("  fast_kv server 6379 io_uring          # io_uring server");
    println!();
    println!("Features:");
    println!("  + Lock-free hash table");
    println!("  + Thread-safe (Send + Sync)");
    println!("  + Redis-compatible protocol (RESP)");
    println!("  + Pipeline support");
    println!("  + WAL persistence (Phase 2)");
    println!("  + String commands: INCR, MGET, APPEND, ... (Phase 3)");
    println!("  + TTL / Expiration with active purging (Phase 4)");
}

/// Parse server arguments: `[port] [mode]`
fn run_server(args: &[String]) {
    let port: u16 = args
        .first()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379);

    let mode = args
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or("tokio");

    // --- Data directory ---
    let data_dir = env::var("FASTKV_DIR").unwrap_or_else(|_| "./fastkv_data".into());
    std::fs::create_dir_all(&data_dir).unwrap_or_else(|e| {
        eprintln!("Warning: could not create data dir '{}': {}", data_dir, e);
    });

    // --- WAL ---
    let fsync_str = env::var("FASTKV_FSYNC").unwrap_or_else(|_| "everysec".into());
    let fsync_policy = match fsync_str.as_str() {
        "always" => FsyncPolicy::Always,
        "never" => FsyncPolicy::Never,
        _ => FsyncPolicy::EverySec,
    };

    let wal_path = format!("{}/fastkv.wal", data_dir);
    let wal = match Wal::open(&wal_path, fsync_policy) {
        Ok(w) => {
            println!("WAL: {} (fsync: {:?})", wal_path, fsync_policy);
            Some(Arc::new(w))
        }
        Err(e) => {
            eprintln!("Warning: WAL disabled (open failed: {})", e);
            None
        }
    };

    // --- KV store ---
    let store = Arc::new(KvStore::new());

    // --- List manager ---
    let lists_mgr = Arc::new(ListManager::new(Arc::clone(&store)));
    let lists = Some(Arc::clone(&lists_mgr));

    // --- Expiration ---
    let expiry = Some(Arc::new(ExpirationManager::with_on_expire(
        Arc::clone(&store),
        {
            let mgr = Arc::clone(&lists_mgr);
            Arc::new(move |key: &[u8]| mgr.remove_key(key))
        },
    )));
    let _expiry_handle = spawn_active_expiration(expiry.as_ref().unwrap().clone());
    println!("Expiration: enabled (active purging)");
    println!("Lists: enabled");

    // --- WAL recovery ---
    // Recover and replay WAL entries exactly once.
    // Phase 1: replay SET and DEL.
    if let Some(ref wal_arc) = wal {
        let entries = match Wal::recover(wal_arc.path()) {
            Ok(e) => e,
            Err(err) => {
                eprintln!("Warning: WAL recovery failed: {}", err);
                Vec::new()
            }
        };
        for entry in &entries {
            match entry.op {
                WalOp::Set => { store.set(&entry.key, &entry.value); }
                WalOp::Del => { store.del(&entry.key); }
                WalOp::Expire => { /* handled in Phase 2 */ }
            }
        }
        if !entries.is_empty() {
            let set_del_count = entries.iter().filter(|e| e.op != WalOp::Expire).count();
            println!("Replayed {} WAL entries (SET/DEL). Current DBSIZE: {}", set_del_count, store.len());
        }

        // Phase 2: replay EXPIRE entries (after all keys exist).
        let expire_entries: Vec<_> = entries.iter().filter(|e| e.op == WalOp::Expire).collect();
        if let Some(ref exp) = expiry {
            for entry in &expire_entries {
                let deadline_ms = u64::from_le_bytes(
                    entry.value.clone().try_into().unwrap_or([0u8; 8]),
                );
                exp.expire_at_deadline_ms(&entry.key, deadline_ms);
            }
            if !expire_entries.is_empty() {
                println!("Restored {} TTL entries from WAL.", expire_entries.len());
            }
        }
    }

    println!();

    match mode {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        "io_uring" => {
            use fast_kv::core::server::IoUringServer;
            println!("Starting FastKV io_uring server on port {}...", port);
            let server = IoUringServer::with_components(port, store, wal, expiry, lists);
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
            let server = TokioServer::with_components(port, store, wal, expiry, lists);
            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
            rt.block_on(server.run());
        }
    }
}

/// Run a suite of smoke tests covering core operations, string commands,
/// expiration, and multi-threaded correctness.
fn run_tests() {
    println!("Running smoke tests...\n");

    println!("Test 1: Basic operations (GET/SET/DEL)");
    let store = KvStore::new();
    store.set(b"hello", b"world");
    assert_eq!(store.get(b"hello"), Some(b"world".to_vec()));
    store.set(b"hello", b"rust");
    assert_eq!(store.get(b"hello"), Some(b"rust".to_vec()));
    assert!(store.del(b"hello"));
    assert_eq!(store.get(b"hello"), None);
    println!("  + PASS");

    println!("\nTest 2: EXISTS");
    store.set(b"exists_key", b"yes");
    assert!(store.exists(b"exists_key"));
    assert!(!store.exists(b"nope"));
    println!("  + PASS");

    println!("\nTest 3: INCR / DECR");
    store.set(b"counter", b"0");
    assert_eq!(store.incr(b"counter", 1).unwrap(), 1);
    assert_eq!(store.incr(b"counter", 5).unwrap(), 6);
    assert_eq!(store.decr(b"counter").unwrap(), 5);
    assert_eq!(store.get(b"counter"), Some(b"5".to_vec()));
    println!("  + PASS");

    println!("\nTest 4: MGET / MSET");
    let pairs: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    assert!(store.mset(&pairs));
    let vals = store.mget(&[b"a", b"b", b"c", b"missing"]);
    assert_eq!(vals[0], Some(b"1".to_vec()));
    assert_eq!(vals[3], None);
    println!("  + PASS");

    println!("\nTest 5: APPEND / STRLEN");
    store.set(b"s", b"hello");
    assert_eq!(store.append(b"s", b" world"), Some(11));
    assert_eq!(store.strlen(b"s"), 11);
    assert_eq!(store.get(b"s"), Some(b"hello world".to_vec()));
    println!("  + PASS");

    println!("\nTest 6: GETRANGE / SETRANGE");
    store.set(b"g", b"Hello World");
    assert_eq!(store.getrange(b"g", 0, 4), Some(b"Hello".to_vec()));
    assert_eq!(store.getrange(b"g", -5, -1), Some(b"World".to_vec()));
    assert_eq!(store.setrange(b"g", 6, b"Redis"), Some(11));
    assert_eq!(store.get(b"g"), Some(b"Hello Redis".to_vec()));
    println!("  + PASS");

    println!("\nTest 7: Many keys (1 000)");
    let store = KvStore::new();
    for i in 0..1000 {
        let key = format!("key:{}", i);
        let value = format!("value:{}", i);
        store.set(key.as_bytes(), value.as_bytes());
    }
    println!("  + Inserted 1000 keys");

    println!("\nTest 8: Multi-threaded (4 threads)");
    let store = Arc::new(KvStore::new());
    let mut handles = vec![];
    for t in 0..4 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = format!("t{}:k{}", t, i);
                let value = format!("t{}:v{}", t, i);
                store.set(key.as_bytes(), value.as_bytes());
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    println!("  + 4 threads wrote 400 keys");

    println!("\nTest 9: Expiration");
    let store = Arc::new(KvStore::new());
    let exp = ExpirationManager::new(Arc::clone(&store));
    store.set(b"temp", b"data");
    assert!(exp.expire(b"temp", std::time::Duration::from_secs(10)));
    let ttl = exp.ttl(b"temp").unwrap();
    assert!(ttl <= std::time::Duration::from_secs(10));
    assert!(exp.persist(b"temp"));
    assert!(exp.ttl(b"temp").is_none());
    println!("  + PASS");

    println!("\nAll tests passed!");
}

/// Run a single-threaded performance benchmark (SET, GET, INCR, EXISTS)
/// and print throughput in ops/sec.
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

    println!("\nBenchmark: INCR");
    for i in 0..1000 {
        let key = format!("incr:{}", i);
        store.set(key.as_bytes(), b"0");
    }
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("incr:{}", i);
        let _ = store.incr(key.as_bytes(), 1);
    }
    let incr_ops = 1000 as f64 / start.elapsed().as_secs_f64();
    println!("  Throughput: {:.0} ops/sec", incr_ops);

    println!("\nBenchmark: EXISTS");
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("bench:key:{}", i);
        let _ = store.exists(key.as_bytes());
    }
    let exists_ops = iterations as f64 / start.elapsed().as_secs_f64();
    println!("  Throughput: {:.0} ops/sec", exists_ops);

    println!("\nSummary: SET={:.0} GET={:.0} INCR={:.0} EXISTS={:.0}",
        set_ops, get_ops, incr_ops, exists_ops);
}

/// Run a multi-threaded performance benchmark with varying thread counts
/// (1, 2, 4, 8) and print SET/GET throughput in ops/sec.
fn run_threaded_benchmark() {
    println!("Running multi-threaded benchmark...\n");

    let iterations = 100_000;

    for num_threads in [1, 2, 4, 8] {
        println!("\n=== {} Thread(s) ===", num_threads);

        let store = Arc::new(KvStore::new());
        let ops_per_thread = iterations / num_threads;

        let start = Instant::now();
        let mut handles = vec![];

        for t in 0..num_threads {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("t{}:key:{}", t, i);
                    let value = format!("t{}:value:{}", t, i);
                    store.set(key.as_bytes(), value.as_bytes());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let set_ops = iterations as f64 / start.elapsed().as_secs_f64();
        println!("  SET: {:.0} ops/sec", set_ops);

        let start = Instant::now();
        let mut handles = vec![];

        for t in 0..num_threads {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("t{}:key:{}", t, i);
                    let _ = store.get(key.as_bytes());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let get_ops = iterations as f64 / start.elapsed().as_secs_f64();
        println!("  GET: {:.0} ops/sec", get_ops);
    }
}
