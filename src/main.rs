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

    // No args or "help" — print usage
    if args.len() < 2 || args[1] == "help" || args[1] == "--help" || args[1] == "-h" {
        print_usage();
        return;
    }

    match args[1].as_str() {
        "server" => run_server(&args[2..]),
        "bench" => run_benchmark(),
        "bench-threads" => run_threaded_benchmark(),
        "test" => run_tests(),
        _ => {
            eprintln!("Unknown command: {}\n", args[1]);
            print_usage();
        }
    }
}

fn print_usage() {
    println!("FastKV v{} (Lock-Free Edition)", VERSION);
    println!();
    println!("Usage: fast_kv <command> [options]");
    println!();
    println!("Commands:");
    println!("  server              Start FastKV server (default command if only flags given)");
    println!("  bench                Run single-threaded benchmark");
    println!("  bench-threads        Run multi-threaded benchmark");
    println!("  test                 Run quick smoke tests");
    println!("  help                 Show this help");
    println!();
    println!("Server options:");
    println!("  --host <addr>        Bind address (default: 0.0.0.0)");
    println!("  --port <port>        Listen port (default: 6379)");
    println!("  --dir <path>         Data directory for WAL (default: ./fastkv_data)");
    println!("  --fsync <policy>     fsync policy: always | everysec | never (default: everysec)");
    println!("  --mode <backend>     Server backend: tokio (default)");
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    println!("                       io_uring (Linux only, maximum performance)");
    println!();
    println!("Environment variables (override defaults):");
    println!("  FASTKV_HOST          Bind address");
    println!("  FASTKV_PORT          Listen port");
    println!("  FASTKV_DIR           Data directory for WAL");
    println!("  FASTKV_FSYNC         fsync policy: always | everysec | never");
    println!();
    println!("Examples:");
    println!("  fast_kv server                        # 0.0.0.0:6379, WAL in ./fastkv_data");
    println!("  fast_kv server --port 6380             # custom port");
    println!("  fast_kv server --host 127.0.0.1 --port 6380");
    println!("  fast_kv server --dir /var/lib/fastkv --fsync always");
    println!("  docker run -p 6379:6379 -v fastkv_data:/data ghcr.io/<user>/fastkv:latest");
    println!();
    println!("Features:");
    println!("  + Lock-free hash table");
    println!("  + Thread-safe (Send + Sync)");
    println!("  + Redis-compatible protocol (RESP)");
    println!("  + Pipeline support");
    println!("  + WAL persistence");
    println!("  + String commands: INCR, MGET, APPEND, ...");
    println!("  + TTL / Expiration with active purging");
}

// ---------------------------------------------------------------------------
// Flag parsing (zero external dependencies)
// ---------------------------------------------------------------------------

fn get_flag(args: &[String], name: &str) -> Option<String> {
    let long = format!("--{}", name);
    for i in 0..args.len() {
        if args[i] == long {
            return args.get(i + 1).cloned();
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

fn run_server(args: &[String]) {
    // --- Parse flags, fall back to env vars, then defaults ---
    let host = get_flag(args, "host")
        .or_else(|| env::var("FASTKV_HOST").ok())
        .unwrap_or_else(|| "0.0.0.0".into());

    let port: u16 = get_flag(args, "port")
        .or_else(|| env::var("FASTKV_PORT").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379);

    let data_dir = get_flag(args, "dir")
        .or_else(|| env::var("FASTKV_DIR").ok())
        .unwrap_or_else(|| "./fastkv_data".into());

    let fsync_str = get_flag(args, "fsync")
        .or_else(|| env::var("FASTKV_FSYNC").ok())
        .unwrap_or_else(|| "everysec".into());

    let mode = get_flag(args, "mode")
        .unwrap_or_else(|| "tokio".into());

    let fsync_policy = match fsync_str.as_str() {
        "always" => FsyncPolicy::Always,
        "never" => FsyncPolicy::Never,
        _ => FsyncPolicy::EverySec,
    };

    println!("FastKV v{} (Lock-Free Edition)", VERSION);
    println!("  host:   {}", host);
    println!("  port:   {}", port);
    println!("  dir:    {}", data_dir);
    println!("  fsync:  {:?}", fsync_policy);
    println!("  mode:   {}", mode);

    // --- Data directory ---
    std::fs::create_dir_all(&data_dir).unwrap_or_else(|e| {
        eprintln!("Warning: could not create data dir '{}': {}", data_dir, e);
    });

    // --- WAL ---
    let wal_path = format!("{}/fastkv.wal", data_dir);
    let wal = match Wal::open(&wal_path, fsync_policy) {
        Ok(w) => {
            println!("  WAL:    {} (fsync: {:?})", wal_path, fsync_policy);
            Some(Arc::new(w))
        }
        Err(e) => {
            eprintln!("  WAL:    disabled (open failed: {})", e);
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

    // --- WAL recovery ---
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
            println!("  Recovered {} entries from WAL (DBSIZE: {})", set_del_count, store.len());
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
                println!("  Restored {} TTL entries from WAL", expire_entries.len());
            }
        }
    }

    println!();

    match mode.as_str() {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        "io_uring" => {
            use fast_kv::core::server::IoUringServer;
            println!("Starting FastKV io_uring server on {}:{}...", host, port);
            let server = IoUringServer::with_components(port, host.clone(), store, wal, expiry, lists);
            server.run();
        }
        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        "io_uring" => {
            eprintln!("io_uring support not compiled in!");
            eprintln!("Build with: cargo run --release --features io-uring -- server --mode io_uring");
        }
        _ => {
            use fast_kv::core::server::TokioServer;
            println!("Starting FastKV server on {}:{}...", host, port);
            let server = TokioServer::with_components(port, host, store, wal, expiry, lists);
            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
            rt.block_on(server.run());
        }
    }
}

// ---------------------------------------------------------------------------
// Benchmarks & tests (unchanged)
// ---------------------------------------------------------------------------

fn run_tests() {
    println!("FastKV v{} — Running smoke tests...\n", VERSION);

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
