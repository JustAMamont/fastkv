//! FastKV — High-Performance Key-Value Store.
//!
//! CLI entry point. Supports running the server, in-memory benchmarks,
//! and quick tests.

use fast_kv::{
    ExpirationManager, FsyncPolicy, KvStoreLockFree, ListManager, VERSION, Wal,
    WalOp,
    core::expiration::spawn_active_expiration,
    core::kv::DEFAULT_INLINE_SIZE,
};
#[cfg(feature = "blob-store")]
use fast_kv::{BlobArena, WalSegment, SegmentConfig, recover_segments};
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
    println!("  --capacity <num>     Hash table buckets (default: 100000)");
    println!("  --inline-size <N>    Per-side inline storage size in bytes (default: 64)");
    println!("                       Supported: 64, 128, 256, 512");
    println!("  --dir <path>         Data directory for WAL (default: ./fastkv_data)");
    println!("  --fsync <policy>     fsync policy: always | everysec | never (default: everysec)");
    println!("  --mode <backend>     Server backend: tokio (default)");
    #[cfg(feature = "blob-store")]
    println!("  --wal-compress       Enable compressed segment-based WAL (saves disk for high-volume)");
    #[cfg(feature = "blob-store")]
    println!("  --wal-segment-size   Max WAL segment size in MB (default: 64, only with --wal-compress)");
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    println!("                       io_uring (Linux only, maximum performance)");
    println!();
    println!("Environment variables (override defaults):");
    println!("  FASTKV_HOST          Bind address");
    println!("  FASTKV_PORT          Listen port");
    println!("  FASTKV_CAPACITY      Hash table buckets");
    println!("  FASTKV_INLINE_SIZE   Per-side inline storage size (64|128|256|512)");
    println!("  FASTKV_DIR           Data directory for WAL");
    println!("  FASTKV_FSYNC         fsync policy: always | everysec | never");
    println!();
    println!("Examples:");
    println!("  fast_kv server                        # 0.0.0.0:6379, WAL in ./fastkv_data");
    println!("  fast_kv server --port 6380             # custom port");
    println!("  fast_kv server --host 127.0.0.1 --port 6380");
    println!("  fast_kv server --dir /var/lib/fastkv --fsync always");
    println!("  fast_kv server --capacity 1000000      # 1M buckets for high-cardinality workloads");
    println!("  fast_kv server --inline-size 256       # 256-byte inline for larger values");
    println!();
    println!("Memory usage guide (per-side inline size × capacity):");
    println!("  --inline-size 64  (default)  ~19 MB / 100K buckets");
    println!("  --inline-size 128            ~27 MB / 100K buckets");
    println!("  --inline-size 256            ~55 MB / 100K buckets");
    println!("  --inline-size 512            ~110 MB / 100K buckets");
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

#[cfg(feature = "blob-store")]
fn has_flag(args: &[String], name: &str) -> bool {
    let long = format!("--{}", name);
    args.iter().any(|a| a == &long)
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Supported inline sizes. Each value produces a separate monomorphized
/// binary path so that lock-free performance is preserved (no dyn dispatch).
const SUPPORTED_INLINE_SIZES: &[usize] = &[64, 128, 256, 512];

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

    let capacity: usize = get_flag(args, "capacity")
        .or_else(|| env::var("FASTKV_CAPACITY").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);

    let inline_size: usize = get_flag(args, "inline-size")
        .or_else(|| env::var("FASTKV_INLINE_SIZE").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_INLINE_SIZE);

    let mode = get_flag(args, "mode")
        .unwrap_or_else(|| "tokio".into());

    #[cfg(feature = "blob-store")]
    let wal_compress = has_flag(args, "wal-compress");
    #[cfg(not(feature = "blob-store"))]
    let wal_compress = false;

    #[cfg(feature = "blob-store")]
    let wal_segment_size_mb: u64 = get_flag(args, "wal-segment-size")
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    #[cfg(not(feature = "blob-store"))]
    let wal_segment_size_mb: u64 = 64;

    let fsync_policy = match fsync_str.as_str() {
        "always" => FsyncPolicy::Always,
        "never" => FsyncPolicy::Never,
        _ => FsyncPolicy::EverySec,
    };

    // Validate inline size.
    if !SUPPORTED_INLINE_SIZES.contains(&inline_size) {
        eprintln!(
            "Error: unsupported --inline-size {}. Supported values: {:?}",
            inline_size, SUPPORTED_INLINE_SIZES
        );
        std::process::exit(1);
    }

    println!("FastKV v{} (Lock-Free Edition)", VERSION);
    println!("  host:        {}", host);
    println!("  port:        {}", port);
    println!("  capacity:    {} buckets", capacity);
    println!("  inline-size: {} bytes (per side)", inline_size);
    println!("  dir:         {}", data_dir);
    println!("  fsync:       {:?}", fsync_policy);
    println!("  mode:        {}", mode);
    #[cfg(feature = "blob-store")]
    println!("  blob-store:  enabled (zstd compression)");
    if wal_compress {
        println!("  wal-compress: enabled (segment size: {} MB)", wal_segment_size_mb);
    }

    // --- Data directory ---
    std::fs::create_dir_all(&data_dir).unwrap_or_else(|e| {
        eprintln!("Warning: could not create data dir '{}': {}", data_dir, e);
    });

    // --- Dispatch to the monomorphized server for the chosen inline size ---
    match inline_size {
        64  => run_server_with_n::<64>(host, port, capacity, data_dir, fsync_policy, mode, wal_compress, wal_segment_size_mb),
        128 => run_server_with_n::<128>(host, port, capacity, data_dir, fsync_policy, mode, wal_compress, wal_segment_size_mb),
        256 => run_server_with_n::<256>(host, port, capacity, data_dir, fsync_policy, mode, wal_compress, wal_segment_size_mb),
        512 => run_server_with_n::<512>(host, port, capacity, data_dir, fsync_policy, mode, wal_compress, wal_segment_size_mb),
        _   => unreachable!(), // validated above
    }
}

/// Monomorphized server entry point. `N` is the per-side inline storage size.
///
/// This function is instantiated once per supported inline size by the
/// `match` in [`run_server`], producing optimal lock-free code for each
/// configuration without any dynamic-dispatch overhead.
fn run_server_with_n<const N: usize>(
    host: String,
    port: u16,
    capacity: usize,
    data_dir: String,
    fsync_policy: FsyncPolicy,
    mode: String,
    wal_compress: bool,
    wal_segment_size_mb: u64,
) {
    // Suppress unused-variable warnings when blob-store is disabled.
    #[cfg(not(feature = "blob-store"))]
    let _ = (wal_compress, wal_segment_size_mb);

    // --- WAL ---
    // When wal_compress is enabled (requires blob-store), use WalSegment.
    // Otherwise, use the legacy v1 Wal.
    #[cfg(feature = "blob-store")]
    let (wal, wal_seg): (Option<Arc<Wal>>, Option<Arc<WalSegment>>) = if wal_compress {
        // Compressed segment WAL.
        let mut seg_config = SegmentConfig::new(&data_dir);
        seg_config.fsync_policy = fsync_policy;
        seg_config.max_segment_size = wal_segment_size_mb * 1024 * 1024;
        match WalSegment::open(seg_config) {
            Ok(seg) => {
                println!("  WAL:    {} (compressed, segment size: {} MB)", data_dir, wal_segment_size_mb);
                (None, Some(Arc::new(seg)))
            }
            Err(e) => {
                eprintln!("  WAL:    disabled (segment open failed: {})", e);
                (None, None)
            }
        }
    } else {
        let wal_path = format!("{}/fastkv.wal", data_dir);
        match Wal::open(&wal_path, fsync_policy) {
            Ok(w) => {
                println!("  WAL:    {} (fsync: {:?})", wal_path, fsync_policy);
                (Some(Arc::new(w)), None)
            }
            Err(e) => {
                eprintln!("  WAL:    disabled (open failed: {})", e);
                (None, None)
            }
        }
    };

    #[cfg(not(feature = "blob-store"))]
    let (wal, _wal_seg): (Option<Arc<Wal>>, Option<()>) = {
        let wal_path = format!("{}/fastkv.wal", data_dir);
        match Wal::open(&wal_path, fsync_policy) {
            Ok(w) => {
                println!("  WAL:    {} (fsync: {:?})", wal_path, fsync_policy);
                (Some(Arc::new(w)), None)
            }
            Err(e) => {
                eprintln!("  WAL:    disabled (open failed: {})", e);
                (None, None)
            }
        }
    };

    // --- KV store ---
    let store = Arc::new(KvStoreLockFree::<N>::with_capacity(capacity));

    // --- List manager ---
    let lists_mgr = Arc::new(ListManager::<N>::new(Arc::clone(&store)));
    let lists = Some(Arc::clone(&lists_mgr));

    // --- Blob arena ---
    #[cfg(feature = "blob-store")]
    let blob: Option<Arc<BlobArena>> = Some(Arc::new(BlobArena::new()));

    // --- Expiration ---
    let expiry = Some(Arc::new(ExpirationManager::<N>::with_on_expire(
        Arc::clone(&store),
        {
            let mgr = Arc::clone(&lists_mgr);
            Arc::new(move |key: &[u8]| mgr.remove_key(key))
        },
    )));
    let _expiry_handle = spawn_active_expiration(expiry.as_ref().unwrap().clone());

    // --- WAL recovery ---
    #[cfg(feature = "blob-store")]
    let entries: Vec<fast_kv::WalEntry> = if let Some(ref seg) = wal_seg {
        // Compressed segment recovery.
        match recover_segments(seg.data_dir()) {
            Ok(e) => e,
            Err(err) => {
                eprintln!("Warning: segment WAL recovery failed: {}", err);
                Vec::new()
            }
        }
    } else if let Some(ref wal_arc) = wal {
        match Wal::recover(wal_arc.path()) {
            Ok(e) => e,
            Err(err) => {
                eprintln!("Warning: WAL recovery failed: {}", err);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    #[cfg(not(feature = "blob-store"))]
    let entries: Vec<fast_kv::WalEntry> = if let Some(ref wal_arc) = wal {
        match Wal::recover(wal_arc.path()) {
            Ok(e) => e,
            Err(err) => {
                eprintln!("Warning: WAL recovery failed: {}", err);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let mut recovered = 0usize;
    let mut skipped = 0usize;
    for entry in &entries {
        match entry.op {
            WalOp::Set => {
                if store.set(&entry.key, &entry.value) {
                    recovered += 1;
                } else {
                    skipped += 1;
                }
            }
            WalOp::Del => {
                if store.del(&entry.key) {
                    recovered += 1;
                }
            }
            WalOp::Expire => { /* handled below */ }
            #[cfg(feature = "blob-store")]
            WalOp::BSet => {
                if let Some(ref blob_arena) = blob {
                    if let Some(blob_ref) = blob_arena.store(&entry.value) {
                        let encoded = blob_ref.encode();
                        if store.set(&entry.key, &encoded) {
                            recovered += 1;
                        } else {
                            blob_arena.free(&blob_ref);
                            skipped += 1;
                        }
                    } else {
                        skipped += 1;
                    }
                } else {
                    skipped += 1;
                }
            }
            #[cfg(feature = "blob-store")]
            WalOp::BDel => {
                if let Some(ref blob_arena) = blob {
                    if let Some(v) = store.get(&entry.key) {
                        if fast_kv::core::blob::BlobArena::is_blob_ref(&v) {
                            if let Some(blob_ref) = fast_kv::core::blob::BlobRef::decode(&v) {
                                blob_arena.free(&blob_ref);
                            }
                        }
                    }
                }
                if store.del(&entry.key) {
                    recovered += 1;
                }
            }
            WalOp::ListOp => {
                fast_kv::core::list::replay_list_op(&lists_mgr, &entry.key, &entry.value);
                recovered += 1;
            }
            #[cfg(not(feature = "blob-store"))]
            _ => {
                skipped += 1;
            }
        }
    }
    if !entries.is_empty() {
        println!("  Recovered {} entries from WAL (DBSIZE: {}){}",
            recovered,
            store.len(),
            if skipped > 0 { format!(", {} skipped (exceeds inline-size={})", skipped, N) } else { String::new() }
        );
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

    #[cfg(feature = "blob-store")]
    if let Some(ref blob_arena) = blob {
        let stats = blob_arena.stats();
        if stats.total_used > 0 {
            println!("  Blob arena: {} bytes used, compression ratio: {:.2}x",
                stats.total_used,
                if stats.compression_ratio > 0.0 { 1.0 / stats.compression_ratio } else { 0.0 }
            );
        }
    }

    println!();

    // Convert WAL references to trait objects for the server.
    #[cfg(feature = "blob-store")]
    let wal_writer: Option<Arc<dyn fast_kv::WalWriter>> = if let Some(ref seg) = wal_seg {
        Some(Arc::clone(seg) as Arc<dyn fast_kv::WalWriter>)
    } else if let Some(ref w) = wal {
        Some(Arc::clone(w) as Arc<dyn fast_kv::WalWriter>)
    } else {
        None
    };

    #[cfg(not(feature = "blob-store"))]
    let wal_writer: Option<Arc<dyn fast_kv::WalWriter>> = wal.map(|w| w as Arc<dyn fast_kv::WalWriter>);

    match mode.as_str() {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        "io_uring" => {
            use fast_kv::core::server::IoUringServer;
            println!("Starting FastKV io_uring server on {}:{} (inline-size={})...", host, port, N);
            #[cfg(feature = "blob-store")]
            let server = IoUringServer::<N>::with_components(port, host, store, wal_writer, expiry, lists, blob);
            #[cfg(not(feature = "blob-store"))]
            let server = IoUringServer::<N>::with_components_no_blob(port, host, store, wal_writer, expiry, lists);
            server.run();
        }
        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        "io_uring" => {
            eprintln!("io_uring support not compiled in!");
            eprintln!("Build with: cargo run --release --features io-uring -- server --mode io_uring");
        }
        _ => {
            use fast_kv::core::server::TokioServer;
            println!("Starting FastKV server on {}:{} (inline-size={})...", host, port, N);
            #[cfg(feature = "blob-store")]
            let server = TokioServer::<N>::with_components(port, host, store, wal_writer, expiry, lists, blob);
            #[cfg(not(feature = "blob-store"))]
            let server = TokioServer::<N>::with_components_no_blob(port, host, store, wal_writer, expiry, lists);
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
    let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100);
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
    let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(2000);
    for i in 0..1000 {
        let key = format!("key:{}", i);
        let value = format!("value:{}", i);
        store.set(key.as_bytes(), value.as_bytes());
    }
    println!("  + Inserted 1000 keys");

    println!("\nTest 8: Multi-threaded (4 threads)");
    let store = Arc::new(KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(1000));
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
    let store = Arc::new(KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(100));
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

    let store = KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(200_000);  // 2× expected key count
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

        let store = Arc::new(KvStoreLockFree::<DEFAULT_INLINE_SIZE>::with_capacity(200_000));  // 2× expected key count
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
