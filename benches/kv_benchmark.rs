//! Criterion Benchmarks for FastKV
//!
//! Run with:
//!   cargo bench
//!   cargo bench --features blob-store          # include Blob Arena benchmarks
//!   cargo bench --features blob-store,similarity  # all benchmarks
//!   cargo bench -- <pattern>                   # e.g. cargo bench -- blob

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fast_kv::KvStore;
use std::sync::Arc;
use std::thread;

// ============================================================================
// SINGLE-THREADED BENCHMARKS (original)
// ============================================================================

fn benchmark_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_operations");

    for size in [100, 1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let store = KvStore::new();
            b.iter(|| {
                for i in 0..size {
                    let key = format!("key_{}", i);
                    let value = format!("value_{}", i);
                    black_box(store.set(key.as_bytes(), value.as_bytes()));
                }
            });
        });
    }
    group.finish();
}

fn benchmark_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_operations");

    for size in [100, 1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let store = KvStore::new();
            for i in 0..size {
                let key = format!("key_{}", i);
                let value = format!("value_{}", i);
                store.set(key.as_bytes(), value.as_bytes());
            }
            b.iter(|| {
                for i in 0..size {
                    let key = format!("key_{}", i);
                    black_box(store.get(key.as_bytes()));
                }
            });
        });
    }
    group.finish();
}

fn benchmark_incr(c: &mut Criterion) {
    let mut group = c.benchmark_group("incr_operations");

    group.bench_function("incr_10k", |b| {
        let store = KvStore::new();
        for i in 0..10_000 {
            let key = format!("counter_{}", i);
            store.set(key.as_bytes(), b"0");
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("counter_{}", i);
                let _ = black_box(store.incr(key.as_bytes(), 1));
            }
        });
    });
    group.finish();
}

fn benchmark_exists(c: &mut Criterion) {
    let mut group = c.benchmark_group("exists_operations");

    group.bench_function("exists_10k", |b| {
        let store = KvStore::new();
        for i in 0..10_000 {
            let key = format!("key_{}", i);
            store.set(key.as_bytes(), b"value");
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("key_{}", i);
                black_box(store.exists(key.as_bytes()));
            }
        });
    });
    group.finish();
}

fn benchmark_mget(c: &mut Criterion) {
    let mut group = c.benchmark_group("mget_operations");

    group.bench_function("mget_100_keys", |b| {
        let store = KvStore::new();
        let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("key_{}", i).into_bytes()).collect();
        for k in &keys {
            store.set(k, b"value");
        }
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        b.iter(|| {
            black_box(store.mget(&key_refs));
        });
    });
    group.finish();
}

// ============================================================================
// MULTI-THREADED BENCHMARKS (original)
// ============================================================================

fn benchmark_threaded_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("threaded_set");

    for num_threads in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let store = Arc::new(KvStore::new());
                    let ops_per_thread = 10_000 / num_threads;

                    let mut handles = vec![];
                    for t in 0..num_threads {
                        let store = Arc::clone(&store);
                        handles.push(thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                let key = format!("t{}:k{}", t, i);
                                let value = format!("t{}:v{}", t, i);
                                store.set(key.as_bytes(), value.as_bytes());
                            }
                        }));
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    black_box(store)
                });
            },
        );
    }
    group.finish();
}

fn benchmark_threaded_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("threaded_get");

    for num_threads in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            num_threads,
            |b, &num_threads| {
                let store = KvStore::new();
                for i in 0..10_000 {
                    let key = format!("key_{}", i);
                    store.set(key.as_bytes(), b"value");
                }
                let store = Arc::new(store);
                let ops_per_thread = 10_000 / num_threads;

                b.iter(|| {
                    let mut handles = vec![];
                    for t in 0..num_threads {
                        let store = Arc::clone(&store);
                        handles.push(thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                let key = format!("key_{}", (t * ops_per_thread + i) % 10_000);
                                black_box(store.get(key.as_bytes()));
                            }
                        }));
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

fn benchmark_threaded_incr(c: &mut Criterion) {
    let mut group = c.benchmark_group("threaded_incr");

    group.bench_function("8_threads_1k_keys", |b| {
        let store = Arc::new(KvStore::new());
        for i in 0..1_000 {
            let key = format!("counter_{}", i);
            store.set(key.as_bytes(), b"0");
        }

        b.iter(|| {
            let mut handles = vec![];
            for _t in 0..8 {
                let store = Arc::clone(&store);
                handles.push(thread::spawn(move || {
                    for i in 0..125 {
                        let key = format!("counter_{}", i);
                        let _ = black_box(store.incr(key.as_bytes(), 1));
                    }
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
    group.finish();
}

// ============================================================================
// WAL BENCHMARKS (original)
// ============================================================================

fn benchmark_wal_write(c: &mut Criterion) {
    use fast_kv::{FsyncPolicy, Wal};

    let mut group = c.benchmark_group("wal_write");

    group.bench_function("set_10k_fsync_never", |b| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bench.wal");
        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();

        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("k{}", i);
                let val = format!("v{}", i);
                let _ = black_box(wal.set(key.as_bytes(), val.as_bytes()));
            }
        });
    });
    group.finish();
}

// ============================================================================
// STRING OPERATION BENCHMARKS (original)
// ============================================================================

fn benchmark_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_operations");

    group.bench_function("append_10k", |b| {
        let store = KvStore::new();
        for i in 0..10_000 {
            let key = format!("key_{}", i);
            store.set(key.as_bytes(), b"hello");
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("key_{}", i);
                black_box(store.append(key.as_bytes(), b" world"));
            }
        });
    });

    group.bench_function("strlen_10k", |b| {
        let store = KvStore::new();
        for i in 0..10_000 {
            let key = format!("key_{}", i);
            store.set(key.as_bytes(), b"hello world!");
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("key_{}", i);
                black_box(store.strlen(key.as_bytes()));
            }
        });
    });

    group.bench_function("getrange_10k", |b| {
        let store = KvStore::new();
        for i in 0..10_000 {
            let key = format!("key_{}", i);
            store.set(key.as_bytes(), b"Hello, World!");
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("key_{}", i);
                black_box(store.getrange(key.as_bytes(), 0, 4));
            }
        });
    });

    group.bench_function("setrange_10k", |b| {
        let store = KvStore::new();
        for i in 0..10_000 {
            let key = format!("key_{}", i);
            store.set(key.as_bytes(), b"Hello World");
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("key_{}", i);
                black_box(store.setrange(key.as_bytes(), 6, b"Redis"));
            }
        });
    });

    group.finish();
}

// ============================================================================
// WAL RECOVERY BENCHMARK (original)
// ============================================================================

fn benchmark_wal_recovery(c: &mut Criterion) {
    use fast_kv::{FsyncPolicy, Wal};

    let mut group = c.benchmark_group("wal_recovery");

    group.bench_function("recover_10k_entries", |b| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bench_recovery.wal");

        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();
        for i in 0..10_000 {
            let key = format!("k{}", i);
            let val = format!("v{}", i);
            wal.set(key.as_bytes(), val.as_bytes()).unwrap();
        }
        drop(wal);

        b.iter(|| {
            black_box(Wal::recover(&path).unwrap());
        });
    });

    group.finish();
}

// ============================================================================
// EXPIRATION BENCHMARK (original)
// ============================================================================

fn benchmark_expiration(c: &mut Criterion) {
    use fast_kv::ExpirationManager;

    let mut group = c.benchmark_group("expiration");

    group.bench_function("expire_10k", |b| {
        let store = Arc::new(KvStore::new());
        let exp = Arc::new(ExpirationManager::new(Arc::clone(&store)));
        for i in 0..10_000 {
            let key = format!("key_{}", i);
            store.set(key.as_bytes(), b"value");
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("key_{}", i);
                black_box(exp.expire(key.as_bytes(), std::time::Duration::from_secs(60)));
            }
        });
    });

    group.bench_function("ttl_10k", |b| {
        let store = Arc::new(KvStore::new());
        let exp = Arc::new(ExpirationManager::new(Arc::clone(&store)));
        for i in 0..10_000 {
            let key = format!("key_{}", i);
            store.set(key.as_bytes(), b"value");
            exp.expire(key.as_bytes(), std::time::Duration::from_secs(60));
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("key_{}", i);
                black_box(exp.ttl(key.as_bytes()));
            }
        });
    });

    group.finish();
}

// ============================================================================
// BLOB ARENA BENCHMARKS (feature: blob-store)
// ============================================================================

#[cfg(feature = "blob-store")]
fn benchmark_blob_arena(c: &mut Criterion) {
    use fast_kv::{BlobArena, BlobRef};

    let mut group = c.benchmark_group("blob_arena");

    // --- Store benchmarks: different payload sizes ---
    for (label, payload_size) in [("256B", 256), ("1KB", 1024), ("4KB", 4096), ("10KB", 10240)] {
        group.bench_with_input(
            BenchmarkId::new("store", label),
            &payload_size,
            |b, &payload_size| {
                let arena = BlobArena::new();
                let payload: Vec<u8> = (0..payload_size).map(|i| (i % 256) as u8).collect();
                b.iter(|| {
                    black_box(arena.store(black_box(&payload)));
                });
            },
        );
    }

    // --- Retrieve benchmarks: different payload sizes ---
    for (label, payload_size) in [("256B", 256), ("1KB", 1024), ("4KB", 4096), ("10KB", 10240)] {
        group.bench_with_input(
            BenchmarkId::new("retrieve", label),
            &payload_size,
            |b, &payload_size| {
                let arena = BlobArena::new();
                let payload: Vec<u8> = (0..payload_size).map(|i| (i % 256) as u8).collect();
                let blob_ref = arena.store(&payload).unwrap();
                b.iter(|| {
                    black_box(arena.retrieve(black_box(&blob_ref)));
                });
            },
        );
    }

    // --- Retrieve raw (compressed) ---
    group.bench_function("retrieve_raw_4kb", |b| {
        let arena = BlobArena::new();
        let payload: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        let blob_ref = arena.store(&payload).unwrap();
        b.iter(|| {
            black_box(arena.retrieve_raw(black_box(&blob_ref)));
        });
    });

    // --- BlobRef encode/decode ---
    group.bench_function("blobref_encode_decode", |b| {
        let arena = BlobArena::new();
        let payload = b"benchmark payload for blobref round-trip testing";
        let blob_ref = arena.store(payload).unwrap();
        b.iter(|| {
            let encoded = blob_ref.encode();
            let decoded = BlobRef::decode(&encoded).unwrap();
            black_box(decoded);
        });
    });

    // --- Stats ---
    group.bench_function("stats_10k_blobs", |b| {
        let arena = BlobArena::new();
        let payload: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        for _ in 0..10_000 {
            let _ = arena.store(&payload);
        }
        b.iter(|| {
            black_box(arena.stats());
        });
    });

    // --- Compression ratio measurement (one-time, not iterated) ---
    group.bench_function("compression_ratio_4kb", |b| {
        let arena = BlobArena::new();
        // Simulate a browser session JSON (~4 KB)
        let payload: Vec<u8> = (0..4096).map(|i| {
            // Make it look somewhat like JSON (repetitive patterns → good compression)
            match i % 64 {
                0..=3 => b'{',
                4..=10 => b'"',
                11..=40 => b'a' + (i % 26) as u8,
                41 => b'"',
                42 => b':',
                43 => b'"',
                44..=60 => b'0' + (i % 10) as u8,
                61 => b'"',
                62 => b',',
                63 => b'}',
                _ => b' ',
            }
        }).collect();
        b.iter(|| {
            let blob_ref = arena.store(black_box(&payload)).unwrap();
            black_box(blob_ref);
        });
    });

    group.finish();
}

#[cfg(feature = "blob-store")]
fn benchmark_blob_vs_inline(c: &mut Criterion) {
    use fast_kv::{BlobArena, BlobRef};

    let mut group = c.benchmark_group("blob_vs_inline");

    // Compare BSET (blob) vs SET (inline) for 4 KB values
    let payload_4kb: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
    let payload_small: Vec<u8> = b"small_value_12345".to_vec();

    // --- Write: small inline vs large blob ---
    group.bench_function("set_inline_10k_small", |b| {
        let store = KvStore::new();
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("k{}", i);
                black_box(store.set(key.as_bytes(), &payload_small));
            }
        });
    });

    group.bench_function("bset_blob_10k_4kb", |b| {
        let store = KvStore::new();
        let arena = BlobArena::new();
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("k{}", i);
                let blob_ref = arena.store(&payload_4kb).unwrap();
                black_box(store.set(key.as_bytes(), &blob_ref.encode()));
            }
        });
    });

    // --- Read: small inline vs large blob (decompress) ---
    group.bench_function("get_inline_10k_small", |b| {
        let store = KvStore::new();
        for i in 0..10_000 {
            let key = format!("k{}", i);
            store.set(key.as_bytes(), &payload_small);
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("k{}", i);
                black_box(store.get(key.as_bytes()));
            }
        });
    });

    group.bench_function("bget_blob_10k_4kb", |b| {
        let store = KvStore::new();
        let arena = BlobArena::new();
        for i in 0..10_000 {
            let key = format!("k{}", i);
            let blob_ref = arena.store(&payload_4kb).unwrap();
            store.set(key.as_bytes(), &blob_ref.encode());
        }
        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("k{}", i);
                if let Some(val) = store.get(key.as_bytes()) {
                    if BlobArena::is_blob_ref(&val) {
                        if let Some(br) = BlobRef::decode(&val) {
                            black_box(arena.retrieve(&br));
                        }
                    }
                }
            }
        });
    });

    group.finish();
}

// ============================================================================
// SIMHASH / MINHASH / LSH BENCHMARKS (feature: similarity)
// ============================================================================

#[cfg(feature = "similarity")]
fn benchmark_simhash(c: &mut Criterion) {
    use fast_kv::core::simhash;

    let mut group = c.benchmark_group("simhash");

    // --- hash64 raw ---
    group.bench_function("hash64_1k", |b| {
        let data: Vec<u8> = (0..256).map(|i| i as u8).cycle().take(256).collect();
        b.iter(|| {
            for _ in 0..1_000 {
                black_box(simhash::hash64(black_box(&data)));
            }
        });
    });

    // --- simhash from features ---
    group.bench_function("simhash_6_features", |b| {
        let features: Vec<(Vec<u8>, i32)> = vec![
            (b"Mozilla/5.0 Chrome/148".to_vec(), 5),
            (b"Windows 11".to_vec(), 4),
            (b"1920x1080".to_vec(), 3),
            (b"NVIDIA RTX 4060".to_vec(), 3),
            (b"America/New_York".to_vec(), 2),
            (b"en-US".to_vec(), 2),
        ];
        b.iter(|| {
            black_box(simhash::simhash(black_box(&features)));
        });
    });

    // --- simhash_uniform ---
    group.bench_function("simhash_uniform_6_features", |b| {
        let features: Vec<&[u8]> = vec![
            b"Mozilla/5.0 Chrome/148",
            b"Windows 11",
            b"1920x1080",
            b"NVIDIA RTX 4060",
            b"America/New_York",
            b"en-US",
        ];
        b.iter(|| {
            black_box(simhash::simhash_uniform(black_box(&features)));
        });
    });

    // --- hamming_distance ---
    group.bench_function("hamming_distance_10k", |bencher| {
        let a: u64 = 0xABCD_EF01_2345_6789;
        let b_val: u64 = 0xABCD_EF01_2345_6780;
        bencher.iter(|| {
            for _ in 0..10_000 {
                black_box(simhash::hamming_distance(a, b_val));
            }
        });
    });

    // --- is_similar ---
    group.bench_function("is_similar_10k", |bencher| {
        let a: u64 = 0xABCD_EF01_2345_6789;
        let b_val: u64 = 0xABCD_EF01_2345_6780;
        bencher.iter(|| {
            for _ in 0..10_000 {
                black_box(simhash::is_similar(a, b_val, 3));
            }
        });
    });

    group.finish();
}

#[cfg(feature = "similarity")]
fn benchmark_minhash(c: &mut Criterion) {
    use fast_kv::core::minhash::MinHashSig;

    let mut group = c.benchmark_group("minhash");

    // --- MinHash signature computation ---
    let elements: Vec<&[u8]> = vec![
        b"Arial", b"Helvetica", b"Times New Roman", b"Courier New",
        b"Verdana", b"Georgia", b"Trebuchet MS", b"Impact",
    ];

    group.bench_function("minhash_128_8_elements", |b| {
        b.iter(|| {
            black_box(MinHashSig::new(black_box(&elements), 128));
        });
    });

    group.bench_function("minhash_256_8_elements", |b| {
        b.iter(|| {
            black_box(MinHashSig::new(black_box(&elements), 256));
        });
    });

    // --- Jaccard similarity ---
    group.bench_function("jaccard_similarity", |b| {
        let sig_a = MinHashSig::new(&elements, 128);
        let elements_b: Vec<&[u8]> = vec![
            b"Arial", b"Helvetica", b"Times New Roman", b"ComicSans",
            b"Verdana", b"Georgia", b"ArialBlack", b"Impact",
        ];
        let sig_b = MinHashSig::new(&elements_b, 128);
        b.iter(|| {
            black_box(sig_a.jaccard_similarity(&sig_b));
        });
    });

    // --- Serialization ---
    group.bench_function("minhash_to_bytes_128", |b| {
        let sig = MinHashSig::new(&elements, 128);
        b.iter(|| {
            black_box(sig.to_bytes());
        });
    });

    group.bench_function("minhash_from_bytes_128", |b| {
        let sig = MinHashSig::new(&elements, 128);
        let bytes = sig.to_bytes();
        b.iter(|| {
            black_box(MinHashSig::from_bytes(&bytes));
        });
    });

    group.finish();
}

#[cfg(feature = "similarity")]
fn benchmark_lsh(c: &mut Criterion) {
    use fast_kv::core::lsh;
    use fast_kv::core::simhash;

    let mut group = c.benchmark_group("lsh");

    // --- lsh_add_sim: index 1K profiles ---
    group.bench_function("lsh_add_sim_1k_profiles", |b| {
        let store = KvStore::new();
        let features: Vec<(Vec<u8>, i32)> = vec![
            (b"Chrome/148".to_vec(), 5),
            (b"Windows".to_vec(), 4),
            (b"1920x1080".to_vec(), 3),
        ];
        b.iter(|| {
            for i in 0..1_000 {
                let key = format!("profile:{}", i);
                // Slightly vary the simhash per profile
                let mut f = features.clone();
                f.push((format!("variant_{}", i).into_bytes(), 1));
                let sh = simhash::simhash(&f);
                lsh::lsh_add_sim(&store, key.as_bytes(), sh, 4);
            }
        });
    });

    // --- find_similar_sim: search among indexed profiles ---
    group.bench_function("find_similar_sim_1k_indexed", |b| {
        let store = KvStore::new();
        // Pre-populate 1K profiles in LSH
        for i in 0..1_000 {
            let key = format!("profile:{}", i);
            let features: Vec<(Vec<u8>, i32)> = vec![
                (b"Chrome/148".to_vec(), 5),
                (b"Windows".to_vec(), 4),
                (b"1920x1080".to_vec(), 3),
                (format!("variant_{}", i).into_bytes(), 1),
            ];
            let sh = simhash::simhash(&features);
            lsh::lsh_add_sim(&store, key.as_bytes(), sh, 4);
        }

        // Search with a near-duplicate query
        let query_features: Vec<(Vec<u8>, i32)> = vec![
            (b"Chrome/148".to_vec(), 5),
            (b"Windows".to_vec(), 4),
            (b"1920x1080".to_vec(), 3),
            (b"variant_42".to_vec(), 1),
        ];
        let query_hash = simhash::simhash(&query_features);

        b.iter(|| {
            black_box(lsh::find_similar_sim(&store, query_hash, 4, 3));
        });
    });

    // --- lsh_rem_sim ---
    group.bench_function("lsh_rem_sim_1k", |b| {
        let store = KvStore::new();
        for i in 0..1_000 {
            let key = format!("profile:{}", i);
            let features: Vec<(Vec<u8>, i32)> = vec![
                (b"Chrome/148".to_vec(), 5),
                (format!("variant_{}", i).into_bytes(), 1),
            ];
            let sh = simhash::simhash(&features);
            lsh::lsh_add_sim(&store, key.as_bytes(), sh, 4);
        }
        // Measure removal speed
        b.iter(|| {
            for i in 0..1_000 {
                let key = format!("profile:{}", i);
                let features: Vec<(Vec<u8>, i32)> = vec![
                    (b"Chrome/148".to_vec(), 5),
                    (format!("variant_{}", i).into_bytes(), 1),
                ];
                let sh = simhash::simhash(&features);
                lsh::lsh_rem_sim(&store, key.as_bytes(), sh, 4);
            }
        });
    });

    group.finish();
}

// ============================================================================
// SCAN / DBSTATS BENCHMARKS
// ============================================================================

fn benchmark_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    // SCAN iterates over all buckets in the hash table, so the store capacity
    // must be close to the key count to avoid scanning 100K empty slots.
    // We use with_capacity(2 * count) for ~50% load factor.
    //
    // IMPORTANT: We measure a SINGLE scan() call per iteration, not a full
    // cursor loop. A full scan loop does O(n) work which makes Criterion's
    // default sampling (100 samples × 5 s) take 20+ minutes. Measuring one
    // scan call gives meaningful per-operation latency and finishes quickly.

    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    // --- Single SCAN call (cursor=0, count=500) ---
    for (label, count) in [("1k", 1_000), ("10k", 10_000)] {
        group.bench_with_input(
            BenchmarkId::new("single", label),
            &count,
            |b, &count| {
                let store = KvStore::with_capacity(count * 2);
                for i in 0..count {
                    let key = format!("session:{}", i);
                    let val = format!("data_{}", i);
                    store.set(key.as_bytes(), val.as_bytes());
                }
                b.iter(|| {
                    black_box(store.scan(0, 500, None));
                });
            },
        );
    }

    // --- Full SCAN loop (all keys, small stores only) ---
    for (label, count) in [("1k", 1_000)] {
        group.bench_with_input(
            BenchmarkId::new("full_loop", label),
            &count,
            |b, &count| {
                let store = KvStore::with_capacity(count * 2);
                for i in 0..count {
                    let key = format!("session:{}", i);
                    let val = format!("data_{}", i);
                    store.set(key.as_bytes(), val.as_bytes());
                }
                b.iter(|| {
                    let mut cursor = 0;
                    loop {
                        let (next_cursor, keys) = store.scan(cursor, 500, None);
                        black_box(&keys);
                        cursor = next_cursor;
                        if cursor == 0 {
                            break;
                        }
                    }
                });
            },
        );
    }

    // --- SCAN with MATCH pattern (single call) ---
    group.bench_function("match_single_10k", |b| {
        let store = KvStore::with_capacity(20_000);
        for i in 0..10_000 {
            let prefix = if i % 3 == 0 { "session" } else if i % 3 == 1 { "profile" } else { "cache" };
            let key = format!("{}:{}", prefix, i);
            let val = format!("data_{}", i);
            store.set(key.as_bytes(), val.as_bytes());
        }
        b.iter(|| {
            black_box(store.scan(0, 500, Some(b"session:*")));
        });
    });

    group.finish();
}

fn benchmark_dbstats(c: &mut Criterion) {
    let mut group = c.benchmark_group("dbstats");
    // DBSTATS iterates over all buckets, so use tight capacity.

    for (label, count) in [("1k", 1_000), ("10k", 10_000)] {
        group.bench_with_input(
            BenchmarkId::new("dbstats", label),
            &count,
            |b, &count| {
                let store = KvStore::with_capacity(count * 2);
                for i in 0..count {
                    let key = format!("key_{}", i);
                    store.set(key.as_bytes(), b"value");
                }
                b.iter(|| {
                    black_box(store.dbstats());
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// COMPRESSED WAL SEGMENT BENCHMARKS (feature: blob-store)
// ============================================================================

#[cfg(feature = "blob-store")]
fn benchmark_wal_segment(c: &mut Criterion) {
    use fast_kv::{SegmentConfig, WalSegment};

    let mut group = c.benchmark_group("wal_segment");

    // --- Write 10K SET entries (compressed segments) ---
    group.bench_function("segment_write_10k_set", |b| {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.max_segment_size = 1024 * 1024; // 1 MB → more rotation
        config.raw_count = 0; // compress everything
        let segment = WalSegment::open(config).unwrap();

        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("k{}", i);
                let val = format!("v{}", i);
                let _ = black_box(segment.set(key.as_bytes(), val.as_bytes()));
            }
        });
    });

    // --- Write 10K BSET entries (large values → good compression) ---
    group.bench_function("segment_write_10k_bset_4kb", |b| {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.max_segment_size = 1024 * 1024; // 1 MB
        config.raw_count = 0;
        let segment = WalSegment::open(config).unwrap();
        let payload: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();

        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("k{}", i);
                let _ = black_box(segment.bset(key.as_bytes(), &payload));
            }
        });
    });

    // --- Recovery: replay from compressed segments ---
    group.bench_function("segment_recover_10k", |b| {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.max_segment_size = 1024 * 1024;
        config.raw_count = 0;
        {
            let segment = WalSegment::open(config).unwrap();
            for i in 0..10_000 {
                let key = format!("k{}", i);
                let val = format!("v{}", i);
                segment.set(key.as_bytes(), val.as_bytes()).unwrap();
            }
            segment.close().unwrap();
        }
        b.iter(|| {
            black_box(fast_kv::recover_segments(dir.path()).unwrap());
        });
    });

    // --- Compressed vs raw WAL write ---
    group.bench_function("segment_write_raw_10k", |b| {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SegmentConfig::new(dir.path());
        config.max_segment_size = 64 * 1024 * 1024; // 64 MB → effectively raw
        config.raw_count = u32::MAX; // never compress
        let segment = WalSegment::open(config).unwrap();

        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("k{}", i);
                let val = format!("v{}", i);
                let _ = black_box(segment.set(key.as_bytes(), val.as_bytes()));
            }
        });
    });

    group.finish();
}

// ============================================================================
// REGISTER ALL BENCHMARKS
// ============================================================================

// Core benchmarks (always compiled)
criterion_group!(
    core_benches,
    benchmark_set,
    benchmark_get,
    benchmark_incr,
    benchmark_exists,
    benchmark_mget,
    benchmark_threaded_set,
    benchmark_threaded_get,
    benchmark_threaded_incr,
    benchmark_wal_write,
    benchmark_append,
    benchmark_wal_recovery,
    benchmark_expiration,
    benchmark_scan,
    benchmark_dbstats,
);

// Blob Arena benchmarks (feature: blob-store)
#[cfg(feature = "blob-store")]
criterion_group!(blob_benches, benchmark_blob_arena, benchmark_blob_vs_inline, benchmark_wal_segment);

// Similarity benchmarks (feature: similarity)
#[cfg(feature = "similarity")]
criterion_group!(similarity_benches, benchmark_simhash, benchmark_minhash, benchmark_lsh);

// Merge all groups
#[cfg(all(feature = "blob-store", feature = "similarity"))]
criterion_main!(core_benches, blob_benches, similarity_benches);

#[cfg(all(feature = "blob-store", not(feature = "similarity")))]
criterion_main!(core_benches, blob_benches);

#[cfg(all(not(feature = "blob-store"), feature = "similarity"))]
criterion_main!(core_benches, similarity_benches);

#[cfg(all(not(feature = "blob-store"), not(feature = "similarity")))]
criterion_main!(core_benches);
