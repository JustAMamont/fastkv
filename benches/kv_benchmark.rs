//! Criterion Benchmarks for FastKV
//!
//! Run with: `cargo bench`
//!
//! Benchmarks cover core operations (SET, GET, INCR, EXISTS, MGET)
//! as well as multi-threaded scenarios.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use fast_kv::KvStore;
use std::sync::Arc;
use std::thread;

// ============================================================================
// SINGLE-THREADED BENCHMARKS
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
                black_box(store.incr(key.as_bytes(), 1));
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
// MULTI-THREADED BENCHMARKS
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
                    for handle in handles { handle.join().unwrap(); }
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
                    for handle in handles { handle.join().unwrap(); }
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
            for t in 0..8 {
                let store = Arc::clone(&store);
                handles.push(thread::spawn(move || {
                    for i in 0..125 {
                        let key = format!("counter_{}", i);
                        black_box(store.incr(key.as_bytes(), 1));
                    }
                }));
            }
            for handle in handles { handle.join().unwrap(); }
        });
    });
    group.finish();
}

// ============================================================================
// WAL BENCHMARKS
// ============================================================================

fn benchmark_wal_write(c: &mut Criterion) {
    use fast_kv::{Wal, FsyncPolicy};
    use std::path::PathBuf;

    let mut group = c.benchmark_group("wal_write");

    group.bench_function("set_10k_fsync_never", |b| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bench.wal");
        let wal = Wal::open(&path, FsyncPolicy::Never).unwrap();

        b.iter(|| {
            for i in 0..10_000 {
                let key = format!("k{}", i);
                let val = format!("v{}", i);
                black_box(wal.set(key.as_bytes(), val.as_bytes()));
            }
        });
    });
    group.finish();
}

// ============================================================================
// STRING OPERATION BENCHMARKS
// ============================================================================

/// Benchmark APPEND — append 10 bytes to existing keys.
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
// WAL RECOVERY BENCHMARK
// ============================================================================

/// Benchmark WAL recovery — replay 10,000 entries from a WAL file.
fn benchmark_wal_recovery(c: &mut Criterion) {
    use fast_kv::{FsyncPolicy, Wal};

    let mut group = c.benchmark_group("wal_recovery");

    group.bench_function("recover_10k_entries", |b| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bench_recovery.wal");

        // Pre-populate WAL with 10,000 entries.
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
// EXPIRATION BENCHMARK
// ============================================================================

/// Benchmark expiration operations (expire, ttl, purge).
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
// REGISTER BENCHMARKS
// ============================================================================

criterion_group!(
    benches,
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
);

criterion_main!(benches);
