//! Criterion Benchmarks for FastKV
//!
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use fast_kv::KvStore;
use std::sync::Arc;
use std::thread;

// ============================================================================
// SINGLE-THREADED BENCHMARKS
// ============================================================================

fn benchmark_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_operations");
    
    for size in [100, 1000, 10000].iter() {
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
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let store = KvStore::new();
            
            // Pre-populate
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

// ============================================================================
// MULTI-THREADED BENCHMARKS
// ============================================================================

fn benchmark_threaded_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("threaded_set");
    
    for num_threads in [1, 2, 4].iter() {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let store = Arc::new(KvStore::new());
                    let ops_per_thread = 10000 / num_threads;
                    
                    let mut handles = vec![];
                    for t in 0..num_threads {
                        let store = Arc::clone(&store);
                        let handle = thread::spawn(move || {
                            for i in 0..ops_per_thread {
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
                    
                    black_box(store)
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_threaded_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("threaded_get");
    
    for num_threads in [1, 2, 4].iter() {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            num_threads,
            |b, &num_threads| {
                // Create and populate store INSIDE the benchmark iteration
                // This ensures each iteration has its own fresh store
                let store = KvStore::new();
                for i in 0..10000 {
                    let key = format!("key_{}", i);
                    store.set(key.as_bytes(), b"value");
                }
                let store = Arc::new(store);
                let ops_per_thread = 10000 / num_threads;
                
                b.iter(|| {
                    let mut handles = vec![];
                    for t in 0..num_threads {
                        // Clone Arc for each thread
                        let store = Arc::clone(&store);
                        let handle = thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                let key = format!("key_{}", (t * ops_per_thread + i) % 10000);
                                black_box(store.get(key.as_bytes()));
                            }
                        });
                        handles.push(handle);
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

// ============================================================================
// REGISTER BENCHMARKS
// ============================================================================

criterion_group!(
    benches,
    benchmark_set,
    benchmark_get,
    benchmark_threaded_set,
    benchmark_threaded_get,
);

criterion_main!(benches);
