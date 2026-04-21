# FastKV

High-performance, Redis-compatible key-value store written in Rust.

## Features

- **Lock-free hash table** — thread-safe without mutexes, uses atomic CAS and optimistic version reads
- **Redis-compatible** — supports RESP protocol, works with `redis-cli`
- **Pipeline support** — batch multiple commands for higher throughput
- **WAL persistence** — crash-consistent write-ahead log with configurable fsync (Phase 2)
- **String operations** — INCR, DECR, INCRBY, APPEND, STRLEN, GETRANGE, SETRANGE, MGET, MSET, EXISTS (Phase 3)
- **TTL / Expiration** — EXPIRE, TTL, PTTL, PERSIST with lazy + active key purging (Phase 4)
- **Hash data type** — HSET, HGET, HDEL, HGETALL, HEXISTS, HLEN, HKEYS, HVALS, HMGET, HMSET (Phase 6)
- **io_uring (Linux)** — optional kernel bypass for maximum performance
- **Cross-platform** — works on Linux, macOS, Windows

## Performance

```
┌─────────────────────────────────────────────────────────────────┐
│  FastKV vs Redis (io_uring, 2 cores)                            │
├─────────────────────────────────────────────────────────────────┤
│  Sequential:   2.2x faster                                      │
│  Pipeline:     1.6x faster (batch=100)                          │
│  Multi-thread: 2.1x faster (8 threads)                          │
├─────────────────────────────────────────────────────────────────┤
│  Peak throughput: ~713,000 ops/sec (GET, pipeline batch=500)    │
└─────────────────────────────────────────────────────────────────┘
```

## Requirements

- Rust 1.90+
- Linux (for io_uring), macOS, or Windows

## Build

```bash
git clone <repo-url>
cd fast_kv

# Debug build (fast compilation)
cargo build

# Release build (maximum performance)
cargo build --release

# With io_uring support (Linux only)
cargo build --release --features io-uring
```

## Run Server

```bash
# Default server (Tokio, cross-platform) — WAL + TTL enabled by default
cargo run --release -- server 6379

# With io_uring (Linux only, requires --features io-uring)
cargo run --release --features io-uring -- server 6379 io_uring

# Custom port
cargo run --release -- server 6380
```

## Environment Variables

| Variable      | Default            | Description                                           |
|---------------|--------------------|-------------------------------------------------------|
| `FASTKV_DIR`  | `./fastkv_data`    | Directory for WAL file                                |
| `FASTKV_FSYNC`| `everysec`         | WAL fsync policy: `always`, `everysec`, or `never`    |

```bash
# Strongest durability (fsync after every write)
FASTKV_FSYNC=always cargo run --release -- server 6379

# Maximum throughput (rely on OS page cache)
FASTKV_FSYNC=never cargo run --release -- server 6379
```

## Test with redis-cli

```bash
redis-cli -p 6379
```

### Core commands

```
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
127.0.0.1:6379> DEL hello
(integer) 1
127.0.0.1:6379> GET hello
(nil)
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> DBSIZE
(integer) 0
127.0.0.1:6379> EXISTS mykey
(integer) 0
```

### String operations (Phase 3)

```
127.0.0.1:6379> SET counter 10
OK
127.0.0.1:6379> INCR counter
(integer) 11
127.0.0.1:6379> INCRBY counter 5
(integer) 16
127.0.0.1:6379> DECR counter
(integer) 15
127.0.0.1:6379> DECRBY counter 10
(integer) 5

127.0.0.1:6379> SET msg Hello
OK
127.0.0.1:6379> APPEND msg " World"
(integer) 11
127.0.0.1:6379> STRLEN msg
(integer) 11
127.0.0.1:6379> GETRANGE msg 0 4
"Hello"
127.0.0.1:6379> SETRANGE msg 6 "Redis"
(integer) 11
127.0.0.1:6379> GET msg
"Hello Redis"

127.0.0.1:6379> MSET a 1 b 2 c 3
OK
127.0.0.1:6379> MGET a b c missing
1) "1"
2) "2"
3) "3"
4) (nil)
```

### Expiration commands (Phase 4)

```
127.0.0.1:6379> SET session abc123
OK
127.0.0.1:6379> EXPIRE session 3600
(integer) 1
127.0.0.1:6379> TTL session
(integer) 3599
127.0.0.1:6379> PTTL session
(integer) 3598976
127.0.0.1:6379> PERSIST session
(integer) 1
127.0.0.1:6379> TTL session
(integer) -2
```

### Hash commands (Phase 6)

```
127.0.0.1:6379> HSET myhash name Alice
(integer) 1
127.0.0.1:6379> HSET myhash age 30
(integer) 1
127.0.0.1:6379> HGET myhash name
"Alice"
127.0.0.1:6379> HGETALL myhash
1) "name"
2) "Alice"
3) "age"
4) "30"
127.0.0.1:6379> HEXISTS myhash name
(integer) 1
127.0.0.1:6379> HLEN myhash
(integer) 2
127.0.0.1:6379> HDEL myhash age
(integer) 1
127.0.0.1:6379> TTL myhash
(integer) -2
```

## Testing

```bash
# Run all unit tests
cargo test

# Run tests for a specific module
cargo test --lib -- core::kv::tests
cargo test --lib -- core::wal::tests
cargo test --lib -- core::expiration::tests
cargo test --lib -- core::resp::tests
cargo test --lib -- core::server::tcp::tests

# Built-in smoke tests (no cargo test required)
cargo run --release -- test
```

### Test coverage (45+ tests)

| Module         | Tests | Coverage                                    |
|----------------|-------|---------------------------------------------|
| `kv.rs`        | 25    | Core GET/SET/DEL, INCR/DECR, MGET/MSET, APPEND, STRLEN, GETRANGE, SETRANGE, edge cases (inline size, capacity, concurrent) |
| `wal.rs`       | 14    | Create/recover, CRC, alignment, binary keys, large values, empty keys/values, error display |
| `expiration.rs`| 14    | Expire/TTL/persist, lazy/active purging, concurrent, edge cases |
| `resp.rs`      | 16    | RESP array/inline parse, encode, roundtrip, binary data, error types |
| `tcp.rs`       | 18    | All command handlers (GET/SET/DEL/INCR/EXPIRE/TTL/...), inline, ping, echo, info |

## Benchmarks

### In-memory (no network)

```bash
# Single-threaded
cargo run --release -- bench

# Multi-threaded
cargo run --release -- bench-threads

# With Criterion — all benchmarks
cargo bench --bench kv_benchmark
```

Criterion benchmark groups:
- `set_operations` — SET with varying key counts (100, 1K, 10K)
- `get_operations` — GET with varying key counts
- `incr_operations` — atomic INCR on 10K counters
- `exists_operations` — EXISTS on 10K keys
- `mget_operations` — MGET 100 keys
- `string_operations` — APPEND, STRLEN, GETRANGE, SETRANGE on 10K keys
- `wal_write` — WAL SET with fsync=never
- `wal_recovery` — replay 10K entries from WAL file
- `expiration` — EXPIRE and TTL on 10K keys
- `threaded_set/get/incr` — concurrent operations (1/2/4/8 threads)

### Network (vs Redis)

```bash
# Terminal 1: Start Redis
docker run -d --name redis -p 6379:6379 redis

# Terminal 2: Start FastKV
cargo run --release --features io-uring -- server 6380 io_uring

# Terminal 3: Run benchmark
cargo run --release --example netbench -- all

# Specific tests
cargo run --release --example netbench -- seq       # Sequential only
cargo run --release --example netbench -- pipeline  # Pipeline only
cargo run --release --example netbench -- multi     # Multi-threaded only

# Custom operations count
cargo run --release --example netbench -- all 50000
```

## Project Structure

```
fast_kv/
├── Cargo.toml                 # Dependencies and features
├── src/
│   ├── lib.rs                 # Library entry point
│   ├── main.rs                # CLI application
│   └── core/
│       ├── mod.rs             # Core module
│       ├── kv.rs              # Lock-free hash table + string operations
│       ├── resp.rs            # RESP protocol parser/encoder
│       ├── wal.rs             # Write-Ahead Log (Phase 2)
│       ├── expiration.rs      # TTL / key expiration (Phase 4)
│       ├── hash.rs             # Hash data type (Phase 6)
│       └── server/
│           ├── mod.rs         # Server module
│           ├── tcp.rs         # Tokio TCP server (cross-platform)
│           └── io_uring.rs    # io_uring server (Linux only)
├── benches/
│   ├── kv_benchmark.rs        # In-memory criterion benchmarks
│   └── network_benchmark.rs   # Network criterion benchmarks
└── examples/
    └── netbench.rs            # CLI network benchmark
```

## Supported Commands

| Command     | Status | Description                                |
|-------------|--------|--------------------------------------------|
| SET         | Done   | Set key-value                              |
| GET         | Done   | Get value by key                           |
| DEL         | Done   | Delete key                                 |
| PING        | Done   | Health check                               |
| ECHO        | Done   | Echo message                               |
| DBSIZE      | Done   | Number of keys                             |
| INFO        | Done   | Server info                                |
| COMMAND     | Done   | Command discovery                          |
| QUIT        | Done   | Close connection                           |
| EXISTS      | Done   | Check key exists                           |
| INCR        | Done   | Increment value by 1                       |
| INCRBY      | Done   | Increment value by N                       |
| DECR        | Done   | Decrement value by 1                       |
| DECRBY      | Done   | Decrement value by N                       |
| APPEND      | Done   | Append to string value                     |
| STRLEN      | Done   | Get string length                          |
| GETRANGE    | Done   | Get substring                              |
| SETRANGE    | Done   | Overwrite substring                        |
| MGET        | Done   | Get multiple keys                          |
| MSET        | Done   | Set multiple keys                          |
| EXPIRE      | Done   | Set TTL (seconds)                          |
| TTL         | Done   | Get remaining TTL (seconds)                |
| PTTL        | Done   | Get remaining TTL (milliseconds)           |
| PERSIST     | Done   | Remove TTL                                 |
| KEYS        | TODO   | List keys (pattern)                        |
| SCAN        | TODO   | Iterate keys                               |
| HGET/HSET   | Done   | Hash operations                            |
| HDEL        | Done   | Delete hash field                          |
| HGETALL     | Done   | Get all hash fields                        |
| HEXISTS     | Done   | Check field exists                         |
| HLEN        | Done   | Number of hash fields                      |
| HKEYS       | Done   | Get all field names                        |
| HVALS       | Done   | Get all field values                       |
| HMGET/HMSET | Done   | Multi-field hash operations                |
| LPUSH/RPUSH | TODO   | List operations                            |
| SADD/SREM   | TODO   | Set operations                             |
| ZADD/ZRANGE | TODO   | Sorted Set operations                      |

## Architecture

### Lock-free Hash Table

```
┌─────────────────────────────────────────────────────────────┐
│  LockFreeEntry (64-byte cache-line aligned)                 │
├─────────────────────────────────────────────────────────────┤
│  hash: AtomicU64      - Hash value (0 = empty)              │
│  key_len: AtomicU32   - Key length                          │
│  value_len: AtomicU32 - Value length                        │
│  version: AtomicU64   - For optimistic reading              │
│  data: [AtomicU8; 128] - Inline key + value storage         │
└─────────────────────────────────────────────────────────────┘

Operations:
- SET: CAS (Compare-And-Swap) for lock-free insertion
- GET: Optimistic read with version check
- DEL: Atomic hash reset
- INCR: CAS loop on version for atomic read-modify-write
```

### Memory Layout

```
Default capacity: 1,000,000 entries
Entry size: ~192 bytes
Total memory: ~192 MB

┌─────────┬─────────┬─────────┬─────┬─────────┐
│ Entry 0 │ Entry 1 │ Entry 2 │ ... │ Entry N │
└─────────┴─────────┴─────────┴─────┴─────────┘
```

### WAL (Write-Ahead Log)

```
┌──────────┬──────────┬──────────┬────────┬────┬────────┬────┬────────┐
│  magic   │  version │  crc32   │  op    │kl  │  key   │vl  │  value │
│ 4 bytes  │ 2 bytes  │ 4 bytes  │ 1 byte │2B  │ kl B   │2B  │  vl B  │
└──────────┴──────────┴──────────┴────────┴────┴────────┴────┴────────┘

Fsync policies:
  always    → fsync after every write (safest)
  everysec  → background thread fsyncs every ~1s (balanced)
  never     → rely on OS page cache (fastest)

Recovery: read all entries, verify CRC, replay into KV store.
Corrupted trailing entries are silently discarded.
```

### Expiration

```
Two strategies:
1. Lazy expiration — check TTL on every GET/EXISTS access
2. Active expiration — background thread purges expired keys every 100ms

Data structures:
  deadlines: Mutex<HashMap<Vec<u8>, Instant>>  (key → deadline)
```

## TODO / Roadmap

### Phase 1: Core (Done)

- [x] Lock-free hash table
- [x] RESP protocol parser
- [x] TCP server (Tokio)
- [x] Pipeline support
- [x] io_uring support (Linux)
- [x] Benchmarks

### Phase 2: Persistence (Done)

- [x] WAL (Write-Ahead Log)
  - [x] Append-only log file with binary format
  - [x] CRC-32C checksum per entry
  - [x] fsync options (always, every-second, never)
  - [x] 16-byte aligned entries (direct-I/O friendly)
- [x] Recovery on startup (replay WAL)
- [x] Environment variables for configuration (`FASTKV_DIR`, `FASTKV_FSYNC`)

### Phase 3: Data Types — String Operations (Done)

- [x] INCR, DECR, INCRBY, DECRBY
- [x] APPEND, STRLEN
- [x] GETRANGE, SETRANGE
- [x] MGET, MSET
- [x] EXISTS

### Phase 4: Expiration (Done)

- [x] TTL support (EXPIRE, TTL, PTTL, PERSIST)
- [x] Lazy expiration (check on access)
- [x] Active expiration (background thread, 200 keys/cycle, 100ms interval)

### Phase 5: Advanced Features (Priority: Low)

- [ ] Pub/Sub (SUBSCRIBE, PUBLISH, PSUBSCRIBE)
- [ ] Transactions (MULTI, EXEC, DISCARD, WATCH)
- [ ] Lua scripting
- [ ] Replication (master-slave, PSYNC)

### Phase 6: Composite Data Types (In Progress)

- [x] Hash (HGET, HSET, HDEL, HGETALL, HEXISTS, HLEN, HKEYS, HVALS, HMGET, HMSET)
- [ ] List (LPUSH, RPUSH, LPOP, RPOP, LRANGE)
- [ ] Set (SADD, SREM, SMEMBERS, SISMEMBER)
- [ ] Sorted Set (ZADD, ZREM, ZRANGE, ZSCORE)

### Phase 7: Vector Search (Future)

- [ ] HNSW (Hierarchical Navigable Small World)
- [ ] ANN search, cosine similarity, euclidean distance
- [ ] VADD, VSEARCH, VDEL commands

### Phase 8: Clustering (Future)

- [ ] Hash-slot sharding
- [ ] Node discovery, data migration, failover

### Phase 9: Performance Optimizations (In Progress)

- [x] **Zero-alloc key comparison** — stack buffers replace heap Vec
- [x] **Relaxed atomic ordering on data reads** — Ordering::Relaxed on AtomicU8
- [x] **Bounded probing** — MAX_PROBE_STEPS = 256
- [x] **Zero-alloc RESP encoding** — write_*() direct to buffer
- [x] **Hardware-accelerated CRC-32C** — SSE4.2/ARM CRC instructions
- [x] **Wyhash-inspired hash function** — 8-byte chunked processing (~2-3x faster than FNV-1a)
- [x] **Batched expiration** — single-lock scan + batch delete (5x less mutex contention)
- [x] **Single-buffer WAL writes** — eliminated extra payload Vec allocation
- [x] **Inline atoi RESP parsing** — no from_utf8+parse overhead
- [x] **Zero-alloc INFO command** — direct buffer writing, no format!()
- [ ] SIMD hash functions
- [ ] Zero-copy RESP parsing
- [ ] Memory pooling
- [ ] Batched network I/O
- [ ] CPU affinity, NUMA awareness

## Troubleshooting

### Connection Refused

```bash
netstat -tlnp | grep 6379
cargo run --release -- server 6380   # try a different port
```

### Low Performance

```bash
# 1. Use release build
cargo build --release

# 2. Check CPU governor (should be "performance", not "powersave")
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# 3. Use io_uring on Linux
cargo run --release --features io-uring -- server 6379 io_uring
```

### Compilation Error with io_uring

```bash
# io_uring requires Linux kernel 5.1+
uname -r
grep -i io_uring /boot/config-$(uname -r)
```

### WAL / Data Issues

```bash
# WAL file location (default)
ls ./fastkv_data/fastkv.wal

# Delete WAL to start fresh
rm ./fastkv_data/fastkv.wal
```

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -am 'Add feature'`
4. Push branch: `git push origin feature/my-feature`
5. Submit Pull Request

## License

MIT

## References

- [Redis Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [io_uring](https://kernel.dk/io_uring.pdf)
- [Lock-free Programming](https://preshing.com/20120612/an-introduction-to-lock-free-programming/)
- [HNSW Paper](https://arxiv.org/abs/1603.09320)
