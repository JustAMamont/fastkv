# FastKV

High-performance, Redis-compatible key-value store written in Rust.

## Features

- **Lock-free hash table** - Thread-safe without mutexes, uses atomic operations
- **Redis-compatible** - Supports RESP protocol, works with `redis-cli`
- **Pipeline support** - Batch multiple commands for higher throughput
- **io_uring (Linux)** - Optional kernel bypass for maximum performance
- **Cross-platform** - Works on Linux, macOS, Windows

## Performance

```
┌─────────────────────────────────────────────────────────────────┐
│  FastKV vs Redis (io_uring, 2 cores)                           │
├─────────────────────────────────────────────────────────────────┤
│  Sequential:   2.2x faster                                      │
│  Pipeline:     1.6x faster (batch=100)                          │
│  Multi-thread: 2.1x faster (8 threads)                          │
├─────────────────────────────────────────────────────────────────┤
│  Peak throughput: ~713,000 ops/sec (GET, pipeline batch=500)   │
└─────────────────────────────────────────────────────────────────┘
```

## Requirements

- Rust 1.90+
- Linux (for io_uring), macOS, or Windows

## Build

```bash
# Clone
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
# Default server (Tokio, cross-platform)
cargo run --release -- server 6379

# With io_uring (Linux only, requires --features io-uring)
cargo run --release --features io-uring -- server 6379 io_uring

# Custom port
cargo run --release -- server 6380
```

## Test with redis-cli

```bash
# Connect
redis-cli -p 6379

# Basic commands
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
```

## Benchmarks

### In-memory (no network)

```bash
# Single-threaded
cargo run --release -- bench

# Multi-threaded
cargo run --release -- bench-threads

# With Criterion
cargo bench --bench kv_benchmark
```

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

### Expected Output

```
================================================================================
  SEQUENTIAL (one request at a time)
================================================================================
Server                         |          SET |          GET |        MIXED
--------------------------------------------------------------------------------
Redis                          |         6251 |         6283 |         6400
FastKV                         |        13692 |        16236 |        14904
--------------------------------------------------------------------------------

================================================================================
  COMPARISON SUMMARY (FastKV vs Redis)
================================================================================
Test                           |        Redis |       FastKV |  Speedup
--------------------------------------------------------------------------------
FastKV                         |         6251 |        13692 |     2.2x
--------------------------------------------------------------------------------
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
│       ├── kv.rs              # Lock-free hash table
│       ├── resp.rs            # RESP protocol parser/encoder
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

| Command | Status | Description |
|---------|--------|-------------|
| SET | ✅ | Set key-value |
| GET | ✅ | Get value by key |
| DEL | ✅ | Delete key |
| PING | ✅ | Health check |
| ECHO | ✅ | Echo message |
| DBSIZE | ✅ | Number of keys |
| INFO | ✅ | Server info |
| COMMAND | ✅ | Command discovery |
| QUIT | ✅ | Close connection |
| MGET | ❌ | Get multiple keys |
| MSET | ❌ | Set multiple keys |
| INCR | ❌ | Increment value |
| DECR | ❌ | Decrement value |
| EXISTS | ❌ | Check key exists |
| EXPIRE | ❌ | Set TTL |
| TTL | ❌ | Get remaining TTL |
| KEYS | ❌ | List keys (pattern) |
| SCAN | ❌ | Iterate keys |

## Configuration

Environment variables (planned):

```bash
FASTKV_PORT=6379
FASTKV_MEMORY=1GB
FASTKV_MAX_KEY_SIZE=256
FASTKV_MAX_VALUE_SIZE=4096
```

## Architecture

### Lock-free Hash Table

```
┌─────────────────────────────────────────────────────────────┐
│  LockFreeEntry (64-byte cache-line aligned)                │
├─────────────────────────────────────────────────────────────┤
│  hash: AtomicU64      - Hash value (0 = empty)             │
│  key_len: AtomicU32   - Key length                         │
│  value_len: AtomicU32 - Value length                       │
│  version: AtomicU64   - For optimistic reading             │
│  data: [AtomicU8; 128] - Inline key + value storage        │
└─────────────────────────────────────────────────────────────┘

Operations:
- SET: CAS (Compare-And-Swap) for lock-free insertion
- GET: Optimistic read with version check
- DEL: Atomic hash reset
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

## TODO / Roadmap

### Phase 1: Core (Completed ✅)

- [x] Lock-free hash table
- [x] RESP protocol parser
- [x] TCP server (Tokio)
- [x] Pipeline support
- [x] io_uring support (Linux)
- [x] Benchmarks

### Phase 2: Persistence (Priority: High)

- [ ] WAL (Write-Ahead Log)
  - [ ] Append-only log file
  - [ ] fsync options (always, every-second, never)
  - [ ] Log compaction
- [ ] Snapshot (RDB-like)
  - [ ] Fork-based snapshot
  - [ ] Binary format
- [ ] Recovery on startup
- [ ] AOF (Append-Only File) format

### Phase 3: Data Types (Priority: Medium)

- [ ] String operations
  - [ ] INCR, DECR, INCRBY
  - [ ] APPEND, STRLEN
  - [ ] GETRANGE, SETRANGE
- [ ] Hash (HGET, HSET, HDEL, HGETALL)
- [ ] List (LPUSH, RPUSH, LPOP, RPOP, LRANGE)
- [ ] Set (SADD, SREM, SMEMBERS, SISMEMBER)
- [ ] Sorted Set (ZADD, ZREM, ZRANGE, ZSCORE)

### Phase 4: Expiration (Priority: Medium)

- [ ] TTL support
  - [ ] EXPIRE, PEXPIRE
  - [ ] EXPIREAT, PEXPIREAT
  - [ ] TTL, PTTL
  - [ ] PERSIST
- [ ] Lazy expiration (check on access)
- [ ] Active expiration (background thread)

### Phase 5: Advanced Features (Priority: Low)

- [ ] Pub/Sub
  - [ ] SUBSCRIBE, UNSUBSCRIBE
  - [ ] PUBLISH
  - [ ] PSUBSCRIBE (pattern)
- [ ] Transactions
  - [ ] MULTI, EXEC, DISCARD
  - [ ] WATCH, UNWATCH
- [ ] Lua scripting
- [ ] Replication
  - [ ] Master-slave
  - [ ] PSYNC

### Phase 6: Vector Search (Future)

- [ ] HNSW (Hierarchical Navigable Small World)
  - [ ] Vector indexing
  - [ ] ANN (Approximate Nearest Neighbor) search
  - [ ] Cosine similarity
  - [ ] Euclidean distance
- [ ] Commands
  - [ ] VADD - Add vector
  - [ ] VSEARCH - Search similar vectors
  - [ ] VDEL - Delete vector

### Phase 7: Clustering (Future)

- [ ] Sharding
  - [ ] Hash slot based (like Redis Cluster)
  - [ ] Consistent hashing
- [ ] Node discovery
- [ ] Data migration
- [ ] Failover

### Phase 8: Performance Optimizations

- [ ] SIMD hash functions
- [ ] Zero-copy parsing
- [ ] Memory pooling
- [ ] Batched network I/O
- [ ] CPU affinity
- [ ] NUMA awareness

## Benchmarking Tips

### Fair Comparison

```bash
# 1. Same machine
# 2. Same port (no other services)
# 3. Release build
# 4. Warm up before measuring
# 5. Multiple runs

# Run 3 times and average
for i in {1..3}; do
    cargo run --release --example netbench -- all
done
```

### Check io_uring is Used

```bash
# io_uring server shows this on startup:
# "🚀 Using io_uring for maximum performance!"

# Verify with strace (Linux)
strace -e trace=io_uring_setup,io_uring_enter -p <pid>
```

## Troubleshooting

### Connection Refused

```bash
# Check if server is running
netstat -tlnp | grep 6379

# Check firewall
sudo ufw status

# Try different port
cargo run --release -- server 6380
```

### Low Performance

```bash
# 1. Use release build
cargo build --release

# 2. Check CPU governor
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
# Should be "performance", not "powersave"

# 3. Use io_uring on Linux
cargo run --release --features io-uring -- server 6379 io_uring

# 4. Increase backlog
# Edit src/core/server/tcp.rs: SOMAXCONN
```

### Compilation Error with io_uring

```bash
# io_uring requires Linux kernel 5.1+
uname -r

# Check if io_uring is supported
grep -i io_uring /boot/config-$(uname -r)
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
