# FastKV

High-performance, Redis-compatible key-value store written in Rust with lock-free data structures and zero-dependency client SDKs in Go, Python, Java, and Node.js.

## Features

- **Lock-free hash table** — thread-safe without mutexes; uses atomic CAS and optimistic version reads
- **Redis-compatible RESP protocol** — works with `redis-cli` and any Redis-compatible tooling
- **Pipeline support** — batch multiple commands in a single round-trip for higher throughput
- **WAL persistence** — crash-consistent write-ahead log with configurable fsync policy and TTL recovery
- **TTL / Expiration** — `EXPIRE`, `TTL`, `PTTL`, `PERSIST` with lazy + active key purging, persisted to WAL
- **Hash data type** — `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HMGET`, `HMSET`
- **List data type** — `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LREM`, `LTRIM`, `LSET`
- **String operations** — `INCR`, `DECR`, `INCRBY`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `MGET`, `MSET`, `EXISTS`
- **io_uring (Linux)** — optional kernel-bypass networking for maximum throughput
- **Client SDKs** — zero-dependency libraries for Go, Python, Java, and Node.js

## Performance

```
FastKV vs Redis (io_uring, 2 cores)
─────────────────────────────────────────────────────────
  Sequential:   2.2x faster
  Pipeline:     1.6x faster (batch=100)
  Multi-thread: 2.1x faster (8 threads)
  Peak throughput: ~713 000 ops/sec (GET, pipeline batch=500)
─────────────────────────────────────────────────────────
```

## Quick Start

### Build

```bash
cargo build --release
```

### Run Server

```bash
# Default — 0.0.0.0:6379, WAL in ./fastkv_data, fsync everysec
fast_kv server

# Custom host, port, data dir, fsync policy
fast_kv server --host 127.0.0.1 --port 6380 --dir /var/lib/fastkv --fsync always

# With io_uring (Linux only, maximum throughput)
cargo build --release --features io-uring
fast_kv server --mode io_uring

# Via environment variables
FASTKV_HOST=0.0.0.0 FASTKV_PORT=6379 FASTKV_FSYNC=always fast_kv server
```

#### Server Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host <addr>` | `0.0.0.0` | Bind address |
| `--port <port>` | `6379` | Listen port |
| `--dir <path>` | `./fastkv_data` | Data directory for WAL |
| `--fsync <policy>` | `everysec` | WAL fsync: `always`, `everysec`, `never` |
| `--mode <backend>` | `tokio` | Server backend: `tokio` or `io_uring` (Linux only) |

### Connect with redis-cli

```bash
redis-cli -p 6379
```

```
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
127.0.0.1:6379> PING
PONG
```

## Installation

### Server Binaries

Download from [GitHub Releases](https://github.com/JustAMamont/fastkv/releases):

| File | Platform |
|------|----------|
| `fastkv-server-linux-amd64` | Linux x86_64 |
| `fastkv-server-windows-amd64.exe` | Windows x86_64 |
| `fastkv-server-darwin-arm64` | macOS Apple Silicon |
| `fastkv-server-darwin-amd64` | macOS Intel |

```bash
# Linux / macOS
chmod +x fastkv-server-linux-amd64
./fastkv-server-linux-amd64 server --port 6379

# Windows
fastkv-server-windows-amd64.exe server --port 6379
```

### Client Libraries

All clients are **libraries** that you import into your own project — they connect to a running FastKV server over TCP.

| Language | Zero Dependencies | Pipeline | Async | Install |
|----------|:-:|:-:|:-:|--------|
| **Python** | stdlib only | Yes | asyncio | `pip install fastkv-{version}-py3-none-any.whl` |
| **Node.js** | stdlib only | Yes | native | `npm install fastkv-client-{version}.tgz` |
| **Java** | JDK 8+ only | Yes | CompletableFuture | add `fastkv-client-java-{version}.jar` to classpath |
| **Go** | stdlib only | Yes | — | download & vendor `fastkv-client-go-v{version}.tar.gz` |
| **Rust** | tokio only | Yes | native | download & add `fastkv-client-rust-v{version}.tar.gz` |

See [`clients/README.md`](clients/README.md) for full API reference and usage examples.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FASTKV_HOST` | `0.0.0.0` | Bind address (overridden by `--host`) |
| `FASTKV_PORT` | `6379` | Listen port (overridden by `--port`) |
| `FASTKV_DIR` | `./fastkv_data` | Directory for WAL file |
| `FASTKV_FSYNC` | `everysec` | WAL fsync policy: `always`, `everysec`, or `never` |

## Supported Commands

### Core

| Command | Description |
|---------|-------------|
| `SET key value [NX\|XX] [EX seconds\|PX ms]` | Set key-value with optional conditions and TTL |
| `GET key` | Get value by key |
| `DEL key [key ...]` | Delete one or more keys |
| `EXISTS key [key ...]` | Count existing keys |
| `PING` | Health check |
| `ECHO msg` | Echo message |
| `DBSIZE` | Number of keys |
| `INFO` | Server info |
| `COMMAND` | Command discovery |
| `QUIT` | Close connection |

### String Operations

| Command | Description |
|---------|-------------|
| `INCR` / `DECR` | Increment / decrement by 1 |
| `INCRBY n` / `DECRBY n` | Increment / decrement by n |
| `APPEND key value` | Append to string |
| `STRLEN key` | String length |
| `GETRANGE key start end` | Substring by byte range |
| `SETRANGE key offset value` | Overwrite at offset |
| `MSET k1 v1 k2 v2 ...` | Set multiple keys |
| `MGET k1 k2 ...` | Get multiple keys |

### Expiration

| Command | Description |
|---------|-------------|
| `EXPIRE key seconds` | Set TTL in seconds |
| `TTL key` | Remaining TTL in seconds |
| `PTTL key` | Remaining TTL in milliseconds |
| `PERSIST key` | Remove TTL |

### Hash

| Command | Description |
|---------|-------------|
| `HSET key field value` | Set hash field |
| `HGET key field` | Get hash field |
| `HDEL key field [field ...]` | Delete hash fields |
| `HGETALL key` | Get all fields and values |
| `HEXISTS key field` | Check field exists |
| `HLEN key` | Number of fields |
| `HKEYS key` | All field names |
| `HVALS key` | All field values |
| `HMGET key field [field ...]` | Get multiple fields |
| `HMSET key f1 v1 f2 v2 ...` | Set multiple fields |

### List

| Command | Description |
|---------|-------------|
| `LPUSH key elem [elem ...]` | Push to head |
| `RPUSH key elem [elem ...]` | Push to tail |
| `LPOP key` / `RPOP key` | Pop from head / tail |
| `LRANGE key start stop` | Get range |
| `LLEN key` | List length |
| `LINDEX key index` | Element by index |
| `LREM key count elem` | Remove elements by value |
| `LTRIM key start stop` | Trim list to range |
| `LSET key index elem` | Set element by index |

> Lists are in-memory only (not persisted to WAL). Suitable for ephemeral data such as queues and buffers.

## Testing

### Server Unit Tests (122 tests)

```bash
cargo test
```

| Module | Tests | Coverage |
|--------|------:|----------|
| `kv.rs` | 29 | GET/SET/DEL, INCR/DECR, MGET/MSET, APPEND, STRLEN, GETRANGE, SETRANGE, lock-free ops, concurrent |
| `wal.rs` | 16 | Create/recover, CRC-32C, alignment, binary keys, large values, EXPIRE entry |
| `expiration.rs` | 14 | EXPIRE/TTL/PERSIST, lazy/active purging, concurrent, DEL cascade |
| `hash.rs` | 20 | HSET/HGET/HDEL, HGETALL, HEXISTS, HLEN, HKEYS/HVALS, HMGET/HMSET, edge cases |
| `list.rs` | 17 | LPUSH/RPUSH, LPOP/RPOP, LRANGE, LLEN, LINDEX, LREM, LTRIM, LSET, WRONGTYPE |
| `resp.rs` | 18 | RESP array/inline parse, encode, roundtrip, binary data, error types |
| `tcp.rs` | 8 | Dispatch handlers, DEL list cleanup, WRONGTYPE, PING, inline parsing |

### Integration Tests (all clients)

```bash
# Install all toolchains (Rust, Go, Python, Java, Node.js)
./scripts/install_deps.sh

# Run everything: server unit tests + all client integration tests
./scripts/setup_and_test.sh
```

| Suite | Tests |
|-------|------:|
| Rust (server) | 122 |
| Rust (client) | 23 |
| Python sync | 27 |
| Python async | 28 |
| Go | 42 |
| Java sync | 52 |
| Java reactive | 23 |
| Node.js | 51 |

## Benchmarks

```bash
# In-memory (Criterion)
cargo bench --bench kv_benchmark

# Network (vs Redis)
cargo run --release --example netbench -- all
```

## Project Structure

```
fastkv/
├── Cargo.toml
├── README.md
├── src/
│   ├── lib.rs
│   ├── main.rs
│   └── core/
│       ├── mod.rs
│       ├── kv.rs               # Lock-free hash table + string operations
│       ├── resp.rs             # RESP protocol parser/encoder
│       ├── wal.rs              # Write-ahead log (SET/DEL/EXPIRE)
│       ├── expiration.rs       # TTL / key expiration
│       ├── hash.rs             # Hash data type
│       ├── list.rs             # List data type
│       └── server/
│           ├── mod.rs
│           ├── tcp.rs          # Tokio TCP server (cross-platform)
│           └── io_uring.rs     # io_uring server (Linux only)
├── clients/
│   ├── README.md                     # Client SDK overview
│   ├── rust/
│   │   ├── src/                      # lib.rs, resp.rs, pipeline.rs
│   │   ├── tests/integration_test.rs  # Integration tests
│   │   └── examples/example.rs       # Usage example
│   ├── go/fastkv/
│   │   ├── fastkv.go                 # Client implementation
│   │   ├── integration_test.go       # Integration tests
│   │   └── example_test.go           # Usage examples (go test)
│   ├── python/
│   │   ├── fastkv/                   # Package (client.py, async_client.py, ...)
│   │   └── tests/
│   │       ├── test_integration.py       # Sync integration tests
│   │       ├── test_async_integration.py # Async integration tests
│   │       └── example.py                 # Usage example
│   ├── java/
│   │   └── src/*.java                 # Sync + reactive clients, pipelines, tests, examples
│   └── node/fastkv/
│       ├── client.js, resp.js, ...   # Client implementation
│       └── tests/
│           ├── test_integration.js   # Integration tests
│           └── example.js            # Usage example
├── scripts/
│   ├── install_deps.sh               # Install all toolchains
│   └── setup_and_test.sh             # Run all tests
├── tests/
│   └── test_fastkv.py                # Server-level tests (uses redis-py)
├── benches/
│   ├── kv_benchmark.rs
│   └── network_benchmark.rs
└── examples/
    └── netbench.rs                   # Network benchmark (cargo run --example)
```

## Architecture

### Lock-free Hash Table

```
LockFreeEntry (64-byte cache-line aligned)
┌─────────────────────────────────────────────┐
│  hash: AtomicU64        — 0 = empty         │
│  key_len: AtomicU32                         │
│  value_len: AtomicU32                       │
│  version: AtomicU64     — optimistic reads  │
│  data: [AtomicU8; 128]  — inline key+value  │
└─────────────────────────────────────────────┘

Operations:
  SET  — CAS (Compare-And-Swap) for lock-free insertion
  GET  — Optimistic read with version check
  DEL  — Atomic hash reset
  INCR — CAS loop on version for atomic read-modify-write
```

### WAL (Write-Ahead Log)

```
┌─────────┬─────────┬─────────┬────────┬────┬──────┬────┬───────┐
│ magic   │ version │ crc32c  │ op     │ kl │ key  │ vl │ value │
│ 4 bytes │ 2 bytes │ 4 bytes │ 1 byte │ 2B │ kl B │ 2B │ vl B  │
└─────────┴─────────┴─────────┴────────┴────┴──────┴────┴───────┘

Fsync policies:  always (safest) | everysec (balanced) | never (fastest)

Recovery:
  Phase 1 — replay SET and DEL entries into KV store
  Phase 2 — replay EXPIRE entries into expiration manager
  Corrupted trailing entries are silently discarded
```

### Expiration

Two strategies work together:

1. **Lazy** — check TTL on every `GET` / `EXISTS` access
2. **Active** — background thread purges expired keys every 100 ms

EXPIRE entries are written to WAL and restored on recovery after keys are loaded.

## Roadmap

- [x] Phase 1 — Core: lock-free hash table, RESP protocol, TCP server (Tokio + io_uring), pipeline
- [x] Phase 2 — Persistence: WAL with CRC-32C, alignment, fsync policies, recovery
- [x] Phase 3 — String ops: INCR/DECR, APPEND/STRLEN, GETRANGE/SETRANGE, MGET/MSET
- [x] Phase 4 — Expiration: EXPIRE/TTL/PTTL/PERSIST, lazy + active purging, WAL persistence
- [x] Phase 5 — Hash: HSET/HGET/HDEL/HGETALL/HEXISTS/HLEN/HKEYS/HVALS/HMGET/HMSET
- [x] Phase 6 — List: LPUSH/RPUSH, LPOP/RPOP, LRANGE/LLEN/LINDEX, LREM/LTRIM/LSET, WRONGTYPE
- [x] Phase 7 — Client SDKs: Go, Python (sync + async), Java (sync + reactive), Node.js, Rust (zero-dependency, pipeline support)
- [ ] Phase 8 — Set: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, SINTER
- [ ] Phase 9 — Sorted Set: ZADD, ZREM, ZRANGE, ZSCORE, ZRANK, ZCARD
- [ ] Phase 10 — Advanced: Pub/Sub, Transactions (MULTI/EXEC), Lua scripting
- [ ] Phase 11 — Cluster: hash-slot sharding, node discovery, failover

## Requirements

- Rust 1.85+ (edition 2024)
- Linux (for io_uring), macOS, or Windows

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Connection refused | `ss -tlnp \| grep 6379` — check if server is running |
| Low performance | Use `cargo build --release`; check CPU governor; enable io_uring on Linux |
| io_uring build error | Requires Linux kernel 5.1+: `uname -r` |
| Stale WAL data | Delete WAL to start fresh: `rm ./fastkv_data/fastkv.wal` |

## License

MIT
