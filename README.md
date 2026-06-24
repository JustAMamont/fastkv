# FastKV

> ⚠️ **Pre-Alpha — NOT production-ready.**
> FastKV is an experimental project under active development. It has known
> correctness bugs (race conditions in `DEL`, O(N²) `FLUSHALL` that blocks the
> event loop, no TLS, no replication/failover, no cluster mode, a 64 KB
> key/value length limit, and more). Do **not** use it for any data you care
> about. The wire protocol (`RESP`), command set, on-disk WAL format, and CLI
> flags may still change in backward-incompatible ways between releases.
> Production deployment is blocked until the P0 issues from the roadmap are
> resolved.

High-performance, Redis-compatible key-value store written in Rust with lock-free data structures. Ships with first-party client SDKs for Rust and Python; for any other language, use any existing Redis client (FastKV speaks standard RESP).

## Project Status

| Aspect | State |
|--------|-------|
| Overall | **Pre-alpha** — experimental, APIs and on-disk formats not stable |
| Single-node core (GET/SET/DEL/INCR/...) | Works for basic workloads, but contains known race-condition and blocking bugs |
| Persistence (WAL + checkpoint) | Implemented; recovery path has not been hardened against all crash scenarios |
| TTL / expiration | Implemented; uses a coarse 1 s ticker with lazy + active purging |
| Hash / List data types | Implemented; persisted to WAL since v1.2.3 |
| Blob arena (zstd) | Implemented behind `blob-store` feature; recovery edge cases exist |
| Similarity search (SimHash / MinHash / LSH) | Experimental, behind `similarity` feature |
| TLS / encryption in transit | **Not implemented** — plaintext TCP only |
| Authentication | `AUTH` password only; no ACL, no constant-time comparison guarantee |
| Replication / failover | **Not implemented** — single node only |
| Cluster mode / sharding | **Not implemented** |
| Backup / restore tooling | Basic `BGSAVE`/`SAVE` only; no PITR, no snapshot verification |
| Observability | Minimal `INFO` output; no Prometheus, no structured logs, no tracing |
| Test coverage | Smoke tests via `redis-py`; no fuzzing, no Jepsen-style concurrency tests |

**Use it for**: local experimentation, benchmarks, learning how a lock-free KV engine is built.

**Do not use it for**: any production workload, any data you cannot afford to lose, any environment exposed to untrusted clients, any HA/DR scenario.

## Features

- **Lock-free hash table** — thread-safe without mutexes; uses atomic CAS and optimistic version reads
- **Configurable inline size** — `--inline-size 64|128|256|512` per-side storage, zero-overhead monomorphization
- **Blob Arena** — zstd-compressed large-value storage (feature `blob-store`); `BSET`/`BGET`/`BGETRAW`/`BSTATS`
- **Similarity search** — SimHash (64-bit near-duplicate), MinHash (Jaccard estimation), LSH O(1) bucket search (feature `similarity`); `SIMHASH`/`FINDSIM`/`LSHADD`/`LSHREM`
- **Redis-compatible RESP protocol** — works with `redis-cli` and any Redis-compatible tooling
- **Pipeline support** — batch multiple commands in a single round-trip for higher throughput
- **WAL persistence** — crash-consistent write-ahead log with configurable fsync policy and TTL recovery (including BSET/BDel ops)
- **Checkpoint / BGSAVE** — automatic or manual WAL compaction via `BGSAVE`/`SAVE` commands; `--checkpoint-interval` flag
- **TTL / Expiration** — `EXPIRE`, `TTL`, `PTTL`, `PERSIST` with lazy + active key purging, persisted to WAL
- **Hash data type** — `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HMGET`, `HMSET`, `HINCRBY`, `HSETNX`
- **List data type** — `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LREM`, `LTRIM`, `LSET` (all persisted to WAL since v1.2.3)
- **String operations** — `INCR`, `DECR`, `INCRBY`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `MGET`, `MSET`, `EXISTS`, `SETNX`, `PSETEX`, `GETSET`, `GETDEL`
- **Key management** — `TYPE`, `RENAME`, `UNLINK`, `FLUSHALL`, `FLUSHDB`
- **AUTH / Security** — `--requirepass` for password authentication, `--max-connections` for connection limiting
- **Graceful shutdown** — SIGTERM/SIGINT handling with 30s drain timeout and WAL sync
- **io_uring (Linux)** — optional kernel-bypass networking for maximum throughput
- **Non-blocking command dispatch** — heavy writes (WAL `fsync`, BSET compression) run on `spawn_blocking` threads so reads (SCAN, GET) never starve under concurrent write load (v1.2.1+)
- **Client SDKs** — first-party libraries for Rust and Python; any Redis-compatible client works for other languages (FastKV speaks standard RESP)

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
# Standard build (without blob arena)
cargo build --release

# With blob arena (zstd-compressed large-value storage)
cargo build --release --features blob-store

# With similarity search (SimHash/MinHash/LSH)
cargo build --release --features similarity

# All features
cargo build --release --features "blob-store,similarity"
```

### Run Server

```bash
# Default — 0.0.0.0:6379, WAL in ./fastkv_data, fsync everysec, 100K buckets, inline-size 64
fast_kv server

# Custom host, port, data dir, fsync policy
fast_kv server --host 127.0.0.1 --port 6380 --dir /var/lib/fastkv --fsync always

# High-cardinality workload (>50K keys): increase capacity
fast_kv server --capacity 1000000

# Larger values (up to 256 bytes per side): increase inline-size
fast_kv server --inline-size 256 --capacity 500000

# With blob arena (zstd-compressed large values)
cargo build --release --features blob-store
fast_kv server

# With io_uring (Linux only, maximum throughput)
cargo build --release --features io-uring
fast_kv server --mode io_uring

# With both blob arena and io_uring
cargo build --release --features "blob-store,io-uring"
fast_kv server --mode io_uring

# With blob arena + similarity
cargo build --release --features "blob-store,similarity"
fast_kv server

# Via environment variables
FASTKV_HOST=0.0.0.0 FASTKV_PORT=6379 FASTKV_CAPACITY=500000 FASTKV_INLINE_SIZE=256 FASTKV_FSYNC=always fast_kv server
```

#### Server Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host <addr>` | `0.0.0.0` | Bind address |
| `--port <port>` | `6379` | Listen port |
| `--capacity <num>` | `100000` | Hash table buckets (see Memory below) |
| `--inline-size <N>` | `64` | Per-side inline storage: `64`, `128`, `256`, or `512` bytes |
| `--dir <path>` | `./fastkv_data` | Data directory for WAL |
| `--fsync <policy>` | `everysec` | WAL fsync: `always`, `everysec`, `never` |
| `--mode <backend>` | `tokio` | Server backend: `tokio` or `io_uring` (Linux only) |
| `--requirepass <pw>` | (disabled) | Require clients to authenticate with `AUTH` command |
| `--max-connections <N>` | `10000` | Maximum concurrent client connections |
| `--checkpoint-interval <secs>` | (disabled) | Auto-checkpoint interval in seconds |

#### Capacity & Memory

The `--capacity` flag controls how many buckets the lock-free hash table pre-allocates at startup. The table **does not resize** — if it fills up, `SET` returns an error. Choose a capacity at least 2× your expected key count.

The `--inline-size` flag controls the per-side inline storage for keys and values. Larger inline sizes allow bigger values but use more memory. Each size is monomorphized at compile time for zero runtime overhead.

| Capacity | N=64 | N=128 | N=256 | N=512 | Max keys (50% load) |
|----------|------|-------|-------|-------|---------------------|
| 1 000 | 0.2 MB | 0.3 MB | 0.5 MB | 1.0 MB | ~500 |
| 10 000 | 1.8 MB | 2.7 MB | 5.5 MB | 10 MB | ~5 000 |
| 100 000 | 19 MB | 27 MB | 55 MB | 110 MB | ~50 000 |
| 500 000 | 92 MB | 137 MB | 268 MB | 537 MB | ~250 000 |
| 1 000 000 | 183 MB | 275 MB | 550 MB | 1.1 GB | ~500 000 |

> **WAL recovery note**: when recovering from a WAL that was created with a different `--inline-size`, entries that exceed the current inline limit are skipped with a warning.

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

### Blob Arena (large values, requires `--features blob-store`)

```bash
# Build with blob-store feature
cargo build --release --features blob-store
fast_kv server
```

```
127.0.0.1:6379> BSET session:1 "{\"ua\":\"Chrome/120\",\"cookies\":\"...\"}"
OK
127.0.0.1:6379> BGET session:1
"{\"ua\":\"Chrome/120\",\"cookies\":\"...\"}"
127.0.0.1:6379> BGETRAW session:1
"<compressed bytes>"
127.0.0.1:6379> BSTATS
# Blob Arena
total_used:1234
total_compressed:1234
total_original:5678
compression_ratio:0.2172
free_slots:0
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

FastKV speaks the standard RESP wire protocol, so any Redis-compatible client works out of the box. We maintain first-party SDKs for Rust and Python; for other languages we recommend using an existing Redis client (see snippets below).

#### First-party SDKs

| Language | Zero Dependencies | Pipeline | Async | Install |
|----------|:-:|:-:|:-:|--------|
| **Python** | stdlib only | Yes | asyncio | `pip install git+https://github.com/JustAMamont/fastkv.git#subdirectory=clients/python` |
| **Rust** | tokio only | Yes | native | download & add `fastkv-client-rust-v{version}.tar.gz` |

See [`clients/README.md`](clients/README.md) for full API reference and usage examples.

#### Using existing Redis clients (community-supported)

For languages where we don't ship a first-party SDK, use any mature Redis client. Custom FastKV commands (`BSET`/`BGET`/`SIMHASH`/`LSHADD`/...) are reachable through each client's raw-command escape hatch.

```python
# Python — redis-py
import redis
r = redis.Redis(host='127.0.0.1', port=6379)
r.set('k', 'v')
r.execute_command('BSET', 'blob_key', b'\x00\x01\x02')   # custom command
```

```go
// Go — github.com/redis/go-redis/v9
import "github.com/redis/go-redis/v9"
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
rdb.Set(ctx, "k", "v", 0)
rdb.Do(ctx, "BSET", "blob_key", []byte{0,1,2})  // custom command
```

```javascript
// Node.js — ioredis (or node-redis)
const Redis = require('ioredis');
const r = new Redis(6379, '127.0.0.1');
await r.set('k', 'v');
await r.call('BSET', 'blob_key', Buffer.from([0,1,2]));  // custom command
```

```java
// Java — Jedis (or Lettuce)
try (Jedis j = new Jedis("127.0.0.1", 6379)) {
    j.set("k", "v");
    j.sendCommand(new CommandObject<>("BSET"), "blob_key", new byte[]{0,1,2});  // custom command
}
```

> Want to maintain a first-party client for a language we don't cover? Open a community repo, link it in [`clients/README.md`](clients/README.md), and we'll list it.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FASTKV_HOST` | `0.0.0.0` | Bind address (overridden by `--host`) |
| `FASTKV_PORT` | `6379` | Listen port (overridden by `--port`) |
| `FASTKV_CAPACITY` | `100000` | Hash table buckets (overridden by `--capacity`) |
| `FASTKV_INLINE_SIZE` | `64` | Per-side inline size: `64`, `128`, `256`, `512` (overridden by `--inline-size`) |
| `FASTKV_DIR` | `./fastkv_data` | Directory for WAL file |
| `FASTKV_FSYNC` | `everysec` | WAL fsync policy: `always`, `everysec`, or `never` |
| `FASTKV_REQUIREPASS` | (disabled) | Require password authentication |
| `FASTKV_MAX_CONNECTIONS` | `10000` | Maximum concurrent client connections |

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
| `AUTH password` | Authenticate (required when `--requirepass` is set) |
| `TYPE key` | Returns type: `string`, `hash`, `list`, or `none` |
| `RENAME key newkey` | Rename a key (atomic GET+SET+DEL with TTL transfer) |
| `GETSET key value` | Set new value, return old value (atomic) |
| `GETDEL key` | Get value and delete key (atomic) |
| `SETNX key value` | Set only if key does not exist |
| `PSETEX key ms value` | Set with millisecond TTL |
| `UNLINK key [key ...]` | Delete keys (async, same as DEL for compatibility) |
| `FLUSHALL` | Delete all keys and checkpoint WAL |
| `FLUSHDB` | Delete all keys and checkpoint WAL |
| `SAVE` | Synchronous WAL checkpoint |
| `BGSAVE` | Background WAL checkpoint |

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
| `HINCRBY key field delta` | Increment hash field by delta (atomic) |
| `HSETNX key field value` | Set hash field only if field does not exist |

### Blob Store (feature: `blob-store`)

| Command | Description |
|---------|-------------|
| `BSET key value` | Set with auto-compression: if value > blob threshold, compress with zstd and store in blob arena |
| `BGET key` | Get with auto-decompression: transparently decompresses blob refs |
| `BGETRAW key` | Get raw compressed bytes (skip decompression, useful for transfer) |
| `BSTATS` | Blob arena statistics: total_used, total_compressed, total_original, compression_ratio, free_slots |

> `GET` transparently decompresses blob refs — use `BGET` for clarity, but regular `GET` works too.
> `DEL` automatically frees blob arena slots.

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

> List operations are persisted to WAL (op 0x06 = LIST_OP with sub-ops LPUSH/RPUSH/LPOP/RPOP/LTRIM). Crash recovery replays list operations from WAL.

### Scan / Stats

| Command | Description |
|---------|-------------|
| `SCAN cursor [COUNT n] [MATCH pattern]` | Cursor-based key iteration; returns `[next_cursor, [key1, key2, ...]]` |
| `DBSTATS` | Aggregate store statistics: `total_keys`, `total_buckets`, `load_factor`, `entry_size`, `total_memory`, `blob_count`, `inline_size` |

> `SCAN` uses optimistic version-check reads (same protocol as `GET`) for lock-free iteration.
> `MATCH` supports glob patterns: `*` (any sequence), `?` (single char), `[abc]` (char class).
> When `next_cursor` is `0`, iteration is complete.
>
> **Python client**: `scan(cursor=0, count=10, match=None)` returns `(next_cursor, keys)`;
> `dbstats()` returns a dict with all statistics fields.

### Similarity (feature: `similarity`)

| Command | Description |
|---------|-------------|
| `SIMHASH key` | Compute 64-bit SimHash for stored value; returns hex string |
| `FINDSIM key [threshold]` | Find similar keys via LSH buckets; default Hamming threshold = 3 |
| `LSHADD key [simhash_hex]` | Index key in LSH buckets; auto-computes SimHash if not provided |
| `LSHREM key [simhash_hex]` | Remove key from LSH buckets; looks up stored SimHash if not provided |

> SimHash splits a 64-bit hash into 4 bands × 16 bits for O(1) approximate nearest-neighbor lookup.
> LSH bucket keys are regular KV entries (`lsh:sim:{band}:{value}`), so they benefit from WAL persistence and TTL.
> `SIMHASH` transparently decompresses blob refs when computing hashes for `BSET`-stored values.
>
> **Python client**: `simhash(key)`, `find_similar(key, threshold=3)`, `lsh_add(key, simhash_hex=None)`, `lsh_rem(key, simhash_hex=None)` (sync + async).

## Testing

### Server Unit Tests

```bash
# Standard tests (124 tests)
cargo test

# With blob-store feature
cargo test --features blob-store

# With blob-store + similarity features
cargo test --features "blob-store,similarity"
```

| Module | Tests | Coverage |
|--------|------:|----------|
| `kv.rs` | 39 | GET/SET/DEL, INCR/DECR, MGET/MSET, APPEND, STRLEN, GETRANGE, SETRANGE, SCAN, glob MATCH, DBSTATS, lock-free ops, concurrent |
| `wal.rs` | 18 | Create/recover, CRC-32C, alignment, binary keys, large values, EXPIRE entry, BSET/BDel entries |
| `expiration.rs` | 14 | EXPIRE/TTL/PERSIST, lazy/active purging, concurrent, DEL cascade |
| `hash.rs` | 20 | HSET/HGET/HDEL, HGETALL, HEXISTS, HLEN, HKEYS/HVALS, HMGET/HMSET, HINCRBY, HSETNX, edge cases |
| `list.rs` | 17 | LPUSH/RPUSH, LPOP/RPOP, LRANGE, LLEN, LINDEX, LREM, LTRIM, LSET, WRONGTYPE |
| `blob.rs` | 14 | BlobRef encode/decode, store/retrieve, free/reuse, hash integrity, stats, BGETRAW, edge cases |
| `simhash.rs` | 10 | SimHash 64-bit, weighted features, hamming distance, similarity threshold, KV integration |
| `minhash.rs` | 10 | MinHash 128-hash signature, Jaccard estimation, serialization, band value extraction |
| `lsh.rs` | 9 | LSH band extraction, add/remove/find, SimHash + MinHash bucket indexing, ID list codec |
| `resp.rs` | 18 | RESP array/inline parse, encode, roundtrip, binary data, error types |
| `tcp.rs` | 8 | Dispatch handlers, DEL list cleanup, WRONGTYPE, PING, inline parsing |
| `checkpoint.rs` | 3 | BGSAVE, SAVE, checkpoint on empty store, no-WAL edge case |

### Integration Tests

```bash
# Install toolchains (Rust, Python, Node.js — Node is used as a portable port-wait helper)
./scripts/install_deps.sh

# Run everything: server unit tests + Python + Rust client integration tests
./scripts/setup_and_test.sh
```

| Suite | Tests |
|-------|------:|
| Rust (server) | 164 (with blob-store + similarity) / 121 (without) |
| Rust (client) | 23 |
| Python sync | 27 |
| Python async | 28 |

## Benchmarks

```bash
# Core benchmarks (always available)
cargo bench --bench kv_benchmark

# Core + Blob Arena benchmarks
cargo bench --bench kv_benchmark --features blob-store

# Core + Similarity benchmarks
cargo bench --bench kv_benchmark --features similarity

# All benchmarks (core + blob arena + similarity)
cargo bench --bench kv_benchmark --features "blob-store,similarity"

# Filter by name
cargo bench -- blob          # blob arena benchmarks only
cargo bench -- simhash       # simhash benchmarks only
cargo bench -- scan          # scan benchmarks only

# Network (vs Redis)
cargo run --release --example netbench -- all
```

### Benchmark Groups

| Group | Feature Flag | What It Measures |
|-------|-------------|------------------|
| `set_operations` | — | SET at 100 / 1K / 10K keys |
| `get_operations` | — | GET at 100 / 1K / 10K keys |
| `incr_operations` | — | INCR on 10K keys |
| `exists_operations` | — | EXISTS on 10K keys |
| `mget_operations` | — | MGET on 100 keys |
| `threaded_set` | — | Multi-threaded SET (1/2/4/8 threads) |
| `threaded_get` | — | Multi-threaded GET (1/2/4/8 threads) |
| `threaded_incr` | — | Multi-threaded INCR (8 threads, 1K keys) |
| `wal_write` | — | WAL write 10K entries (fsync=never) |
| `string_operations` | — | APPEND/STRLEN/GETRANGE/SETRANGE on 10K keys |
| `wal_recovery` | — | WAL recovery 10K entries |
| `expiration` | — | EXPIRE/TTL on 10K keys |
| `scan` | — | SCAN at 1K/10K/50K keys, with/without MATCH |
| `dbstats` | — | DBSTATS at 1K/10K/50K keys |
| `blob_arena` | `blob-store` | BSET store/retrieve at 256B/1KB/4KB/10KB, BGETRAW, BlobRef encode/decode, stats, compression ratio |
| `blob_vs_inline` | `blob-store` | SET (inline) vs BSET (blob) for 10K ops, small vs 4KB payloads |
| `wal_segment` | `blob-store` | Compressed WAL: SET/BSET write, recovery, raw vs compressed |
| `simhash` | `similarity` | hash64, simhash (weighted/uniform), hamming_distance, is_similar |
| `minhash` | `similarity` | MinHash 128/256 hashes, Jaccard similarity, serialization |
| `lsh` | `similarity` | LSH add/find/remove for 1K profiles |

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
│       ├── wal.rs              # Write-ahead log (SET/DEL/EXPIRE/BSET/BDel)
│       ├── blob.rs             # Blob Arena — zstd-compressed large-value storage
│       ├── simhash.rs          # SimHash 64-bit locality-sensitive hashing
│       ├── minhash.rs          # MinHash signature for Jaccard similarity
│       ├── lsh.rs              # LSH bucket indexing + O(1) similarity search
│       ├── expiration.rs       # TTL / key expiration
│       ├── hash.rs             # Hash data type
│       ├── list.rs             # List data type
│       └── server/
│           ├── mod.rs
│           ├── tcp.rs          # Tokio TCP server (cross-platform)
│           └── io_uring.rs     # io_uring server (Linux only)
├── clients/
│   ├── README.md                     # Client SDK overview + community-client policy
│   ├── rust/
│   │   ├── src/                      # lib.rs, resp.rs, pipeline.rs
│   │   ├── tests/integration_test.rs  # Integration tests
│   │   └── examples/example.rs       # Usage example
│   └── python/
│       ├── fastkv/                   # Package (client.py, async_client.py, ...)
│       └── tests/
│           ├── test_integration.py       # Sync integration tests
│           ├── test_async_integration.py # Async integration tests
│           └── example.py                 # Usage example
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

### Concurrency Model (v1.2.1+)

FastKV separates **socket I/O** (async, on tokio worker threads) from **command processing** (sync, on `spawn_blocking` threads). This division is critical for read/write isolation under heavy concurrent load.

```
                   ┌──────────────────────────────────┐
   Client A ─────► │ tokio worker thread              │
   (writes)        │  ├─ socket.read()                │
                   │  └─ spawn_blocking(process_buf) ─┼──► blocking pool thread
                   │                                  │      ├─ WAL Mutex + write_all
                   │                                  │      └─ optional fsync
                   └──────────────────────────────────┘
                                                              ▲ does NOT block worker
   Client B ─────► ┌──────────────────────────────────┐       │   threads
   (reads,         │ tokio worker thread              │       │
    SCAN)          │  ├─ socket.read()                │       │
                   │  └─ spawn_blocking(process_buf) ─┼───────┘
                   │      ├─ lock-free hash scan      │
                   │      └─ return keys              │
                   └──────────────────────────────────┘
```

**Before v1.2.1**: command processing ran inline on tokio worker threads. A heavy write load (e.g. thousands of `BSET` calls with `fsync=always`) would saturate all worker threads on `std::sync::Mutex<File>` contention, causing `SCAN` / `GET` from other clients to hang for seconds.

**After v1.2.1**: every command batch is dispatched to `tokio::task::spawn_blocking`. The tokio runtime grows its blocking thread pool on demand (up to 512 threads by default), so:

* Heavy writes block only blocking-pool threads — async worker threads stay free to accept new connections and read more socket data.
* `SCAN` commands get their own blocking thread immediately and return results even while other connections are doing heavy WAL writes.
* The `authenticated` flag was changed from `Cell<bool>` (thread-local) to `Arc<AtomicBool>` (shared across the blocking thread boundary).

The trade-off is a small per-read overhead (one channel send + thread wake-up), which is dwarfed by the WAL I/O cost it isolates. For pure in-memory workloads (no WAL, no blob arena), the inline path is still faster — but correctness under concurrent writes wins.

### Lock-free Hash Table

```
LockFreeEntry<N> (cache-line aligned, N = --inline-size)
┌──────────────────────────────────────────────────────────┐
│  hash: AtomicU64            — 0 = empty                  │
│  key_len: AtomicU32                                      │
│  value_len: AtomicU32                                    │
│  version: AtomicU64         — optimistic reads           │
│  data: [[AtomicU8; N]; 2]   — bank[0]=key, bank[1]=value │
└──────────────────────────────────────────────────────────┘

Default N=64: each key and value up to 64 bytes stored inline (no heap).
Supported N: 64 (default), 128, 256, 512 — monomorphized for zero overhead.

Operations:
  SET  — CAS (Compare-And-Swap) for lock-free insertion
  GET  — Optimistic read with version check (auto-decompresses blob refs)
  DEL  — Atomic hash reset (auto-frees blob arena slots)
  INCR — CAS loop on version for atomic read-modify-write
```

### Blob Arena (feature: `blob-store`)

```
Blob Ref (inline value with flag 0xFD):
┌──────────┬──────────┬───────────┬───────────┬──────────────┐
│ flag: 0xFD│ offset:  │ comp_len: │ orig_len: │ data_hash:  │
│ 1 byte    │ u64 8B   │ u32 4B    │ u32 4B    │ dual-crc32c │
│           │          │           │           │ 16B         │
└──────────┴──────────┴───────────┴───────────┴──────────────┘
Total: 33 bytes — fits even in N=64 inline size

Architecture:
  - Chunked allocation: 64 MB chunks, new chunks on demand
  - Lock-free write: CAS on write_offset to atomically claim space
  - Lock-free read: data is immutable after write
  - Free list: sorted best-fit with binary search O(log n) reuse
  - Compression: zstd level 3
  - Integrity: dual crc32c (two seeds) for 16-byte hash
  - WAL: BSET (op 0x04) and BDel (op 0x05) for crash recovery

Expected characteristics:
  - Session compression: 4 KB -> ~600 B (6-7x)
  - Write throughput: >500K ops/sec (pipeline)
  - Read throughput: >700K ops/sec (pipeline)
  - Memory for 1M sessions: ~600 MB arena + ~500 MB KV = ~1.1 GB
```

### WAL (Write-Ahead Log)

```
┌─────────┬─────────┬─────────┬────────┬────┬──────┬────┬───────┐
│ magic   │ version │ crc32c  │ op     │ kl │ key  │ vl │ value │
│ 4 bytes │ 2 bytes │ 4 bytes │ 1 byte │ 2B │ kl B │ 2B │ vl B  │
└─────────┴─────────┴─────────┴────────┴────┴──────┴────┴───────┘

Fsync policies:  always (safest) | everysec (balanced) | never (fastest)

WAL Operations:
  0x00 = SET     0x01 = DEL     0x02 = EXPIRE
  0x04 = BSET    0x05 = BDel   (blob-store feature only)
  0x06 = LIST_OP              (sub-ops: LPUSH=0x01, RPUSH=0x02, LPOP=0x03, RPOP=0x04, LTRIM=0x05)

Recovery:
  Phase 1 — replay SET, DEL, BSET, BDel, and LIST_OP entries into KV store
  Phase 2 — replay EXPIRE entries into expiration manager
  Corrupted trailing entries are silently discarded
```

#### Checkpoint / BGSAVE and blob keys (v1.2.2+)

The WAL is append-only. Periodically (every `--checkpoint-interval` seconds),
on `BGSAVE`/`SAVE`, and on `FLUSHALL`, FastKV writes a fresh compact WAL
containing only the current live state and atomically swaps it in for the
old WAL.

For blob keys the compact WAL must contain a `BSET` entry with the
**original uncompressed** payload, so that on recovery the existing
BSET-replay path (`WalOp::BSet` in `main.rs`) can rebuild the blob arena
(compress → arena.store → BlobRef in hash table).

Versions **prior to v1.2.2 had a bug** where checkpoint wrote blob keys
as plain `SET` entries carrying the 33-byte `BlobRef` as the value. After
recovery the hash table held a `BlobRef` pointing into a freshly
initialized (empty) blob arena, so `BGET` returned `nil` and raw
`redis-cli GET` reported `blob decompression failed`. v1.2.2 threads
`Option<&BlobArena>` through `checkpoint()` and `spawn_checkpoint_thread()`
so each blob key is correctly decoded, decompressed from the arena, and
written as a proper `BSET` entry to the compact WAL.

> **Upgrading from v1.2.1 or earlier**: existing WAL files are still
> readable, but blob keys whose data was already clobbered by a v1.2.1
> checkpoint cannot be recovered (the original payload was never
> persisted). After upgrading, re-issue `BSET` for any blob keys whose
> data must survive future restarts, then call `BGSAVE` to capture a
> correct compact WAL.

### Expiration

Two strategies work together:

1. **Lazy** — check TTL on every `GET` / `EXISTS` access
2. **Active** — background thread purges expired keys every 100 ms

EXPIRE entries are written to WAL and restored on recovery after keys are loaded.

### Similarity (feature: `similarity`)

```
SimHash (64-bit near-duplicate detection)
┌───────────────────────────────────────────────────────────┐
│  1. Hash each feature with 64-bit wyhash                  │
│  2. Weighted vote vector: +weight for 1-bits, -weight     │
│     for 0-bits                                            │
│  3. Final hash: bit = 1 if vote > 0 else 0                │
│  4. Hamming distance: popcnt(a XOR b) — single x86 POPCNT │
│  5. Configurable per-field weights                        │
│  6. Default threshold: Hamming distance ≤ 3 = similar     │
└───────────────────────────────────────────────────────────┘

MinHash (Jaccard similarity for set-type data)
┌───────────────────────────────────────────────────────────┐
│  128 hash functions via LCG permutation coefficients      │
│  h_i(x) = (a[i] * hash(x) + b[i]) mod 2^64                │
│  Signature: Vec<u32> of 128 minimums (512 bytes)          │
│  Jaccard estimate = fraction of matching components       │
└───────────────────────────────────────────────────────────┘

LSH (O(1) approximate nearest-neighbor search)
┌───────────────────────────────────────────────────────────┐
│  SimHash: 64 bits → 4 bands × 16 bits                     │
│  Bucket keys: lsh:sim:{band}:{value} → list of IDs        │
│  MinHash:  128 values → 4 bands × 32 rows                 │
│  Bucket keys: lsh:min:{band}:{value} → list of IDs        │
│                                                           │
│  Search: look up all 4 bands, deduplicate candidates,     │
│  optionally filter by Hamming distance threshold (≤ 3)    │
│  Metadata: lsh:simhash:{id} → stored 64-bit SimHash       │
└───────────────────────────────────────────────────────────┘
```

## Roadmap

- [x] Phase 1 — Core: lock-free hash table, RESP protocol, TCP server (Tokio + io_uring), pipeline
- [x] Phase 2 — Persistence: WAL with CRC-32C, alignment, fsync policies, recovery
- [x] Phase 3 — String ops: INCR/DECR, APPEND/STRLEN, GETRANGE/SETRANGE, MGET/MSET
- [x] Phase 4 — Expiration: EXPIRE/TTL/PTTL/PERSIST, lazy + active purging, WAL persistence
- [x] Phase 5 — Hash: HSET/HGET/HDEL/HGETALL/HEXISTS/HLEN/HKEYS/HVALS/HMGET/HMSET/HINCRBY/HSETNX
- [x] Phase 6 — List: LPUSH/RPUSH, LPOP/RPOP, LRANGE/LLEN/LINDEX, LREM/LTRIM/LSET, WRONGTYPE
- [x] Phase 7 — Client SDKs: Python (sync + async) + Rust (zero-dependency, pipeline support). Java/Node.js/Go clients were moved to community repos — FastKV speaks standard RESP, so any Redis client works
- [x] Phase 8 — Blob Arena: BSET/BGET/BGETRAW/BSTATS, zstd compression, lock-free arena, WAL persistence, Python client
- [x] Phase 9 — SimHash/MinHash/LSH: similarity search for near-duplicate detection (feature: `similarity`)
- [x] Phase 10 — SCAN/KEYS: cursor-based key iteration, glob MATCH, DBSTATS
- [ ] Phase 11 — Set: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, SINTER
- [x] Phase 12 — List WAL: persistent list operations (LPUSH/RPUSH/LPOP/RPOP/LTRIM)
- [x] Phase 13 — Compressed WAL Segments: segment-based WAL with zstd batch compression
- [x] Phase 14 — Production readiness: AUTH, connection limits, graceful shutdown, checkpoint/BGSAVE, TYPE, RENAME, GETSET, GETDEL, SETNX, PSETEX, UNLINK, FLUSHALL/FLUSHDB, HINCRBY, HSETNX, critical bug fixes
- [x] Phase 14.1 — v1.2.2 hotfix: WAL checkpoint now correctly persists blob keys as `BSET` entries (was: `SET` with bare `BlobRef`, causing `BGET` to return `nil` after restart)
- [x] Phase 14.2 — v1.2.3 hotfix: LIST operations (LPUSH/RPUSH/LPOP/RPOP/LREM/LTRIM/LSET) now persist to WAL (were in-memory only, lost on restart); checkpoint streaming rewrite (v1.2.2) avoids OOM under high blob-key count; Python client gains `get_str`/`set_str` helpers
- [ ] Phase 15 — Advanced: Pub/Sub, Transactions (MULTI/EXEC), BLPOP/BRPOP, Lua scripting
- [ ] Phase 16 — Cluster: hash-slot sharding, node discovery, failover
- [ ] Phase 17 — TLS: optional `--tls-cert` / `--tls-key` via tokio-rustls
- [ ] Phase 18 — CI/CD, Docker, monitoring /metrics endpoint

## Requirements

- Rust 1.85+ (edition 2024)
- Linux (for io_uring), macOS, or Windows

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Connection refused | `ss -tlnp \| grep 6379` — check if server is running |
| SET returns error | Table is full — increase `--capacity`; or value too large — increase `--inline-size` |
| Low performance | Use `cargo build --release`; check CPU governor; enable io_uring on Linux |
| io_uring build error | Requires Linux kernel 5.1+: `uname -r` |
| Stale WAL data | Delete WAL to start fresh: `rm ./fastkv_data/fastkv.wal` |
| WAL recovery skips entries | Entry exceeds current `--inline-size` — use the same inline-size as when WAL was created |
| SCAN / GET hangs under heavy writes | Upgrade to v1.2.1+ — command dispatch now uses `spawn_blocking` so async worker threads are never blocked on WAL I/O. If you still see hangs, lower `--fsync` to `everysec` (default) or `never` |
| `connection reset by peer` under load | Client socket timeout too low for WAL-fsync latency. Increase client timeout to 30s+, or upgrade server to v1.2.1+ where reads are not blocked by writes |

## License

MIT
