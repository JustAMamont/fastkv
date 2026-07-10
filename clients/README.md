# FastKV Client SDKs

First-party client libraries for FastKV — a Redis-compatible lock-free key-value store written in Rust.

FastKV speaks the standard RESP wire protocol, so any Redis-compatible client works out of the box. We maintain first-party SDKs for **Rust** and **Python**; for other languages, use an existing Redis client (see [`README.md`](../README.md#using-existing-redis-clients-community-supported) for snippets).

If you maintain a community FastKV client in another language, open a PR to list it in the table below.

---

## Supported Languages

| Language | Dependencies | Pipeline | Async | Install |
|----------|:------------:|:--------:|:-----:|---------|
| **Python** | stdlib only | Yes | asyncio | `pip install git+https://github.com/JustAMamont/fastkv.git#subdirectory=clients/python` |
| **Rust** | tokio only | Yes | native (tokio) | extract `fastkv-client-rust-v{version}.tar.gz` |
| _other_ | — | — | — | use any Redis-compatible client (`redis-py`, `go-redis`, `ioredis`, `jedis`, ...) |

The first-party clients are **libraries** — you import them into your project, they connect to a running FastKV server over TCP. They implement the RESP protocol from scratch and have **no Redis SDK dependency**.

---

## Python

### Installation

```bash
# From GitHub
pip install git+https://github.com/JustAMamont/fastkv.git#subdirectory=clients/python

# Or from source
cd clients/python
pip install .
```

### Sync Client

```python
from fastkv import FastKVClient

# Connect to FastKV server
with FastKVClient("localhost", 6379) as c:
    # Basic operations
    c.set("user:1", "Alice")
    print(c.get("user:1"))          # b"Alice"
    print(c.dbsize())               # 1

    # TTL
    c.set("session:abc", "data", ex=3600)
    print(c.ttl("session:abc"))     # ~3600

    # Hash
    c.hset("user:1", "name", "Alice")
    c.hset("user:1", "age", "30")
    print(c.hgetall("user:1"))      # {b"name": b"Alice", b"age": b"30"}

    # List
    c.lpush("queue:tasks", "task1", "task2", "task3")
    print(c.lrange("queue:tasks", 0, -1))  # [b"task3", b"task2", b"task1"]

    # Blob Arena (large-value storage, zstd-compressed)
    c.bset("doc:1", b"x" * 5000)    # ~5 KB blob
    print(c.bget("doc:1")[:10])     # b"xxxxxxxxxx"

    # Similarity search (SimHash + LSH)
    c.lsh_add("doc:1")
    c.lsh_add("doc:2")
    print(c.find_similar("doc:1"))  # [b"doc:2"]

    # Pipeline (batch commands in one round-trip)
    with c.pipeline() as pipe:
        pipe.set("k1", "v1")
        pipe.set("k2", "v2")
        pipe.get("k1")
        results = pipe.execute()     # [True, True, b"v1"]

# Custom host, port, timeout
c = FastKVClient("10.0.0.1", 6380, socket_timeout=10)
```

### Async Client (FastAPI, aiohttp, etc.)

The async client class is **`FastKVAsyncClient`** (declared in `fastkv/async_client.py` and re-exported from `fastkv/__init__.py`).

```python
import asyncio
from fastkv import FastKVAsyncClient

async def main():
    async with FastKVAsyncClient("localhost", 6379) as c:
        await c.set("key", "value")
        result = await c.get("key")
        print(result)                # b"value"

        # Async pipeline
        async with c.pipeline() as pipe:
            pipe.set("a", "1")
            pipe.set("b", "2")
            pipe.incr("a")
            results = await pipe.execute()  # [True, True, 2]

asyncio.run(main())
```

### Use in Django / Flask

```python
# settings.py or config
from fastkv import FastKVClient

# Single shared connection (FastKVClient is synchronized)
kv = FastKVClient("localhost", 6379)

# In your views
def get_user(request, user_id):
    data = kv.get(f"user:{user_id}")
    if data is None:
        raise Http404
    return JsonResponse({"data": data.decode()})
```

---

## Rust

### Installation

```bash
# Option 1: from GitHub Release
mkdir -p fastkv-client && tar xzf fastkv-client-rust-v{version}.tar.gz -C fastkv-client
# Then add as path dependency in Cargo.toml:
# [dependencies]
# fastkv-client = { path = "../fastkv-client" }

# Option 2: download from releases page
# https://github.com/JustAMamont/fastkv/releases

# Option 3: from source (clone repo)
cd clients/rust
# In your project: cargo add --path ../clients/rust
```

### Cargo.toml

```toml
[dependencies]
fastkv-client = { path = "../fastkv-client" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Basic Usage

```rust
use fastkv_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect
    let mut c = Client::connect("localhost", 6379).await?;

    // Basic operations
    c.set("greeting", "Hello, FastKV!").await?;
    let val = c.get("greeting").await?;
    println!("{:?}", val);    // Some("Hello, FastKV!")

    // TTL
    c.expire("session", 3600).await?;
    let ttl = c.ttl("session").await?;
    println!("{:?}", ttl);    // 3600

    // Hash
    c.hset("user:1", "name", "Alice").await?;
    c.hset("user:1", "age", "30").await?;
    let user = c.hgetall("user:1").await?;

    // List
    c.lpush("queue", &["task1", "task2"]).await?;
    let tasks = c.lrange("queue", 0, -1).await?;

    // Pipeline
    let mut pipe = c.pipeline();
    pipe.set("k1", "v1");
    pipe.set("k2", "v2");
    pipe.get("k1");
    let results = pipe.execute().await?;
    println!("{:?}", results);

    c.close().await;
    Ok(())
}
```

### Use with Environment Variables

```rust
use std::env;

fn get_kv_url() -> (String, u16) {
    let host = env::var("FASTKV_HOST").unwrap_or_else(|_| "localhost".into());
    let port = env::var("FASTKV_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379);
    (host, port)
}
```

---

## Pipeline

Pipeline batches multiple commands into a single TCP round-trip for higher throughput. Available in both first-party client SDKs.

```python
# Python
with client.pipeline() as pipe:
    pipe.set("a", "1")
    pipe.set("b", "2")
    pipe.set("c", "3")
    pipe.get("a")
    pipe.mget("a", "b", "c")
    results = pipe.execute()
```

```rust
// Rust
let mut pipe = client.pipeline();
pipe.set("a", "1");
pipe.set("b", "2");
let results = pipe.execute().await?;
```

---

## Command Coverage Matrix

The server implements the full command set listed in the main [`README.md`](../README.md#supported-commands). The first-party clients cover a subset — anything not covered can still be invoked through each client's raw-command escape hatch (Python: `_execute_command(*args)`; Rust: add a new method on `Client` calling `self.request(&[...])`).

| Category | Commands | Python sync | Python async | Rust |
|----------|----------|:---:|:---:|:---:|
| Core | `PING`, `ECHO`, `DBSIZE`, `INFO`, `QUIT`, `AUTH` | ✅ | ✅ | ✅ |
| String | `SET` (NX/XX/EX/PX), `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `MSET`, `MGET`, `SETNX`, `PSETEX`, `GETSET`, `GETDEL` | ✅ | ✅ | ✅ |
| TTL | `EXPIRE`, `TTL`, `PTTL`, `PERSIST` | ✅ | ✅ | ✅ |
| Hash | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HMGET`, `HMSET`, `HINCRBY`, `HSETNX` | ✅ | ✅ | ✅ |
| List | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LREM`, `LTRIM`, `LSET` | ✅ | ✅ | ✅ |
| Key mgmt | `TYPE`, `RENAME`, `UNLINK`, `FLUSHALL`, `FLUSHDB` | ✅ | ✅ | ✅ |
| Server | `SAVE`, `BGSAVE` | ✅ | ✅ | ✅ |
| Scan | `SCAN`, `DBSTATS` | ✅ | ✅ | ❌ (use raw command) |
| Blob Arena | `BSET`, `BGET`, `BGETRAW`, `BSTATS` | ✅ | ✅ | ❌ (use raw command) |
| Similarity | `SIMHASH`, `FINDSIM`, `LSHADD`, `LSHREM` | ✅ | ✅ | ❌ (use raw command) |
| Pub/Sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PUBSUB CHANNELS`, `PUBSUB NUMSUB` | ❌ (use raw socket) | ❌ (use raw socket) | ❌ (use raw command) |
| Sorted Sets | `ZADD`, `ZSCORE`, `ZCARD`, `ZRANGE`, `ZREVRANGE`, `ZREVRANGEBYSCORE`, `ZREM`, `ZINCRBY` | ❌ (use raw command) | ❌ (use raw command) | ❌ (use raw command) |

> **Pub/Sub note**: subscribe mode requires a long-lived socket read loop that is incompatible with the request/response pipeline pattern of the current sync / async clients. Use a raw socket with the `fastkv.resp` encoder/decoder, or any Redis-compatible client's `SUBSCRIBE` API.

---

## Design Principles

- **No Redis dependency** — hand-rolled RESP protocol implementation
- **Zero external deps** — Python: standard library only; Rust: tokio only
- **Consistent API** — same method names across sync and async variants
- **Sync + Async** — blocking (`FastKVClient`) and non-blocking (`FastKVAsyncClient`) variants in Python; native async in Rust
- **Pipeline support** — batch commands for maximum throughput
- **Thread-safe** — Rust: async (tokio); Python sync client is guarded by an internal lock for shared use across threads

---

## Testing

```bash
# Install toolchains (Rust + Python + Node — Node is used as a portable
# port-wait helper by setup_and_test.sh)
./scripts/install_deps.sh

# Run all integration tests (server unit tests + Python sync + Python async
# + Rust client integration tests)
./scripts/setup_and_test.sh
```

| Suite | Tests |
|-------|------:|
| Rust (server unit + integration) | 206 |
| Rust (client SDK) | 23 |
| Python sync | 27 |
| Python async | 28 |
