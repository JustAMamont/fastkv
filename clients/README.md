# FastKV Client SDKs

First-party client libraries for FastKV — a Redis-compatible lock-free key-value store written in Rust.

FastKV speaks the standard RESP wire protocol, so any Redis-compatible client works out of the box. We maintain first-party SDKs for **Rust** and **Python**; for other languages, use an existing Redis client (see [`README.md`](../README.md#using-existing-redis-clients-community-supported) for snippets).

If you maintain a community FastKV client in another language, open a PR to list it in the table below.

---

## Supported Languages

| Language | Dependencies | Pipeline | Async | Install |
|----------|:------------:|:--------:|:-----:|---------|
| **Python** | stdlib only | Yes | asyncio | `pip install git+https://github.com/JustAMamont/fastkv.git#subdirectory=clients/python` |
| **Rust** | tokio only | Yes | native | extract `fastkv-client-rust-v{version}.tar.gz` |
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
    c.set_expires("session", "data", 3600).await?;
    let ttl = c.ttl("session").await?;
    println!("{:?}", ttl);    // Some(3600)

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

## Supported Commands

All clients support the same command set:

**Core:** `ping`, `echo`, `info`, `dbsize`

**String:** `set` (NX/XX/EX/PX), `get`, `del`, `exists`, `incr`, `decr`, `incrby`, `decrby`, `append`, `strlen`, `getrange`, `setrange`, `mset`, `mget`, `setnx`, `psetex`, `getset`, `getdel`

**TTL:** `expire`, `ttl`, `pttl`, `persist`

**Hash:** `hset`, `hget`, `hdel`, `hgetall`, `hexists`, `hlen`, `hkeys`, `hvals`, `hmget`, `hmset`, `hincrby`, `hsetnx`

**List:** `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen`, `lindex`, `lrem`, `ltrim`, `lset`

**Key management:** `type`, `rename`, `unlink`, `flushall`, `flushdb`

**Server:** `auth`, `save`, `bsave`

---

## Design Principles

- **No Redis dependency** — hand-rolled RESP protocol implementation
- **Zero external deps** — only standard library (except Rust: tokio)
- **Consistent API** — same method names across all languages
- **Sync + Async** — blocking and non-blocking variants (Python asyncio + Rust tokio)
- **Pipeline support** — batch commands for maximum throughput
- **Thread-safe** — Rust: async, Python: sync lock

---

## Testing

```bash
# Install toolchains
./scripts/install_deps.sh

# Run all integration tests
./scripts/setup_and_test.sh
```
