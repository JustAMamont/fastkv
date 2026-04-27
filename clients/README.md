# FastKV Client SDKs

Lightweight, zero-dependency client libraries for FastKV — a Redis-compatible lock-free key-value store written in Rust.

All clients implement the RESP protocol from scratch over TCP. **No Redis SDK dependency.**

## Supported Languages

| Language | Directory | Dependencies | Pipeline |
|----------|-----------|:------------:|:--------:|
| **Rust** | `rust/` | tokio | Yes |
| Go | `go/fastkv/` | stdlib only | Yes |
| Python | `python/fastkv/` | stdlib only | Yes |
| Java | `java/` | JDK 8+ only | Yes |
| Node.js | `node/fastkv/` | stdlib only | Yes |

## Quick Start

### Rust (async)

```rust
use fastkv_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = Client::connect("localhost", 8379).await?;
    c.set("greeting", "Hello, FastKV!").await?;
    println!("{:?}", c.get("greeting").await?);
    c.close().await;
    Ok(())
}
```

### Go

```go
package main

import (
    "fmt"
    "github.com/fastkv/fastkv"
)

func main() {
    c := fastkv.NewClient("localhost:6379")
    defer c.Close()

    c.Set("greeting", "Hello, FastKV!")
    val, _ := c.Get("greeting")
    fmt.Println(val) // Hello, FastKV!
}
```

### Python (sync)

```python
from fastkv import FastKVClient

with FastKVClient("localhost", 6379) as c:
    c.set("greeting", "Hello, FastKV!")
    print(c.get("greeting"))  # b"Hello, FastKV!"
```

### Python (async)

```python
import asyncio
from fastkv import AsyncFastKVClient

async def main():
    async with AsyncFastKVClient("localhost", 6379) as c:
        await c.set("greeting", "Hello, FastKV!")
        print(await c.get("greeting"))  # b"Hello, FastKV!"

asyncio.run(main())
```

### Java (sync)

```java
import com.fastkv.client.FastKVClient;

try (FastKVClient c = new FastKVClient("localhost", 6379)) {
    c.set("greeting", "Hello, FastKV!");
    System.out.println(c.get("greeting")); // Hello, FastKV!
}
```

### Java (reactive)

```java
import com.fastkv.client.FastKVReactiveClient;

try (FastKVReactiveClient c = new FastKVReactiveClient("localhost", 6379)) {
    String pong = c.ping().get(5, TimeUnit.SECONDS);
    c.set("greeting", "Hello, FastKV!")
     .thenCompose(v -> c.get("greeting"))
     .thenAccept(System.out::println)
     .get(5, TimeUnit.SECONDS);
}
```

### Node.js

```js
const { FastKVClient } = require('fastkv-client');

async function main() {
    const c = await FastKVClient.create({ host: 'localhost', port: 6379 });
    await c.set('greeting', 'Hello, FastKV!');
    console.log(await c.get('greeting')); // Hello, FastKV!
    await c.close();
}

main();
```

## Supported Commands

All clients support the same command set:

**Core:** `ping`, `echo`, `info`, `dbsize`

**String:** `set` (NX/XX/EX/PX), `get`, `del`, `exists`, `incr`, `decr`, `incrby`, `decrby`, `append`, `strlen`, `getrange`, `setrange`, `mset`, `mget`

**TTL:** `expire`, `ttl`, `pttl`, `persist`

**Hash:** `hset`, `hget`, `hdel`, `hgetall`, `hexists`, `hlen`, `hkeys`, `hvals`, `hmget`, `hmset`

**List:** `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen`, `lindex`, `lrem`, `ltrim`, `lset`

**Pipeline:** Buffer multiple commands and execute in a single round-trip.

## Async / Reactive Clients

| Language | API Style | Class |
|----------|-----------|-------|
| Python | `async/await` + asyncio | `AsyncFastKVClient` |
| Java | `CompletableFuture<T>` | `FastKVReactiveClient` |

Both provide the same command set as their synchronous counterparts and support reactive pipelines. The Java reactive client bridges trivially to Project Reactor (`Mono.fromCompletionStage()`) and RxJava 3 (`Single.fromFuture()`).

## Design Principles

- **No Redis dependency** — hand-rolled RESP protocol implementation
- **Zero external deps** — only standard library
- **Consistent API** — same method names across all languages
- **Sync + Async** — blocking and non-blocking variants
- **Pipeline support** — batch commands for maximum throughput

## Testing

```bash
# Install toolchains
./scripts/install_deps.sh

# Run all integration tests
./scripts/setup_and_test.sh
```
