# FastKV Client SDKs

Lightweight, zero-dependency client libraries for FastKV — a Redis-compatible lock-free key-value store written in Rust.

All clients implement the RESP protocol from scratch over TCP. **No Redis SDK dependency.**

---

## Supported Languages

| Language | Dependencies | Pipeline | Async | Install from Release |
|----------|:------------:|:--------:|:-----:|---------------------|
| **Python** | stdlib only | Yes | asyncio | `pip install fastkv-0.2.0-py3-none-any.whl` |
| **Node.js** | stdlib only | Yes | native | `npm install fastkv-client-1.0.0.tgz` |
| **Java** | JDK 8+ only | Yes | CompletableFuture | add `fastkv-client-java-0.1.0.jar` to classpath |
| **Go** | stdlib only | Yes | — | extract `fastkv-client-go-v0.1.0.tar.gz` |
| **Rust** | tokio only | Yes | native | extract `fastkv-client-rust-v0.1.0.tar.gz` |

All clients are **libraries** — you import them into your project, they connect to a running FastKV server over TCP.

---

## Python

### Installation

```bash
# Option 1: from PyPI (when published)
pip install fastkv

# Option 2: from release .whl file
pip install fastkv-0.2.0-py3-none-any.whl

# Option 3: from source
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

## Node.js

### Installation

```bash
# Option 1: from release tarball
npm install fastkv-client-1.0.0.tgz

# Option 2: from local source
npm install ./clients/node/fastkv
```

### Basic Usage

```js
const { FastKVClient } = require('fastkv-client');

async function main() {
    // Connect (factory method, returns a Promise)
    const c = await FastKVClient.create({ host: 'localhost', port: 6379 });

    // Basic operations
    await c.set('greeting', 'Hello, FastKV!');
    const val = await c.get('greeting');
    console.log(val);                // Hello, FastKV!

    // TTL
    await c.set('token', 'abc123', 'EX', 3600);
    const ttl = await c.ttl('token');
    console.log(ttl);                // ~3600

    // Hash
    await c.hset('user:1', 'name', 'Alice');
    await c.hset('user:1', 'email', 'alice@example.com');
    const user = await c.hgetall('user:1');
    console.log(user);               // { name: 'Alice', email: 'alice@example.com' }

    // Pipeline
    const pipe = c.pipeline();
    pipe.set('k1', 'v1');
    pipe.set('k2', 'v2');
    pipe.get('k1');
    pipe.get('k2');
    const results = await pipe.execute();
    console.log(results);            // ['OK', 'OK', 'v1', 'v2']

    await c.close();
}

main();
```

### Use in Express.js

```js
const express = require('express');
const { FastKVClient } = require('fastkv-client');

let kv;

async function start() {
    kv = await FastKVClient.create({ host: 'localhost', port: 6379 });

    const app = express();

    app.get('/cache/:key', async (req, res) => {
        const value = await kv.get(req.params.key);
        res.json({ key: req.params.key, value });
    });

    app.post('/cache/:key', async (req, res) => {
        await kv.set(req.params.key, req.body.value);
        res.json({ ok: true });
    });

    app.listen(3000);
}

start();
```

---

## Java

### Installation

```bash
# Option 1: from release jar — add to classpath
javac -cp fastkv-client-java-0.1.0.jar:. MyApp.java
java -cp fastkv-client-java-0.1.0.jar:. MyApp

# Option 2: from source
cd clients/java
javac -encoding UTF-8 -d out src/FastKVClient.java src/Pipeline.java \
    src/RespEncoder.java src/RespDecoder.java src/FastKV*.java
jar cf fastkv-client.jar -C out .
```

### Maven (install jar to local repo)

```bash
mvn install:install-file \
    -Dfile=fastkv-client-java-0.1.0.jar \
    -DgroupId=com.fastkv \
    -DartifactId=fastkv-client \
    -Dversion=0.1.0 \
    -Dpackaging=jar
```

```xml
<!-- pom.xml -->
<dependency>
    <groupId>com.fastkv</groupId>
    <artifactId>fastkv-client</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Sync Client

```java
import com.fastkv.client.FastKVClient;
import com.fastkv.client.Pipeline;

// try-with-resources for auto-close
try (FastKVClient c = new FastKVClient("localhost", 6379)) {

    // Basic operations
    c.set("greeting", "Hello, FastKV!");
    String val = c.get("greeting");
    System.out.println(val);     // Hello, FastKV!

    // TTL
    c.setEx("session", "data", 3600);
    long ttl = c.ttl("session");
    System.out.println(ttl);     // ~3600

    // Hash
    c.hset("user:1", "name", "Alice");
    c.hset("user:1", "age", "30");
    Map<String, String> user = c.hgetAll("user:1");
    System.out.println(user);    // {name=Alice, age=30}

    // List
    c.lpush("queue", "task1", "task2");
    List<String> tasks = c.lrange("queue", 0, -1);

    // Pipeline (own dedicated connection)
    try (Pipeline p = c.pipeline()) {
        p.set("k1", "v1");
        p.set("k2", "v2");
        p.get("k1");
        List<Object> results = p.executeAndClose();
        System.out.println(results);  // [OK, OK, v1]
    }
}

// Custom timeouts
try (FastKVClient c = new FastKVClient("10.0.0.1", 6380, 3000, 5000)) {
    // connect timeout = 3s, socket read timeout = 5s
}
```

### Reactive Client (CompletableFuture)

```java
import com.fastkv.client.FastKVReactiveClient;
import java.util.concurrent.TimeUnit;

try (FastKVReactiveClient c = new FastKVReactiveClient("localhost", 6379)) {

    // Chain async operations
    c.set("greeting", "Hello!")
     .thenCompose(v -> c.get("greeting"))
     .thenAccept(val -> System.out.println(val))
     .get(5, TimeUnit.SECONDS);

    // Works with Project Reactor: Mono.fromCompletionStage(future)
    // Works with RxJava 3: Single.fromFuture(future)
}
```

---

## Go

### Installation

```bash
# Option 1: go get (when published)
go get github.com/fastkv/fastkv

# Option 2: from release tarball
tar xzf fastkv-client-go-v0.1.0.tar.gz
cp -r fastkv/ $GOPATH/src/github.com/fastkv/fastkv/

# Option 3: vendor in your project
mkdir -p vendor/github.com/fastkv/fastkv
tar xzf fastkv-client-go-v0.1.0.tar.gz -C vendor/github.com/fastkv/fastkv --strip-components=1
```

### Basic Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/fastkv/fastkv"
)

func main() {
    // Connect (lazy dial — connects on first command)
    c := fastkv.NewClient("localhost:6379")
    defer c.Close()

    // Basic operations
    c.Set("greeting", "Hello, FastKV!")
    val, err := c.Get("greeting")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(val)   // Hello, FastKV!

    // TTL
    c.SetEx("session", "abc", 3600)
    ttl, _ := c.TTL("session")
    fmt.Println(ttl)   // ~3600

    // Hash
    c.HSet("user:1", "name", "Alice")
    c.HSet("user:1", "email", "alice@example.com")
    user, _ := c.HGetAll("user:1")
    fmt.Println(user)  // map[name:Alice email:alice@example.com]

    // List
    c.LPush("queue", "task1", "task2")
    tasks, _ := c.LRange("queue", 0, -1)
    fmt.Println(tasks) // [task2 task1]

    // Pipeline (batch commands)
    pipe := c.Pipeline()
    pipe.Set("k1", "v1")
    pipe.Set("k2", "v2")
    pipe.Get("k1")
    results := pipe.Execute()
    fmt.Println(results) // [OK OK Hello, FastKV!]
}

// FastKVClient is safe for concurrent use (all methods are synchronized).
// You can share a single client across goroutines.
```

### Use in HTTP Handler

```go
package main

import (
    "net/http"
    "github.com/fastkv/fastkv"
)

var kv *fastkv.Client

func main() {
    kv = fastkv.NewClient("localhost:6379")
    defer kv.Close()

    http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
        key := r.URL.Query().Get("key")
        val, err := kv.Get(key)
        if err != nil {
            http.Error(w, err.Error(), 500)
            return
        }
        w.Write([]byte(val))
    })

    http.ListenAndServe(":8080", nil)
}
```

---

## Rust

### Installation

```bash
# Option 1: from release tarball
tar xzf fastkv-client-rust-v0.1.0.tar.gz
cd fastkv-client-rust
# Add as path dependency in your Cargo.toml:
# [dependencies]
# fastkv-client = { path = "../fastkv-client-rust" }

# Option 2: from source
cd clients/rust
# In your project: cargo add --path ../clients/rust
```

### Cargo.toml

```toml
[dependencies]
fastkv-client = { path = "../fastkv-client-rust" }
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

## Pipeline (All Languages)

Pipeline batches multiple commands into a single TCP round-trip for higher throughput. Available in all client SDKs.

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

```js
// Node.js
const pipe = client.pipeline();
pipe.set('a', '1');
pipe.set('b', '2');
const results = await pipe.execute();
```

```java
// Java
try (Pipeline p = client.pipeline()) {
    p.set("a", "1");
    p.set("b", "2");
    List<Object> results = p.executeAndClose();
}
```

```go
// Go
pipe := client.Pipeline()
pipe.Set("a", "1")
pipe.Set("b", "2")
results := pipe.Execute()
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

**String:** `set` (NX/XX/EX/PX), `get`, `del`, `exists`, `incr`, `decr`, `incrby`, `decrby`, `append`, `strlen`, `getrange`, `setrange`, `mset`, `mget`

**TTL:** `expire`, `ttl`, `pttl`, `persist`

**Hash:** `hset`, `hget`, `hdel`, `hgetall`, `hexists`, `hlen`, `hkeys`, `hvals`, `hmget`, `hmset`

**List:** `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen`, `lindex`, `lrem`, `ltrim`, `lset`

---

## Design Principles

- **No Redis dependency** — hand-rolled RESP protocol implementation
- **Zero external deps** — only standard library (except Rust: tokio)
- **Consistent API** — same method names across all languages
- **Sync + Async** — blocking and non-blocking variants (Python, Java)
- **Pipeline support** — batch commands for maximum throughput
- **Thread-safe** — Go: synchronized, Java: synchronized, Python: sync lock, Rust: async

---

## Testing

```bash
# Install toolchains
./scripts/install_deps.sh

# Run all integration tests
./scripts/setup_and_test.sh
```
