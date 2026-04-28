# FastKV Rust Client

Async FastKV client built on tokio.

## Installation

```bash
# From GitHub Release
mkdir -p fastkv-client && tar xzf fastkv-client-rust-v{version}.tar.gz -C fastkv-client

# Download from https://github.com/JustAMamont/fastkv/releases
```

## Usage

```toml
# Cargo.toml
[dependencies]
fastkv-client = { path = "../fastkv-client" }
```

```rust
use fastkv_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = Client::connect("127.0.0.1", 6379).await?;

    c.set("hello", "world").await?;
    let val = c.get("hello").await?;
    println!("{val:?}"); // Some("world")

    c.close().await;
    Ok(())
}
```

## API

| Method | Returns | Description |
|--------|---------|-------------|
| `ping()` | `String` | PONG |
| `echo(msg)` | `String` | Echo message |
| `dbsize()` | `i64` | Key count |
| `set(k, v)` | `()` | Set key |
| `get(k)` | `Option<String>` | Get key |
| `del(keys)` | `i64` | Delete keys |
| `exists(keys)` | `i64` | Count existing |
| `incr(k)` / `decr(k)` | `i64` | Increment / decrement |
| `incr_by(k, n)` / `decr_by(k, n)` | `i64` | Increment / decrement by n |
| `append(k, v)` | `i64` | Append to string |
| `strlen(k)` | `i64` | String length |
| `getrange(k, start, end)` | `String` | Substring |
| `setrange(k, off, v)` | `i64` | Overwrite at offset |
| `mset(pairs)` | `()` | Set multiple keys |
| `mget(keys)` | `Vec<Option<String>>` | Get multiple keys |
| `expire(k, sec)` | `bool` | Set TTL |
| `ttl(k)` / `pttl(k)` | `i64` | Remaining time |
| `persist(k)` | `bool` | Remove TTL |
| `hset(k, f, v)` | `i64` | Set hash field |
| `hget(k, f)` | `Option<String>` | Get hash field |
| `hdel(k, fields)` | `i64` | Delete hash fields |
| `hgetall(k)` | `Vec<(String, String)>` | All fields |
| `hlen(k)` | `i64` | Field count |
| `hkeys(k)` / `hvals(k)` | `Vec<String>` | Field names / values |
| `hmget(k, fields)` | `Vec<Option<String>>` | Get multiple fields |
| `hmset(k, pairs)` | `()` | Set multiple fields |
| `lpush(k, elems)` / `rpush(k, elems)` | `i64` | Push to list |
| `lpop(k)` / `rpop(k)` | `Option<String>` | Pop from list |
| `llen(k)` | `i64` | List length |
| `lrange(k, start, stop)` | `Vec<String>` | Get range |
| `lindex(k, i)` | `Option<String>` | Element by index |
| `lrem(k, n, elem)` | `i64` | Remove elements |
| `ltrim(k, start, stop)` | `()` | Trim list |
| `lset(k, i, elem)` | `()` | Set element |
| `pipeline()` | `Pipeline` | Batch commands |

### Pipeline

```rust
let mut p = c.pipeline();
p.set("k1", "v1");
p.set("k2", "v2");
p.incr("k1");
p.get("k1");
let res = p.execute().await?;
assert_eq!(res.integer(2)?, 1);
assert_eq!(res.string(3)?, Some("1".into()));
```

## Run Tests

```bash
FASTKV_HOST=localhost FASTKV_PORT=6379 cargo test -- --test-threads=1
```

## Run Example

```bash
cargo run --example example
```

## Requirements

- Rust 1.85+
- tokio

## License

MIT
