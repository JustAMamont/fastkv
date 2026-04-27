# FastKV Python Client

Zero-dependency Python client for FastKV — a Redis-compatible key-value store speaking **RESP** over TCP.

Implements the RESP protocol from scratch using only Python's built-in `socket` module. No third-party packages required.

## Installation

Copy the `fastkv/` package into your project or place it on your Python path. No `pip install` needed.

## Quick Start

```python
from fastkv import FastKVClient

with FastKVClient("localhost", 6379) as c:
    c.set("greeting", "Hello, FastKV!")
    print(c.get("greeting"))  # b"Hello, FastKV!"

    # TTL
    c.expire("greeting", 3600)
    print(c.ttl("greeting"))  # ~3600

    # Pipeline
    with c.pipeline() as pipe:
        pipe.set("k1", "v1")
        pipe.set("k2", "v2")
        pipe.get("k1")
        results = pipe.execute()  # [True, True, b"v1"]
```

## API Reference

### Core

| Method | Description |
|--------|-------------|
| `ping()` | Returns `b"PONG"` |
| `echo(msg)` | Echo a message |
| `dbsize()` | Number of keys |

### String

| Method | Description |
|--------|-------------|
| `set(key, value, ex=None, px=None, nx=False, xx=False)` | Set a key |
| `get(key)` | Get a key (`bytes` or `None`) |
| `delete(keys)` | Delete one or more keys |
| `exists(keys)` | Count existing keys |
| `incr(key)` / `decr(key)` | Increment / decrement by 1 |
| `incrby(key, delta)` / `decrby(key, delta)` | By delta |
| `append(key, value)` | Append to string |
| `strlen(key)` | String length |
| `getrange(key, start, end)` | Substring by byte range |
| `setrange(key, offset, value)` | Overwrite at offset |
| `mset(pairs)` | Set multiple keys (`dict`) |
| `mget(keys)` | Get multiple keys |

### TTL

| Method | Description |
|--------|-------------|
| `expire(key, seconds)` | Set expiry in seconds |
| `ttl(key)` | Remaining TTL in seconds |
| `pttl(key)` | Remaining TTL in milliseconds |
| `persist(key)` | Remove expiry |

### Hash

| Method | Description |
|--------|-------------|
| `hset(key, field, value)` | Set hash field |
| `hget(key, field)` | Get hash field |
| `hdel(key, *fields)` | Delete hash fields |
| `hgetall(key)` | Get all fields and values |
| `hexists(key, field)` | Check field exists |
| `hlen(key)` | Number of fields |
| `hkeys(key)` / `hvals(key)` | All fields / all values |
| `hmget(key, *fields)` | Get multiple fields |
| `hmset(key, mapping)` | Set multiple fields |

### List

| Method | Description |
|--------|-------------|
| `lpush(key, *elements)` / `rpush(key, *elements)` | Push to head / tail |
| `lpop(key, count=None)` / `rpop(key, count=None)` | Pop from head / tail |
| `lrange(key, start, stop)` | Get range |
| `llen(key)` | List length |
| `lindex(key, index)` | Element at index |
| `lrem(key, count, element)` | Remove elements |
| `ltrim(key, start, stop)` | Trim list to range |
| `lset(key, index, element)` | Set element at index |

### Pipeline

```python
with client.pipeline() as pipe:
    pipe.set("x", "1")
    pipe.incr("x")
    results = pipe.execute()  # [True, 2]
```

## Exceptions

| Exception | Description |
|-----------|-------------|
| `FastKVError` | Base exception |
| `FastKVConnectionError` | Connection / socket error |
| `FastKVResponseError` | Server returned an error |

## Requirements

- Python 3.7+

## License

MIT
