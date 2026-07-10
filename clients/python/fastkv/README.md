# FastKV Python Client

Zero-dependency Python client for FastKV — a Redis-compatible key-value store speaking **RESP** over TCP.

Implements the RESP protocol from scratch using only Python's built-in `socket` module. No third-party packages required.

## Installation

```bash
pip install git+https://github.com/JustAMamont/fastkv.git#subdirectory=clients/python
```

## Quick Start

```python
from fastkv import FastKVClient

with FastKVClient("localhost", 6379) as c:
    c.set("greeting", "Hello, FastKV!")
    print(c.get("greeting"))  # b"Hello, FastKV!"

    # TTL
    c.expire("greeting", 3600)
    print(c.ttl("greeting"))  # ~3600

    # Blob Arena (large values, zstd-compressed server-side)
    c.bset("doc:1", b"x" * 5000)
    print(c.bget("doc:1")[:10])  # b"xxxxxxxxxx"

    # Similarity search (SimHash + LSH)
    c.lsh_add("doc:1")
    c.lsh_add("doc:2")
    print(c.find_similar("doc:1"))  # [b"doc:2"]

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
| `info(section=None)` | Server info (optional section filter) |
| `dbsize()` | Number of keys |
| `scan(cursor=0, count=None, match=None)` | Cursor-based key iteration → `(next_cursor, keys)` |
| `dbstats()` | Aggregate store statistics dict |

### String

| Method | Description |
|--------|-------------|
| `set(key, value, ex=None, px=None, nx=False, xx=False)` | Set a key (returns `True`/`False`) |
| `get(key)` | Get a key (`bytes` or `None`) |
| `get_str(key)` / `set_str(key, value)` | UTF-8 string convenience helpers (v1.2.3+) |
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
| `set_nx(key, value)` | Set if not exists |
| `get_set(key, value)` | Set new value, return old value |
| `get_del(key)` | Get and delete atomically |
| `pset_ex(key, value, milliseconds)` | Set with millisecond TTL |

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
| `h_incr_by(key, field, delta)` | Increment hash field by delta |
| `h_set_nx(key, field, value)` | Set hash field only if not exists |

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

### Blob Arena (large values, zstd-compressed server-side)

| Method | Description |
|--------|-------------|
| `bset(key, value)` | Store value in Blob Arena with zstd compression |
| `bget(key)` | Retrieve value (auto-decompresses BlobRefs) |
| `bgetraw(key)` | Get raw compressed bytes (skip decompression) |
| `bstats()` | Blob arena statistics dict (`total_used`, `total_compressed`, `total_original`, `compression_ratio`, `free_slots`) |

### Similarity Search

| Method | Description |
|--------|-------------|
| `simhash(key)` | Compute 64-bit SimHash for stored value; returns hex string |
| `find_similar(key, threshold=3)` | Find similar keys via LSH buckets (default Hamming threshold = 3) |
| `lsh_add(key, simhash_hex=None)` | Index key in LSH buckets (auto-computes SimHash if omitted) |
| `lsh_rem(key, simhash_hex=None)` | Remove key from LSH buckets |

### Key Management

| Method | Description |
|--------|-------------|
| `type_of(key)` | Returns type: `string`, `hash`, `list`, or `none` |
| `rename(key, new_key)` | Rename a key (atomic, transfers TTL) |
| `unlink(keys)` | Delete keys (same as `delete` for compatibility) |
| `flush_all()` / `flush_db()` | Delete all keys + WAL checkpoint |
| `auth(password)` | Authenticate (required when server runs with `--requirepass`) |
| `save()` / `bgsave()` | Synchronous / background WAL checkpoint |

### Pipeline

```python
with client.pipeline() as pipe:
    pipe.set("x", "1")
    pipe.incr("x")
    results = pipe.execute()  # [True, 2]
```

## Async Client

The async client class is **`FastKVAsyncClient`** (declared in `fastkv/async_client.py` and re-exported from `fastkv/__init__.py`). It provides the same API as `FastKVClient` over `asyncio`. All methods are `async` and return awaitables.

```python
import asyncio
from fastkv import FastKVAsyncClient

async def main():
    async with FastKVAsyncClient("localhost", 6379) as c:
        await c.set("greeting", "Hello, FastKV!")
        print(await c.get("greeting"))  # b"Hello, FastKV!"

        # Pipeline
        async with c.pipeline() as pipe:
            pipe.set("k1", "v1")
            pipe.set("k2", "v2")
            pipe.get("k1")
            results = await pipe.execute()  # [True, True, b"v1"]

asyncio.run(main())
```

The async client has the same method names and return types as the sync client, with all methods being `async`.

## Unsupported Commands (use raw socket)

Pub/Sub (`SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PUBSUB`) and Sorted Set commands (`ZADD`, `ZRANGE`, ...) are not wrapped by the Python client because they require a long-lived socket read loop (subscribe mode) or were added after the client API was frozen. To use them, drop down to the RESP encoder/decoder in `fastkv.resp` and manage the socket directly. See `tests/test_fastkv.py` for examples of issuing raw commands.

## Exceptions

| Exception | Description |
|-----------|-------------|
| `FastKVError` | Base exception |
| `FastKVConnectionError` | Connection / socket error |
| `FastKVResponseError` | Server returned an error |

## Requirements

- Python 3.8+ (3.7 may work but is not tested)

## License

MIT
