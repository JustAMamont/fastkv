# FastKV Java Client

Zero-dependency Java client for FastKV — a Redis-compatible key-value store speaking **RESP** over TCP.

Uses only `java.net` and `java.io` from the JDK. No external libraries. Thread-safe (all commands are `synchronized`).

## Quick Start

```bash
javac -d target/classes src/main/java/com/fastkv/client/*.java
```

```java
import com.fastkv.client.FastKVClient;

public class App {
    public static void main(String[] args) throws Exception {
        try (FastKVClient c = new FastKVClient("localhost", 6379)) {
            c.set("hello", "world");
            System.out.println(c.get("hello")); // world
            System.out.println(c.ping());       // PONG
        }
    }
}
```

## Constructor

```java
// Default: 5s connect timeout, 5s read timeout
FastKVClient client = new FastKVClient("localhost", 6379);

// Custom timeouts (ms)
FastKVClient client = new FastKVClient("localhost", 6379, 3000, 10000);
```

## API Reference

### Core

| Method | Returns | Description |
|--------|---------|-------------|
| `ping()` | `String` | PONG |
| `echo(msg)` | `String` | Echo |
| `info()` | `String` | Server info |
| `dbsize()` | `long` | Key count |

### String

| Method | Returns | Description |
|--------|---------|-------------|
| `set(key, value)` | `String` | OK |
| `setEx(key, value, secs)` | `String` | OK |
| `setPx(key, value, ms)` | `String` | OK |
| `setNx(key, value)` | `boolean` | True if set |
| `setXx(key, value)` | `boolean` | True if set |
| `get(key)` | `String` | Value or null |
| `del(keys...)` | `long` | Deleted count |
| `exists(keys...)` | `long` | Existing count |
| `incr(key)` | `long` | New value |
| `decr(key)` | `long` | New value |
| `incrBy(key, delta)` | `long` | New value |
| `decrBy(key, delta)` | `long` | New value |
| `append(key, value)` | `long` | New length |
| `strlen(key)` | `long` | Length |
| `getRange(key, start, end)` | `String` | Substring |
| `setRange(key, offset, value)` | `long` | New length |
| `mset(pairs)` | `String` | OK |
| `mget(keys...)` | `List<String>` | Values (null for missing) |

### TTL

| Method | Returns | Description |
|--------|---------|-------------|
| `expire(key, secs)` | `boolean` | True if set |
| `ttl(key)` | `long` | Seconds (-1=persistent, -2=missing) |
| `pttl(key)` | `long` | Milliseconds |
| `persist(key)` | `boolean` | True if removed |

### Hash

| Method | Returns | Description |
|--------|---------|-------------|
| `hset(key, field, value)` | `boolean` | True if new field |
| `hget(key, field)` | `String` | Value or null |
| `hdel(key, fields...)` | `long` | Deleted count |
| `hgetAll(key)` | `Map<String,String>` | All fields |
| `hexists(key, field)` | `boolean` | True if exists |
| `hlen(key)` | `long` | Field count |
| `hkeys(key)` | `List<String>` | All field names |
| `hvals(key)` | `List<String>` | All field values |
| `hmget(key, fields...)` | `List<String>` | Values |
| `hmset(key, fields)` | `String` | OK |

### List

| Method | Returns | Description |
|--------|---------|-------------|
| `lpush(key, elems...)` | `long` | List length |
| `rpush(key, elems...)` | `long` | List length |
| `lpop(key)` | `String` | Popped element |
| `rpop(key)` | `String` | Popped element |
| `lrange(key, start, stop)` | `List<String>` | Elements |
| `llen(key)` | `long` | List length |
| `lindex(key, idx)` | `String` | Element at index |
| `lrem(key, count, elem)` | `long` | Removed count |
| `ltrim(key, start, stop)` | `String` | OK |
| `lset(key, idx, elem)` | `String` | OK |

### Pipeline

```java
try (Pipeline p = client.pipeline()) {
    p.set("k1", "v1");
    p.set("k2", "v2");
    p.incr("counter");
    p.get("k1");

    List<Object> results = p.sync();
    // results.get(0) → "OK"
    // results.get(1) → "OK"
    // results.get(2) → 1L
    // results.get(3) → "v1"
}
```

Pipeline opens its own TCP connection and is NOT thread-safe.

## Error Handling

| Exception | When |
|-----------|------|
| `FastKVException` | Base class for all FastKV errors |
| `FastKVConnectionException` | Socket / timeout errors |
| `FastKVResponseException` | Server returned `-ERR ...` |

```java
try {
    client.llen("not_a_list");
} catch (FastKVResponseException e) {
    System.err.println("Error: " + e.getMessage());
} catch (FastKVConnectionException e) {
    System.err.println("Connection failed: " + e.getMessage());
}
```

## Source Files

| File | Description |
|------|-------------|
| `FastKVClient.java` | Main client |
| `RespEncoder.java` | RESP encoder |
| `RespDecoder.java` | RESP decoder |
| `Pipeline.java` | Batched commands |
| `FastKVException.java` | Base exception |
| `FastKVConnectionException.java` | Connection error |
| `FastKVResponseException.java` | Server error |
| `Example.java` | Usage examples |

## Requirements

- JDK 8+

## License

MIT
