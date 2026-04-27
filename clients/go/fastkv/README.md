# fastkv-go

Zero-dependency Go client for FastKV — a Redis-compatible key-value store speaking **RESP** over TCP.

Implements the full RESP protocol using only Go standard library packages. No third-party modules.

## Features

- Zero dependencies — `net`, `bufio`, `io`, `strconv`, `strings`, `sync`, `fmt`
- Full command coverage: core, string, TTL, hash, list
- Pipeline support — batch commands in a single round-trip
- Concurrent-safe — mutex-protected connection
- Lazy connection — TCP dial on first command

## Installation

```bash
go get github.com/fastkv/fastkv
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"
    "github.com/fastkv/fastkv"
)

func main() {
    c := fastkv.NewClient("localhost:6379")
    defer c.Close()

    if err := c.Set("hello", "world"); err != nil {
        log.Fatal(err)
    }
    val, err := c.Get("hello")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(val) // world
}
```

## API Reference

### Core

| Method | Returns | Description |
|--------|---------|-------------|
| `Ping()` | `(string, error)` | PONG |
| `Echo(msg)` | `(string, error)` | Echo |
| `Info()` | `(string, error)` | Server info |
| `Dbsize()` | `(int, error)` | Key count |

### String

| Method | Returns | Description |
|--------|---------|-------------|
| `Set(key, value)` | `error` | Set a key |
| `SetNX(key, value)` | `(bool, error)` | Set if not exists |
| `SetXX(key, value)` | `(bool, error)` | Set if exists |
| `SetEx(key, value, secs)` | `error` | Set with TTL (seconds) |
| `SetPx(key, value, ms)` | `error` | Set with TTL (milliseconds) |
| `Get(key)` | `(string, error)` | Get a key |
| `Del(keys...)` | `(int, error)` | Delete keys |
| `Exists(keys...)` | `(int, error)` | Count existing keys |
| `Incr(key)` | `(int, error)` | Increment by 1 |
| `Decr(key)` | `(int, error)` | Decrement by 1 |
| `IncrBy(key, n)` | `(int, error)` | Increment by n |
| `DecrBy(key, n)` | `(int, error)` | Decrement by n |
| `Append(key, value)` | `(int, error)` | Append to string |
| `Strlen(key)` | `(int, error)` | String length |
| `GetRange(key, start, end)` | `(string, error)` | Substring |
| `SetRange(key, offset, value)` | `(int, error)` | Overwrite at offset |
| `MSet(map)` | `error` | Set multiple keys |
| `MGet(keys...)` | `([]string, error)` | Get multiple keys |

### TTL

| Method | Returns | Description |
|--------|---------|-------------|
| `Expire(key, secs)` | `(bool, error)` | Set TTL |
| `Ttl(key)` | `(int, error)` | Remaining TTL (seconds) |
| `Pttl(key)` | `(int, error)` | Remaining TTL (ms) |
| `Persist(key)` | `(bool, error)` | Remove TTL |

### Hash

| Method | Returns | Description |
|--------|---------|-------------|
| `HSet(key, field, value)` | `(bool, error)` | Set field (true=new) |
| `HGet(key, field)` | `(string, error)` | Get field |
| `HDel(key, fields...)` | `(int, error)` | Delete fields |
| `HGetAll(key)` | `(map[string]string, error)` | All fields |
| `HExists(key, field)` | `(bool, error)` | Check field exists |
| `HLen(key)` | `(int, error)` | Field count |
| `HKeys(key)` | `([]string, error)` | All field names |
| `HVals(key)` | `([]string, error)` | All field values |
| `HMGet(key, fields...)` | `([]string, error)` | Get multiple fields |
| `HMSet(key, map)` | `error` | Set multiple fields |

### List

| Method | Returns | Description |
|--------|---------|-------------|
| `LPush(key, elems...)` | `(int, error)` | Push to head |
| `RPush(key, elems...)` | `(int, error)` | Push to tail |
| `LPop(key)` | `(string, error)` | Pop from head |
| `RPop(key)` | `(string, error)` | Pop from tail |
| `LRange(key, start, stop)` | `([]string, error)` | Get range |
| `LLen(key)` | `(int, error)` | List length |
| `LIndex(key, idx)` | `(string, error)` | Element by index |
| `LRem(key, count, elem)` | `(int, error)` | Remove elements |
| `LTrim(key, start, stop)` | `error` | Trim list |
| `LSet(key, idx, elem)` | `error` | Set element by index |

### Pipeline

```go
p := client.Pipeline()
p.Set("a", "1")
p.Set("b", "2")
p.Incr("a")
p.Get("a")

res, err := p.Execute()
val, _ := res.String(3)   // "2"
n, _ := res.Int(2)         // 2
```

## Error Handling

```go
val, err := client.Get("nonexistent")
if errors.Is(err, fastkv.ErrNil) {
    fmt.Println("key does not exist")
}
```

## License

MIT
