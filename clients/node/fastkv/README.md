# fastkv-client

Zero-dependency Node.js client for FastKV — a Redis-compatible key-value store speaking **RESP** over TCP.

Built entirely on `net` and `events` from the Node.js standard library. No ioredis, no node-redis.

## Install

```bash
npm install fastkv-client
```

## Quick Start

```js
const { FastKVClient } = require('fastkv-client');

async function main() {
  const client = await FastKVClient.create({ host: '127.0.0.1', port: 6379 });

  await client.set('hello', 'world');
  const value = await client.get('hello'); // 'world'
  console.log(value);

  await client.close();
}

main().catch(console.error);
```

## Constructor Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | `string` | `'127.0.0.1'` | Server hostname |
| `port` | `number` | `6379` | Server port |
| `socketTimeout` | `number` | `5000` | Socket timeout in ms |

### Connecting

The constructor creates an unconnected instance. Use the static factory to connect immediately:

```js
// Recommended — connect in one step
const client = await FastKVClient.create({ host: '127.0.0.1', port: 6379 });

// Or connect manually (e.g. to attach event listeners first)
const client = new FastKVClient({ host: '127.0.0.1', port: 6379 });
client.on('close', () => console.log('disconnected'));
await client.connect();
```

## API Reference

### Server

| Method | Returns | Description |
|--------|---------|-------------|
| `ping()` | `Promise<string>` | PONG |
| `echo(msg)` | `Promise<string>` | Echo a message |
| `info()` | `Promise<string>` | Server info |
| `dbsize()` | `Promise<number>` | Number of keys |

### String

| Method | Returns | Description |
|--------|---------|-------------|
| `set(key, value, { ex, px, nx, xx })` | `Promise<string\|null>` | Set a key |
| `get(key)` | `Promise<string\|null>` | Get a key |
| `del(...keys)` | `Promise<number>` | Delete keys |
| `exists(...keys)` | `Promise<number>` | Check if keys exist |
| `incr(key)` | `Promise<number>` | Increment by 1 |
| `decr(key)` | `Promise<number>` | Decrement by 1 |
| `incrBy(key, delta)` | `Promise<number>` | Increment by delta |
| `decrBy(key, delta)` | `Promise<number>` | Decrement by delta |
| `append(key, value)` | `Promise<number>` | Append to value |
| `strlen(key)` | `Promise<number>` | String length |
| `getRange(key, start, end)` | `Promise<string>` | Substring |
| `setRange(key, offset, value)` | `Promise<number>` | Overwrite substring |
| `mset(pairs)` | `Promise<string>` | Set multiple keys |
| `mget(...keys)` | `Promise<(string\|null)[]>` | Get multiple keys |

### TTL

| Method | Returns | Description |
|--------|---------|-------------|
| `expire(key, secs)` | `Promise<number>` | Set expiry in seconds |
| `ttl(key)` | `Promise<number>` | Time-to-live in seconds |
| `pttl(key)` | `Promise<number>` | Time-to-live in ms |
| `persist(key)` | `Promise<number>` | Remove expiry |

### Hash

| Method | Returns | Description |
|--------|---------|-------------|
| `hset(key, field, value)` | `Promise<number>` | Set hash field |
| `hget(key, field)` | `Promise<string\|null>` | Get hash field |
| `hdel(key, ...fields)` | `Promise<number>` | Delete hash fields |
| `hgetAll(key)` | `Promise<Object>` | Get all fields |
| `hexists(key, field)` | `Promise<number>` | Check field exists |
| `hlen(key)` | `Promise<number>` | Number of fields |
| `hkeys(key)` | `Promise<string[]>` | All field names |
| `hvals(key)` | `Promise<string[]>` | All field values |
| `hmget(key, ...fields)` | `Promise<(string\|null)[]>` | Get multiple fields |
| `hmset(key, fields)` | `Promise<string>` | Set multiple fields |

### List

| Method | Returns | Description |
|--------|---------|-------------|
| `lpush(key, ...elements)` | `Promise<number>` | Push left |
| `rpush(key, ...elements)` | `Promise<number>` | Push right |
| `lpop(key, count?)` | `Promise<string\|null>` | Pop left |
| `rpop(key, count?)` | `Promise<string\|null>` | Pop right |
| `lrange(key, start, stop)` | `Promise<string[]>` | Range query |
| `llen(key)` | `Promise<number>` | List length |
| `lindex(key, index)` | `Promise<string\|null>` | Element by index |
| `lrem(key, count, element)` | `Promise<number>` | Remove elements |
| `ltrim(key, start, stop)` | `Promise<string>` | Trim list |
| `lset(key, index, element)` | `Promise<string>` | Set element by index |

### Pipeline

```js
const results = await client.pipeline()
  .set('a', '1')
  .set('b', '2')
  .incr('a')
  .get('a')
  .execute();
// results → ['OK', 'OK', 2, '2']
```

### Lifecycle

```js
client.close();
client.isClosed;                    // boolean
client.on('close', () => {});       // close event
```

## Error Handling

| Class | When |
|-------|------|
| `FastKVConnectionError` | Connection refused / lost |
| `FastKVTimeoutError` | Connect or command timeout |
| `FastKVResponseError` | Server returned a RESP error |
| `FastKVClientClosedError` | Client used after `close()` |

```js
const { FastKVResponseError } = require('fastkv-client');
try {
  await client.set('key', 'value');
} catch (err) {
  if (err instanceof FastKVResponseError) {
    console.error('Server error:', err.message);
  }
}
```

## License

MIT
