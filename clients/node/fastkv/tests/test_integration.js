'use strict';

// Force stdout unbuffered when piped (needed for CI/scripts)
if (process.stdout && process.stdout._handle && typeof process.stdout._handle.setBlocking === 'function') {
  process.stdout._handle.setBlocking(true);
}

/**
 * FastKV Node.js client — integration tests.
 *
 * Run:  node test_integration.js [host] [port]
 * Defaults: host=127.0.0.1  port=8379
 */

const net = require('net');
const { FastKVClient } = require('../client');

// Helper: connect a client (replaces old `await new FastKVClient(...)`)
async function makeClient() {
  return FastKVClient.create({ host: HOST, port: PORT });
}

const HOST = process.argv[2] || '127.0.0.1';
const PORT = parseInt(process.argv[3] || '8379', 10);

let passed = 0;
let failed = 0;

function check(name, condition, msg) {
  if (condition) {
    passed++;
    console.log(`  PASS  ${name}`);
  } else {
    failed++;
    console.log(`  FAIL  ${name} — ${msg || ''}`);
  }
}

// ── helpers ─────────────────────────────────────────────────────────────────

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

const TEST_TIMEOUT = 10000; // 10s per test

async function runTest(fn) {
  const timer = setTimeout(() => {
    throw new Error(`${fn.name} timed out after ${TEST_TIMEOUT}ms`);
  }, TEST_TIMEOUT);
  try {
    await fn();
  } finally {
    clearTimeout(timer);
  }
}

// ── Raw TCP pre-check (bypasses client entirely) ────────────────────────────

function rawTcpPing(host, port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port }, () => {
      // Connected — send RESP PING
      socket.write(Buffer.from('*1\r\n$4\r\nPING\r\n'));
    });

    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error(`Raw TCP PING timed out after 5000ms`));
    }, 5000);

    let buf = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      // We expect +PONG\r\n (7 bytes) — wait for at least that
      if (buf.length >= 5) {
        clearTimeout(timer);
        socket.destroy();
        resolve(buf.toString('utf8').trim());
      }
    });

    socket.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });

    socket.on('close', () => {
      clearTimeout(timer);
      if (buf.length === 0) {
        reject(new Error('Connection closed before any data received'));
      }
    });
  });
}

// ── Core ─────────────────────────────────────────────────────────────────────

async function testPing() {
  const c = await makeClient();
  try {
    const r = await c.ping();
    check('PING', r === 'PONG', `got ${JSON.stringify(r)}`);
  } finally { c.close(); }
}

async function testEcho() {
  const c = await makeClient();
  try {
    const r = await c.echo('hello');
    check('ECHO', r === 'hello', `got ${JSON.stringify(r)}`);
  } finally { c.close(); }
}

async function testDbsize() {
  const c = await makeClient();
  try {
    const n = await c.dbsize();
    check('DBSIZE', typeof n === 'number' && n >= 0, `got ${n}`);
  } finally { c.close(); }
}

// ── String ───────────────────────────────────────────────────────────────────

async function testSetGet() {
  const c = await makeClient();
  try {
    await c.del('n:sg');
    await c.set('n:sg', 'world');
    const v = await c.get('n:sg');
    check('SET/GET', v === 'world', `got ${JSON.stringify(v)}`);
    await c.del('n:sg');
  } finally { c.close(); }
}

async function testGetMissing() {
  const c = await makeClient();
  try {
    const v = await c.get('n:nonexistent_xyz');
    check('GET missing', v === null, `got ${JSON.stringify(v)}`);
  } finally { c.close(); }
}

async function testSetNX() {
  const c = await makeClient();
  try {
    await c.del('n:nx');
    await c.set('n:nx', 'orig');
    const r = await c.set('n:nx', 'other', { nx: true });
    check('SETNX existing', r === null, `got ${JSON.stringify(r)}`);
    await c.del('n:nx');
    const r2 = await c.set('n:nx', 'new', { nx: true });
    check('SETNX missing', r2 === 'OK', `got ${JSON.stringify(r2)}`);
    await c.del('n:nx');
  } finally { c.close(); }
}

async function testSetXX() {
  const c = await makeClient();
  try {
    await c.del('n:xx');
    const r = await c.set('n:xx', 'v', { xx: true });
    check('SETXX missing', r === null, `got ${JSON.stringify(r)}`);
    await c.set('n:xx', 'orig');
    const r2 = await c.set('n:xx', 'upd', { xx: true });
    check('SETXX existing', r2 === 'OK', `got ${JSON.stringify(r2)}`);
    const v = await c.get('n:xx');
    check('SETXX value', v === 'upd', `got ${JSON.stringify(v)}`);
    await c.del('n:xx');
  } finally { c.close(); }
}

async function testDelMulti() {
  const c = await makeClient();
  try {
    await c.set('n:d1', 'a'); await c.set('n:d2', 'b'); await c.set('n:d3', 'c');
    const n = await c.del('n:d1', 'n:d2', 'n:d3');
    check('DEL multi', n === 3, `got ${n}`);
    const v = await c.get('n:d2');
    check('DEL verify', v === null, 'still exists');
  } finally { c.close(); }
}

async function testIncrDecr() {
  const c = await makeClient();
  try {
    await c.del('n:inc');
    check('INCR init', await c.incr('n:inc') === 1, '');
    check('INCRBY', await c.incrBy('n:inc', 9) === 10, '');
    check('DECR', await c.decr('n:inc') === 9, '');
    check('DECRBY', await c.decrBy('n:inc', 4) === 5, '');
    await c.del('n:inc');
  } finally { c.close(); }
}

async function testMSetMGet() {
  const c = await makeClient();
  try {
    await c.mset({ 'n:m1': 'a', 'n:m2': 'b', 'n:m3': 'c' });
    const vals = await c.mget('n:m1', 'n:m2', 'n:m3', 'n:missing');
    check('MSET/MGET', vals[0] === 'a' && vals[1] === 'b' && vals[2] === 'c', `got ${JSON.stringify(vals)}`);
    check('MGET null', vals[3] === null, 'not null');
    await c.del('n:m1', 'n:m2', 'n:m3');
  } finally { c.close(); }
}

async function testAppendStrlen() {
  const c = await makeClient();
  try {
    await c.del('n:ap'); await c.set('n:ap', 'hello');
    const n = await c.append('n:ap', ' world');
    check('APPEND', n === 11, `got ${n}`);
    check('STRLEN', await c.strlen('n:ap') === 11, '');
    await c.del('n:ap');
  } finally { c.close(); }
}

async function testGetRangeSetRange() {
  const c = await makeClient();
  try {
    await c.del('n:gr'); await c.set('n:gr', 'Hello, World!');
    check('GETRANGE', (await c.getRange('n:gr', 0, 4)) === 'Hello', '');
    const n = await c.setRange('n:gr', 7, 'FastKV');
    check('SETRANGE len', n === 13, `got ${n}`);
    check('SETRANGE val', (await c.get('n:gr')) === 'Hello, FastKV', '');
    await c.del('n:gr');
  } finally { c.close(); }
}

async function testExists() {
  const c = await makeClient();
  try {
    await c.del('n:ex1', 'n:ex2'); await c.set('n:ex1', 'v');
    check('EXISTS', await c.exists('n:ex1', 'n:ex2') === 1, '');
    await c.del('n:ex1');
  } finally { c.close(); }
}

// ── TTL ──────────────────────────────────────────────────────────────────────

async function testExpireTtlPersist() {
  const c = await makeClient();
  try {
    await c.del('n:ttl'); await c.set('n:ttl', 'data');
    check('TTL no-expiry', await c.ttl('n:ttl') === -1, '');
    await c.expire('n:ttl', 60);
    const ttl = await c.ttl('n:ttl');
    check('TTL after EXPIRE', ttl > 0 && ttl <= 60, `got ${ttl}`);
    await c.persist('n:ttl');
    check('TTL after PERSIST', await c.ttl('n:ttl') === -1, '');
    await c.del('n:ttl');
  } finally { c.close(); }
}

async function testPttl() {
  const c = await makeClient();
  try {
    await c.del('n:pttl'); await c.set('n:pttl', 'data'); await c.expire('n:pttl', 60);
    const pt = await c.pttl('n:pttl');
    check('PTTL', pt > 0 && pt <= 60000, `got ${pt}`);
    await c.del('n:pttl');
  } finally { c.close(); }
}

// ── Hash ─────────────────────────────────────────────────────────────────────

async function testHashCRUD() {
  const c = await makeClient();
  try {
    await c.del('n:h');
    check('HSET new', await c.hset('n:h', 'name', 'Alice') === 1, '');
    check('HSET update', await c.hset('n:h', 'name', 'Bob') === 0, '');
    check('HGET', (await c.hget('n:h', 'name')) === 'Bob', '');
    check('HEXISTS', await c.hexists('n:h', 'name') === 1, '');
    check('HLEN', await c.hlen('n:h') === 1, '');
    await c.del('n:h');
  } finally { c.close(); }
}

async function testHMSetHGetAll() {
  const c = await makeClient();
  try {
    await c.del('n:hm');
    await c.hmset('n:hm', { f1: 'v1', f2: 'v2', f3: 'v3' });
    const m = await c.hgetAll('n:hm');
    check('HGETALL', Object.keys(m).length === 3 && m.f1 === 'v1', JSON.stringify(m));
    const vals = await c.hmget('n:hm', 'f1', 'f3');
    check('HMGET', vals[0] === 'v1' && vals[1] === 'v3', JSON.stringify(vals));
    await c.del('n:hm');
  } finally { c.close(); }
}

async function testHDelHKeysHVals() {
  const c = await makeClient();
  try {
    await c.del('n:hd');
    await c.hmset('n:hd', { a: '1', b: '2', c: '3' });
    check('HDEL', await c.hdel('n:hd', 'b') === 1, '');
    check('HKEYS', (await c.hkeys('n:hd')).length === 2, '');
    check('HVALS', (await c.hvals('n:hd')).length === 2, '');
    await c.del('n:hd');
  } finally { c.close(); }
}

// ── List ─────────────────────────────────────────────────────────────────────

async function testListPushPop() {
  const c = await makeClient();
  try {
    await c.del('n:l');
    await c.rpush('n:l', 'a', 'b', 'c');
    await c.lpush('n:l', 'z');
    const items = await c.lrange('n:l', 0, -1);
    check('LPUSH+RPUSH', items.length === 4 && items[0] === 'z', JSON.stringify(items));
    check('LPOP', (await c.lpop('n:l')) === 'z', '');
    check('RPOP', (await c.rpop('n:l')) === 'c', '');
    await c.del('n:l');
  } finally { c.close(); }
}

async function testListLTrimLSetLRem() {
  const c = await makeClient();
  try {
    await c.del('n:lt');
    await c.rpush('n:lt', 'a', 'b', 'c', 'd', 'e');
    await c.ltrim('n:lt', 1, 3);
    check('LTRIM', (await c.lrange('n:lt', 0, -1)).length === 3, '');
    await c.lset('n:lt', 0, 'X');
    check('LSET+LINDEX', (await c.lindex('n:lt', 0)) === 'X', '');
    await c.rpush('n:lt', 'b', 'b');
    check('LREM', await c.lrem('n:lt', 2, 'b') === 2, '');
    await c.del('n:lt');
  } finally { c.close(); }
}

async function testListLenIndex() {
  const c = await makeClient();
  try {
    await c.del('n:ll');
    await c.rpush('n:ll', 'x', 'y');
    check('LLEN', await c.llen('n:ll') === 2, '');
    check('LINDEX -1', (await c.lindex('n:ll', -1)) === 'y', '');
    await c.del('n:ll');
  } finally { c.close(); }
}

// ── Pipeline (dedicated connection) ──────────────────────────────────────────

async function testPipeline() {
  const c = await makeClient();
  try {
    await c.del('n:p1', 'n:p2', 'n:p3');
    const pipe = await c.pipeline();
    try {
      pipe.set('n:p1', '10');
      pipe.set('n:p2', '20');
      pipe.incr('n:p1');
      pipe.get('n:p1');
      pipe.get('n:p2');
      pipe.dbsize();
      const results = await pipe.execute();
      check('Pipeline count', results.length === 6, `got ${results.length}`);
      check('Pipeline INCR', results[3] === '11', `got ${JSON.stringify(results[3])}`);
      check('Pipeline GET', results[4] === '20', `got ${JSON.stringify(results[4])}`);
    } finally {
      pipe.close();
    }
    await c.del('n:p1', 'n:p2', 'n:p3');
  } finally { c.close(); }
}

// ── WRONGTYPE ────────────────────────────────────────────────────────────────

async function testWrongType() {
  const c = await makeClient();
  try {
    await c.del('n:wt');
    await c.set('n:wt', 'string');
    try {
      await c.llen('n:wt');
      check('WRONGTYPE', false, 'should have thrown');
    } catch (e) {
      check('WRONGTYPE', e.message && e.message.includes('WRONGTYPE'), e.message);
    }
    await c.del('n:wt');
  } finally { c.close(); }
}

// ── Concurrent ───────────────────────────────────────────────────────────────

async function testConcurrent() {
  const c = await makeClient();
  try {
    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push((async (tid) => {
        await c.set(`n:conc:${tid}`, 'v');
        await c.get(`n:conc:${tid}`);
      })(i));
    }
    await Promise.all(promises);
    check('Concurrent', true, '');
    await c.del(...Array.from({ length: 10 }, (_, i) => `n:conc:${i}`));
  } finally { c.close(); }
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`FastKV Node.js Integration Tests — ${HOST}:${PORT}`);
  console.log('='.repeat(55));

  // Pre-check: raw TCP PING (bypasses client code entirely)
  console.log('  [diag] Raw TCP PING ...');
  try {
    const raw = await rawTcpPing(HOST, PORT);
    console.log(`  [diag] Raw TCP OK — server replied: ${JSON.stringify(raw)}`);
  } catch (e) {
    console.error(`  [diag] Raw TCP FAILED — ${e.message}`);
    console.error('  [diag] Skipping all client tests (server unreachable from Node.js net)');
    process.exit(1);
  }

  const tests = [
    testPing, testEcho, testDbsize,
    testSetGet, testGetMissing, testSetNX, testSetXX,
    testDelMulti, testIncrDecr, testMSetMGet,
    testAppendStrlen, testGetRangeSetRange, testExists,
    testExpireTtlPersist, testPttl,
    testHashCRUD, testHMSetHGetAll, testHDelHKeysHVals,
    testListPushPop, testListLTrimLSetLRem, testListLenIndex,
    testPipeline, testWrongType, testConcurrent,
  ];

  for (const fn of tests) {
    try {
      await runTest(fn);
    } catch (e) {
      failed++;
      console.error(`  FAIL  ${fn.name} — ${e.message}`);
    }
  }

  console.log('='.repeat(55));
  console.log(`Results: ${passed} passed, ${failed} failed, ${passed + failed} total`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => { console.error(e); process.exit(1); });
