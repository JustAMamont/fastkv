#!/usr/bin/env node
'use strict';

const { FastKVClient } = require('../index');

/**
 * Example: basic usage of the FastKV Node.js client.
 *
 * Make sure a FastKV (or Redis-compatible) server is running on 127.0.0.1:7379
 * before running this script:
 *
 *   node example.js
 */

async function main() {
  let client;

  try {
    // ── Connect ──────────────────────────────────────────────────────────
    console.log('Connecting to FastKV…');
    client = await FastKVClient.create({
      host: '127.0.0.1',
      port: 7379,
      socketTimeout: 3000,
    });
    console.log('Connected!\n');

    // ── Server commands ──────────────────────────────────────────────────
    console.log('=== Server ===');
    const pong = await client.ping();
    console.log('PING  →', pong);

    const echo = await client.echo('hello fastkv');
    console.log('ECHO  →', echo);

    const dbSize = await client.dbsize();
    console.log('DBSIZE →', dbSize);
    console.log();

    // ── String commands ──────────────────────────────────────────────────
    console.log('=== Strings ===');
    await client.set('greeting', 'Hello, World!');
    console.log('SET greeting "Hello, World!" → OK');

    const val = await client.get('greeting');
    console.log('GET greeting →', val);

    await client.set('counter', '10');
    const afterIncr = await client.incr('counter');
    console.log('INCR counter  (was 10) →', afterIncr);

    const afterDecrBy = await client.decrBy('counter', 3);
    console.log('DECRBY counter 3 →', afterDecrBy);

    const appended = await client.append('greeting', ' 🎉');
    console.log('APPEND greeting → new length', appended);
    console.log('GET greeting →', await client.get('greeting'));

    const sub = await client.getRange('greeting', 0, 4);
    console.log('GETRANGE greeting 0 4 →', sub);

    // MSET / MGET
    await client.mset({ k1: 'v1', k2: 'v2', k3: 'v3' });
    const multi = await client.mget('k1', 'k2', 'k3', 'nonexistent');
    console.log('MGET k1 k2 k3 nonexistent →', multi);
    console.log();

    // ── TTL commands ─────────────────────────────────────────────────────
    console.log('=== TTL ===');
    await client.set('temp', 'expires-soon');
    await client.expire('temp', 60);
    const ttl = await client.ttl('temp');
    console.log('TTL temp →', ttl);

    const persisted = await client.persist('temp');
    console.log('PERSIST temp →', persisted);
    const ttlAfter = await client.ttl('temp');
    console.log('TTL temp (after persist) →', ttlAfter);
    console.log();

    // ── Hash commands ────────────────────────────────────────────────────
    console.log('=== Hashes ===');
    await client.hset('user:1', 'name', 'Alice');
    await client.hset('user:1', 'email', 'alice@example.com');
    await client.hset('user:1', 'age', '30');
    console.log('HSET user:1 name/email/age');

    const name = await client.hget('user:1', 'name');
    console.log('HGET user:1 name →', name);

    const exists = await client.hexists('user:1', 'email');
    console.log('HEXISTS user:1 email →', exists);

    const hashLen = await client.hlen('user:1');
    console.log('HLEN user:1 →', hashLen);

    const all = await client.hgetAll('user:1');
    console.log('HGETALL user:1 →', all);

    const keys = await client.hkeys('user:1');
    console.log('HKEYS user:1 →', keys);

    const vals = await client.hvals('user:1');
    console.log('HVALS user:1 →', vals);

    // HMSET / HMGET
    await client.hmset('user:2', { name: 'Bob', email: 'bob@example.com' });
    const hmget = await client.hmget('user:2', 'name', 'email', 'missing');
    console.log('HMGET user:2 name email missing →', hmget);

    await client.hdel('user:1', 'age');
    console.log('HDEL user:1 age → done');
    console.log();

    // ── List commands ────────────────────────────────────────────────────
    console.log('=== Lists ===');
    await client.del('mylist'); // clean up
    const pushLen = await client.rpush('mylist', 'a', 'b', 'c');
    console.log('RPUSH mylist a b c → length', pushLen);

    await client.lpush('mylist', 'z');
    console.log('LPUSH mylist z');

    const listLen = await client.llen('mylist');
    console.log('LLEN mylist →', listLen);

    const range = await client.lrange('mylist', 0, -1);
    console.log('LRANGE mylist 0 -1 →', range);

    const idx = await client.lindex('mylist', 0);
    console.log('LINDEX mylist 0 →', idx);

    const popped = await client.lpop('mylist');
    console.log('LPOP mylist →', popped);

    const rPopped = await client.rpop('mylist');
    console.log('RPOP mylist →', rPopped);

    await client.lset('mylist', 0, 'X');
    console.log('LSET mylist 0 X');
    console.log('LRANGE mylist 0 -1 →', await client.lrange('mylist', 0, -1));

    await client.ltrim('mylist', 0, 1);
    console.log('LTRIM mylist 0 1');
    console.log('LRANGE mylist 0 -1 →', await client.lrange('mylist', 0, -1));
    console.log();

    // ── Pipeline ─────────────────────────────────────────────────────────
    console.log('=== Pipeline ===');
    const results = await client.pipeline()
      .set('p1', '100')
      .set('p2', '200')
      .incr('p1')
      .get('p1')
      .get('p2')
      .del('p1', 'p2')
      .execute();
    console.log('Pipeline results →', results);

    // ── EXISTS / DEL ─────────────────────────────────────────────────────
    console.log();
    console.log('=== Exists / Del ===');
    const ex1 = await client.exists('greeting', 'nonexistent');
    console.log('EXISTS greeting nonexistent →', ex1);

    const delCount = await client.del('greeting', 'counter', 'temp');
    console.log('DEL greeting counter temp →', delCount);

  } catch (err) {
    console.error('Error:', err.message);
    if (err.code) console.error('Code:', err.code);
  } finally {
    if (client) {
      client.close();
      console.log('\nConnection closed.');
    }
  }
}

main();
