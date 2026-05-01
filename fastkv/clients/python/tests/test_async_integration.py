#!/usr/bin/env python3
"""
FastKV Python async client — integration tests.

Runs against a live FastKV server.  Set FASTKV_HOST / FASTKV_PORT
to override the defaults (localhost:8379).
"""

from __future__ import annotations

import asyncio
import os
import sys
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from fastkv.async_client import FastKVAsyncClient
from fastkv.exceptions import FastKVResponseError

HOST = os.environ.get("FASTKV_HOST", "localhost")
PORT = int(os.environ.get("FASTKV_PORT", "8379"))

passed = 0
failed = 0
errors: list[str] = []


async def run(name: str, coro) -> None:
    global passed, failed
    try:
        await coro
        passed += 1
        print(f"  PASS  {name}")
    except Exception:
        failed += 1
        tb = traceback.format_exc()
        errors.append(f"FAIL {name}:\n{tb}")
        print(f"  FAIL  {name}")


# ── helpers ──────────────────────────────────────────────────────────────────

async def client() -> FastKVAsyncClient:
    c = FastKVAsyncClient(HOST, PORT)
    await c.connect()
    return c


# ── Core ─────────────────────────────────────────────────────────────────────

async def test_ping():
    c = await client()
    try:
        r = await c.ping()
        assert r == "PONG" or r == b"PONG", f"got {r!r}"
    finally:
        await c.close()

async def test_echo():
    c = await client()
    try:
        r = await c.echo("hello async")
        assert r == "hello async" or r == b"hello async", f"got {r!r}"
    finally:
        await c.close()

async def test_dbsize():
    c = await client()
    try:
        n = await c.dbsize()
        assert isinstance(n, int)
    finally:
        await c.close()

# ── String ───────────────────────────────────────────────────────────────────

async def test_set_get():
    c = await client()
    try:
        await c.delete("async:sg")
        assert await c.set("async:sg", "world") is True
        assert await c.get("async:sg") == b"world"
        await c.delete("async:sg")
    finally:
        await c.close()

async def test_get_missing():
    c = await client()
    try:
        assert await c.get("async:nonexistent_xyz") is None
    finally:
        await c.close()

async def test_set_nx_xx():
    c = await client()
    try:
        await c.delete("async:nx")
        await c.set("async:nx", "original")
        r = await c.set("async:nx", "overwrite", nx=True)
        assert r is None  # NX failed — key exists
        assert await c.get("async:nx") == b"original"
        await c.delete("async:nx")
        r = await c.set("async:nx", "new", nx=True)
        assert r is True
        await c.set("async:xx", "orig")
        r = await c.set("async:xx", "upd", xx=True)
        assert r is True
        assert await c.get("async:xx") == b"upd"
        await c.delete(["async:nx", "async:xx"])
    finally:
        await c.close()

async def test_del_multi():
    c = await client()
    try:
        for k in ("async:d1", "async:d2", "async:d3"):
            await c.set(k, "v")
        n = await c.delete(["async:d1", "async:d2", "async:d3"])
        assert n == 3, f"DEL returned {n}"
        assert await c.get("async:d2") is None
    finally:
        await c.close()

async def test_del_single():
    c = await client()
    try:
        await c.set("async:ds", "v")
        n = await c.delete("async:ds")
        assert n == 1
        assert await c.get("async:ds") is None
    finally:
        await c.close()

async def test_incr_decr():
    c = await client()
    try:
        await c.delete("async:inc")
        assert await c.incr("async:inc") == 1  # init
        assert await c.incrby("async:inc", 9) == 10
        assert await c.decr("async:inc") == 9
        assert await c.decrby("async:inc", 4) == 5
        await c.delete("async:inc")
    finally:
        await c.close()

async def test_mset_mget():
    c = await client()
    try:
        await c.mset({"async:m1": "a", "async:m2": "b", "async:m3": "c"})
        vals = await c.mget(["async:m1", "async:m2", "async:m3"])
        assert vals == [b"a", b"b", b"c"], f"got {vals!r}"
        await c.delete(["async:m1", "async:m2", "async:m3"])
    finally:
        await c.close()

async def test_append_strlen():
    c = await client()
    try:
        await c.delete("async:ap")
        await c.set("async:ap", "hello")
        assert await c.append("async:ap", " world") == 11
        assert await c.strlen("async:ap") == 11
        await c.delete("async:ap")
    finally:
        await c.close()

async def test_getrange():
    c = await client()
    try:
        await c.delete("async:gr")
        await c.set("async:gr", "Hello, World!")
        r = await c.getrange("async:gr", 0, 4)
        assert r == b"Hello", f"got {r!r}"
        await c.delete("async:gr")
    finally:
        await c.close()

async def test_setrange():
    c = await client()
    try:
        await c.delete("async:sr")
        await c.set("async:sr", "Hello, World!")
        r = await c.setrange("async:sr", 7, "FastKV")
        assert r == 13, f"SETRANGE returned {r}"
        v = await c.get("async:sr")
        assert v == b"Hello, FastKV", f"got {v!r}"
        await c.delete("async:sr")
    finally:
        await c.close()

async def test_exists():
    c = await client()
    try:
        await c.delete(["async:ex1", "async:ex2"])
        await c.set("async:ex1", "v")
        n = await c.exists(["async:ex1", "async:ex2"])
        assert n == 1, f"EXISTS returned {n}"
        await c.delete("async:ex1")
    finally:
        await c.close()

# ── TTL ──────────────────────────────────────────────────────────────────────

async def test_expire_ttl_persist():
    c = await client()
    try:
        await c.delete("async:ttl")
        await c.set("async:ttl", "data")
        assert await c.ttl("async:ttl") == -1
        assert await c.expire("async:ttl", 60) is True
        ttl = await c.ttl("async:ttl")
        assert 1 <= ttl <= 60, f"TTL={ttl}"
        assert await c.persist("async:ttl") is True
        assert await c.ttl("async:ttl") == -1
        await c.delete("async:ttl")
    finally:
        await c.close()

async def test_pttl():
    c = await client()
    try:
        await c.delete("async:pttl")
        await c.set("async:pttl", "data")
        await c.expire("async:pttl", 60)
        pt = await c.pttl("async:pttl")
        assert 1000 <= pt <= 60000, f"PTTL={pt}"
        await c.delete("async:pttl")
    finally:
        await c.close()

# ── Hash ─────────────────────────────────────────────────────────────────────

async def test_hash_crud():
    c = await client()
    try:
        await c.delete("async:h")
        assert await c.hset("async:h", "name", "Alice") == 1
        assert await c.hset("async:h", "name", "Bob") == 0  # update
        assert await c.hget("async:h", "name") == b"Bob"
        assert await c.hexists("async:h", "name") is True
        assert await c.hlen("async:h") == 1
        await c.delete("async:h")
    finally:
        await c.close()

async def test_hmset_hgetall():
    c = await client()
    try:
        await c.delete("async:hm")
        await c.hmset("async:hm", {"f1": "v1", "f2": "v2", "f3": "v3"})
        m = await c.hgetall("async:hm")
        assert len(m) == 3, f"HGETALL got {len(m)} fields"
        assert m[b"f1"] == b"v1"
        vals = await c.hmget("async:hm", "f1", "f3")
        assert vals == [b"v1", b"v3"], f"got {vals!r}"
        await c.delete("async:hm")
    finally:
        await c.close()

async def test_hdel_hkeys_hvals():
    c = await client()
    try:
        await c.delete("async:hd")
        await c.hmset("async:hd", {"a": "1", "b": "2", "c": "3"})
        assert await c.hdel("async:hd", "b") == 1
        assert len(await c.hkeys("async:hd")) == 2
        assert len(await c.hvals("async:hd")) == 2
        await c.delete("async:hd")
    finally:
        await c.close()

# ── List ─────────────────────────────────────────────────────────────────────

async def test_list_push_pop():
    c = await client()
    try:
        await c.delete("async:l")
        await c.rpush("async:l", "a", "b", "c")
        await c.lpush("async:l", "z")
        items = await c.lrange("async:l", 0, -1)
        assert items == [b"z", b"a", b"b", b"c"], f"got {items}"
        assert await c.lpop("async:l") == b"z"
        assert await c.rpop("async:l") == b"c"
        await c.delete("async:l")
    finally:
        await c.close()

async def test_list_ltrim_lset_lrem():
    c = await client()
    try:
        await c.delete("async:lt")
        await c.rpush("async:lt", "a", "b", "c", "d", "e")
        assert await c.ltrim("async:lt", 1, 3) is True
        items = await c.lrange("async:lt", 0, -1)
        assert len(items) == 3
        assert await c.lset("async:lt", 0, "X") is True
        assert await c.lindex("async:lt", 0) == b"X"
        await c.rpush("async:lt", "b", "b")
        assert await c.lrem("async:lt", 2, "b") == 2
        await c.delete("async:lt")
    finally:
        await c.close()

async def test_list_len_index():
    c = await client()
    try:
        await c.delete("async:ll")
        await c.rpush("async:ll", "x", "y")
        assert await c.llen("async:ll") == 2
        assert await c.lindex("async:ll", -1) == b"y"
        await c.delete("async:ll")
    finally:
        await c.close()

# ── Pipeline ─────────────────────────────────────────────────────────────────

async def test_pipeline():
    c = await client()
    try:
        await c.delete(["async:p1", "async:p2", "async:p3"])
        async with c.pipeline() as p:
            p.set("async:p1", "10")
            p.set("async:p2", "20")
            p.incr("async:p1")
            p.get("async:p1")
            p.get("async:p2")
            p.mget(["async:p1", "async:p2", "async:p3"])
            results = await p.execute()

        assert len(results) == 6, f"expected 6 results, got {len(results)}"
        assert results[3] == b"11", f"INCR result: {results[3]!r}"
        assert results[4] == b"20", f"GET result: {results[4]!r}"
        await c.delete(["async:p1", "async:p2", "async:p3"])
    finally:
        await c.close()

async def test_pipeline_backpressure():
    c = await client()
    try:
        from fastkv.async_pipeline import AsyncPipeline
        p = AsyncPipeline(c, max_size=100)
        for i in range(200):
            p.get(f"async:bp:{i}")
        try:
            await p.execute()
            assert False, "should have raised ValueError"
        except ValueError as e:
            assert "exceeds" in str(e).lower()
    finally:
        await c.close()

# ── WRONGTYPE ────────────────────────────────────────────────────────────────

async def test_wrongtype():
    c = await client()
    try:
        await c.delete("async:wt")
        await c.set("async:wt", "string")
        try:
            await c.llen("async:wt")
            assert False, "should raise"
        except FastKVResponseError as e:
            assert "WRONGTYPE" in str(e)
        await c.delete("async:wt")
    finally:
        await c.close()

# ── Context manager ──────────────────────────────────────────────────────────

async def test_context_manager():
    async with FastKVAsyncClient(HOST, PORT) as c:
        pong = await c.ping()
        assert pong == "PONG" or pong == b"PONG"
    assert c._writer is None

# ── Concurrent access ────────────────────────────────────────────────────────

async def test_concurrent():
    """Fire 10 concurrent connections."""
    async def single(i: int):
        async with FastKVAsyncClient(HOST, PORT) as c:
            key = f"async:conc:{i}"
            await c.set(key, "val")
            await c.get(key)

    await asyncio.gather(*[single(i) for i in range(10)])
    c = await client()
    try:
        await c.delete([f"async:conc:{i}" for i in range(10)])
    finally:
        await c.close()


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    global passed, failed

    print(f"FastKV Python Async Integration Tests — {HOST}:{PORT}")
    print("=" * 55)

    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in tests:
        await run(fn.__name__, fn())

    print("=" * 55)
    print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")
    if errors:
        print("\nFailures:")
        for e in errors:
            print(e)
    return failed


if __name__ == "__main__":
    asyncio.run(main())
