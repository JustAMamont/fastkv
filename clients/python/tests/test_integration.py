#!/usr/bin/env python3
"""
FastKV Python client — integration tests.

Runs against a live FastKV server.  Set FASTKV_HOST / FASTKV_PORT
to override the defaults (localhost:8379).
"""

from __future__ import annotations

import os
import sys
import threading
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from fastkv.client import FastKVClient
from fastkv.exceptions import FastKVResponseError, FastKVConnectionError

HOST = os.environ.get("FASTKV_HOST", "localhost")
PORT = int(os.environ.get("FASTKV_PORT", "6379"))

passed = 0
failed = 0
errors: list[str] = []


def run(name: str, fn) -> None:
    global passed, failed
    try:
        fn()
        passed += 1
        print(f"  PASS  {name}")
    except Exception:
        failed += 1
        tb = traceback.format_exc()
        errors.append(f"FAIL {name}:\n{tb}")
        print(f"  FAIL  {name}")


# ── helpers ──────────────────────────────────────────────────────────────────

def client() -> FastKVClient:
    return FastKVClient(HOST, PORT, socket_timeout=5)


# ── Core ─────────────────────────────────────────────────────────────────────

def test_ping():
    with client() as c:
        r = c.ping()
        assert r == b"PONG" or r == "PONG", f"got {r!r}"

def test_echo():
    with client() as c:
        r = c.echo("hello")
        assert r == b"hello" or r == "hello", f"got {r!r}"

def test_dbsize():
    with client() as c:
        n = c.dbsize()
        assert isinstance(n, int)

# ── String ───────────────────────────────────────────────────────────────────

def test_set_get():
    with client() as c:
        c.set("py:sg", "world")
        assert c.get("py:sg") == b"world"
        c.delete("py:sg")

def test_get_missing():
    with client() as c:
        assert c.get("py:nonexistent_xyz") is None

def test_setnx():
    with client() as c:
        c.delete(["py:nx"])
        assert c.set("py:nx", "orig") is True
        r = c.set("py:nx", "new", nx=True)
        assert r is None  # NX failed — key exists
        assert c.get("py:nx") == b"orig"
        c.delete(["py:nx"])
        r = c.set("py:nx", "new", nx=True)
        assert r is True
        c.delete(["py:nx"])

def test_setxx():
    with client() as c:
        c.delete(["py:xx"])
        r = c.set("py:xx", "v", xx=True)
        assert r is None  # XX on missing key
        c.set("py:xx", "orig")
        r = c.set("py:xx", "upd", xx=True)
        assert r is True
        assert c.get("py:xx") == b"upd"
        c.delete(["py:xx"])

def test_del_multi():
    with client() as c:
        for k in ("py:d1", "py:d2", "py:d3"):
            c.set(k, "v")
        n = c.delete(["py:d1", "py:d2", "py:d3"])
        assert n == 3, f"DEL returned {n}"
        assert c.get("py:d2") is None

def test_del_single():
    with client() as c:
        c.set("py:ds", "v")
        n = c.delete("py:ds")
        assert n == 1
        assert c.get("py:ds") is None

def test_incr_decr():
    with client() as c:
        c.delete(["py:inc"])
        assert c.incr("py:inc") == 1  # init
        assert c.incrby("py:inc", 9) == 10
        assert c.decr("py:inc") == 9
        assert c.decrby("py:inc", 4) == 5
        c.delete(["py:inc"])

def test_mset_mget():
    with client() as c:
        c.mset({"py:m1": "a", "py:m2": "b", "py:m3": "c"})
        vals = c.mget(["py:m1", "py:m2", "py:m3"])
        assert vals == [b"a", b"b", b"c"], f"got {vals!r}"
        c.delete(["py:m1", "py:m2", "py:m3"])

def test_append_strlen():
    with client() as c:
        c.delete(["py:ap"])
        c.set("py:ap", "hello")
        assert c.append("py:ap", " world") == 11
        assert c.strlen("py:ap") == 11
        c.delete(["py:ap"])

def test_getrange():
    with client() as c:
        c.delete(["py:gr"])
        c.set("py:gr", "Hello, World!")
        r = c.getrange("py:gr", 0, 4)
        assert r == b"Hello", f"got {r!r}"
        c.delete(["py:gr"])

def test_setrange():
    with client() as c:
        c.delete(["py:sr"])
        c.set("py:sr", "Hello, World!")
        r = c.setrange("py:sr", 7, "FastKV")
        assert r == 13, f"SETRANGE returned {r}"
        v = c.get("py:sr")
        assert v == b"Hello, FastKV", f"got {v!r}"
        c.delete(["py:sr"])

def test_exists():
    with client() as c:
        c.delete(["py:ex1", "py:ex2"])
        c.set("py:ex1", "v")
        n = c.exists(["py:ex1", "py:ex2"])
        assert n == 1, f"EXISTS returned {n}"
        c.delete(["py:ex1"])

# ── TTL ──────────────────────────────────────────────────────────────────────

def test_expire_ttl_persist():
    with client() as c:
        c.delete(["py:ttl"])
        c.set("py:ttl", "data")
        assert c.ttl("py:ttl") == -1
        assert c.expire("py:ttl", 60) is True
        ttl = c.ttl("py:ttl")
        assert 1 <= ttl <= 60, f"TTL={ttl}"
        assert c.persist("py:ttl") is True
        assert c.ttl("py:ttl") == -1
        c.delete(["py:ttl"])

def test_pttl():
    with client() as c:
        c.delete(["py:pttl"])
        c.set("py:pttl", "data")
        c.expire("py:pttl", 60)
        pt = c.pttl("py:pttl")
        assert 1000 <= pt <= 60000, f"PTTL={pt}"
        c.delete(["py:pttl"])

# ── Hash ─────────────────────────────────────────────────────────────────────

def test_hash_crud():
    with client() as c:
        c.delete(["py:h"])
        assert c.hset("py:h", "name", "Alice") == 1
        assert c.hset("py:h", "name", "Bob") == 0  # update
        assert c.hget("py:h", "name") == b"Bob"
        assert c.hexists("py:h", "name") is True
        assert c.hlen("py:h") == 1
        c.delete(["py:h"])

def test_hmset_hgetall():
    with client() as c:
        c.delete(["py:hm"])
        c.hmset("py:hm", {"f1": "v1", "f2": "v2", "f3": "v3"})
        m = c.hgetall("py:hm")
        assert len(m) == 3, f"HGETALL got {len(m)} fields"
        assert m[b"f1"] == b"v1"
        vals = c.hmget("py:hm", "f1", "f3")
        assert vals == [b"v1", b"v3"], f"got {vals!r}"
        c.delete(["py:hm"])

def test_hdel_hkeys_hvals():
    with client() as c:
        c.delete(["py:hd"])
        c.hmset("py:hd", {"a": "1", "b": "2", "c": "3"})
        assert c.hdel("py:hd", "b") == 1
        assert len(c.hkeys("py:hd")) == 2
        assert len(c.hvals("py:hd")) == 2
        c.delete(["py:hd"])

# ── List ─────────────────────────────────────────────────────────────────────

def test_list_push_pop():
    with client() as c:
        c.delete(["py:l"])
        c.rpush("py:l", "a", "b", "c")
        c.lpush("py:l", "z")
        items = c.lrange("py:l", 0, -1)
        assert items == [b"z", b"a", b"b", b"c"], f"got {items}"
        assert c.lpop("py:l") == b"z"
        assert c.rpop("py:l") == b"c"
        c.delete(["py:l"])

def test_list_ltrim_lset_lrem():
    with client() as c:
        c.delete(["py:lt"])
        c.rpush("py:lt", "a", "b", "c", "d", "e")
        assert c.ltrim("py:lt", 1, 3) is True
        items = c.lrange("py:lt", 0, -1)
        assert len(items) == 3
        assert c.lset("py:lt", 0, "X") is True
        assert c.lindex("py:lt", 0) == b"X"
        c.rpush("py:lt", "b", "b")
        assert c.lrem("py:lt", 2, "b") == 2
        c.delete(["py:lt"])

def test_list_len_index():
    with client() as c:
        c.delete(["py:ll"])
        c.rpush("py:ll", "x", "y")
        assert c.llen("py:ll") == 2
        assert c.lindex("py:ll", -1) == b"y"
        c.delete(["py:ll"])

# ── Pipeline ─────────────────────────────────────────────────────────────────

def test_pipeline():
    with client() as c:
        c.delete(["py:p1", "py:p2", "py:p3"])
        with c.pipeline() as p:
            p.set("py:p1", "10")
            p.set("py:p2", "20")
            p.incr("py:p1")
            p.get("py:p1")
            p.get("py:p2")
            p.mget(["py:p1", "py:p2", "py:p3"])
            results = p.execute()

        assert len(results) == 6, f"expected 6 results, got {len(results)}"
        assert results[3] == b"11", f"INCR result: {results[3]!r}"
        assert results[4] == b"20", f"GET result: {results[4]!r}"
        c.delete(["py:p1", "py:p2", "py:p3"])

def test_pipeline_backpressure():
    with client() as c:
        from fastkv.pipeline import Pipeline
        p = Pipeline(c, max_size=100)
        for i in range(200):
            p.get(f"py:bp:{i}")
        try:
            p.execute()
            assert False, "should have raised ValueError"
        except ValueError as e:
            assert "exceeds" in str(e).lower()

# ── WRONGTYPE ────────────────────────────────────────────────────────────────

def test_wrongtype():
    with client() as c:
        c.delete(["py:wt"])
        c.set("py:wt", "string")
        try:
            c.llen("py:wt")
            assert False, "should raise"
        except FastKVResponseError as e:
            assert "WRONGTYPE" in str(e)
        c.delete(["py:wt"])

# ── Concurrent access ────────────────────────────────────────────────────────

def test_concurrent():
    errs = []

    def worker(tid):
        try:
            with client() as c:
                c.set(f"py:conc:{tid}", "v")
                c.get(f"py:conc:{tid}")
        except Exception as e:
            errs.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errs, f"concurrent errors: {errs}"
    with client() as c:
        c.delete([f"py:conc:{i}" for i in range(10)])


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"FastKV Python Integration Tests — {HOST}:{PORT}")
    print("=" * 55)

    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in tests:
        run(fn.__name__, fn)

    print("=" * 55)
    print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")
    if errors:
        print("\nFailures:")
        for e in errors:
            print(e)
    sys.exit(1 if failed else 0)
