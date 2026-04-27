#!/usr/bin/env python3
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
"""
FastKV Python Client SDK — Usage Examples
==========================================

This script demonstrates common operations with the FastKV client.
Make sure a FastKV server is running before executing:

    python example.py
"""

from fastkv import FastKVClient, FastKVError, Pipeline


def main() -> None:
    # ------------------------------------------------------------------
    # 1. Basic connection (context manager auto-closes on exit)
    # ------------------------------------------------------------------
    print("=== Basic Connection ===")
    with FastKVClient(host="localhost", port=6379, socket_timeout=5) as client:
        # -- Core commands --------------------------------------------------
        print("PING:", client.ping())  # PONG

        print("ECHO:", client.echo("Hello FastKV!"))

        info = client.info()
        print("INFO (first 80 chars):", info[:80] if len(info) > 80 else info)

        print("DBSIZE:", client.dbsize())

        # -- String commands ------------------------------------------------
        print("\n=== String Commands ===")
        client.set("greeting", "hello")
        val = client.get("greeting")
        print("GET greeting:", val)  # b'hello'

        client.set("temp", "bye", ex=10)  # expires in 10 seconds
        print("GET temp:", client.get("temp"))
        print("TTL temp:", client.ttl("temp"))

        # NX — only set if key does not exist
        result = client.set("greeting", "world", nx=True)
        print("SET NX on existing key:", result)  # None (not set)

        client.delete("greeting")
        result = client.set("greeting", "world", nx=True)
        print("SET NX on missing key:", result)  # True

        # XX — only set if key already exists
        result = client.set("greeting", "updated", xx=True)
        print("SET XX on existing key:", result)  # True

        # INCR / DECR
        client.set("counter", "0")
        print("INCR:", client.incr("counter"))   # 1
        print("INCR:", client.incr("counter"))   # 2
        print("INCRBY 5:", client.incrby("counter", 5))  # 7
        print("DECR:", client.decr("counter"))   # 6
        print("DECRBY 3:", client.decrby("counter", 3))  # 3

        # APPEND / STRLEN / GETRANGE / SETRANGE
        client.set("msg", "Hello")
        client.append("msg", " World")
        print("GET msg:", client.get("msg"))  # b'Hello World'
        print("STRLEN:", client.strlen("msg"))  # 11
        print("GETRANGE 0-4:", client.getrange("msg", 0, 4))  # b'Hello'
        client.setrange("msg", 6, "FastKV")
        print("After SETRANGE:", client.get("msg"))  # b'Hello FastKV'

        # MSET / MGET
        client.mset({"k1": "v1", "k2": "v2", "k3": "v3"})
        print("MGET:", client.mget(["k1", "k2", "k3", "nonexistent"]))
        # [b'v1', b'v2', b'v3', None]

        # EXISTS / DEL
        print("EXISTS k1:", client.exists("k1"))  # 1
        print("EXISTS [k1, k2, missing]:", client.exists(["k1", "k2", "missing"]))  # 2
        print("DEL k1, k2, k3:", client.delete(["k1", "k2", "k3"]))

        # -- TTL commands ---------------------------------------------------
        print("\n=== TTL Commands ===")
        client.set("ttl_demo", "value")
        print("TTL (no expiry):", client.ttl("ttl_demo"))  # -1
        client.expire("ttl_demo", 3600)
        print("TTL (after EXPIRE 3600):", client.ttl("ttl_demo"))
        print("PTTL:", client.pttl("ttl_demo"))
        client.persist("ttl_demo")
        print("TTL (after PERSIST):", client.ttl("ttl_demo"))  # -1

        # -- Hash commands --------------------------------------------------
        print("\n=== Hash Commands ===")
        client.hset("user:1", "name", "Alice")
        client.hset("user:1", "email", "alice@example.com")
        client.hset("user:1", "age", "30")
        print("HGET name:", client.hget("user:1", "name"))  # b'Alice'
        print("HEXISTS name:", client.hexists("user:1", "name"))  # True
        print("HLEN:", client.hlen("user:1"))  # 3
        print("HKEYS:", client.hkeys("user:1"))
        print("HVALS:", client.hvals("user:1"))
        print("HGETALL:", client.hgetall("user:1"))
        print("HMGET:", client.hmget("user:1", "name", "email"))
        client.hmset("user:1", {"city": "NYC", "role": "engineer"})
        print("HGETALL after HMSET:", client.hgetall("user:1"))
        print("HDEL age:", client.hdel("user:1", "age"))  # 1

        # -- List commands --------------------------------------------------
        print("\n=== List Commands ===")
        client.delete("mylist")  # clean up first
        client.rpush("mylist", "a", "b", "c")
        client.lpush("mylist", "z")
        print("LRANGE:", client.lrange("mylist", 0, -1))  # [b'z', b'a', b'b', b'c']
        print("LLEN:", client.llen("mylist"))  # 4
        print("LINDEX 0:", client.lindex("mylist", 0))  # b'z'
        print("LINDEX -1:", client.lindex("mylist", -1))  # b'c'

        popped = client.lpop("mylist")
        print("LPOP:", popped)  # b'z'
        popped = client.rpop("mylist")
        print("RPOP:", popped)  # b'c'

        client.lset("mylist", 0, "A")
        print("After LSET:", client.lrange("mylist", 0, -1))  # [b'A', b'b']

        client.lpush("mylist", "b", "b", "c", "b")
        removed = client.lrem("mylist", 2, "b")  # remove first 2 "b"s from head
        print("LREM 2x 'b':", removed)
        print("After LREM:", client.lrange("mylist", 0, -1))

        client.ltrim("mylist", 0, 1)
        print("After LTRIM:", client.lrange("mylist", 0, -1))

        # -- Pipeline -------------------------------------------------------
        print("\n=== Pipeline ===")
        with client.pipeline() as pipe:
            pipe.set("pk1", "pv1")
            pipe.set("pk2", "pv2")
            pipe.get("pk1")
            pipe.get("pk2")
            pipe.incr("counter")
            results = pipe.execute()
            print("Pipeline results:", results)

        # Manual pipeline (without context manager)
        pipe = client.pipeline()
        pipe.ping()
        pipe.dbsize()
        results = pipe.execute()
        print("Manual pipeline:", results)

        # -- Error handling -------------------------------------------------
        print("\n=== Error Handling ===")
        try:
            from fastkv import FastKVResponseError
            # Try a command with wrong number of arguments
            client._execute_command("SET")  # type: ignore[arg-type]
        except Exception as e:
            print(f"Caught error (type={type(e).__name__}): {e}")

        # -- Cleanup --------------------------------------------------------
        print("\n=== Cleanup ===")
        for key in ["greeting", "temp", "counter", "msg", "ttl_demo",
                     "user:1", "mylist", "pk1", "pk2"]:
            client.delete(key)
        print("DBSIZE after cleanup:", client.dbsize())

    print("\nDone — connection closed automatically.")


if __name__ == "__main__":
    main()
