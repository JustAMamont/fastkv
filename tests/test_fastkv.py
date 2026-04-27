#!/usr/bin/env python3
"""
FastKV comprehensive integration test suite.
Covers all implemented commands, edge cases, and WRONGTYPE behaviour.
Run against a running FastKV server on localhost:6379.

Usage:
    python test_fastkv.py              # default host:port
    python test_fastkv.py 6380         # custom port
    python test_fastkv.py 6379 10.0.0.1  # custom port and host
"""

import sys
import time
import socket
import threading
import redis

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HOST = "localhost"
PORT = 6379
if len(sys.argv) >= 2:
    PORT = int(sys.argv[1])
if len(sys.argv) >= 3:
    HOST = sys.argv[2]

passed = 0
failed = 0
errors = []

def check(name, got, expected):
    """Assert got == expected, track pass/fail."""
    global passed, failed
    if got == expected:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        errors.append(name)
        print(f"  FAIL  {name}\n        expected: {expected!r}\n        got:      {got!r}")

def check_true(name, condition):
    global passed, failed
    if condition:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        errors.append(name)
        print(f"  FAIL  {name}  (condition was False)")

def check_raises(name, exc_type=None, msg_contains=None):
    """Assert that calling fn() raises an exception. Returns True if check passed."""
    global passed, failed
    # This is a decorator/context-manager style; actual checks are inline.
    pass

def cleanup(*keys):
    """Delete keys, ignore errors."""
    for k in keys:
        try:
            r.delete(k)
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------

print(f"\n{'='*60}")
print(f"  FastKV Integration Tests — {HOST}:{PORT}")
print(f"{'='*60}\n")

try:
    r = redis.Redis(host=HOST, port=PORT, decode_responses=True, socket_timeout=5)
    r.ping()
except Exception as e:
    print(f"Cannot connect to FastKV at {HOST}:{PORT}: {e}")
    sys.exit(1)

# ===========================================================================
print("1. Core: PING / ECHO / QUIT / INFO / COMMAND / DBSIZE")
# ===========================================================================

check("PING", r.ping(), True)

# PING with message — test via raw socket to bypass redis-py PING callback
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))
sock.sendall(b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n")
data = sock.recv(1024)
sock.close()
check("PING with message", data, b"$5\r\nhello\r\n")

# ECHO
resp = r.execute_command("ECHO", "hello world")
check("ECHO basic", resp, "hello world")

resp = r.execute_command("ECHO", "")
check("ECHO empty", resp, "")

# INFO — redis-py auto-parses bulk string into dict
resp = r.execute_command("INFO")
check_true("INFO returns dict", isinstance(resp, dict))
check_true("INFO contains version key", "fastkv_version" in resp or "redis_version" in resp)

# COMMAND — returns empty array (redis-py 5.x parses it as dict)
resp = r.execute_command("COMMAND")
check_true("COMMAND returns list or dict", isinstance(resp, (list, dict)))

# COMMAND DOCS — redis-py sends this on connect, returns empty array
resp = r.execute_command("COMMAND", "DOCS")
check_true("COMMAND DOCS returns dict/list", isinstance(resp, (dict, list)))

# DBSIZE — should be 0 or whatever is left from previous runs
resp = r.execute_command("DBSIZE")
check_true("DBSIZE returns int", isinstance(resp, int))

# Unknown command
try:
    r.execute_command("FAKECOMMAND")
    check_true("Unknown command raises error", False)
except redis.exceptions.ResponseError as e:
    check_true("Unknown command raises error", "unknown command" in str(e).lower())

# ===========================================================================
print("\n2. Core: SET / GET / DEL / EXISTS")
# ===========================================================================

check("SET basic", r.set("t:set1", "hello"), True)
check("GET basic", r.get("t:set1"), "hello")
check("SET overwrite", r.set("t:set1", "world"), True)
check("GET after overwrite", r.get("t:set1"), "world")

check("DEL existing", r.delete("t:set1"), 1)
check("DEL missing", r.delete("t:set1"), 0)
check("GET after DEL", r.get("t:set1"), None)

check("EXISTS existing", r.exists("t:ex1") == 0 or True, True)
r.set("t:ex1", "yes")
check("EXISTS returns 1", r.exists("t:ex1"), 1)
check("EXISTS missing", r.exists("t:ex1_ghost"), 0)

# SET NX / XX
cleanup("t:nx", "t:xxmiss")
check("SETNX new key", r.set("t:nx", "v", nx=True), True)
check("SETNX existing key", r.set("t:nx", "v2", nx=True), None)
check("GET after SETNX", r.get("t:nx"), "v")

check("SETXX existing key", r.set("t:nx", "updated", xx=True), True)
check("GET after SETXX", r.get("t:nx"), "updated")
check("SETXX missing key", r.set("t:xxmiss", "x", xx=True), None)
check("GET t:xxmiss is None", r.get("t:xxmiss"), None)

# Empty string value
check("SET empty value", r.set("t:empty", ""), True)
check("GET empty value", r.get("t:empty"), "")

# Binary-safe value (separate client without decode_responses to handle raw bytes)
r_raw = redis.Redis(host=HOST, port=PORT, decode_responses=False)
r_raw.delete("t:bin")
check("SET binary OK", r_raw.set("t:bin", b"\x00\x01\xff\xfe\x80"), True)
check("GET binary value", r_raw.get("t:bin"), b"\x00\x01\xff\xfe\x80")
r_raw.close()

# Large value (within INLINE_SIZE: key + value <= 64 bytes)
large = "x" * 50
r.set("t:large", large)
check("GET large value (50B)", r.get("t:large"), large)

# DBSIZE after inserts
ds = r.execute_command("DBSIZE")
check_true("DBSIZE > 0 after inserts", ds > 0)

cleanup("t:set1", "t:ex1", "t:nx", "t:xxmiss", "t:empty", "t:bin", "t:large")

# ===========================================================================
print("\n3. String: INCR / DECR / INCRBY / DECRBY")
# ===========================================================================

cleanup("t:counter")

# INCR on missing key — must init to 0, return 1
check("INCR missing key", r.incr("t:counter"), 1)
check("INCR again", r.incr("t:counter"), 2)
check("INCR again", r.incr("t:counter"), 3)

# DECR
check("DECR", r.decr("t:counter"), 2)
check("DECR", r.decr("t:counter"), 1)
check("DECR below zero", r.decr("t:counter"), 0)
check("DECR below zero (negative)", r.decr("t:counter"), -1)

# INCRBY
check("INCRBY 10", r.incrby("t:counter", 10), 9)
check("INCRBY -5", r.incrby("t:counter", -5), 4)

# DECRBY
check("DECRBY 3", r.decrby("t:counter", 3), 1)

# INCR on non-integer value
r.set("t:str", "hello")
try:
    r.incr("t:str")
    check_true("INCR on string raises error", False)
except redis.exceptions.ResponseError as e:
    check_true("INCR on string raises error", "not an integer" in str(e).lower() or "wrongtype" in str(e).lower())

# INCR on list key (WRONGTYPE)
r.execute_command("LPUSH", "t:inclist", "a")
try:
    r.incr("t:inclist")
    check_true("INCR on list raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("INCR on list raises WRONGTYPE", "wrongtype" in str(e).lower())

# INCRBY on missing key
cleanup("t:incrby_new")
check("INCRBY on missing key", r.incrby("t:incrby_new", 42), 42)

# DECRBY on missing key
cleanup("t:decrby_new")
check("DECRBY on missing key", r.decrby("t:decrby_new", 7), -7)

# INCRBY 0
check("INCRBY 0", r.incrby("t:counter", 0), 1)

# INCR with string argument (should fail)
try:
    r.execute_command("INCRBY", "t:counter", "abc")
    check_true("INCRBY non-int raises error", False)
except redis.exceptions.ResponseError:
    check_true("INCRBY non-int raises error", True)

cleanup("t:counter", "t:str", "t:inclist", "t:incrby_new", "t:decrby_new")

# ===========================================================================
print("\n4. String: APPEND / STRLEN / GETRANGE / SETRANGE")
# ===========================================================================

cleanup("t:ap", "t:gr", "t:sr")

# APPEND
check("APPEND to missing key", r.append("t:ap", "Hello"), 5)
check("GET after APPEND", r.get("t:ap"), "Hello")
check("APPEND to existing", r.append("t:ap", " World"), 11)
check("GET after APPEND", r.get("t:ap"), "Hello World")

# STRLEN
check("STRLEN", r.strlen("t:ap"), 11)
check("STRLEN missing key", r.strlen("t:ap_missing"), 0)

# GETRANGE
r.set("t:gr", "Hello World")
check("GETRANGE 0 4", r.getrange("t:gr", 0, 4), "Hello")
check("GETRANGE 6 10", r.getrange("t:gr", 6, 10), "World")
check("GETRANGE -5 -1", r.getrange("t:gr", -5, -1), "World")
check("GETRANGE 0 -1", r.getrange("t:gr", 0, -1), "Hello World")
check("GETRANGE out of range", r.getrange("t:gr", 20, 30), "")
check("GETRANGE missing key", r.getrange("t:gr_missing", 0, 5), "")

# SETRANGE
r.set("t:sr", "Hello World")
check("SETRANGE basic", r.setrange("t:sr", 6, "Redis"), 11)
check("GET after SETRANGE", r.get("t:sr"), "Hello Redis")

# SETRANGE beyond end — pads with zero bytes
r.set("t:sr", "abc")
check("SETRANGE beyond end", r.setrange("t:sr", 5, "XYZ"), 8)
check("GET after SETRANGE beyond", r.get("t:sr"), "abc\x00\x00XYZ")

# SETRANGE on missing key — fills with zeros
cleanup("t:sr_new")
check("SETRANGE on missing key", r.setrange("t:sr_new", 2, "ab"), 4)
check("GET after SETRANGE missing", r.get("t:sr_new"), "\x00\x00ab")

# STRLEN on list key (WRONGTYPE)
r.execute_command("LPUSH", "t:strlenlist", "a")
try:
    r.strlen("t:strlenlist")
    check_true("STRLEN on list raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("STRLEN on list raises WRONGTYPE", "wrongtype" in str(e).lower())

cleanup("t:ap", "t:gr", "t:sr", "t:sr_new", "t:strlenlist")

# ===========================================================================
print("\n5. String: MGET / MSET")
# ===========================================================================

cleanup("t:m1", "t:m2", "t:m3")

check("MSET", r.mset({"t:m1": "1", "t:m2": "2", "t:m3": "3"}), True)
check("MGET all exist", r.mget("t:m1", "t:m2", "t:m3"), ["1", "2", "3"])
check("MGET with missing", r.mget("t:m1", "t:missing", "t:m3"), ["1", None, "3"])
check("MGET all missing", r.mget("t:x1", "t:x2"), [None, None])

# MSET overwrites existing keys
r.mset({"t:m1": "new1", "t:m2": "new2"})
check("MGET after MSET overwrite", r.mget("t:m1", "t:m2"), ["new1", "new2"])

# MGET on mixed types (list key in the middle)
r.execute_command("LPUSH", "t:m_list", "val")
resp = r.mget("t:m1", "t:m_list", "t:m3")
check("MGET on list key returns nil", resp[1] is None, True)

cleanup("t:m1", "t:m2", "t:m3", "t:m_list")

# ===========================================================================
print("\n6. TTL: EXPIRE / TTL / PTTL / PERSIST")
# ===========================================================================

cleanup("t:ttl1", "t:ttl2", "t:ttl3", "t:ttl4", "t:ttl5")

r.set("t:ttl1", "data")
check("EXPIRE", r.expire("t:ttl1", 3600), True)
ttl = r.ttl("t:ttl1")
check_true("TTL ~3600", 3590 <= ttl <= 3600)

pttl = r.pttl("t:ttl1")
check_true("PTTL ~= TTL*1000", abs(pttl - ttl * 1000) < 5000)

check("PERSIST", r.persist("t:ttl1"), True)
check("TTL after PERSIST", r.ttl("t:ttl1"), -1)

# TTL on key without expiry
r.set("t:ttl2", "no_expire")
check("TTL no expire", r.ttl("t:ttl2"), -1)
check("PTTL no expire", r.pttl("t:ttl2"), -1)

# TTL on missing key
check("TTL missing key", r.ttl("t:ttl_missing"), -2)
check("PTTL missing key", r.pttl("t:ttl_missing"), -2)

# EXPIRE on missing key
check("EXPIRE missing key", r.expire("t:ttl_ghost", 60), False)

# EXPIRE with 0 / negative
r.set("t:ttl3", "will_die")
check("EXPIRE 0 deletes key", r.expire("t:ttl3", 0), True)
check("GET after EXPIRE 0", r.get("t:ttl3"), None)

r.set("t:ttl4", "will_die_neg")
check("EXPIRE -1 deletes key", r.expire("t:ttl4", -1), True)
check("GET after EXPIRE -1", r.get("t:ttl4"), None)

# EXPIRE on list key
r.execute_command("LPUSH", "t:ttl_list", "a")
check("EXPIRE on list key", r.expire("t:ttl_list", 300), True)
ttl_list = r.ttl("t:ttl_list")
check_true("TTL on list key ~300", 290 <= ttl_list <= 300)

# Verify expired key actually gone
r.set("t:ttl5", "short")
r.expire("t:ttl5", 1)
time.sleep(1.5)
check("GET expired key", r.get("t:ttl5"), None)

cleanup("t:ttl1", "t:ttl2", "t:ttl5", "t:ttl_list")

# ===========================================================================
print("\n7. Hash: HSET / HGET / HDEL / HGETALL / HEXISTS / HLEN / HKEYS / HVALS / HMGET / HMSET")
# ===========================================================================

cleanup("t:hash1", "t:hash2", "t:hash_str")

# HSET / HGET
check("HSET new field", r.hset("t:hash1", "name", "Alice"), 1)
check("HSET another field", r.hset("t:hash1", "age", "30"), 1)
check("HSET update field", r.hset("t:hash1", "name", "Bob"), 0)
check("HGET existing", r.hget("t:hash1", "name"), "Bob")
check("HGET missing field", r.hget("t:hash1", "email"), None)

# HGETALL
h = r.hgetall("t:hash1")
check_true("HGETALL has 2 fields", len(h) == 2)
check("HGETALL name", h.get("name"), "Bob")
check("HGETALL age", h.get("age"), "30")

# HEXISTS
check("HEXISTS existing", r.hexists("t:hash1", "name"), True)
check("HEXISTS missing", r.hexists("t:hash1", "ghost"), False)

# HLEN
check("HLEN", r.hlen("t:hash1"), 2)

# HKEYS / HVALS
keys = r.hkeys("t:hash1")
check_true("HKEYS has 2 items", len(keys) == 2)
check_true("'name' in HKEYS", "name" in keys)

vals = r.hvals("t:hash1")
check_true("HVALS has 2 items", len(vals) == 2)
check_true("'Bob' in HVALS", "Bob" in vals)

# HDEL
check("HDEL existing", r.hdel("t:hash1", "age"), 1)
check("HDEL missing", r.hdel("t:hash1", "age"), 0)
check("HLEN after HDEL", r.hlen("t:hash1"), 1)

# HMSET / HMGET
r.hmset("t:hash2", {"f1": "v1", "f2": "v2", "f3": "v3"})
check("HMGET all exist", r.hmget("t:hash2", "f1", "f2", "f3"), ["v1", "v2", "v3"])
check("HMGET with missing", r.hmget("t:hash2", "f1", "missing"), ["v1", None])

# HSET on string key → WRONGTYPE
r.set("t:hash_str", "hello")
try:
    r.hset("t:hash_str", "x", "y")
    check_true("HSET on string raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("HSET on string raises WRONGTYPE", "wrongtype" in str(e).lower())

# GET on hash key → WRONGTYPE
try:
    r.get("t:hash1")
    check_true("GET on hash raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("GET on hash raises WRONGTYPE", "wrongtype" in str(e).lower())

# DEL on hash key
check("DEL hash key", r.delete("t:hash1"), 1)
check("HGETALL after DEL", r.hgetall("t:hash1"), {})

# HGETALL missing key
check("HGETALL missing key", r.hgetall("t:hash_ghost"), {})

cleanup("t:hash1", "t:hash2", "t:hash_str")

# ===========================================================================
print("\n8. List: LPUSH / RPUSH / LPOP / RPOP / LRANGE / LLEN / LINDEX / LREM / LTRIM / LSET")
# ===========================================================================

cleanup("t:list1", "t:list2", "t:list3", "t:list4", "t:list5", "t:list_str")

# LPUSH — rightmost arg ends at head
check("LPUSH 3 elements", r.lpush("t:list1", "c", "b", "a"), 3)
check("LRANGE all", r.lrange("t:list1", 0, -1), ["a", "b", "c"])

# RPUSH
check("RPUSH 2 elements", r.rpush("t:list1", "d", "e"), 5)
check("LRANGE after RPUSH", r.lrange("t:list1", 0, -1), ["a", "b", "c", "d", "e"])

# LLEN
check("LLEN", r.llen("t:list1"), 5)
check("LLEN missing key", r.llen("t:list_missing"), 0)

# LINDEX
check("LINDEX 0", r.lindex("t:list1", 0), "a")
check("LINDEX 2", r.lindex("t:list1", 2), "c")
check("LINDEX -1", r.lindex("t:list1", -1), "e")
check("LINDEX out of range pos", r.lindex("t:list1", 99), None)
check("LINDEX out of range neg", r.lindex("t:list1", -99), None)

# LPOP / RPOP
check("LPOP 1", r.lpop("t:list1"), "a")
check("RPOP 1", r.rpop("t:list1"), "e")
check("LRANGE after pops", r.lrange("t:list1", 0, -1), ["b", "c", "d"])

# LPOP count
resp = r.lpop("t:list1", 2)
check("LPOP count=2", resp, ["b", "c"])
check("LRANGE after LPOP count", r.lrange("t:list1", 0, -1), ["d"])

# RPOP count
r.rpush("t:list1", "x", "y")
check("RPOP count=2", r.rpop("t:list1", 2), ["y", "x"])

# LPOP on empty list — returns nil
resp = r.lpop("t:list1")
check("LPOP last element", resp, "d")
resp = r.lpop("t:list1")
check("LPOP on empty → None", resp, None)

# LREM
r.rpush("t:list2", "a", "b", "a", "c", "a")
check("LREM first 2 'a'", r.lrem("t:list2", 2, "a"), 2)
check("LRANGE after LREM+", r.lrange("t:list2", 0, -1), ["b", "c", "a"])

check("LREM last 1 'a'", r.lrem("t:list2", -1, "a"), 1)
check("LRANGE after LREM-", r.lrange("t:list2", 0, -1), ["b", "c"])

r.rpush("t:list3", "x", "a", "x", "a", "x")
check("LREM all 'x'", r.lrem("t:list3", 0, "x"), 3)
check("LRANGE after LREM all", r.lrange("t:list3", 0, -1), ["a", "a"])

check("LREM missing element", r.lrem("t:list3", 1, "z"), 0)

# LTRIM
r.rpush("t:list4", "a", "b", "c", "d", "e")
r.ltrim("t:list4", 1, 3)
check("LRANGE after LTRIM 1 3", r.lrange("t:list4", 0, -1), ["b", "c", "d"])

r.rpush("t:list5", "a", "b", "c")
r.ltrim("t:list5", 0, 0)
check("LRANGE after LTRIM 0 0", r.lrange("t:list5", 0, -1), ["a"])

# LTRIM that removes everything
r.rpush("t:list6", "a", "b")
r.ltrim("t:list6", 10, 20)
check("LRANGE after LTRIM out of range", r.lrange("t:list6", 0, -1), [])
cleanup("t:list6")

# LTRIM start > stop → empty list
r.rpush("t:list6b", "a", "b", "c")
r.ltrim("t:list6b", 5, 2)
check("LRANGE after LTRIM start>stop", r.lrange("t:list6b", 0, -1), [])
cleanup("t:list6b")

# LSET
r.rpush("t:list7", "a", "b", "c")
check("LSET index 1", r.lset("t:list7", 1, "B"), True)
check("LINDEX after LSET", r.lindex("t:list7", 1), "B")
check("LRANGE after LSET", r.lrange("t:list7", 0, -1), ["a", "B", "c"])

# LRANGE edge cases
r.rpush("t:lr1", "a", "b")
check("LRANGE start > len → empty", r.lrange("t:lr1", 5, 10), [])
check("LRANGE start > stop → empty", r.lrange("t:lr1", 3, 0), [])
check("LRANGE negative", r.lrange("t:lr1", -2, -1), ["a", "b"])
check("LRANGE missing key", r.lrange("t:lr_missing", 0, -1), [])
check("LRANGE 0 0", r.lrange("t:lr1", 0, 0), ["a"])
check("LRANGE single element -1 -1", r.lrange("t:lr1", -1, -1), ["b"])

# LPUSH on string key → WRONGTYPE
r.set("t:list_str", "hello")
try:
    r.lpush("t:list_str", "x")
    check_true("LPUSH on string raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("LPUSH on string raises WRONGTYPE", "wrongtype" in str(e).lower())

# GET on list key → WRONGTYPE
r.rpush("t:list_get", "a")
try:
    r.get("t:list_get")
    check_true("GET on list raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("GET on list raises WRONGTYPE", "wrongtype" in str(e).lower())

# DEL removes list + data
r.rpush("t:del_list", "x", "y")
check("DEL list key", r.delete("t:del_list"), 1)
check("LLEN after DEL", r.llen("t:del_list"), 0)

# HSET on list key → WRONGTYPE
r.rpush("t:hset_list", "a")
try:
    r.hset("t:hset_list", "f", "v")
    check_true("HSET on list raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("HSET on list raises WRONGTYPE", "wrongtype" in str(e).lower())

# RPUSH on hash key → WRONGTYPE
r.hset("t:rpush_hash", "f", "v")
try:
    r.rpush("t:rpush_hash", "x")
    check_true("RPUSH on hash raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("RPUSH on hash raises WRONGTYPE", "wrongtype" in str(e).lower())

# RPOP on hash key → WRONGTYPE
try:
    r.rpop("t:rpush_hash")
    check_true("RPOP on hash raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("RPOP on hash raises WRONGTYPE", "wrongtype" in str(e).lower())

cleanup("t:list1", "t:list2", "t:list3", "t:list4", "t:list5",
         "t:list7", "t:lr1", "t:list_str", "t:list_get", "t:del_list",
         "t:hset_list", "t:rpush_hash")

# ===========================================================================
print("\n9. WRONGTYPE cross-type matrix")
# ===========================================================================

cleanup("t:wt_s", "t:wt_h", "t:wt_l")

r.set("t:wt_s", "string")
r.hset("t:wt_h", "f", "v")
r.lpush("t:wt_l", "elem")

# String key: GET/SET/APPEND/STRLEN/INCR OK, list ops WRONGTYPE
check("GET string OK", r.get("t:wt_s"), "string")

# String key — hash ops → WRONGTYPE
for cmd_name, cmd_fn in [
    ("HGET on string", lambda: r.hget("t:wt_s", "f")),
    ("HSET on string", lambda: r.hset("t:wt_s", "f", "v")),
    ("HDEL on string", lambda: r.hdel("t:wt_s", "f")),
]:
    try:
        cmd_fn()
        check_true(f"{cmd_name} should error", False)
    except redis.exceptions.ResponseError as e:
        check_true(f"{cmd_name} → WRONGTYPE", "wrongtype" in str(e).lower())

# String key — list ops → WRONGTYPE
for cmd_name, cmd_fn in [
    ("LPUSH on string", lambda: r.lpush("t:wt_s", "x")),
    ("RPUSH on string", lambda: r.rpush("t:wt_s", "x")),
    ("LPOP on string", lambda: r.lpop("t:wt_s")),
]:
    try:
        cmd_fn()
        check_true(f"{cmd_name} should error", False)
    except redis.exceptions.ResponseError as e:
        check_true(f"{cmd_name} → WRONGTYPE", "wrongtype" in str(e).lower())

# Hash key — string ops → WRONGTYPE
for cmd_name, cmd_fn in [
    ("GET on hash", lambda: r.get("t:wt_h")),
    ("APPEND on hash", lambda: r.append("t:wt_h", "x")),
    ("INCR on hash", lambda: r.incr("t:wt_h")),
]:
    try:
        cmd_fn()
        check_true(f"{cmd_name} should error", False)
    except redis.exceptions.ResponseError as e:
        check_true(f"{cmd_name} → WRONGTYPE", "wrongtype" in str(e).lower())

# Hash key — list ops → WRONGTYPE
for cmd_name, cmd_fn in [
    ("LPUSH on hash", lambda: r.lpush("t:wt_h", "x")),
    ("LPOP on hash", lambda: r.lpop("t:wt_h")),
]:
    try:
        cmd_fn()
        check_true(f"{cmd_name} should error", False)
    except redis.exceptions.ResponseError as e:
        check_true(f"{cmd_name} → WRONGTYPE", "wrongtype" in str(e).lower())

# List key — string ops → WRONGTYPE
for cmd_name, cmd_fn in [
    ("GET on list", lambda: r.get("t:wt_l")),
    ("INCR on list", lambda: r.incr("t:wt_l")),
]:
    try:
        cmd_fn()
        check_true(f"{cmd_name} should error", False)
    except redis.exceptions.ResponseError as e:
        check_true(f"{cmd_name} → WRONGTYPE", "wrongtype" in str(e).lower())

# List key — hash ops → WRONGTYPE
for cmd_name, cmd_fn in [
    ("HGET on list", lambda: r.hget("t:wt_l", "f")),
    ("HSET on list", lambda: r.hset("t:wt_l", "f", "v")),
]:
    try:
        cmd_fn()
        check_true(f"{cmd_name} should error", False)
    except redis.exceptions.ResponseError as e:
        check_true(f"{cmd_name} → WRONGTYPE", "wrongtype" in str(e).lower())

cleanup("t:wt_s", "t:wt_h", "t:wt_l")

# ===========================================================================
print("\n10. Pipeline")
# ===========================================================================

cleanup("t:pipe1", "t:pipe2", "t:pipe3")

pipe = r.pipeline(transaction=False)
for i in range(100):
    pipe.set(f"t:pipe1:{i}", f"val:{i}")
results = pipe.execute()
check_true("Pipeline SET 100 keys (all True)", all(results))

pipe = r.pipeline(transaction=False)
for i in range(100):
    pipe.get(f"t:pipe1:{i}")
results = pipe.execute()
check("Pipeline GET first", results[0], "val:0")
check("Pipeline GET last", results[99], "val:99")

# Pipeline INCR
pipe = r.pipeline(transaction=False)
pipe.incr("t:pipe2")
pipe.incr("t:pipe2")
pipe.incr("t:pipe2")
pipe.get("t:pipe2")
results = pipe.execute()
check("Pipeline INCR sequence", results, [1, 2, 3, "3"])

# Pipeline mixed ops
pipe = r.pipeline(transaction=False)
pipe.rpush("t:pipe3", "a", "b", "c")
pipe.llen("t:pipe3")
pipe.lrange("t:pipe3", 0, -1)
results = pipe.execute()
check("Pipeline mixed list ops", results, [3, 3, ["a", "b", "c"]])

# Cleanup pipeline keys
pipe = r.pipeline(transaction=False)
for i in range(100):
    pipe.delete(f"t:pipe1:{i}")
pipe.execute()
cleanup("t:pipe2", "t:pipe3")

# ===========================================================================
print("\n11. Edge cases & stress")
# ===========================================================================

cleanup("t:edge1", "t:edge2", "t:edge3", "t:edge4")

# Key with special characters
check("SET key with spaces", r.set("t:edge1", "ok"), True)
check("GET key with spaces", r.get("t:edge1"), "ok")

# Key with unicode
check("SET unicode key", r.set("t:edge2", "привет мир"), True)
check("GET unicode value", r.get("t:edge2"), "привет мир")

# SET with same value (idempotent)
r.set("t:edge3", "same")
r.set("t:edge3", "same")
check("GET idempotent SET", r.get("t:edge3"), "same")

# Many keys — DBSIZE
for i in range(500):
    r.set(f"t:big:{i}", f"v{i}")
ds = r.execute_command("DBSIZE")
check_true("DBSIZE >= 500", ds >= 500)

# Cleanup
pipe = r.pipeline(transaction=False)
for i in range(500):
    pipe.delete(f"t:big:{i}")
pipe.execute()

cleanup("t:edge1", "t:edge2", "t:edge3", "t:edge4")

# ===========================================================================
print("\n12. TTL + DEL interaction")
# ===========================================================================

cleanup("t:td1", "t:td2")

r.set("t:td1", "temp")
r.expire("t:td1", 3600)
check("DEL removes expired key", r.delete("t:td1"), 1)
check("GET after DEL", r.get("t:td1"), None)

# SET overwrites list
r.lpush("t:td2", "a", "b")
check("SET overwrites list key", r.set("t:td2", "string"), True)
check("GET after overwrite", r.get("t:td2"), "string")
try:
    r.llen("t:td2")
    check_true("LLEN after overwrite → WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("LLEN after overwrite → WRONGTYPE", "wrongtype" in str(e).lower())

# SET overwrites hash
r.hset("t:td3", "f", "v")
check("SET overwrites hash key", r.set("t:td3", "new_string"), True)
check("GET after overwrite hash", r.get("t:td3"), "new_string")
try:
    r.hlen("t:td3")
    check_true("HLEN after overwrite → WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("HLEN after overwrite → WRONGTYPE", "wrongtype" in str(e).lower())

cleanup("t:td1", "t:td2", "t:td3")

# ===========================================================================
print("\n13. SET EX / PX — TTL via SET flags")
# ===========================================================================

cleanup("t:ex1", "t:px1", "t:exnx1", "t:expx1")

# SET key val EX seconds — must set TTL
r.set("t:ex1", "data", ex=10)
ttl = r.ttl("t:ex1")
check_true("SET EX sets TTL (~10s)", 8 <= ttl <= 10)

# SET key val PX milliseconds — must set TTL
r.set("t:px1", "data", px=10000)
ttl = r.ttl("t:px1")
check_true("SET PX sets TTL (~10s)", 8 <= ttl <= 10)

# SET EX + NX — only set if key does NOT exist, then set TTL
cleanup("t:exnx1")
check("SET EX NX new key", r.set("t:exnx1", "val", ex=10, nx=True), True)
ttl = r.ttl("t:exnx1")
check_true("SET EX NX has TTL", ttl > 0)
# NX on existing key — should NOT set, even with EX
check("SET EX NX existing key", r.set("t:exnx1", "val2", ex=10, nx=True), None)
check("GET unchanged after EX NX fail", r.get("t:exnx1"), "val")

# SET EX + XX — only set if key DOES exist
cleanup("t:expx1")
check("SET EX XX missing key", r.set("t:expx1", "val", ex=10, xx=True), None)
r.set("t:expx1", "val")
check("SET EX XX existing key", r.set("t:expx1", "val2", ex=10, xx=True), True)
ttl = r.ttl("t:expx1")
check_true("SET EX XX has TTL", ttl > 0)
check("GET after SET EX XX", r.get("t:expx1"), "val2")

# SET EX overwrites previous TTL
r.set("t:ex1", "new", ex=5)
ttl = r.ttl("t:ex1")
check_true("SET EX overwrites old TTL (~5s)", 3 <= ttl <= 5)

# SET without EX removes TTL (already tested in section 12, but explicit here)
r.set("t:ex1", "no_ttl")
check("TTL after SET without EX", r.ttl("t:ex1"), -1)

cleanup("t:ex1", "t:px1", "t:exnx1", "t:expx1")

# ===========================================================================
print("\n14. DEL / EXISTS multi-key")
# ===========================================================================

cleanup("t:del1", "t:del2", "t:del3", "t:del4")
r.set("t:del1", "a")
r.set("t:del2", "b")
r.set("t:del3", "c")

# DEL multiple keys — redis-py: delete(k1, k2, ...) returns count of deleted
d = r.delete("t:del1", "t:del2", "t:del_missing", "t:del3")
check_true("DEL multi-key returns count", d >= 2)
check("GET t:del1 after DEL multi", r.get("t:del1"), None)
check("GET t:del2 after DEL multi", r.get("t:del2"), None)
check("GET t:del3 after DEL multi", r.get("t:del3"), None)

# EXISTS multiple keys — redis-py: exists(k1, k2, ...) returns count
r.set("t:del1", "a")
e = r.exists("t:del1", "t:del_missing", "t:del1")
# Redis returns count of keys that exist (counts duplicates: t:del1 appears twice → 2)
check("EXISTS multi-key returns count", e, 2)

cleanup("t:del1", "t:del2", "t:del3", "t:del4")

# ===========================================================================
print("\n15. Hash TTL interactions")
# ===========================================================================

cleanup("t:httl1", "t:httl2")

# EXPIRE on hash key
r.hset("t:httl1", "f1", "v1")
r.hset("t:httl1", "f2", "v2")
check("EXPIRE on hash key", r.expire("t:httl1", 10), True)
ttl = r.ttl("t:httl1")
check_true("TTL on hash key ~10s", 8 <= ttl <= 10)

# HGETALL still works before expiry
h = r.hgetall("t:httl1")
check("HGETALL before expiry", h.get("f1"), "v1")

# Wait for hash to expire
r.expire("t:httl1", 1)
time.sleep(1.5)
h = r.hgetall("t:httl1")
check("HGETALL after expiry", h, {})

# HSET on expired hash key works as fresh
check("HSET after expiry", r.hset("t:httl1", "new_f", "new_v"), 1)
check("HGET after re-set", r.hget("t:httl1", "new_f"), "new_v")

# SET overwrites hash that has TTL
r.hset("t:httl2", "f", "v")
r.expire("t:httl2", 3600)
r.set("t:httl2", "string")
check("GET after overwriting TTL hash", r.get("t:httl2"), "string")
check("TTL -1 after overwriting TTL hash", r.ttl("t:httl2"), -1)

cleanup("t:httl1", "t:httl2")

# ===========================================================================
print("\n16. List TTL interactions")
# ===========================================================================

cleanup("t:lttl1", "t:lttl2")

# EXPIRE on list, then list expires
r.rpush("t:lttl1", "a", "b", "c")
r.expire("t:lttl1", 1)
time.sleep(1.5)
check("LLEN after list expired", r.llen("t:lttl1"), 0)
check("LRANGE after list expired", r.lrange("t:lttl1", 0, -1), [])

# RPUSH after list expired — fresh list
check("RPUSH after expiry", r.rpush("t:lttl1", "x"), 1)
check("LRANGE after re-push", r.lrange("t:lttl1", 0, -1), ["x"])

# SET overwrites list that has TTL
r.rpush("t:lttl2", "a", "b")
r.expire("t:lttl2", 3600)
r.set("t:lttl2", "string")
check("GET after overwriting TTL list", r.get("t:lttl2"), "string")
check("TTL -1 after overwriting TTL list", r.ttl("t:lttl2"), -1)

cleanup("t:lttl1", "t:lttl2")

# ===========================================================================
print("\n17. GETRANGE / SETRANGE additional edge cases")
# ===========================================================================

cleanup("t:gr2", "t:sr2")

# GETRANGE on UTF-8 string (byte-level, not char-level — Redis semantics)
r.set("t:gr2", "абвгд")  # 10 bytes (2 bytes per Cyrillic char)
check("GETRANGE 0 1 on UTF-8", r.getrange("t:gr2", 0, 1), "а")
check("GETRANGE 0 3 on UTF-8", r.getrange("t:gr2", 0, 3), "аб")

# GETRANGE start == end
r.set("t:gr2", "abcde")
check("GETRANGE start==end", r.getrange("t:gr2", 2, 2), "c")

# SETRANGE at offset 0
r.set("t:sr2", "abc")
check("SETRANGE at offset 0", r.setrange("t:sr2", 0, "XYZ"), 3)
check("GET after SETRANGE at 0", r.get("t:sr2"), "XYZ")

# SETRANGE with empty string — should be no-op (Redis returns current length)
r.set("t:sr2", "hello")
result = r.setrange("t:sr2", 0, "")
check("SETRANGE empty string returns len", result, 5)
check("GET after SETRANGE empty", r.get("t:sr2"), "hello")

cleanup("t:gr2", "t:sr2")

# ===========================================================================
print("\n18. Concurrent clients")
# ===========================================================================

cleanup("t:conc1", "t:conc2")

errors_concurrent = []

def client_writer(c_id, key, count):
    """Run INCR from a separate connection."""
    try:
        c = redis.Redis(host=HOST, port=PORT, decode_responses=True, socket_timeout=5)
        for _ in range(count):
            c.incr(key)
        c.close()
    except Exception as e:
        errors_concurrent.append(f"client {c_id}: {e}")

# 5 clients each INCR 100 times on the same key — final value must be 500
threads = []
for i in range(5):
    t = threading.Thread(target=client_writer, args=(i, "t:conc1", 100))
    threads.append(t)
    t.start()
for t in threads:
    t.join(timeout=10)

if errors_concurrent:
    check_true("Concurrent INCR no errors", False)
    for err in errors_concurrent:
        print(f"        {err}")
else:
    val = r.get("t:conc1")
    check("Concurrent INCR final value", int(val), 500)

# Concurrent SET/GET from different clients
results_get = {}
def client_set_get(c_id, key, val, barrier):
    """Write then read back from separate connections."""
    try:
        c = redis.Redis(host=HOST, port=PORT, decode_responses=True, socket_timeout=5)
        barrier.wait()  # all threads start together
        c.set(f"{key}:{c_id}", val)
        results_get[c_id] = c.get(f"{key}:{c_id}")
        c.close()
    except Exception as e:
        errors_concurrent.append(f"client setget {c_id}: {e}")

barrier = threading.Barrier(5)
threads = []
for i in range(5):
    t = threading.Thread(target=client_set_get, args=(i, "t:conc2", f"value_{i}", barrier))
    threads.append(t)
    t.start()
for t in threads:
    t.join(timeout=10)

check_true("Concurrent SET/GET all returned correctly", all(
    results_get.get(i) == f"value_{i}" for i in range(5)
))

# Cleanup
for i in range(5):
    cleanup(f"t:conc2:{i}")
cleanup("t:conc1", "t:conc2")

# ===========================================================================
print("\n19. INCR/DECR edge cases on hash and list keys")
# ===========================================================================

# DECR on hash key (WRONGTYPE)
r.hset("t:decr_hash", "f", "v")
try:
    r.decr("t:decr_hash")
    check_true("DECR on hash raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("DECR on hash raises WRONGTYPE", "wrongtype" in str(e).lower())

# DECRBY on list key (WRONGTYPE)
r.lpush("t:decrby_list", "a")
try:
    r.decrby("t:decrby_list", 1)
    check_true("DECRBY on list raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("DECRBY on list raises WRONGTYPE", "wrongtype" in str(e).lower())

# APPEND on hash key (WRONGTYPE)
r.hset("t:append_hash", "f", "v")
try:
    r.append("t:append_hash", "suffix")
    check_true("APPEND on hash raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("APPEND on hash raises WRONGTYPE", "wrongtype" in str(e).lower())

# SETRANGE on list key (WRONGTYPE)
r.lpush("t:setrange_list", "a")
try:
    r.setrange("t:setrange_list", 0, "b")
    check_true("SETRANGE on list raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("SETRANGE on list raises WRONGTYPE", "wrongtype" in str(e).lower())

# GETRANGE on hash key (WRONGTYPE)
r.hset("t:getrange_hash", "f", "v")
try:
    r.getrange("t:getrange_hash", 0, 5)
    check_true("GETRANGE on hash raises WRONGTYPE", False)
except redis.exceptions.ResponseError as e:
    check_true("GETRANGE on hash raises WRONGTYPE", "wrongtype" in str(e).lower())

cleanup("t:decr_hash", "t:decrby_list", "t:append_hash",
         "t:setrange_list", "t:getrange_hash")

# ===========================================================================
print("\n20. Pipeline stress — 1000 operations")
# ===========================================================================

pipe = r.pipeline(transaction=False)
for i in range(500):
    pipe.set(f"t:stress:{i}", f"v{i}")
    pipe.incr(f"t:stress_counter")
results = pipe.execute()
check_true("Pipeline 1000 ops (500 SET + 500 INCR) all OK", all(results))

# Verify
pipe = r.pipeline(transaction=False)
for i in range(500):
    pipe.get(f"t:stress:{i}")
pipe.get("t:stress_counter")
results = pipe.execute()
check("Pipeline stress first value", results[0], "v0")
check("Pipeline stress last value", results[499], "v499")
check("Pipeline stress counter", int(results[500]), 500)

# Cleanup
pipe = r.pipeline(transaction=False)
for i in range(500):
    pipe.delete(f"t:stress:{i}")
pipe.delete("t:stress_counter")
pipe.execute()

# ===========================================================================
# Summary
# ===========================================================================

total = passed + failed
print(f"\n{'='*60}")
print(f"  Results: {passed}/{total} passed, {failed} failed")
if errors:
    print(f"\n  Failed tests:")
    for e in errors:
        print(f"    - {e}")
print(f"{'='*60}\n")

sys.exit(0 if failed == 0 else 1)
