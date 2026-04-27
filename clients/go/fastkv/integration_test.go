package fastkv_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/fastkv/fastkv"
)

// Integration tests that run against a live FastKV server.
// Set FASTKV_HOST and FASTKV_PORT to override the defaults.
func addr() string {
	host := os.Getenv("FASTKV_HOST")
	port := os.Getenv("FASTKV_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "6379"
	}
	return host + ":" + port
}

func client(t *testing.T) *fastkv.Client {
	t.Helper()
	c := fastkv.NewClient(addr())
	t.Cleanup(func() { c.Close() })
	return c
}

// ========== Core ==========

func TestPing(t *testing.T) {
	c := client(t)
	s, err := c.Ping()
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}
	if s != "PONG" {
		t.Fatalf("expected PONG, got %q", s)
	}
}

func TestEcho(t *testing.T) {
	c := client(t)
	s, err := c.Echo("hello fastkv")
	if err != nil {
		t.Fatalf("ECHO failed: %v", err)
	}
	if s != "hello fastkv" {
		t.Fatalf("expected %q, got %q", "hello fastkv", s)
	}
}

func TestDbsize(t *testing.T) {
	c := client(t)
	c.Del("it:db:1", "it:db:2")
	c.Set("it:db:1", "v")
	c.Set("it:db:2", "v")
	defer c.Del("it:db:1", "it:db:2")

	n, err := c.Dbsize()
	if err != nil {
		t.Fatalf("DBSIZE failed: %v", err)
	}
	if n < 2 {
		t.Fatalf("DBSIZE expected >= 2, got %d", n)
	}
}

// ========== String commands ==========

func TestSetGet(t *testing.T) {
	c := client(t)
	defer c.Del("it:setget")

	if err := c.Set("it:setget", "world"); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	val, err := c.Get("it:setget")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if val != "world" {
		t.Fatalf("expected %q, got %q", "world", val)
	}
}

func TestGetMissing(t *testing.T) {
	c := client(t)
	_, err := c.Get("it:nonexistent_key_xyz")
	if err != fastkv.ErrNil {
		t.Fatalf("expected ErrNil, got %v", err)
	}
}

func TestSetNX(t *testing.T) {
	c := client(t)
	defer c.Del("it:setnx")

	_ = c.Set("it:setnx", "original")
	ok, err := c.SetNX("it:setnx", "other")
	if err != nil {
		t.Fatalf("SETNX failed: %v", err)
	}
	if ok {
		t.Fatal("SETNX should return false for existing key")
	}
	c.Del("it:setnx")
	ok, err = c.SetNX("it:setnx", "new")
	if err != nil {
		t.Fatalf("SETNX failed: %v", err)
	}
	if !ok {
		t.Fatal("SETNX should return true for missing key")
	}
}

func TestSetXX(t *testing.T) {
	c := client(t)
	defer c.Del("it:setxx")

	ok, _ := c.SetXX("it:setxx", "nope")
	if ok {
		t.Fatal("SETXX on missing key should return false")
	}
	_ = c.Set("it:setxx", "original")
	ok, err := c.SetXX("it:setxx", "updated")
	if err != nil {
		t.Fatalf("SETXX failed: %v", err)
	}
	if !ok {
		t.Fatal("SETXX on existing key should return true")
	}
	val, _ := c.Get("it:setxx")
	if val != "updated" {
		t.Fatalf("expected %q, got %q", "updated", val)
	}
}

func TestDelMultiKey(t *testing.T) {
	c := client(t)
	_ = c.Set("it:del1", "a")
	_ = c.Set("it:del2", "b")
	_ = c.Set("it:del3", "c")

	n, err := c.Del("it:del1", "it:del2", "it:del3")
	if err != nil {
		t.Fatalf("DEL failed: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected 3 deleted, got %d", n)
	}
}

func TestIncrDecr(t *testing.T) {
	c := client(t)
	defer c.Del("it:incr")

	// INCR on missing key initializes to 0
	v, err := c.Incr("it:incr")
	if err != nil || v != 1 {
		t.Fatalf("INCR missing: expected 1, got %d (err=%v)", v, err)
	}
	v, _ = c.IncrBy("it:incr", 9)
	if v != 10 {
		t.Fatalf("INCRBY 9: expected 10, got %d", v)
	}
	v, _ = c.Decr("it:incr")
	if v != 9 {
		t.Fatalf("DECR: expected 9, got %d", v)
	}
	v, _ = c.DecrBy("it:incr", 4)
	if v != 5 {
		t.Fatalf("DECRBY 4: expected 5, got %d", v)
	}
}

func TestMSetMGet(t *testing.T) {
	c := client(t)
	defer c.Del("it:m1", "it:m2", "it:m3")

	_ = c.MSet(map[string]string{"it:m1": "a", "it:m2": "b", "it:m3": "c"})
	vals, err := c.MGet("it:m1", "it:m2", "it:m3", "it:missing")
	if err != nil {
		t.Fatalf("MGET failed: %v", err)
	}
	if len(vals) != 4 {
		t.Fatalf("expected 4 values, got %d", len(vals))
	}
	if vals[0] != "a" || vals[1] != "b" || vals[2] != "c" {
		t.Fatalf("expected [a b c ...], got %v", vals)
	}
}

func TestAppendStrlen(t *testing.T) {
	c := client(t)
	defer c.Del("it:append")

	_ = c.Set("it:append", "hello")
	n, _ := c.Append("it:append", " world")
	if n != 11 {
		t.Fatalf("APPEND: expected len 11, got %d", n)
	}
	n, _ = c.Strlen("it:append")
	if n != 11 {
		t.Fatalf("STRLEN: expected 11, got %d", n)
	}
}

func TestGetRangeSetRange(t *testing.T) {
	c := client(t)
	defer c.Del("it:range")

	_ = c.Set("it:range", "Hello, World!")
	s, _ := c.GetRange("it:range", 0, 4)
	if s != "Hello" {
		t.Fatalf("GETRANGE: expected %q, got %q", "Hello", s)
	}
	n, _ := c.SetRange("it:range", 7, "FastKV")
	if n != 13 {
		t.Fatalf("SETRANGE: expected len 13, got %d", n)
	}
	val, _ := c.Get("it:range")
	if val != "Hello, FastKV" {
		t.Fatalf("after SETRANGE: expected %q, got %q", "Hello, FastKV", val)
	}
}

// ========== TTL commands ==========

func TestExpireTtlPersist(t *testing.T) {
	c := client(t)
	defer c.Del("it:ttl")

	_ = c.Set("it:ttl", "data")
	ttl, _ := c.Ttl("it:ttl")
	if ttl != -1 {
		t.Fatalf("TTL before EXPIRE: expected -1, got %d", ttl)
	}
	ok, _ := c.Expire("it:ttl", 60)
	if !ok {
		t.Fatal("EXPIRE should return true")
	}
	ttl, _ = c.Ttl("it:ttl")
	if ttl <= 0 || ttl > 60 {
		t.Fatalf("TTL after EXPIRE: expected 1..60, got %d", ttl)
	}
	ok, _ = c.Persist("it:ttl")
	if !ok {
		t.Fatal("PERSIST should return true")
	}
	ttl, _ = c.Ttl("it:ttl")
	if ttl != -1 {
		t.Fatalf("TTL after PERSIST: expected -1, got %d", ttl)
	}
}

func TestExpireZero(t *testing.T) {
	c := client(t)
	defer c.Del("it:exp0")

	_ = c.Set("it:exp0", "temp")
	c.Expire("it:exp0", 0)
	_, err := c.Get("it:exp0")
	if err != fastkv.ErrNil {
		t.Fatal("key should be gone after EXPIRE 0")
	}
}

// ========== Hash commands ==========

func TestHashCRUD(t *testing.T) {
	c := client(t)
	defer c.Del("it:hash")

	n, _ := c.HSet("it:hash", "name", "Alice")
	if n != 1 {
		t.Fatalf("HSET new field: expected 1, got %d", n)
	}
	n, _ = c.HSet("it:hash", "name", "Bob")
	if n != 0 {
		t.Fatalf("HSET existing field: expected 0, got %d", n)
	}
	val, _ := c.HGet("it:hash", "name")
	if val != "Bob" {
		t.Fatalf("HGET: expected %q, got %q", "Bob", val)
	}
	ok, _ := c.HExists("it:hash", "name")
	if !ok {
		t.Fatal("HEXISTS should return true")
	}
	hlen, _ := c.HLen("it:hash")
	if hlen != 1 {
		t.Fatalf("HLEN: expected 1, got %d", hlen)
	}
}

func TestHMSetHGetAll(t *testing.T) {
	c := client(t)
	defer c.Del("it:hm")

	_ = c.HMSet("it:hm", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})
	m, err := c.HGetAll("it:hm")
	if err != nil {
		t.Fatalf("HGETALL failed: %v", err)
	}
	if len(m) != 3 {
		t.Fatalf("HGETALL: expected 3 fields, got %d", len(m))
	}
	if m["f1"] != "v1" || m["f2"] != "v2" || m["f3"] != "v3" {
		t.Fatalf("HGETALL values wrong: %v", m)
	}
	vals, _ := c.HMGet("it:hm", "f1", "f3")
	if vals[0] != "v1" || vals[1] != "v3" {
		t.Fatalf("HMGET: expected [v1 v3], got %v", vals)
	}
}

func TestHDelHKeysHVals(t *testing.T) {
	c := client(t)
	defer c.Del("it:hd")

	_ = c.HMSet("it:hd", map[string]string{"a": "1", "b": "2", "c": "3"})
	n, _ := c.HDel("it:hd", "b")
	if n != 1 {
		t.Fatalf("HDEL: expected 1, got %d", n)
	}
	keys, _ := c.HKeys("it:hd")
	if len(keys) != 2 {
		t.Fatalf("HKEYS: expected 2, got %d", len(keys))
	}
	vals, _ := c.HVals("it:hd")
	if len(vals) != 2 {
		t.Fatalf("HVALS: expected 2, got %d", len(vals))
	}
}

// ========== List commands ==========

func TestListPushPopRange(t *testing.T) {
	c := client(t)
	defer c.Del("it:list")

	n, _ := c.RPush("it:list", "a", "b", "c")
	if n != 3 {
		t.Fatalf("RPUSH: expected 3, got %d", n)
	}
	_, _ = c.LPush("it:list", "z")
	items, _ := c.LRange("it:list", 0, -1)
	if len(items) != 4 || items[0] != "z" || items[3] != "c" {
		t.Fatalf("LRANGE: expected [z a b c], got %v", items)
	}
	head, _ := c.LPop("it:list")
	if head != "z" {
		t.Fatalf("LPOP: expected %q, got %q", "z", head)
	}
	tail, _ := c.RPop("it:list")
	if tail != "c" {
		t.Fatalf("RPOP: expected %q, got %q", "c", tail)
	}
}

func TestListLTrimLSetLRem(t *testing.T) {
	c := client(t)
	defer c.Del("it:lt")

	_, _ = c.RPush("it:lt", "a", "b", "c", "d", "e")
	_ = c.LTrim("it:lt", 1, 3)
	items, _ := c.LRange("it:lt", 0, -1)
	if len(items) != 3 {
		t.Fatalf("LTRIM: expected 3 items, got %d", len(items))
	}
	_ = c.LSet("it:lt", 0, "X")
	val, _ := c.LIndex("it:lt", 0)
	if val != "X" {
		t.Fatalf("LSET+LINDEX: expected %q, got %q", "X", val)
	}
	_, _ = c.RPush("it:lt", "b", "b")
	n, _ := c.LRem("it:lt", 2, "b")
	if n != 2 {
		t.Fatalf("LREM 2: expected 2, got %d", n)
	}
}

func TestListLLenLIndex(t *testing.T) {
	c := client(t)
	defer c.Del("it:llen")

	_, _ = c.RPush("it:llen", "x", "y")
	n, _ := c.LLen("it:llen")
	if n != 2 {
		t.Fatalf("LLEN: expected 2, got %d", n)
	}
	last, _ := c.LIndex("it:llen", -1)
	if last != "y" {
		t.Fatalf("LINDEX -1: expected %q, got %q", "y", last)
	}
}

// ========== Pipeline ==========

func TestPipeline(t *testing.T) {
	c := client(t)
	defer c.Del("it:pipe1", "it:pipe2", "it:pipe3")

	p := c.Pipeline()
	p.Set("it:pipe1", "10")
	p.Set("it:pipe2", "20")
	p.Incr("it:pipe1")
	p.Get("it:pipe1")
	p.Get("it:pipe2")
	p.MGet("it:pipe1", "it:pipe2", "it:pipe3")

	res, err := p.Execute()
	if err != nil {
		t.Fatalf("Pipeline Execute failed: %v", err)
	}
	if res.Len() != 6 {
		t.Fatalf("expected 6 replies, got %d", res.Len())
	}
	s, _ := res.String(3)
	if s != "11" {
		t.Fatalf("pipeline GET: expected %q, got %q", "11", s)
	}
	s, _ = res.String(4)
	if s != "20" {
		t.Fatalf("pipeline GET2: expected %q, got %q", "20", s)
	}
}

func TestPipelineHash(t *testing.T) {
	c := client(t)
	defer c.Del("it:phash")

	p := c.Pipeline()
	p.HSet("it:phash", "a", "1")
	p.HSet("it:phash", "b", "2")
	p.HGetAll("it:phash")
	p.HLen("it:phash")

	res, _ := p.Execute()
	m, _ := res.StringMap(2)
	if m["a"] != "1" || m["b"] != "2" {
		t.Fatalf("pipeline HGETALL: %v", m)
	}
	n, _ := res.Int(3)
	if n != 2 {
		t.Fatalf("pipeline HLEN: expected 2, got %d", n)
	}
}

// ========== WRONGTYPE ==========

func TestWrongType(t *testing.T) {
	c := client(t)
	defer c.Del("it:wt")

	_ = c.Set("it:wt", "string")
	_, err := c.LLen("it:wt")
	if err == nil {
		t.Fatal("expected error for LLEN on string key")
	}
	var respErr *fastkv.RespError
	if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("expected WRONGTYPE error, got: %v", err)
	}
	_ = respErr // used for type check import
}

// ========== Exists ==========

func TestExists(t *testing.T) {
	c := client(t)
	defer c.Del("it:ex1", "it:ex2")

	_ = c.Set("it:ex1", "v")
	n, _ := c.Exists("it:ex1", "it:ex2")
	if n != 1 {
		t.Fatalf("EXISTS: expected 1, got %d", n)
	}
}

// ========== Timeout / Reconnect ==========

func TestClientTimeoutConfig(t *testing.T) {
	c := fastkv.NewClient(addr())
	c.DialTimeout = 1  // 1 nanosecond — should fail if server unreachable
	c.ReadTimeout = 1

	// Just verify the fields are set; real timeout test needs an unreachable server
	if c.DialTimeout != 1 {
		t.Fatal("DialTimeout not set")
	}
	c.Close()
}

// ========== Concurrent access ==========

func TestConcurrent(t *testing.T) {
	c := client(t)

	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := fmt.Sprintf("it:conc:%d", id)
			err := c.Set(key, "val")
			if err != nil {
				done <- err
				return
			}
			_, err = c.Get(key)
			done <- err
		}(i)
	}

	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Fatalf("goroutine %d failed: %v", i, err)
		}
	}
	// cleanup
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("it:conc:%d", i)
	}
	c.Del(keys...)
}
