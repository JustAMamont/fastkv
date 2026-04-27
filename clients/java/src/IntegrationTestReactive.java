package com.fastkv.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * FastKV Reactive Client — integration tests.
 * <p>
 * Same coverage as {@link IntegrationTest} but exercises the
 * {@link FastKVReactiveClient} (CompletableFuture-based) API.
 * <p>
 * Run:  java -Dfastkv.host=localhost -Dfastkv.port=8379 com.fastkv.client.IntegrationTestReactive
 */
public class IntegrationTestReactive {

    private static final String HOST = System.getProperty("fastkv.host", "localhost");
    private static final int    PORT = Integer.getInteger("fastkv.port", 8379);
    private static final long   TIMEOUT_MS = 5_000;

    private static int passed = 0;
    private static int failed = 0;

    // ──────────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────────

    private static FastKVReactiveClient client() {
        return new FastKVReactiveClient(HOST, PORT);
    }

    private static <T> T await(CompletableFuture<T> cf, String label) {
        try {
            return cf.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException(label + ": timed out after " + TIMEOUT_MS + " ms", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(label + ": interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) throw (RuntimeException) cause;
            throw new RuntimeException(label + ": " + cause.getMessage(), cause);
        }
    }

    private static void pass(String name) { passed++; System.out.println("  PASS  " + name); }
    private static void fail(String name, String reason) { failed++; System.out.println("  FAIL  " + name + " — " + reason); }

    // ──────────────────────────────────────────────────────────────────────
    // Core
    // ──────────────────────────────────────────────────────────────────────

    private static void testPing() {
        try (FastKVReactiveClient c = client()) {
            String r = await(c.ping(), "PING");
            if ("PONG".equals(r)) pass("PING"); else fail("PING", "got " + r);
        }
    }

    private static void testEcho() {
        try (FastKVReactiveClient c = client()) {
            String r = await(c.echo("hello reactive"), "ECHO");
            if ("hello reactive".equals(r)) pass("ECHO"); else fail("ECHO", "got " + r);
        }
    }

    private static void testDbsize() {
        try (FastKVReactiveClient c = client()) {
            long n = await(c.dbsize(), "DBSIZE");
            if (n >= 0) pass("DBSIZE"); else fail("DBSIZE", "negative");
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // String
    // ──────────────────────────────────────────────────────────────────────

    private static void testSetGet() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:sg"), "DEL");
            await(c.set("rj:sg", "world"), "SET");
            String v = await(c.get("rj:sg"), "GET");
            await(c.del("rj:sg"), "DEL");
            if ("world".equals(v)) pass("SET/GET"); else fail("SET/GET", "got " + v);
        }
    }

    private static void testGetMissing() {
        try (FastKVReactiveClient c = client()) {
            String v = await(c.get("rj:nonexistent_xyz"), "GET");
            if (v == null) pass("GET missing"); else fail("GET missing", "got " + v);
        }
    }

    private static void testSetNxXx() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:nx"), "DEL");
            await(c.set("rj:nx", "orig"), "SET");

            Boolean nx1 = await(c.setNx("rj:nx", "overwrite"), "SETNX existing");
            String v = await(c.get("rj:nx"), "GET");
            if (nx1 || !"orig".equals(v)) { fail("SETNX existing", "nx1=" + nx1 + " v=" + v); return; }

            Boolean nx2 = await(c.setNx("rj:nx_new", "fresh"), "SETNX missing");
            if (!nx2) { fail("SETNX missing", "should be true"); return; }

            Boolean xx1 = await(c.setXx("rj:nx", "updated"), "SETXX existing");
            v = await(c.get("rj:nx"), "GET");
            if (!xx1 || !"updated".equals(v)) { fail("SETXX existing", "xx1=" + xx1 + " v=" + v); return; }

            Boolean xx2 = await(c.setXx("rj:ghost", "x"), "SETXX missing");
            await(c.del("rj:nx", "rj:nx_new"), "DEL");
            if (xx2) { fail("SETXX missing", "should be false"); return; }

            pass("SETNX/SETXX");
        }
    }

    private static void testDel() {
        try (FastKVReactiveClient c = client()) {
            await(c.set("rj:d1", "a"), "SET");
            await(c.set("rj:d2", "b"), "SET");
            long n = await(c.del("rj:d1", "rj:d2", "rj:d3_ghost"), "DEL");
            if (n == 2) pass("DEL multi"); else fail("DEL multi", "got " + n);
        }
    }

    private static void testIncrDecr() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:inc"), "DEL");
            long v;
            v = await(c.incr("rj:inc"), "INCR");        if (v != 1) { fail("INCR", ""+v); return; }
            v = await(c.incrBy("rj:inc", 9), "INCRBY");   if (v != 10) { fail("INCRBY", ""+v); return; }
            v = await(c.decr("rj:inc"), "DECR");        if (v != 9) { fail("DECR", ""+v); return; }
            v = await(c.decrBy("rj:inc", 4), "DECRBY");   if (v != 5) { fail("DECRBY", ""+v); return; }
            await(c.del("rj:inc"), "DEL");
            pass("INCR/DECR");
        }
    }

    private static void testMsetMget() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:m1", "rj:m2", "rj:m3"), "DEL");
            await(c.mset(Map.of("rj:m1", "a", "rj:m2", "b", "rj:m3", "c")), "MSET");
            List<String> vals = await(c.mget("rj:m1", "rj:m2", "rj:m3"), "MGET");
            await(c.del("rj:m1", "rj:m2", "rj:m3"), "DEL");
            if (vals != null && vals.size() == 3
                    && "a".equals(vals.get(0)) && "b".equals(vals.get(1)) && "c".equals(vals.get(2)))
                pass("MSET/MGET");
            else
                fail("MSET/MGET", "got " + vals);
        }
    }

    private static void testAppendStrlen() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:ap"), "DEL");
            await(c.set("rj:ap", "hello"), "SET");
            long len = await(c.append("rj:ap", " world"), "APPEND");
            long sl  = await(c.strlen("rj:ap"), "STRLEN");
            await(c.del("rj:ap"), "DEL");
            if (len == 11 && sl == 11) pass("APPEND/STRLEN"); else fail("APPEND/STRLEN", len+"/"+sl);
        }
    }

    private static void testGetrangeSetrange() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:gr"), "DEL");
            await(c.set("rj:gr", "Hello, World!"), "SET");
            String sub = await(c.getRange("rj:gr", 0, 4), "GETRANGE");
            if (!"Hello".equals(sub)) { fail("GETRANGE", sub); return; }
            long n = await(c.setRange("rj:gr", 7, "FastKV"), "SETRANGE");
            String v = await(c.get("rj:gr"), "GET");
            await(c.del("rj:gr"), "DEL");
            if (n == 13 && "Hello, FastKV".equals(v)) pass("GETRANGE/SETRANGE");
            else fail("GETRANGE/SETRANGE", n + " / " + v);
        }
    }

    private static void testExists() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:ex1", "rj:ex2"), "DEL");
            await(c.set("rj:ex1", "v"), "SET");
            long n = await(c.exists("rj:ex1", "rj:ex2"), "EXISTS");
            await(c.del("rj:ex1"), "DEL");
            if (n == 1) pass("EXISTS"); else fail("EXISTS", "" + n);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // TTL
    // ──────────────────────────────────────────────────────────────────────

    private static void testExpireTtlPersist() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:ttl"), "DEL");
            await(c.set("rj:ttl", "data"), "SET");

            long t = await(c.ttl("rj:ttl"), "TTL");
            if (t != -1) { fail("TTL no-expiry", "" + t); return; }

            Boolean ok = await(c.expire("rj:ttl", 60), "EXPIRE");
            if (!ok) { fail("EXPIRE", "false"); return; }

            t = await(c.ttl("rj:ttl"), "TTL");
            if (t < 1 || t > 60) { fail("TTL after EXPIRE", "" + t); return; }

            ok = await(c.persist("rj:ttl"), "PERSIST");
            if (!ok) { fail("PERSIST", "false"); return; }

            t = await(c.ttl("rj:ttl"), "TTL");
            await(c.del("rj:ttl"), "DEL");
            if (t == -1) pass("EXPIRE/TTL/PERSIST"); else fail("EXPIRE/TTL/PERSIST", "" + t);
        }
    }

    private static void testPttl() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:pttl"), "DEL");
            await(c.set("rj:pttl", "data"), "SET");
            await(c.expire("rj:pttl", 60), "EXPIRE");
            long pt = await(c.pttl("rj:pttl"), "PTTL");
            await(c.del("rj:pttl"), "DEL");
            if (pt >= 1000 && pt <= 60000) pass("PTTL"); else fail("PTTL", "" + pt);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Hash
    // ──────────────────────────────────────────────────────────────────────

    private static void testHashCrud() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:h"), "DEL");
            Boolean isNew = await(c.hset("rj:h", "name", "Alice"), "HSET");
            Boolean isUpd = await(c.hset("rj:h", "name", "Bob"), "HSET");
            String v   = await(c.hget("rj:h", "name"), "HGET");
            Boolean ex  = await(c.hexists("rj:h", "name"), "HEXISTS");
            long   len = await(c.hlen("rj:h"), "HLEN");
            await(c.del("rj:h"), "DEL");
            if (isNew && !isUpd && "Bob".equals(v) && ex && len == 1)
                pass("HSET/HGET/HEXISTS/HLEN");
            else
                fail("HSET/HGET/HEXISTS/HLEN", isNew+"/"+isUpd+"/"+v+"/"+ex+"/"+len);
        }
    }

    private static void testHmsetHgetall() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:hm"), "DEL");
            await(c.hmset("rj:hm", Map.of("f1", "v1", "f2", "v2", "f3", "v3")), "HMSET");
            Map<String, String> m = await(c.hgetAll("rj:hm"), "HGETALL");
            List<String> vals = await(c.hmget("rj:hm", "f1", "f3"), "HMGET");
            await(c.del("rj:hm"), "DEL");
            if (m != null && m.size() == 3 && "v1".equals(m.get("f1"))
                    && vals != null && vals.size() == 2 && "v1".equals(vals.get(0)) && "v3".equals(vals.get(1)))
                pass("HMSET/HGETALL/HMGET");
            else
                fail("HMSET/HGETALL/HMGET", m + " / " + vals);
        }
    }

    private static void testHdelHkeysHvals() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:hd"), "DEL");
            await(c.hmset("rj:hd", Map.of("a", "1", "b", "2", "c", "3")), "HMSET");
            long n = await(c.hdel("rj:hd", "b"), "HDEL");
            List<String> keys = await(c.hkeys("rj:hd"), "HKEYS");
            List<String> vals = await(c.hvals("rj:hd"), "HVALS");
            await(c.del("rj:hd"), "DEL");
            if (n == 1 && keys != null && keys.size() == 2 && vals != null && vals.size() == 2)
                pass("HDEL/HKEYS/HVALS");
            else
                fail("HDEL/HKEYS/HVALS", n + " / " + keys + " / " + vals);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // List
    // ──────────────────────────────────────────────────────────────────────

    private static void testListPushPop() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:l"), "DEL");
            await(c.rpush("rj:l", "a", "b", "c"), "RPUSH");
            await(c.lpush("rj:l", "z"), "LPUSH");
            List<String> items = await(c.lrange("rj:l", 0, -1), "LRANGE");
            String head = await(c.lpop("rj:l"), "LPOP");
            String tail = await(c.rpop("rj:l"), "RPOP");
            await(c.del("rj:l"), "DEL");
            if ("z".equals(head) && "c".equals(tail) && items != null
                    && items.size() == 4 && "z".equals(items.get(0)))
                pass("LPUSH/RPUSH/LPOP/RPOP/LRANGE");
            else
                fail("LPUSH/RPUSH/LPOP/RPOP/LRANGE", head+"/"+tail+"/"+items);
        }
    }

    private static void testListLtrimLsetLrem() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:lt"), "DEL");
            await(c.rpush("rj:lt", "a", "b", "c", "d", "e"), "RPUSH");
            await(c.ltrim("rj:lt", 1, 3), "LTRIM");
            List<String> items = await(c.lrange("rj:lt", 0, -1), "LRANGE");
            if (items == null || items.size() != 3) { fail("LTRIM", "" + items); return; }
            await(c.lset("rj:lt", 0, "X"), "LSET");
            String v = await(c.lindex("rj:lt", 0), "LINDEX");
            if (!"X".equals(v)) { fail("LSET+LINDEX", v); return; }
            await(c.rpush("rj:lt", "b", "b"), "RPUSH");
            long n = await(c.lrem("rj:lt", 2, "b"), "LREM");
            await(c.del("rj:lt"), "DEL");
            if (n == 2) pass("LTRIM/LSET/LINDEX/LREM"); else fail("LTRIM/LSET/LINDEX/LREM", "" + n);
        }
    }

    private static void testListLenIndex() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:ll"), "DEL");
            await(c.rpush("rj:ll", "x", "y"), "RPUSH");
            long len = await(c.llen("rj:ll"), "LLEN");
            String last = await(c.lindex("rj:ll", -1), "LINDEX");
            await(c.del("rj:ll"), "DEL");
            if (len == 2 && "y".equals(last)) pass("LLEN/LINDEX"); else fail("LLEN/LINDEX", len+"/"+last);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Pipeline
    // ──────────────────────────────────────────────────────────────────────

    private static void testPipeline() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:p1", "rj:p2", "rj:p3"), "DEL");

            ReactivePipeline pipe = c.pipeline();
            pipe.set("rj:p1", "10");
            pipe.set("rj:p2", "20");
            pipe.incr("rj:p1");
            pipe.get("rj:p1");
            pipe.get("rj:p2");
            pipe.mget("rj:p1", "rj:p2", "rj:p3");

            List<Object> results = await(pipe.execute(), "PIPELINE");
            await(c.del("rj:p1", "rj:p2", "rj:p3"), "DEL");

            if (results != null && results.size() == 6
                    && "11".equals(String.valueOf(results.get(3)))
                    && "20".equals(String.valueOf(results.get(4))))
                pass("Pipeline");
            else
                fail("Pipeline", "results=" + results);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // WRONGTYPE
    // ──────────────────────────────────────────────────────────────────────

    private static void testWrongType() {
        try (FastKVReactiveClient c = client()) {
            await(c.del("rj:wt"), "DEL");
            await(c.set("rj:wt", "string"), "SET");
            try {
                await(c.llen("rj:wt"), "LLEN");
                fail("WRONGTYPE", "no exception");
            } catch (FastKVResponseException e) {
                if (e.getMessage().contains("WRONGTYPE")) {
                    pass("WRONGTYPE");
                } else {
                    fail("WRONGTYPE", e.getMessage());
                }
            }
            await(c.del("rj:wt"), "DEL");
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Concurrent (multiple async operations in flight)
    // ──────────────────────────────────────────────────────────────────────

    private static void testConcurrent() {
        try (FastKVReactiveClient c = client()) {
            CompletableFuture<?>[] futures = new CompletableFuture[10];
            for (int i = 0; i < 10; i++) {
                final int idx = i;
                futures[i] = c.set("rj:conc:" + idx, "v")
                        .thenCompose(v -> c.get("rj:conc:" + idx));
            }
            CompletableFuture.allOf(futures).join();
            await(c.del("rj:conc:0", "rj:conc:1", "rj:conc:2", "rj:conc:3", "rj:conc:4",
                        "rj:conc:5", "rj:conc:6", "rj:conc:7", "rj:conc:8", "rj:conc:9"), "DEL");
            pass("Concurrent");
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Main
    // ──────────────────────────────────────────────────────────────────────

    public static void main(String[] args) {
        System.out.println("FastKV Java Reactive Integration Tests — " + HOST + ":" + PORT);
        System.out.println("=======================================================");

        // Core
        testPing();
        testEcho();
        testDbsize();
        // String
        testSetGet();
        testGetMissing();
        testSetNxXx();
        testDel();
        testIncrDecr();
        testMsetMget();
        testAppendStrlen();
        testGetrangeSetrange();
        testExists();
        // TTL
        testExpireTtlPersist();
        testPttl();
        // Hash
        testHashCrud();
        testHmsetHgetall();
        testHdelHkeysHvals();
        // List
        testListPushPop();
        testListLtrimLsetLrem();
        testListLenIndex();
        // Pipeline
        testPipeline();
        // WRONGTYPE
        testWrongType();
        // Concurrent
        testConcurrent();

        System.out.println("=======================================================");
        System.out.println("Results: " + passed + " passed, " + failed + " failed, " + (passed + failed) + " total");

        if (failed > 0) System.exit(1);
    }
}
