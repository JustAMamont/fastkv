package com.fastkv.client;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for the FastKV Java client.
 * <p>
 * Runs against a live FastKV server.  Use system properties
 * {@code fastkv.host} and {@code fastkv.port} to override defaults.
 */
public class IntegrationTest {

    private static final String HOST = System.getProperty("fastkv.host", "localhost");
    private static final int PORT = Integer.parseInt(System.getProperty("fastkv.port", "8379"));

    private static int passed = 0;
    private static int failed = 0;

    private static void check(String name, boolean condition, String message) {
        if (condition) {
            passed++;
            System.out.println("  PASS  " + name);
        } else {
            failed++;
            System.out.println("  FAIL  " + name + " — " + message);
        }
    }

    private static FastKVClient client() {
        return new FastKVClient(HOST, PORT);
    }

    // ========== Core ==========

    private static void testPing() {
        try (FastKVClient c = client()) {
            check("PING", "PONG".equals(c.ping()), "got " + c.ping());
        } catch (Exception e) { check("PING", false, e.getMessage()); }
    }

    private static void testEcho() {
        try (FastKVClient c = client()) {
            check("ECHO", "hello".equals(c.echo("hello")), "got " + c.echo("hello"));
        } catch (Exception e) { check("ECHO", false, e.getMessage()); }
    }

    private static void testDbsize() {
        try (FastKVClient c = client()) {
            check("DBSIZE", c.dbsize() >= 0, "negative");
        } catch (Exception e) { check("DBSIZE", false, e.getMessage()); }
    }

    // ========== String ==========

    private static void testSetGet() {
        try (FastKVClient c = client()) {
            c.del("j:sg"); c.set("j:sg", "world");
            check("SET/GET", "world".equals(c.get("j:sg")), "got " + c.get("j:sg"));
            c.del("j:sg");
        } catch (Exception e) { check("SET/GET", false, e.getMessage()); }
    }

    private static void testGetMissing() {
        try (FastKVClient c = client()) {
            check("GET missing", c.get("j:nonexistent_xyz") == null, "not null");
        } catch (Exception e) { check("GET missing", false, e.getMessage()); }
    }

    private static void testSetNX() {
        try (FastKVClient c = client()) {
            c.del("j:nx"); c.set("j:nx", "orig");
            check("SETNX existing", !c.setNx("j:nx", "other"), "should be false");
            c.del("j:nx");
            check("SETNX missing", c.setNx("j:nx", "new"), "should be true");
            c.del("j:nx");
        } catch (Exception e) { check("SETNX", false, e.getMessage()); }
    }

    private static void testSetXX() {
        try (FastKVClient c = client()) {
            c.del("j:xx");
            check("SETXX missing", !c.setXx("j:xx", "nope"), "should be false");
            c.set("j:xx", "orig");
            check("SETXX existing", c.setXx("j:xx", "upd"), "should be true");
            check("SETXX value", "upd".equals(c.get("j:xx")), "got " + c.get("j:xx"));
            c.del("j:xx");
        } catch (Exception e) { check("SETXX", false, e.getMessage()); }
    }

    private static void testDelMulti() {
        try (FastKVClient c = client()) {
            c.set("j:d1", "a"); c.set("j:d2", "b"); c.set("j:d3", "c");
            long n = c.del("j:d1", "j:d2", "j:d3");
            check("DEL multi", n == 3, "got " + n);
            check("DEL verify", c.get("j:d2") == null, "still exists");
        } catch (Exception e) { check("DEL multi", false, e.getMessage()); }
    }

    private static void testIncrDecr() {
        try (FastKVClient c = client()) {
            c.del("j:inc");
            check("INCR init", c.incr("j:inc") == 1, "");
            check("INCRBY", c.incrBy("j:inc", 9) == 10, "");
            check("DECR", c.decr("j:inc") == 9, "");
            check("DECRBY", c.decrBy("j:inc", 4) == 5, "");
            c.del("j:inc");
        } catch (Exception e) { check("INCR/DECR", false, e.getMessage()); }
    }

    private static void testMSetMGet() {
        try (FastKVClient c = client()) {
            Map<String, String> batch = new HashMap<>();
            batch.put("j:m1", "a"); batch.put("j:m2", "b"); batch.put("j:m3", "c");
            c.mset(batch);
            List<String> vals = c.mget("j:m1", "j:m2", "j:m3", "j:missing");
            check("MSET/MGET", "a".equals(vals.get(0)) && "b".equals(vals.get(1)),
                    "got " + vals);
            check("MGET null", vals.get(3) == null, "not null");
            c.del("j:m1", "j:m2", "j:m3");
        } catch (Exception e) { check("MSET/MGET", false, e.getMessage()); }
    }

    private static void testAppendStrlen() {
        try (FastKVClient c = client()) {
            c.del("j:ap"); c.set("j:ap", "hello");
            check("APPEND", c.append("j:ap", " world") == 11, "");
            check("STRLEN", c.strlen("j:ap") == 11, "");
            c.del("j:ap");
        } catch (Exception e) { check("APPEND/STRLEN", false, e.getMessage()); }
    }

    private static void testGetRangeSetRange() {
        try (FastKVClient c = client()) {
            c.del("j:gr"); c.set("j:gr", "Hello, World!");
            check("GETRANGE", "Hello".equals(c.getRange("j:gr", 0, 4)), "");
            check("SETRANGE", c.setRange("j:gr", 7, "FastKV") == 13, "");
            check("SETRANGE val", "Hello, FastKV".equals(c.get("j:gr")), "");
            c.del("j:gr");
        } catch (Exception e) { check("GETRANGE/SETRANGE", false, e.getMessage()); }
    }

    private static void testExists() {
        try (FastKVClient c = client()) {
            c.del("j:ex1", "j:ex2"); c.set("j:ex1", "v");
            check("EXISTS", c.exists("j:ex1", "j:ex2") == 1, "");
            c.del("j:ex1");
        } catch (Exception e) { check("EXISTS", false, e.getMessage()); }
    }

    // ========== TTL ==========

    private static void testExpireTtlPersist() {
        try (FastKVClient c = client()) {
            c.del("j:ttl"); c.set("j:ttl", "data");
            check("TTL no-expiry", c.ttl("j:ttl") == -1, "");
            c.expire("j:ttl", 60);
            long ttl = c.ttl("j:ttl");
            check("TTL after EXPIRE", ttl > 0 && ttl <= 60, "got " + ttl);
            c.persist("j:ttl");
            check("TTL after PERSIST", c.ttl("j:ttl") == -1, "");
            c.del("j:ttl");
        } catch (Exception e) { check("TTL", false, e.getMessage()); }
    }

    private static void testPttl() {
        try (FastKVClient c = client()) {
            c.del("j:pttl"); c.set("j:pttl", "data"); c.expire("j:pttl", 60);
            long pt = c.pttl("j:pttl");
            check("PTTL", pt > 0 && pt <= 60000, "got " + pt);
            c.del("j:pttl");
        } catch (Exception e) { check("PTTL", false, e.getMessage()); }
    }

    // ========== Hash ==========

    private static void testHashCRUD() {
        try (FastKVClient c = client()) {
            c.del("j:h");
            check("HSET new", c.hset("j:h", "name", "Alice"), "");
            check("HSET update", !c.hset("j:h", "name", "Bob"), "");
            check("HGET", "Bob".equals(c.hget("j:h", "name")), "");
            check("HEXISTS", c.hexists("j:h", "name"), "");
            check("HLEN", c.hlen("j:h") == 1, "");
            c.del("j:h");
        } catch (Exception e) { check("HASH CRUD", false, e.getMessage()); }
    }

    private static void testHMSetHGetAll() {
        try (FastKVClient c = client()) {
            c.del("j:hm");
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("f1", "v1"); fields.put("f2", "v2"); fields.put("f3", "v3");
            c.hmset("j:hm", fields);
            Map<String, String> m = c.hgetAll("j:hm");
            check("HGETALL", m.size() == 3 && "v1".equals(m.get("f1")), "");
            List<String> vals = c.hmget("j:hm", "f1", "f3");
            check("HMGET", "v1".equals(vals.get(0)) && "v3".equals(vals.get(1)), "");
            c.del("j:hm");
        } catch (Exception e) { check("HMSET/HGETALL", false, e.getMessage()); }
    }

    private static void testHDelHKeysHVals() {
        try (FastKVClient c = client()) {
            c.del("j:hd");
            c.hmset("j:hd", new HashMap<String, String>() {{
                put("a", "1"); put("b", "2"); put("c", "3");
            }});
            check("HDEL", c.hdel("j:hd", "b") == 1, "");
            check("HKEYS", c.hkeys("j:hd").size() == 2, "");
            check("HVALS", c.hvals("j:hd").size() == 2, "");
            c.del("j:hd");
        } catch (Exception e) { check("HDEL/HKEYS/HVALS", false, e.getMessage()); }
    }

    // ========== List ==========

    private static void testListPushPop() {
        try (FastKVClient c = client()) {
            c.del("j:l");
            c.rpush("j:l", "a", "b", "c"); c.lpush("j:l", "z");
            List<String> items = c.lrange("j:l", 0, -1);
            check("LPUSH+RPUSH", items.size() == 4 && "z".equals(items.get(0)), "");
            check("LPOP", "z".equals(c.lpop("j:l")), "");
            check("RPOP", "c".equals(c.rpop("j:l")), "");
            c.del("j:l");
        } catch (Exception e) { check("LIST push/pop", false, e.getMessage()); }
    }

    private static void testListLTrimLSetLRem() {
        try (FastKVClient c = client()) {
            c.del("j:lt");
            c.rpush("j:lt", "a", "b", "c", "d", "e");
            c.ltrim("j:lt", 1, 3);
            check("LTRIM", c.lrange("j:lt", 0, -1).size() == 3, "");
            c.lset("j:lt", 0, "X");
            check("LSET+LINDEX", "X".equals(c.lindex("j:lt", 0)), "");
            c.rpush("j:lt", "b", "b");
            check("LREM", c.lrem("j:lt", 2, "b") == 2, "");
            c.del("j:lt");
        } catch (Exception e) { check("LIST trim/set/rem", false, e.getMessage()); }
    }

    private static void testListLenIndex() {
        try (FastKVClient c = client()) {
            c.del("j:ll");
            c.rpush("j:ll", "x", "y");
            check("LLEN", c.llen("j:ll") == 2, "");
            check("LINDEX -1", "y".equals(c.lindex("j:ll", -1)), "");
            c.del("j:ll");
        } catch (Exception e) { check("LIST len/index", false, e.getMessage()); }
    }

    // ========== Pipeline ==========

    private static void testPipeline() {
        try (FastKVClient c = client()) {
            c.del("j:p1", "j:p2", "j:p3");
            try (Pipeline p = c.pipeline()) {
                p.set("j:p1", "10"); p.set("j:p2", "20");
                p.incr("j:p1"); p.get("j:p1"); p.get("j:p2");
                List<Object> results = p.sync();
                check("Pipeline count", results.size() == 5, "got " + results.size());
                check("Pipeline INCR", results.get(3).equals("11"), "got " + results.get(3));
                check("Pipeline GET", results.get(4).equals("20"), "got " + results.get(4));
            }
            c.del("j:p1", "j:p2", "j:p3");
        } catch (Exception e) { check("Pipeline", false, e.getMessage()); }
    }

    // ========== WRONGTYPE ==========

    private static void testWrongType() {
        try (FastKVClient c = client()) {
            c.del("j:wt"); c.set("j:wt", "string");
            try {
                c.llen("j:wt");
                check("WRONGTYPE", false, "should have thrown");
            } catch (FastKVResponseException e) {
                check("WRONGTYPE", e.getMessage().contains("WRONGTYPE"), e.getMessage());
            }
            c.del("j:wt");
        } catch (Exception e) { check("WRONGTYPE", false, e.getMessage()); }
    }

    // ========== Reconnect ==========

    private static void testReconnect() {
        try (FastKVClient c = new FastKVClient(HOST, PORT, 3000, 3000)) {
            c.set("j:recon", "v");
            check("Reconnect", "v".equals(c.get("j:recon")), "");
            c.del("j:recon");
        } catch (Exception e) { check("Reconnect", false, e.getMessage()); }
    }

    // ========== Concurrent ==========

    private static void testConcurrent() throws Exception {
        try (FastKVClient c = new FastKVClient(HOST, PORT)) {
            int n = 10;
            CountDownLatch latch = new CountDownLatch(n);
            AtomicReference<Exception> err = new AtomicReference<>();
            for (int i = 0; i < n; i++) {
                final int tid = i;
                new Thread(() -> {
                    try { c.set("j:conc:" + tid, "v"); c.get("j:conc:" + tid); }
                    catch (Exception e) { err.compareAndSet(null, e); }
                    finally { latch.countDown(); }
                }).start();
            }
            latch.await();
            check("Concurrent", err.get() == null, err.get() != null ? err.get().getMessage() : "");
            String[] keys = new String[n];
            for (int i = 0; i < n; i++) keys[i] = "j:conc:" + i;
            c.del(keys);
        }
    }

    // ========== Main ==========

    public static void main(String[] args) throws Exception {
        System.out.println("FastKV Java Integration Tests — " + HOST + ":" + PORT);
        System.out.println("=======================================================");

        testPing(); testEcho(); testDbsize();
        testSetGet(); testGetMissing(); testSetNX(); testSetXX();
        testDelMulti(); testIncrDecr(); testMSetMGet();
        testAppendStrlen(); testGetRangeSetRange(); testExists();
        testExpireTtlPersist(); testPttl();
        testHashCRUD(); testHMSetHGetAll(); testHDelHKeysHVals();
        testListPushPop(); testListLTrimLSetLRem(); testListLenIndex();
        testPipeline(); testWrongType(); testReconnect();
        testConcurrent();

        System.out.println("=======================================================");
        System.out.println("Results: " + passed + " passed, " + failed + " failed, " + (passed + failed) + " total");
        if (failed > 0) System.exit(1);
    }
}
