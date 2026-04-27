package com.fastkv.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates usage of the FastKV Java client SDK.
 * <p>
 * Run with: {@code java -cp target/classes com.fastkv.client.Example}
 */
public class Example {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6379;

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;

        System.out.println("=== FastKV Java Client SDK Examples ===");
        System.out.println("Connecting to " + host + ":" + port + "\n");

        try (FastKVClient client = new FastKVClient(host, port)) {

            // ── Core Commands ──────────────────────────────────────────────
            System.out.println("─── Core Commands ───");
            System.out.println("PING  -> " + client.ping());
            System.out.println("ECHO  -> " + client.echo("Hello FastKV!"));
            System.out.println("DBSIZE -> " + client.dbsize());

            // ── String Commands ────────────────────────────────────────────
            System.out.println("\n─── String Commands ───");

            client.set("greeting", "Hello, World!");
            System.out.println("GET greeting       -> " + client.get("greeting"));

            // SET with NX (only set if not exists)
            boolean nxResult = client.setNx("greeting", "should not overwrite");
            System.out.println("SETNX greeting     -> " + nxResult); // false
            System.out.println("GET greeting       -> " + client.get("greeting")); // still "Hello, World!"

            // SET with EX (expiry in seconds)
            client.setEx("temp_key", "temporary", 60);
            System.out.println("SETEX temp_key 60s -> OK");
            System.out.println("TTL temp_key       -> " + client.ttl("temp_key"));

            // INCR / DECR
            client.set("counter", "10");
            System.out.println("INCR counter       -> " + client.incr("counter")); // 11
            System.out.println("DECR counter       -> " + client.decr("counter")); // 10
            System.out.println("INCRBY counter 5   -> " + client.incrBy("counter", 5)); // 15
            System.out.println("DECRBY counter 3   -> " + client.decrBy("counter", 3)); // 12

            // APPEND / STRLEN
            client.set("msg", "Hello");
            long newLen = client.append("msg", ", FastKV!");
            System.out.println("APPEND msg         -> len=" + newLen + ", value=" + client.get("msg"));

            // GETRANGE / SETRANGE
            System.out.println("GETRANGE msg 0 4   -> " + client.getRange("msg", 0, 4)); // "Hello"
            long srlen = client.setRange("msg", 6, "KV");
            System.out.println("SETRANGE msg 6 KV  -> len=" + srlen + ", value=" + client.get("msg"));

            // MSET / MGET
            Map<String, String> batch = new HashMap<>();
            batch.put("k1", "v1");
            batch.put("k2", "v2");
            batch.put("k3", "v3");
            client.mset(batch);
            System.out.println("MGET k1 k2 k3      -> " + client.mget("k1", "k2", "k3"));

            // DEL / EXISTS
            long delCount = client.del("k1", "k2", "k3");
            System.out.println("DEL k1 k2 k3       -> " + delCount);
            System.out.println("EXISTS k1          -> " + client.exists("k1"));

            // ── Hash Commands ──────────────────────────────────────────────
            System.out.println("\n─── Hash Commands ───");

            client.hset("user:1", "name", "Alice");
            client.hset("user:1", "email", "alice@example.com");
            client.hset("user:1", "age", "30");
            System.out.println("HGET user:1 name   -> " + client.hget("user:1", "name"));
            System.out.println("HEXISTS user:1 age -> " + client.hexists("user:1", "age"));
            System.out.println("HLEN user:1        -> " + client.hlen("user:1"));

            // HGETALL
            Map<String, String> user = client.hgetAll("user:1");
            System.out.println("HGETALL user:1     -> " + user);

            // HMSET / HMGET
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("city", "New York");
            fields.put("country", "US");
            client.hmset("user:1", fields);
            List<String> hmget = client.hmget("user:1", "name", "city", "country");
            System.out.println("HMGET name,city,cn -> " + hmget);

            // HKEYS / HVALS
            System.out.println("HKEYS user:1       -> " + client.hkeys("user:1"));
            System.out.println("HVALS user:1       -> " + client.hvals("user:1"));

            // HDEL
            System.out.println("HDEL user:1 age    -> " + client.hdel("user:1", "age"));

            // ── List Commands ──────────────────────────────────────────────
            System.out.println("\n─── List Commands ───");

            client.del("mylist"); // clean up first
            client.rpush("mylist", "a", "b", "c");
            client.lpush("mylist", "z", "y", "x");
            System.out.println("LPUSH + RPUSH      -> LLEN=" + client.llen("mylist"));
            System.out.println("LRANGE 0 -1        -> " + client.lrange("mylist", 0, -1));

            System.out.println("LINDEX 0           -> " + client.lindex("mylist", 0));
            System.out.println("LINDEX -1          -> " + client.lindex("mylist", -1));

            String popped = client.lpop("mylist");
            System.out.println("LPOP               -> " + popped);
            System.out.println("LRANGE 0 -1        -> " + client.lrange("mylist", 0, -1));

            String rpoped = client.rpop("mylist");
            System.out.println("RPOP               -> " + rpoped);

            System.out.println("LPOP count=2       -> " + client.lpop("mylist", 2));

            // LREM / LTRIM / LSET
            client.del("mylist2");
            client.rpush("mylist2", "a", "b", "a", "c", "a");
            System.out.println("Before LREM         -> " + client.lrange("mylist2", 0, -1));
            long removed = client.lrem("mylist2", 2, "a");
            System.out.println("LREM 2 \"a\"         -> removed=" + removed + ", list=" + client.lrange("mylist2", 0, -1));

            client.lset("mylist2", 0, "X");
            System.out.println("LSET 0 X           -> list=" + client.lrange("mylist2", 0, -1));

            client.ltrim("mylist2", 0, 1);
            System.out.println("LTRIM 0 1          -> list=" + client.lrange("mylist2", 0, -1));

            // ── Pipeline ───────────────────────────────────────────────────
            System.out.println("\n─── Pipeline ───");

            try (Pipeline pipeline = client.pipeline()) {
                pipeline.set("pk1", "pv1");
                pipeline.set("pk2", "pv2");
                pipeline.set("pk3", "pv3");
                pipeline.get("pk1");
                pipeline.get("pk2");
                pipeline.get("pk3");
                pipeline.incr("counter");
                pipeline.dbsize();

                List<Object> results = pipeline.sync();
                System.out.println("Pipeline results (" + results.size() + " responses):");
                for (int i = 0; i < results.size(); i++) {
                    System.out.println("  [" + i + "] " + results.get(i));
                }
            }

            // ── Exception Handling ─────────────────────────────────────────
            System.out.println("\n─── Exception Handling ───");

            try {
                // Trying to get a non-string key as a list should return error from server
                client.set("notalist", "hello");
                client.llen("notalist");
            } catch (FastKVResponseException e) {
                System.out.println("Caught expected response error: " + e.getMessage());
            }

            // ── Cleanup ────────────────────────────────────────────────────
            System.out.println("\n─── Cleanup ───");
            long cleaned = client.del("greeting", "temp_key", "counter", "msg",
                    "user:1", "mylist", "mylist2", "notalist",
                    "pk1", "pk2", "pk3");
            System.out.println("Cleaned up " + cleaned + " keys");
            System.out.println("Final DBSIZE        -> " + client.dbsize());

            System.out.println("\n=== All examples completed successfully! ===");

        } catch (FastKVConnectionException e) {
            System.err.println("Connection error: " + e.getMessage());
            System.err.println("Make sure FastKV is running at " + host + ":" + port);
            System.exit(1);
        } catch (FastKVResponseException e) {
            System.err.println("Server error: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
