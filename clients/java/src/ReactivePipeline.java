package com.fastkv.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Non-blocking pipeline for {@link FastKVReactiveClient}.
 * <p>
 * Commands are buffered in memory (zero I/O during buffering).  When
 * {@link #execute()} is called, all buffered commands are flushed in a single
 * round-trip on the I/O thread, and responses are read in order.
 * <p>
 * Usage:
 * <pre>
 *   ReactivePipeline pipe = client.pipeline();
 *   pipe.set("k1", "v1");
 *   pipe.get("k1");
 *   pipe.incr("counter");
 *   CompletableFuture&lt;List&lt;Object&gt;&gt; results = pipe.execute();
 * </pre>
 * <p>
 * <b>Thread safety:</b> Pipeline is NOT thread-safe.  Buffer commands from a
 * single thread, then call {@link #execute()} once.
 */
public class ReactivePipeline {

    private final String host;
    private final int port;
    private final ExecutorService io;

    private final List<byte[]> commandBuffer = new ArrayList<>();
    private final List<ResponseConverter> converters = new ArrayList<>();

    ReactivePipeline(String host, int port, ExecutorService io) {
        this.host = host;
        this.port = port;
        this.io = io;
    }

    // ──────────────────────────────────────────────────────────────────────
    // ResponseConverter
    // ──────────────────────────────────────────────────────────────────────

    @FunctionalInterface
    private interface ResponseConverter {
        Object convert(Object raw);
    }

    private static final ResponseConverter PASSTHROUGH = raw -> raw;
    private static final ResponseConverter TO_BOOLEAN = raw -> toBoolean(raw);
    private static final ResponseConverter TO_LONG    = raw -> toLong(raw);

    // ──────────────────────────────────────────────────────────────────────
    // Core commands
    // ──────────────────────────────────────────────────────────────────────

    public ReactivePipeline ping()                { return addCommand("PING"); }
    public ReactivePipeline echo(String msg)      { return addCommand("ECHO", msg); }
    public ReactivePipeline info()                { return addCommand("INFO"); }
    public ReactivePipeline dbsize()              { return addCommand("DBSIZE"); }

    // ──────────────────────────────────────────────────────────────────────
    // String commands
    // ──────────────────────────────────────────────────────────────────────

    public ReactivePipeline set(String key, String value) {
        return addCommand("SET", key, value);
    }

    public ReactivePipeline setEx(String key, String value, long seconds) {
        return addCommand("SET", key, value, "EX", String.valueOf(seconds));
    }

    public ReactivePipeline setPx(String key, String value, long millis) {
        return addCommand("SET", key, value, "PX", String.valueOf(millis));
    }

    public ReactivePipeline setNx(String key, String value) {
        return addCommand("SET", key, value, "NX").withConverter(TO_BOOLEAN);
    }

    public ReactivePipeline setXx(String key, String value) {
        return addCommand("SET", key, value, "XX").withConverter(TO_BOOLEAN);
    }

    public ReactivePipeline get(String key) {
        return addCommand("GET", key);
    }

    public ReactivePipeline del(String... keys) {
        return addCommand(prefixed("DEL", keys)).withConverter(TO_LONG);
    }

    public ReactivePipeline exists(String... keys) {
        return addCommand(prefixed("EXISTS", keys)).withConverter(TO_LONG);
    }

    public ReactivePipeline incr(String key) {
        return addCommand("INCR", key).withConverter(TO_LONG);
    }

    public ReactivePipeline decr(String key) {
        return addCommand("DECR", key).withConverter(TO_LONG);
    }

    public ReactivePipeline incrBy(String key, long delta) {
        return addCommand("INCRBY", key, String.valueOf(delta)).withConverter(TO_LONG);
    }

    public ReactivePipeline decrBy(String key, long delta) {
        return addCommand("DECRBY", key, String.valueOf(delta)).withConverter(TO_LONG);
    }

    public ReactivePipeline append(String key, String value) {
        return addCommand("APPEND", key, value).withConverter(TO_LONG);
    }

    public ReactivePipeline strlen(String key) {
        return addCommand("STRLEN", key).withConverter(TO_LONG);
    }

    public ReactivePipeline getRange(String key, int start, int end) {
        return addCommand("GETRANGE", key, String.valueOf(start), String.valueOf(end));
    }

    public ReactivePipeline setRange(String key, int offset, String value) {
        return addCommand("SETRANGE", key, String.valueOf(offset), value).withConverter(TO_LONG);
    }

    public ReactivePipeline mset(Map<String, String> pairs) {
        String[] args = new String[pairs.size() * 2 + 1];
        args[0] = "MSET";
        int i = 1;
        for (Map.Entry<String, String> e : pairs.entrySet()) {
            args[i++] = e.getKey();
            args[i++] = e.getValue();
        }
        return addCommand(args);
    }

    public ReactivePipeline mget(String... keys) {
        return addCommand(prefixed("MGET", keys));
    }

    // ──────────────────────────────────────────────────────────────────────
    // TTL commands
    // ──────────────────────────────────────────────────────────────────────

    public ReactivePipeline expire(String key, long seconds) {
        return addCommand("EXPIRE", key, String.valueOf(seconds)).withConverter(TO_BOOLEAN);
    }

    public ReactivePipeline ttl(String key) {
        return addCommand("TTL", key).withConverter(TO_LONG);
    }

    public ReactivePipeline pttl(String key) {
        return addCommand("PTTL", key).withConverter(TO_LONG);
    }

    public ReactivePipeline persist(String key) {
        return addCommand("PERSIST", key).withConverter(TO_BOOLEAN);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Hash commands
    // ──────────────────────────────────────────────────────────────────────

    public ReactivePipeline hset(String key, String field, String value) {
        return addCommand("HSET", key, field, value).withConverter(TO_BOOLEAN);
    }

    public ReactivePipeline hget(String key, String field) {
        return addCommand("HGET", key, field);
    }

    public ReactivePipeline hdel(String key, String... fields) {
        String[] args = new String[fields.length + 2];
        args[0] = "HDEL"; args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        return addCommand(args).withConverter(TO_LONG);
    }

    public ReactivePipeline hgetAll(String key) {
        return addCommand("HGETALL", key);
    }

    public ReactivePipeline hexists(String key, String field) {
        return addCommand("HEXISTS", key, field).withConverter(TO_BOOLEAN);
    }

    public ReactivePipeline hlen(String key) {
        return addCommand("HLEN", key).withConverter(TO_LONG);
    }

    public ReactivePipeline hkeys(String key) {
        return addCommand("HKEYS", key);
    }

    public ReactivePipeline hvals(String key) {
        return addCommand("HVALS", key);
    }

    public ReactivePipeline hmget(String key, String... fields) {
        String[] args = new String[fields.length + 2];
        args[0] = "HMGET"; args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        return addCommand(args);
    }

    public ReactivePipeline hmset(String key, Map<String, String> fields) {
        String[] args = new String[fields.size() * 2 + 2];
        args[0] = "HMSET"; args[1] = key;
        int i = 2;
        for (Map.Entry<String, String> e : fields.entrySet()) {
            args[i++] = e.getKey();
            args[i++] = e.getValue();
        }
        return addCommand(args);
    }

    // ──────────────────────────────────────────────────────────────────────
    // List commands
    // ──────────────────────────────────────────────────────────────────────

    public ReactivePipeline lpush(String key, String... elements) {
        return addCommand(prefixed("LPUSH", key, elements)).withConverter(TO_LONG);
    }

    public ReactivePipeline rpush(String key, String... elements) {
        return addCommand(prefixed("RPUSH", key, elements)).withConverter(TO_LONG);
    }

    public ReactivePipeline lpop(String key) {
        return addCommand("LPOP", key);
    }

    public ReactivePipeline lpop(String key, int count) {
        return addCommand("LPOP", key, String.valueOf(count));
    }

    public ReactivePipeline rpop(String key) {
        return addCommand("RPOP", key);
    }

    public ReactivePipeline rpop(String key, int count) {
        return addCommand("RPOP", key, String.valueOf(count));
    }

    public ReactivePipeline lrange(String key, int start, int stop) {
        return addCommand("LRANGE", key, String.valueOf(start), String.valueOf(stop));
    }

    public ReactivePipeline llen(String key) {
        return addCommand("LLEN", key).withConverter(TO_LONG);
    }

    public ReactivePipeline lindex(String key, int index) {
        return addCommand("LINDEX", key, String.valueOf(index));
    }

    public ReactivePipeline lrem(String key, int count, String element) {
        return addCommand("LREM", key, String.valueOf(count), element).withConverter(TO_LONG);
    }

    public ReactivePipeline ltrim(String key, int start, int stop) {
        return addCommand("LTRIM", key, String.valueOf(start), String.valueOf(stop));
    }

    public ReactivePipeline lset(String key, int index, String element) {
        return addCommand("LSET", key, String.valueOf(index), element);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Execute
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Sends all buffered commands in a single batch and returns all responses.
     * All socket I/O happens on the I/O thread — the calling thread does not block.
     *
     * @return a future that completes with the list of responses (one per command)
     */
    public CompletableFuture<List<Object>> execute() {
        if (commandBuffer.isEmpty()) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        // Snapshot and clear the buffers
        final List<byte[]> cmds = new ArrayList<>(commandBuffer);
        final List<ResponseConverter> convs = new ArrayList<>(converters);
        commandBuffer.clear();
        converters.clear();

        CompletableFuture<List<Object>> cf = new CompletableFuture<>();
        io.submit(() -> {
            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(5000);
                socket.connect(new InetSocketAddress(host, port), 5000);

                OutputStream out = new BufferedOutputStream(socket.getOutputStream(), 16384);
                InputStream in  = new BufferedInputStream(socket.getInputStream(), 16384);

                // Send all commands at once
                for (byte[] cmd : cmds) {
                    out.write(cmd);
                }
                out.flush();

                // Read responses in order
                List<Object> results = new ArrayList<>(cmds.size());
                for (int i = 0; i < cmds.size(); i++) {
                    Object raw = RespDecoder.decode(in);
                    results.add(convs.get(i).convert(raw));
                }
                cf.complete(results);

            } catch (Throwable e) {
                cf.completeExceptionally(new FastKVConnectionException(
                        "Pipeline I/O error on " + host + ":" + port, e));
            }
        });
        return cf;
    }

    /** Discards all buffered commands. */
    public void clear() {
        commandBuffer.clear();
        converters.clear();
    }

    /** Returns the number of buffered commands. */
    public int size() {
        return commandBuffer.size();
    }

    // ──────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ──────────────────────────────────────────────────────────────────────

    private ReactivePipeline addCommand(String... args) {
        commandBuffer.add(RespEncoder.encode(args));
        converters.add(PASSTHROUGH);
        return this;
    }

    private ReactivePipeline withConverter(ResponseConverter converter) {
        converters.set(converters.size() - 1, converter);
        return this;
    }

    private static String[] prefixed(String cmd, String firstArg, String... rest) {
        String[] args = new String[rest.length + 2];
        args[0] = cmd;
        args[1] = firstArg;
        System.arraycopy(rest, 0, args, 2, rest.length);
        return args;
    }

    private static String[] prefixed(String cmd, String... keys) {
        String[] args = new String[keys.length + 1];
        args[0] = cmd;
        System.arraycopy(keys, 0, args, 1, keys.length);
        return args;
    }

    // ── Conversion helpers (duplicated from Pipeline to stay self-contained) ──

    private static Long toLong(Object raw) {
        if (raw == RespDecoder.NULL) return null;
        if (raw instanceof Long) return (Long) raw;
        if (raw instanceof String) {
            try { return Long.parseLong((String) raw); }
            catch (NumberFormatException e) {
                throw new FastKVResponseException("Expected integer, got: " + raw);
            }
        }
        throw new FastKVResponseException("Expected integer, got: " + raw);
    }

    private static Boolean toBoolean(Object raw) {
        if (raw == RespDecoder.NULL) return null;
        if (raw instanceof Long) return ((Long) raw) == 1L;
        if (raw instanceof String) return "OK".equalsIgnoreCase((String) raw);
        throw new FastKVResponseException("Expected boolean, got: " + raw);
    }
}
