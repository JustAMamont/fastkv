package com.fastkv.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A pipeline that buffers multiple FastKV commands and sends them in a single round-trip.
 * <p>
 * Usage pattern:
 * <pre>
 *   try (Pipeline p = client.pipeline()) {
 *       p.set("k1", "v1");
 *       p.set("k2", "v2");
 *       p.get("k1");
 *       p.get("k2");
 *       List&lt;Object&gt; results = p.sync();
 *   }
 * </pre>
 * <p>
 * The pipeline opens its own dedicated socket connection so that no other threads
 * interleave commands on the same connection. The connection is closed when the
 * pipeline is closed.
 * <p>
 * <b>Thread safety:</b> Pipeline is NOT thread-safe. It is intended to be used by
 * a single thread. Each {@link #sync()} call flushes all buffered commands and
 * reads all their responses.
 */
public class Pipeline implements AutoCloseable {

    private final String host;
    private final int port;
    private final int connectTimeout;
    private final int soTimeout;

    private Socket socket;
    private OutputStream out;
    private InputStream in;

    /** Buffer of encoded RESP command byte arrays. */
    private final List<byte[]> commandBuffer = new ArrayList<>();

    /** Track which commands expect a boolean response (SET NX/XX, EXPIRE, etc.). */
    private final List<ResponseConverter> converters = new ArrayList<>();

    Pipeline(String host, int port, int connectTimeout, int soTimeout) {
        this.host = host;
        this.port = port;
        this.connectTimeout = connectTimeout;
        this.soTimeout = soTimeout;
    }

    // ──────────────────────────────────────────────────────────────────────
    // Connection management
    // ──────────────────────────────────────────────────────────────────────

    private void ensureConnected() {
        try {
            if (socket == null || socket.isClosed()) {
                socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(soTimeout);
                socket.connect(new java.net.InetSocketAddress(host, port), connectTimeout);
                out = new BufferedOutputStream(socket.getOutputStream(), 8192);
                in = new BufferedInputStream(socket.getInputStream(), 8192);
            }
        } catch (IOException e) {
            throw new FastKVConnectionException("Failed to connect to " + host + ":" + port, e);
        }
    }

    private void writeCommand(byte[] encoded) {
        ensureConnected();
        try {
            out.write(encoded);
        } catch (IOException e) {
            throw new FastKVConnectionException("Failed to write command to pipeline", e);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // ResponseConverter – describes how to convert a raw RESP reply
    // ──────────────────────────────────────────────────────────────────────

    @FunctionalInterface
    private interface ResponseConverter {
        Object convert(Object raw);
    }

    private static final ResponseConverter PASSTHROUGH = raw -> raw;
    private static final ResponseConverter TO_BOOLEAN = raw -> toBoolean(raw);
    private static final ResponseConverter TO_LONG = raw -> toLong(raw);

    // ──────────────────────────────────────────────────────────────────────
    // Flushing & syncing
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Flushes all buffered commands to the server and reads all responses.
     *
     * @return a list of decoded responses, one per buffered command (in order)
     */
    public List<Object> sync() {
        ensureConnected();
        try {
            out.flush();
        } catch (IOException e) {
            throw new FastKVConnectionException("Failed to flush pipeline commands", e);
        }

        int count = commandBuffer.size();
        List<Object> results = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Object raw = RespDecoder.decode(in);
            results.add(converters.get(i).convert(raw));
        }
        commandBuffer.clear();
        converters.clear();
        return results;
    }

    // ──────────────────────────────────────────────────────────────────────
    // Core commands
    // ──────────────────────────────────────────────────────────────────────

    public Pipeline ping() {
        return addCommand("PING");
    }

    public Pipeline echo(String message) {
        return addCommand("ECHO", message);
    }

    public Pipeline info() {
        return addCommand("INFO");
    }

    public Pipeline dbsize() {
        return addCommand("DBSIZE");
    }

    // ──────────────────────────────────────────────────────────────────────
    // String commands
    // ──────────────────────────────────────────────────────────────────────

    public Pipeline set(String key, String value) {
        return addCommand("SET", key, value);
    }

    public Pipeline setEx(String key, String value, long seconds) {
        return addCommand("SET", key, value, "EX", String.valueOf(seconds));
    }

    public Pipeline setPx(String key, String value, long millis) {
        return addCommand("SET", key, value, "PX", String.valueOf(millis));
    }

    public Pipeline setNx(String key, String value) {
        return addCommand("SET", key, value, "NX").withConverter(TO_BOOLEAN);
    }

    public Pipeline setXx(String key, String value) {
        return addCommand("SET", key, value, "XX").withConverter(TO_BOOLEAN);
    }

    public Pipeline get(String key) {
        return addCommand("GET", key);
    }

    public Pipeline del(String... keys) {
        String[] args = new String[keys.length + 1];
        args[0] = "DEL";
        System.arraycopy(keys, 0, args, 1, keys.length);
        return addCommand(args).withConverter(TO_LONG);
    }

    public Pipeline exists(String... keys) {
        String[] args = new String[keys.length + 1];
        args[0] = "EXISTS";
        System.arraycopy(keys, 0, args, 1, keys.length);
        return addCommand(args).withConverter(TO_LONG);
    }

    public Pipeline incr(String key) {
        return addCommand("INCR", key).withConverter(TO_LONG);
    }

    public Pipeline decr(String key) {
        return addCommand("DECR", key).withConverter(TO_LONG);
    }

    public Pipeline incrBy(String key, long delta) {
        return addCommand("INCRBY", key, String.valueOf(delta)).withConverter(TO_LONG);
    }

    public Pipeline decrBy(String key, long delta) {
        return addCommand("DECRBY", key, String.valueOf(delta)).withConverter(TO_LONG);
    }

    public Pipeline append(String key, String value) {
        return addCommand("APPEND", key, value).withConverter(TO_LONG);
    }

    public Pipeline strlen(String key) {
        return addCommand("STRLEN", key).withConverter(TO_LONG);
    }

    public Pipeline getRange(String key, int start, int end) {
        return addCommand("GETRANGE", key, String.valueOf(start), String.valueOf(end));
    }

    public Pipeline setRange(String key, int offset, String value) {
        return addCommand("SETRANGE", key, String.valueOf(offset), value).withConverter(TO_LONG);
    }

    public Pipeline mset(Map<String, String> pairs) {
        String[] args = new String[pairs.size() * 2 + 1];
        args[0] = "MSET";
        int i = 1;
        for (Map.Entry<String, String> e : pairs.entrySet()) {
            args[i++] = e.getKey();
            args[i++] = e.getValue();
        }
        return addCommand(args);
    }

    public Pipeline mget(String... keys) {
        String[] args = new String[keys.length + 1];
        args[0] = "MGET";
        System.arraycopy(keys, 0, args, 1, keys.length);
        return addCommand(args);
    }

    // ──────────────────────────────────────────────────────────────────────
    // TTL commands
    // ──────────────────────────────────────────────────────────────────────

    public Pipeline expire(String key, long seconds) {
        return addCommand("EXPIRE", key, String.valueOf(seconds)).withConverter(TO_BOOLEAN);
    }

    public Pipeline ttl(String key) {
        return addCommand("TTL", key).withConverter(TO_LONG);
    }

    public Pipeline pttl(String key) {
        return addCommand("PTTL", key).withConverter(TO_LONG);
    }

    public Pipeline persist(String key) {
        return addCommand("PERSIST", key).withConverter(TO_BOOLEAN);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Hash commands
    // ──────────────────────────────────────────────────────────────────────

    public Pipeline hset(String key, String field, String value) {
        return addCommand("HSET", key, field, value).withConverter(TO_BOOLEAN);
    }

    public Pipeline hget(String key, String field) {
        return addCommand("HGET", key, field);
    }

    public Pipeline hdel(String key, String... fields) {
        String[] args = new String[fields.length + 2];
        args[0] = "HDEL";
        args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        return addCommand(args).withConverter(TO_LONG);
    }

    public Pipeline hgetAll(String key) {
        return addCommand("HGETALL", key);
    }

    public Pipeline hexists(String key, String field) {
        return addCommand("HEXISTS", key, field).withConverter(TO_BOOLEAN);
    }

    public Pipeline hlen(String key) {
        return addCommand("HLEN", key).withConverter(TO_LONG);
    }

    public Pipeline hkeys(String key) {
        return addCommand("HKEYS", key);
    }

    public Pipeline hvals(String key) {
        return addCommand("HVALS", key);
    }

    public Pipeline hmget(String key, String... fields) {
        String[] args = new String[fields.length + 2];
        args[0] = "HMGET";
        args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        return addCommand(args);
    }

    public Pipeline hmset(String key, Map<String, String> fields) {
        String[] args = new String[fields.size() * 2 + 2];
        args[0] = "HMSET";
        args[1] = key;
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

    public Pipeline lpush(String key, String... elements) {
        String[] args = new String[elements.length + 2];
        args[0] = "LPUSH";
        args[1] = key;
        System.arraycopy(elements, 0, args, 2, elements.length);
        return addCommand(args).withConverter(TO_LONG);
    }

    public Pipeline rpush(String key, String... elements) {
        String[] args = new String[elements.length + 2];
        args[0] = "RPUSH";
        args[1] = key;
        System.arraycopy(elements, 0, args, 2, elements.length);
        return addCommand(args).withConverter(TO_LONG);
    }

    public Pipeline lpop(String key) {
        return addCommand("LPOP", key);
    }

    public Pipeline lpop(String key, int count) {
        return addCommand("LPOP", key, String.valueOf(count));
    }

    public Pipeline rpop(String key) {
        return addCommand("RPOP", key);
    }

    public Pipeline rpop(String key, int count) {
        return addCommand("RPOP", key, String.valueOf(count));
    }

    public Pipeline lrange(String key, int start, int stop) {
        return addCommand("LRANGE", key, String.valueOf(start), String.valueOf(stop));
    }

    public Pipeline llen(String key) {
        return addCommand("LLEN", key).withConverter(TO_LONG);
    }

    public Pipeline lindex(String key, int index) {
        return addCommand("LINDEX", key, String.valueOf(index));
    }

    public Pipeline lrem(String key, int count, String element) {
        return addCommand("LREM", key, String.valueOf(count), element).withConverter(TO_LONG);
    }

    public Pipeline ltrim(String key, int start, int stop) {
        return addCommand("LTRIM", key, String.valueOf(start), String.valueOf(stop));
    }

    public Pipeline lset(String key, int index, String element) {
        return addCommand("LSET", key, String.valueOf(index), element);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ──────────────────────────────────────────────────────────────────────

    private Pipeline addCommand(String... args) {
        byte[] encoded = RespEncoder.encode(args);
        writeCommand(encoded);
        commandBuffer.add(encoded);
        converters.add(PASSTHROUGH);
        return this;
    }

    private Pipeline withConverter(ResponseConverter converter) {
        // Replace the last inserted converter
        converters.set(converters.size() - 1, converter);
        return this;
    }

    private static Long toLong(Object raw) {
        if (raw == RespDecoder.NULL) return null;
        if (raw instanceof Long) return (Long) raw;
        if (raw instanceof String) {
            try {
                return Long.parseLong((String) raw);
            } catch (NumberFormatException e) {
                throw new FastKVResponseException("Expected integer response, got: " + raw);
            }
        }
        throw new FastKVResponseException("Expected integer response, got: " + raw);
    }

    private static Boolean toBoolean(Object raw) {
        if (raw == RespDecoder.NULL) return null;
        if (raw instanceof Long) return ((Long) raw) == 1L;
        if (raw instanceof String) return "OK".equalsIgnoreCase((String) raw);
        throw new FastKVResponseException("Expected boolean response, got: " + raw);
    }

    // ──────────────────────────────────────────────────────────────────────
    // Close
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Closes the pipeline's dedicated connection. Any buffered but un-synced
     * commands are discarded.
     */
    @Override
    public void close() {
        commandBuffer.clear();
        converters.clear();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            socket = null;
            out = null;
            in = null;
        }
    }

}
