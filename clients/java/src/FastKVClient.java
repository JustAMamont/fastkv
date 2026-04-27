package com.fastkv.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lightweight Java client for <b>FastKV</b> — a Redis-compatible key-value store.
 * <p>
 * This client speaks the RESP (Redis Serialization Protocol) directly over TCP
 * and has <b>zero external dependencies</b> beyond the Java standard library.
 * <p>
 * <b>Thread safety:</b> All public command methods are {@code synchronized}, so a
 * single {@code FastKVClient} instance can be safely shared across multiple threads.
 * For maximum throughput with high concurrency, consider using a connection pool or
 * per-thread instances.
 * <p>
 * <b>Usage example:</b>
 * <pre>
 *   try (FastKVClient client = new FastKVClient("localhost", 6379)) {
 *       client.set("hello", "world");
 *       String val = client.get("hello"); // "world"
 *       System.out.println(client.ping()); // "PONG"
 *   }
 * </pre>
 */
public class FastKVClient implements AutoCloseable {

    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;

    private Socket socket;
    private OutputStream out;
    private InputStream in;

    /**
     * Creates a new FastKV client with default timeouts (5s connect, 5s socket read).
     *
     * @param host the server hostname or IP address
     * @param port the server port
     */
    public FastKVClient(String host, int port) {
        this(host, port, 5000, 5000);
    }

    /**
     * Creates a new FastKV client with configurable timeouts.
     *
     * @param host             the server hostname or IP address
     * @param port             the server port
     * @param connectTimeoutMs connection timeout in milliseconds (0 = infinite)
     * @param socketTimeoutMs  socket read timeout in milliseconds (0 = infinite)
     */
    public FastKVClient(String host, int port, int connectTimeoutMs, int socketTimeoutMs) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Host must not be null or empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535: " + port);
        }
        this.host = host;
        this.port = port;
        this.connectTimeoutMs = connectTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Connection management
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Lazily opens (or re-opens) the TCP connection.
     */
    private synchronized void ensureConnected() {
        try {
            if (socket != null && !socket.isClosed() && socket.isConnected()) {
                return;
            }
            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(socketTimeoutMs);
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
            out = new BufferedOutputStream(socket.getOutputStream(), 16384);
            in = new BufferedInputStream(socket.getInputStream(), 16384);
        } catch (IOException e) {
            throw new FastKVConnectionException("Failed to connect to " + host + ":" + port, e);
        }
    }

    /**
     * Returns true if the underlying socket is currently connected.
     */
    public synchronized boolean isConnected() {
        return socket != null && !socket.isClosed() && socket.isConnected();
    }

    /**
     * Sends a command and reads the raw RESP reply.
     * On I/O error the connection is closed and one automatic reconnect attempt
     * is made before re-throwing.
     */
    private synchronized Object sendCommand(String... args) {
        ensureConnected();
        try {
            byte[] encoded = RespEncoder.encode(args);
            out.write(encoded);
            out.flush();
            return RespDecoder.decode(in);
        } catch (IOException e) {
            // Connection is likely dead — close it and retry once
            close();
            ensureConnected();
            try {
                byte[] encoded = RespEncoder.encode(args);
                out.write(encoded);
                out.flush();
                return RespDecoder.decode(in);
            } catch (IOException e2) {
                close();
                throw new FastKVConnectionException("I/O error while communicating with " + host + ":" + port, e2);
            }
        }
    }

    /**
     * Closes the connection and releases resources. Safe to call multiple times.
     */
    @Override
    public synchronized void close() {
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

    /**
     * Creates a new pipeline that uses its own dedicated connection.
     * The pipeline should be closed after use (try-with-resources recommended).
     */
    public Pipeline pipeline() {
        return new Pipeline(host, port, connectTimeoutMs, socketTimeoutMs);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Response type conversion helpers
    // ═══════════════════════════════════════════════════════════════════════

    private static String requireString(Object raw) {
        if (raw == RespDecoder.NULL) return null;
        if (raw instanceof String) return (String) raw;
        throw new FastKVResponseException("Expected string response, got: " + raw);
    }

    private static Long requireLong(Object raw) {
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

    private static Boolean requireBoolean(Object raw) {
        if (raw == RespDecoder.NULL) return null;
        if (raw instanceof Long) return ((Long) raw) == 1L;
        if (raw instanceof String) return "OK".equalsIgnoreCase((String) raw);
        throw new FastKVResponseException("Expected boolean-like response, got: " + raw);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> requireList(Object raw) {
        if (raw == RespDecoder.NULL) return Collections.emptyList();
        if (raw instanceof List) return (List<Object>) raw;
        throw new FastKVResponseException("Expected array response, got: " + raw);
    }

    /**
     * Converts a flat RESP array [f1, v1, f2, v2, ...] to a LinkedHashMap.
     */
    private static Map<String, String> listToMap(List<Object> list) {
        if (list.size() % 2 != 0) {
            throw new FastKVResponseException("Expected even number of elements for map, got: " + list.size());
        }
        Map<String, String> map = new LinkedHashMap<>(list.size() / 2);
        for (int i = 0; i < list.size(); i += 2) {
            String field = requireString(list.get(i));
            String value = requireString(list.get(i + 1));
            if (field != null) {
                map.put(field, value);
            }
        }
        return map;
    }

    /**
     * Converts a RESP array of strings to a List&lt;String&gt; (null elements become null).
     */
    private static List<String> listToStringList(List<Object> list) {
        List<String> result = new ArrayList<>(list.size());
        for (Object item : list) {
            result.add(requireString(item));
        }
        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Core commands
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Tests the connection. Returns "PONG" on success.
     */
    public String ping() {
        return requireString(sendCommand("PING"));
    }

    /**
     * Echoes the given message back from the server.
     */
    public String echo(String message) {
        return requireString(sendCommand("ECHO", message));
    }

    /**
     * Returns information and statistics about the server.
     */
    public String info() {
        return requireString(sendCommand("INFO"));
    }

    /**
     * Returns the number of keys in the currently selected database.
     */
    public long dbsize() {
        Long result = requireLong(sendCommand("DBSIZE"));
        return result != null ? result : 0L;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // String commands
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Sets {@code key} to hold {@code value}.
     *
     * @return "OK"
     */
    public String set(String key, String value) {
        return requireString(sendCommand("SET", key, value));
    }

    /**
     * Sets {@code key} to hold {@code value} with a TTL of {@code seconds}.
     *
     * @return "OK"
     */
    public String setEx(String key, String value, long seconds) {
        return requireString(sendCommand("SET", key, value, "EX", String.valueOf(seconds)));
    }

    /**
     * Sets {@code key} to hold {@code value} with a TTL of {@code millis} milliseconds.
     *
     * @return "OK"
     */
    public String setPx(String key, String value, long millis) {
        return requireString(sendCommand("SET", key, value, "PX", String.valueOf(millis)));
    }

    /**
     * Sets {@code key} to hold {@code value} only if the key does not already exist.
     *
     * @return true if the key was set, false if it already existed
     */
    public boolean setNx(String key, String value) {
        Boolean result = requireBoolean(sendCommand("SET", key, value, "NX"));
        return result != null && result;
    }

    /**
     * Sets {@code key} to hold {@code value} only if the key already exists.
     *
     * @return true if the key was set, false if it did not exist
     */
    public boolean setXx(String key, String value) {
        Boolean result = requireBoolean(sendCommand("SET", key, value, "XX"));
        return result != null && result;
    }

    /**
     * Gets the value of {@code key}. Returns null if the key does not exist.
     */
    public String get(String key) {
        return requireString(sendCommand("GET", key));
    }

    /**
     * Removes the specified keys. Returns the number of keys that were removed.
     */
    public long del(String... keys) {
        String[] args = new String[keys.length + 1];
        args[0] = "DEL";
        System.arraycopy(keys, 0, args, 1, keys.length);
        Long result = requireLong(sendCommand(args));
        return result != null ? result : 0L;
    }

    /**
     * Returns the number of keys that exist among the given arguments.
     */
    public long exists(String... keys) {
        String[] args = new String[keys.length + 1];
        args[0] = "EXISTS";
        System.arraycopy(keys, 0, args, 1, keys.length);
        Long result = requireLong(sendCommand(args));
        return result != null ? result : 0L;
    }

    /**
     * Increments the integer value of {@code key} by one.
     *
     * @return the new value
     */
    public long incr(String key) {
        return requireLong(sendCommand("INCR", key));
    }

    /**
     * Decrements the integer value of {@code key} by one.
     *
     * @return the new value
     */
    public long decr(String key) {
        return requireLong(sendCommand("DECR", key));
    }

    /**
     * Increments the integer value of {@code key} by {@code delta}.
     *
     * @return the new value
     */
    public long incrBy(String key, long delta) {
        return requireLong(sendCommand("INCRBY", key, String.valueOf(delta)));
    }

    /**
     * Decrements the integer value of {@code key} by {@code delta}.
     *
     * @return the new value
     */
    public long decrBy(String key, long delta) {
        return requireLong(sendCommand("DECRBY", key, String.valueOf(delta)));
    }

    /**
     * Appends {@code value} to the string stored at {@code key}.
     * If the key does not exist, it is set to {@code value}.
     *
     * @return the length of the string after the append operation
     */
    public long append(String key, String value) {
        return requireLong(sendCommand("APPEND", key, value));
    }

    /**
     * Returns the length of the string value stored at {@code key}.
     * Returns 0 when the key does not exist.
     */
    public long strlen(String key) {
        Long result = requireLong(sendCommand("STRLEN", key));
        return result != null ? result : 0L;
    }

    /**
     * Returns the substring of the string value stored at {@code key},
     * determined by the offsets {@code start} and {@code end} (both inclusive).
     * Negative offsets count from the end of the string.
     */
    public String getRange(String key, int start, int end) {
        return requireString(sendCommand("GETRANGE", key, String.valueOf(start), String.valueOf(end)));
    }

    /**
     * Overwrites part of the string value at {@code key}, starting at
     * {@code offset}, with {@code value}.
     *
     * @return the length of the string after the operation
     */
    public long setRange(String key, int offset, String value) {
        return requireLong(sendCommand("SETRANGE", key, String.valueOf(offset), value));
    }

    /**
     * Sets multiple keys to multiple values in a single atomic operation.
     *
     * @return "OK"
     */
    public String mset(Map<String, String> pairs) {
        String[] args = new String[pairs.size() * 2 + 1];
        args[0] = "MSET";
        int i = 1;
        for (Map.Entry<String, String> e : pairs.entrySet()) {
            args[i++] = e.getKey();
            args[i++] = e.getValue();
        }
        return requireString(sendCommand(args));
    }

    /**
     * Returns the values of all specified keys.
     * For every key that does not hold a string value or does not exist,
     * the corresponding element in the list is null.
     */
    public List<String> mget(String... keys) {
        String[] args = new String[keys.length + 1];
        args[0] = "MGET";
        System.arraycopy(keys, 0, args, 1, keys.length);
        List<Object> raw = requireList(sendCommand(args));
        return listToStringList(raw);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // TTL commands
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Sets a timeout of {@code seconds} on {@code key}.
     *
     * @return true if the timeout was set, false if the key does not exist
     */
    public boolean expire(String key, long seconds) {
        Boolean result = requireBoolean(sendCommand("EXPIRE", key, String.valueOf(seconds)));
        return result != null && result;
    }

    /**
     * Returns the remaining time to live of {@code key} in seconds.
     * Returns -1 if the key exists but has no associated expire.
     * Returns -2 if the key does not exist.
     */
    public long ttl(String key) {
        Long result = requireLong(sendCommand("TTL", key));
        return result != null ? result : -2L;
    }

    /**
     * Like {@link #ttl(String)} but returns milliseconds.
     */
    public long pttl(String key) {
        Long result = requireLong(sendCommand("PTTL", key));
        return result != null ? result : -2L;
    }

    /**
     * Removes the expiration from {@code key}, making it persist indefinitely.
     *
     * @return true if the timeout was removed, false if the key does not exist or has no timeout
     */
    public boolean persist(String key) {
        Boolean result = requireBoolean(sendCommand("PERSIST", key));
        return result != null && result;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Hash commands
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Sets the field {@code field} in the hash stored at {@code key} to {@code value}.
     *
     * @return true if the field is new, false if the field already existed and was updated
     */
    public boolean hset(String key, String field, String value) {
        Boolean result = requireBoolean(sendCommand("HSET", key, field, value));
        return result != null && result;
    }

    /**
     * Returns the value associated with {@code field} in the hash stored at {@code key}.
     */
    public String hget(String key, String field) {
        return requireString(sendCommand("HGET", key, field));
    }

    /**
     * Removes the specified fields from the hash stored at {@code key}.
     *
     * @return the number of fields that were removed
     */
    public long hdel(String key, String... fields) {
        String[] args = new String[fields.length + 2];
        args[0] = "HDEL";
        args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        Long result = requireLong(sendCommand(args));
        return result != null ? result : 0L;
    }

    /**
     * Returns all fields and values of the hash stored at {@code key}.
     */
    public Map<String, String> hgetAll(String key) {
        List<Object> raw = requireList(sendCommand("HGETALL", key));
        return listToMap(raw);
    }

    /**
     * Returns whether the field is an existing field in the hash stored at {@code key}.
     */
    public boolean hexists(String key, String field) {
        Boolean result = requireBoolean(sendCommand("HEXISTS", key, field));
        return result != null && result;
    }

    /**
     * Returns the number of fields contained in the hash stored at {@code key}.
     */
    public long hlen(String key) {
        Long result = requireLong(sendCommand("HLEN", key));
        return result != null ? result : 0L;
    }

    /**
     * Returns all field names in the hash stored at {@code key}.
     */
    public List<String> hkeys(String key) {
        List<Object> raw = requireList(sendCommand("HKEYS", key));
        return listToStringList(raw);
    }

    /**
     * Returns all values in the hash stored at {@code key}.
     */
    public List<String> hvals(String key) {
        List<Object> raw = requireList(sendCommand("HVALS", key));
        return listToStringList(raw);
    }

    /**
     * Returns the values associated with the specified fields in the hash stored at {@code key}.
     */
    public List<String> hmget(String key, String... fields) {
        String[] args = new String[fields.length + 2];
        args[0] = "HMGET";
        args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        List<Object> raw = requireList(sendCommand(args));
        return listToStringList(raw);
    }

    /**
     * Sets the specified fields to their respective values in the hash stored at {@code key}.
     * This command overwrites any specified fields that already exist.
     *
     * @return "OK"
     */
    public String hmset(String key, Map<String, String> fields) {
        String[] args = new String[fields.size() * 2 + 2];
        args[0] = "HMSET";
        args[1] = key;
        int i = 2;
        for (Map.Entry<String, String> e : fields.entrySet()) {
            args[i++] = e.getKey();
            args[i++] = e.getValue();
        }
        return requireString(sendCommand(args));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // List commands
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Inserts all the specified values at the head of the list stored at {@code key}.
     *
     * @return the length of the list after the push operations
     */
    public long lpush(String key, String... elements) {
        String[] args = new String[elements.length + 2];
        args[0] = "LPUSH";
        args[1] = key;
        System.arraycopy(elements, 0, args, 2, elements.length);
        return requireLong(sendCommand(args));
    }

    /**
     * Inserts all the specified values at the tail of the list stored at {@code key}.
     *
     * @return the length of the list after the push operations
     */
    public long rpush(String key, String... elements) {
        String[] args = new String[elements.length + 2];
        args[0] = "RPUSH";
        args[1] = key;
        System.arraycopy(elements, 0, args, 2, elements.length);
        return requireLong(sendCommand(args));
    }

    /**
     * Removes and returns the first element of the list stored at {@code key}.
     * Returns null if the key does not exist.
     */
    public String lpop(String key) {
        return requireString(sendCommand("LPOP", key));
    }

    /**
     * Removes and returns the first {@code count} elements of the list stored at {@code key}.
     *
     * @return a list of the popped elements, or an empty list if the key does not exist
     */
    public List<String> lpop(String key, int count) {
        List<Object> raw = requireList(sendCommand("LPOP", key, String.valueOf(count)));
        return listToStringList(raw);
    }

    /**
     * Removes and returns the last element of the list stored at {@code key}.
     * Returns null if the key does not exist.
     */
    public String rpop(String key) {
        return requireString(sendCommand("RPOP", key));
    }

    /**
     * Removes and returns the last {@code count} elements of the list stored at {@code key}.
     *
     * @return a list of the popped elements, or an empty list if the key does not exist
     */
    public List<String> rpop(String key, int count) {
        List<Object> raw = requireList(sendCommand("RPOP", key, String.valueOf(count)));
        return listToStringList(raw);
    }

    /**
     * Returns the specified elements of the list stored at {@code key}.
     * Offsets start and stop are zero-based and inclusive.
     * Negative offsets count from the end of the list.
     */
    public List<String> lrange(String key, int start, int stop) {
        List<Object> raw = requireList(sendCommand("LRANGE", key, String.valueOf(start), String.valueOf(stop)));
        return listToStringList(raw);
    }

    /**
     * Returns the length of the list stored at {@code key}.
     */
    public long llen(String key) {
        Long result = requireLong(sendCommand("LLEN", key));
        return result != null ? result : 0L;
    }

    /**
     * Returns the element at {@code index} in the list stored at {@code key}.
     * Negative indexes count from the tail.
     */
    public String lindex(String key, int index) {
        return requireString(sendCommand("LINDEX", key, String.valueOf(index)));
    }

    /**
     * Removes the first {@code count} occurrences of {@code element} from the list stored at {@code key}.
     * If {@code count} is negative, removes from the tail. If zero, removes all.
     *
     * @return the number of removed elements
     */
    public long lrem(String key, int count, String element) {
        Long result = requireLong(sendCommand("LREM", key, String.valueOf(count), element));
        return result != null ? result : 0L;
    }

    /**
     * Trims the list stored at {@code key} to the specified range.
     *
     * @return "OK"
     */
    public String ltrim(String key, int start, int stop) {
        return requireString(sendCommand("LTRIM", key, String.valueOf(start), String.valueOf(stop)));
    }

    /**
     * Sets the element at {@code index} in the list stored at {@code key} to {@code element}.
     *
     * @return "OK"
     */
    public String lset(String key, int index, String element) {
        return requireString(sendCommand("LSET", key, String.valueOf(index), element));
    }
}
