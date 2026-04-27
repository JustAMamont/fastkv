package com.fastkv.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Non-blocking (reactive) Java client for <b>FastKV</b>.
 * <p>
 * Every public command method returns a {@link CompletableFuture}, so the caller
 * never blocks the calling thread.  Internally a single dedicated I/O thread
 * serializes all socket operations, which means:
 * <ul>
 *   <li>The event-loop thread of your framework (Netty, WebFlux, Vert.x) is never blocked.</li>
 *   <li>RESP request-response ordering is naturally preserved.</li>
 *   <li>The client works with zero external dependencies.</li>
 * </ul>
 * <p>
 * <b>Bridging to reactive types:</b>
 * <pre>
 *   // Spring WebFlux / Project Reactor
 *   Mono&lt;String&gt; pong = Mono.fromCompletionStage(client.ping());
 *
 *   // RxJava 3
 *   Single&lt;String&gt; pong = Single.fromFuture(client.ping());
 * </pre>
 * <p>
 * <b>Usage:</b>
 * <pre>
 *   try (FastKVReactiveClient c = new FastKVReactiveClient("localhost", 8379)) {
 *       String pong = c.ping().get(5, TimeUnit.SECONDS);
 *       c.set("key", "value").thenCompose(v -&gt; c.get("key"))
 *        .thenAccept(val -&gt; System.out.println(val))
 *        .get(5, TimeUnit.SECONDS);
 *   }
 * </pre>
 */
public class FastKVReactiveClient implements AutoCloseable {

    private final FastKVClient delegate;
    private final ExecutorService io;
    private final String host;
    private final int port;
    private volatile boolean closed = false;

    // ──────────────────────────────────────────────────────────────────────────
    // Construction
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Creates a reactive client with default timeouts (5 s connect, 5 s socket).
     */
    public FastKVReactiveClient(String host, int port) {
        this(host, port, 5000, 5000);
    }

    /**
     * Creates a reactive client with configurable timeouts.
     */
    public FastKVReactiveClient(String host, int port, int connectTimeoutMs, int socketTimeoutMs) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Host must not be null or empty");
        }
        this.host = host;
        this.port = port;
        this.delegate = new FastKVClient(host, port, connectTimeoutMs, socketTimeoutMs);
        this.io = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "fastkv-reactive-io");
            t.setDaemon(true);
            return t;
        });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ──────────────────────────────────────────────────────────────────────────

    /** Returns {@code true} if the underlying TCP connection is alive. */
    public boolean isConnected() {
        return delegate.isConnected();
    }

    /** Closes the connection and shuts down the I/O thread. */
    @Override
    public void close() {
        closed = true;
        io.shutdownNow();
        delegate.close();
    }

    /** Returns a new {@link ReactivePipeline} bound to the same host/port. */
    public ReactivePipeline pipeline() {
        return new ReactivePipeline(host, port, io);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Internal helper
    // ──────────────────────────────────────────────────────────────────────────

    /** Submits {@code supplier} to the I/O thread and returns a {@code CompletableFuture}. */
    private <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
        if (closed) {
            CompletableFuture<T> cf = new CompletableFuture<>();
            cf.completeExceptionally(new FastKVConnectionException("Client is closed"));
            return cf;
        }
        CompletableFuture<T> cf = new CompletableFuture<>();
        io.submit(() -> {
            try {
                cf.complete(supplier.get());
            } catch (Throwable e) {
                cf.completeExceptionally(e);
            }
        });
        return cf;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Core commands
    // ═══════════════════════════════════════════════════════════════════════════

    /** Tests the connection. Returns {@code "PONG"}. */
    public CompletableFuture<String> ping() {
        return supplyAsync(delegate::ping);
    }

    /** Echoes the given message back from the server. */
    public CompletableFuture<String> echo(String message) {
        return supplyAsync(() -> delegate.echo(message));
    }

    /** Returns server information and statistics. */
    public CompletableFuture<String> info() {
        return supplyAsync(delegate::info);
    }

    /** Returns the number of keys in the database. */
    public CompletableFuture<Long> dbsize() {
        return supplyAsync(delegate::dbsize);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // String commands
    // ═══════════════════════════════════════════════════════════════════════════

    /** Sets {@code key} to hold {@code value}. */
    public CompletableFuture<String> set(String key, String value) {
        return supplyAsync(() -> delegate.set(key, value));
    }

    /** Sets {@code key} with a TTL of {@code seconds}. */
    public CompletableFuture<String> setEx(String key, String value, long seconds) {
        return supplyAsync(() -> delegate.setEx(key, value, seconds));
    }

    /** Sets {@code key} with a TTL of {@code millis} milliseconds. */
    public CompletableFuture<String> setPx(String key, String value, long millis) {
        return supplyAsync(() -> delegate.setPx(key, value, millis));
    }

    /** Sets {@code key} only if it does not already exist. */
    public CompletableFuture<Boolean> setNx(String key, String value) {
        return supplyAsync(() -> delegate.setNx(key, value));
    }

    /** Sets {@code key} only if it already exists. */
    public CompletableFuture<Boolean> setXx(String key, String value) {
        return supplyAsync(() -> delegate.setXx(key, value));
    }

    /** Gets the value of {@code key}. Returns {@code null} if missing. */
    public CompletableFuture<String> get(String key) {
        return supplyAsync(() -> delegate.get(key));
    }

    /** Removes one or more keys. Returns the number of keys removed. */
    public CompletableFuture<Long> del(String... keys) {
        return supplyAsync(() -> delegate.del(keys));
    }

    /** Returns the number of keys that exist among the arguments. */
    public CompletableFuture<Long> exists(String... keys) {
        return supplyAsync(() -> delegate.exists(keys));
    }

    /** Increments the integer value of {@code key} by one. */
    public CompletableFuture<Long> incr(String key) {
        return supplyAsync(() -> delegate.incr(key));
    }

    /** Decrements the integer value of {@code key} by one. */
    public CompletableFuture<Long> decr(String key) {
        return supplyAsync(() -> delegate.decr(key));
    }

    /** Increments by {@code delta}. */
    public CompletableFuture<Long> incrBy(String key, long delta) {
        return supplyAsync(() -> delegate.incrBy(key, delta));
    }

    /** Decrements by {@code delta}. */
    public CompletableFuture<Long> decrBy(String key, long delta) {
        return supplyAsync(() -> delegate.decrBy(key, delta));
    }

    /** Appends {@code value} to the string at {@code key}. Returns new length. */
    public CompletableFuture<Long> append(String key, String value) {
        return supplyAsync(() -> delegate.append(key, value));
    }

    /** Returns the length of the string at {@code key}. */
    public CompletableFuture<Long> strlen(String key) {
        return supplyAsync(() -> delegate.strlen(key));
    }

    /** Returns the substring determined by offsets {@code start} and {@code end}. */
    public CompletableFuture<String> getRange(String key, int start, int end) {
        return supplyAsync(() -> delegate.getRange(key, start, end));
    }

    /** Overwrites part of the string at {@code key} starting at {@code offset}. */
    public CompletableFuture<Long> setRange(String key, int offset, String value) {
        return supplyAsync(() -> delegate.setRange(key, offset, value));
    }

    /** Sets multiple keys to multiple values atomically. */
    public CompletableFuture<String> mset(Map<String, String> pairs) {
        return supplyAsync(() -> delegate.mset(pairs));
    }

    /** Returns values of all specified keys (null for missing). */
    public CompletableFuture<List<String>> mget(String... keys) {
        return supplyAsync(() -> delegate.mget(keys));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // TTL commands
    // ═══════════════════════════════════════════════════════════════════════════

    /** Sets a timeout of {@code seconds} on {@code key}. */
    public CompletableFuture<Boolean> expire(String key, long seconds) {
        return supplyAsync(() -> delegate.expire(key, seconds));
    }

    /** Returns remaining TTL in seconds (-1 = no expiry, -2 = key missing). */
    public CompletableFuture<Long> ttl(String key) {
        return supplyAsync(() -> delegate.ttl(key));
    }

    /** Like {@link #ttl} but in milliseconds. */
    public CompletableFuture<Long> pttl(String key) {
        return supplyAsync(() -> delegate.pttl(key));
    }

    /** Removes the expiration from {@code key}. */
    public CompletableFuture<Boolean> persist(String key) {
        return supplyAsync(() -> delegate.persist(key));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Hash commands
    // ═══════════════════════════════════════════════════════════════════════════

    /** Sets a field in a hash. Returns {@code true} if the field is new. */
    public CompletableFuture<Boolean> hset(String key, String field, String value) {
        return supplyAsync(() -> delegate.hset(key, field, value));
    }

    /** Returns the value of a hash field. */
    public CompletableFuture<String> hget(String key, String field) {
        return supplyAsync(() -> delegate.hget(key, field));
    }

    /** Removes fields from a hash. Returns the number removed. */
    public CompletableFuture<Long> hdel(String key, String... fields) {
        return supplyAsync(() -> delegate.hdel(key, fields));
    }

    /** Returns all fields and values of a hash. */
    public CompletableFuture<Map<String, String>> hgetAll(String key) {
        return supplyAsync(() -> delegate.hgetAll(key));
    }

    /** Returns whether a field exists in a hash. */
    public CompletableFuture<Boolean> hexists(String key, String field) {
        return supplyAsync(() -> delegate.hexists(key, field));
    }

    /** Returns the number of fields in a hash. */
    public CompletableFuture<Long> hlen(String key) {
        return supplyAsync(() -> delegate.hlen(key));
    }

    /** Returns all field names of a hash. */
    public CompletableFuture<List<String>> hkeys(String key) {
        return supplyAsync(() -> delegate.hkeys(key));
    }

    /** Returns all values of a hash. */
    public CompletableFuture<List<String>> hvals(String key) {
        return supplyAsync(() -> delegate.hvals(key));
    }

    /** Returns values of specified hash fields. */
    public CompletableFuture<List<String>> hmget(String key, String... fields) {
        return supplyAsync(() -> delegate.hmget(key, fields));
    }

    /** Sets multiple hash fields. */
    public CompletableFuture<String> hmset(String key, Map<String, String> fields) {
        return supplyAsync(() -> delegate.hmset(key, fields));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // List commands
    // ═══════════════════════════════════════════════════════════════════════════

    /** Inserts values at the head of a list. Returns new length. */
    public CompletableFuture<Long> lpush(String key, String... elements) {
        return supplyAsync(() -> delegate.lpush(key, elements));
    }

    /** Inserts values at the tail of a list. Returns new length. */
    public CompletableFuture<Long> rpush(String key, String... elements) {
        return supplyAsync(() -> delegate.rpush(key, elements));
    }

    /** Removes and returns the first element. */
    public CompletableFuture<String> lpop(String key) {
        return supplyAsync(() -> delegate.lpop(key));
    }

    /** Removes and returns the first {@code count} elements. */
    public CompletableFuture<List<String>> lpop(String key, int count) {
        return supplyAsync(() -> delegate.lpop(key, count));
    }

    /** Removes and returns the last element. */
    public CompletableFuture<String> rpop(String key) {
        return supplyAsync(() -> delegate.rpop(key));
    }

    /** Removes and returns the last {@code count} elements. */
    public CompletableFuture<List<String>> rpop(String key, int count) {
        return supplyAsync(() -> delegate.rpop(key, count));
    }

    /** Returns elements in the range [start, stop]. */
    public CompletableFuture<List<String>> lrange(String key, int start, int stop) {
        return supplyAsync(() -> delegate.lrange(key, start, stop));
    }

    /** Returns the length of a list. */
    public CompletableFuture<Long> llen(String key) {
        return supplyAsync(() -> delegate.llen(key));
    }

    /** Returns the element at {@code index}. */
    public CompletableFuture<String> lindex(String key, int index) {
        return supplyAsync(() -> delegate.lindex(key, index));
    }

    /** Removes first {@code count} occurrences of {@code element}. */
    public CompletableFuture<Long> lrem(String key, int count, String element) {
        return supplyAsync(() -> delegate.lrem(key, count, element));
    }

    /** Trims a list to the specified range. */
    public CompletableFuture<String> ltrim(String key, int start, int stop) {
        return supplyAsync(() -> delegate.ltrim(key, start, stop));
    }

    /** Sets the element at {@code index} in a list. */
    public CompletableFuture<String> lset(String key, int index, String element) {
        return supplyAsync(() -> delegate.lset(key, index, element));
    }
}
