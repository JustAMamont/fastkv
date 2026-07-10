//! End-to-end integration test: spawn a real FastKV server in a background
//! thread and drive commands over a TCP socket.
//!
//! This catches regressions where a command is registered in the source
//! but missing from the dispatcher (the v1.5.1 bug where ZADD/ZSCORE/etc.
//! were silently absent from `dispatch_command`).

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Once;
use std::thread;
use std::time::Duration;

use fast_kv::core::server::tcp::TokioServer;

/// Port counter so that each test file can spin up its own server without
/// colliding with another. Starts above the ephemeral range to avoid
/// accidental clashes with system services during local dev.
static NEXT_PORT: AtomicU16 = AtomicU16::new(37000);
static TOKIO_RT_INIT: Once = Once::new();

fn ensure_tokio_rt() {
    TOKIO_RT_INIT.call_once(|| {
        // No-op: we create a fresh runtime per server in `start_server`.
    });
}

fn start_server() -> u16 {
    ensure_tokio_rt();
    let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);

    thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");
        let store = std::sync::Arc::new(fast_kv::KvStoreLockFree::<64>::with_capacity(1000));
        let lists = std::sync::Arc::new(fast_kv::core::list::ListManager::<64>::new(std::sync::Arc::clone(&store)));
        let sorted_sets = std::sync::Arc::new(fast_kv::core::sortedset::SortedSetStore::new());
        let pubsub = std::sync::Arc::new(fast_kv::core::pubsub::PubSubRegistry::new());
        let blob = std::sync::Arc::new(fast_kv::core::blob::BlobArena::new());
        let expiry = std::sync::Arc::new(
            fast_kv::core::expiration::ExpirationManager::<64>::with_on_expire(
                std::sync::Arc::clone(&store),
                {
                    let mgr = std::sync::Arc::clone(&lists);
                    std::sync::Arc::new(move |key: &[u8]| mgr.remove_key(key))
                },
            ),
        );
        let server = TokioServer::<64>::with_components(
            port,
            "127.0.0.1".to_string(),
            store,
            None,
            Some(expiry),
            Some(lists),
            Some(sorted_sets),
            Some(pubsub),
            Some(blob),
            None,
            None,
            100,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        let _ = rt.block_on(server.run());
    });

    // Wait up to 5s for the server to come up.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return port;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not start on port {}", port);
}

fn send(port: u16, cmd: &[&[u8]]) -> Vec<u8> {
    use std::io::Write;
    let mut sock = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    sock.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    // Sockets are blocking by default — keep them that way so we can do
    // bounded reads without hitting EAGAIN.

    // Encode as RESP array.
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", cmd.len()).as_bytes());
    for a in cmd {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    sock.write_all(&out).unwrap();
    // Nudge the kernel to push the data; without this the server may not
    // see our request before our read blocks.
    let _ = sock.flush();

    // Read exactly one RESP reply. We parse incrementally so we stop at the
    // end of the first reply rather than blocking until the server closes
    // the connection (which never happens for a long-lived server).
    read_one_reply(&mut sock)
}

/// Read exactly one complete RESP reply from a blocking TCP stream.
///
/// Recognises: +simple\r\n, -error\r\n, :integer\r\n, $bulk\r\n...,
/// *array\r\n... (recursively).
fn read_one_reply(sock: &mut std::net::TcpStream) -> Vec<u8> {
    use std::io::Read;
    let mut buf = Vec::new();
    // Read the first line (up to \r\n).
    let first_line = read_line(sock);
    if first_line.is_empty() {
        return buf;
    }
    buf.extend_from_slice(&first_line);
    let kind = first_line[0];

    match kind {
        b'+' | b'-' | b':' => {
            // Simple string / error / integer — first line is the whole reply.
        }
        b'$' => {
            // Bulk string: read the payload + trailing CRLF (if len >= 0).
            let len_str = std::str::from_utf8(&first_line[1..first_line.len().saturating_sub(2)]).unwrap_or("0");
            let len: i64 = len_str.trim().parse().unwrap_or(-1);
            if len >= 0 {
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                sock.read_exact(&mut data).expect("read bulk payload");
                buf.extend_from_slice(&data);
            }
        }
        b'*' => {
            // Array: read N nested replies.
            let count_str = std::str::from_utf8(&first_line[1..first_line.len().saturating_sub(2)]).unwrap_or("0");
            let count: i64 = count_str.trim().parse().unwrap_or(-1);
            if count > 0 {
                for _ in 0..count {
                    let elem = read_one_reply(sock);
                    buf.extend_from_slice(&elem);
                }
            }
        }
        _ => {}
    }
    buf
}

fn read_line(sock: &mut std::net::TcpStream) -> Vec<u8> {
    use std::io::Read;
    let mut buf = Vec::with_capacity(32);
    loop {
        let mut byte = [0u8; 1];
        match sock.read_exact(&mut byte) {
            Ok(_) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n") {
                    return buf;
                }
            }
            Err(e) => {
                if buf.is_empty() {
                    panic!("read_line: connection closed: {}", e);
                }
                return buf;
            }
        }
    }
}

/// Split a single RESP reply into its raw byte form. We only assert on
/// prefixes here — the goal is to detect missing commands (which would
/// return `-ERR unknown command`), not to fully validate every reply.
fn assert_not_error(buf: &[u8], label: &str) {
    assert!(
        !buf.starts_with(b"-"),
        "{} returned error: {}",
        label,
        std::str::from_utf8(buf).unwrap_or("<non-utf8>")
    );
}

#[test]
fn test_string_commands_round_trip() {
    let port = start_server();
    assert_not_error(&send(port, &[b"PING"]), "PING");
    assert_not_error(&send(port, &[b"SET", b"k1", b"v1"]), "SET");
    assert_not_error(&send(port, &[b"GET", b"k1"]), "GET");
    assert_not_error(&send(port, &[b"INCR", b"counter"]), "INCR");
    assert_not_error(&send(port, &[b"INCRBY", b"counter", b"5"]), "INCRBY");
    assert_not_error(&send(port, &[b"MSET", b"a", b"1", b"b", b"2"]), "MSET");
    assert_not_error(&send(port, &[b"MGET", b"a", b"b"]), "MGET");
    assert_not_error(&send(port, &[b"APPEND", b"k1", b"_suffix"]), "APPEND");
    assert_not_error(&send(port, &[b"STRLEN", b"k1"]), "STRLEN");
    assert_not_error(&send(port, &[b"GETRANGE", b"k1", b"0", b"3"]), "GETRANGE");
    assert_not_error(&send(port, &[b"SETRANGE", b"k1", b"0", b"X"]), "SETRANGE");
    assert_not_error(&send(port, &[b"DEL", b"k1"]), "DEL");
    assert_not_error(&send(port, &[b"EXISTS", b"k1"]), "EXISTS");
    assert_not_error(&send(port, &[b"TYPE", b"k1"]), "TYPE");
}

#[test]
fn test_ttl_commands_round_trip() {
    let port = start_server();
    send(port, &[b"SET", b"temp", b"data"]);
    assert_not_error(&send(port, &[b"EXPIRE", b"temp", b"100"]), "EXPIRE");
    assert_not_error(&send(port, &[b"TTL", b"temp"]), "TTL");
    assert_not_error(&send(port, &[b"PTTL", b"temp"]), "PTTL");
    assert_not_error(&send(port, &[b"PERSIST", b"temp"]), "PERSIST");
}

#[test]
fn test_hash_commands_round_trip() {
    let port = start_server();
    assert_not_error(&send(port, &[b"HSET", b"h", b"f1", b"v1"]), "HSET");
    assert_not_error(&send(port, &[b"HGET", b"h", b"f1"]), "HGET");
    assert_not_error(&send(port, &[b"HEXISTS", b"h", b"f1"]), "HEXISTS");
    assert_not_error(&send(port, &[b"HLEN", b"h"]), "HLEN");
    assert_not_error(&send(port, &[b"HKEYS", b"h"]), "HKEYS");
    assert_not_error(&send(port, &[b"HVALS", b"h"]), "HVALS");
    assert_not_error(&send(port, &[b"HGETALL", b"h"]), "HGETALL");
    assert_not_error(&send(port, &[b"HMSET", b"h", b"f2", b"v2"]), "HMSET");
    assert_not_error(&send(port, &[b"HMGET", b"h", b"f1", b"f2"]), "HMGET");
    assert_not_error(&send(port, &[b"HINCRBY", b"h", b"cnt", b"1"]), "HINCRBY");
    assert_not_error(&send(port, &[b"HSETNX", b"h", b"f3", b"v3"]), "HSETNX");
    assert_not_error(&send(port, &[b"HDEL", b"h", b"f1"]), "HDEL");
}

#[test]
fn test_list_commands_round_trip() {
    let port = start_server();
    assert_not_error(&send(port, &[b"RPUSH", b"l", b"a", b"b", b"c"]), "RPUSH");
    assert_not_error(&send(port, &[b"LPUSH", b"l", b"z"]), "LPUSH");
    assert_not_error(&send(port, &[b"LRANGE", b"l", b"0", b"-1"]), "LRANGE");
    assert_not_error(&send(port, &[b"LLEN", b"l"]), "LLEN");
    assert_not_error(&send(port, &[b"LINDEX", b"l", b"0"]), "LINDEX");
    assert_not_error(&send(port, &[b"LPOP", b"l"]), "LPOP");
    assert_not_error(&send(port, &[b"RPOP", b"l"]), "RPOP");
    assert_not_error(&send(port, &[b"LSET", b"l", b"0", b"X"]), "LSET");
    assert_not_error(&send(port, &[b"LREM", b"l", b"1", b"X"]), "LREM");
    assert_not_error(&send(port, &[b"LTRIM", b"l", b"0", b"1"]), "LTRIM");
}

#[test]
fn test_sorted_set_commands_round_trip() {
    let port = start_server();
    // This was the v1.5.1 regression: all Z* commands silently returned
    // "ERR unknown command". Make sure every Z command is wired in.
    let zadd_reply = send(port, &[b"ZADD", b"z", b"1", b"one", b"2", b"two", b"3", b"three"]);
    assert_not_error(&zadd_reply, "ZADD");
    assert!(zadd_reply.starts_with(b":3"), "ZADD should return integer 3, got: {:?}",
        std::str::from_utf8(&zadd_reply));

    let zcard_reply = send(port, &[b"ZCARD", b"z"]);
    assert_not_error(&zcard_reply, "ZCARD");
    assert!(zcard_reply.starts_with(b":3"), "ZCARD should return 3, got: {:?}",
        std::str::from_utf8(&zcard_reply));

    let zscore_reply = send(port, &[b"ZSCORE", b"z", b"two"]);
    assert_not_error(&zscore_reply, "ZSCORE");
    assert!(zscore_reply.starts_with(b"$1\r\n2"), "ZSCORE should return bulk '2', got: {:?}",
        std::str::from_utf8(&zscore_reply));

    assert_not_error(&send(port, &[b"ZRANGE", b"z", b"0", b"-1"]), "ZRANGE");
    assert_not_error(&send(port, &[b"ZREVRANGE", b"z", b"0", b"-1"]), "ZREVRANGE");
    assert_not_error(&send(port, &[b"ZREVRANGEBYSCORE", b"z", b"+inf", b"-inf"]), "ZREVRANGEBYSCORE");
    assert_not_error(&send(port, &[b"ZINCRBY", b"z", b"10", b"one"]), "ZINCRBY");

    let zrem_reply = send(port, &[b"ZREM", b"z", b"two"]);
    assert_not_error(&zrem_reply, "ZREM");
    assert!(zrem_reply.starts_with(b":1"), "ZREM should return 1, got: {:?}",
        std::str::from_utf8(&zrem_reply));
}

#[test]
fn test_blob_commands_round_trip() {
    let port = start_server();
    let big = b"x".repeat(5000);
    assert_not_error(&send(port, &[b"BSET", b"blob", big.as_slice()]), "BSET");
    assert_not_error(&send(port, &[b"BGET", b"blob"]), "BGET");
    assert_not_error(&send(port, &[b"BGETRAW", b"blob"]), "BGETRAW");
    assert_not_error(&send(port, &[b"BSTATS"]), "BSTATS");
}

#[test]
fn test_similarity_commands_round_trip() {
    let port = start_server();
    send(port, &[b"SET", b"doc1", b"hello world"]);
    send(port, &[b"SET", b"doc2", b"hello world"]);
    assert_not_error(&send(port, &[b"SIMHASH", b"doc1"]), "SIMHASH");
    assert_not_error(&send(port, &[b"LSHADD", b"doc1"]), "LSHADD");
    assert_not_error(&send(port, &[b"LSHADD", b"doc2"]), "LSHADD");
    assert_not_error(&send(port, &[b"FINDSIM", b"doc1"]), "FINDSIM");
    assert_not_error(&send(port, &[b"LSHREM", b"doc1"]), "LSHREM");
}

#[test]
fn test_scan_and_stats_round_trip() {
    let port = start_server();
    send(port, &[b"SET", b"key1", b"v1"]);
    send(port, &[b"SET", b"key2", b"v2"]);
    assert_not_error(&send(port, &[b"SCAN", b"0", b"COUNT", b"100"]), "SCAN");
    assert_not_error(&send(port, &[b"DBSIZE"]), "DBSIZE");
    assert_not_error(&send(port, &[b"DBSTATS"]), "DBSTATS");
}

#[test]
fn test_pubsub_publish_no_subscribers() {
    let port = start_server();
    // PUBLISH with no subscribers should return integer 0, not an error.
    let reply = send(port, &[b"PUBLISH", b"ch1", b"hello"]);
    assert_not_error(&reply, "PUBLISH");
    assert!(reply.starts_with(b":0"), "PUBLISH with no subscribers should return 0, got: {:?}",
        std::str::from_utf8(&reply));

    // PUBSUB CHANNELS with no subscribers should return an empty array.
    let reply = send(port, &[b"PUBSUB", b"CHANNELS"]);
    assert_not_error(&reply, "PUBSUB CHANNELS");

    // PUBSUB NUMSUB on a non-existent channel should return [channel, "0"].
    let reply = send(port, &[b"PUBSUB", b"NUMSUB", b"ch1"]);
    assert_not_error(&reply, "PUBSUB NUMSUB");
}
