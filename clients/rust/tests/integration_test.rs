//! FastKV Rust client — integration tests.
//!
//! Run against a live FastKV server.
//!
//!   FASTKV_HOST=localhost FASTKV_PORT=8379 cargo test -- --test-threads=1

use fastkv_client::Client;
use std::env;

fn host() -> String {
    env::var("FASTKV_HOST").unwrap_or_else(|_| "localhost".into())
}

fn port() -> u16 {
    env::var("FASTKV_PORT").unwrap_or_else(|_| "8379".into()).parse().unwrap_or(8379)
}

/// Connect, delete key, run body, delete key, close.
macro_rules! with_client {
    ($key:expr, |$c:ident| $body:expr) => {{
        let mut $c = Client::connect(&host(), port()).await.expect("connect");
        let _ = $c.del(&[$key]).await;
        $body;
        let _ = $c.del(&[$key]).await;
        $c.close().await;
    }};
}

// ── Core ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ping() {
    let mut c = Client::connect(&host(), port()).await.unwrap();
    assert_eq!(c.ping().await.unwrap(), "PONG");
    c.close().await;
}

#[tokio::test]
async fn test_echo() {
    let mut c = Client::connect(&host(), port()).await.unwrap();
    assert_eq!(c.echo("hello fastkv").await.unwrap(), "hello fastkv");
    c.close().await;
}

#[tokio::test]
async fn test_dbsize() {
    let mut c = Client::connect(&host(), port()).await.unwrap();
    let n = c.dbsize().await.unwrap();
    assert!(n >= 0);
    c.close().await;
}

// ── String ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_set_get() {
    with_client!("rust:sg", |c| {
        c.set("rust:sg", "world").await.unwrap();
        assert_eq!(c.get("rust:sg").await.unwrap(), Some("world".into()));
    });
}

#[tokio::test]
async fn test_get_missing() {
    let mut c = Client::connect(&host(), port()).await.unwrap();
    assert_eq!(c.get("rust:nonexistent_xyz").await.unwrap(), None);
    c.close().await;
}

#[tokio::test]
async fn test_del() {
    let mut c = Client::connect(&host(), port()).await.unwrap();
    c.set("rust:d1", "a").await.unwrap();
    c.set("rust:d2", "b").await.unwrap();
    let n = c.del(&["rust:d1", "rust:d2", "rust:d3_ghost"]).await.unwrap();
    assert_eq!(n, 2);
    c.close().await;
}

#[tokio::test]
async fn test_exists() {
    let mut c = Client::connect(&host(), port()).await.unwrap();
    c.set("rust:ex1", "v").await.unwrap();
    let n = c.exists(&["rust:ex1", "rust:ex2_ghost"]).await.unwrap();
    assert_eq!(n, 1);
    c.del(&["rust:ex1"]).await.unwrap();
    c.close().await;
}

#[tokio::test]
async fn test_incr_decr() {
    with_client!("rust:inc", |c| {
        assert_eq!(c.incr("rust:inc").await.unwrap(), 1);
        assert_eq!(c.incr_by("rust:inc", 9).await.unwrap(), 10);
        assert_eq!(c.decr("rust:inc").await.unwrap(), 9);
        assert_eq!(c.decr_by("rust:inc", 4).await.unwrap(), 5);
    });
}

#[tokio::test]
async fn test_mset_mget() {
    let mut c = Client::connect(&host(), port()).await.unwrap();
    c.mset(&[("rust:m1", "a"), ("rust:m2", "b"), ("rust:m3", "c")]).await.unwrap();
    let vals = c.mget(&["rust:m1", "rust:m2", "rust:m3", "rust:missing"]).await.unwrap();
    assert_eq!(vals[0], Some("a".into()));
    assert_eq!(vals[1], Some("b".into()));
    assert_eq!(vals[2], Some("c".into()));
    assert_eq!(vals[3], None);
    c.del(&["rust:m1", "rust:m2", "rust:m3"]).await.unwrap();
    c.close().await;
}

#[tokio::test]
async fn test_append_strlen() {
    with_client!("rust:ap", |c| {
        c.set("rust:ap", "hello").await.unwrap();
        assert_eq!(c.append("rust:ap", " world").await.unwrap(), 11);
        assert_eq!(c.strlen("rust:ap").await.unwrap(), 11);
    });
}

#[tokio::test]
async fn test_getrange_setrange() {
    with_client!("rust:gr", |c| {
        c.set("rust:gr", "Hello, World!").await.unwrap();
        assert_eq!(c.getrange("rust:gr", 0, 4).await.unwrap(), "Hello");
        let n = c.setrange("rust:gr", 7, "FastKV").await.unwrap();
        assert_eq!(n, 13);
        assert_eq!(c.get("rust:gr").await.unwrap(), Some("Hello, FastKV".into()));
    });
}

// ── TTL ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_expire_ttl_persist() {
    with_client!("rust:ttl", |c| {
        c.set("rust:ttl", "data").await.unwrap();
        assert_eq!(c.ttl("rust:ttl").await.unwrap(), -1);
        assert!(c.expire("rust:ttl", 60).await.unwrap());
        let ttl = c.ttl("rust:ttl").await.unwrap();
        assert!(ttl > 0 && ttl <= 60);
        assert!(c.persist("rust:ttl").await.unwrap());
        assert_eq!(c.ttl("rust:ttl").await.unwrap(), -1);
    });
}

// ── Hash ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_crud() {
    with_client!("rust:h", |c| {
        assert_eq!(c.hset("rust:h", "name", "Alice").await.unwrap(), 1);
        assert_eq!(c.hset("rust:h", "name", "Bob").await.unwrap(), 0);
        assert_eq!(c.hget("rust:h", "name").await.unwrap(), Some("Bob".into()));
        assert!(c.hexists("rust:h", "name").await.unwrap());
        assert_eq!(c.hlen("rust:h").await.unwrap(), 1);
    });
}

#[tokio::test]
async fn test_hmset_hgetall() {
    with_client!("rust:hm", |c| {
        c.hmset("rust:hm", &[("f1", "v1"), ("f2", "v2"), ("f3", "v3")]).await.unwrap();
        let map = c.hgetall("rust:hm").await.unwrap();
        assert_eq!(map.len(), 3);
        let vals = c.hmget("rust:hm", &["f1", "f3"]).await.unwrap();
        assert_eq!(vals[0], Some("v1".into()));
        assert_eq!(vals[1], Some("v3".into()));
    });
}

#[tokio::test]
async fn test_hdel_hkeys_hvals() {
    with_client!("rust:hd", |c| {
        c.hmset("rust:hd", &[("a", "1"), ("b", "2"), ("c", "3")]).await.unwrap();
        assert_eq!(c.hdel("rust:hd", &["b"]).await.unwrap(), 1);
        assert_eq!(c.hkeys("rust:hd").await.unwrap().len(), 2);
        assert_eq!(c.hvals("rust:hd").await.unwrap().len(), 2);
    });
}

// ── List ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_push_pop() {
    with_client!("rust:l", |c| {
        c.rpush("rust:l", &["a", "b", "c"]).await.unwrap();
        c.lpush("rust:l", &["z"]).await.unwrap();
        let items = c.lrange("rust:l", 0, -1).await.unwrap();
        assert_eq!(items, vec!["z", "a", "b", "c"]);
        assert_eq!(c.lpop("rust:l").await.unwrap(), Some("z".into()));
        assert_eq!(c.rpop("rust:l").await.unwrap(), Some("c".into()));
    });
}

#[tokio::test]
async fn test_list_ltrim_lset_lrem() {
    with_client!("rust:lt", |c| {
        c.rpush("rust:lt", &["a", "b", "c", "d", "e"]).await.unwrap();
        c.ltrim("rust:lt", 1, 3).await.unwrap();
        let items = c.lrange("rust:lt", 0, -1).await.unwrap();
        assert_eq!(items.len(), 3);
        c.lset("rust:lt", 0, "X").await.unwrap();
        assert_eq!(c.lindex("rust:lt", 0).await.unwrap(), Some("X".into()));
        c.rpush("rust:lt", &["b", "b"]).await.unwrap();
        assert_eq!(c.lrem("rust:lt", 2, "b").await.unwrap(), 2);
    });
}

#[tokio::test]
async fn test_list_len_index() {
    with_client!("rust:ll", |c| {
        c.rpush("rust:ll", &["x", "y"]).await.unwrap();
        assert_eq!(c.llen("rust:ll").await.unwrap(), 2);
        assert_eq!(c.lindex("rust:ll", -1).await.unwrap(), Some("y".into()));
    });
}

// ── Pipeline ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_pipeline() {
    let mut c = Client::connect(&host(), port()).await.unwrap();
    c.del(&["rust:p1", "rust:p2", "rust:p3"]).await.unwrap();

    let mut p = c.pipeline();
    p.set("rust:p1", "10");
    p.set("rust:p2", "20");
    p.incr("rust:p1");
    p.get("rust:p1");
    p.get("rust:p2");
    p.mget(&["rust:p1", "rust:p2", "rust:p3"]);

    let res = p.execute().await.unwrap();
    assert_eq!(res.len(), 6);
    assert!(res.ok(0).is_ok());
    assert!(res.ok(1).is_ok());
    assert_eq!(res.integer(2).unwrap(), 11);
    assert_eq!(res.string(3).unwrap(), Some("11".into()));
    assert_eq!(res.string(4).unwrap(), Some("20".into()));

    c.del(&["rust:p1", "rust:p2", "rust:p3"]).await.unwrap();
    c.close().await;
}

// ── WRONGTYPE ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_wrongtype() {
    with_client!("rust:wt", |c| {
        c.set("rust:wt", "string").await.unwrap();
        let err = c.llen("rust:wt").await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");
    });
}

// ── Concurrent ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_concurrent() {
    let mut c = Client::connect(&host(), port()).await.unwrap();
    let mut handles = vec![];

    for i in 0..10i32 {
        let key = format!("rust:conc:{i}");
        let h = host();
        let p = port();
        handles.push(tokio::spawn(async move {
            let mut c = Client::connect(&h, p).await.unwrap();
            c.set(&key, "val").await.unwrap();
            c.get(&key).await.unwrap();
            let _ = c.del(&[&key]).await;
            c.close().await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
    c.close().await;
}
