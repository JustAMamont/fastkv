//! FastKV async client.
//!
//! Zero-dependency (except tokio) RESP client for FastKV / Redis-compatible servers.
//!
//! # Quick start
//!
//! ```no_run
//! use fastkv_client::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut c = Client::connect("127.0.0.1", 8379).await?;
//!
//!     c.set("hello", "world").await?;
//!     let val = c.get("hello").await?;
//!     println!("{val:?}"); // Some("world")
//!
//!     let pong = c.ping().await?;
//!     println!("{pong}"); // PONG
//!
//!     c.close().await;
//!     Ok(())
//! }
//! ```

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

mod pipeline;
mod resp;

pub use pipeline::{Pipeline, PipelineResult};
pub use resp::{Error, Reply};

/// Async FastKV client.
pub struct Client {
    stream: Option<TcpStream>,
}

impl Client {
    /// Connect to a FastKV server.
    pub async fn connect(addr: &str, port: u16) -> Result<Self, Error> {
        let stream = TcpStream::connect((addr, port)).await?;
        Ok(Self {
            stream: Some(stream),
        })
    }

    /// Borrow the underlying stream. Panics if closed.
    fn stream(&mut self) -> &mut TcpStream {
        self.stream.as_mut().expect("client is closed")
    }

    /// Send a command and return the reply.
    async fn request(&mut self, args: &[&[u8]]) -> Result<Reply, Error> {
        let encoded = resp::encode_command(args);
        let s = self.stream();
        s.write_all(&encoded).await?;
        s.flush().await?;

        let mut reader = BufReader::new(s);
        let reply = resp::read_reply(&mut reader).await?;
        Ok(reply)
    }

    // ── Core ────────────────────────────────────────────────────────────

    /// PING → "PONG"
    pub async fn ping(&mut self) -> Result<String, Error> {
        let r = self.request(&[b"PING"]).await?;
        match r {
            Reply::Ok(s) => Ok(s),
            Reply::Error(e) => Err(Error::Server(e)),
            other => Err(Error::Unexpected(format!("PING: expected OK, got {other:?}"))),
        }
    }

    /// ECHO msg → msg
    pub async fn echo(&mut self, msg: &str) -> Result<String, Error> {
        let r = self.request(&[b"ECHO", msg.as_bytes()]).await?;
        match r {
            Reply::Ok(s) => Ok(s),
            Reply::Bulk(Some(b)) => Ok(String::from_utf8_lossy(&b).into()),
            Reply::Error(e) => Err(Error::Server(e)),
            other => Err(Error::Unexpected(format!("ECHO: {other:?}"))),
        }
    }

    /// DBSIZE → number of keys
    pub async fn dbsize(&mut self) -> Result<i64, Error> {
        self.request(&[b"DBSIZE"]).await?.to_i64()
    }

    /// INFO → server info string
    pub async fn info(&mut self) -> Result<String, Error> {
        let r = self.request(&[b"INFO"]).await?;
        match r {
            Reply::Ok(s) => Ok(s),
            Reply::Bulk(Some(b)) => Ok(String::from_utf8_lossy(&b).into()),
            Reply::Error(e) => Err(Error::Server(e)),
            other => Err(Error::Unexpected(format!("INFO: {other:?}"))),
        }
    }

    /// QUIT — close the connection gracefully.
    pub async fn quit(&mut self) -> Result<(), Error> {
        let _ = self.request(&[b"QUIT"]).await;
        self.close().await;
        Ok(())
    }

    // ── String ──────────────────────────────────────────────────────────

    /// SET key value → OK
    pub async fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self.request(&[b"SET", key.as_bytes(), value.as_bytes()])
            .await?
            .expect_ok()
    }

    /// GET key → value (None if missing)
    pub async fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.request(&[b"GET", key.as_bytes()])
            .await?
            .to_string_opt()
    }

    /// DEL key [key ...] → number of deleted keys
    pub async fn del(&mut self, keys: &[&str]) -> Result<i64, Error> {
        let mut args: Vec<&[u8]> = vec![b"DEL"];
        for k in keys {
            args.push(k.as_bytes());
        }
        self.request(&args).await?.to_i64()
    }

    /// EXISTS key [key ...] → count of existing keys
    pub async fn exists(&mut self, keys: &[&str]) -> Result<i64, Error> {
        let mut args: Vec<&[u8]> = vec![b"EXISTS"];
        for k in keys {
            args.push(k.as_bytes());
        }
        self.request(&args).await?.to_i64()
    }

    /// INCR key → new value
    pub async fn incr(&mut self, key: &str) -> Result<i64, Error> {
        self.request(&[b"INCR", key.as_bytes()]).await?.to_i64()
    }

    /// INCRBY key n → new value
    pub async fn incr_by(&mut self, key: &str, n: i64) -> Result<i64, Error> {
        self.request(&[b"INCRBY", key.as_bytes(), n.to_string().as_bytes()])
            .await?
            .to_i64()
    }

    /// DECR key → new value
    pub async fn decr(&mut self, key: &str) -> Result<i64, Error> {
        self.request(&[b"DECR", key.as_bytes()]).await?.to_i64()
    }

    /// DECRBY key n → new value
    pub async fn decr_by(&mut self, key: &str, n: i64) -> Result<i64, Error> {
        self.request(&[b"DECRBY", key.as_bytes(), n.to_string().as_bytes()])
            .await?
            .to_i64()
    }

    /// APPEND key value → new string length
    pub async fn append(&mut self, key: &str, value: &str) -> Result<i64, Error> {
        self.request(&[b"APPEND", key.as_bytes(), value.as_bytes()])
            .await?
            .to_i64()
    }

    /// STRLEN key → string length (0 if missing)
    pub async fn strlen(&mut self, key: &str) -> Result<i64, Error> {
        self.request(&[b"STRLEN", key.as_bytes()]).await?.to_i64()
    }

    /// GETRANGE key start end → substring
    pub async fn getrange(&mut self, key: &str, start: i64, end: i64) -> Result<String, Error> {
        let r = self.request(&[b"GETRANGE", key.as_bytes(), start.to_string().as_bytes(), end.to_string().as_bytes()]).await?;
        match r {
            Reply::Ok(s) => Ok(s),
            Reply::Bulk(Some(b)) => Ok(String::from_utf8_lossy(&b).into()),
            Reply::Error(e) => Err(Error::Server(e)),
            other => Err(Error::Unexpected(format!("GETRANGE: {other:?}"))),
        }
    }

    /// SETRANGE key offset value → new string length
    pub async fn setrange(&mut self, key: &str, offset: i64, value: &str) -> Result<i64, Error> {
        self.request(&[b"SETRANGE", key.as_bytes(), offset.to_string().as_bytes(), value.as_bytes()])
            .await?
            .to_i64()
    }

    /// MSET key value [key value ...]
    pub async fn mset(&mut self, pairs: &[(&str, &str)]) -> Result<(), Error> {
        let mut args: Vec<&[u8]> = vec![b"MSET"];
        for (k, v) in pairs {
            args.push(k.as_bytes());
            args.push(v.as_bytes());
        }
        self.request(&args).await?.expect_ok()
    }

    /// MGET key [key ...] → list of values (None for missing)
    pub async fn mget(&mut self, keys: &[&str]) -> Result<Vec<Option<String>>, Error> {
        let mut args: Vec<&[u8]> = vec![b"MGET"];
        for k in keys {
            args.push(k.as_bytes());
        }
        let r = self.request(&args).await?;
        match r {
            Reply::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in &items {
                    out.push(item.to_string_opt()?);
                }
                Ok(out)
            }
            Reply::Error(e) => Err(Error::Server(e)),
            other => Err(Error::Unexpected(format!("MGET: {other:?}"))),
        }
    }

    // ── TTL ─────────────────────────────────────────────────────────────

    /// EXPIRE key seconds → true if set
    pub async fn expire(&mut self, key: &str, seconds: i64) -> Result<bool, Error> {
        let n = self.request(&[b"EXPIRE", key.as_bytes(), seconds.to_string().as_bytes()])
            .await?
            .to_i64()?;
        Ok(n == 1)
    }

    /// TTL key → remaining seconds (-1 = no expiry, -2 = missing)
    pub async fn ttl(&mut self, key: &str) -> Result<i64, Error> {
        self.request(&[b"TTL", key.as_bytes()]).await?.to_i64()
    }

    /// PTTL key → remaining milliseconds
    pub async fn pttl(&mut self, key: &str) -> Result<i64, Error> {
        self.request(&[b"PTTL", key.as_bytes()]).await?.to_i64()
    }

    /// PERSIST key → true if TTL was removed
    pub async fn persist(&mut self, key: &str) -> Result<bool, Error> {
        let n = self.request(&[b"PERSIST", key.as_bytes()])
            .await?
            .to_i64()?;
        Ok(n == 1)
    }

    // ── Hash ────────────────────────────────────────────────────────────

    /// HSET key field value → 1 if new field, 0 if update
    pub async fn hset(&mut self, key: &str, field: &str, value: &str) -> Result<i64, Error> {
        self.request(&[b"HSET", key.as_bytes(), field.as_bytes(), value.as_bytes()])
            .await?
            .to_i64()
    }

    /// HGET key field → value (None if missing)
    pub async fn hget(&mut self, key: &str, field: &str) -> Result<Option<String>, Error> {
        self.request(&[b"HGET", key.as_bytes(), field.as_bytes()])
            .await?
            .to_string_opt()
    }

    /// HDEL key field [field ...] → number of deleted fields
    pub async fn hdel(&mut self, key: &str, fields: &[&str]) -> Result<i64, Error> {
        let mut args: Vec<&[u8]> = vec![b"HDEL", key.as_bytes()];
        for f in fields {
            args.push(f.as_bytes());
        }
        self.request(&args).await?.to_i64()
    }

    /// HEXISTS key field → true if field exists
    pub async fn hexists(&mut self, key: &str, field: &str) -> Result<bool, Error> {
        let n = self.request(&[b"HEXISTS", key.as_bytes(), field.as_bytes()])
            .await?
            .to_i64()?;
        Ok(n == 1)
    }

    /// HLEN key → number of fields
    pub async fn hlen(&mut self, key: &str) -> Result<i64, Error> {
        self.request(&[b"HLEN", key.as_bytes()]).await?.to_i64()
    }

    /// HGETALL key → list of (field, value) pairs
    pub async fn hgetall(&mut self, key: &str) -> Result<Vec<(String, String)>, Error> {
        let r = self.request(&[b"HGETALL", key.as_bytes()]).await?;
        resp::decode_string_map(&r)
    }

    /// HKEYS key → list of field names
    pub async fn hkeys(&mut self, key: &str) -> Result<Vec<String>, Error> {
        let r = self.request(&[b"HKEYS", key.as_bytes()]).await?;
        resp::decode_string_slice(&r)
    }

    /// HVALS key → list of field values
    pub async fn hvals(&mut self, key: &str) -> Result<Vec<String>, Error> {
        let r = self.request(&[b"HVALS", key.as_bytes()]).await?;
        resp::decode_string_slice(&r)
    }

    /// HMGET key field [field ...] → list of values
    pub async fn hmget(&mut self, key: &str, fields: &[&str]) -> Result<Vec<Option<String>>, Error> {
        let mut args: Vec<&[u8]> = vec![b"HMGET", key.as_bytes()];
        for f in fields {
            args.push(f.as_bytes());
        }
        let r = self.request(&args).await?;
        match r {
            Reply::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in &items {
                    out.push(item.to_string_opt()?);
                }
                Ok(out)
            }
            Reply::Error(e) => Err(Error::Server(e)),
            other => Err(Error::Unexpected(format!("HMGET: {other:?}"))),
        }
    }

    /// HMSET key field value [field value ...]
    pub async fn hmset(&mut self, key: &str, pairs: &[(&str, &str)]) -> Result<(), Error> {
        let mut args: Vec<&[u8]> = vec![b"HMSET", key.as_bytes()];
        for (f, v) in pairs {
            args.push(f.as_bytes());
            args.push(v.as_bytes());
        }
        self.request(&args).await?.expect_ok()
    }

    // ── List ────────────────────────────────────────────────────────────

    /// LPUSH key elem [elem ...] → list length
    pub async fn lpush(&mut self, key: &str, elems: &[&str]) -> Result<i64, Error> {
        let mut args: Vec<&[u8]> = vec![b"LPUSH", key.as_bytes()];
        for e in elems {
            args.push(e.as_bytes());
        }
        self.request(&args).await?.to_i64()
    }

    /// RPUSH key elem [elem ...] → list length
    pub async fn rpush(&mut self, key: &str, elems: &[&str]) -> Result<i64, Error> {
        let mut args: Vec<&[u8]> = vec![b"RPUSH", key.as_bytes()];
        for e in elems {
            args.push(e.as_bytes());
        }
        self.request(&args).await?.to_i64()
    }

    /// LPOP key → popped element (None if empty)
    pub async fn lpop(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.request(&[b"LPOP", key.as_bytes()])
            .await?
            .to_string_opt()
    }

    /// RPOP key → popped element (None if empty)
    pub async fn rpop(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.request(&[b"RPOP", key.as_bytes()])
            .await?
            .to_string_opt()
    }

    /// LLEN key → list length (0 if missing)
    pub async fn llen(&mut self, key: &str) -> Result<i64, Error> {
        self.request(&[b"LLEN", key.as_bytes()]).await?.to_i64()
    }

    /// LRANGE key start stop → list of elements
    pub async fn lrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, Error> {
        let r = self.request(&[b"LRANGE", key.as_bytes(), start.to_string().as_bytes(), stop.to_string().as_bytes()]).await?;
        resp::decode_string_slice(&r)
    }

    /// LINDEX key index → element at index (None if out of range)
    pub async fn lindex(&mut self, key: &str, index: i64) -> Result<Option<String>, Error> {
        self.request(&[b"LINDEX", key.as_bytes(), index.to_string().as_bytes()])
            .await?
            .to_string_opt()
    }

    /// LREM key count elem → number of removed elements
    pub async fn lrem(&mut self, key: &str, count: i64, elem: &str) -> Result<i64, Error> {
        self.request(&[b"LREM", key.as_bytes(), count.to_string().as_bytes(), elem.as_bytes()])
            .await?
            .to_i64()
    }

    /// LTRIM key start stop
    pub async fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> Result<(), Error> {
        self.request(&[b"LTRIM", key.as_bytes(), start.to_string().as_bytes(), stop.to_string().as_bytes()])
            .await?
            .expect_ok()
    }

    /// LSET key index elem
    pub async fn lset(&mut self, key: &str, index: i64, elem: &str) -> Result<(), Error> {
        self.request(&[b"LSET", key.as_bytes(), index.to_string().as_bytes(), elem.as_bytes()])
            .await?
            .expect_ok()
    }

    // ── Pipeline ────────────────────────────────────────────────────────

    /// Create a new pipeline. Commands are buffered locally and sent
    /// in a single write when `execute()` is called.
    pub fn pipeline(&mut self) -> Pipeline<'_> {
        Pipeline::new(self.stream())
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    /// Close the connection.
    pub async fn close(&mut self) {
        if let Some(mut s) = self.stream.take() {
            let _ = s.shutdown().await;
        }
    }
}
