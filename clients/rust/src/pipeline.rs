//! FastKV async client — pipeline support.
//!
//! Pipeline batches multiple commands into a single write, then reads
//! all replies sequentially.

use crate::resp::{encode_command, read_reply, Error, Reply};
use tokio::io::BufReader;
use tokio::net::TcpStream;

/// A pipeline that collects commands and sends them in a single batch.
///
/// # Example
/// ```no_run
/// use fastkv_client::Client;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let mut c = Client::connect("127.0.0.1", 8379).await?;
///     let mut pipe = c.pipeline();
///     pipe.set("k1", "v1");
///     pipe.set("k2", "v2");
///     pipe.get("k1");
///     let res = pipe.execute().await?;
///     assert_eq!(res.string(2)?, Some("v1".into()));
///     # Ok(())
/// }
/// ```
pub struct Pipeline<'a> {
    stream: &'a mut TcpStream,
    bufs: Vec<Vec<u8>>,
}

impl<'a> Pipeline<'a> {
    pub(crate) fn new(stream: &'a mut TcpStream) -> Self {
        Self {
            stream,
            bufs: Vec::new(),
        }
    }

    // ── String commands ──────────────────────────────────────────────────

    /// SET key value
    pub fn set(&mut self, key: &str, value: &str) {
        self.command(&[b"SET", key.as_bytes(), value.as_bytes()]);
    }

    /// GET key
    pub fn get(&mut self, key: &str) {
        self.command(&[b"GET", key.as_bytes()]);
    }

    /// DEL key [key ...]
    pub fn del(&mut self, keys: &[&str]) {
        let mut args: Vec<&[u8]> = vec![b"DEL"];
        for k in keys {
            args.push(k.as_bytes());
        }
        self.command(&args);
    }

    /// INCR key
    pub fn incr(&mut self, key: &str) {
        self.command(&[b"INCR", key.as_bytes()]);
    }

    /// INCRBY key n
    pub fn incr_by(&mut self, key: &str, n: i64) {
        self.command(&[b"INCRBY", key.as_bytes(), n.to_string().as_bytes()]);
    }

    /// DECR key
    pub fn decr(&mut self, key: &str) {
        self.command(&[b"DECR", key.as_bytes()]);
    }

    /// APPEND key value
    pub fn append(&mut self, key: &str, value: &str) {
        self.command(&[b"APPEND", key.as_bytes(), value.as_bytes()]);
    }

    /// MGET key [key ...]
    pub fn mget(&mut self, keys: &[&str]) {
        let mut args: Vec<&[u8]> = vec![b"MGET"];
        for k in keys {
            args.push(k.as_bytes());
        }
        self.command(&args);
    }

    /// EXISTS key [key ...]
    pub fn exists(&mut self, keys: &[&str]) {
        let mut args: Vec<&[u8]> = vec![b"EXISTS"];
        for k in keys {
            args.push(k.as_bytes());
        }
        self.command(&args);
    }

    /// DBSIZE
    pub fn dbsize(&mut self) {
        self.command(&[b"DBSIZE"]);
    }

    // ── TTL commands ────────────────────────────────────────────────────

    /// EXPIRE key seconds
    pub fn expire(&mut self, key: &str, seconds: i64) {
        self.command(&[b"EXPIRE", key.as_bytes(), seconds.to_string().as_bytes()]);
    }

    /// TTL key
    pub fn ttl(&mut self, key: &str) {
        self.command(&[b"TTL", key.as_bytes()]);
    }

    /// PERSIST key
    pub fn persist(&mut self, key: &str) {
        self.command(&[b"PERSIST", key.as_bytes()]);
    }

    // ── Hash commands ───────────────────────────────────────────────────

    /// HSET key field value
    pub fn hset(&mut self, key: &str, field: &str, value: &str) {
        self.command(&[b"HSET", key.as_bytes(), field.as_bytes(), value.as_bytes()]);
    }

    /// HGET key field
    pub fn hget(&mut self, key: &str, field: &str) {
        self.command(&[b"HGET", key.as_bytes(), field.as_bytes()]);
    }

    /// HDEL key field [field ...]
    pub fn hdel(&mut self, keys: &[&str]) {
        let mut args: Vec<&[u8]> = vec![b"HDEL"];
        for k in keys {
            args.push(k.as_bytes());
        }
        self.command(&args);
    }

    /// HGETALL key
    pub fn hgetall(&mut self, key: &str) {
        self.command(&[b"HGETALL", key.as_bytes()]);
    }

    /// HLEN key
    pub fn hlen(&mut self, key: &str) {
        self.command(&[b"HLEN", key.as_bytes()]);
    }

    // ── List commands ───────────────────────────────────────────────────

    /// RPUSH key elem [elem ...]
    pub fn rpush(&mut self, key: &str, elems: &[&str]) {
        let mut args: Vec<&[u8]> = vec![b"RPUSH", key.as_bytes()];
        for e in elems {
            args.push(e.as_bytes());
        }
        self.command(&args);
    }

    /// LPUSH key elem [elem ...]
    pub fn lpush(&mut self, key: &str, elems: &[&str]) {
        let mut args: Vec<&[u8]> = vec![b"LPUSH", key.as_bytes()];
        for e in elems {
            args.push(e.as_bytes());
        }
        self.command(&args);
    }

    /// LLEN key
    pub fn llen(&mut self, key: &str) {
        self.command(&[b"LLEN", key.as_bytes()]);
    }

    /// LRANGE key start stop
    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) {
        self.command(&[b"LRANGE", key.as_bytes(), start.to_string().as_bytes(), stop.to_string().as_bytes()]);
    }

    /// LPOP key
    pub fn lpop(&mut self, key: &str) {
        self.command(&[b"LPOP", key.as_bytes()]);
    }

    /// RPOP key
    pub fn rpop(&mut self, key: &str) {
        self.command(&[b"RPOP", key.as_bytes()]);
    }

    /// LTRIM key start stop
    pub fn ltrim(&mut self, key: &str, start: i64, stop: i64) {
        self.command(&[b"LTRIM", key.as_bytes(), start.to_string().as_bytes(), stop.to_string().as_bytes()]);
    }

    // ── Raw command ─────────────────────────────────────────────────────

    /// Add a raw RESP command.
    pub fn command(&mut self, args: &[&[u8]]) {
        self.bufs.push(encode_command(args));
    }

    // ── Execute ─────────────────────────────────────────────────────────

    /// Send all buffered commands and read all replies.
    ///
    /// Returns a `PipelineResult` that can be indexed to extract
    /// individual replies.
    pub async fn execute(self) -> Result<PipelineResult, Error> {
        if self.bufs.is_empty() {
            return Err(Error::PipelineEmpty);
        }

        // Write all commands in one shot
        let total_len: usize = self.bufs.iter().map(|b| b.len()).sum();
        let mut flat = Vec::with_capacity(total_len);
        for buf in &self.bufs {
            flat.extend_from_slice(buf);
        }

        use tokio::io::AsyncWriteExt;
        self.stream.write_all(&flat).await?;
        self.stream.flush().await?;

        // Read all replies
        let mut reader = BufReader::new(&mut *self.stream);
        let mut replies = Vec::with_capacity(self.bufs.len());
        for _ in &self.bufs {
            replies.push(read_reply(&mut reader).await?);
        }

        Ok(PipelineResult(replies))
    }
}

/// Holds replies from a pipeline execution.
pub struct PipelineResult(Vec<Reply>);

impl PipelineResult {
    /// Number of replies.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// True if no replies.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get raw reply at index.
    pub fn get(&self, index: usize) -> Option<&Reply> {
        self.0.get(index)
    }

    /// Interpret reply at `index` as a string (null → None).
    pub fn string(&self, index: usize) -> Result<Option<String>, Error> {
        self.0.get(index)
            .ok_or_else(|| Error::Unexpected(format!("pipeline index {index} out of range")))?
            .to_string_opt()
    }

    /// Interpret reply at `index` as i64.
    pub fn integer(&self, index: usize) -> Result<i64, Error> {
        self.0.get(index)
            .ok_or_else(|| Error::Unexpected(format!("pipeline index {index} out of range")))?
            .to_i64()
    }

    /// Interpret reply at `index` as OK status.
    pub fn ok(&self, index: usize) -> Result<(), Error> {
        self.0.get(index)
            .ok_or_else(|| Error::Unexpected(format!("pipeline index {index} out of range")))?
            .expect_ok()
    }

    /// Interpret reply at `index` as a string slice (for LRANGE, HKEYS, etc.)
    pub fn string_slice(&self, index: usize) -> Result<Vec<String>, Error> {
        let reply = self.0.get(index)
            .ok_or_else(|| Error::Unexpected(format!("pipeline index {index} out of range")))?;
        crate::resp::decode_string_slice(reply)
    }

    /// Interpret reply at `index` as a hash map (for HGETALL).
    pub fn string_map(&self, index: usize) -> Result<Vec<(String, String)>, Error> {
        let reply = self.0.get(index)
            .ok_or_else(|| Error::Unexpected(format!("pipeline index {index} out of range")))?;
        crate::resp::decode_string_map(reply)
    }
}
