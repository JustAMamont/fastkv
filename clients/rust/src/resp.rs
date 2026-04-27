//! FastKV async client — RESP protocol codec.
//!
//! Handles encoding commands into RESP wire format and decoding
//! server replies back into Rust values.

use std::io;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::TcpStream;

/// RESP reply from the server.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Ok(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
}

impl Reply {
    /// Interpret as a string.  Returns `None` for null bulk strings and arrays.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Reply::Ok(s) => Some(s),
            Reply::Bulk(Some(b)) => Some(std::str::from_utf8(b).ok()?),
            _ => None,
        }
    }

    /// Return the error message, if this is an error reply.
    pub fn error_message(&self) -> Option<&str> {
        match self {
            Reply::Error(s) => Some(s),
            _ => None,
        }
    }

    /// Interpret as `i64`.  Returns `None` for non-integer replies.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Reply::Integer(n) => Some(*n),
            Reply::Bulk(Some(b)) => std::str::from_utf8(b).ok()?.parse().ok(),
            _ => None,
        }
    }

    /// Unwrap a simple-string reply that must be "OK".
    pub fn expect_ok(&self) -> Result<(), Error> {
        match self {
            Reply::Ok(s) if s == "OK" => Ok(()),
            Reply::Error(e) => Err(Error::Server(e.clone())),
            other => Err(Error::Unexpected(format!("expected OK, got {other:?}"))),
        }
    }

    /// Unwrap as a string, returning `None` for null bulk.
    pub fn to_string_opt(&self) -> Result<Option<String>, Error> {
        match self {
            Reply::Ok(s) => Ok(Some(s.clone())),
            Reply::Bulk(None) => Ok(None),
            Reply::Bulk(Some(b)) => Ok(Some(String::from_utf8_lossy(b).into())),
            Reply::Error(e) => Err(Error::Server(e.clone())),
            other => Err(Error::Unexpected(format!("expected string, got {other:?}"))),
        }
    }

    /// Unwrap as integer.
    pub fn to_i64(&self) -> Result<i64, Error> {
        match self {
            Reply::Integer(n) => Ok(*n),
            Reply::Error(e) => Err(Error::Server(e.clone())),
            other => Err(Error::Unexpected(format!("expected integer, got {other:?}"))),
        }
    }
}

/// Possible errors from the client.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Server(String),
    Unexpected(String),
    PipelineEmpty,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Server(s) => write!(f, "server error: {s}"),
            Error::Unexpected(s) => write!(f, "unexpected reply: {s}"),
            Error::PipelineEmpty => write!(f, "pipeline has no commands"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

/// Read the type-prefix line, then dispatch to the appropriate parser.
/// For arrays, elements are read in a loop (no recursion needed since
/// array elements are always scalar in FastKV protocol).
pub async fn read_reply(buf: &mut BufReader<&mut TcpStream>) -> Result<Reply, Error> {
    let mut line = String::new();
    buf.read_line(&mut line).await?;

    let bytes = line.as_bytes();
    if bytes.is_empty() {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed",
        )));
    }

    match bytes[0] {
        b'+' => Ok(Reply::Ok(trim_crlf(&line))),
        b'-' => Ok(Reply::Error(trim_crlf(&line))),
        b':' => {
            let n: i64 = trim_crlf(&line).parse().map_err(|_| {
                Error::Unexpected(format!("invalid integer: {line}"))
            })?;
            Ok(Reply::Integer(n))
        }
        b'$' => {
            let len: i64 = trim_crlf(&line).parse().map_err(|_| {
                Error::Unexpected(format!("invalid bulk length: {line}"))
            })?;
            if len < 0 {
                Ok(Reply::Bulk(None))
            } else {
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                buf.read_exact(&mut data).await?;
                data.truncate(len);
                Ok(Reply::Bulk(Some(data)))
            }
        }
        b'*' => {
            let count: i64 = trim_crlf(&line).parse().map_err(|_| {
                Error::Unexpected(format!("invalid array length: {line}"))
            })?;
            if count < 0 {
                Ok(Reply::Array(vec![]))
            } else {
                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    items.push(read_scalar(buf).await?);
                }
                Ok(Reply::Array(items))
            }
        }
        first => Err(Error::Unexpected(format!(
            "unknown RESP prefix: {first} ({line:?})"
        ))),
    }
}

/// Read a single scalar reply (+, -, :, $). Panics on * (use read_reply for arrays).
async fn read_scalar(buf: &mut BufReader<&mut TcpStream>) -> Result<Reply, Error> {
    let mut line = String::new();
    buf.read_line(&mut line).await?;

    match line.as_bytes().first() {
        Some(&b'+') => Ok(Reply::Ok(trim_crlf(&line))),
        Some(&b'-') => Ok(Reply::Error(trim_crlf(&line))),
        Some(&b':') => {
            let n: i64 = trim_crlf(&line).parse().map_err(|_| {
                Error::Unexpected(format!("invalid integer: {line}"))
            })?;
            Ok(Reply::Integer(n))
        }
        Some(&b'$') => {
            let len: i64 = trim_crlf(&line).parse().map_err(|_| {
                Error::Unexpected(format!("invalid bulk length: {line}"))
            })?;
            if len < 0 {
                Ok(Reply::Bulk(None))
            } else {
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                buf.read_exact(&mut data).await?;
                data.truncate(len);
                Ok(Reply::Bulk(Some(data)))
            }
        }
        other => Err(Error::Unexpected(format!(
            "expected scalar reply, got {other:?}"
        ))),
    }
}

/// Encode a command into RESP array format.
pub fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

fn trim_crlf(s: &str) -> String {
    let trimmed = s.trim_end_matches("\r\n").trim_end_matches('\n');
    trimmed[1..].to_string() // skip type prefix (+/-/:)
}

/// Decode a reply as a list of strings (for LRANGE, HKEYS, etc.)
pub fn decode_string_slice(reply: &Reply) -> Result<Vec<String>, Error> {
    match reply {
        Reply::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Reply::Bulk(Some(b)) => out.push(String::from_utf8_lossy(b).into()),
                    Reply::Bulk(None) => out.push(String::new()),
                    _ => return Err(Error::Unexpected(format!("expected bulk in array, got {item:?}"))),
                }
            }
            Ok(out)
        }
        Reply::Error(e) => Err(Error::Server(e.clone())),
        other => Err(Error::Unexpected(format!("expected array, got {other:?}"))),
    }
}

/// Decode a reply as a list of (field, value) pairs (for HGETALL).
pub fn decode_string_map(reply: &Reply) -> Result<Vec<(String, String)>, Error> {
    match reply {
        Reply::Array(items) => {
            if items.len() % 2 != 0 {
                return Err(Error::Unexpected("HGETALL returned odd number of fields".into()));
            }
            let mut out = Vec::with_capacity(items.len() / 2);
            for chunk in items.chunks(2) {
                let k = chunk[0].to_string_opt()?.unwrap_or_default();
                let v = chunk[1].to_string_opt()?.unwrap_or_default();
                out.push((k, v));
            }
            Ok(out)
        }
        Reply::Error(e) => Err(Error::Server(e.clone())),
        other => Err(Error::Unexpected(format!("expected array, got {other:?}"))),
    }
}
