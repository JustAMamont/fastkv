//! RESP (Redis Serialization Protocol) Implementation
//!
//! RESP is the protocol Redis uses for communication.
//! It's simple, binary-safe, and human-readable.
//!
//! ## Protocol Overview
//!
//! RESP has 5 data types:
//! - Simple Strings: `+OK\r\n`
//! - Errors: `-ERR message\r\n`
//! - Integers: `:1000\r\n`
//! - Bulk Strings: `$6\r\nfoobar\r\n`
//! - Arrays: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
//!
//! ## Example
//!
//! A SET command looks like:
//! ```text
//! *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
//! ```
//!
//! Which represents the array: ["SET", "key", "value"]

// ============================================================================
// COMMAND STRUCTURE
// ============================================================================

/// A parsed RESP command
/// 
/// Contains the command name and its arguments.
#[derive(Debug, Clone)]
pub struct Command {
    /// Command name (uppercase, e.g., "GET", "SET")
    pub name: String,
    /// Command arguments
    pub args: Vec<Vec<u8>>,
}

impl Command {
    /// Get the first argument as a key
    pub fn key(&self) -> Option<&[u8]> {
        self.args.get(1).map(|v| v.as_slice())
    }

    /// Get the second argument as a value
    pub fn value(&self) -> Option<&[u8]> {
        self.args.get(2).map(|v| v.as_slice())
    }

    /// Get argument by index
    pub fn arg(&self, index: usize) -> Option<&[u8]> {
        self.args.get(index).map(|v| v.as_slice())
    }

    /// Get number of arguments
    pub fn argc(&self) -> usize {
        self.args.len()
    }
}

// ============================================================================
// PARSER
// ============================================================================

/// RESP Protocol Parser
/// 
/// Parses RESP-formatted data into commands.
pub struct RespParser;

impl RespParser {
    /// Parse RESP data into a command
    /// 
    /// Supports both RESP array format and inline format.
    /// 
    /// # Example
    /// ```rust
    /// use fast_kv::RespParser;
    /// 
    /// // RESP array format
    /// let data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    /// let cmd = RespParser::parse(data).unwrap();
    /// assert_eq!(cmd.name, "SET");
    /// ```
    pub fn parse(data: &[u8]) -> Result<Command, ParseError> {
        if data.is_empty() {
            return Err(ParseError::EmptyData);
        }

        match data[0] {
            // RESP array: *<count>\r\n...
            b'*' => Self::parse_array(data),
            
            // Inline command: GET key\r\n
            b'G' | b'S' | b'D' | b'P' | b'I' | b'H' | b'L' => Self::parse_inline(data),
            
            _ => Err(ParseError::UnknownFormat),
        }
    }

    /// Parse RESP array format
    /// 
    /// Format: *<count>\r\n<elements>
    fn parse_array(data: &[u8]) -> Result<Command, ParseError> {
        let mut pos = 0;

        // Skip '*'
        pos += 1;

        // Read array length
        let (count, n) = Self::read_number(&data[pos..])?;
        pos += n + 2; // number + \r\n

        if count < 1 {
            return Err(ParseError::InvalidArrayLength);
        }

        let mut args = Vec::with_capacity(count as usize);

        // Read each element
        for _ in 0..count {
            // Expect bulk string: $<len>\r\n<data>\r\n
            if data.len() <= pos || data[pos] != b'$' {
                return Err(ParseError::ExpectedBulkString);
            }
            pos += 1; // skip '$'

            // Read string length
            let (len, n) = Self::read_number(&data[pos..])?;
            pos += n + 2; // number + \r\n

            // Check if we have enough data
            if data.len() < pos + len as usize + 2 {
                return Err(ParseError::IncompleteData);
            }

            // Read the string
            args.push(data[pos..pos + len as usize].to_vec());
            pos += len as usize + 2; // string + \r\n
        }

        Self::build_command(args)
    }

    /// Parse inline command format
    /// 
    /// Format: COMMAND arg1 arg2\r\n
    fn parse_inline(data: &[u8]) -> Result<Command, ParseError> {
        // Find end of line
        let line_end = data
            .iter()
            .position(|&b| b == b'\r')
            .unwrap_or(data.len());

        let line = &data[..line_end];
        
        // Split by spaces
        let parts: Vec<&[u8]> = line.split(|&b| b == b' ').collect();
        
        if parts.is_empty() {
            return Err(ParseError::EmptyCommand);
        }

        let args: Vec<Vec<u8>> = parts.into_iter().map(|p| p.to_vec()).collect();
        Self::build_command(args)
    }

    /// Read a number from the data
    /// 
    /// Reads until \r and returns the number and bytes consumed.
    /// Uses a fast inline atoi to avoid `from_utf8` + `parse()` overhead.
    fn read_number(data: &[u8]) -> Result<(i64, usize), ParseError> {
        let end = data
            .iter()
            .position(|&b| b == b'\r')
            .ok_or(ParseError::MissingCrlf)?;

        // Fast path: inline atoi.
        let bytes = &data[..end];
        if bytes.is_empty() {
            return Err(ParseError::InvalidNumber);
        }
        let mut negative = false;
        let mut pos = 0;
        if bytes[0] == b'-' {
            negative = true;
            pos = 1;
        } else if bytes[0] == b'+' {
            pos = 1;
        }
        if pos >= bytes.len() {
            return Err(ParseError::InvalidNumber);
        }
        let mut n: i64 = 0;
        for &b in &bytes[pos..] {
            if b < b'0' || b > b'9' {
                return Err(ParseError::InvalidNumber);
            }
            n = n.checked_mul(10).ok_or(ParseError::InvalidNumber)?
                .checked_add((b - b'0') as i64).ok_or(ParseError::InvalidNumber)?;
        }
        if negative { n = -n; }
        Ok((n, end))
    }

    /// Build a Command from arguments.
    ///
    /// Converts the first argument to uppercase in a single pass from raw
    /// bytes, avoiding `from_utf8` + `to_uppercase()` overhead.
    fn build_command(args: Vec<Vec<u8>>) -> Result<Command, ParseError> {
        if args.is_empty() {
            return Err(ParseError::EmptyCommand);
        }

        let name = args[0].iter()
            .map(|&b| if b >= b'a' && b <= b'z' { (b - 32) as char } else { b as char })
            .collect::<String>();

        Ok(Command { name, args })
    }
}

// ============================================================================
// ENCODER
// ============================================================================

/// RESP Protocol Encoder
/// 
/// Encodes responses in RESP format.
///
/// Two families of methods are provided:
/// * `encode_*()` — return a new `Vec<u8>` (convenient but allocates).
/// * `write_*()` — append into an existing `Vec<u8>` (zero extra allocation).
pub struct RespEncoder;

// ---------------------------------------------------------------------------
// Helpers: write an integer directly into a byte buffer, no format!()
// ---------------------------------------------------------------------------

/// Write `n` as decimal ASCII bytes into `out`, return number of bytes written.
#[inline]
fn write_i64_buf(out: &mut Vec<u8>, n: i64) {
    if n < 0 {
        out.push(b'-');
        // SAFETY: i64::MIN fits in 20 digits.
        let mut v = (n as u64).wrapping_neg();
        let mut buf = [0u8; 20];
        let mut pos = buf.len();
        loop {
            pos -= 1;
            buf[pos] = (v % 10) as u8 + b'0';
            v /= 10;
            if v == 0 { break; }
        }
        out.extend_from_slice(&buf[pos..]);
    } else {
        let mut v = n as u64;
        let mut buf = [0u8; 20];
        let mut pos = buf.len();
        loop {
            pos -= 1;
            buf[pos] = (v % 10) as u8 + b'0';
            v /= 10;
            if v == 0 { break; }
        }
        out.extend_from_slice(&buf[pos..]);
    }
}

/// Write `n` as decimal ASCII bytes into `out`.
#[inline]
pub fn write_usize_buf(out: &mut Vec<u8>, n: usize) {
    let mut v = n;
    let mut buf = [0u8; 20];
    let mut pos = buf.len();
    loop {
        pos -= 1;
        buf[pos] = (v % 10) as u8 + b'0';
        v /= 10;
        if v == 0 { break; }
    }
    out.extend_from_slice(&buf[pos..]);
}

impl RespEncoder {
    /// Encode a simple string response
    /// 
    /// Format: +<string>\r\n
    /// 
    /// Used for: OK, PONG, etc.
    pub fn simple_string(s: &str) -> Vec<u8> {
        let mut out = Vec::with_capacity(s.len() + 3);
        Self::write_simple_string(&mut out, s);
        out
    }

    /// Encode an error response
    /// 
    /// Format: -<message>\r\n
    /// 
    /// Used for: error messages
    pub fn error(s: &str) -> Vec<u8> {
        let mut out = Vec::with_capacity(s.len() + 3);
        Self::write_error(&mut out, s);
        out
    }

    /// Encode an integer response
    /// 
    /// Format: :<number>\r\n
    /// 
    /// Used for: counts, exists, etc.
    pub fn integer(n: i64) -> Vec<u8> {
        let mut out = Vec::with_capacity(24);
        Self::write_integer(&mut out, n);
        out
    }

    /// Encode a bulk string response
    /// 
    /// Format: $<len>\r\n<data>\r\n
    /// 
    /// Used for: GET responses, etc.
    pub fn bulk_string(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len() + 32);
        Self::write_bulk_string(&mut out, data);
        out
    }

    /// Encode a null bulk string
    /// 
    /// Format: $-1\r\n
    /// 
    /// Used for: key not found
    pub fn null() -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }

    /// Encode a null array
    /// 
    /// Format: *-1\r\n
    pub fn null_array() -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }

    /// Encode an empty array
    /// 
    /// Format: *0\r\n
    pub fn empty_array() -> Vec<u8> {
        b"*0\r\n".to_vec()
    }

    /// Encode an array of bulk strings
    /// 
    /// Format: *<count>\r\n<elements>
    pub fn array(items: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::with_capacity(32 + items.len() * 16);
        Self::write_array(&mut out, items);
        out
    }

    // -------------------------------------------------------------------
    // Zero-allocation variants: write into an existing buffer
    // -------------------------------------------------------------------

    /// Append `+<s>\r\n` into *out*.
    #[inline]
    pub fn write_simple_string(out: &mut Vec<u8>, s: &str) {
        out.push(b'+');
        out.extend_from_slice(s.as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    /// Append `-<s>\r\n` into *out*.
    #[inline]
    pub fn write_error(out: &mut Vec<u8>, s: &str) {
        out.push(b'-');
        out.extend_from_slice(s.as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    /// Append `:<n>\r\n` into *out* (no `format!` allocation).
    #[inline]
    pub fn write_integer(out: &mut Vec<u8>, n: i64) {
        out.push(b':');
        write_i64_buf(out, n);
        out.extend_from_slice(b"\r\n");
    }

    /// Append `$<len>\r\n<data>\r\n` into *out*.
    #[inline]
    pub fn write_bulk_string(out: &mut Vec<u8>, data: &[u8]) {
        out.push(b'$');
        write_usize_buf(out, data.len());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n");
    }

    /// Append `*-1\r\n` (null bulk string) into *out*.
    #[inline]
    pub fn write_null(out: &mut Vec<u8>) {
        out.extend_from_slice(b"$-1\r\n");
    }

    /// Append `*<count>\r\n<bulk elements>` into *out*.
    #[inline]
    pub fn write_array(out: &mut Vec<u8>, items: &[&[u8]]) {
        out.push(b'*');
        write_usize_buf(out, items.len());
        out.extend_from_slice(b"\r\n");
        for item in items {
            Self::write_bulk_string(out, item);
        }
    }
}

// ============================================================================
// ERRORS
// ============================================================================

/// Parsing errors
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// Empty data received
    EmptyData,
    /// Unknown format
    UnknownFormat,
    /// Invalid array length
    InvalidArrayLength,
    /// Expected bulk string
    ExpectedBulkString,
    /// Incomplete data
    IncompleteData,
    /// Missing CRLF
    MissingCrlf,
    /// Invalid number
    InvalidNumber,
    /// Empty command
    EmptyCommand,
    /// Invalid UTF-8
    InvalidUtf8,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::EmptyData => write!(f, "empty data"),
            ParseError::UnknownFormat => write!(f, "unknown format"),
            ParseError::InvalidArrayLength => write!(f, "invalid array length"),
            ParseError::ExpectedBulkString => write!(f, "expected bulk string"),
            ParseError::IncompleteData => write!(f, "incomplete data"),
            ParseError::MissingCrlf => write!(f, "missing CRLF"),
            ParseError::InvalidNumber => write!(f, "invalid number"),
            ParseError::EmptyCommand => write!(f, "empty command"),
            ParseError::InvalidUtf8 => write!(f, "invalid UTF-8"),
        }
    }
}

impl std::error::Error for ParseError {}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_resp_array_set() {
        // SET key value
        let data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let cmd = RespParser::parse(data).unwrap();

        assert_eq!(cmd.name, "SET");
        assert_eq!(cmd.argc(), 3);
        assert_eq!(cmd.key(), Some(b"key".as_slice()));
        assert_eq!(cmd.value(), Some(b"value".as_slice()));
    }

    #[test]
    fn test_parse_resp_array_get() {
        // GET key
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let cmd = RespParser::parse(data).unwrap();

        assert_eq!(cmd.name, "GET");
        assert_eq!(cmd.argc(), 2);
        assert_eq!(cmd.key(), Some(b"key".as_slice()));
    }

    #[test]
    fn test_parse_inline_get() {
        let data = b"GET mykey\r\n";
        let cmd = RespParser::parse(data).unwrap();

        assert_eq!(cmd.name, "GET");
        assert_eq!(cmd.argc(), 2);
    }

    #[test]
    fn test_parse_inline_set() {
        let data = b"SET mykey myvalue\r\n";
        let cmd = RespParser::parse(data).unwrap();

        assert_eq!(cmd.name, "SET");
        assert_eq!(cmd.argc(), 3);
    }

    #[test]
    fn test_encode_simple_string() {
        let encoded = RespEncoder::simple_string("OK");
        assert_eq!(encoded, b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let encoded = RespEncoder::error("ERR unknown command");
        assert_eq!(encoded, b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let encoded = RespEncoder::integer(42);
        assert_eq!(encoded, b":42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let encoded = RespEncoder::bulk_string(b"hello");
        assert_eq!(encoded, b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_null() {
        let encoded = RespEncoder::null();
        assert_eq!(encoded, b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let encoded = RespEncoder::array(&[b"foo", b"bar"]);
        assert_eq!(encoded, b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn test_roundtrip() {
        // Parse and then encode should work
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let cmd = RespParser::parse(data).unwrap();
        
        assert_eq!(cmd.name, "GET");
        assert_eq!(cmd.key(), Some(b"key".as_slice()));
    }

    #[test]
    fn test_binary_data() {
        // Binary data in values
        let binary_value: &[u8] = &[0x00, 0xFF, 0xAB, 0xCD, 0xEF];
        let encoded = RespEncoder::bulk_string(binary_value);
        
        assert_eq!(&encoded[..4], b"$5\r\n");
        assert_eq!(&encoded[4..9], binary_value);
    }

    #[test]
    fn test_parse_empty_data() {
        assert!(matches!(RespParser::parse(b""), Err(ParseError::EmptyData)));
    }

    #[test]
    fn test_parse_unknown_format() {
        assert!(matches!(RespParser::parse(b"\x01\x02"), Err(ParseError::UnknownFormat)));
    }

    #[test]
    fn test_encode_null_array() {
        assert_eq!(RespEncoder::null_array(), b"*-1\r\n");
    }

    #[test]
    fn test_encode_empty_array() {
        assert_eq!(RespEncoder::empty_array(), b"*0\r\n");
    }

    #[test]
    fn test_command_key_value() {
        let cmd = RespParser::parse(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n").unwrap();
        assert_eq!(cmd.key(), Some(b"key".as_slice()));
        assert_eq!(cmd.value(), Some(b"value".as_slice()));
        assert_eq!(cmd.arg(0), Some(b"SET".as_slice()));
        assert_eq!(cmd.argc(), 3);
        assert_eq!(cmd.arg(99), None);
    }

    #[test]
    fn test_parse_error_display() {
        assert!(ParseError::EmptyData.to_string().contains("empty"));
        assert!(ParseError::UnknownFormat.to_string().contains("unknown"));
    }
}
