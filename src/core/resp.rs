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
    /// use fastkv::RespParser;
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
    fn read_number(data: &[u8]) -> Result<(i64, usize), ParseError> {
        // Find \r
        let end = data
            .iter()
            .position(|&b| b == b'\r')
            .ok_or(ParseError::MissingCrlf)?;

        let num_str = std::str::from_utf8(&data[..end])
            .map_err(|_| ParseError::InvalidNumber)?;

        let num: i64 = num_str
            .parse()
            .map_err(|_| ParseError::InvalidNumber)?;

        Ok((num, end))
    }

    /// Build a Command from arguments
    fn build_command(args: Vec<Vec<u8>>) -> Result<Command, ParseError> {
        if args.is_empty() {
            return Err(ParseError::EmptyCommand);
        }

        // Convert first argument to uppercase command name
        let name = std::str::from_utf8(&args[0])
            .map_err(|_| ParseError::InvalidUtf8)?
            .to_uppercase();

        Ok(Command { name, args })
    }
}

// ============================================================================
// ENCODER
// ============================================================================

/// RESP Protocol Encoder
/// 
/// Encodes responses in RESP format.
pub struct RespEncoder;

impl RespEncoder {
    /// Encode a simple string response
    /// 
    /// Format: +<string>\r\n
    /// 
    /// Used for: OK, PONG, etc.
    pub fn simple_string(s: &str) -> Vec<u8> {
        format!("+{}\r\n", s).into_bytes()
    }

    /// Encode an error response
    /// 
    /// Format: -<message>\r\n
    /// 
    /// Used for: error messages
    pub fn error(s: &str) -> Vec<u8> {
        format!("-{}\r\n", s).into_bytes()
    }

    /// Encode an integer response
    /// 
    /// Format: :<number>\r\n
    /// 
    /// Used for: counts, exists, etc.
    pub fn integer(n: i64) -> Vec<u8> {
        format!(":{}\r\n", n).into_bytes()
    }

    /// Encode a bulk string response
    /// 
    /// Format: $<len>\r\n<data>\r\n
    /// 
    /// Used for: GET responses, etc.
    pub fn bulk_string(data: &[u8]) -> Vec<u8> {
        let mut result = format!("${}\r\n", data.len()).into_bytes();
        result.extend_from_slice(data);
        result.extend_from_slice(b"\r\n");
        result
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
        let mut result = format!("*{}\r\n", items.len()).into_bytes();
        for item in items {
            result.extend_from_slice(&format!("${}\r\n", item.len()).into_bytes());
            result.extend_from_slice(item);
            result.extend_from_slice(b"\r\n");
        }
        result
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
        
        assert_eq!(&encoded[..6], b"$5\r\n");
        assert_eq!(&encoded[6..11], binary_value);
    }
}
