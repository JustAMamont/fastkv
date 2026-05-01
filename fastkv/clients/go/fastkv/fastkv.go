// Package fastkv provides a lightweight Go client SDK for FastKV,
// a Redis-compatible key-value store that speaks the RESP (REdis Serialization Protocol)
// over TCP. This package implements RESP encoding/decoding from scratch and depends
// only on the Go standard library.
//
// # Quick Start
//
//      client := fastkv.NewClient("localhost:8379")
//      defer client.Close()
//
//      err := client.Set("hello", "world")
//      if err != nil {
//              log.Fatal(err)
//      }
//
//      val, err := client.Get("hello")
//      if err != nil {
//              log.Fatal(err)
//      }
//      fmt.Println(val) // world
package fastkv

import (
        "bufio"
        "errors"
        "fmt"
        "io"
        "net"
        "strconv"
        "strings"
        "sync"
        "time"
)

// ---------------------------------------------------------------------------
// RESP value types
// ---------------------------------------------------------------------------

// respType represents the five RESP data types.
type respType byte

const (
        respSimpleString respType = '+'
        respError        respType = '-'
        respInteger      respType = ':'
        respBulkString   respType = '$'
        respArray        respType = '*'
)

// Reply is the parsed representation of a RESP response from the server.
// Callers typically do not interact with this type directly; the Client
// methods unmarshal replies into concrete Go types. It is exported so that
// Pipeline consumers can inspect raw responses.
type Reply struct {
        Type  respType
        Str   string   // for SimpleString, Error, BulkString (empty on null)
        Int   int64    // for Integer
        Array []Reply  // for Array
        Null  bool     // true when BulkString is $-1 or Array is *-1
}

// String returns a human-readable representation of the reply.
func (r Reply) String() string {
        switch r.Type {
        case respSimpleString:
                return fmt.Sprintf("+%s", r.Str)
        case respError:
                return fmt.Sprintf("-%s", r.Str)
        case respInteger:
                return fmt.Sprintf(":%d", r.Int)
        case respBulkString:
                if r.Null {
                        return "(nil)"
                }
                return fmt.Sprintf("%q", r.Str)
        case respArray:
                if r.Null {
                        return "(nil array)"
                }
                var sb strings.Builder
                sb.WriteByte('[')
                for i, v := range r.Array {
                        if i > 0 {
                                sb.WriteString(", ")
                        }
                        sb.WriteString(v.String())
                }
                sb.WriteByte(']')
                return sb.String()
        default:
                return "<unknown>"
        }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
        // ErrNil indicates that the server returned a nil bulk string (key does not exist, etc.).
        ErrNil = errors.New("fastkv: nil reply")

        // ErrProtocol is returned when the server sends an unexpected RESP response.
        ErrProtocol = errors.New("fastkv: protocol error")

        // ErrClosed indicates the client connection has been closed.
        ErrClosed = errors.New("fastkv: connection closed")
)

// RespError wraps a server-side error message (RESP error reply).
// Callers can use errors.As(err, &fastkv.RespError{}) to detect and inspect
// server-side errors.
type RespError struct {
        Msg string
}

func (e *RespError) Error() string { return e.Msg }

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

// Client is a FastKV client that communicates over a single TCP connection
// using the RESP protocol. It is safe for concurrent use by multiple goroutines.
type Client struct {
        mu     sync.Mutex
        conn   net.Conn
        rdr    *bufio.Reader
        addr   string
        closed bool
        // DialTimeout is the timeout for establishing a TCP connection. Default: 5s.
        DialTimeout time.Duration
        // ReadTimeout is the per-operation read deadline. Default: 5s.
        ReadTimeout time.Duration
}

// DefaultDialTimeout is the default connection timeout.
const DefaultDialTimeout = 5 * time.Second

// DefaultReadTimeout is the default per-operation read/write timeout.
const DefaultReadTimeout = 5 * time.Second

// NewClient creates a Client that will connect to the FastKV server at addr
// (e.g. "localhost:8379").  The connection is established lazily on the first
// command.  Use DialTimeout / ReadTimeout to customise time limits.
func NewClient(addr string) *Client {
        return &Client{
                addr:         addr,
                DialTimeout:  DefaultDialTimeout,
                ReadTimeout:  DefaultReadTimeout,
        }
}

// dial ensures the TCP connection is established. It is called lazily on the
// first command so that construction of the Client never fails.
// The connection is guarded by a 5-second dial timeout (configurable via DialTimeout).
func (c *Client) dial() error {
        c.mu.Lock()
        defer c.mu.Unlock()
        if c.conn != nil {
                return nil
        }
        conn, err := net.DialTimeout("tcp", c.addr, c.DialTimeout)
        if err != nil {
                return fmt.Errorf("fastkv: dial %s: %w", c.addr, err)
        }
        c.conn = conn
        c.rdr = bufio.NewReaderSize(conn, 32*1024)
        return nil
}

// resetConn safely closes and nils the connection under the mutex.
// Called after an I/O error to force reconnection on next use.
func (c *Client) resetConn() {
        c.mu.Lock()
        defer c.mu.Unlock()
        if c.conn != nil {
                c.conn.Close()
                c.conn = nil
                c.rdr = nil
        }
}

// Close closes the underlying TCP connection. It is safe to call multiple times.
func (c *Client) Close() error {
        c.mu.Lock()
        defer c.mu.Unlock()
        if c.closed {
                return nil
        }
        c.closed = true
        if c.conn != nil {
                return c.conn.Close()
        }
        return nil
}

// do sends a command and returns the parsed reply. It acquires the connection
// mutex for the entire send+recv sequence so that concurrent goroutines
// cannot interleave commands on the shared TCP connection.
// A per-operation read deadline is set to prevent indefinite blocking.
// On any I/O error the connection is reset so the next call will reconnect.
func (c *Client) do(args ...string) (Reply, error) {
        if err := c.dial(); err != nil {
                return Reply{}, err
        }
        c.mu.Lock()
        defer c.mu.Unlock()
        if c.conn == nil {
                return Reply{}, ErrClosed
        }
        // Apply a per-operation deadline so reads cannot block forever.
        deadline := time.Now().Add(c.ReadTimeout)
        if err := c.conn.SetDeadline(deadline); err != nil {
                c.resetConn()
                return Reply{}, fmt.Errorf("fastkv: set deadline: %w", err)
        }
        buf := encodeArray(args)
        if _, err := c.conn.Write(buf); err != nil {
                c.resetConn()
                return Reply{}, fmt.Errorf("fastkv: send %s: %w", args[0], err)
        }
        reply, err := parseReply(c.rdr)
        if err != nil {
                c.resetConn()
                return Reply{}, err
        }
        // Clear deadline so the connection stays alive for future ops.
        _ = c.conn.SetDeadline(time.Time{})
        return reply, nil
}

// doString sends a command and expects a simple-string or bulk-string reply.
func (c *Client) doString(args ...string) (string, error) {
        r, err := c.do(args...)
        if err != nil {
                return "", err
        }
        if r.Type == respError {
                return "", &RespError{Msg: r.Str}
        }
        if r.Type == respBulkString && r.Null {
                return "", ErrNil
        }
        return r.Str, nil
}

// doInt sends a command and expects an integer reply.
func (c *Client) doInt(args ...string) (int64, error) {
        r, err := c.do(args...)
        if err != nil {
                return 0, err
        }
        if r.Type == respError {
                return 0, &RespError{Msg: r.Str}
        }
        return r.Int, nil
}

// doBool sends a command and expects an integer reply interpreted as a boolean
// (1 = true, 0 = false).
func (c *Client) doBool(args ...string) (bool, error) {
        r, err := c.do(args...)
        if err != nil {
                return false, err
        }
        if r.Type == respError {
                return false, &RespError{Msg: r.Str}
        }
        return r.Int == 1, nil
}

// doSetCondition sends a SET command with NX or XX and interprets the reply.
// FastKV returns +OK (simple string) on success, $-1 (null bulk string) on no-op.
func (c *Client) doSetCondition(args ...string) (bool, error) {
        r, err := c.do(args...)
        if err != nil {
                return false, err
        }
        if r.Type == respError {
                return false, &RespError{Msg: r.Str}
        }
        if r.Type == respBulkString && r.Null {
                return false, nil // key already exists (NX) or missing (XX)
        }
        return true, nil // simple string "OK" = condition met, key was set
}

// doStringSlice sends a command and expects an array-of-bulk-strings reply.
func (c *Client) doStringSlice(args ...string) ([]string, error) {
        r, err := c.do(args...)
        if err != nil {
                return nil, err
        }
        if r.Type == respError {
                return nil, &RespError{Msg: r.Str}
        }
        if r.Type == respArray {
                if r.Null {
                        return nil, nil
                }
                out := make([]string, len(r.Array))
                for i, v := range r.Array {
                        if v.Null {
                                out[i] = ""
                                continue
                        }
                        out[i] = v.Str
                }
                return out, nil
        }
        return nil, fmt.Errorf("%w: expected array, got %c", ErrProtocol, r.Type)
}

// ---------------------------------------------------------------------------
// RESP encoding helpers
// ---------------------------------------------------------------------------

// encodeArray encodes a RESP array of bulk strings. Each element is written
// as a bulk string: $len\r\ndata\r\n. The array header is *N\r\n.
func encodeArray(parts []string) []byte {
        // Pre-calculate capacity to avoid repeated allocations.
        cap := 1 + numLen(len(parts)) + 2 // *N\r\n
        for _, p := range parts {
                cap += 1 + numLen(len(p)) + 2 + len(p) + 2 // $len\r\ndata\r\n
        }
        buf := make([]byte, 0, cap)
        buf = append(buf, '*')
        buf = strconv.AppendInt(buf, int64(len(parts)), 10)
        buf = append(buf, '\r', '\n')
        for _, p := range parts {
                buf = append(buf, '$')
                buf = strconv.AppendInt(buf, int64(len(p)), 10)
                buf = append(buf, '\r', '\n')
                buf = append(buf, p...)
                buf = append(buf, '\r', '\n')
        }
        return buf
}

func numLen(n int) int {
        if n <= 0 {
                return 1
        }
        l := 0
        for n > 0 {
                l++
                n /= 10
        }
        return l
}

// ---------------------------------------------------------------------------
// RESP decoding
// ---------------------------------------------------------------------------

// parseReply reads one complete RESP value from the buffered reader.
func parseReply(r *bufio.Reader) (Reply, error) {
        line, err := r.ReadString('\n')
        if err != nil {
                return Reply{}, fmt.Errorf("fastkv: read reply line: %w", err)
        }
        if len(line) < 3 {
                return Reply{}, fmt.Errorf("%w: line too short: %q", ErrProtocol, line)
        }
        // Strip trailing \r\n.
        line = line[:len(line)-2]

        switch respType(line[0]) {
        case respSimpleString:
                return Reply{Type: respSimpleString, Str: line[1:]}, nil

        case respError:
                return Reply{Type: respError, Str: line[1:]}, nil

        case respInteger:
                v, err := strconv.ParseInt(line[1:], 10, 64)
                if err != nil {
                        return Reply{}, fmt.Errorf("%w: bad integer %q: %w", ErrProtocol, line[1:], err)
                }
                return Reply{Type: respInteger, Int: v}, nil

        case respBulkString:
                return parseBulkString(r, line[1:])

        case respArray:
                return parseArray(r, line[1:])

        default:
                return Reply{}, fmt.Errorf("%w: unknown type byte %c", ErrProtocol, line[0])
        }
}

// parseBulkString reads the bulk string payload following the $N\r\n header.
func parseBulkString(r *bufio.Reader, lenStr string) (Reply, error) {
        n, err := strconv.ParseInt(lenStr, 10, 64)
        if err != nil {
                return Reply{}, fmt.Errorf("%w: bad bulk length %q: %w", ErrProtocol, lenStr, err)
        }
        if n < 0 {
                // Null bulk string.
                return Reply{Type: respBulkString, Null: true}, nil
        }
        data := make([]byte, n)
        if _, err := io.ReadFull(r, data); err != nil {
                return Reply{}, fmt.Errorf("fastkv: read bulk payload: %w", err)
        }
        // Consume trailing \r\n.
        crlf := make([]byte, 2)
        if _, err := io.ReadFull(r, crlf); err != nil {
                return Reply{}, fmt.Errorf("fastkv: read bulk CRLF: %w", err)
        }
        return Reply{Type: respBulkString, Str: string(data)}, nil
}

// parseArray reads N sub-replies following the *N\r\n header.
func parseArray(r *bufio.Reader, lenStr string) (Reply, error) {
        n, err := strconv.ParseInt(lenStr, 10, 64)
        if err != nil {
                return Reply{}, fmt.Errorf("%w: bad array length %q: %w", ErrProtocol, lenStr, err)
        }
        if n < 0 {
                // Null array.
                return Reply{Type: respArray, Null: true}, nil
        }
        arr := make([]Reply, n)
        for i := int64(0); i < n; i++ {
                arr[i], err = parseReply(r)
                if err != nil {
                        return Reply{}, err
                }
        }
        return Reply{Type: respArray, Array: arr}, nil
}

// ---------------------------------------------------------------------------
// Core commands
// ---------------------------------------------------------------------------

// Ping sends a PING command. It returns the server's response string (usually
// "PONG", or the echo argument if one was provided).
func (c *Client) Ping() (string, error) {
        return c.doString("PING")
}

// Echo sends the server a message and returns it back.
func (c *Client) Echo(msg string) (string, error) {
        return c.doString("ECHO", msg)
}

// Info returns the server's INFO output as a string.
func (c *Client) Info() (string, error) {
        return c.doString("INFO")
}

// Dbsize returns the number of keys stored in the currently selected database.
func (c *Client) Dbsize() (int64, error) {
        return c.doInt("DBSIZE")
}

// Quit asks the server to close the connection.
func (c *Client) Quit() error {
        _, err := c.do("QUIT")
        return err
}

// ---------------------------------------------------------------------------
// String commands
// ---------------------------------------------------------------------------

// Set stores key with value. If the key already exists it is overwritten.
// For finer control use SetNX, SetXX, SetEx, or SetPx.
func (c *Client) Set(key, value string) error {
        r, err := c.do("SET", key, value)
        if err != nil {
                return err
        }
        if r.Type == respError {
                return &RespError{Msg: r.Str}
        }
        return nil
}

// SetNX sets key to value only if the key does not already exist. It returns
// true if the key was set, false otherwise.
func (c *Client) SetNX(key, value string) (bool, error) {
        return c.doSetCondition("SET", key, value, "NX")
}

// SetXX sets key to value only if the key already exists. It returns true if
// the key was set, false otherwise.
func (c *Client) SetXX(key, value string) (bool, error) {
        return c.doSetCondition("SET", key, value, "XX")
}

// SetEx sets key to hold the string value and set the key to timeout after the
// given number of seconds.
func (c *Client) SetEx(key, value string, seconds int) error {
        r, err := c.do("SETEX", key, strconv.Itoa(seconds), value)
        if err != nil {
                return err
        }
        if r.Type == respError {
                return &RespError{Msg: r.Str}
        }
        return nil
}

// SetPx sets key to hold the string value and set the key to timeout after the
// given number of milliseconds.
func (c *Client) SetPx(key, value string, ms int) error {
        r, err := c.do("SET", key, value, "PX", strconv.Itoa(ms))
        if err != nil {
                return err
        }
        if r.Type == respError {
                return &RespError{Msg: r.Str}
        }
        return nil
}

// Get returns the value of key. If the key does not exist ErrNil is returned.
func (c *Client) Get(key string) (string, error) {
        return c.doString("GET", key)
}

// Del removes the specified keys. It returns the number of keys that were
// removed.
func (c *Client) Del(keys ...string) (int64, error) {
        args := make([]string, 1+len(keys))
        args[0] = "DEL"
        copy(args[1:], keys)
        return c.doInt(args...)
}

// Exists returns the number of keys that exist among the specified keys.
func (c *Client) Exists(keys ...string) (int64, error) {
        args := make([]string, 1+len(keys))
        args[0] = "EXISTS"
        copy(args[1:], keys)
        return c.doInt(args...)
}

// Incr increments the integer value of key by one. If the key does not exist
// it is set to 0 before performing the operation.
func (c *Client) Incr(key string) (int64, error) {
        return c.doInt("INCR", key)
}

// Decr decrements the integer value of key by one.
func (c *Client) Decr(key string) (int64, error) {
        return c.doInt("DECR", key)
}

// IncrBy increments the integer value of key by delta.
func (c *Client) IncrBy(key string, delta int64) (int64, error) {
        return c.doInt("INCRBY", key, strconv.FormatInt(delta, 10))
}

// DecrBy decrements the integer value of key by delta.
func (c *Client) DecrBy(key string, delta int64) (int64, error) {
        return c.doInt("DECRBY", key, strconv.FormatInt(delta, 10))
}

// Append appends value at the end of the string stored at key. If key does not
// exist it is created and set to value. It returns the length of the string
// after the operation.
func (c *Client) Append(key, value string) (int64, error) {
        return c.doInt("APPEND", key, value)
}

// Strlen returns the length of the string value stored at key.
func (c *Client) Strlen(key string) (int64, error) {
        return c.doInt("STRLEN", key)
}

// GetRange returns the substring of the string value stored at key, determined
// by the offsets start and end (both inclusive).
func (c *Client) GetRange(key string, start, end int) (string, error) {
        return c.doString("GETRANGE", key, strconv.Itoa(start), strconv.Itoa(end))
}

// SetRange overwrites part of the string stored at key, starting at offset, with
// value. It returns the length of the string after the operation.
func (c *Client) SetRange(key string, offset int, value string) (int64, error) {
        return c.doInt("SETRANGE", key, strconv.Itoa(offset), value)
}

// MSet sets multiple keys to multiple values in a single atomic operation.
func (c *Client) MSet(pairs map[string]string) error {
        if len(pairs) == 0 {
                return nil
        }
        args := make([]string, 1, 1+len(pairs)*2)
        args[0] = "MSET"
        for k, v := range pairs {
                args = append(args, k, v)
        }
        r, err := c.do(args...)
        if err != nil {
                return err
        }
        if r.Type == respError {
                return &RespError{Msg: r.Str}
        }
        return nil
}

// MGet returns the values of all specified keys. For every key that does not
// hold a string value or does not exist, the corresponding element in the
// returned slice is "".
func (c *Client) MGet(keys ...string) ([]string, error) {
        args := make([]string, 1+len(keys))
        args[0] = "MGET"
        copy(args[1:], keys)
        return c.doStringSlice(args...)
}

// ---------------------------------------------------------------------------
// TTL commands
// ---------------------------------------------------------------------------

// Expire sets a timeout on key in seconds.
func (c *Client) Expire(key string, seconds int) (bool, error) {
        return c.doBool("EXPIRE", key, strconv.Itoa(seconds))
}

// Ttl returns the remaining time to live of key in seconds.
func (c *Client) Ttl(key string) (int64, error) {
        return c.doInt("TTL", key)
}

// Pttl returns the remaining time to live of key in milliseconds.
func (c *Client) Pttl(key string) (int64, error) {
        return c.doInt("PTTL", key)
}

// Persist removes the existing timeout on key.
func (c *Client) Persist(key string) (bool, error) {
        return c.doBool("PERSIST", key)
}

// ---------------------------------------------------------------------------
// Hash commands
// ---------------------------------------------------------------------------

// HSet sets field in the hash stored at key to value. If key does not exist a
// new hash is created. It returns 1 if the field is new, 0 if the field was
// updated.
func (c *Client) HSet(key, field, value string) (int64, error) {
        return c.doInt("HSET", key, field, value)
}

// HGet returns the value associated with field in the hash stored at key.
func (c *Client) HGet(key, field string) (string, error) {
        return c.doString("HGET", key, field)
}

// HDel removes the specified fields from the hash stored at key. It returns
// the number of fields that were removed.
func (c *Client) HDel(key string, fields ...string) (int64, error) {
        args := make([]string, 2+len(fields))
        args[0] = "HDEL"
        args[1] = key
        copy(args[2:], fields)
        return c.doInt(args...)
}

// HGetAll returns all fields and values of the hash stored at key as a map.
func (c *Client) HGetAll(key string) (map[string]string, error) {
        r, err := c.do("HGETALL", key)
        if err != nil {
                return nil, err
        }
        if r.Type == respError {
                return nil, &RespError{Msg: r.Str}
        }
        if r.Type == respArray {
                if r.Null {
                        return nil, nil
                }
                m := make(map[string]string, len(r.Array)/2)
                for i := 0; i < len(r.Array)-1; i += 2 {
                        m[r.Array[i].Str] = r.Array[i+1].Str
                }
                return m, nil
        }
        return nil, fmt.Errorf("%w: expected array for HGETALL, got %c", ErrProtocol, r.Type)
}

// HExists returns whether field is an existing field in the hash stored at key.
func (c *Client) HExists(key, field string) (bool, error) {
        return c.doBool("HEXISTS", key, field)
}

// HLen returns the number of fields contained in the hash stored at key.
func (c *Client) HLen(key string) (int64, error) {
        return c.doInt("HLEN", key)
}

// HKeys returns all field names in the hash stored at key.
func (c *Client) HKeys(key string) ([]string, error) {
        return c.doStringSlice("HKEYS", key)
}

// HVals returns all values in the hash stored at key.
func (c *Client) HVals(key string) ([]string, error) {
        return c.doStringSlice("HVALS", key)
}

// HMGet returns the values associated with the specified fields in the hash
// stored at key.
func (c *Client) HMGet(key string, fields ...string) ([]string, error) {
        args := make([]string, 2+len(fields))
        args[0] = "HMGET"
        args[1] = key
        copy(args[2:], fields)
        return c.doStringSlice(args...)
}

// HMSet sets multiple field-value pairs in the hash stored at key in a single
// operation.
func (c *Client) HMSet(key string, fields map[string]string) error {
        if len(fields) == 0 {
                return nil
        }
        args := make([]string, 2, 2+len(fields)*2)
        args[0] = "HMSET"
        args[1] = key
        for f, v := range fields {
                args = append(args, f, v)
        }
        r, err := c.do(args...)
        if err != nil {
                return err
        }
        if r.Type == respError {
                return &RespError{Msg: r.Str}
        }
        return nil
}

// ---------------------------------------------------------------------------
// List commands
// ---------------------------------------------------------------------------

// LPush inserts all the specified values at the head of the list stored at key.
// It returns the length of the list after the push operation.
func (c *Client) LPush(key string, elements ...string) (int64, error) {
        args := make([]string, 2+len(elements))
        args[0] = "LPUSH"
        args[1] = key
        copy(args[2:], elements)
        return c.doInt(args...)
}

// RPush inserts all the specified values at the tail of the list stored at key.
// It returns the length of the list after the push operation.
func (c *Client) RPush(key string, elements ...string) (int64, error) {
        args := make([]string, 2+len(elements))
        args[0] = "RPUSH"
        args[1] = key
        copy(args[2:], elements)
        return c.doInt(args...)
}

// LPop removes and returns the first element of the list stored at key.
func (c *Client) LPop(key string) (string, error) {
        return c.doString("LPOP", key)
}

// LPopCount removes and returns up to count elements from the head of the list
// stored at key.
func (c *Client) LPopCount(key string, count int) ([]string, error) {
        return c.doStringSlice("LPOP", key, strconv.Itoa(count))
}

// RPop removes and returns the last element of the list stored at key.
func (c *Client) RPop(key string) (string, error) {
        return c.doString("RPOP", key)
}

// RPopCount removes and returns up to count elements from the tail of the list
// stored at key.
func (c *Client) RPopCount(key string, count int) ([]string, error) {
        return c.doStringSlice("RPOP", key, strconv.Itoa(count))
}

// LRange returns the specified elements of the list stored at key. The offsets
// start and stop are zero-based and inclusive.
func (c *Client) LRange(key string, start, stop int) ([]string, error) {
        return c.doStringSlice("LRANGE", key, strconv.Itoa(start), strconv.Itoa(stop))
}

// LLen returns the length of the list stored at key.
func (c *Client) LLen(key string) (int64, error) {
        return c.doInt("LLEN", key)
}

// LIndex returns the element at index in the list stored at key.
func (c *Client) LIndex(key string, index int) (string, error) {
        return c.doString("LINDEX", key, strconv.Itoa(index))
}

// LRem removes the first count occurrences of element from the list stored at
// key. It returns the number of removed elements.
func (c *Client) LRem(key string, count int, element string) (int64, error) {
        return c.doInt("LREM", key, strconv.Itoa(count), element)
}

// LTrim trims an existing list so that it contains only the specified range of
// elements specified by start and stop (inclusive, zero-based).
func (c *Client) LTrim(key string, start, stop int) error {
        r, err := c.do("LTRIM", key, strconv.Itoa(start), strconv.Itoa(stop))
        if err != nil {
                return err
        }
        if r.Type == respError {
                return &RespError{Msg: r.Str}
        }
        return nil
}

// LSet sets the element at index to element in the list stored at key.
func (c *Client) LSet(key string, index int, element string) error {
        r, err := c.do("LSET", key, strconv.Itoa(index), element)
        if err != nil {
                return err
        }
        if r.Type == respError {
                return &RespError{Msg: r.Str}
        }
        return nil
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

// Pipeline batches multiple commands and sends them to the server in a single
// round-trip when Execute is called. It is NOT goroutine-safe; create a new
// Pipeline for each batch.
type Pipeline struct {
        client *Client
        cmds   [][]string
}

// Pipeline returns a new Pipeline bound to this client. The pipeline collects
// commands via its cmd methods and sends them atomically with Execute.
//
// Example:
//
//      p := client.Pipeline()
//      p.Set("a", "1")
//      p.Set("b", "2")
//      p.Get("a")
//      res, err := p.Execute()
func (c *Client) Pipeline() *Pipeline {
        return &Pipeline{client: c}
}

// ---------------------------------------------------------------------------
// Pipeline — Core commands
// ---------------------------------------------------------------------------

// Ping queues a PING command.
func (p *Pipeline) Ping() { p.cmds = append(p.cmds, []string{"PING"}) }

// Echo queues an ECHO command.
func (p *Pipeline) Echo(msg string) { p.cmds = append(p.cmds, []string{"ECHO", msg}) }

// Info queues an INFO command.
func (p *Pipeline) Info() { p.cmds = append(p.cmds, []string{"INFO"}) }

// Dbsize queues a DBSIZE command.
func (p *Pipeline) Dbsize() { p.cmds = append(p.cmds, []string{"DBSIZE"}) }

// ---------------------------------------------------------------------------
// Pipeline — String commands
// ---------------------------------------------------------------------------

// Set queues a SET command.
func (p *Pipeline) Set(key, value string) {
        p.cmds = append(p.cmds, []string{"SET", key, value})
}

// SetNX queues a SET ... NX command.
func (p *Pipeline) SetNX(key, value string) {
        p.cmds = append(p.cmds, []string{"SET", key, value, "NX"})
}

// SetXX queues a SET ... XX command.
func (p *Pipeline) SetXX(key, value string) {
        p.cmds = append(p.cmds, []string{"SET", key, value, "XX"})
}

// SetEx queues a SETEX command.
func (p *Pipeline) SetEx(key, value string, seconds int) {
        p.cmds = append(p.cmds, []string{"SETEX", key, strconv.Itoa(seconds), value})
}

// SetPx queues a SET ... PX command.
func (p *Pipeline) SetPx(key, value string, ms int) {
        p.cmds = append(p.cmds, []string{"SET", key, value, "PX", strconv.Itoa(ms)})
}

// Get queues a GET command.
func (p *Pipeline) Get(key string) {
        p.cmds = append(p.cmds, []string{"GET", key})
}

// Del queues a DEL command.
func (p *Pipeline) Del(keys ...string) {
        args := make([]string, 1+len(keys))
        args[0] = "DEL"
        copy(args[1:], keys)
        p.cmds = append(p.cmds, args)
}

// Exists queues an EXISTS command.
func (p *Pipeline) Exists(keys ...string) {
        args := make([]string, 1+len(keys))
        args[0] = "EXISTS"
        copy(args[1:], keys)
        p.cmds = append(p.cmds, args)
}

// Incr queues an INCR command.
func (p *Pipeline) Incr(key string) {
        p.cmds = append(p.cmds, []string{"INCR", key})
}

// Decr queues a DECR command.
func (p *Pipeline) Decr(key string) {
        p.cmds = append(p.cmds, []string{"DECR", key})
}

// IncrBy queues an INCRBY command.
func (p *Pipeline) IncrBy(key string, delta int64) {
        p.cmds = append(p.cmds, []string{"INCRBY", key, strconv.FormatInt(delta, 10)})
}

// DecrBy queues a DECRBY command.
func (p *Pipeline) DecrBy(key string, delta int64) {
        p.cmds = append(p.cmds, []string{"DECRBY", key, strconv.FormatInt(delta, 10)})
}

// MSet queues an MSET command.
func (p *Pipeline) MSet(pairs map[string]string) {
        if len(pairs) == 0 {
                return
        }
        args := make([]string, 1, 1+len(pairs)*2)
        args[0] = "MSET"
        for k, v := range pairs {
                args = append(args, k, v)
        }
        p.cmds = append(p.cmds, args)
}

// MGet queues an MGET command.
func (p *Pipeline) MGet(keys ...string) {
        args := make([]string, 1+len(keys))
        args[0] = "MGET"
        copy(args[1:], keys)
        p.cmds = append(p.cmds, args)
}

// ---------------------------------------------------------------------------
// Pipeline — TTL commands
// ---------------------------------------------------------------------------

// Expire queues an EXPIRE command.
func (p *Pipeline) Expire(key string, seconds int) {
        p.cmds = append(p.cmds, []string{"EXPIRE", key, strconv.Itoa(seconds)})
}

// Ttl queues a TTL command.
func (p *Pipeline) Ttl(key string) {
        p.cmds = append(p.cmds, []string{"TTL", key})
}

// Pttl queues a PTTL command.
func (p *Pipeline) Pttl(key string) {
        p.cmds = append(p.cmds, []string{"PTTL", key})
}

// Persist queues a PERSIST command.
func (p *Pipeline) Persist(key string) {
        p.cmds = append(p.cmds, []string{"PERSIST", key})
}

// ---------------------------------------------------------------------------
// Pipeline — Hash commands
// ---------------------------------------------------------------------------

// HSet queues an HSET command.
func (p *Pipeline) HSet(key, field, value string) {
        p.cmds = append(p.cmds, []string{"HSET", key, field, value})
}

// HGet queues an HGET command.
func (p *Pipeline) HGet(key, field string) {
        p.cmds = append(p.cmds, []string{"HGET", key, field})
}

// HDel queues an HDEL command.
func (p *Pipeline) HDel(key string, fields ...string) {
        args := make([]string, 2+len(fields))
        args[0] = "HDEL"
        args[1] = key
        copy(args[2:], fields)
        p.cmds = append(p.cmds, args)
}

// HGetAll queues an HGETALL command.
func (p *Pipeline) HGetAll(key string) {
        p.cmds = append(p.cmds, []string{"HGETALL", key})
}

// HExists queues an HEXISTS command.
func (p *Pipeline) HExists(key, field string) {
        p.cmds = append(p.cmds, []string{"HEXISTS", key, field})
}

// HLen queues an HLEN command.
func (p *Pipeline) HLen(key string) {
        p.cmds = append(p.cmds, []string{"HLEN", key})
}

// HKeys queues an HKEYS command.
func (p *Pipeline) HKeys(key string) {
        p.cmds = append(p.cmds, []string{"HKEYS", key})
}

// HVals queues an HVALS command.
func (p *Pipeline) HVals(key string) {
        p.cmds = append(p.cmds, []string{"HVALS", key})
}

// HMGet queues an HMGET command.
func (p *Pipeline) HMGet(key string, fields ...string) {
        args := make([]string, 2+len(fields))
        args[0] = "HMGET"
        args[1] = key
        copy(args[2:], fields)
        p.cmds = append(p.cmds, args)
}

// HMSet queues an HMSET command.
func (p *Pipeline) HMSet(key string, fields map[string]string) {
        if len(fields) == 0 {
                return
        }
        args := make([]string, 2, 2+len(fields)*2)
        args[0] = "HMSET"
        args[1] = key
        for f, v := range fields {
                args = append(args, f, v)
        }
        p.cmds = append(p.cmds, args)
}

// ---------------------------------------------------------------------------
// Pipeline — List commands
// ---------------------------------------------------------------------------

// LPush queues an LPUSH command.
func (p *Pipeline) LPush(key string, elements ...string) {
        args := make([]string, 2+len(elements))
        args[0] = "LPUSH"
        args[1] = key
        copy(args[2:], elements)
        p.cmds = append(p.cmds, args)
}

// RPush queues an RPUSH command.
func (p *Pipeline) RPush(key string, elements ...string) {
        args := make([]string, 2+len(elements))
        args[0] = "RPUSH"
        args[1] = key
        copy(args[2:], elements)
        p.cmds = append(p.cmds, args)
}

// LPop queues an LPOP command.
func (p *Pipeline) LPop(key string) {
        p.cmds = append(p.cmds, []string{"LPOP", key})
}

// RPop queues an RPOP command.
func (p *Pipeline) RPop(key string) {
        p.cmds = append(p.cmds, []string{"RPOP", key})
}

// LRange queues an LRANGE command.
func (p *Pipeline) LRange(key string, start, stop int) {
        p.cmds = append(p.cmds, []string{"LRANGE", key, strconv.Itoa(start), strconv.Itoa(stop)})
}

// LLen queues an LLEN command.
func (p *Pipeline) LLen(key string) {
        p.cmds = append(p.cmds, []string{"LLEN", key})
}

// LIndex queues an LINDEX command.
func (p *Pipeline) LIndex(key string, index int) {
        p.cmds = append(p.cmds, []string{"LINDEX", key, strconv.Itoa(index)})
}

// LRem queues an LREM command.
func (p *Pipeline) LRem(key string, count int, element string) {
        p.cmds = append(p.cmds, []string{"LREM", key, strconv.Itoa(count), element})
}

// LTrim queues an LTRIM command.
func (p *Pipeline) LTrim(key string, start, stop int) {
        p.cmds = append(p.cmds, []string{"LTRIM", key, strconv.Itoa(start), strconv.Itoa(stop)})
}

// LSet queues an LSET command.
func (p *Pipeline) LSet(key string, index int, element string) {
        p.cmds = append(p.cmds, []string{"LSET", key, strconv.Itoa(index), element})
}

// ---------------------------------------------------------------------------
// Pipeline — Execute
// ---------------------------------------------------------------------------

// PipelineResult holds the ordered list of replies returned by a pipeline
// execution. Use the typed accessors (String, Int, Bool, StringSlice,
// StringMap) to unmarshal individual replies.
type PipelineResult struct {
        Replies []Reply
}

// String returns the i-th reply as a string (simple string or bulk string).
// If the reply is a null bulk string, ErrNil is returned.
func (pr *PipelineResult) String(i int) (string, error) {
        if i < 0 || i >= len(pr.Replies) {
                return "", fmt.Errorf("fastkv: pipeline index %d out of range", i)
        }
        r := pr.Replies[i]
        if r.Type == respError {
                return "", &RespError{Msg: r.Str}
        }
        if r.Type == respBulkString && r.Null {
                return "", ErrNil
        }
        return r.Str, nil
}

// Int returns the i-th reply as an integer.
func (pr *PipelineResult) Int(i int) (int64, error) {
        if i < 0 || i >= len(pr.Replies) {
                return 0, fmt.Errorf("fastkv: pipeline index %d out of range", i)
        }
        r := pr.Replies[i]
        if r.Type == respError {
                return 0, &RespError{Msg: r.Str}
        }
        return r.Int, nil
}

// Bool returns the i-th reply as a boolean (integer 1/0).
func (pr *PipelineResult) Bool(i int) (bool, error) {
        if i < 0 || i >= len(pr.Replies) {
                return false, fmt.Errorf("fastkv: pipeline index %d out of range", i)
        }
        r := pr.Replies[i]
        if r.Type == respError {
                return false, &RespError{Msg: r.Str}
        }
        return r.Int == 1, nil
}

// StringSlice returns the i-th reply as a slice of strings.
func (pr *PipelineResult) StringSlice(i int) ([]string, error) {
        if i < 0 || i >= len(pr.Replies) {
                return nil, fmt.Errorf("fastkv: pipeline index %d out of range", i)
        }
        r := pr.Replies[i]
        if r.Type == respError {
                return nil, &RespError{Msg: r.Str}
        }
        if r.Type == respArray {
                if r.Null {
                        return nil, nil
                }
                out := make([]string, len(r.Array))
                for j, v := range r.Array {
                        if v.Null {
                                out[j] = ""
                        } else {
                                out[j] = v.Str
                        }
                }
                return out, nil
        }
        return nil, fmt.Errorf("%w: expected array, got %c", ErrProtocol, r.Type)
}

// StringMap returns the i-th reply as a map[string]string, interpreting a
// flat array of field/value pairs (as returned by HGETALL).
func (pr *PipelineResult) StringMap(i int) (map[string]string, error) {
        if i < 0 || i >= len(pr.Replies) {
                return nil, fmt.Errorf("fastkv: pipeline index %d out of range", i)
        }
        r := pr.Replies[i]
        if r.Type == respError {
                return nil, &RespError{Msg: r.Str}
        }
        if r.Type == respArray {
                if r.Null {
                        return nil, nil
                }
                m := make(map[string]string, len(r.Array)/2)
                for j := 0; j < len(r.Array)-1; j += 2 {
                        m[r.Array[j].Str] = r.Array[j+1].Str
                }
                return m, nil
        }
        return nil, fmt.Errorf("%w: expected array for map, got %c", ErrProtocol, r.Type)
}

// OK checks whether the i-th reply indicates success (simple string "OK").
func (pr *PipelineResult) OK(i int) error {
        if i < 0 || i >= len(pr.Replies) {
                return fmt.Errorf("fastkv: pipeline index %d out of range", i)
        }
        r := pr.Replies[i]
        if r.Type == respError {
                return &RespError{Msg: r.Str}
        }
        return nil
}

// Len returns the number of replies in the result.
func (pr *PipelineResult) Len() int { return len(pr.Replies) }

// Execute flushes all queued commands to the server in a single write, then
// reads back all replies. The returned PipelineResult has one Reply per queued
// command, in the same order they were queued.
func (p *Pipeline) Execute() (*PipelineResult, error) {
        if len(p.cmds) == 0 {
                return &PipelineResult{}, nil
        }

        cl := p.client
        if err := cl.dial(); err != nil {
                return nil, err
        }

        // Acquire the client lock for the entire pipeline round-trip so that
        // no other goroutine interleaves commands on the same connection.
        cl.mu.Lock()
        defer cl.mu.Unlock()

        // Write all commands.
        var buf []byte
        for _, cmd := range p.cmds {
                buf = append(buf, encodeArray(cmd)...)
        }
        if _, err := cl.conn.Write(buf); err != nil {
                return nil, fmt.Errorf("fastkv: pipeline write: %w", err)
        }

        // Read all replies.
        replies := make([]Reply, len(p.cmds))
        for i := range p.cmds {
                r, err := parseReply(cl.rdr)
                if err != nil {
                        return nil, fmt.Errorf("fastkv: pipeline reply %d: %w", i, err)
                }
                replies[i] = r
        }

        return &PipelineResult{Replies: replies}, nil
}
