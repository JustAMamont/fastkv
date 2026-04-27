'use strict';

/**
 * RESP (REdis Serialization Protocol) encoder and decoder.
 *
 * RESP types:
 *   Simple Strings  →  +OK\r\n
 *   Errors          →  -ERR message\r\n
 *   Integers        →  :1000\r\n
 *   Bulk Strings    →  $6\r\nfoobar\r\n   or   $-1\r\n  (null)
 *   Arrays          →  *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n   or   *-1\r\n  (null)
 */

const { FastKVResponseError } = require('./errors');

const CRLF = '\r\n';
const CRLF_BUF = Buffer.from(CRLF);

// ── Encoder ──────────────────────────────────────────────────────────────────

/**
 * Encode a single value into a RESP bulk string buffer.
 * @param {string|Buffer|number|boolean|null} value
 * @returns {Buffer}
 */
function encodeBulkString(value) {
  if (value === null || value === undefined) {
    return Buffer.from('$-1\r\n');
  }
  const buf = typeof value === 'string'
    ? Buffer.from(value, 'utf8')
    : Buffer.isBuffer(value)
      ? value
      : Buffer.from(String(value), 'utf8');
  return Buffer.concat([
    Buffer.from(`$${buf.length}\r\n`),
    buf,
    CRLF_BUF,
  ]);
}

/**
 * Encode a RESP command (array of arguments) into a single Buffer.
 * @param {Array<string|Buffer|number>} args
 * @returns {Buffer}
 */
function encodeCommand(args) {
  if (!args || args.length === 0) {
    return Buffer.from('*0\r\n');
  }
  const parts = [Buffer.from(`*${args.length}\r\n`)];
  for (const arg of args) {
    parts.push(encodeBulkString(arg));
  }
  return Buffer.concat(parts);
}

// ── Decoder ──────────────────────────────────────────────────────────────────

/**
 * RESP streaming decoder.
 *
 * Feed chunks of data via {@link RespDecoder#feed} and read decoded
 * values via {@link RespDecoder#read} (or listen for the 'reply' event).
 */
class RespDecoder {
  constructor() {
    this._buffer = Buffer.alloc(0);
    /** @type {any[]} queue of fully parsed replies */
    this._queue = [];
  }

  /**
   * Append raw bytes received from the socket.
   * @param {Buffer} chunk
   */
  feed(chunk) {
    this._buffer = Buffer.concat([this._buffer, chunk]);
    this._tryParse();
  }

  /**
   * Try to parse as many complete RESP messages as possible from the buffer.
   * If a RESP error is encountered it is pushed into the queue as an Error
   * instance so the consumer can reject the appropriate pending promise.
   * @private
   */
  _tryParse() {
    while (this._buffer.length > 0) {
      const startLen = this._buffer.length;
      try {
        const result = this._parseValue();
        // If buffer was not consumed, we need more data — break out
        if (this._buffer.length === startLen && result === undefined) {
          break;
        }
        if (result !== undefined) {
          this._queue.push(result);
        }
      } catch (err) {
        // RESP errors (and other protocol errors) are pushed into the queue
        // so the caller can reject the correct pending promise.
        this._queue.push(err);
        // Safety: if the parse error did NOT consume any buffer bytes,
        // break to prevent an infinite loop (e.g. unknown type byte).
        if (this._buffer.length === startLen) {
          // Advance past the offending byte so the loop can make progress
          this._buffer = this._buffer.subarray(1);
        }
      }
    }
  }

  /**
   * Read the next fully-decoded reply.
   * Returns `undefined` when no complete reply is available yet.
   * Returns `null` for a valid RESP null bulk string ($-1) or null array (*-1).
   * @returns {any|undefined}
   */
  read() {
    if (this._queue.length > 0) {
      return this._queue.shift();
    }
    return undefined;
  }

  /**
   * @private
   * Try to parse one RESP value from the front of the buffer.
   * @returns {any|undefined}  Parsed value, or undefined if incomplete.
   */
  _parseValue() {
    if (this._buffer.length === 0) return undefined;
    const typeByte = this._buffer[0];

    switch (typeByte) {
      case 0x2b: // '+'  Simple String
        return this._parseLine((line) => line);

      case 0x2d: // '-'  Error
        return this._parseLine((line) => {
          throw new FastKVResponseError(line);
        });

      case 0x3a: // ':'  Integer
        return this._parseLine((line) => {
          const n = parseInt(line, 10);
          if (Number.isNaN(n)) throw new FastKVResponseError(`Invalid integer: ${line}`);
          return n;
        });

      case 0x24: // '$'  Bulk String
        return this._parseBulkString();

      case 0x2a: // '*'  Array
        return this._parseArray();

      default:
        throw new FastKVResponseError(
          `Unknown RESP type byte: 0x${typeByte.toString(16).padStart(2, '0')}`,
        );
    }
  }

  /**
   * Parse a line terminated by \r\n.
   * @private
   * @param {(line: string) => any} transform
   * @returns {any|undefined}
   */
  _parseLine(transform) {
    const idx = this._buffer.indexOf(CRLF_BUF);
    if (idx === -1) return undefined;

    // line starts after the type byte (+, -, :)
    const line = this._buffer.subarray(1, idx).toString('utf8');
    this._buffer = this._buffer.subarray(idx + 2); // skip \r\n
    return transform(line);
  }

  /**
   * @private
   * @returns {Buffer|string|null|undefined}
   */
  _parseBulkString() {
    const idx = this._buffer.indexOf(CRLF_BUF);
    if (idx === -1) return undefined;

    const lenStr = this._buffer.subarray(1, idx).toString('utf8');
    const len = parseInt(lenStr, 10);

    if (Number.isNaN(len)) {
      throw new FastKVResponseError(`Invalid bulk string length: ${lenStr}`);
    }

    // Null bulk string
    if (len === -1) {
      this._buffer = this._buffer.subarray(idx + 2);
      return null;
    }

    const totalLen = idx + 2 + len + 2; // header + \r\n + payload + \r\n
    if (this._buffer.length < totalLen) return undefined;

    const payload = this._buffer.subarray(idx + 2, idx + 2 + len);
    this._buffer = this._buffer.subarray(totalLen);
    return payload;
  }

  /**
   * @private
   * @returns {Array|null|undefined}
   */
  _parseArray() {
    const idx = this._buffer.indexOf(CRLF_BUF);
    if (idx === -1) return undefined;

    const countStr = this._buffer.subarray(1, idx).toString('utf8');
    const count = parseInt(countStr, 10);

    if (Number.isNaN(count)) {
      throw new FastKVResponseError(`Invalid array length: ${countStr}`);
    }

    // Null array
    if (count === -1) {
      this._buffer = this._buffer.subarray(idx + 2);
      return null;
    }

    // Save position to roll back if we don't have enough data
    const savedBuffer = this._buffer;

    // Move past the array header
    this._buffer = this._buffer.subarray(idx + 2);

    const arr = new Array(count);
    for (let i = 0; i < count; i++) {
      const val = this._parseValue();
      if (val === undefined) {
        // Incomplete — restore buffer and wait for more data
        this._buffer = savedBuffer;
        return undefined;
      }
      arr[i] = val;
    }
    return arr;
  }

  /**
   * Destroy the decoder and release buffer memory.
   */
  destroy() {
    this._buffer = Buffer.alloc(0);
    this._queue = [];
  }
}

module.exports = {
  encodeCommand,
  encodeBulkString,
  RespDecoder,
  CRLF,
  CRLF_BUF,
};
