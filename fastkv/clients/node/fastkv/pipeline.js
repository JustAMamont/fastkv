'use strict';

const net = require('net');
const { encodeCommand, RespDecoder } = require('./resp');
const { FastKVConnectionError, FastKVTimeoutError, FastKVClientClosedError } = require('./errors');

/**
 * Pipeline accumulates commands and sends them all at once to the server.
 *
 * **Important:** The pipeline opens its *own dedicated* TCP connection so that
 * pipeline responses can never interleave with regular commands on the parent
 * client.  The connection is opened eagerly when the pipeline is created via
 * {@link FastKVClient#pipeline} and closed when {@link Pipeline#close} (or
 * {@link Pipeline#discard}) is called.
 *
 * Usage:
 *   const pipe = await client.pipeline();
 *   pipe.set('a', 1).get('a').incr('counter');
 *   const results = await pipe.execute();
 *   pipe.close();
 */
class Pipeline {
  /**
   * @param {string} host
   * @param {number} port
   * @param {number} socketTimeout
   */
  constructor(host, port, socketTimeout) {
    this._host = host;
    this._port = port;
    this._socketTimeout = socketTimeout;

    /** @type {net.Socket|null} */
    this._socket = null;
    /** @type {RespDecoder} */
    this._decoder = new RespDecoder();
    /** @type {boolean} */
    this._closed = false;
    /** @type {Buffer[]} */
    this._commands = [];
  }

  // ── Connection ──────────────────────────────────────────────────────────

  /**
   * Open a dedicated TCP connection for this pipeline.
   * Called by `client.pipeline()` after construction.
   * @returns {Promise<void>}
   */
  _connect() {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection(
        { host: this._host, port: this._port },
        () => {
          clearTimeout(timer);
          resolve();
        },
      );

      const timer = setTimeout(() => {
        socket.destroy();
        reject(new FastKVTimeoutError(
          `Pipeline connection to ${this._host}:${this._port} timed out after ${this._socketTimeout}ms`,
        ));
      }, this._socketTimeout);

      socket.on('error', (err) => {
        clearTimeout(timer);
        reject(new FastKVConnectionError(
          `Pipeline failed to connect to ${this._host}:${this._port} — ${err.message}`,
          err,
        ));
      });

      socket.on('data', (chunk) => {
        this._decoder.feed(chunk);
      });

      socket.on('close', () => {
        this._closed = true;
      });

      this._socket = socket;
    });
  }

  // ── Helpers ─────────────────────────────────────────────────────────────

  /**
   * Push an arbitrary RESP command onto the pipeline.
   * @param {Array<string|number>} args
   * @returns {Pipeline} this
   */
  _addCommand(args) {
    if (this._closed) {
      throw new FastKVClientClosedError('Pipeline is closed');
    }
    this._commands.push(encodeCommand(args));
    return this;
  }

  // ── String commands ──────────────────────────────────────────────────────

  set(key, value, opts) {
    const args = ['SET', key, value];
    if (opts) {
      if (opts.ex != null) args.push('EX', String(opts.ex));
      if (opts.px != null) args.push('PX', String(opts.px));
      if (opts.nx) args.push('NX');
      if (opts.xx) args.push('XX');
    }
    return this._addCommand(args);
  }

  get(key) { return this._addCommand(['GET', key]); }
  del(...keys) { return this._addCommand(['DEL', ...keys]); }
  exists(...keys) { return this._addCommand(['EXISTS', ...keys]); }
  incr(key) { return this._addCommand(['INCR', key]); }
  decr(key) { return this._addCommand(['DECR', key]); }
  incrBy(key, delta) { return this._addCommand(['INCRBY', key, delta]); }
  decrBy(key, delta) { return this._addCommand(['DECRBY', key, delta]); }
  append(key, value) { return this._addCommand(['APPEND', key, value]); }
  strlen(key) { return this._addCommand(['STRLEN', key]); }
  getRange(key, start, end) { return this._addCommand(['GETRANGE', key, start, end]); }
  setRange(key, offset, value) { return this._addCommand(['SETRANGE', key, offset, value]); }

  mset(pairs) {
    const args = ['MSET'];
    for (const [k, v] of Object.entries(pairs)) args.push(k, v);
    return this._addCommand(args);
  }

  mget(...keys) { return this._addCommand(['MGET', ...keys]); }

  // ── TTL commands ─────────────────────────────────────────────────────────

  expire(key, seconds) { return this._addCommand(['EXPIRE', key, seconds]); }
  ttl(key) { return this._addCommand(['TTL', key]); }
  pttl(key) { return this._addCommand(['PTTL', key]); }
  persist(key) { return this._addCommand(['PERSIST', key]); }

  // ── Hash commands ────────────────────────────────────────────────────────

  hset(key, field, value) { return this._addCommand(['HSET', key, field, value]); }
  hget(key, field) { return this._addCommand(['HGET', key, field]); }
  hdel(key, ...fields) { return this._addCommand(['HDEL', key, ...fields]); }
  hgetAll(key) { return this._addCommand(['HGETALL', key]); }
  hexists(key, field) { return this._addCommand(['HEXISTS', key, field]); }
  hlen(key) { return this._addCommand(['HLEN', key]); }
  hkeys(key) { return this._addCommand(['HKEYS', key]); }
  hvals(key) { return this._addCommand(['HVALS', key]); }
  hmget(key, ...fields) { return this._addCommand(['HMGET', key, ...fields]); }

  hmset(key, fields) {
    const args = ['HMSET', key];
    for (const [f, v] of Object.entries(fields)) args.push(f, v);
    return this._addCommand(args);
  }

  // ── List commands ────────────────────────────────────────────────────────

  lpush(key, ...elements) { return this._addCommand(['LPUSH', key, ...elements]); }
  rpush(key, ...elements) { return this._addCommand(['RPUSH', key, ...elements]); }

  lpop(key, count) {
    const args = ['LPOP', key];
    if (count != null) args.push(String(count));
    return this._addCommand(args);
  }

  rpop(key, count) {
    const args = ['RPOP', key];
    if (count != null) args.push(String(count));
    return this._addCommand(args);
  }

  lrange(key, start, stop) { return this._addCommand(['LRANGE', key, start, stop]); }
  llen(key) { return this._addCommand(['LLEN', key]); }
  lindex(key, index) { return this._addCommand(['LINDEX', key, index]); }
  lrem(key, count, element) { return this._addCommand(['LREM', key, count, element]); }
  ltrim(key, start, stop) { return this._addCommand(['LTRIM', key, start, stop]); }
  lset(key, index, element) { return this._addCommand(['LSET', key, index, element]); }

  // ── Server commands ──────────────────────────────────────────────────────

  ping() { return this._addCommand(['PING']); }
  echo(msg) { return this._addCommand(['ECHO', msg]); }
  info() { return this._addCommand(['INFO']); }
  dbsize() { return this._addCommand(['DBSIZE']); }

  // ── Execute ──────────────────────────────────────────────────────────────

  /**
   * Send all buffered commands in one write, then read back one reply per
   * command.  Because the pipeline owns its own connection, no interleaving
   * with other commands is possible.
   *
   * @returns {Promise<Array>} results
   */
  async execute() {
    if (this._commands.length === 0) return [];
    if (this._closed || !this._socket) {
      throw new FastKVClientClosedError('Pipeline connection is closed');
    }

    const count = this._commands.length;
    const payload = Buffer.concat(this._commands);

    // Write all commands at once
    this._socket.write(payload);

    // Read `count` replies from the dedicated connection.
    // Use a simple loop with setTimeout-based timeout per read to avoid
    // blocking forever if the server hangs.
    const results = [];
    for (let i = 0; i < count; i++) {
      results.push(_decode(await this._readReply()));
    }

    // Clear buffered commands so the pipeline cannot be re-executed
    this._commands = [];

    return results;
  }

  /**
   * Read the next reply from the pipeline's own decoder/socket.
   * @private\n   * @returns {Promise<any>}
   */
  _readReply() {
    if (this._closed) {
      return Promise.reject(new FastKVClientClosedError('Pipeline connection is closed'));
    }

    // Check if a reply was already buffered by the 'data' handler
    const cached = this._decoder.read();
    if (cached !== undefined) {
      if (cached instanceof Error) return Promise.reject(cached);
      return Promise.resolve(cached);
    }

    // Wait for the next 'data' event to deliver a complete reply
    return new Promise((resolve, reject) => {
      const onData = () => {
        const reply = this._decoder.read();
        if (reply !== undefined) {
          cleanup();
          if (reply instanceof Error) {
            reject(reply);
          } else {
            resolve(reply);
          }
        }
        // else: data arrived but the reply is not yet complete — keep waiting
      };

      const onClose = () => {
        cleanup();
        reject(new FastKVConnectionError('Pipeline connection closed while reading'));
      };

      const timer = setTimeout(() => {
        cleanup();
        reject(new FastKVTimeoutError(
          `Pipeline reply timed out after ${this._socketTimeout}ms`,
        ));
      }, this._socketTimeout);

      const cleanup = () => {
        clearTimeout(timer);
        this._socket.removeListener('data', onData);
        this._socket.removeListener('close', onClose);
      };

      this._socket.on('data', onData);
      this._socket.on('close', onClose);
    });
  }

  // ── Close / discard ─────────────────────────────────────────────────────

  /**
   * Close the pipeline's dedicated connection.
   * Buffered but un-executed commands are discarded.
   */
  close() {
    this._commands = [];
    if (this._socket) {
      this._socket.destroy();
      this._socket = null;
    }
    this._decoder.destroy();
    this._closed = true;
  }

  /**
   * Alias for {@link Pipeline#close} — discards buffered commands and releases
   * the connection.
   */
  discard() {
    this.close();
  }
}

module.exports = { Pipeline };

// ── Internal utilities ────────────────────────────────────────────────────────

/**
 * Recursively decode Buffer values to UTF-8 strings.
 * @param {any} val
 * @returns {any}
 */
function _decode(val) {
  if (val === null || val === undefined) return val;
  if (Buffer.isBuffer(val)) return val.toString('utf8');
  if (Array.isArray(val)) return val.map(_decode);
  return val;
}