'use strict';

const net = require('net');
const { EventEmitter } = require('events');
const { encodeCommand, RespDecoder } = require('./resp');
const { Pipeline } = require('./pipeline');
const {
  FastKVConnectionError,
  FastKVTimeoutError,
  FastKVClientClosedError,
} = require('./errors');

/**
 * Lightweight async FastKV client using RESP over TCP.
 *
 * Use the static {@link FastKVClient.create} factory to connect:
 *
 * @example
 *   const client = await FastKVClient.create({ host: '127.0.0.1', port: 7379 });
 *   await client.set('foo', 'bar');
 *   const val = await client.get('foo');
 *   await client.close();
 */
class FastKVClient extends EventEmitter {
  /**
   * Create a new (unconnected) FastKVClient instance.
   *
   * **Prefer** the static {@link FastKVClient.create} factory unless you need
   * to set up event listeners before connecting.
   *
   * @param {object}  opts
   * @param {string}  [opts.host='127.0.0.1']
   * @param {number}  [opts.port=7379]
   * @param {number}  [opts.socketTimeout=5000]  — connect timeout in ms
   */
  constructor({
    host = '127.0.0.1',
    port = 7379,
    socketTimeout = 5000,
  } = {}) {
    super();

    /** @type {net.Socket|null} */
    this._socket = null;
    /** @type {RespDecoder} */
    this._decoder = new RespDecoder();
    /** @type {boolean} */
    this._closed = true;
    /** @type {string} */
    this._host = host;
    /** @type {number} */
    this._port = port;
    /** @type {number} */
    this._socketTimeout = socketTimeout;

    /** @type {{ resolve: Function, reject: Function, timer: NodeJS.Timeout }[]} */
    this._pending = [];
  }

  /**
   * Connect to the FastKV server.  Must be called before sending commands
   * (unless you used {@link FastKVClient.create}).
   *
   * @returns {Promise<FastKVClient>} resolves to `this` once connected
   */
  async connect() {
    if (this._socket && !this._closed) return this;
    this._closed = false;
    await this._connect(this._socketTimeout);
    return this;
  }

  /**
   * Static factory: create a FastKVClient and connect in one step.
   *
   * @param {object}  opts  — same as constructor options
   * @returns {Promise<FastKVClient>}
   */
  static async create(opts = {}) {
    const client = new FastKVClient(opts);
    await client.connect();
    return client;
  }

  // ── Internal helpers ─────────────────────────────────────────────────────

  /**
   * Open a TCP connection, attach data/close/error handlers.
   * @private
   * @param {number} timeout
   * @returns {Promise<void>}
   */
  _connect(timeout) {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: this._host, port: this._port }, () => {
        clearTimeout(timer);
        resolve();
      });

      const timer = setTimeout(() => {
        socket.destroy();
        reject(new FastKVTimeoutError(
          `Connection to ${this._host}:${this._port} timed out after ${timeout}ms`
        ));
      }, timeout);

      socket.on('error', (err) => {
        clearTimeout(timer);
        reject(new FastKVConnectionError(
          `Failed to connect to ${this._host}:${this._port} — ${err.message}`,
          err
        ));
      });

      socket.on('data', (chunk) => {
        this._decoder.feed(chunk);
        this._flushDecoder();
      });

      socket.on('close', (hadError) => {
        // Reject every pending request
        for (const p of this._pending) {
          clearTimeout(p.timer);
          p.reject(new FastKVConnectionError('Connection closed by server'));
        }
        this._pending = [];
        this._closed = true;
        this.emit('close', hadError);
      });

      this._socket = socket;
    });
  }

  /**
   * Drain the decoder queue and resolve pending requests in FIFO order.
   * @private
   */
  _flushDecoder() {
    while (this._pending.length > 0) {
      const reply = this._decoder.read();
      if (reply === undefined) break; // no complete reply yet

      const pending = this._pending.shift();
      clearTimeout(pending.timer);
      // If the reply is an Error (e.g. a RESP error), reject instead of resolve
      if (reply instanceof Error) {
        pending.reject(reply);
      } else {
        pending.resolve(reply);
      }
    }
  }

  /**
   * Send a RESP command and return a Promise that resolves with the raw reply.
   * @private
   * @param {Array<string|number>} args
   * @param {number} [requestTimeout] — per-request timeout in ms
   * @returns {Promise<any>}
   */
  _sendCommand(args, requestTimeout) {
    if (this._closed) {
      return Promise.reject(new FastKVClientClosedError());
    }

    const timeout = requestTimeout ?? this._socketTimeout;

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        // Remove from pending
        const idx = this._pending.findIndex((p) => p.resolve === resolve);
        if (idx !== -1) this._pending.splice(idx, 1);
        reject(new FastKVTimeoutError(
          `Command ${args[0]} timed out after ${timeout}ms`
        ));
      }, timeout);

      this._pending.push({ resolve, reject, timer });
      this._socket.write(encodeCommand(args));
    });
  }

  /**
   * Convenience: send a command, get the reply, and decode Buffer → string.
   * @private
   * @param {Array<string|number>} args
   * @returns {Promise<any>}
   */
  async _command(args) {
    const raw = await this._sendCommand(args);
    return _decode(raw);
  }

  /**
   * Read the next reply from the decoder (used by Pipeline).
   * @private
   * @returns {Promise<any>}
   */
  _readReply() {
    if (this._closed) {
      return Promise.reject(new FastKVClientClosedError());
    }

    return new Promise((resolve, reject) => {
      // Check if there's already a parsed reply
      const cached = this._decoder.read();
      if (cached !== undefined) {
        // If the reply is an Error (e.g. a RESP error), reject instead of resolve
        if (cached instanceof Error) {
          reject(cached);
        } else {
          resolve(cached);
        }
        return;
      }

      const timer = setTimeout(() => {
        const idx = this._pending.findIndex((p) => p.resolve === resolve);
        if (idx !== -1) this._pending.splice(idx, 1);
        reject(new FastKVTimeoutError('Pipeline reply timed out'));
      }, this._socketTimeout);

      this._pending.push({ resolve, reject, timer });
    });
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────

  /**
   * Gracefully close the TCP connection.
   */
  close() {
    if (this._closed) return;
    this._closed = true;
    if (this._socket) {
      this._socket.destroy();
      this._socket = null;
    }
    this._decoder.destroy();
    this.emit('close', false);
  }

  /**
   * Returns true if the client connection is closed.
   * @returns {boolean}
   */
  get isClosed() {
    return this._closed;
  }

  // ── Server ───────────────────────────────────────────────────────────────

  /** @returns {Promise<string>} */
  ping() {
    return this._command(['PING']);
  }

  /** @param {string} msg @returns {Promise<string>} */
  echo(msg) {
    return this._command(['ECHO', msg]);
  }

  /** @returns {Promise<string>} */
  info() {
    return this._command(['INFO']);
  }

  /** @returns {Promise<number>} */
  dbsize() {
    return this._command(['DBSIZE']);
  }

  // ── String ───────────────────────────────────────────────────────────────

  /**
   * @param {string} key
   * @param {string|number|Buffer} value
   * @param {{ ex?: number, px?: number, nx?: boolean, xx?: boolean }} [opts]
   * @returns {Promise<string|null>}
   */
  set(key, value, opts) {
    const args = ['SET', key, value];
    if (opts) {
      if (opts.ex != null) args.push('EX', String(opts.ex));
      if (opts.px != null) args.push('PX', String(opts.px));
      if (opts.nx) args.push('NX');
      if (opts.xx) args.push('XX');
    }
    return this._command(args);
  }

  /** @param {string} key @returns {Promise<string|null>} */
  get(key) {
    return this._command(['GET', key]);
  }

  /** @param {...string} keys @returns {Promise<number>} */
  del(...keys) {
    return this._command(['DEL', ...keys]);
  }

  /** @param {...string} keys @returns {Promise<number>} */
  exists(...keys) {
    return this._command(['EXISTS', ...keys]);
  }

  /** @param {string} key @returns {Promise<number>} */
  incr(key) {
    return this._command(['INCR', key]);
  }

  /** @param {string} key @returns {Promise<number>} */
  decr(key) {
    return this._command(['DECR', key]);
  }

  /** @param {string} key @param {number} delta @returns {Promise<number>} */
  incrBy(key, delta) {
    return this._command(['INCRBY', key, delta]);
  }

  /** @param {string} key @param {number} delta @returns {Promise<number>} */
  decrBy(key, delta) {
    return this._command(['DECRBY', key, delta]);
  }

  /** @param {string} key @param {string} value @returns {Promise<number>} */
  append(key, value) {
    return this._command(['APPEND', key, value]);
  }

  /** @param {string} key @returns {Promise<number>} */
  strlen(key) {
    return this._command(['STRLEN', key]);
  }

  /** @param {string} key @param {number} start @param {number} end @returns {Promise<string>} */
  getRange(key, start, end) {
    return this._command(['GETRANGE', key, start, end]);
  }

  /** @param {string} key @param {number} offset @param {string} value @returns {Promise<number>} */
  setRange(key, offset, value) {
    return this._command(['SETRANGE', key, offset, value]);
  }

  /**
   * @param {Object<string, string>} pairs
   * @returns {Promise<string>}
   */
  mset(pairs) {
    const args = ['MSET'];
    for (const [k, v] of Object.entries(pairs)) {
      args.push(k, v);
    }
    return this._command(args);
  }

  /** @param {...string} keys @returns {Promise<Array<string|null>>} */
  mget(...keys) {
    return this._command(['MGET', ...keys]);
  }

  // ── TTL ──────────────────────────────────────────────────────────────────

  /** @param {string} key @param {number} seconds @returns {Promise<number>} */
  expire(key, seconds) {
    return this._command(['EXPIRE', key, seconds]);
  }

  /** @param {string} key @returns {Promise<number>} */
  ttl(key) {
    return this._command(['TTL', key]);
  }

  /** @param {string} key @returns {Promise<number>} */
  pttl(key) {
    return this._command(['PTTL', key]);
  }

  /** @param {string} key @returns {Promise<number>} */
  persist(key) {
    return this._command(['PERSIST', key]);
  }

  // ── Hash ─────────────────────────────────────────────────────────────────

  /** @param {string} key @param {string} field @param {string} value @returns {Promise<number>} */
  hset(key, field, value) {
    return this._command(['HSET', key, field, value]);
  }

  /** @param {string} key @param {string} field @returns {Promise<string|null>} */
  hget(key, field) {
    return this._command(['HGET', key, field]);
  }

  /** @param {string} key @param {...string} fields @returns {Promise<number>} */
  hdel(key, ...fields) {
    return this._command(['HDEL', key, ...fields]);
  }

  /**
   * @param {string} key
   * @returns {Promise<Object<string, string>>}
   */
  async hgetAll(key) {
    const arr = await this._command(['HGETALL', key]);
    if (!arr || arr.length === 0) return {};
    const obj = {};
    for (let i = 0; i < arr.length; i += 2) {
      obj[arr[i]] = arr[i + 1];
    }
    return obj;
  }

  /** @param {string} key @param {string} field @returns {Promise<number>} */
  hexists(key, field) {
    return this._command(['HEXISTS', key, field]);
  }

  /** @param {string} key @returns {Promise<number>} */
  hlen(key) {
    return this._command(['HLEN', key]);
  }

  /** @param {string} key @returns {Promise<string[]>} */
  hkeys(key) {
    return this._command(['HKEYS', key]);
  }

  /** @param {string} key @returns {Promise<string[]>} */
  hvals(key) {
    return this._command(['HVALS', key]);
  }

  /** @param {string} key @param {...string} fields @returns {Promise<Array<string|null>>} */
  hmget(key, ...fields) {
    return this._command(['HMGET', key, ...fields]);
  }

  /**
   * @param {string} key
   * @param {Object<string, string>} fields
   * @returns {Promise<string>}
   */
  hmset(key, fields) {
    const args = ['HMSET', key];
    for (const [f, v] of Object.entries(fields)) {
      args.push(f, v);
    }
    return this._command(args);
  }

  // ── List ─────────────────────────────────────────────────────────────────

  /** @param {string} key @param {...string} elements @returns {Promise<number>} */
  lpush(key, ...elements) {
    return this._command(['LPUSH', key, ...elements]);
  }

  /** @param {string} key @param {...string} elements @returns {Promise<number>} */
  rpush(key, ...elements) {
    return this._command(['RPUSH', key, ...elements]);
  }

  /** @param {string} key @param {number} [count] @returns {Promise<string|null|string[]>} */
  lpop(key, count) {
    const args = ['LPOP', key];
    if (count != null) args.push(String(count));
    return this._command(args);
  }

  /** @param {string} key @param {number} [count] @returns {Promise<string|null|string[]>} */
  rpop(key, count) {
    const args = ['RPOP', key];
    if (count != null) args.push(String(count));
    return this._command(args);
  }

  /** @param {string} key @param {number} start @param {number} stop @returns {Promise<string[]>} */
  lrange(key, start, stop) {
    return this._command(['LRANGE', key, start, stop]);
  }

  /** @param {string} key @returns {Promise<number>} */
  llen(key) {
    return this._command(['LLEN', key]);
  }

  /** @param {string} key @param {number} index @returns {Promise<string|null>} */
  lindex(key, index) {
    return this._command(['LINDEX', key, index]);
  }

  /** @param {string} key @param {number} count @param {string} element @returns {Promise<number>} */
  lrem(key, count, element) {
    return this._command(['LREM', key, count, element]);
  }

  /** @param {string} key @param {number} start @param {number} stop @returns {Promise<string>} */
  ltrim(key, start, stop) {
    return this._command(['LTRIM', key, start, stop]);
  }

  /** @param {string} key @param {number} index @param {string} element @returns {Promise<string>} */
  lset(key, index, element) {
    return this._command(['LSET', key, index, element]);
  }

  // ── Pipeline ─────────────────────────────────────────────────────────────

  /**
   * Create a new pipeline that opens its **own dedicated TCP connection**
   * so that pipeline responses cannot interleave with regular commands.
   * @returns {Promise<Pipeline>}
   */
  async pipeline() {
    const pipe = new Pipeline(this._host, this._port, this._socketTimeout);
    await pipe._connect();
    return pipe;
  }
}

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

module.exports = { FastKVClient };
