'use strict';

/**
 * Base error class for all FastKV client errors.
 */
class FastKVError extends Error {
  constructor(message) {
    super(message);
    this.name = 'FastKVError';
  }
}

/**
 * Thrown when the connection to FastKV server cannot be established.
 */
class FastKVConnectionError extends FastKVError {
  constructor(message, cause) {
    super(message);
    this.name = 'FastKVConnectionError';
    this.cause = cause;
  }
}

/**
 * Thrown when a socket timeout occurs.
 */
class FastKVTimeoutError extends FastKVError {
  constructor(message) {
    super(message);
    this.name = 'FastKVTimeoutError';
  }
}

/**
 * Thrown when the server returns a RESP error reply.
 */
class FastKVResponseError extends FastKVError {
  /**
   * @param {string} message — The error message returned by the server.
   */
  constructor(message) {
    super(message);
    this.name = 'FastKVResponseError';
  }
}

/**
 * Thrown when attempting to use a client that has been closed.
 */
class FastKVClientClosedError extends FastKVError {
  constructor() {
    super('Client is closed');
    this.name = 'FastKVClientClosedError';
  }
}

module.exports = {
  FastKVError,
  FastKVConnectionError,
  FastKVTimeoutError,
  FastKVResponseError,
  FastKVClientClosedError,
};
