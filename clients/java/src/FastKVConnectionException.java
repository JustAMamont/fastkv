package com.fastkv.client;

/**
 * Thrown when a connection-level error occurs (socket closed, timeout, etc.).
 */
public class FastKVConnectionException extends FastKVException {

    public FastKVConnectionException(String message) {
        super(message);
    }

    public FastKVConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
