package com.fastkv.client;

/**
 * Base exception for all FastKV client errors.
 */
public class FastKVException extends RuntimeException {

    public FastKVException(String message) {
        super(message);
    }

    public FastKVException(String message, Throwable cause) {
        super(message, cause);
    }
}
