package com.fastkv.client;

/**
 * Thrown when the server returns an ERROR response (prefix '-').
 */
public class FastKVResponseException extends FastKVException {

    private final String errorType;

    public FastKVResponseException(String message) {
        super(message);
        this.errorType = extractErrorType(message);
    }

    public FastKVResponseException(String errorType, String message) {
        super(message);
        this.errorType = errorType;
    }

    /**
     * Returns the error type code (e.g., "ERR", "WRONGTYPE", "EXECABORT").
     * May be null if the error string has no recognizable prefix.
     */
    public String getErrorType() {
        return errorType;
    }

    private static String extractErrorType(String message) {
        if (message != null) {
            int space = message.indexOf(' ');
            if (space > 0) {
                return message.substring(0, space);
            }
            return message;
        }
        return null;
    }
}
