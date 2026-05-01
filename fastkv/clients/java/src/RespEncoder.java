package com.fastkv.client;

import java.nio.charset.StandardCharsets;

/**
 * Encodes commands into the Redis Serialization Protocol (RESP) binary format.
 * <p>
 * Each command is encoded as a RESP array of bulk strings:
 * <pre>
 * *<count>\r\n
 * $<len1>\r\n
 * <arg1>\r\n
 * $<len2>\r\n
 * <arg2>\r\n
 * ...
 * </pre>
 */
final class RespEncoder {

    private static final byte[] CRLF = {'\r', '\n'};
    private static final byte ASTERISK = '*';
    private static final byte DOLLAR = '$';

    private RespEncoder() {
        // utility class
    }

    /**
     * Encodes a command (one or more string arguments) into RESP bytes.
     *
     * @param args the command and its arguments, e.g. {"SET", "key", "value"}
     * @return the RESP-encoded byte array ready to be sent over the wire
     */
    static byte[] encode(String... args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("Command must have at least one argument");
        }

        // Pre-calculate total size to allocate a single byte array
        int totalSize = sizeOfInt(args.length) + 2; // *<count>\r\n
        byte[][] argBytes = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            argBytes[i] = toBytes(args[i]);
            totalSize += sizeOfInt(argBytes[i].length) + 2 + argBytes[i].length + 2;
        }

        byte[] buf = new byte[totalSize];
        int pos = 0;

        // Array header: *<count>\r\n
        pos = writeInt(buf, pos, ASTERISK, args.length);
        buf[pos++] = '\r';
        buf[pos++] = '\n';

        // Each argument: $<len>\r\n<data>\r\n
        for (int i = 0; i < args.length; i++) {
            pos = writeInt(buf, pos, DOLLAR, argBytes[i].length);
            buf[pos++] = '\r';
            buf[pos++] = '\n';
            System.arraycopy(argBytes[i], 0, buf, pos, argBytes[i].length);
            pos += argBytes[i].length;
            buf[pos++] = '\r';
            buf[pos++] = '\n';
        }

        return buf;
    }

    /**
     * Returns the number of bytes needed to represent a non-negative integer as ASCII decimal.
     */
    private static int sizeOfInt(int value) {
        // '*' or '$' prefix char + decimal digits
        if (value < 0) {
            return 1 + 1; // single '-' + digit (shouldn't happen for our use)
        }
        int digits = 1;
        int v = value;
        while (v >= 10) {
            v /= 10;
            digits++;
        }
        return 1 + digits; // prefix char + digits
    }

    /**
     * Writes a prefix byte + decimal integer into buf starting at pos.
     *
     * @return the position after the written bytes (does NOT include CRLF)
     */
    private static int writeInt(byte[] buf, int pos, byte prefix, int value) {
        buf[pos++] = prefix;
        if (value == 0) {
            buf[pos++] = '0';
            return pos;
        }
        // Write digits in reverse, then reverse them
        int start = pos;
        int v = value;
        while (v > 0) {
            buf[pos++] = (byte) ('0' + (v % 10));
            v /= 10;
        }
        // Reverse the digits in-place
        int end = pos - 1;
        while (start < end) {
            byte tmp = buf[start];
            buf[start] = buf[end];
            buf[end] = tmp;
            start++;
            end--;
        }
        return pos;
    }

    private static byte[] toBytes(String s) {
        if (s == null) {
            return new byte[0];
        }
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
