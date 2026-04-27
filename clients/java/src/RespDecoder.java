package com.fastkv.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes RESP (Redis Serialization Protocol) responses from an input stream.
 * <p>
 * Handles all five RESP types:
 * <ul>
 *   <li>Simple Strings  (+OK\r\n)</li>
 *   <li>Errors           (-ERR message\r\n)</li>
 *   <li>Integers         (:42\r\n)</li>
 *   <li>Bulk Strings     ($6\r\nfoobar\r\n, $-1\r\n for null)</li>
 *   <li>Arrays           (*2\r\n...\r\n, *-1\r\n for null)</li>
 * </ul>
 */
final class RespDecoder {

    /**
     * Sentinel object representing a RESP null bulk string or null array.
     */
    static final Object NULL = new Object() {
        @Override
        public String toString() {
            return "RESP_NULL";
        }
    };

    private RespDecoder() {
        // utility class
    }

    /**
     * Reads and decodes the next complete RESP value from the stream.
     *
     * @param in the input stream (must be buffered for efficiency)
     * @return the decoded value — String, Long, List&lt;Object&gt;, or {@link #NULL}
     * @throws FastKVConnectionException on I/O errors
     * @throws FastKVResponseException  when the server returns an error reply
     */
    static Object decode(InputStream in) {
        try {
            int first = readByte(in);
            switch (first) {
                case '+':
                    return readSimpleString(in);
                case '-':
                    return readError(in);
                case ':':
                    return readInteger(in);
                case '$':
                    return readBulkString(in);
                case '*':
                    return readArray(in);
                default:
                    throw new FastKVConnectionException(
                            "Unknown RESP type prefix: " + (char) first + " (0x" + Integer.toHexString(first) + ")");
            }
        } catch (IOException e) {
            throw new FastKVConnectionException("Failed to read RESP response", e);
        }
    }

    /**
     * Reads a line ending with \r\n (the CRLF is consumed but not returned).
     */
    private static String readLine(InputStream in) throws IOException {
        // We use a simple loop reading one byte at a time.
        // For production use with high throughput, callers should wrap the InputStream
        // in a BufferedInputStream, which makes single-byte reads efficient.
        ByteArrayOutputStreamPlus buf = new ByteArrayOutputStreamPlus(64);
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next == -1) {
                    throw new FastKVConnectionException("Unexpected end of stream after CR");
                }
                if (next != '\n') {
                    throw new FastKVConnectionException("Expected LF after CR, got 0x" + Integer.toHexString(next));
                }
                break;
            }
            buf.write(b);
        }
        if (b == -1 && buf.size() == 0) {
            throw new FastKVConnectionException("Unexpected end of stream");
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

    private static int readByte(InputStream in) throws IOException {
        int b = in.read();
        if (b == -1) {
            throw new FastKVConnectionException("Unexpected end of stream");
        }
        return b;
    }

    private static String readSimpleString(InputStream in) throws IOException {
        return readLine(in);
    }

    private static Object readError(InputStream in) throws IOException {
        String msg = readLine(in);
        throw new FastKVResponseException(msg);
    }

    private static long readInteger(InputStream in) throws IOException {
        String line = readLine(in);
        try {
            return Long.parseLong(line);
        } catch (NumberFormatException e) {
            throw new FastKVConnectionException("Invalid RESP integer: " + line, e);
        }
    }

    private static Object readBulkString(InputStream in) throws IOException {
        String lengthStr = readLine(in);
        long length = parseLong(lengthStr);
        if (length == -1) {
            return NULL;
        }
        if (length < 0) {
            throw new FastKVConnectionException("Invalid bulk string length: " + length);
        }
        if (length > Integer.MAX_VALUE) {
            throw new FastKVConnectionException("Bulk string too large: " + length + " bytes");
        }
        int len = (int) length;
        byte[] data = new byte[len];
        int totalRead = 0;
        while (totalRead < len) {
            int n = in.read(data, totalRead, len - totalRead);
            if (n == -1) {
                throw new FastKVConnectionException("Unexpected end of stream while reading bulk string");
            }
            totalRead += n;
        }
        // Consume the trailing \r\n
        int cr = in.read();
        int lf = in.read();
        if (cr != '\r' || lf != '\n') {
            throw new FastKVConnectionException("Missing CRLF after bulk string data");
        }
        return new String(data, StandardCharsets.UTF_8);
    }

    private static Object readArray(InputStream in) throws IOException {
        String lengthStr = readLine(in);
        long length = parseLong(lengthStr);
        if (length == -1) {
            return NULL;
        }
        if (length < 0) {
            throw new FastKVConnectionException("Invalid array length: " + length);
        }
        if (length > Integer.MAX_VALUE) {
            throw new FastKVConnectionException("Array too large: " + length + " elements");
        }
        int count = (int) length;
        List<Object> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(decode(in));
        }
        return list;
    }

    private static long parseLong(String s) {
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            throw new FastKVConnectionException("Invalid RESP number: " + s, e);
        }
    }

    /**
     * Minimal reusable byte-array output stream (avoids synchronization overhead of ByteArrayOutputStream).
     */
    private static final class ByteArrayOutputStreamPlus {
        private byte[] buf;
        private int count;

        ByteArrayOutputStreamPlus(int initialSize) {
            this.buf = new byte[initialSize];
        }

        void write(int b) {
            if (count == buf.length) {
                byte[] newBuf = new byte[buf.length << 1];
                System.arraycopy(buf, 0, newBuf, 0, count);
                buf = newBuf;
            }
            buf[count++] = (byte) b;
        }

        int size() {
            return count;
        }

        String toString(java.nio.charset.Charset charset) {
            return new String(buf, 0, count, charset);
        }
    }
}
