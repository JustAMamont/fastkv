"""
RESP (REdis Serialization Protocol) encoder and decoder.

Implements the RESP protocol as defined in the Redis documentation.
Supports all five RESP types:
  - Simple Strings  (+)
  - Errors           (-)
  - Integers         (:)
  - Bulk Strings     ($)
  - Arrays           (*)
"""

from __future__ import annotations

import socket
from typing import Any, Union

from .exceptions import FastKVConnectionError, FastKVResponseError

# ---------------------------------------------------------------------------
# RESP type prefix bytes
# ---------------------------------------------------------------------------
PREFIX_SIMPLE_STRING = ord("+")
PREFIX_ERROR = ord("-")
PREFIX_INTEGER = ord(":")
PREFIX_BULK_STRING = ord("$")
PREFIX_ARRAY = ord("*")

CRLF = b"\r\n"


# ---------------------------------------------------------------------------
# Encoder
# ---------------------------------------------------------------------------

def _encode_bulk_string(value: Union[str, bytes, int, float]) -> bytes:
    """Encode a single value as a RESP bulk string."""
    if isinstance(value, bytes):
        payload = value
    elif isinstance(value, (int, float)):
        payload = str(value).encode("utf-8")
    else:
        payload = str(value).encode("utf-8")
    return b"$%d\r\n%s\r\n" % (len(payload), payload)


def encode_command(*args: Any) -> bytes:
    """Encode a command and its arguments into a RESP array of bulk strings.

    Example::

        >>> encode_command("SET", "mykey", "myvalue")
        b'*3\\r\\n$3\\r\\nSET\\r\\n$5\\r\\nmykey\\r\\n$7\\r\\nmyvalue\\r\\n'

    Parameters
    ----------
    *args:
        Command name followed by zero or more arguments.  Each argument is
        encoded as a bulk string.

    Returns
    -------
    bytes
        The fully-encoded RESP command ready to be sent over a socket.
    """
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        parts.append(_encode_bulk_string(arg))
    return b"".join(parts)


# ---------------------------------------------------------------------------
# Decoder helpers
# ---------------------------------------------------------------------------

def _readline(sock: socket.socket) -> bytes:
    """Read bytes from *sock* until ``\\r\\n`` is encountered.

    Returns the line **including** the trailing ``\\r\\n``.
    """
    buf = bytearray()
    while True:
        try:
            chunk = sock.recv(1)
        except OSError as exc:
            raise FastKVConnectionError(
                "Socket error while reading from server"
            ) from exc
        if not chunk:
            raise FastKVConnectionError(
                "Connection closed by the FastKV server while reading"
            )
        buf.extend(chunk)
        if len(buf) >= 2 and buf[-2:] == b"\r\n":
            return bytes(buf)


def _read_crlf_terminated(sock: socket.socket) -> bytes:
    """Read a CRLF-terminated line and return the content *without* CRLF."""
    line = _readline(sock)
    return line[:-2]  # strip \r\n


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_response(sock: socket.socket) -> Any:
    """Read and decode one complete RESP response from *sock*.

    Returns the parsed Python value according to the RESP type prefix:

    ==========  ===========
    RESP type   Python type
    ==========  ===========
    +           str
    -           raises ``FastKVResponseError``
    :           int
    $           bytes | ``None`` (null bulk string)
    *           list | ``None`` (null array)
    ==========  ===========

    Parameters
    ----------
    sock:
        A connected TCP socket.

    Returns
    -------
    Any
        The decoded value.

    Raises
    ------
    FastKVConnectionError
        If the connection is lost or a socket error occurs.
    FastKVResponseError
        If the server returns an error response.
    """
    try:
        prefix_byte = sock.recv(1)
    except OSError as exc:
        raise FastKVConnectionError(
            "Socket error while reading from server"
        ) from exc

    if not prefix_byte:
        raise FastKVConnectionError(
            "Connection closed by the FastKV server"
        )

    prefix = prefix_byte[0]

    # -- Simple String --------------------------------------------------------
    if prefix == PREFIX_SIMPLE_STRING:
        line = _read_crlf_terminated(sock)
        return line.decode("utf-8", errors="replace")

    # -- Error ----------------------------------------------------------------
    if prefix == PREFIX_ERROR:
        line = _read_crlf_terminated(sock)
        message = line.decode("utf-8", errors="replace")
        raise FastKVResponseError(message)

    # -- Integer --------------------------------------------------------------
    if prefix == PREFIX_INTEGER:
        line = _read_crlf_terminated(sock)
        return int(line)

    # -- Bulk String ----------------------------------------------------------
    if prefix == PREFIX_BULK_STRING:
        length_line = _read_crlf_terminated(sock)
        length = int(length_line)
        if length == -1:
            return None  # null bulk string
        data = _recv_exact(sock, length)
        # consume trailing CRLF
        _read_crlf_terminated(sock)  # reads the empty line after data
        return data

    # -- Array ----------------------------------------------------------------
    if prefix == PREFIX_ARRAY:
        count_line = _read_crlf_terminated(sock)
        count = int(count_line)
        if count == -1:
            return None  # null array
        return [decode_response(sock) for _ in range(count)]

    # -- Unknown prefix -------------------------------------------------------
    raise FastKVResponseError(
        f"Unknown RESP prefix byte: 0x{prefix:02x}"
    )


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly *n* bytes from *sock*."""
    buf = bytearray()
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError as exc:
            raise FastKVConnectionError(
                "Socket error while reading from server"
            ) from exc
        if not chunk:
            raise FastKVConnectionError(
                f"Expected {n} bytes but connection closed after {len(buf)}"
            )
        buf.extend(chunk)
    return bytes(buf)
