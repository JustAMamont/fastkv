"""
Async RESP decoder for the FastKV client.

Like :mod:`resp` but reads from an :class:`asyncio.StreamReader` instead
of a raw socket.  The encoder (:func:`resp.encode_command`) is I/O-free
and is reused as-is.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .exceptions import FastKVConnectionError, FastKVResponseError


async def _readline(reader: asyncio.StreamReader) -> bytes:
    """Read bytes until ``\\r\\n`` is encountered.  Returns the line
    **including** the trailing ``\\r\\n``."""
    try:
        line = await reader.readline()
    except (ConnectionError, OSError) as exc:
        raise FastKVConnectionError("Socket error while reading from server") from exc
    if not line:
        raise FastKVConnectionError("Connection closed by the FastKV server while reading")
    return line


async def _recv_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    """Read exactly *n* bytes from *reader*."""
    data = bytearray()
    while len(data) < n:
        try:
            chunk = await reader.read(n - len(data))
        except (ConnectionError, OSError) as exc:
            raise FastKVConnectionError("Socket error while reading from server") from exc
        if not chunk:
            raise FastKVConnectionError(
                f"Expected {n} bytes but connection closed after {len(data)}"
            )
        data.extend(chunk)
    return bytes(data)


async def decode_response(reader: asyncio.StreamReader) -> Any:
    """Read and decode one complete RESP response from *reader*.

    Returns the parsed Python value according to the RESP type prefix:

    ==========  ===========
    RESP type   Python type
    ==========  ===========
    +           str
    -           raises :class:`FastKVResponseError`
    :           int
    $           bytes | ``None``
    *           list | ``None``
    ==========  ===========
    """
    try:
        prefix_byte = await reader.readexactly(1)
    except (asyncio.IncompleteReadError, ConnectionError, OSError) as exc:
        raise FastKVConnectionError("Connection closed by the FastKV server") from exc

    prefix = prefix_byte[0]

    # Simple String (+)
    if prefix == 0x2B:
        line = await _readline(reader)
        return line[:-2].decode("utf-8", errors="replace")

    # Error (-)
    if prefix == 0x2D:
        line = await _readline(reader)
        message = line[:-2].decode("utf-8", errors="replace")
        raise FastKVResponseError(message)

    # Integer (:)
    if prefix == 0x3A:
        line = await _readline(reader)
        return int(line)

    # Bulk String ($)
    if prefix == 0x24:
        length_line = await _readline(reader)
        length = int(length_line)
        if length == -1:
            return None
        data = await _recv_exact(reader, length + 2)  # +2 for trailing CRLF
        return data[:length]  # strip CRLF

    # Array (*)
    if prefix == 0x2A:
        count_line = await _readline(reader)
        count = int(count_line)
        if count == -1:
            return None
        return [await decode_response(reader) for _ in range(count)]

    raise FastKVResponseError(f"Unknown RESP prefix byte: 0x{prefix:02x}")
