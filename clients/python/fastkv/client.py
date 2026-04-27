"""
FastKV Python client.

A lightweight, standalone RESP (REdis Serialization Protocol) client for
communicating with a FastKV server over TCP.  Uses only the Python standard
library — no third-party dependencies.
"""

from __future__ import annotations

import socket
from typing import Any, Dict, List, Optional, Union

from .exceptions import FastKVConnectionError
from .pipeline import Pipeline
from .resp import decode_response, encode_command


class FastKVClient:
    """Synchronous TCP client for a FastKV (RESP-speaking) server.

    Parameters
    ----------
    host : str
        Server hostname or IP address.  Defaults to ``"localhost"``.
    port : int
        Server port.  Defaults to ``6379``.
    socket_timeout : float | None
        Timeout in seconds for socket operations.  ``None`` means blocking
        forever.  Defaults to ``5``.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        socket_timeout: Optional[float] = 5,
    ) -> None:
        self._host = host
        self._port = port
        self._socket_timeout = socket_timeout
        self._sock: Optional[socket.socket] = None
        self._connect()

    # -- connection lifecycle -------------------------------------------------

    def _connect(self) -> None:
        """Establish a TCP connection to the FastKV server."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self._socket_timeout is not None:
                sock.settimeout(self._socket_timeout)
            sock.connect((self._host, self._port))
            self._sock = sock
        except OSError as exc:
            self._sock = None
            raise FastKVConnectionError(
                f"Could not connect to FastKV at {self._host}:{self._port} — {exc}"
            ) from exc

    def _reconnect(self) -> None:
        """Close the current socket (if any) and reconnect."""
        self._close_socket()
        self._connect()

    def _close_socket(self) -> None:
        """Shutdown and close the underlying socket."""
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def _ensure_connected(self) -> socket.socket:
        """Return the socket, reconnecting automatically if needed.

        A lightweight liveness probe (``getpeername``) is used to detect
        half-closed connections before the first read/write attempt.
        """
        if self._sock is None:
            self._reconnect()
        elif self._socket_timeout is not None:
            # Liveness probe — detect half-closed sockets early
            old_timeout = self._sock.gettimeout()
            try:
                self._sock.settimeout(0)
                self._sock.getpeername()
            except OSError:
                self._sock = None
                self._reconnect()
            finally:
                self._sock.settimeout(old_timeout)
        assert self._sock is not None  # for type checkers
        return self._sock

    def _send_all(self, data: bytes) -> None:
        """Send all *data* bytes over the socket."""
        sock = self._ensure_connected()
        try:
            sock.sendall(data)
        except OSError as exc:
            self._sock = None
            raise FastKVConnectionError(
                "Connection lost while sending data"
            ) from exc

    def _execute_command(self, *args: Any) -> Any:
        """Encode *args* as a RESP command, send it, and return the decoded
        response.

        On connection errors the method will attempt one automatic reconnect
        before raising.
        """
        cmd = encode_command(*args)
        try:
            self._send_all(cmd)
            return decode_response(self._ensure_connected())
        except FastKVConnectionError:
            # One automatic reconnect attempt
            self._reconnect()
            self._send_all(cmd)
            return decode_response(self._ensure_connected())

    def close(self) -> None:
        """Close the connection to the FastKV server."""
        self._close_socket()

    # -- context manager ------------------------------------------------------

    def __enter__(self) -> "FastKVClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    # -- pipeline -------------------------------------------------------------

    def pipeline(self) -> Pipeline:
        """Return a new :class:`Pipeline` that shares this client's connection.

        Usage::

            with client.pipeline() as pipe:
                pipe.set("a", 1)
                pipe.set("b", 2)
                results = pipe.execute()
            # results == [True, True]
        """
        return Pipeline(self)

    # =========================================================================
    # Core commands
    # =========================================================================

    def ping(self) -> str:
        """Send a PING command.  Returns ``"PONG"``."""
        return self._execute_command("PING")

    def echo(self, message: str) -> str:
        """Echo the given *message* back from the server."""
        return self._execute_command("ECHO", message)

    def info(self, section: Optional[str] = None) -> str:
        """Return server information.  Optionally pass a *section* name."""
        if section is None:
            return self._execute_command("INFO")
        return self._execute_command("INFO", section)

    def dbsize(self) -> int:
        """Return the number of keys in the currently selected database."""
        return self._execute_command("DBSIZE")

    # =========================================================================
    # String commands
    # =========================================================================

    def set(
        self,
        key: str,
        value: str,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Optional[bool]:
        """Set *key* to hold the string *value*.

        Parameters
        ----------
        key, value : str
            Key and value to set.
        ex : int | None
            Set the expiry in **seconds**.
        px : int | None
            Set the expiry in **milliseconds**.
        nx : bool
            Only set if the key does **not** already exist.
        xx : bool
            Only set if the key **already** exists.

        Returns
        -------
        bool | None
            ``True`` if the key was set, ``None`` if the operation was not
            performed (due to ``NX``/``XX`` condition).
        """
        args: list = ["SET", key, value]
        if ex is not None:
            args.append("EX")
            args.append(str(ex))
        if px is not None:
            args.append("PX")
            args.append(str(px))
        if nx:
            args.append("NX")
        if xx:
            args.append("XX")
        result = self._execute_command(*args)
        if result is None:
            return None
        return result == b"OK" or result == "OK"

    def get(self, key: str) -> Optional[bytes]:
        """Get the value of *key*.  Returns ``None`` if the key does not exist."""
        return self._execute_command("GET", key)

    def delete(self, keys: Any) -> int:
        """Remove one or more keys.  Returns the number of keys removed.

        *keys* can be a single string or an iterable of strings.
        """
        if isinstance(keys, (str, bytes)):
            keys = (keys,)
        return self._execute_command("DEL", *keys)

    def exists(self, keys: Any) -> int:
        """Return the number of *keys* that exist.

        *keys* can be a single string or an iterable of strings.
        """
        if isinstance(keys, (str, bytes)):
            keys = (keys,)
        return self._execute_command("EXISTS", *keys)

    def incr(self, key: str) -> int:
        """Increment the integer value of *key* by one."""
        return self._execute_command("INCR", key)

    def decr(self, key: str) -> int:
        """Decrement the integer value of *key* by one."""
        return self._execute_command("DECR", key)

    def incrby(self, key: str, delta: int) -> int:
        """Increment the integer value of *key* by *delta*."""
        return self._execute_command("INCRBY", key, delta)

    def decrby(self, key: str, delta: int) -> int:
        """Decrement the integer value of *key* by *delta*."""
        return self._execute_command("DECRBY", key, delta)

    def append(self, key: str, value: str) -> int:
        """Append *value* to the string stored at *key*.

        If *key* does not exist it is created.  Returns the new length.
        """
        return self._execute_command("APPEND", key, value)

    def strlen(self, key: str) -> int:
        """Return the length of the string value stored at *key*."""
        return self._execute_command("STRLEN", key)

    def getrange(self, key: str, start: int, end: int) -> bytes:
        """Return the substring of the string value stored at *key*."""
        return self._execute_command("GETRANGE", key, start, end)

    def setrange(self, key: str, offset: int, value: str) -> int:
        """Overwrite part of the string at *key* starting at *offset*.

        Returns the new string length.
        """
        return self._execute_command("SETRANGE", key, offset, value)

    def mset(self, pairs: Dict[str, str]) -> bool:
        """Set multiple keys to multiple values.

        *pairs* is a dict of ``{key: value, ...}``.

        Returns ``True`` on success.
        """
        flat: list = []
        for k, v in pairs.items():
            flat.append(k)
            flat.append(v)
        result = self._execute_command("MSET", *flat)
        return result == b"OK" or result == "OK"

    def mget(self, keys: Any) -> List[Optional[bytes]]:
        """Return the values of all specified keys.

        *keys* can be a single string or an iterable of strings.
        """
        if isinstance(keys, str):
            keys = (keys,)
        return self._execute_command("MGET", *keys)

    # =========================================================================
    # TTL commands
    # =========================================================================

    def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on *key* of *seconds* seconds.  Returns ``True`` on
        success, ``False`` if the key does not exist."""
        result = self._execute_command("EXPIRE", key, seconds)
        return result == 1

    def ttl(self, key: str) -> int:
        """Return the remaining time to live of *key* in seconds.

        Returns ``-2`` if the key does not exist, ``-1`` if the key has no
        expiry.
        """
        return self._execute_command("TTL", key)

    def pttl(self, key: str) -> int:
        """Return the remaining time to live of *key* in milliseconds."""
        return self._execute_command("PTTL", key)

    def persist(self, key: str) -> bool:
        """Remove the expiry from *key*.  Returns ``True`` if the timeout was
        removed, ``False`` if the key does not exist or has no expiry."""
        result = self._execute_command("PERSIST", key)
        return result == 1

    # =========================================================================
    # Hash commands
    # =========================================================================

    def hset(self, key: str, field: str, value: str) -> int:
        """Set *field* in the hash stored at *key* to *value*.

        Returns ``1`` if the field is new, ``0`` if it was updated.
        """
        return self._execute_command("HSET", key, field, value)

    def hget(self, key: str, field: str) -> Optional[bytes]:
        """Return the value of *field* in the hash stored at *key*.

        Returns ``None`` if the field or key does not exist.
        """
        return self._execute_command("HGET", key, field)

    def hdel(self, key: str, *fields: str) -> int:
        """Remove one or more fields from the hash at *key*.

        Returns the number of fields that were removed.
        """
        return self._execute_command("HDEL", key, *fields)

    def hgetall(self, key: str) -> Dict[bytes, bytes]:
        """Return all fields and values of the hash stored at *key*.

        Returns a dict mapping field ``bytes`` → value ``bytes``.
        """
        raw: list = self._execute_command("HGETALL", key)
        result: Dict[bytes, bytes] = {}
        it = iter(raw)
        for f, v in zip(it, it):
            result[f] = v
        return result

    def hexists(self, key: str, field: str) -> bool:
        """Return ``True`` if *field* exists in the hash at *key*."""
        result = self._execute_command("HEXISTS", key, field)
        return result == 1

    def hlen(self, key: str) -> int:
        """Return the number of fields in the hash at *key*."""
        return self._execute_command("HLEN", key)

    def hkeys(self, key: str) -> List[bytes]:
        """Return all field names in the hash at *key*."""
        return self._execute_command("HKEYS", key)

    def hvals(self, key: str) -> List[bytes]:
        """Return all values in the hash at *key*."""
        return self._execute_command("HVALS", key)

    def hmget(self, key: str, *fields: str) -> List[Optional[bytes]]:
        """Return the values associated with the specified *fields* in the hash
        stored at *key*."""
        return self._execute_command("HMGET", key, *fields)

    def hmset(self, key: str, mapping: Dict[str, str]) -> bool:
        """Set multiple fields in the hash at *key*.

        *mapping* is a dict of ``{field: value, ...}``.

        Returns ``True`` on success.
        """
        flat: list = []
        for f, v in mapping.items():
            flat.append(f)
            flat.append(v)
        result = self._execute_command("HMSET", key, *flat)
        return result == b"OK" or result == "OK"

    # =========================================================================
    # List commands
    # =========================================================================

    def lpush(self, key: str, *elements: str) -> int:
        """Insert all *elements* at the head of the list stored at *key*.

        Returns the length of the list after the operation.
        """
        return self._execute_command("LPUSH", key, *elements)

    def rpush(self, key: str, *elements: str) -> int:
        """Insert all *elements* at the tail of the list stored at *key*.

        Returns the length of the list after the operation.
        """
        return self._execute_command("RPUSH", key, *elements)

    def lpop(self, key: str, count: Optional[int] = None) -> Optional[bytes]:
        """Remove and return the first element(s) of the list at *key*.

        If *count* is given, returns a list of up to *count* elements;
        otherwise returns a single value (or ``None``).
        """
        if count is not None:
            return self._execute_command("LPOP", key, count)
        return self._execute_command("LPOP", key)

    def rpop(self, key: str, count: Optional[int] = None) -> Optional[bytes]:
        """Remove and return the last element(s) of the list at *key*.

        If *count* is given, returns a list of up to *count* elements;
        otherwise returns a single value (or ``None``).
        """
        if count is not None:
            return self._execute_command("RPOP", key, count)
        return self._execute_command("RPOP", key)

    def lrange(self, key: str, start: int, stop: int) -> List[bytes]:
        """Return the elements of the list at *key* between offsets *start*
        and *stop* (inclusive)."""
        return self._execute_command("LRANGE", key, start, stop)

    def llen(self, key: str) -> int:
        """Return the length of the list stored at *key*."""
        return self._execute_command("LLEN", key)

    def lindex(self, key: str, index: int) -> Optional[bytes]:
        """Return the element at *index* in the list stored at *key*."""
        return self._execute_command("LINDEX", key, index)

    def lrem(self, key: str, count: int, element: str) -> int:
        """Remove the first *count* occurrences of *element* from the list at
        *key*.  Returns the number of elements removed."""
        return self._execute_command("LREM", key, count, element)

    def ltrim(self, key: str, start: int, stop: int) -> bool:
        """Trim the list at *key* to the range [*start*, *stop*].

        Returns ``True`` on success.
        """
        result = self._execute_command("LTRIM", key, start, stop)
        return result == b"OK" or result == "OK"

    def lset(self, key: str, index: int, element: str) -> bool:
        """Set the element at *index* in the list at *key* to *element*.

        Returns ``True`` on success.
        """
        result = self._execute_command("LSET", key, index, element)
        return result == b"OK" or result == "OK"
