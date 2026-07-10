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
                self._sock.settimeout(old_timeout) # type: ignore
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

    def scan(
        self,
        cursor: int = 0,
        count: int = 10,
        match: Optional[str] = None,
    ) -> tuple:
        """Iterate over keys using a cursor.

        Parameters
        ----------
        cursor : int
            Starting cursor (0 to start from the beginning).
        count : int
            Hint for the number of keys to return per call.
        match : str | None
            Optional glob pattern (e.g., ``"session:*"``).

        Returns
        -------
        tuple[int, list[str]]
            A ``(next_cursor, keys)`` pair.  When *next_cursor* is ``0``,
            iteration is complete.
        """
        args: list = ["SCAN", str(cursor)]
        if count != 10:
            args.extend(["COUNT", str(count)])
        if match is not None:
            args.extend(["MATCH", match])
        raw = self._execute_command(*args)
        # Response is [next_cursor_bytes, [key1_bytes, key2_bytes, ...]]
        next_cursor_raw, keys_raw = raw[0], raw[1]
        next_cursor = int(
            next_cursor_raw.decode("utf-8")
            if isinstance(next_cursor_raw, bytes)
            else str(next_cursor_raw)
        )
        keys = [
            k.decode("utf-8") if isinstance(k, bytes) else str(k)
            for k in keys_raw
        ]
        return (next_cursor, keys)

    def dbstats(self) -> Dict[str, Any]:
        """Get aggregate store statistics.

        Returns
        -------
        dict
            Statistics including ``total_keys``, ``total_buckets``,
            ``load_factor``, ``entry_size``, ``total_memory``,
            ``blob_count``, and ``inline_size``.
        """
        raw = self._execute_command("DBSTATS")
        if raw is None:
            return {}
        # Response is a flat array of key-value pairs
        result: Dict[str, Any] = {}
        it = iter(raw)
        for k, v in zip(it, it):
            key = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            val = v.decode("utf-8") if isinstance(v, bytes) else str(v)
            try:
                if key == "load_factor":
                    result[key] = float(val)
                else:
                    result[key] = int(val)
            except ValueError:
                result[key] = val
        return result

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

    def get_str(self, key: str) -> Optional[str]:
        """Get the value of *key* as a UTF-8 string.  Returns ``None`` if missing."""
        v = self._execute_command("GET", key)
        if v is None:
            return None
        if isinstance(v, bytes):
            return v.decode("utf-8", errors="replace")
        return str(v)

    def set_str(self, key: str, value: str) -> Optional[bool]:
        """Set *key* to hold string *value*.  Returns ``True`` if set."""
        return self.set(key, value)

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

    def set_nx(self, key: str, value: str) -> bool:
        """Set *key* to *value* only if *key* does not exist.

        Returns ``True`` if the key was set, ``False`` if it already existed.
        """
        result = self._execute_command("SETNX", key, value)
        return result == 1

    def get_set(self, key: str, value: str) -> Optional[bytes]:
        """Atomically set *key* to *value* and return the old value.

        Returns ``None`` if the key did not exist.
        """
        return self._execute_command("GETSET", key, value)

    def get_del(self, key: str) -> Optional[bytes]:
        """Atomically get the value of *key* and delete the key.

        Returns ``None`` if the key did not exist.
        """
        return self._execute_command("GETDEL", key)

    def type_of(self, key: str) -> str:
        """Return the type of value stored at *key*.

        Returns one of ``"string"``, ``"hash"``, ``"list"``, or ``"none"``.
        """
        result = self._execute_command("TYPE", key)
        return result.decode("utf-8") if isinstance(result, bytes) else str(result)

    def rename(self, key: str, new_key: str) -> bool:
        """Rename *key* to *new_key*.

        Returns ``True`` on success.
        """
        result = self._execute_command("RENAME", key, new_key)
        return result == b"OK" or result == "OK"

    def pset_ex(self, key: str, value: str, milliseconds: int) -> bool:
        """Set *key* to *value* with an expiry of *milliseconds*.

        Returns ``True`` on success.
        """
        result = self._execute_command("PSETEX", key, milliseconds, value)
        return result == b"OK" or result == "OK"

    def unlink(self, keys: Any) -> int:
        """Asynchronously delete one or more keys.

        *keys* can be a single string or an iterable of strings.
        Returns the number of keys that were removed.
        """
        if isinstance(keys, (str, bytes)):
            keys = (keys,)
        return self._execute_command("UNLINK", *keys)

    # =========================================================================
    # Key management / server commands
    # =========================================================================

    def flush_all(self) -> bool:
        """Remove all keys from all databases.

        Returns ``True`` on success.
        """
        result = self._execute_command("FLUSHALL")
        return result == b"OK" or result == "OK"

    def flush_db(self) -> bool:
        """Remove all keys from the current database.

        Returns ``True`` on success.
        """
        result = self._execute_command("FLUSHDB")
        return result == b"OK" or result == "OK"

    def auth(self, password: str) -> bool:
        """Authenticate to the server.

        Returns ``True`` on success.
        """
        result = self._execute_command("AUTH", password)
        return result == b"OK" or result == "OK"

    def save(self) -> bool:
        """Synchronously save the dataset to disk (checkpoint).

        Returns ``True`` on success.
        """
        result = self._execute_command("SAVE")
        return result == b"OK" or result == "OK"

    def bgsave(self) -> bool:
        """Asynchronously save the dataset to disk (background checkpoint).

        Returns ``True`` on success.
        """
        result = self._execute_command("BGSAVE")
        return result == b"OK" or result == "OK"

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

    def h_incr_by(self, key: str, field: str, delta: int) -> int:
        """Increment the integer value of *field* in the hash at *key* by *delta*.

        Returns the new value of the field.
        """
        return self._execute_command("HINCRBY", key, field, delta)

    def h_set_nx(self, key: str, field: str, value: str) -> bool:
        """Set *field* in the hash at *key* to *value* only if *field* does not exist.

        Returns ``True`` if the field was set, ``False`` if it already existed.
        """
        result = self._execute_command("HSETNX", key, field, value)
        return result == 1

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

    # =========================================================================
    # Blob commands (require Blob Arena enabled on server — default on;
    # disable with --no-blob-store)
    # =========================================================================

    def bset(self, key: str, value: Union[str, bytes]) -> bool:
        """Store a large value as a compressed blob.

        The server compresses *value* with zstd and stores it in the blob
        arena.  A 33-byte reference is kept in the hash table.  Use this
        for values that exceed the inline size limit (default 64 bytes) or
        for any data that benefits from compression (sessions, JSON, etc.).

        Parameters
        ----------
        key : str
            The key to set.
        value : str | bytes
            The value to store.  Strings are encoded as UTF-8.

        Returns
        -------
        bool
            ``True`` on success.

        Raises
        ------
        FastKVResponseError
            If the blob store is not enabled on the server or the value
            could not be stored.
        """
        if isinstance(value, str):
            value = value.encode("utf-8")
        result = self._execute_command("BSET", key, value)
        return result == b"OK" or result == "OK"

    def bget(self, key: str) -> Optional[bytes]:
        """Retrieve and decompress a blob value.

        If the value at *key* is a blob reference, it is transparently
        decompressed.  If it is a plain string value, it is returned as-is.

        Parameters
        ----------
        key : str
            The key to retrieve.

        Returns
        -------
        bytes | None
            The decompressed value, or ``None`` if the key does not exist.
        """
        return self._execute_command("BGET", key)

    def bgetraw(self, key: str) -> Optional[bytes]:
        """Retrieve the raw compressed bytes of a blob value.

        Useful for transferring blob data without decompression overhead
        (e.g. replicating to another node).

        Parameters
        ----------
        key : str
            The key to retrieve.

        Returns
        -------
        bytes | None
            The compressed bytes, or ``None`` if the key does not exist
            or is not a blob reference.
        """
        return self._execute_command("BGETRAW", key)

    def bstats(self) -> Dict[str, Any]:
        """Return blob arena statistics.

        Returns
        -------
        dict
            A dictionary with keys:

            - ``total_used`` (int) — bytes currently used in the arena.
            - ``total_compressed`` (int) — total compressed bytes ever stored.
            - ``total_original`` (int) — total original (uncompressed) bytes
              ever stored.
            - ``compression_ratio`` (float) — compressed / original ratio.
              Lower is better; e.g. 0.15 means 6.7× compression.
            - ``free_slots`` (int) — number of reclaimed slots available for
              reuse.
        """
        raw = self._execute_command("BSTATS")
        if raw is None:
            return {}
        # Parse the text format: "key:value\r\n..."
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        result: Dict[str, Any] = {}
        for line in text.strip().split("\r\n"):
            if ":" in line:
                k, v = line.split(":", 1)
                k = k.strip().replace(" ", "_").replace("#_", "")
                try:
                    if k == "compression_ratio":
                        result[k] = float(v)
                    else:
                        result[k] = int(v)
                except ValueError:
                    result[k] = v
        return result

    # -- similarity commands --------------------------------------------------

    def simhash(self, key: str) -> Optional[str]:
        """Compute SimHash for a stored value.

        Returns the 64-bit SimHash as a zero-padded hex string (16 chars),
        or ``None`` if the key does not exist.

        Parameters
        ----------
        key : str
            The key to compute the SimHash for.

        Returns
        -------
        str | None
            The hex-encoded SimHash (e.g. ``"a1b2c3d4e5f6a7b8"``), or
            ``None`` if the key doesn't exist.
        """
        result = self._execute_command("SIMHASH", key)
        if result is None:
            return None
        return result.decode("utf-8") if isinstance(result, bytes) else str(result)

    def find_similar(self, key: str, threshold: int = 3) -> List[str]:
        """Find keys with similar SimHash via LSH.

        Uses the stored LSH index to find candidate similar profiles
        within the given Hamming distance threshold.

        Parameters
        ----------
        key : str
            The key to find similar profiles for.
        threshold : int
            Maximum Hamming distance for similarity (default 3).

        Returns
        -------
        list[str]
            A list of similar key names.
        """
        result = self._execute_command("FINDSIM", key, str(threshold))
        if result is None:
            return []
        # The response is a RESP array — the client should decode it.
        # _execute_command returns the raw decoded value.
        if isinstance(result, list):
            return [item.decode("utf-8") if isinstance(item, bytes) else str(item) for item in result]
        return []

    def lsh_add(self, key: str, simhash_hex: Optional[str] = None) -> int:
        """Index a key in the LSH similarity index.

        If *simhash_hex* is provided, it is used directly. Otherwise, the
        SimHash is computed from the key's stored value.

        Parameters
        ----------
        key : str
            The key to index.
        simhash_hex : str | None
            Optional pre-computed SimHash as a hex string.

        Returns
        -------
        int
            The number of band entries created.
        """
        if simhash_hex is not None:
            result = self._execute_command("LSHADD", key, simhash_hex)
        else:
            result = self._execute_command("LSHADD", key)
        if isinstance(result, int):
            return result
        if isinstance(result, bytes):
            return int(result)
        return 0

    def lsh_rem(self, key: str, simhash_hex: Optional[str] = None) -> int:
        """Remove a key from the LSH similarity index.

        If *simhash_hex* is provided, it is used directly. Otherwise, the
        stored SimHash metadata is looked up.

        Parameters
        ----------
        key : str
            The key to remove from the index.
        simhash_hex : str | None
            Optional pre-computed SimHash as a hex string.

        Returns
        -------
        int
            The number of band entries removed.
        """
        if simhash_hex is not None:
            result = self._execute_command("LSHREM", key, simhash_hex)
        else:
            result = self._execute_command("LSHREM", key)
        if isinstance(result, int):
            return result
        if isinstance(result, bytes):
            return int(result)
        return 0
