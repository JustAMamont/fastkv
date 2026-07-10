"""
FastKV async Python client.

An ``asyncio``-based TCP client for communicating with a FastKV server over
RESP protocol.  Safe to use inside ``asyncio`` event loops (FastAPI, aiohttp,
etc.) — it never blocks.

Usage::

    import asyncio
    from fastkv import FastKVAsyncClient

    async def main():
        async with FastKVAsyncClient("localhost", 6379) as c:
            await c.set("hello", "world")
            print(await c.get("hello"))  # b'world'

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Union

from .exceptions import FastKVConnectionError
from .resp import encode_command
from .async_resp import decode_response
from .async_pipeline import AsyncPipeline


class FastKVAsyncClient:
    """Async TCP client for a FastKV (RESP-speaking) server.

    Parameters
    ----------
    host : str
        Server hostname or IP address.  Defaults to ``"localhost"``.
    port : int
        Server port.  Defaults to ``6379``.
    socket_timeout : float | None
        Timeout in seconds for socket operations.  ``None`` means wait
        forever.  Defaults to ``5``.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        socket_timeout: Optional[float] = 5,
        auto_connect: bool = False,
    ) -> None:
        self._host = host
        self._port = port
        self._socket_timeout = socket_timeout
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._auto_connect = auto_connect

    # -- connection lifecycle -------------------------------------------------

    async def connect(self) -> None:
        """Establish a TCP connection to the FastKV server."""
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                self._socket_timeout,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            self._reader = self._writer = None
            raise FastKVConnectionError(
                f"Could not connect to FastKV at {self._host}:{self._port} — {exc}"
            ) from exc

    async def close(self) -> None:
        """Close the connection."""
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._reader = self._writer = None

    async def aclose(self) -> None:
        """Async close — alias for :meth:`close`.  Compatible with ``async with``
        and frameworks that expect ``aclose()`` (e.g. ``anyio``, ``trio``)."""
        await self.close()

    async def _ensure_connected(self) -> None:
        """Ensure we are connected, auto-connecting if configured."""
        if self._reader is None or self._writer is None:
            if self._auto_connect:
                await self.connect()
            else:
                raise FastKVConnectionError("Not connected — call connect() first")

    async def _send_all(self, data: bytes) -> None:
        """Send all *data* bytes."""
        await self._ensure_connected()
        assert self._writer is not None
        try:
            self._writer.write(data)
            await self._writer.drain()
        except (ConnectionError, OSError) as exc:
            self._reader = self._writer = None
            raise FastKVConnectionError("Connection lost while sending data") from exc

    async def _execute_command(self, *args: Any) -> Any:
        """Encode, send, and return decoded response."""
        await self._ensure_connected()
        assert self._reader is not None
        cmd = encode_command(*args)
        try:
            await self._send_all(cmd)
            return await decode_response(self._reader)
        except FastKVConnectionError:
            raise

    # -- context manager (manual connect) --------------------------------------

    async def __aenter__(self) -> "FastKVAsyncClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    # -- pipeline -------------------------------------------------------------

    def pipeline(self) -> AsyncPipeline:
        """Return a new :class:`AsyncPipeline` that shares this connection."""
        return AsyncPipeline(self)

    # =========================================================================
    # Core commands
    # =========================================================================

    async def ping(self) -> str:
        """Send a PING command.  Returns ``"PONG"``."""
        return await self._execute_command("PING")

    async def echo(self, message: str) -> str:
        """Echo the given *message* back from the server."""
        return await self._execute_command("ECHO", message)

    async def info(self, section: Optional[str] = None) -> str:
        """Return server information."""
        if section is None:
            return await self._execute_command("INFO")
        return await self._execute_command("INFO", section)

    async def dbsize(self) -> int:
        """Return the number of keys in the database."""
        return await self._execute_command("DBSIZE")

    async def scan(
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
        raw = await self._execute_command(*args)
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

    async def dbstats(self) -> Dict[str, Any]:
        """Get aggregate store statistics.

        Returns
        -------
        dict
            Statistics including ``total_keys``, ``total_buckets``,
            ``load_factor``, ``entry_size``, ``total_memory``,
            ``blob_count``, and ``inline_size``.
        """
        raw = await self._execute_command("DBSTATS")
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

    async def set(
        self,
        key: str,
        value: str,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Optional[bool]:
        """Set *key* to hold *value*.  Returns ``True`` if set, ``None`` if
        not performed (NX/XX condition)."""
        args: list = ["SET", key, value]
        if ex is not None:
            args += ["EX", str(ex)]
        if px is not None:
            args += ["PX", str(px)]
        if nx:
            args.append("NX")
        if xx:
            args.append("XX")
        result = await self._execute_command(*args)
        if result is None:
            return None
        return result == b"OK" or result == "OK"

    async def get(self, key: str) -> Optional[bytes]:
        """Get the value of *key*.  Returns ``None`` if missing."""
        return await self._execute_command("GET", key)

    async def get_str(self, key: str) -> Optional[str]:
        """Get the value of *key* as a UTF-8 string.  Returns ``None`` if missing."""
        v = await self._execute_command("GET", key)
        if v is None:
            return None
        if isinstance(v, bytes):
            return v.decode("utf-8", errors="replace")
        return str(v)

    async def set_str(self, key: str, value: str) -> Optional[bool]:
        """Set *key* to hold string *value*.  Returns ``True`` if set."""
        return await self.set(key, value)

    async def delete(self, keys: Any) -> int:
        """Remove one or more keys.  Returns count removed."""
        if isinstance(keys, (str, bytes)):
            keys = (keys,)
        return await self._execute_command("DEL", *keys)

    async def exists(self, keys: Any) -> int:
        """Return count of *keys* that exist."""
        if isinstance(keys, (str, bytes)):
            keys = (keys,)
        return await self._execute_command("EXISTS", *keys)

    async def incr(self, key: str) -> int:
        """Increment *key* by one."""
        return await self._execute_command("INCR", key)

    async def decr(self, key: str) -> int:
        """Decrement *key* by one."""
        return await self._execute_command("DECR", key)

    async def incrby(self, key: str, delta: int) -> int:
        """Increment *key* by *delta*."""
        return await self._execute_command("INCRBY", key, delta)

    async def decrby(self, key: str, delta: int) -> int:
        """Decrement *key* by *delta*."""
        return await self._execute_command("DECRBY", key, delta)

    async def append(self, key: str, value: str) -> int:
        """Append *value* to string at *key*.  Returns new length."""
        return await self._execute_command("APPEND", key, value)

    async def strlen(self, key: str) -> int:
        """Return length of string at *key*."""
        return await self._execute_command("STRLEN", key)

    async def getrange(self, key: str, start: int, end: int) -> bytes:
        """Return substring of string at *key*."""
        return await self._execute_command("GETRANGE", key, start, end)

    async def setrange(self, key: str, offset: int, value: str) -> int:
        """Overwrite part of string at *key*.  Returns new length."""
        return await self._execute_command("SETRANGE", key, offset, value)

    async def mset(self, pairs: Dict[str, str]) -> bool:
        """Set multiple keys.  Returns ``True`` on success."""
        flat: list = []
        for k, v in pairs.items():
            flat += [k, v]
        result = await self._execute_command("MSET", *flat)
        return result == b"OK" or result == "OK"

    async def mget(self, keys: Any) -> List[Optional[bytes]]:
        """Return values of all specified *keys*."""
        if isinstance(keys, str):
            keys = (keys,)
        return await self._execute_command("MGET", *keys)

    async def set_nx(self, key: str, value: str) -> bool:
        """Set *key* to *value* only if *key* does not exist.

        Returns ``True`` if the key was set, ``False`` if it already existed.
        """
        result = await self._execute_command("SETNX", key, value)
        return result == 1

    async def get_set(self, key: str, value: str) -> Optional[bytes]:
        """Atomically set *key* to *value* and return the old value."""
        return await self._execute_command("GETSET", key, value)

    async def get_del(self, key: str) -> Optional[bytes]:
        """Atomically get the value of *key* and delete the key."""
        return await self._execute_command("GETDEL", key)

    async def type_of(self, key: str) -> str:
        """Return the type of value stored at *key*.

        Returns one of ``"string"``, ``"hash"``, ``"list"``, or ``"none"``.
        """
        result = await self._execute_command("TYPE", key)
        return result.decode("utf-8") if isinstance(result, bytes) else str(result)

    async def rename(self, key: str, new_key: str) -> bool:
        """Rename *key* to *new_key*.  Returns ``True`` on success."""
        result = await self._execute_command("RENAME", key, new_key)
        return result == b"OK" or result == "OK"

    async def pset_ex(self, key: str, value: str, milliseconds: int) -> bool:
        """Set *key* to *value* with an expiry of *milliseconds*."""
        result = await self._execute_command("PSETEX", key, milliseconds, value)
        return result == b"OK" or result == "OK"

    async def unlink(self, keys: Any) -> int:
        """Asynchronously delete one or more keys.  Returns count removed."""
        if isinstance(keys, (str, bytes)):
            keys = (keys,)
        return await self._execute_command("UNLINK", *keys)

    # =========================================================================
    # Key management / server commands
    # =========================================================================

    async def flush_all(self) -> bool:
        """Remove all keys from all databases.  Returns ``True`` on success."""
        result = await self._execute_command("FLUSHALL")
        return result == b"OK" or result == "OK"

    async def flush_db(self) -> bool:
        """Remove all keys from current database.  Returns ``True`` on success."""
        result = await self._execute_command("FLUSHDB")
        return result == b"OK" or result == "OK"

    async def auth(self, password: str) -> bool:
        """Authenticate to the server.  Returns ``True`` on success."""
        result = await self._execute_command("AUTH", password)
        return result == b"OK" or result == "OK"

    async def save(self) -> bool:
        """Synchronously save the dataset to disk.  Returns ``True`` on success."""
        result = await self._execute_command("SAVE")
        return result == b"OK" or result == "OK"

    async def bgsave(self) -> bool:
        """Asynchronously save the dataset to disk.  Returns ``True`` on success."""
        result = await self._execute_command("BGSAVE")
        return result == b"OK" or result == "OK"

    # =========================================================================
    # TTL commands
    # =========================================================================

    async def expire(self, key: str, seconds: int) -> bool:
        """Set timeout on *key*.  Returns ``True`` on success."""
        result = await self._execute_command("EXPIRE", key, seconds)
        return result == 1

    async def ttl(self, key: str) -> int:
        """Return remaining TTL of *key* in seconds."""
        return await self._execute_command("TTL", key)

    async def pttl(self, key: str) -> int:
        """Return remaining TTL of *key* in milliseconds."""
        return await self._execute_command("PTTL", key)

    async def persist(self, key: str) -> bool:
        """Remove expiry from *key*."""
        result = await self._execute_command("PERSIST", key)
        return result == 1

    # =========================================================================
    # Hash commands
    # =========================================================================

    async def hset(self, key: str, field: str, value: str) -> int:
        """Set *field* in hash at *key*.  Returns 1 if new, 0 if update."""
        return await self._execute_command("HSET", key, field, value)

    async def hget(self, key: str, field: str) -> Optional[bytes]:
        """Return value of *field* in hash at *key*."""
        return await self._execute_command("HGET", key, field)

    async def hdel(self, key: str, *fields: str) -> int:
        """Remove fields from hash at *key*.  Returns count removed."""
        return await self._execute_command("HDEL", key, *fields)

    async def hgetall(self, key: str) -> Dict[bytes, bytes]:
        """Return all fields and values of hash at *key*."""
        raw: list = await self._execute_command("HGETALL", key)
        result: Dict[bytes, bytes] = {}
        it = iter(raw)
        for f, v in zip(it, it):
            result[f] = v
        return result

    async def hexists(self, key: str, field: str) -> bool:
        """Return ``True`` if *field* exists in hash at *key*."""
        return await self._execute_command("HEXISTS", key, field) == 1

    async def hlen(self, key: str) -> int:
        """Return number of fields in hash at *key*."""
        return await self._execute_command("HLEN", key)

    async def hkeys(self, key: str) -> List[bytes]:
        """Return all field names in hash at *key*."""
        return await self._execute_command("HKEYS", key)

    async def hvals(self, key: str) -> List[bytes]:
        """Return all values in hash at *key*."""
        return await self._execute_command("HVALS", key)

    async def hmget(self, key: str, *fields: str) -> List[Optional[bytes]]:
        """Return values of specified fields in hash at *key*."""
        return await self._execute_command("HMGET", key, *fields)

    async def hmset(self, key: str, mapping: Dict[str, str]) -> bool:
        """Set multiple fields in hash at *key*.  Returns ``True`` on success."""
        flat: list = []
        for f, v in mapping.items():
            flat += [f, v]
        result = await self._execute_command("HMSET", key, *flat)
        return result == b"OK" or result == "OK"

    async def h_incr_by(self, key: str, field: str, delta: int) -> int:
        """Increment the integer value of *field* in hash at *key* by *delta*."""
        return await self._execute_command("HINCRBY", key, field, delta)

    async def h_set_nx(self, key: str, field: str, value: str) -> bool:
        """Set *field* in hash at *key* only if *field* does not exist.

        Returns ``True`` if the field was set, ``False`` if it already existed.
        """
        result = await self._execute_command("HSETNX", key, field, value)
        return result == 1

    # =========================================================================
    # List commands
    # =========================================================================

    async def lpush(self, key: str, *elements: str) -> int:
        """Insert *elements* at head of list at *key*.  Returns new length."""
        return await self._execute_command("LPUSH", key, *elements)

    async def rpush(self, key: str, *elements: str) -> int:
        """Insert *elements* at tail of list at *key*.  Returns new length."""
        return await self._execute_command("RPUSH", key, *elements)

    async def lpop(self, key: str, count: Optional[int] = None) -> Optional[bytes]:
        """Remove and return first element(s) of list at *key*."""
        if count is not None:
            return await self._execute_command("LPOP", key, count)
        return await self._execute_command("LPOP", key)

    async def rpop(self, key: str, count: Optional[int] = None) -> Optional[bytes]:
        """Remove and return last element(s) of list at *key*."""
        if count is not None:
            return await self._execute_command("RPOP", key, count)
        return await self._execute_command("RPOP", key)

    async def lrange(self, key: str, start: int, stop: int) -> List[bytes]:
        """Return elements of list at *key* between *start* and *stop*."""
        return await self._execute_command("LRANGE", key, start, stop)

    async def llen(self, key: str) -> int:
        """Return length of list at *key*."""
        return await self._execute_command("LLEN", key)

    async def lindex(self, key: str, index: int) -> Optional[bytes]:
        """Return element at *index* in list at *key*."""
        return await self._execute_command("LINDEX", key, index)

    async def lrem(self, key: str, count: int, element: str) -> int:
        """Remove first *count* occurrences of *element* from list at *key*."""
        return await self._execute_command("LREM", key, count, element)

    async def ltrim(self, key: str, start: int, stop: int) -> bool:
        """Trim list at *key* to range [*start*, *stop*]."""
        result = await self._execute_command("LTRIM", key, start, stop)
        return result == b"OK" or result == "OK"

    async def lset(self, key: str, index: int, element: str) -> bool:
        """Set element at *index* in list at *key*.  Returns ``True`` on success."""
        result = await self._execute_command("LSET", key, index, element)
        return result == b"OK" or result == "OK"

    # =========================================================================
    # Blob commands (require Blob Arena enabled on server — default on;
    # disable with --no-blob-store)
    # =========================================================================

    async def bset(self, key: str, value: Union[str, bytes]) -> bool:
        """Store a large value as a compressed blob.

        The server compresses *value* with zstd and stores it in the blob
        arena.  Use for values that exceed inline size or benefit from
        compression (sessions, JSON, etc.).
        """
        if isinstance(value, str):
            value = value.encode("utf-8")
        result = await self._execute_command("BSET", key, value)
        return result == b"OK" or result == "OK"

    async def bget(self, key: str) -> Optional[bytes]:
        """Retrieve and decompress a blob value.

        If the value is a blob reference, it is transparently decompressed.
        Plain string values are returned as-is.
        """
        return await self._execute_command("BGET", key)

    async def bgetraw(self, key: str) -> Optional[bytes]:
        """Retrieve the raw compressed bytes of a blob value.

        Useful for transferring blob data without decompression overhead.
        """
        return await self._execute_command("BGETRAW", key)

    async def bstats(self) -> Dict[str, Any]:
        """Return blob arena statistics as a dictionary."""
        raw = await self._execute_command("BSTATS")
        if raw is None:
            return {}
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

    async def simhash(self, key: str) -> Optional[str]:
        """Compute SimHash for a stored value.

        Returns the 64-bit SimHash as a zero-padded hex string (16 chars),
        or ``None`` if the key does not exist.
        """
        result = await self._execute_command("SIMHASH", key)
        if result is None:
            return None
        return result.decode("utf-8") if isinstance(result, bytes) else str(result)

    async def find_similar(self, key: str, threshold: int = 3) -> List[str]:
        """Find keys with similar SimHash via LSH.

        Uses the stored LSH index to find candidate similar profiles
        within the given Hamming distance threshold.
        """
        result = await self._execute_command("FINDSIM", key, str(threshold))
        if result is None:
            return []
        if isinstance(result, list):
            return [item.decode("utf-8") if isinstance(item, bytes) else str(item) for item in result]
        return []

    async def lsh_add(self, key: str, simhash_hex: Optional[str] = None) -> int:
        """Index a key in the LSH similarity index.

        If *simhash_hex* is provided, it is used directly. Otherwise, the
        SimHash is computed from the key's stored value.
        """
        if simhash_hex is not None:
            result = await self._execute_command("LSHADD", key, simhash_hex)
        else:
            result = await self._execute_command("LSHADD", key)
        if isinstance(result, int):
            return result
        if isinstance(result, bytes):
            return int(result)
        return 0

    async def lsh_rem(self, key: str, simhash_hex: Optional[str] = None) -> int:
        """Remove a key from the LSH similarity index.

        If *simhash_hex* is provided, it is used directly. Otherwise, the
        stored SimHash metadata is looked up.
        """
        if simhash_hex is not None:
            result = await self._execute_command("LSHREM", key, simhash_hex)
        else:
            result = await self._execute_command("LSHREM", key)
        if isinstance(result, int):
            return result
        if isinstance(result, bytes):
            return int(result)
        return 0
