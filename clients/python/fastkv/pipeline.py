"""
Pipeline support for the FastKV client.

A :class:`Pipeline` buffers multiple commands and sends them all to the server
in a single batch when :meth:`execute` is called (either explicitly or
implicitly via the context-manager ``__exit__``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

from .exceptions import FastKVConnectionError
from .resp import decode_response, encode_command

if TYPE_CHECKING:
    from .client import FastKVClient


# Default maximum number of commands a pipeline may buffer before raising.
_DEFAULT_MAX_PIPELINE_SIZE = 10_000


class Pipeline:
    """Buffers commands and executes them as a single batch.

    Usage::

        with client.pipeline() as pipe:
            pipe.set("k1", "v1")
            pipe.get("k1")
            results = pipe.execute()
        # results == [True, b'v1']

    Or without a context manager::

        pipe = client.pipeline()
        pipe.set("k1", "v1")
        pipe.get("k1")
        results = pipe.execute()

    Parameters
    ----------
    client:
        The :class:`FastKVClient` instance whose connection is shared.
    max_size : int | None
        Maximum number of commands the pipeline may buffer.  If exceeded,
        :meth:`execute` raises :class:`ValueError`.  ``None`` means unlimited.
    """

    def __init__(self, client: "FastKVClient", max_size: int | None = _DEFAULT_MAX_PIPELINE_SIZE) -> None:
        self._client = client
        self._commands: List[bytes] = []
        self._max_size = max_size
        self._is_context_manager = False

    # -- command buffering ----------------------------------------------------

    def set(self, key: str, value: str, ex: int = None, px: int = None,
            nx: bool = False, xx: bool = False) -> "Pipeline":
        """Buffer a SET command.  See :meth:`FastKVClient.set` for docs."""
        args = ["SET", key, value]
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
        self._commands.append(encode_command(*args))
        return self

    def get(self, key: str) -> "Pipeline":
        """Buffer a GET command."""
        self._commands.append(encode_command("GET", key))
        return self

    def delete(self, keys: Any) -> "Pipeline":
        """Buffer a DEL command.  ``del`` is a Python keyword so the method
        is named ``delete``."""
        if isinstance(keys, (str, bytes)):
            keys = (keys,)
        self._commands.append(encode_command("DEL", *keys))
        return self

    def exists(self, keys: Any) -> "Pipeline":
        """Buffer an EXISTS command."""
        if isinstance(keys, (str, bytes)):
            keys = (keys,)
        self._commands.append(encode_command("EXISTS", *keys))
        return self

    def incr(self, key: str) -> "Pipeline":
        """Buffer an INCR command."""
        self._commands.append(encode_command("INCR", key))
        return self

    def decr(self, key: str) -> "Pipeline":
        """Buffer a DECR command."""
        self._commands.append(encode_command("DECR", key))
        return self

    def incrby(self, key: str, delta: int) -> "Pipeline":
        """Buffer an INCRBY command."""
        self._commands.append(encode_command("INCRBY", key, delta))
        return self

    def decrby(self, key: str, delta: int) -> "Pipeline":
        """Buffer a DECRBY command."""
        self._commands.append(encode_command("DECRBY", key, delta))
        return self

    def append(self, key: str, value: str) -> "Pipeline":
        """Buffer an APPEND command."""
        self._commands.append(encode_command("APPEND", key, value))
        return self

    def strlen(self, key: str) -> "Pipeline":
        """Buffer a STRLEN command."""
        self._commands.append(encode_command("STRLEN", key))
        return self

    def getrange(self, key: str, start: int, end: int) -> "Pipeline":
        """Buffer a GETRANGE command."""
        self._commands.append(encode_command("GETRANGE", key, start, end))
        return self

    def setrange(self, key: str, offset: int, value: str) -> "Pipeline":
        """Buffer a SETRANGE command."""
        self._commands.append(encode_command("SETRANGE", key, offset, value))
        return self

    def mset(self, pairs: dict) -> "Pipeline":
        """Buffer an MSET command."""
        flat: list = []
        for k, v in pairs.items():
            flat.append(k)
            flat.append(v)
        self._commands.append(encode_command("MSET", *flat))
        return self

    def mget(self, keys: Any) -> "Pipeline":
        """Buffer an MGET command."""
        if isinstance(keys, str):
            keys = (keys,)
        self._commands.append(encode_command("MGET", *keys))
        return self

    def expire(self, key: str, seconds: int) -> "Pipeline":
        """Buffer an EXPIRE command."""
        self._commands.append(encode_command("EXPIRE", key, seconds))
        return self

    def ttl(self, key: str) -> "Pipeline":
        """Buffer a TTL command."""
        self._commands.append(encode_command("TTL", key))
        return self

    def pttl(self, key: str) -> "Pipeline":
        """Buffer a PTTL command."""
        self._commands.append(encode_command("PTTL", key))
        return self

    def persist(self, key: str) -> "Pipeline":
        """Buffer a PERSIST command."""
        self._commands.append(encode_command("PERSIST", key))
        return self

    def hset(self, key: str, field: str, value: str) -> "Pipeline":
        """Buffer an HSET command."""
        self._commands.append(encode_command("HSET", key, field, value))
        return self

    def hget(self, key: str, field: str) -> "Pipeline":
        """Buffer an HGET command."""
        self._commands.append(encode_command("HGET", key, field))
        return self

    def hdel(self, key: str, *fields: str) -> "Pipeline":
        """Buffer an HDEL command."""
        self._commands.append(encode_command("HDEL", key, *fields))
        return self

    def hgetall(self, key: str) -> "Pipeline":
        """Buffer an HGETALL command."""
        self._commands.append(encode_command("HGETALL", key))
        return self

    def hexists(self, key: str, field: str) -> "Pipeline":
        """Buffer a HEXISTS command."""
        self._commands.append(encode_command("HEXISTS", key, field))
        return self

    def hlen(self, key: str) -> "Pipeline":
        """Buffer an HLEN command."""
        self._commands.append(encode_command("HLEN", key))
        return self

    def hkeys(self, key: str) -> "Pipeline":
        """Buffer an HKEYS command."""
        self._commands.append(encode_command("HKEYS", key))
        return self

    def hvals(self, key: str) -> "Pipeline":
        """Buffer an HVALS command."""
        self._commands.append(encode_command("HVALS", key))
        return self

    def hmget(self, key: str, *fields: str) -> "Pipeline":
        """Buffer an HMGET command."""
        self._commands.append(encode_command("HMGET", key, *fields))
        return self

    def hmset(self, key: str, mapping: dict) -> "Pipeline":
        """Buffer an HMSET command."""
        flat: list = []
        for f, v in mapping.items():
            flat.append(f)
            flat.append(v)
        self._commands.append(encode_command("HMSET", key, *flat))
        return self

    def lpush(self, key: str, *elements: str) -> "Pipeline":
        """Buffer an LPUSH command."""
        self._commands.append(encode_command("LPUSH", key, *elements))
        return self

    def rpush(self, key: str, *elements: str) -> "Pipeline":
        """Buffer an RPUSH command."""
        self._commands.append(encode_command("RPUSH", key, *elements))
        return self

    def lpop(self, key: str, count: int = None) -> "Pipeline":
        """Buffer an LPOP command."""
        if count is not None:
            self._commands.append(encode_command("LPOP", key, count))
        else:
            self._commands.append(encode_command("LPOP", key))
        return self

    def rpop(self, key: str, count: int = None) -> "Pipeline":
        """Buffer an RPOP command."""
        if count is not None:
            self._commands.append(encode_command("RPOP", key, count))
        else:
            self._commands.append(encode_command("RPOP", key))
        return self

    def lrange(self, key: str, start: int, stop: int) -> "Pipeline":
        """Buffer an LRANGE command."""
        self._commands.append(encode_command("LRANGE", key, start, stop))
        return self

    def llen(self, key: str) -> "Pipeline":
        """Buffer an LLEN command."""
        self._commands.append(encode_command("LLEN", key))
        return self

    def lindex(self, key: str, index: int) -> "Pipeline":
        """Buffer an LINDEX command."""
        self._commands.append(encode_command("LINDEX", key, index))
        return self

    def lrem(self, key: str, count: int, element: str) -> "Pipeline":
        """Buffer an LREM command."""
        self._commands.append(encode_command("LREM", key, count, element))
        return self

    def ltrim(self, key: str, start: int, stop: int) -> "Pipeline":
        """Buffer an LTRIM command."""
        self._commands.append(encode_command("LTRIM", key, start, stop))
        return self

    def lset(self, key: str, index: int, element: str) -> "Pipeline":
        """Buffer an LSET command."""
        self._commands.append(encode_command("LSET", key, index, element))
        return self

    def ping(self) -> "Pipeline":
        """Buffer a PING command."""
        self._commands.append(encode_command("PING"))
        return self

    # -- execution ------------------------------------------------------------

    def execute(self) -> List[Any]:
        """Send all buffered commands to the server and return a list of
        responses.

        Returns
        -------
        list
            One element per buffered command, in order.

        Raises
        ------
        ValueError
            If the number of buffered commands exceeds ``max_size``.
        """
        if not self._commands:
            return []

        if self._max_size is not None and len(self._commands) > self._max_size:
            raise ValueError(
                f"Pipeline buffer size {len(self._commands)} exceeds "
                f"max_size {self._max_size}"
            )

        # Combine all commands into one payload and send at once.
        payload = b"".join(self._commands)
        self._client._send_all(payload)  # noqa: SLF001

        # IMPORTANT: grab the socket reference AFTER _send_all so that if
        # _send_all triggered a reconnect we read from the *live* socket.
        sock = self._client._ensure_connected()  # noqa: SLF001

        results: List[Any] = []
        for _ in self._commands:
            results.append(decode_response(sock))

        self._commands.clear()
        return results

    def clear(self) -> None:
        """Discard all buffered commands without executing them."""
        self._commands.clear()

    @property
    def command_count(self) -> int:
        """Number of commands currently buffered."""
        return len(self._commands)

    # -- context manager ------------------------------------------------------

    def __enter__(self) -> "Pipeline":
        self._is_context_manager = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.execute()
        else:
            self.clear()
