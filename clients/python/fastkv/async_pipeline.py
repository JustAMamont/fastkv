"""
Async pipeline support for the FastKV client.

Buffers multiple commands and executes them as a single batch when
:meth:`execute` is called.  Like :class:`Pipeline` but async.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

from .async_resp import decode_response
from .exceptions import FastKVConnectionError
from .resp import encode_command

if TYPE_CHECKING:
    from .async_client import FastKVAsyncClient


_DEFAULT_MAX_PIPELINE_SIZE = 10_000


class AsyncPipeline:
    """Async pipeline — buffers commands and executes them in one batch.

    Usage::

        async with client.pipeline() as pipe:
            pipe.set("k1", "v1")
            pipe.get("k1")
            results = await pipe.execute()
        # results == [True, b'v1']
    """

    def __init__(
        self,
        client: "FastKVAsyncClient",
        max_size: int | None = _DEFAULT_MAX_PIPELINE_SIZE,
    ) -> None:
        self._client = client
        self._commands: List[bytes] = []
        self._max_size = max_size
        self._is_context_manager = False

    # -- command buffering (same API as sync Pipeline) ------------------------

    def set(self, key: str, value: str, ex: int = None, px: int = None,
            nx: bool = False, xx: bool = False) -> "AsyncPipeline":
        args = ["SET", key, value]
        if ex is not None: args += ["EX", str(ex)]
        if px is not None: args += ["PX", str(px)]
        if nx: args.append("NX")
        if xx: args.append("XX")
        self._commands.append(encode_command(*args))
        return self

    def get(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("GET", key))
        return self

    def delete(self, keys: Any) -> "AsyncPipeline":
        if isinstance(keys, (str, bytes)): keys = (keys,)
        self._commands.append(encode_command("DEL", *keys))
        return self

    def exists(self, keys: Any) -> "AsyncPipeline":
        if isinstance(keys, (str, bytes)): keys = (keys,)
        self._commands.append(encode_command("EXISTS", *keys))
        return self

    def incr(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("INCR", key))
        return self

    def decr(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("DECR", key))
        return self

    def incrby(self, key: str, delta: int) -> "AsyncPipeline":
        self._commands.append(encode_command("INCRBY", key, delta))
        return self

    def decrby(self, key: str, delta: int) -> "AsyncPipeline":
        self._commands.append(encode_command("DECRBY", key, delta))
        return self

    def append(self, key: str, value: str) -> "AsyncPipeline":
        self._commands.append(encode_command("APPEND", key, value))
        return self

    def strlen(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("STRLEN", key))
        return self

    def getrange(self, key: str, start: int, end: int) -> "AsyncPipeline":
        self._commands.append(encode_command("GETRANGE", key, start, end))
        return self

    def setrange(self, key: str, offset: int, value: str) -> "AsyncPipeline":
        self._commands.append(encode_command("SETRANGE", key, offset, value))
        return self

    def mset(self, pairs: dict) -> "AsyncPipeline":
        flat: list = []
        for k, v in pairs.items(): flat += [k, v]
        self._commands.append(encode_command("MSET", *flat))
        return self

    def mget(self, keys: Any) -> "AsyncPipeline":
        if isinstance(keys, str): keys = (keys,)
        self._commands.append(encode_command("MGET", *keys))
        return self

    def expire(self, key: str, seconds: int) -> "AsyncPipeline":
        self._commands.append(encode_command("EXPIRE", key, seconds))
        return self

    def ttl(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("TTL", key))
        return self

    def pttl(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("PTTL", key))
        return self

    def persist(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("PERSIST", key))
        return self

    def hset(self, key: str, field: str, value: str) -> "AsyncPipeline":
        self._commands.append(encode_command("HSET", key, field, value))
        return self

    def hget(self, key: str, field: str) -> "AsyncPipeline":
        self._commands.append(encode_command("HGET", key, field))
        return self

    def hdel(self, key: str, *fields: str) -> "AsyncPipeline":
        self._commands.append(encode_command("HDEL", key, *fields))
        return self

    def hgetall(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("HGETALL", key))
        return self

    def hexists(self, key: str, field: str) -> "AsyncPipeline":
        self._commands.append(encode_command("HEXISTS", key, field))
        return self

    def hlen(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("HLEN", key))
        return self

    def hkeys(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("HKEYS", key))
        return self

    def hvals(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("HVALS", key))
        return self

    def hmget(self, key: str, *fields: str) -> "AsyncPipeline":
        self._commands.append(encode_command("HMGET", key, *fields))
        return self

    def hmset(self, key: str, mapping: dict) -> "AsyncPipeline":
        flat: list = []
        for f, v in mapping.items(): flat += [f, v]
        self._commands.append(encode_command("HMSET", key, *flat))
        return self

    def lpush(self, key: str, *elements: str) -> "AsyncPipeline":
        self._commands.append(encode_command("LPUSH", key, *elements))
        return self

    def rpush(self, key: str, *elements: str) -> "AsyncPipeline":
        self._commands.append(encode_command("RPUSH", key, *elements))
        return self

    def lpop(self, key: str, count: int = None) -> "AsyncPipeline":
        if count is not None:
            self._commands.append(encode_command("LPOP", key, count))
        else:
            self._commands.append(encode_command("LPOP", key))
        return self

    def rpop(self, key: str, count: int = None) -> "AsyncPipeline":
        if count is not None:
            self._commands.append(encode_command("RPOP", key, count))
        else:
            self._commands.append(encode_command("RPOP", key))
        return self

    def lrange(self, key: str, start: int, stop: int) -> "AsyncPipeline":
        self._commands.append(encode_command("LRANGE", key, start, stop))
        return self

    def llen(self, key: str) -> "AsyncPipeline":
        self._commands.append(encode_command("LLEN", key))
        return self

    def lindex(self, key: str, index: int) -> "AsyncPipeline":
        self._commands.append(encode_command("LINDEX", key, index))
        return self

    def lrem(self, key: str, count: int, element: str) -> "AsyncPipeline":
        self._commands.append(encode_command("LREM", key, count, element))
        return self

    def ltrim(self, key: str, start: int, stop: int) -> "AsyncPipeline":
        self._commands.append(encode_command("LTRIM", key, start, stop))
        return self

    def lset(self, key: str, index: int, element: str) -> "AsyncPipeline":
        self._commands.append(encode_command("LSET", key, index, element))
        return self

    def ping(self) -> "AsyncPipeline":
        self._commands.append(encode_command("PING"))
        return self

    # -- execution ------------------------------------------------------------

    async def execute(self) -> List[Any]:
        """Send all buffered commands and return a list of responses."""
        if not self._commands:
            return []

        if self._max_size is not None and len(self._commands) > self._max_size:
            raise ValueError(
                f"Pipeline buffer size {len(self._commands)} exceeds "
                f"max_size {self._max_size}"
            )

        payload = b"".join(self._commands)
        await self._client._send_all(payload)

        assert self._client._reader is not None
        results: List[Any] = []
        for _ in self._commands:
            results.append(await decode_response(self._client._reader))

        self._commands.clear()
        return results

    def clear(self) -> None:
        """Discard all buffered commands."""
        self._commands.clear()

    @property
    def command_count(self) -> int:
        return len(self._commands)

    # -- async context manager -------------------------------------------------

    async def __aenter__(self) -> "AsyncPipeline":
        self._is_context_manager = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            await self.execute()
        else:
            self.clear()
