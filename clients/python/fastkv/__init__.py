"""
FastKV Python Client SDK
========================

Lightweight, standalone RESP client for communicating with a FastKV server
over TCP.  No third-party dependencies — only the Python standard library.

Both synchronous and asynchronous clients are provided:

* :class:`FastKVClient` — blocking, for scripts / Django / Flask.
* :class:`FastKVAsyncClient` — ``asyncio``-based, for FastAPI / aiohttp /
  any async event loop.
"""

__version__ = "0.2.0"

from .client import FastKVClient
from .async_client import FastKVAsyncClient
from .pipeline import Pipeline
from .async_pipeline import AsyncPipeline
from .exceptions import FastKVConnectionError, FastKVError, FastKVResponseError

__all__ = [
    "FastKVClient",
    "FastKVAsyncClient",
    "Pipeline",
    "AsyncPipeline",
    "FastKVError",
    "FastKVConnectionError",
    "FastKVResponseError",
]
