"""
Custom exceptions for the FastKV Python client SDK.
"""


class FastKVError(Exception):
    """Base exception for all FastKV client errors."""

    pass


class FastKVConnectionError(FastKVError):
    """Raised when a connection to the FastKV server cannot be established,
    is lost, or a socket-level error occurs."""

    pass


class FastKVResponseError(FastKVError):
    """Raised when the FastKV server returns an error response."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)
