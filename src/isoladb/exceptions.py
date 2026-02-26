"""Custom exceptions for isoladb."""


class IsolaDBError(Exception):
    """Base exception for all isoladb errors."""


class BinaryDownloadError(IsolaDBError):
    """Failed to download or extract PostgreSQL binary."""


class BinaryNotFoundError(IsolaDBError):
    """PostgreSQL binary not found in cache or on disk."""


class ServerStartError(IsolaDBError):
    """Failed to start PostgreSQL server."""


class ServerStopError(IsolaDBError):
    """Failed to stop PostgreSQL server."""


class DatabaseError(IsolaDBError):
    """Failed to create or drop a database."""


class RamDiskError(IsolaDBError):
    """Failed to create or destroy a RAM disk."""


class UnsupportedPlatformError(IsolaDBError):
    """Current platform is not supported."""
