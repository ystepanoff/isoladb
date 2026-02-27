"""isoladb — Ephemeral PostgreSQL instances for unit testing."""

from isoladb.async_database import AsyncIsolaDB
from isoladb.config import IsolaDBConfig
from isoladb.database import IsolaDB, shutdown
from isoladb.exceptions import (
    BinaryDownloadError,
    BinaryNotFoundError,
    DatabaseError,
    IsolaDBError,
    RamDiskError,
    ServerStartError,
    ServerStopError,
    UnsupportedPlatformError,
)
from isoladb.server import IsolaDBServer

__version__ = "0.1.0"

__all__ = [
    "IsolaDB",
    "AsyncIsolaDB",
    "IsolaDBConfig",
    "IsolaDBServer",
    "shutdown",
    # Exceptions
    "BinaryDownloadError",
    "BinaryNotFoundError",
    "DatabaseError",
    "IsolaDBError",
    "RamDiskError",
    "ServerStartError",
    "ServerStopError",
    "UnsupportedPlatformError",
]
