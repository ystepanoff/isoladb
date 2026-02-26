"""isoladb — Ephemeral PostgreSQL instances for unit testing."""

from isoladb.database import IsolaDB, shutdown
from isoladb.async_database import AsyncIsolaDB
from isoladb.server import IsolaDBServer
from isoladb.config import IsolaDBConfig
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
