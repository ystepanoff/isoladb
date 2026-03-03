"""Configuration for isoladb."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

DEFAULT_PG_VERSION = "17.2.0"
DEFAULT_CACHE_DIR = os.path.join(Path.home(), ".cache", "isoladb")


@dataclass
class IsolaDBConfig:
    """Configuration for an IsolaDB instance.

    Attributes:
        pg_version: PostgreSQL version to use. Defaults to latest stable.
        cache_dir: Directory to cache downloaded PostgreSQL binaries.
        ram: If True, force tmpfs (Linux) or RAM disk (macOS) for the data directory.
        ram_size_mb: Size of the RAM disk in megabytes (only used when ram=True).
        startup_timeout: Seconds to wait for PostgreSQL server to become ready.
        use_system_pg: If True, use system-installed PostgreSQL when available.
        pg_conf: Extra postgresql.conf settings as key-value pairs.
    """

    pg_version: str = DEFAULT_PG_VERSION
    cache_dir: str = DEFAULT_CACHE_DIR
    ram: bool = False
    ram_size_mb: int = 256
    startup_timeout: float = 30.0
    use_system_pg: bool = True
    pg_conf: dict = field(default_factory=dict)  # type: ignore[type-arg]
    _data_dir: Optional[str] = field(default=None, repr=False)
