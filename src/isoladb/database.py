"""Main public API — IsolaDB context manager."""

import logging
import threading
import uuid
from pathlib import Path
from typing import Any, Callable, Optional, Union

import psycopg

from isoladb.config import IsolaDBConfig
from isoladb.server import IsolaDBServer

logger = logging.getLogger("isoladb.database")

_shared_servers = {}  # type: dict[str, IsolaDBServer]
_lock = threading.Lock()

# Type alias for the setup callable: receives a connection URL string
SetupFunc = Callable[[str], None]


def _config_key(config: IsolaDBConfig) -> str:
    """Generate a hashable key for a config to identify shared servers."""
    return "{}:{}:{}".format(config.pg_version, config.ram, config.ram_size_mb)


def _run_schema_file(url: str, schema_path: Path) -> None:
    """Execute a SQL file against the database."""
    sql_text = schema_path.read_text(encoding="utf-8")
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(sql_text)


def _apply_setup(url: str, schema: Optional[Union[str, Path]], setup: Optional[SetupFunc]) -> None:
    """Apply schema file and/or setup callable to a freshly created database."""
    if schema is not None:
        schema_path = Path(schema)
        if not schema_path.exists():
            raise FileNotFoundError("Schema file not found: {}".format(schema_path))
        logger.debug("Applying schema from %s", schema_path)
        _run_schema_file(url, schema_path)

    if setup is not None:
        logger.debug("Running setup function")
        setup(url)


class IsolaDB:
    """Ephemeral PostgreSQL database for testing.

    Use as a context manager to get an isolated database backed by
    an automatically managed PostgreSQL server.

    Examples::

        # Basic usage
        with IsolaDB() as db:
            conn = psycopg.connect(db.url)
            conn.execute("CREATE TABLE test (id serial PRIMARY KEY)")

        # With schema file — applied automatically after DB creation
        with IsolaDB(schema="schema.sql") as db:
            conn = db.connect()
            conn.execute("INSERT INTO users (name) VALUES ('Alice')")

        # With setup callable — receives the connection URL
        def apply_migrations(url):
            from alembic.config import Config
            from alembic import command
            cfg = Config("alembic.ini")
            cfg.set_main_option("sqlalchemy.url", url)
            command.upgrade(cfg, "head")

        with IsolaDB(setup=apply_migrations) as db:
            conn = db.connect()
            # tables from migrations are ready
    """

    def __init__(
        self,
        pg_version: Optional[str] = None,
        ram: Optional[bool] = None,
        schema: Optional[Union[str, Path]] = None,
        setup: Optional[SetupFunc] = None,
        **kwargs: Any,
    ) -> None:
        config_args = {}  # type: dict[str, Any]
        if pg_version is not None:
            config_args["pg_version"] = pg_version
        if ram is not None:
            config_args["ram"] = ram
        config_args.update(kwargs)
        self._config = IsolaDBConfig(**config_args)
        self._schema = schema
        self._setup = setup
        self._dbname = None  # type: Optional[str]
        self._server = None  # type: Optional[IsolaDBServer]

    def __enter__(self) -> "IsolaDB":
        with _lock:
            key = _config_key(self._config)
            if key not in _shared_servers or not _shared_servers[key].is_running:
                server = IsolaDBServer(self._config)
                server.start()
                _shared_servers[key] = server
            self._server = _shared_servers[key]

        self._dbname = "isoladb_test_{}".format(uuid.uuid4().hex[:12])
        self._server.create_database(self._dbname)
        _apply_setup(self.url, self._schema, self._setup)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._server is not None and self._dbname is not None:
            try:
                self._server.drop_database(self._dbname)
            except Exception:
                pass

    @property
    def url(self) -> str:
        """PostgreSQL connection URL for the test database."""
        return "postgresql://localhost/{}?host={}&port={}".format(
            self._dbname, self._server.socket_dir, self._server.port  # type: ignore[union-attr]
        )

    @property
    def dbname(self) -> str:
        """Name of the test database."""
        if self._dbname is None:
            raise RuntimeError("IsolaDB context not entered")
        return self._dbname

    @property
    def host(self) -> str:
        """Unix socket directory."""
        return self._server.socket_dir  # type: ignore[union-attr]

    @property
    def port(self) -> int:
        """Server port number."""
        return self._server.port  # type: ignore[union-attr]

    def connect(self, **kwargs: Any) -> "psycopg.Connection[Any]":
        """Create a psycopg connection to the test database.

        Args:
            **kwargs: Additional arguments passed to psycopg.connect().

        Returns:
            A psycopg connection.
        """
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            **kwargs,
        )


def shutdown() -> None:
    """Explicitly stop all shared servers.

    Called automatically via atexit, but can be called manually
    for immediate cleanup.
    """
    with _lock:
        for server in _shared_servers.values():
            try:
                server.stop()
            except Exception:
                pass
        _shared_servers.clear()
