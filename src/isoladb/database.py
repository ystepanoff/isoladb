"""Main public API — IsolaDB context manager."""

import atexit
import logging
import threading
import uuid
from pathlib import Path
from typing import Any, Callable, Optional, Union

from isoladb._pg_proto import execute as _pg_execute
from isoladb.config import IsolaDBConfig
from isoladb.server import IsolaDBServer

logger = logging.getLogger("isoladb.database")

_shared_servers = {}  # type: dict[str, IsolaDBServer]
_lock = threading.Lock()

# Type alias for the setup callable: receives a connection URL string
SetupFunc = Callable[[str], None]


def _config_key(config: IsolaDBConfig) -> str:
    """Generate a hashable key for a config to identify shared servers."""
    return f"{config.pg_version}:{config.ram}:{config.ram_size_mb}"


def _run_schema_file(socket_dir: str, port: int, dbname: str, schema_path: Path) -> None:
    """Execute a SQL file against the database using the PostgreSQL wire protocol."""
    sql_text = schema_path.read_text(encoding="utf-8")
    _pg_execute(socket_dir, port, sql_text, database=dbname)


def _apply_setup(
    url: str,
    socket_dir: str,
    port: int,
    dbname: str,
    schema: Optional[Union[str, Path]],
    setup: Optional[SetupFunc],
) -> None:
    """Apply schema file/directory and/or setup callable to a freshly created database."""
    if schema is not None:
        schema_path = Path(schema)
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema path not found: {schema_path}")
        if schema_path.is_dir():
            sql_files = sorted(schema_path.glob("*.sql"))
            if not sql_files:
                logger.warning("No .sql files found in %s", schema_path)
            for sql_file in sql_files:
                logger.debug("Applying schema from %s", sql_file)
                _run_schema_file(socket_dir, port, dbname, sql_file)
        else:
            logger.debug("Applying schema from %s", schema_path)
            _run_schema_file(socket_dir, port, dbname, schema_path)

    if setup is not None:
        logger.debug("Running setup function")
        setup(url)


class IsolaDB:
    """Ephemeral PostgreSQL database for testing.

    Use as a context manager to get an isolated database backed by
    an automatically managed PostgreSQL server.

    Examples::

        # Connect with any PostgreSQL library you prefer
        with IsolaDB() as db:
            # psycopg v3
            conn = psycopg.connect(db.url)
            # psycopg2
            conn = psycopg2.connect(host=db.host, port=db.port, dbname=db.dbname)
            # asyncpg
            conn = await asyncpg.connect(host=db.host, port=db.port, database=db.dbname)
            # SQLAlchemy
            engine = create_engine(db.url)

        # With schema file — applied automatically after DB creation
        with IsolaDB(schema="schema.sql") as db:
            conn = psycopg.connect(db.url)
            conn.execute("INSERT INTO users (name) VALUES ('Alice')")

        # With setup callable — receives the connection URL
        def apply_migrations(url):
            from alembic.config import Config
            from alembic import command
            cfg = Config("alembic.ini")
            cfg.set_main_option("sqlalchemy.url", url)
            command.upgrade(cfg, "head")

        with IsolaDB(setup=apply_migrations) as db:
            engine = create_engine(db.url)
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

        self._dbname = f"isoladb_test_{uuid.uuid4().hex[:12]}"
        self._server.create_database(self._dbname)
        _apply_setup(
            self.url, self._server.socket_dir, self._server.port,
            self._dbname, self._schema, self._setup,
        )
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
        socket = self._server.socket_dir  # type: ignore[union-attr]
        port = self._server.port  # type: ignore[union-attr]
        return f"postgresql://postgres@localhost/{self._dbname}?host={socket}&port={port}"

    @property
    def dbname(self) -> str:
        """Name of the test database."""
        if self._dbname is None:
            raise RuntimeError("IsolaDB context not entered")
        return self._dbname

    @property
    def user(self) -> str:
        """PostgreSQL superuser name."""
        return "postgres"

    @property
    def host(self) -> str:
        """Unix socket directory."""
        return self._server.socket_dir  # type: ignore[union-attr]

    @property
    def port(self) -> int:
        """Server port number."""
        return self._server.port  # type: ignore[union-attr]

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


atexit.register(shutdown)
