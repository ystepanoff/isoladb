"""Async public API — AsyncIsolaDB context manager."""

import logging
import threading
import uuid
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional, Union

from isoladb.config import IsolaDBConfig
from isoladb.server import IsolaDBServer

logger = logging.getLogger("isoladb.async_database")

_shared_servers = {}  # type: dict[str, IsolaDBServer]
_lock = threading.Lock()

# Type aliases for setup callables
SyncSetupFunc = Callable[[str], None]
AsyncSetupFunc = Callable[[str], Coroutine[Any, Any, None]]


def _config_key(config: IsolaDBConfig) -> str:
    """Generate a hashable key for a config to identify shared servers."""
    return "{}:{}:{}".format(config.pg_version, config.ram, config.ram_size_mb)


class AsyncIsolaDB:
    """Async ephemeral PostgreSQL database for testing.

    Use as an async context manager to get an isolated database backed by
    an automatically managed PostgreSQL server.

    The server lifecycle (start/stop) is synchronous (subprocess management),
    but the user-facing API is fully async.

    Examples::

        # Basic usage with asyncpg
        async with AsyncIsolaDB() as db:
            conn = await asyncpg.connect(
                host=db.host, port=db.port, database=db.dbname
            )
            await conn.execute("CREATE TABLE test (id serial PRIMARY KEY)")
            await conn.close()

        # With schema file
        async with AsyncIsolaDB(schema="schema.sql") as db:
            conn = await asyncpg.connect(
                host=db.host, port=db.port, database=db.dbname
            )
            await conn.execute("INSERT INTO users (name) VALUES ($1)", "Alice")
            await conn.close()

        # With async setup callable
        async def apply_migrations(url: str) -> None:
            engine = create_async_engine(url)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            await engine.dispose()

        async with AsyncIsolaDB(setup=apply_migrations) as db:
            ...
    """

    def __init__(
        self,
        pg_version: Optional[str] = None,
        ram: Optional[bool] = None,
        schema: Optional[Union[str, Path]] = None,
        setup: Optional[Union[SyncSetupFunc, AsyncSetupFunc]] = None,
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

    async def __aenter__(self) -> "AsyncIsolaDB":
        import asyncio

        # Server lifecycle is synchronous — run in executor to avoid blocking
        loop = asyncio.get_event_loop()

        with _lock:
            key = _config_key(self._config)
            if key not in _shared_servers or not _shared_servers[key].is_running:
                server = IsolaDBServer(self._config)
                await loop.run_in_executor(None, server.start)
                _shared_servers[key] = server
            self._server = _shared_servers[key]

        self._dbname = "isoladb_test_{}".format(uuid.uuid4().hex[:12])
        await loop.run_in_executor(None, self._server.create_database, self._dbname)

        # Apply schema/setup
        await self._apply_setup()

        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        import asyncio

        if self._server is not None and self._dbname is not None:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, self._server.drop_database, self._dbname
                )
            except Exception:
                pass

    async def _apply_setup(self) -> None:
        """Apply schema file and/or setup callable."""
        import asyncio
        import inspect

        if self._schema is not None:
            schema_path = Path(self._schema)
            if not schema_path.exists():
                raise FileNotFoundError("Schema file not found: {}".format(schema_path))
            logger.debug("Applying schema from %s", schema_path)
            # Schema application is synchronous (reads file, executes SQL)
            loop = asyncio.get_event_loop()
            from isoladb.database import _run_schema_file
            await loop.run_in_executor(None, _run_schema_file, self.url, schema_path)

        if self._setup is not None:
            logger.debug("Running setup function")
            if inspect.iscoroutinefunction(self._setup):
                await self._setup(self.url)
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._setup, self.url)

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
            raise RuntimeError("AsyncIsolaDB context not entered")
        return self._dbname

    @property
    def host(self) -> str:
        """Unix socket directory."""
        return self._server.socket_dir  # type: ignore[union-attr]

    @property
    def port(self) -> int:
        """Server port number."""
        return self._server.port  # type: ignore[union-attr]

    async def connect(self, **kwargs: Any) -> Any:
        """Create an asyncpg connection to the test database.

        Requires asyncpg to be installed.

        Args:
            **kwargs: Additional arguments passed to asyncpg.connect().

        Returns:
            An asyncpg connection.
        """
        try:
            import asyncpg
        except ImportError:
            raise ImportError(
                "asyncpg is required for AsyncIsolaDB.connect(). "
                "Install it with: pip install asyncpg"
            )
        return await asyncpg.connect(
            host=self.host,
            port=self.port,
            database=self.dbname,
            **kwargs,
        )
