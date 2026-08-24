"""Async public API — AsyncIsolaDB context manager."""

import atexit
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


def _async_shutdown() -> None:
    """Stop all async shared servers. Called via atexit."""
    with _lock:
        for server in _shared_servers.values():
            try:
                server.stop()
            except BaseException:
                pass
        _shared_servers.clear()


atexit.register(_async_shutdown)

# Type aliases for setup callables
SyncSetupFunc = Callable[[str], None]
AsyncSetupFunc = Callable[[str], Coroutine[Any, Any, None]]


def _config_key(config: IsolaDBConfig) -> str:
    """Generate a hashable key for a config to identify shared servers.

    Covers every field that affects server behaviour; startup_timeout is
    deliberately excluded (it only affects how long start() waits).
    Keep in sync with isoladb.database._config_key.
    """
    pg_conf = ",".join(f"{k}={v}" for k, v in sorted(config.pg_conf.items()))
    return (
        f"{config.pg_version}:{config.cache_dir}:{config.ram}:"
        f"{config.ram_size_mb}:{config.use_system_pg}:{pg_conf}"
    )


def _get_or_start_server(config: IsolaDBConfig) -> IsolaDBServer:
    """Get or start the shared server for a config.

    Must run in an executor thread, never on the event-loop thread:
    blocking on the lock there would freeze the loop and deadlock against
    whichever task is starting the server.
    """
    with _lock:
        key = _config_key(config)
        if key not in _shared_servers or not _shared_servers[key].is_running:
            server = IsolaDBServer(config)
            server.start()
            _shared_servers[key] = server
        return _shared_servers[key]


class AsyncIsolaDB:
    """Async ephemeral PostgreSQL database for testing.

    Use as an async context manager to get an isolated database backed by
    an automatically managed PostgreSQL server.

    The server lifecycle (start/stop) is synchronous (subprocess management),
    but the user-facing API is fully async.

    Examples::

        # Connect with any async PostgreSQL library
        async with AsyncIsolaDB() as db:
            # asyncpg
            conn = await asyncpg.connect(
                host=db.host, port=db.port, database=db.dbname
            )
            # psycopg v3 async
            conn = await psycopg.AsyncConnection.connect(db.url)
            # SQLAlchemy async
            engine = create_async_engine(db.url)

        # With schema file — applied automatically via wire protocol
        async with AsyncIsolaDB(schema="schema.sql") as db:
            conn = await asyncpg.connect(
                host=db.host, port=db.port, database=db.dbname
            )
            await conn.execute("INSERT INTO users (name) VALUES ($1)", "Alice")
            await conn.close()

        # With async setup callable — receives the connection URL
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

        # Server lifecycle is synchronous — run in executor to avoid blocking.
        # The shared-server lock is only ever taken inside the executor thread.
        loop = asyncio.get_event_loop()

        server = await loop.run_in_executor(None, _get_or_start_server, self._config)
        self._server = server

        self._dbname = f"isoladb_test_{uuid.uuid4().hex[:12]}"
        await loop.run_in_executor(None, server.create_database, self._dbname)

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
        """Apply schema file/directory and/or setup callable."""
        import asyncio
        import inspect

        from isoladb.database import _apply_setup as _sync_apply_setup

        server = self._server
        dbname = self._dbname
        if server is None or dbname is None:
            raise RuntimeError("AsyncIsolaDB context not entered")

        loop = asyncio.get_event_loop()

        if self._schema is not None:
            # Reuse the sync implementation so single files and directories
            # of .sql files behave identically in both APIs.
            await loop.run_in_executor(
                None, _sync_apply_setup,
                self.url, server.socket_dir, server.port,
                dbname, self._schema, None,
            )

        if self._setup is not None:
            logger.debug("Running setup function")
            if inspect.iscoroutinefunction(self._setup):
                await self._setup(self.url)
            else:
                await loop.run_in_executor(None, self._setup, self.url)

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
            raise RuntimeError("AsyncIsolaDB context not entered")
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

