"""Pytest plugin providing isoladb fixtures."""

import uuid
from pathlib import Path
from typing import Any, Callable, Generator, Optional, Union

import pytest

from isoladb.config import IsolaDBConfig
from isoladb.server import IsolaDBServer


class IsolaDBConnection:
    """Connection info for a test database, yielded by the isoladb fixture."""

    __slots__ = ("url", "host", "port", "dbname")

    def __init__(self, url: str, host: str, port: int, dbname: str) -> None:
        self.url = url
        self.host = host
        self.port = port
        self.dbname = dbname

    def connect(self, **kwargs: Any) -> Any:
        """Create a psycopg connection to the test database."""
        import psycopg

        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            **kwargs,
        )

    def __repr__(self) -> str:
        return "IsolaDBConnection(dbname={!r}, port={})".format(self.dbname, self.port)


def pytest_addoption(parser: Any) -> None:
    """Register isoladb ini options."""
    parser.addini(
        "isoladb_pg_version",
        "PostgreSQL version for isoladb (default: latest stable)",
        default=None,
    )
    parser.addini(
        "isoladb_ram",
        "Use RAM disk for isoladb data directory (default: false)",
        type="bool",
        default=False,
    )
    parser.addini(
        "isoladb_schema",
        "Path to a SQL file to apply after creating each test database",
        default=None,
    )


@pytest.fixture(scope="session")
def isoladb_server(request: pytest.FixtureRequest) -> Generator[IsolaDBServer, None, None]:
    """Session-scoped fixture that provides a running PostgreSQL server.

    The server starts once per test session and is stopped at the end.
    """
    config_kwargs = {}  # type: dict[str, Any]

    pg_version = request.config.getini("isoladb_pg_version")
    if pg_version:
        config_kwargs["pg_version"] = pg_version

    ram = request.config.getini("isoladb_ram")  # type: Optional[bool]
    if ram:
        config_kwargs["ram"] = True

    config = IsolaDBConfig(**config_kwargs)
    server = IsolaDBServer(config)
    server.start()

    yield server

    server.stop()


@pytest.fixture(scope="session")
def isoladb_schema(request: pytest.FixtureRequest) -> Optional[str]:
    """Session-scoped fixture returning the configured schema path (if any)."""
    return request.config.getini("isoladb_schema") or None


@pytest.fixture(scope="session")
def isoladb_setup() -> Optional[Callable[[str], None]]:
    """Session-scoped fixture returning a setup callable.

    Override this fixture in your conftest.py to provide a custom
    setup function that runs after each test database is created::

        @pytest.fixture(scope="session")
        def isoladb_setup():
            def apply_migrations(url):
                from alembic.config import Config
                from alembic import command
                cfg = Config("alembic.ini")
                cfg.set_main_option("sqlalchemy.url", url)
                command.upgrade(cfg, "head")
            return apply_migrations
    """
    return None


def _make_db(
    server: IsolaDBServer,
    schema: Optional[str],
    setup: Optional[Callable[[str], None]],
) -> tuple:
    """Create a database and apply schema/setup. Returns (conn_info, dbname)."""
    from isoladb.database import _apply_setup

    dbname = "isoladb_test_{}".format(uuid.uuid4().hex[:12])
    server.create_database(dbname)

    url = "postgresql://localhost/{}?host={}&port={}".format(
        dbname, server.socket_dir, server.port
    )

    _apply_setup(url, schema, setup)

    conn_info = IsolaDBConnection(
        url=url,
        host=server.socket_dir,
        port=server.port,
        dbname=dbname,
    )
    return conn_info, dbname


@pytest.fixture
def isoladb(
    isoladb_server: IsolaDBServer,
    isoladb_schema: Optional[str],
    isoladb_setup: Optional[Callable[[str], None]],
) -> Generator[IsolaDBConnection, None, None]:
    """Per-test fixture that provides an isolated database.

    Creates a fresh database for each test and drops it after.
    Schema and setup are applied automatically if configured.
    """
    conn_info, dbname = _make_db(isoladb_server, isoladb_schema, isoladb_setup)
    yield conn_info
    isoladb_server.drop_database(dbname)


@pytest.fixture
def isoladb_engine(isoladb: IsolaDBConnection) -> Generator[Any, None, None]:
    """Per-test fixture that provides a SQLAlchemy engine.

    Requires sqlalchemy to be installed. Skips the test if not available.
    """
    try:
        from sqlalchemy import create_engine
    except ImportError:
        pytest.skip("sqlalchemy not installed")

    engine = create_engine(isoladb.url)

    yield engine

    engine.dispose()


# --- Async fixtures ---


@pytest.fixture
async def isoladb_async(
    isoladb_server: IsolaDBServer,
    isoladb_schema: Optional[str],
    isoladb_setup: Optional[Callable[[str], None]],
) -> Any:
    """Per-test async fixture that provides an isolated database.

    Works with pytest-asyncio. Creates a fresh database for each test
    and drops it after. Schema and setup are applied automatically.

    Usage requires pytest-asyncio to be installed and configured.
    """
    conn_info, dbname = _make_db(isoladb_server, isoladb_schema, isoladb_setup)
    yield conn_info
    isoladb_server.drop_database(dbname)


@pytest.fixture
async def isoladb_async_engine(isoladb_async: IsolaDBConnection) -> Any:
    """Per-test async fixture that provides a SQLAlchemy async engine.

    Requires sqlalchemy[asyncio] and asyncpg to be installed.
    """
    try:
        from sqlalchemy.ext.asyncio import create_async_engine
    except ImportError:
        pytest.skip("sqlalchemy[asyncio] not installed")

    # Convert postgresql:// to postgresql+asyncpg://
    url = isoladb_async.url.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(url)

    yield engine

    await engine.dispose()
