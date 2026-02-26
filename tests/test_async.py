"""Tests for async API."""

import asyncio
import sys

import pytest

from isoladb import AsyncIsolaDB

pytestmark = pytest.mark.integration


@pytest.fixture
def event_loop_policy():
    """Use default event loop policy."""
    return asyncio.DefaultEventLoopPolicy()


def test_async_context_manager_basic():
    """AsyncIsolaDB works as an async context manager."""

    async def run():
        async with AsyncIsolaDB() as db:
            assert db.dbname.startswith("isoladb_test_")
            assert db.url.startswith("postgresql://")
            assert db.port > 0

            # Use psycopg synchronously via the URL (simplest test)
            import psycopg

            with psycopg.connect(db.url) as conn:
                conn.execute("CREATE TABLE async_test (id serial PRIMARY KEY, val text)")
                conn.execute("INSERT INTO async_test (val) VALUES ('async works')")
                conn.commit()
                result = conn.execute("SELECT val FROM async_test").fetchone()
                assert result is not None
                assert result[0] == "async works"

    asyncio.run(run())


def test_async_isolation():
    """Each async context gets an isolated database."""

    async def run():
        async with AsyncIsolaDB() as db1:
            import psycopg

            with psycopg.connect(db1.url) as conn:
                conn.execute("CREATE TABLE iso_test (id serial)")
                conn.commit()

            async with AsyncIsolaDB() as db2:
                assert db1.dbname != db2.dbname
                with psycopg.connect(db2.url) as conn:
                    # Table should not exist in db2
                    conn.execute("CREATE TABLE iso_test (id serial)")
                    conn.commit()

    asyncio.run(run())


def test_async_with_schema(tmp_path):
    """AsyncIsolaDB supports schema files."""
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text(
        "CREATE TABLE async_schema_test (id serial PRIMARY KEY, name text);\n"
    )

    async def run():
        async with AsyncIsolaDB(schema=str(schema_file)) as db:
            import psycopg

            with psycopg.connect(db.url) as conn:
                conn.execute("INSERT INTO async_schema_test (name) VALUES ('from schema')")
                conn.commit()
                result = conn.execute("SELECT name FROM async_schema_test").fetchone()
                assert result is not None
                assert result[0] == "from schema"

    asyncio.run(run())


def test_async_with_sync_setup():
    """AsyncIsolaDB supports synchronous setup callables."""

    def my_sync_setup(url):
        import psycopg

        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute("CREATE TABLE sync_setup_test (id serial PRIMARY KEY)")

    async def run():
        async with AsyncIsolaDB(setup=my_sync_setup) as db:
            import psycopg

            with psycopg.connect(db.url) as conn:
                conn.execute("INSERT INTO sync_setup_test DEFAULT VALUES")
                conn.commit()
                result = conn.execute("SELECT count(*) FROM sync_setup_test").fetchone()
                assert result[0] == 1

    asyncio.run(run())


def test_async_with_async_setup():
    """AsyncIsolaDB supports async setup callables."""

    async def my_async_setup(url):
        import psycopg

        # Even in async setup, we can use sync psycopg for schema creation
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute("CREATE TABLE async_setup_test (id serial PRIMARY KEY, val int)")
            conn.execute("INSERT INTO async_setup_test (val) VALUES (42)")

    async def run():
        async with AsyncIsolaDB(setup=my_async_setup) as db:
            import psycopg

            with psycopg.connect(db.url) as conn:
                result = conn.execute("SELECT val FROM async_setup_test").fetchone()
                assert result is not None
                assert result[0] == 42

    asyncio.run(run())
