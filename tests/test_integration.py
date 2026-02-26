"""Integration tests — requires PostgreSQL binary download.

These tests actually start a PostgreSQL server and run queries.
They are slow on first run (binary download) but fast after caching.

Skip with: pytest -m "not integration"
"""

import pytest

from isoladb import IsolaDB

pytestmark = pytest.mark.integration


def test_context_manager_basic():
    """Basic context manager starts server, creates DB, runs query."""
    with IsolaDB() as db:
        assert db.dbname.startswith("isoladb_test_")
        assert db.url.startswith("postgresql://")
        assert db.port > 0

        conn = db.connect()
        try:
            conn.execute("CREATE TABLE test_table (id serial PRIMARY KEY, name text)")
            conn.execute("INSERT INTO test_table (name) VALUES ('hello')")
            conn.commit()
            result = conn.execute("SELECT name FROM test_table").fetchone()
            assert result is not None
            assert result[0] == "hello"
        finally:
            conn.close()


def test_isolation_between_contexts():
    """Each context manager invocation gets a separate database."""
    with IsolaDB() as db1:
        conn1 = db1.connect()
        try:
            conn1.execute("CREATE TABLE shared_name (id serial)")
            conn1.commit()
        finally:
            conn1.close()

        with IsolaDB() as db2:
            assert db1.dbname != db2.dbname
            conn2 = db2.connect()
            try:
                # This table should NOT exist in db2
                conn2.execute("CREATE TABLE shared_name (id serial)")
                conn2.commit()
            finally:
                conn2.close()


def test_url_is_connectable():
    """The .url property works with psycopg.connect()."""
    import psycopg

    with IsolaDB() as db:
        conn = psycopg.connect(db.url)
        try:
            result = conn.execute("SELECT 1").fetchone()
            assert result is not None
            assert result[0] == 1
        finally:
            conn.close()


def test_pytest_fixture(isoladb):
    """Test the pytest fixture provides a working database."""
    conn = isoladb.connect()
    try:
        conn.execute("CREATE TABLE fixture_test (val int)")
        conn.execute("INSERT INTO fixture_test VALUES (42)")
        conn.commit()
        result = conn.execute("SELECT val FROM fixture_test").fetchone()
        assert result is not None
        assert result[0] == 42
    finally:
        conn.close()
