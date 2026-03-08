"""Integration tests — requires PostgreSQL binary download.

These tests actually start a PostgreSQL server and run queries.
They are slow on first run (binary download) but fast after caching.

Skip with: pytest -m "not integration"
"""

import errno
import os
import signal

import psycopg
import pytest

from isoladb import IsolaDB
from isoladb.server import IsolaDBServer

pytestmark = pytest.mark.integration


def test_context_manager_basic():
    """Basic context manager starts server, creates DB, runs query."""
    with IsolaDB() as db:
        assert db.dbname.startswith("isoladb_test_")
        assert db.url.startswith("postgresql://")
        assert db.port > 0

        with psycopg.connect(db.url) as conn:
            conn.execute("CREATE TABLE test_table (id serial PRIMARY KEY, name text)")
            conn.execute("INSERT INTO test_table (name) VALUES ('hello')")
            conn.commit()
            result = conn.execute("SELECT name FROM test_table").fetchone()
            assert result is not None
            assert result[0] == "hello"


def test_isolation_between_contexts():
    """Each context manager invocation gets a separate database."""
    with IsolaDB() as db1:
        with psycopg.connect(db1.url) as conn1:
            conn1.execute("CREATE TABLE shared_name (id serial)")
            conn1.commit()

        with IsolaDB() as db2:
            assert db1.dbname != db2.dbname
            with psycopg.connect(db2.url) as conn2:
                # This table should NOT exist in db2
                conn2.execute("CREATE TABLE shared_name (id serial)")
                conn2.commit()


def test_url_is_connectable():
    """The .url property works with psycopg.connect()."""
    with IsolaDB() as db:
        with psycopg.connect(db.url) as conn:
            result = conn.execute("SELECT 1").fetchone()
            assert result is not None
            assert result[0] == 1


def test_host_port_connectable():
    """The .host/.port/.dbname properties work for direct connections."""
    with IsolaDB() as db:
        with psycopg.connect(
            host=db.host, port=db.port, dbname=db.dbname, user="postgres",
        ) as conn:
            result = conn.execute("SELECT 1").fetchone()
            assert result is not None
            assert result[0] == 1


def test_pytest_fixture(isoladb):
    """Test the pytest fixture provides a working database."""
    with psycopg.connect(isoladb.url) as conn:
        conn.execute("CREATE TABLE fixture_test (val int)")
        conn.execute("INSERT INTO fixture_test VALUES (42)")
        conn.commit()
        result = conn.execute("SELECT val FROM fixture_test").fetchone()
        assert result is not None
        assert result[0] == 42


def test_no_orphan_process_after_stop():
    """Server.stop() must leave no postgres process behind."""
    server = IsolaDBServer()
    server.start()

    # Read the postmaster PID while the server is still up.
    pid_file = server._data_dir / "postmaster.pid"
    assert pid_file.exists(), "postmaster.pid not found after start"
    with open(str(pid_file)) as f:
        pid = int(f.readline().strip())

    # Confirm the process is alive before we stop it.
    os.kill(pid, 0)

    server.stop()

    # The process must be gone now.
    try:
        os.kill(pid, 0)
        pytest.fail(f"postgres process (pid={pid}) is still running after stop()")
    except OSError as e:
        assert e.errno == errno.ESRCH, f"unexpected OSError: {e}"


def test_ram_disk():
    """Start a server with ram=True and verify data directory is on a RAM disk."""
    import sys

    with IsolaDB(ram=True) as db:
        server = db._server

        # Verify a RamDisk object was created (None means fallback to disk)
        assert server._ramdisk is not None, (
            "Expected RamDisk, got None (fell back to disk)"
        )

        # Platform-specific device checks
        if sys.platform == "darwin":
            assert server._ramdisk.device is not None
            assert server._ramdisk.device.startswith("/dev/disk")
        elif sys.platform == "linux":
            assert server._ramdisk.path is not None

        # Verify the data directory is on the ramdisk and PG is functional
        assert server._data_dir is not None
        assert (server._data_dir / "PG_VERSION").exists()

        with psycopg.connect(db.url) as conn:
            conn.execute("CREATE TABLE ram_test (id serial PRIMARY KEY, val text)")
            conn.execute("INSERT INTO ram_test (val) VALUES ('ram')")
            conn.commit()
            result = conn.execute("SELECT val FROM ram_test").fetchone()
            assert result is not None
            assert result[0] == "ram"
