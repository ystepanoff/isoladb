"""PostgreSQL server lifecycle management."""

import atexit
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional

import psycopg
from psycopg import sql

from isoladb.binary import get_or_download
from isoladb.config import IsolaDBConfig
from isoladb.exceptions import DatabaseError, ServerStartError, ServerStopError
from isoladb.ramdisk import RamDisk, create_data_directory

logger = logging.getLogger("isoladb.server")


def _find_free_port() -> int:
    """Find an available TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class IsolaDBServer:
    """Manages a single ephemeral PostgreSQL server instance.

    The server uses a temporary data directory and unix domain sockets
    to avoid conflicts with other PostgreSQL installations.
    """

    def __init__(self, config: Optional[IsolaDBConfig] = None) -> None:
        self._config = config or IsolaDBConfig()
        self._pg_dir = None  # type: Optional[Path]
        self._data_dir = None  # type: Optional[Path]
        self._socket_dir = None  # type: Optional[str]
        self._port = 0
        self._log_file = None  # type: Optional[str]
        self._ramdisk = None  # type: Optional[RamDisk]
        self._tmpdir = None  # type: Optional[str]
        self._running = False
        self._atexit_registered = False

    @property
    def socket_dir(self) -> str:
        """Unix socket directory path."""
        if self._socket_dir is None:
            raise ServerStartError("Server has not been started")
        return self._socket_dir

    @property
    def port(self) -> int:
        """Port number the server is listening on."""
        return self._port

    @property
    def is_running(self) -> bool:
        """Whether the server is currently running."""
        return self._running

    def start(self) -> None:
        """Start the PostgreSQL server.

        Downloads the binary if needed, initializes the data directory,
        and starts the server process.

        Raises:
            ServerStartError: If the server fails to start.
        """
        if self._running:
            logger.debug("Server already running")
            return

        # Get PostgreSQL binaries
        self._pg_dir = get_or_download(self._config)
        logger.info("Using PostgreSQL binaries at %s", self._pg_dir)

        # Create temp base directory for socket and logs
        self._tmpdir = tempfile.mkdtemp(prefix="isoladb_run_")

        # Create data directory (optionally RAM-backed)
        self._data_dir, self._ramdisk = create_data_directory(
            ram=self._config.ram,
            size_mb=self._config.ram_size_mb,
        )

        # Socket directory (must be short path on macOS — max 104 chars for unix socket)
        self._socket_dir = tempfile.mkdtemp(prefix="pg_", dir="/tmp")
        self._log_file = os.path.join(self._tmpdir, "postgresql.log")
        self._port = _find_free_port()

        try:
            self._run_initdb()
            self._configure_postgresql()
            self._start_server()
            self._wait_for_ready()
        except Exception:
            # Clean up on failure
            self._cleanup()
            raise

        self._running = True

        # Register atexit handler
        if not self._atexit_registered:
            atexit.register(self._atexit_stop)
            self._atexit_registered = True

        logger.info(
            "PostgreSQL server started (port=%d, socket=%s, data=%s)",
            self._port,
            self._socket_dir,
            self._data_dir,
        )

    def stop(self) -> None:
        """Stop the PostgreSQL server and clean up resources.

        Raises:
            ServerStopError: If the server fails to stop cleanly.
        """
        if not self._running:
            return

        self._running = False

        # Stop the server
        pg_ctl = self._pg_dir / "bin" / "pg_ctl"  # type: ignore[union-attr]
        try:
            subprocess.run(
                [
                    str(pg_ctl),
                    "-D", str(self._data_dir),
                    "stop",
                    "-m", "fast",
                    "-w",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            logger.info("PostgreSQL server stopped")
        except subprocess.CalledProcessError as e:
            logger.warning("pg_ctl stop failed: %s", e.stderr)
            # Try immediate shutdown
            try:
                subprocess.run(
                    [
                        str(pg_ctl),
                        "-D", str(self._data_dir),
                        "stop",
                        "-m", "immediate",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
            except Exception:
                logger.error("Failed to stop PostgreSQL server even with immediate mode")
        except subprocess.TimeoutExpired:
            raise ServerStopError("PostgreSQL server stop timed out")

        self._cleanup()

        # Deregister atexit
        if self._atexit_registered:
            atexit.unregister(self._atexit_stop)
            self._atexit_registered = False

    def create_database(self, name: str) -> None:
        """Create a new database on this server.

        Args:
            name: Database name to create.

        Raises:
            DatabaseError: If database creation fails.
        """
        if not self._running:
            raise DatabaseError("Server is not running")

        try:
            with psycopg.connect(
                host=self._socket_dir,
                port=self._port,
                dbname="postgres",
                autocommit=True,
            ) as conn:
                conn.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
                )
        except psycopg.Error as e:
            raise DatabaseError("Failed to create database {!r}: {}".format(name, e)) from e

        logger.debug("Created database %s", name)

    def drop_database(self, name: str) -> None:
        """Drop a database from this server.

        First terminates any active connections to the database.

        Args:
            name: Database name to drop.

        Raises:
            DatabaseError: If database drop fails.
        """
        if not self._running:
            return

        try:
            with psycopg.connect(
                host=self._socket_dir,
                port=self._port,
                dbname="postgres",
                autocommit=True,
            ) as conn:
                # Terminate connections to the target database
                conn.execute(
                    sql.SQL(
                        "SELECT pg_terminate_backend(pid) "
                        "FROM pg_stat_activity "
                        "WHERE datname = {} AND pid <> pg_backend_pid()"
                    ).format(sql.Literal(name))
                )
                conn.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name))
                )
        except psycopg.Error as e:
            raise DatabaseError("Failed to drop database {!r}: {}".format(name, e)) from e

        logger.debug("Dropped database %s", name)

    def _run_initdb(self) -> None:
        """Initialize the PostgreSQL data directory."""
        initdb = self._pg_dir / "bin" / "initdb"  # type: ignore[union-attr]
        try:
            subprocess.run(
                [
                    str(initdb),
                    "-D", str(self._data_dir),
                    "--no-locale",
                    "--encoding=UTF8",
                    "--auth=trust",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.CalledProcessError as e:
            raise ServerStartError(
                "initdb failed:\nstdout: {}\nstderr: {}".format(e.stdout, e.stderr)
            )
        except FileNotFoundError:
            raise ServerStartError(
                "initdb not found at {}".format(initdb)
            )

    def _configure_postgresql(self) -> None:
        """Write performance-optimized postgresql.conf settings."""
        conf_path = self._data_dir / "postgresql.conf"  # type: ignore[union-attr]
        settings = {
            "fsync": "off",
            "synchronous_commit": "off",
            "full_page_writes": "off",
            "shared_buffers": "64MB",
            "work_mem": "16MB",
        }
        # Merge user-provided settings (they take precedence)
        settings.update(self._config.pg_conf)

        with open(str(conf_path), "a") as f:
            f.write("\n# isoladb settings\n")
            for key, value in settings.items():
                f.write("{} = {}\n".format(key, value))

    def _start_server(self) -> None:
        """Start the PostgreSQL server process."""
        pg_ctl = self._pg_dir / "bin" / "pg_ctl"  # type: ignore[union-attr]
        server_opts = "-p {} -k {} -h ''".format(self._port, self._socket_dir)

        try:
            subprocess.run(
                [
                    str(pg_ctl),
                    "-D", str(self._data_dir),
                    "-l", str(self._log_file),
                    "-o", server_opts,
                    "start",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except subprocess.CalledProcessError as e:
            log_content = self._read_log()
            raise ServerStartError(
                "pg_ctl start failed:\nstderr: {}\nlog: {}".format(e.stderr, log_content)
            )

    def _wait_for_ready(self) -> None:
        """Poll until the server is accepting connections.

        Uses psycopg connection attempts instead of pg_isready,
        since the zonkyio binary distribution does not include pg_isready.
        """
        deadline = time.monotonic() + self._config.startup_timeout

        while time.monotonic() < deadline:
            try:
                with psycopg.connect(
                    host=self._socket_dir,
                    port=self._port,
                    dbname="postgres",
                    connect_timeout=2,
                ) as conn:
                    conn.execute("SELECT 1")
                    return
            except psycopg.OperationalError:
                time.sleep(0.1)

        log_content = self._read_log()
        raise ServerStartError(
            "PostgreSQL server did not become ready within {} seconds.\nLog:\n{}".format(
                self._config.startup_timeout, log_content
            )
        )

    def _read_log(self) -> str:
        """Read the PostgreSQL log file contents."""
        if self._log_file and os.path.exists(self._log_file):
            try:
                with open(self._log_file, "r") as f:
                    return f.read()
            except OSError:
                return "<could not read log>"
        return "<no log file>"

    def _cleanup(self) -> None:
        """Remove temporary directories and RAM disk."""
        if self._ramdisk is not None:
            try:
                self._ramdisk.destroy()
            except Exception:
                logger.warning("Failed to destroy ramdisk", exc_info=True)
            self._ramdisk = None

        if self._data_dir is not None:
            parent = self._data_dir.parent
            shutil.rmtree(str(parent), ignore_errors=True)
            self._data_dir = None

        if self._socket_dir is not None:
            shutil.rmtree(self._socket_dir, ignore_errors=True)
            self._socket_dir = None

        if self._tmpdir is not None:
            shutil.rmtree(self._tmpdir, ignore_errors=True)
            self._tmpdir = None

    def _atexit_stop(self) -> None:
        """Atexit handler to ensure the server is stopped."""
        try:
            self.stop()
        except Exception:
            pass
