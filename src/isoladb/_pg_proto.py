"""Minimal PostgreSQL wire protocol client.

Speaks just enough of the PostgreSQL v3 protocol to execute simple
statements (CREATE DATABASE, DROP DATABASE, SELECT 1) without
requiring any external PostgreSQL client library.

This makes isoladb completely library-agnostic — users install
whatever PG library their application needs.

Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html
"""

import logging
import socket
import struct
from pathlib import Path

from isoladb.exceptions import DatabaseError

logger = logging.getLogger("isoladb._pg_proto")

# Message type bytes
_AUTH_REQUEST = ord("R")
_READY_FOR_QUERY = ord("Z")
_ERROR_RESPONSE = ord("E")
_COMMAND_COMPLETE = ord("C")
_DATA_ROW = ord("D")
_ROW_DESCRIPTION = ord("T")
_NOTICE_RESPONSE = ord("N")
_PARAMETER_STATUS = ord("S")
_BACKEND_KEY_DATA = ord("K")


def _encode_startup(user: str, database: str) -> bytes:
    """Encode a StartupMessage (protocol v3.0)."""
    # Protocol version 3.0
    payload = struct.pack("!I", 196608)  # 3 << 16 | 0
    payload += b"user\x00" + user.encode("utf-8") + b"\x00"
    payload += b"database\x00" + database.encode("utf-8") + b"\x00"
    payload += b"\x00"  # terminator
    # Length prefix includes itself but not the initial non-existent type byte
    return struct.pack("!I", len(payload) + 4) + payload


def _encode_query(sql: str) -> bytes:
    """Encode a simple Query message."""
    encoded = sql.encode("utf-8") + b"\x00"
    return b"Q" + struct.pack("!I", len(encoded) + 4) + encoded


def _encode_terminate() -> bytes:
    """Encode a Terminate message."""
    return b"X" + struct.pack("!I", 4)


def _read_message(sock: socket.socket) -> tuple:
    """Read a single message from the server. Returns (type_byte, payload)."""
    header = _recv_exact(sock, 5)
    msg_type = header[0]
    length = struct.unpack("!I", header[1:5])[0]
    payload = _recv_exact(sock, length - 4) if length > 4 else b""
    return msg_type, payload


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes from the socket."""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed by server")
        data += chunk
    return data


def _parse_error(payload: bytes) -> str:
    """Extract the error message from an ErrorResponse payload."""
    fields = {}
    i = 0
    while i < len(payload):
        field_type = payload[i]
        if field_type == 0:
            break
        i += 1
        end = payload.index(b"\x00", i)
        fields[chr(field_type)] = payload[i:end].decode("utf-8", errors="replace")
        i = end + 1
    # 'M' is the primary message, 'S' is severity
    return fields.get("M", "Unknown error")


def _connect_unix(socket_dir: str, port: int) -> socket.socket:
    """Connect to PostgreSQL via unix domain socket."""
    socket_path = str(Path(socket_dir) / f".s.PGSQL.{port}")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect(socket_path)
    return sock


def _startup(sock: socket.socket, database: str) -> None:
    """Perform startup handshake."""
    sock.sendall(_encode_startup("postgres", database))

    # Read messages until ReadyForQuery
    while True:
        msg_type, payload = _read_message(sock)

        if msg_type == _AUTH_REQUEST:
            auth_type = struct.unpack("!I", payload[:4])[0]
            if auth_type == 0:
                # AuthenticationOk
                continue
            else:
                raise DatabaseError(
                    f"Unexpected auth type: {auth_type} (isoladb uses trust auth)"
                )
        elif msg_type == _READY_FOR_QUERY:
            return
        elif msg_type == _ERROR_RESPONSE:
            raise DatabaseError(f"Startup failed: {_parse_error(payload)}")
        elif msg_type in (_PARAMETER_STATUS, _BACKEND_KEY_DATA, _NOTICE_RESPONSE):
            continue
        else:
            continue


def _execute_simple(sock: socket.socket, sql: str) -> None:
    """Execute a simple query and wait for completion."""
    sock.sendall(_encode_query(sql))

    while True:
        msg_type, payload = _read_message(sock)

        if msg_type == _READY_FOR_QUERY:
            return
        elif msg_type == _ERROR_RESPONSE:
            raise DatabaseError(f"Query failed: {_parse_error(payload)}")
        elif msg_type in (
            _COMMAND_COMPLETE,
            _DATA_ROW,
            _ROW_DESCRIPTION,
            _NOTICE_RESPONSE,
        ):
            continue
        else:
            continue


def execute(socket_dir: str, port: int, sql: str, database: str = "postgres") -> None:
    """Connect to PostgreSQL and execute a SQL statement.

    Uses the PostgreSQL wire protocol directly — no client library needed.

    Args:
        socket_dir: Unix socket directory.
        port: Server port.
        sql: SQL statement to execute.
        database: Database to connect to.
    """
    sock = _connect_unix(socket_dir, port)
    try:
        _startup(sock, database)
        _execute_simple(sock, sql)
        sock.sendall(_encode_terminate())
    finally:
        sock.close()


def create_database(socket_dir: str, port: int, name: str) -> None:
    """Create a database with safe quoting."""
    # PostgreSQL identifier quoting: double-quote, escape internal double-quotes
    safe_name = '"{}"'.format(name.replace('"', '""'))
    execute(socket_dir, port, f"CREATE DATABASE {safe_name}")
    logger.debug("Created database %s", name)


def drop_database(socket_dir: str, port: int, name: str) -> None:
    """Drop a database, terminating active connections first."""
    safe_name = '"{}"'.format(name.replace('"', '""'))
    # Use dollar-quoting for the literal to avoid escaping issues
    terminate_sql = (
        "SELECT pg_terminate_backend(pid) "
        "FROM pg_stat_activity "
        f"WHERE datname = $${name}$$ AND pid <> pg_backend_pid()"
    )
    try:
        execute(socket_dir, port, terminate_sql)
    except DatabaseError:
        pass  # Best effort — connections may already be gone
    execute(socket_dir, port, f"DROP DATABASE IF EXISTS {safe_name}")
    logger.debug("Dropped database %s", name)


def check_ready(socket_dir: str, port: int) -> bool:
    """Check if the server is accepting connections."""
    try:
        execute(socket_dir, port, "SELECT 1")
        return True
    except Exception:
        return False
