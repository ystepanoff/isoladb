"""Tests for shared-server cache keys and async lock behaviour."""

import asyncio
import threading
import time
from unittest import mock

import pytest

from isoladb import AsyncIsolaDB, async_database, database
from isoladb.config import IsolaDBConfig

_MODULES = pytest.mark.parametrize(
    "module", [database, async_database], ids=["database", "async_database"]
)


@_MODULES
def test_config_key_same_config_shares_server(module):
    assert module._config_key(IsolaDBConfig()) == module._config_key(IsolaDBConfig())


@_MODULES
def test_config_key_distinguishes_pg_conf(module):
    base = IsolaDBConfig()
    tuned = IsolaDBConfig(pg_conf={"max_connections": "200"})
    assert module._config_key(base) != module._config_key(tuned)


@_MODULES
def test_config_key_pg_conf_order_irrelevant(module):
    a = IsolaDBConfig(pg_conf={"fsync": "on", "work_mem": "32MB"})
    b = IsolaDBConfig(pg_conf={"work_mem": "32MB", "fsync": "on"})
    assert module._config_key(a) == module._config_key(b)


@_MODULES
def test_config_key_distinguishes_use_system_pg(module):
    assert module._config_key(IsolaDBConfig(use_system_pg=True)) != module._config_key(
        IsolaDBConfig(use_system_pg=False)
    )


@_MODULES
def test_config_key_distinguishes_cache_dir(module, tmp_path):
    assert module._config_key(IsolaDBConfig()) != module._config_key(
        IsolaDBConfig(cache_dir=str(tmp_path))
    )


class _FakeServer:
    """Stands in for IsolaDBServer; start() is slow like real startup."""

    socket_dir = "/tmp/fake"
    port = 5432

    def __init__(self, config):
        self.is_running = True

    def start(self):
        time.sleep(0.2)

    def stop(self):
        pass

    def create_database(self, name):
        pass

    def drop_database(self, name):
        pass


def test_concurrent_async_enter_does_not_deadlock():
    """Regression: __aenter__ once held the shared-server lock across an
    await, freezing the event loop whenever two contexts entered concurrently.
    """
    saved = dict(async_database._shared_servers)
    async_database._shared_servers.clear()
    result = {}

    def run():
        async def enter(ram):
            async with AsyncIsolaDB(ram=ram):
                pass

        async def main():
            # Different configs, but the lock is global — both contend on it
            await asyncio.gather(enter(False), enter(True))

        try:
            asyncio.run(main())
            result["ok"] = True
        except BaseException as exc:
            result["error"] = exc

    try:
        with mock.patch.object(async_database, "IsolaDBServer", _FakeServer):
            worker = threading.Thread(target=run, daemon=True)
            worker.start()
            worker.join(timeout=10)
        assert "error" not in result, f"concurrent __aenter__ raised: {result['error']!r}"
        assert result.get("ok"), "concurrent AsyncIsolaDB __aenter__ deadlocked"
    finally:
        async_database._shared_servers.clear()
        async_database._shared_servers.update(saved)
