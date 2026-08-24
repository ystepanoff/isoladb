"""Tests for the pytest plugin report header — must never download or raise."""

from unittest import mock

from isoladb import pytest_plugin
from isoladb._compat import detect_platform
from isoladb.binary import find_local
from isoladb.config import DEFAULT_PG_VERSION, IsolaDBConfig
from isoladb.exceptions import UnsupportedPlatformError


class _FakeConfig:
    """Minimal stand-in for pytest's Config with ini values."""

    def __init__(self, **ini):
        self._ini = ini

    def getini(self, name):
        return self._ini.get(name)


def test_find_local_returns_none_without_download(tmp_path):
    """Nothing cached, system PG disabled — no path and no network access."""
    config = IsolaDBConfig(cache_dir=str(tmp_path), use_system_pg=False)
    assert find_local(config) is None


def test_find_local_returns_cached(tmp_path):
    config = IsolaDBConfig(cache_dir=str(tmp_path), use_system_pg=False)
    os_name, arch = detect_platform()
    bin_dir = tmp_path / config.pg_version / f"{os_name}-{arch}" / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "pg_ctl").touch()

    assert find_local(config) == bin_dir.parent


def test_report_header_reports_local_install(tmp_path):
    with mock.patch.object(pytest_plugin, "find_local", return_value=tmp_path):
        lines = pytest_plugin.pytest_report_header(_FakeConfig())
    assert lines == [f"isoladb: PostgreSQL at {tmp_path}"]


def test_report_header_does_not_download():
    """No local installation available — report that, don't fetch 50MB."""
    with mock.patch.object(pytest_plugin, "find_local", return_value=None):
        lines = pytest_plugin.pytest_report_header(_FakeConfig())
    assert lines == [
        f"isoladb: PostgreSQL {DEFAULT_PG_VERSION} (will download on first use)"
    ]


def test_report_header_swallows_platform_errors():
    """Unsupported platform must not break pytest startup."""
    with mock.patch.object(
        pytest_plugin, "find_local", side_effect=UnsupportedPlatformError("win32")
    ):
        assert pytest_plugin.pytest_report_header(_FakeConfig()) == []
