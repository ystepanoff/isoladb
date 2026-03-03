"""Tests for binary download and caching."""

import io
import logging
import subprocess
import zipfile
from pathlib import Path
from unittest import mock

from isoladb.binary import (
    _build_download_url,
    _cache_path,
    _check_version_compatibility,
    _detect_system_pg,
    _find_txz_in_zip,
    _get_system_pg_version,
    _is_cached,
    get_or_download,
)
from isoladb.config import IsolaDBConfig


def test_build_download_url():
    url = _build_download_url("darwin", "arm64v8", "17.2.0")
    assert url == (
        "https://repo1.maven.org/maven2/io/zonky/test/postgres/"
        "embedded-postgres-binaries-darwin-arm64v8/17.2.0/"
        "embedded-postgres-binaries-darwin-arm64v8-17.2.0.jar"
    )


def test_build_download_url_linux():
    url = _build_download_url("linux", "amd64", "16.1.0")
    assert "linux-amd64" in url
    assert "16.1.0" in url


def test_cache_path():
    config = IsolaDBConfig(pg_version="17.2.0", cache_dir="/tmp/test_cache")
    path = _cache_path(config, "darwin", "arm64v8")
    assert path == Path("/tmp/test_cache/17.2.0/darwin-arm64v8")


def test_is_cached_false(tmp_path):
    assert _is_cached(tmp_path) is False


def test_is_cached_true(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    pg_ctl = bin_dir / "pg_ctl"
    pg_ctl.touch()
    assert _is_cached(tmp_path) is True


def test_find_txz_in_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("META-INF/MANIFEST.MF", "test")
        zf.writestr("postgres-linux-x86_64.txz", "fake-data")
    buf.seek(0)
    with zipfile.ZipFile(buf) as zf:
        result = _find_txz_in_zip(zf)
    assert result == "postgres-linux-x86_64.txz"


def test_find_txz_in_zip_not_found():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("META-INF/MANIFEST.MF", "test")
    buf.seek(0)
    with zipfile.ZipFile(buf) as zf:
        result = _find_txz_in_zip(zf)
    assert result is None


def test_get_or_download_uses_cache(tmp_path):
    """If binary is already cached, don't download again."""
    cache_dir = tmp_path / "cache"
    pg_dir = cache_dir / "17.2.0" / "darwin-arm64v8"
    pg_dir.mkdir(parents=True)
    (pg_dir / "bin").mkdir()
    (pg_dir / "bin" / "pg_ctl").touch()

    config = IsolaDBConfig(cache_dir=str(cache_dir), use_system_pg=False)

    with mock.patch("isoladb.binary.detect_platform", return_value=("darwin", "arm64v8")):
        with mock.patch("isoladb.binary._download_and_extract") as mock_download:
            result = get_or_download(config)
            mock_download.assert_not_called()
    assert result == pg_dir


def test_get_or_download_triggers_download(tmp_path):
    """If binary is not cached, download it."""
    cache_dir = tmp_path / "cache"
    config = IsolaDBConfig(cache_dir=str(cache_dir), use_system_pg=False)

    def fake_download(url, dest):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "bin").mkdir()
        (dest / "bin" / "pg_ctl").touch()

    with mock.patch("isoladb.binary.detect_platform", return_value=("linux", "amd64")):
        with mock.patch("isoladb.binary._download_and_extract", side_effect=fake_download):
            result = get_or_download(config)

    assert _is_cached(result)


# --- System PG detection tests ---


def test_detect_system_pg_found(tmp_path):
    """When pg_ctl is on PATH and the installation is valid, return root path."""
    root = tmp_path / "pg"
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "pg_ctl").touch()
    (bin_dir / "initdb").touch()

    config = IsolaDBConfig()

    with mock.patch("isoladb.binary.shutil.which", return_value=str(bin_dir / "pg_ctl")):
        with mock.patch.object(Path, "resolve", return_value=bin_dir / "pg_ctl"):
            with mock.patch(
                "isoladb.binary._get_system_pg_version", return_value="14.19"
            ):
                result = _detect_system_pg(config)

    assert result == root


def test_detect_system_pg_not_found():
    """When pg_ctl is not on PATH, return None."""
    config = IsolaDBConfig()

    with mock.patch("isoladb.binary.shutil.which", return_value=None):
        result = _detect_system_pg(config)

    assert result is None


def test_detect_system_pg_missing_initdb(tmp_path):
    """When pg_ctl exists but initdb does not, return None."""
    root = tmp_path / "pg"
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "pg_ctl").touch()
    # No initdb

    config = IsolaDBConfig()

    with mock.patch("isoladb.binary.shutil.which", return_value=str(bin_dir / "pg_ctl")):
        with mock.patch.object(Path, "resolve", return_value=bin_dir / "pg_ctl"):
            result = _detect_system_pg(config)

    assert result is None


def test_get_system_pg_version_parses():
    """pg_ctl --version output is parsed correctly."""
    mock_result = mock.Mock()
    mock_result.stdout = "pg_ctl (PostgreSQL) 14.19\n"

    with mock.patch("isoladb.binary.subprocess.run", return_value=mock_result):
        version = _get_system_pg_version(Path("/usr/bin/pg_ctl"))

    assert version == "14.19"


def test_get_system_pg_version_failure():
    """If pg_ctl --version fails, return None."""
    with mock.patch(
        "isoladb.binary.subprocess.run",
        side_effect=subprocess.SubprocessError("fail"),
    ):
        version = _get_system_pg_version(Path("/usr/bin/pg_ctl"))

    assert version is None


def test_version_compatibility_warns_on_major_mismatch(caplog):
    """A warning is logged when major versions differ."""
    with caplog.at_level(logging.WARNING, logger="isoladb.binary"):
        _check_version_compatibility("14.19", "17.2.0")

    assert "differs from configured" in caplog.text


def test_version_compatibility_silent_on_match(caplog):
    """No warning when major versions match."""
    with caplog.at_level(logging.WARNING, logger="isoladb.binary"):
        _check_version_compatibility("17.3", "17.2.0")

    assert caplog.text == ""


def test_get_or_download_prefers_system(tmp_path):
    """When system PG is detected, download is not called."""
    system_root = tmp_path / "system_pg"
    system_root.mkdir()

    config = IsolaDBConfig(use_system_pg=True)

    with mock.patch("isoladb.binary._detect_system_pg", return_value=system_root):
        with mock.patch("isoladb.binary.detect_platform") as mock_platform:
            with mock.patch("isoladb.binary._download_and_extract") as mock_download:
                result = get_or_download(config)
                mock_platform.assert_not_called()
                mock_download.assert_not_called()

    assert result == system_root


def test_get_or_download_skips_system_when_disabled(tmp_path):
    """When use_system_pg=False, system detection is not called."""
    cache_dir = tmp_path / "cache"
    pg_dir = cache_dir / "17.2.0" / "darwin-arm64v8"
    pg_dir.mkdir(parents=True)
    (pg_dir / "bin").mkdir()
    (pg_dir / "bin" / "pg_ctl").touch()

    config = IsolaDBConfig(cache_dir=str(cache_dir), use_system_pg=False)

    with mock.patch("isoladb.binary._detect_system_pg") as mock_detect:
        with mock.patch("isoladb.binary.detect_platform", return_value=("darwin", "arm64v8")):
            result = get_or_download(config)
            mock_detect.assert_not_called()

    assert result == pg_dir
