"""Tests for binary download and caching."""

import io
import zipfile
from pathlib import Path
from unittest import mock

from isoladb.binary import (
    _build_download_url,
    _cache_path,
    _find_txz_in_zip,
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

    config = IsolaDBConfig(cache_dir=str(cache_dir))

    with mock.patch("isoladb.binary.detect_platform", return_value=("darwin", "arm64v8")):
        with mock.patch("isoladb.binary._download_and_extract") as mock_download:
            result = get_or_download(config)
            mock_download.assert_not_called()
    assert result == pg_dir


def test_get_or_download_triggers_download(tmp_path):
    """If binary is not cached, download it."""
    cache_dir = tmp_path / "cache"
    config = IsolaDBConfig(cache_dir=str(cache_dir))

    def fake_download(url, dest):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "bin").mkdir()
        (dest / "bin" / "pg_ctl").touch()

    with mock.patch("isoladb.binary.detect_platform", return_value=("linux", "amd64")):
        with mock.patch("isoladb.binary._download_and_extract", side_effect=fake_download):
            result = get_or_download(config)

    assert _is_cached(result)
