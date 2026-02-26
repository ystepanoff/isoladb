"""PostgreSQL binary download and cache management."""

import io
import logging
import os
import tarfile
import zipfile
from pathlib import Path
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from isoladb._compat import detect_platform
from isoladb.config import IsolaDBConfig
from isoladb.exceptions import BinaryDownloadError

logger = logging.getLogger("isoladb.binary")

# Maven Central base URL for zonkyio embedded-postgres-binaries
_MAVEN_BASE = "https://repo1.maven.org/maven2/io/zonky/test/postgres"


def _build_download_url(os_name: str, arch: str, version: str) -> str:
    """Build the Maven Central URL for a specific platform binary."""
    artifact = "embedded-postgres-binaries-{os}-{arch}".format(os=os_name, arch=arch)
    return (
        "{base}/{artifact}/{version}/{artifact}-{version}.jar".format(
            base=_MAVEN_BASE, artifact=artifact, version=version
        )
    )


def _cache_path(config: IsolaDBConfig, os_name: str, arch: str) -> Path:
    """Return the cache directory path for a specific version/platform."""
    return Path(config.cache_dir) / config.pg_version / "{}-{}".format(os_name, arch)


def _is_cached(cache_dir: Path) -> bool:
    """Check if a valid cached binary exists."""
    pg_ctl = cache_dir / "bin" / "pg_ctl"
    return pg_ctl.exists()


def _download_and_extract(url: str, dest: Path) -> None:
    """Download a JAR (zip) from Maven Central and extract the PostgreSQL tarball inside."""
    logger.info("Downloading PostgreSQL binary from %s", url)
    try:
        req = Request(url, headers={"User-Agent": "isoladb"})
        response = urlopen(req, timeout=120)
        jar_bytes = response.read()
    except HTTPError as e:
        raise BinaryDownloadError(
            "Failed to download PostgreSQL binary: HTTP {code} from {url}".format(
                code=e.code, url=url
            )
        ) from e
    except URLError as e:
        raise BinaryDownloadError(
            "Failed to download PostgreSQL binary: {reason}".format(reason=e.reason)
        ) from e

    logger.info("Downloaded %d bytes, extracting...", len(jar_bytes))

    # The JAR is a zip file containing a .txz (tar.xz) archive
    try:
        with zipfile.ZipFile(io.BytesIO(jar_bytes)) as zf:
            txz_name = _find_txz_in_zip(zf)
            if txz_name is None:
                raise BinaryDownloadError(
                    "No .txz/.tar.xz archive found inside downloaded JAR"
                )
            txz_data = zf.read(txz_name)
    except zipfile.BadZipFile as e:
        raise BinaryDownloadError("Downloaded file is not a valid JAR/ZIP") from e

    # Extract the tarball
    dest.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(fileobj=io.BytesIO(txz_data), mode="r:xz") as tf:
            tf.extractall(path=str(dest))
    except tarfile.TarError as e:
        raise BinaryDownloadError(
            "Failed to extract PostgreSQL tarball: {}".format(e)
        ) from e

    # The tarball typically extracts with a top-level directory; flatten if needed
    _flatten_if_nested(dest)

    # Make binaries executable
    bin_dir = dest / "bin"
    if bin_dir.exists():
        for f in bin_dir.iterdir():
            if f.is_file():
                f.chmod(f.stat().st_mode | 0o755)

    logger.info("PostgreSQL binary extracted to %s", dest)


def _find_txz_in_zip(zf: zipfile.ZipFile) -> Optional[str]:
    """Find the .txz or .tar.xz file inside a zip archive."""
    for name in zf.namelist():
        if name.endswith(".txz") or name.endswith(".tar.xz"):
            return name
    return None


def _flatten_if_nested(dest: Path) -> None:
    """If extraction created a single subdirectory, move its contents up."""
    children = list(dest.iterdir())
    if len(children) == 1 and children[0].is_dir():
        nested = children[0]
        # Move all contents of the nested directory up to dest
        for item in nested.iterdir():
            target = dest / item.name
            if not target.exists():
                item.rename(target)
        # Remove the now-empty nested directory if possible
        try:
            nested.rmdir()
        except OSError:
            pass


def get_or_download(config: IsolaDBConfig) -> Path:
    """Get the path to a PostgreSQL installation, downloading if necessary.

    Args:
        config: IsolaDB configuration with version and cache settings.

    Returns:
        Path to the PostgreSQL installation directory (parent of bin/).

    Raises:
        BinaryDownloadError: If download or extraction fails.
        UnsupportedPlatformError: If the current platform is not supported.
    """
    os_name, arch = detect_platform()
    cache_dir = _cache_path(config, os_name, arch)

    if _is_cached(cache_dir):
        logger.debug("Using cached PostgreSQL binary at %s", cache_dir)
        return cache_dir

    url = _build_download_url(os_name, arch, config.pg_version)
    _download_and_extract(url, cache_dir)

    if not _is_cached(cache_dir):
        raise BinaryDownloadError(
            "Extraction completed but pg_ctl not found at {}".format(cache_dir / "bin" / "pg_ctl")
        )

    return cache_dir
