"""PostgreSQL binary download and cache management."""

import io
import logging
import re
import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from isoladb._compat import detect_platform
from isoladb.config import IsolaDBConfig
from isoladb.exceptions import BinaryDownloadError

logger = logging.getLogger("isoladb.binary")

# Maven Central base URL for zonkyio embedded-postgres-binaries
_MAVEN_BASE = "https://repo1.maven.org/maven2/io/zonky/test/postgres"


def _build_download_url(os_name: str, arch: str, version: str) -> str:
    """Build the Maven Central URL for a specific platform binary."""
    artifact = f"embedded-postgres-binaries-{os_name}-{arch}"
    return (
        f"{_MAVEN_BASE}/{artifact}/{version}/{artifact}-{version}.jar"
    )


def _cache_path(config: IsolaDBConfig, os_name: str, arch: str) -> Path:
    """Return the cache directory path for a specific version/platform."""
    return Path(config.cache_dir) / config.pg_version / f"{os_name}-{arch}"


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
            f"Failed to download PostgreSQL binary: HTTP {e.code} from {url}"
        ) from e
    except URLError as e:
        raise BinaryDownloadError(
            f"Failed to download PostgreSQL binary: {e.reason}"
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
            # Use 'data' filter on Python 3.12+ to avoid DeprecationWarning
            if hasattr(tarfile, "data_filter"):
                tf.extractall(path=str(dest), filter="data")
            else:
                tf.extractall(path=str(dest))
    except tarfile.TarError as e:
        raise BinaryDownloadError(
            f"Failed to extract PostgreSQL tarball: {e}"
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


def _get_system_pg_version(pg_ctl: Path) -> Optional[str]:
    """Run pg_ctl --version and parse the version string.

    Returns the version string (e.g. "14.19") or None on failure.
    """
    try:
        result = subprocess.run(
            [str(pg_ctl), "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        match = re.search(r"(\d+(?:\.\d+)*)", result.stdout)
        if match:
            return match.group(1)
    except (subprocess.SubprocessError, OSError):
        pass
    return None


def _check_version_compatibility(system_version: str, configured_version: str) -> None:
    """Log a warning if the system PG major version differs from configured."""
    system_major = system_version.split(".")[0]
    configured_major = configured_version.split(".")[0]
    if system_major != configured_major:
        logger.warning(
            "System PostgreSQL major version %s differs from configured %s",
            system_version,
            configured_version,
        )


def _detect_system_pg(config: IsolaDBConfig) -> Optional[Path]:
    """Detect a system-installed PostgreSQL and return its root path.

    Uses shutil.which("pg_ctl") to find the binary, resolves symlinks,
    and walks up to the installation root. Validates that both pg_ctl
    and initdb exist in the bin/ directory.

    Returns the installation root Path or None if not found/valid.
    """
    pg_ctl_path = shutil.which("pg_ctl")
    if pg_ctl_path is None:
        return None

    resolved = Path(pg_ctl_path).resolve()
    # pg_ctl lives at <root>/bin/pg_ctl, so go up two levels
    root = resolved.parent.parent

    if not (root / "bin" / "pg_ctl").exists():
        return None
    if not (root / "bin" / "initdb").exists():
        return None

    version = _get_system_pg_version(resolved)
    if version is not None:
        _check_version_compatibility(version, config.pg_version)
        logger.info("Using system PostgreSQL %s at %s", version, root)
    else:
        logger.info("Using system PostgreSQL at %s (version unknown)", root)

    return root


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
    if config.use_system_pg:
        system_pg = _detect_system_pg(config)
        if system_pg is not None:
            return system_pg

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
