"""RAM disk / tmpfs management for isoladb."""

import logging
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional, Tuple

from isoladb.exceptions import RamDiskError

logger = logging.getLogger("isoladb.ramdisk")


class RamDisk:
    """Represents a mounted RAM disk that can be destroyed on cleanup."""

    def __init__(self, path: Path, device: Optional[str] = None) -> None:
        self.path = path
        self.device = device  # macOS disk device, e.g. /dev/disk4
        self._destroyed = False

    def destroy(self) -> None:
        """Unmount and destroy the RAM disk."""
        if self._destroyed:
            return
        self._destroyed = True

        if sys.platform == "darwin" and self.device:
            _destroy_macos_ramdisk(self.device, self.path)
        elif sys.platform == "linux":
            _destroy_linux_tmpfs(self.path)

    def __repr__(self) -> str:
        return "RamDisk(path={!r}, device={!r})".format(self.path, self.device)


def create_data_directory(
    ram: bool = False, size_mb: int = 256
) -> Tuple[Path, Optional[RamDisk]]:
    """Create a data directory, optionally backed by a RAM disk.

    Args:
        ram: If True, attempt to create a tmpfs/ramdisk.
        size_mb: Size of the RAM disk in megabytes.

    Returns:
        Tuple of (data_directory_path, optional_ramdisk).
        If ram=False or ramdisk creation fails, ramdisk will be None.
    """
    if not ram:
        tmpdir = Path(tempfile.mkdtemp(prefix="isoladb_"))
        data_dir = tmpdir / "data"
        data_dir.mkdir()
        return data_dir, None

    try:
        if sys.platform == "linux":
            return _create_linux_tmpfs(size_mb)
        elif sys.platform == "darwin":
            return _create_macos_ramdisk(size_mb)
        else:
            logger.warning(
                "RAM disk not supported on %s, falling back to temp directory",
                sys.platform,
            )
            tmpdir = Path(tempfile.mkdtemp(prefix="isoladb_"))
            data_dir = tmpdir / "data"
            data_dir.mkdir()
            return data_dir, None
    except RamDiskError:
        logger.warning("Failed to create RAM disk, falling back to temp directory", exc_info=True)
        tmpdir = Path(tempfile.mkdtemp(prefix="isoladb_"))
        data_dir = tmpdir / "data"
        data_dir.mkdir()
        return data_dir, None


def _create_linux_tmpfs(size_mb: int) -> Tuple[Path, Optional[RamDisk]]:
    """Create a tmpfs mount on Linux."""
    tmpdir = Path(tempfile.mkdtemp(prefix="isoladb_"))
    mount_point = tmpdir / "data"
    mount_point.mkdir()

    try:
        subprocess.run(
            [
                "mount", "-t", "tmpfs",
                "-o", "size={}m".format(size_mb),
                "tmpfs",
                str(mount_point),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        # Try with sudo
        try:
            subprocess.run(
                [
                    "sudo", "-n", "mount", "-t", "tmpfs",
                    "-o", "size={}m".format(size_mb),
                    "tmpfs",
                    str(mount_point),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            shutil.rmtree(str(tmpdir), ignore_errors=True)
            raise RamDiskError(
                "Failed to mount tmpfs (tried with and without sudo): {}".format(
                    e.stderr.strip() if e.stderr else str(e)
                )
            )

    logger.info("Created tmpfs at %s (%d MB)", mount_point, size_mb)
    return mount_point, RamDisk(path=mount_point)


def _destroy_linux_tmpfs(path: Path) -> None:
    """Unmount a tmpfs mount on Linux."""
    try:
        subprocess.run(
            ["umount", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        try:
            subprocess.run(
                ["sudo", "-n", "umount", str(path)],
                check=True,
                capture_output=True,
                text=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("Failed to unmount tmpfs at %s", path)
            return

    # Clean up the mount point directory
    parent = path.parent
    shutil.rmtree(str(parent), ignore_errors=True)
    logger.info("Destroyed tmpfs at %s", path)


def _create_macos_ramdisk(size_mb: int) -> Tuple[Path, Optional[RamDisk]]:
    """Create a RAM disk on macOS using hdiutil."""
    # macOS RAM disk sectors: size_mb * 2048 (512-byte sectors per MB)
    sectors = size_mb * 2048

    try:
        result = subprocess.run(
            ["hdiutil", "attach", "-nomount", "ram://{}".format(sectors)],
            check=True,
            capture_output=True,
            text=True,
        )
        device = result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise RamDiskError(
            "Failed to create macOS RAM disk: {}".format(
                e.stderr.strip() if e.stderr else str(e)
            )
        )

    # Format the RAM disk
    tmpdir = Path(tempfile.mkdtemp(prefix="isoladb_"))
    mount_point = tmpdir / "data"
    mount_point.mkdir()

    try:
        subprocess.run(
            [
                "diskutil", "erasevolume", "APFS",
                "isoladb",
                device,
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        # Find where it was mounted and remount to our target
        # diskutil erasevolume mounts automatically, find the mount point
        result = subprocess.run(
            ["diskutil", "info", "-plist", device],
            check=True,
            capture_output=True,
            text=True,
        )

        # Parse the mount point from diskutil output
        auto_mount = _parse_mount_point(result.stdout)
        if auto_mount:
            # Use the auto-mounted path directly
            shutil.rmtree(str(tmpdir), ignore_errors=True)
            data_dir = Path(auto_mount)
        else:
            data_dir = mount_point

    except subprocess.CalledProcessError as e:
        # Clean up the RAM disk device on failure
        subprocess.run(
            ["hdiutil", "detach", device],
            capture_output=True,
        )
        shutil.rmtree(str(tmpdir), ignore_errors=True)
        raise RamDiskError(
            "Failed to format macOS RAM disk: {}".format(
                e.stderr.strip() if e.stderr else str(e)
            )
        )

    logger.info("Created macOS RAM disk at %s (device: %s, %d MB)", data_dir, device, size_mb)
    return data_dir, RamDisk(path=data_dir, device=device)


def _parse_mount_point(plist_output: str) -> Optional[str]:
    """Extract mount point from diskutil info plist output."""
    # Simple parsing — look for MountPoint key
    lines = plist_output.split("\n")
    for i, line in enumerate(lines):
        if "<key>MountPoint</key>" in line and i + 1 < len(lines):
            value_line = lines[i + 1].strip()
            if value_line.startswith("<string>") and value_line.endswith("</string>"):
                return value_line[len("<string>"):-len("</string>")]
    return None


def _destroy_macos_ramdisk(device: str, path: Path) -> None:
    """Detach a macOS RAM disk."""
    try:
        subprocess.run(
            ["hdiutil", "detach", device, "-force"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        logger.warning("Failed to detach macOS RAM disk %s", device)

    # Clean up any leftover temp directories
    if path.exists():
        parent = path.parent
        if parent.name.startswith("isoladb_"):
            shutil.rmtree(str(parent), ignore_errors=True)

    logger.info("Destroyed macOS RAM disk (device: %s)", device)
