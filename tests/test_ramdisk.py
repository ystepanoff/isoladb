"""Tests for RAM disk / tmpfs management."""

from pathlib import Path

from isoladb.ramdisk import RamDisk, create_data_directory


def test_create_data_directory_no_ram():
    """Default mode creates a regular temp directory."""
    data_dir, ramdisk = create_data_directory(ram=False)
    try:
        assert data_dir.exists()
        assert data_dir.is_dir()
        assert ramdisk is None
    finally:
        import shutil

        shutil.rmtree(str(data_dir.parent), ignore_errors=True)


def test_create_data_directory_fallback():
    """If RAM disk creation fails, falls back to temp directory."""
    # On most test environments, RAM disk creation will fail (no sudo)
    # and should fall back gracefully
    data_dir, ramdisk = create_data_directory(ram=True, size_mb=64)
    try:
        assert data_dir.exists()
        assert data_dir.is_dir()
        # ramdisk may or may not be None depending on permissions
    finally:
        if ramdisk is not None:
            ramdisk.destroy()
        else:
            import shutil

            shutil.rmtree(str(data_dir.parent), ignore_errors=True)


def test_ramdisk_destroy_idempotent():
    """Calling destroy() multiple times is safe."""
    rd = RamDisk(path=Path("/tmp/fake"), device=None)
    rd.destroy()
    rd.destroy()  # Should not raise


def test_ramdisk_repr():
    rd = RamDisk(path=Path("/tmp/test"), device="/dev/disk4")
    r = repr(rd)
    assert "/tmp/test" in r
    assert "/dev/disk4" in r
