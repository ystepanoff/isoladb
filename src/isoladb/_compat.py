"""Platform detection utilities."""

import platform
import sys
from typing import Tuple

from isoladb.exceptions import UnsupportedPlatformError

# Mapping from Python's platform identifiers to zonkyio binary naming
_OS_MAP = {
    "darwin": "darwin",
    "linux": "linux",
}

_ARCH_MAP = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64v8",
    "arm64": "arm64v8",
}


def detect_platform() -> Tuple[str, str]:
    """Detect the current OS and architecture.

    Returns:
        Tuple of (os_name, arch) matching zonkyio binary naming convention.
        E.g. ("darwin", "arm64v8") or ("linux", "amd64").

    Raises:
        UnsupportedPlatformError: If the current platform is not supported.
    """
    raw_os = sys.platform
    raw_arch = platform.machine().lower()

    os_name = _OS_MAP.get(raw_os)
    if os_name is None:
        raise UnsupportedPlatformError(
            f"Unsupported operating system: {raw_os!r}. "
            f"Supported: {', '.join(sorted(_OS_MAP.keys()))}"
        )

    arch = _ARCH_MAP.get(raw_arch)
    if arch is None:
        raise UnsupportedPlatformError(
            f"Unsupported architecture: {raw_arch!r}. "
            f"Supported: {', '.join(sorted(_ARCH_MAP.keys()))}"
        )

    return os_name, arch
