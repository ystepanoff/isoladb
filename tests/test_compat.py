"""Tests for platform detection."""

import sys
from unittest import mock

import pytest

from isoladb._compat import detect_platform
from isoladb.exceptions import UnsupportedPlatformError


def test_detect_platform_returns_tuple():
    os_name, arch = detect_platform()
    assert isinstance(os_name, str)
    assert isinstance(arch, str)


def test_detect_platform_macos_arm64():
    with mock.patch.object(sys, "platform", "darwin"):
        with mock.patch("platform.machine", return_value="arm64"):
            os_name, arch = detect_platform()
            assert os_name == "darwin"
            assert arch == "arm64v8"


def test_detect_platform_linux_amd64():
    with mock.patch.object(sys, "platform", "linux"):
        with mock.patch("platform.machine", return_value="x86_64"):
            os_name, arch = detect_platform()
            assert os_name == "linux"
            assert arch == "amd64"


def test_detect_platform_linux_arm64():
    with mock.patch.object(sys, "platform", "linux"):
        with mock.patch("platform.machine", return_value="aarch64"):
            os_name, arch = detect_platform()
            assert os_name == "linux"
            assert arch == "arm64v8"


def test_detect_platform_unsupported_os():
    with mock.patch.object(sys, "platform", "win32"):
        with pytest.raises(UnsupportedPlatformError, match="win32"):
            detect_platform()


def test_detect_platform_unsupported_arch():
    with mock.patch.object(sys, "platform", "linux"):
        with mock.patch("platform.machine", return_value="mips"):
            with pytest.raises(UnsupportedPlatformError, match="mips"):
                detect_platform()
