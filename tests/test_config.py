"""Tests for IsolaDBConfig."""

from isoladb.config import DEFAULT_CACHE_DIR, DEFAULT_PG_VERSION, IsolaDBConfig


def test_defaults():
    config = IsolaDBConfig()
    assert config.pg_version == DEFAULT_PG_VERSION
    assert config.cache_dir == DEFAULT_CACHE_DIR
    assert config.ram is False
    assert config.ram_size_mb == 256
    assert config.startup_timeout == 30.0
    assert config.use_system_pg is True
    assert config.pg_conf == {}


def test_custom_values():
    config = IsolaDBConfig(
        pg_version="16.0.0",
        ram=True,
        ram_size_mb=512,
        startup_timeout=60.0,
        pg_conf={"max_connections": "50"},
    )
    assert config.pg_version == "16.0.0"
    assert config.ram is True
    assert config.ram_size_mb == 512
    assert config.startup_timeout == 60.0
    assert config.pg_conf == {"max_connections": "50"}
