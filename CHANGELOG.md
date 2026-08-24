# Changelog

## [Unreleased]

### Added

- CI: Python 3.14 added to the test matrix (with classifier); Python 3.15
  pre-release tested in a non-blocking experimental lane.

### Fixed

- **Event-loop deadlock in `AsyncIsolaDB`**: `__aenter__` held the shared-server
  `threading.Lock` across an `await`, freezing the event loop whenever two async
  contexts were entered concurrently. The lock is now only acquired inside the
  executor thread (`_get_or_start_server`).
- **pytest startup no longer downloads binaries or crashes**: `pytest_report_header`
  called `get_or_download()` at every pytest session start, downloading ~50MB when no
  system PostgreSQL was present (even for runs that never use isoladb) and aborting
  pytest entirely on unsupported platforms. It now uses the new `binary.find_local()`
  (system installation or cache only, never a download) and swallows all errors.
- **Shared-server cache key ignored config fields**: servers were shared across
  contexts that differed in `pg_conf`, `cache_dir`, or `use_system_pg`, silently
  dropping the second context's settings. The key now covers all behaviour-affecting
  fields.
- **`AsyncIsolaDB` schema directories**: `schema=` pointing at a directory of `.sql`
  files raised `IsADirectoryError` in the async API (sync only). Both APIs now share
  the same schema application logic.
- **mypy gate restored**: `python_version = "3.8"` is rejected by modern mypy, so the
  type check had been failing at config load (masked by `continue-on-error` in CI).
  Target bumped to 3.9, all strict-mode errors fixed (including `Path`/`RamDisk`
  referenced but not imported in `server.py`), and the CI step is now blocking on
  the newest matrix Python.

## [0.1.1] - 2026-03-08

### Fixed

- **Orphaned postgres processes after pytest**: `IsolaDBServer.stop()` previously set
  `_running = False` before invoking `pg_ctl stop`, making the `atexit` handler a no-op
  if the graceful stop failed or timed out. The flag is now only cleared after the process
  is confirmed dead.
- **No fallback kill on pg_ctl failure**: added `_kill_postmaster()` which reads
  `postmaster.pid` and sends `SIGKILL` as a last resort when both `pg_ctl stop -m fast`
  and `pg_ctl stop -m immediate` fail.
- **Immediate-mode shutdown did not wait**: added `-w` to the `pg_ctl stop -m immediate`
  fallback so it waits for the server to stop before cleanup proceeds.
- **Shared servers not stopped on process exit**: `IsolaDB` and `AsyncIsolaDB` now
  register a module-level `atexit` handler (`shutdown` / `_async_shutdown`) to stop all
  servers in their `_shared_servers` caches, complementing the per-server atexit handlers.

## [0.1.0] - 2026-02-01

### Added

- Initial release: ephemeral PostgreSQL instances for unit testing.
- Zero external PostgreSQL dependency — downloads pre-built binaries from Maven Central.
- `IsolaDB` sync and `AsyncIsolaDB` async context managers.
- Shared server cache: one server process reused across tests with the same config.
- Per-test database create/drop via PostgreSQL wire protocol (no client library required).
- Optional RAM disk support (tmpfs on Linux, hdiutil/APFS on macOS).
- pytest plugin with `isoladb`, `isoladb_engine`, `isoladb_async`, `isoladb_async_engine`
  fixtures and `isoladb_setup` hook for custom initialization (e.g. Alembic migrations).
- `schema` parameter: apply a SQL file or directory of SQL files after DB creation.
