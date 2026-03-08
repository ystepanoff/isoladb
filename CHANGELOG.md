# Changelog

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
