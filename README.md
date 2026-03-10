# isoladb

Ephemeral PostgreSQL instances for unit testing. No pre-installed PostgreSQL required — just Python 3.8+.

isoladb downloads pre-built PostgreSQL binaries (or uses your system installation), starts an isolated server, creates per-test databases, and cleans up automatically.

## Installation

```bash
pip install isoladb
```

With pytest support:

```bash
pip install isoladb[pytest]
```

With psycopg (PostgreSQL client):

```bash
pip install isoladb[psycopg]
```

## Quick Start

```python
import psycopg
from isoladb import IsolaDB

with IsolaDB() as db:
    with psycopg.connect(db.url) as conn:
        conn.execute("CREATE TABLE users (id serial PRIMARY KEY, name text)")
        conn.execute("INSERT INTO users (name) VALUES ('Alice')")
        conn.commit()
        result = conn.execute("SELECT name FROM users").fetchone()
        assert result[0] == "Alice"
# Server and database are cleaned up automatically
```

Each `IsolaDB()` context manager creates a fresh, isolated database. The underlying PostgreSQL server is shared and reused across invocations with the same configuration.

## Connection Properties

The context manager yields an object with these properties:

| Property | Description | Example |
|---|---|---|
| `db.url` | Full connection URL | `postgresql://postgres@localhost/isoladb_test_a1b2c3?host=/tmp/pg_xyz&port=54321` |
| `db.host` | Unix socket directory | `/tmp/pg_xyz` |
| `db.port` | Server port | `54321` |
| `db.dbname` | Database name | `isoladb_test_a1b2c3` |
| `db.user` | Superuser name | `postgres` |

Works with any PostgreSQL client library:

```python
# psycopg v3
conn = psycopg.connect(db.url)

# psycopg2
conn = psycopg2.connect(host=db.host, port=db.port, dbname=db.dbname, user=db.user)

# asyncpg
conn = await asyncpg.connect(host=db.host, port=db.port, database=db.dbname, user=db.user)

# SQLAlchemy
engine = create_engine(db.url)
```

## PostgreSQL Binary Resolution

By default, isoladb looks for PostgreSQL in this order:

1. **System PostgreSQL** — detected via `pg_ctl` on `PATH` (e.g., Homebrew, apt)
2. **Cached download** — previously downloaded binaries in `~/.cache/isoladb`
3. **Fresh download** — fetched from Maven Central (~50MB, cached for future use)

To always use downloaded binaries instead of a system installation:

```python
with IsolaDB(use_system_pg=False) as db:
    ...
```

## Schema and Setup

Apply a SQL schema file automatically after each database is created:

```python
with IsolaDB(schema="schema.sql") as db:
    # Tables from schema.sql are already created
    with psycopg.connect(db.url) as conn:
        conn.execute("INSERT INTO users (name) VALUES ('Alice')")
```

Or point to a directory of `.sql` files — they are sorted by filename and applied in order:

```
migrations/
  001_create_users.sql
  002_create_posts.sql
  003_seed_data.sql
```

```python
with IsolaDB(schema="migrations/") as db:
    # All .sql files applied in sorted order
    ...
```

Non-`.sql` files in the directory are ignored.

For programmatic initialisation (e.g., Alembic migrations), use a setup callable:

```python
def apply_migrations(url):
    from alembic.config import Config
    from alembic import command
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", url)
    command.upgrade(cfg, "head")

with IsolaDB(setup=apply_migrations) as db:
    ...
```

Both can be combined — schema is applied first, then setup.

## RAM Disk

Run the PostgreSQL data directory on a RAM disk for faster I/O:

```python
with IsolaDB(ram=True) as db:
    ...
```

Uses tmpfs on Linux and hdiutil RAM disk on macOS. Falls back to a regular temp directory if RAM disk creation fails.

## Async Support

```python
from isoladb import AsyncIsolaDB

async with AsyncIsolaDB() as db:
    conn = await asyncpg.connect(
        host=db.host, port=db.port, database=db.dbname, user=db.user
    )
    await conn.execute("CREATE TABLE test (id serial PRIMARY KEY)")
    await conn.close()
```

The async setup callable can be either sync or async:

```python
async def apply_migrations(url: str) -> None:
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

async with AsyncIsolaDB(setup=apply_migrations) as db:
    ...
```

## Configuration

All options can be passed to `IsolaDB()` / `AsyncIsolaDB()`:

| Option | Default | Description |
|---|---|---|
| `pg_version` | `"17.2.0"` | PostgreSQL version (for downloaded binaries) |
| `ram` | `False` | Use RAM disk for the data directory |
| `ram_size_mb` | `256` | RAM disk size in megabytes |
| `use_system_pg` | `True` | Prefer system PostgreSQL over downloading |
| `schema` | `None` | Path to a SQL file or directory of `.sql` files |
| `setup` | `None` | Callable receiving the connection URL for custom setup |
| `cache_dir` | `~/.cache/isoladb` | Directory for cached PostgreSQL binaries |
| `startup_timeout` | `30.0` | Seconds to wait for the server to start |
| `pg_conf` | `{}` | Extra postgresql.conf settings as `{"key": "value"}` |

## Pytest Plugin

isoladb includes a pytest plugin that provides fixtures automatically when `isoladb[pytest]` is installed.

### Fixtures

**`isoladb`** — per-test fixture yielding an `IsolaDBConnection` with `.url`, `.host`, `.port`, `.dbname`:

```python
def test_users(isoladb):
    with psycopg.connect(isoladb.url) as conn:
        conn.execute("CREATE TABLE users (id serial PRIMARY KEY, name text)")
        conn.execute("INSERT INTO users (name) VALUES ('Alice')")
        conn.commit()
        result = conn.execute("SELECT count(*) FROM users").fetchone()
        assert result[0] == 1
```

**`isoladb_engine`** — per-test fixture yielding a SQLAlchemy engine (requires `sqlalchemy`):

```python
def test_with_engine(isoladb_engine):
    with isoladb_engine.connect() as conn:
        conn.execute(text("SELECT 1"))
```

**`isoladb_async`** — per-test async fixture (requires `pytest-asyncio`):

```python
async def test_async(isoladb_async):
    conn = await asyncpg.connect(
        host=isoladb_async.host, port=isoladb_async.port,
        database=isoladb_async.dbname, user="postgres",
    )
    await conn.execute("SELECT 1")
    await conn.close()
```

**`isoladb_async_engine`** — per-test async SQLAlchemy engine (requires `sqlalchemy[asyncio]`, `asyncpg`):

```python
async def test_async_engine(isoladb_async_engine):
    async with isoladb_async_engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
```

**`isoladb_server`** — session-scoped fixture exposing the underlying `IsolaDBServer`. Useful for custom fixture composition.

**`isoladb_setup`** — session-scoped fixture to override with a custom setup callable:

```python
# conftest.py
@pytest.fixture(scope="session")
def isoladb_setup():
    def apply_migrations(url):
        from alembic.config import Config
        from alembic import command
        cfg = Config("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", url)
        command.upgrade(cfg, "head")
    return apply_migrations
```

### Ini Options

Configure in `pyproject.toml`, `pytest.ini`, or `setup.cfg`:

```toml
# pyproject.toml
[tool.pytest.ini_options]
isoladb_pg_version = "16.1.0"
isoladb_ram = true
isoladb_use_system_pg = false
isoladb_schema = "tests/schema.sql"
```

| Option | Default | Description |
|---|---|---|
| `isoladb_pg_version` | latest stable | PostgreSQL version |
| `isoladb_ram` | `false` | Use RAM disk |
| `isoladb_use_system_pg` | `true` | Prefer system PostgreSQL |
| `isoladb_schema` | none | SQL schema file path |

The pytest header shows which PostgreSQL binary is being used:

```
============================= test session starts ==============================
platform darwin -- Python 3.13.6
isoladb: PostgreSQL at /opt/homebrew/Cellar/postgresql@14/14.19
```

## Requirements

- Python 3.8+
- No pre-installed PostgreSQL needed (downloads automatically if not found)
- Linux (x86_64, arm64) or macOS (x86_64, arm64)

## License

MIT
