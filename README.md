# isoladb

Ephemeral PostgreSQL instances for unit testing.

```python
from isoladb import IsolaDB

with IsolaDB() as db:
    conn = db.connect()
    conn.execute("CREATE TABLE test (id serial PRIMARY KEY, name text)")
    conn.execute("INSERT INTO test (name) VALUES ('hello')")
    conn.commit()
```

## Installation

```bash
pip install isoladb
```

With pytest support:

```bash
pip install isoladb[pytest]
```
