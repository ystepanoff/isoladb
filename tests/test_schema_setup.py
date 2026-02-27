"""Tests for schema and setup migration support."""


import pytest

from isoladb import IsolaDB


def test_schema_file(tmp_path):
    """Schema SQL file is applied automatically after DB creation."""
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text(
        "CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL);\n"
        "CREATE TABLE posts "
        "(id serial PRIMARY KEY, user_id int REFERENCES users(id), body text);\n"
    )

    with IsolaDB(schema=str(schema_file)) as db:
        conn = db.connect()
        try:
            conn.execute("INSERT INTO users (name) VALUES ('Alice')")
            conn.execute(
                "INSERT INTO posts (user_id, body) VALUES (1, 'Hello world')"
            )
            conn.commit()

            result = conn.execute(
                "SELECT u.name, p.body FROM users u "
                "JOIN posts p ON p.user_id = u.id"
            ).fetchone()
            assert result is not None
            assert result[0] == "Alice"
            assert result[1] == "Hello world"
        finally:
            conn.close()


def test_schema_file_not_found():
    """Missing schema file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="Schema file not found"):
        with IsolaDB(schema="/nonexistent/schema.sql"):
            pass


def test_setup_callable():
    """Setup callable receives URL and can create schema."""
    tables_created = []

    def my_setup(url):
        import psycopg

        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute(
                "CREATE TABLE migrated_table (id serial PRIMARY KEY, val text)"
            )
            tables_created.append("migrated_table")

    with IsolaDB(setup=my_setup) as db:
        assert tables_created == ["migrated_table"]
        conn = db.connect()
        try:
            conn.execute("INSERT INTO migrated_table (val) VALUES ('works')")
            conn.commit()
            result = conn.execute("SELECT val FROM migrated_table").fetchone()
            assert result is not None
            assert result[0] == "works"
        finally:
            conn.close()


def test_schema_and_setup_together(tmp_path):
    """Schema file runs first, then setup callable."""
    schema_file = tmp_path / "base.sql"
    schema_file.write_text(
        "CREATE TABLE base_table (id serial PRIMARY KEY, name text);\n"
    )

    def add_data(url):
        import psycopg

        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute("INSERT INTO base_table (name) VALUES ('seeded')")

    with IsolaDB(schema=str(schema_file), setup=add_data) as db:
        conn = db.connect()
        try:
            result = conn.execute("SELECT name FROM base_table").fetchone()
            assert result is not None
            assert result[0] == "seeded"
        finally:
            conn.close()


def test_each_context_gets_fresh_schema(tmp_path):
    """Each IsolaDB context gets schema applied independently."""
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text(
        "CREATE TABLE fresh_test (id serial PRIMARY KEY);\n"
    )

    with IsolaDB(schema=str(schema_file)) as db1:
        conn1 = db1.connect()
        try:
            conn1.execute("INSERT INTO fresh_test DEFAULT VALUES")
            conn1.commit()
            count1 = conn1.execute("SELECT count(*) FROM fresh_test").fetchone()
            assert count1[0] == 1
        finally:
            conn1.close()

    with IsolaDB(schema=str(schema_file)) as db2:
        conn2 = db2.connect()
        try:
            # New DB — table exists (from schema) but should be empty
            count2 = conn2.execute("SELECT count(*) FROM fresh_test").fetchone()
            assert count2[0] == 0
        finally:
            conn2.close()
