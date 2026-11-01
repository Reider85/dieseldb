# DieselDB Python Driver (`diesel-python`)

Pure-Python, **zero-dependency** driver for DieselDB exposing a
**psycopg2-compatible** API over the libpq wire protocol (the subset spoken
by `diesel.LibpqServer`).

## Installation

```bash
pip install diesel-python
```

## Usage

### Synchronous (psycopg2-style)

```python
import diesel

conn = diesel.connect("host=localhost port=5432 dbname=testdb "
                      "user=alice password=secret")
# or: diesel.connect(host="localhost", port=5432, database="testdb", ...)

cur = conn.cursor()
cur.execute("CREATE TABLE demo (ID INTEGER, NAME STRING)")
cur.execute("INSERT INTO demo (ID, NAME) VALUES (%s, %s)", (1, "Alice"))
cur.execute("SELECT ID, NAME FROM demo WHERE ID = %s", (1,))
print(cur.fetchone())        # (1, 'Alice')
print(cur.fetchall())         # remaining rows
conn.commit()                # no-op: DieselDB libpq path is autocommit
conn.close()
```

Parameter binding uses the DB-API `pyformat` style (`%s` positional and
`%(name)s` named placeholders) and is performed **client-side** (the server's
extended protocol Parse/Bind/Execute is a stub).

### Asynchronous (asyncio)

```python
import asyncio
import diesel.aio

async def main():
    conn = await diesel.aio.connect(host="localhost", port=5432,
                                    database="testdb", user="alice",
                                    password="secret")
    cur = await conn.cursor()
    await cur.execute("SELECT %s AS answer", (42,))
    print(await cur.fetchone())
    await conn.close()

asyncio.run(main())
```

## Error handling

DB-API 2.0 exception hierarchy is available as `diesel.errors`
(`Error`, `DatabaseError`, `OperationalError`, `ProgrammingError`,
`IntegrityError`, `InterfaceError`, `NotSupportedError`, ...). The DieselDB
server currently reports SQLSTATE `00000` for every error, which maps to the
generic `DatabaseError`.

## Compatibility notes / limitations

* `commit()` / `rollback()` are no-ops (server-side autocommit semantics).
* Only trust and cleartext-password authentication are supported (no SCRAM).
* Empty `SELECT` results send no `RowDescription`, so `cursor.description`
  stays `None` for zero-row results.
* `CommandComplete` row counts are approximate (hardcoded by the server).

## Running the tests

Start a DieselDB libpq server (port 5432 or pass `DIESEL_PORT`), then:

```bash
python -m unittest discover -s tests -v
```

## Publishing (maintainers)

```bash
python -m build           # sdist + wheel into dist/
python -m twine upload dist/*
```

CI: tag `driver-python-v*` triggers `.github/workflows/publish-drivers.yml`
(requires the `PYPI_API_TOKEN` repository secret).
