"""DieselDB Python driver (psycopg2-compatible API).

Pure-Python, zero-dependency libpq-subset client for the DieselDB server.

Quick start::

    import diesel

    conn = diesel.connect("host=localhost port=5432 dbname=testdb "
                          "user=alice password=secret")
    cur = conn.cursor()
    cur.execute("SELECT %s AS answer", (42,))
    print(cur.fetchone())   # (42,)
    conn.close()

Async variant::

    import asyncio
    import diesel.aio

    async def main():
        conn = await diesel.aio.connect(host="localhost", port=5432)
        cur = await conn.cursor()
        await cur.execute("SELECT 1")
        print(await cur.fetchone())
        await conn.close()

    asyncio.run(main())
"""

from .connection import Connection, connect, parse_dsn
from .cursor import Cursor
from .protocol import ProtocolClient
from . import errors

__version__ = "0.1.0"

# DB-API 2.0 module-level hooks.
paramstyle = "pyformat"
apilevel = "2.0"
threadsafety = 1  # threads may share the module, not connections

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "ProtocolClient",
    "parse_dsn",
    "errors",
    "paramstyle",
    "apilevel",
    "threadsafety",
    "__version__",
]
