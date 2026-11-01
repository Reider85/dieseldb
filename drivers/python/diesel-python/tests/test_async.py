"""Asyncio CRUD acceptance tests for the DieselDB Python driver (diesel.aio)."""

import asyncio
import os
import unittest

import diesel.aio
from diesel import errors

HOST = os.environ.get("DIESEL_HOST", "localhost")
PORT = int(os.environ.get("DIESEL_PORT", "5432"))
USER = os.environ.get("DIESEL_USER", "testuser")
PASSWORD = os.environ.get("DIESEL_PASSWORD", "testpass")
DBNAME = os.environ.get("DIESEL_DB", "testdb")


async def _drop_if_exists(cur, table):
    try:
        await cur.execute("DROP TABLE " + table)
    except errors.Error:
        pass


class DieselAsyncCrudTest(unittest.TestCase):

    def test_async_basic_crud(self):
        async def scenario():
            conn = await diesel.aio.connect(
                host=HOST, port=PORT, database=DBNAME,
                user=USER, password=PASSWORD)
            try:
                cur = await conn.cursor()
                await _drop_if_exists(cur, "py_async_demo")
                await cur.execute(
                    "CREATE TABLE py_async_demo "
                    "(ID INTEGER, NAME STRING, AGE INTEGER)")
                await cur.execute(
                    "INSERT INTO py_async_demo (ID, NAME, AGE) "
                    "VALUES (%s, %s, %s)",
                    (1, "Alice", 25))
                await cur.execute(
                    "INSERT INTO py_async_demo (ID, NAME, AGE) "
                    "VALUES (%s, %s, %s)",
                    (2, "Bob", 30))

                await cur.execute("SELECT ID, NAME, AGE FROM py_async_demo")
                rows = await cur.fetchall()
                self.assertEqual(rows, [(1, "Alice", 25), (2, "Bob", 30)])

                await cur.execute(
                    "SELECT NAME FROM py_async_demo WHERE ID = %s", (2,))
                self.assertEqual(await cur.fetchone(), ("Bob",))

                await cur.execute(
                    "UPDATE py_async_demo SET AGE = %s WHERE ID = %s",
                    (26, 1))
                await cur.execute(
                    "SELECT AGE FROM py_async_demo WHERE ID = 1")
                self.assertEqual(await cur.fetchone(), (26,))

                await cur.execute("DELETE FROM py_async_demo WHERE ID = 1")
                await cur.execute("SELECT COUNT(*) FROM py_async_demo")
                self.assertEqual(await cur.fetchone(), (1,))

                await _drop_if_exists(cur, "py_async_demo")
                await cur.close()
            finally:
                await conn.close()

        asyncio.run(scenario())

    def test_async_error_then_reusable(self):
        async def scenario():
            conn = await diesel.aio.connect(host=HOST, port=PORT,
                                            database=DBNAME, user=USER,
                                            password=PASSWORD)
            try:
                cur = await conn.cursor()
                with self.assertRaises(errors.Error):
                    await cur.execute(
                        "SELECT * FROM table_that_does_not_exist")
                await cur.execute("SELECT 1")
                self.assertEqual(await cur.fetchone(), (1,))
                await cur.close()
            finally:
                await conn.close()

        asyncio.run(scenario())

    def test_async_context_manager(self):
        async def scenario():
            async with await diesel.aio.connect(
                    host=HOST, port=PORT, database=DBNAME,
                    user=USER, password=PASSWORD) as conn:
                cur = await conn.cursor()
                await cur.execute("SELECT 1")
                self.assertEqual(await cur.fetchone(), (1,))
                await cur.close()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
