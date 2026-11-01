"""Synchronous CRUD acceptance tests for the DieselDB Python driver.

The JUnit harness (PythonTest) starts an in-process LibpqServer and passes
the port via environment variables; defaults keep the tests runnable by hand
against a local server on 5432.
"""

import os
import unittest

import diesel
from diesel import errors

HOST = os.environ.get("DIESEL_HOST", "localhost")
PORT = int(os.environ.get("DIESEL_PORT", "5432"))
USER = os.environ.get("DIESEL_USER", "testuser")
PASSWORD = os.environ.get("DIESEL_PASSWORD", "testpass")
DBNAME = os.environ.get("DIESEL_DB", "testdb")

DSN = "host=%s port=%d dbname=%s user=%s password=%s" % (
    HOST, PORT, DBNAME, USER, PASSWORD)


def drop_if_exists(cur, table):
    try:
        cur.execute("DROP TABLE " + table)
    except errors.Error:
        pass


class DieselCrudTest(unittest.TestCase):

    def setUp(self):
        self.conn = diesel.connect(DSN)
        self.cur = self.conn.cursor()
        # Clean slate: DROP has no IF EXISTS; ignore missing-table errors.
        drop_if_exists(self.cur, "py_demo")

    def tearDown(self):
        try:
            # Fresh cursor: some tests close self.cur via context manager.
            cur = self.conn.cursor()
            drop_if_exists(cur, "py_demo")
            cur.close()
        except errors.Error:
            pass
        self.conn.close()

    def test_connect_via_dsn_string(self):
        self.assertFalse(self.conn.closed)
        self.cur.execute("SELECT 1")
        self.assertEqual(self.cur.fetchone(), (1,))

    def test_connect_via_kwargs(self):
        conn = diesel.connect(host=HOST, port=PORT, database=DBNAME,
                              user=USER, password=PASSWORD)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone(), (1,))
        finally:
            conn.close()

    def test_basic_crud(self):
        self.cur.execute(
            "CREATE TABLE py_demo (ID INTEGER, NAME STRING, AGE INTEGER)")

        # INSERT with %s pyformat parameters
        self.cur.execute(
            "INSERT INTO py_demo (ID, NAME, AGE) VALUES (%s, %s, %s)",
            (1, "Alice", 25))
        self.cur.execute(
            "INSERT INTO py_demo (ID, NAME, AGE) VALUES (%s, %s, %s)",
            (2, "Bob", 30))
        self.conn.commit()  # no-op, autocommit semantics

        # SELECT all rows back
        self.cur.execute("SELECT ID, NAME, AGE FROM py_demo")
        self.assertIsNotNone(self.cur.description)
        self.assertEqual([d[0] for d in self.cur.description],
                         ["ID", "NAME", "AGE"])
        rows = self.cur.fetchall()
        self.assertEqual(rows, [(1, "Alice", 25), (2, "Bob", 30)])

        # SELECT with WHERE %s
        self.cur.execute(
            "SELECT NAME FROM py_demo WHERE ID = %s", (2,))
        self.assertEqual(self.cur.fetchone(), ("Bob",))
        self.assertIsNone(self.cur.fetchone())

        # named pyformat parameters
        self.cur.execute(
            "SELECT AGE FROM py_demo WHERE ID = %(id)s", {"id": 1})
        self.assertEqual(self.cur.fetchone(), (25,))

        # UPDATE
        self.cur.execute(
            "UPDATE py_demo SET AGE = %s WHERE ID = %s", (26, 1))
        self.cur.execute("SELECT AGE FROM py_demo WHERE ID = 1")
        self.assertEqual(self.cur.fetchone(), (26,))

        # DELETE
        self.cur.execute("DELETE FROM py_demo WHERE ID = 1")
        self.cur.execute("SELECT COUNT(*) FROM py_demo")
        self.assertEqual(self.cur.fetchone(), (1,))

    def test_string_escaping_in_params(self):
        self.cur.execute(
            "CREATE TABLE py_demo (ID INTEGER, NAME STRING, AGE INTEGER)")
        self.cur.execute(
            "INSERT INTO py_demo (ID, NAME, AGE) VALUES (%s, %s, %s)",
            (7, "O'Brien", 40))
        self.cur.execute("SELECT NAME FROM py_demo WHERE ID = 7")
        self.assertEqual(self.cur.fetchone(), ("O'Brien",))

    def test_null_param(self):
        self.cur.execute(
            "CREATE TABLE py_demo (ID INTEGER, NAME STRING, AGE INTEGER)")
        self.cur.execute(
            "INSERT INTO py_demo (ID, NAME, AGE) VALUES (%s, %s, %s)",
            (8, None, None))
        self.cur.execute("SELECT ID, NAME, AGE FROM py_demo WHERE ID = 8")
        self.assertEqual(self.cur.fetchone(), (8, None, None))

    def test_error_then_connection_reusable(self):
        with self.assertRaises(errors.Error):
            self.cur.execute("SELECT * FROM table_that_does_not_exist")
        # After an error the connection must still answer queries.
        self.cur.execute("SELECT 1")
        self.assertEqual(self.cur.fetchone(), (1,))

    def test_fetchone_and_iteration(self):
        self.cur.execute(
            "CREATE TABLE py_demo (ID INTEGER, NAME STRING, AGE INTEGER)")
        for i in range(3):
            self.cur.execute(
                "INSERT INTO py_demo (ID, NAME, AGE) VALUES (%s, %s, %s)",
                (i, "n%d" % i, i * 10))
        self.cur.execute("SELECT ID FROM py_demo")
        seen = [row[0] for row in self.cur]
        self.assertEqual(seen, [0, 1, 2])

    def test_closed_connection_rejects_cursor(self):
        conn = diesel.connect(DSN)
        conn.close()
        self.assertTrue(conn.closed)
        with self.assertRaises(errors.InterfaceError):
            conn.cursor()

    def test_arity_mismatch_raises_programming_error(self):
        with self.assertRaises(errors.ProgrammingError):
            self.cur.execute("SELECT %s, %s", (1,))

    def test_context_manager_closes_cursor(self):
        with self.cur as cur:
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone(), (1,))
        self.assertTrue(self.cur.closed)


if __name__ == "__main__":
    unittest.main()
