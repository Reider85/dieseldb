"""Cursor implementation (psycopg2-compatible surface) for DieselDB."""

from __future__ import annotations

from decimal import Decimal
from typing import List, Optional, Sequence, Tuple

from . import errors
from .protocol import QueryResult

# PostgreSQL type OIDs the DieselDB server can emit (LibpqClientHandler
# .typeOidFor) plus common text-family OIDs for forward compatibility.
_OID_BOOL = 16
_OID_INT8 = 20
_OID_INT2 = 21
_OID_INT4 = 23
_OID_TEXT = 25
_OID_FLOAT4 = 700
_OID_FLOAT8 = 701
_OID_VARCHAR = 1043
_OID_NUMERIC = 1700


def _decode_value(text: Optional[str], oid: int) -> object:
    """Convert a text-format wire value to a Python object by type OID."""
    if text is None:
        return None
    if oid in (_OID_INT2, _OID_INT4, _OID_INT8):
        try:
            return int(text)
        except ValueError:
            return text
    if oid in (_OID_FLOAT4, _OID_FLOAT8):
        try:
            return float(text)
        except ValueError:
            return text
    if oid == _OID_NUMERIC:
        try:
            return Decimal(text)
        except Exception:
            return text
    if oid == _OID_BOOL:
        if text in ("t", "true", "TRUE", "True", "1", "y", "yes"):
            return True
        if text in ("f", "false", "FALSE", "False", "0", "n", "no"):
            return False
        return text
    return text  # text / varchar / unknown


def _quote_literal(value: object) -> str:
    """Render a Python value as a SQL literal (client-side binding)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        # DieselDB stores booleans as integers in the engine's type system.
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        escaped = bytes(value).decode("utf-8", "replace").replace("'", "''")
        return "'" + escaped + "'"
    escaped = str(value).replace("'", "''")
    return "'" + escaped + "'"


def _bind_pyformat(query: str, vars) -> str:
    """Substitute ``%s`` / ``%(name)s`` placeholders (client-side)."""
    if vars is None:
        return query
    if isinstance(vars, dict):
        try:
            return query % {k: _quote_literal(v) for k, v in vars.items()}
        except (KeyError, TypeError, ValueError) as exc:
            raise errors.ProgrammingError(
                "Parameter binding failed: %s" % exc) from exc
    if isinstance(vars, (list, tuple)):
        try:
            return query % tuple(_quote_literal(v) for v in vars)
        except (TypeError, ValueError) as exc:
            raise errors.ProgrammingError(
                "Parameter binding failed (arity mismatch?): %s" % exc) from exc
    raise errors.ProgrammingError(
        "Parameter sequence must be a tuple/list or a dict, got %s"
        % type(vars).__name__)


class Cursor:
    """DB-API 2.0 cursor over a DieselDB connection."""

    def __init__(self, connection):
        self.connection = connection
        self._rows: List[Tuple] = []
        self._pos = 0
        self._column_names: Optional[List[str]] = None
        self._column_oids: Optional[List[int]] = None
        self._closed = False
        self.rowcount = -1
        self.description = None
        self.last_rowid = None
        self.statusmessage = None

    # ---- execution ----

    def execute(self, operation: str, vars=None) -> "Cursor":
        self._check_open()
        sql = _bind_pyformat(operation, vars)
        result = self.connection._protocol.simple_query(sql)
        self._set_result(result)
        return self

    def executemany(self, operation: str, seq_of_vars) -> "Cursor":
        self._check_open()
        total = 0
        for vars_item in seq_of_vars:
            sql = _bind_pyformat(operation, vars_item)
            result = self.connection._protocol.simple_query(sql)
            self._set_result(result)
            if self.rowcount and self.rowcount > 0:
                total += self.rowcount
        if self.rowcount >= 0:
            self.rowcount = total
        return self

    def _set_result(self, result: QueryResult):
        oids = result.column_type_oids
        self._rows = [
            tuple(_decode_value(v, oids[i] if oids else 25)
                  for i, v in enumerate(row))
            for row in result.rows
        ]
        self._pos = 0
        self._column_names = result.column_names
        self._column_oids = oids
        self.statusmessage = result.command_tag
        if result.column_names is not None:
            # DB-API description: 7-tuple per column.
            self.description = tuple(
                (name, oids[i] if oids else None,
                 None, None, None, None, None)
                for i, name in enumerate(result.column_names))
            self.rowcount = len(self._rows)
        else:
            self.description = None
            self.rowcount = _rows_from_tag(result.command_tag)

    # ---- fetch ----

    def fetchone(self):
        self._check_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size=None):
        self._check_open()
        if size is None:
            size = self.arraysize
        end = min(self._pos + size, len(self._rows))
        chunk = self._rows[self._pos:end]
        self._pos = end
        return chunk

    def fetchall(self):
        self._check_open()
        chunk = self._rows[self._pos:]
        self._pos = len(self._rows)
        return chunk

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    # ---- lifecycle ----

    arraysize = 1

    def close(self):
        self._closed = True
        self._rows = []
        self.description = None

    @property
    def closed(self):
        return self._closed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _check_open(self):
        if self._closed:
            raise errors.InterfaceError("Cursor is closed")
        if self.connection.closed:
            raise errors.InterfaceError("Connection is closed")


def _rows_from_tag(tag: Optional[str]) -> int:
    """Best-effort rowcount from a CommandComplete tag.

    The DieselDB server hardcodes counts (``SELECT 1``/``INSERT 0 1``/
    ``UPDATE 1``/``DELETE 1`` - LibpqClientHandler.getCommandCompleteTag), so
    this only reflects the (approximate) server-provided number.
    """
    if not tag:
        return -1
    parts = tag.split()
    for part in reversed(parts):
        try:
            return int(part)
        except ValueError:
            continue
    return -1
