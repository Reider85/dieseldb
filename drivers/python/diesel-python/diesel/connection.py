"""Connection implementation (psycopg2-compatible surface) for DieselDB."""

from __future__ import annotations

import re
from typing import Optional

from . import errors
from .cursor import Cursor
from .protocol import ProtocolClient

_DSN_PAIR = re.compile(
    r"""^\s*
        (?P<key>[A-Za-z_][A-Za-z0-9_]*)
        \s*=\s*
        (?P<value>
            '[^']*'           # single-quoted
            |"[^"]*"          # double-quoted
            |[^\s]+           # bare word
        )
        \s*$""", re.VERBOSE)

_DSN_KWARGS_ALIASES = {
    "dbname": "database",
    "db": "database",
}


def parse_dsn(dsn: str) -> dict:
    """Parse a libpq-style keyword DSN: ``host=... port=... dbname=...``."""
    if "=" not in dsn:
        raise errors.ProgrammingError(
            "Invalid DSN string (expected keyword=value pairs): %r" % dsn)
    result = {}
    # libpq DSNs are space-separated key=value pairs; also accept one
    # leading URI-ish token without '=' being impossible (we reject it).
    for token in dsn.split():
        match = _DSN_PAIR.match(token)
        if not match:
            # Value with spaces: re-join is not supported for bare tokens;
            # quoted values containing spaces must use the 'k=v with spaces'
            # form - fall back to a manual scan of the whole string.
            return _parse_dsn_manual(dsn)
        key = match.group("key").lower()
        value = match.group("value")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        result[key] = value
    return result


def _parse_dsn_manual(dsn: str) -> dict:
    """Fallback parser for DSNs whose values contain spaces."""
    result = {}
    i = 0
    n = len(dsn)
    while i < n:
        while i < n and dsn[i].isspace():
            i += 1
        if i >= n:
            break
        key_start = i
        while i < n and dsn[i] != "=" and not dsn[i].isspace():
            i += 1
        if i >= n or dsn[i] != "=":
            raise errors.ProgrammingError(
                "Invalid DSN string (expected key=value): %r" % dsn)
        key = dsn[key_start:i].lower()
        i += 1  # skip '='
        if i < n and dsn[i] in "'\"":
            quote = dsn[i]
            i += 1
            value_start = i
            while i < n and dsn[i] != quote:
                i += 1
            value = dsn[value_start:i]
            i += 1  # skip closing quote
        else:
            value_start = i
            while i < n and not dsn[i].isspace():
                i += 1
            value = dsn[value_start:i]
        result[key] = value
    return result


class Connection:
    """DB-API 2.0 connection to a DieselDB libpq server.

    Semantics notes (documented deviations from psycopg2):

    * the connection is effectively autocommit - ``commit()`` and
      ``rollback()`` are no-ops (the DieselDB libpq handler executes each
      simple query independently with no per-session transaction state);
    * ``isolation_level`` / ``autocommit`` attributes exist for API
      compatibility but setting them has no effect.
    """

    def __init__(self, dsn=None, connection_factory=None, cursor_factory=None,
                 **kwargs):
        if connection_factory is not None:
            raise errors.NotSupportedError(
                "connection_factory is not supported by the DieselDB driver")
        params = {}
        if dsn:
            params.update(parse_dsn(dsn))
        for key, value in kwargs.items():
            params[_DSN_KWARGS_ALIASES.get(key.lower(), key.lower())] = value

        host = params.get("host", "localhost")
        port = int(params.get("port", 5432))
        user = params.get("user", "postgres")
        database = params.get("dbname", params.get("database", "postgres"))
        password = params.get("password", "")
        connect_timeout = float(params.get("connect_timeout", "30"))

        self._cursor_factory = cursor_factory or Cursor
        self._protocol = ProtocolClient(
            host, port, user, database, password, connect_timeout)
        self._closed = False
        # psycopg2-compatible attributes (no-ops; server is autocommit).
        self.autocommit = True
        self.isolation_level = 0
        self.encoding = "UTF8"
        self.server_version = 130000  # server reports "13.0"

    # ---- DB-API surface ----

    def cursor(self, *args, **kwargs) -> Cursor:
        self._check_open()
        if args or kwargs:
            raise errors.NotSupportedError(
                "cursor(*args, **kwargs) factory arguments are not supported")
        return self._cursor_factory(self)

    def commit(self):
        """No-op: the DieselDB libpq path is autocommit (documented)."""
        self._check_open()

    def rollback(self):
        """No-op: the DieselDB libpq path is autocommit (documented)."""
        self._check_open()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._protocol.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # psycopg2 semantics: commit on success, rollback on exception.
        # Both are no-ops here; still close on exit only if user asks - we
        # mirror psycopg2 and do NOT close (caller controls close).
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False

    def _check_open(self):
        if self._closed or self._protocol.closed:
            raise errors.InterfaceError("Connection is closed")


def connect(dsn=None, connection_factory=None, cursor_factory=None,
            **kwargs) -> Connection:
    """Open a DieselDB connection (psycopg2-compatible signature).

    Accepts a libpq keyword DSN string::

        diesel.connect("host=localhost port=5432 dbname=testdb "
                       "user=alice password=secret")

    or keyword arguments::

        diesel.connect(host="localhost", port=5432, database="testdb",
                       user="alice", password="secret")
    """
    return Connection(dsn, connection_factory=connection_factory,
                      cursor_factory=cursor_factory, **kwargs)
