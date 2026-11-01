"""Asyncio variant of the DieselDB Python driver.

Mirrors the synchronous API with ``await``:

* :func:`connect` - open an :class:`AsyncConnection`,
* :meth:`AsyncConnection.cursor` - create an :class:`AsyncCursor`,
* :meth:`AsyncCursor.execute` / ``fetchone`` / ``fetchall`` / ``fetchmany``,
* :meth:`AsyncConnection.close`.
"""

from __future__ import annotations

import asyncio
import struct
from typing import List, Optional, Tuple

from . import errors
from .connection import parse_dsn, _DSN_KWARGS_ALIASES
from .cursor import _bind_pyformat, _decode_value, _rows_from_tag
from .protocol import (
    AUTH_CLEARTEXT_PASSWORD,
    AUTH_OK,
    AUTH_REQUEST,
    AUTH_SASL,
    COMMAND_COMPLETE,
    DATA_ROW,
    EMPTY_QUERY_RESPONSE,
    ERROR_RESPONSE,
    NOTICE_RESPONSE,
    PARAMETER_STATUS,
    PASSWORD_MESSAGE,
    PROTOCOL_VERSION_3,
    READY_FOR_QUERY,
    ROW_DESCRIPTION,
    SIMPLE_QUERY,
    STARTUP,
    TERMINATE,
    parse_error_fields,
    parse_row_description,
    parse_data_row,
    read_cstring,
)


class AsyncProtocolClient:
    """Asyncio streams client speaking the DieselDB libpq subset."""

    def __init__(self, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self._closed = False

    @classmethod
    async def connect(cls, host, port, user, database, password,
                      connect_timeout_seconds=30.0) -> "AsyncProtocolClient":
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), connect_timeout_seconds)
        except (OSError, asyncio.TimeoutError) as exc:
            raise errors.OperationalError(
                "could not connect to %s:%s: %s" % (host, port, exc)) from exc
        client = cls(reader, writer)
        try:
            await client._handshake(user or "postgres", database or "postgres",
                                    password or "")
        except Exception:
            await client.close()
            raise
        return client

    async def _handshake(self, user, database, password):
        await self._send_startup(user, database)
        while True:
            mtype, payload = await self._read_message()
            if mtype == AUTH_REQUEST:
                await self._handle_auth_request(payload, password)
            elif mtype in (PARAMETER_STATUS, NOTICE_RESPONSE):
                continue
            elif mtype == READY_FOR_QUERY:
                return
            elif mtype == ERROR_RESPONSE:
                err = parse_error_fields(payload)
                raise errors.InterfaceError("Startup failed: %s" % err.message)

    async def _send_startup(self, user, database):
        params = b"user\x00" + user.encode("utf-8")
        params += b"\x00database\x00" + database.encode("utf-8")
        params += b"\x00client_encoding\x00UTF8\x00"
        params += b"\x00"
        payload = struct.pack("!i", PROTOCOL_VERSION_3) + params
        self._writer.write(STARTUP + struct.pack("!i", 4 + len(payload)) + payload)
        await self._writer.drain()

    async def _handle_auth_request(self, payload, password):
        if len(payload) < 4:
            raise errors.InterfaceError("Malformed AuthenticationRequest")
        (auth_type,) = struct.unpack_from("!i", payload, 0)
        if auth_type == AUTH_OK:
            return
        if auth_type == AUTH_CLEARTEXT_PASSWORD:
            pwd = (password or "").encode("utf-8") + b"\x00"
            await self._write_message(PASSWORD_MESSAGE, pwd)
            return
        raise errors.NotSupportedError(
            "Unsupported authentication method: %d "
            "(supported: trust, cleartext password)" % auth_type)

    async def simple_query(self, sql: str):
        from .protocol import QueryResult
        await self._write_message(SIMPLE_QUERY, sql.encode("utf-8") + b"\x00")

        column_names = None
        column_type_oids = None
        rows: List[Tuple[Optional[str], ...]] = []
        command_tag = None
        error = None

        while True:
            mtype, payload = await self._read_message()
            if mtype == ROW_DESCRIPTION:
                column_names, column_type_oids = parse_row_description(payload)
            elif mtype == DATA_ROW:
                rows.append(parse_data_row(payload))
            elif mtype == COMMAND_COMPLETE:
                command_tag = read_cstring(payload, 0)
            elif mtype == ERROR_RESPONSE:
                error = parse_error_fields(payload)
            elif mtype in (NOTICE_RESPONSE, PARAMETER_STATUS,
                           EMPTY_QUERY_RESPONSE):
                continue
            elif mtype == READY_FOR_QUERY:
                break

        if error is not None:
            raise errors.exception_for_sqlstate(error.sqlstate, error.message)
        return QueryResult(column_names, column_type_oids, rows, command_tag)

    async def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            await self._write_message(TERMINATE, b"")
        except OSError:
            pass
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except (OSError, RuntimeError):
            pass

    @property
    def closed(self):
        return self._closed

    async def _write_message(self, mtype: bytes, payload: bytes):
        self._writer.write(mtype + struct.pack("!i", 4 + len(payload)) + payload)
        await self._writer.drain()

    async def _read_message(self):
        header = await self._reader.readexactly(5)
        mtype = header[0:1]
        (length,) = struct.unpack_from("!i", header, 1)
        if length < 4:
            raise errors.InterfaceError("Invalid message length: %d" % length)
        payload = await self._reader.readexactly(length - 4)
        return mtype, payload


class AsyncCursor:
    """Async variant of :class:`diesel.cursor.Cursor`."""

    def __init__(self, connection: "AsyncConnection"):
        self.connection = connection
        self._rows: List[Tuple] = []
        self._pos = 0
        self._closed = False
        self.rowcount = -1
        self.description = None
        self.statusmessage = None
        self.arraysize = 1

    async def execute(self, operation: str, vars=None) -> "AsyncCursor":
        self._check_open()
        sql = _bind_pyformat(operation, vars)
        result = await self.connection._protocol.simple_query(sql)
        self._set_result(result)
        return self

    def _set_result(self, result):
        oids = result.column_type_oids
        self._rows = [
            tuple(_decode_value(v, oids[i] if oids else 25)
                  for i, v in enumerate(row))
            for row in result.rows
        ]
        self._pos = 0
        self.statusmessage = result.command_tag
        if result.column_names is not None:
            self.description = tuple(
                (name, oids[i] if oids else None, None, None, None, None, None)
                for i, name in enumerate(result.column_names))
            self.rowcount = len(self._rows)
        else:
            self.description = None
            self.rowcount = _rows_from_tag(result.command_tag)

    async def fetchone(self):
        self._check_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    async def fetchmany(self, size=None):
        self._check_open()
        if size is None:
            size = self.arraysize
        end = min(self._pos + size, len(self._rows))
        chunk = self._rows[self._pos:end]
        self._pos = end
        return chunk

    async def fetchall(self):
        self._check_open()
        chunk = self._rows[self._pos:]
        self._pos = len(self._rows)
        return chunk

    async def close(self):
        self._closed = True
        self._rows = []
        self.description = None

    @property
    def closed(self):
        return self._closed

    def _check_open(self):
        if self._closed:
            raise errors.InterfaceError("Cursor is closed")
        if self.connection.closed:
            raise errors.InterfaceError("Connection is closed")


class AsyncConnection:
    """Async variant of :class:`diesel.connection.Connection`."""

    def __init__(self, protocol: AsyncProtocolClient):
        self._protocol = protocol
        self._closed = False
        self.autocommit = True
        self.isolation_level = 0
        self.encoding = "UTF8"
        self.server_version = 130000

    async def cursor(self) -> AsyncCursor:
        self._check_open()
        return AsyncCursor(self)

    async def commit(self):
        """No-op: the DieselDB libpq path is autocommit (documented)."""
        self._check_open()

    async def rollback(self):
        """No-op: the DieselDB libpq path is autocommit (documented)."""
        self._check_open()

    async def close(self):
        if self._closed:
            return
        self._closed = True
        await self._protocol.close()

    @property
    def closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> "AsyncConnection":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()
        return False

    def _check_open(self):
        if self._closed or self._protocol.closed:
            raise errors.InterfaceError("Connection is closed")


async def connect(dsn=None, **kwargs) -> AsyncConnection:
    """Open an async DieselDB connection (psycopg2-compatible parameters)."""
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

    protocol = await AsyncProtocolClient.connect(
        host, port, user, database, password, connect_timeout)
    return AsyncConnection(protocol)
