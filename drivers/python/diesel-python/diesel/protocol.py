"""Minimal libpq wire-protocol client for the DieselDB Python driver.

Implements the subset of the protocol spoken by ``diesel.LibpqServer``:

* startup handshake (protocol 3.0, DieselDB-style leading type byte ``'0'`` -
  the server's ``LibpqClientHandler.readMessage`` expects a 5-byte header on
  the startup message too, unlike stock PostgreSQL),
* trust / cleartext-password authentication (the server's password mode sends
  ``AuthenticationOk`` immediately without a challenge),
* simple-query protocol,
* response decoding: RowDescription, DataRow, CommandComplete, ErrorResponse,
  NoticeResponse, ParameterStatus, ReadyForQuery.

The server's extended protocol (Parse/Bind/Execute) is a stub that executes a
hardcoded ``SELECT 1``, so this driver only ever uses simple queries with
client-side parameter substitution (mirrors the DieselDB JDBC driver).
"""

from __future__ import annotations

import socket
import struct
from typing import List, Optional, Sequence, Tuple

from . import errors

# Backend message type bytes (subset used by the DieselDB server).
AUTH_REQUEST = b"R"
PARAMETER_STATUS = b"S"
READY_FOR_QUERY = b"Z"
ROW_DESCRIPTION = b"T"
DATA_ROW = b"D"
COMMAND_COMPLETE = b"C"
ERROR_RESPONSE = b"E"
NOTICE_RESPONSE = b"N"
EMPTY_QUERY_RESPONSE = b"I"

# Frontend message type bytes.
STARTUP = b"0"  # DieselDB-style startup framing (type byte + length)
SIMPLE_QUERY = b"Q"
PASSWORD_MESSAGE = b"p"
TERMINATE = b"X"

# Authentication sub-codes inside AuthenticationRequest.
AUTH_OK = 0
AUTH_CLEARTEXT_PASSWORD = 3
AUTH_SASL = 10

PROTOCOL_VERSION_3 = 196608

TEXT_OID = 25


class ServerError(Exception):
    """Raw server error with SQLSTATE and severity, before DB-API mapping."""

    def __init__(self, severity, sqlstate, message):
        super().__init__(message)
        self.severity = severity
        self.sqlstate = sqlstate
        self.message = message


class QueryResult:
    """Outcome of one simple-query round trip."""

    __slots__ = ("column_names", "column_type_oids", "rows", "command_tag")

    def __init__(self, column_names, column_type_oids, rows, command_tag):
        self.column_names = column_names
        self.column_type_oids = column_type_oids
        self.rows = rows
        self.command_tag = command_tag


class ProtocolClient:
    """Blocking socket client speaking the DieselDB libpq subset."""

    def __init__(self, host, port, user, database, password,
                 connect_timeout_seconds=30.0):
        self._socket = socket.create_connection((host, port), connect_timeout_seconds)
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._file = self._socket.makefile("rwb")
        self._closed = False
        try:
            self._handshake(user or "postgres", database or "postgres",
                            password or "")
        except Exception:
            self.close()
            raise

    # ---- handshake ----

    def _handshake(self, user, database, password):
        self._send_startup(user, database)
        while True:
            msg = self._read_message()
            if msg is None:
                raise errors.InterfaceError(
                    "Connection closed during startup handshake")
            mtype, payload = msg
            if mtype == AUTH_REQUEST:
                self._handle_auth_request(payload, password)
            elif mtype in (PARAMETER_STATUS, NOTICE_RESPONSE):
                continue
            elif mtype == READY_FOR_QUERY:
                return
            elif mtype == ERROR_RESPONSE:
                err = parse_error_fields(payload)
                raise errors.InterfaceError("Startup failed: %s" % err.message)
            # ignore unknown startup-phase messages

    def _send_startup(self, user, database):
        params = b"user\x00" + user.encode("utf-8")
        params += b"\x00database\x00" + database.encode("utf-8")
        params += b"\x00client_encoding\x00UTF8\x00"
        params += b"\x00"  # terminator
        payload = struct.pack("!i", PROTOCOL_VERSION_3) + params
        # DieselDB framing: type byte '0' + length (includes itself) + payload.
        self._file.write(STARTUP + struct.pack("!i", 4 + len(payload)) + payload)
        self._file.flush()

    def _handle_auth_request(self, payload, password):
        if len(payload) < 4:
            raise errors.InterfaceError("Malformed AuthenticationRequest")
        (auth_type,) = struct.unpack_from("!i", payload, 0)
        if auth_type == AUTH_OK:
            return
        if auth_type == AUTH_CLEARTEXT_PASSWORD:
            pwd = (password or "").encode("utf-8") + b"\x00"
            self._write_message(PASSWORD_MESSAGE, pwd)
            return
        raise errors.NotSupportedError(
            "Unsupported authentication method: %d "
            "(supported: trust, cleartext password)" % auth_type)

    # ---- public API ----

    def simple_query(self, sql: str) -> QueryResult:
        """Execute one statement via the simple-query protocol."""
        self._write_message(SIMPLE_QUERY, sql.encode("utf-8") + b"\x00")

        column_names: Optional[List[str]] = None
        column_type_oids: Optional[List[int]] = None
        rows: List[Tuple[Optional[str], ...]] = []
        command_tag = None
        error = None

        while True:
            msg = self._read_message()
            if msg is None:
                raise errors.InterfaceError(
                    "Connection closed while reading query result")
            mtype, payload = msg
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

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._write_message(TERMINATE, b"")
        except OSError:
            pass
        try:
            self._file.close()
        except OSError:
            pass
        try:
            self._socket.close()
        except OSError:
            pass

    @property
    def closed(self):
        return self._closed

    # ---- framing ----

    def _write_message(self, mtype: bytes, payload: bytes):
        self._file.write(mtype + struct.pack("!i", 4 + len(payload)) + payload)
        self._file.flush()

    def _read_message(self) -> Optional[Tuple[bytes, bytes]]:
        header = self._file.read(5)
        if header is None or len(header) == 0:
            return None  # clean EOF
        if len(header) < 5:
            raise errors.InterfaceError("Connection closed mid-header")
        mtype = header[0:1]
        (length,) = struct.unpack_from("!i", header, 1)
        if length < 4:
            raise errors.InterfaceError("Invalid message length: %d" % length)
        payload = self._file.read(length - 4)
        if payload is None or len(payload) < length - 4:
            raise errors.InterfaceError("Connection closed mid-payload")
        return mtype, payload


def parse_error_fields(payload: bytes) -> ServerError:
    """Parse ErrorResponse: sequence of (byte fieldType, cstring) + NUL."""
    severity = None
    sqlstate = None
    message = None
    first = None
    i = 0
    while i < len(payload):
        field_type = payload[i]
        i += 1
        if field_type == 0:
            break
        end = i
        while end < len(payload) and payload[end] != 0:
            end += 1
        value = payload[i:end].decode("utf-8", "replace")
        i = end + 1
        if first is None:
            first = value
        if field_type == ord("S"):
            severity = value
        elif field_type == ord("V"):  # nonlocalized severity, prefer if present
            severity = value
        elif field_type == ord("C"):
            sqlstate = value
        elif field_type == ord("M"):
            message = value
    if message is None:
        message = first or "unknown error"
    return ServerError(severity or "ERROR", sqlstate or "00000", message)


def parse_row_description(payload: bytes):
    """RowDescription: int16 count, then per field (DieselDB layout):

    int32 tableOid, cstring name, int16 tableId, int16 columnId,
    int32 typeOid, int16 typeSize, int16 typeModifier, int16 formatCode.
    """
    (count,) = struct.unpack_from("!h", payload, 0)
    offset = 2
    names = []
    oids = []
    for _ in range(count):
        _table_oid = struct.unpack_from("!i", payload, offset)[0]
        offset += 4
        end = offset
        while end < len(payload) and payload[end] != 0:
            end += 1
        name = payload[offset:end].decode("utf-8")
        offset = end + 1
        _table_id, _column_id, type_oid, _type_size, _type_mod, _format = \
            struct.unpack_from("!hhihhh", payload, offset)
        offset += 14
        names.append(name)
        oids.append(type_oid)
    return names, oids


def parse_data_row(payload: bytes):
    """DataRow: int16 count, then per column int32 length (-1 = NULL) + bytes."""
    (count,) = struct.unpack_from("!h", payload, 0)
    offset = 2
    row = []
    for _ in range(count):
        (length,) = struct.unpack_from("!i", payload, offset)
        offset += 4
        if length < 0:
            row.append(None)
        else:
            row.append(payload[offset:offset + length].decode("utf-8"))
            offset += length
    return tuple(row)


def read_cstring(payload: bytes, offset: int) -> str:
    end = offset
    while end < len(payload) and payload[end] != 0:
        end += 1
    return payload[offset:end].decode("utf-8")
