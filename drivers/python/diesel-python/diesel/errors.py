"""DB-API 2.0 exception hierarchy for the DieselDB Python driver.

The DieselDB libpq server always reports SQLSTATE ``00000`` in ErrorResponse
fields (LibpqClientHandler hardcodes it), so classification falls back to the
generic :class:`DatabaseError` unless the server someday sends a real code.
"""

from __future__ import annotations


class Warning(Exception):
    """Base class for warnings raised by the driver (DB-API 2.0)."""


class Error(Exception):
    """Base class for all driver errors (DB-API 2.0)."""


class InterfaceError(Error):
    """Error related to the database interface rather than the database itself."""


class DatabaseError(Error):
    """Error related to the database."""


class DataError(DatabaseError):
    """Error due to problems with the processed data (division by zero, etc.)."""


class OperationalError(DatabaseError):
    """Error related to the database's operation (unexpected disconnect, ...)."""


class IntegrityError(DatabaseError):
    """Integrity constraint violation (foreign key, unique, ...)."""


class InternalError(DatabaseError):
    """Internal database error (invalid cursor, transaction mismatch, ...)."""


class ProgrammingError(DatabaseError):
    """Programming error (table not found, syntax error, wrong arity, ...)."""


class NotSupportedError(DatabaseError):
    """Unsupported operation (nonzero autocommit requests, ...)."""


def exception_for_sqlstate(sqlstate, message):
    """Map a PostgreSQL SQLSTATE to the DB-API exception class.

    The DieselDB server currently sends ``00000`` for every error, which maps
    to :class:`DatabaseError` (the generic base). Real codes, when present,
    follow the usual PostgreSQL class-letter convention.
    """
    if sqlstate and len(sqlstate) >= 2:
        cls = sqlstate[:2]
        if cls == "08":
            return OperationalError(message)
        if cls == "22":
            return DataError(message)
        if cls == "23":
            return IntegrityError(message)
        if cls == "24":
            return InternalError(message)
        if cls == "25":
            return InternalError(message)
        if cls == "38":
            return InternalError(message)
        if cls == "39":
            return InternalError(message)
        if cls == "40":
            return OperationalError(message)
        if cls == "42":
            return ProgrammingError(message)
        if cls == "53":
            return OperationalError(message)
        if cls == "54":
            return OperationalError(message)
        if cls == "55":
            return ProgrammingError(message)
        if cls == "57":
            return OperationalError(message)
        if cls == "58":
            return InternalError(message)
        if cls == "F0":
            return InternalError(message)
        if cls == "P0":
            return InternalError(message)
        if cls == "XX":
            return InternalError(message)
    return DatabaseError(message)
