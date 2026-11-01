/**
 * Error types for the DieselDB Node.js driver (pg-compatible `DbError`).
 */

/** Database error with SQLSTATE (pg exposes `e.code`). */
export class DbError extends Error {
  /** SQLSTATE code (the DieselDB server currently always sends `00000`). */
  code?: string;
  /** Severity reported by the server (`ERROR`, ...). */
  severity?: string;
  /** Localized severity (`ERROR`, ...), when present. */
  routine?: string;

  constructor(message: string, code?: string, severity?: string) {
    super(message);
    this.name = 'DbError';
    this.code = code;
    this.severity = severity;
    Object.setPrototypeOf(this, DbError.prototype);
  }
}

/** Interface-level error (connection closed, malformed framing, ...). */
export class InterfaceError extends DbError {
  constructor(message: string) {
    super(message, '08000', 'ERROR');
    this.name = 'InterfaceError';
    Object.setPrototypeOf(this, InterfaceError.prototype);
  }
}

/** Unsupported operation (e.g. SCRAM authentication). */
export class NotSupportedError extends DbError {
  constructor(message: string) {
    super(message, '0A000', 'ERROR');
    this.name = 'NotSupportedError';
    Object.setPrototypeOf(this, NotSupportedError.prototype);
  }
}
