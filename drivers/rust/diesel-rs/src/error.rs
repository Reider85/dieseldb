//! Error types for the DieselDB Rust driver.

use std::fmt;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors surfaced by the driver.
#[derive(Debug)]
pub enum Error {
    /// Socket or OS-level I/O failure.
    Io(std::io::Error),
    /// Framing / decoding problem on the wire.
    Protocol(String),
    /// The server returned an ErrorResponse.
    Server {
        severity: String,
        sqlstate: String,
        message: String,
    },
    /// Feature the DieselDB server does not support (e.g. SCRAM auth).
    NotSupported(String),
    /// Parameter binding problem (arity, placeholder syntax).
    Bind(String),
    /// Text-to-typed-value conversion failure.
    Conversion(String),
    /// Column index/name lookup failure on a Row.
    Column(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Error::Server {
                severity,
                sqlstate,
                message,
            } => write!(f, "server error {severity} [{sqlstate}]: {message}"),
            Error::NotSupported(msg) => write!(f, "not supported: {msg}"),
            Error::Bind(msg) => write!(f, "parameter binding failed: {msg}"),
            Error::Conversion(msg) => write!(f, "value conversion failed: {msg}"),
            Error::Column(msg) => write!(f, "column lookup failed: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
