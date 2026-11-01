//! Minimal libpq wire-protocol client for the DieselDB Rust driver.
//!
//! Implements the subset of the protocol spoken by `diesel.LibpqServer`:
//!
//! * startup handshake (protocol 3.0, DieselDB-style leading type byte `b'0'` -
//!   the server's `LibpqClientHandler.read_message` expects a 5-byte header on
//!   the startup message too, unlike stock PostgreSQL),
//! * trust / cleartext-password authentication (the server's password mode
//!   sends `AuthenticationOk` immediately without a challenge),
//! * simple-query protocol,
//! * response decoding: RowDescription, DataRow, CommandComplete,
//!   ErrorResponse, NoticeResponse, ParameterStatus, ReadyForQuery.
//!
//! The server's extended protocol (Parse/Bind/Execute) is a stub that executes
//! a hardcoded `SELECT 1`, so this driver only ever uses simple queries with
//! client-side parameter substitution (mirrors the DieselDB JDBC/Python/Node/
//! Go drivers).

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Error, Result};

// Backend message type bytes (subset used by the DieselDB server).
const MSG_AUTH_REQUEST: u8 = b'R';
const MSG_PARAMETER_STATUS: u8 = b'S';
const MSG_READY_FOR_QUERY: u8 = b'Z';
const MSG_ROW_DESCRIPTION: u8 = b'T';
const MSG_DATA_ROW: u8 = b'D';
const MSG_COMMAND_COMPLETE: u8 = b'C';
const MSG_ERROR_RESPONSE: u8 = b'E';
const MSG_NOTICE_RESPONSE: u8 = b'N';
const MSG_EMPTY_QUERY: u8 = b'I';

// Frontend message type bytes.
const MSG_STARTUP: u8 = b'0'; // DieselDB-style startup framing (type byte + length)
const MSG_SIMPLE_QUERY: u8 = b'Q';
const MSG_PASSWORD: u8 = b'p';
const MSG_TERMINATE: u8 = b'X';

const PROTOCOL_VERSION_3: u32 = 196608;

// Authentication sub-codes inside AuthenticationRequest.
const AUTH_OK: i32 = 0;
const AUTH_CLEARTEXT_PASSWORD: i32 = 3;

/// Outcome of one simple-query round trip.
#[derive(Debug, Default)]
pub struct QueryResult {
    pub column_names: Option<Vec<String>>,
    pub column_type_oids: Option<Vec<i32>>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tag: Option<String>,
}

/// Raw server error with SQLSTATE and severity.
#[derive(Debug)]
pub struct ServerError {
    pub severity: String,
    pub sqlstate: String,
    pub message: String,
}

impl From<ServerError> for Error {
    fn from(e: ServerError) -> Self {
        Error::Server {
            severity: e.severity,
            sqlstate: e.sqlstate,
            message: e.message,
        }
    }
}

/// Blocking-socket-free async client speaking the DieselDB libpq subset.
pub struct WireClient {
    stream: TcpStream,
}

impl WireClient {
    /// Dial the server and run the startup handshake.
    pub async fn dial(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: &str,
        timeout: Duration,
    ) -> Result<Self> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect((host, port)))
            .await
            .map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("connect to {host}:{port} timed out"),
                ))
            })??;
        stream.set_nodelay(true).ok();
        let mut client = WireClient { stream };
        client.handshake(user, database, password).await?;
        Ok(client)
    }

    // ---- handshake ----

    async fn handshake(&mut self, user: &str, database: &str, password: &str) -> Result<()> {
        self.send_startup(user, database).await?;
        loop {
            let (mtype, payload) = self.read_message().await.map_err(|e| {
                Error::Protocol(format!("connection closed during startup handshake: {e}"))
            })?;
            match mtype {
                MSG_AUTH_REQUEST => self.handle_auth_request(&payload, password).await?,
                MSG_PARAMETER_STATUS | MSG_NOTICE_RESPONSE => {}
                MSG_READY_FOR_QUERY => return Ok(()),
                MSG_ERROR_RESPONSE => {
                    let err = parse_error_fields(&payload);
                    return Err(Error::Server {
                        severity: err.severity,
                        sqlstate: err.sqlstate,
                        message: format!("startup failed: {}", err.message),
                    });
                }
                _ => {} // ignore unknown startup-phase messages
            }
        }
    }

    async fn send_startup(&mut self, user: &str, database: &str) -> Result<()> {
        let mut params: Vec<u8> = Vec::new();
        params.extend_from_slice(b"user\0");
        params.extend_from_slice(user.as_bytes());
        params.extend_from_slice(b"\0database\0");
        params.extend_from_slice(database.as_bytes());
        params.extend_from_slice(b"\0client_encoding\0UTF8\0");
        params.push(0); // terminator

        let mut payload = Vec::with_capacity(4 + params.len());
        payload.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
        payload.extend_from_slice(&params);

        // DieselDB framing: type byte '0' + length (includes itself) + payload.
        let mut message = Vec::with_capacity(5 + payload.len());
        message.push(MSG_STARTUP);
        message.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        message.extend_from_slice(&payload);
        self.stream.write_all(&message).await?;
        Ok(())
    }

    async fn handle_auth_request(&mut self, payload: &[u8], password: &str) -> Result<()> {
        if payload.len() < 4 {
            return Err(Error::Protocol("malformed AuthenticationRequest".into()));
        }
        let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        match auth_type {
            AUTH_OK => Ok(()),
            AUTH_CLEARTEXT_PASSWORD => {
                let mut pwd = password.as_bytes().to_vec();
                pwd.push(0);
                self.write_message(MSG_PASSWORD, &pwd).await
            }
            other => Err(Error::NotSupported(format!(
                "unsupported authentication method: {other} (supported: trust, cleartext password)"
            ))),
        }
    }

    // ---- public API ----

    /// Execute one statement via the simple-query protocol.
    pub async fn simple_query(&mut self, sql: &str) -> Result<QueryResult> {
        let mut payload = sql.as_bytes().to_vec();
        payload.push(0);
        self.write_message(MSG_SIMPLE_QUERY, &payload).await?;

        let mut result = QueryResult::default();
        let mut error: Option<ServerError> = None;

        loop {
            let (mtype, payload) = self.read_message().await.map_err(|e| {
                Error::Protocol(format!("connection closed while reading query result: {e}"))
            })?;
            match mtype {
                MSG_ROW_DESCRIPTION => {
                    let (names, oids) = parse_row_description(&payload)?;
                    result.column_names = Some(names);
                    result.column_type_oids = Some(oids);
                }
                MSG_DATA_ROW => result.rows.push(parse_data_row(&payload)?),
                MSG_COMMAND_COMPLETE => result.command_tag = Some(read_cstring(&payload, 0)),
                MSG_ERROR_RESPONSE => error = Some(parse_error_fields(&payload)),
                MSG_NOTICE_RESPONSE | MSG_PARAMETER_STATUS | MSG_EMPTY_QUERY => {}
                MSG_READY_FOR_QUERY => break,
                _ => {}
            }
        }

        match error {
            Some(err) => Err(err.into()),
            None => Ok(result),
        }
    }

    /// Send Terminate and close the socket.
    pub async fn close(&mut self) {
        // Best effort - the socket is closed either way.
        let _ = self.write_message(MSG_TERMINATE, &[]).await;
        let _ = self.stream.shutdown().await;
    }

    // ---- framing ----

    async fn write_message(&mut self, mtype: u8, payload: &[u8]) -> Result<()> {
        let mut message = Vec::with_capacity(5 + payload.len());
        message.push(mtype);
        message.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        message.extend_from_slice(payload);
        self.stream.write_all(&message).await?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<(u8, Vec<u8>)> {
        let mut header = [0u8; 5];
        match self.stream.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(Error::Protocol("connection closed".into()))
            }
            Err(e) => return Err(e.into()),
        }
        let mtype = header[0];
        let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        if length < 4 {
            return Err(Error::Protocol(format!("invalid message length: {length}")));
        }
        let mut payload = vec![0u8; (length - 4) as usize];
        self.stream.read_exact(&mut payload).await.map_err(|e| {
            Error::Protocol(format!("connection closed mid-payload: {e}"))
        })?;
        Ok((mtype, payload))
    }
}

// ---- parsing ----

fn parse_error_fields(payload: &[u8]) -> ServerError {
    let mut severity = String::new();
    let mut sqlstate = String::new();
    let mut message = String::new();
    let mut first = String::new();
    let mut i = 0;
    while i < payload.len() {
        let field_type = payload[i];
        i += 1;
        if field_type == 0 {
            break;
        }
        let start = i;
        let mut end = i;
        while end < payload.len() && payload[end] != 0 {
            end += 1;
        }
        let value = String::from_utf8_lossy(&payload[start..end]).into_owned();
        i = end + 1;
        if first.is_empty() {
            first = value.clone();
        }
        match field_type {
            b'S' | b'V' => severity = value,
            b'C' => sqlstate = value,
            b'M' => message = value,
            _ => {}
        }
    }
    if message.is_empty() {
        message = if first.is_empty() {
            "unknown error".to_string()
        } else {
            first
        };
    }
    ServerError {
        severity: if severity.is_empty() {
            "ERROR".to_string()
        } else {
            severity
        },
        sqlstate: if sqlstate.is_empty() {
            "00000".to_string()
        } else {
            sqlstate
        },
        message,
    }
}

/// Parse the DieselDB RowDescription layout: int16 count, then per field
/// int32 tableOid, cstring name, int16 tableId, int16 columnId, int32 typeOid,
/// int16 typeSize, int16 typeModifier, int16 formatCode.
fn parse_row_description(payload: &[u8]) -> Result<(Vec<String>, Vec<i32>)> {
    if payload.len() < 2 {
        return Err(Error::Protocol("short RowDescription".into()));
    }
    let count = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2;
    let mut names = Vec::with_capacity(count);
    let mut oids = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > payload.len() {
            return Err(Error::Protocol("short RowDescription field".into()));
        }
        offset += 4; // tableOid (unused)
        let mut end = offset;
        while end < payload.len() && payload[end] != 0 {
            end += 1;
        }
        names.push(String::from_utf8_lossy(&payload[offset..end]).into_owned());
        offset = end + 1;
        if offset + 14 > payload.len() {
            return Err(Error::Protocol("short RowDescription tail".into()));
        }
        let type_oid = i32::from_be_bytes([
            payload[offset + 4],
            payload[offset + 5],
            payload[offset + 6],
            payload[offset + 7],
        ]);
        oids.push(type_oid);
        offset += 14; // tableId, columnId, typeOid, typeSize, typeMod, format
    }
    Ok((names, oids))
}

/// Parse int16 count, then per column int32 length (-1 = NULL) + length bytes
/// (text format).
fn parse_data_row(payload: &[u8]) -> Result<Vec<Option<String>>> {
    if payload.len() < 2 {
        return Err(Error::Protocol("short DataRow".into()));
    }
    let count = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2;
    let mut row = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > payload.len() {
            return Err(Error::Protocol("short DataRow column".into()));
        }
        let length = i32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        offset += 4;
        if length < 0 {
            row.push(None);
            continue;
        }
        let length = length as usize;
        if offset + length > payload.len() {
            return Err(Error::Protocol("short DataRow value".into()));
        }
        row.push(Some(
            String::from_utf8_lossy(&payload[offset..offset + length]).into_owned(),
        ));
        offset += length;
    }
    Ok(row)
}

fn read_cstring(payload: &[u8], offset: usize) -> String {
    let mut end = offset;
    while end < payload.len() && payload[end] != 0 {
        end += 1;
    }
    String::from_utf8_lossy(&payload[offset..end]).into_owned()
}
