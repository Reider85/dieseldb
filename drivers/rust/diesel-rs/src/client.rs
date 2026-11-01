//! Tokio-postgres-compatible client surface for the DieselDB libpq server.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::value::{bind_dollar, rows_from_tag, FromSql, ToSql};
use crate::wire::{QueryResult, WireClient};

const DEFAULT_PORT: u16 = 5432;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// A connection to a DieselDB libpq server.
///
/// Unlike tokio-postgres, there is no separate `Connection` future to spawn:
/// the DieselDB simple-query protocol completes each request/response round
/// trip inside the call, so all I/O happens in `query`/`execute` directly.
pub struct Client {
    wire: WireClient,
}

/// Connect with an explicit host and a libpq keyword config string
/// (`user=u password=p dbname=d port=5432`). The `host` argument is used
/// unless the config string itself contains `host=`.
pub async fn connect(host: &str, config: &str) -> Result<Client> {
    let params = parse_keywords(config)?;
    let effective_host = params
        .iter()
        .find(|(key, _)| key == "host")
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| host.to_string());
    let port = params
        .iter()
        .find(|(key, _)| key == "port")
        .map(|(_, value)| value.parse())
        .transpose()
        .map_err(|_| Error::Bind("invalid port in config".into()))?
        .unwrap_or(DEFAULT_PORT);
    let user = lookup(&params, "user").unwrap_or_else(|| "postgres".to_string());
    let database = lookup(&params, "dbname")
        .or_else(|| lookup(&params, "database"))
        .unwrap_or_else(|| "postgres".to_string());
    let password = lookup(&params, "password").unwrap_or_default();
    let wire = WireClient::dial(&effective_host, port, &user, &database, &password, CONNECT_TIMEOUT).await?;
    Ok(Client { wire })
}

/// Connect from a connection string: a `postgres://` URL or a libpq keyword
/// string (`host=... port=... user=... dbname=...`).
pub async fn connect_str(conn_str: &str) -> Result<Client> {
    let trimmed = conn_str.trim();
    if trimmed.contains("://") {
        let (host, params) = parse_url(trimmed)?;
        let port = params
            .iter()
            .find(|(key, _)| key == "port")
            .map(|(_, value)| value.parse())
            .transpose()
            .map_err(|_| Error::Bind("invalid port in URL".into()))?
            .unwrap_or(DEFAULT_PORT);
        let user = lookup(&params, "user").unwrap_or_else(|| "postgres".to_string());
        let database = lookup(&params, "dbname")
            .or_else(|| lookup(&params, "database"))
            .unwrap_or_else(|| "postgres".to_string());
        let password = lookup(&params, "password").unwrap_or_default();
        let wire = WireClient::dial(&host, port, &user, &database, &password, CONNECT_TIMEOUT).await?;
        return Ok(Client { wire });
    }
    // Keyword string without a scheme: host defaults to localhost.
    connect("localhost", trimmed).await
}

impl Client {
    /// Execute a statement with optional `$1..$n` parameters and return all
    /// result rows.
    pub async fn query(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<Row>> {
        let result = self.roundtrip(sql, params).await?;
        Ok(rows_from_result(result))
    }

    /// Execute a statement expected to return exactly one row.
    pub async fn query_one(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<Row> {
        let mut rows = self.query(sql, params).await?;
        match rows.len() {
            1 => Ok(rows.remove(0)),
            0 => Err(Error::Protocol("query returned no rows".into())),
            n => Err(Error::Protocol(format!("query returned {n} rows, expected 1"))),
        }
    }

    /// Execute a statement expected to return zero or one row.
    pub async fn query_opt(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<Option<Row>> {
        let mut rows = self.query(sql, params).await?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.remove(0))),
            n => Err(Error::Protocol(format!("query returned {n} rows, expected 0 or 1"))),
        }
    }

    /// Execute a statement and return the (server-provided, possibly
    /// hardcoded) affected-row count from CommandComplete.
    pub async fn execute(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<u64> {
        let result = self.roundtrip(sql, params).await?;
        Ok(result
            .command_tag
            .as_deref()
            .map(rows_from_tag)
            .unwrap_or(0))
    }

    /// Close the connection (best-effort Terminate + socket shutdown).
    pub async fn close(&mut self) {
        self.wire.close().await;
    }

    async fn roundtrip(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<QueryResult> {
        let bound = bind_dollar(sql, params)?;
        self.wire.simple_query(&bound).await
    }
}

fn rows_from_result(result: QueryResult) -> Vec<Row> {
    let (names, oids) = match (&result.column_names, &result.column_type_oids) {
        (Some(names), Some(oids)) => (names.clone(), oids.clone()),
        // Empty SELECT: the server sends no RowDescription at all.
        _ => return Vec::new(),
    };
    result
        .rows
        .into_iter()
        .map(|values| Row {
            columns: names.clone(),
            oids: oids.clone(),
            values,
        })
        .collect()
}

/// One result row with text-format values decoded on demand by type OID.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<String>,
    oids: Vec<i32>,
    values: Vec<Option<String>>,
}

impl Row {
    /// Column names in SELECT-list order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// True when the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a typed value by column index or name. Errors on NULL or a
    /// conversion failure (use [`Row::try_get`] to accept NULL).
    pub fn get<I: RowIndex, T: FromSql>(&self, idx: I) -> Result<T> {
        match self.try_get::<I, T>(idx)? {
            Some(value) => Ok(value),
            None => Err(Error::Conversion("NULL value".into())),
        }
    }

    /// Get an optional typed value by column index or name; NULL becomes
    /// `None`.
    pub fn try_get<I: RowIndex, T: FromSql>(&self, idx: I) -> Result<Option<T>> {
        let column = idx.idx(&self.columns)?;
        let text = self.values.get(column).ok_or_else(|| {
            Error::Column(format!("column index {column} out of bounds"))
        })?;
        match text {
            None => Ok(None),
            Some(text) => {
                let oid = self.oids.get(column).copied().unwrap_or(25);
                T::from_sql(text, oid).map(Some)
            }
        }
    }
}

/// Lookup a column by index or name (tokio-postgres `RowIndex` analogue).
pub trait RowIndex {
    fn idx(&self, columns: &[String]) -> Result<usize>;
}

impl RowIndex for usize {
    fn idx(&self, columns: &[String]) -> Result<usize> {
        if *self < columns.len() {
            Ok(*self)
        } else {
            Err(Error::Column(format!(
                "index {self} out of bounds ({} columns)",
                columns.len()
            )))
        }
    }
}

impl RowIndex for &str {
    fn idx(&self, columns: &[String]) -> Result<usize> {
        columns
            .iter()
            .position(|name| name == self)
            .ok_or_else(|| Error::Column(format!("column {self:?} not found")))
    }
}

fn parse_keywords(input: &str) -> Result<Vec<(String, String)>> {
    let mut params = Vec::new();
    for pair in input.split_whitespace() {
        match pair.split_once('=') {
            Some((key, value)) if !key.is_empty() => {
                params.push((key.to_string(), value.to_string()));
            }
            _ => {
                return Err(Error::Bind(format!(
                    "invalid DSN pair {pair:?} (want key=value)"
                )))
            }
        }
    }
    Ok(params)
}

fn lookup(params: &[(String, String)], key: &str) -> Option<String> {
    params
        .iter()
        .find(|(param_key, _)| param_key == key)
        .map(|(_, value)| value.clone())
}

/// Parse a `postgres://user:pass@host:port/dbname?k=v` URL into a host and a
/// keyword parameter list.
fn parse_url(raw: &str) -> Result<(String, Vec<(String, String)>)> {
    let rest = raw
        .strip_prefix("postgres://")
        .or_else(|| raw.strip_prefix("postgresql://"))
        .ok_or_else(|| Error::Bind(format!("unsupported DSN scheme in {raw:?}")))?;

    let (authority_and_path, query) = match rest.split_once('?') {
        Some((left, right)) => (left, Some(right)),
        None => (rest, None),
    };
    let (userinfo, hostport_path) = match authority_and_path.split_once('@') {
        Some((userinfo, hostport)) => (Some(userinfo), hostport),
        None => (None, authority_and_path),
    };
    // hostport_path = host[:port][/dbname] - split off the path first.
    let (hostport, dbname) = match hostport_path.split_once('/') {
        Some((hostport, dbname)) => (hostport, Some(dbname)),
        None => (hostport_path, None),
    };
    let (host, port) = match hostport.split_once(':') {
        Some((host, port)) if !host.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
            (host.to_string(), Some(port.to_string()))
        }
        _ => (hostport.to_string(), None),
    };

    let mut params: Vec<(String, String)> = Vec::new();
    if let Some(userinfo) = userinfo {
        match userinfo.split_once(':') {
            Some((user, password)) => {
                params.push(("user".into(), user.to_string()));
                params.push(("password".into(), password.to_string()));
            }
            None => params.push(("user".into(), userinfo.to_string())),
        }
    }
    if let Some(port) = port {
        params.push(("port".into(), port));
    }
    if let Some(dbname) = dbname {
        if !dbname.is_empty() {
            params.push(("dbname".into(), dbname.to_string()));
        }
    }
    if let Some(query) = query {
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                if !key.is_empty() {
                    params.push((key.to_string(), value.to_string()));
                }
            }
        }
    }
    Ok((host, params))
}
