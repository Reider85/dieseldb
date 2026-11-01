//! Text-format value conversion (decode by type OID, encode as SQL literals).

use crate::error::{Error, Result};

// PostgreSQL type OIDs the DieselDB server can emit (LibpqClientHandler
// .typeOid_for) plus common text-family OIDs for forward compatibility.
// Kept as documentation of the wire contract; decoding dispatches on the
// caller's target type rather than the OID.
#[allow(dead_code)]
pub(crate) mod oids {
    pub const BOOL: i32 = 16;
    pub const INT8: i32 = 20;
    pub const INT2: i32 = 21;
    pub const INT4: i32 = 23;
    pub const TEXT: i32 = 25;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const VARCHAR: i32 = 1043;
    pub const NUMERIC: i32 = 1700;
}

/// Decode a text-format wire value to `T` by type OID.
pub trait FromSql: Sized {
    fn from_sql(text: &str, oid: i32) -> Result<Self>;
}

impl FromSql for bool {
    fn from_sql(text: &str, _oid: i32) -> Result<Self> {
        match text {
            "t" | "true" | "TRUE" | "True" | "1" | "y" | "yes" => Ok(true),
            "f" | "false" | "FALSE" | "False" | "0" | "n" | "no" => Ok(false),
            other => Err(Error::Conversion(format!("cannot parse {other:?} as bool"))),
        }
    }
}

impl FromSql for i16 {
    fn from_sql(text: &str, _oid: i32) -> Result<Self> {
        text.parse()
            .map_err(|e| Error::Conversion(format!("cannot parse {text:?} as i16: {e}")))
    }
}

impl FromSql for i32 {
    fn from_sql(text: &str, _oid: i32) -> Result<Self> {
        text.parse()
            .map_err(|e| Error::Conversion(format!("cannot parse {text:?} as i32: {e}")))
    }
}

impl FromSql for i64 {
    fn from_sql(text: &str, _oid: i32) -> Result<Self> {
        text.parse()
            .map_err(|e| Error::Conversion(format!("cannot parse {text:?} as i64: {e}")))
    }
}

impl FromSql for f32 {
    fn from_sql(text: &str, _oid: i32) -> Result<Self> {
        text.parse()
            .map_err(|e| Error::Conversion(format!("cannot parse {text:?} as f32: {e}")))
    }
}

impl FromSql for f64 {
    fn from_sql(text: &str, _oid: i32) -> Result<Self> {
        text.parse()
            .map_err(|e| Error::Conversion(format!("cannot parse {text:?} as f64: {e}")))
    }
}

impl FromSql for String {
    fn from_sql(text: &str, _oid: i32) -> Result<Self> {
        Ok(text.to_string())
    }
}

impl FromSql for Vec<u8> {
    fn from_sql(text: &str, _oid: i32) -> Result<Self> {
        Ok(text.as_bytes().to_vec())
    }
}

/// Encode a Rust value as a SQL literal (client-side binding).
pub trait ToSql {
    fn to_sql_literal(&self) -> String;
}

impl ToSql for bool {
    fn to_sql_literal(&self) -> String {
        // DieselDB stores booleans as integers in the engine's type system.
        if *self {
            "1".to_string()
        } else {
            "0".to_string()
        }
    }
}

impl ToSql for i16 {
    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl ToSql for i32 {
    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl ToSql for i64 {
    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl ToSql for f32 {
    fn to_sql_literal(&self) -> String {
        format!("{}", self)
    }
}

impl ToSql for f64 {
    fn to_sql_literal(&self) -> String {
        format!("{}", self)
    }
}

impl ToSql for str {
    fn to_sql_literal(&self) -> String {
        format!("'{}'", self.replace('\'', "''"))
    }
}

impl ToSql for &str {
    fn to_sql_literal(&self) -> String {
        (*self).to_sql_literal()
    }
}

impl ToSql for String {
    fn to_sql_literal(&self) -> String {
        self.as_str().to_sql_literal()
    }
}

impl ToSql for &[u8] {
    fn to_sql_literal(&self) -> String {
        let text = String::from_utf8_lossy(self);
        format!("'{}'", text.replace('\'', "''"))
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql_literal(&self) -> String {
        match self {
            Some(value) => value.to_sql_literal(),
            None => "NULL".to_string(),
        }
    }
}

/// Substitute `$1..$n` placeholders with quoted literals (client-side binding;
/// the server's extended protocol is a stub).
pub(crate) fn bind_dollar(query: &str, params: &[&dyn ToSql]) -> Result<String> {
    if params.is_empty() && !query.contains('$') {
        return Ok(query.to_string());
    }
    let bytes = query.as_bytes();
    let mut out = String::with_capacity(query.len() + 32);
    let mut max_index = 0usize;
    let mut last_copied = 0;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                // '$' and digits are ASCII, so [last_copied..i] and [i+1..j]
                // are valid UTF-8 boundaries.
                out.push_str(&query[last_copied..i]);
                let n: usize = query[i + 1..j].parse().map_err(|_| {
                    Error::Bind(format!("invalid placeholder {}", &query[i..j]))
                })?;
                if n < 1 {
                    return Err(Error::Bind(format!("invalid placeholder {}", &query[i..j])));
                }
                max_index = max_index.max(n);
                if n > params.len() {
                    return Err(Error::Bind(format!("missing argument for placeholder ${n}")));
                }
                out.push_str(&params[n - 1].to_sql_literal());
                last_copied = j;
                i = j;
                continue;
            }
        }
        i += 1;
    }
    out.push_str(&query[last_copied..]);
    if max_index != params.len() {
        return Err(Error::Bind(format!(
            "arity mismatch: {max_index} placeholders, {} arguments",
            params.len()
        )));
    }
    Ok(out)
}

/// Best-effort affected-row count from a CommandComplete tag.
/// The DieselDB server hardcodes counts (SELECT 1 / INSERT 0 1 / UPDATE 1 /
/// DELETE 1 - LibpqClientHandler.get_command_complete_tag).
pub(crate) fn rows_from_tag(tag: &str) -> u64 {
    tag.split_whitespace()
        .rev()
        .find_map(|part| part.parse().ok())
        .unwrap_or(0)
}
