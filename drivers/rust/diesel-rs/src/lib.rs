//! Tokio-postgres-compatible async driver for the DieselDB libpq server.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), diesel_db::Error> {
//! let mut client = diesel_db::connect("localhost", "user=postgres dbname=postgres").await?;
//! let rows = client.query("SELECT 1", &[]).await?;
//! let value: i32 = rows[0].get(0)?;
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Limitations
//!
//! The DieselDB server speaks the simple-query protocol only, so parameter
//! binding is client-side (`$1..$n`), there is no separate `Connection`
//! future to spawn (each query round trip completes inside the call),
//! commit/rollback semantics are autocommit per statement, SCRAM
//! authentication is not supported (trust + cleartext password only), and
//! `CommandComplete` row counts are hardcoded server-side. These match the
//! Python/Node/JDBC/Go DieselDB drivers.

mod client;
mod error;
mod value;
mod wire;

pub use client::{connect, connect_str, Client, Row, RowIndex};
pub use error::{Error, Result};
pub use value::{FromSql, ToSql};

/// Driver version (crates.io publish is tagged `driver-rust-v<version>`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
