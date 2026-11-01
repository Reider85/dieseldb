# diesel-db

Tokio-postgres-compatible async driver for the [DieselDB](https://github.com/Reider85/dieseldb) libpq server (port 5432). Crate name `diesel-db` (import `diesel_db`); depends only on [tokio](https://crates.io/crates/tokio) (`net`, `io-util`, `time`).

> Named `diesel-db` to avoid clashing with the [Diesel ORM](https://crates.io/crates/diesel) published as `diesel`.

## Install

```toml
[dependencies]
diesel-db = "0.1"
tokio = { version = "1", features = ["full"] }
```

Published to [crates.io](https://crates.io/crates/diesel-db) by the `Publish Drivers` workflow (`driver-rust-v*` tags / dispatch, secret `CRATES_IO_TOKEN`).

## Usage

```rust
use diesel_db::connect;

#[tokio::main]
async fn main() -> Result<(), diesel_db::Error> {
    // libpq keyword config (tokio-postgres connect(host, config) shape):
    let mut client = connect("localhost", "user=postgres password=secret dbname=postgres").await?;

    client
        .execute(
            "CREATE TABLE demo (ID INTEGER, NAME STRING, AGE INTEGER)",
            &[],
        )
        .await?;
    client
        .execute(
            "INSERT INTO demo (ID, NAME, AGE) VALUES ($1, $2, $3)",
            &[&1i32, &"Alice", &25i32],
        )
        .await?;

    let row = client
        .query_one("SELECT NAME FROM demo WHERE ID = $1", &[&1i32])
        .await?;
    let name: String = row.get(0)?;
    assert_eq!(name, "Alice");

    client.close().await;
    Ok(())
}
```

`connect_str` also accepts `postgres://user:pass@host:port/dbname` URLs. Placeholders are `$1..$n` and are substituted **client-side** before the statement is sent.

### API surface (tokio-postgres parallels)

| diesel_db | tokio_postgres |
|---|---|
| `connect(host, config)` -> `Client` | `connect(host, config)` -> `(Client, Connection)` |
| `connect_str(url_or_keywords)` | `connect_str` / `Config::connect_str` |
| `Client::query/sql, &[..])` -> `Vec<Row>` | `Client::query` -> `Vec<Row>` |
| `Client::query_one` / `query_opt` | `Client::query_one` / `query_opt` |
| `Client::execute` -> `u64` | `Client::execute` -> `u64` |
| `Row::get::<I, T>` / `try_get` | `Row::get` / `try_get` |
| `ToSql` / `FromSql` traits | same names |

**Deviation:** there is no separate `Connection` future to `tokio::spawn` — the DieselDB simple-query protocol completes each request/response round trip inside `query`/`execute`, so all I/O happens in the returned `Client`.

## Testing

The JUnit harness (`RustTest` in the `diesel-jdbc` module) starts an in-process DieselDB libpq server and runs:

```bash
cargo test
```

Environment variables (all optional, defaults target a hand-started server on 5432): `DIESEL_HOST`, `DIESEL_PORT`, `DIESEL_USER`, `DIESEL_PASSWORD`, `DIESEL_DB`.

## Limitations

Identical to the DieselDB Python/Node/JDBC/Go drivers — the server speaks the simple-query protocol subset only:

- parameter binding is client-side (`$1..$n`); the server's extended protocol is a stub;
- autocommit per statement — no multi-statement transactions over the wire;
- authentication: trust and cleartext password only (no SCRAM);
- `CommandComplete` row counts are hardcoded server-side;
- an empty `SELECT` returns no `RowDescription`.

## Publishing

```bash
cargo publish    # requires CRATES_IO_TOKEN in the Publish Drivers workflow
```

CI: tag `driver-rust-vX.Y.Z` (or `drivers-vX.Y.Z`) triggers the `crates` job; `workflow_dispatch` accepts `rust` / `all`.
