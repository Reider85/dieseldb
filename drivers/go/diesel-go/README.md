# diesel-go

Go `database/sql` driver for the [DieselDB](https://github.com/Reider85/dieseldb) libpq server (port 5432). API-compatible with [lib/pq](https://pkg.go.dev/github.com/lib/pq)-style usage; zero runtime dependencies (standard library only).

## Install

```bash
go get github.com/Reider85/dieseldb/drivers/go/diesel-go
```

pkg.go.dev indexes the module automatically from the monorepo subdirectory tag:

```bash
git tag drivers/go/diesel-go/v0.1.0
git push origin drivers/go/diesel-go/v0.1.0
# https://pkg.go.dev/github.com/Reider85/dieseldb/drivers/go/diesel-go appears
# after the Go module proxy picks the tag up (usually within minutes).
```

## Usage

```go
import (
    "database/sql"

    _ "github.com/Reider85/dieseldb/drivers/go/diesel-go"
)

func main() {
    db, err := sql.Open("diesel", "host=localhost port=5432 user=postgres password=secret dbname=postgres")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // postgres:// URLs work too:
    // db, err := sql.Open("diesel", "postgres://postgres:secret@localhost:5432/postgres")

    if _, err := db.Exec("CREATE TABLE demo (ID INTEGER, NAME STRING, AGE INTEGER)"); err != nil {
        panic(err)
    }
    if _, err := db.Exec("INSERT INTO demo (ID, NAME, AGE) VALUES ($1, $2, $3)", 1, "Alice", 25); err != nil {
        panic(err)
    }

    var name string
    if err := db.QueryRow("SELECT NAME FROM demo WHERE ID = $1", 1).Scan(&name); err != nil {
        panic(err)
    }
}
```

Placeholders are `$1..$n` (lib/pq style) and are substituted **client-side** before the statement is sent.

## Testing

The JUnit harness (`GoTest` in the `diesel-jdbc` module) starts an in-process DieselDB libpq server and runs:

```bash
go test ./...
```

Environment variables (all optional, defaults target a hand-started server on 5432): `DIESEL_HOST`, `DIESEL_PORT`, `DIESEL_USER`, `DIESEL_PASSWORD`, `DIESEL_DB`.

## Limitations

Identical to the DieselDB Python/Node/JDBC drivers — the server speaks the simple-query protocol subset only:

- parameter binding is client-side (`$1..$n`); the server's extended protocol is a stub;
- `Begin`/`Commit`/`Rollback` are no-ops (autocommit per statement) — DieselDB has no multi-statement transactions over the wire;
- authentication: trust and cleartext password only (no SCRAM);
- `CommandComplete` row counts are hardcoded server-side (`SELECT 1` / `INSERT 0 1` / ...);
- an empty `SELECT` returns no `RowDescription`.

## Publishing

Push the monorepo subdirectory tag `drivers/go/diesel-go/vX.Y.Z`; the Go module proxy and pkg.go.dev index it automatically (no secrets required). The `Publish Drivers` workflow (`driver-go-v*` tags / dispatch) runs a `go vet` + `go test` smoke gate.
