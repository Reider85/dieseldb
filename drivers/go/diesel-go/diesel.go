// Package diesel implements a database/sql driver for the DieselDB libpq
// server (port 5432). It is compatible with the API style of lib/pq:
//
//	sql.Open("diesel", "host=localhost port=5432 user=x password=y dbname=z")
//
// Zero runtime dependencies: the standard library only.
//
// Limitations (mirrors the Python/Node/JDBC drivers): the server speaks the
// simple-query protocol only, so parameter binding is client-side ($1..$n),
// commit/rollback are no-ops (autocommit per statement), SCRAM authentication
// is not supported (trust + cleartext password only), and CommandComplete row
// counts are hardcoded server-side.
package diesel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func init() {
	sql.Register("diesel", &Driver{})
}

// Driver implements database/sql/driver.Driver.
type Driver struct{}

// Open opens a new connection with the given DSN.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	w, err := dialWire(cfg.host, cfg.port, cfg.user, cfg.database, cfg.password, cfg.connectTimeout)
	if err != nil {
		return nil, err
	}
	return &conn{wire: w}, nil
}

type config struct {
	host           string
	port           int
	user           string
	database       string
	password       string
	connectTimeout time.Duration
}

// parseDSN accepts the lib/pq keyword/value form
// ("host=localhost port=5432 user=u password=p dbname=d") and postgres:// URLs.
func parseDSN(dsn string) (*config, error) {
	cfg := &config{
		host:           "localhost",
		port:           5432,
		user:           "postgres",
		database:       "postgres",
		password:       "",
		connectTimeout: 30 * time.Second,
	}
	trimmed := strings.TrimSpace(dsn)
	if strings.Contains(trimmed, "://") {
		return parseURLDSN(trimmed, cfg)
	}
	// libpq keyword/value: space-separated key=value pairs.
	for _, pair := range strings.Fields(trimmed) {
		eq := strings.IndexByte(pair, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("invalid DSN pair %q (want key=value)", pair)
		}
		key, value := pair[:eq], pair[eq+1:]
		if err := cfg.set(key, value); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func parseURLDSN(raw string, cfg *config) (*config, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN URL: %w", err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("unsupported DSN scheme %q", u.Scheme)
	}
	if u.Hostname() != "" {
		cfg.host = u.Hostname()
	}
	if port := u.Port(); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("invalid port %q", port)
		}
		cfg.port = p
	}
	if u.User != nil {
		cfg.user = u.User.Username()
		if password, ok := u.User.Password(); ok {
			cfg.password = password
		}
	}
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		cfg.database = db
	}
	query := u.Query()
	for key, values := range query {
		if err := cfg.set(key, values[len(values)-1]); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func (c *config) set(key, value string) error {
	switch key {
	case "host":
		c.host = value
	case "port":
		p, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid port %q", value)
		}
		c.port = p
	case "user", "username":
		c.user = value
	case "dbname", "database":
		c.database = value
	case "password":
		c.password = value
	case "connect_timeout":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid connect_timeout %q", value)
		}
		c.connectTimeout = time.Duration(seconds) * time.Second
	default:
		// libpq accepts many keywords (sslmode, application_name, ...);
		// ignore the ones the DieselDB server does not use.
	}
	return nil
}

// conn implements driver.Conn.
type conn struct {
	wire   *wireClient
	closed bool
}

func (c *conn) prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, errors.New("connection is closed")
	}
	return &stmt{conn: c, query: query}, nil
}

// Prepare implements driver.Conn.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// Close implements driver.Conn.
func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.wire.close()
}

// Begin implements driver.Conn. DieselDB has no multi-statement transactions
// over the wire protocol: the returned Tx is a no-op (Commit/Rollback
// succeed), matching the autocommit semantics of the other DieselDB drivers.
func (c *conn) Begin() (driver.Tx, error) {
	if c.closed {
		return nil, errors.New("connection is closed")
	}
	return noOpTx{}, nil
}

type noOpTx struct{}

func (noOpTx) Commit() error   { return nil }
func (noOpTx) Rollback() error { return nil }

// ExecContext implements driver.ExecerContext.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, err := c.exec(query, args)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(rowsFromTag(result.CommandTag)), nil
}

// QueryContext implements driver.QueryerContext.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.query(query, args)
}

func (c *conn) exec(query string, args []driver.NamedValue) (*QueryResult, error) {
	values, err := namedValuesToDriver(args)
	if err != nil {
		return nil, err
	}
	bound, err := bindDollar(query, values)
	if err != nil {
		return nil, err
	}
	return c.wire.simpleQuery(bound)
}

func (c *conn) query(query string, args []driver.NamedValue) (driver.Rows, error) {
	result, err := c.exec(query, args)
	if err != nil {
		return nil, err
	}
	return newRows(result), nil
}

// stmt implements driver.Stmt.
type stmt struct {
	conn  *conn
	query string
}

// Close implements driver.Stmt.
func (s *stmt) Close() error { return nil }

// NumInput implements driver.Stmt: -1 because binding is client-side and the
// placeholder count is validated at execution time.
func (s *stmt) NumInput() int { return -1 }

// Exec implements driver.Stmt.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	result, err := s.conn.exec(s.query, named)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(rowsFromTag(result.CommandTag)), nil
}

// Query implements driver.Stmt.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return s.conn.query(s.query, named)
}

// rows implements driver.Rows over a buffered QueryResult.
type rows struct {
	cols []string
	oids []int32
	data [][]*string
	pos  int
}

func newRows(result *QueryResult) *rows {
	return &rows{
		cols: result.ColumnNames,
		oids: result.ColumnTypeOids,
		data: result.Rows,
	}
}

// Columns implements driver.Rows.
func (r *rows) Columns() []string { return r.cols }

// Close implements driver.Rows.
func (r *rows) Close() error { r.data = nil; return nil }

// Next implements driver.Rows.
func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	row := r.data[r.pos]
	r.pos++
	for i := range dest {
		var text *string
		if i < len(row) {
			text = row[i]
		}
		oid := int32(oidText)
		if i < len(r.oids) {
			oid = r.oids[i]
		}
		dest[i] = decodeValue(text, oid)
	}
	return nil
}
