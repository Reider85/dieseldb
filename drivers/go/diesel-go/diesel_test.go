package diesel

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

// The JUnit harness (GoTest) starts an in-process LibpqServer and passes the
// port via environment variables; defaults keep the tests runnable by hand
// against a local server on 5432.
func testDSN(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		envOr("DIESEL_HOST", "localhost"),
		envOr("DIESEL_PORT", "5432"),
		envOr("DIESEL_USER", "testuser"),
		envOr("DIESEL_PASSWORD", "testpass"),
		envOr("DIESEL_DB", "testdb"))
}

func envOr(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("diesel", testDSN(t))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func dropIfExists(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	// DROP has no IF EXISTS; ignore missing-table errors.
	_, _ = db.Exec("DROP TABLE " + table)
}

func TestConnectAndSelectOne(t *testing.T) {
	db := openTestDB(t)
	var one int64
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 = %d, want 1", one)
	}
}

func TestConnectViaURL(t *testing.T) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		envOr("DIESEL_USER", "testuser"),
		envOr("DIESEL_PASSWORD", "testpass"),
		envOr("DIESEL_HOST", "localhost"),
		envOr("DIESEL_PORT", "5432"),
		envOr("DIESEL_DB", "testdb"))
	db, err := sql.Open("diesel", dsn)
	if err != nil {
		t.Fatalf("sql.Open(url): %v", err)
	}
	defer db.Close()
	var one int64
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1 via url: %v", err)
	}
}

func TestBasicCRUD(t *testing.T) {
	db := openTestDB(t)
	dropIfExists(t, db, "go_demo")

	if _, err := db.Exec("CREATE TABLE go_demo (ID INTEGER, NAME STRING, AGE INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	if _, err := db.Exec("INSERT INTO go_demo (ID, NAME, AGE) VALUES ($1, $2, $3)",
		int64(1), "Alice", int64(25)); err != nil {
		t.Fatalf("INSERT Alice: %v", err)
	}
	if _, err := db.Exec("INSERT INTO go_demo (ID, NAME, AGE) VALUES ($1, $2, $3)",
		int64(2), "Bob", int64(30)); err != nil {
		t.Fatalf("INSERT Bob: %v", err)
	}

	rows, err := db.Query("SELECT ID, NAME, AGE FROM go_demo")
	if err != nil {
		t.Fatalf("SELECT all: %v", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns: %v", err)
	}
	if len(cols) != 3 || cols[0] != "ID" || cols[1] != "NAME" || cols[2] != "AGE" {
		t.Fatalf("columns = %v, want [ID NAME AGE]", cols)
	}
	type row struct {
		id   int64
		name string
		age  int64
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name, &r.age); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(got) != 2 || got[0] != (row{1, "Alice", 25}) || got[1] != (row{2, "Bob", 30}) {
		t.Fatalf("rows = %+v, want Alice(1,25) Bob(2,30)", got)
	}

	var name string
	if err := db.QueryRow("SELECT NAME FROM go_demo WHERE ID = $1", int64(2)).Scan(&name); err != nil {
		t.Fatalf("SELECT WHERE: %v", err)
	}
	if name != "Bob" {
		t.Fatalf("WHERE ID=2 name = %q, want Bob", name)
	}

	if _, err := db.Exec("UPDATE go_demo SET AGE = $1 WHERE ID = $2", int64(26), int64(1)); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	var age int64
	if err := db.QueryRow("SELECT AGE FROM go_demo WHERE ID = 1").Scan(&age); err != nil {
		t.Fatalf("SELECT after UPDATE: %v", err)
	}
	if age != 26 {
		t.Fatalf("AGE after UPDATE = %d, want 26", age)
	}

	if _, err := db.Exec("DELETE FROM go_demo WHERE ID = 1"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM go_demo").Scan(&count); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if count != 1 {
		t.Fatalf("COUNT after DELETE = %d, want 1", count)
	}
}

func TestStringEscapingInParams(t *testing.T) {
	db := openTestDB(t)
	dropIfExists(t, db, "go_demo")
	if _, err := db.Exec("CREATE TABLE go_demo (ID INTEGER, NAME STRING, AGE INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO go_demo (ID, NAME, AGE) VALUES ($1, $2, $3)",
		int64(7), "O'Brien", int64(40)); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	var name string
	if err := db.QueryRow("SELECT NAME FROM go_demo WHERE ID = 7").Scan(&name); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if name != "O'Brien" {
		t.Fatalf("name = %q, want O'Brien", name)
	}
}

func TestNullParam(t *testing.T) {
	db := openTestDB(t)
	dropIfExists(t, db, "go_demo")
	if _, err := db.Exec("CREATE TABLE go_demo (ID INTEGER, NAME STRING, AGE INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	var age sql.NullInt64
	if _, err := db.Exec("INSERT INTO go_demo (ID, NAME, AGE) VALUES ($1, $2, $3)",
		int64(8), nil, nil); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	var id int64
	var name sql.NullString
	if err := db.QueryRow("SELECT ID, NAME, AGE FROM go_demo WHERE ID = 8").
		Scan(&id, &name, &age); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if id != 8 || name.Valid || age.Valid {
		t.Fatalf("got id=%d name.Valid=%v age.Valid=%v, want 8/NULL/NULL", id, name.Valid, age.Valid)
	}
}

func TestErrorThenConnectionReusable(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Query("SELECT * FROM table_that_does_not_exist"); err == nil {
		t.Fatal("expected error for missing table")
	}
	var one int64
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1 after error: %v", err)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 = %d, want 1", one)
	}
}

func TestPreparedStatements(t *testing.T) {
	db := openTestDB(t)
	dropIfExists(t, db, "go_demo")
	if _, err := db.Exec("CREATE TABLE go_demo (ID INTEGER, NAME STRING, AGE INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	stmt, err := db.Prepare("INSERT INTO go_demo (ID, NAME, AGE) VALUES ($1, $2, $3)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()
	if _, err := stmt.Exec(int64(1), "n1", int64(10)); err != nil {
		t.Fatalf("stmt.Exec: %v", err)
	}
	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM go_demo").Scan(&count); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if count != 1 {
		t.Fatalf("COUNT = %d, want 1", count)
	}
}

func TestTransactionNoOp(t *testing.T) {
	db := openTestDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	var one int64
	if err := tx.QueryRow("SELECT 1").Scan(&one); err != nil {
		t.Fatalf("tx SELECT 1: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestClosedConnectionRejects(t *testing.T) {
	db := openTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var one int64
	if err := db.QueryRow("SELECT 1").Scan(&one); err == nil {
		t.Fatal("expected error querying a closed database")
	}
}

func TestArityMismatch(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec("SELECT $1, $2", int64(1)); err == nil {
		t.Fatal("expected arity mismatch error")
	}
}
