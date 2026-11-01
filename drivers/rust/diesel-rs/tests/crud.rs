//! Async CRUD acceptance tests for the DieselDB Rust driver.
//!
//! The JUnit harness (RustTest) starts an in-process LibpqServer and passes
//! the port via environment variables; defaults keep the tests runnable by
//! hand against a local server on 5432.

use diesel_db::{connect, connect_str, Error};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn test_config() -> (String, String) {
    let host = env_or("DIESEL_HOST", "localhost");
    let config = format!(
        "port={} user={} password={} dbname={}",
        env_or("DIESEL_PORT", "5432"),
        env_or("DIESEL_USER", "testuser"),
        env_or("DIESEL_PASSWORD", "testpass"),
        env_or("DIESEL_DB", "testdb"),
    );
    (host, config)
}

async fn open_client() -> diesel_db::Client {
    let (host, config) = test_config();
    connect(&host, &config)
        .await
        .expect("connect to DieselDB libpq server")
}

async fn drop_if_exists(client: &mut diesel_db::Client, table: &str) {
    // DROP has no IF EXISTS; ignore missing-table errors.
    let _ = client.execute(&format!("DROP TABLE {table}"), &[]).await;
}

#[tokio::test]
async fn connect_and_select_one() {
    let mut client = open_client().await;
    let rows = client.query("SELECT 1", &[]).await.expect("SELECT 1");
    assert_eq!(rows.len(), 1);
    let value: i64 = rows[0].get(0).expect("decode SELECT 1");
    assert_eq!(value, 1);
    client.close().await;
}

#[tokio::test]
async fn connect_via_url() {
    let dsn = format!(
        "postgres://{}:{}@{}:{}/{}",
        env_or("DIESEL_USER", "testuser"),
        env_or("DIESEL_PASSWORD", "testpass"),
        env_or("DIESEL_HOST", "localhost"),
        env_or("DIESEL_PORT", "5432"),
        env_or("DIESEL_DB", "testdb"),
    );
    let mut client = connect_str(&dsn).await.expect("connect via URL");
    let rows = client.query("SELECT 1", &[]).await.expect("SELECT 1");
    let value: i64 = rows[0].get(0).expect("decode");
    assert_eq!(value, 1);
    client.close().await;
}

#[tokio::test]
async fn basic_crud() {
    let mut client = open_client().await;
    // cargo test runs tests in parallel - each test owns its table.
    drop_if_exists(&mut client, "rs_basic_demo").await;

    client
        .execute("CREATE TABLE rs_basic_demo (ID INTEGER, NAME STRING, AGE INTEGER)", &[])
        .await
        .expect("CREATE TABLE");

    client
        .execute(
            "INSERT INTO rs_basic_demo (ID, NAME, AGE) VALUES ($1, $2, $3)",
            &[&1i32, &"Alice", &25i32],
        )
        .await
        .expect("INSERT Alice");
    client
        .execute(
            "INSERT INTO rs_basic_demo (ID, NAME, AGE) VALUES ($1, $2, $3)",
            &[&2i32, &"Bob", &30i32],
        )
        .await
        .expect("INSERT Bob");

    let rows = client
        .query("SELECT ID, NAME, AGE FROM rs_basic_demo", &[])
        .await
        .expect("SELECT all");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].columns(), &["ID", "NAME", "AGE"]);
    let (id, name, age): (i32, String, i32) = (
        rows[0].get(0).unwrap(),
        rows[0].get(1).unwrap(),
        rows[0].get(2).unwrap(),
    );
    assert_eq!((id, name.as_str(), age), (1, "Alice", 25));
    let name_bob: String = rows[1].get("NAME").unwrap();
    assert_eq!(name_bob, "Bob");

    let row = client
        .query_one("SELECT NAME FROM rs_basic_demo WHERE ID = $1", &[&2i32])
        .await
        .expect("SELECT WHERE");
    let name: String = row.get(0).unwrap();
    assert_eq!(name, "Bob");

    client
        .execute("UPDATE rs_basic_demo SET AGE = $1 WHERE ID = $2", &[&26i32, &1i32])
        .await
        .expect("UPDATE");
    let row = client
        .query_one("SELECT AGE FROM rs_basic_demo WHERE ID = 1", &[])
        .await
        .expect("SELECT after UPDATE");
    let age: i32 = row.get(0).unwrap();
    assert_eq!(age, 26);

    client
        .execute("DELETE FROM rs_basic_demo WHERE ID = 1", &[])
        .await
        .expect("DELETE");
    let row = client
        .query_one("SELECT COUNT(*) FROM rs_basic_demo", &[])
        .await
        .expect("COUNT");
    let count: i64 = row.get(0).unwrap();
    assert_eq!(count, 1);

    drop_if_exists(&mut client, "rs_basic_demo").await;
    client.close().await;
}

#[tokio::test]
async fn string_escaping_in_params() {
    let mut client = open_client().await;
    drop_if_exists(&mut client, "rs_escape_demo").await;
    client
        .execute("CREATE TABLE rs_escape_demo (ID INTEGER, NAME STRING, AGE INTEGER)", &[])
        .await
        .expect("CREATE TABLE");
    client
        .execute(
            "INSERT INTO rs_escape_demo (ID, NAME, AGE) VALUES ($1, $2, $3)",
            &[&7i32, &"O'Brien", &40i32],
        )
        .await
        .expect("INSERT");
    let row = client
        .query_one("SELECT NAME FROM rs_escape_demo WHERE ID = 7", &[])
        .await
        .expect("SELECT");
    let name: String = row.get(0).unwrap();
    assert_eq!(name, "O'Brien");
    drop_if_exists(&mut client, "rs_escape_demo").await;
    client.close().await;
}

#[tokio::test]
async fn null_param() {
    let mut client = open_client().await;
    drop_if_exists(&mut client, "rs_null_demo").await;
    client
        .execute("CREATE TABLE rs_null_demo (ID INTEGER, NAME STRING, AGE INTEGER)", &[])
        .await
        .expect("CREATE TABLE");
    client
        .execute(
            "INSERT INTO rs_null_demo (ID, NAME, AGE) VALUES ($1, $2, $3)",
            &[&8i32, &Option::<&str>::None, &Option::<i32>::None],
        )
        .await
        .expect("INSERT NULLs");
    let row = client
        .query_one("SELECT ID, NAME, AGE FROM rs_null_demo WHERE ID = 8", &[])
        .await
        .expect("SELECT");
    let id: i32 = row.get(0).unwrap();
    assert_eq!(id, 8);
    let name: Option<String> = row.try_get(1).unwrap();
    assert!(name.is_none());
    let age: Option<i32> = row.try_get("AGE").unwrap();
    assert!(age.is_none());
    drop_if_exists(&mut client, "rs_null_demo").await;
    client.close().await;
}

#[tokio::test]
async fn error_then_connection_reusable() {
    let mut client = open_client().await;
    let err = client
        .query("SELECT * FROM table_that_does_not_exist", &[])
        .await
        .expect_err("missing table must fail");
    match &err {
        Error::Server { .. } | Error::Protocol(_) => {}
        other => panic!("unexpected error kind: {other:?}"),
    }
    // After an error the connection must still answer queries.
    let rows = client.query("SELECT 1", &[]).await.expect("SELECT 1 after error");
    let value: i64 = rows[0].get(0).unwrap();
    assert_eq!(value, 1);
    client.close().await;
}

#[tokio::test]
async fn query_opt_and_arity_mismatch() {
    let mut client = open_client().await;
    drop_if_exists(&mut client, "rs_opt_demo").await;
    client
        .execute("CREATE TABLE rs_opt_demo (ID INTEGER)", &[])
        .await
        .expect("CREATE TABLE");
    let missing = client
        .query_opt("SELECT ID FROM rs_opt_demo WHERE ID = 999", &[])
        .await
        .expect("query_opt on empty result");
    assert!(missing.is_none());

    let err = client
        .query("SELECT $1, $2", &[&1i32])
        .await
        .expect_err("arity mismatch must fail");
    assert!(matches!(err, Error::Bind(_)));
    drop_if_exists(&mut client, "rs_opt_demo").await;
    client.close().await;
}
