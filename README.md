# DieselDB

> **Production-grade Java database engine with ARIES recovery, WAL, page-based storage, online schema changes, and SSI concurrency control**
>
> Production-grade база данных на Java 21 (LTS) с фокусом на современных DB-engineering практиках: ARIES crash recovery, segmented WAL с group commit, page-based storage с BufferPool (LRU eviction), MVCC с SSI для SERIALIZABLE, online `ALTER TABLE` (DEFAULT/COPY/INPLACE + LOCK), RBAC + TLS/mTLS/SNI multi-tenant, async audit log.

[![Java Version](https://img.shields.io/badge/Java-21%20LTS-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Maven](https://img.shields.io/badge/Maven-3.9%2B-blue.svg)](https://maven.apache.org/)
[![Version](https://img.shields.io/badge/version-3.3.99-green.svg)](#releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/Reider85/dieseldb/ci.yml)](https://github.com/Reider85/dieseldb/actions)
[![Stars](https://img.shields.io/github/stars/Reider85/dieseldb?style=social)](https://github.com/Reider85/dieseldb/stargazers)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](#license)

> 🚀 **Status:** Production-like. Current version `3.3.99` — see [§ Releases](#releases) and [§ Roadmap](#roadmap).

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [SQL Reference](#sql-reference)
- [Storage Backends](#storage-backends)
- [Transactions & MVCC](#transactions--mvcc)
- [Indexes](#indexes)
- [Online Schema Changes](#online-schema-changes)
- [WAL & ARIES Recovery](#wal--aries-recovery)
- [Network Protocol](#network-protocol)
- [Replication & HA](#replication--ha)
- [Security: RBAC, TLS, Audit](#security-rbac-tls-audit)
- [Backup & Restore](#backup--restore)
- [Observability & Health](#observability--health)
- [Configuration](#configuration)
- [Build & Test](#build--test)
- [Project Structure](#project-structure)
- [Development Status](#development-status)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Releases](#releases)
- [License](#license)
- [Acknowledgments](#acknowledgments)

---

## Overview

DieselDB — это production-grade встраиваемая (embedded) и клиент-серверная СУБД, написанная на Java 21 (LTS). Проект реализует современную архитектуру production БД с акцентом на надёжность, производительность и security:

- **ARIES recovery** (Analysis → Redo → Undo) с 25+ тест-классами, покрывающими crash-сценарии
- **Segmented WAL** с CRC32C checksums, group commit (5ms / 64 batch), async writer с 4 fsync policies
- **Page-based storage** с BufferPool (LRU eviction, pin/unpin), slotted page layout, tombstone reuse, defrag
- **5 storage backends**: in-memory, CSV, TSV, JSONL (streaming + block manager + delta compaction), AVRO (с отдельной экосистемой из 58 классов)
- **MVCC** с tuple-level versioning (`xmin`/`xmax`/`commandId`), SSI `ConflictDetector` для SERIALIZABLE
- **Granular LockManager** с 5 modes (IS/IX/S/X/SIX) + `WaitForGraph` deadlock detector (daemon thread + cycle detection + victim selection)
- **Online `ALTER TABLE`** полностью (DEFAULT/COPY/INPLACE + LOCK=NONE/SHARED/EXCLUSIVE + `DualWriteCoordinator`)
- **6 типов индексов**: BTree, BTreeClustered, CompositeBTree, CoveringBTree (index-only scans), Hash, Unique
- **RBAC** с 8 actions (SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/ADMIN), users, roles, tenants
- **TLS 1.3/1.2 AEAD-only** + mTLS + SNI multi-tenant (cipher whitelist, не deny-list — соответствует SSL Labs A+)
- **Async audit log** с bounded queue + retention + gzip archive
- **Binary wire protocol v2** с 21 message types, CRC32 footer, ZSTD/GZIP compression
- **libpq wire protocol** (port 5432) с SCRAM-SHA-256 auth и psql meta-commands
- **5 официальных драйверов**: JDBC (Hibernate-compatible `diesel-jdbc`), Python (`diesel-python`, psycopg2-compatible), Node.js (`diesel-node`, pg-compatible), Go (`diesel-go`, database/sql, lib/pq-style), Rust (`diesel-rs`, tokio-postgres-style)
- **Physical streaming replication** (WalSender/WalReceiver, port 5433) + replication slots + synchronous standby ACK
- **PITR** (Point-in-Time Recovery) через commit-timestamped WAL replay
- **Incremental physical backup** + `BackupScheduler` с 5-field cron
- **CRC32C checksummed pages** + background `PageScrubber` + `CheckpointStrategy` (sharp/fuzzy)
- **7 JMX MBeans** для monitoring (BufferPool, BufferPoolFlusher, CheckpointManager, RecoveryManager, QueryProfiler, WALWriter, AsyncWALWriter, GroupCommitCoordinator)
- **Prometheus `/metrics`** + `/health`/`/health/live`/`/health/ready` endpoints + `MetricsRegistry` (lock-free counters, p50/p95/p99 latency histograms)
- **SIMD-vectorized aggregates** (COUNT/MIN/MAX/AVG/SUM через `jdk.incubator.vector`)
- **Adaptive Query Optimizer** с plan cache (LRU) и fingerprint normalization
- **O(1) eviction в BufferPool** (unpinned-page queue), O(1) catalog lookup (LinkedHashMap name index), O(1) dirty-page index (LinkedHashSet), inverse position-to-rowId maps в CSV/TSV/JSONL index managers

### Why DieselDB?

DieselDB — это **production-grade database engine**, реализующий:

- Page-based storage engine с BufferPool LRU (как в InnoDB / Postgres shared_buffers)
- ARIES recovery с полной трёхфазной реализацией (Analysis → Redo → Undo)
- MVCC с SSI conflict detection (как в Postgres Serializable Snapshot Isolation)
- Online schema changes с dual-write coordinator (как в MySQL 8.0+)
- Multi-tenant TLS с SNI-based keystore routing (как в современных enterprise API gateways)

Проект находится в активной разработке (current version `3.3.99`), движется к v1.0.0 stable release. Основные subsystems (storage, WAL, recovery, concurrency, security, network, replication, observability) — production-implemented и покрыты test-suite из 368 test-классов. Часть high-level features (logical replication, FK constraints, JSON type, CTE, window functions) — в active development, см. [§ Roadmap](#roadmap).

---

## Key Features

### ✅ Production-Implemented (готово к использованию)

| Category | Реализация в коде |
|---|---|
| **SQL DDL** | `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD/DROP COLUMN`, `CREATE INDEX` (6 типов), `CREATE USER`, `CREATE ROLE`, `CREATE TENANT`, `GRANT`, `REVOKE`, `ALTER USER` |
| **SQL DML** | `INSERT INTO ... VALUES`, `INSERT INTO ... SELECT`, `UPDATE`, `DELETE` |
| **SQL DQL** | `SELECT` с `WHERE` (11 операторов), 7 `JOIN` типов, `GROUP BY`, `HAVING` (агрегаты), `ORDER BY`, `LIMIT`/`OFFSET`, `DISTINCT`, `AS`, scalar+IN-list subqueries, derived tables |
| **Transactions** | `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `SET AUTOCOMMIT`, `SET TRANSACTION ISOLATION LEVEL`, `BEGIN BATCH` / `END BATCH` |
| **Isolation levels** | `READ_UNCOMMITTED` (default), `READ_COMMITTED`, `REPEATABLE_READ`, `SERIALIZABLE` — см. [Development Status](#development-status) про semantics polishing |
| **Indexes** | 6 типов: `BTREE`, `UNIQUE`, `HASH`, `UNIQUE CLUSTERED`, `COMPOSITE`, `COVERING` (+ `AVRO` для Avro storage) |
| **Meta** | `SHOW TABLES`, `SHOW CREATE TABLE`, `SHOW CREATE INDEXES` |
| **Admin** | `EXPLAIN [ANALYZE]`, `ANALYZE TABLE`, `VACUUM`, `VACUUM TABLE`, `ALTER STATUS`, `/* MAX_ROWS=N */` hint |
| **Prepared statements** | Server-side (`?`-placeholders, LRU plan cache, Prompt 79) |
| **Server-side cursors** | `OPEN_CURSOR`, `FETCH_CURSOR`, `CLOSE_CURSOR` (Prompt 81) |
| **Storage backends** | `in_memory` (default), `csv`, `tsv`, `jsonl` (streaming + block + delta), `avro` (58-классовая подсистема) |
| **Compression codecs** | `zstd`, `lz4`, `snappy` (для CSV/TSV/JSONL); `BZip2`, `Deflate`, `Snappy optimized`, `ZStandard` (для AVRO) |
| **Crash recovery** | ARIES (Analysis → Redo → Undo), segmented WAL, group commit, async writer, 4 fsync policies, WAL archiver, checkpoint manager |
| **Concurrency** | MVCC (tuple-level), SSI `ConflictDetector`, `LockManager` (5 modes IS/IX/S/X/SIX), `WaitForGraph` deadlock detector (daemon), `LockTimeoutManager`, `SavepointManager` + nested stack |
| **Online ALTER** | `ALGORITHM=DEFAULT/COPY/INPLACE` + `LOCK=NONE/SHARED/EXCLUSIVE`, `DualWriteCoordinator`, `ALTER STATUS` для мониторинга, `Database.cancelAlter()` |
| **Security** | RBAC (8 actions, users, roles, tenants), `Authenticator` + `PasswordHasher` (constant-time), `Session`/`SessionToken`, async `AuditLogger` (bounded queue + 90d retention + gzip archive) |
| **TLS** | TLS 1.3/1.2 AEAD-only, mTLS (`none`/`request`/`require`), SNI multi-tenant (per-hostname keystore) |
| **Network** | Binary protocol v2 (DSEL magic, CRC32, 21 message types), ZSTD/GZIP wire compression, legacy v1 protocol on port 5440 (deprecated, scheduled for removal); libpq protocol (port 5432) с SCRAM-SHA-256 auth для psql/JDBC/Python/Node/Go/Rust клиентов |
| **Backup** | `PhysicalBackup` + `PhysicalRestore` (с `BackupManifest` JSON + checksums), `IncrementalBackup` (page-level CRC delta + parent-linked manifest), `BackupScheduler` (5-field cron, default daily 02:00), `AvroBackupManager` + `AvroRestoreManager` (полные для Avro storage) |
| **Monitoring** | 7 JMX MBeans (`diesel:type=...`), `QueryProfiler` с per-phase breakdown (parse/plan/execute/sort), slow query log; `MetricsRegistry` (lock-free counters `LongAdder`, bucketed latency histograms p50/p95/p99), `PrometheusExporter` + `MetricsHttpServer` на `/metrics` (port 9090), `HealthCheck`/`LivenessCheck`/`ReadinessCheck` на `/health`/`/health/live`/`/health/ready` |
| **Query optimizer** | Adaptive (`QueryOptimizer`, Prompt 82) с LRU plan cache + fingerprint normalization + replan threshold (0.5 / 2.0) |
| **Aggregates** | `COUNT`, `MIN`, `MAX`, `AVG`, `SUM` — SIMD-vectorized через `jdk.incubator.vector` |
| **Schema versioning** | `SchemaVersion` (immutable), `SchemaVersionRegistry` (bounded history + pin-based protection), `AlterAlgorithm` enum, `AlterLockMode` enum |
| **CLI** | `CliRepl` interactive REPL с командами `HELP`, `DUMP`, `BACKUP`, `PHYSICAL-RESTORE`, `RESTORE`; интерактивный prompt логина/пароля через `System.console().readPassword()` (без echo), piped-stdin fallback |
| **Replication** | Physical streaming (WalSender/WalReceiver на replication.port 5433, PostgreSQL standby convention), replication slots (`ReplicationSlotRegistry`), synchronous standby ACK (`replication.role = standby` + `WITH (sync=true)`), `CREATE PUBLICATION`/`CREATE SUBSCRIPTION` (logical CDC via `LogicalDecoder`) |
| **PITR** | `PitrManager` + `PointInTimeRecovery` — recovery по commit-timestamped WAL replay (RECOVERY_TARGET_TIME) |
| **Checksummed pages** | `CRC32C` (hardware-accelerated via `jdk.incubator.vector`), `ChecksummedPage` с corruption detection, `PageValidator`, background `PageScrubber` + `CheckpointStrategy` (sharp/fuzzy) |
| **Drivers** | JDBC (`diesel-jdbc`, Hibernate-compatible `DatabaseMetaData`), Python (`diesel-python` на PyPI, psycopg2-compatible asyncio + sync), Node.js (`diesel-node` на npm, pg-compatible Promise/async), Go (`diesel-go` на pkg.go.dev, `database/sql` + lib/pq-style), Rust (`diesel-rs` на crates.io, tokio-postgres-style) |
| **BufferPool O(1)** | `LruEvictionPolicy` с unpinned-page queue (O(1) eviction под pinned-head), `CatalogTable` с LinkedHashMap name index (O(1) getTableSchema/dropTableSchema), `BufferPool` LinkedHashSet dirty-page index (O(1) getDirtyPageCount/flushDirtyMatching), inverse position-to-rowId maps в `DelimitedIndexManager`/`JsonlIndexManager` (O(1) вместо O(R) entry-set scan) |

### 🚧 In Active Development (planned for v1.0.0)

- Logical replication auto-failover (Patroni-equivalent) — physical streaming + sync standby + slots уже реализованы
- FOREIGN KEY / CHECK / NOT NULL / DEFAULT constraints
- JSON column type (binary JSONB-like storage)
- CTE / WITH / recursive CTE
- UNION / INTERSECT / EXCEPT
- Window functions (OVER, PARTITION BY, ROW_NUMBER, RANK)
- Stored procedures / functions / triggers
- Geo / spatial types
- BLOB / CLOB / TEXT / VARCHAR / DECIMAL(p,s) / TIMESTAMP / INTERVAL / ARRAY / MAP / ENUM

См. [§ Roadmap](#roadmap) для полного списка planned features.

---

## Architecture

```mermaid
flowchart TB
    subgraph Client["Client layer"]
        Cli["CliRepl (REPL)"]
        DbClient["DatabaseClient<br/>binary v2 wire protocol"]
    end

    subgraph Server["Server layer"]
        DbServer["DatabaseServer<br/>port 3306, pool 100"]
        Legacy["LegacyProtocolAdapter<br/>port 5440 (DEPRECATED)"]
        Libpq["LibpqServer<br/>port 5432 (SCRAM-SHA-256)"]
        Auth["Authenticator + RBAC"]
        Audit["AuditLogger (async)"]
    end

    subgraph Drivers["Drivers (separate modules)"]
        JDBC["diesel-jdbc<br/>(Hibernate-compatible)"]
        Py["diesel-python (psycopg2)"]
        Node["diesel-node (pg)"]
        Go["diesel-go (database/sql)"]
        Rust["diesel-rs (tokio-postgres)"]
    end

    subgraph Observability["Observability"]
        Metrics["MetricsRegistry<br/>(LongAdder + p50/p95/p99 histograms)"]
        Prom["PrometheusExporter<br/>/metrics on :9090"]
        Health["HealthCheck<br/>/health /live /ready"]
    end

    subgraph Replication["Replication & HA"]
        WalSender["WalSender (master :5433)"]
        WalReceiver["WalReceiver (standby)"]
        Slots["ReplicationSlotRegistry"]
        Sync["Sync standby ACK"]
        Logical["LogicalDecoder + Publication/Subscription"]
        Pitr["PitrManager (PITR)"]
    end

    subgraph Checksum["Checksum"]
        CRC32C["CRC32C (hw-accelerated)"]
        CkPage["ChecksummedPage + PageValidator"]
        Scrubber["PageScrubber (background)"]
        CkStrategy["CheckpointStrategy (sharp/fuzzy)"]
    end

    subgraph Engine["Engine (diesel.Database)"]
        Cache["QueryCache (AST cache)"]
        Optimizer["QueryOptimizer (adaptive)"]
        Profiler["QueryProfiler (JMX)"]
    end

    subgraph Parser["SQL Parser"]
        Lexer["SqlLexer"]
        Parser2["QueryParser (25+ strategies)"]
        Sub["SubqueryParser"]
    end

    subgraph Executor["Executor"]
        Select["SelectQuery.execute (4557 LoC)"]
        AvroExec["AvroQueryExecutor"]
        Agg["AggregateFunctions (SIMD)"]
    end

    subgraph Storage["Storage"]
        Factory["StorageFactory"]
        Backends["5 backends:<br/>InMemory / CSV / TSV / JSONL / AVRO"]
        Page["Page-based layer:<br/>Page + BufferPool (LRU)<br/>+ SlottedPageLayout + Defrag<br/>+ PageManager + CheckpointManager"]
        Catalog["CatalogTable (page 0)"]
    end

    subgraph Tx["Transaction"]
        Transaction["Transaction + UndoLog"]
        TxTracker["TxStatusTracker"]
        TupleVis["TupleVisibility (MVCC)"]
        Conflict["ConflictDetector (SSI)"]
    end

    subgraph Concurrency["Concurrency"]
        LockMgr["LockManager (IS/IX/S/X/SIX)"]
        Deadlock["DeadlockDetector + WaitForGraph"]
        Savepoint["SavepointManager"]
    end

    subgraph WAL["WAL + ARIES"]
        WALMgr["WALManager (segmented)"]
        AsyncWriter["AsyncWALWriter + GroupCommitCoordinator"]
        ARIES["ARIESAlgorithm<br/>(Analysis → Redo → Undo)"]
    end

    subgraph Security["Security"]
        RBAC["Privilege (8 actions)"]
        Users["User + Role + TenantStore"]
        TLS["TLS 1.3/1.2 + mTLS + SNI"]
    end

    Client --> Server
    Drivers --> Libpq
    Server --> Engine
    Engine --> Parser
    Engine --> Executor
    Executor --> Storage
    Executor --> Tx
    Storage --> Page
    Storage --> Catalog
    Tx --> Concurrency
    Tx --> WAL
    Engine --> Security
    Engine --> Observability
    WAL --> Replication
    WAL --> Checksum
    Replication --> Pitr
    Checksum --> Page
```

### Главные пакеты

| Package | .java файлов | Роль |
|---|---|---|
| `diesel` (root) | 131 | Engine core: `Database`, `QueryParser`, `SelectQuery`, `Index` types, `Table`, `LibpqServer`, `CreateSubscriptionQuery` |
| `diesel.storage` | 34 | `StorageFactory`, `RowStorage` interface, all 5 backends, `CompressionFactory` |
| `diesel.storage.avro` | 58 | AVRO subsystem: `AvroRowStorage`, `AvroBackupManager`, `AvroRecoveryManager`, `AvroBloomFilter`, `SchemaEvolutionManager`, etc. |
| `diesel.storage.page` | 28 | Page-based: `Page`, `BufferPool`, `PageManager`, `SlottedPageLayout`, `CatalogTable`, `PageScrubber`, `AdaptiveFlushStrategy`, `AtomicFileWriter`, JMX MBeans |
| `diesel.storage.json` | 15 | Streaming JSON (library-neutral): `JsonStreamParser`, Jackson/Gson adapters |
| `diesel.storage.tablespace` | 3 | `Tablespace` + `TablespaceFile` + `TablespaceRegistry` (in development) |
| `diesel.net` | 12 | `WireProtocol`, `MessageType`, TLS handlers, compression handshake |
| `diesel.net.messages` | 37 | Wire-сообщения v2 + codecs (21 message type) |
| `diesel.net.libpq` | 4 | libpq wire protocol: `LibpqProtocol` (port 5432), `LibpqMessage`/`LibpqMessageDecoder`, `ScramSha256` |
| `diesel.security` | 23 | RBAC (`Privilege`, `User`, `Role`, `TenantStore`), `Authenticator`, `AuditLogger` |
| `diesel.wal` | 19 | `WALManager`, `WALSegment`, `AsyncWALWriter`, `GroupCommitCoordinator`, `WALArchiver` |
| `diesel.recovery` | 20 | ARIES: `ARIESAlgorithm`, `AnalysisPhase`, `RedoPhase`, `UndoPhase`, `SharpCheckpoint`/`FuzzyCheckpoint`, `CheckpointStrategy`, `PointInTimeRecovery`/`PitrManager` |
| `diesel.concurrency` | 13 | `LockManager`, `DeadlockDetector`, `WaitForGraph`, `ConflictDetector`, `SavepointManager` |
| `diesel.backup` | 13 | `DieselBackup`, `PhysicalBackup`, `IncrementalBackup`, `BackupScheduler` (cron), `LogicalDump` (in development), `BackupManifest` |
| `diesel.replication` | 15 | `WalSender`/`WalReceiver`, `ReplicationSlot`/`ReplicationSlotRegistry`, `HotStandbyHandler`, `LogicalDecoder`/`Publication`/`Subscription`/`StreamingApplier` |
| `diesel.observability` | 11 | `MetricsRegistry` (lock-free `Counter`/`Gauge`/`Histogram`), `PrometheusExporter`, `MetricsHttpServer`, `HealthCheck`/`LivenessCheck`/`ReadinessCheck`, `HealthJson` |
| `diesel.checksum` | 3 | `CRC32C` (hardware-accelerated), `ChecksummedPage`, `PageValidator` |
| `diesel.schema` | 5 | `SchemaVersion`, `SchemaVersionRegistry`, `AlterAlgorithm`, `AlterLockMode` |

**Итого: 444 main-класса + 368 test-классов ≈ 112 664 строк кода.** Дополнительно: 5 отдельных драйвер-модулей в `drivers/` (JDBC, Python, Node.js, Go, Rust).

---

## Quick Start

### Requirements

- JDK 21+ (verified with JDK 21 LTS)
- Maven 3.9+ (or use Maven bundled with IntelliJ IDEA)

### Build from source

```bash
git clone https://github.com/Reider85/dieseldb.git
cd dieseldb

# Quick test (smoke tests only)
mvn test

# Full test suite (all test classes)
mvn -Ptest test

# Build JAR
mvn package

# Install to local Maven repository
mvn install
```

### Start server

```bash
# Linux/macOS
./start-server.sh

# Windows
start-server.bat
```

Default port: **3306** (binary protocol v2), legacy port: **5440** (deprecated Java Object Serialization — disable in production).

### Connect via CLI client

```bash
# Linux/macOS
./start-client.sh

# Windows
start-client.bat
```

Interactive REPL prompt: `diesel> `

### Embedded mode (in-process)

```java
import diesel.Database;
import diesel.DieselDatabase;

// Create in-memory database (default)
Database db = new Database();

// Or with persistent storage
Database db = new Database("/path/to/data/dir");

// Execute SQL
Object result = db.executeQuery("SELECT * FROM users WHERE age > 18", null);

// With explicit transaction
UUID txId = db.beginTransaction(IsolationLevel.READ_COMMITTED);
db.executeQuery("INSERT INTO users (name, age) VALUES ('Alice', 30)", txId);
db.executeQuery("UPDATE users SET age = 31 WHERE name = 'Alice'", txId);
db.executeQuery("COMMIT", txId);

db.close();
```

### Network mode (client-server)

```java
import diesel.DatabaseClient;

DatabaseClient client = new DatabaseClient("localhost", 3306, "admin", "password");
client.connect();

Object result = client.executeQuery("SELECT name, age FROM users WHERE active = TRUE");

// Prepared statement
String stmtId = client.prepareStatement("SELECT * FROM users WHERE age > ? AND name LIKE ?");
Object rs = client.executePrepared(stmtId, 18, "Al%");

// Server-side cursor
String cursorId = client.openCursor("SELECT * FROM large_table", 1000);
List<Map<String,Object>> batch = client.fetchCursor(cursorId);
client.closeCursor(cursorId);

client.disconnect();
```

---

## SQL Reference

### Поддерживаемые типы данных (14 типов)

```sql
CREATE TABLE users (
  id           INTEGER PRIMARY KEY,
  name         STRING,
  email        STRING,
  age          INTEGER,
  score        BIGDECIMAL,
  balance      BIGDECIMAL,
  active       BOOLEAN,
  user_code    STRING,
  created      DATETIME,
  created_ms   DATETIME_MS,
  uuid         UUID,
  level        SHORT,
  byte_val     BYTE,
  long_val     LONG,
  float_val    FLOAT,
  double_val   DOUBLE,
  char_val     CHAR,
  seq_val      INTEGER SEQUENCE(seq1 1 1)
);
```

> 🚧 **In development** (planned for v1.0.0): `VARCHAR`, `NVARCHAR`, `DECIMAL(p,s)`, `NUMERIC`, `TEXT`, `BLOB`, `CLOB`, `TIMESTAMP`, `TIMESTAMPTZ`, `TIME`, `INTERVAL`, `ARRAY`, `MAP`, `ENUM`, `JSON`, `GEOMETRY`. См. [Roadmap](#roadmap).

### DDL

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING, age INTEGER);
DROP TABLE users;

-- Online ALTER TABLE with algorithm + lock specification
ALTER TABLE users ADD COLUMN email STRING ALGORITHM=INPLACE LOCK=NONE;
ALTER TABLE users DROP COLUMN age ALGORITHM=COPY LOCK=SHARED;
ALTER STATUS;  -- show running/finished COPY operations

-- Indexes (6 types)
CREATE INDEX idx_name ON users (name);
CREATE UNIQUE INDEX idx_email ON users (email);
CREATE HASH INDEX h_code ON users (user_code);
CREATE UNIQUE CLUSTERED INDEX pk_id ON users (id);
CREATE COMPOSITE INDEX idx_age_name ON users (age, name);
CREATE COVERING INDEX idx_cover ON users (age) COVERING (name, email);

-- RBAC
CREATE USER alice PASSWORD 'secret' MUST CHANGE;
CREATE ROLE reader;
CREATE TENANT tenant1 SNI 'tenant1.example.com' KEYSTORE '/path/keystore.p12';
DROP TENANT tenant1;
GRANT SELECT, INSERT ON users TO reader;
GRANT ROLE reader TO alice;
REVOKE INSERT ON users FROM reader;
ALTER USER alice PASSWORD 'newsecret';
```

### DML

```sql
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25), (3, 'Carol', 28);
INSERT INTO archive SELECT * FROM users WHERE active = FALSE;

UPDATE users SET age = 31 WHERE name = 'Alice';
DELETE FROM users WHERE id = 2;
```

### DQL

```sql
-- WHERE with 11 operators
SELECT name, age FROM users
WHERE age >= 18
  AND name LIKE 'Al%'
  AND user_code IN ('A1', 'A2', 'A3')
  AND email IS NOT NULL
ORDER BY age DESC, name ASC
LIMIT 50 OFFSET 10;

-- 7 JOIN types
SELECT u.name, o.id
FROM users u
INNER JOIN orders o ON o.user_id = u.id
LEFT OUTER JOIN payments p ON p.order_id = o.id
WHERE u.age > 18;

-- Aggregates
SELECT name, COUNT(*) AS order_count, SUM(o.total) AS total
FROM users u
INNER JOIN orders o ON o.user_id = u.id
GROUP BY u.name
HAVING COUNT(*) > 5
ORDER BY total DESC;

-- Subqueries (scalar + IN-list)
SELECT name FROM users
WHERE id IN (SELECT user_id FROM orders WHERE total > 100);

-- Derived tables
SELECT t.name, t.cnt
FROM (
  SELECT name, COUNT(*) AS cnt
  FROM users
  GROUP BY name
) t
WHERE t.cnt > 1;
```

### Transactions

```sql
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO accounts (id, balance) VALUES (1, 100);
UPDATE accounts SET balance = balance - 50 WHERE id = 1;

-- Savepoints
SAVEPOINT before_critical;
UPDATE accounts SET balance = balance + 50 WHERE id = 1;
ROLLBACK TO SAVEPOINT before_critical;
RELEASE SAVEPOINT before_critical;

COMMIT;

-- Auto-commit toggle
SET AUTOCOMMIT = OFF;

-- Batch mode
BEGIN BATCH;
INSERT INTO logs (msg) VALUES ('msg1');
INSERT INTO logs (msg) VALUES ('msg2');
INSERT INTO logs (msg) VALUES ('msg3');
END BATCH;
```

### Meta & Admin

```sql
SHOW TABLES;
SHOW CREATE TABLE users;
SHOW CREATE INDEXES FROM users;

EXPLAIN ANALYZE SELECT * FROM users WHERE age > 18;
ANALYZE TABLE users;
VACUUM TABLE users;

-- Hint to override max result rows (default 1M)
SELECT /* MAX_ROWS=5000000 */ * FROM huge_table;
```

### Prepared statements & cursors (via wire protocol)

```java
// Server-side prepared statement (?-placeholders + LRU plan cache)
String stmtId = client.prepareStatement(
  "SELECT name, age FROM users WHERE age > ? AND name LIKE ?"
);
Object rs = client.executePrepared(stmtId, 18, "Al%");
client.closePrepared(stmtId);

// Server-side cursor (fetch in batches)
String cursorId = client.openCursor("SELECT * FROM huge_table", 1000);
List<Map<String,Object>> batch = client.fetchCursor(cursorId);
client.closeCursor(cursorId);
```

---

## Storage Backends

DieselDB поддерживает 5 storage backends. Выбор — **глобальная настройка** через `diesel.storage.type` system property или `storage.type` в `config.properties`:

| Backend | Описание | Compression codecs |
|---|---|---|
| `in_memory` (default) | In-memory, List<Map>. Lost on JVM exit. | — |
| `csv` | CSV files + `.table` metadata. Per-table `.csv` + index files. | `none`, `zstd`, `lz4`, `snappy` |
| `tsv` | Tab-Separated Values, same structure as CSV. | Same as CSV |
| `jsonl` | Streaming JSONL with block manager + delta compaction + parallel loader (1310 LoC `JsonlRowStorage` + 1161 LoC `JsonlRowReader` + 1011 LoC `JsonlIndexManager` + 721 LoC `JsonlSchemaManager`). | Same + library-neutral JSON (Jackson/Gson adapters) |
| `avro` | AVRO storage with full ecosystem: 57 classes including `AvroPrimaryKeyIndex`, `AvroBackupManager`, `AvroBloomFilter`, `AvroIntegrityChecker`, `SchemaEvolutionManager`, `SchemaConflictResolver`, `SchemaCompatibilityChecker`. | `BZip2`, `Deflate`, `Snappy optimized`, `ZStandard` |

```properties
# config.properties
storage.type = jsonl
jsonl.compression.codec = zstd
jsonl.compression.level = 3
compression.threshold.bytes = 4096
```

### Page-based storage layer

For all backends, page-based storage is available via `diesel.storage.page`:

- `Page` (8K / 16K / 64K configurable)
- `PageHeader` (24/32-byte header + slot directory + free-slot stack for tombstone reuse)
- `SlottedPageLayout` (insert/delete/defrag)
- `BufferPool` (fixed-capacity, LRU eviction, pin/unpin)
- `LruEvictionPolicy` (O(1) eviction — stores only unpinned frames)
- `PageManager` (manages pages, owns BufferPool)
- `BufferPoolFlusher` (background flushing)
- `CheckpointManager` (background checkpoints)
- `CatalogTable` + `CatalogSchema` (system catalog in page 0, JSON-serialized)

---

## Transactions & MVCC

### Isolation levels

| Level | DieselDB semantics | Notes |
|---|---|---|
| `READ_UNCOMMITTED` | Dirty reads allowed | **Default** in `Database.java` |
| `READ_COMMITTED` | Snapshot isolation | 🚧 In `TupleVisibility`: semantics polishing planned (currently treated same as REPEATABLE_READ) |
| `REPEATABLE_READ` | Snapshot isolation | Implemented via `MvccReadContext` |
| `SERIALIZABLE` | Snapshot + SSI | SSI via `ConflictDetector`, semantics polishing planned |

### MVCC implementation

- `Transaction` — UUID + `snapshotTxid` + `snapshotCsn` + `modifiedTables` (CoW on first DML) + `UndoLog` (with spill-to-disk)
- `TxStatusTracker` — `ACTIVE`/`COMMITTED`/`ABORTED` + commit CSN (monotonic)
- `RowVersionMeta` — `xmin` (creator txid), `xmax` (deleter txid), `commandId`, `committedValues` (pre-image)
- `TupleVisibility` — MVCC visibility logic per isolation level
- `MvccReadContext` — ThreadLocal reader context (auto-commit reader / explicit tx / batch tx)
- `VacuumManager` — dead-row GC

### Concurrency primitives

- `LockManager` — 5 modes (`IS`, `IX`, `S`, `X`, `SIX`), `LockCompatibility` bit-mask checking, resources as strings (`"table:USERS"`, `"row:USERS#42"`)
- `DeadlockDetector` — daemon thread `"diesel-deadlock-detector"`, periodic wait-for graph scan, cycle detection, victim selection (least work loses)
- `WaitForGraph` — `Map<Long, Set<Long>> adjacency`, DFS cycle detection
- `LockTimeoutManager` — timeout-based abort
- `ConflictDetector` — SSI for SERIALIZABLE: read/write sets per tx, victim = writer committing last, throws `SerializationFailureException`
- `SavepointManager` + `NestedSavepointStack` — partial rollback
- `UndoLog` with spill-to-disk — `InsertUndo`, `UpdateUndo`, `DeleteUndo`

---

## Indexes

6 типов индексов (классы в корневом пакете `diesel`):

| Type | Class | Использование |
|---|---|---|
| **BTree** | `BTreeIndex` (820 LoC) | Secondary B-tree index, parallel scan via `ForkJoinPool` |
| **BTreeClustered** | `BTreeClusteredIndex` | Unique clustered B-tree over PK, physical row order follows key order |
| **CompositeBTree** | `CompositeBTreeIndex` | Multi-column B-tree, exact lookup + prefix lookup for leading columns |
| **CoveringBTree** | `CoveringBTreeIndex` | Index-only scans (if SELECT-columns ⊂ covering set) |
| **Hash** | `HashIndex` | `ConcurrentHashMap<Object, Set<Integer>>`, O(1) exact lookup, no range |
| **Unique** | `UniqueIndex` | `Map<Object, Integer>`, 1:1 mapping, duplicates throw `IllegalStateException` |
| **BloomFilter** | `BloomFilter` | Bloom filter optimization (used by `AvroBloomFilter` for storage) |

For AVRO storage: `AvroPrimaryKeyIndex` (860 LoC) + `AvroSecondaryIndexManager`.

---

## Online Schema Changes

DieselDB реализует полноценный online `ALTER TABLE` через 3-step roadmap (Prompts #37-39):

### Algorithms

```sql
-- DEFAULT: metadata-only, instant (for ADD COLUMN)
ALTER TABLE users ADD COLUMN email STRING ALGORITHM=DEFAULT;

-- COPY: background rewrite + atomic swap (for any structural change)
ALTER TABLE users ADD COLUMN age INTEGER ALGORITHM=COPY;

-- INPLACE: metadata-only for ADD, tombstone for DROP
ALTER TABLE users DROP COLUMN age ALGORITHM=INPLACE;
```

### Lock modes

```sql
-- NONE: no extra blocking (default)
ALTER TABLE users ADD COLUMN x STRING LOCK=NONE;

-- SHARED: writers blocked during ALTER window
ALTER TABLE users ADD COLUMN x STRING LOCK=SHARED;

-- EXCLUSIVE: readers + writers blocked during ALTER window
ALTER TABLE users ADD COLUMN x STRING LOCK=EXCLUSIVE;
```

### Implementation classes

| Class | Роль |
|---|---|
| `diesel.schema.AlterAlgorithm` enum | `DEFAULT`, `COPY`, `INPLACE` |
| `diesel.schema.AlterLockMode` enum | `NONE`, `SHARED`, `EXCLUSIVE` |
| `diesel.AlterTableCopyAlgorithm` (Runnable) | Background copy process: Start → Background copy → Finalize → Rollback |
| `diesel.DualWriteCoordinator` | Write journal for online ALTER COPY: `RECORDING → SEALED → RECORDING → SEALED → DONE/ROLLED_BACK` |
| `diesel.AlterTableAddColumnQuery`, `AlterTableDropColumnQuery`, `AlterStatusQuery` | Query-objects |
| `diesel.schema.SchemaVersionRegistry` | Bounded history + pinned versions, timeout-based killing |
| `diesel.schema.SchemaVersion` | Immutable point-in-time snapshot (columns + types + description) |
| `diesel.schema.PinnedSchema` | `AutoCloseable` pin handle |
| `Database.cancelAlter(String tableName)` | Cancel running COPY (rollback if before finalize) |

### Monitoring

```sql
ALTER STATUS;
-- Returns: TABLE_NAME, STATE, PROGRESS_PERCENT, ROWS_COPIED/ROWS_TOTAL, BARRIER_WAIT_MS, MESSAGE
```

---

## WAL & ARIES Recovery

### WAL subsystem (`diesel.wal`, 19 классов)

- `WALManager` (899 LoC) — segment files `wal-NNNN.log` (4-digit zero-padded, 1-based), monotonic LSN allocator + checkpoint persistence
- `WALSegment` (741 LoC) — single segment, header 24 bytes (v1) / 32 bytes (v2) + entries, CRC32C checksums
- `WALWriter` — single-writer thread with bounded queue + backpressure
- `AsyncWALWriter` — policy-aware with `FsyncPolicy` enum: `ALWAYS`, `GROUP`, `EVERYSEC`, `NONE`
- `GroupCommitCoordinator` — group commits up to 5ms timeout or 64 group size (default)
- `WALArchiver` — local archive of old segments
- `WALSegmentRotator`, `WALQueue`, `WALWriteRequest`, `WALEntry`, `WALFormat`, `WALConfig`

### WAL opcodes (`WALOpcode` enum)

```
BEGIN(0), INSERT(1), UPDATE(2), DELETE(3), COMMIT(4), ABORT(5),
TRUNCATE(6), CHECKPOINT(7), PAGE_IMAGE(8), SAVEPOINT(9), RELEASE_SAVEPOINT(10)
```

> 🚧 `TRUNCATE` WAL opcode exists, but SQL `TRUNCATE TABLE` statement is planned for addition.

### ARIES Recovery (`diesel.recovery`, 20 классов)

Full ARIES implementation with three phases in canonical order:

1. **AnalysisPhase** — scans WAL from last checkpoint to end-of-log, rebuilds committed + active transaction sets
2. **RedoPhase** — replays each WAL record with `checkpoint.lastLSN < lsn <= lastLSN`. PAGE_IMAGE records (WALOpcode=8) restore after-image on page. COMMIT records delivered to `MvccRedoSink`.
3. **UndoPhase** — reverses logical DML records of each still-active transaction (no COMMIT seen). Scans WAL segments in **reverse LSN order** (newest first).

| Class | Role |
|---|---|
| `diesel.recovery.ARIESAlgorithm` (public final) | Orchestrator: `static RecoveryResult recover(WALManager, PageManager, MvccRedoSink, MvccUndoSink)` |
| `diesel.recovery.AnalysisPhase` | Phase 1 |
| `diesel.recovery.RedoPhase` | Phase 2 |
| `diesel.recovery.UndoPhase` | Phase 3 (reverse LSN order) |
| `diesel.recovery.MvccRedoSink` + `MvccUndoSink` (interfaces) | MVCC integration (impl: `diesel.DatabaseRecoverySink`) |
| `diesel.recovery.CheckpointRecord` + `CheckpointPointerFile` | Checkpoint persistence (separate pointer file) |
| `diesel.recovery.SharpCheckpoint` + `FuzzyCheckpoint` + `CheckpointStrategy` + `CheckpointMode` | Checkpoint modes — quiescent (sharp) vs. concurrent (fuzzy), R3-014 |
| `diesel.recovery.PointInTimeRecovery` + `PitrManager` + `PitrResult` + `PitrSink` | PITR — commit-timestamped WAL replay to RECOVERY_TARGET_TIME (Prompt4 #43) |
| `diesel.recovery.RecoveryResult`, `AnalysisResult`, `RedoResult`, `UndoResult` | Immutable result objects |

Entry point: `diesel.RecoveryManager` (public final, JMX MBean `diesel:type=RecoveryManager`). Запускается из `DatabaseServer.start()` строго перед принятием клиентских соединений. `Database.runRecovery()` — публичный wrapper.

### Test coverage (25+ тест-классов)

`RecoveryIntegrationTest`, `RecoveryManagerTest`, `RecoveryWithLongTransactionTest`, `RecoveryPerformanceTest`, `RecoverySinglePassIOTest`, `AnalysisTest`, `AnalysisPerformanceTest`, `AnalysisCheckpointIsLinearTest`, `RedoTest`, `RedoPerformanceTest`, `UndoPhaseTest`, `UndoPhaseDescendingIterationTest`, `IdempotencyTest`, `CheckpointIntegrationTest`, `CheckpointPointerFileTest`, `CheckpointRecordTest`, `WALCrashRecoveryTest`, `WALManagerTest`, `WALWriterTest`, `WALWriterThroughputTest`, `WALBackpressureTest`, `WALEntryTest`, `WALFindSegmentIsLogNTest`, `WALReadByLsnIsOLogNTest`, `WALReadAllIsAlreadySortedTest`, `WALReadRangeOnlyRelevantSegmentsTest`, `WALReadRangeSkipNonOverlappingTest`, `WALReopenIndexAndLastLsnTest`, `SegmentRotationTest`, `AsyncWALWriterTest`, `GroupCommitCoordinatorTest`, `GroupCommitThroughputTest`, `DmlPayloadTest`, `RecoveryCacheLRUEvictionTest`.

---

## Network Protocol

### Binary protocol v2 (`diesel.net.WireProtocol`)

- Magic bytes: `"DSEL"` (ASCII)
- Version: 2
- `COMPRESSED_FLAG` for compressed frames
- Frame: `magic + header + payload + CRC32 footer`
- Unknown magic byte → `InvalidMessageException`
- Wrong version → reject

### 21 message types (`diesel/net/MessageType.java`)

```
QUERY(1), QUERY_RESULT(2),
PREPARE(3), EXECUTE_PREPARED(4), BATCH_QUERY(5), CLOSE_PREPARED(6),
OPEN_CURSOR(7), FETCH_CURSOR(8), CLOSE_CURSOR(9),
HEALTH_CHECK(10), HEALTH_CHECK_RESPONSE(11),
COMPRESSION_HANDSHAKE(12), COMPRESSION_HANDSHAKE_RESPONSE(13),
ERROR(14),
PREPARE_RESPONSE(15), CLOSE_PREPARED_RESPONSE(16),
OPEN_CURSOR_RESPONSE(17), FETCH_CURSOR_RESPONSE(18), CLOSE_CURSOR_RESPONSE(19),
AUTH(20), AUTH_RESPONSE(21)
```

37 message+codec классов в `diesel/net/messages/` (по паре Message+Codec на каждый тип).

### TLS hardening

| Class | Role |
|---|---|
| `diesel.net.TlsHandshakeHandler` | Per-connection TLS wrapping (lazy handshake — after accept, on first read in worker thread) |
| `diesel.net.SslContextFactory` | Keystore loading (PKCS12 / JKS) |
| `diesel.net.MutualTlsHandler` | Client-cert policy: `none` / `request` / `require` via `tls.client.auth` |
| `diesel.net.SniSelector` | SNI hostname → tenant keystore mapping (via `X509ExtendedKeyManager`) |
| `diesel.net.CipherSuiteWhitelist` | Allow-list: TLS 1.3 suites + ECDHE/DHE AES-GCM + CHACHA20-POLY1305. Disabled: SSLv3, TLS 1.0/1.1, static RSA/ECDH, CBC, RC4, 3DES, NULL/EXPORT |

### Wire compression

`diesel.net.CompressionHandshakeHandler` + messages `CompressionHandshakeMessage` / `CompressionHandshakeResponseMessage`:
- Algorithms: `ZSTD`, `GZIP` (client preference: ZSTD first, level 6, threshold 4096 bytes)

### Legacy protocol v1 (`diesel.LegacyProtocolAdapter`)

> 🚧 **DEPRECATED — scheduled for removal in Phase 2**

- Plain Java Object Serialization over socket
- Default port: **5440** (configurable via `legacy.port`, `0` = disabled)
- Frame format: `[marker: 0x00|0x01][length: i32][bytes]` (0x01 = GZIP-compressed Java-serialized objects)
- Disable in production: `legacy.port = 0`

### libpq protocol (`diesel.net.libpq` + `diesel.LibpqServer`)

Реализован на уровне v3 wire protocol (PostgreSQL-совместимый), позволяет подключаться любым стандартным PostgreSQL-клиентом (`psql`, JDBC, psycopg2, pg, lib/pq, tokio-postgres). Prompt4 #54 + #55.

| Class | Role |
|---|---|
| `diesel.LibpqServer` | TCP listener (default port `5432`), `libpq.port = 0` disables it |
| `diesel.net.libpq.LibpqProtocol` | v3 wire framing: `Q` (SimpleQuery), `P`/`B` (Parse + Bind), `E` (Execute), `X` (Terminate), `S` (Sync), `T`/`D` (RowDescription + DataRow), `Z` (ReadyForQuery) |
| `diesel.net.libpq.LibpqMessage` + `LibpqMessageDecoder` | Message codecs (frontend + backend messages) |
| `diesel.net.libpq.ScramSha256` | SCRAM-SHA-256 authentication (RFC 7677): client-first → server-first (nonce + salt) → client-final → server-final |

```properties
# config.properties
libpq.port = 5432                 # 0 = disabled
libpq.auth.method = scram-sha-256 # only secure method supported
```

Supported psql meta-commands (Prompt4 #54): `\d`, `\dt`, `\l`, `\du`, `\dn`, `\di`, `\df`, `\dx`, `\conninfo`, `\q`. SELECT projection columns resolved server-side (Prompt4 #55 fix).

---

## Replication & HA

DieselDB реализует PostgreSQL-style replication stack (`diesel.replication`, 15 классов). Prompt4 #58 (physical streaming), #43 (PITR), R3-016.

### Roles

```properties
# Master (primary)
replication.role = master
replication.port = 5433           # PostgreSQL standby convention

# Standby (replica)
replication.role = standby
replication.upstream.host = primary.example.com
replication.upstream.port = 5433
```

### Physical streaming replication

- `WalSender` (master) — streams WAL records to subscribed standbys over `replication.port` (5433)
- `WalReceiver` (standby) — bootstraps from upstream master, applies WAL in read-only mode
- `WalSource` + `WalApplier` — WAL plumbing on the standby side
- `HotStandbyHandler` — read-only query dispatch on standby during catch-up

### Replication slots

- `ReplicationSlot` + `ReplicationSlotRegistry` — durable, named WAL retention per consumer (master does not drop WAL until standby ACKs)
- `ReplicationWire` — wire framing for slot create/drop/advance
- Survives master restart: slots persisted to disk

```sql
-- Logical replication (CDC) via Publication/Subscription
CREATE PUBLICATION my_pub FOR TABLE users, orders;
CREATE SUBSCRIPTION my_sub TO PUBLICATION my_pub
  CONNECTION 'localhost:5432' WITH (conflict_resolution = replace);
```

### Synchronous replication

- `replication.role = standby` + `WITH (sync = true)` — master waits for sync standby ACK before COMMIT returns
- 3 ACK modes: `off` (async), `request` (no wait), `require` (block on ack)
- `StreamingApplier` + `ReplicationApplyPipeline` — ordered apply on standby

### Logical replication (CDC)

- `Publication` (collection of tables + filter + ConflictResolutionStrategy)
- `Subscription` + `LogicalDecoder` — emit `LogicalChangeRecord` per row change
- `ReplicationRegistry` — top-level coordinator

### Point-in-time recovery (PITR)

- `PointInTimeRecovery` + `PitrManager` + `PitrSink` + `PitrResult` (in `diesel.recovery`)
- Recovery by `RECOVERY_TARGET_TIME` — replays commit-timestamped WAL until target timestamp
- Prompt4 #43

### Standalone physical streaming tool

`drivers/jdbc/diesel-jdbc/src/test/java/diesel/LibpqTestServer.java` provides a minimal harness for end-to-end replication testing.

---

## Security: RBAC, TLS, Audit

### RBAC (`diesel.security`, 23 класса)

| Entity | Class | Description |
|---|---|---|
| Privilege | `Privilege` (immutable) | UUID id + roleName + `Action` enum + tableName (empty = database-wide) |
| Action enum | `Action` | 8 actions: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `ADMIN` |
| User | `User` | User entity |
| Role | `Role` | Role entity |
| User storage | `UserStore` + `UserStoreBackend` | User storage with pluggable backend |
| Tenant storage | `TenantStore` + `TenantStoreBackend` | Multi-tenant (SNI-based) |
| Privilege cache | `PrivilegeCache` | Per-session cache |
| Exceptions | `RoleExistsException`, `RoleNotFoundException`, `UserExistsException`, `UserNotFoundException`, `AccessDeniedException`, `AuthException` | RBAC exceptions |

### Auth

| Class | Role |
|---|---|
| `Authenticator` (public final) | Verifies credentials via `PasswordHasher` (constant-time), opens `Session` with `SessionToken` (UUID) |
| `PasswordHasher` | Constant-time password verification |
| `Session` (public final) | userId, userName, token, mustChangePassword flag, sourceIp (default `"embedded"`) |
| `SessionToken` | UUID-based token |

Sessions stored in `ConcurrentHashMap<UUID, Session>`.

Wire messages: `AuthMessage` (type 20), `AuthResponseMessage` (type 21).

### Audit log

| Class | Role |
|---|---|
| `AuditLogger` (public final, async) | Bounded queue + single daemon thread. Producers hand records via non-blocking `offer`; full queue drops record + increments `droppedCount` (main query never blocks). Retention: rows older than `audit.retention.days` (default 90) archived to gzip cold-archive, then deleted |
| `AuditRecord` (immutable) | id, eventTime, userName, sourceIp, query, status (OK/ERROR), durationMs |
| `AuditBackend` (interface) | Pluggable backend |
| `AvroAuditLogger` (in `diesel.storage.avro`) | Structured audit for AVRO storage operations (pipe-delimited, AuditLevel, AuditCategory) |

Config: `audit.enabled` (default **false** — dev workloads pay nothing), `audit.retention.days` (90), `audit.queue.capacity` (10000), `audit.retention.interval.ms` (86400000), `audit.archive.dir` (`data/audit-archive`).

> `DIESEL_AUDIT_LOG` protected from DROP (guard in `Database.dropTable` — all `diesel_*` system tables are protected).

### TLS

See [Network Protocol](#network-protocol). All production deployments should set:

```properties
tls.enabled = true
tls.keystore.path = /path/to/keystore.p12
tls.keystore.password = changeit
tls.keystore.type = PKCS12
tls.client.auth = require
tls.ca.path = /path/to/truststore.p12
```

Generate test keystore:
```bash
keytool -genkeypair -alias dieseldb -keyalg RSA -keysize 2048 \
  -storetype PKCS12 -keystore data/diesel-keystore.p12 -storepass changeit \
  -dname "CN=localhost" -ext "SAN=dns:localhost,ip:127.0.0.1"
```

---

## Backup & Restore

### `diesel.backup` (13 классов)

| Class | Status | Description |
|---|---|---|
| `DieselBackup` + `DieselRestore` | ✅ | CLI tools (`main(String[] args)`), version `1.0.0`, commands: `backup`, `restore`, `--help`, `--version` |
| `PhysicalBackup` | ✅ Implemented | Triggers checkpoint, copies data pages (`.table`, `.csv`, `.tsv`, `.jsonl`, `.avro`), copies WAL segments (`.log`, `.wal`), creates `backup_manifest.json` with checksums. 🚧 `triggerCheckpoint()` is currently simplified (placeholder LSN) — full integration planned |
| `PhysicalRestore` | ✅ Implemented | Copies backup files to target dir, replays WAL segments, validates. Methods: `restore()`, `displayBackupInfo()`, `isTargetSafe()`, `static validateBackupDirectory(Path)` |
| `IncrementalBackup` | ✅ Implemented (Prompt4 #42) | Page-level CRC delta of `pages.db` + changed-file delta with parent-linked manifest + cumulative `backup_state.idx` chain |
| `BackupScheduler` | ✅ Implemented | 5-field cron parser (`backup.schedule`, default `0 2 * * *` = daily 02:00), `backup.enabled = true` to enable. Run-via-operator pattern — scheduler does not start by itself |
| `BackupManifest` | ✅ Implemented | JSON-serialized via Jackson `ObjectMapper`. `BackupInfo` + `List<FileEntry>` + `Map<String,String> metadata`. Has checksums for integrity verification |
| `DumpFormat` enum | ✅ | `SQL`, `CSV`, `JSONL`, `AVRO` |
| `DumpTool` + `RestoreTool` | 🚧 In development | CLI tools using `LogicalDump`/`LogicalRestore` (currently stubs) |
| `LogicalDump` | 🚧 In development | Currently writes only placeholder header — full implementation planned |
| `LogicalRestore` | 🚧 In development | Currently does basic file reading — full implementation planned |

### PITR (Point-in-Time Recovery)

`PitrManager` (in `diesel.recovery`, Prompt4 #43) — commit-timestamped WAL replay to `RECOVERY_TARGET_TIME`. Exposed via SQL `RECOVERY_TARGET_TIME = '2026-10-30 14:30:00'`. See [§ Replication & HA → PITR](#point-in-time-recovery-pitr).

### Avro-specific backup

For AVRO storage there's a separate full-featured backup infrastructure (`diesel.storage.avro`):

- `AvroBackupManager` (public final, 647 LoC):
  - `backupFull(File)` — full backup
  - `backupIncremental(File)` — incremental backup
  - `pruneOldBackups()` — retention policy
  - `readLatestManifest()` — metadata
- `AvroRestoreManager` — restore from backup

### CLI usage

```bash
# Physical backup
java -jar diesel-backup.jar backup --type physical --output /backup/path

# Physical restore
java -jar diesel-backup.jar restore --input /backup/path --target /data/dir

# Or via CliRepl
diesel> BACKUP /path/to/backup
diesel> PHYSICAL-RESTORE /path/to/backup
diesel> DUMP /path/to/dump.sql    # 🚧 in development (currently writes only header)
```

---

## Observability & Health

DieselDB включает полноценную observability-стек на основе `diesel/observability` (11 классов). Prompt4 #44 + #46.

### MetricsRegistry (`diesel.observability.MetricsRegistry`)

- `Counter` (lock-free `LongAdder`)
- `Gauge` (supplier-backed)
- `Histogram` — bucketed latency (1µs-100s), `p50`/`p95`/`p99` percentiles
- Wired into the `Database` query path (parse/plan/execute/sort/total), BufferPool, WAL writer, recovery

### Prometheus exporter (`diesel.observability.PrometheusExporter` + `MetricsHttpServer`)

```properties
metrics.enabled = true
metrics.port = 9090
metrics.host = 0.0.0.0
```

- JDK `httpserver`, non-fatal bind (does not crash the JVM if port busy)
- Endpoint: `/metrics` (text exposition: `# HELP`/`# TYPE`, cumulative histogram buckets `+Inf`, label escaping)
- Optional Avro-specific metrics: `avro.metrics.enabled`, `avro.metrics.prometheus.enabled`, `avro.metrics.prometheus.path = /metrics`

### Health endpoint (`diesel.observability.HealthCheck`)

- `GET /health` — combined JSON (`HealthJson`) of liveness + readiness
- `GET /health/live` — `LivenessCheck`: dead WAL writer, OOM-imminent
- `GET /health/ready` — `ReadinessCheck`: recovery in progress, cold buffer pool

Returns `200 OK` if healthy, `503 Service Unavailable` if not. Designed for K8s liveness/readiness probes — see `k8s/dieseldb-probes.yaml`.

### Existing JMX

7 MBeans remain available: `diesel:type=BufferPool`, `BufferPoolFlusher`, `CheckpointManager`, `RecoveryManager`, `QueryProfiler`, `WALWriter`, `AsyncWALWriter`, `GroupCommitCoordinator`.

---

## Configuration

Main config: `config.properties` in working directory. All keys can be overridden via system properties (`-Dkey=value`).

### Storage

```properties
storage.type = in_memory              # in_memory | csv | tsv | jsonl | avro
csv.compression.codec = zstd         # none | zstd | lz4 | snappy
tsv.compression.codec = zstd
jsonl.compression.codec = zstd
jsonl.compression.level = 3         # 1-22 (ZSTD), ignored for lz4/snappy
jsonl.type.coercion = LENIENT        # STRICT | LENIENT
compression.threshold.bytes = 4096
```

### Memory limits

```properties
max.result.rows = 1000000            # max rows per query result (use /* MAX_ROWS=N */ hint to override)
max.inmemory.rows = 10000            # streaming/spill threshold
max.hash_table.size.mb = 512         # hash-join budget (exceeds → partition + spill)
```

### Transactions

```properties
transaction.isolation.level = SERIALIZABLE  # default isolation level
                                          # Note: defaultIsolationLevel = READ_UNCOMMITTED in Database.java
                                          # Override via config or BEGIN ... ISOLATION LEVEL ...
```

### Server

```properties
server.pool.size = 100               # thread pool size
server.queue.capacity = 100          # work queue capacity
server.backlog = 512                 # TCP backlog
server.socket.timeout = 30000       # idle client timeout (ms)
legacy.port = 5440                  # legacy v1 protocol port (0 = disabled in production)
libpq.port = 5432                   # libpq wire protocol (0 = disabled)
libpq.auth.method = scram-sha-256   # only secure method supported

# Replication (Prompt4 #58, R3-016)
replication.role = off              # off | master | standby
replication.port = 5433             # master: WalSender listens here
replication.upstream.host = localhost  # standby: upstream master host
replication.upstream.port = 5433    # standby: upstream master replication port

# Backup scheduler (Prompt4 #42)
backup.enabled = false              # set true to start the BackupScheduler
backup.schedule = 0 2 * * *         # 5-field cron (default daily 02:00)

# Metrics & health (Prompt4 #44 + #46)
metrics.enabled = true
metrics.port = 9090
metrics.host = 0.0.0.0

# String-literal case-insensitivity (R3-013)
compat.uppercase.string.literals = false
```

### WAL

```properties
wal.enabled = true                   # enable WAL (disables recovery too)
wal.fsync.policy = GROUP             # ALWAYS | GROUP | EVERYSEC | NONE
wal.group.commit.timeout.ms = 5
wal.group.commit.size = 64
```

### Security

```properties
security.auth.required = true        # require auth on all connections
security.auth.bootstrap-admin = true # bootstrap admin user on first start

audit.enabled = false                 # 🚧 enable for production (compliance)
audit.retention.days = 90
audit.queue.capacity = 10000
audit.retention.interval.ms = 86400000
audit.archive.dir = data/audit-archive

tls.enabled = false                   # 🚧 enable for production
tls.keystore.path = data/diesel-keystore.p12
tls.keystore.password = changeit
tls.keystore.type = PKCS12
tls.client.auth = none                # none | request | require
tls.ca.path =                        # server truststore for mTLS
tls.truststore.type = AUTO

# SNI multi-tenant (per-hostname keystore)
# tls.sni.<hostname>.keystore.path = ...
# tls.sni.<hostname>.keystore.password = ...
# tls.sni.<hostname>.keystore.type = PKCS12
```

### Schema versioning

```properties
schema.old.version.timeout.min = 10  # old schema version lifetime (minutes)
```

### Online ALTER

```properties
alter.copy.batch.size = 2048         # batch size for background copy
alter.copy.batch.sleep.ms = 0        # sleep between batches (test pacing)
alter.copy.gate.timeout.ms = 10000   # barrier timeout (writers/readers block window)
alter.inplace.hold.ms = 0            # test hook: hold INPLACE gate
```

### Profiling

```properties
diesel.profile.slow.threshold.ms = 1000  # slow query threshold (0 = disable)
```

---

## Build & Test

### Build requirements

- JDK 21+ (LTS)
- Maven 3.9+

### Build commands

```bash
mvn clean                # clean target/
mvn compile              # compile main code
mvn test                 # run default test group (smoke, query, index)
mvn -Ptest test          # run full test suite (all test classes)
mvn package              # build JAR
mvn install              # install to local Maven repository
```

### Test profiles (16 Surefire profiles)

| Profile | Memory | Threads | Groups |
|---|---|---|---|
| `fast` (default) | 512m | 4 | smoke, query, index |
| `core` | 512m | 4 | query-full, storage |
| `concurrency` | 1g | 1 | concurrency |
| `network` | 1g | 1 | network |
| `perf` | 2g | 1 | perf |
| `large` | 4g | 1 | large |
| `all` | 1g | 2 | smoke, query, index, query-full, storage, concurrency, network |
| `flaky` | — | — | flaky |
| `flaky-jsonl` | — | — | flaky-jsonl |
| `timing-quick` | — | — | timing-quick |
| `test` | 512m | 4 | all tests |
| `ci` | 1g | 2 | CI groups |
| `storage-csv` | 1g | 2 | storage-csv |
| `storage-tsv` | 1g | 2 | storage-tsv |
| `storage-jsonl` | 1g | 2 | storage-jsonl |
| `storage-avro` | 1g | 2 | storage-avro |

Default `excludedGroups = large,flaky`.

Examples:

```bash
mvn -Pcore test                  # core tests only
mvn -Pconcurrency test          # concurrency tests
mvn -Plarge -Dtest=RecoveryIntegrationTest test  # single large test
mvn -Pstorage-jsonl test        # JSONL storage tests
```

### Test coverage

368 test-classes covering:
- AVRO storage: 33 classes (AvroBackupManager, AvroBatchOperator, AvroBlockManager, AvroBloomFilter, AvroCompression, AvroDataFileReader, AvroIntegrityChecker, AvroPrimaryKeyIndex, AvroQueryExecutor, AvroRecoveryManager, AvroRestoreManager, AvroRowStorage, AvroSchemaManager, AvroTransactionManager, ...)
- JSONL storage: 18 classes (JsonlAppendMode, JsonlBlock, JsonlCompression, JsonlIndex, JsonlLazyLoad, JsonlNested, JsonlNullSemantics, JsonlParallelLoad, JsonlProjection, JsonlSchema, ...)
- WAL: 5+ classes (WALManager, WALWriter, WALWriterThroughput, WALBackpressure, WALEntry, WALFindSegmentIsLogN, WALReadByLsnIsOLogN, ...)
- Recovery (ARIES + PITR): 13 classes (RecoveryIntegration, RecoveryManager, RecoveryPerformance, AnalysisTest, RedoTest, UndoPhaseTest, IdempotencyTest, PointInTimeRecoveryTest, ...)
- Lock/Deadlock: 5+ classes (LockCompatibility, LockManager, LockRelease, LockTimeout, ChainDeadlock, DeadlockOverhead, WaitForGraph)
- MVCC: 2+ classes (MvccDelete, MvccUpdate, CompactWithLargeRowVersions, TupleVisibility, TxStatusTracker, IsolationSemantics, CopyOnWriteIsolation, SerializationConflict, ConcurrentConflict, ConflictDetectorCartesianBenchmark)
- Alter: 4+ classes (AlterDualWrite, AlterLock, AlterProgress, AlterRollback, OnlineAlter, CopyAlgorithm, InplaceTest)
- Audit: 4 classes (Audit, AuditAsync, AuditRetention, AvroAuditLogger)
- Page: 5 classes (Page, PageManager, PageManagerCrash, PageManagerThroughput, EvictionUnderPinnedHeadTest)
- Buffer: 4+ classes (BufferPool, BufferPoolFlusher, BufferPoolHitRate, BufferPoolStress, DirtyPageTrackingIsODTest, CatalogLookupIsO1Test)
- Replication: 11 classes (WalSender, WalReceiver, ReplicationSlot, ReplicationSlotRegistry, HotStandbyHandler, SyncReplicationTest, LogicalDecoderTest, StreamingApplierTest, ...)
- Observability: 8 classes (MetricsRegistry, PrometheusExporter, MetricsHttpServer, HealthCheck, LivenessCheck, ReadinessCheck, ...)
- Backup: 4 classes (PhysicalBackupRestore, IncrementalBackup, BackupScheduler, PitrManager)
- Other: ~150 classes (cursor, prepared statement, batch, explain, subquery, join, in, like, limit/offset, group by, insert, update, delete, vacuum, schema, sni, mtls, rbac, regex, redo, undo, savepoint, libpq handshake, SCRAM auth, ...)

---

## Project Structure

```
dieseldb/
├── diesel/                          # Main source root
│   ├── Database.java                # 3545 LoC — engine entry point
│   ├── DieselDatabase.java          # public facade
│   ├── DatabaseServer.java          # 1181 LoC — TCP server (port 3306)
│   ├── DatabaseClient.java          # 682 LoC — binary v2 client
│   ├── CliRepl.java                 # 853 LoC — interactive REPL
│   ├── QueryParser.java             # 4389 LoC — SQL parser (25+ strategies)
│   ├── SqlLexer.java                # tokenizer
│   ├── SqlKeywords.java             # keyword set
│   ├── SelectQuery.java             # 4557 LoC — SELECT executor
│   ├── SubqueryParser.java          # 1850 LoC — subquery parser
│   ├── QueryOptimizer.java          # adaptive optimizer
│   ├── QueryCache.java              # AST cache
│   ├── QueryProfiler.java           # JMX MBean, slow log
│   ├── AggregateFunctions.java      # SIMD-vectorized
│   ├── Transaction.java             # MVCC + UndoLog
│   ├── TxStatusTracker.java         # ACTIVE/COMMITTED/ABORTED
│   ├── TupleVisibility.java         # MVCC visibility per isolation
│   ├── RowVersionMeta.java          # xmin/xmax/commandId
│   ├── BTreeIndex.java              # 820 LoC
│   ├── BTreeClusteredIndex.java
│   ├── CompositeBTreeIndex.java
│   ├── CoveringBTreeIndex.java
│   ├── HashIndex.java
│   ├── UniqueIndex.java
│   ├── BloomFilter.java
│   ├── AlterTableCopyAlgorithm.java # online ALTER
│   ├── DualWriteCoordinator.java    # write journal
│   ├── LegacyProtocolAdapter.java   # deprecated port 5440
│   ├── LibpqServer.java             # libpq v3 wire protocol (port 5432)
│   ├── CreateSubscriptionQuery.java # CREATE SUBSCRIPTION (logical CDC)
│   ├── RecoveryManager.java         # ARIES entry point
│   ├── DatabaseRecoverySink.java    # MvccRedoSink + MvccUndoSink impl
│   ├── VacuumManager.java           # dead-row GC
│   ├── storage/                     # 34 files
│   │   ├── RowStorage.java          # interface
│   │   ├── StorageFactory.java      # switch by type
│   │   ├── StorageType.java         # enum: CSV/TSV/JSONL/AVRO/IN_MEMORY
│   │   ├── InMemoryRowStorage.java
│   │   ├── CsvRowStorage.java + CsvIndexManager + CsvRowReader + CsvRowWriter + DelimitedByteParser
│   │   ├── TsvRowStorage.java + TsvIndexManager
│   │   ├── JsonlRowStorage.java     # 1310 LoC
│   │   ├── JsonlRowReader.java      # 1161 LoC
│   │   ├── JsonlIndexManager.java   # 1011 LoC
│   │   ├── JsonlSchemaManager.java  # 721 LoC
│   │   ├── AvroRowStorage.java      # 845 LoC
│   │   ├── CompressionFactory.java + CompressionCodec
│   │   ├── avro/                    # 58 files (AvroPrimaryKeyIndex 860 LoC, AvroBackupManager 647 LoC, ...)
│   │   ├── page/                    # 28 files (Page, BufferPool, PageManager, SlottedPageLayout, CatalogTable, PageScrubber, AdaptiveFlushStrategy, AtomicFileWriter, JMX MBeans)
│   │   ├── json/                    # 15 files (library-neutral streaming + Jackson/Gson adapters)
│   │   └── tablespace/             # 3 files (TablespaceRegistry in development)
│   ├── net/                         # 12 files (WireProtocol, MessageType, TLS handlers, compression)
│   │   ├── messages/                # 37 files (Message+Codec pairs)
│   │   └── libpq/                   # 4 files (LibpqProtocol, LibpqMessage, LibpqMessageDecoder, ScramSha256)
│   ├── security/                    # 23 files (RBAC, Auth, Audit, Tenant)
│   ├── wal/                         # 19 files (WALManager, WALSegment, AsyncWALWriter, GroupCommitCoordinator, WALArchiver)
│   ├── recovery/                    # 20 files (ARIES + SharpCheckpoint/FuzzyCheckpoint + CheckpointStrategy + PointInTimeRecovery + PitrManager)
│   ├── concurrency/                 # 13 files (LockManager, DeadlockDetector, WaitForGraph, ConflictDetector, SavepointManager)
│   ├── replication/                 # 15 files (WalSender, WalReceiver, ReplicationSlot, HotStandbyHandler, LogicalDecoder, Publication, Subscription, StreamingApplier)
│   ├── observability/               # 11 files (MetricsRegistry, Counter, Gauge, Histogram, PrometheusExporter, MetricsHttpServer, HealthCheck, LivenessCheck, ReadinessCheck, HealthJson, HealthResult)
│   ├── checksum/                    # 3 files (CRC32C, ChecksummedPage, PageValidator)
│   ├── backup/                      # 13 files (DieselBackup, PhysicalBackup, IncrementalBackup, BackupScheduler, LogicalDump (in dev), BackupManifest, PitrManager)
│   └── schema/                      # 5 files (SchemaVersion, SchemaVersionRegistry, AlterAlgorithm, AlterLockMode)
├── drivers/                         # external driver modules (separate Maven/Cargo/npm/PyPI packages)
│   ├── jdbc/diesel-jdbc/            # JDBC driver (Hibernate-compatible DatabaseMetaData, libpq wire client)
│   ├── python/diesel-python/        # psycopg2-compatible (asyncio + sync)
│   ├── nodejs/diesel-node/           # pg-compatible (Promise/async)
│   ├── go/diesel-go/                 # database/sql, lib/pq-style
│   └── rust/diesel-rs/               # tokio-postgres-style
├── k8s/
│   └── dieseldb-probes.yaml         # K8s liveness/readiness probes (/health/live, /health/ready)
├── src/
│   ├── main/
│   │   ├── avro/                    # AVRO schema source dir (output → diesel/storage/avro/generated)
│   │   └── resources/
│   │       └── logback.xml
│   └── test/java/diesel/            # 368 test-classes
│       ├── (root): 292 files
│       ├── recovery/: 13 files
│       ├── wal/: 9 files
│       ├── security/: 7 files
│       ├── storage/page/: 5 files
│       ├── net/: 2 files
│       ├── storage/tablespace/: 1 file
│       ├── net/messages/: 1 file
│       ├── replication/: 11 files
│       ├── observability/: 8 files
│       └── backup/: 4 files (PhysicalBackupRestore, IncrementalBackup, BackupScheduler, PitrManager)
├── pom.xml                          # Maven build config (Java 21)
├── config.properties                # default config
├── Makefile                         # make wrappers
├── start-server.sh / .bat           # server launch scripts
├── start-client.sh / .bat           # client launch scripts
├── KNOWN_LIMITATIONS.md             # known limitations and workarounds
├── Changelog.md                     # changelog
├── AGENTS.md / AGENTS2.md / AGENTS3.md  # AI-agent process docs
├── PERSISTENCE_README.md            # persistence layer docs
├── PROMPT_STATUS.md                 # AI prompt status
├── PHASE0_CHANGES.md               # Phase 0 changes
├── Roadmap_Parquet_Stage1.md        # Parquet migration roadmap
├── benchmark_report.md              # benchmark results
└── examples/
    └── avro-examples.sql            # examples (some planned SQL features not yet parsed)
```

---

## Development Status

DieselDB is in active development toward v1.0.0 stable release. The core engine subsystems are implemented and tested, while several high-level features are in active development.

### ✅ Production-Implemented Subsystems

| Subsystem | Status | Test Coverage |
|---|---|---|
| **Page-based storage** (`diesel.storage.page`, 28 classes) | ✅ Implemented | PageTest, PageManagerTest, PageManagerCrashTest, PageManagerThroughputTest, BufferPoolTest, BufferPoolFlusherTest, BufferPoolHitRateTest, BufferPoolStressTest, DefragTest, DefragCompactsSlotDirectoryTest, DefragIsOLiveTest, EvictionUnderPinnedHeadTest, DirtyPageTrackingIsODTest, CatalogLookupIsO1Test |
| **WAL** (`diesel.wal`, 19 classes) | ✅ Implemented | 16+ test classes including throughput, backpressure, segment rotation, async writer, group commit, DML payload |
| **ARIES Recovery + PITR** (`diesel.recovery`, 20 classes) | ✅ Implemented | RecoveryIntegrationTest, RecoveryManagerTest, RecoveryWithLongTransactionTest, RecoveryPerformanceTest, AnalysisTest, RedoTest, UndoPhaseTest, IdempotencyTest, CheckpointIntegrationTest, WALCrashRecoveryTest, PointInTimeRecoveryTest, PitrManagerTest (+15 more) |
| **Concurrency** (`diesel.concurrency`, 13 classes) | ✅ Implemented | LockCompatibilityTest, LockManagerTest, LockReleaseTest, LockTimeoutTest, ChainDeadlockTest, DeadlockOverheadTest, WaitForGraphTest, SerializationConflictTest, ConcurrentConflictTest, ConflictDetectorCartesianBenchmarkTest |
| **Online ALTER** (`AlterTableCopyAlgorithm`, `DualWriteCoordinator`) | ✅ Implemented | AlterDualWriteTest, AlterLockTest, AlterProgressTest, AlterRollbackTest, OnlineAlterTest, CopyAlgorithmTest, InplaceTest |
| **Network protocol v2 + libpq** (`diesel.net` 12 + 37 + 4 libpq classes) | ✅ Implemented | TlsConnectionTest, MtlsTest, MtlsRequestTest, SniTest, SniSelectorTest, SelfSignedTest, ExpiredCertTest, CipherAuditTest, MutualTlsHandlerTest, SocketTimeoutTest, ServerConnectionLimitTest, GracefulShutdownTest, LibpqHandshakeTest, ScramSha256Test |
| **RBAC + Auth** (`diesel.security`, 23 classes) | ✅ Implemented | AuthTest, DefaultAdminTest, ResetAdminTest, UserStoreTest, RbacTest, RbacDdlTest, GrantTest, TenantDdlTest, PasswordHasherTest, AuthHandshakeTest |
| **Audit log** (`diesel.security.AuditLogger`) | ✅ Implemented | AuditTest, AuditAsyncTest, AuditRetentionTest, AvroAuditLoggerTest |
| **Schema versioning** (`diesel.schema`, 5 classes) | ✅ Implemented | SchemaVersionTest, SchemaEvolutionTest, SchemaConflictResolverTest |
| **Adaptive Query Optimizer** (`QueryOptimizer`) | ✅ Implemented | QueryOptimizerTest, QueryCacheTest, QueryProfilerTest |
| **6 index types** (BTree + variants + Hash + Unique) | ✅ Implemented | BTreeClusteredIndexBulkLoadTest, BTreeIndexBulkLoadTest, CompositeIndexTest, CoveringIndexTest, ParallelIndexScanTest |
| **MVCC** (`Transaction`, `TxStatusTracker`, `TupleVisibility`) | ✅ Implemented | MvccDeleteTest, MvccUpdateTest, TupleVisibilityTest, TxStatusTrackerTest, IsolationSemanticsTest, CopyOnWriteIsolationTest, CompactWithLargeRowVersionsTest, OldVersionTimeoutTest |
| **Avro storage subsystem** (58 classes) | ✅ Implemented | 33 Avro-specific test classes (AvroBackupManagerTest, AvroBatchOperatorTest, AvroBlockManagerTest, AvroBloomFilterTest, AvroCompressionTest, AvroIntegrityCheckerTest, AvroRecoveryManagerTest, AvroRestoreManagerTest, AvroRowStorageTest, AvroTransactionManagerTest, ...) |
| **JSONL storage** (streaming + block + delta) | ✅ Implemented | 18 JSONL-specific test classes (JsonlAppendModeTest, JsonlBlockManagerTest, JsonlCompressionTest, JsonlIndexTest, JsonlLazyLoadTest, JsonlNestedModeTest, JsonlNullSemanticsTest, JsonlParallelLoadTest, JsonlProjectionPushdownTest, JsonlSchemaModeTest, JsonlStorageTest, JsonlTypeMapperTest, ...) |
| **Replication & HA** (`diesel.replication`, 15 classes) | ✅ Implemented (Prompt4 #58, R3-016) | WalSenderTest, WalReceiverTest, ReplicationSlotTest, ReplicationSlotRegistryTest, HotStandbyHandlerTest, SyncReplicationTest, LogicalDecoderTest, StreamingApplierTest, PublicationTest, SubscriptionTest, ReplicationWireTest |
| **PITR** (`PitrManager`, `PointInTimeRecovery`) | ✅ Implemented (Prompt4 #43) | PointInTimeRecoveryTest, PitrManagerTest |
| **Incremental backup + scheduler** (`IncrementalBackup`, `BackupScheduler`) | ✅ Implemented (Prompt4 #42) | IncrementalBackupTest, BackupSchedulerTest |
| **Checksummed pages** (`diesel.checksum`, 3 classes) | ✅ Implemented (Prompt4 #51, R3-014) | ChecksummedPageTest, Crc32cTest, PageValidatorTest |
| **Observability** (`diesel.observability`, 11 classes) | ✅ Implemented (Prompt4 #44 + #46) | MetricsRegistryTest, PrometheusExporterTest, MetricsHttpServerTest, HealthCheckTest, LivenessCheckTest, ReadinessCheckTest, HealthJsonTest, CounterTest, HistogramTest |
| **Drivers** (5 modules in `drivers/`) | ✅ Implemented (Prompt4 #55-#57) | JdbcTest, HibernateTest, DriverTestSupport, NodeTest, PythonTest, GoTest, RustTest, LibpqTestServer |
| **7 JMX MBeans** | ✅ Implemented | (production-grade observability) |

### 🚧 In Active Development (planned for v1.0.0)

| Area | Current State | Roadmap |
|---|---|---|
| **FOREIGN KEY / CHECK / NOT NULL / DEFAULT constraints** | Not parsed (not in `SqlKeywords`) | [Phase 2](#roadmap) — 2.3 FTE-months |
| **JSON column type** | Not implemented (JSONL is storage backend, not column type) | [Phase 3](#roadmap) — 1.5 FTE-months |
| **CTE / WITH / recursive CTE** | Not parsed | [Phase 2](#roadmap) — 1.0 FTE-months |
| **UNION / INTERSECT / EXCEPT** | Not parsed | [Phase 2](#roadmap) — 0.3 FTE-months |
| **Window functions** | Not parsed | [Phase 2](#roadmap) — 1.5 FTE-months |
| **TRUNCATE TABLE statement** | WAL opcode exists (`TRUNCATE=6`), SQL parsing planned | [Phase 2](#roadmap) — 0.2 FTE-months |
| **`LogicalDump` / `LogicalRestore`** | Currently stubs (write only header / read file) | [Phase 1](#roadmap) — 2.0 FTE-months |
| **`TablespaceRegistry` multi-tablespace** | Currently stub ("Phase 2" comment) | [Phase 1](#roadmap) — 1.0 FTE-months |
| **`PhysicalBackup.triggerCheckpoint()`** | Currently simplified (placeholder LSN `1000L`) | [Phase 1](#roadmap) — 0.5 FTE-months |
| **`TupleVisibility` proper semantics** | RC/RR/S currently treated same — polishing planned | [Phase 1](#roadmap) — 1.5 FTE-months |
| **Default isolation level** | Currently `READ_UNCOMMITTED` — change to `READ_COMMITTED` planned | [Phase 1](#roadmap) — 0.1 FTE-months |
| **`executeBatch` race condition** | Documented bug for parallel INSERTs to same table | [Phase 1](#roadmap) — 0.5 FTE-months |
| **`LegacyProtocolAdapter` (port 5440)** | Deprecated, scheduled for removal | [Phase 1](#roadmap) — 0.3 FTE-months |
| **Logical replication auto-failover** | Physical streaming + sync standby + slots + logical decoder are implemented; Patroni-equivalent auto-failover tool not yet built | [Phase 4](#roadmap) — 2.0 FTE-months |
| **Connection pooling** | Not implemented (server-side PgBouncer-equivalent) | [Phase 6](#roadmap) — 0.5 FTE-months |
| **Parallel single-query execution** | Only index scan + ORDER BY (no parallel scan partitions) | [Phase 6](#roadmap) — 2.0 FTE-months |
| **`--password` in process list** | CLI password visible via `ps`; interactive prompt via `System.console().readPassword()` (no echo) is supported, but env/file/stdin inputs are not | [Phase 1](#roadmap) — secret file / env var / stdin |
| **BLOB / CLOB / TEXT / VARCHAR / DECIMAL(p,s) / TIMESTAMP / INTERVAL / ARRAY / MAP / ENUM / geo types** | Not in `resolveColumnJavaType` switch | [Phase 3](#roadmap) — 10.3 FTE-months |
| **Stored procedures / functions / triggers** | Not implemented | [Phase 2](#roadmap) — 3.0 FTE-months |
| **Views / Materialized views** | Not implemented | [Phase 2](#roadmap) — 0.5 FTE-months |
| **EXISTS / ANY / ALL / SOME / BETWEEN / regex operators / RETURNING / UPSERT / LATERAL JOIN** | Not parsed | [Phase 2](#roadmap) — 3.0 FTE-months |
| **LICENSE file** | ✅ Apache-2.0 — [LICENSE](LICENSE) | Done (Phase 0) |

For a comprehensive list with workarounds for current limitations, see [`KNOWN_LIMITATIONS.md`](./KNOWN_LIMITATIONS.md).

### Documentation note

The `examples/avro-examples.sql` file demonstrates SQL features planned for future versions. Some statements (e.g., `VARCHAR(100)`, `DECIMAL(10,2)`, `TIMESTAMP`, `MAP(STRING, STRING)`, `STORED AS AVRO`, `PARTITIONED BY`) are not yet parsed by the current SQL parser and will be added in [Phase 2 (SQL Standard)](#roadmap) and [Phase 3 (Data Types)](#roadmap).

---

## Roadmap

### Phase 0: Legal & Governance (1 month)

- [x] Add OSI-compatible LICENSE file (Apache-2.0 — [LICENSE](LICENSE))
- [x] Add [`CONTRIBUTING.md`](CONTRIBUTING.md)
- [ ] Add `CODE_OF_CONDUCT.md`, `SECURITY.md`
- [ ] Publish v0.1.0 with SemVer promise
- [x] Maven Central publication pipeline for `diesel-jdbc` (Prompt4 #55) — engine artifact still unpublished
- [x] PyPI publication for `diesel-python` (Prompt4 #56)
- [x] npm publication for `diesel-node` (Prompt4 #56)
- [x] pkg.go.dev publication for `diesel-go` (Prompt4 #57)
- [x] crates.io publication for `diesel-rs` (Prompt4 #57)
- [ ] Create `dieseldb.org` website with quickstart, API docs, FAQ
- [ ] Recruit additional core maintainers

### Phase 1: Polish existing subsystems (2 months)

- [ ] Implement `LogicalDump.execute()` — full SQL-level dump
- [ ] Implement `LogicalRestore.execute()` — restore from dump
- [ ] Implement `PhysicalBackup.triggerCheckpoint()` — real interaction with CheckpointManager
- [ ] Implement `TablespaceRegistry` — multi-tablespace
- [ ] Fix `executeBatch` race condition (`QueryExecutor`)
- [ ] Implement `TupleVisibility` with proper RC vs RR vs S semantics
- [ ] Change default isolation to `READ_COMMITTED`
- [ ] Remove `LegacyProtocolAdapter` (port 5440)
- [ ] Implement secret file / env var / stdin password input

### Phase 2: SQL Standard (4 months)

- [ ] FOREIGN KEY constraints (CASCADE, SET NULL, RESTRICT)
- [ ] CHECK constraints
- [ ] NOT NULL + DEFAULT in column definition
- [ ] CTE / WITH clause (recursive + non-recursive)
- [ ] Window functions (OVER, PARTITION BY, ROW_NUMBER, RANK, LAG, LEAD)
- [ ] UNION / INTERSECT / EXCEPT
- [ ] HAVING with DISTINCT (COUNT(DISTINCT x))
- [ ] STDDEV, VARIANCE, GROUP_CONCAT
- [ ] EXISTS / ANY / ALL / SOME subqueries
- [ ] BETWEEN
- [ ] Regex operators (`~`, `REGEXP`)
- [ ] CASE expression, CAST
- [ ] TRUNCATE TABLE statement
- [ ] RENAME TABLE / MODIFY COLUMN / ADD/DROP CONSTRAINT
- [ ] CREATE VIEW / DROP VIEW
- [ ] RETURNING clause
- [ ] UPSERT / MERGE (ON CONFLICT, INSERT IGNORE, REPLACE INTO)
- [ ] ALTER TABLE ALGORITHM=INSTANT (instant ADD COLUMN)
- [ ] Stored procedures (PL/DSL)
- [ ] Triggers (BEFORE/AFTER INSERT/UPDATE/DELETE)
- [ ] NIST SQL Test Suite compliance

### Phase 3: Data Types (2 months)

- [ ] JSON column type (binary JSONB-like storage, indexed)
- [ ] BLOB / CLOB / BINARY / VARBINARY / TEXT types
- [ ] VARCHAR / NVARCHAR / DECIMAL(p,s) / NUMERIC
- [ ] TIMESTAMP / TIMESTAMPTZ / TIME / INTERVAL
- [ ] ARRAY column type
- [ ] MAP column type
- [ ] ENUM column type
- [ ] Composite/custom types
- [ ] Geo / spatial (PostGIS-equivalent)
- [ ] Full-text search (tsvector, tsquery, GIN index)

### Phase 4: Replication & HA (6 months)

- [x] WALStreamingReplicator (master → standby, physical) — `WalSender`/`WalReceiver` (Prompt4 #58)
- [x] Sync standby support — `replication.role = standby` + sync ACK (3.3.96)
- [x] LogicalReplicator (CDC, pub/sub) — `LogicalDecoder`/`Publication`/`Subscription` (3.3.95)
- [ ] Auto-failover tool (Patroni-equivalent)
- [ ] Read replicas (read-only scale-out pool)
- [x] PITR via SQL (RECOVERY_TARGET_TIME) — `PitrManager`/`PointInTimeRecovery` (Prompt4 #43)
- [x] Replication slot / lag monitoring — `ReplicationSlotRegistry` (3.3.95)

### Phase 5: JDBC & Ecosystem (2 months)

- [x] JDBC driver (`java.sql.Driver` implementation) — `drivers/jdbc/diesel-jdbc` (Prompt4 #55, Hibernate-compatible `DatabaseMetaData`)
- [x] Hibernate dialect — verified via `HibernateTest` (Prompt4 #55)
- [ ] jOOQ support
- [ ] Flyway/Liquibase support
- [ ] GUI client (like pgAdmin / H2 web console)
- [ ] Docker image
- [x] K8s liveness/readiness probes — `k8s/dieseldb-probes.yaml` + `/health/live` + `/health/ready` (Prompt4 #46)
- [ ] Documentation: API docs (Javadoc), quickstart, tutorials

### Phase 6: Performance (3 months)

- [ ] Parallel single-query execution (parallel scan partitions, work-stealing)
- [ ] Cost-based optimizer (full, not adaptive only)
- [ ] Columnar storage option
- [ ] Materialized views
- [ ] Connection pooling (PgBouncer-equivalent protocol)
- [ ] TPC-C / TPC-H benchmarks

**Total: ~55 FTE-months** — двигаясь к v1.0.0 stable release.

---

## Contributing

> 📄 Full contribution guide: [`CONTRIBUTING.md`](CONTRIBUTING.md) — dev setup, test gates, commit format, PR process.

### How to contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make changes, add tests in `src/test/java/diesel/`
4. Run tests: `make test` (see [Test gates](CONTRIBUTING.md#test-gates-by-change-type) by change type)
5. Run SonarQube analysis: `mvn sonar:sonar`
6. Commit changes following existing commit message format (e.g., `3.x.y Prompt N: description`)
7. Open a Pull Request

### Code style

- Java 21 (LTS) — uses modern features (records, sealed classes, pattern matching where applicable)
- SLF4J + Logback for logging
- JUnit Jupiter 5.10.0 for tests
- Awaitility 4.2.0 for async test conditions
- JSR-305 annotations for null-safety
- SonarQube analysis via `sonar-maven-plugin`

### Priority areas for contribution

- **Critical**: complete stub implementations (LogicalDump, LogicalRestore, TablespaceRegistry, PhysicalBackup.triggerCheckpoint)
- **High**: Polish TupleVisibility semantics (RC vs RR vs S), add FOREIGN KEY / CHECK / NOT NULL / DEFAULT constraints
- **Medium**: Add JSON column type, CTE, window functions, UNION/INTERSECT/EXCEPT
- **Performance**: Parallel single-query execution, connection pooling, cost-based optimizer
- **Ecosystem**: jOOQ support, Docker image, K8s operator (probes already shipped), GUI client

### Sponsors

If DieselDB helps your production workloads, consider sponsoring:
- [GitHub Sponsors](https://github.com/sponsors/Reider85) — main developer (Vasiliy Zhila)

---

## Releases

> 🚧 First stable release (v1.0.0) is planned after Phase 0-1 completion. Current line is `3.3.x`.

DieselDB is in active development (current version `3.3.99` in `pom.xml`). There are:
- 0 GitHub releases tagged as v1.0.0 (first v0.1.0 SemVer tag planned soon)
- ✅ Driver publish pipelines shipped for `diesel-jdbc` (Maven Central), `diesel-python` (PyPI), `diesel-node` (npm), `diesel-go` (pkg.go.dev), `diesel-rs` (crates.io) — engine artifact itself still 0 Maven Central publications
- 0 tags on the engine repository
- 1000+ commits on `main` branch (current: `3.3.99 Optimize LRU eviction policy with unpinned-page queue for O(1) eviction`, 2026-11-05)

Recent release highlights (last 100 commits, 3.3.3 → 3.3.99):
- **3.3.99** — O(1) LRU eviction policy with unpinned-page queue
- **3.3.98 / 3.3.97 / 3.3.94** — Avro/JSONL/CSV/TSV inverse-map shift optimizations (insertAt/deleteRow O(1))
- **3.3.96 / 3.3.95 / 3.3.93** — Replication: sync standby ACK, replication slots, physical WAL streaming
- **3.3.92** — Builder pattern refactor (SonarQube `java:S107`) across CliArgs, WALConfig, RowVersionMeta, etc.
- **3.3.91-3.3.89** — Avro column-statistics and compression-pattern O(1) optimizations
- **3.3.88 / 3.3.80 / 3.3.77** — Avro/JSONL index manager overhauls (lazy `RowCiView`, `SchemaIndex` cache, `validateFlattenSchema` O(C log C))
- **3.3.87 / 3.3.86 / 3.3.85** — Drivers: Go (`diesel-go`), Rust (`diesel-rs`), Python (`diesel-python`), Node.js (`diesel-node`), JDBC (`diesel-jdbc`, Hibernate-compatible)
- **3.3.85 / 3.3.84** — libpq v3 wire protocol + SCRAM-SHA-256 auth + psql meta-commands
- **3.3.83 / 3.3.82 / 3.3.81** — CRC32C hardware-accelerated checksums, `ChecksummedPage`, background `PageScrubber` + `CheckpointStrategy`
- **3.3.79 / 3.3.78** — String-literal case-insensitivity (R3-013), `TRUE`/`FALSE`/`NULL` literals as boolean-as-integer
- **3.3.74 / 3.3.69 / 3.3.68** — Health endpoints (`/health`, `/live`, `/ready`), Prometheus `/metrics`, `MetricsRegistry` (lock-free counters + p50/p95/p99 histograms)
- **3.3.66** — PITR (Point-in-Time Recovery) via commit-timestamped WAL replay
- **3.3.63 / 3.3.44** — Incremental physical backup + `BackupScheduler` (5-field cron)
- **3.3.62 / 3.3.61 / 3.3.60 / 3.3.59 / 3.3.56 / 3.3.55 / 3.3.54** — BufferPool O(1) optimizations: liveCount cache, slot-directory compaction, tombstone reuse, unpinned-page queue, dirty-page index, catalog lookup

For installation today, use `mvn install` from source:

```bash
git clone https://github.com/Reider85/dieseldb.git
cd dieseldb
mvn install -DskipTests
```

Then in your `pom.xml` (local Maven repo only):

```xml
<dependency>
  <groupId>com.dieseldb</groupId>
  <artifactId>dieseldb</artifactId>
  <version>3.3.99</version>
</dependency>
```

Drivers (published to package registries):

```xml
<!-- JDBC driver (Maven Central) -->
<dependency>
  <groupId>com.dieseldb</groupId>
  <artifactId>diesel-jdbc</artifactId>
  <version>0.1.0</version>
</dependency>
```

```bash
# Python (PyPI)
pip install diesel-python

# Node.js (npm)
npm install diesel-node

# Go (pkg.go.dev)
go get github.com/Reider85/dieseldb/drivers/go/diesel-go

# Rust (crates.io)
cargo add diesel-rs
```

---

## License

DieselDB is licensed under the **Apache License, Version 2.0** — see [LICENSE](LICENSE) for the full text.

```
Copyright 2026 Reider85 (Vasiliy Zhila)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

Contributions are submitted under the same license — see [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Acknowledgments

DieselDB is developed by:

- **Reider85 (Vasiliy Zhila)** — main developer
- **qwen-intl** — AI agent (Qwen International)
- **openhands-agent** — AI agent (OpenHands)

The project follows an **AI-driven development workflow** — each commit corresponds to a specific "prompt" given to an AI agent, visible in commit messages (e.g., `3.3.32 Prompt4 #37: online schema changes step 1/3`).

### Architectural inspiration

DieselDB implements patterns from:
- **PostgreSQL** — ARIES recovery, MVCC, SSI (Serializable Snapshot Isolation)
- **InnoDB (MySQL)** — Buffer pool LRU, page-based storage, slotted page layout
- **DB2 / Oracle** — ARIES (Algorithm for Recovery and Isolation Exploiting Semantics)
- **MySQL 8.0+** — Online `ALTER TABLE` algorithms (DEFAULT/COPY/INPLACE)
- **Patroni** — HA failover patterns (planned)

### Dependencies

| Library | Version | Purpose |
|---|---|---|
| `org.junit.jupiter:junit-jupiter` | 5.10.0 | Test framework |
| `org.slf4j:slf4j-api` | 2.0.12 | Logging facade |
| `ch.qos.logback:logback-classic` | 1.5.6 | Logging implementation |
| `org.awaitility:awaitility` | 4.2.0 | Async test conditions |
| `com.github.luben:zstd-jni` | 1.5.6-6 | ZSTD compression |
| `org.lz4:lz4-java` | 1.8.0 | LZ4 compression |
| `org.xerial.snappy:snappy-java` | 1.1.10.5 | Snappy compression |
| `com.fasterxml.jackson.core:jackson-core` | 2.17.2 | JSON (wrapped via streaming adapters) |
| `com.google.code.gson:gson` | 2.8.9 | JSON (alternative adapter) |
| `org.apache.avro:avro` | 1.12.0 | AVRO serialization |
| `com.google.code.findbugs:jsr305` | 3.0.2 | `@Nullable` / `@Nonnull` annotations |

---

## Contact

- **Repository**: [https://github.com/Reider85/dieseldb](https://github.com/Reider85/dieseldb)
- **Issues**: [https://github.com/Reider85/dieseldb/issues](https://github.com/Reider85/dieseldb/issues) — for bug reports and feature requests
- **Discussions**: not yet established (planned)
- **Author**: Reider85 (Vasiliy Zhila)

---

*DieselDB — production-grade Java database engine with ARIES recovery, WAL, page-based storage, online schema changes, SSI concurrency control, physical streaming replication, and Prometheus-based observability. Under active development toward v1.0.0 stable release.*
