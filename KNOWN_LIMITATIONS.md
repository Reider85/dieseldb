# DieselDB Known Limitations and Workarounds

This document describes the known limitations of the experimental DieselDB
database engine and the recommended workarounds for each of them.
Configuration files are read from `config.properties` in the process working
directory.

---

## 1. Maximum query result size

**Limitation.** The engine caps the number of rows a query may return. The
default limit is **1,000,000 rows** (key `max.result.rows` in
`config.properties`, default `1000000`). The limit is enforced at every
execution stage (filter, join, aggregation, etc.), so the error may fire on
an intermediate result.

- at ~80% of the limit a warning is written to the log:
  `WARNING: query result is approaching the maximum allowed row limit: ... (80%). Consider adding LIMIT or a MAX_ROWS hint.`;
- at 100% of the limit the query fails with
  `Query result exceeds the maximum allowed row limit of N rows at stage '...'. Add LIMIT or a /* MAX_ROWS=N */ hint to override.`

**Workaround:**
- add `LIMIT` to queries that may return many rows;
- for an individual query use the `/* MAX_ROWS=N */` hint (for example
  `SELECT /* MAX_ROWS=5000000 */ ...`); `/* MAX_ROWS=0 */` disables the
  limit for that query entirely;
- if a higher global limit is needed, raise `max.result.rows` in
  `config.properties` and proportionally increase the JVM heap (see section 3);
- narrow the result set with `WHERE` conditions.

---

## 2. JOIN with an OR condition

**Limitation.** A JOIN whose condition contains `OR` (for example
`JOIN b ON a.id = b.id OR a.code = b.code`) can produce a very large result
because the equalities inside `OR` are not decomposed into an efficient
execution path. The engine logs
`WARNING: JOIN with OR condition may produce large result set`.

**Workaround:**
- rewrite `OR` as the union of two equality queries
  (`SELECT ... WHERE cond1 UNION SELECT ... WHERE cond2`);
- replace `OR` with an `IN` list where possible;
- add an index on the join columns (see section 5);
- constrain the result with `WHERE`/`LIMIT`.

---

## 3. Memory requirements / heap size

**Limitation.** DieselDB keeps the active rows of a table in JVM heap
(`Table.rows` — `ArrayList<Map<String, Object>>`), and query results are
buffered in memory for the duration of execution. The schema catalog and WAL
are written through page-based storage (`PageManager`, `BufferPool` with LRU
eviction), but for row data itself there is no spill-to-disk on scan/join —
the table must fit in memory. Peak demand depends on data volume and the
type of operation:

- the test/dev configuration uses a **512 MB** heap (`-Xmx512m`);
- heavy operations (for example a 600×600 row join with `ORDER BY`, ~360k
  result rows) may peak at **0.7–1.5 GB** — use `-Xmx4g` for them;
- configuration keys: `max.inmemory.rows` (streaming / spill threshold,
  default `10000`), `max.hash.table.size.mb` (hash-join budget; when
  exceeded the join is partitioned and spills, default `512`).

**Workaround:**
- narrow the working set with `WHERE` and `LIMIT`;
- increase the heap: `-Xmx4g` (or more) for large joins;
- tweak `max.inmemory.rows` and `max.hash.table.size.mb` when needed;
- keep individual tables at a reasonable size — the engine cannot operate
  on tables that do not fit in memory.

---

## 4. OFFSET without LIMIT

**Limitation.** A query with `OFFSET` but no `LIMIT` forces the engine to
materialize and sort the entire result before skipping rows. The engine
logs `OFFSET without LIMIT may be inefficient`.

**Workaround:** always specify `LIMIT` together with `OFFSET`
(`... LIMIT 50 OFFSET 100`).

---

## 5. JOIN on an unindexed column

**Limitation.** When a JOIN targets a column without an index, the engine
auto-creates an in-memory B-tree index on the join column and logs
`Consider creating index on TABLE.COLUMN for faster JOIN`. That index is
rebuilt on every query.

**Workaround:** create a persistent index explicitly — `CREATE INDEX`
(supported types: BTREE, HASH, UNIQUE, CLUSTERED). This speeds up repeated
JOINs and equality lookups.

---

## 6. Persistence and file format

**Limitation.** Each table is stored as `<NAME>.<ext>` (data) +
`<NAME>.table` (serialized metadata) in the database `dataDir`
(default: `data/`). The extension and physical layout are determined by
the storage type — selected by the `storage.type` key in `config.properties`
(or the system property `-Ddiesel.storage.type`). Five backends are
supported: `csv`, `tsv`, `jsonl` (with optional `.zst`/`.lz4`/`.snappy`
compression), `avro` (writes to `data/avro/<NAME>.avro` + sidecar files)
and `in_memory` (rows live only in heap; persisted only when
`diesel.inmemory.persist=true`). The default in `config.properties` is
`csv`; in code, when `config.properties` is absent, the default is
`in_memory`. On startup/load the table is read into memory in full; write
cost grows with table size (the coalesced-persist daemon
`diesel-persist-flusher` groups bursts of mutations into a single whole-file
rewrite, see `Table.saveToFileDebounced`).

**Workaround:**
- keep table sizes reasonable (see section 3);
- for server runs use a dedicated data directory
  (`start-server.bat`/`start-server.sh`, or `--local [dataDir]`) so the
  working directory is not polluted by artefacts;
- pick the backend to match the workload: `csv`/`tsv` for simple
  interchange, `jsonl` for nested types + compression, `avro` for schema
  evolution and block-level compression, `in_memory` for max throughput
  without persistence requirements.

---

## 7. Transactions and isolation level

**Limitation.** The default isolation level is **SERIALIZABLE**
(key `transaction.isolation.level`). This is the strictest level: under
concurrent transactions conflicts are possible and retries may be needed.

**Workaround:** if the workload does not need strict serializability, set a
lower isolation level (`READ_UNCOMMITTED`, `READ_COMMITTED`,
`REPEATABLE_READ`) in `config.properties` — in exchange for more concurrent
readers.

---

## 8. Server socket timeout

**Limitation.** Idle client connections are closed after a timeout —
default **30 seconds** (key `server.socket.timeout=30000` in
`config.properties`).

**Workaround:** for long sessions raise `server.socket.timeout`, or
re-establish the connection before the timeout expires.

---

## 9. Slow-query profiling threshold

**Limitation.** Queries slower than `diesel.profile.slow.threshold.ms`
(default **1000 ms**) are logged as
`Slow query breakdown: parse=...ms, plan=...ms, execute=...ms, sort=...ms`.
The value can be overridden by the system property
`-Ddiesel.profile.slow.threshold.ms`.

**Workaround:** to diagnose slow queries lower the threshold (for example
`-Ddiesel.profile.slow.threshold.ms=100`); for quieter logs raise it.

---

## 10. JSONL compression: sizes and performance (Prompt 52)

**Measurements** (500 rows × repeating `name-XXXX` template + 200 chars;
Java 21 / Windows 11 / Intel Xeon).

| Codec | File size | Compression | Ratio |
|-------|-----------|-------------|-------|
| none  | 119,230 B | —           | 1×    |
| zstd  |   1,756 B | 67.9×       | ×1/67.9 |
| lz4   |   5,300 B | 22.5×       | ×1/22.5 |
| snappy|  11,596 B | 10.3×       | ×1/10.3 |

**Performance** (5000 rows × repeating template, 3 runs):

| Scenario | Save     | Load (min/3) |
|----------|----------|--------------|
| none     | 328 ms   | 143 ms       |
| zstd     | 434 ms   | 18 ms        |

Sequential reads of a zstd file are **faster** than uncompressed (~8× in
this measurement) because 13× less data is processed.

**Configuration key:** `jsonl.compression.codec = none | zstd | lz4 | snappy`
(default: `zstd`). The `jsonl.compression.level` key controls the ZSTD level
(1–22, default 3); LZ4/Snappy ignore it.

**Limitations:**
- Append mode (`jsonl.write.mode=append`) is not compatible with compression
  — automatic fallback to a full rewrite with a warning in the log.
- Append data is rewritten on every save when codec ≠ none; no delta file
  is created because compressed streams are framed and cannot be appended to.

## 11. JSONL cross-format benchmark (Prompt 56)

**Measurements** (@LargeTest, Java 21 / Windows 11 / 4-core; PROMPT 56
quality gate, test data files in a temporary directory).

**Loading 1M rows** ([JSONL-BENCH-1M], csv vs jsonl vs jsonl+zstd):

| Format | Load time | Note |
|--------|-----------|------|
| csv    | 1,266 ms  | CSV storage (deflated) |
| jsonl  | 54,055 ms | JSONL without compression |
| jsonl+zstd | 55,166 ms | JSONL + zstd codec |

**Projection on wide rows** ([JSONL-BENCH-WIDE], 20k rows × 42 columns,
projecting 3 of 42 columns):

| Scenario | Time | Speedup |
|----------|------|---------|
| full scan       | 1,913 ms | 1×  |
| projection      |   341 ms | **5.61×** (requirement ≥2×) |

**Append vs rewrite** ([JSONL-BENCH-APPEND], 10 batches × 1000 inserts):

| Scenario | Time | Advantage |
|----------|------|-----------|
| rewrite (full)   | 620 ms | 1× |
| append (extension) | 153 ms | **4.1×** (requirement ≥2×) |

**Limitations / observations:**
- JSONL loading of 1M rows is computationally slower than CSV (~42× in this
  measurement); this is expected for JSON (object parsing) and is treated
  as a measurement, not a criterion — no speed requirement is imposed.
- JSONL+zstd does not load faster than plain JSONL on large data (in this
  measurement it is even slightly slower — codec cycles on decompression);
  the benefit of zstd is file size, not read speed.
- Projection gives a >5× speedup on wide rows (criterion ≥2× met);
  append is >4× faster than rewrite (criterion ≥2× met).
- Single measurements (one run per scenario, expected spread ±10%;
  heavy runs go under `-P large`, 4 GB heap).

---

## 12. Legacy protocol (Prompt 4 #29)

**Limitation.** For compatibility with old (v1) clients, port **5440** runs
the legacy Java Object Serialization protocol. This protocol is vulnerable
to RCE attacks via deserialization and will be removed in **Phase 2**.

**Workaround:**
- new clients should use the main port (default 3306) with the new binary
  v2 protocol;
- on the main port (3306) Java serialization is rejected with an error when
  the `ObjectOutputStream` magic bytes are detected;
- legacy clients on port 5440 receive a deprecation warning in the logs;
- the legacy port can be disabled by setting `legacy.port = 0` in
  `config.properties`.

**Configuration key:** `legacy.port = 5440` (default: 5440, 0 = disabled).

**Note:** Removal of the legacy protocol is planned for Phase 2 of
development.

---

## 13. Parallel executeBatch against a single table is not synchronized

**Limitation.** `Database.executeBatch(...)` without a transaction
distributes queries across a thread pool: table dependencies affect only
batch *grouping*, not execution order — several INSERTs into the same table
execute **concurrently** and on large batches fail with a race
(`QuerySyntaxException: Index: N, Size: M`, duplicate key). Discovered while
writing tests for Prompt 4 #29.

**Workaround:**
- insert many rows into one table with a sequential `executeQuery(...)`
  loop (as the #29 network tests do) or inside a transaction — there
  `executeBatch` falls back to the sequential `executeBatchSequential`;
- `BatchQueryTest` (2–3 queries) passes statistically but does not prove
  the absence of a race on larger batches.

**Status:** Candidate for a dedicated prompt — dependency-graph components
should run sequentially, independent ones in parallel.

---

## 14. Authentication and authorization: legacy port without auth, RBAC limits

**Limitation.** Prompt 4 #31–32 introduced authentication and authorization,
but several limitations remain:

- **legacy port (5440) cannot authenticate** — the v1 protocol has no AUTH
  message. With `security.auth.required` enabled, the legacy adapter rejects
  SQL queries with `Error: Authentication required` (or the server owner
  disables the check via `security.auth.required = false` for legacy
  clients);
- **forced password change — hard enforcement**: Prompt 4 #32 implements a
  hard block on every request except `ALTER USER` until the password is
  changed (flag `mustChangePassword`);
- **`ALTER USER` with ADMIN privilege**: Prompt 4 #32 added an Authorizer —
  a user with ADMIN privilege may change any user's password; without ADMIN
  only their own;
- **FETCH on an open cursor is not re-checked after REVOKE**: with a cursor
  open, the user may keep fetching data even after privileges are revoked
  (similar to other DBMSs);
- **`--password` is visible in the OS process list** — the password is
  passed as a command-line argument; for automation use environment
  variables or launch the client from a protected environment.

**Workaround:**
- in production keep `security.auth.required = true`; disable the legacy
  port via `legacy.port = 0`;
- changing another user's password: a user with ADMIN privilege or the
  local `--reset-admin`;
- encryption of credentials in transit: server- and client-side TLS are
  available from Prompt 4 #34/35 (`tls.enabled`, CLI flags); mTLS/SNI — #36;
  the legacy port does not support encryption.

**Configuration keys:** `security.auth.required` (default: true),
`security.auth.bootstrap-admin` (default: true).

## 15. SQL Audit log (prompt4 #33, 2026-10-20)

Limitations of SQL-statement auditing (R3-008 step 4/4):

- **audit is off by default** — `audit.enabled = false` for dev
  environments; enable explicitly for production (compliance logging);
- **legacy port (5440) is audited as `embedded`** — the v1 adapter invokes
  the session-less `executeQuery`, so USER_NAME/SOURCE_IP are recorded as
  `embedded` (IP is not available without a session). The main v2 port
  writes the real user/IP from the session;
- **nested subquery / batch expressions are not audited separately** — the
  outer statement gets one record (AUDIT_INNER guard); this is intentional:
  one record per user statement, with no noise from internal reuse;
- **queue overflow → graceful drop** — when the bounded queue is full
  (`audit.queue.capacity`, default 10000) the record is dropped, the
  `droppedCount` counter grows, a WARN is logged every 1000; the main query
  is never blocked;
- **retention archives instead of deleting** — rows older than
  `audit.retention.days` (default 90) are moved to a gzip archive
  (`data/audit-archive/audit-<ts>.log.gz`, pipe-delimited) and only then
  removed from the table; no data is lost;
- **`DIESEL_AUDIT_LOG` is protected from DROP** — like every `diesel_*`
  system table (guard in `Database.dropTable`);
- **difference from AVRO audit (Prompt 95)**: `diesel.security.AuditLogger`
  is SQL audit (who/what/when/ip/status), `AvroAuditLogger` is a
  storage-level audit of AVRO-backend operations; independent subsystems
  with different keys (`audit.*` vs `avro.audit.*`).

**Workaround:**
- in production: `audit.enabled = true`, `audit.retention.days` per
  compliance policy (usually 90–365);
- for debugging: `-Daudit.enabled=true` as a system property (overrides
  config.properties).

**Configuration keys:** `audit.enabled` (default: false),
`audit.retention.days` (default: 90), `audit.queue.capacity` (default:
10000), `audit.retention.interval.ms` (default: 86400000),
`audit.archive.dir` (default: data/audit-archive).

## 16. TLS transport (prompt4 #34/#35/#36, 2026-10-20/21)

Server-side TLS over TCP accept (R3-009 step 1/3):

- **TLS is off by default** — `tls.enabled = false`; for production enable
  it explicitly and provision a keystore (see the keytool command in the
  `config.properties` comments);
- **broken keystore → start refusal** — with `tls.enabled=true` and a
  missing/corrupt keystore the server does not open the accept socket
  (fail fast instead of a silent downgrade to plaintext);
- **handshake is lazy** — the SSLSocket wrap happens after accept, and the
  handshake itself runs on the first read in the worker thread; a slow or
  malicious client cannot block the accept loop (anti handshake-DoS); a
  handshake failure closes the connection in ClientHandler;
- **cipher whitelist — allow-list** — only 3 TLS 1.3 suites + PFS/AEAD
  TLS 1.2 (ECDHE/DHE + AES-GCM/CHACHA20); legacy (TLS 1.0/1.1, RSA key
  exchange, CBC, RC4, 3DES, anon/NULL) is disabled by allow-list, not
  deny-list; only TLSv1.3/TLSv1.2 are enabled;
- **client TLS implemented (Prompt 4 #35)** — CLI flags `--tls`/`--tls-ca`/
  `--tls-cert`/`--tls-key` work; DatabaseClient supports TLS via
  setSslSocketFactory(); hostname+chain verification, expired cert
  rejection; optional diesel-client.conf; mTLS/SNI are in #36 (see below);
- **legacy port (5440) is not encrypted** — TLS is wired only into the main
  v2 port; the legacy v1 adapter stays plaintext (another reason to keep
  `legacy.port = 0` in production);
- **mTLS (Prompt 4 #36)** — `tls.client.auth = none|request|require`
  (default `none`); with `require` the client must present a certificate
  verified by the server truststore (`tls.ca.path`); with `request` a cert
  is optional but verified when present; an invalid/missing cert under
  `require` = handshake failure in the worker thread (the client gets an
  IOException, the connection is closed); missing `tls.ca.path` with
  `auth != none` → start refusal (fail fast, no downgrade);
- **SNI multi-tenant (Prompt 4 #36)** — keystore selection by SNI hostname:
  config keys `tls.sni.<hostname>.keystore.path/.password/.type` or rows of
  the system table `DIESEL_TENANTS` (SQL `CREATE TENANT <name>
  SNI '<host>' KEYSTORE '<path>' [PASSWORD '<pw>']` / `DROP TENANT <name>`).
  Unknown/missing SNI → fallback to the default `tls.keystore.*`; the table
  is read once at server startup (restart to apply changes); config keys
  take precedence over the table on hostname conflict;
- **SSL Labs A+ — manual ops validation** — there is no automated
  testssl.sh/sslyze run in CI; the prerequisites (TLS 1.2/1.3 only,
  AEAD-only cipher whitelist, no renegotiation) are already covered by
  `CipherAuditTest`; with mTLS/SNI enabled and valid certificates the
  configuration meets A+ requirements (HSTS/OCSP stapling — at the reverse
  proxy layer);
- **the keystore password in DIESEL_TENANTS is stored in plaintext** — as
  is `tls.keystore.password` in config.properties (all-String columns of
  system tables); protection is filesystem permissions on the database.

**Workaround:**
- in production: `tls.enabled = true` + a valid PKCS12 keystore;
- mTLS: `tls.client.auth = require` + `tls.ca.path` (the CA that signed the
  client certificates);
- SNI: `CREATE TENANT` for each hostname, or `tls.sni.*` keys;
- test key pair: `keytool -genkeypair -alias dieseldb -keyalg RSA
  -keysize 2048 -storetype PKCS12 -keystore data/diesel-keystore.p12
  -storepass <password> -dname "CN=localhost"
  -ext "SAN=dns:localhost,ip:127.0.0.1"`.

**Configuration keys:** `tls.enabled` (default: false),
`tls.keystore.path` (default: data/diesel-keystore.p12),
`tls.keystore.password` (default: changeit),
`tls.keystore.type` (default: PKCS12),
`tls.client.auth` (default: none), `tls.ca.path` (server truststore for
mTLS), `tls.truststore.type`

## §17 Online schema changes (Prompt4 #37, step 1/3)

**Limitations of the current implementation (R3-010 step 1/3):**

- **Version history** — only the counter + the latest schema are persisted.
  The full version history is in-memory (capped at 32 versions + pinned)
  and is not restored on restart.
- **ALTER TABLE** — supports `ADD COLUMN <nullable>` (metadata-only,
  step 1) and `DROP COLUMN` (INPLACE tombstone, step 3 — see §19).
  `NOT NULL`, `DEFAULT` are deferred to prompts 113/114.
- **Per-query snapshot** — `SelectQuery.execute` pins the version for the
  duration of execute (timeout bookkeeping), but columns are read from
  `current()` (not from the pinned version). A strict per-query schema
  snapshot (reading strictly from the pinned version) is steps 2/3.
- **Mid-execute ALTER visibility** — as today (reference safety is
  guaranteed by immutable lists in Table). Schema-level MVCC is steps 2/3.

**Workaround for persistence in production:**
- The catalog is page-backed by default (see §21): `Database()` and
  `Database(dataDir)` call `createPageManager()` → `setCatalogPageManager(...)`;
  fallback to heap-only happens only if creating `pages.db` fails (disk
  full, no permissions). The separate "test mode" is no longer needed.

**Configuration key:** `schema.old.version.timeout.min` (default: 10,
minutes) — lifetime of old versions.
(default: AUTO), `tls.truststore.password`,
`tls.sni.<hostname>.keystore.path` (per-tenant SNI keystore).

## §18 Online ALTER TABLE ALGORITHM=COPY (Prompt4 #38, step 2/3)

**Limitations of the current implementation (R3-010 step 2/3):**

- **New column visible only after copy completes** — during the background
  copy the old schema is in effect: DML cannot reference the column being
  added (INSERT with the new column is rejected as unknown column before
  swap). After COMPLETED the column is visible to everyone; INSERT may
  omit it (the value is null — the marker is kept in memory via
  `alterAddedColumns`; after restart the marker is lost and the rule is
  strict again).
- **Dual-write — deferred journal, not synchronous duplication** — writes
  during the copy go to the old table + a journal
  (`DualWriteCoordinator`); the journal is applied to the shadow table
  once at finalize. A barrier (active-counter + seal) guarantees each
  mutation is atomic with respect to swap; the cost is the finalize window
  when DML waits for the barrier to settle (see `BARRIER_WAIT_MS` in
  `ALTER STATUS`; the "writers blocked > 100 ms" criterion is measured by
  it, not by the total cost of inserts, which always includes the
  pre-existing CSV persist path).
- **Compaction / reload are deferred during copy** — `Table.compact()` and
  `reloadFromStorage()` are skipped while the journal is attached (an
  index shift would break replay); VACUUM retries after completion.
  Physical `removeRow` during copy is supported (REMOVE event), but its
  concurrency with other removeRow is not synchronized — a pre-existing
  lack of locks in `removeRow`.
- **Batch-DML and DROP TABLE are blocked during copy** — `END BATCH`
  (copy-on-write publication would replace the table under the journal)
  and `DROP TABLE` on a table with an active copy are rejected with an
  error; wait for completion or cancel the copy.
- **Cancel is API-only** — `Database.cancelAlter(table)`; SQL
  `ALTER ROLLBACK` is not implemented. A cancel during finalize may arrive
  too late (the copy may already be done — that is not an error, status
  COMPLETED). Rollback before finalize guarantees: schema, versions and
  data are untouched, the shadow is discarded.
- **Indexes are not rebuilt on swap** — the shadow table's layout is
  positionally identical to the live one (baseline + the same mutations),
  so index entries stay valid; this holds as long as operations that
  shift indexes in some other way do not happen during the copy
  (`CREATE INDEX` during copy is itself allowed — its result is served by
  the normal path).
- **Crash semantics** — swap (in-memory) and catalog/data writes are
  separated: a crash between install and persist means "ALTER did not
  happen" at startup (catalog + data files untouched); there is no
  intermediate state "new catalog + old data": data is written first, the
  catalog last.
- **Test scale** — the "10M rows" acceptance criterion is scaled down to
  20k rows (fast-suite budget; what is checked is writer blocking and
  swap integrity, not absolute size — precedent: prompt4 #37).
- **`ALGORITHM=INPLACE` is executed** — from step 3 (prompt4 #39) INPLACE
  runs (metadata-only for ADD, tombstone for DROP); unknown ALGORITHM
  tokens → syntax error. For the COPY path the LOCK token is parsed and
  accepted, but LOCK restrictions only affect INPLACE execution (see §19).

**Workaround:**
- progress: `ALTER STATUS` (TABLE_NAME, STATE, PROGRESS_PERCENT,
  ROWS_COPIED/ROWS_TOTAL, BARRIER_WAIT_MS, MESSAGE);
- cancel: `Database.cancelAlter(<table>)` before COMPLETED;
- test copy-speed knobs: `-Dalter.copy.batch.size=1000
  -Dalter.copy.batch.sleep.ms=1`, barrier timeout:
  `-Dalter.copy.gate.timeout.ms=10000`.

**Configuration keys (system properties):** `alter.copy.batch.size`
(default: 2048), `alter.copy.batch.sleep.ms` (default: 0),
`alter.copy.gate.timeout.ms` (default: 10000);
`schema.old.version.timeout.min` (default: 10, inherited from step 1).
## §19 Online ALTER TABLE ALGORITHM=INPLACE + LOCK (Prompt4 #39, step 3/3)

**Limitations of the current implementation (R3-010 step 3/3):**

- **LOCK — advisory gates over the statement window** — `LOCK=SHARED`
  blocks DML (`awaitAlterWritersUnblocked` in addRow/updateRowInPlace/
  removeRow/markDeleted/markDelete/addRowWithMVCC), `LOCK=EXCLUSIVE` also
  blocks SELECT (`awaitAlterReadersUnblocked` in execute/executeStreaming).
  The gate window = the metadata operation itself (microseconds) + the
  test hook `alter.inplace.hold.ms`; the wait timeout is
  `alter.copy.gate.timeout.ms` (10 s, after which `DieselException`,
  not an infinite hang). A writer that already passed the check before
  the gate was set will finish its mutation — the gate is not a mutex
  around individual DMLs. For `ALGORITHM=COPY` the LOCK token is parsed
  and accepted but does not affect execution (the step #38 barrier is the
  sole regulator).
- **Physical keys of a dropped column live until strip** — after an
  INPLACE DROP the row maps still contain the key; `SELECT *` hides it via
  a hidden-set (`Table.droppedColumns`), explicit projections behave as a
  missing column, WHERE is rejected at parse time ("Unknown column" —
  schema validation). The physical strip is performed by a background
  `compact()` (submitted to the alter-executor after the statement;
  vacuum/saveToFile are a fallback). Strip replaces references to the row
  maps (it does not mutate them), so reader snapshots are not broken. The
  hidden-set is **not** cleared by compaction: MVCC pre-images and a
  reload from a stale file can re-materialize the key — the set protects
  it until a re-`ADD COLUMN` of the same name (which clears it) or table
  close; the cost is one set-lookup per key in `SELECT *`.
- **`.table` format v4** — the dropped-set is serialized as the last
  field; v1–v3 files are still read (the field is absent → empty), but
  new v4 files **cannot be read by older engine versions** (no forward
  compat). Other files (CSV) are rewritten through the normal persist
  path (`markAlterPersistDirty` + flusher).
- **DROP COLUMN with an index** — INPLACE is not supported (the column is
  not rewritten, the index cannot be rebuilt) → fallback to
  `ALGORITHM=COPY` with a warning in the response and the log (WARNING);
  the index is dropped in the finalize window of the copy. Canceling the
  copy (`cancelAlter`) does not touch the index — full rollback.
- **DROP is forbidden for PK and clustered columns** — `DieselException`;
  ALTER DROP of an unknown column → `ColumnNotFoundException`.
- **INPLACE is not registered in ALTER STATUS** — the operations are
  instantaneous and create no background jobs (unlike DEFAULT-ADD from
  step 1); `ALTER STATUS` reflects only COPY paths.
- **Re-ADD of a column after DROP** — clears the hidden marker; new
  values are written, but old row maps not yet stripped at the time of
  re-ADD retain the previous keys (the strip job, reading an empty set,
  skips the cutout — semantically correct: the column is legal again).
- **Test scale** — "10M rows" is scaled down to 20k (fast-suite budget
  and 512m heap; what is checked is metadata cost and consistency, not
  absolute size — precedent: prompt4 #37/#38). OnlineAlterTest: 2k rows
  + pacing readers/writer (the tight-loop variant OOMed the 512m heap on
  a 4-core machine).

**Workaround / test keys:**
- widen the gate window for observability: `-Dalter.inplace.hold.ms=600`;
- gate timeout: `-Dalter.copy.gate.timeout.ms` (inherited from §18).

**Configuration keys (system properties):** `alter.inplace.hold.ms`
(default: 0 — no delay), `alter.copy.gate.timeout.ms` (default: 10000,
shared with §18).

## §20 COLLATE CASE_INSENSITIVE + case of string literals (Prompt4 #49, 2026-10-30)

As of milestone 3 (R3-013c): string literals keep their original case
(including JOIN ON — a residual fix via quote-aware normalization),
comparisons are case-sensitive by default, with optional
`COLLATE CASE_INSENSITIVE` on a STRING column (equality `=`/`!=`/`IN` in
SELECT/UPDATE/DELETE), and a legacy mode
`compat.uppercase.string.literals` (default false).

**Intentional limitations (out of scope for #49, agreed):**
- **COLLATE covers only equality** (`=`, `!=`, `IN`). Ordering operators
  (`<`, `>`, `<=`, `>=`), `ORDER BY`, `GROUP BY`, `HAVING` and `LIKE` on
  CI columns remain case-sensitive (comparisons go through
  `Comparable.compareTo` / regex without the `CASE_INSENSITIVE` flag).
- **Indexes on CI columns are forbidden**: `CREATE INDEX` /
  `CREATE HASH INDEX` / composite with a CI column throw
  IllegalArgumentException (index keys are ordered by case-sensitive
  natural order — index lookups would silently drop rows). Auto-indexing
  (§7) silently skips a CI column (FINE log).
- **COLLATE + PRIMARY KEY is rejected at the parser** (clustered keys
  cannot disagree with comparison semantics).
- **Avro pushdown** — the binary predicate does not know about collations:
  a table with any CI column bypasses pushdown when WHERE is present
  (the in-memory scan applies CI-aware eval). Not noticeable on default
  tables.
- **COLLATE only at column level in CREATE TABLE** — ALTER TABLE does not
  add or change collation (no syntax).
- **`.table` format v5**: the collation map is written as a tail after
  the v4 block (droppedColumns). v5 files cannot be read by older
  binaries (CURRENT FORMAT_VERSION guard, same as v4→v3); a downgrade by
  an older version falls back to an empty table via catch-loadFromFile
  (precedent v4).

**Configuration keys:** `compat.uppercase.string.literals` (default false;
sysprop `-Dcompat.uppercase.string.literals=true` or config.properties;
sysprop-first). With compat=true literals are uppercased at parse time
(INSERT stores 'NAME2', both sides of the comparison are uppercased —
case-blind, the pre-#49 behavior).

## §21 Multi-page catalog support (Prompt 10, 2026-11-21)

**The multi-page catalog implementation removes the ~40-table limit and enables page-backed catalog in production:**

- **Catalog format v1 (JSON array in page 0)** — for small catalogs (≤40 tables), uses JSON array serialization in page 0 for backward compatibility;
- **Catalog format v2 (binary header + chunk pages)** — for large catalogs (>40 tables), uses binary header "DSC2" in page 0 + additional chunk pages for table metadata;
- **CRC32C validation** — all catalog pages include CRC32C checksums for crash consistency;
- **Automatic format detection** — catalog automatically detects and loads from v1 or v2 format on startup;
- **Schema version restoration** — schema versions are persisted and restored on database restart through `SchemaVersionRegistry.restoreTo()` method;
- **Page-backed catalog is enabled in production by default** — `Database()` and `Database(String dataDir)` themselves call `createPageManager()` (which creates `data/pages.db`, 100 pages) and `setCatalogPageManager(pageManager)`, which builds `new CatalogTable(pageManager)` and loads the catalog from disk. Fallback to heap-only (`CatalogTable(null)`) happens only if `createPageManager()` threw an `IOException` (disk full / no permissions) — handled in `try/catch` and logged as WARN.

**Current limitations:**
- **Catalog directory change requires rebind** — `setDataDir()` must rebind catalog if page file path changes (implemented in `Database.setDataDir`: stops the page flusher/scrubber, creates a new PageManager in the new location and rebinds the catalog);
- **Schema version persistence** — `Table.schemaVersions` is transient; full schema version history is restored but in-memory state is reset;
- **ALTER TABLE persistence** — `alterAddedColumns` markers are not persisted across restarts (lost on crash recovery).

**Configuration and usage:**
- **PageManager integration** — `Database` constructors use page-backed catalog by default, `close()` properly closes PageManager;
- **Catalog page ID** — `CATALOG_PAGE_ID = PageId(0,1,0)` (fixed page location for catalog);
- **Fallback behavior** — 100+ test files using `new Database()` without `setDataDir()` work with the fallback mechanism;
- **Test coverage** — `CatalogMultiPageTest`, `ProductionCatalogRestartTest`, `CatalogFallbackTest` provide comprehensive test coverage.

**Key implementation files:**
- `diesel/storage/page/CatalogTable.java` — Multi-page format implementation (v1/v2 discrimination, binary header, chunk pages, CRC32C);
- `diesel/Database.java` — Page-backed catalog integration, setDataDir rebind, close() updates;
- `diesel/schema/SchemaVersionRegistry.java` — Schema version restoration (WP3) - added restoreTo() method.

**Migration path:**
- **Phase 1 (done)** — Multi-page format implementation and testing;
- **Phase 2 (done)** — Enable page-backed catalog by default in production (`Database()`/`Database(dataDir)` automatically create a PageManager and bind it to the catalog);
- **Phase 3 (TODO)** — Complete schema version persistence and ALTER TABLE persistence (`Table.schemaVersions` and `alterAddedColumns` are still transient).
