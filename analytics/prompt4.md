# prompt4.md — DieselDB Production-Readiness Implementation Prompts

> **Документ:** prompt4.md  
> **Версия:** 3.0  
> **Дата:** 2026-10-06  
> **Цель:** доведение DieselDB до production-ready (Phase 1 → Phase 3 из `ROADMAP3.md`).  
> **Источники:**
> - `analytics/ROADMAP3.md` — R3-001..R3-045 (45 карточек, полный production-readiness план).
> - `analytics/prompt3.md` — доборочные промпты начиная с #97, не дублирующие ROADMAP3.  
> **Нумерация:** только цифры, 1..159. Сквозная, без префикса «Промпт». Каждый промпт самостоятелен (ранее под-промпты были вложены в родительские; теперь выпрямлены в плоский список).  
> **Принцип составления:** каждый промпт самодостаточен (проблема → контекст → задача → файлы → DoD), может быть передан агенту-исполнителю без дополнительного контекста.  
> **Изменения v3.0:**
> - Под-промпты из v2.0 выпрямлены в самостоятельные промпты со сквозной нумерацией 1..N.
> - Удалён промпт «Parquet storage (нативный)» (R3-043) и все упоминания Parquet как формата хранения.
> - Заголовок промпта комбинирует родительскую тему и под-заголовок шага (для уникальности).

---

## 0. Карта промптов и стратегия выполнения

| # | ID ROADMAP3 | Заголовок | Фаза | Приоритет |
|---|-------------|-----------|------|-----------|
| 1 | R3-001 | MVCC через версионность строк: Версионная строка (xmin/xmax/commandId) + TupleVisibility | 1 | CRITICAL |
| 2 | R3-001 | MVCC через версионность строк: Undo log + TransactionTableSnapshot (без клонирования) | 1 | CRITICAL |
| 3 | R3-001 | MVCC через версионность строк: Vacuum Manager (очистка мёртвых версий) | 1 | CRITICAL |
| 4 | R3-001 | MVCC через версионность строк: Адаптация SelectQuery/InsertQuery/UpdateQuery/DeleteQuery к версиям | 1 | CRITICAL |
| 5 | R3-001 | MVCC через версионность строк: SERIALIZABLE — conflict detection на запись | 1 | CRITICAL |
| 6 | R3-002 | Page-based storage + buffer pool (LRU): Page + PageId + формат сериализации (8/16/64 KB) | 1 | CRITICAL |
| 7 | R3-002 | Page-based storage + buffer pool (LRU): BufferPool (LRU + pinned pages) | 1 | CRITICAL |
| 8 | R3-002 | Page-based storage + buffer pool (LRU): PageManager (чтение/запись через FileChannel + O_DIRECT) | 1 | CRITICAL |
| 9 | R3-002 | Page-based storage + buffer pool (LRU): CatalogTable (системные таблицы в страницах) | 1 | CRITICAL |
| 10 | R3-002 | Page-based storage + buffer pool (LRU): Tablespace stub (один каталог = один tablespace) | 1 | CRITICAL |
| 11 | R3-003 | WAL + group commit: WAL формат + WALEntry (LSN/txid/op/before-after/CRC32C) | 1 | CRITICAL |
| 12 | R3-003 | WAL + group commit: WALManager + WALSegment (сегментированный лог) | 1 | CRITICAL |
| 13 | R3-003 | WAL + group commit: WALWriter (single-writer thread + queue) | 1 | CRITICAL |
| 14 | R3-003 | WAL + group commit: Segment rotation + архивация (gzip) | 1 | CRITICAL |
| 15 | R3-003 | WAL + group commit: GroupCommitCoordinator + AsyncWALWriter | 1 | CRITICAL |
| 16 | R3-004 | ARIES Recovery Manager: CheckpointRecord + checkpoint.ptr (atomic write) | 1 | CRITICAL |
| 17 | R3-004 | ARIES Recovery Manager: Analysis phase (построение active tx list) | 1 | CRITICAL |
| 18 | R3-004 | ARIES Recovery Manager: Redo phase (replay операций с LSN > checkpoint) | 1 | CRITICAL |
| 19 | R3-004 | ARIES Recovery Manager: Undo phase + RecoveryManager (оркестрация на startup) | 1 | CRITICAL |
| 20 | R3-005 | Background writer / flusher: BufferPoolFlusher (адаптивная стратегия) | 1 | HIGH |
| 21 | R3-005 | Background writer / flusher: CheckpointManager (интеграция с WAL) | 1 | HIGH |
| 22 | R3-005 | Background writer / flusher: Fuzzy checkpoint (без quiescent state) | 1 | HIGH |
| 23 | R3-006 | Savepoint + Deadlock + Lock timeout: LockManager (гранулярные блокировки S/X/IS/IX) | 1 | HIGH |
| 24 | R3-006 | Savepoint + Deadlock + Lock timeout: WaitForGraph + DeadlockDetector | 1 | HIGH |
| 25 | R3-006 | Savepoint + Deadlock + Lock timeout: LockTimeoutManager (lock.timeout.ms) | 1 | HIGH |
| 26 | R3-006 | Savepoint + Deadlock + Lock timeout: SavepointManager + NestedSavepointStack | 1 | HIGH |
| 27 | R3-007 | Замена Java Object Serialization в net-протоколе: WireProtocol v2 + MessageCodec (binary) | 1 | CRITICAL |
| 28 | R3-007 | Замена Java Object Serialization в net-протоколе: Message types (Query/Result/Prepare/Batch/Health) | 1 | CRITICAL |
| 29 | R3-007 | Замена Java Object Serialization в net-протоколе: Compression (ZSTD > 4KB) + legacy port compat | 1 | CRITICAL |
| 30 | R3-008 | RBAC + Audit log: User/Role/Privilege модель + password hashing | 1 | HIGH |
| 31 | R3-008 | RBAC + Audit log: Authenticator (handshake) + CLI login | 1 | HIGH |
| 32 | R3-008 | RBAC + Audit log: Authorizer (проверка прав перед каждым запросом) | 1 | HIGH |
| 33 | R3-008 | RBAC + Audit log: AuditLogger (diesel_audit_log table) | 1 | HIGH |
| 34 | R3-009 | SSL/TLS transport: SslContextFactory + TLS handshake handler | 1 | HIGH |
| 35 | R3-009 | SSL/TLS transport: CLI client TLS flags + truststore | 1 | HIGH |
| 36 | R3-009 | SSL/TLS transport: mTLS (client cert) + SNI for multi-tenant | 1 | HIGH |
| 37 | R3-010 | Online schema changes (ALTER без блокировки): SchemaVersion + catalog versioning | 1 | HIGH |
| 38 | R3-010 | Online schema changes (ALTER без блокировки): ALGORITHM=copy (background copy + atomic rename) | 1 | HIGH |
| 39 | R3-010 | Online schema changes (ALTER без блокировки): ALGORITHM=inplace + LOCK=NONE/SHARED/EXCLUSIVE | 1 | HIGH |
| 40 | R3-011 | Backup / Restore (logical + physical): diesel_dump (logical backup CLI) + diesel_restore | 1 | HIGH |
| 41 | R3-011 | Backup / Restore (logical + physical): diesel_backup (physical: WAL checkpoint + pages + tail WAL) | 1 | HIGH |
| 42 | R3-011 | Backup / Restore (logical + physical): IncrementalBackup (только изменившиеся pages) | 1 | HIGH |
| 43 | R3-011 | Backup / Restore (logical + physical): PITR (Point-in-Time Recovery) через WAL replay | 1 | HIGH |
| 44 | R3-012 | Metrics + Prometheus + Health check: MetricsRegistry (counters/histograms/gauges) | 1 | HIGH |
| 45 | R3-012 | Metrics + Prometheus + Health check: PrometheusExporter (/metrics HTTP endpoint) | 1 | HIGH |
| 46 | R3-012 | Metrics + Prometheus + Health check: HealthCheck endpoint (/health JSON) | 1 | HIGH |
| 47 | R3-013 | Fix всех blocking bugs из problems.md: R3-013a: стабильные row-id (CRITICAL correctness fix) | 1 | CRITICAL |
| 48 | R3-013 | Fix всех blocking bugs из problems.md: R3-013b: TRUE/FALSE/NULL как литералы в парсере | 1 | CRITICAL |
| 49 | R3-013 | Fix всех blocking bugs из problems.md: R3-013c: регистр строковых литералов сохраняется | 1 | CRITICAL |
| 50 | R3-013 | Fix всех blocking bugs из problems.md: R3-013d: явная сериализация indexDefinitions | 1 | CRITICAL |
| 51 | R3-014 | Checkpoint + Checksummed pages (CRC32C): CRC32C implementation (hardware-accelerated) | 2 | HIGH |
| 52 | R3-014 | Checkpoint + Checksummed pages (CRC32C): ChecksummedPage (CRC32C в header + verify on read) | 2 | HIGH |
| 53 | R3-014 | Checkpoint + Checksummed pages (CRC32C): Background PageScrubber + checkpoint strategy | 2 | HIGH |
| 54 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers: libpq message protocol (separate port 5432) | 2 | HIGH |
| 55 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers: JDBC driver (diesel-jdbc module) | 2 | HIGH |
| 56 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers: Python + Node.js drivers | 2 | HIGH |
| 57 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers: Go + Rust drivers | 2 | HIGH |
| 58 | R3-016 | Replication (logical + physical + quorum/Raft): Physical streaming replication (WAL streaming to standbys) | 2 | HIGH |
| 59 | R3-016 | Replication (logical + physical + quorum/Raft): Replication slots (защита от WAL удаления) | 2 | HIGH |
| 60 | R3-016 | Replication (logical + physical + quorum/Raft): Synchronous replication (wait for N standby acks) | 2 | HIGH |
| 61 | R3-016 | Replication (logical + physical + quorum/Raft): Logical replication (WAL decoding to change events) | 2 | HIGH |
| 62 | R3-016 | Replication (logical + physical + quorum/Raft): Raft consensus + Failover (multi-master elections) | 2 | HIGH |
| 63 | R3-017 | Partitioning (range / list / hash): PARTITION BY RANGE/LIST/HASH syntax + storage layout | 2 | MEDIUM |
| 64 | R3-017 | Partitioning (range / list / hash): Partition pruning в query optimizer | 2 | MEDIUM |
| 65 | R3-017 | Partitioning (range / list / hash): EXCHANGE PARTITION + subpartitioning | 2 | MEDIUM |
| 66 | R3-018 | Cost-Based Optimizer + статистика: StatisticsCollector (pg_stats analog) | 2 | HIGH |
| 67 | R3-018 | Cost-Based Optimizer + статистика: CostEstimator + access path selection | 2 | HIGH |
| 68 | R3-018 | Cost-Based Optimizer + статистика: Join algorithms cost + plan selection | 2 | HIGH |
| 69 | R3-018 | Cost-Based Optimizer + статистика: Subquery unnesting + PlanCache | 2 | HIGH |
| 70 | R3-019 | UPSERT / RETURNING / UPSERT-on-conflict: ON CONFLICT DO UPDATE / NOTHING (UPSERT) | 2 | HIGH |
| 71 | R3-019 | UPSERT / RETURNING / UPSERT-on-conflict: RETURNING для INSERT/UPDATE/DELETE | 2 | HIGH |
| 72 | R3-019 | UPSERT / RETURNING / UPSERT-on-conflict: Conflict target (column / constraint name) | 2 | HIGH |
| 73 | R3-020 | CI/CD v2 — coverage, matrix, releases: Surefire pattern fix + JaCoCo + Sonar | 2 | HIGH |
| 74 | R3-020 | CI/CD v2 — coverage, matrix, releases: Matrix build (JDK 17/21/25, OS matrix) | 2 | HIGH |
| 75 | R3-020 | CI/CD v2 — coverage, matrix, releases: Release pipeline + Docker + Helm | 2 | HIGH |
| 76 | R3-021 | CTE + рекурсивные CTE: Non-recursive CTE (WITH name AS (SELECT ...)) | 3 | MEDIUM |
| 77 | R3-021 | CTE + рекурсивные CTE: Recursive CTE (WITH RECURSIVE) + termination check | 3 | MEDIUM |
| 78 | R3-021 | CTE + рекурсивные CTE: MATERIALIZED / NOT MATERIALIZED hints + CBO integration | 3 | MEDIUM |
| 79 | R3-022 | Оконные функции: Базовые функции: ROW_NUMBER / RANK / DENSE_RANK / NTILE | 3 | MEDIUM |
| 80 | R3-022 | Оконные функции: Navigation: LAG / LEAD / FIRST_VALUE / LAST_VALUE / NTH_VALUE | 3 | MEDIUM |
| 81 | R3-022 | Оконные функции: Aggregates OVER + Frame (ROWS/RANGE BETWEEN) | 3 | MEDIUM |
| 82 | R3-023 | FULL OUTER JOIN, LATERAL, CROSS APPLY: FULL OUTER JOIN | 3 | MEDIUM |
| 83 | R3-023 | FULL OUTER JOIN, LATERAL, CROSS APPLY: LATERAL JOIN (correlated subquery in FROM) | 3 | MEDIUM |
| 84 | R3-023 | FULL OUTER JOIN, LATERAL, CROSS APPLY: CROSS APPLY / OUTER APPLY (T-SQL compat) | 3 | MEDIUM |
| 85 | R3-024 | UNION / INTERSECT / EXCEPT: UNION + UNION ALL (streaming merge) | 3 | MEDIUM |
| 86 | R3-024 | UNION / INTERSECT / EXCEPT: INTERSECT + EXCEPT (set difference) | 3 | MEDIUM |
| 87 | R3-024 | UNION / INTERSECT / EXCEPT: Precedence + скобки | 3 | MEDIUM |
| 88 | R3-025 | Foreign Keys с CASCADE: FOREIGN KEY + проверка referential integrity | 3 | MEDIUM |
| 89 | R3-025 | Foreign Keys с CASCADE: CASCADE DELETE/UPDATE/SET NULL + multi-level | 3 | MEDIUM |
| 90 | R3-025 | Foreign Keys с CASCADE: Online ADD CONSTRAINT NOT VALID + background validation | 3 | MEDIUM |
| 91 | R3-026 | CHECK constraints, NOT NULL, DEFAULT: CHECK constraint + named constraints | 3 | MEDIUM |
| 92 | R3-026 | CHECK constraints, NOT NULL, DEFAULT: NOT NULL enforcement + DEFAULT expressions | 3 | MEDIUM |
| 93 | R3-026 | CHECK constraints, NOT NULL, DEFAULT: DROP CONSTRAINT + dependency check | 3 | MEDIUM |
| 94 | R3-027 | Materialized Views + refresh: CREATE MATERIALIZED VIEW + REFRESH MANUAL | 3 | LOW |
| 95 | R3-027 | Materialized Views + refresh: REFRESH ON COMMIT + REFRESH EVERY + CONCURRENTLY | 3 | LOW |
| 96 | R3-027 | Materialized Views + refresh: Query rewrite (CBO automatically uses MV) | 3 | LOW |
| 97 | R3-028 | Triggers BEFORE/AFTER: BEFORE triggers (modify NEW.row перед INSERT) | 3 | LOW |
| 98 | R3-028 | Triggers BEFORE/AFTER: AFTER triggers (side effects: audit log, cascade) | 3 | LOW |
| 99 | R3-028 | Triggers BEFORE/AFTER: Statement-level triggers (FOR EACH STATEMENT) | 3 | LOW |
| 100 | R3-029 | VIEW (non-materialized): CREATE VIEW + SELECT * FROM view (expansion) | 3 | LOW |
| 101 | R3-029 | VIEW (non-materialized): Updatable views (INSERT/UPDATE/DELETE through view) | 3 | LOW |
| 102 | R3-029 | VIEW (non-materialized): CHECK OPTION + DROP TABLE/VIEW dependency | 3 | LOW |
| 103 | R3-030 | Full-text search: Tokenizer + Stemmer (Snowball) | 3 | LOW |
| 104 | R3-030 | Full-text search: FullTextIndex (GIN-структура) + MATCH/AGAINST | 3 | LOW |
| 105 | R3-030 | Full-text search: Relevance scoring (BM25) + Highlighter | 3 | LOW |
| 106 | R3-031 | Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: JSONB (бинарный JSON + индексация по пути) | 3 | MEDIUM |
| 107 | R3-031 | Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: UUID + ARRAY | 3 | MEDIUM |
| 108 | R3-031 | Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: ENUM + INTERVAL | 3 | MEDIUM |
| 109 | R3-031 | Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: INET/CIDR + BIT | 3 | MEDIUM |
| 110 | R3-032 | Хранимые функции (SQL-only, без PL/pgSQL): CREATE FUNCTION + RETURNS | 3 | LOW |
| 111 | R3-032 | Хранимые функции (SQL-only, без PL/pgSQL): Volatility (IMMUTABLE/STABLE/VOLATILE) + inlining | 3 | LOW |
| 112 | R3-032 | Хранимые функции (SQL-only, без PL/pgSQL): TABLE-returning + named arguments | 3 | LOW |
| 113 | R3-033 | Шардинг + distributed query planner: CREATE SHARDED TABLE + ShardMap | 3 | MEDIUM |
| 114 | R3-033 | Шардинг + distributed query planner: Distributed query planner (push-down + fan-out) | 3 | MEDIUM |
| 115 | R3-033 | Шардинг + distributed query planner: Distributed JOIN (co-located + broadcast) + 2PC writes | 3 | MEDIUM |
| 116 | R3-034 | 2PC distributed transactions: TwoPhaseCommitCoordinator (PREPARE + COMMIT/ABORT) | 3 | LOW |
| 117 | R3-034 | 2PC distributed transactions: CoordinatorLog (persistent state machine) | 3 | LOW |
| 118 | R3-034 | 2PC distributed transactions: Timeout + failure handling | 3 | LOW |
| 119 | R3-035 | Row-Level Security (RLS): CREATE POLICY + ENABLE ROW LEVEL SECURITY | 3 | MEDIUM |
| 120 | R3-035 | Row-Level Security (RLS): Multiple policies (OR / AND combination) | 3 | MEDIUM |
| 121 | R3-035 | Row-Level Security (RLS): BYPASSRLS + current_tenant() function | 3 | MEDIUM |
| 122 | R3-036 | Column-level privileges + masking | 3 | LOW |
| 123 | R3-037 | TDE at rest encryption: AES-256-GCM page cipher + MasterKeyProvider | 3 | MEDIUM |
| 124 | R3-037 | TDE at rest encryption: KMS providers (AWS KMS, Vault) + key rotation | 3 | MEDIUM |
| 125 | R3-037 | TDE at rest encryption: Tablespace-level encryption + WAL encryption | 3 | MEDIUM |
| 126 | R3-038 | Vectorized execution (batch): Batch container (columnar) + VectorizedScan | 3 | MEDIUM |
| 127 | R3-038 | Vectorized execution (batch): VectorizedFilter + VectorizedProject + expressions | 3 | MEDIUM |
| 128 | R3-038 | Vectorized execution (batch): VectorizedAggregate + RowBatchAdapter (interop) | 3 | MEDIUM |
| 129 | R3-039 | Adaptive joins (runtime switching): AdaptiveJoinExecutor (runtime cardinality check) | 3 | LOW |
| 130 | R3-039 | Adaptive joins (runtime switching): RuntimeStatisticsCollector | 3 | LOW |
| 131 | R3-039 | Adaptive joins (runtime switching): PlanFeedback (persistent для будущих планов) | 3 | LOW |
| 132 | R3-040 | Parallel query scan + aggregation: ParallelScanExecutor + RangeSplitter | 3 | MEDIUM |
| 133 | R3-040 | Parallel query scan + aggregation: ParallelAggregationExecutor + Gather merge | 3 | MEDIUM |
| 134 | R3-040 | Parallel query scan + aggregation: Adaptive parallelism + partition-aware | 3 | MEDIUM |
| 135 | R3-041 | Bitmap indexes: BitmapIndex + CompressedBitmap (WAH) | 3 | LOW |
| 136 | R3-041 | Bitmap indexes: Bitwise operations (AND/OR/NOT) + BitmapScanExecutor | 3 | LOW |
| 137 | R3-041 | Bitmap indexes: CBO integration (cost model for bitmap scan) | 3 | LOW |
| 138 | R3-042 | Covering indexes (INCLUDE) | 3 | LOW |
| 139 | R3-044 | TPC-C / TPC-H сертификация: TPC-C workload (10 warehouses, ACID проверка) | 3 | LOW |
| 140 | R3-044 | TPC-C / TPC-H сертификация: TPC-H (22 queries, SF=1, results verification) | 3 | LOW |
| 141 | R3-044 | TPC-C / TPC-H сертификация: Regression tracking + public benchmark report | 3 | LOW |
| 142 | R3-045 | GUI админ-панель (аналог pgAdmin): Web UI dashboard + schema viewer | 3 | LOW |
| 143 | R3-045 | GUI админ-панель (аналог pgAdmin): SQL editor + EXPLAIN visualizer | 3 | LOW |
| 144 | R3-045 | GUI админ-панель (аналог pgAdmin): User management + Backup UI + Replication monitor | 3 | LOW |
| 145 | Промпт 111 | Lock — deadlock prevention стратегии: Wait-die + Wound-wait стратегии | — | MEDIUM |
| 146 | Промпт 111 | Lock — deadlock prevention стратегии: No-wait (immediate abort on conflict) | — | MEDIUM |
| 147 | Промпт 111 | Lock — deadlock prevention стратегии: Config switch + benchmark comparison | — | MEDIUM |
| 148 | Промпт 113 | ALTER TABLE ADD COLUMN — базовый SQL | — | HIGH |
| 149 | Промпт 114 | ALTER TABLE DROP COLUMN — базовый SQL | — | MEDIUM |
| 150 | Промпт 116 | DROP INDEX | — | MEDIUM |
| 151 | Промпт 117 | TRUNCATE TABLE | — | HIGH |
| 152 | Промпт 118 | CREATE SEQUENCE | — | MEDIUM |
| 153 | Промпт 119 | DROP SEQUENCE | — | LOW |
| 154 | Промпт 120 | Query Result Cache | — | MEDIUM |
| 155 | Промпт 121 | Bulk Insert / Copy API: BULK INSERT FROM file (CSV/TSV/JSONL/AVRO) | — | HIGH |
| 156 | Промпт 121 | Bulk Insert / Copy API: PostgreSQL COPY FROM / COPY TO compat | — | HIGH |
| 157 | Промпт 121 | Bulk Insert / Copy API: Batching + error handling + progress | — | HIGH |
| 158 | Промпт 125 | Virtual Threads для concurrency | — | LOW |
| 159 | Промпт 126 | Record Patterns для чистоты кода | — | LOW |

---

**Дедупликация против `prompt3.md` (97-131):** в `prompt4.md` НЕ переносятся
следующие промпты, поскольку их функционал покрыт карточками ROADMAP3:

| Промпт prompt3.md | Дублирован в ROADMAP3 | Обоснование |
|-------------------|------------------------|-------------|
| 97 WAL basic | R3-003 | R3-003 включает WAL basics + group commit + segment rotation + WALWriter. |
| 98 WAL recovery | R3-004 | R3-004 (ARIES) покрывает recovery целиком. |
| 99 ARIES | R3-004 | Идентично. |
| 100 Checkpoint Manager | R3-005 + R3-014 | Background writer + checkpoint+CRC. |
| 101 Fuzzy vs sharp checkpoint | R3-014 | Fuzzy checkpoint — явная часть R3-014. |
| 102 Checksummed Page | R3-014 | CRC32C на страницах — ядро R3-014. |
| 103 CRC32C algorithm | R3-014 | Тот же алгоритм в составе R3-014. |
| 104 Deadlock Detector | R3-006 | WaitForGraph + DeadlockDetector — часть R3-006. |
| 105 Lock Timeout Manager | R3-006 | LockTimeout + Exception — часть R3-006. |
| 106 Savepoint Manager | R3-006 | SavepointManager — часть R3-006. |
| 107 Nested savepoints | R3-006 | Иерархия savepoint — часть R3-006. |
| 108 WAL segment rotation | R3-003 | Сегментная ротация и архивация — часть R3-003. |
| 109 WAL async write / group commit | R3-003 | Group commit + WALWriter — часть R3-003. |
| 110 PITR | R3-011 | PITR — часть R3-011 Backup/Restore. |
| 115 UNION/INTERSECT/EXCEPT | R3-024 | Полное совпадение. |
| 122 Bitmap Indexes | R3-041 | Полное совпадение. |
| 123 Parallel Scan | R3-040 | Полное совпадение. |
| 124 Parallel Aggregation | R3-040 | Полное совпадение. |
| 127 Materialized Views | R3-027 | Полное совпадение. |
| 128 Foreign Keys CASCADE | R3-025 | Полное совпадение. |
| 129 CHECK Constraints | R3-026 | Полное совпадение. |
| 130 Full Text Search | R3-030 | Полное совпадение. |
| 131 Window Functions | R3-022 | Полное совпадение. |

**Промпт R3-043 (Parquet storage) удалён** из плана. Parquet не
рассматривается как целевой формат хранения (после консультации
владельца проекта). Все упоминания Parquet как storage removed.

---

## Фаза 1 — Critical Production-Readiness (3-6 месяцев)

**Цель:** закрыть 13 CRITICAL/HIGH дыр, без которых DieselDB **нельзя** назвать production-ready даже для single-tenant low-stakes применений. По завершении — метрика готовности ≥ 55 %.

### 1. MVCC через версионность строк: Версионная строка (xmin/xmax/commandId) + TupleVisibility

**ID ROADMAP3:** R3-001  
**Категория:** B. Concurrency  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое, не покрыто. В `Roadmap.md` упомянуто как «уже есть», но фактически уровни изоляции реализованы через глубокое клонирование таблиц сериализацией — это не MVCC. `problems.md` §Транзакции-1 описывает проблему.

**Проблема.**

Текущий `Transaction.cloneTable()` использует `ByteArrayOutputStream` + `ObjectOutputStream` для глубокого клонирования таблиц при `BEGIN TRANSACTION` и `COMMIT`. На таблице 100k строк × 5 индексов это секунды на каждое BEGIN. Нет версионности строк, нет tuple visibility check, нет undo log для read-side snapshot. Десятки параллельных транзакций невозможны физически.

**Контекст этого шага.**

Шаг 1/5 из плана MVCC. Фундамент: вводится версионность на уровне строк и контракт видимости. До этого шага интеграции с `Transaction`/`SelectQuery` нет — она появится в шагах 2 и 4.

**Задача.**

1. Расширить `Row` контейнерами `xmin` (txid, создавший версию), `xmax` (txid, удаливший версию) и `commandId` (для within-tx видимости собственных изменений).
2. Реализовать `TupleVisibility.visible(row, snapshotTxid, isolationLevel)` с раздельной логикой для READ COMMITTED, REPEATABLE READ, SERIALIZABLE.
3. Юнит-тесты `TupleVisibilityTest` на каждый уровень изоляции × 6 базовых сценариев (insert+commit, insert+rollback, update, delete, self-write видимость, чужой незакоммиченный).
4. Контракт: `xmin > snapshot` → невидима; `xmax <= snapshot && xmax != 0` → невидима; иначе видна.

**Ключевые файлы.**

- `diesel/Row.java (контейнер xmin/xmax/commandId)`
- `diesel/TupleVisibility.java (новый, visibility per isolation level)`
- `diesel/IsolationLevel.java (если отсутствует — enum)`

**Критерии приёмки.**

- [ ] Row хранит xmin/xmax/commandId, сериализация устойчива к restart.
- [ ] TupleVisibilityTest: ≥ 24 сценария (3 уровня × 8 кейсов), все green.
- [ ] Документация: doc/mvcc/visibility.md описывает правила видимости.

---

### 2. MVCC через версионность строк: Undo log + TransactionTableSnapshot (без клонирования)

**ID ROADMAP3:** R3-001  
**Категория:** B. Concurrency  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, не покрыто. В `Roadmap.md` упомянуто как «уже есть», но фактически уровни изоляции реализованы через глубокое клонирование таблиц сериализацией — это не MVCC. `problems.md` §Транзакции-1 описывает проблему.

**Проблема.**

Текущий `Transaction.cloneTable()` использует `ByteArrayOutputStream` + `ObjectOutputStream` для глубокого клонирования таблиц при `BEGIN TRANSACTION` и `COMMIT`. На таблице 100k строк × 5 индексов это секунды на каждое BEGIN. Нет версионности строк, нет tuple visibility check, нет undo log для read-side snapshot. Десятки параллельных транзакций невозможны физически.

**Контекст этого шага.**

Шаг 2/5. Опирается на шаг 1 (TupleVisibility). Заменяет текущий клон `Transaction.cloneTable()` на легковесный snapshot. Шаги 3 (Vacuum) и 4 (адаптация DML) — отдельно.

**Задача.**

1. Реализовать `UndoLog`: хранит обратные операции (insert → delete, update → restore old version) для текущей транзакции. In-memory с spill в temp-файл при превышении `undo.spill.threshold.mb`.
2. Реализовать `TransactionTableSnapshot`: view над физической таблицей через TupleVisibility, без копирования данных.
3. Переработать `Transaction`: убрать `cloneTable()`, ввести `getSnapshot(table)`.
4. ROLLBACK: применить UndoLog в обратном порядке, очистить in-memory версии, пометить все свои версии как aborted (xmax = txid).

**Ключевые файлы.**

- `diesel/UndoLog.java (новый)`
- `diesel/TransactionTableSnapshot.java (новый, замена cloneTable)`
- `diesel/Transaction.java (переработка BEGIN/ROLLBACK)`

**Критерии приёмки.**

- [ ] BEGIN TRANSACTION на 100k-строчной таблице < 1 ms (было секунды).
- [ ] RollbackTest: 1000 операций в транзакции, ROLLBACK → состояние восстановлено точно, heap не растёт.
- [ ] UndoLog spill: при 100k вставок + `undo.spill.threshold.mb=1` — spill корректно пишет и читает temp-файл.

---

### 3. MVCC через версионность строк: Vacuum Manager (очистка мёртвых версий)

**ID ROADMAP3:** R3-001  
**Категория:** B. Concurrency  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое, не покрыто. В `Roadmap.md` упомянуто как «уже есть», но фактически уровни изоляции реализованы через глубокое клонирование таблиц сериализацией — это не MVCC. `problems.md` §Транзакции-1 описывает проблему.

**Проблема.**

Текущий `Transaction.cloneTable()` использует `ByteArrayOutputStream` + `ObjectOutputStream` для глубокого клонирования таблиц при `BEGIN TRANSACTION` и `COMMIT`. На таблице 100k строк × 5 индексов это секунды на каждое BEGIN. Нет версионности строк, нет tuple visibility check, нет undo log для read-side snapshot. Десятки параллельных транзакций невозможны физически.

**Контекст этого шага.**

Шаг 3/5. Опирается на шаги 1–2. Без Vacuum heap будет расти бесконечно. Шаги 4–5 (DML-адаптация и SERIALIZABLE) не зависят от этого шага, могут делаться параллельно.

**Задача.**

1. Реализовать `VacuumManager` — background thread (плановый запуск по `vacuum.interval.ms`, default 60 sec).
2. Алгоритм: для каждой таблицы обходит версии, удаляет те, у которых `xmax < oldestActiveTx && xmax != 0`.
3. После физической очистки — обновить индексы (через `IndexManager.removeDeadEntries`).
4. Метрики `vacuum.duration.ms`, `vacuum.dead_tuples_removed`, экспорт в `MetricsRegistry` (если доступен — интеграция в промпте 12).

**Ключевые файлы.**

- `diesel/VacuumManager.java (новый)`
- `diesel/storage/DelimitedIndexManager.java (метод removeDeadEntries)`

**Критерии приёмки.**

- [ ] VacuumTest: 1M inserts + 100k deletes + VACUUM → heap уменьшается ≥ 30 % (verified via JMX).
- [ ] Vacuum не блокирует writers > 100 ms на 1M-строчной таблице.
- [ ] Auto-vacuum включается по расписанию, ручной `VACUUM table_name` работает из CLI.

---

### 4. MVCC через версионность строк: Адаптация SelectQuery/InsertQuery/UpdateQuery/DeleteQuery к версиям

**ID ROADMAP3:** R3-001  
**Категория:** B. Concurrency  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое, не покрыто. В `Roadmap.md` упомянуто как «уже есть», но фактически уровни изоляции реализованы через глубокое клонирование таблиц сериализацией — это не MVCC. `problems.md` §Транзакции-1 описывает проблему.

**Проблема.**

Текущий `Transaction.cloneTable()` использует `ByteArrayOutputStream` + `ObjectOutputStream` для глубокого клонирования таблиц при `BEGIN TRANSACTION` и `COMMIT`. На таблице 100k строк × 5 индексов это секунды на каждое BEGIN. Нет версионности строк, нет tuple visibility check, нет undo log для read-side snapshot. Десятки параллельных транзакций невозможны физически.

**Контекст этого шага.**

Шаг 4/5. Опирается на шаги 1–2 (TupleVisibility, UndoLog). Без этого шага DML не использует новую версионность. Шаг 5 (SERIALIZABLE conflict detection) — сверху.

**Задача.**

1. `SelectQuery`: при сканировании фильтровать строки через `TupleVisibility.visible(row, snapshotTxid, isolationLevel)`.
2. `InsertQuery`: создавать версию с `xmin = currentTxid`, `xmax = 0`.
3. `UpdateQuery`: не мутировать строку — создать новую версию, старую пометить `xmax = currentTxid`; в UndoLog записать обратное.
4. `DeleteQuery`: пометить `xmax = currentTxid` (без физического удаления — это делает Vacuum).
5. ConcurrentConflictTest расширен до 100 параллельных писателей.

**Ключевые файлы.**

- `diesel/SelectQuery.java`
- `diesel/InsertQuery.java`
- `diesel/UpdateQuery.java`
- `diesel/DeleteQuery.java`

**Критерии приёмки.**

- [ ] ConcurrentConflictTest (100 writers) проходит без deadlock/exception.
- [ ] READ COMMITTED видит данные, закоммиченные до начала оператора; REPEATABLE READ — до начала транзакции.
- [ ] UpdateTest: UPDATE → SELECT в той же транзакции видит новое значение, ROLLBACK → старое.

---

### 5. MVCC через версионность строк: SERIALIZABLE — conflict detection на запись

**ID ROADMAP3:** R3-001  
**Категория:** B. Concurrency  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент)  
**Зависимости этого шага:** шаги 1, 2, 4  
**Связь с prompt3.md:** новое, не покрыто. В `Roadmap.md` упомянуто как «уже есть», но фактически уровни изоляции реализованы через глубокое клонирование таблиц сериализацией — это не MVCC. `problems.md` §Транзакции-1 описывает проблему.

**Проблема.**

Текущий `Transaction.cloneTable()` использует `ByteArrayOutputStream` + `ObjectOutputStream` для глубокого клонирования таблиц при `BEGIN TRANSACTION` и `COMMIT`. На таблице 100k строк × 5 индексов это секунды на каждое BEGIN. Нет версионности строк, нет tuple visibility check, нет undo log для read-side snapshot. Десятки параллельных транзакций невозможны физически.

**Контекст этого шага.**

Шаг 5/5. Завершает MVCC. Опирается на шаги 1–4. Без него SERIALIZABLE деградирует к REPEATABLE READ (некорректно).

**Задача.**

1. На UPDATE/DELETE в режиме SERIALIZABLE: проверить, что целевая версия не была изменена другой закоммиченной транзакцией после начала нашей.
2. Если изменена — `SerializationFailureException` (транзакция должна быть повторена клиентом).
3. Контракт: SSI (Serializable Snapshot Isolation) — упрощённая версия: detect rw-conflicts, abort жертву с минимальным txid.
4. Тест на chain of 1000 transactions (вложенные BEGIN/COMMIT), heap не растёт.

**Ключевые файлы.**

- `diesel/TupleVisibility.java (расширение для SSI)`
- `diesel/SerializationFailureException.java (новый)`
- `diesel/concurrency/ConflictDetector.java (новый)`

**Критерии приёмки.**

- [ ] SerializableTest: 2 писателя на одну строку — один успешно коммитит, второй получает SerializationFailureException.
- [ ] ChainOf1000TxTest: 1000 вложенных BEGIN/COMMIT, heap не растёт после vacuum, ни одной ошибки.
- [ ] Документация: doc/mvcc/serializable.md с описанием detect/abort policy.

---

### 6. Page-based storage + buffer pool (LRU): Page + PageId + формат сериализации (8/16/64 KB)

**ID ROADMAP3:** R3-002  
**Категория:** C. Storage engine  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент, концептуально не блокирует R3-001)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `problems.md` (Map-per-row ~48 байт на entry).

**Проблема.**

Текущее хранилище — `List<Map<String,Object>>` в куче JVM. На 1M строк × 20 колонок это ~2 GB heap (5-10× overhead от автобоксинга). Нет понятия страницы, нет buffer pool, нет eviction: данные либо в памяти, либо на диске через CSV/JSONL/AVRO. Тяжёлые таблицы физически не помещаются.

**Контекст этого шага.**

Шаг 1/5. Вводит низкоуровневую единицу хранения. Не зависит от остальных шагов; шаги 2 (BufferPool) и 3 (PageManager) строятся на этом.

**Задача.**

1. Реализовать `Page` (фиксированный размер 8 KB / 16 KB / 64 KB — настраивается через `page.size`). Содержит header (pageId, LSN, checksum placeholder, free-space-pointer) + payload (слоты строк).
2. `PageId` — tablespaceId + fileId + pageNum (Long-адресация).
3. Формат строк в странице: slotted-page (offset + length + tuple bytes), с defrag-методом для compactification.
4. Сериализация: `Page.writeTo(ByteBuffer)` / `Page.readFrom(ByteBuffer)` — явный binary layout, без Java-сериализации.

**Ключевые файлы.**

- `diesel/storage/page/Page.java`
- `diesel/storage/page/PageId.java`
- `diesel/storage/page/PageHeader.java`
- `diesel/storage/page/SlottedPageLayout.java`

**Критерии приёмки.**

- [ ] PageTest: page 8KB вмещает 100 строк по 50 байт, записывает и читает round-trip без потерь.
- [ ] DefragTest: 50 вставок + 25 удалений → defrag восстанавливает free space ≥ 30 % страницы.
- [ ] Page size configurable: 8K/16K/64K проходят тест.

---

### 7. Page-based storage + buffer pool (LRU): BufferPool (LRU + pinned pages)

**ID ROADMAP3:** R3-002  
**Категория:** C. Storage engine  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент, концептуально не блокирует R3-001)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `problems.md` (Map-per-row ~48 байт на entry).

**Проблема.**

Текущее хранилище — `List<Map<String,Object>>` в куче JVM. На 1M строк × 20 колонок это ~2 GB heap (5-10× overhead от автобоксинга). Нет понятия страницы, нет buffer pool, нет eviction: данные либо в памяти, либо на диске через CSV/JSONL/AVRO. Тяжёлые таблицы физически не помещаются.

**Контекст этого шага.**

Шаг 2/5. Опирается на шаг 1 (Page). Шаг 3 (PageManager) пользуется BufferPool; шаги 4 (catalog) и 5 (tablespace) — выше.

**Задача.**

1. Реализовать `BufferPool` с LRU eviction и pinned pages (для системных каталогов).
2. Размер буфера — `bufferpool.size.mb` (default 256 MB).
3. `pin(pageId)` / `unpin(pageId)` — AutoCloseable через `PinnedPage implements AutoCloseable`.
4. Eviction: при переполнении найти unpinned LRU, вытеснить (dirty → flush, clean → discard).
5. `BufferPoolMXBean` экспонирует hit/miss/pinned counters в JMX.

**Ключевые файлы.**

- `diesel/storage/page/BufferPool.java`
- `diesel/storage/page/PinnedPage.java`
- `diesel/storage/page/LruEvictionPolicy.java`
- `diesel/storage/page/BufferPoolMXBean.java`

**Критерии приёмки.**

- [ ] BufferPoolStressTest: 10 потоков × 100k pin/unpin операций, не падает, не теряет данные.
- [ ] Hit rate > 95 % на workload `SELECT by PK` 1M запросов на 100MB буфера и 1GB данных.
- [ ] LRU eviction корректно вытесняет холодные страницы под давлением (verified via MXBean).

---

### 8. Page-based storage + buffer pool (LRU): PageManager (чтение/запись через FileChannel + O_DIRECT)

**ID ROADMAP3:** R3-002  
**Категория:** C. Storage engine  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент, концептуально не блокирует R3-001)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `problems.md` (Map-per-row ~48 байт на entry).

**Проблема.**

Текущее хранилище — `List<Map<String,Object>>` в куче JVM. На 1M строк × 20 колонок это ~2 GB heap (5-10× overhead от автобоксинга). Нет понятия страницы, нет buffer pool, нет eviction: данные либо в памяти, либо на диске через CSV/JSONL/AVRO. Тяжёлые таблицы физически не помещаются.

**Контекст этого шага.**

Шаг 3/5. Опирается на шаги 1–2. Чтение/запись страниц на диск через BufferPool. Шаг 4 (catalog) использует PageManager.

**Задача.**

1. Реализовать `PageManager`: чтение/запись страниц через `FileChannel` (опционально O_DIRECT на Linux через JNI, fallback на `FileChannel.map`).
2. `readPage(pageId)` → fetch from BufferPool, если miss — read from disk.
3. `writePage(page)` → mark dirty in BufferPool; физическая запись через flusher (см. промпт 5).
4. `allocatePage(tablespaceId)` — extend file, вернуть новый pageId.
5. Атомарная запись: temp file + rename для crash safety.

**Ключевые файлы.**

- `diesel/storage/page/PageManager.java`
- `diesel/storage/page/AtomicFileWriter.java`
- `diesel/storage/page/FileChannelIO.java`

**Критерии приёмки.**

- [ ] PageManagerTest: запись 10k страниц + рестарт JVM + чтение — все 10k страниц корректны.
- [ ] CrashTest: simulate kill -9 во время flush — данные на диске консистентны (после recovery в промпте 4).
- [ ] Throughput: 50k pages/sec при буфере 256 MB.

---

### 9. Page-based storage + buffer pool (LRU): CatalogTable (системные таблицы в страницах)

**ID ROADMAP3:** R3-002  
**Категория:** C. Storage engine  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент, концептуально не блокирует R3-001)  
**Зависимости этого шага:** шаги 1, 2, 3  
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `problems.md` (Map-per-row ~48 байт на entry).

**Проблема.**

Текущее хранилище — `List<Map<String,Object>>` в куче JVM. На 1M строк × 20 колонок это ~2 GB heap (5-10× overhead от автобоксинга). Нет понятия страницы, нет buffer pool, нет eviction: данные либо в памяти, либо на диске через CSV/JSONL/AVRO. Тяжёлые таблицы физически не помещаются.

**Контекст этого шага.**

Шаг 4/5. Опирается на шаги 1–3. Перенос метаданных (схемы, индексы, sequence state) в страницы — чтобы survive restart. Шаг 5 (tablespace) — слой выше.

**Задача.**

1. Реализовать `CatalogTable` — системная таблица, хранящая схему каждой user table (колонки, типы, индексы, sequence bindings).
2. Schema сериализуется в JSON (Jackson) в специальной странице `pageId=0` каждого tablespace.
3. `CREATE TABLE` / `ALTER TABLE` обновляют catalog page + flush синхронно (до ACK клиенту).
4. На старте сервера: `CatalogTable.load()` читает page 0, восстанавливает in-memory схему.

**Ключевые файлы.**

- `diesel/storage/page/CatalogTable.java`
- `diesel/storage/page/CatalogSchema.java`
- `diesel/DatabaseServer.java (загрузка catalog на startup)`

**Критерии приёмки.**

- [ ] CatalogTest: CREATE TABLE + restart → схема полностью восстановлена.
- [ ] ALTER TABLE ADD COLUMN (после промпта 47) обновляет catalog атомарно.
- [ ] Коррумпирование catalog page (имитация) → понятная ошибка + путь восстановления из backup (промпт 11).

---

### 10. Page-based storage + buffer pool (LRU): Tablespace stub (один каталог = один tablespace)

**ID ROADMAP3:** R3-002  
**Категория:** C. Storage engine  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (фундамент, концептуально не блокирует R3-001)  
**Зависимости этого шага:** шаги 1, 2, 3, 4  
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `problems.md` (Map-per-row ~48 байт на entry).

**Проблема.**

Текущее хранилище — `List<Map<String,Object>>` в куче JVM. На 1M строк × 20 колонок это ~2 GB heap (5-10× overhead от автобоксинга). Нет понятия страницы, нет buffer pool, нет eviction: данные либо в памяти, либо на диске через CSV/JSONL/AVRO. Тяжёлые таблицы физически не помещаются.

**Контекст этого шага.**

Шаг 5/5. Опирается на шаги 1–4. Заглушка для будущего multi-tablespace (расширение — Фаза 2).

**Задача.**

1. Реализовать `Tablespace` как каталог на диске с набором файлов страниц (`file-001.dat`, `file-002.dat`, ...).
2. Default tablespace: `data/` (один каталог, один tablespace).
3. Конфиг: `tablespace.default.path` (default `./data`).
4. Расширение файла: при заполнении последнего файла до `tablespace.file.max.size.mb` создаётся новый.
5. Подготовить контракт для multi-tablespace (интерфейс `TablespaceRegistry`), но реализация — заглушка на 1 tablespace.

**Ключевые файлы.**

- `diesel/storage/tablespace/Tablespace.java`
- `diesel/storage/tablespace/TablespaceRegistry.java`
- `diesel/storage/tablespace/TablespaceFile.java`
- `diesel/ConfigLoader.java (tablespace.default.path)`

**Критерии приёмки.**

- [ ] TablespaceTest: заполнение 1GB данных → создаются 16 файлов по 64MB каждый.
- [ ] Restart: tablespaces корректно reopened, все страницы доступны.
- [ ] Контракт `TablespaceRegistry.create(name)` присутствует, но выбрасывает `UnsupportedOperationException` (заглушка для Фазы 2).

---

### 11. WAL + group commit: WAL формат + WALEntry (LSN/txid/op/before-after/CRC32C)

**ID ROADMAP3:** R3-003  
**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет Промпт 97 (WAL базовая реализация). Промпт 97 не покрывает group commit — без этого latency коммита недопустимая на проде. Также поглощает Промпты 108 (segment rotation) и 109 (async write / group commit).

**Проблема.**

Сейчас на COMMIT происходит `saveTablesToDisk()` — сериализация всех таблиц полностью. Для таблицы 100k строк это секунды. Fsync на каждый commit убьёт throughput (десятки commits/sec максимум). Аварийное отключение теряет все изменения с последнего `saveTablesToDisk()`. Group commit не упоминается ни в одном документе проекта.

**Контекст этого шага.**

Шаг 1/5. Определяет on-disk формат WAL. Не зависит от других шагов. Шаги 2 (WALManager), 3 (WALWriter), 4 (segment rotation), 5 (group commit) — поверх.

**Задача.**

1. Спроектировать `WALEntry`: LSN (8 bytes monotonic), txid (8 bytes), op (1 byte: INSERT/UPDATE/DELETE/COMMIT/ABORT/TRUNCATE/CHECKPOINT), before-image (optional), after-image (optional), CRC32C (4 bytes).
2. Бинарный формат: length-prefixed, alignment 8 bytes для direct-IO compatibility.
3. Сериализация: `WALEntry.writeTo(ByteBuffer)` / `readFrom(ByteBuffer)` — явная, без Java-сериализации.
4. CRC32C: использовать `java.util.zip.CRC32C` (hardware-accelerated через JNI в промпте 14).

**Ключевые файлы.**

- `diesel/wal/WALEntry.java`
- `diesel/wal/WALFormat.java`
- `diesel/wal/WALOpcode.java`

**Критерии приёмки.**

- [ ] WALEntryTest: round-trip всех opcodes, корректная сериализация before/after images.
- [ ] CRC32C verification: битый байт в payload → `InvalidCRCException`.
- [ ] Формат документирован: docs/wal/format.md с диаграммой byte layout.

---

### 12. WAL + group commit: WALManager + WALSegment (сегментированный лог)

**ID ROADMAP3:** R3-003  
**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет Промпт 97 (WAL базовая реализация). Промпт 97 не покрывает group commit — без этого latency коммита недопустимая на проде. Также поглощает Промпты 108 (segment rotation) и 109 (async write / group commit).

**Проблема.**

Сейчас на COMMIT происходит `saveTablesToDisk()` — сериализация всех таблиц полностью. Для таблицы 100k строк это секунды. Fsync на каждый commit убьёт throughput (десятки commits/sec максимум). Аварийное отключение теряет все изменения с последнего `saveTablesToDisk()`. Group commit не упоминается ни в одном документе проекта.

**Контекст этого шага.**

Шаг 2/5. Опирается на шаг 1. Управляет файлами сегментов WAL. Шаги 3–5 — сверху.

**Задача.**

1. Реализовать `WALManager`: создаёт/открывает сегменты `wal-0001.log`, `wal-0002.log`, ... (каждый до 64 MB).
2. `WALSegment`: file channel + позиция, методы append/read.
3. Текущий сегмент — `currentSegment`, через `getCurrent()`.
4. LSN allocator: monotonic global counter, persisted в `checkpoint.ptr` (см. промпт 4).
5. Config: `wal.dir` (default `./wal`), `wal.segment.max.size.mb` (default 64).

**Ключевые файлы.**

- `diesel/wal/WALManager.java`
- `diesel/wal/WALSegment.java`
- `diesel/wal/WALConfig.java`

**Критерии приёмки.**

- [ ] WALManagerTest: append 10k entries → корректно читаются обратно по LSN.
- [ ] Segment rotation: при 64 MB создаётся wal-0002.log автоматически.
- [ ] Restart: WAL reopened, currentSegment determined correctly.

---

### 13. WAL + group commit: WALWriter (single-writer thread + queue)

**ID ROADMAP3:** R3-003  
**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** уточняет Промпт 97 (WAL базовая реализация). Промпт 97 не покрывает group commit — без этого latency коммита недопустимая на проде. Также поглощает Промпты 108 (segment rotation) и 109 (async write / group commit).

**Проблема.**

Сейчас на COMMIT происходит `saveTablesToDisk()` — сериализация всех таблиц полностью. Для таблицы 100k строк это секунды. Fsync на каждый commit убьёт throughput (десятки commits/sec максимум). Аварийное отключение теряет все изменения с последнего `saveTablesToDisk()`. Group commit не упоминается ни в одном документе проекта.

**Контекст этого шага.**

Шаг 3/5. Опирается на шаги 1–2. Single-writer для сериализации append операций. Шаг 4 (rotation), 5 (group commit) — выше.

**Задача.**

1. Реализовать `WALWriter` — single background thread, consumes из `BlockingQueue<WALEntry>`.
2. Все writers (InsertQuery/UpdateQuery/...) enqueue, не обращаются к файлу напрямую.
3. Backpressure: при queue size > `wal.queue.max.size` (default 100k) — блокирующая запись (backpressure to clients).
4. `flush()`: форсировать fsync текущего сегмента.
5. Метрики: `wal.queue.size`, `wal.append.latency.p99`.

**Ключевые файлы.**

- `diesel/wal/WALWriter.java`
- `diesel/wal/WALQueue.java`

**Критерии приёмки.**

- [ ] WALWriterTest: 100 потоков × 1000 inserts → все записи в WAL, LSN strictly monotonic.
- [ ] BackpressureTest: при queue full — клиент блокируется, не теряет данные.
- [ ] Throughput: 50k inserts/sec при 4-core VM, queue.size < 1000.

---

### 14. WAL + group commit: Segment rotation + архивация (gzip)

**ID ROADMAP3:** R3-003  
**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2, 3  
**Связь с prompt3.md:** уточняет Промпт 97 (WAL базовая реализация). Промпт 97 не покрывает group commit — без этого latency коммита недопустимая на проде. Также поглощает Промпты 108 (segment rotation) и 109 (async write / group commit).

**Проблема.**

Сейчас на COMMIT происходит `saveTablesToDisk()` — сериализация всех таблиц полностью. Для таблицы 100k строк это секунды. Fsync на каждый commit убьёт throughput (десятки commits/sec максимум). Аварийное отключение теряет все изменения с последнего `saveTablesToDisk()`. Group commit не упоминается ни в одном документе проекта.

**Контекст этого шага.**

Шаг 4/5. Опирается на шаги 1–3. Ротация по размеру или по возрасту. Архивация — для backup (промпт 11) и replication (промпт 16).

**Задача.**

1. Ротация по размеру: при достижении 64 MB — закрыть текущий, создать новый.
2. Ротация по возрасту: `wal.segment.max.age.ms` = 5 min — принудительно rotate даже при незаполненном сегменте.
3. `WALArchiver`: gzip-архивация старых сегментов в `wal/archive/`.
4. Cleanup policy: удалять архивы старше `wal.archive.retention.days` (default 7).
5. Backward compat: текущая сериализация `.table` остаётся для cold backup, но не для durability.

**Ключевые файлы.**

- `diesel/wal/WALSegmentRotator.java`
- `diesel/wal/WALArchiver.java`

**Критерии приёмки.**

- [ ] SegmentRotationTest: both size-triggered and age-triggered rotation covered.
- [ ] ArchiveTest: 5 сегментов → 5 gzip-архивов, restore через gunzip → original bytes.
- [ ] RetentionTest: архивы старше 7 дней удаляются автоматически.

---

### 15. WAL + group commit: GroupCommitCoordinator + AsyncWALWriter

**ID ROADMAP3:** R3-003  
**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2, 3, 4  
**Связь с prompt3.md:** уточняет Промпт 97 (WAL базовая реализация). Промпт 97 не покрывает group commit — без этого latency коммита недопустимая на проде. Также поглощает Промпты 108 (segment rotation) и 109 (async write / group commit).

**Проблема.**

Сейчас на COMMIT происходит `saveTablesToDisk()` — сериализация всех таблиц полностью. Для таблицы 100k строк это секунды. Fsync на каждый commit убьёт throughput (десятки commits/sec максимум). Аварийное отключение теряет все изменения с последнего `saveTablesToDisk()`. Group commit не упоминается ни в одном документе проекта.

**Контекст этого шага.**

Шаг 5/5. Опирается на шаги 1–4. Без group commit throughput commit < 50/sec (fsync per commit). Шаг критичен для production throughput.

**Задача.**

1. Реализовать `GroupCommitCoordinator`: собирает коммиты в группы, один fsync для всей группы.
2. Окно group commit: 5-10 ms ИЛИ 64 транзакций — что раньше.
3. `AsyncWALWriter` — обёртка над WALWriter, возвращается future, клиент ждёт через `CompletableFuture`.
4. Config: `wal.fsync.policy = always | group | everysec | none` (default `group`).
5. `Transaction.java`: COMMIT path использует GroupCommitCoordinator, не fsync напрямую.

**Ключевые файлы.**

- `diesel/wal/GroupCommitCoordinator.java`
- `diesel/wal/AsyncWALWriter.java`
- `diesel/Transaction.java (изменение COMMIT path)`

**Критерии приёмки.**

- [ ] Throughput COMMIT > 5000/sec на 4-core VM (было < 50/sec).
- [ ] p99 commit latency < 20 ms в режиме `group`, < 1 ms в режиме `none` (dev).
- [ ] WALCrashRecoveryTest: 10k commits, kill -9, restart — все committed txs на месте, ни одной потери.
- [ ] Uncommitted tx полностью откатывается (см. промпт 4).

---

### 16. ARIES Recovery Manager: CheckpointRecord + checkpoint.ptr (atomic write)

**ID ROADMAP3:** R3-004  
**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** промпт 3 (WAL)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет Промпт 99 (ARIES Recovery Manager) и поглощает Промпт 98 (WAL recovery). В `prompt3.md` описан алгоритм, но не детализованы integration-точки с MVCC и page storage.

**Проблема.**

Без recovery WAL бесполезен: после краша нужно replay журнала. Промпт 99 даёт концепцию, но integration с MVCC (промпт 1) и page storage (промпт 2) не описан — это и есть пробел ROADMAP3.

**Контекст этого шага.**

Шаг 1/4. Фундамент для recovery. Не зависит от других шагов ARIES. Шаги 2 (analysis), 3 (redo), 4 (undo) — поверх.

**Задача.**

1. Реализовать `CheckpointRecord`: lastLSN, list of active txids, timestamp.
2. `checkpoint.ptr` файл: atomic write через `AtomicFileWriter` (temp + rename).
3. На каждом checkpoint (см. промпт 5) — записать новый CheckpointRecord в WAL + обновить checkpoint.ptr.
4. На старте сервера: `CheckpointRecord.load()` → точка старта recovery.

**Ключевые файлы.**

- `diesel/recovery/CheckpointRecord.java`
- `diesel/recovery/CheckpointPointerFile.java`
- `diesel/storage/page/AtomicFileWriter.java (shared с промптом 2)`

**Критерии приёмки.**

- [ ] CheckpointTest: 100 checkpoints, restart — pointer корректно указывает на последний.
- [ ] CrashTest: kill -9 во время write checkpoint.ptr — pointer остаётся валидным (через atomic rename).

---

### 17. ARIES Recovery Manager: Analysis phase (построение active tx list)

**ID ROADMAP3:** R3-004  
**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** промпт 3 (WAL)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет Промпт 99 (ARIES Recovery Manager) и поглощает Промпт 98 (WAL recovery). В `prompt3.md` описан алгоритм, но не детализованы integration-точки с MVCC и page storage.

**Проблема.**

Без recovery WAL бесполезен: после краша нужно replay журнала. Промпт 99 даёт концепцию, но integration с MVCC (промпт 1) и page storage (промпт 2) не описан — это и есть пробел ROADMAP3.

**Контекст этого шага.**

Шаг 2/4. Опирается на шаг 1. Читает WAL с последнего checkpoint, строит список активных txids (для undo). Шаги 3 (redo), 4 (undo) пользуются этим списком.

**Задача.**

1. Реализовать `AnalysisPhase`: читает WAL от lastCheckpointLSN до конца.
2. Для каждого COMMIT record → txid в committed set.
3. Для каждого BEGIN record → txid в active set.
4. Для каждого ABORT/COMMIT → убрать из active set.
5. На выходе: `{committed, active, lastLSN}`.

**Ключевые файлы.**

- `diesel/recovery/AnalysisPhase.java`
- `diesel/recovery/AnalysisResult.java`

**Критерии приёмки.**

- [ ] AnalysisTest: WAL с 50 commit / 50 no-commit → analysis корректно разделяет множества.
- [ ] Performance: analysis на 1 GB WAL < 5 сек.

---

### 18. ARIES Recovery Manager: Redo phase (replay операций с LSN > checkpoint)

**ID ROADMAP3:** R3-004  
**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** промпт 3 (WAL)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** уточняет Промпт 99 (ARIES Recovery Manager) и поглощает Промпт 98 (WAL recovery). В `prompt3.md` описан алгоритм, но не детализованы integration-точки с MVCC и page storage.

**Проблема.**

Без recovery WAL бесполезен: после краша нужно replay журнала. Промпт 99 даёт концепцию, но integration с MVCC (промпт 1) и page storage (промпт 2) не описан — это и есть пробел ROADMAP3.

**Контекст этого шага.**

Шаг 3/4. Опирается на шаги 1–2. Replay физических изменений страниц. Шаг 4 (undo) — отдельно для неоткатанных tx.

**Задача.**

1. Реализовать `RedoPhase`: для каждого WAL entry с LSN > lastCheckpointLSN — повторить операцию.
2. Интеграция с page storage: `PageManager.applyRedo(entry)` — перезаписывает страницу из after-image.
3. Идемпотентность: redo безопасен для повторного применения (LSN-check на странице).
4. Интеграция с MVCC: redo восстанавливает xmin/xmax версии.

**Ключевые файлы.**

- `diesel/recovery/RedoPhase.java`
- `diesel/storage/page/PageManager.java (applyRedo метод)`

**Критерии приёмки.**

- [ ] RedoTest: 1000 mixed ops → kill → redo → состояние страниц соответствует last committed.
- [ ] IdempotencyTest: повторный redo на уже-redone странице — no-op.
- [ ] Performance: redo на 1 GB WAL < 20 сек.

---

### 19. ARIES Recovery Manager: Undo phase + RecoveryManager (оркестрация на startup)

**ID ROADMAP3:** R3-004  
**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** промпт 3 (WAL)  
**Зависимости этого шага:** шаги 1, 2, 3  
**Связь с prompt3.md:** уточняет Промпт 99 (ARIES Recovery Manager) и поглощает Промпт 98 (WAL recovery). В `prompt3.md` описан алгоритм, но не детализованы integration-точки с MVCC и page storage.

**Проблема.**

Без recovery WAL бесполезен: после краша нужно replay журнала. Промпт 99 даёт концепцию, но integration с MVCC (промпт 1) и page storage (промпт 2) не описан — это и есть пробел ROADMAP3.

**Контекст этого шага.**

Шаг 4/4. Завершает ARIES. Опирается на шаги 1–3. Undo откатывает транзакции без commit record (из active set).

**Задача.**

1. Реализовать `UndoPhase`: для каждого txid из active set — откат через before-image из WAL.
2. Интеграция с MVCC: undo помечает версии как aborted (xmax = txid).
3. Реализовать `ARIESAlgorithm` — оркестрация: analysis → redo → undo.
4. Реализовать `RecoveryManager.recover()` — вызывается на старте `DatabaseServer`, до принятия клиентских соединений.
5. Метрика `recovery.duration.ms` экспортируется.

**Ключевые файлы.**

- `diesel/recovery/UndoPhase.java`
- `diesel/recovery/ARIESAlgorithm.java`
- `diesel/recovery/RecoveryManager.java`
- `diesel/DatabaseServer.java (startup sequence)`

**Критерии приёмки.**

- [ ] RecoveryIntegrationTest: 100 mixed tx (50 commit / 50 no-commit), kill, restart → 50 видны, 50 откачены.
- [ ] RecoveryWithLongTransactionTest: 1 длинная tx на 1M insert + kill посередине → все её изменения откачены.
- [ ] Recovery на WAL 1 GB < 30 секунд (суммарно analysis+redo+undo).

---

### 20. Background writer / flusher: BufferPoolFlusher (адаптивная стратегия)

**ID ROADMAP3:** R3-005  
**Категория:** A. Durability & Recovery  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 2 (page storage), промпт 3 (WAL)  
**Зависимости этого шага:** промпты 2, 3  
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `resilence.md`, но **не попал** в `prompt3.md` и `Roadmap.md`. Поглощает Промпт 100 (Checkpoint Manager) и его базовую часть.

**Проблема.**

Если dirty pages пишутся на диск только на checkpoint, восстановление после краша будет долгим (много WAL нужно replay). Нужен фоновый writer, который мягко флашит dirty pages в фоне, не блокируя writers.

**Контекст этого шага.**

Шаг 1/3. Опирается на промпты 2, 3. Сканирует buffer pool, флашит dirty pages. Шаги 2 (checkpoint integration), 3 (fuzzy checkpoint) — выше.

**Задача.**

1. Реализовать `BufferPoolFlusher` — background thread, период `bufferpool.flush.interval.ms` (default 200 ms).
2. Адаптивная стратегия: если dirty pages > 25 % буфера — flush агрессивнее (interval × 0.5); если < 5 % — реже (interval × 2).
3. Запись dirty page: атомарно (temp file + rename), через `PageManager.writePage()`.
4. Не флашить страницы, чей LSN > lastWALFlushLSN (WAL rule: сначала WAL, потом page).
5. Метрики: `bufferpool.dirty.pages`, `flusher.duration.ms`.

**Ключевые файлы.**

- `diesel/storage/page/BufferPoolFlusher.java`
- `diesel/storage/page/AdaptiveFlushStrategy.java`
- `diesel/ConfigLoader.java (bufferpool.flush.interval.ms, bufferpool.dirty.threshold)`

**Критерии приёмки.**

- [ ] FlusherTest: 100k inserts без COMMIT, kill, restart → все inserts либо committed, либо откачены (по WAL).
- [ ] Throughput writers не падает больше чем на 10 % при включённом flusher.
- [ ] AdaptiveFlushTest: dirty ratio > 25 % → interval сокращается; < 5 % → увеличивается.

---

### 21. Background writer / flusher: CheckpointManager (интеграция с WAL)

**ID ROADMAP3:** R3-005  
**Категория:** A. Durability & Recovery  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 2 (page storage), промпт 3 (WAL)  
**Зависимости этого шага:** шаг 1, промпт 3  
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `resilence.md`, но **не попал** в `prompt3.md` и `Roadmap.md`. Поглощает Промпт 100 (Checkpoint Manager) и его базовую часть.

**Проблема.**

Если dirty pages пишутся на диск только на checkpoint, восстановление после краша будет долгим (много WAL нужно replay). Нужен фоновый writer, который мягко флашит dirty pages в фоне, не блокируя writers.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Периодический checkpoint: sync flush всех dirty + запись checkpoint record в WAL.

**Задача.**

1. Реализовать `CheckpointManager`: каждые 5 min ИЛИ N MB WAL (default 256 MB) — триггер checkpoint.
2. Алгоритм: 
   a. Запретить новые writes (через short-lived lock).
   b. Flush всех dirty pages через BufferPoolFlusher.
   c. Записать `CheckpointRecord` в WAL (lastLSN, active txids).
   d. Обновить `checkpoint.ptr`.
   e. Снять lock.
3. После checkpoint — старые WAL segments можно архивировать (через промпт 3 шаг 4).

**Ключевые файлы.**

- `diesel/storage/page/CheckpointManager.java`
- `diesel/recovery/CheckpointRecord.java (используется)`

**Критерии приёмки.**

- [ ] CheckpointTest: checkpoint завершается за < 5 сек на 1M-строчной базе с dirty 50 % buffer pool.
- [ ] Recovery после checkpoint — не нужно replay всего WAL (только от lastCheckpointLSN).
- [ ] WAL segments до checkpoint — архивируются автоматически.

---

### 22. Background writer / flusher: Fuzzy checkpoint (без quiescent state)

**ID ROADMAP3:** R3-005  
**Категория:** A. Durability & Recovery  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 2 (page storage), промпт 3 (WAL)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `resilence.md`, но **не попал** в `prompt3.md` и `Roadmap.md`. Поглощает Промпт 100 (Checkpoint Manager) и его базовую часть.

**Проблема.**

Если dirty pages пишутся на диск только на checkpoint, восстановление после краша будет долгим (много WAL нужно replay). Нужен фоновый writer, который мягко флашит dirty pages в фоне, не блокируя writers.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Fuzzy checkpoint: не останавливает writers, фиксирует согласованное состояние с активными транзакциями.

**Задача.**

1. Реализовать `FuzzyCheckpoint` — variant of CheckpointManager, не требующий quiescent state.
2. Вместо short-lived write lock: log-based consistency — checkpoint record ссылается на LSN, до которого WAL консистентен.
3. Active transactions продолжают работать; их изменения после checkpoint LSN не включаются в redo.
4. Конфигурация: `checkpoint.mode = fuzzy | sharp` (default `fuzzy`; `sharp` — для maintenance window).

**Ключевые файлы.**

- `diesel/recovery/FuzzyCheckpoint.java`
- `diesel/recovery/CheckpointStrategy.java`
- `diesel/ConfigLoader.java (checkpoint.mode, checkpoint.interval.ms)`

**Критерии приёмки.**

- [ ] FuzzyCheckpointTest: 1M-строчная база + writers active + checkpoint → writers blocked < 50 ms total.
- [ ] RecoveryTest: fuzzy checkpoint → recovery корректен, все committed txs на месте.
- [ ] SharpCheckpointTest: sharp mode → writes blocked до завершения checkpoint.

---

### 23. Savepoint + Deadlock + Lock timeout: LockManager (гранулярные блокировки S/X/IS/IX)

**ID ROADMAP3:** R3-006  
**Категория:** B. Concurrency & Locking  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 1 (MVCC), промпт 3 (WAL для savepoint records)  
**Зависимости этого шага:** промпт 1  
**Связь с prompt3.md:** уточняет Промпты 104 (Deadlock), 105 (Lock timeout), 106-107 (Savepoint). Поглощает их, поскольку без интеграции с MVCC они не имеют смысла.

**Проблема.**

С появлением MVCC (промпт 1) нужны: deadlock detection для wait-for graph, lock timeout (иначе вечное ожидание), savepoints (частичный откат транзакции). В коде нет ни одного из этих классов.

**Контекст этого шага.**

Шаг 1/4. Опирается на промпт 1 (MVCC). Базовый lock manager с матрицей совместимости. Шаги 2 (deadlock), 3 (timeout), 4 (savepoint) — выше.

**Задача.**

1. Реализовать `LockManager`: таблица ресурсов (table-level / row-level) → список holders + waiters.
2. Режимы: S (shared), X (exclusive), IS (intent shared), IX (intent exclusive), SIX.
3. Матрица совместимости — константа в `LockCompatibility`.
4. `acquire(resource, mode, txid)` → блокирует или возвращает полученный lock.
5. `release(txid)` — освободить все locks транзакции при commit/abort.

**Ключевые файлы.**

- `diesel/concurrency/LockManager.java`
- `diesel/concurrency/Lock.java`
- `diesel/concurrency/LockMode.java`
- `diesel/concurrency/LockCompatibility.java`

**Критерии приёмки.**

- [ ] LockManagerTest: S+S совместимы, S+X конфликтуют (второй ждёт), X+X конфликтуют.
- [ ] IS/IX matrix test: все 25 комбинаций (5×5) корректны.
- [ ] ReleaseTest: release(txid) освобождает все locks, waiters получают уведомление.

---

### 24. Savepoint + Deadlock + Lock timeout: WaitForGraph + DeadlockDetector

**ID ROADMAP3:** R3-006  
**Категория:** B. Concurrency & Locking  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 1 (MVCC), промпт 3 (WAL для savepoint records)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет Промпты 104 (Deadlock), 105 (Lock timeout), 106-107 (Savepoint). Поглощает их, поскольку без интеграции с MVCC они не имеют смысла.

**Проблема.**

С появлением MVCC (промпт 1) нужны: deadlock detection для wait-for graph, lock timeout (иначе вечное ожидание), savepoints (частичный откат транзакции). В коде нет ни одного из этих классов.

**Контекст этого шага.**

Шаг 2/4. Опирается на шаг 1. Periodic check на циклы в wait-for graph, жертва = txid с минимальным возрастом.

**Задача.**

1. Реализовать `WaitForGraph`: узлы = txids, ребро A→B если A ждёт lock, занятый B.
2. `DeadlockDetector` — background thread каждые 500 ms (config `deadlock.check.interval.ms`), ищет циклы (DFS).
3. При обнаружении цикла: жертва = txid с минимальным txid (т.е. младшая транзакция abort-ится).
4. `DeadlockVictimException` — клиент получает понятное сообщение, может retry.

**Ключевые файлы.**

- `diesel/concurrency/WaitForGraph.java`
- `diesel/concurrency/DeadlockDetector.java`
- `diesel/concurrency/DeadlockVictimException.java`

**Критерии приёмки.**

- [ ] DeadlockTest: 2 tx в цикле (A→B→A) → одна убита DeadlockVictimException.
- [ ] PerformanceTest: 1000 concurrent tx, detector overhead < 1 % throughput.
- [ ] ChainDeadlockTest: 5 tx в цикле → одна жертва, остальные продолжают.

---

### 25. Savepoint + Deadlock + Lock timeout: LockTimeoutManager (lock.timeout.ms)

**ID ROADMAP3:** R3-006  
**Категория:** B. Concurrency & Locking  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 1 (MVCC), промпт 3 (WAL для savepoint records)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет Промпты 104 (Deadlock), 105 (Lock timeout), 106-107 (Savepoint). Поглощает их, поскольку без интеграции с MVCC они не имеют смысла.

**Проблема.**

С появлением MVCC (промпт 1) нужны: deadlock detection для wait-for graph, lock timeout (иначе вечное ожидание), savepoints (частичный откат транзакции). В коде нет ни одного из этих классов.

**Контекст этого шага.**

Шаг 3/4. Опирается на шаг 1. Без timeout клиенты могут бесконечно ждать. Независим от шага 2 (deadlock), но они взаимодействуют.

**Задача.**

1. Реализовать `LockTimeoutManager`: при ожидании lock запускается таймер `lock.timeout.ms` (default 30000).
2. При истечении — `LockTimeoutException`, txid получает уведомление, abort-ится.
3. Атомарность: таймер отменяется, если lock получен.
4. Метрика `lock.timeout.count` экспортируется.

**Ключевые файлы.**

- `diesel/concurrency/LockTimeoutManager.java`
- `diesel/concurrency/LockTimeoutException.java`
- `diesel/ConfigLoader.java (lock.timeout.ms)`

**Критерии приёмки.**

- [ ] LockTimeoutTest: tx1 держит X lock на строке 100 сек, tx2 ждёт 30 сек → LockTimeoutException.
- [ ] CancelTimerTest: tx1 отпускает lock до timeout → tx2 получает lock, таймер отменён.
- [ ] ConfigTest: lock.timeout.ms=5000 → timeout ровно через 5 сек.

---

### 26. Savepoint + Deadlock + Lock timeout: SavepointManager + NestedSavepointStack

**ID ROADMAP3:** R3-006  
**Категория:** B. Concurrency & Locking  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 1 (MVCC), промпт 3 (WAL для savepoint records)  
**Зависимости этого шага:** шаг 1, промпт 1  
**Связь с prompt3.md:** уточняет Промпты 104 (Deadlock), 105 (Lock timeout), 106-107 (Savepoint). Поглощает их, поскольку без интеграции с MVCC они не имеют смысла.

**Проблема.**

С появлением MVCC (промпт 1) нужны: deadlock detection для wait-for graph, lock timeout (иначе вечное ожидание), savepoints (частичный откат транзакции). В коде нет ни одного из этих классов.

**Контекст этого шага.**

Шаг 4/4. Опирается на шаг 1 и промпт 1 (UndoLog). Частичный откат транзакции через savepoint.

**Задача.**

1. Реализовать `SavepointManager`: `SAVEPOINT name`, `ROLLBACK TO name`, `RELEASE name`.
2. Savepoint = marker в UndoLog, до которого откатывается.
3. `NestedSavepointStack`: иерархия savepoints, при RELEASE родителя — очищаются все дочерние.
4. WAL records для savepoint create/release (для recovery).
5. Парсер: `QueryParser` расширить для `SAVEPOINT` / `ROLLBACK TO` / `RELEASE`.

**Ключевые файлы.**

- `diesel/concurrency/SavepointManager.java`
- `diesel/concurrency/Savepoint.java`
- `diesel/concurrency/NestedSavepointStack.java`
- `diesel/QueryParser.java (SAVEPOINT/ROLLBACK TO/RELEASE syntax)`

**Критерии приёмки.**

- [ ] SavepointTest: SAVEPOINT + частичный ROLLBACK TO восстанавливает состояние до savepoint, не теряя изменения после savepoint.
- [ ] NestedSavepointTest: 5 уровней вложенности, ROLLBACK TO уровня 3 сохраняет уровни 1-2, очищает 4-5.
- [ ] SavepointWALTest: SAVEPOINT + COMMIT + kill → recovery корректно восстанавливает commit.
- [ ] Метрика `savepoint.count` экспортируется.

---

### 27. Замена Java Object Serialization в net-протоколе: WireProtocol v2 + MessageCodec (binary)

**ID ROADMAP3:** R3-007  
**Категория:** F. Security + L. Serialization  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (можно делать параллельно с промптами 1/2/3)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое, не покрыто. `problems.md` §Сериализация-6 указывает проблему, но ни в одном плане её нет. Это **RCE-риск** — Java deserialization gadgets.

**Проблема.**

`DatabaseClient` / `DatabaseServer` общаются через `ObjectInputStream.readObject()` — это известная RCE-уязвимость (deserialization gadgets: commons-collections, spring, etc.). Любой клиент может послать сериализованный payload и получить RCE на сервере. Для production это **blocker**.

**Контекст этого шага.**

Шаг 1/3. Фундамент: бинарный формат протокола v2. Шаг 2 (message types), 3 (legacy compat + compression) — выше.

**Задача.**

1. Спроектировать `WireProtocol` v2: magic bytes `DSEL` + version (2 bytes) + message-type (2 bytes) + length (4 bytes) + payload + CRC32 footer (4 bytes).
2. `MessageCodec`: enc/dec интерфейс с методами `encode(Message, DataOutput)` / `decode(DataInput)`.
3. Никакого `ObjectInputStream` — только явные поля.
4. CRC32 verification на каждый message.

**Ключевые файлы.**

- `diesel/net/WireProtocol.java`
- `diesel/net/MessageCodec.java`
- `diesel/net/WireException.java`

**Критерии приёмки.**

- [ ] WireProtocolTest: round-trip encode/decode всех message types (см. шаг 2).
- [ ] CRC32: битый байт в payload → InvalidMessageException.
- [ ] Versioning: protocol v1 rejected, v2 accepted.

---

### 28. Замена Java Object Serialization в net-протоколе: Message types (Query/Result/Prepare/Batch/Health)

**ID ROADMAP3:** R3-007  
**Категория:** F. Security + L. Serialization  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (можно делать параллельно с промптами 1/2/3)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, не покрыто. `problems.md` §Сериализация-6 указывает проблему, но ни в одном плане её нет. Это **RCE-риск** — Java deserialization gadgets.

**Проблема.**

`DatabaseClient` / `DatabaseServer` общаются через `ObjectInputStream.readObject()` — это известная RCE-уязвимость (deserialization gadgets: commons-collections, spring, etc.). Любой клиент может послать сериализованный payload и получить RCE на сервере. Для production это **blocker**.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Конкретные message classes с явными полями.

**Задача.**

1. Реализовать message classes (каждый — отдельный класс с `writeTo(DataOutput)` / `readFrom(DataInput)`):
   - `QueryMessage` (SQL string + bind variables)
   - `QueryResultMessage` (columns + rows + status + duration)
   - `PrepareMessage` (prepared SQL)
   - `ExecutePreparedMessage` (params + preparedId)
   - `BatchQueryMessage` (N queries)
   - `HealthCheckMessage` / `HealthCheckResponse`
   - `CompressionHandshakeMessage`
2. Integration с `DatabaseServer.ClientHandler` — handler читает message через MessageCodec.
3. Integration с `DatabaseClient` — send/receive через MessageCodec.

**Ключевые файлы.**

- `diesel/net/messages/QueryMessage.java`
- `diesel/net/messages/QueryResultMessage.java`
- `diesel/net/messages/PrepareMessage.java`
- `diesel/net/messages/ExecutePreparedMessage.java`
- `diesel/net/messages/BatchQueryMessage.java`
- `diesel/net/messages/HealthCheckMessage.java`
- `diesel/net/messages/CompressionHandshakeMessage.java`
- `diesel/DatabaseServer.java (ClientHandler)`
- `diesel/DatabaseClient.java (send/receive)`

**Критерии приёмки.**

- [ ] WireProtocolSecurityTest: попытка послать сериализованный Java payload → connection closed with InvalidMessageException.
- [ ] Throughput не падает vs legacy (или растёт: см. шаг 3 compression).
- [ ] MessageTypesTest: каждый тип round-trip тестируется.

---

### 29. Замена Java Object Serialization в net-протоколе: Compression (ZSTD > 4KB) + legacy port compat

**ID ROADMAP3:** R3-007  
**Категория:** F. Security + L. Serialization  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (можно делать параллельно с промптами 1/2/3)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое, не покрыто. `problems.md` §Сериализация-6 указывает проблему, но ни в одном плане её нет. Это **RCE-риск** — Java deserialization gadgets.

**Проблема.**

`DatabaseClient` / `DatabaseServer` общаются через `ObjectInputStream.readObject()` — это известная RCE-уязвимость (deserialization gadgets: commons-collections, spring, etc.). Любой клиент может послать сериализованный payload и получить RCE на сервере. Для production это **blocker**.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Compression для больших результатов, backward compat для старых клиентов.

**Задача.**

1. ZSTD compression для сообщений > 4 KB (использует `CompressionCodec`).
2. Handshake: `CompressionHandshakeMessage` — клиент и сервер договариваются о compression algorithm + threshold.
3. Legacy port: порт 5440 (default legacy), deprecated warning в логах при подключении legacy клиента.
4. Удаление legacy протокола — Фаза 2 (помечено в KNOWN_LIMITATIONS.md).

**Ключевые файлы.**

- `diesel/net/CompressionHandshakeHandler.java`
- `diesel/net/LegacyProtocolAdapter.java`
- `diesel/ConfigLoader.java (compression.threshold, legacy.port)`

**Критерии приёмки.**

- [ ] CompressionTest: результат 100 KB сжимается до 5-10 KB, throughput не падает.
- [ ] LegacyCompatTest: legacy клиент (v1) подключается к legacy порту, получает warning, работает.
- [ ] CompressionDisableTest: compression.threshold=0 → все сообщения несжатые.

---

### 30. RBAC + Audit log: User/Role/Privilege модель + password hashing

**ID ROADMAP3:** R3-008  
**Категория:** F. Security  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** — (промпт 7 для transport; промпт 1 для session-scoped role)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 5 упоминается RBAC, но без деталей и слишком поздно (6-12 мес). Для production нужно раньше.

**Проблема.**

Сейчас сервер принимает любое соединение без аутентификации. Любой сетевой клиент может выполнить любой SQL. Audit отсутствует — нельзя понять, кто и что делал.

**Контекст этого шага.**

Шаг 1/4. Фундамент: модель пользователей и ролей, хранение учёток. Шаги 2 (auth), 3 (authz), 4 (audit) — сверху.

**Задача.**

1. Реализовать `User`, `Role`, `Privilege` (SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/ADMIN на таблицу / базу).
2. Хранилище: системные таблицы `diesel_users` (с salted hash пароля, PBKDF2 / Argon2), `diesel_roles`, `diesel_user_roles`, `diesel_privileges`.
3. `PasswordHasher`: PBKDF2 (default) с configurable iterations; опционально Argon2 через библиотеку.
4. SQL: `CREATE USER`, `CREATE ROLE`, `GRANT role TO user`.

**Ключевые файлы.**

- `diesel/security/User.java`
- `diesel/security/Role.java`
- `diesel/security/Privilege.java`
- `diesel/security/PasswordHasher.java`
- `diesel/security/UserStore.java`

**Критерии приёмки.**

- [ ] UserStoreTest: create user → пароль хранится как hash+salt, не plaintext.
- [ ] PasswordHasherTest: PBKDF2 с 100k iterations < 100 ms, brute force нерентабелен.
- [ ] SQL: `CREATE USER alice PASSWORD 'secret'` работает, повторное создание → UserExistsException.

---

### 31. RBAC + Audit log: Authenticator (handshake) + CLI login

**ID ROADMAP3:** R3-008  
**Категория:** F. Security  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** — (промпт 7 для transport; промпт 1 для session-scoped role)  
**Зависимости этого шага:** шаг 1, промпт 7  
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 5 упоминается RBAC, но без деталей и слишком поздно (6-12 мес). Для production нужно раньше.

**Проблема.**

Сейчас сервер принимает любое соединение без аутентификации. Любой сетевой клиент может выполнить любой SQL. Audit отсутствует — нельзя понять, кто и что делал.

**Контекст этого шага.**

Шаг 2/4. Опирается на шаг 1. Аутентификация на handshake: клиент присылает user/pass, сервер возвращает session token.

**Задача.**

1. Реализовать `Authenticator`: на handshake (через wire-протокол промпта 7) — verify user/pass через PasswordHasher.
2. Session: `Connection.getUserId()`, проверка на каждый запрос.
3. CLI: `--user`, `--password`, `--database` flags.
4. Default: после первого запуска создаётся `admin` / `admin` с принудительной сменой пароля при первом login.
5. CLI флаг `--reset-admin` для локального сброса (только с filesystem access).

**Ключевые файлы.**

- `diesel/security/Authenticator.java`
- `diesel/security/Session.java`
- `diesel/security/SessionToken.java`
- `diesel/DatabaseServer.java (handshake)`
- `diesel/CliRepl.java (login flags)`

**Критерии приёмки.**

- [ ] AuthTest: неверный пароль → AuthException.
- [ ] DefaultAdminTest: первый запуск создаёт admin/admin, первый login требует смены пароля.
- [ ] ResetAdminTest: `--reset-admin` сбрасывает пароль, работает только при доступе к filesystem.

---

### 32. RBAC + Audit log: Authorizer (проверка прав перед каждым запросом)

**ID ROADMAP3:** R3-008  
**Категория:** F. Security  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** — (промпт 7 для transport; промпт 1 для session-scoped role)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 5 упоминается RBAC, но без деталей и слишком поздно (6-12 мес). Для production нужно раньше.

**Проблема.**

Сейчас сервер принимает любое соединение без аутентификации. Любой сетевой клиент может выполнить любой SQL. Audit отсутствует — нельзя понять, кто и что делал.

**Контекст этого шага.**

Шаг 3/4. Опирается на шаги 1–2. Проверка privilege перед выполнением SQL. Шаг 4 (audit) — отдельно.

**Задача.**

1. Реализовать `Authorizer`: для каждого запроса — lookup privilege в `diesel_privileges` для (userId, table, action).
2. Если нет права → `AccessDeniedException` с понятным сообщением.
3. SQL: `GRANT SELECT ON table TO role`, `REVOKE SELECT ON table FROM role`.
4. ADMIN privilege — мета-право, включает все остальные.
5. Кэш privilege в session, инвалидация при GRANT/REVOKE.

**Ключевые файлы.**

- `diesel/security/Authorizer.java`
- `diesel/security/PrivilegeCache.java`
- `diesel/security/AccessDeniedException.java`
- `diesel/QueryParser.java (GRANT/REVOKE)`

**Критерии приёмки.**

- [ ] RbacTest: user без SELECT на `users` → AccessDeniedException.
- [ ] GrantTest: GRANT SELECT → SELECT работает, REVOKE → снова AccessDenied.
- [ ] AdminTest: ADMIN user может всё, включая GRANT/REVOKE.
- [ ] CacheInvalidationTest: GRANT/REVOKE сразу отражается на active sessions.

---

### 33. RBAC + Audit log: AuditLogger (diesel_audit_log table)

**ID ROADMAP3:** R3-008  
**Категория:** F. Security  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** — (промпт 7 для transport; промпт 1 для session-scoped role)  
**Зависимости этого шага:** шаги 1, 2, 3  
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 5 упоминается RBAC, но без деталей и слишком поздно (6-12 мес). Для production нужно раньше.

**Проблема.**

Сейчас сервер принимает любое соединение без аутентификации. Любой сетевой клиент может выполнить любой SQL. Audit отсутствует — нельзя понять, кто и что делал.

**Контекст этого шага.**

Шаг 4/4. Опирается на шаги 1–3. Логирование всех SQL-операций для соответствия compliance.

**Задача.**

1. Реализовать `AuditLogger`: таблица `diesel_audit_log` (event_time, user, source_ip, query, status, duration_ms).
2. Включается через `audit.enabled = true` (default false для dev, true для production).
3. Audit записывается в отдельную таблицу, не блокирует основной query (async через bounded queue).
4. Retention policy: `audit.retention.days` (default 90), старые записи парсятся в cold archive.

**Ключевые файлы.**

- `diesel/security/AuditLogger.java`
- `diesel/security/AuditRecord.java`
- `diesel/ConfigLoader.java (audit.enabled, audit.retention.days)`

**Критерии приёмки.**

- [ ] AuditTest: 100 запросов → 100 записей в audit log с корректным user, ip, query, status, duration.
- [ ] AsyncTest: audit не блокирует основной запрос, queue overflow → graceful drop с метрикой.
- [ ] RetentionTest: записи старше 90 дней архивируются, не теряются (через архив).

---

### 34. SSL/TLS transport: SslContextFactory + TLS handshake handler

**ID ROADMAP3:** R3-009  
**Категория:** F. Security  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 7 (новый wire protocol)  
**Зависимости этого шага:** промпт 7  
**Связь с prompt3.md:** новое, частично в `Roadmap.md` Этап 5. Нужно раньше — без TLS нельзя пускать трафик через сеть.

**Проблема.**

Даже с новым wire-протоколом (промпт 7) данные идут в открытом виде. SQL-запросы, результаты, пароли на handshake — всё читаемо в MITM.

**Контекст этого шага.**

Шаг 1/3. Опирается на промпт 7. Создание SSLContext, handshake после TCP accept. Шаги 2 (config), 3 (mTLS + SNI) — выше.

**Задача.**

1. Реализовать `SslContextFactory`: загружает keystore, создаёт SSLContext (TLS 1.3 preferred, TLS 1.2 fallback).
2. `TlsHandshakeHandler`: после TCP accept — опциональный TLS handshake.
3. `tls.enabled = false | true` config switch (default false).
4. Cipher suites whitelist: TLS 1.3 modern + TLS 1.2 PFS ciphers; legacy (TLS 1.0/1.1, non-PFS) отключены.

**Ключевые файлы.**

- `diesel/net/TlsHandshakeHandler.java`
- `diesel/net/SslContextFactory.java`
- `diesel/net/CipherSuiteWhitelist.java`
- `diesel/ConfigLoader.java (tls.enabled, tls.keystore.path, tls.keystore.password)`

**Критерии приёмки.**

- [ ] TlsConnectionTest: клиент с TLS 1.3 подключается, handshake успешен, трафик зашифрован.
- [ ] CipherAuditTest: только whitelist ciphers доступны, legacy отклоняются.
- [ ] DisableTlsTest: tls.enabled=false → plain TCP, без TLS.

---

### 35. SSL/TLS transport: CLI client TLS flags + truststore

**ID ROADMAP3:** R3-009  
**Категория:** F. Security  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 7 (новый wire protocol)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, частично в `Roadmap.md` Этап 5. Нужно раньше — без TLS нельзя пускать трафик через сеть.

**Проблема.**

Даже с новым wire-протоколом (промпт 7) данные идут в открытом виде. SQL-запросы, результаты, пароли на handshake — всё читаемо в MITM.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. CLI клиент должен поддерживать TLS, доверять сертификату сервера через truststore.

**Задача.**

1. CLI флаги: `--tls`, `--tls-ca <path>`, `--tls-cert <path>`, `--tls-key <path>`.
2. Если `--tls` без `--tls-ca` — использовать системный truststore (default JVM cacerts).
3. Verification: hostname + chain; expired cert rejected.
4. Config in `diesel-client.conf` (optional, for REPL usage).

**Ключевые файлы.**

- `diesel/CliRepl.java (TLS flags)`
- `diesel/net/SslContextFactory.java (client variant)`

**Критерии приёмки.**

- [ ] CliTlsTest: `diesel-cli --tls --tls-ca ca.pem` подключается к TLS-enabled серверу.
- [ ] SelfSignedTest: self-signed cert без `--tls-ca` → CertificateException.
- [ ] ExpiredCertTest: expired cert rejected.

---

### 36. SSL/TLS transport: mTLS (client cert) + SNI for multi-tenant

**ID ROADMAP3:** R3-009  
**Категория:** F. Security  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 7 (новый wire protocol)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое, частично в `Roadmap.md` Этап 5. Нужно раньше — без TLS нельзя пускать трафик через сеть.

**Проблема.**

Даже с новым wire-протоколом (промпт 7) данные идут в открытом виде. SQL-запросы, результаты, пароли на handshake — всё читаемо в MITM.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. mutual TLS для 2-way auth, SNI для multi-tenant развертываний.

**Задача.**

1. mutual TLS: `tls.client.auth = none | request | require` (default `none`).
2. При `require` — клиент обязан предоставить cert, верифицируемый серверным truststore.
3. SNI: сервер использует SNI hostname для выбора keystore (multi-tenant: tenant1.example.com → /etc/diesel/tenant1.p12).
4. Mapping SNI → tenant в `diesel_tenants` таблице.

**Ключевые файлы.**

- `diesel/net/MutualTlsHandler.java`
- `diesel/net/SniSelector.java`
- `diesel/ConfigLoader.java (tls.client.auth)`

**Критерии приёмки.**

- [ ] MtlsTest: client без cert при `tls.client.auth=require` → connection refused.
- [ ] MtlsRequestTest: при `request` — cert опционален, но если представлен — verified.
- [ ] SniTest: SNI `tenant1.example.com` → выбран правильный keystore.
- [ ] Testssl.sh / sslyze scan — класс A+ по SSL Labs.

---

### 37. Online schema changes (ALTER без блокировки): SchemaVersion + catalog versioning

**ID ROADMAP3:** R3-010  
**Категория:** C. Storage engine + G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 2 (page storage), промпт 7 (schema version broadcast)  
**Зависимости этого шага:** промпт 2  
**Связь с prompt3.md:** уточняет Промпты 113-114 (ALTER TABLE ADD/DROP COLUMN). Промпты 113-114 описывают SQL-парсинг, R3-010 добавляет online-семантику. Базовый SQL остаётся в доборочных промптах 47-48.

**Проблема.**

Production БД должна менять схему без даунтайма. Текущий `ALTER TABLE` (когда будет реализован в Промптах 113-114) заблокирует таблицу на время операции.

**Контекст этого шага.**

Шаг 1/3. Опирается на промпт 2. Каждое изменение схемы — новая version. Шаги 2 (copy algorithm), 3 (inplace + LOCK) — сверху.

**Задача.**

1. Реализовать `SchemaVersion`: monotonic counter, сохраняется в CatalogTable.
2. При ALTER — создать новую версию схемы, старая остаётся видимой для running queries.
3. `Table` хранит несколько schema versions одновременно, каждый SELECT использует snapshot-вид.
4. Timeout для old schema version readers: после N минут — killing oldest (config `schema.old.version.timeout.min`, default 10).

**Ключевые файлы.**

- `diesel/schema/SchemaVersion.java`
- `diesel/Table.java (поддержка нескольких schema versions)`
- `diesel/storage/page/CatalogTable.java (версии схем)`
- `diesel/ConfigLoader.java (schema.old.version.timeout.min)`

**Критерии приёмки.**

- [ ] SchemaVersionTest: 10 последовательных ALTER — 10 версий, старые видны running queries.
- [ ] OldVersionTimeoutTest: reader держит старую версию 15 min → killed.
- [ ] CatalogTest: restart server → schema versions восстановлены.

---

### 38. Online schema changes (ALTER без блокировки): ALGORITHM=copy (background copy + atomic rename)

**ID ROADMAP3:** R3-010  
**Категория:** C. Storage engine + G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 2 (page storage), промпт 7 (schema version broadcast)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет Промпты 113-114 (ALTER TABLE ADD/DROP COLUMN). Промпты 113-114 описывают SQL-парсинг, R3-010 добавляет online-семантику. Базовый SQL остаётся в доборочных промптах 47-48.

**Проблема.**

Production БД должна менять схему без даунтайма. Текущий `ALTER TABLE` (когда будет реализован в Промптах 113-114) заблокирует таблицу на время операции.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Copy-алгоритм для тяжёлых изменений (NEW COLUMN NOT NULL, изменение типа).

**Задача.**

1. Реализовать `AlterTableCopyAlgorithm`: новая версия таблицы создаётся в фоне, writers дублируются в старую и новую (dual-write).
2. Background copy: читает старую таблицу, копирует в новую, прогресс reporting через `ALTER STATUS`.
3. По окончании copy — atomic rename (через CatalogTable).
4. Rollback: если copy не завершилась — drop новой, оставить старую.

**Ключевые файлы.**

- `diesel/schema/AlterTableCopyAlgorithm.java`
- `diesel/schema/OnlineAlterTable.java`
- `diesel/schema/DualWriteCoordinator.java`

**Критерии приёмки.**

- [ ] CopyAlgorithmTest: ALTER на таблице 10M строк без блокировки writers > 100 ms total.
- [ ] DualWriteTest: writes во время copy → видны в новой версии после rename.
- [ ] RollbackTest: прерывание copy → старая версия сохранена.
- [ ] ProgressTest: `ALTER STATUS` возвращает % completion.

---

### 39. Online schema changes (ALTER без блокировки): ALGORITHM=inplace + LOCK=NONE/SHARED/EXCLUSIVE

**ID ROADMAP3:** R3-010  
**Категория:** C. Storage engine + G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 2 (page storage), промпт 7 (schema version broadcast)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** уточняет Промпты 113-114 (ALTER TABLE ADD/DROP COLUMN). Промпты 113-114 описывают SQL-парсинг, R3-010 добавляет online-семантику. Базовый SQL остаётся в доборочных промптах 47-48.

**Проблема.**

Production БД должна менять схему без даунтайма. Текущий `ALTER TABLE` (когда будет реализован в Промптах 113-114) заблокирует таблицу на время операции.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Inplace для лёгких изменений (ADD COLUMN nullable, DROP COLUMN через tombstone).

**Задача.**

1. Реализовать `AlterTableInplaceAlgorithm`: ADD COLUMN nullable — только metadata change (no copy).
2. DROP COLUMN через tombstone: пометить колонку как deleted, скрыть из SELECT, физическое удаление в background compaction.
3. LOCK clause: NONE (no blocking), SHARED (readers ok, writers blocked), EXCLUSIVE (full block).
4. SQL syntax: `ALTER TABLE ... ALGORITHM=INPLACE, LOCK=NONE`.
5. Если запрошен INPLACE, но не поддерживается — fallback на COPY с warning.

**Ключевые файлы.**

- `diesel/schema/AlterTableInplaceAlgorithm.java`
- `diesel/schema/AlterLockMode.java`
- `diesel/schema/AlterAlgorithm.java`
- `diesel/QueryParser.java (расширение ALTER синтаксиса)`

**Критерии приёмки.**

- [ ] InplaceTest: `ADD COLUMN nullable_col` на 10M строк — < 100 ms total (metadata only).
- [ ] LockNoneTest: 100 параллельных SELECT видят consistent схему (либо старую, либо новую).
- [ ] TombstoneTest: DROP COLUMN → SELECT * не возвращает колонку, physically удалена в compaction.
- [ ] FallbackTest: INPLACE для неподдерживаемого → COPY с warning.
- [ ] OnlineAlterTest: 1 writer + 1 alter + 10 readers, все завершаются успешно.

---

### 40. Backup / Restore (logical + physical): diesel_dump (logical backup CLI) + diesel_restore

**ID ROADMAP3:** R3-011  
**Категория:** J. Observability & operations  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 3 (WAL), промпт 4 (Recovery)  
**Зависимости этого шага:** промпт 4  
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 4 упоминается «аналог pg_dump/pg_restore», но без деталей. Поглощает Промпт 110 (PITR).

**Проблема.**

Без бэкапов production невозможен. Сейчас только ручное копирование `.csv` / `.table` файлов — это не consistent snapshot (нет точки во времени, нет гарантии целостности).

**Контекст этого шага.**

Шаг 1/4. Опирается на промпт 4. Logical backup: SQL dump (CREATE TABLE + INSERT INTO). Шаги 2 (physical), 3 (incremental), 4 (PITR) — выше.

**Задача.**

1. Реализовать `diesel_dump` CLI: logical backup — SQL dump (`CREATE TABLE` + `INSERT INTO`), опционально с `--format=sql|csv|jsonl|avro`.
2. `diesel_restore` CLI: restore из dump, с `--on-conflict=skip|replace|error`.
3. Schema + data + sequences + indexes (если --include-indexes).
4. Streaming output (не загружать весь dump в память).

**Ключевые файлы.**

- `diesel/backup/LogicalDump.java`
- `diesel/backup/LogicalRestore.java`
- `diesel/backup/DumpFormat.java`
- `diesel/CliRepl.java (команды DUMP/RESTORE)`

**Критерии приёмки.**

- [ ] DumpTest: `diesel_dump` на 1M-строчной базе — < 30 с, файл < 100 MB.
- [ ] RestoreTest: restore из dump → данные идентичны исходным (10 тестовых таблиц сравниваются row-by-row).
- [ ] StreamingTest: dump 10M строк → heap < 500 MB (streaming output).
- [ ] OnConflictTest: `--on-conflict=skip` пропускает дубликаты, `=replace` перезаписывает, `=error` выбрасывает исключение.

---

### 41. Backup / Restore (logical + physical): diesel_backup (physical: WAL checkpoint + pages + tail WAL)

**ID ROADMAP3:** R3-011  
**Категория:** J. Observability & operations  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 3 (WAL), промпт 4 (Recovery)  
**Зависимости этого шага:** шаг 1, промпты 3, 4, 5  
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 4 упоминается «аналог pg_dump/pg_restore», но без деталей. Поглощает Промпт 110 (PITR).

**Проблема.**

Без бэкапов production невозможен. Сейчас только ручное копирование `.csv` / `.table` файлов — это не consistent snapshot (нет точки во времени, нет гарантии целостности).

**Контекст этого шага.**

Шаг 2/4. Опирается на шаг 1. Physical backup: consistent snapshot через WAL checkpoint + копирование pages + tail WAL.

**Задача.**

1. Реализовать `diesel_backup` CLI: consistent snapshot работающей базы (writers active).
2. Алгоритм: 
   a. Trigger checkpoint (см. промпт 5).
   b. Copy data pages (frozen snapshot).
   c. Copy WAL segments с checkpoint LSN до current.
   d. Backup manifest: list of files + checksums.
3. Restore: copy files + replay tail WAL через recovery.

**Ключевые файлы.**

- `diesel/backup/PhysicalBackup.java`
- `diesel/backup/BackupManifest.java`
- `diesel/backup/PhysicalRestore.java`

**Критерии приёмки.**

- [ ] PhysicalBackupTest: backup работающей базы (writers active) — consistent на момент backup start.
- [ ] RestoreTest: restore из physical backup → база в состоянии на момент backup.
- [ ] ManifestTest: corrupted page file → детектируется через checksum в manifest.

---

### 42. Backup / Restore (logical + physical): IncrementalBackup (только изменившиеся pages)

**ID ROADMAP3:** R3-011  
**Категория:** J. Observability & operations  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 3 (WAL), промпт 4 (Recovery)  
**Зависимости этого шага:** шаг 2  
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 4 упоминается «аналог pg_dump/pg_restore», но без деталей. Поглощает Промпт 110 (PITR).

**Проблема.**

Без бэкапов production невозможен. Сейчас только ручное копирование `.csv` / `.table` файлов — это не consistent snapshot (нет точки во времени, нет гарантии целостности).

**Контекст этого шага.**

Шаг 3/4. Опирается на шаг 2. Incremental — только delta с последнего full backup, через page LSN.

**Задача.**

1. Реализовать `IncrementalBackup`: сравнение page LSN с last backup LSN, копирование только изменившихся pages.
2. Manifest incremental backup ссылается на full backup.
3. Restore: full + N incrementals applied последовательно.
4. Schedule: `diesel_backup_cron` (cron-like syntax in config).

**Ключевые файлы.**

- `diesel/backup/IncrementalBackup.java`
- `diesel/backup/BackupScheduler.java`
- `diesel/ConfigLoader.java (backup.schedule)`

**Критерии приёмки.**

- [ ] IncrementalTest: 100 GB база, 1 % изменённых pages — incremental backup < 5 минут.
- [ ] RestoreChainTest: full + 3 incrementals → restore корректно применяет все.
- [ ] ScheduleTest: cron `0 2 * * *` (ежедневно в 2:00) — триггерится автоматически.

---

### 43. Backup / Restore (logical + physical): PITR (Point-in-Time Recovery) через WAL replay

**ID ROADMAP3:** R3-011  
**Категория:** J. Observability & operations  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** промпт 3 (WAL), промпт 4 (Recovery)  
**Зависимости этого шага:** шаги 2, 3  
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 4 упоминается «аналог pg_dump/pg_restore», но без деталей. Поглощает Промпт 110 (PITR).

**Проблема.**

Без бэкапов production невозможен. Сейчас только ручное копирование `.csv` / `.table` файлов — это не consistent snapshot (нет точки во времени, нет гарантии целостности).

**Контекст этого шага.**

Шаг 4/4. Опирается на шаги 2–3. Restore full + replay WAL до указанного timestamp.

**Задача.**

1. Реализовать `PointInTimeRecovery`: restore full backup + replay WAL до указанного timestamp.
2. Алгоритм: 
   a. Restore full backup (шаг 2).
   b. Apply incrementals (шаг 3).
   c. Replay WAL records с LSN > last backup LSN, до timestamp.
   d. Abort all transactions active at timestamp.
3. CLI: `diesel_restore --pitr '2026-10-05 14:30:00'`.

**Ключевые файлы.**

- `diesel/recovery/PointInTimeRecovery.java`
- `diesel/backup/PitrManager.java`

**Критерии приёмки.**

- [ ] PitrTest: restore на момент «5 минут назад» → точно соответствует состоянию БД в тот момент.
- [ ] PitrWithActiveTxTest: 2 active tx at timestamp → обе откачены, committed tx visible.
- [ ] PitrCliTest: `--pitr '2026-10-05 14:30:00'` работает.

---

### 44. Metrics + Prometheus + Health check: MetricsRegistry (counters/histograms/gauges)

**ID ROADMAP3:** R3-012  
**Категория:** J. Observability  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** — (можно параллельно с остальным)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое. В `monitoring.md` и `Roadmap.md` Этап 4 описано, но без конкретики.

**Проблема.**

Сейчас мониторинг — только логи (`Slow query breakdown`). Без metrics endpoint невозможно подключить Prometheus / Grafana.

**Контекст этого шага.**

Шаг 1/3. Фундамент: registry метрик. Шаги 2 (Prometheus), 3 (health) — выше.

**Задача.**

1. Реализовать `MetricsRegistry`: счётчики (QPS, errors), гистограммы (latency p50/p95/p99), gauges (heap, connections, active tx).
2. Thread-safe, lock-free где возможно (для histogram — HDRHistogram или простой synchronized).
3. Стандартные метрики:
   - `diesel_queries_total{type, status}` (counter)
   - `diesel_query_duration_seconds{type}` (histogram)
   - `diesel_transactions_active` (gauge)
   - `diesel_connections_active`, `diesel_connections_rejected`
   - `diesel_buffer_pool_hit_ratio`
   - `diesel_wal_lag_ms`
   - `diesel_deadlocks_total`, `diesel_lock_timeouts_total`
4. Integration points: query executor, lock manager, WAL writer, buffer pool — все инкрементируют метрики.

**Ключевые файлы.**

- `diesel/observability/MetricsRegistry.java`
- `diesel/observability/Histogram.java`
- `diesel/observability/Counter.java`
- `diesel/observability/Gauge.java`

**Критерии приёмки.**

- [ ] MetricsRegistryTest: 1M increments на 10 потоках — без race conditions.
- [ ] HistogramTest: p99 latency корректно считается на 10k samples.
- [ ] IntegrationTest: query execution инкрементирует `diesel_queries_total`.

---

### 45. Metrics + Prometheus + Health check: PrometheusExporter (/metrics HTTP endpoint)

**ID ROADMAP3:** R3-012  
**Категория:** J. Observability  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** — (можно параллельно с остальным)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое. В `monitoring.md` и `Roadmap.md` Этап 4 описано, но без конкретики.

**Проблема.**

Сейчас мониторинг — только логи (`Slow query breakdown`). Без metrics endpoint невозможно подключить Prometheus / Grafana.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. HTTP endpoint на отдельном порту (default 9090), формат Prometheus text.

**Задача.**

1. Реализовать `PrometheusExporter`: формат Prometheus text (# HELP, # TYPE, metric_name{labels} value).
2. `MetricsHttpServer`: HTTP server на порту 9090, endpoint `/metrics`.
3. `metrics.enabled`, `metrics.port` config.
4. Grafana dashboard: `dashboards/dieseldb.json` — QPS, p99 latency, hit ratio, connections.

**Ключевые файлы.**

- `diesel/observability/PrometheusExporter.java`
- `diesel/observability/MetricsHttpServer.java`
- `diesel/ConfigLoader.java (metrics.port, metrics.enabled)`
- `dashboards/dieseldb.json`

**Критерии приёмки.**

- [ ] `curl http://localhost:9090/metrics` возвращает Prometheus-совместимый текст с # HELP и # TYPE.
- [ ] Grafana import test: dashboard JSON импортируется без ошибок, показывает live метрики.
- [ ] Latency overhead метрик < 1 % throughput (verified via benchmark).

---

### 46. Metrics + Prometheus + Health check: HealthCheck endpoint (/health JSON)

**ID ROADMAP3:** R3-012  
**Категория:** J. Observability  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости родителя:** — (можно параллельно с остальным)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое. В `monitoring.md` и `Roadmap.md` Этап 4 описано, но без конкретики.

**Проблема.**

Сейчас мониторинг — только логи (`Slow query breakdown`). Без metrics endpoint невозможно подключить Prometheus / Grafana.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. JSON health endpoint для kubernetes liveness/readiness probes.

**Задача.**

1. Реализовать `HealthCheck`: HTTP endpoint `/health` возвращает JSON `{"status":"UP|DOWN","uptime":3600,"activeTx":5,"diskFree":1024,"walLag":50}`.
2. Liveness: `status=DOWN` если WAL writer thread dead или OOM imminent.
3. Readiness: `status=DOWN` если recovery in progress или buffer pool cold-start.
4. Endpoints: `/health/live` (liveness), `/health/ready` (readiness), `/health` (combined).

**Ключевые файлы.**

- `diesel/observability/HealthCheck.java`
- `diesel/observability/LivenessCheck.java`
- `diesel/observability/ReadinessCheck.java`

**Критерии приёмки.**

- [ ] `curl http://localhost:9090/health` возвращает UP/DOWN.
- [ ] LivenessTest: kill WAL writer thread → status=DOWN within 5 sec.
- [ ] ReadinessTest: during recovery startup → /health/ready=DOWN, after recovery → UP.
- [ ] K8s integration: manifest yaml с livenessProbe/readinessProbe пример.

---

### 47. Fix всех blocking bugs из problems.md: R3-013a: стабильные row-id (CRITICAL correctness fix)

**ID ROADMAP3:** R3-013  
**Категория:** M. Correctness  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (можно параллельно)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет Промпт 25 (стабильные row-id) и косвенно Промпт 35 (батчинг rebuild). Остальные баги (TRUE, регистр строковых литералов, `indexDefinitions` сериализация) — новые, не покрыто.

**Проблема.**

Открытые баги из `problems.md` и `PERSISTENCE_README.md` §5:
1. `DelimitedIndexManager`: после `insertAt` в середину `searchByPrimaryKey` возвращает **неверные позиции** (Промпт 25 CRITICAL, не сделан).
2. `Transaction.cloneTable` через сериализацию — O(n·m·log n) на каждое BEGIN.
3. `PerformanceTest` падает: `Unknown column: USERS.TRUE` — парсер трактует `TRUE` как колонку.
4. Регистр строковых литералов: INSERT uppercase, SELECT case-sensitive → `WHERE NAME='Name2'` не работает.
5. `delete` → полный rebuild индексов на каждое удаление → O(n²).
6. `readObject` перестраивает все индексы → O(n·m·log n).
7. `indexDefinitions` не сериализуется явно в `writeObject` — потенциальная потеря.

**Контекст этого шага.**

Шаг 1/4. Самый критичный correctness bug — индексы теряют позицию после `insertAt` в середину. Промпт 1 шаг 1 и промпт 2 шаг 1 также помогают, но bug нужно фиксить явно.

**Задача.**

1. Реализовать стабильные row-id (immutable): вместо позиции в List — генерировать monotonically increasing long ID.
2. `DelimitedIndexManager`: хранить sorted array of row-ids, `searchByPrimaryKey` возвращает row-id, не позицию.
3. `Table.getRow(rowId)` — lookup через array index, поддерживаемый `insertAt`/`deleteAt` без пересортировки.
4. Тест `RowIdStabilityTest`: 10000 операций insert в середину + delete из середину, индексы корректны.

**Ключевые файлы.**

- `diesel/storage/DelimitedIndexManager.java`
- `diesel/RowId.java (новый, long wrapper)`
- `diesel/Table.java (getRow by rowId)`

**Критерии приёмки.**

- [ ] RowIdStabilityTest: 10000 случайных insertAt/deleteAt, `searchByPrimaryKey` всегда возвращает корректный row-id.
- [ ] StressTest: 1M inserts + 100k deletes в середину, без несоответствий.
- [ ] PerformanceTest (старый failing) — passes.

---

### 48. Fix всех blocking bugs из problems.md: R3-013b: TRUE/FALSE/NULL как литералы в парсере

**ID ROADMAP3:** R3-013  
**Категория:** M. Correctness  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (можно параллельно)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет Промпт 25 (стабильные row-id) и косвенно Промпт 35 (батчинг rebuild). Остальные баги (TRUE, регистр строковых литералов, `indexDefinitions` сериализация) — новые, не покрыто.

**Проблема.**

Открытые баги из `problems.md` и `PERSISTENCE_README.md` §5:
1. `DelimitedIndexManager`: после `insertAt` в середину `searchByPrimaryKey` возвращает **неверные позиции** (Промпт 25 CRITICAL, не сделан).
2. `Transaction.cloneTable` через сериализацию — O(n·m·log n) на каждое BEGIN.
3. `PerformanceTest` падает: `Unknown column: USERS.TRUE` — парсер трактует `TRUE` как колонку.
4. Регистр строковых литералов: INSERT uppercase, SELECT case-sensitive → `WHERE NAME='Name2'` не работает.
5. `delete` → полный rebuild индексов на каждое удаление → O(n²).
6. `readObject` перестраивает все индексы → O(n·m·log n).
7. `indexDefinitions` не сериализуется явно в `writeObject` — потенциальная потеря.

**Контекст этого шага.**

Шаг 2/4. Локальный фикс парсера — `TRUE`/`FALSE`/`NULL` трактуются как литералы, не как column references. Независим от других шагов.

**Задача.**

1. Расширить лексер/парсер: `TRUE`, `FALSE`, `NULL` — ключевые слова, parse как LiteralExpression.
2. `WHERE active = TRUE` → эквивалент `WHERE active = 1` (boolean как integer).
3. `WHERE data IS NULL` / `IS NOT NULL` — работает.
4. Boolean как тип возвращаемого значения: `SELECT 1 = 1` возвращает TRUE.

**Ключевые файлы.**

- `diesel/QueryParser.java (TRUE/FALSE/NULL literals)`
- `diesel/expression/BooleanLiteral.java`

**Критерии приёмки.**

- [ ] TrueFalseLiteralTest: `WHERE active = TRUE` возвращает корректные строки.
- [ ] NullLiteralTest: `WHERE col IS NULL` работает корректно.
- [ ] PerformanceTest не падает с `Unknown column: USERS.TRUE`.

---

### 49. Fix всех blocking bugs из problems.md: R3-013c: регистр строковых литералов сохраняется

**ID ROADMAP3:** R3-013  
**Категория:** M. Correctness  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (можно параллельно)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет Промпт 25 (стабильные row-id) и косвенно Промпт 35 (батчинг rebuild). Остальные баги (TRUE, регистр строковых литералов, `indexDefinitions` сериализация) — новые, не покрыто.

**Проблема.**

Открытые баги из `problems.md` и `PERSISTENCE_README.md` §5:
1. `DelimitedIndexManager`: после `insertAt` в середину `searchByPrimaryKey` возвращает **неверные позиции** (Промпт 25 CRITICAL, не сделан).
2. `Transaction.cloneTable` через сериализацию — O(n·m·log n) на каждое BEGIN.
3. `PerformanceTest` падает: `Unknown column: USERS.TRUE` — парсер трактует `TRUE` как колонку.
4. Регистр строковых литералов: INSERT uppercase, SELECT case-sensitive → `WHERE NAME='Name2'` не работает.
5. `delete` → полный rebuild индексов на каждое удаление → O(n²).
6. `readObject` перестраивает все индексы → O(n·m·log n).
7. `indexDefinitions` не сериализуется явно в `writeObject` — потенциальная потеря.

**Контекст этого шага.**

Шаг 3/4. Парсер нормализует строковые литералы через `toUpperCase()` — ломает case-sensitive comparison. Независим от других шагов.

**Задача.**

1. Хранить строковые литералы в оригинальном регистре, не нормализовать через `toUpperCase()` в парсере.
2. Case-sensitive comparison по умолчанию; опционально `COLLATE CASE_INSENSITIVE` для всей колонки.
3. Round-trip: INSERT 'Name2' → SELECT WHERE col='Name2' возвращает строку; `WHERE col='name2'` — нет (case-sensitive).
4. Migration: существующие данные с uppercase сохраняются, для обратной совместимости — config `compat.uppercase.string.literals = true` (default false).

**Ключевые файлы.**

- `diesel/QueryParser.java (сохранение регистра)`
- `diesel/expression/StringLiteral.java`
- `diesel/ConfigLoader.java (compat.uppercase.string.literals)`

**Критерии приёмки.**

- [ ] StringCaseSensitivityTest: round-trip INSERT + SELECT сохраняет регистр.
- [ ] CaseInsensitiveTest: `COLLATE CASE_INSENSITIVE` — comparison case-insensitive.
- [ ] CompatModeTest: при `compat.uppercase.string.literals=true` старое поведение сохраняется.

---

### 50. Fix всех blocking bugs из problems.md: R3-013d: явная сериализация indexDefinitions

**ID ROADMAP3:** R3-013  
**Категория:** M. Correctness  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости родителя:** — (можно параллельно)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет Промпт 25 (стабильные row-id) и косвенно Промпт 35 (батчинг rebuild). Остальные баги (TRUE, регистр строковых литералов, `indexDefinitions` сериализация) — новые, не покрыто.

**Проблема.**

Открытые баги из `problems.md` и `PERSISTENCE_README.md` §5:
1. `DelimitedIndexManager`: после `insertAt` в середину `searchByPrimaryKey` возвращает **неверные позиции** (Промпт 25 CRITICAL, не сделан).
2. `Transaction.cloneTable` через сериализацию — O(n·m·log n) на каждое BEGIN.
3. `PerformanceTest` падает: `Unknown column: USERS.TRUE` — парсер трактует `TRUE` как колонку.
4. Регистр строковых литералов: INSERT uppercase, SELECT case-sensitive → `WHERE NAME='Name2'` не работает.
5. `delete` → полный rebuild индексов на каждое удаление → O(n²).
6. `readObject` перестраивает все индексы → O(n·m·log n).
7. `indexDefinitions` не сериализуется явно в `writeObject` — потенциальная потеря.

**Контекст этого шага.**

Шаг 4/4. При `readObject`/`writeObject` indexDefinitions могут теряться. Независим от других шагов, но связан с промптом 2 (page storage заменит сериализацию).

**Задача.**

1. Явная сериализация `indexDefinitions` в `writeObject` (через writeInt + writeUTF per index).
2. `readObject` восстанавливает индексы без O(n·m·log n) перестройки (через persisted b-tree structure).
3. Проверка: после рестарта индексы готовы к использованию без rebuild.
4. Migration: если файл старого формата — rebuild индексов автоматически (с warning в логах).

**Ключевые файлы.**

- `diesel/Table.java (writeObject/readObject indexDefinitions)`
- `diesel/IndexDefinition.java (serializable)`

**Критерии приёмки.**

- [ ] IndexSerializationTest: create index + write table + read → index доступен без rebuild.
- [ ] BackwardCompatTest: файл старого формата → rebuild автоматически с warning.
- [ ] PerformanceTest: загрузка таблицы 100k строк с 5 индексами — без O(n·m·log n) rebuild.

---

---

## Фаза 2 — Production-Grade (6-12 месяцев)

**Цель:** закрыть HIGH-приоритетные пробелы, без которых DieselDB не подходит для multi-tenant production. По завершении — метрика готовности ≥ 75 %.

### 51. Checkpoint + Checksummed pages (CRC32C): CRC32C implementation (hardware-accelerated)

**ID ROADMAP3:** R3-014  
**Категория:** A. Durability  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпты 3, 4, 5  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет и поглощает Промпты 100-103 (Checkpoint Manager, fuzzy vs sharp, Checksummed Page, CRC32C algorithm). Промпты описаны концептуально, R3-014 интегрирует их с MVCC/page storage/WAL.

**Проблема.**

Fuzzy checkpoint нужен для ограничения WAL replay; checksummed pages — для детекции silent corruption (bit rot). Промпты описаны, но интегрировать с промптами 1/2/3 нужно явно.

**Контекст этого шага.**

Шаг 1/3. Алгоритм CRC32C. Шаги 2 (checksummed page), 3 (scrubber) — выше.

**Задача.**

1. Реализовать `CRC32C` через `java.util.zip.CRC32C` (hardware SSE4.2 accelerated на supported JVM).
2. Опционально JNI bridge для direct SSE4.2 instructions (если JVM не использует hardware).
3. Benchmark: throughput ≥ 5 GB/sec на CPU с SSE4.2.
4. Fallback: pure Java реализация если hardware недоступен.

**Ключевые файлы.**

- `diesel/checksum/CRC32C.java`
- `diesel/checksum/CRC32CNative.java (optional JNI)`

**Критерии приёмки.**

- [ ] CRC32CTest: known vectors (RFC 3720) — корректные значения.
- [ ] BenchmarkTest: throughput ≥ 5 GB/sec на supported CPU.
- [ ] FallbackTest: на CPU без SSE4.2 — pure Java работает (медленнее).

---

### 52. Checkpoint + Checksummed pages (CRC32C): ChecksummedPage (CRC32C в header + verify on read)

**ID ROADMAP3:** R3-014  
**Категория:** A. Durability  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпты 3, 4, 5  
**Зависимости этого шага:** шаг 1, промпт 4  
**Связь с prompt3.md:** уточняет и поглощает Промпты 100-103 (Checkpoint Manager, fuzzy vs sharp, Checksummed Page, CRC32C algorithm). Промпты описаны концептуально, R3-014 интегрирует их с MVCC/page storage/WAL.

**Проблема.**

Fuzzy checkpoint нужен для ограничения WAL replay; checksummed pages — для детекции silent corruption (bit rot). Промпты описаны, но интегрировать с промптами 1/2/3 нужно явно.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. CRC32C в page header, verify при чтении.

**Задача.**

1. Реализовать `ChecksummedPage`: CRC32C в header (4 bytes), вычисляется over header + payload.
2. На запись страницы (`PageManager.writePage`): вычислить CRC, записать.
3. На чтение (`PageManager.readPage`): verify CRC, при провале — `PageCorruptedException`, попытка восстановить из WAL redo (через промпт 4 шаг 3).
4. Метрика `diesel_page_checksum_failures_total`.

**Ключевые файлы.**

- `diesel/checksum/ChecksummedPage.java`
- `diesel/checksum/PageValidator.java`
- `diesel/storage/page/Page.java (CRC field)`
- `diesel/storage/page/PageManager.java (verify on read)`

**Критерии приёмки.**

- [ ] CorruptionDetectionTest: битый байт в странице → PageCorruptedException при чтении.
- [ ] RecoveryFromCorruptionTest: corrupt page → recovery через WAL redo работает.
- [ ] MetricTest: `diesel_page_checksum_failures_total` инкрементируется при corruption.

---

### 53. Checkpoint + Checksummed pages (CRC32C): Background PageScrubber + checkpoint strategy

**ID ROADMAP3:** R3-014  
**Категория:** A. Durability  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпты 3, 4, 5  
**Зависимости этого шага:** шаги 1, 2, промпт 5  
**Связь с prompt3.md:** уточняет и поглощает Промпты 100-103 (Checkpoint Manager, fuzzy vs sharp, Checksummed Page, CRC32C algorithm). Промпты описаны концептуально, R3-014 интегрирует их с MVCC/page storage/WAL.

**Проблема.**

Fuzzy checkpoint нужен для ограничения WAL replay; checksummed pages — для детекции silent corruption (bit rot). Промпты описаны, но интегрировать с промптами 1/2/3 нужно явно.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Background scrubber: раз в N дней — все страницы verify, репорт corrupted.

**Задача.**

1. Реализовать `PageScrubber`: background thread, `scrubber.interval.days` (default 7).
2. Сканирует все страницы, verify CRC, репорт corrupted в логи + metric.
3. Checkpoint strategy enum: `fuzzy` (default), `sharp` (maintenance).
4. `CheckpointStrategy` интерфейс с двумя реализациями (использует промпт 5 шаг 3 FuzzyCheckpoint).

**Ключевые файлы.**

- `diesel/storage/page/PageScrubber.java`
- `diesel/recovery/CheckpointStrategy.java`
- `diesel/ConfigLoader.java (scrubber.interval.days)`

**Критерии приёмки.**

- [ ] ScrubberTest: 100 GB база — scrubber completes < 1 часа, не блокирует writers.
- [ ] ScrubberDetectTest: преднамеренно corrupted page → scrubber детектирует, репортит.
- [ ] StrategyTest: `checkpoint.mode=fuzzy` (default) vs `sharp` — оба работают.
- [ ] Метрики: `diesel_scrubber_duration_seconds`, `diesel_page_checksum_failures_total`.

---

### 54. libpq + JDBC/Python/Node/Go/Rust drivers: libpq message protocol (separate port 5432)

**ID ROADMAP3:** R3-015  
**Категория:** I. Network & drivers  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 7 (wire protocol)  
**Зависимости этого шага:** промпт 7  
**Связь с prompt3.md:** новое, частично в `Roadmap.md` Этап 4.

**Проблема.**

Собственный протокол = пользователи должны писать своего клиента. libpq-совместимость даёт доступ ко всей экосистеме (psql, Hibernate, SQLAlchemy, pgx, prisma, ...).

**Контекст этого шага.**

Шаг 1/4. Ядро: libpq-совместимый протокол на порту 5432. Шаги 2–5 (drivers) — поверх.

**Задача.**

1. Реализовать `LibpqProtocol`: Startup, Query, Parse, Bind, Execute, Sync messages.
2. Отдельный порт (default 5432), параллельный с native wire protocol (промпт 7, порт 5440).
3. Совместимость с psql CLI (минимум: `\d`, `\dt`, `\l`, `\q`, basic queries).
4. Extended query protocol (Parse/Bind/Execute) для preparedStatement.

**Ключевые файлы.**

- `diesel/net/libpq/LibpqProtocol.java`
- `diesel/net/libpq/LibpqMessage.java`
- `diesel/net/libpq/LibpqMessageDecoder.java`
- `diesel/DatabaseServer.java (additional acceptor on 5432)`

**Критерии приёмки.**

- [ ] LibpqTest: `psql -h localhost -p 5432` работает для `SELECT * FROM users LIMIT 5`.
- [ ] ExtendedQueryTest: prepared statements через Parse/Bind/Execute.
- [ ] PsqlCompatTest: `\d table`, `\dt`, `\l` работают как в PostgreSQL.

---

### 55. libpq + JDBC/Python/Node/Go/Rust drivers: JDBC driver (diesel-jdbc module)

**ID ROADMAP3:** R3-015  
**Категория:** I. Network & drivers  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 7 (wire protocol)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, частично в `Roadmap.md` Этап 4.

**Проблема.**

Собственный протокол = пользователи должны писать своего клиента. libpq-совместимость даёт доступ ко всей экосистеме (psql, Hibernate, SQLAlchemy, pgx, prisma, ...).

**Контекст этого шага.**

Шаг 2/4. Опирается на шаг 1. JDBC driver для Java-приложений.

**Задача.**

1. Реализовать `diesel-jdbc` module: классы `Driver`, `Connection`, `PreparedStatement`, `ResultSet`.
2. Connection URL: `jdbc:diesel://host:5432/db`.
3. Поддержка `DatabaseMetaData` для Hibernate compatibility.
4. Публикация в Maven Central (CI release pipeline).

**Ключевые файлы.**

- `drivers/jdbc/diesel-jdbc/pom.xml`
- `drivers/jdbc/diesel-jdbc/src/main/java/com/diesel/jdbc/Driver.java`
- `drivers/jdbc/diesel-jdbc/src/main/java/com/diesel/jdbc/DieselConnection.java`
- `drivers/jdbc/diesel-jdbc/src/main/java/com/diesel/jdbc/DieselPreparedStatement.java`
- `drivers/jdbc/diesel-jdbc/src/main/java/com/diesel/jdbc/DieselResultSet.java`

**Критерии приёмки.**

- [ ] JdbcTest: `DriverManager.getConnection("jdbc:diesel://localhost:5432/db")` работает.
- [ ] HibernateTest: simple entity CRUD через Hibernate работает.
- [ ] MavenCentralTest: artifact опубликован, `mvn dependency:tree` подтягивает.

---

### 56. libpq + JDBC/Python/Node/Go/Rust drivers: Python + Node.js drivers

**ID ROADMAP3:** R3-015  
**Категория:** I. Network & drivers  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 7 (wire protocol)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, частично в `Roadmap.md` Этап 4.

**Проблема.**

Собственный протокол = пользователи должны писать своего клиента. libpq-совместимость даёт доступ ко всей экосистеме (psql, Hibernate, SQLAlchemy, pgx, prisma, ...).

**Контекст этого шага.**

Шаг 3/4. Опирается на шаг 1. Python psycopg2-compatible и Node.js pg-compatible drivers.

**Задача.**

1. Python: pure-Python driver в `drivers/python/diesel-python/`, совместимый с psycopg2 API (`connect`, `cursor`, `execute`).
2. Node.js: TypeScript driver в `drivers/nodejs/diesel-node/`, совместимый с `pg` package API.
3. Async variants: asyncio (Python), Promise (Node.js).
4. Публикация в PyPI и npm (CI pipeline).

**Ключевые файлы.**

- `drivers/python/diesel-python/setup.py`
- `drivers/python/diesel-python/diesel/__init__.py`
- `drivers/python/diesel-python/diesel/connection.py`
- `drivers/nodejs/diesel-node/package.json`
- `drivers/nodejs/diesel-node/src/index.ts`
- `drivers/nodejs/diesel-node/src/connection.ts`

**Критерии приёмки.**

- [ ] PythonTest: `psycopg2.connect("...")` (через diesel driver) — basic CRUD работает.
- [ ] NodeTest: `pg.Client` подключается к diesel, CRUD работает.
- [ ] PyPI: пакет публикуется, `pip install diesel-python` работает.
- [ ] npm: пакет публикуется, `npm install diesel-node` работает.

---

### 57. libpq + JDBC/Python/Node/Go/Rust drivers: Go + Rust drivers

**ID ROADMAP3:** R3-015  
**Категория:** I. Network & drivers  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 7 (wire protocol)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, частично в `Roadmap.md` Этап 4.

**Проблема.**

Собственный протокол = пользователи должны писать своего клиента. libpq-совместимость даёт доступ ко всей экосистеме (psql, Hibernate, SQLAlchemy, pgx, prisma, ...).

**Контекст этого шага.**

Шаг 4/4. Опирается на шаг 1. Go pq-compatible и Rust tokio-postgres-compatible drivers.

**Задача.**

1. Go: driver в `drivers/go/diesel-go/`, совместимый с `database/sql` + `lib/pq` API.
2. Rust: async driver в `drivers/rust/diesel-rs/`, совместимый с `tokio-postgres` API.
3. Публикация в pkg.go.dev и crates.io.

**Ключевые файлы.**

- `drivers/go/diesel-go/go.mod`
- `drivers/go/diesel-go/diesel.go`
- `drivers/rust/diesel-rs/Cargo.toml`
- `drivers/rust/diesel-rs/src/lib.rs`
- `drivers/rust/diesel-rs/src/client.rs`

**Критерии приёмки.**

- [ ] GoTest: `sql.Open("diesel", "host=localhost port=5432")` — basic CRUD работает.
- [ ] RustTest: `tokio_postgres::connect`-compatible — async CRUD работает.
- [ ] pkg.go.dev: пакет опубликован.
- [ ] crates.io: пакет опубликован.

---

### 58. Replication (logical + physical + quorum/Raft): Physical streaming replication (WAL streaming to standbys)

**ID ROADMAP3:** R3-016  
**Категория:** D. Replication & HA  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 3 (WAL), промпт 4 (Recovery), промпт 7 (transport)  
**Зависимости этого шага:** промпты 3, 4, 7  
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает, но без деталей. `replication.md` описывает теорию.

**Проблема.**

Без репликации — нет HA, нет read scaling. Single point of failure.

**Контекст этого шага.**

Шаг 1/5. Опирается на промпты 3, 4, 7. Master стримит WAL слейвам, слейвы в hot standby (read-only).

**Задача.**

1. Реализовать `WalSender` (master side): стримит WAL segments активным standby.
2. `WalReceiver` (standby side): получает WAL, applies через RedoPhase (промпт 4 шаг 3).
3. Standby в hot standby mode: read-only queries разрешены, writes rejected.
4. Config: `replication.role = master | standby | off`, `replication.upstream.host`, `replication.upstream.port`.

**Ключевые файлы.**

- `diesel/replication/WalSender.java`
- `diesel/replication/WalReceiver.java`
- `diesel/replication/HotStandbyHandler.java`
- `diesel/ConfigLoader.java (replication.*)`

**Критерии приёмки.**

- [ ] StreamingReplTest: master + 1 standby, 1000 inserts/sec — lag slave-behind-master < 100 ms.
- [ ] HotStandbyTest: SELECT на standby работает, writes rejected.
- [ ] ReconnectTest: standby disconnect + reconnect → автоматический catch-up.

---

### 59. Replication (logical + physical + quorum/Raft): Replication slots (защита от WAL удаления)

**ID ROADMAP3:** R3-016  
**Категория:** D. Replication & HA  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 3 (WAL), промпт 4 (Recovery), промпт 7 (transport)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает, но без деталей. `replication.md` описывает теорию.

**Проблема.**

Без репликации — нет HA, нет read scaling. Single point of failure.

**Контекст этого шага.**

Шаг 2/5. Опирается на шаг 1. Slot гарантирует, что WAL не удаляется, пока standby не подтвердил получение.

**Задача.**

1. Реализовать `ReplicationSlot`: name, confirmed_flush_lsn, restart_lsn.
2. WAL segments не архивируются/удаляются, если confirmed_flush_lsn < segment_max_lsn.
3. SQL: `CREATE_REPLICATION_SLOT name`, `DROP_REPLICATION_SLOT name`.
4. Slot metadata persisted на master, survive restart.

**Ключевые файлы.**

- `diesel/replication/ReplicationSlot.java`
- `diesel/replication/ReplicationSlotRegistry.java`
- `diesel/wal/WALArchiver.java (integration)`

**Критерии приёмки.**

- [ ] SlotTest: standby offline > 5 min — WAL не удаляется, standby может догнать после reconnect.
- [ ] SlotDropTest: DROP_REPLICATION_SLOT → WAL segments можно удалить.
- [ ] SlotPersistTest: master restart → slots восстановлены.

---

### 60. Replication (logical + physical + quorum/Raft): Synchronous replication (wait for N standby acks)

**ID ROADMAP3:** R3-016  
**Категория:** D. Replication & HA  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 3 (WAL), промпт 4 (Recovery), промпт 7 (transport)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает, но без деталей. `replication.md` описывает теорию.

**Проблема.**

Без репликации — нет HA, нет read scaling. Single point of failure.

**Контекст этого шага.**

Шаг 3/5. Опирается на шаги 1–2. `synchronous_commit=on` — ждать подтверждения от N синхронных standby перед ack клиенту.

**Задача.**

1. Реализовать `SyncReplicationCoordinator`: на COMMIT — ждать ack от N standby (config `synchronous_standby_names`).
2. Ack: standby подтвердил apply WAL record до LSN коммита.
3. Timeout: `synchronous_commit.timeout.ms` (default 5000) → если standby не ответил → COMMIT провален.
4. Mode `off` (default) / `local` (no sync wait) / `on` (wait N).

**Ключевые файлы.**

- `diesel/replication/SyncReplicationCoordinator.java`
- `diesel/replication/SyncAck.java`
- `diesel/ConfigLoader.java (synchronous_commit, synchronous_standby_names)`

**Критерии приёмки.**

- [ ] SyncReplTest: 1 sync standby, COMMIT подтверждён только после ack от standby.
- [ ] SyncReplTimeoutTest: standby недоступен 5 сек → COMMIT провален (timeout).
- [ ] SyncReplOffTest: `synchronous_commit=off` → COMMIT не ждёт standby.

---

### 61. Replication (logical + physical + quorum/Raft): Logical replication (WAL decoding to change events)

**ID ROADMAP3:** R3-016  
**Категория:** D. Replication & HA  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 3 (WAL), промпт 4 (Recovery), промпт 7 (transport)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает, но без деталей. `replication.md` описывает теорию.

**Проблема.**

Без репликации — нет HA, нет read scaling. Single point of failure.

**Контекст этого шага.**

Шаг 4/5. Опирается на шаг 1. Logical decoder: WAL → insert/update/delete events, подписчики применяют.

**Задача.**

1. Реализовать `LogicalDecoder`: читает WAL, декодирует в logical change events (table, op, before, after).
2. Publication/Subscription model: `CREATE PUBLICATION`, `CREATE SUBSCRIPTION`.
3. Подписчик применяет events через обычные DML (без WAL redo).
4. Conflict resolution: `on_conflict = error | skip | replace` (default error).

**Ключевые файлы.**

- `diesel/replication/LogicalDecoder.java`
- `diesel/replication/Publication.java`
- `diesel/replication/Subscription.java`
- `diesel/replication/LogicalChangeRecord.java`
- `diesel/QueryParser.java (CREATE PUBLICATION/SUBSCRIPTION)`

**Критерии приёмки.**

- [ ] LogicalReplTest: INSERT на master → INSERT на подписчике < 1 сек.
UpdateDeleteTest: UPDATE/DELETE синхронизируются корректно.
- [ ] ConflictTest: одинаковый PK на master и subscriber → on_conflict=error/skip/replace работает.

---

### 62. Replication (logical + physical + quorum/Raft): Raft consensus + Failover (multi-master elections)

**ID ROADMAP3:** R3-016  
**Категория:** D. Replication & HA  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 3 (WAL), промпт 4 (Recovery), промпт 7 (transport)  
**Зависимости этого шага:** шаги 1, 2, 3  
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает, но без деталей. `replication.md` описывает теорию.

**Проблема.**

Без репликации — нет HA, нет read scaling. Single point of failure.

**Контекст этого шага.**

Шаг 5/5. Опирается на шаги 1–4. Встроенный Raft для multi-master elections без внешнего Patroni.

**Задача.**

1. Реализовать `RaftNode`: leader election, log replication через Raft protocol.
2. `RaftLog`: persistent log of leadership changes + configuration changes.
3. `FailoverManager`: при `kill -9` мастера — automatic promotion нового leader через Raft election.
4. Quorum: majority of N nodes (3 → 2, 5 → 3).
5. Split-brain prevention: только leader принимает writes.

**Ключевые файлы.**

- `diesel/replication/raft/RaftNode.java`
- `diesel/replication/raft/RaftLog.java`
- `diesel/replication/raft/RaftState.java`
- `diesel/replication/FailoverManager.java`

**Критерии приёмки.**

- [ ] RaftElectionTest: kill leader → новый лидер выбран < 5 сек.
- [ ] RaftQuorumTest: 3 nodes, kill 1 → кластер продолжает принимать writes (quorum 2).
- [ ] SplitBrainTest: network partition 2/1 → меньшая часть не принимает writes (no quorum).
- [ ] RaftLogPersistTest: restart всех нод → лидер выбран корректно.

---

### 63. Partitioning (range / list / hash): PARTITION BY RANGE/LIST/HASH syntax + storage layout

**ID ROADMAP3:** R3-017  
**Категория:** E. Sharding & Partitioning  
**Приоритет:** MEDIUM  
**Фаза:** 2  
**Зависимости родителя:** промпт 2 (page storage), промпт 10 (online schema)  
**Зависимости этого шага:** промпты 2, 10  
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает.

**Проблема.**

Большие таблицы (>100M строк) нужно делить на партиции для manageability (DROP old partition) и query speed (partition pruning).

**Контекст этого шага.**

Шаг 1/3. Синтаксис + хранение. Шаги 2 (pruning), 3 (exchange/subpartition) — сверху.

**Задача.**

1. SQL: `CREATE TABLE ... PARTITION BY RANGE/LIST/HASH (col)`.
2. `CREATE TABLE ... PARTITION OF parent FOR VALUES ...`.
3. Storage: каждая партиция — отдельная таблица (в catalog помечена как partition of parent).
4. `PartitionManager`: lookup партиции для insert/select.
5. Default partition для не-матчащихся строк.

**Ключевые файлы.**

- `diesel/partition/PartitionManager.java`
- `diesel/partition/RangePartition.java`
- `diesel/partition/ListPartition.java`
- `diesel/partition/HashPartition.java`
- `diesel/QueryParser.java (PARTITION BY)`

**Критерии приёмки.**

- [ ] PartitionCreateTest: RANGE/LIST/HASH — все 3 типа создаются корректно.
- [ ] PartitionInsertTest: insert маршрутизируется в правильную партицию.
- [ ] DefaultPartitionTest: не-матчащаяся строка идёт в default.

---

### 64. Partitioning (range / list / hash): Partition pruning в query optimizer

**ID ROADMAP3:** R3-017  
**Категория:** E. Sharding & Partitioning  
**Приоритет:** MEDIUM  
**Фаза:** 2  
**Зависимости родителя:** промпт 2 (page storage), промпт 10 (online schema)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает.

**Проблема.**

Большие таблицы (>100M строк) нужно делить на партиции для manageability (DROP old partition) и query speed (partition pruning).

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Skip партиции на основе WHERE условия.

**Задача.**

1. Реализовать `PartitionPruner`: analyse WHERE, исключить партиции, не содержащие matching rows.
2. RANGE: `WHERE date < '2024-06-01'` → skip партиции позже 2024-06.
3. LIST: `WHERE country = 'US'` → skip партиции других стран.
4. HASH: pruning невозможен (только full scan всех hash partitions).
5. EXPLAIN показывает prune info.

**Ключевые файлы.**

- `diesel/partition/PartitionPruner.java`
- `diesel/QueryOptimizer.java (partition pruning integration)`

**Критерии приёмки.**

- [ ] PruningTest: 12 партиций × 10M строк, `WHERE date='2024-01-15'` → scan только 1 партиции (через EXPLAIN).
- [ ] RangePruningTest: `WHERE date BETWEEN '2024-01' AND '2024-03'` → 3 партиции.
- [ ] HashPruningTest: HASH partition — full scan всех партиций (pruning невозможен).

---

### 65. Partitioning (range / list / hash): EXCHANGE PARTITION + subpartitioning

**ID ROADMAP3:** R3-017  
**Категория:** E. Sharding & Partitioning  
**Приоритет:** MEDIUM  
**Фаза:** 2  
**Зависимости родителя:** промпт 2 (page storage), промпт 10 (online schema)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает.

**Проблема.**

Большие таблицы (>100M строк) нужно делить на партиции для manageability (DROP old partition) и query speed (partition pruning).

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Fast load/unload через metadata swap + 2-level partitioning.

**Задача.**

1. SQL: `ALTER TABLE ... EXCHANGE PARTITION name WITH TABLE other` — metadata swap, мгновенно.
2. Subpartitioning: 2-level (RANGE → LIST, RANGE → HASH).
3. Partition pruning на обоих уровнях.
4. Optional partition-wise join (Фаза 3).

**Ключевые файлы.**

- `diesel/partition/PartitionExchange.java`
- `diesel/partition/Subpartition.java`
- `diesel/QueryParser.java (EXCHANGE PARTITION)`

**Критерии приёмки.**

- [ ] ExchangeTest: EXCHANGE PARTITION — < 100 ms (metadata swap).
- [ ] SubpartitionTest: 2-level partition pruning корректно отсекает на обоих уровнях.
- [ ] ExchangeWithDataTest: exchange с таблицей, содержащей data — data swapped atomically.

---

### 66. Cost-Based Optimizer + статистика: StatisticsCollector (pg_stats analog)

**ID ROADMAP3:** R3-018  
**Категория:** H. Query optimizer  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 1 (MVCC), промпт 13 (row-id)  
**Зависимости этого шага:** промпты 1, 13  
**Связь с prompt3.md:** новое, косвенно Промпт 14 (статистика). `Roadmap.md` Этап 2.

**Проблема.**

Текущий `QueryOptimizer` — rule-based. Cost-based нужен для: выбора hash vs nested loop join, порядка таблиц в multi-join, использования index vs seq scan.

**Контекст этого шага.**

Шаг 1/4. Сбор статистики: null_frac, distinct, most_common_vals, histogram_bounds. Шаги 2 (cost model), 3 (join cost), 4 (plan cache + subquery unnesting) — выше.

**Задача.**

1. Реализовать `StatisticsCollector` (расширение `AnalyzeTableQuery`): для каждой колонки таблицы собирает:
   - `null_frac`: доля NULL
   - `distinct`: число distinct values
   - `most_common_vals`: top-N + frequencies
   - `histogram_bounds`: equi-depth histogram (default 100 buckets)
2. Storage: `diesel_stats` системная таблица.
3. SQL: `ANALYZE table_name` — пересбор статистики.
4. Auto-analyze: при изменении > 20 % строк (config `auto_analyze.threshold`).

**Ключевые файлы.**

- `diesel/optimizer/StatisticsCollector.java`
- `diesel/optimizer/ColumnStatistics.java`
- `diesel/optimizer/Histogram.java`
- `diesel/QueryParser.java (ANALYZE)`

**Критерии приёмки.**

- [ ] StatsTest: ANALYZE на 1M строк собирает корректную статистику (< 5 сек).
- [ ] HistogramTest: 100 buckets, distinct values распределены равномерно (equi-depth).
- [ ] AutoAnalyzeTest: 30 % DELETE + INSERT → auto-analyze триггерится.

---

### 67. Cost-Based Optimizer + статистика: CostEstimator + access path selection

**ID ROADMAP3:** R3-018  
**Категория:** H. Query optimizer  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 1 (MVCC), промпт 13 (row-id)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, косвенно Промпт 14 (статистика). `Roadmap.md` Этап 2.

**Проблема.**

Текущий `QueryOptimizer` — rule-based. Cost-based нужен для: выбора hash vs nested loop join, порядка таблиц в multi-join, использования index vs seq scan.

**Контекст этого шага.**

Шаг 2/4. Опирается на шаг 1. Cost model для scan/index access.

**Задача.**

1. Реализовать `CostEstimator`: cost = io_cost + cpu_cost.
2. Parameters (configurable): `seq_page_cost` (default 1.0), `random_page_cost` (default 4.0), `cpu_tuple_cost` (default 0.01).
3. Access paths:
   - Seq scan: cost ∝ total_rows × seq_page_cost
   - Index scan: cost ∝ matching_rows × random_page_cost
   - Index-only scan: covering index (см. промпт 42) — no heap access
   - Bitmap scan: hybrid
4. `WHERE id = 5` с unique index → index-only scan, не seq scan.
5. `WHERE non_indexed_col = 'X'` с selectivity 0.01 → seq scan (правильно).

**Ключевые файлы.**

- `diesel/optimizer/CostEstimator.java`
- `diesel/optimizer/AccessPath.java`
- `diesel/optimizer/SeqScanPath.java`
- `diesel/optimizer/IndexScanPath.java`
- `diesel/optimizer/BitmapScanPath.java`
- `diesel/ConfigLoader.java (seq_page_cost, random_page_cost, cpu_tuple_cost)`

**Критерии приёмки.**

- [ ] AccessPathTest: `WHERE id = 5` с unique index → index-only scan.
- [ ] SeqScanTest: `WHERE non_indexed_col = 'X'` selectivity 0.01 → seq scan.
- [ ] BitmapScanTest: medium selectivity (0.05) → bitmap scan.

---

### 68. Cost-Based Optimizer + статистика: Join algorithms cost + plan selection

**ID ROADMAP3:** R3-018  
**Категория:** H. Query optimizer  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 1 (MVCC), промпт 13 (row-id)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое, косвенно Промпт 14 (статистика). `Roadmap.md` Этап 2.

**Проблема.**

Текущий `QueryOptimizer` — rule-based. Cost-based нужен для: выбора hash vs nested loop join, порядка таблиц в multi-join, использования index vs seq scan.

**Контекст этого шага.**

Шаг 3/4. Опирается на шаги 1–2. Cost model для hash/nested loop/merge joins.

**Задача.**

1. Join algorithms cost:
   - Hash join: build_hash_cost(rows_outer) + probe_cost(rows_inner)
   - Nested loop: rows_outer × rows_inner × cpu_tuple_cost
   - Merge join: requires sorted input, cost ∝ (rows_outer + rows_inner)
2. Join order enumeration: dynamic programming для N-way joins (≤ 8 tables).
3. Push-down predicates в joins (eager WHERE).
4. `JoinAlgorithm` enum: NESTED_LOOP, HASH, MERGE.

**Ключевые файлы.**

- `diesel/optimizer/JoinAlgorithm.java`
- `diesel/optimizer/JoinCostModel.java`
- `diesel/optimizer/JoinOrderEnumerator.java`
- `diesel/optimizer/PredicatePushDown.java`

**Критерии приёмки.**

- [ ] HashJoinTest: 1k + 1M таблицы → hash join (small in hash-side).
- [ ] NestedLoopTest: оба маленькие → nested loop.
- [ ] JoinOrderTest: 4-table join — optimal order выбран (через EXPLAIN).
- [ ] PredicatePushDownTest: WHERE в outer join pushed to inner scan.

---

### 69. Cost-Based Optimizer + статистика: Subquery unnesting + PlanCache

**ID ROADMAP3:** R3-018  
**Категория:** H. Query optimizer  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** промпт 1 (MVCC), промпт 13 (row-id)  
**Зависимости этого шага:** шаги 1, 2, 3  
**Связь с prompt3.md:** новое, косвенно Промпт 14 (статистика). `Roadmap.md` Этап 2.

**Проблема.**

Текущий `QueryOptimizer` — rule-based. Cost-based нужен для: выбора hash vs nested loop join, порядка таблиц в multi-join, использования index vs seq scan.

**Контекст этого шага.**

Шаг 4/4. Опирается на шаги 1–3. Flatten correlated subqueries + plan caching.

**Задача.**

1. Subquery unnesting: flatten correlated subqueries в joins (для SELECT WHERE col IN (SELECT ...)).
2. PlanCache: ключ = normalized SQL (placeholders) + statistics snapshot version.
3. Cache invalidation при ANALYZE или schema change.
4. `enable_cbo = true | false` config switch для A/B тестирования.
5. EXPLAIN VERBOSE показывает выбранный план + cost breakdown.

**Ключевые файлы.**

- `diesel/optimizer/SubqueryUnnester.java`
- `diesel/optimizer/PlanCache.java`
- `diesel/optimizer/PlanCacheKey.java`
- `diesel/QueryOptimizer.java (переработка, integration)`
- `diesel/ConfigLoader.java (enable_cbo)`

**Критерии приёмки.**

- [ ] PlanStabilityTest: 100 случайных запросов — план deterministic при той же статистике.
- [ ] UnnestTest: correlated subquery → переписан в join.
- [ ] PlanCacheTest: повторный тот же запрос — cache hit, < 0.1 ms planning time.
- [ ] EnableCboTest: `enable_cbo=false` → rule-based fallback.

---

### 70. UPSERT / RETURNING / UPSERT-on-conflict: ON CONFLICT DO UPDATE / NOTHING (UPSERT)

**ID ROADMAP3:** R3-019  
**Категория:** G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое.

**Проблема.**

`INSERT ... ON CONFLICT DO UPDATE` (UPSERT) и `INSERT ... RETURNING *` — стандартные SQL:2003+. Без них сложно строить idempotent API.

**Контекст этого шага.**

Шаг 1/3. Ядро UPSERT. Шаги 2 (RETURNING), 3 (conflict target) — выше.

**Задача.**

1. SQL: `INSERT INTO ... ON CONFLICT (col) DO UPDATE SET ...` (UPSERT).
2. `ON CONFLICT DO NOTHING` — silent skip on conflict.
3. Атомарность: lock целевую строку, check conflict, insert/update — без race condition.
4. Параметры `EXCLUDED.col` для ссылки на insert-значения.

**Ключевые файлы.**

- `diesel/InsertQuery.java (расширение)`
- `diesel/QueryParser.java (ON CONFLICT syntax)`
- `diesel/expression/ExcludedReference.java`

**Критерии приёмки.**

- [ ] UpsertBasicTest: insert с existing PK → update.
UpsertNothingTest: ON CONFLICT DO NOTHING — silent skip.
- [ ] UpsertConcurrencyTest: 100 параллельных UPSERT на тот же PK — ровно 1 победитель, остальные update.
- [ ] ExcludedTest: `SET count = EXCLUDED.count + 1` работает.

---

### 71. UPSERT / RETURNING / UPSERT-on-conflict: RETURNING для INSERT/UPDATE/DELETE

**ID ROADMAP3:** R3-019  
**Категория:** G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

`INSERT ... ON CONFLICT DO UPDATE` (UPSERT) и `INSERT ... RETURNING *` — стандартные SQL:2003+. Без них сложно строить idempotent API.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. RETURNING clause для всех DML.

**Задача.**

1. `INSERT INTO ... RETURNING col1, col2` — вернуть вставленные строки.
2. `UPDATE ... RETURNING ...` — вернуть изменённые.
3. `DELETE ... RETURNING ...` — вернуть удалённые.
4. `RETURNING *` — все columns.
5. Integration с QueryResultMessage (промпт 7).

**Ключевые файлы.**

- `diesel/InsertQuery.java`
- `diesel/UpdateQuery.java`
- `diesel/DeleteQuery.java`
- `diesel/QueryParser.java (RETURNING syntax)`

**Критерии приёмки.**

- [ ] InsertReturningTest: RETURNING * возвращает все columns вставленной строки.
- [ ] UpdateReturningTest: UPDATE ... RETURNING — изменённые значения.
- [ ] DeleteReturningTest: DELETE ... RETURNING — удалённые строки.

---

### 72. UPSERT / RETURNING / UPSERT-on-conflict: Conflict target (column / constraint name)

**ID ROADMAP3:** R3-019  
**Категория:** G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

`INSERT ... ON CONFLICT DO UPDATE` (UPSERT) и `INSERT ... RETURNING *` — стандартные SQL:2003+. Без них сложно строить idempotent API.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Conflict target: по column или по constraint name.

**Задача.**

1. `ON CONFLICT (col)` — конфликт по конкретной колонке.
2. `ON CONFLICT ON CONSTRAINT constraint_name` — по имени constraint (unique index, FK).
3. `ON CONFLICT DO NOTHING` без target — любой conflict.
4. Тест: unique constraint + UPSERT по constraint name.

**Ключевые файлы.**

- `diesel/InsertQuery.java (conflict target resolution)`
- `diesel/QueryParser.java (ON CONSTRAINT)`

**Критерии приёмки.**

- [ ] ColumnTargetTest: ON CONFLICT (col) — корректно определяет conflict по колонке.
- [ ] ConstraintTargetTest: ON CONFLICT ON CONSTRAINT name — по имени constraint.
- [ ] NoTargetTest: ON CONFLICT DO NOTHING без target — любой conflict silent.

---

### 73. CI/CD v2 — coverage, matrix, releases: Surefire pattern fix + JaCoCo + Sonar

**ID ROADMAP3:** R3-020  
**Категория:** K. CI/CD  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое. `cicd.md` P0/P1 описано, но не сделано.

**Проблема.**

Сейчас: только 2 теста из ~100 запускаются в CI; нет coverage; нет release pipeline.

**Контекст этого шага.**

Шаг 1/3. Базовый CI: все тесты запускаются, coverage считается. Шаги 2 (matrix), 3 (release) — выше.

**Задача.**

1. Surefire pattern fix: `**/*Test.java, **/*Tests.java, **/Test*.java`.
2. JaCoCo plugin в pom.xml: coverage threshold 60 %, master > 70 %.
3. Codecov integration (codecov.yml).
4. SonarQube Quality Gate: bugs=0, critical=0 → блокирует merge.
5. `.github/workflows/ci.yml` переработан.

**Ключевые файлы.**

- `.github/workflows/ci.yml`
- `pom.xml (jacoco-maven-plugin, surefire config)`
- `codecov.yml`
- `sonar-project.properties`

**Критерии приёмки.**

- [ ] AllTestsRunTest: все ~100 тестов запускаются в CI < 10 мин.
- [ ] CoverageTest: coverage > 60 % для проекта, > 80 % для `diesel/` core.
- [ ] SonarGateTest: bugs > 0 → workflow fail, merge blocked.

---

### 74. CI/CD v2 — coverage, matrix, releases: Matrix build (JDK 17/21/25, OS matrix)

**ID ROADMAP3:** R3-020  
**Категория:** K. CI/CD  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое. `cicd.md` P0/P1 описано, но не сделано.

**Проблема.**

Сейчас: только 2 теста из ~100 запускаются в CI; нет coverage; нет release pipeline.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Matrix для кросс-платформенной совместимости.

**Задача.**

1. Matrix build: JDK 17 / 21 / 25 (LTS).
2. OS matrix: Windows / Linux / macOS.
3. Кеширование `.m2` для ускорения build.
4. Fail-fast: false (все combinations запускаются).

**Ключевые файлы.**

- `.github/workflows/ci.yml (matrix)`

**Критерии приёмки.**

- [ ] MatrixTest: 9 combinations (3 JDK × 3 OS) — все green.
- [ ] CacheTest: build с кешем .m2 — < 3 мин (vs 10 мин без кеша).

---

### 75. CI/CD v2 — coverage, matrix, releases: Release pipeline + Docker + Helm

**ID ROADMAP3:** R3-020  
**Категория:** K. CI/CD  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое. `cicd.md` P0/P1 описано, но не сделано.

**Проблема.**

Сейчас: только 2 теста из ~100 запускаются в CI; нет coverage; нет release pipeline.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Release на Maven Central, Docker image, Helm chart, perf regression.

**Задача.**

1. Release pipeline: git tag → Maven Central + GitHub Release + Docker image.
2. Container images: `dieseldb/dieseldb:latest`, `:lts`, `:X.Y.Z`.
3. Helm chart в `charts/dieseldb/`.
4. Performance regression: TPC-C small (10 warehouses) и TPC-H small (SF=1) еженедельно.
5. `.github/workflows/release.yml`, `.github/workflows/perf.yml`.

**Ключевые файлы.**

- `.github/workflows/release.yml`
- `.github/workflows/perf.yml`
- `Dockerfile`
- `charts/dieseldb/Chart.yaml`
- `charts/dieseldb/values.yaml`

**Критерии приёмки.**

- [ ] ReleaseTest: git tag v1.0.0 → release pipeline triggers, Maven Central published.
- [ ] DockerTest: `docker pull dieseldb/dieseldb:latest` + `docker run -p 5432:5432` работает.
- [ ] HelmTest: `helm install dieseldb ./charts/dieseldb` — кластер поднимается.
- [ ] PerfRegressionTest: TPC-C/H weekly — результаты в `analytics/perf_history.csv`.

---

---

## Фаза 3 — PostgreSQL-Parity (12-24+ месяцев)

**Цель:** достичь ~90 % production-readiness и ~85 % PostgreSQL SQL coverage. Промпты ниже развёрнуты из таблицы §7 ROADMAP3 в полные карточки.

### 76. CTE + рекурсивные CTE: Non-recursive CTE (WITH name AS (SELECT ...))

**ID ROADMAP3:** R3-021  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** промпт 18  
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 2.

**Проблема.**

Без CTE невозможно выражать иерархические запросы (деревья, графы) и сложные аналитические запросы в читаемом виде. Текущий парсер не понимает `WITH`-clause.

**Контекст этого шага.**

Шаг 1/3. Базовый CTE без рекурсии. Шаги 2 (recursive), 3 (MATERIALIZED hint) — сверху.

**Задача.**

1. SQL: `WITH name AS (SELECT ...) SELECT ... FROM name`.
2. Несколько CTE в одном запросе: `WITH a AS (...), b AS (...) ...` с возможностью ссылаться на предыдущие.
3. `CteRegistry`: scope-chain для resolution (внутренний CTE тенит outer с тем же именем).
4. CTE в подзапросах, INSERT INTO ... SELECT FROM cte.

**Ключевые файлы.**

- `diesel/cte/CommonTableExpression.java`
- `diesel/cte/CteRegistry.java`
- `diesel/QueryParser.java (WITH syntax)`

**Критерии приёмки.**

- [ ] NonRecursiveCteTest: WITH clause с 3 CTE — все разрешаются корректно.
- [ ] CteInSubqueryTest: CTE в подзапросе работает.
- [ ] CteShadowingTest: внутренний CTE тенит outer — корректно.

---

### 77. CTE + рекурсивные CTE: Recursive CTE (WITH RECURSIVE) + termination check

**ID ROADMAP3:** R3-021  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 2.

**Проблема.**

Без CTE невозможно выражать иерархические запросы (деревья, графы) и сложные аналитические запросы в читаемом виде. Текущий парсер не понимает `WITH`-clause.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Recursive CTE с итеративным вычислением.

**Задача.**

1. SQL: `WITH RECURSIVE name AS (SELECT ... UNION ALL SELECT ... FROM name WHERE ...) SELECT ... FROM name`.
2. Алгоритм: iterative evaluation — повторять recursive part пока новые строки генерируются.
3. Termination check: если N итераций без новых строк — stop (default max 1000 итераций, configurable).
4. `RecursiveCteIterator` — pull-based iterator, не материализует всё в память.

**Ключевые файлы.**

- `diesel/cte/RecursiveCteIterator.java`
- `diesel/cte/RecursiveCteTerminator.java`
- `diesel/QueryParser.java (RECURSIVE keyword)`

**Критерии приёмки.**

- [ ] RecursiveCteTest: дерево 1000 узлов, запрос всех потомков корня → корректный результат за < 1 сек.
- [ ] CycleProtectionTest: цикл в данных → termination by max iterations, не infinite loop.
- [ ] MemoryTest: рекурсия на 1M узлов — heap < 500 MB (streaming iterator).

---

### 78. CTE + рекурсивные CTE: MATERIALIZED / NOT MATERIALIZED hints + CBO integration

**ID ROADMAP3:** R3-021  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаги 1, 2, промпт 18  
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 2.

**Проблема.**

Без CTE невозможно выражать иерархические запросы (деревья, графы) и сложные аналитические запросы в читаемом виде. Текущий парсер не понимает `WITH`-clause.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Hint для контроля inline vs materialize.

**Задача.**

1. SQL: `WITH name AS MATERIALIZED (SELECT ...)` — форсировать materialization.
2. `WITH name AS NOT MATERIALIZED (SELECT ...)` — форсировать inline (subquery expansion).
3. CBO (промпт 18) принимает решение по умолчанию (по cost).
4. EXPLAIN показывает решение (materialized vs inline).

**Ключевые файлы.**

- `diesel/cte/MaterializationHint.java`
- `diesel/QueryOptimizer.java (CTE materialization decision)`

**Критерии приёмки.**

- [ ] MaterializedTest: MATERIALIZED — CTE вычисляется один раз, inline НЕ.
NotMaterializedTest: NOT MATERIALIZED — CTE inline в query, видно в EXPLAIN.
- [ ] CboDecisionTest: без hint CBO выбирает по cost.
- [ ] PerformanceTest: TPC-H Q1 (с CTE) не хуже, чем без CTE на 10 %.

---

### 79. Оконные функции: Базовые функции: ROW_NUMBER / RANK / DENSE_RANK / NTILE

**ID ROADMAP3:** R3-022  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет и поглощает Промпт 131 (Window Functions). Промпт 131 описывает базовый набор; R3-022 добавляет frame-семантику и оптимизацию.

**Проблема.**

Оконные функции (ROW_NUMBER, RANK, LAG, LEAD, NTILE, агрегаты OVER) — стандарт SQL:2003, без них невозможно писать top-N-per-group, running totals, time-series аналитику.

**Контекст этого шага.**

Шаг 1/3. Базовый набор ranking функций. Шаги 2 (navigation + aggregates), 3 (frame) — сверху.

**Задача.**

1. Реализовать `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`.
2. OVER clause: `PARTITION BY col ORDER BY col`.
3. `WindowSpec` парсит PARTITION BY / ORDER BY.
4. Сортировка один раз на partition, обход window-by-window.

**Ключевые файлы.**

- `diesel/window/WindowFunction.java`
- `diesel/window/WindowSpec.java`
- `diesel/window/RowNumberFunction.java`
- `diesel/window/RankFunction.java`
- `diesel/window/DenseRankFunction.java`
- `diesel/window/NtileFunction.java`
- `diesel/QueryParser.java (OVER clause)`

**Критерии приёмки.**

- [ ] RowNumberTest: ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) — корректная нумерация.
- [ ] RankTest: RANK и DENSE_RANK корректно обрабатывают ties.
- [ ] NtileTest: NTILE(4) на 100 строк → 4 buckets по 25.

---

### 80. Оконные функции: Navigation: LAG / LEAD / FIRST_VALUE / LAST_VALUE / NTH_VALUE

**ID ROADMAP3:** R3-022  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет и поглощает Промпт 131 (Window Functions). Промпт 131 описывает базовый набор; R3-022 добавляет frame-семантику и оптимизацию.

**Проблема.**

Оконные функции (ROW_NUMBER, RANK, LAG, LEAD, NTILE, агрегаты OVER) — стандарт SQL:2003, без них невозможно писать top-N-per-group, running totals, time-series аналитику.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Navigation functions для time-series и adjacent-row comparisons.

**Задача.**

1. `LAG(col, n, default)` — значение n строк назад.
2. `LEAD(col, n, default)` — значение n строк вперёд.
3. `FIRST_VALUE(col)`, `LAST_VALUE(col)`, `NTH_VALUE(col, n)`.
4. Default для LAG/LEAD: NULL если не задан explicit default.

**Ключевые файлы.**

- `diesel/window/LagFunction.java`
- `diesel/window/LeadFunction.java`
- `diesel/window/FirstValueFunction.java`
- `diesel/window/LastValueFunction.java`
- `diesel/window/NthValueFunction.java`

**Критерии приёмки.**

- [ ] LagLeadTest: time-series с LAG/LEAD — корректные значения соседних строк.
- [ ] FirstLastValueTest: FIRST_VALUE = первой строке partition, LAST_VALUE = последней.
- [ ] DefaultTest: LAG без default → NULL на первой строке.

---

### 81. Оконные функции: Aggregates OVER + Frame (ROWS/RANGE BETWEEN)

**ID ROADMAP3:** R3-022  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** уточняет и поглощает Промпт 131 (Window Functions). Промпт 131 описывает базовый набор; R3-022 добавляет frame-семантику и оптимизацию.

**Проблема.**

Оконные функции (ROW_NUMBER, RANK, LAG, LEAD, NTILE, агрегаты OVER) — стандарт SQL:2003, без них невозможно писать top-N-per-group, running totals, time-series аналитику.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Агрегаты OVER + frame-спецификации.

**Задача.**

1. `SUM/AVG/COUNT/MIN/MAX(col) OVER (PARTITION BY ... ORDER BY ...)`.
2. Frame: `ROWS BETWEEN N PRECEDING AND N FOLLOWING`, `RANGE BETWEEN ...`, `UNBOUNDED PRECEDING/FOLLOWING`, `CURRENT ROW`.
3. Default frame: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
4. Оптимизация: сортировка один раз, агрегация sliding window через prefix-sum где возможно.

**Ключевые файлы.**

- `diesel/window/WindowFrameEvaluator.java`
- `diesel/window/WindowFrameSpec.java`
- `diesel/window/AggregateOverFunction.java`
- `diesel/QueryExecutor.java (window pipeline)`

**Критерии приёмки.**

- [ ] AggregateOverTest: SUM(amount) OVER (PARTITION BY dept) — dept-wide total.
- [ ] RunningTotalTest: SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) — running total.
- [ ] FrameTest: 5 frame variants × 20 функций = 100 случаев, все green.
- [ ] PerformanceTest: TPC-H Q1 (с оконными функциями) не хуже PG × 2.

---

### 82. FULL OUTER JOIN, LATERAL, CROSS APPLY: FULL OUTER JOIN

**ID ROADMAP3:** R3-023  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 2.

**Проблема.**

Без FULL OUTER JOIN нельзя объединять таблицы с preservation строк с обеих сторон. LATERAL / CROSS APPLY нужны для коррелированных подзапросов в FROM.

**Контекст этого шага.**

Шаг 1/3. Ядро FULL OUTER. Шаги 2 (LATERAL), 3 (APPLY) — сверху.

**Задача.**

1. Реализовать `FULL OUTER JOIN`: left + right + unmatched с обеих сторон (NULL-fill).
2. Алгоритм: hash join с обеими sides в hash-table, unmatched из обеих идут в result с NULL.
3. Проверить корректность LEFT/RIGHT OUTER JOIN (existing) — NULL-fill должен быть корректным.

**Ключевые файлы.**

- `diesel/join/FullOuterJoinExecutor.java`
- `diesel/QueryParser.java (FULL OUTER syntax)`

**Критерии приёмки.**

- [ ] FullOuterJoinTest: 100 + 80 строк, overlap 50 → 130 строк в результате.
- [ ] NullFillTest: unmatched строки с обеих сторон — NULL в соответствующих колонках.

---

### 83. FULL OUTER JOIN, LATERAL, CROSS APPLY: LATERAL JOIN (correlated subquery in FROM)

**ID ROADMAP3:** R3-023  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 2.

**Проблема.**

Без FULL OUTER JOIN нельзя объединять таблицы с preservation строк с обеих сторон. LATERAL / CROSS APPLY нужны для коррелированных подзапросов в FROM.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Коррелированный подзапрос в FROM с доступом к колонкам outer.

**Задача.**

1. SQL: `CROSS JOIN LATERAL (subquery referencing outer columns)`.
2. Семантика: subquery выполняется один раз для каждой строки outer.
3. Оптимизация: переписать в nested-loop join где возможно.
4. Поддержка multiple LATERAL в одном FROM.

**Ключевые файлы.**

- `diesel/join/LateralJoinExecutor.java`
- `diesel/QueryParser.java (LATERAL syntax)`

**Критерии приёмки.**

- [ ] LateralJoinTest: коррелированный подзапрос возвращает N строк per outer row.
- [ ] MultipleLateralTest: 2 LATERAL в одном FROM разрешаются корректно.

---

### 84. FULL OUTER JOIN, LATERAL, CROSS APPLY: CROSS APPLY / OUTER APPLY (T-SQL compat)

**ID ROADMAP3:** R3-023  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 2.

**Проблема.**

Без FULL OUTER JOIN нельзя объединять таблицы с preservation строк с обеих сторон. LATERAL / CROSS APPLY нужны для коррелированных подзапросов в FROM.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. T-SQL совместимый синтаксис.

**Задача.**

1. `CROSS APPLY (subquery)` — эквивалент INNER JOIN LATERAL (unmatched outer rows dropped).
2. `OUTER APPLY (subquery)` — эквивалент LEFT JOIN LATERAL (unmatched outer rows preserved with NULL).
3. Совместимость с SQL Server / Sybase синтаксисом.

**Ключевые файлы.**

- `diesel/join/CrossApplyExecutor.java`
- `diesel/join/OuterApplyExecutor.java`
- `diesel/QueryParser.java (APPLY syntax)`

**Критерии приёмки.**

- [ ] CrossApplyTest: эквивалент INNER JOIN LATERAL — unmatched dropped.
- [ ] OuterApplyTest: unmatched outer строки сохраняются с NULL.

---

### 85. UNION / INTERSECT / EXCEPT: UNION + UNION ALL (streaming merge)

**ID ROADMAP3:** R3-024  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет и поглощает Промпт 115.

**Проблема.**

Set operations нужны для слияния результатов нескольких SELECT. Сейчас не реализованы.

**Контекст этого шага.**

Шаг 1/3. Ядро UNION. Шаги 2 (INTERSECT/EXCEPT), 3 (precedence + скобки) — сверху.

**Задача.**

1. `UNION` — удалить дубликаты через hash/sort distinct.
2. `UNION ALL` — без удаления дубликатов, потоковая обработка.
3. Type compatibility check: колонки всех SELECT должны иметь совместимые типы.
4. Column naming: имена из первого SELECT.

**Ключевые файлы.**

- `diesel/setop/UnionQuery.java`
- `diesel/setop/SetOperationExecutor.java`
- `diesel/QueryParser.java (UNION syntax)`

**Критерии приёмки.**

- [ ] UnionTest: UNION корректно удаляет дубликаты (1M строк → ~500k уникальных).
- [ ] UnionAllTest: UNION ALL не удаляет дубликаты, throughput ≥ 1M rows/sec.
- [ ] TypeCompatTest: разные типы колонок → понятная ошибка.

---

### 86. UNION / INTERSECT / EXCEPT: INTERSECT + EXCEPT (set difference)

**ID ROADMAP3:** R3-024  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет и поглощает Промпт 115.

**Проблема.**

Set operations нужны для слияния результатов нескольких SELECT. Сейчас не реализованы.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. INTERSECT (общие строки), EXCEPT (разность).

**Задача.**

1. `INTERSECT` — общие строки двух запросов (hash-join semantics).
2. `EXCEPT` — строки первого запроса минус строки второго.
3. NULL-семантика: NULL = NULL при set operations (в отличие от обычного comparison).
4. Удаление дубликатов по умолчанию (INTERSECT DISTINCT), `INTERSECT ALL` сохраняет дубликаты.

**Ключевые файлы.**

- `diesel/setop/IntersectQuery.java`
- `diesel/setop/ExceptQuery.java`
- `diesel/QueryParser.java (INTERSECT/EXCEPT)`

**Критерии приёмки.**

- [ ] IntersectTest: 100 + 80 строк, overlap 50 → 50 строк в результате.
- [ ] ExceptTest: 100 - 50 = 50 строк.
- [ ] NullSemanticsTest: NULL = NULL при INTERSECT/EXCEPT, корректно.

---

### 87. UNION / INTERSECT / EXCEPT: Precedence + скобки

**ID ROADMAP3:** R3-024  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** уточняет и поглощает Промпт 115.

**Проблема.**

Set operations нужны для слияния результатов нескольких SELECT. Сейчас не реализованы.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Цепочки set ops с правильным приоритетом.

**Задача.**

1. Цепочки: `SELECT ... UNION SELECT ... INTERSECT SELECT ...` с правильным приоритетом (INTERSECT > UNION).
2. Скобки: `(SELECT ... UNION SELECT ...) EXCEPT SELECT ...` — управление порядком.
3. Парсер: поддержка скобок в set operations.

**Ключевые файлы.**

- `diesel/QueryParser.java (precedence, parens)`
- `diesel/setop/SetOperationPrecedence.java`

**Критерии приёмки.**

- [ ] PrecedenceTest: `A UNION B INTERSECT C` → `A UNION (B INTERSECT C)`.
- [ ] ParensTest: `(A UNION B) INTERSECT C` → корректный порядок.
- [ ] ComplexChainTest: 5 SELECTs со смесью UNION/INTERSECT/EXCEPT — корректный результат.

---

### 88. Foreign Keys с CASCADE: FOREIGN KEY + проверка referential integrity

**ID ROADMAP3:** R3-025  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 10 (online alter для constraint)  
**Зависимости этого шага:** промпт 10  
**Связь с prompt3.md:** уточняет и поглощает Промпт 128 (Foreign Keys с каскадными операциями). R3-025 добавляет online constraint addition и multi-level cascade.

**Проблема.**

Referential integrity — основа реляционной БД. Сейчас FK нет, любой INSERT/UPDATE может нарушить целостность.

**Контекст этого шага.**

Шаг 1/3. Ядро FK: определение + проверка на INSERT/UPDATE child. Шаги 2 (CASCADE), 3 (online add) — сверху.

**Задача.**

1. SQL: `FOREIGN KEY (col) REFERENCES parent(id)` при CREATE TABLE / ALTER TABLE.
2. Проверка на INSERT/UPDATE в child: lookup parent, если нет — ForeignKeyViolationException.
3. `ON DELETE NO ACTION` / `ON DELETE RESTRICT` (default) — forbid delete parent if children exist.
4. `ON UPDATE NO ACTION` — forbid update parent PK if children reference it.

**Ключевые файлы.**

- `diesel/constraint/ForeignKeyConstraint.java`
- `diesel/constraint/ConstraintValidator.java`
- `diesel/QueryParser.java (REFERENCES syntax)`

**Критерии приёмки.**

- [ ] FkBasicTest: INSERT child с несуществующим parent → exception.
- [ ] FkRestrictTest: DELETE parent с children → exception.
- [ ] FkUpdateParentTest: UPDATE parent PK с children → exception.

---

### 89. Foreign Keys с CASCADE: CASCADE DELETE/UPDATE/SET NULL + multi-level

**ID ROADMAP3:** R3-025  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 10 (online alter для constraint)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет и поглощает Промпт 128 (Foreign Keys с каскадными операциями). R3-025 добавляет online constraint addition и multi-level cascade.

**Проблема.**

Referential integrity — основа реляционной БД. Сейчас FK нет, любой INSERT/UPDATE может нарушить целостность.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Cascade operations + многоуровневые.

**Задача.**

1. `ON DELETE CASCADE` — удалить children при delete parent.
2. `ON DELETE SET NULL` — выставить NULL в child.
3. `ON UPDATE CASCADE` — обновить FK в child при update parent PK.
4. Multi-level cascade (parent → child → grandchild) с защитой от бесконечной рекурсии (depth limit, cycle detection).

**Ключевые файлы.**

- `diesel/constraint/CascadeDeleteQuery.java`
- `diesel/constraint/CascadeUpdateQuery.java`
- `diesel/constraint/CascadeExecutor.java (cycle protection)`

**Критерии приёмки.**

- [ ] CascadeDeleteTest: DELETE parent → cascade DELETE детей и внуков (3 уровня).
- [ ] SetNullTest: ON DELETE SET NULL корректно выставляет NULL.
- [ ] MultiLevelTest: 3 уровня cascade, всё корректно удаляется.
- [ ] CycleProtectionTest: циклический FK → exception, не infinite loop.

---

### 90. Foreign Keys с CASCADE: Online ADD CONSTRAINT NOT VALID + background validation

**ID ROADMAP3:** R3-025  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 10 (online alter для constraint)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** уточняет и поглощает Промпт 128 (Foreign Keys с каскадными операциями). R3-025 добавляет online constraint addition и multi-level cascade.

**Проблема.**

Referential integrity — основа реляционной БД. Сейчас FK нет, любой INSERT/UPDATE может нарушить целостность.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Online constraint addition без блокировки writers.

**Задача.**

1. SQL: `ALTER TABLE ... ADD CONSTRAINT ... NOT VALID` — добавить FK без проверки существующих данных.
2. Background validation: `VALIDATE CONSTRAINT name` — проверяет существующие данные в фоне.
3. Если validation fails — constraint помечен as invalid, DBA может fix или drop.
4. Не блокирует writers > 100 ms.

**Ключевые файлы.**

- `diesel/constraint/ConstraintValidator.java (background mode)`
- `diesel/QueryParser.java (NOT VALID, VALIDATE CONSTRAINT)`

**Критерии приёмки.**

- [ ] OnlineAddConstraintTest: ADD CONSTRAINT NOT VALID — < 100 ms на 10M-строчной таблице.
- [ ] ValidateTest: VALIDATE CONSTRAINT — проходит на consistent данных, fails на inconsistent.
- [ ] InvalidConstraintTest: invalid constraint не блокирует DML, но помечен в catalog.

---

### 91. CHECK constraints, NOT NULL, DEFAULT: CHECK constraint + named constraints

**ID ROADMAP3:** R3-026  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет и поглощает Промпт 129 (CHECK Constraints). R3-026 добавляет NOT NULL enforcement, DEFAULT expressions, named constraints.

**Проблема.**

CHECK constraints нужны для domain integrity. NOT NULL и DEFAULT — основа schema design.

**Контекст этого шага.**

Шаг 1/3. CHECK constraint с condition. Шаги 2 (NOT NULL), 3 (DEFAULT) — сверху.

**Задача.**

1. SQL: `CHECK (condition)` при CREATE TABLE / ALTER TABLE.
2. Валидация при INSERT/UPDATE: вычисление condition на каждой изменённой строке.
3. Составные условия: `CHECK (age > 0 AND age < 150 AND email LIKE '%@%')`.
4. Named constraints: `CONSTRAINT name CHECK (...)` для управления (drop by name).

**Ключевые файлы.**

- `diesel/constraint/CheckConstraint.java`
- `diesel/constraint/NamedConstraint.java`
- `diesel/constraint/ConstraintRegistry.java`
- `diesel/QueryParser.java (CHECK/CONSTRAINT)`

**Критерии приёмки.**

- [ ] CheckConstraintTest: 10 различных CHECK conditions, все violation cases ловятся.
- [ ] NamedConstraintTest: DROP CONSTRAINT name — удаляет именно этот constraint.
- [ ] CompositeCheckTest: составные условия корректно вычисляются.

---

### 92. CHECK constraints, NOT NULL, DEFAULT: NOT NULL enforcement + DEFAULT expressions

**ID ROADMAP3:** R3-026  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет и поглощает Промпт 129 (CHECK Constraints). R3-026 добавляет NOT NULL enforcement, DEFAULT expressions, named constraints.

**Проблема.**

CHECK constraints нужны для domain integrity. NOT NULL и DEFAULT — основа schema design.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. NOT NULL на уровне колонки + DEFAULT для INSERT без значения.

**Задача.**

1. `col TYPE NOT NULL` — forbid NULL on INSERT/UPDATE.
2. `col TYPE DEFAULT expr` — подставлять default если колонка не указана в INSERT.
3. Default expressions: literals (число, строка), `NOW()`, `CURRENT_USER`, `NEXTVAL(seq)`, deterministic expressions.
4. `DEFAULT NULL` явно — для clarity.

**Ключевые файлы.**

- `diesel/constraint/NotNullConstraint.java`
- `diesel/constraint/DefaultExpression.java`
- `diesel/QueryParser.java (NOT NULL, DEFAULT)`

**Критерии приёмки.**

- [ ] NotNullTest: INSERT без значения в NOT NULL колонку → exception.
- [ ] NotNullUpdateTest: UPDATE col=NULL на NOT NULL → exception.
- [ ] DefaultLiteralTest: DEFAULT 0 → 0 при INSERT без значения.
- [ ] DefaultExpressionTest: DEFAULT NOW() — корректная timestamp.
- [ ] DefaultSequenceTest: DEFAULT NEXTVAL(seq) — корректный sequence value.

---

### 93. CHECK constraints, NOT NULL, DEFAULT: DROP CONSTRAINT + dependency check

**ID ROADMAP3:** R3-026  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** уточняет и поглощает Промпт 129 (CHECK Constraints). R3-026 добавляет NOT NULL enforcement, DEFAULT expressions, named constraints.

**Проблема.**

CHECK constraints нужны для domain integrity. NOT NULL и DEFAULT — основа schema design.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Управление constraints после создания.

**Задача.**

1. SQL: `ALTER TABLE ... DROP CONSTRAINT name`.
2. Если constraint зависит от другого объекта (FK depends on unique index) — forbid без CASCADE.
3. `DROP CONSTRAINT name CASCADE` — каскадно удалить зависимые объекты.
4. Constraint metadata persisted, survive restart.

**Ключевые файлы.**

- `diesel/constraint/ConstraintRegistry.java (drop method)`
- `diesel/constraint/DependencyChecker.java`
- `diesel/QueryParser.java (DROP CONSTRAINT)`

**Критерии приёмки.**

- [ ] DropConstraintTest: DROP CONSTRAINT — удаляет, INSERT больше не валидирует.
- [ ] DropCascadeTest: DROP CONSTRAINT CASCADE — каскадно удаляет зависимые FK.
- [ ] DropWithDependencyTest: DROP без CASCADE при dependent FK → exception.

---

### 94. Materialized Views + refresh: CREATE MATERIALIZED VIEW + REFRESH MANUAL

**ID ROADMAP3:** R3-027  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет и поглощает Промпт 127 (Materialized Views). R3-027 добавляет refresh strategies и query rewrite.

**Проблема.**

Materialized views позволяют кэшировать дорогие запросы. Без них OLAP-нагрузки на DieselDB будут медленными.

**Контекст этого шага.**

Шаг 1/3. Базовая MV: создание, manual refresh. Шаги 2 (concurrent refresh), 3 (query rewrite) — сверху.

**Задача.**

1. SQL: `CREATE MATERIALIZED VIEW name AS SELECT ...` — физическое хранение результата.
2. `REFRESH MATERIALIZED VIEW name` — пересчёт (блокирует readers на время).
3. MV хранится как обычная таблица, но с пометкой `is_materialized_view=true` в catalog.
4. Indexes на MV (как на обычной таблице).

**Ключевые файлы.**

- `diesel/materializedview/CreateMaterializedViewQuery.java`
- `diesel/materializedview/MaterializedViewManager.java`
- `diesel/materializedview/MaterializedViewRefresher.java`
- `diesel/QueryParser.java (MATERIALIZED VIEW)`

**Критерии приёмки.**

- [ ] CreateMvTest: CREATE MATERIALIZED VIEW на 1M-строчной базе — < 30 сек.
- [ ] RefreshManualTest: REFRESH — пересчёт, новые данные видны.
- [ ] MvWithIndexTest: CREATE INDEX на MV работает.

---

### 95. Materialized Views + refresh: REFRESH ON COMMIT + REFRESH EVERY + CONCURRENTLY

**ID ROADMAP3:** R3-027  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет и поглощает Промпт 127 (Materialized Views). R3-027 добавляет refresh strategies и query rewrite.

**Проблема.**

Materialized views позволяют кэшировать дорогие запросы. Без них OLAP-нагрузки на DieselDB будут медленными.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Стратегии refresh: на COMMIT, по расписанию, concurrent.

**Задача.**

1. `REFRESH ON COMMIT` — пересчёт в той же транзакции, что и DML на underlying table.
2. `REFRESH EVERY '1 hour'` — фоновый refresh по расписанию (через scheduler).
3. `REFRESH MATERIALIZED VIEW name CONCURRENTLY` — concurrent refresh без блокировки readers (через diff-based refresh).
4. Incremental refresh: только изменившиеся строки (через delta-tables).

**Ключевые файлы.**

- `diesel/materializedview/IncrementalRefresher.java`
- `diesel/materializedview/RefreshStrategy.java`
- `diesel/materializedview/ConcurrentRefresher.java`

**Критерии приёмки.**

- [ ] RefreshOnCommitTest: INSERT в underlying + COMMIT → MV обновлена.
- [ ] RefreshEveryTest: cron `EVERY '1 hour'` — триггерится автоматически.
- [ ] ConcurrentRefreshTest: SELECT во время REFRESH CONCURRENTLY — не блокируется.
- [ ] IncrementalRefreshTest: 1 % изменений → refresh time < 5 % от full refresh.

---

### 96. Materialized Views + refresh: Query rewrite (CBO automatically uses MV)

**ID ROADMAP3:** R3-027  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2, промпт 18  
**Связь с prompt3.md:** уточняет и поглощает Промпт 127 (Materialized Views). R3-027 добавляет refresh strategies и query rewrite.

**Проблема.**

Materialized views позволяют кэшировать дорогие запросы. Без них OLAP-нагрузки на DieselDB будут медленными.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Оптимизатор автоматически переписывает запрос на использование MV при совпадении.

**Задача.**

1. `QueryRewriter` в CBO (промпт 18): pattern matching SQL запроса с определением MV.
2. Если запрос = `SELECT count(*) FROM big_table` и есть MV с тем же запросом → rewrite to `SELECT count FROM mv`.
3. View matching: поддерживает и subset queries (если MV содержит больше колонок, чем запрос — rewrite возможен).
4. EXPLAIN показывает, что использован MV.

**Ключевые файлы.**

- `diesel/materializedview/QueryRewriter.java`
- `diesel/QueryOptimizer.java (MV rewrite integration)`

**Критерии приёмки.**

- [ ] QueryRewriteTest: `SELECT count(*) FROM big_table` → автоматически использует MV, latency < 1 ms.
- [ ] SubsetQueryTest: query использует subset of MV columns → rewrite работает.
- [ ] ExplainShowsMvTest: EXPLAIN показывает `MaterializedView Scan`.

---

### 97. Triggers BEFORE/AFTER: BEFORE triggers (modify NEW.row перед INSERT)

**ID ROADMAP3:** R3-028  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 1 (MVCC)  
**Зависимости этого шага:** промпт 1  
**Связь с prompt3.md:** новое.

**Проблема.**

Triggers нужны для audit logging, derived columns, cross-table consistency checks.

**Контекст этого шага.**

Шаг 1/3. BEFORE triggers для модификации строки до записи. Шаги 2 (AFTER), 3 (statement-level) — сверху.

**Задача.**

1. SQL: `CREATE TRIGGER name BEFORE INSERT OR UPDATE ON table FOR EACH ROW EXECUTE ...`.
2. BEFORE triggers могут модифицировать `NEW.row` (изменить значения перед INSERT).
3. Trigger body — SQL-only (без PL/pgSQL): один SELECT/INSERT/UPDATE/DELETE.
4. Если BEFORE trigger fails (exception) — DML откачено.

**Ключевые файлы.**

- `diesel/trigger/Trigger.java`
- `diesel/trigger/TriggerManager.java`
- `diesel/trigger/TriggerExecutor.java`
- `diesel/QueryParser.java (CREATE TRIGGER)`

**Критерии приёмки.**

- [ ] BeforeInsertTriggerTest: trigger модифицирует `created_at = NOW()` перед INSERT.
- [ ] BeforeUpdateTriggerTest: trigger может отменить UPDATE через exception.

---

### 98. Triggers BEFORE/AFTER: AFTER triggers (side effects: audit log, cascade)

**ID ROADMAP3:** R3-028  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 1 (MVCC)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

Triggers нужны для audit logging, derived columns, cross-table consistency checks.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. AFTER triggers для side effects (выполняются после DML).

**Задача.**

1. SQL: `AFTER INSERT OR UPDATE OR DELETE ON table FOR EACH ROW EXECUTE ...`.
2. AFTER triggers видят OLD.row (для DELETE/UPDATE) и NEW.row (для INSERT/UPDATE).
3. Side effects: INSERT в audit_log, cascade updates на другие таблицы.
4. Trigger ordering: multiple AFTER triggers на одной таблице — упорядочение по priority field.

**Ключевые файлы.**

- `diesel/trigger/AfterTriggerExecutor.java`
- `diesel/trigger/TriggerPriorityComparator.java`

**Критерии приёмки.**

- [ ] AfterDeleteTriggerTest: trigger пишет в audit_log после DELETE.
- [ ] MultipleTriggersTest: 3 AFTER triggers с разными priorities — выполняются в порядке priority.

---

### 99. Triggers BEFORE/AFTER: Statement-level triggers (FOR EACH STATEMENT)

**ID ROADMAP3:** R3-028  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 1 (MVCC)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

Triggers нужны для audit logging, derived columns, cross-table consistency checks.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Один вызов на запрос, не на строку.

**Задача.**

1. SQL: `FOR EACH STATEMENT` — trigger вызывается один раз на DML statement.
2. Не видит конкретных строк, но знает: операция (INSERT/UPDATE/DELETE), таблица, affected_rows count.
3. Полезно для bulk operations (audit log одного события вместо 1000 row-level).

**Ключевые файлы.**

- `diesel/trigger/StatementTriggerExecutor.java`

**Критерии приёмки.**

- [ ] StatementTriggerTest: `DELETE FROM table` с 1000 строк → trigger вызывается ровно 1 раз.
- [ ] BulkInsertStatementTriggerTest: INSERT 1000 строк — trigger 1 раз.

---

### 100. VIEW (non-materialized): CREATE VIEW + SELECT * FROM view (expansion)

**ID ROADMAP3:** R3-029  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое.

**Проблема.**

Non-materialized views — это сохранённые SQL-запросы с имененем. Нужны для абстракции schema, security (ограничение видимых колонок), упрощения сложных запросов.

**Контекст этого шага.**

Шаг 1/3. Базовый view: сохранение SQL, expansion при SELECT. Шаги 2 (updatable), 3 (CHECK OPTION + DROP CASCADE) — сверху.

**Задача.**

1. SQL: `CREATE VIEW name AS SELECT ...` — сохранение SQL-текста.
2. `SELECT * FROM view_name` — подстановка SQL-текста view в запрос (через `ViewExpander` в query rewriter).
3. `CREATE OR REPLACE VIEW` — обновить определение.
4. View metadata persisted в `diesel_views` системной таблице.

**Ключевые файлы.**

- `diesel/view/CreateViewQuery.java`
- `diesel/view/ViewRegistry.java`
- `diesel/view/ViewExpander.java`
- `diesel/QueryParser.java (CREATE VIEW)`

**Критерии приёмки.**

- [ ] ViewSelectTest: SELECT * FROM view возвращает те же данные, что и исходный SELECT.
- [ ] ReplaceViewTest: CREATE OR REPLACE VIEW — обновляет определение.
- [ ] ViewWithJoinTest: view с JOIN — expansion корректный.

---

### 101. VIEW (non-materialized): Updatable views (INSERT/UPDATE/DELETE through view)

**ID ROADMAP3:** R3-029  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

Non-materialized views — это сохранённые SQL-запросы с имененем. Нужны для абстракции schema, security (ограничение видимых колонок), упрощения сложных запросов.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. DML через view переписывается в DML на underlying table.

**Задача.**

1. `UPDATE view SET col = val WHERE ...` → переписывается в `UPDATE underlying_table SET col = val WHERE view-condition AND ...`.
2. `INSERT INTO view (...) VALUES (...)` → переписывается в INSERT в underlying table.
3. `DELETE FROM view` → переписывается в DELETE.
4. Updatable view checker: view должен быть simple (one table, no aggregation, no DISTINCT) для updatable.

**Ключевые файлы.**

- `diesel/view/UpdatableViewChecker.java`
- `diesel/view/ViewDmlRewriter.java`

**Критерии приёмки.**

- [ ] UpdatableViewInsertTest: INSERT через view создаёт строку в underlying table.
- [ ] UpdatableViewUpdateTest: UPDATE через view изменяет underlying.
- [ ] NonUpdatableViewTest: view с aggregation → INSERT/UPDATE запрещены (NonUpdatableViewException).

---

### 102. VIEW (non-materialized): CHECK OPTION + DROP TABLE/VIEW dependency

**ID ROADMAP3:** R3-029  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

Non-materialized views — это сохранённые SQL-запросы с имененем. Нужны для абстракции schema, security (ограничение видимых колонок), упрощения сложных запросов.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. CHECK OPTION для updatable views + dependency management.

**Задача.**

1. `WITH CHECK OPTION` — INSERT/UPDATE через view должны удовлетворять WHERE условию view (иначе exception).
2. `DROP VIEW name` — удаляет view.
3. `DROP TABLE` с зависимым view → error без CASCADE.
4. `DROP TABLE ... CASCADE` — каскадно удалить зависимые views.

**Ключевые файлы.**

- `diesel/view/CheckOptionValidator.java`
- `diesel/view/ViewDependencyChecker.java`
- `diesel/QueryParser.java (CHECK OPTION, DROP VIEW)`

**Критерии приёмки.**

- [ ] CheckOptionTest: INSERT violates view WHERE → exception.
- [ ] DropViewTest: DROP VIEW — удаляет, SELECT * FROM view → ViewNotFoundException.
- [ ] DropTableWithViewTest: DROP TABLE с зависимым view без CASCADE → exception.
- [ ] DropTableCascadeTest: DROP TABLE CASCADE → view удалена тоже.

---

### 103. Full-text search: Tokenizer + Stemmer (Snowball)

**ID ROADMAP3:** R3-030  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет и поглощает Промпт 130 (Full Text Search). R3-030 добавляет GiST/GIN-аналоги и relevance scoring.

**Проблема.**

Без FTS невозможно эффективно искать по тексту (LIKE '%word%' — full scan).

**Контекст этого шага.**

Шаг 1/3. Tokenization + stemming для русского и английского. Шаги 2 (inverted index), 3 (scoring) — сверху.

**Задача.**

1. `Tokenizer`: разбиение текста на токены, lowercase, удаление стоп-слов (configurable per language).
2. `Stemmer`: Snowball-аналог для русского и английского (портирование с C).
3. Stop-word lists: `stopwords_ru.txt`, `stopwords_en.txt` (resource files).
4. `tsvector` analog: array of (token, position).

**Ключевые файлы.**

- `diesel/fts/Tokenizer.java`
- `diesel/fts/Stemmer.java`
- `diesel/fts/StopWords.java`
- `diesel/fts/TsVector.java`

**Критерии приёмки.**

- [ ] TokenizerTest: 'Hello, World!' → ['hello', 'world'].
- [ ] StemmerTest: 'бегущий' → 'бег', 'running' → 'run'.
- [ ] StopWordsTest: 'the cat' → ['cat'] (the — stop word).

---

### 104. Full-text search: FullTextIndex (GIN-структура) + MATCH/AGAINST

**ID ROADMAP3:** R3-030  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет и поглощает Промпт 130 (Full Text Search). R3-030 добавляет GiST/GIN-аналоги и relevance scoring.

**Проблема.**

Без FTS невозможно эффективно искать по тексту (LIKE '%word%' — full scan).

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Инвертированный индекс + SQL syntax для поиска.

**Задача.**

1. Реализовать `FullTextIndex`: inverted index (token → list of row-ids).
2. SQL: `CREATE FTS INDEX ON table(col)`.
3. Query: `MATCH(col) AGAINST('word1 word2')` или `col @@ 'word1 & word2'`.
4. Boolean operators: `&` (AND), `|` (OR), `!` (NOT).

**Ключевые файлы.**

- `diesel/fts/FullTextIndex.java`
- `diesel/fts/InvertedIndex.java`
- `diesel/QueryParser.java (MATCH/AGAINST, @@)`

**Критерии приёмки.**

- [ ] FtsIndexTest: 1M документов, поиск по слову → < 50 ms, top-10 результатов.
- [ ] BooleanQueryTest: `word1 & word2` — intersection, `word1 | word2` — union, `!word1` — complement.
- [ ] PhraseQueryTest: `"word1 word2"` (phrase) — последовательные токены.

---

### 105. Full-text search: Relevance scoring (BM25) + Highlighter

**ID ROADMAP3:** R3-030  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** уточняет и поглощает Промпт 130 (Full Text Search). R3-030 добавляет GiST/GIN-аналоги и relevance scoring.

**Проблема.**

Без FTS невозможно эффективно искать по тексту (LIKE '%word%' — full scan).

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Relevance + highlight для UX.

**Задача.**

1. `RelevanceScorer`: BM25 (Okapi BM25 standard).
2. `ORDER BY relevance DESC` — сортировка по релевантности.
3. `Highlighter`: возвращать snippet с подсвеченными терминами (`<b>word</b>`).
4. `snippet(col, 'word1 word2', max_length=200)` — extract context around match.

**Ключевые файлы.**

- `diesel/fts/RelevanceScorer.java`
- `diesel/fts/Highlighter.java`
- `diesel/fts/Bm25Calculator.java`

**Критерии приёмки.**

- [ ] Bm25Test: точные совпадения выше частичных (verified через ranking).
- [ ] HighlightTest: snippet с подсвеченными терминами возвращается.
- [ ] RankingTest: 10 документов с разной frequency — сортировка по BM25 корректна.

---

### 106. Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: JSONB (бинарный JSON + индексация по пути)

**ID ROADMAP3:** R3-031  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое.

**Проблема.**

PostgreSQL-совместимость требует расширенных типов. Без JSONB нельзя хранить полуструктурированные данные; без UUID — нельзя распределённые ID; без ARRAY — нельзя хранить списки.

**Контекст этого шага.**

Шаг 1/4. JSONB — самый важный из расширенных типов. Шаги 2 (UUID+ARRAY), 3 (ENUM+INTERVAL), 4 (INET+BIT) — сверху.

**Задача.**

1. `JSONB` тип: бинарное хранение JSON (token-based, не string).
2. Операторы: `col->'key'` (extract sub-document), `col->>'key'` (extract as text), `col @> '{"key":1}'` (containment), `col ? 'key'` (key exists).
3. Indexing: GIN-индекс на JSONB для fast lookup.
4. Mutation: `jsonb_set(col, '{key}', 'value')`.

**Ключевые файлы.**

- `diesel/types/JsonbType.java`
- `diesel/types/JsonbValue.java`
- `diesel/QueryParser.java (->, ->>, @>, ?, jsonb_set)`

**Критерии приёмки.**

- [ ] JsonbExtractTest: `col->'key'` возвращает sub-document, `col->>'key'` — text.
- [ ] JsonbContainmentTest: `col @> '{"key":"value"}'` работает.
- [ ] JsonbIndexTest: GIN index on JSONB → lookup < 10 ms на 1M rows.
- [ ] JsonbSetTest: `jsonb_set` мутирует значение.

---

### 107. Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: UUID + ARRAY

**ID ROADMAP3:** R3-031  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

PostgreSQL-совместимость требует расширенных типов. Без JSONB нельзя хранить полуструктурированные данные; без UUID — нельзя распределённые ID; без ARRAY — нельзя хранить списки.

**Контекст этого шага.**

Шаг 2/4. Опирается на шаг 1 (для type system). UUID для distributed IDs, ARRAY для списков.

**Задача.**

1. `UUID`: RFC 4122 v4 (random) и v7 (time-ordered), `gen_random_uuid()` функция.
2. Indexing on UUID (unique B-tree).
3. `ARRAY`: `INTEGER[]`, `TEXT[][]`, etc.
4. Array operators: `&&` (overlap), `@>` (contains), `<@` (contained by), `||` (concat).
5. GIN index on ARRAY for fast containment queries.

**Ключевые файлы.**

- `diesel/types/UuidType.java`
- `diesel/types/ArrayType.java`
- `diesel/QueryParser.java (UUID/ARRAY literals, operators)`

**Критерии приёмки.**

- [ ] UuidTest: `gen_random_uuid()` возвращает v4 UUID; index on UUID — unique enforced.
- [ ] UuidV7Test: v7 UUID — time-ordered (можно сортировать по creation time).
- [ ] ArrayTest: `'{1,2,3}'::INTEGER[]` хранится, `col && '{3,4,5}'` возвращает true (overlap).
- [ ] ArrayContainsTest: `col @> '{2,3}'` (contains).

---

### 108. Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: ENUM + INTERVAL

**ID ROADMAP3:** R3-031  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

PostgreSQL-совместимость требует расширенных типов. Без JSONB нельзя хранить полуструктурированные данные; без UUID — нельзя распределённые ID; без ARRAY — нельзя хранить списки.

**Контекст этого шага.**

Шаг 3/4. ENUM для type-safe перечислений, INTERVAL для time arithmetic.

**Задача.**

1. `ENUM`: `CREATE TYPE color AS ENUM ('red', 'green', 'blue')`, типобезопасность.
2. INSERT значения не из enum → exception.
3. `INTERVAL`: `INTERVAL '1 day 2 hours'`, арифметика с TIMESTAMP (`TIMESTAMP + INTERVAL = TIMESTAMP`).
4. INTERVAL units: microseconds, milliseconds, seconds, minutes, hours, days, weeks, months, years.

**Ключевые файлы.**

- `diesel/types/EnumType.java`
- `diesel/types/IntervalType.java`
- `diesel/QueryParser.java (CREATE TYPE ENUM, INTERVAL literals)`

**Критерии приёмки.**

- [ ] EnumTest: INSERT значения не из enum → exception.
- [ ] EnumCastTest: `'red'::color` работает.
- [ ] IntervalTest: `TIMESTAMP '2024-01-01' + INTERVAL '1 day'` = `2024-01-02`.
- [ ] IntervalArithmeticTest: `INTERVAL '1 hour' * 2 = INTERVAL '2 hours'`.

---

### 109. Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: INET/CIDR + BIT

**ID ROADMAP3:** R3-031  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

PostgreSQL-совместимость требует расширенных типов. Без JSONB нельзя хранить полуструктурированные данные; без UUID — нельзя распределённые ID; без ARRAY — нельзя хранить списки.

**Контекст этого шага.**

Шаг 4/4. Опирается на шаг 1. INET/CIDR для IP-адресов, BIT для битовых строк.

**Задача.**

1. `INET` / `CIDR`: IPv4 и IPv6 адреса.
2. Operators: `<<` (subnet), `>>` (supernet), `&&` (overlap).
3. `BIT` / `BIT VARYING`: битовые строки.
4. Bitwise operations: `&` (AND), `|` (OR), `#` (XOR), `~` (NOT).

**Ключевые файлы.**

- `diesel/types/InetType.java`
- `diesel/types/BitType.java`
- `diesel/QueryParser.java (INET, CIDR, BIT literals, operators)`

**Критерии приёмки.**

- [ ] InetTest: `'192.168.1.5' << '192.168.0.0/16'` = true (subnet).
- [ ] InetIpv6Test: IPv6 addresses поддерживаются.
- [ ] BitTest: `B'1010' & B'1100'` = `B'1000'`.
- [ ] BitVaryingTest: `BIT VARYING(8)` хранит строки переменной длины до 8 bit.

---

### 110. Хранимые функции (SQL-only, без PL/pgSQL): CREATE FUNCTION + RETURNS

**ID ROADMAP3:** R3-032  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** новое.

**Проблема.**

Хранимые функции позволяют инкапсулировать бизнес-логику в БД. Без PL/pgSQL DieselDB всё равно может предоставить SQL-only функции (как PostgreSQL `LANGUAGE SQL`).

**Контекст этого шага.**

Шаг 1/3. Базовое создание функций. Шаги 2 (volatility), 3 (TABLE + named args) — сверху.

**Задача.**

1. SQL: `CREATE FUNCTION name(arg1 TYPE, ...) RETURNS TYPE LANGUAGE SQL AS $$ SELECT ... $$`.
2. Function body — single SQL statement.
3. `DROP FUNCTION name(args)` (сигнатура для overload).
4. `FunctionRegistry`: lookup по name + arg types.

**Ключевые файлы.**

- `diesel/function/CreateFunctionQuery.java`
- `diesel/function/SqlFunction.java`
- `diesel/function/FunctionRegistry.java`
- `diesel/QueryParser.java (CREATE FUNCTION)`

**Критерии приёмки.**

- [ ] CreateFunctionTest: `CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b $$` работает.
- [ ] CallFunctionTest: `SELECT add(1, 2)` возвращает 3.
- [ ] DropFunctionTest: DROP FUNCTION — последующий SELECT падает с FunctionNotFoundException.
- [ ] OverloadTest: 2 функции с одним именем, разными args — разрешаются по типам.

---

### 111. Хранимые функции (SQL-only, без PL/pgSQL): Volatility (IMMUTABLE/STABLE/VOLATILE) + inlining

**ID ROADMAP3:** R3-032  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

Хранимые функции позволяют инкапсулировать бизнес-логику в БД. Без PL/pgSQL DieselDB всё равно может предоставить SQL-only функции (как PostgreSQL `LANGUAGE SQL`).

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Volatility для оптимизации + inlining IMMUTABLE функций.

**Задача.**

1. `IMMUTABLE` — для одинаковых args всегда возвращает тот же результат (можно inlining, можно индексировать).
2. `STABLE` — в рамках одной транзакции не меняется.
3. `VOLATILE` (default) — может меняться (NOW(), random()).
4. Inlining: IMMUTABLE функции inlining в query на parse time где возможно.

**Ключевые файлы.**

- `diesel/function/FunctionVolatility.java`
- `diesel/function/FunctionInliningRewriter.java`

**Критерии приёмки.**

- [ ] ImmutableInliningTest: `SELECT add(1, 2)` → EXPLAIN показывает `SELECT 3` (inlined).
- [ ] VolatileNotInlinedTest: `SELECT now()` не inlined (вызывается каждый раз).
- [ ] IndexOnFunctionTest: index on IMMUTABLE function expression работает (e.g., `lower(email)`).

---

### 112. Хранимые функции (SQL-only, без PL/pgSQL): TABLE-returning + named arguments

**ID ROADMAP3:** R3-032  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

Хранимые функции позволяют инкапсулировать бизнес-логику в БД. Без PL/pgSQL DieselDB всё равно может предоставить SQL-only функции (как PostgreSQL `LANGUAGE SQL`).

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Functions returning TABLE + named args.

**Задача.**

1. `RETURNS TABLE(col1 TYPE, col2 TYPE)` — функция возвращает множество строк.
2. `SELECT * FROM generate_series(1, 5)` — calling table-returning function in FROM clause.
3. Named arguments: `SELECT add(b := 2, a := 1)`.
4. Default arguments: `CREATE FUNCTION f(a INT, b INT DEFAULT 10)`.

**Ключевые файлы.**

- `diesel/function/TableReturningFunction.java`
- `diesel/function/NamedArgument.java`
- `diesel/function/DefaultArgument.java`
- `diesel/QueryParser.java (TABLE return, named args)`

**Критерии приёмки.**

- [ ] TableReturnTest: `SELECT * FROM generate_series(1, 5)` возвращает 5 строк.
- [ ] NamedArgsTest: `SELECT add(b := 2, a := 1)` работает.
- [ ] DefaultArgsTest: `SELECT f(5)` — b defaults to 10.

---

### 113. Шардинг + distributed query planner: CREATE SHARDED TABLE + ShardMap

**ID ROADMAP3:** R3-033  
**Категория:** E. Sharding & Partitioning  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 16 (replication), промпт 17 (partition)  
**Зависимости этого шага:** промпты 16, 17  
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 5.

**Проблема.**

При росте данных на одну ноду (vertical scaling упирается в CPU/RAM) нужен horizontal scaling — шардинг.

**Контекст этого шага.**

Шаг 1/3. Базовый sharding: размещение данных по нодам. Шаги 2 (distributed planner), 3 (distributed join + 2PC) — сверху.

**Задача.**

1. SQL: `CREATE SHARDED TABLE ... WITH (sharding_key=col, shard_count=N)`.
2. ShardMap: shard_id → node_id mapping, persisted.
3. Placement: hash(sharding_key) % shard_count → shard_id → node.
4. ShardManager: координация между нодами.

**Ключевые файлы.**

- `diesel/sharding/ShardManager.java`
- `diesel/sharding/ShardMap.java`
- `diesel/sharding/ShardPlacement.java`
- `diesel/QueryParser.java (SHARDED syntax)`

**Критерии приёмки.**

- [ ] ShardCreateTest: CREATE SHARDED TABLE → данные распределены по N нодам.
- [ ] ShardRoutingTest: INSERT с sharding_key → routed в правильный shard.
- [ ] ShardLookupTest: SELECT с sharding_key → routed в 1 shard, не fan-out.

---

### 114. Шардинг + distributed query planner: Distributed query planner (push-down + fan-out)

**ID ROADMAP3:** R3-033  
**Категория:** E. Sharding & Partitioning  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 16 (replication), промпт 17 (partition)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 5.

**Проблема.**

При росте данных на одну ноду (vertical scaling упирается в CPU/RAM) нужен horizontal scaling — шардинг.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Планирование distributed запросов.

**Задача.**

1. Push-down filters: WHERE sharding_key=X → route в 1 shard.
2. Fan-out SELECT: SELECT без sharding_key → все shards, merge results.
3. Distributed aggregation: каждый shard локально агрегирует, coordinator merge-ит (SUM, COUNT, MIN, MAX).
4. `DistributedQueryPlanner` — оркестрация.

**Ключевые файлы.**

- `diesel/sharding/DistributedQueryPlanner.java`
- `diesel/sharding/DistributedAggregate.java`
- `diesel/sharding/ResultMerger.java`

**Критерии приёмки.**

- [ ] DistributedSelectTest: 4 ноды × 1M строк, `SELECT count(*) WHERE shard_key=X` → routed to 1 shard.
- [ ] FanOutAggregateTest: `SELECT sum(col)` без filter → каждый shard возвращает partial sum, merge → total.
- [ ] PushDownTest: WHERE clause push-down в shard (verified via EXPLAIN).

---

### 115. Шардинг + distributed query planner: Distributed JOIN (co-located + broadcast) + 2PC writes

**ID ROADMAP3:** R3-033  
**Категория:** E. Sharding & Partitioning  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 16 (replication), промпт 17 (partition)  
**Зависимости этого шага:** шаги 1, 2, промпт 34  
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 5.

**Проблема.**

При росте данных на одну ноду (vertical scaling упирается в CPU/RAM) нужен horizontal scaling — шардинг.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Distributed joins + atomic multi-shard writes.

**Задача.**

1. Co-located join: 2 таблицы пошардинжены по одному ключу → local join на каждом shard (no repartition).
2. Cross-shard join: broadcast small dim table на все shards, или repartition (shuffle).
3. Distributed transactions: INSERT на 2 шарда → 2PC commit/rollback (использует промпт 34).
4. Failure handling: один shard недоступен → whole tx aborted.

**Ключевые файлы.**

- `diesel/sharding/DistributedJoinExecutor.java`
- `diesel/sharding/ColocatedJoinChecker.java`
- `diesel/sharding/BroadcastJoinExecutor.java`

**Критерии приёмки.**

- [ ] ColocatedJoinTest: 2 таблицы пошардинжены по user_id → join без repartition.
- [ ] BroadcastJoinTest: small dim table broadcast-ится на все shards.
- [ ] DistributedTxTest: INSERT на 2 шарда → 2PC commit, оба видны атомарно.
- [ ] ShardFailureTest: 1 shard недоступен → tx aborted, другой shard rollback.

---

### 116. 2PC distributed transactions: TwoPhaseCommitCoordinator (PREPARE + COMMIT/ABORT)

**ID ROADMAP3:** R3-034  
**Категория:** D. Replication & HA + E. Sharding  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 16 (replication)  
**Зависимости этого шага:** промпт 16  
**Связь с prompt3.md:** новое.

**Проблема.**

Multi-shard writes требуют distributed transactions. Без 2PC атомарность невозможна.

**Контекст этого шага.**

Шаг 1/3. Ядро 2PC: coordinator state machine. Шаги 2 (recovery), 3 (timeout) — сверху.

**Задача.**

1. PREPARE phase: coordinator просит всех participants prepare, каждый голосует YES/NO.
2. Если все YES → COMMIT phase: coordinator просит commit. Если хоть один NO → ABORT.
3. `TransactionParticipant` на каждом shard: prepare + commit/abort local transaction.
4. `TwoPhaseCommitCoordinator`: оркестрация.

**Ключевые файлы.**

- `diesel/tx/TwoPhaseCommitCoordinator.java`
- `diesel/tx/TransactionParticipant.java`
- `diesel/tx/ParticipantVote.java`

**Критерии приёмки.**

- [ ] TwoPcCommitTest: 3 shards, все YES → COMMIT, данные видны на всех.
- [ ] TwoPcAbortTest: 1 из 3 голосует NO → ABORT на всех.
- [ ] AtomicityTest: partial failure (1 shard commit, other aborted) — impossible по алгоритму.

---

### 117. 2PC distributed transactions: CoordinatorLog (persistent state machine)

**ID ROADMAP3:** R3-034  
**Категория:** D. Replication & HA + E. Sharding  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 16 (replication)  
**Зависимости этого шага:** шаг 1, промпт 3  
**Связь с prompt3.md:** новое.

**Проблема.**

Multi-shard writes требуют distributed transactions. Без 2PC атомарность невозможна.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Persistent log для recovery при краше coordinator.

**Задача.**

1. `CoordinatorLog`: persistent state per transaction (PREPARING, PREPARED, COMMITTING, COMMITTED, ABORTING, ABORTED).
2. Лог persists на диске (через WAL промпта 3) перед каждой state transition.
3. Recovery: при старте coordinator — читает log, завершает incomplete transactions.
4. Heuristic decisions: если participant недоступен долго → heuristic commit/abort (manual DBA).

**Ключевые файлы.**

- `diesel/tx/CoordinatorLog.java`
- `diesel/tx/CoordinatorState.java`
- `diesel/tx/HeuristicDecision.java`

**Критерии приёмки.**

- [ ] CoordinatorRecoveryTest: kill coordinator между PREPARE и COMMIT → restart → recovery по log, tx завершена.
- [ ] HeuristicDecisionTest: participant недоступен 1 час → DBA может heuristic commit/abort.
- [ ] LogPersistTest: log survives restart, state восстановлен.

---

### 118. 2PC distributed transactions: Timeout + failure handling

**ID ROADMAP3:** R3-034  
**Категория:** D. Replication & HA + E. Sharding  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 16 (replication)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

Multi-shard writes требуют distributed transactions. Без 2PC атомарность невозможна.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Timeout для non-responsive participants.

**Задача.**

1. Timeout per phase: `2pc.prepare.timeout.ms` (default 30000), `2pc.commit.timeout.ms` (default 30000).
2. Если participant не отвечает → ABORT (если в PREPARE phase) или heuristic decision (если в COMMIT phase).
3. Retry policy: 3 retries с exponential backoff.
4. `HeuristicException` для manual resolution.

**Ключевые файлы.**

- `diesel/tx/TwoPcTimeoutManager.java`
- `diesel/tx/HeuristicException.java`
- `diesel/ConfigLoader.java (2pc.* timeouts)`

**Критерии приёмки.**

- [ ] TimeoutTest: participant не отвечает 30 сек → ABORT.
- [ ] RetryTest: 3 retries с exponential backoff перед ABORT.
- [ ] HeuristicExceptionTest: после 1 часа недоступности → HeuristicException, DBA решает.

---

### 119. Row-Level Security (RLS): CREATE POLICY + ENABLE ROW LEVEL SECURITY

**ID ROADMAP3:** R3-035  
**Категория:** F. Security  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 8 (RBAC)  
**Зависимости этого шага:** промпт 8  
**Связь с prompt3.md:** новое.

**Проблема.**

Multi-tenant системы требуют, чтобы каждый tenant видел только свои строки. Без RLS это делается в приложении (и часто с ошибками).

**Контекст этого шага.**

Шаг 1/3. Базовый RLS: политика на таблицу. Шаги 2 (multiple policies), 3 (BYPASSRLS + tenant) — сверху.

**Задача.**

1. SQL: `CREATE POLICY name ON table FOR SELECT/INSERT/UPDATE/DELETE USING (condition)`.
2. `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`.
3. Policy evaluation: на каждый запрос автоматически добавляется WHERE condition в query rewriter.
4. `RlsApplier` в query pipeline.

**Ключевые файлы.**

- `diesel/security/rls/RowLevelSecurityPolicy.java`
- `diesel/security/rls/RlsApplier.java`
- `diesel/security/rls/PolicyRegistry.java`
- `diesel/QueryParser.java (CREATE POLICY)`

**Критерии приёмки.**

- [ ] RlsBasicTest: tenant A видит только свои строки (10), tenant B видит свои (10), не видит строки A.
- [ ] RlsDisabledTest: без ENABLE ROW LEVEL SECURITY — policy не применяется.
- [ ] RlsPerCommandTest: SELECT/INSERT/UPDATE/DELETE policies применяются раздельно.

---

### 120. Row-Level Security (RLS): Multiple policies (OR / AND combination)

**ID ROADMAP3:** R3-035  
**Категория:** F. Security  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 8 (RBAC)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

Multi-tenant системы требуют, чтобы каждый tenant видел только свои строки. Без RLS это делается в приложении (и часто с ошибками).

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Multiple policies на одну таблицу.

**Задача.**

1. Permissive policies combined через OR (любая policy match → row visible).
2. Restrictive policies combined через AND (все должны match).
3. Policy scope: FOR SELECT vs FOR ALL.
4. `DROP POLICY name ON table`.

**Ключевые файлы.**

- `diesel/security/rls/PolicyCombinator.java`
- `diesel/QueryParser.java (DROP POLICY)`

**Критерии приёмки.**

- [ ] MultiplePoliciesTest: 2 permissive policies → OR, видны строки matching любой.
- [ ] RestrictivePoliciesTest: restrictive policy → AND, должны match все.
- [ ] DropPolicyTest: DROP POLICY → больше не применяется.

---

### 121. Row-Level Security (RLS): BYPASSRLS + current_tenant() function

**ID ROADMAP3:** R3-035  
**Категория:** F. Security  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 8 (RBAC)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

Multi-tenant системы требуют, чтобы каждый tenant видел только свои строки. Без RLS это делается в приложении (и часто с ошибками).

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Admin bypass + tenant isolation helper.

**Задача.**

1. `BYPASSRLS` role attribute — admin видит все строки (аналог PostgreSQL `BYPASSRLS`).
2. `current_tenant()` function — возвращает tenant_id из session.
3. Policy example: `USING (tenant_id = current_tenant())`.
4. `SET ROLE` для смены active role в session.

**Ключевые файлы.**

- `diesel/security/rls/BypassRlsChecker.java`
- `diesel/function/CurrentTenantFunction.java`

**Критерии приёмки.**

- [ ] BypassRlsTest: BYPASSRLS role видит все строки.
- [ ] CurrentTenantTest: `current_tenant()` возвращает tenant из session.
- [ ] SetRoleTest: `SET ROLE tenant_a` → RLS для tenant_a.

---

### 122. Column-level privileges + masking

**ID ROADMAP3:** R3-036  
**Категория:** F. Security  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 8 (RBAC)  
**Зависимости этого шага:** промпт 8  
**Связь с prompt3.md:** новое.

**Проблема.**

Table-level RBAC недостаточно: часто нужно скрыть отдельные колонки (например, `salary` от роли `intern`).

**Контекст этого шага.**

Единственный под-промпт. Column-level privileges и masking — оба small, реализуются вместе.

**Задача.**

1. SQL: `GRANT SELECT (col1, col2) ON table TO role` — column-level SELECT privilege.
2. SQL: `GRANT UPDATE (col) ON table TO role` — column-level UPDATE.
3. Masking: `CREATE MASKING POLICY name ON table COLUMN col USING (case when current_user() = 'admin' then col else '***' end)`.
4. Masking применяется в query rewriter (после SELECT, до возврата клиенту).
5. Сочетание с RLS: masking не заменяет RLS, дополняет.

**Ключевые файлы.**

- `diesel/security/column/ColumnPrivilege.java`
- `diesel/security/column/ColumnAccessChecker.java`
- `diesel/security/column/MaskingPolicy.java`
- `diesel/QueryParser.java (GRANT column-level, CREATE MASKING POLICY)`

**Критерии приёмки.**

- [ ] ColumnPrivilegeTest: пользователь без SELECT на `salary` → `SELECT salary FROM users` → AccessDenied.
- [ ] ColumnSelectTest: `SELECT (col1, col2) FROM table` для user с правами только на col1, col2 — работает.
- [ ] MaskingTest: `intern` видит `salary = '***'`, `admin` видит реальное значение.
- [ ] CombinedWithRlsTest: RLS + masking применяются вместе корректно.

---

### 123. TDE at rest encryption: AES-256-GCM page cipher + MasterKeyProvider

**ID ROADMAP3:** R3-037  
**Категория:** F. Security  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 2 (page storage)  
**Зависимости этого шага:** промпт 2  
**Связь с prompt3.md:** новое.

**Проблема.**

Если диск скомпрометирован (украден, backup попал не туда), данные в открытом виде — утечка. TDE шифрует страницы на диске, расшифровывает в памяти.

**Контекст этого шага.**

Шаг 1/3. Ядро TDE: шифрование страниц. Шаги 2 (key rotation), 3 (tablespace + WAL encryption) — сверху.

**Задача.**

1. Реализовать `TdeCipher`: AES-256-GCM на каждую страницу при записи, расшифровка при чтении.
2. IV per page (random 12 bytes, хранится в page header).
3. `MasterKeyProvider` interface: реализация FileMasterKeyProvider (passphrase-protected local file).
4. Расшифрованные страницы кэшируются в buffer pool (не расшифровывать на каждый read).

**Ключевые файлы.**

- `diesel/security/tde/TdeCipher.java`
- `diesel/security/tde/MasterKeyProvider.java`
- `diesel/security/tde/FileMasterKeyProvider.java`
- `diesel/storage/page/PageCipher.java (wrapper над Page)`

**Критерии приёмки.**

- [ ] TdeTest: данные на диске зашифрованы (`hexdump data/page.bin` не показывает readable текст).
- [ ] PerformanceTest: overhead < 5 % на read-heavy workload (с AES-NI).
- [ ] BufferPoolCacheTest: повторный read страницы — из buffer pool, без повторной расшифровки.

---

### 124. TDE at rest encryption: KMS providers (AWS KMS, Vault) + key rotation

**ID ROADMAP3:** R3-037  
**Категория:** F. Security  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 2 (page storage)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

Если диск скомпрометирован (украден, backup попал не туда), данные в открытом виде — утечка. TDE шифрует страницы на диске, расшифровывает в памяти.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. External KMS для production deployments.

**Задача.**

1. `KmsMasterKeyProvider`: AWS KMS integration (через AWS SDK).
2. `VaultMasterKeyProvider`: HashiCorp Vault integration.
3. Key rotation: перегенерация master key + re-encrypt всех страниц в фоне.
4. `KeyRotationManager`: background thread, `tde.key.rotation.interval.days` (default 90).

**Ключевые файлы.**

- `diesel/security/tde/KmsMasterKeyProvider.java`
- `diesel/security/tde/VaultMasterKeyProvider.java`
- `diesel/security/tde/KeyRotationManager.java`

**Критерии приёмки.**

- [ ] KmsProviderTest: AWS KMS — fetch master key, encrypt/decrypt работает (mock для unit test).
- [ ] VaultProviderTest: Vault — fetch key работает (mock).
- [ ] RotationTest: rotate master key → данные всё ещё читаются, во время rotation не блокирует writers > 100 ms.

---

### 125. TDE at rest encryption: Tablespace-level encryption + WAL encryption

**ID ROADMAP3:** R3-037  
**Категория:** F. Security  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 2 (page storage)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

Если диск скомпрометирован (украден, backup попал не туда), данные в открытом виде — утечка. TDE шифрует страницы на диске, расшифровывает в памяти.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Per-tablespace + WAL encryption.

**Задача.**

1. `CREATE TABLESPACE ... ENCRYPTION = 'AES-256-GCM'` — per-tablespace.
2. Default tablespace encryption configurable via `tablespace.default.encryption = true|false`.
3. WAL segments шифруются тем же master key.
4. Encryption metadata в `diesel_encryption` системной таблице.

**Ключевые файлы.**

- `diesel/storage/tablespace/Tablespace.java (encryption support)`
- `diesel/wal/WALSegment.java (encryption on write)`
- `diesel/QueryParser.java (CREATE TABLESPACE ENCRYPTION)`

**Критерии приёмки.**

- [ ] TablespaceEncryptionTest: разные tablespaces — разные encryption policies.
- [ ] WalEncryptionTest: WAL segments зашифрованы, `hexdump wal-0001.log` — no readable data.
- [ ] EncryptionMetadataTest: restart → encryption configurations восстановлены.

---

### 126. Vectorized execution (batch): Batch container (columnar) + VectorizedScan

**ID ROADMAP3:** R3-038  
**Категория:** H. Query optimizer  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** промпт 18  
**Связь с prompt3.md:** новое.

**Проблема.**

Row-by-row execution (tuple-at-a-time) — медленно на OLAP. Vectorized (batch of N rows, e.g. 1024) даёт 5-10× speedup за счёт CPU cache locality и SIMD.

**Контекст этого шага.**

Шаг 1/3. Фундамент: Batch контейнер + vectorized scan. Шаги 2 (filter+project), 3 (aggregate + adapter) — сверху.

**Задача.**

1. Реализовать `Batch`: array-of-columns (columnar) вместо list-of-rows (row-wise). Размер 1024 rows (configurable).
2. `VectorizedScan`: читает batch из таблицы, материализует в columnar layout.
3. Type-specific columns: `IntColumn`, `LongColumn`, `StringColumn`, `DoubleColumn`.
4. Memory: contiguous arrays для CPU cache locality.

**Ключевые файлы.**

- `diesel/executor/vectorized/Batch.java`
- `diesel/executor/vectorized/IntColumn.java`
- `diesel/executor/vectorized/LongColumn.java`
- `diesel/executor/vectorized/StringColumn.java`
- `diesel/executor/vectorized/DoubleColumn.java`
- `diesel/executor/vectorized/VectorizedScan.java`

**Критерии приёмки.**

- [ ] BatchTest: batch 1024 rows × 10 columns round-trip сохраняет данные.
- [ ] ColumnarLayoutTest: memory layout — contiguous arrays (verified via JMH).
- [ ] VectorizedScanTest: scan на 1M rows → batches материализуются корректно.

---

### 127. Vectorized execution (batch): VectorizedFilter + VectorizedProject + expressions

**ID ROADMAP3:** R3-038  
**Категория:** H. Query optimizer  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

Row-by-row execution (tuple-at-a-time) — медленно на OLAP. Vectorized (batch of N rows, e.g. 1024) даёт 5-10× speedup за счёт CPU cache locality и SIMD.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Filter, projection, expression evaluation на batches.

**Задача.**

1. `VectorizedFilter`: применяет predicate на batch, возвращает filtered batch (compact).
2. `VectorizedProject`: выбирает subset columns, вычисляет expressions.
3. `VectorizedExpression`: `col + 1`, `col > 5`, `LOWER(col)` — SIMD где возможно (через Panama Vector API или JNI).
4. Short-circuit evaluation для AND/OR.

**Ключевые файлы.**

- `diesel/executor/vectorized/VectorizedFilter.java`
- `diesel/executor/vectorized/VectorizedProject.java`
- `diesel/executor/vectorized/VectorizedExpression.java`
- `diesel/executor/vectorized/SimdAccelerator.java`

**Критерии приёмки.**

- [ ] VectorizedFilterTest: filter на batch 1024 rows → отфильтровано корректно.
- [ ] VectorizedExpressionTest: `col + 1` — все 1024 значения вычислены.
- [ ] SimdTest: SIMD usage — verified via JIT assembly dump или perf counter (на supported CPU).

---

### 128. Vectorized execution (batch): VectorizedAggregate + RowBatchAdapter (interop)

**ID ROADMAP3:** R3-038  
**Категория:** H. Query optimizer  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

Row-by-row execution (tuple-at-a-time) — медленно на OLAP. Vectorized (batch of N rows, e.g. 1024) даёт 5-10× speedup за счёт CPU cache locality и SIMD.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Aggregate + adapter для interop с legacy row-based operators.

**Задача.**

1. `VectorizedAggregate`: SUM/AVG/COUNT/MIN/MAX на batches, SIMD для SUM/MIN/MAX.
2. Group-by: hash-based aggregation, batches обрабатываются потоково.
3. `RowBatchAdapter`: конвертация batch → row-wise для interop с legacy operators (если в плане смешаны).
4. Config: `executor.mode = row | vectorized`, `executor.batch.size` (default 1024).

**Ключевые файлы.**

- `diesel/executor/vectorized/VectorizedAggregate.java`
- `diesel/executor/vectorized/VectorizedGroupBy.java`
- `diesel/executor/RowBatchAdapter.java`
- `diesel/ConfigLoader.java (executor.mode, executor.batch.size)`

**Критерии приёмки.**

- [ ] VectorizedAggregateTest: SUM(col) на 10M строк — 3× быстрее row-by-row.
- [ ] MixedPlanTest: row-based scan + vectorized aggregate — работает через adapter.
- [ ] ModeSwitchTest: `executor.mode=row` → row-based, `=vectorized` → vectorized.

---

### 129. Adaptive joins (runtime switching): AdaptiveJoinExecutor (runtime cardinality check)

**ID ROADMAP3:** R3-039  
**Категория:** H. Query optimizer  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** промпт 18  
**Связь с prompt3.md:** новое.

**Проблема.**

CBO может ошибаться в оценке cardinality (например, статистика устарела). Adaptive join: начинается как hash join, если build-side слишком большой — переключается на nested loop.

**Контекст этого шага.**

Шаг 1/3. Ядро adaptive join: оценка cardinality в runtime, переключение алгоритма. Шаги 2 (runtime stats), 3 (plan feedback) — сверху.

**Задача.**

1. `AdaptiveJoinExecutor`: до build-phase оценивает cardinality build-side (через streaming count).
2. Если cardinality < threshold (config `adaptive.join.hash.threshold`, default 10000) → hash join.
3. Иначе → nested loop join.
4. Switch происходит до материализации build-side, без wasted work.

**Ключевые файлы.**

- `diesel/optimizer/AdaptiveJoinExecutor.java`
- `diesel/ConfigLoader.java (adaptive.join.hash.threshold)`

**Критерии приёмки.**

- [ ] AdaptiveJoinTest: малый build-side (100 rows) → hash join; большой (1M rows) → nested loop.
- [ ] SwitchTest: переключение алгоритма в runtime — verified via EXPLAIN ANALYZE.

---

### 130. Adaptive joins (runtime switching): RuntimeStatisticsCollector

**ID ROADMAP3:** R3-039  
**Категория:** H. Query optimizer  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

CBO может ошибаться в оценке cardinality (например, статистика устарела). Adaptive join: начинается как hash join, если build-side слишком большой — переключается на nested loop.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Сбор runtime stats, обновление `pg_stats`-аналог.

**Задача.**

1. `RuntimeStatisticsCollector`: actual cardinality, actual time per operator.
2. EXPLAIN ANALYZE показывает actual vs estimated cardinality.
3. Auto-update statistics при большом расхождении (>10×).
4. Stats persisted в `diesel_stats_history`.

**Ключевые файлы.**

- `diesel/optimizer/RuntimeStatisticsCollector.java`
- `diesel/optimizer/RuntimeStats.java`

**Критерии приёмки.**

- [ ] RuntimeStatsTest: actual cardinality показан в EXPLAIN ANALYZE.
- [ ] AutoUpdateStatsTest: actual >> estimated (10×) → auto ANALYZE.

---

### 131. Adaptive joins (runtime switching): PlanFeedback (persistent для будущих планов)

**ID ROADMAP3:** R3-039  
**Категория:** H. Query optimizer  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

CBO может ошибаться в оценке cardinality (например, статистика устарела). Adaptive join: начинается как hash join, если build-side слишком большой — переключается на nested loop.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Persistent feedback, используемый при будущих planning.

**Задача.**

1. `PlanFeedback`: actual cardinality per (query hash, operator) → persisted.
2. CBO (промпт 18) использует feedback для коррекции estimates.
3. Feedback decay: старые feedback (90 дней) удаляются или вес уменьшается.
4. `RESET PLAN FEEDBACK` для очистки.

**Ключевые файлы.**

- `diesel/optimizer/PlanFeedback.java`
- `diesel/optimizer/PlanFeedbackStore.java`
- `diesel/QueryParser.java (RESET PLAN FEEDBACK)`

**Критерии приёмки.**

- [ ] PlanFeedbackTest: повторный запуск того же запроса использует actual cardinality.
- [ ] FeedbackDecayTest: feedback старше 90 дней — вес уменьшается.
- [ ] ComparisonTest: 10 mismatched-cardinality запросов — adaptive быстрее на 30 % vs non-adaptive.

---

### 132. Parallel query scan + aggregation: ParallelScanExecutor + RangeSplitter

**ID ROADMAP3:** R3-040  
**Категория:** H. Query optimizer  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** промпт 18  
**Связь с prompt3.md:** уточняет и поглощает Промпты 123 (Parallel Scan) и 124 (Parallel Aggregation). R3-040 добавляет partition-aware parallelism и Gather node.

**Проблема.**

Single-threaded scan на multi-core CPU не утилизирует ресурсы. Parallel scan делит таблицу на ranges, каждый worker сканирует свой range.

**Контекст этого шага.**

Шаг 1/3. Базовый parallel scan. Шаги 2 (parallel aggregation), 3 (adaptive + partition-aware) — сверху.

**Задача.**

1. `ParallelScanExecutor`: split table на N ranges (по row-id или page-id), N = `max_parallel_workers`.
2. `RangeSplitter`: вычисляет range boundaries (по min/max row-id).
3. Каждый worker сканирует свой range, результаты merge через `GatherNode`.
4. `SET max_parallel_workers = N` для контроля ресурсов.

**Ключевые файлы.**

- `diesel/executor/parallel/ParallelScanExecutor.java`
- `diesel/executor/parallel/RangeSplitter.java`
- `diesel/executor/parallel/GatherNode.java`

**Критерии приёмки.**

- [ ] ParallelScanTest: SUM на 100M строк, 8 cores → 6-8× speedup vs single-thread.
- [ ] RangeSplitTest: ranges не пересекаются, покрывают всю таблицу.
- [ ] GatherNodeTest: merge результатов корректен, no duplicates.

---

### 133. Parallel query scan + aggregation: ParallelAggregationExecutor + Gather merge

**ID ROADMAP3:** R3-040  
**Категория:** H. Query optimizer  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет и поглощает Промпты 123 (Parallel Scan) и 124 (Parallel Aggregation). R3-040 добавляет partition-aware parallelism и Gather node.

**Проблема.**

Single-threaded scan на multi-core CPU не утилизирует ресурсы. Parallel scan делит таблицу на ranges, каждый worker сканирует свой range.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Parallel aggregation: каждый worker локально агрегирует.

**Задача.**

1. `ParallelAggregationExecutor`: каждый worker локально агрегирует (SUM, COUNT, MIN, MAX).
2. `GatherNode` merge-ит partial aggregates: SUM → sum of sums, COUNT → sum of counts, MIN → min of mins, MAX → max of maxes.
3. AVG: SUM/COUNT после merge.
4. GROUP BY: hash partitioning по group key, local aggregate, then merge.

**Ключевые файлы.**

- `diesel/executor/parallel/ParallelAggregationExecutor.java`
- `diesel/executor/parallel/PartialAggregate.java`
- `diesel/executor/parallel/AggregateMerger.java`

**Критерии приёмки.**

- [ ] ParallelSumTest: SUM(col) на 100M строк — 6-8× speedup.
- [ ] ParallelCountTest: COUNT(*) — 6-8× speedup.
- [ ] ParallelGroupByTest: GROUP BY → partitioned correctly, merged without duplicates.
- [ ] ParallelAvgTest: AVG = SUM/COUNT после merge.

---

### 134. Parallel query scan + aggregation: Adaptive parallelism + partition-aware

**ID ROADMAP3:** R3-040  
**Категория:** H. Query optimizer  
**Приоритет:** MEDIUM  
**Фаза:** 3  
**Зависимости родителя:** промпт 18 (CBO)  
**Зависимости этого шага:** шаги 1, 2, промпты 17, 18  
**Связь с prompt3.md:** уточняет и поглощает Промпты 123 (Parallel Scan) и 124 (Parallel Aggregation). R3-040 добавляет partition-aware parallelism и Gather node.

**Проблема.**

Single-threaded scan на multi-core CPU не утилизирует ресурсы. Parallel scan делит таблицу на ranges, каждый worker сканирует свой range.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Adaptive (не параллелить маленькие таблицы) + partition-aware для partitioned tables.

**Задача.**

1. Adaptive parallelism: при маленьких таблицах (cardinality < threshold, default 100k) — не параллелить (overhead > gain).
2. Partition-aware: для partitioned tables (промпт 17), каждый worker берёт свою партицию, no range split needed.
3. Cost model в CBO (промпт 18): parallel_setup_cost, parallel_tuple_cost.
4. EXPLAIN показывает parallel plan + worker count.

**Ключевые файлы.**

- `diesel/executor/parallel/AdaptiveParallelismDecider.java`
- `diesel/executor/parallel/PartitionAwareParallelScan.java`
- `diesel/optimizer/ParallelCostModel.java`

**Критерии приёмки.**

- [ ] AdaptiveTest: на 10k-строчной таблице parallelism не включается.
- [ ] PartitionAwareTest: 12 партиций, 12 workers → каждый worker scan одну партицию.
- [ ] CostModelTest: CBO выбирает parallel или sequential по cost.

---

### 135. Bitmap indexes: BitmapIndex + CompressedBitmap (WAH)

**ID ROADMAP3:** R3-041  
**Категория:** C. Storage engine  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** уточняет и поглощает Промпт 122 (Bitmap Indexes). R3-041 добавляет сжатие WAH/BBC и integration с CBO.

**Проблема.**

Bitmap indexes эффективны для low-cardinality колонок (gender, status, country): одна bitmap на distinct value, быстрые bitwise operations.

**Контекст этого шага.**

Шаг 1/3. Ядро bitmap index + WAH compression. Шаги 2 (bitwise operations), 3 (CBO integration) — сверху.

**Задача.**

1. SQL: `CREATE BITMAP INDEX name ON table(col)`.
2. `BitmapIndex`: один bitmap (bitset) на distinct value.
3. WAH (Word-Aligned Hybrid) compression: literal vs fill words, 4-8× compression.
4. Поддержка NULL bitmap (отдельный bitmap для NULL values).

**Ключевые файлы.**

- `diesel/index/bitmap/BitmapIndex.java`
- `diesel/index/bitmap/CompressedBitmap.java`
- `diesel/index/bitmap/WahCompressor.java`
- `diesel/QueryParser.java (CREATE BITMAP INDEX)`

**Критерии приёмки.**

- [ ] BitmapCreateTest: CREATE BITMAP INDEX — index создаётся.
- [ ] WahCompressionTest: raw bitmap 1MB → WAH < 100KB.
- [ ] NullBitmapTest: NULL values — отдельный bitmap, корректно ищутся.

---

### 136. Bitmap indexes: Bitwise operations (AND/OR/NOT) + BitmapScanExecutor

**ID ROADMAP3:** R3-041  
**Категория:** C. Storage engine  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** уточняет и поглощает Промпт 122 (Bitmap Indexes). R3-041 добавляет сжатие WAH/BBC и integration с CBO.

**Проблема.**

Bitmap indexes эффективны для low-cardinality колонок (gender, status, country): одна bitmap на distinct value, быстрые bitwise operations.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. Bitwise ops для комбинированных WHERE + bitmap → row-ids conversion.

**Задача.**

1. Bitwise AND: `WHERE col1='A' AND col2='B'` → AND of bitmaps.
2. Bitwise OR: `WHERE col1='A' OR col2='B'` → OR of bitmaps.
3. Bitwise NOT: `WHERE col1 != 'A'` → NOT of bitmap.
4. `BitmapScanExecutor`: bitmap → list of row-ids, fetch rows from table.

**Ключевые файлы.**

- `diesel/index/bitmap/BitmapOperations.java`
- `diesel/index/bitmap/BitmapScanExecutor.java`

**Критерии приёмки.**

- [ ] BitwiseAndTest: `WHERE col1='A' AND col2='B'` — AND of bitmaps, 10× faster than btree.
- [ ] BitwiseOrTest: OR — union of row-ids.
- [ ] BitmapScanTest: bitmap → row-ids → rows fetched корректно.

---

### 137. Bitmap indexes: CBO integration (cost model for bitmap scan)

**ID ROADMAP3:** R3-041  
**Категория:** C. Storage engine  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** шаги 1, 2, промпт 18  
**Связь с prompt3.md:** уточняет и поглощает Промпт 122 (Bitmap Indexes). R3-041 добавляет сжатие WAH/BBC и integration с CBO.

**Проблема.**

Bitmap indexes эффективны для low-cardinality колонок (gender, status, country): одна bitmap на distinct value, быстрые bitwise operations.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. CBO выбирает bitmap scan когда cardinality < threshold.

**Задача.**

1. Cost model: bitmap scan cost = bitmaps_read × decode_cost + matching_rows × cpu_tuple_cost.
2. CBO выбирает bitmap scan когда cardinality < 100 distinct values (configurable).
3. Combined bitmap + btree: `WHERE col1='A' AND indexed_col=5` → bitmap on col1 + btree lookup on indexed_col.
4. EXPLAIN показывает bitmap scan.

**Ключевые файлы.**

- `diesel/optimizer/BitmapScanCostModel.java`
- `diesel/QueryOptimizer.java (bitmap path consideration)`

**Критерии приёмки.**

- [ ] CboBitmapChoiceTest: low-cardinality (10 distinct) → bitmap scan chosen.
- [ ] CboBtreeChoiceTest: high-cardinality (10k distinct) → btree scan chosen.
- [ ] CombinedIndexTest: bitmap + btree combined — optimal plan.

---

### 138. Covering indexes (INCLUDE)

**ID ROADMAP3:** R3-042  
**Категория:** C. Storage engine  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** —  
**Зависимости этого шага:** промпты 1, 18  
**Связь с prompt3.md:** новое.

**Проблема.**

Index-only scan требует, чтобы все нужные колонки были в индексе. Без INCLUDE приходится добавлять колонки в index key (ухудшает selectivity).

**Контекст этого шага.**

Единственный под-промпт. Covering index + index-only scan + visibility map — связаны и реализуются вместе.

**Задача.**

1. SQL: `CREATE INDEX name ON table(key_col) INCLUDE (col1, col2, ...)`.
2. INCLUDE columns хранятся в leaf pages, не участвуют в tree navigation.
3. `IndexOnlyScanExecutor`: если все нужные колонки в index (key + INCLUDE), не обращаться к heap.
4. `VisibilityMap`: для index-only scan нужно знать, видна ли строка в текущем snapshot (через MVCC промпта 1).
5. Visibility map обновляется на COMMIT/ABORT.

**Ключевые файлы.**

- `diesel/index/CoveringIndex.java`
- `diesel/index/IndexOnlyScanExecutor.java`
- `diesel/mvcc/VisibilityMap.java`
- `diesel/QueryParser.java (INCLUDE syntax)`

**Критерии приёмки.**

- [ ] CoveringIndexTest: `SELECT col1 FROM table WHERE key_col = X` → index-only scan, no heap access (по EXPLAIN).
- [ ] IncludeColumnsTest: INCLUDE columns не влияют на B-tree navigation.
- [ ] VisibilityMapTest: visibility map корректно помечает страницы, где все строки committed.
- [ ] VisibilityMapUpdateTest: COMMIT/ABORT обновляет visibility map.

---

### 139. TPC-C / TPC-H сертификация: TPC-C workload (10 warehouses, ACID проверка)

**ID ROADMAP3:** R3-044  
**Категория:** K. CI/CD & quality  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** все остальные промпты  
**Зависимости этого шага:** все промпты Фазы 1-3  
**Связь с prompt3.md:** новое.

**Проблема.**

TPC-C / TPC-H — industry-standard benchmarks. Без них невозможно сравнить DieselDB с PostgreSQL / MySQL / ClickHouse.

**Контекст этого шага.**

Шаг 1/3. TPC-C: OLTP benchmark. Шаги 2 (TPC-H), 3 (regression + report) — сверху.

**Задача.**

1. Реализовать `TpcCWorkload`: 5 transaction types (New-Order, Payment, Order-Status, Delivery, Stock-Level).
2. 10 warehouses (small benchmark for CI).
3. ACID properties: проверить isolation levels, durability после kill -9.
4. Throughput metric: tpmC (transactions per minute).

**Ключевые файлы.**

- `benchmarks/tpcc/TpcCWorkload.java`
- `benchmarks/tpcc/TpcCTransaction.java`
- `benchmarks/tpcc/TpcCAcidChecker.java`
- `benchmarks/BenchmarkRunner.java`

**Критерии приёмки.**

- [ ] TpcCTest: 10 warehouses, ACID properties passed, throughput > 100 tpmC.
- [ ] AcidIsolationTest: SERIALIZABLE prevents all anomalies (dirty read, non-repeatable read, phantom).
- [ ] DurabilityTest: kill -9 после commit → все transactions на месте после recovery.

---

### 140. TPC-C / TPC-H сертификация: TPC-H (22 queries, SF=1, results verification)

**ID ROADMAP3:** R3-044  
**Категория:** K. CI/CD & quality  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** все остальные промпты  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

TPC-C / TPC-H — industry-standard benchmarks. Без них невозможно сравнить DieselDB с PostgreSQL / MySQL / ClickHouse.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. TPC-H: OLAP benchmark.

**Задача.**

1. Реализовать `TpcHQueries`: 22 standard queries.
2. SF=1 (1 GB data, small for CI).
3. `TpcHResultsVerifier`: comparison с эталонными результатами (полученными на PostgreSQL).
4. Latency measurement per query.

**Ключевые файлы.**

- `benchmarks/tpch/TpcHQueries.java`
- `benchmarks/tpch/TpcHResultsVerifier.java`
- `benchmarks/tpch/TpcHDataGenerator.java`

**Критерии приёмки.**

- [ ] TpcHCorrectnessTest: 22 queries, результаты совпадают с эталоном (PostgreSQL).
- [ ] TpcHLatencyTest: latency не хуже PG × 2 на TPC-H SF=1.
- [ ] DataGenTest: генерация SF=1 < 5 минут.

---

### 141. TPC-C / TPC-H сертификация: Regression tracking + public benchmark report

**ID ROADMAP3:** R3-044  
**Категория:** K. CI/CD & quality  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** все остальные промпты  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** новое.

**Проблема.**

TPC-C / TPC-H — industry-standard benchmarks. Без них невозможно сравнить DieselDB с PostgreSQL / MySQL / ClickHouse.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Регрессионное тестирование + публикация отчёта.

**Задача.**

1. Weekly CI run: TPC-C + TPC-H в `.github/workflows/perf.yml`.
2. Metrics в `analytics/perf_history.csv`: tpmC, p99 latency, throughput per query.
3. Public benchmark report в `docs/benchmarks/` (markdown + charts).
4. Comparison with PostgreSQL 16, MySQL 8, ClickHouse.

**Ключевые файлы.**

- `.github/workflows/perf.yml (расширение для TPC)`
- `docs/benchmarks/tpc-c-results.md`
- `docs/benchmarks/tpc-h-results.md`

**Критерии приёмки.**

- [ ] RegressionTest: weekly perf run — результаты в perf_history.csv, regression > 10 % → alert.
- [ ] PublicReportTest: markdown reports published, charts generated.
- [ ] ComparisonTest: DieselDB vs PG/MySQL/ClickHouse — numbers in report.

---

### 142. GUI админ-панель (аналог pgAdmin): Web UI dashboard + schema viewer

**ID ROADMAP3:** R3-045  
**Категория:** — (tooling)  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 12 (metrics)  
**Зависимости этого шага:** промпты 8, 12  
**Связь с prompt3.md:** новое.

**Проблема.**

Без GUI admin-панели разработчикам неудобно: psql-only теряет аудиторию, привыкшую к pgAdmin/DBeaver.

**Контекст этого шага.**

Шаг 1/3. Базовый UI: dashboard + schema. Шаги 2 (SQL editor), 3 (users/backups/replication) — сверху.

**Задача.**

1. Web UI (Next.js): dashboard с метриками (QPS, latency, connections), список таблиц, schema viewer.
2. Auth: login form, интеграция с RBAC (промпт 8).
3. Schema viewer: таблицы, колонки, типы, индексы, FK.
4. Real-time metrics через polling /metrics endpoint (промпт 12).

**Ключевые файлы.**

- `admin-ui/package.json`
- `admin-ui/src/pages/Dashboard.tsx`
- `admin-ui/src/pages/Tables.tsx`
- `admin-ui/src/pages/Schema.tsx`
- `admin-ui/src/lib/api.ts`

**Критерии приёмки.**

- [ ] DashboardTest: dashboard показывает QPS, latency, active connections из metrics endpoint.
- [ ] SchemaViewerTest: список таблиц, колонок, индексов отображается.
- [ ] AuthTest: login form работает, unauthorized → redirect to login.

---

### 143. GUI админ-панель (аналог pgAdmin): SQL editor + EXPLAIN visualizer

**ID ROADMAP3:** R3-045  
**Категория:** — (tooling)  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 12 (metrics)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** новое.

**Проблема.**

Без GUI admin-панели разработчикам неудобно: psql-only теряет аудиторию, привыкшую к pgAdmin/DBeaver.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. SQL editor с results table + EXPLAIN viz.

**Задача.**

1. SQL editor: textarea с syntax highlighting, autocomplete для table/column names.
2. Results table: rendered SELECT results с pagination.
3. EXPLAIN visualizer: tree view плана запроса, cardinality, cost per node.
4. Query history: последние 100 запросов.

**Ключевые файлы.**

- `admin-ui/src/pages/QueryEditor.tsx`
- `admin-ui/src/components/ResultsTable.tsx`
- `admin-ui/src/components/ExplainVisualizer.tsx`
- `admin-ui/src/components/QueryHistory.tsx`

**Критерии приёмки.**

- [ ] SqlEditorTest: `SELECT * FROM users LIMIT 10` → table with results.
- [ ] ExplainTest: EXPLAIN возвращает tree, visualizer показывает nodes.
- [ ] HistoryTest: 100 запросов сохранены, можно re-run.

---

### 144. GUI админ-панель (аналог pgAdmin): User management + Backup UI + Replication monitor

**ID ROADMAP3:** R3-045  
**Категория:** — (tooling)  
**Приоритет:** LOW  
**Фаза:** 3  
**Зависимости родителя:** промпт 12 (metrics)  
**Зависимости этого шага:** шаги 1, 2, промпты 8, 11, 16  
**Связь с prompt3.md:** новое.

**Проблема.**

Без GUI admin-панели разработчикам неудобно: psql-only теряет аудиторию, привыкшую к pgAdmin/DBeaver.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Полный функционал админки.

**Задача.**

1. User management: CRUD users/roles (интеграция с промптом 8).
2. Backup / Restore UI: кнопка 'Backup now' запускает `diesel_backup` и показывает прогресс.
3. Replication monitor: статус master/standby, lag, sync mode (интеграция с промптом 16).
4. OpenTelemetry tracing viewer (если включён в промпте 12).

**Ключевые файлы.**

- `admin-ui/src/pages/Users.tsx`
- `admin-ui/src/pages/Backups.tsx`
- `admin-ui/src/pages/Replication.tsx`

**Критерии приёмки.**

- [ ] UsersCrudTest: create/list/delete users работает через UI.
- [ ] BackupUiTest: кнопка 'Backup now' запускает backup, показывает прогресс, по завершении — success.
- [ ] ReplicationMonitorTest: статус master/standby, lag, sync mode отображаются.

---

---

## Доборочные промпты из `prompt3.md` (без дублирования ROADMAP3)

Промпты ниже перенесены из `prompt3.md` (с #97 и далее), за исключением тех, функционал которых уже покрыт карточками ROADMAP3 (см. таблицу дедупликации в начале документа). Сохранена формулировка оригинала для совместимости с историей планирования; нумерация продолжена.

### 145. Lock — deadlock prevention стратегии: Wait-die + Wound-wait стратегии

**ID ROADMAP3:** Промпт 111  
**Категория:** B. Concurrency  
**Приоритет:** MEDIUM  
**Фаза:** —  
**Зависимости родителя:** промпт 6 (Savepoint + Deadlock + Lock timeout)  
**Зависимости этого шага:** промпт 6  
**Связь с prompt3.md:** перенесено из prompt3.md #111.

**Проблема.**

Detection-based deadlock handling (промпт 6) работает, но для некоторых workloads выгоднее prevention — abort заранее, не дожидаясь cycle.

**Контекст этого шага.**

Шаг 1/3. Две классические prevention схемы на основе timestamp txid.

**Задача.**

1. `WaitDiePolicy`: старшая транзакция (lower txid) ждёт, младшая abort-ится (die).
2. `WoundWaitPolicy`: старшая убивает младшую (wound), младшая ждёт.
3. Обе политики гарантируют отсутствие deadlock.
4. `DeadlockPreventionPolicy` interface.

**Ключевые файлы.**

- `diesel/concurrency/DeadlockPreventionPolicy.java`
- `diesel/concurrency/WaitDiePolicy.java`
- `diesel/concurrency/WoundWaitPolicy.java`

**Критерии приёмки.**

- [ ] WaitDieTest: старая tx ждёт, младшая abort-ится с TransactionAbortedException.
- [ ] WoundWaitTest: старая tx убивает младшую, младшая abort-ится.
- [ ] NoDeadlockTest: ни одна из политик не допускает cycle (verified на 1000 random workloads).

---

### 146. Lock — deadlock prevention стратегии: No-wait (immediate abort on conflict)

**ID ROADMAP3:** Промпт 111  
**Категория:** B. Concurrency  
**Приоритет:** MEDIUM  
**Фаза:** —  
**Зависимости родителя:** промпт 6 (Savepoint + Deadlock + Lock timeout)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** перенесено из prompt3.md #111.

**Проблема.**

Detection-based deadlock handling (промпт 6) работает, но для некоторых workloads выгоднее prevention — abort заранее, не дожидаясь cycle.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1 (для interface). No-wait: immediate abort при первом конфликте.

**Задача.**

1. `NoWaitPolicy`: при попытке acquire lock, который уже занят — immediate abort текущей транзакции.
2. Преимущество: низкая latency на low-contention workloads.
3. Недостаток: высокий abort rate на high-contention.
4. Retry logic на стороне клиента.

**Ключевые файлы.**

- `diesel/concurrency/NoWaitPolicy.java`

**Критерии приёмки.**

- [ ] NoWaitTest: при первом конфликте — immediate abort.
- [ ] LowContentionBenchmark: latency ниже detection-based на workload с 1 % conflicts.
- [ ] HighContentionWarningTest: на 50 % conflicts — abort rate > 50 %, warning в логах.

---

### 147. Lock — deadlock prevention стратегии: Config switch + benchmark comparison

**ID ROADMAP3:** Промпт 111  
**Категория:** B. Concurrency  
**Приоритет:** MEDIUM  
**Фаза:** —  
**Зависимости родителя:** промпт 6 (Savepoint + Deadlock + Lock timeout)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** перенесено из prompt3.md #111.

**Проблема.**

Detection-based deadlock handling (промпт 6) работает, но для некоторых workloads выгоднее prevention — abort заранее, не дожидаясь cycle.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. Configuration + сравнение с detection-based.

**Задача.**

1. Config: `lock.prevention.policy = wait_die | wound_wait | no_wait | detection` (default `detection`).
2. Benchmark suite: 3 workload types (low, medium, high contention).
3. Comparison report: latency, abort rate, throughput per policy.
4. Documentation: когда использовать какую policy.

**Ключевые файлы.**

- `diesel/ConfigLoader.java (lock.prevention.policy)`
- `benchmarks/lock_policy_comparison.md`
- `docs/concurrency/deadlock-prevention.md`

**Критерии приёмки.**

- [ ] PolicySwitchTest: 4 policies — switchable via config, все работают.
- [ ] BenchmarkTest: comparison report generated, numbers documented.
- [ ] DocTest: documentation описывает, когда использовать какую policy.

---

### 148. ALTER TABLE ADD COLUMN — базовый SQL

**ID ROADMAP3:** Промпт 113  
**Категория:** G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** —  
**Зависимости родителя:** — (prerequisite для промпта 10 — Online schema changes)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** перенесено из prompt3.md #113.

**Проблема.**

Базовый ALTER TABLE ADD COLUMN не реализован. Online schema (промпт 10) строится поверх этой базовой операции.

**Контекст этого шага.**

Единственный под-промпт. Базовый SQL — малый scope, реализуется целиком.

**Задача.**

1. SQL: `ALTER TABLE table_name ADD COLUMN column_name data_type`.
2. Добавление колонки со значением по умолчанию (`ADD COLUMN col INT DEFAULT 0`).
3. Обновление метаданных таблицы (CatalogTable).
4. Обратная совместимость со старыми данными: существующие строки получают default value для новой колонки (NULL если default не задан).
5. Сериализация обновлённой схемы в catalog (persist на disk).

**Ключевые файлы.**

- `diesel/AlterTableAddColumnQuery.java`
- `diesel/Table.java (addColumn метод)`
- `diesel/storage/page/CatalogTable.java (обновление metadata)`
- `diesel/QueryParser.java (ALTER TABLE ADD COLUMN)`

**Критерии приёмки.**

- [ ] AlterAddColumnTest: ADD COLUMN без default → NULL для существующих строк.
- [ ] AddColumnWithDefaultTest: ADD COLUMN с DEFAULT 0 → 0 для существующих строк.
- [ ] PersistAfterRestartTest: после restart сервера — колонка сохранена (схема persisted).
- [ ] InsertAfterAddTest: ADD COLUMN после которой сразу INSERT → INSERT видит новую колонку.

---

### 149. ALTER TABLE DROP COLUMN — базовый SQL

**ID ROADMAP3:** Промпт 114  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** —  
**Зависимости родителя:** промпт 47 (ALTER ADD COLUMN — для симметрии API)  
**Зависимости этого шага:** промпт 47  
**Связь с prompt3.md:** перенесено из prompt3.md #114.

**Проблема.**

Базовый ALTER TABLE DROP COLUMN не реализован. Online schema (промпт 10) строит tombstone на этой операции.

**Контекст этого шага.**

Единственный под-промпт. Малый scope.

**Задача.**

1. SQL: `ALTER TABLE table_name DROP COLUMN column_name`.
2. Физическое удаление данных из строк (или lazy deletion через tombstone с последующим compaction).
3. Обновление индексов: удаление записей, указывающих на удалённую колонку.
4. Зависимости: если есть CHECK constraint / foreign key / view на колонку — запретить DROP без CASCADE.
5. `DROP COLUMN CASCADE` — каскадно удалить зависимые объекты.

**Ключевые файлы.**

- `diesel/AlterTableDropColumnQuery.java`
- `diesel/Table.java (dropColumn метод)`
- `diesel/storage/page/CatalogTable.java (обновление metadata)`
- `diesel/constraint/DependencyChecker.java (новый, проверка зависимостей)`
- `diesel/QueryParser.java (ALTER TABLE DROP COLUMN)`

**Критерии приёмки.**

- [ ] AlterDropColumnTest: DROP COLUMN → SELECT * больше не возвращает эту колонку.
- [ ] DropWithDependencyTest: DROP COLUMN с зависимым CHECK constraint → exception без CASCADE.
- [ ] DropCascadeTest: DROP COLUMN CASCADE → constraint тоже удалён.
- [ ] PersistAfterRestartTest: после restart — колонка не возвращается (persisted).

---

### 150. DROP INDEX

**ID ROADMAP3:** Промпт 116  
**Категория:** C. Storage engine + G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** —  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** перенесено из prompt3.md #116.

**Проблема.**

DROP INDEX не реализован.

**Контекст этого шага.**

Единственный под-промпт. Малый scope.

**Задача.**

1. SQL: `DROP INDEX index_name ON table_name` (и `DROP INDEX index_name` для schema-level).
2. Удаление структуры индекса из памяти и диска.
3. Освобождение ресурсов: file handles, memory buffers.
4. Обновление метаданных: catalog больше не содержит index.
5. Транзакционность: DROP INDEX в транзакции → rollback восстанавливает index.

**Ключевые файлы.**

- `diesel/DropIndexQuery.java`
- `diesel/IndexManager.java (dropIndex метод)`
- `diesel/storage/page/CatalogTable.java (обновление metadata)`
- `diesel/QueryParser.java (DROP INDEX)`

**Критерии приёмки.**

- [ ] DropIndexTest: CREATE INDEX → DROP INDEX → SELECT не использует индекс (по EXPLAIN).
- [ ] DropInTransactionTest: DROP INDEX в транзакции, ROLLBACK → индекс восстановлен.
- [ ] DropNonExistentTest: DROP несуществующего index → exception с понятным сообщением.
- [ ] ResourceReleaseTest: DROP INDEX освобождает память (heap usage до/после — в метриках).

---

### 151. TRUNCATE TABLE

**ID ROADMAP3:** Промпт 117  
**Категория:** G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** —  
**Зависимости родителя:** промпт 3 (WAL для minimal logging)  
**Зависимости этого шага:** промпт 3  
**Связь с prompt3.md:** перенесено из prompt3.md #117.

**Проблема.**

TRUNCATE TABLE не реализован (для быстрого очищения таблицы).

**Контекст этого шага.**

Единственный под-промпт. Малый scope, но HIGH priority.

**Задача.**

1. SQL: `TRUNCATE TABLE table_name`.
2. Быстрое удаление всех данных (без построчного удаления) — обнуление указателя на data pages.
3. Сброс auto-increment counters (если есть SEQUENCE на этой таблице).
4. Минимальное WAL logging: одна запись TRUNCATE вместо N row-records.
5. TRUNCATE в транзакции: rollback восстанавливает данные (через WAL undo).
6. `TRUNCATE TABLE a, b, c` — несколько таблиц за раз.

**Ключевые файлы.**

- `diesel/TruncateTableQuery.java`
- `diesel/Table.java (truncate метод)`
- `diesel/wal/WALManager.java (запись TRUNCATE record)`
- `diesel/QueryParser.java (TRUNCATE TABLE)`

**Критерии приёмки.**

- [ ] TruncateTableTest: 1M rows → TRUNCATE → 0 rows, < 50 ms.
- [ ] TruncateInTransactionTest: TRUNCATE в транзакции, ROLLBACK → данные восстановлены.
- [ ] TruncateMultipleTest: TRUNCATE a, b, c → все три пустые за одну операцию.
- [ ] AutoIncrementResetTest: TRUNCATE → auto-increment counter сброшен.

---

### 152. CREATE SEQUENCE

**ID ROADMAP3:** Промпт 118  
**Категория:** G. SQL coverage  
**Приоритет:** MEDIUM  
**Фаза:** —  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** перенесено из prompt3.md #118.

**Проблема.**

Sequences не реализованы (нужны для auto-increment).

**Контекст этого шага.**

Единственный под-промпт. Малый scope.

**Задача.**

1. SQL: `CREATE SEQUENCE seq_name START WITH n INCREMENT BY m`.
2. Опциональные параметры: `MINVALUE`, `MAXVALUE`, `CYCLE` / `NO CYCLE`, `CACHE`.
3. Хранение текущего значения в `diesel_sequences` (системная таблица).
4. `NEXTVAL(seq_name)` — следующее значение.
5. `CURRVAL(seq_name)` — текущее (только после NEXTVAL в этой сессии).
6. Кэширование: выдача по N значений за раз (уменьшает contention).
7. Persistence: текущее значение survive restart.

**Ключевые файлы.**

- `diesel/CreateSequenceQuery.java`
- `diesel/SequenceManager.java`
- `diesel/sequence/Sequence.java`
- `diesel/sequence/SequenceCache.java`
- `diesel/QueryParser.java (CREATE SEQUENCE, NEXTVAL, CURRVAL)`

**Критерии приёмки.**

- [ ] CreateSequenceTest: CREATE SEQUENCE seq START 10 INCREMENT 5 → NEXTVAL возвращает 10, 15, 20, ...
- [ ] CacheTest: CACHE 100 → 1000 NEXTVAL → 10 disk reads вместо 1000.
- [ ] CycleTest: после MAXVALUE — снова START.
- [ ] PersistAfterRestartTest: после restart — sequence продолжается с последнего persisted значения.

---

### 153. DROP SEQUENCE

**ID ROADMAP3:** Промпт 119  
**Категория:** G. SQL coverage  
**Приоритет:** LOW  
**Фаза:** —  
**Зависимости родителя:** промпт 51 (CREATE SEQUENCE)  
**Зависимости этого шага:** промпт 51  
**Связь с prompt3.md:** перенесено из prompt3.md #119.

**Проблема.**

DROP SEQUENCE не реализован.

**Контекст этого шага.**

Единственный под-промпт. Малый scope.

**Задача.**

1. SQL: `DROP SEQUENCE sequence_name`.
2. Очистка ресурсов: удаление из `SequenceManager`, удаление persisted state.
3. Проверка зависимостей: если есть DEFAULT NEXTVAL(seq) на колонке → запретить DROP без CASCADE.
4. `DROP SEQUENCE CASCADE` — каскадно удалить DEFAULT из колонок.

**Ключевые файлы.**

- `diesel/DropSequenceQuery.java`
- `diesel/SequenceManager.java (dropSequence метод)`
- `diesel/constraint/DependencyChecker.java (проверка DEFAULT nextval)`
- `diesel/QueryParser.java (DROP SEQUENCE)`

**Критерии приёмки.**

- [ ] DropSequenceTest: CREATE → DROP → NEXTVAL → exception.
- [ ] DropWithDependencyTest: DROP с зависимым DEFAULT → exception без CASCADE.
- [ ] DropCascadeTest: DROP CASCADE → DEFAULT в колонке удалён, column остаётся.
- [ ] PersistAfterRestartTest: после restart — sequence не восстановлен.

---

### 154. Query Result Cache

**ID ROADMAP3:** Промпт 120  
**Категория:** H. Query optimizer  
**Приоритет:** MEDIUM  
**Фаза:** —  
**Зависимости родителя:** —  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** перенесено из prompt3.md #120.

**Проблема.**

Идентичные SELECT запросы выполняются повторно. Cache results ускоряет read-heavy workloads.

**Контекст этого шага.**

Единственный под-промпт. Малый scope.

**Задача.**

1. Кэширование результатов SELECT запросов в памяти.
2. Ключ: normalized SQL + bind variables hash.
3. TTL-based инвалидация (default 60 sec, configurable `cache.ttl.seconds`).
4. Automatic invalidation при INSERT/UPDATE/DELETE на table — помечать cache entries зависящие от этой таблицы как stale.
5. `cache.size`, `cache.enabled` конфигурация.
6. `EXPLAIN` показывает hit/miss.
7. Manual flush: `FLUSH CACHE` / `FLUSH CACHE table_name`.

**Ключевые файлы.**

- `diesel/cache/QueryResultCache.java`
- `diesel/cache/CachedResult.java`
- `diesel/cache/CacheInvalidator.java`
- `diesel/cache/CacheKey.java`
- `diesel/QueryExecutor.java (lookup cache before execution)`
- `diesel/QueryParser.java (FLUSH CACHE)`
- `diesel/ConfigLoader.java (cache.size, cache.ttl.seconds, cache.enabled)`

**Критерии приёмки.**

- [ ] CacheHitTest: SELECT cache hit → повторный запрос < 1 ms.
- [ ] AutoInvalidationTest: INSERT на table → cache entries этой table помечены stale.
- [ ] TtlExpiryTest: TTL истёк → cache miss, новый execution.
- [ ] ExplainShowsCacheTest: EXPLAIN показывает cache hit/miss.
- [ ] LruEvictionTest: cache size limit → LRU eviction при достижении `cache.size`.
- [ ] ManualFlushTest: `FLUSH CACHE table_name` → cache очищен для table.

---

### 155. Bulk Insert / Copy API: BULK INSERT FROM file (CSV/TSV/JSONL/AVRO)

**ID ROADMAP3:** Промпт 121  
**Категория:** H. Performance  
**Приоритет:** HIGH  
**Фаза:** —  
**Зависимости родителя:** промпт 3 (WAL), промпт 2 (page storage)  
**Зависимости этого шага:** промпты 2, 3  
**Связь с prompt3.md:** перенесено из prompt3.md #121.

**Проблема.**

Построчный INSERT медленный на больших загрузках. COPY FROM / BULK INSERT — стандарт для ETL.

**Контекст этого шага.**

Шаг 1/3. Базовый BULK INSERT из файла. Шаги 2 (COPY FROM/TO compat), 3 (batching + error handling) — сверху.

**Задача.**

1. SQL: `BULK INSERT INTO table_name FROM 'file.csv'`.
2. Поддержка форматов: CSV, TSV, JSONL, AVRO.
3. Парсинг файла streaming (не загружать весь в память).
4. Single transaction (по умолчанию).

**Ключевые файлы.**

- `diesel/BulkInsertQuery.java`
- `diesel/BulkLoader.java`
- `diesel/bulk/BulkFileReader.java`
- `diesel/bulk/CsvBulkReader.java`
- `diesel/bulk/JsonlBulkReader.java`
- `diesel/QueryParser.java (BULK INSERT)`

**Критерии приёмки.**

- [ ] BulkInsertTest: 1M rows CSV → < 30 sec (vs 5 min построчно).
- [ ] StreamingTest: bulk load 10M rows → heap < 500 MB.
- [ ] FormatSupportTest: CSV/TSV/JSONL/AVRO — все форматы работают.

---

### 156. Bulk Insert / Copy API: PostgreSQL COPY FROM / COPY TO compat

**ID ROADMAP3:** Промпт 121  
**Категория:** H. Performance  
**Приоритет:** HIGH  
**Фаза:** —  
**Зависимости родителя:** промпт 3 (WAL), промпт 2 (page storage)  
**Зависимости этого шага:** шаг 1  
**Связь с prompt3.md:** перенесено из prompt3.md #121.

**Проблема.**

Построчный INSERT медленный на больших загрузках. COPY FROM / BULK INSERT — стандарт для ETL.

**Контекст этого шага.**

Шаг 2/3. Опирается на шаг 1. PostgreSQL-совместимый COPY синтаксис.

**Задача.**

1. SQL: `COPY table_name FROM 'file.csv' WITH (FORMAT csv, HEADER true)`.
2. `COPY table_name TO 'file.csv'` — экспорт.
3. Совместимость с `psql \copy` (client-side COPY).
4. Опции: DELIMITER, HEADER, NULL string, QUOTE, ESCAPE.

**Ключевые файлы.**

- `diesel/bulk/CopyFromExecutor.java`
- `diesel/bulk/CopyToExecutor.java`
- `diesel/QueryParser.java (COPY FROM/TO)`

**Критерии приёмки.**

- [ ] CopyFromTest: COPY FROM совместим с psql output.
CopyToTest: COPY TO — output читается psql.
- [ ] PsqlCopyTest: `psql \copy table from file.csv` работает через diesel.

---

### 157. Bulk Insert / Copy API: Batching + error handling + progress

**ID ROADMAP3:** Промпт 121  
**Категория:** H. Performance  
**Приоритет:** HIGH  
**Фаза:** —  
**Зависимости родителя:** промпт 3 (WAL), промпт 2 (page storage)  
**Зависимости этого шага:** шаги 1, 2  
**Связь с prompt3.md:** перенесено из prompt3.md #121.

**Проблема.**

Построчный INSERT медленный на больших загрузках. COPY FROM / BULK INSERT — стандарт для ETL.

**Контекст этого шага.**

Шаг 3/3. Опирается на шаги 1–2. BATCH SIZE, ON-ERROR=continue, progress reporting.

**Задача.**

1. `BATCH SIZE N` (default 1000) — коммит каждые N строк (если autocommit mode).
2. `ON ERROR = stop | continue` (default stop). При continue — bad rows skipped, отчёт в логах.
3. Отчёт об ошибках: `file:line:column` для каждой bad row.
4. Progress reporting: callback или polling endpoint.
5. Опционально: disable indexes на время загрузки, rebuild после.

**Ключевые файлы.**

- `diesel/bulk/BulkBatchCommitter.java`
- `diesel/bulk/BulkErrorHandler.java`
- `diesel/bulk/BulkProgressReporter.java`

**Критерии приёмки.**

- [ ] BatchSizeTest: BATCH SIZE 10000 → оптимальный throughput.
- [ ] OnErrorContinueTest: ошибка в строке 5000 → отчёт `file:line:column`, остальные строки загружены.
- [ ] OnErrorStopTest: ON ERROR = stop → abort на первой ошибке, transaction rolled back.
- [ ] ProgressTest: progress callback получает % completion.

---

### 158. Virtual Threads для concurrency

**ID ROADMAP3:** Промпт 125  
**Категория:** H. Performance  
**Приоритет:** LOW  
**Фаза:** —  
**Зависимости родителя:** — (требует Java 21+)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** перенесено из prompt3.md #125.

**Проблема.**

Platform threads тяжёлые (1MB stack each), 10000 connections = OOM. Virtual threads (Java 21) решают это.

**Контекст этого шага.**

Единственный под-промпт. Малый scope.

**Задача.**

1. Интеграция Java Virtual Threads (Project Loom, JEP 444 — Java 21).
2. Замена thread pool (`Executors.newFixedThreadPool`) на `Executors.newVirtualThreadPerTaskExecutor`.
3. Adapt IO-bound operations (network, file) на virtual threads (высокий concurrency, низкая память).
4. CPU-bound operations остаются на platform threads (virtual threads не дают benefit для CPU-bound).
5. Benchmark: virtual threads vs platform threads на 10000 concurrent connections.
6. Config: `concurrency.mode = platform | virtual | hybrid`.

**Ключевые файлы.**

- `diesel/concurrent/VirtualThreadScheduler.java`
- `diesel/concurrent/ThreadModeSelector.java`
- `diesel/DatabaseServer.java (использование virtual threads для client handlers)`
- `pom.xml (требование Java 21+)`
- `diesel/ConfigLoader.java (concurrency.mode)`

**Критерии приёмки.**

- [ ] VirtualThreadConcurrencyTest: 10000 concurrent SELECT — работает (на platform threads — OOM или thread starvation).
- [ ] MemoryTest: 10000 virtual threads < 100 MB heap (vs 10000 platform threads = OOM).
- [ ] ConfigSwitchTest: можно отключить virtual threads (`concurrency.mode = platform`).
- [ ] CpuBoundStaysPlatformTest: CPU-bound queries (агрегации) остаются на platform threads (verified via thread dump).

---

### 159. Record Patterns для чистоты кода

**ID ROADMAP3:** Промпт 126  
**Категория:** — (code quality)  
**Приоритет:** LOW  
**Фаза:** —  
**Зависимости родителя:** — (требует Java 21+)  
**Зависимости этого шага:** —  
**Связь с prompt3.md:** перенесено из prompt3.md #126.

**Проблема.**

Каскады `if instanceof + cast` ухудшают читаемость. Java 21 record patterns + pattern matching switch — современная альтернатива.

**Контекст этого шага.**

Единственный под-промпт. Code quality refactoring.

**Задача.**

1. Refactoring существующего кода с использованием record patterns (JEP 440, Java 21).
2. Pattern matching for switch (JEP 441) — замена каскадов `if instanceof` на `case Type(var a, var b) ->`.
3. Снижение boilerplate в парсере AST nodes: `Query`, `Expression`, `Predicate` и т.д.
4. Замена `instanceof + cast` на `switch (obj) { case Foo foo -> ...; case Bar bar -> ...; }`.
5. Документация: code style guide для новых contributions.

**Ключевые файлы.**

- `diesel/Query.java (и подклассы — refactoring to records where appropriate)`
- `diesel/Expression.java (record patterns)`
- `diesel/Predicate.java (record patterns)`
- `diesel/QueryOptimizer.java (pattern matching switch)`
- `docs/code-style-guide.md (новый)`

**Критерии приёмки.**

- [ ] RefactoringCountTest: 5+ классов переработаны на records / record patterns (видно в diff).
- [ ] PatternMatchingSwitchTest: в `QueryOptimizer` — заменяет каскад if-instanceof.
- [ ] AllTestsGreenTest: все тесты green после refactoring (без изменения поведения).
- [ ] CodeStyleGuidePublishedTest: code style guide опубликован, contributions следуют ему.

---

---

## Сводная таблица промптов prompt4.md

| # | ID ROADMAP3 / источник | Заголовок | Фаза | Приоритет |
|---|------------------------|-----------|------|-----------|
| 1 | R3-001 | MVCC через версионность строк: Версионная строка (xmin/xmax/commandId) + TupleVisibility | 1 | CRITICAL |
| 2 | R3-001 | MVCC через версионность строк: Undo log + TransactionTableSnapshot (без клонирования) | 1 | CRITICAL |
| 3 | R3-001 | MVCC через версионность строк: Vacuum Manager (очистка мёртвых версий) | 1 | CRITICAL |
| 4 | R3-001 | MVCC через версионность строк: Адаптация SelectQuery/InsertQuery/UpdateQuery/DeleteQuery к версиям | 1 | CRITICAL |
| 5 | R3-001 | MVCC через версионность строк: SERIALIZABLE — conflict detection на запись | 1 | CRITICAL |
| 6 | R3-002 | Page-based storage + buffer pool (LRU): Page + PageId + формат сериализации (8/16/64 KB) | 1 | CRITICAL |
| 7 | R3-002 | Page-based storage + buffer pool (LRU): BufferPool (LRU + pinned pages) | 1 | CRITICAL |
| 8 | R3-002 | Page-based storage + buffer pool (LRU): PageManager (чтение/запись через FileChannel + O_DIRECT) | 1 | CRITICAL |
| 9 | R3-002 | Page-based storage + buffer pool (LRU): CatalogTable (системные таблицы в страницах) | 1 | CRITICAL |
| 10 | R3-002 | Page-based storage + buffer pool (LRU): Tablespace stub (один каталог = один tablespace) | 1 | CRITICAL |
| 11 | R3-003 | WAL + group commit: WAL формат + WALEntry (LSN/txid/op/before-after/CRC32C) | 1 | CRITICAL |
| 12 | R3-003 | WAL + group commit: WALManager + WALSegment (сегментированный лог) | 1 | CRITICAL |
| 13 | R3-003 | WAL + group commit: WALWriter (single-writer thread + queue) | 1 | CRITICAL |
| 14 | R3-003 | WAL + group commit: Segment rotation + архивация (gzip) | 1 | CRITICAL |
| 15 | R3-003 | WAL + group commit: GroupCommitCoordinator + AsyncWALWriter | 1 | CRITICAL |
| 16 | R3-004 | ARIES Recovery Manager: CheckpointRecord + checkpoint.ptr (atomic write) | 1 | CRITICAL |
| 17 | R3-004 | ARIES Recovery Manager: Analysis phase (построение active tx list) | 1 | CRITICAL |
| 18 | R3-004 | ARIES Recovery Manager: Redo phase (replay операций с LSN > checkpoint) | 1 | CRITICAL |
| 19 | R3-004 | ARIES Recovery Manager: Undo phase + RecoveryManager (оркестрация на startup) | 1 | CRITICAL |
| 20 | R3-005 | Background writer / flusher: BufferPoolFlusher (адаптивная стратегия) | 1 | HIGH |
| 21 | R3-005 | Background writer / flusher: CheckpointManager (интеграция с WAL) | 1 | HIGH |
| 22 | R3-005 | Background writer / flusher: Fuzzy checkpoint (без quiescent state) | 1 | HIGH |
| 23 | R3-006 | Savepoint + Deadlock + Lock timeout: LockManager (гранулярные блокировки S/X/IS/IX) | 1 | HIGH |
| 24 | R3-006 | Savepoint + Deadlock + Lock timeout: WaitForGraph + DeadlockDetector | 1 | HIGH |
| 25 | R3-006 | Savepoint + Deadlock + Lock timeout: LockTimeoutManager (lock.timeout.ms) | 1 | HIGH |
| 26 | R3-006 | Savepoint + Deadlock + Lock timeout: SavepointManager + NestedSavepointStack | 1 | HIGH |
| 27 | R3-007 | Замена Java Object Serialization в net-протоколе: WireProtocol v2 + MessageCodec (binary) | 1 | CRITICAL |
| 28 | R3-007 | Замена Java Object Serialization в net-протоколе: Message types (Query/Result/Prepare/Batch/Health) | 1 | CRITICAL |
| 29 | R3-007 | Замена Java Object Serialization в net-протоколе: Compression (ZSTD > 4KB) + legacy port compat | 1 | CRITICAL |
| 30 | R3-008 | RBAC + Audit log: User/Role/Privilege модель + password hashing | 1 | HIGH |
| 31 | R3-008 | RBAC + Audit log: Authenticator (handshake) + CLI login | 1 | HIGH |
| 32 | R3-008 | RBAC + Audit log: Authorizer (проверка прав перед каждым запросом) | 1 | HIGH |
| 33 | R3-008 | RBAC + Audit log: AuditLogger (diesel_audit_log table) | 1 | HIGH |
| 34 | R3-009 | SSL/TLS transport: SslContextFactory + TLS handshake handler | 1 | HIGH |
| 35 | R3-009 | SSL/TLS transport: CLI client TLS flags + truststore | 1 | HIGH |
| 36 | R3-009 | SSL/TLS transport: mTLS (client cert) + SNI for multi-tenant | 1 | HIGH |
| 37 | R3-010 | Online schema changes (ALTER без блокировки): SchemaVersion + catalog versioning | 1 | HIGH |
| 38 | R3-010 | Online schema changes (ALTER без блокировки): ALGORITHM=copy (background copy + atomic rename) | 1 | HIGH |
| 39 | R3-010 | Online schema changes (ALTER без блокировки): ALGORITHM=inplace + LOCK=NONE/SHARED/EXCLUSIVE | 1 | HIGH |
| 40 | R3-011 | Backup / Restore (logical + physical): diesel_dump (logical backup CLI) + diesel_restore | 1 | HIGH |
| 41 | R3-011 | Backup / Restore (logical + physical): diesel_backup (physical: WAL checkpoint + pages + tail WAL) | 1 | HIGH |
| 42 | R3-011 | Backup / Restore (logical + physical): IncrementalBackup (только изменившиеся pages) | 1 | HIGH |
| 43 | R3-011 | Backup / Restore (logical + physical): PITR (Point-in-Time Recovery) через WAL replay | 1 | HIGH |
| 44 | R3-012 | Metrics + Prometheus + Health check: MetricsRegistry (counters/histograms/gauges) | 1 | HIGH |
| 45 | R3-012 | Metrics + Prometheus + Health check: PrometheusExporter (/metrics HTTP endpoint) | 1 | HIGH |
| 46 | R3-012 | Metrics + Prometheus + Health check: HealthCheck endpoint (/health JSON) | 1 | HIGH |
| 47 | R3-013 | Fix всех blocking bugs из problems.md: R3-013a: стабильные row-id (CRITICAL correctness fix) | 1 | CRITICAL |
| 48 | R3-013 | Fix всех blocking bugs из problems.md: R3-013b: TRUE/FALSE/NULL как литералы в парсере | 1 | CRITICAL |
| 49 | R3-013 | Fix всех blocking bugs из problems.md: R3-013c: регистр строковых литералов сохраняется | 1 | CRITICAL |
| 50 | R3-013 | Fix всех blocking bugs из problems.md: R3-013d: явная сериализация indexDefinitions | 1 | CRITICAL |
| 51 | R3-014 | Checkpoint + Checksummed pages (CRC32C): CRC32C implementation (hardware-accelerated) | 2 | HIGH |
| 52 | R3-014 | Checkpoint + Checksummed pages (CRC32C): ChecksummedPage (CRC32C в header + verify on read) | 2 | HIGH |
| 53 | R3-014 | Checkpoint + Checksummed pages (CRC32C): Background PageScrubber + checkpoint strategy | 2 | HIGH |
| 54 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers: libpq message protocol (separate port 5432) | 2 | HIGH |
| 55 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers: JDBC driver (diesel-jdbc module) | 2 | HIGH |
| 56 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers: Python + Node.js drivers | 2 | HIGH |
| 57 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers: Go + Rust drivers | 2 | HIGH |
| 58 | R3-016 | Replication (logical + physical + quorum/Raft): Physical streaming replication (WAL streaming to standbys) | 2 | HIGH |
| 59 | R3-016 | Replication (logical + physical + quorum/Raft): Replication slots (защита от WAL удаления) | 2 | HIGH |
| 60 | R3-016 | Replication (logical + physical + quorum/Raft): Synchronous replication (wait for N standby acks) | 2 | HIGH |
| 61 | R3-016 | Replication (logical + physical + quorum/Raft): Logical replication (WAL decoding to change events) | 2 | HIGH |
| 62 | R3-016 | Replication (logical + physical + quorum/Raft): Raft consensus + Failover (multi-master elections) | 2 | HIGH |
| 63 | R3-017 | Partitioning (range / list / hash): PARTITION BY RANGE/LIST/HASH syntax + storage layout | 2 | MEDIUM |
| 64 | R3-017 | Partitioning (range / list / hash): Partition pruning в query optimizer | 2 | MEDIUM |
| 65 | R3-017 | Partitioning (range / list / hash): EXCHANGE PARTITION + subpartitioning | 2 | MEDIUM |
| 66 | R3-018 | Cost-Based Optimizer + статистика: StatisticsCollector (pg_stats analog) | 2 | HIGH |
| 67 | R3-018 | Cost-Based Optimizer + статистика: CostEstimator + access path selection | 2 | HIGH |
| 68 | R3-018 | Cost-Based Optimizer + статистика: Join algorithms cost + plan selection | 2 | HIGH |
| 69 | R3-018 | Cost-Based Optimizer + статистика: Subquery unnesting + PlanCache | 2 | HIGH |
| 70 | R3-019 | UPSERT / RETURNING / UPSERT-on-conflict: ON CONFLICT DO UPDATE / NOTHING (UPSERT) | 2 | HIGH |
| 71 | R3-019 | UPSERT / RETURNING / UPSERT-on-conflict: RETURNING для INSERT/UPDATE/DELETE | 2 | HIGH |
| 72 | R3-019 | UPSERT / RETURNING / UPSERT-on-conflict: Conflict target (column / constraint name) | 2 | HIGH |
| 73 | R3-020 | CI/CD v2 — coverage, matrix, releases: Surefire pattern fix + JaCoCo + Sonar | 2 | HIGH |
| 74 | R3-020 | CI/CD v2 — coverage, matrix, releases: Matrix build (JDK 17/21/25, OS matrix) | 2 | HIGH |
| 75 | R3-020 | CI/CD v2 — coverage, matrix, releases: Release pipeline + Docker + Helm | 2 | HIGH |
| 76 | R3-021 | CTE + рекурсивные CTE: Non-recursive CTE (WITH name AS (SELECT ...)) | 3 | MEDIUM |
| 77 | R3-021 | CTE + рекурсивные CTE: Recursive CTE (WITH RECURSIVE) + termination check | 3 | MEDIUM |
| 78 | R3-021 | CTE + рекурсивные CTE: MATERIALIZED / NOT MATERIALIZED hints + CBO integration | 3 | MEDIUM |
| 79 | R3-022 | Оконные функции: Базовые функции: ROW_NUMBER / RANK / DENSE_RANK / NTILE | 3 | MEDIUM |
| 80 | R3-022 | Оконные функции: Navigation: LAG / LEAD / FIRST_VALUE / LAST_VALUE / NTH_VALUE | 3 | MEDIUM |
| 81 | R3-022 | Оконные функции: Aggregates OVER + Frame (ROWS/RANGE BETWEEN) | 3 | MEDIUM |
| 82 | R3-023 | FULL OUTER JOIN, LATERAL, CROSS APPLY: FULL OUTER JOIN | 3 | MEDIUM |
| 83 | R3-023 | FULL OUTER JOIN, LATERAL, CROSS APPLY: LATERAL JOIN (correlated subquery in FROM) | 3 | MEDIUM |
| 84 | R3-023 | FULL OUTER JOIN, LATERAL, CROSS APPLY: CROSS APPLY / OUTER APPLY (T-SQL compat) | 3 | MEDIUM |
| 85 | R3-024 | UNION / INTERSECT / EXCEPT: UNION + UNION ALL (streaming merge) | 3 | MEDIUM |
| 86 | R3-024 | UNION / INTERSECT / EXCEPT: INTERSECT + EXCEPT (set difference) | 3 | MEDIUM |
| 87 | R3-024 | UNION / INTERSECT / EXCEPT: Precedence + скобки | 3 | MEDIUM |
| 88 | R3-025 | Foreign Keys с CASCADE: FOREIGN KEY + проверка referential integrity | 3 | MEDIUM |
| 89 | R3-025 | Foreign Keys с CASCADE: CASCADE DELETE/UPDATE/SET NULL + multi-level | 3 | MEDIUM |
| 90 | R3-025 | Foreign Keys с CASCADE: Online ADD CONSTRAINT NOT VALID + background validation | 3 | MEDIUM |
| 91 | R3-026 | CHECK constraints, NOT NULL, DEFAULT: CHECK constraint + named constraints | 3 | MEDIUM |
| 92 | R3-026 | CHECK constraints, NOT NULL, DEFAULT: NOT NULL enforcement + DEFAULT expressions | 3 | MEDIUM |
| 93 | R3-026 | CHECK constraints, NOT NULL, DEFAULT: DROP CONSTRAINT + dependency check | 3 | MEDIUM |
| 94 | R3-027 | Materialized Views + refresh: CREATE MATERIALIZED VIEW + REFRESH MANUAL | 3 | LOW |
| 95 | R3-027 | Materialized Views + refresh: REFRESH ON COMMIT + REFRESH EVERY + CONCURRENTLY | 3 | LOW |
| 96 | R3-027 | Materialized Views + refresh: Query rewrite (CBO automatically uses MV) | 3 | LOW |
| 97 | R3-028 | Triggers BEFORE/AFTER: BEFORE triggers (modify NEW.row перед INSERT) | 3 | LOW |
| 98 | R3-028 | Triggers BEFORE/AFTER: AFTER triggers (side effects: audit log, cascade) | 3 | LOW |
| 99 | R3-028 | Triggers BEFORE/AFTER: Statement-level triggers (FOR EACH STATEMENT) | 3 | LOW |
| 100 | R3-029 | VIEW (non-materialized): CREATE VIEW + SELECT * FROM view (expansion) | 3 | LOW |
| 101 | R3-029 | VIEW (non-materialized): Updatable views (INSERT/UPDATE/DELETE through view) | 3 | LOW |
| 102 | R3-029 | VIEW (non-materialized): CHECK OPTION + DROP TABLE/VIEW dependency | 3 | LOW |
| 103 | R3-030 | Full-text search: Tokenizer + Stemmer (Snowball) | 3 | LOW |
| 104 | R3-030 | Full-text search: FullTextIndex (GIN-структура) + MATCH/AGAINST | 3 | LOW |
| 105 | R3-030 | Full-text search: Relevance scoring (BM25) + Highlighter | 3 | LOW |
| 106 | R3-031 | Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: JSONB (бинарный JSON + индексация по пути) | 3 | MEDIUM |
| 107 | R3-031 | Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: UUID + ARRAY | 3 | MEDIUM |
| 108 | R3-031 | Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: ENUM + INTERVAL | 3 | MEDIUM |
| 109 | R3-031 | Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT: INET/CIDR + BIT | 3 | MEDIUM |
| 110 | R3-032 | Хранимые функции (SQL-only, без PL/pgSQL): CREATE FUNCTION + RETURNS | 3 | LOW |
| 111 | R3-032 | Хранимые функции (SQL-only, без PL/pgSQL): Volatility (IMMUTABLE/STABLE/VOLATILE) + inlining | 3 | LOW |
| 112 | R3-032 | Хранимые функции (SQL-only, без PL/pgSQL): TABLE-returning + named arguments | 3 | LOW |
| 113 | R3-033 | Шардинг + distributed query planner: CREATE SHARDED TABLE + ShardMap | 3 | MEDIUM |
| 114 | R3-033 | Шардинг + distributed query planner: Distributed query planner (push-down + fan-out) | 3 | MEDIUM |
| 115 | R3-033 | Шардинг + distributed query planner: Distributed JOIN (co-located + broadcast) + 2PC writes | 3 | MEDIUM |
| 116 | R3-034 | 2PC distributed transactions: TwoPhaseCommitCoordinator (PREPARE + COMMIT/ABORT) | 3 | LOW |
| 117 | R3-034 | 2PC distributed transactions: CoordinatorLog (persistent state machine) | 3 | LOW |
| 118 | R3-034 | 2PC distributed transactions: Timeout + failure handling | 3 | LOW |
| 119 | R3-035 | Row-Level Security (RLS): CREATE POLICY + ENABLE ROW LEVEL SECURITY | 3 | MEDIUM |
| 120 | R3-035 | Row-Level Security (RLS): Multiple policies (OR / AND combination) | 3 | MEDIUM |
| 121 | R3-035 | Row-Level Security (RLS): BYPASSRLS + current_tenant() function | 3 | MEDIUM |
| 122 | R3-036 | Column-level privileges + masking | 3 | LOW |
| 123 | R3-037 | TDE at rest encryption: AES-256-GCM page cipher + MasterKeyProvider | 3 | MEDIUM |
| 124 | R3-037 | TDE at rest encryption: KMS providers (AWS KMS, Vault) + key rotation | 3 | MEDIUM |
| 125 | R3-037 | TDE at rest encryption: Tablespace-level encryption + WAL encryption | 3 | MEDIUM |
| 126 | R3-038 | Vectorized execution (batch): Batch container (columnar) + VectorizedScan | 3 | MEDIUM |
| 127 | R3-038 | Vectorized execution (batch): VectorizedFilter + VectorizedProject + expressions | 3 | MEDIUM |
| 128 | R3-038 | Vectorized execution (batch): VectorizedAggregate + RowBatchAdapter (interop) | 3 | MEDIUM |
| 129 | R3-039 | Adaptive joins (runtime switching): AdaptiveJoinExecutor (runtime cardinality check) | 3 | LOW |
| 130 | R3-039 | Adaptive joins (runtime switching): RuntimeStatisticsCollector | 3 | LOW |
| 131 | R3-039 | Adaptive joins (runtime switching): PlanFeedback (persistent для будущих планов) | 3 | LOW |
| 132 | R3-040 | Parallel query scan + aggregation: ParallelScanExecutor + RangeSplitter | 3 | MEDIUM |
| 133 | R3-040 | Parallel query scan + aggregation: ParallelAggregationExecutor + Gather merge | 3 | MEDIUM |
| 134 | R3-040 | Parallel query scan + aggregation: Adaptive parallelism + partition-aware | 3 | MEDIUM |
| 135 | R3-041 | Bitmap indexes: BitmapIndex + CompressedBitmap (WAH) | 3 | LOW |
| 136 | R3-041 | Bitmap indexes: Bitwise operations (AND/OR/NOT) + BitmapScanExecutor | 3 | LOW |
| 137 | R3-041 | Bitmap indexes: CBO integration (cost model for bitmap scan) | 3 | LOW |
| 138 | R3-042 | Covering indexes (INCLUDE) | 3 | LOW |
| 139 | R3-044 | TPC-C / TPC-H сертификация: TPC-C workload (10 warehouses, ACID проверка) | 3 | LOW |
| 140 | R3-044 | TPC-C / TPC-H сертификация: TPC-H (22 queries, SF=1, results verification) | 3 | LOW |
| 141 | R3-044 | TPC-C / TPC-H сертификация: Regression tracking + public benchmark report | 3 | LOW |
| 142 | R3-045 | GUI админ-панель (аналог pgAdmin): Web UI dashboard + schema viewer | 3 | LOW |
| 143 | R3-045 | GUI админ-панель (аналог pgAdmin): SQL editor + EXPLAIN visualizer | 3 | LOW |
| 144 | R3-045 | GUI админ-панель (аналог pgAdmin): User management + Backup UI + Replication monitor | 3 | LOW |
| 145 | Промпт 111 | Lock — deadlock prevention стратегии: Wait-die + Wound-wait стратегии | — | MEDIUM |
| 146 | Промпт 111 | Lock — deadlock prevention стратегии: No-wait (immediate abort on conflict) | — | MEDIUM |
| 147 | Промпт 111 | Lock — deadlock prevention стратегии: Config switch + benchmark comparison | — | MEDIUM |
| 148 | Промпт 113 | ALTER TABLE ADD COLUMN — базовый SQL | — | HIGH |
| 149 | Промпт 114 | ALTER TABLE DROP COLUMN — базовый SQL | — | MEDIUM |
| 150 | Промпт 116 | DROP INDEX | — | MEDIUM |
| 151 | Промпт 117 | TRUNCATE TABLE | — | HIGH |
| 152 | Промпт 118 | CREATE SEQUENCE | — | MEDIUM |
| 153 | Промпт 119 | DROP SEQUENCE | — | LOW |
| 154 | Промпт 120 | Query Result Cache | — | MEDIUM |
| 155 | Промпт 121 | Bulk Insert / Copy API: BULK INSERT FROM file (CSV/TSV/JSONL/AVRO) | — | HIGH |
| 156 | Промпт 121 | Bulk Insert / Copy API: PostgreSQL COPY FROM / COPY TO compat | — | HIGH |
| 157 | Промпт 121 | Bulk Insert / Copy API: Batching + error handling + progress | — | HIGH |
| 158 | Промпт 125 | Virtual Threads для concurrency | — | LOW |
| 159 | Промпт 126 | Record Patterns для чистоты кода | — | LOW |

---

## Рекомендуемый порядок выполнения

Ссылки на номера ниже — на плоские номера промптов (1..N).
Поиск нужного номера — в карте промптов в начале документа.

### Sprint 1 (недели 1-4): фундамент
- Промпты по теме Page storage + buffer pool (R3-002, 5 шт.)
- Промпты по теме Замена Java-сериализации в протоколе (R3-007, 3 шт.)
- Быстрые фиксы парсера из R3-013 (TRUE/FALSE/NULL, регистр, indexDefinitions)

### Sprint 2 (недели 5-8): MVCC + WAL
- Промпты по теме MVCC (R3-001, 5 шт.)
- Промпты по теме WAL + group commit (R3-003, 5 шт.) — параллельно
- Стабильные row-id (R3-013a)

### Sprint 3 (недели 9-12): recovery + observability
- Промпты по теме ARIES recovery (R3-004, 4 шт.)
- Промпты по теме Background writer (R3-005, 3 шт.)
- Промпты по теме Metrics + Prometheus (R3-012, 3 шт.) — параллельно

### Sprint 4 (недели 13-16): security + ops
- Промпты по Savepoint + Deadlock + Lock timeout (R3-006, 4 шт.)
- Промпты по RBAC + audit (R3-008, 4 шт.)
- Промпты по TLS transport (R3-009, 3 шт.)

### Sprint 5 (недели 17-20): polish
- Промпты по Online schema changes (R3-010, 3 шт.)
- Промпты по Backup / restore (R3-011, 4 шт.)
- Закрытие оставшихся багов из R3-013
- Базовый ALTER (R3-010 prereq, доборочные промпты 47-48 v2 → теперь в плоской нумерации)
- Performance testing, bug bash, документация

### Sprint 6 (недели 21-24): buffer
- Регрессии, edge cases, hardening
- Deadlock prevention, TRUNCATE, Bulk Insert (доборочные)
- Подготовка release notes
- Migration guide для существующих пользователей

### Фаза 2 (6-12 мес): multi-tenant production
- R3-014..R3-020 (Checkpoint+CRC, libpq drivers, Replication, Partitioning, CBO, UPSERT, CI/CD)

### Фаза 3 (12-24+ мес): PostgreSQL-parity
- R3-021..R3-045 (кроме R3-043 Parquet, удалён)
- Доборочные: DROP INDEX, CREATE/DROP SEQUENCE, Query Cache, Virtual Threads, Record Patterns — по мере необходимости

---

## Примечания

1. **Параллелизм.** Промпты Фазы 1 по темам MVCC (R3-001), Page storage (R3-002), WAL (R3-003), Wire protocol (R3-007) — фундаментальные, могут делаться параллельно при наличии 2+ разработчиков.
2. **Документация.** После завершения каждой задачи: обновление `KNOWN_LIMITATIONS.md`, `PERSISTENCE_README.md`, `README.md`.
3. **Версионирование.** Каждый промпт — отдельный feature branch → PR → squash merge.
4. **Совместимость.** Весь Фаза 1 поддерживает legacy format через feature flags.
5. **Benchmarks.** До/после каждого промпта — измерение на 3 scenarios: (a) 1k rows, (b) 100k rows, (c) 1M rows; сохранение в `analytics/performance_history.csv`.
6. **Backward-compat тесты.** Перед закрытием PR: все существующие тесты green; regression > 10 % latency — блокирующий merge.
7. **Плоская нумерация.** Все промпты пронумерованы сквозной нумерацией 1..N. Внутри одного ROADMAP3 ID промпты упорядочены по логике реализации (шаги 1, 2, 3, ...). Зависимости указаны в поле «Зависимости этого шага».
8. **Parquet исключён.** R3-043 (Parquet storage) удалён из плана. Все ссылки на Parquet как формат хранения аннулированы. Если в будущем потребуется columnar — рассматривать через собственный page-based columnar layout, не Parquet.
