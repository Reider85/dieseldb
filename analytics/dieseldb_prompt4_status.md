# DieselDB — статус реализации промптов `prompt4.md`

**Репозиторий:** https://github.com/Reider85/dieseldb  
**Ветка/коммит:** `main` @ `a326048` (pom `3.4.89`)  
**Дата проверки:** 2026-11-20  
**Метод:** каждый промпт верифицирован поиском исходников и тестов в дереве репозитория. Для статуса используется:

- **DONE** — основной класс(ы) существует, нетривиальная реализация, есть хотя бы один тест.
- **PARTIAL** — класс существует, но не хватает ключевых методов/DoD, либо нет теста, либо `KNOWN_LIMITATIONS.md` явно помечает пробел.
- **MISSING** — файла нет либо только stub/placeholder.

---

## 1. Сводная статистика

| Фаза | Промпты | DONE | PARTIAL | MISSING | Готовность |
|---|---|---|---|---|---|
| **Phase 1 — Critical (R3-001..R3-013)** | 1–50 | 43 | 7 | 0 | **93 %** |
| **Phase 2 — Production-Grade (R3-014..R3-020)** + начало Phase 3 | 51–100 | 12 | 7 | 31 | **31 %** |
| **Phase 3 — PostgreSQL-Parity (R3-021..R3-045)** + доборочные | 101–159 | 3 | 6 | 50 | **10 %** |
| **ИТОГО** | **1–159** | **58** | **20** | **81** | **37 % DONE, 13 % PARTIAL, 51 % MISSING** |

Вывод: **Phase 1 фактически закрыта**, Phase 2 реализована точечно (CRC32C, libpq, drivers, replication, CBO-join, CI/JaCoCo — есть; partitioning, Raft, UPSERT, RETURNING — нет), **Phase 3 практически не начиналась** (только Covering Index и пара доборочных ALTER).

---

## 2. Карта по ROADMAP3 ID

| ROADMAP3 ID | Промпты | DONE | PARTIAL | MISSING | Комментарий |
|---|---|---|---|---|---|
| R3-001 MVCC | 1–5 | 5 | 0 | 0 | Полностью: версии строк, undo log, vacuum, SERIALIZABLE |
| R3-002 Page storage + buffer pool | 6–10 | 4 | 1 | 0 | O_DIRECT не реализован, CatalogTable prod — heap-only |
| R3-003 WAL + group commit | 11–15 | 5 | 0 | 0 | Полностью: формат, сегменты, writer, archive, group commit |
| R3-004 ARIES Recovery | 16–19 | 4 | 0 | 0 | Полностью: checkpoint.ptr, Analysis/Redo/Undo |
| R3-005 Background writer | 20–22 | 3 | 0 | 0 | Полностью: flusher, CheckpointManager, Fuzzy |
| R3-006 Locks + Savepoint | 23–26 | 3 | 1 | 0 | SavepointManager без тестов и без SQL SAVEPOINT |
| R3-007 Wire protocol v2 | 27–29 | 3 | 0 | 0 | Полностью: binary codec, message types, compression |
| R3-008 RBAC + Audit | 30–33 | 4 | 0 | 0 | Полностью: User/Role/Privilege, Auth, Authorizer, AuditLogger |
| R3-009 SSL/TLS | 34–36 | 3 | 0 | 0 | Полностью: TLS, mTLS, SNI, CLI flags |
| R3-010 Online ALTER | 37–39 | 0 | 3 | 0 | Классы есть, но heap-only catalog + COPY-only fallback для DROP COLUMN с индексом |
| R3-011 Backup/Restore | 40–43 | 3 | 1 | 0 | **LogicalDump/Restore — заглушки**, остальное реализовано |
| R3-012 Metrics + Health | 44–46 | 3 | 0 | 0 | Полностью: MetricsRegistry, Prometheus, Health |
| R3-013 Parser bugs | 47–50 | 4 | 0 | 0 | Все 4 фикса (row-id, литералы, регистр, indexDefs) |
| R3-014 Checksummed pages | 51–53 | 2 | 1 | 0 | CRC32C + ChecksummedPage готовы, PageScrubber без тестов |
| R3-015 libpq + drivers | 54–57 | 4 | 0 | 0 | Полностью: libpq, JDBC, Python/Node, Go/Rust |
| R3-016 Replication | 58–62 | 4 | 0 | 1 | **Raft отсутствует полностью**, остальное есть |
| R3-017 Partitioning | 63–65 | 0 | 0 | 3 | Не начато |
| R3-018 CBO + статистика | 66–69 | 1 | 3 | 0 | Join cost есть; column stats, access-path CBO, subquery unnesting — PARTIAL |
| R3-019 UPSERT + RETURNING | 70–72 | 0 | 0 | 3 | Не начато (нет токенов ON CONFLICT / RETURNING) |
| R3-020 CI/CD v2 | 73–75 | 1 | 2 | 0 | JaCoCo/Sonar есть, matrix без JDK 17/25, нет Docker/Helm |
| R3-021 CTE | 76–78 | 0 | 0 | 3 | Не начато |
| R3-022 Window functions | 79–81 | 0 | 0 | 3 | Не начато |
| R3-023 FULL OUTER / LATERAL / APPLY | 82–84 | 0 | 1 | 2 | Только парсинг FULL OUTER без executor-ветки |
| R3-024 UNION/INTERSECT/EXCEPT | 85–87 | 0 | 0 | 3 | Не начато |
| R3-025 Foreign Keys | 88–90 | 0 | 0 | 3 | Не начато |
| R3-026 CHECK/NOT NULL/DEFAULT | 91–93 | 0 | 0 | 3 | Не начато |
| R3-027 Materialized Views | 94–96 | 0 | 0 | 3 | Не начато |
| R3-028 Triggers | 97–99 | 0 | 0 | 3 | Не начато |
| R3-029 VIEW | 100–102 | 0 | 0 | 3 | Не начато |
| R3-030 Full-text search | 103–105 | 0 | 0 | 3 | Не начато |
| R3-031 Расширенные типы | 106–109 | 0 | 2 | 2 | UUID/ARRAY/ENUM есть на уровне Avro, нет SQL-операторов; JSONB/INET/BIT отсутствуют |
| R3-032 Stored functions | 110–112 | 0 | 0 | 3 | Не начато |
| R3-033 Sharding | 113–115 | 0 | 0 | 3 | Не начато |
| R3-034 2PC | 116–118 | 0 | 0 | 3 | Не начато |
| R3-035 RLS | 119–121 | 0 | 0 | 3 | Не начато (есть tenants, но не RLS) |
| R3-036 Column privileges + masking | 122 | 0 | 0 | 1 | Не начато |
| R3-037 TDE | 123–125 | 0 | 0 | 3 | Не начато |
| R3-038 Vectorized execution | 126–128 | 0 | 0 | 3 | Не начато |
| R3-039 Adaptive joins | 129–131 | 0 | 0 | 3 | Не начато |
| R3-040 Parallel query | 132–134 | 0 | 0 | 3 | Не начато |
| R3-041 Bitmap indexes | 135–137 | 0 | 0 | 3 | Не начато |
| R3-042 Covering indexes (INCLUDE) | 138 | 1 | 0 | 0 | Готово (через `COVERING(...)`) |
| R3-044 TPC-C / TPC-H | 139–141 | 0 | 0 | 3 | Не начато |
| R3-045 Web admin UI | 142–144 | 0 | 0 | 3 | Не начато (только Grafana JSON) |
| Доборочный Промпт 111 (deadlock prevention) | 145–147 | 0 | 0 | 3 | Не начато (только detection-based) |
| Доборочный Промпт 113 ALTER ADD COLUMN | 148 | 1 | 0 | 0 | Готово |
| Доборочный Промпт 114 ALTER DROP COLUMN | 149 | 1 | 0 | 0 | Готово |
| Доборочный Промпт 116 DROP INDEX | 150 | 0 | 0 | 1 | Не начато |
| Доборочный Промпт 117 TRUNCATE | 151 | 0 | 0 | 1 | Opcode есть в enum, но не обрабатывается |
| Доборочный Промпт 118 CREATE SEQUENCE | 152 | 0 | 1 | 0 | Sequence-класс есть, но только inline в CREATE TABLE |
| Доборочный Промпт 119 DROP SEQUENCE | 153 | 0 | 0 | 1 | Не начато |
| Доборочный Промпт 120 Query Result Cache | 154 | 0 | 1 | 0 | `QueryCache` кэширует планы, не результаты |
| Доборочный Промпт 121 Bulk Insert / COPY | 155–157 | 0 | 3 | 0 | Java API + JSONL loader готовы, нет SQL `BULK INSERT` / `COPY` |
| Доборочный Промпт 125 Virtual Threads | 158 | 0 | 0 | 1 | Не начато |
| Доборочный Промпт 126 Record Patterns | 159 | 0 | 0 | 1 | Не начато |

---

## 3. Полная таблица по всем 159 промптам

| # | ID | Заголовок | Статус | Главные файлы | Тесты | Заметка |
|---|---|---|---|---|---|---|
| 1 | R3-001 | Версионная строка + TupleVisibility | DONE | `diesel/TupleVisibility.java`, `RowVersionMeta.java`, `MvccReadContext.java` | `TupleVisibilityTest.java` | xmin/xmax/commandId в RowVersionMeta |
| 2 | R3-001 | Undo log + TxTableSnapshot | DONE | `diesel/UndoLog.java`, `TransactionTableSnapshot.java`, `TxStatusTracker.java` | `UndoLogTest.java`, `TransactionTableSnapshotTest.java`, `UndoLogSpillTest.java` | Без клонирования |
| 3 | R3-001 | Vacuum Manager | DONE | `diesel/VacuumManager.java`, `VacuumQuery.java` | `VacuumTest.java`, `VacuumManagerTest.java` | С reindex |
| 4 | R3-001 | MVCC в Select/Insert/Update/Delete | DONE | `diesel/SelectQuery.java`, `InsertQuery.java`, `UpdateQuery.java`, `DeleteQuery.java` | `MvccUpdateTest.java`, `MvccDeleteTest.java` | Все четыре ссылаются на RowVersionMeta |
| 5 | R3-001 | SERIALIZABLE conflict detection | DONE | `diesel/concurrency/ConflictDetector.java`, `IsolationLevel.java`, `SerializationFailureException.java` | `SerializationConflictTest.java`, `ConcurrentConflictTest.java` | Реальная conflict detection |
| 6 | R3-002 | Page + PageId + 8/16/64 KB | DONE | `diesel/storage/page/Page.java`, `PageId.java`, `PageConfig.java`, `PageHeader.java` | `PageTest.java`, `BufferPoolTest.java` | PageConfig поддерживает размеры |
| 7 | R3-002 | BufferPool LRU + pinned | DONE | `diesel/storage/page/BufferPool.java`, `LruEvictionPolicy.java`, `PinnedPage.java` | `BufferPoolTest.java`, `LruEvictionPolicyUnpinnedQueueTest.java` | Полный LRU с pin |
| 8 | R3-002 | PageManager FileChannel + O_DIRECT | DONE | `diesel/storage/page/PageManager.java`, `FileChannelIO.java`, `O_DIRECT_Natives.java`, `O_DIRECT_IO.java`, `O_DIRECT_BufferPool.java`, `FallbackIO.java` | `O_DIRECT_IO_Test.java`, `O_DIRECT_Integration_Test.java`, `O_DIRECT_Performance_Test.java`, `O_DIRECT_CrashRecovery_Test.java` | Complete O_DIRECT implementation with native library, JNI bindings, aligned buffers, graceful fallback on Windows |
| 9 | R3-002 | CatalogTable системные таблицы | PARTIAL | `diesel/storage/page/CatalogTable.java`, `CatalogSchema.java` | `CatalogTest.java`, `CatalogLookupIsO1Test.java` | KNOWN_LIMITATIONS §17: prod `CatalogTable(null)` heap-only |
| 10 | R3-002 | Tablespace stub | DONE | `diesel/storage/tablespace/Tablespace.java`, `TablespaceRegistry.java` | `TablespaceTest.java` | Stub → вышел в нетривиальную impl |
| 11 | R3-003 | WAL формат + WALEntry + CRC32C | DONE | `diesel/wal/WALEntry.java`, `WALFormat.java`, `WALOpcode.java` | `WALEntryTest.java`, `WALManagerTest.java` | LSN/txid/op/before-after/CRC32C |
| 12 | R3-003 | WALManager + WALSegment | DONE | `diesel/wal/WALManager.java`, `WALSegment.java` | `WALManagerTest.java`, `WALCrashRecoveryTest.java` | Сегментированный лог |
| 13 | R3-003 | WALWriter + queue | DONE | `diesel/wal/WALWriter.java`, `WALQueue.java`, `WALWriteRequest.java` | `WALWriterTest.java`, `WALBackpressureTest.java` | Single-writer thread |
| 14 | R3-003 | Segment rotation + gzip archive | DONE | `diesel/wal/WALSegmentRotator.java`, `WALArchiver.java` | `SegmentRotationTest.java`, `ArchiveTest.java` | С retention |
| 15 | R3-003 | GroupCommit + AsyncWALWriter | DONE | `diesel/wal/GroupCommitCoordinator.java`, `AsyncWALWriter.java` | `GroupCommitCoordinatorTest.java`, `AsyncWALWriterTest.java` | Batched fsync |
| 16 | R3-004 | CheckpointRecord + checkpoint.ptr | DONE | `diesel/recovery/CheckpointRecord.java`, `CheckpointPointerFile.java` | `CheckpointRecordTest.java`, `CheckpointPointerFileTest.java` | Atomic write |
| 17 | R3-004 | Analysis phase | DONE | `diesel/recovery/AnalysisPhase.java`, `AnalysisResult.java` | `AnalysisTest.java`, `AnalysisPerformanceTest.java` | dirty page table + active tx |
| 18 | R3-004 | Redo phase | DONE | `diesel/recovery/RedoPhase.java`, `RedoResult.java`, `MvccRedoSink.java` | `RedoTest.java`, `RedoPerformanceTest.java` | Replay с LSN > checkpoint |
| 19 | R3-004 | Undo phase + RecoveryManager | DONE | `diesel/recovery/UndoPhase.java`, `diesel/RecoveryManager.java`, `ARIESAlgorithm.java` | `UndoPhaseTest.java`, `RecoveryManagerTest.java`, `RecoveryIntegrationTest.java` | Полный startup pipeline |
| 20 | R3-005 | BufferPoolFlusher adaptive | DONE | `diesel/storage/page/BufferPoolFlusher.java`, `AdaptiveFlushStrategy.java` | `BufferPoolFlusherTest.java`, `AdaptiveFlushTest.java` | Adaptive dirty ratio |
| 21 | R3-005 | CheckpointManager (WAL) | DONE | `diesel/storage/page/CheckpointManager.java` | `CheckpointManagerTest.java`, `CheckpointIntegrationTest.java` | С archive hooks |
| 22 | R3-005 | Fuzzy checkpoint | DONE | `diesel/recovery/FuzzyCheckpoint.java`, `CheckpointMode.java` | `CheckpointIntegrationTest.java` (косвенно) | Без quiescent |
| 23 | R3-006 | LockManager S/X/IS/IX | DONE | `diesel/concurrency/LockManager.java`, `Lock.java`, `LockMode.java`, `LockCompatibility.java` | `LockManagerTest.java`, `LockCompatibilityTest.java` | Полная intent-lock матрица |
| 24 | R3-006 | WaitForGraph + DeadlockDetector | DONE | `diesel/concurrency/WaitForGraph.java`, `DeadlockDetector.java` | `WaitForGraphTest.java`, `DeadlockTest.java`, `ChainDeadlockTest.java` | Cycle detection + victim |
| 25 | R3-006 | LockTimeoutManager | DONE | `diesel/concurrency/LockTimeoutManager.java`, `LockTimeoutException.java` | `LockTimeoutTest.java`, `LockTimeoutConfigTest.java` | lock.timeout.ms |
| 26 | R3-006 | SavepointManager + NestedStack | PARTIAL | `diesel/concurrency/SavepointManager.java`, `NestedSavepointStack.java`, `Savepoint.java` | **нет** | Классы нетривиальны, но нет тестов и нет SQL `SAVEPOINT` |
| 27 | R3-007 | WireProtocol v2 + MessageCodec | DONE | `diesel/net/WireProtocol.java`, `MessageCodec.java`, `MessageType.java`, `Message.java` | `WireProtocolTest.java`, `WireProtocolSecurityTest.java` | Binary codec, versioned framing |
| 28 | R3-007 | Message types Q/R/P/B/H | DONE | `diesel/net/messages/*.java` (Query/Result/Prepare/Batch/Health/Auth/Compression/Cursor) | `BatchQueryTest.java`, `PreparedStatementTest.java`, `CursorTest.java` | Все типы + кодеки |
| 29 | R3-007 | Compression (ZSTD > 4KB) + legacy | DONE | `diesel/net/CompressionHandshakeHandler.java`, `LegacyProtocolAdapter.java` | `NetCompressionTest.java`, `LegacyCompatTest.java` | ZSTD preferred + GZIP fallback |
| 30 | R3-008 | User/Role/Privilege + hashing | DONE | `diesel/security/User.java`, `Role.java`, `Privilege.java`, `PasswordHasher.java` | `PasswordHasherTest.java`, `UserStoreTest.java`, `DefaultAdminTest.java` | PBKDF2/scrypt |
| 31 | R3-008 | Authenticator + CLI login | DONE | `diesel/security/Authenticator.java`, `Session.java`, `SessionToken.java` | `AuthTest.java`, `AuthHandshakeTest.java` | Handshake + token |
| 32 | R3-008 | Authorizer | DONE | `diesel/security/Authorizer.java`, `PrivilegeCache.java` | `RbacTest.java`, `RbacDdlTest.java`, `GrantTest.java` | Privilege cache |
| 33 | R3-008 | AuditLogger | DONE | `diesel/security/AuditLogger.java`, `AuditRecord.java`, `AuditBackend.java` | `AuditTest.java`, `AuditLoggerTest.java`, `AuditAsyncTest.java`, `AuditRetentionTest.java` | Async + retention gzip |
| 34 | R3-009 | SslContextFactory + TLS | DONE | `diesel/net/SslContextFactory.java`, `TlsHandshakeHandler.java`, `CipherSuiteWhitelist.java` | `TlsConnectionTest.java`, `CipherAuditTest.java`, `ExpiredCertTest.java`, `DisableTlsTest.java` | TLS 1.2/1.3 |
| 35 | R3-009 | CLI TLS flags + truststore | DONE | `diesel/DatabaseClient.java`, `diesel/CliRepl.java` | `CliTlsTest.java`, `CliReplPromptTest.java` | `--tls`, `--tls-ca`, `--tls-cert`, `--tls-key` (нет точного имени `--truststore`) |
| 36 | R3-009 | mTLS + SNI | DONE | `diesel/net/MutualTlsHandler.java`, `SniSelector.java` | `MtlsTest.java`, `MtlsRequestTest.java`, `SniTest.java`, `SniSelectorTest.java`, `TenantDdlTest.java` | per-hostname keystore через CREATE TENANT |
| 37 | R3-010 | SchemaVersion + catalog versioning | PARTIAL | `diesel/schema/SchemaVersion.java`, `SchemaVersionRegistry.java` | `SchemaVersionTest.java`, `SchemaEvolutionTest.java` | KNOWN_LIMITATIONS §17: persistence — test-only |
| 38 | R3-010 | ALGORITHM=copy | PARTIAL | `diesel/AlterTableCopyAlgorithm.java`, `diesel/schema/AlterAlgorithm.java` | `CopyAlgorithmTest.java`, `AlterDualWriteTest.java`, `AlterProgressTest.java`, `AlterRollbackTest.java` | KNOWN_LIMITATIONS §18: alterAddedColumns memory-only на heap, нет SQL `ALTER ROLLBACK` |
| 39 | R3-010 | ALGORITHM=inplace + LOCK | PARTIAL | `diesel/schema/AlterLockMode.java`, `AlterTableAddColumnQuery.java`, `AlterTableDropColumnQuery.java` | `InplaceTest.java`, `OnlineAlterTest.java`, `AlterLockTest.java` | KNOWN_LIMITATIONS §19: DROP COLUMN с индексом → COPY fallback, INPLACE не в ALTER STATUS |
| 40 | R3-011 | diesel_dump + diesel_restore | PARTIAL | `diesel/backup/LogicalDump.java`, `LogicalRestore.java`, `DumpTool.java`, `RestoreTool.java`, `*.sh`, `*.bat` | **нет** | `LogicalDump.execute()` пишет "placeholder dump" — заглушка |
| 41 | R3-011 | diesel_backup (physical) | DONE | `diesel/backup/PhysicalBackup.java`, `PhysicalRestore.java`, `DieselBackup.java`, `DieselRestore.java`, `BackupManifest.java` | `PhysicalBackupRestoreTest.java`, `BackupChainRestoreTest.java`, `AvroBackupManagerTest.java` | checkpoint + copy + tail WAL |
| 42 | R3-011 | IncrementalBackup | DONE | `diesel/backup/IncrementalBackup.java` | `IncrementalBackupTest.java` | WAL-based |
| 43 | R3-011 | PITR через WAL replay | DONE | `diesel/backup/PitrManager.java`, `diesel/recovery/PointInTimeRecovery.java`, `diesel/PitrSupport.java`, `diesel/DatabasePitrSink.java` | `PitrTest.java`, `PitrCliTest.java`, `PitrWithActiveTxTest.java` | stage → replay → persist |
| 44 | R3-012 | MetricsRegistry | DONE | `diesel/observability/MetricsRegistry.java`, `Counter.java`, `Histogram.java`, `Gauge.java` | `MetricsRegistryTest.java`, `HistogramTest.java`, `MetricsIntegrationTest.java` | Counter/Histogram/Gauge |
| 45 | R3-012 | PrometheusExporter /metrics | DONE | `diesel/observability/PrometheusExporter.java`, `MetricsHttpServer.java` | `PrometheusExporterTest.java`, `MetricsHttpServerTest.java` | Exposition + HTTP |
| 46 | R3-012 | HealthCheck /health JSON | DONE | `diesel/observability/HealthCheck.java`, `LivenessCheck.java`, `ReadinessCheck.java`, `HealthJson.java`, `diesel/HealthSupport.java` | `LivenessCheckTest.java`, `ReadinessCheckTest.java`, `HealthHttpTest.java`, `HealthSupportTest.java` | JSON /health |
| 47 | R3-013a | Стабильные row-id | DONE | `diesel/Table.java` (rowId + positionToRowIdMap), `diesel/Row.java` | `RowIdStabilityTest.java`, `InsertReusesTombstoneSlotsTest.java`, `PositionToRowIdInverseMapTest.java` | Случайные DML сохраняют rowId |
| 48 | R3-013b | TRUE/FALSE/NULL литералы | DONE | `diesel/QueryParser.java` (строки 2370, 3511, 4149), `SqlKeywords.java`, `ThreeValuedLogic.java` | `TrueFalseLiteralTest.java`, `NullLiteralTest.java` | 3VL в условиях |
| 49 | R3-013c | Регистр строковых литералов | DONE | `diesel/SqlLexer.java`, `diesel/QueryParser.java` | `StringCaseSensitivityTest.java`, `CharsetEncodingTest.java` | KNOWN_LIMITATIONS §20 подтверждает |
| 50 | R3-013d | indexDefinitions serialization | DONE | `diesel/Table.java` (L210, writeObject L2925, readObject L2980), `CreateIndexQuery.java`, `CreateIndexQueryBase.java` | `PersistenceTest.java` | Явный writeObject/readObject |
| 51 | R3-014 | CRC32C hardware-accelerated | DONE | `diesel/checksum/CRC32C.java` | `CRC32CTest.java`, `CRC32CThroughputTest.java` | HotSpot SSE4.2 intrinsic |
| 52 | R3-014 | ChecksummedPage + verify on read | DONE | `diesel/checksum/ChecksummedPage.java`, `PageValidator.java` | `CorruptionDetectionTest.java`, `RecoveryFromCorruptionTest.java`, `SimpleChecksumTest.java`, `MetricTest.java` | CRC32C в header, validate-on-read |
| 53 | R3-014 | Background PageScrubber + checkpoint | PARTIAL | `diesel/storage/page/PageScrubber.java` | **нет** | Daemon-thread impl, wired в PageManager/Database, **нет тестов** |
| 54 | R3-015 | libpq protocol (port 5432) | DONE | `diesel/LibpqServer.java`, `LibpqClientHandler.java`, `diesel/net/libpq/{LibpqProtocol,LibpqMessage,LibpqMessageDecoder,ScramSha256}.java` | `LibpqProtocolTest.java`, `PsqlCompatTest.java`, `ExtendedQueryTest.java`, `ScramSha256Test.java` | simple+extended + SCRAM-SHA-256 |
| 55 | R3-015 | JDBC driver | DONE | `drivers/jdbc/diesel-jdbc/src/main/java/com/diesel/jdbc/*` | `JdbcTest.java`, `HibernateTest.java`, `LibpqTestServer.java` | Полный JDBC + Hibernate |
| 56 | R3-015 | Python + Node.js drivers | DONE | `drivers/python/diesel-python/diesel/*`, `drivers/nodejs/diesel-node/src/*` | `test_driver.py`, `test_async.py`, `diesel-node.test.js` | CRUD + async |
| 57 | R3-015 | Go + Rust drivers | DONE | `drivers/go/diesel-go/*`, `drivers/rust/diesel-rs/src/*` | `diesel_test.go`, `crud.rs` | CRUD |
| 58 | R3-016 | Physical streaming replication | DONE | `diesel/replication/{WalSender,WalReceiver,WalSource,WalApplier,StreamingApplier,ReplicationApplyPipeline,ReplicationWire}.java` | `StreamingReplTest.java`, `HotStandbyTest.java` | slave-lag <100ms @ 1000 inserts/sec |
| 59 | R3-016 | Replication slots | DONE | `diesel/replication/{ReplicationSlot,ReplicationSlotRegistry}.java`, `diesel/{Create,Drop}ReplicationSlotQuery.java` | `ReplicationSlot*Test.java`, `ReplicationSlotIntegrationTest.java`, `WALArchiverReplicationSlotTest.java` | Parser + execution |
| 60 | R3-016 | Synchronous replication | DONE | `diesel/SyncReplicationCoordinator.java`, `diesel/SyncAck.java` | `SyncReplTest.java`, `SyncReplOffTest.java`, `SyncReplTimeoutTest.java` | Wait-for-N-acks + timeout |
| 61 | R3-016 | Logical replication (WAL decoding) | DONE | `diesel/replication/{LogicalDecoder,LogicalChangeRecord,Publication,Subscription}.java`, `diesel/{Create,Drop}{Publication,Subscription}Query.java` | `LogicalReplTest.java` | CREATE/DROP PUBLICATION/SUBSCRIPTION |
| 62 | R3-016 | Raft consensus + Failover | MISSING | — | — | Нет Java-классов Raft/LeaderElection/Consensus |
| 63 | R3-017 | PARTITION BY RANGE/LIST/HASH | MISSING | — | — | Нет `PARTITION BY` в QueryParser (Avro `*Partitioner` — storage layout, не DDL) |
| 64 | R3-017 | Partition pruning | MISSING | — | — | Нет partitions → нет pruning в QueryOptimizer |
| 65 | R3-017 | EXCHANGE PARTITION + subpartition | MISSING | — | — | Нет токена |
| 66 | R3-018 | StatisticsCollector (pg_stats) | PARTIAL | `diesel/AnalyzeTableQuery.java` (45 LOC) | `AnalyzeTableTest.java` | Только row-count + avg-row-size, нет column-level NDV/MCV/histogram, **нет StatisticsCollector class** |
| 67 | R3-018 | CostEstimator + access path | PARTIAL | `diesel/QueryOptimizer.java`, cost в `SelectQuery.preferNestedLoopByStatistics` | `QueryOptimizerTest.java` | Adaptive replanning + PlanCache, но только join-cost, **нет scan-vs-index selection** |
| 68 | R3-018 | Join algorithms cost + plan | DONE | `diesel/SelectQuery.java` (hashJoin, runBlockNestedLoopJoin, runPartitionedHashJoin, preferNestedLoopByStatistics) | `HashJoinMemoryTest.java`, `JoinTest.java`, `JoinQueryTest.java` | In-memory + partitioned hash + block nested loop |
| 69 | R3-018 | Subquery unnesting + PlanCache | PARTIAL | `diesel/{SubqueryParser,SubqueryResolver,SubqueryConstants,QueryCache}.java` | `SubqueriesTest.java`, `QueryCacheTest.java` | Subquery execution + PlanCache есть, **нет unnesting/decorrelation** |
| 70 | R3-019 | ON CONFLICT DO UPDATE/NOTHING | MISSING | — | — | Нет `ON CONFLICT` в QueryParser/InsertQuery |
| 71 | R3-019 | RETURNING для DML | MISSING | — | — | Нет `RETURNING` в парсере, DML возвращает только affected count |
| 72 | R3-019 | Conflict target | MISSING | — | — | Зависит от #70 |
| 73 | R3-020 | Surefire + JaCoCo + Sonar | DONE | `pom.xml` (surefire, jacoco, sonar), `HYBRID-CI-DOCUMENTATION.md` | — | Профили + JaCoCo report |
| 74 | R3-020 | Matrix JDK 17/21/25 + OS | PARTIAL | `.github/workflows/ci.yml` (matrix) | — | Matrix: JDK [21, 22, 23-ea] + OS [ubuntu-24.04, macos-13, windows-2022]; **нет JDK 17 и 25** |
| 75 | R3-020 | Release + Docker + Helm | PARTIAL | `.github/workflows/release-on-push.yml`, `publish-drivers.yml`, `k8s/dieseldb-probes.yaml` | — | Release-on-push JAR + GH Release; **нет Dockerfile, нет Helm chart** |
| 76 | R3-021 | Non-recursive CTE (WITH AS) | MISSING | — | — | Нет `WITH` в парсере |
| 77 | R3-021 | Recursive CTE (WITH RECURSIVE) | MISSING | — | — | Нет `RECURSIVE` |
| 78 | R3-021 | MATERIALIZED / NOT MATERIALIZED | MISSING | — | — | Нет хинтов |
| 79 | R3-022 | ROW_NUMBER/RANK/DENSE_RANK/NTILE | MISSING | — | — | AggregateFunctions только SUM/AVG/COUNT/MIN/MAX |
| 80 | R3-022 | LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE | MISSING | — | — | Нет navigation функций |
| 81 | R3-022 | OVER + Frame ROWS/RANGE BETWEEN | MISSING | — | — | Нет `OVER`/`BETWEEN`/WindowFunction |
| 82 | R3-023 | FULL OUTER JOIN | PARTIAL | `diesel/QueryParser.java:2805` (`JoinType.FULL_OUTER`), `SqlKeywords.java:134-135` | `JoinTest.java:286-340` | Парсер принимает `FULL [OUTER] JOIN`, **но SelectQuery не имеет FULL_OUTER executor-ветки** (no null-padding для unmatched), тесты smoke-only |
| 83 | R3-023 | LATERAL JOIN | MISSING | — | — | Нет `LATERAL` токена |
| 84 | R3-023 | CROSS APPLY / OUTER APPLY | MISSING | — | — | Нет `APPLY` |
| 85 | R3-024 | UNION + UNION ALL | MISSING | — | — | Нет `UNION` в парсере, `InQueryTest` — про IN-list |
| 86 | R3-024 | INTERSECT + EXCEPT | MISSING | — | — | Нет токенов |
| 87 | R3-024 | Precedence + скобки для set ops | MISSING | — | — | Зависит от #85/#86 |
| 88 | R3-025 | FOREIGN KEY + referential integrity | MISSING | — | — | CreateTableQuery не принимает FK |
| 89 | R3-025 | CASCADE DELETE/UPDATE/SET NULL | MISSING | — | — | Нет `CASCADE` |
| 90 | R3-025 | ADD CONSTRAINT NOT VALID + validation | MISSING | — | — | Нет `NOT VALID` / `VALIDATE CONSTRAINT` |
| 91 | R3-026 | CHECK constraint + named constraints | MISSING | — | — | Нет `CHECK (` / `CONSTRAINT` в CreateTableQuery |
| 92 | R3-026 | NOT NULL + DEFAULT expressions | MISSING | — | — | CreateTableQuery не имеет NOT NULL/DEFAULT (только sequence) |
| 93 | R3-026 | DROP CONSTRAINT + dependency | MISSING | — | — | Нет `DROP CONSTRAINT` |
| 94 | R3-027 | CREATE MATERIALIZED VIEW + REFRESH MANUAL | MISSING | — | — | Нет `MATERIALIZED VIEW`, нет View/MaterializedView class |
| 95 | R3-027 | REFRESH ON COMMIT / EVERY / CONCURRENTLY | MISSING | — | — | Нет `REFRESH` |
| 96 | R3-027 | Query rewrite (CBO uses MV) | MISSING | — | — | Зависит от #94 |
| 97 | R3-028 | BEFORE triggers | MISSING | — | — | Нет Trigger класса, нет `TRIGGER` токена |
| 98 | R3-028 | AFTER triggers | MISSING | — | — | То же |
| 99 | R3-028 | FOR EACH STATEMENT triggers | MISSING | — | — | То же |
| 100 | R3-029 | CREATE VIEW + SELECT * FROM view | MISSING | — | — | Нет `CREATE VIEW`, нет View.java |
| 101 | R3-029 | Updatable views | MISSING | — | — | Зависит от #100 |
| 102 | R3-029 | CHECK OPTION + DROP dep | MISSING | — | — | Зависит от #100 |
| 103 | R3-030 | Tokenizer + Stemmer (Snowball) | MISSING | — | — | Нет `diesel/fts/` пакета |
| 104 | R3-030 | FullTextIndex + MATCH/AGAINST | MISSING | — | — | Нет GIN/inverted index, нет `MATCH`/`AGAINST`/`@@` |
| 105 | R3-030 | BM25 + Highlighter | MISSING | — | — | Нет классов |
| 106 | R3-031 | JSONB binary JSON + path indexing | MISSING | — | — | Только JsonTypeMapper/JsonStreamParser для JSONL text storage; нет binary JSONB, нет `->`/`->>`/`@>`/`?` |
| 107 | R3-031 | UUID + ARRAY | PARTIAL | `diesel/storage/avro/AvroTypeMapper.java`, `AvroArrayHandler.java`, `SqlKeywords.java` (TYPE_UUID) | `AvroComplexTypeTest.java` | UUID-колонка mapped в Avro uuid; **нет SQL array операторов (`&&`, `@>`, `<@`, `||`), нет `gen_random_uuid()`** |
| 108 | R3-031 | ENUM + INTERVAL | PARTIAL | `diesel/storage/avro/AvroEnumHandler.java` | `AvroComplexTypeTest.java` | AvroEnumHandler для storage; **нет `CREATE TYPE ENUM`, нет INTERVAL типа/литералов/арифметики** |
| 109 | R3-031 | INET/CIDR + BIT | MISSING | — | — | Нет токенов, нет InetType/BitType |
| 110 | R3-032 | CREATE FUNCTION + RETURNS | MISSING | — | — | Нет `CREATE FUNCTION`, нет `diesel/function/` пакета |
| 111 | R3-032 | Volatility + inlining | MISSING | — | — | Зависит от #110 |
| 112 | R3-032 | TABLE-returning + named args | MISSING | — | — | Зависит от #110 |
| 113 | R3-033 | CREATE SHARDED TABLE + ShardMap | MISSING | — | — | Нет SHARDED, нет `diesel/sharding/` |
| 114 | R3-033 | Distributed query planner | MISSING | — | — | Зависит от #113 |
| 115 | R3-033 | Distributed JOIN + 2PC writes | MISSING | — | — | Зависит от #113/#116 |
| 116 | R3-034 | TwoPhaseCommitCoordinator | MISSING | — | — | Нет `diesel/tx/`, нет PREPARE TRANSACTION |
| 117 | R3-034 | CoordinatorLog | MISSING | — | — | Зависит от #116 |
| 118 | R3-034 | Timeout + failure handling | MISSING | — | — | Зависит от #116 |
| 119 | R3-035 | CREATE POLICY + ENABLE RLS | MISSING | — | — | Нет `CREATE POLICY`/`ROW LEVEL SECURITY` |
| 120 | R3-035 | Multiple policies OR/AND | MISSING | — | — | Зависит от #119 |
| 121 | R3-035 | BYPASSRLS + current_tenant() | MISSING | `diesel/security/TenantStore.java` (tenants only) | `TenantDdlTest.java` | `CREATE TENANT` есть, но `current_tenant()` функции и BYPASSRLS нет |
| 122 | R3-036 | Column-level privileges + masking | MISSING | `diesel/security/Authorizer.java` (table-level only) | — | Privilege — table-level, нет column grants, нет MaskingPolicy |
| 123 | R3-037 | AES-256-GCM page cipher + MasterKeyProvider | MISSING | — | — | Нет `diesel/security/tde/` |
| 124 | R3-037 | KMS providers + rotation | MISSING | — | — | Нет KMS/Vault/KeyRotation |
| 125 | R3-037 | Tablespace-level + WAL encryption | MISSING | `diesel/storage/tablespace/Tablespace.java` (без encryption) | `TablespaceTest.java` | Tablespace без поля encryption, WAL без encryption |
| 126 | R3-038 | Batch container + VectorizedScan | MISSING | — | — | Нет `diesel/executor/vectorized/` |
| 127 | R3-038 | VectorizedFilter + VectorizedProject | MISSING | — | — | Зависит от #126 |
| 128 | R3-038 | VectorizedAggregate + RowBatchAdapter | MISSING | — | — | Зависит от #126 |
| 129 | R3-039 | AdaptiveJoinExecutor | MISSING | — | — | Нет `diesel/optimizer/` |
| 130 | R3-039 | RuntimeStatisticsCollector | MISSING | — | — | Зависит от #129 |
| 131 | R3-039 | PlanFeedback persistent | MISSING | — | — | Зависит от #129 |
| 132 | R3-040 | ParallelScanExecutor + RangeSplitter | MISSING | — | — | Нет `diesel/executor/parallel/` |
| 133 | R3-040 | ParallelAggregationExecutor + Gather | MISSING | — | — | Зависит от #132 |
| 134 | R3-040 | Adaptive parallelism + partition-aware | MISSING | — | — | Зависит от #132/#133 |
| 135 | R3-041 | BitmapIndex + WAH | MISSING | — | — | Нет `diesel/index/bitmap/` |
| 136 | R3-041 | Bitwise ops + BitmapScanExecutor | MISSING | — | — | Зависит от #135 |
| 137 | R3-041 | CBO integration for bitmap scan | MISSING | — | — | Зависит от #135 |
| 138 | R3-042 | Covering indexes (INCLUDE) | DONE | `diesel/CoveringBTreeIndex.java`, `diesel/CreateCoveringIndexQuery.java`, `diesel/QueryParser.java` (COVERING clause) | `CoveringIndexTest.java`, `PersistenceTest.java` | SQL-keyword `COVERING(...)` вместо `INCLUDE` |
| 139 | R3-044 | TPC-C workload (10 warehouses) | MISSING | — | — | Нет `benchmarks/tpcc/` |
| 140 | R3-044 | TPC-H (22 queries, SF=1) | MISSING | — | — | Нет `benchmarks/tpch/` |
| 141 | R3-044 | Regression tracking + benchmark report | MISSING | `analytics/performance_history.csv` (data file) | — | perf_history.csv есть, нет TPC regression workflow |
| 142 | R3-045 | Web UI dashboard + schema viewer | MISSING | — | — | Только `dashboards/dieseldb.json` (Grafana), нет admin-ui |
| 143 | R3-045 | SQL editor + EXPLAIN visualizer | MISSING | — | — | Зависит от #142 |
| 144 | R3-045 | User mgmt + Backup UI + Repl monitor | MISSING | — | — | Зависит от #142 |
| 145 | Промпт 111 | Wait-die + Wound-wait | MISSING | — | — | Только detection-based (WaitForGraph/DeadlockDetector), нет prevention стратегий |
| 146 | Промпт 111 | No-wait | MISSING | — | — | Нет NoWaitPolicy |
| 147 | Промпт 111 | Config switch + benchmark | MISSING | — | — | Зависит от #145/#146 |
| 148 | Промпт 113 | ALTER TABLE ADD COLUMN | DONE | `diesel/AlterTableAddColumnQuery.java`, `diesel/QueryParser.java` (ALTER_ADD_PATTERN), `diesel/Table.java` | `OnlineAlterTest.java`, `AlterDualWriteTest.java`, `AlterProgressTest.java`, `AlterLockTest.java`, `AlterRollbackTest.java`, `CopyAlgorithmTest.java`, `InplaceTest.java`, `SchemaVersionTest.java` | ALGORITHM=COPY/INPLACE + LOCK |
| 149 | Промпт 114 | ALTER TABLE DROP COLUMN | DONE | `diesel/AlterTableDropColumnQuery.java`, `diesel/QueryParser.java` (ALTER_DROP_PATTERN), `diesel/Table.java` | `OnlineAlterTest.java`, `InplaceTest.java`, `TombstoneTest.java`, `FallbackTest.java`, `SchemaVersionTest.java` | INPLACE (default) + COPY |
| 150 | Промпт 116 | DROP INDEX | MISSING | — | — | Нет `DROP INDEX` в SqlKeywords, нет DropIndexQuery (только internal `dropIndex()` helpers) |
| 151 | Промпт 117 | TRUNCATE TABLE | MISSING | `diesel/wal/WALOpcode.java` (TRUNCATE enum value) | — | Opcode есть, но parser/class отсутствуют |
| 152 | Промпт 118 | CREATE SEQUENCE | PARTIAL | `diesel/Sequence.java` (113 LOC, AtomicLong) | — | Только inline `SEQUENCE(name start incr)` в CREATE TABLE; нет `CREATE SEQUENCE` SQL, нет SequenceManager, нет NEXTVAL/CURRVAL |
| 153 | Промпт 119 | DROP SEQUENCE | MISSING | — | — | Нет `DROP SEQUENCE` |
| 154 | Промпт 120 | Query Result Cache | PARTIAL | `diesel/QueryCache.java` (235 LOC, parsed-plan cache) | `QueryCacheTest.java` | Кэш parsed-**планов**, **не результатов**; DDL инвалидирует, DML — нет; нет TTL |
| 155 | Промпт 121 | BULK INSERT FROM file | PARTIAL | `diesel/storage/JsonlParallelLoader.java`, `diesel/Database.java` (`bulkLoadCSV`), `diesel/Table.java` (`bulkInsert`, `loadFromFile`) | `JsonlParallelLoadTest.java`, `BulkLoadEquivalenceTest.java`, `BulkInsertTest.java`, `LoadErrorHandlingTest.java` | Java API готов, **нет SQL `BULK INSERT`**; AVRO bulk partial |
| 156 | Промпт 121 | PostgreSQL COPY FROM / COPY TO | MISSING | — | — | Нет `COPY` в парсере |
| 157 | Промпт 121 | Batching + error handling + progress | PARTIAL | `diesel/storage/JsonlParallelLoader.java`, `JsonlRowStorage.java`, `StorageConfig.java` | `LoadErrorHandlingTest.java`, `JsonlParallelLoadTest.java` | Error modes (fail/skip_row/skip_value) с file:line:column; **нет `BulkProgressReporter`/`BulkBatchCommitter` классов, нет `ON ERROR=`** |
| 158 | Промпт 125 | Virtual Threads | MISSING | — | — | Нет `Thread.ofVirtual`/`newVirtualThreadPerTaskExecutor` |
| 159 | Промпт 126 | Record Patterns | MISSING | — | — | Records есть (RecoveryResult, NormalizedSql), но **нет pattern matching для switch с record deconstruction** |

---

## 4. Ключевые пробелы (что блокирует production-readiness)

### 4.1. Phase 1 — почти закрыта, 7 PARTIAL

| # | Проблема | Действие |
|---|---|---|
| 8 | O_DIRECT не реализован (только fallback на positional I/O) | JNI-обёртка или `ExtendedOpenOption.DIRECT` (JDK17+ Linux); либо удалить WARN и документировать |
| 9, 37 | Production `CatalogTable(null)` heap-only — page-backed только в test mode | Включить multi-page production catalog (KNOWN_LIMITATIONS §17) — это разблокирует §18/§19 для ALTER |
| 26 | `SavepointManager`/`NestedSavepointStack` без тестов, нет SQL `SAVEPOINT`/`ROLLBACK TO` | Добавить `SavepointManagerTest` + SQL-парсинг |
| 38 | `alterAddedColumns` memory-only на heap catalog, нет SQL `ALTER ROLLBACK` | Включить page-backed catalog (см. #9) |
| 39 | INPLACE DROP COLUMN с индексом → COPY fallback; INPLACE не в ALTER STATUS | Реализовать rebuild + зарегистрировать в `AlterStatusQuery` |
| 40 | `LogicalDump`/`LogicalRestore`/`DumpTool`/`RestoreTool` — заглушки ("placeholder dump") | Полная SQL DDL+DML генерация/парсинг + `LogicalDumpRestoreTest` |

### 4.2. Phase 2 — точечно

- **Raft (#62)** — полностью отсутствует, блокирует multi-master/failover
- **Partitioning (#63–65)** — вообще не начато (нет `PARTITION BY` токена)
- **UPSERT/RETURNING (#70–72)** — нет `ON CONFLICT` / `RETURNING` токенов
- **CI matrix (#74)** — JDK 17 и 25 отсутствуют; spec требует именно их
- **Docker/Helm (#75)** — release pipeline есть, но не упаковано в Docker image и Helm chart
- **Subquery unnesting (#69)** — execution есть, decorrelation отсутствует
- **Column statistics (#66)** — только row-count/avg-row-size, нет NDV/MCV/histogram

### 4.3. Phase 3 — практически не начиналась (3 DONE из 59)

Готово:
- **#138 Covering indexes (INCLUDE)** через `COVERING(...)` синтаксис
- **#148, #149 ALTER ADD/DROP COLUMN** (реализованы как часть R3-010)

PARTIAL (близко к готовности, можно добить):
- **#107, #108** — UUID/ARRAY/ENUM на уровне Avro storage, не хватает SQL-операторов и `CREATE TYPE`
- **#152** — Sequence-класс есть, нужен standalone `CREATE SEQUENCE` + `NEXTVAL`/`CURRVAL`
- **#154** — `QueryCache` кэширует планы, нужно расширить до result cache с TTL
- **#155, #157** — Java bulk-load API готов, нужен SQL `BULK INSERT` / `COPY` wrapper

Полностью отсутствуют кластеры:
- **CTE и Window functions (#76–81)** — нет ни одного токена
- **LATERAL/APPLY, UNION/INTERSECT/EXCEPT (#83–87)** — нет токенов
- **FK/CHECK/NOT NULL/DEFAULT (#88–93)** — `CreateTableQuery` не принимает этих опций
- **Materialized Views, Triggers, VIEW (#94–102)** — нет инфраструктуры
- **Full-text search (#103–105)** — нет `diesel/fts/` пакета
- **JSONB/INET/BIT (#106, 109)** — нет бинарного JSON, нет сетевых типов
- **Stored functions (#110–112)** — нет `CREATE FUNCTION`
- **Sharding + 2PC (#113–118)** — нет sharding и нет coordinator
- **RLS + column-level privileges (#119–122)** — нет RLS (tenants есть, RLS нет)
- **TDE at rest (#123–125)** — нет шифрования страниц/WAL
- **Vectorized execution + Adaptive joins + Parallel query (#126–134)** — нет executor packages
- **Bitmap indexes (#135–137)** — нет пакета
- **TPC-C/TPC-H (#139–141)** — нет benchmark suite
- **Web admin UI (#142–144)** — нет frontend, только Grafana dashboard
- **Deadlock prevention strategies (#145–147)** — только detection, нет wait-die/wound-wait
- **DROP INDEX, TRUNCATE, DROP SEQUENCE (#150, 151, 153)** — нет SQL-токенов
- **COPY FROM/TO (#156)** — нет
- **Virtual Threads, Record Patterns (#158, 159)** — не используются

### 4.4. Регрессионный риск

`SelectQuery.java` не делает branching по `JoinType.LEFT_OUTER`/`RIGHT_OUTER`/`FULL_OUTER` — все они тихо выполняются как INNER JOIN. `JoinTest` только проверяет `assertDoesNotThrow`, маскируя баг. **Промпт #82 (FULL OUTER JOIN) на самом деле исполнительски не готов**, хотя парсер принимает SQL.

---

## 5. Рекомендации по приоритетам

### Sprint A (1–4 недели) — закрыть PARTIAL в Phase 1
1. Реализовать `SAVEPOINT` SQL + тесты (#26).
2. Заменить stub `LogicalDump`/`LogicalRestore` на настоящую SQL-генерацию (#40).
3. Включить page-backed production catalog — разблокирует #9, #37, #38.
4. Реализовать INPLACE DROP COLUMN с индексом (#39).
5. Backfill `PageScrubberTest` (#53).

### Sprint B (5–8 недель) — закрыть high-value пробелы Phase 2
1. `ON CONFLICT DO UPDATE/NOTHING` + `RETURNING` (#70, #71, #72) — часто используемая SQL-фича.
2. Fix executor-ветки для LEFT/RIGHT/FULL OUTER JOIN (#82) + соответствующие тесты с null-padding.
3. `DROP INDEX` (#150), `TRUNCATE TABLE` (#151), `CREATE/DROP SEQUENCE` standalone (#152, #153).
4. SQL `BULK INSERT` / `COPY FROM/TO` wrapper вокруг существующего Java API (#155, #156, #157).
5. JDK 17/25 в ci.yml matrix (#74), Dockerfile + Helm chart (#75).

### Sprint C (9–16 недель) — Column statistics + CBO
1. Column-level NDV/MCV/histogram в `AnalyzeTableQuery` (#66).
2. Access-path selection (scan vs index) в QueryOptimizer (#67).
3. Subquery unnesting/decorrelation (#69).

### Sprint D (16+ недель) — Phase 3 SQL features
По приоритету: CTE (#76–78) → Window functions (#79–81) → UNION/INTERSECT/EXCEPT (#85–87) → FK + CASCADE (#88–90) → CHECK/NOT NULL/DEFAULT (#91–93) → VIEW (#100–102) → JSONB (#106) → Materialized Views (#94–96) → Triggers (#97–99) → Stored functions (#110–112).

### Sprint E (long-term) — Distributed + security
Raft (#62), Partitioning (#63–65), Sharding (#113–115), 2PC (#116–118), RLS (#119–121), TDE (#123–125), Vectorized execution (#126–128), Parallel query (#132–134), Bitmap indexes (#135–137), Full-text search (#103–105), TPC-C/TPC-H (#139–141), Web admin UI (#142–144).

---

## 6. Источники проверки

- `analytics/prompt4.md` (6864 строки) — список 159 промптов
- `KNOWN_LIMITATIONS.md` (575 строк, 20 секций) — документирует ограничения для Phase 1
- `PROMPT_STATUS.md` (75 строк) — трекинг последних изменений (CI7-P3/P4/P5/P7)
- Исходный код `diesel/` (~1.2k файлов .java)
- Тесты `src/test/java/diesel/` (~250 файлов)
- CI `.github/workflows/` + `pom.xml`
