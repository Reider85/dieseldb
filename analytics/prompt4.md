# prompt4.md — DieselDB Production-Readiness Implementation Prompts

> **Документ:** prompt4.md
> **Версия:** 1.0
> **Дата:** 2026-09-22
> **Цель:** доведение DieselDB до production-ready (Phase 1 → Phase 3 из `ROADMAP3.md`).
> **Источники:**
> - `analytics/ROADMAP3.md` — R3-001..R3-045 (45 карточек, полный production-readiness план).
> - `analytics/prompt3.md` — доборочные промпты начиная с #97, не дублирующие ROADMAP3.
> **Нумерация:** только цифры, 1..56. Сквозная, без префикса «Промпт».
> **Принцип составления:** каждый промпт самодостаточен (проблема → задача → файлы → DoD), может быть передан агенту-исполнителю без дополнительного контекста.

---

## 0. Карта промптов и стратегия выполнения

| # | ID ROADMAP3 | Заголовок | Фаза | Приоритет |
|---|-------------|-----------|------|-----------|
| 1 | R3-001 | MVCC через версионность строк | 1 | CRITICAL |
| 2 | R3-002 | Page-based storage + buffer pool (LRU) | 1 | CRITICAL |
| 3 | R3-003 | WAL + group commit | 1 | CRITICAL |
| 4 | R3-004 | ARIES Recovery Manager | 1 | CRITICAL |
| 5 | R3-005 | Background writer / flusher | 1 | HIGH |
| 6 | R3-006 | Savepoint + Deadlock + Lock timeout | 1 | HIGH |
| 7 | R3-007 | Замена Java Object Serialization в net-протоколе | 1 | CRITICAL |
| 8 | R3-008 | RBAC + Audit log | 1 | HIGH |
| 9 | R3-009 | SSL/TLS transport | 1 | HIGH |
| 10 | R3-010 | Online schema changes (ALTER без блокировки) | 1 | HIGH |
| 11 | R3-011 | Backup / Restore (logical + physical) | 1 | HIGH |
| 12 | R3-012 | Metrics + Prometheus + Health check | 1 | HIGH |
| 13 | R3-013 | Fix blocking bugs из problems.md | 1 | CRITICAL |
| 14 | R3-014 | Checkpoint + Checksummed pages (CRC32C) | 2 | HIGH |
| 15 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers | 2 | HIGH |
| 16 | R3-016 | Replication (logical + physical + quorum/Raft) | 2 | HIGH |
| 17 | R3-017 | Partitioning (range/list/hash) | 2 | MEDIUM |
| 18 | R3-018 | Cost-Based Optimizer + статистика | 2 | HIGH |
| 19 | R3-019 | UPSERT / RETURNING / ON CONFLICT | 2 | HIGH |
| 20 | R3-020 | CI/CD v2 — coverage, matrix, releases | 2 | HIGH |
| 21 | R3-021 | CTE + рекурсивные CTE | 3 | MEDIUM |
| 22 | R3-022 | Оконные функции | 3 | MEDIUM |
| 23 | R3-023 | FULL OUTER JOIN, LATERAL, CROSS APPLY | 3 | MEDIUM |
| 24 | R3-024 | UNION / INTERSECT / EXCEPT | 3 | MEDIUM |
| 25 | R3-025 | Foreign Keys с CASCADE | 3 | MEDIUM |
| 26 | R3-026 | CHECK constraints, NOT NULL, DEFAULT | 3 | MEDIUM |
| 27 | R3-027 | Materialized Views + refresh | 3 | LOW |
| 28 | R3-028 | Triggers BEFORE/AFTER | 3 | LOW |
| 29 | R3-029 | VIEW (non-materialized) | 3 | LOW |
| 30 | R3-030 | Full-text search | 3 | LOW |
| 31 | R3-031 | Расширенные типы (JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT) | 3 | MEDIUM |
| 32 | R3-032 | Хранимые функции (SQL-only) | 3 | LOW |
| 33 | R3-033 | Шардинг + distributed query planner | 3 | MEDIUM |
| 34 | R3-034 | 2PC distributed transactions | 3 | LOW |
| 35 | R3-035 | Row-Level Security (RLS) | 3 | MEDIUM |
| 36 | R3-036 | Column-level privileges | 3 | LOW |
| 37 | R3-037 | TDE at rest encryption | 3 | MEDIUM |
| 38 | R3-038 | Vectorized execution (batch) | 3 | MEDIUM |
| 39 | R3-039 | Adaptive joins (runtime switching) | 3 | LOW |
| 40 | R3-040 | Parallel query scan + aggregation | 3 | MEDIUM |
| 41 | R3-041 | Bitmap indexes | 3 | LOW |
| 42 | R3-042 | Covering indexes (INCLUDE) | 3 | LOW |
| 43 | R3-043 | Parquet storage (нативный) | 3 | MEDIUM |
| 44 | R3-044 | TPC-C / TPC-H сертификация | 3 | LOW |
| 45 | R3-045 | GUI админ-панель (аналог pgAdmin) | 3 | LOW |
| 46 | (Промпт 111) | Lock — deadlock prevention стратегии | — | MEDIUM |
| 47 | (Промпт 113) | ALTER TABLE ADD COLUMN — базовый SQL | — | HIGH |
| 48 | (Промпт 114) | ALTER TABLE DROP COLUMN — базовый SQL | — | MEDIUM |
| 49 | (Промпт 116) | DROP INDEX | — | MEDIUM |
| 50 | (Промпт 117) | TRUNCATE TABLE | — | HIGH |
| 51 | (Промпт 118) | CREATE SEQUENCE | — | MEDIUM |
| 52 | (Промпт 119) | DROP SEQUENCE | — | LOW |
| 53 | (Промпт 120) | Query Result Cache | — | MEDIUM |
| 54 | (Промпт 121) | Bulk Insert / Copy API | — | HIGH |
| 55 | (Промпт 125) | Virtual Threads для concurrency | — | LOW |
| 56 | (Промпт 126) | Record Patterns для чистоты кода | — | LOW |

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

**Промпты 47 (ALTER ADD) и 48 (ALTER DROP)** оставлены отдельно, т.к. R3-010
фокусируется на **online-семантике** (ALGORITHM=INPLACE/COPY, LOCK=NONE), а
базовый SQL-парсинг и метаданные — это необходимая pré-задача, не дубль.

---

## Фаза 1 — Critical Production-Readiness (3-6 месяцев)

**Цель:** закрыть 13 CRITICAL/HIGH дыр, без которых DieselDB **нельзя** назвать production-ready даже для single-tenant low-stakes применений. По завершении — метрика готовности ≥ 55 %.

### 1. MVCC через версионность строк (R3-001)

**Категория:** B. Concurrency
**Приоритет:** CRITICAL
**Фаза:** 1
**Зависимости:** — (фундамент)
**Связь с prompt3.md:** новое, не покрыто. В `Roadmap.md` упомянуто как «уже есть», но фактически уровни изоляции реализованы через глубокое клонирование таблиц сериализацией — это не MVCC. `problems.md` §Транзакции-1 описывает проблему.

**Проблема.**
Текущий `Transaction.cloneTable()` использует `ByteArrayOutputStream` + `ObjectOutputStream` для глубокого клонирования таблиц при `BEGIN TRANSACTION` и `COMMIT`. На таблице 100k строк × 5 индексов это секунды на каждое BEGIN. Нет версионности строк, нет tuple visibility check, нет undo log для read-side snapshot. Десятки параллельных транзакций невозможны физически.

**Задача.**
1. Ввести `xmin` (txid, создавший версию) и `xmax` (txid, удаливший версию) на каждую строку.
2. Snapshot isolation через `TupleVisibility.visible(row, snapshotTxid)`, а не клонирование.
3. Undo log для отката незакоммиченных транзакций (в памяти + spill при нехватке).
4. Vacuum (background thread) для очистки мёртвых версий после `xmin < oldestActiveTx`.
5. Переработать `Transaction.java`: заменить `cloneTable()` на `getSnapshot(table)` = view через visibility check.
6. Адаптировать `SelectQuery` / `DeleteQuery` / `UpdateQuery` / `InsertQuery` для работы с версиями.

**Ключевые файлы.**
- `diesel/Row.java` (новый контейнер `xmin`/`xmax`/`commandId`)
- `diesel/TupleVisibility.java` (новый, для каждого уровня изоляции своя логика)
- `diesel/TransactionTableSnapshot.java` (новый, замена клонирования)
- `diesel/Transaction.java` (переработка)
- `diesel/VacuumManager.java` (новый)
- `diesel/UndoLog.java` (новый)

**Критерии приёмки.**
- [ ] `BEGIN TRANSACTION` на таблице 100k строк < 1 ms (сейчас секунды).
- [ ] 50 параллельных писателей держат throughput > 1000 inserts/sec (не деградирует квадратично).
- [ ] READ COMMITTED видит данные, закоммиченные до начала оператора; REPEATABLE_READ — до начала транзакции; SERIALIZABLE — плюс конфликт-детекция на запись.
- [ ] Тест `ConcurrentConflictTest` расширен до 100 параллельных транзакций, не падает.
- [ ] Тест на chain of 1000 transactions (вложенные BEGIN/COMMIT), heap не растёт.

---

### 2. Page-based storage + buffer pool (LRU) (R3-002)

**Категория:** C. Storage engine
**Приоритет:** CRITICAL
**Фаза:** 1
**Зависимости:** — (фундамент, концептуально не блокирует R3-001; можно делать параллельно)
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `problems.md` (Map-per-row ~48 байт на entry).

**Проблема.**
Текущее хранилище — `List<Map<String,Object>>` в куче JVM. На 1M строк × 20 колонок это ~2 GB heap (5-10× overhead от автобоксинга). Нет понятия страницы, нет buffer pool, нет eviction: данные либо в памяти, либо на диске через CSV/JSONL/AVRO. Тяжёлые таблицы физически не помещаются.

**Задача.**
1. Ввести `Page` (фиксированный размер, 8 KB / 16 KB / 64 KB — настраивается).
2. `BufferPool` с LRU eviction и pinned pages (для системных каталогов).
3. `PageManager`: чтение/запись страниц через `FileChannel` с прямым I/O (опционально `O_DIRECT` на Linux через JNI или `FileChannel.map`).
4. Конвертация существующих storage (CSV/TSV/JSONL/AVRO) в page-based: их оставить как формат экспорта/импорта, но не как primary storage.
5. Catalog: `system_tables` хранит схему, индексы, sequence state — в страницах.
6. Заглушка для будущего `Tablespace` (пока один tablespace = один каталог).

**Ключевые файлы.**
- `diesel/storage/page/Page.java` (новый)
- `diesel/storage/page/BufferPool.java` (новый, LRU)
- `diesel/storage/page/PageManager.java` (новый)
- `diesel/storage/page/PageId.java` (новый)
- `diesel/storage/page/PinnedPage.java` (новый, AutoCloseable для pin/unpin)
- `diesel/storage/page/CatalogTable.java` (новый)

**Критерии приёмки.**
- [ ] Таблица 10M строк помещается в heap 512 MB (сейчас физически невозможно).
- [ ] Buffer pool hit rate > 95 % на workload `SELECT by PK` 1M запросов.
- [ ] LRU eviction корректно вытесняет холодные страницы под давлением.
- [ ] `BufferPoolMXBean` экспонирует hit/miss/pinned в JMX.
- [ ] Тест `BufferPoolStressTest` — 10 потоков × 100k операций, не падает.

---

### 3. WAL + group commit (R3-003)

**Категория:** A. Durability & Recovery
**Приоритет:** CRITICAL
**Фаза:** 1
**Зависимости:** —
**Связь с prompt3.md:** уточняет Промпт 97 (WAL базовая реализация). Промпт 97 не покрывает group commit — без этого latency коммита недопустимая на проде. Также поглощает Промпты 108 (segment rotation) и 109 (async write / group commit).

**Проблема.**
Сейчас на COMMIT происходит `saveTablesToDisk()` — сериализация всех таблиц полностью. Для таблицы 100k строк это секунды. Fsync на каждый commit убьёт throughput (десятки commits/sec максимум). Аварийное отключение теряет все изменения с последнего `saveTablesToDisk()`. Group commit не упоминается ни в одном документе проекта.

**Задача.**
1. Реализовать WAL как сегментированный бинарный лог (`wal-0001.log`, `wal-0002.log`, ...), каждый entry — LSN + txid + operation + before/after image + CRC32C.
2. На COMMIT: добавить commit record в WAL, **один** fsync для группы транзакций (group commit, окно 5-10 ms или 64 транзакций — что раньше).
3. Concurrent writers в WAL — single-writer thread + queue (`WALWriter`), readers — все.
4. WAL segment rotation: при достижении 64 MB или `wal.segment.max.age.ms` = 5 min.
5. Архивация старых сегментов (gzip) — для backup и replication.
6. Config: `wal.fsync.policy = always | group | everysec | none` (default `group`).
7. Backward compat: текущая сериализация `.table` остаётся для cold backup, но не для durability.

**Ключевые файлы.**
- `diesel/wal/WALManager.java` (новый)
- `diesel/wal/WALEntry.java` (новый)
- `diesel/wal/WALWriter.java` (новый, single-writer)
- `diesel/wal/WALSegment.java` (новый)
- `diesel/wal/WALArchiver.java` (новый)
- `diesel/wal/GroupCommitCoordinator.java` (новый)
- `diesel/wal/AsyncWALWriter.java` (новый, async-обёртка над WALWriter)
- `diesel/wal/WALConfig.java` (новый)
- `diesel/Transaction.java` (изменение пути COMMIT)

**Критерии приёмки.**
- [ ] Throughput COMMIT > 5000/sec на 4-core VM (сейчас < 50/sec на 100k-строчной таблице).
- [ ] p99 commit latency < 20 ms в режиме `group`, < 1 ms в режиме `none` (для dev).
- [ ] После `kill -9` процесса и рестарта — все закоммиченные транзакции на месте, ни одной потери.
- [ ] Незакоммиченная транзакция полностью откатывается (см. промпт 4).
- [ ] Тест `WALCrashRecoveryTest`: 10k коммитов, kill, restart, проверка целостности.
- [ ] Сегменты ротируются по размеру и по возрасту (оба условия покрыты тестами).

---

### 4. ARIES Recovery Manager (R3-004)

**Категория:** A. Durability & Recovery
**Приоритет:** CRITICAL
**Фаза:** 1
**Зависимости:** промпт 3 (WAL)
**Связь с prompt3.md:** уточняет Промпт 99 (ARIES Recovery Manager) и поглощает Промпт 98 (WAL recovery). В `prompt3.md` описан алгоритм, но не детализованы integration-точки с MVCC и page storage.

**Проблема.**
Без recovery WAL бесполезен: после краша нужно replay журнала. Промпт 99 даёт концепцию, но integration с MVCC (промпт 1) и page storage (промпт 2) не описан — это и есть пробел ROADMAP3.

**Задача.**
1. Analysis phase: чтение WAL с последнего checkpoint, построение списка активных транзакций.
2. Redo phase: повтор всех операций с LSN > lastCheckpointLSN.
3. Undo phase: откат транзакций без commit record.
4. Интеграция с MVCC: undo повторяет создание/удаление версий, а не физические байты.
5. Интеграция с page storage: redo восстанавливает страницы через `PageManager.applyRedo()`.
6. Checkpoint pointer file (`checkpoint.ptr`) — атомарная запись через `AtomicFileWriter`.
7. На старте сервера: `RecoveryManager.recover()` выполняется до принятия клиентских соединений.

**Ключевые файлы.**
- `diesel/recovery/RecoveryManager.java` (новый)
- `diesel/recovery/AnalysisPhase.java` (новый)
- `diesel/recovery/RedoPhase.java` (новый)
- `diesel/recovery/UndoPhase.java` (новый)
- `diesel/recovery/CheckpointRecord.java` (новый)
- `diesel/recovery/ARIESAlgorithm.java` (новый, оркестрация)
- `diesel/DatabaseServer.java` (изменение startup sequence)

**Критерии приёмки.**
- [ ] Recovery после краша на WAL 1 GB — < 30 секунд.
- [ ] Тест `RecoveryIntegrationTest`: 100 смешанных транзакций, 50 commit / 50 no-commit, kill, restart → 50 видны, 50 откачены.
- [ ] Тест `RecoveryWithLongTransactionTest`: 1 длинная транзакция на 1M insert + kill посередине → все её изменения откачены.
- [ ] Метрика `recovery.duration.ms` экспортируется в metrics endpoint.

---

### 5. Background writer / flusher (R3-005)

**Категория:** A. Durability & Recovery
**Приоритет:** HIGH
**Фаза:** 1
**Зависимости:** промпт 2 (page storage), промпт 3 (WAL)
**Связь с prompt3.md:** новое, не покрыто. Упоминается в `resilence.md`, но **не попал** в `prompt3.md` и `Roadmap.md`. Поглощает Промпт 100 (Checkpoint Manager) и его базовую часть.

**Проблема.**
Если dirty pages пишутся на диск только на checkpoint, восстановление после краша будет долгим (много WAL нужно replay). Нужен фоновый writer, который мягко флашит dirty pages в фоне, не блокируя writers.

**Задача.**
1. Background thread `BufferPoolFlusher`: каждые N ms сканирует buffer pool на dirty pages.
2. Адаптивная стратегия: если dirty pages > 25 % буфера — flush агрессивнее; если < 5 % — реже.
3. Запись dirty page: атомарно (temp file + rename), через `PageManager.writePage()`.
4. Не флашить страницы, чей LSN > lastWALFlushLSN (иначе нарушение WAL rule).
5. Checkpoint (каждые 5 min или N MB WAL): sync flush всех dirty + запись checkpoint record в WAL.
6. Fuzzy checkpoint: не ждать quiescent state, фиксировать согласованное состояние с активными транзакциями.

**Ключевые файлы.**
- `diesel/storage/page/BufferPoolFlusher.java` (новый)
- `diesel/storage/page/CheckpointManager.java` (новый, интегрирован с WAL)
- `diesel/recovery/FuzzyCheckpoint.java` (новый)
- `diesel/ConfigLoader.java` (новые ключи: `bufferpool.flush.interval.ms`, `bufferpool.dirty.threshold`, `checkpoint.interval.ms`)

**Критерии приёмки.**
- [ ] Recovery после краша при active workload — < 10 секунд (мало WAL нужно replay).
- [ ] Throughput writers не падает больше чем на 10 % при включённом flusher.
- [ ] Тест `BackgroundFlusherTest`: 100k inserts без COMMIT, kill, restart → все inserts либо закоммичены (если были commit records), либо откачены.
- [ ] Fuzzy checkpoint не блокирует writers > 50 ms на 1M-строчной базе.

---

### 6. Savepoint + Deadlock detector + Lock timeout (R3-006)

**Категория:** B. Concurrency & Locking
**Приоритет:** HIGH
**Фаза:** 1
**Зависимости:** промпт 1 (MVCC), промпт 3 (WAL для savepoint records)
**Связь с prompt3.md:** уточняет Промпты 104 (Deadlock), 105 (Lock timeout), 106-107 (Savepoint). Поглощает их, поскольку без интеграции с MVCC они не имеют смысла.

**Проблема.**
С появлением MVCC (промпт 1) нужны: deadlock detection для wait-for graph, lock timeout (иначе вечное ожидание), savepoints (частичный откат транзакции). В коде нет ни одного из этих классов.

**Задача.**
1. `LockManager`: гранулярные блокировки (table-level / row-level), совместимость по матрице режимов (S/X/IS/IX).
2. `WaitForGraph`: периодическая проверка (каждые 500 ms) на циклы; при обнаружении — жертва = транзакция с наименьшим txid.
3. `LockTimeout`: настраиваемый `lock.timeout.ms` (default 30000); по истечении — `LockTimeoutException`.
4. `SavepointManager`: `SAVEPOINT name`, `ROLLBACK TO name`, `RELEASE name` — частичный undo через undo log (промпт 1).
5. Поддержка вложенных savepoint (иерархия + очистка вложенных при release родителя).
6. WAL records для savepoint create/release.

**Ключевые файлы.**
- `diesel/concurrency/LockManager.java` (новый)
- `diesel/concurrency/Lock.java` (новый)
- `diesel/concurrency/WaitForGraph.java` (новый)
- `diesel/concurrency/DeadlockDetector.java` (новый)
- `diesel/concurrency/LockTimeoutManager.java` (новый)
- `diesel/concurrency/LockTimeoutException.java` (новый)
- `diesel/concurrency/SavepointManager.java` (новый)
- `diesel/concurrency/Savepoint.java` (новый)
- `diesel/concurrency/NestedSavepointStack.java` (новый)
- `diesel/QueryParser.java` (парсинг SAVEPOINT / ROLLBACK TO)

**Критерии приёмки.**
- [ ] Тест `DeadlockTest`: 2 транзакции в цикле → одна убита `DeadlockVictimException`.
- [ ] Тест `LockTimeoutTest`: транзакция ждёт > `lock.timeout.ms` → `LockTimeoutException`.
- [ ] Тест `SavepointTest`: SAVEPOINT + частичный ROLLBACK TO восстанавливает состояние до savepoint, не теряя изменения после savepoint.
- [ ] Тест `NestedSavepointTest`: 5 уровней вложенности, ROLLBACK TO уровня 3 сохраняет уровни 1-2, очищает 4-5.
- [ ] Метрики `deadlock.count`, `lock.timeout.count`, `savepoint.count` экспортируются.

---

### 7. Замена Java Object Serialization в сетевом протоколе (R3-007)

**Категория:** F. Security + L. Serialization
**Приоритет:** CRITICAL
**Фаза:** 1
**Зависимости:** — (можно делать параллельно с промптами 1/2/3)
**Связь с prompt3.md:** новое, не покрыто. `problems.md` §Сериализация-6 указывает проблему, но ни в одном плане её нет. Это **RCE-риск** — Java deserialization gadgets.

**Проблема.**
`DatabaseClient` / `DatabaseServer` общаются через `ObjectInputStream.readObject()` — это известная RCE-уязвимость (deserialization gadgets: commons-collections, spring, etc.). Любой клиент может послать сериализованный payload и получить RCE на сервере. Для production это **blocker**.

**Задача.**
1. Спроектировать бинарный wire-протокол (TLS-like handshake → length-prefixed messages → CRC32 footer).
2. Каждый message тип — явный класс `XxxMessage` с методами `writeTo(DataOutput)` / `readFrom(DataInput)`.
3. Никакого `ObjectInputStream` — только явные поля.
4. Опционально: сжатие сообщений > 4 KB через ZSTD (использует существующий `CompressionCodec`).
5. Совместимость: новый протокол version 2; старые клиенты подключаются к порту legacy (deprecation warning); удаление legacy протокола — Фаза 2.
6. Раздельные message types: `QueryMessage`, `QueryResultMessage`, `PrepareMessage`, `ExecutePreparedMessage`, `BatchQueryMessage`, `HealthCheckMessage`, `CompressionHandshakeMessage`.

**Ключевые файлы.**
- `diesel/net/WireProtocol.java` (новый, константы и version)
- `diesel/net/MessageCodec.java` (новый, enc/dec)
- `diesel/net/messages/*Message.java` (переработка существующих)
- `diesel/DatabaseServer.java` (изменение ClientHandler)
- `diesel/DatabaseClient.java` (изменение send/receive)

**Критерии приёмки.**
- [ ] Тест `WireProtocolSecurityTest`: попытка послать сериализованный payload → connection closed with `InvalidMessageException`.
- [ ] Backward compat: legacy клиенты на legacy порту работают с warning.
- [ ] Throughput не падает (или растёт: ZSTD сжатие результатов > 4 KB даёт выигрыш).
- [ ] Сообщения > 4 KB сжимаются автоматически.

---

### 8. RBAC + Audit log (R3-008)

**Категория:** F. Security
**Приоритет:** HIGH
**Фаза:** 1
**Зависимости:** — (промпт 7 для transport; промпт 1 для session-scoped role)
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 5 упоминается RBAC, но без деталей и слишком поздно (6-12 мес). Для production нужно раньше.

**Проблема.**
Сейчас сервер принимает любое соединение без аутентификации. Любой сетевой клиент может выполнить любой SQL. Audit отсутствует — нельзя понять, кто и что делал.

**Задача.**
1. `User` + `Role` + `Privilege` (SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/ADMIN на таблицу / базу).
2. Хранилище учёток: таблица `diesel_users` (с salted hash пароля, PBKDF2 / Argon2), `diesel_roles`, `diesel_user_roles`, `diesel_privileges`.
3. Аутентификация на handshake: `CREATE USER`, `GRANT`, `REVOKE` SQL-команды.
4. Session: `Connection.getUserId()`, проверка прав перед выполнением каждого запроса.
5. Audit log: таблица `diesel_audit_log` (event_time, user, source_ip, query, status, duration_ms). Включается через `audit.enabled = true`.
6. Default: после первого запуска создаётся `admin` / `admin` с принудительной сменой пароля.

**Ключевые файлы.**
- `diesel/security/User.java`, `Role.java`, `Privilege.java` (новые)
- `diesel/security/UserManager.java` (новый)
- `diesel/security/Authenticator.java` (новый, PBKDF2)
- `diesel/security/Authorizer.java` (новый, проверка прав)
- `diesel/security/AuditLogger.java` (новый)
- `diesel/security/PasswordHasher.java` (новый, Argon2 / PBKDF2)
- `diesel/QueryParser.java` (парсинг CREATE USER / GRANT / REVOKE)

**Критерии приёмки.**
- [ ] Тест `RbacTest`: пользователь без `SELECT` на `users` → `AccessDeniedException`.
- [ ] Тест `AuditTest`: 100 запросов → 100 записей в audit log с корректным user, ip, query.
- [ ] Пароли не хранятся в открытом виде (только hash + salt).
- [ ] Default admin при первом старте + принудительная смена пароля.
- [ ] CLI флаг `--reset-admin` для локального сброса (только с filesystem access).

---

### 9. SSL/TLS transport (R3-009)

**Категория:** F. Security
**Приоритет:** HIGH
**Фаза:** 1
**Зависимости:** промпт 7 (новый wire protocol)
**Связь с prompt3.md:** новое, частично в `Roadmap.md` Этап 5. Нужно раньше — без TLS нельзя пускать трафик через сеть.

**Проблема.**
Даже с новым wire-протоколом (промпт 7) данные идут в открытом виде. SQL-запросы, результаты, пароли на handshake — всё читаемо в MITM.

**Задача.**
1. Опциональный TLS handshake после TCP accept.
2. Конфиг: `tls.enabled = false | true`, `tls.keystore.path`, `tls.keystore.password`, `tls.truststore.path` (для client cert).
3. mutual TLS (mTLS) для клиентских сертификатов (опционально, `tls.client.auth = none | request | require`).
4. Cipher suites whitelist: TLS 1.3, TLS 1.2 с современными шифрами; legacy отключены.
5. CLI клиент: флаги `--tls`, `--tls-ca`, `--tls-cert`, `--tls-key`.
6. SNI support для multi-tenant развертываний.

**Ключевые файлы.**
- `diesel/net/TlsHandshakeHandler.java` (новый)
- `diesel/net/SslContextFactory.java` (новый)
- `diesel/ConfigLoader.java` (новые ключи)
- `diesel/DatabaseServer.java` (accept loop с TLS branch)
- `diesel/DatabaseClient.java` (connect с TLS)
- `diesel/CliRepl.java` (CLI flags)

**Критерии приёмки.**
- [ ] Тест `TlsConnectionTest`: клиент с невалидным сертификатом при `tls.client.auth=require` → connection refused.
- [ ] Cipher suite audit: только TLS 1.3 и TLS 1.2 с PFS cipher suites.
- [ ] Testssl.sh / sslyze скан — классы A+ по SSL Labs.
- [ ] mTLS: 2-way auth работает.

---

### 10. Online schema changes (ALTER без блокировки) (R3-010)

**Категория:** C. Storage engine + G. SQL coverage
**Приоритет:** HIGH
**Фаза:** 1
**Зависимости:** промпт 2 (page storage — для in-place schema), промпт 7 (для coordinated schema version broadcast)
**Связь с prompt3.md:** уточняет Промпты 113-114 (ALTER TABLE ADD/DROP COLUMN). Промпты 113-114 описывают SQL-парсинг, R3-010 добавляет online-семантику. Базовый SQL остаётся в доборочных промптах 47-48.

**Проблема.**
Production БД должна менять схему без даунтайма. Текущий `ALTER TABLE` (когда будет реализован в Промптах 113-114) заблокирует таблицу на время операции.

**Задача.**
1. ALGORITHM=copy: новая версия таблицы создаётся в фоне, writers дублируются в старую и новую, по окончании — atomic rename.
2. ALGORITHM=inplace: где возможно (ADD COLUMN nullable, DROP COLUMN через tombstone), без копирования.
3. LOCK=NONE / SHARED / EXCLUSIVE — уровень блокировки во время ALTER.
4. Schema version в catalog: readers видят consistent snapshot своей версии schema.
5. Не блокирует readers (на промпте 1 MVCC + на schema version).
6. SQL syntax: `ALTER TABLE ... ALGORITHM=INPLACE, LOCK=NONE`.

**Ключевые файлы.**
- `diesel/schema/SchemaVersion.java` (новый)
- `diesel/schema/OnlineAlterTable.java` (новый)
- `diesel/schema/AlterTableCopyAlgorithm.java` (новый)
- `diesel/schema/AlterTableInplaceAlgorithm.java` (новый)
- `diesel/QueryParser.java` (расширение ALTER синтаксиса)
- `diesel/Table.java` (поддержка нескольких schema versions)

**Критерии приёмки.**
- [ ] `ALTER TABLE ADD COLUMN nullable_col` на таблице 10M строк — без блокировки writers > 100 ms.
- [ ] Во время ALTER: 100 параллельных SELECT видят consistent схему (либо старую, либо новую).
- [ ] Тест `OnlineAlterTest`: 1 writer thread + 1 alter + 10 reader threads, все завершаются успешно.
- [ ] `ALTER TABLE ADD COLUMN NOT NULL default X` использует copy-алгоритм (не блокирует readers).
- [ ] Timeout для old schema version readers; после N минут — killing oldest.

---

### 11. Backup / Restore (logical + physical) (R3-011)

**Категория:** J. Observability & operations
**Приоритет:** HIGH
**Фаза:** 1
**Зависимости:** промпт 3 (WAL — для consistent snapshot), промпт 4 (Recovery — для restore)
**Связь с prompt3.md:** новое. В `Roadmap.md` Этап 4 упоминается «аналог pg_dump/pg_restore», но без деталей. Поглощает Промпт 110 (PITR).

**Проблема.**
Без бэкапов production невозможен. Сейчас только ручное копирование `.csv` / `.table` файлов — это не consistent snapshot (нет точки во времени, нет гарантии целостности).

**Задача.**
1. `diesel_dump` (CLI): logical backup — SQL dump (`CREATE TABLE` + `INSERT INTO`), опционально с `--format=sql|csv|jsonl|avro|parquet`.
2. `diesel_restore` (CLI): restore из dump, с `--on-conflict=skip|replace|error`.
3. Physical backup: `diesel_backup` берёт consistent snapshot через WAL checkpoint + копирование pages + tail WAL.
4. Incremental backup: только изменившиеся pages с последнего full backup (через page LSN).
5. Point-in-time recovery: restore full + replay WAL до указанного timestamp.
6. Schedule: `diesel_backup_cron` (cron-like syntax в config).

**Ключевые файлы.**
- `diesel/backup/LogicalDump.java` (новый)
- `diesel/backup/LogicalRestore.java` (новый)
- `diesel/backup/PhysicalBackup.java` (новый)
- `diesel/backup/IncrementalBackup.java` (новый)
- `diesel/backup/PitrManager.java` (новый)
- `diesel/backup/BackupScheduler.java` (новый)
- `diesel/recovery/PointInTimeRecovery.java` (новый)
- `diesel/CliRepl.java` (новые команды `BACKUP`, `RESTORE`)

**Критерии приёмки.**
- [ ] `diesel_dump` на 1M-строчной базе — < 30 с, файл < 100 MB.
- [ ] `diesel_restore` из дампа — данные идентичны исходным (10 тестовых таблиц сравниваются row-by-row).
- [ ] Physical backup работающей базы (writers active) — consistent на момент backup start.
- [ ] PITR: restore на момент «5 минут назад» — точно соответствует состоянию БД в тот момент.
- [ ] Incremental backup на 100 GB базе (1 % изменённых pages) — < 5 минут.

---

### 12. Metrics + Prometheus + Health check (R3-012)

**Категория:** J. Observability
**Приоритет:** HIGH
**Фаза:** 1
**Зависимости:** — (можно параллельно с остальным)
**Связь с prompt3.md:** новое. В `monitoring.md` и `Roadmap.md` Этап 4 описано, но без конкретики.

**Проблема.**
Сейчас мониторинг — только логи (`Slow query breakdown`). Без metrics endpoint невозможно подключить Prometheus / Grafana.

**Задача.**
1. `MetricsRegistry`: счётчики (QPS, errors), гистограммы (latency p50/p95/p99), gauges (heap, connections, active tx).
2. `/metrics` HTTP endpoint на отдельном порту (default `9090`), формат Prometheus text.
3. Health check endpoint `/health`: JSON `{"status":"UP","uptime":3600,"activeTx":5,"diskFree":1024}`.
4. Ключевые метрики:
   - `diesel_queries_total{type, status}` (counter)
   - `diesel_query_duration_seconds{type}` (histogram)
   - `diesel_transactions_active` (gauge)
   - `diesel_connections_active`, `diesel_connections_rejected`
   - `diesel_buffer_pool_hit_ratio`
   - `diesel_wal_lag_ms`
   - `diesel_deadlocks_total`
   - `diesel_lock_timeouts_total`
5. Опционально: OpenTelemetry tracing для SQL execution spans.

**Ключевые файлы.**
- `diesel/observability/MetricsRegistry.java` (новый)
- `diesel/observability/PrometheusExporter.java` (новый)
- `diesel/observability/HealthCheck.java` (новый)
- `diesel/observability/Histogram.java` (новый, lock-free)
- `diesel/observability/MetricsHttpServer.java` (новый)
- `diesel/ConfigLoader.java` (ключи `metrics.port`, `metrics.enabled`)

**Критерии приёмки.**
- [ ] `curl http://localhost:9090/metrics` возвращает Prometheus-совместимый текст.
- [ ] Grafana dashboard (JSON прилагается в `dashboards/dieseldb.json`) показывает QPS, p99 latency, hit ratio.
- [ ] `curl http://localhost:9090/health` возвращает UP/DOWN.
- [ ] Latency overhead метрик < 1 % throughput.

---

### 13. Fix всех blocking bugs из `problems.md` и `PERSISTENCE_README.md` (R3-013)

**Категория:** M. Correctness
**Приоритет:** CRITICAL
**Фаза:** 1
**Зависимости:** — (можно параллельно)
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

**Задача.**
Закрыть все 7 багов. Промпт 1 решает (2) и (6). Промпт 5 решает (5). Остальные — отдельные фиксы.
1. R3-013a: Реализовать Промпт 25 (стабильные row-id) — это critical correctness fix.
2. R3-013b: Распарсить `TRUE`/`FALSE`/`NULL` как литералы, не как column references.
3. R3-013c: Хранить строковые литералы в оригинальном регистре, не нормализовать через `toUpperCase()` в парсере.
4. R3-013d: Явная сериализация `indexDefinitions` в `writeObject`.

**Ключевые файлы.**
- `diesel/storage/DelimitedIndexManager.java` (R3-013a)
- `diesel/QueryParser.java` (R3-013b, R3-013c)
- `diesel/Table.java` (R3-013d)
- Тесты: `TrueFalseLiteralTest`, `StringCaseSensitivityTest`, `RowIdStabilityTest`

**Критерии приёмки.**
- [ ] Все 7 багов из `problems.md` / `PERSISTENCE_README.md` имеют воспроизводящийся тест (red) → фикс → тест green.
- [ ] `PerformanceTest` проходит без ошибок.
- [ ] Регистр строк сохраняется round-trip через INSERT + SELECT.
- [ ] `searchByPrimaryKey` после `insertAt` в середину возвращает корректные позиции (10000 тестов).

---

## Фаза 2 — Production-Grade (6-12 месяцев)

**Цель:** закрыть HIGH-приоритетные пробелы, без которых DieselDB не подходит для multi-tenant production. По завершении — метрика готовности ≥ 75 %.

### 14. Checkpoint + Checksummed pages (CRC32C) (R3-014)

**Категория:** A. Durability
**Приоритет:** HIGH
**Фаза:** 2
**Зависимости:** промпты 3, 4, 5
**Связь с prompt3.md:** уточняет и поглощает Промпты 100-103 (Checkpoint Manager, fuzzy vs sharp, Checksummed Page, CRC32C algorithm). Промпты описаны концептуально, R3-014 интегрирует их с MVCC/page storage/WAL.

**Проблема.**
Fuzzy checkpoint нужен для ограничения WAL replay; checksummed pages — для детекции silent corruption (bit rot). Промпты описаны, но интегрировать с промптами 1/2/3 нужно явно.

**Задача.**
1. Fuzzy checkpoint: не ждать quiescent state, а делать consistent snapshot с активными транзакциями.
2. CRC32C на каждой странице (header + payload).
3. На чтение страницы: verify checksum, при провале — `PageCorruptedException`, попытка восстановить из WAL redo.
4. Background scrubber: раз в N дней — все страницы verify, репорт corrupted.
5. CRC32C оптимизация: hardware SSE4.2 инструкции через JNI (fallback на pure Java реализацию через `java.util.zip.CRC32C`).
6. Конфигурация типа checkpoint (fuzzy по умолчанию, sharp для maintenance).

**Ключевые файлы.**
- `diesel/storage/page/PageChecksum.java` (новый, CRC32C)
- `diesel/checksum/CRC32C.java` (новый, hardware-accelerated когда доступно)
- `diesel/checksum/CRC32CNative.java` (новый, optional JNI)
- `diesel/checksum/ChecksummedPage.java` (новый)
- `diesel/checksum/PageValidator.java` (новый)
- `diesel/recovery/FuzzyCheckpoint.java` (новый/расширенный)
- `diesel/recovery/CheckpointStrategy.java` (новый, fuzzy/sharp)
- `diesel/storage/page/PageScrubber.java` (новый)

**Критерии приёмки.**
- [ ] Тест `CorruptionDetectionTest`: битый байт в странице → `PageCorruptedException`.
- [ ] Recovery после fuzzy checkpoint — без replay всего WAL.
- [ ] Scrubber на 100 GB базе — < 1 часа, не блокирует writers.
- [ ] CRC32C throughput ≥ 5 GB/sec на CPU с SSE4.2.
- [ ] Метрики: `diesel_page_checksum_failures_total`, `diesel_scrubber_duration_seconds`.

---

### 15. libpq protocol + JDBC + Python + Node.js + Go + Rust drivers (R3-015)

**Категория:** I. Network & drivers
**Приоритет:** HIGH
**Фаза:** 2
**Зависимости:** промпт 7 (wire protocol)
**Связь с prompt3.md:** новое, частично в `Roadmap.md` Этап 4.

**Проблема.**
Собственный протокол = пользователи должны писать своего клиента. libpq-совместимость даёт доступ ко всей экосистеме (psql, Hibernate, SQLAlchemy, pgx, prisma, ...).

**Задача.**
1. libpq message protocol (Startup, Query, Parse, Bind, Execute, Sync) — отдельный порт (default 5432).
2. JDBC driver (классы `Driver`, `Connection`, `PreparedStatement`, `ResultSet`).
3. Python psycopg2-compatible driver (pure Python или C extension).
4. Node.js pg-compatible driver.
5. Go pq-compatible driver.
6. Rust tokio-postgres-compatible driver.
7. Совместимость с psql CLI (минимум: \d, \dt, \l, \q, basic queries).

**Ключевые файлы.**
- `diesel/net/libpq/LibpqProtocol.java` (новый)
- `diesel/net/libpq/LibpqMessage.java` (новый)
- `drivers/jdbc/diesel-jdbc/` (новый module)
- `drivers/python/diesel-python/` (новый)
- `drivers/nodejs/diesel-node/` (новый)
- `drivers/go/diesel-go/` (новый)
- `drivers/rust/diesel-rs/` (новый)

**Критерии приёмки.**
- [ ] `psql -h localhost -p 5432` работает для `SELECT * FROM users LIMIT 5`.
- [ ] JDBC: `DriverManager.getConnection("jdbc:diesel://localhost:5432/db")` — работает.
- [ ] Python: `psycopg2.connect("...")` — работает для базовых CRUD.
- [ ] Тест на совместимость с Hibernate для simple entity.
- [ ] Все 5 drivers публикуются в соответствующие registry (Maven Central, PyPI, npm, pkg.go.dev, crates.io).

---

### 16. Replication (logical + physical + quorum-based) (R3-016)

**Категория:** D. Replication & HA
**Приоритет:** HIGH
**Фаза:** 2
**Зависимости:** промпт 3 (WAL), промпт 4 (Recovery), промпт 7 (transport)
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает, но без деталей. `replication.md` описывает теорию.

**Проблема.**
Без репликации — нет HA, нет read scaling. Single point of failure.

**Задача.**
1. **Physical streaming replication**: мастер стримит WAL слейвам, слейвы в hot standby (read-only).
2. **Logical replication**: декодирование WAL в logical change events (insert/update/delete), подписчики применяют.
3. **Synchronous replication**: `synchronous_commit=on` — ждать подтверждения от N синхронных standby.
4. **Quorum-based (Raft)**: встроенный Raft-консенсус для multi-master elections, без внешнего Patroni.
5. **Failover**: автоматическое переключение при падении мастера (через Raft leader election).
6. **Replication slots**: защита от WAL удаления, пока слейв не подтвердил получение.

**Ключевые файлы.**
- `diesel/replication/WalSender.java` (новый)
- `diesel/replication/WalReceiver.java` (новый)
- `diesel/replication/LogicalDecoder.java` (новый)
- `diesel/replication/ReplicationSlot.java` (новый)
- `diesel/replication/raft/RaftNode.java` (новый)
- `diesel/replication/raft/RaftLog.java` (новый)
- `diesel/replication/FailoverManager.java` (новый)
- `diesel/replication/SyncReplicationCoordinator.java` (новый)

**Критерии приёмки.**
- [ ] Streaming replication: lag slave-behind-master < 100 ms на workload 1000 inserts/sec.
- [ ] Synchronous: COMMIT подтверждён только после ack от 1 sync standby.
- [ ] Failover: при `kill -9` мастера, новый лидер выбран < 5 сек.
- [ ] Logical replication: INSERT на мастере → INSERT на подписчике < 1 сек.
- [ ] Replication slot: при offline standby > 5 min — WAL не удаляется, standby может догнать.

---

### 17. Partitioning (range / list / hash) (R3-017)

**Категория:** E. Sharding & Partitioning
**Приоритет:** MEDIUM
**Фаза:** 2
**Зависимости:** промпт 2 (page storage), промпт 10 (online schema для partition management)
**Связь с prompt3.md:** новое. `Roadmap.md` Этап 5 упоминает.

**Проблема.**
Большие таблицы (>100M строк) нужно делить на партиции для manageability (DROP old partition) и query speed (partition pruning).

**Задача.**
1. `CREATE TABLE ... PARTITION BY RANGE/LIST/HASH (...)`.
2. Partition pruning в query optimizer (skip партиции на основе WHERE).
3. Partition switching: `ALTER TABLE ... EXCHANGE PARTITION` (для fast load/unload).
4. Subpartitioning: 2-level partitioning.
5. Default partition для не-матчащихся строк.
6. Partition-wise join (опционально, Фаза 3).

**Ключевые файлы.**
- `diesel/partition/PartitionManager.java` (новый)
- `diesel/partition/PartitionPruner.java` (новый)
- `diesel/partition/RangePartition.java` (новый)
- `diesel/partition/ListPartition.java` (новый)
- `diesel/partition/HashPartition.java` (новый)
- `diesel/QueryParser.java` (синтаксис PARTITION BY)

**Критерии приёмки.**
- [ ] Таблица 12 партиций × 10M строк, `WHERE date='2024-01-15'` → scan только 1 партиции (через EXPLAIN).
- [ ] `EXCHANGE PARTITION` — < 100 ms (metadata swap).
- [ ] Тест `PartitionPruningTest` на 100 запросов с разными WHERE.
- [ ] Subpartitioning: 2-level partition pruning корректно отсекает на обоих уровнях.

---

### 18. Cost-Based Optimizer + статистика (R3-018)

**Категория:** H. Query optimizer
**Приоритет:** HIGH
**Фаза:** 2
**Зависимости:** промпт 1 (MVCC для snapshot статистики), промпт 13 (row-id для выборки)
**Связь с prompt3.md:** новое, косвенно Промпт 14 (статистика). `Roadmap.md` Этап 2.

**Проблема.**
Текущий `QueryOptimizer` — rule-based. Cost-based нужен для: выбора hash vs nested loop join, порядка таблиц в multi-join, использования index vs seq scan.

**Задача.**
1. Статистика: `pg_stats`-аналог — `null_frac`, `distinct`, `most_common_vals`, `histogram_bounds`.
2. Cost model: `seq_page_cost`, `random_page_cost`, `cpu_tuple_cost`, `parallel_setup_cost`.
3. Join algorithms cost: hash (build + probe), nested loop, merge (для отсортированных).
4. Index access paths: index-only scan (covering), bitmap scan.
5. Subquery unnesting: flatten correlated subqueries в joins.
6. Plan caching: ключ = нормализованный SQL + planner-статистика-снапшот.

**Ключевые файлы.**
- `diesel/optimizer/CostEstimator.java` (новый)
- `diesel/optimizer/StatisticsCollector.java` (новый, расширение AnalyzeTableQuery)
- `diesel/optimizer/JoinAlgorithm.java` (enum: NESTED_LOOP, HASH, MERGE)
- `diesel/optimizer/AccessPath.java` (новый)
- `diesel/optimizer/PlanCache.java` (новый)
- `diesel/QueryOptimizer.java` (переработка)

**Критерии приёмки.**
- [ ] На таблицах 1k + 1M строк CBO выбирает hash join (маленькое в hash-сторону).
- [ ] `WHERE id = 5` с уникальным индексом → index-only scan, не seq scan.
- [ ] `WHERE non_indexed_col = 'X'` с selectivity 0.01 → seq scan (правильно).
- [ ] Тест `CboPlanStabilityTest`: 100 случайных запросов — план deterministic при той же статистике.
- [ ] `enable_cbo = true | false` config switch для A/B тестирования.

---

### 19. UPSERT / RETURNING / UPSERT-on-conflict (R3-019)

**Категория:** G. SQL coverage
**Приоритет:** HIGH
**Фаза:** 2
**Зависимости:** —
**Связь с prompt3.md:** новое.

**Проблема.**
`INSERT ... ON CONFLICT DO UPDATE` (UPSERT) и `INSERT ... RETURNING *` — стандартные SQL:2003+. Без них сложно строить idempotent API.

**Задача.**
1. `INSERT INTO ... ON CONFLICT (col) DO UPDATE SET ...` (UPSERT).
2. `INSERT INTO ... RETURNING col1, col2` (возвращать вставленные строки).
3. `UPDATE ... RETURNING ...`, `DELETE ... RETURNING ...`.
4. `ON CONFLICT DO NOTHING`.
5. Conflict target: по column или по constraint name.

**Ключевые файлы.**
- `diesel/InsertQuery.java` (расширение)
- `diesel/UpdateQuery.java`, `diesel/DeleteQuery.java` (RETURNING)
- `diesel/QueryParser.java` (синтаксис)

**Критерии приёмки.**
- [ ] `INSERT ON CONFLICT DO UPDATE` атомарный (no race condition).
- [ ] `RETURNING *` возвращает все columns вставленной/изменённой строки.
- [ ] Тест `UpsertConcurrencyTest`: 100 параллельных UPSERT на тот же PK — ровно 1 победитель.
- [ ] `ON CONFLICT DO NOTHING` не выбрасывает исключение при конфликте.

---

### 20. CI/CD v2 — coverage, matrix, releases (R3-020)

**Категория:** K. CI/CD
**Приоритет:** HIGH
**Фаза:** 2
**Зависимости:** —
**Связь с prompt3.md:** новое. `cicd.md` P0/P1 описано, но не сделано.

**Проблема.**
Сейчас: только 2 теста из ~100 запускаются в CI; нет coverage; нет release pipeline.

**Задача.**
1. Surefire pattern fix: `**/*Test.java, **/*Tests.java, **/Test*.java`.
2. JaCoCo + Codecov, coverage threshold 60 %, master > 70 %.
3. Matrix build: JDK 17 / 21 / 25 (LTS), Windows / Linux / macOS.
4. SonarQube Quality Gate блокирует merge при bugs > 0 / critical > 0.
5. Release pipeline: tag → Maven Central + GitHub Release + Docker image.
6. Performance regression: TPC-C small (10 warehouses) и TPC-H small (SF=1) еженедельно.
7. Container images: `dieseldb/dieseldb:latest`, `:lts`, `:X.Y.Z`.
8. Helm chart для Kubernetes.

**Ключевые файлы.**
- `.github/workflows/ci.yml` (переработка)
- `.github/workflows/release.yml` (новый)
- `.github/workflows/perf.yml` (новый)
- `Dockerfile` (новый)
- `charts/dieseldb/` (новый Helm chart)
- `pom.xml` (jacoco, surefire plugin)

**Критерии приёмки.**
- [ ] Все ~100 тестов запускаются в CI за < 10 мин.
- [ ] Coverage > 60 % для всего проекта, > 80 % для `diesel/` core.
- [ ] Sonar Quality Gate: bugs=0, critical=0 → иначе workflow fail.
- [ ] `docker pull dieseldb/dieseldb:latest` и `docker run -p 5432:5432` — работает.
- [ ] `helm install dieseldb ./charts/dieseldb` — кластер поднимается.

---

## Фаза 3 — PostgreSQL-Parity (12-24+ месяцев)

**Цель:** достичь ~90 % production-readiness и ~85 % PostgreSQL SQL coverage. Промпты ниже развёрнуты из таблицы §7 ROADMAP3 в полные карточки.

### 21. CTE + рекурсивные CTE (R3-021)

**Категория:** G. SQL coverage
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** промпт 18 (CBO)
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 2.

**Проблема.**
Без CTE невозможно выражать иерархические запросы (деревья, графы) и сложные аналитические запросы в читаемом виде. Текущий парсер не понимает `WITH`-clause.

**Задача.**
1. `WITH name AS (SELECT ...) SELECT ... FROM name` — non-recursive CTE.
2. `WITH RECURSIVE name AS (... UNION ALL SELECT ... FROM name WHERE ...)` — рекурсивные CTE с termination check.
3. Несколько CTE в одном запросе (`WITH a AS (...), b AS (...) ...`), с возможностью ссылаться на предыдущие.
4. MATERIALIZED / NOT MATERIALIZED hints для контроля inline vs materialize.
5. CTE в подзапросах, INSERT INTO ... SELECT FROM cte, UPDATE FROM cte.

**Ключевые файлы.**
- `diesel/cte/CommonTableExpression.java` (новый)
- `diesel/cte/CteRegistry.java` (новый, scope-chain для resolution)
- `diesel/cte/RecursiveCteIterator.java` (новый, итеративное вычисление)
- `diesel/QueryParser.java` (синтаксис WITH)
- `diesel/QueryOptimizer.java` (materialization decision)

**Критерии приёмки.**
- [ ] Тест `RecursiveCteTest`: дерево 1000 узлов, запрос всех потомков корня → корректный результат за < 1 сек.
- [ ] Несколько CTE разрешаются в правильном порядке (a перед b, b перед c).
- [ ] `MATERIALIZED` форсирует materialization, `NOT MATERIALIZED` форсирует inline.
- [ ] Производительность на TPC-H Q1 (с CTE) не хуже, чем без CTE на 10 %.

---

### 22. Оконные функции (R3-022)

**Категория:** G. SQL coverage
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** уточняет и поглощает Промпт 131 (Window Functions). Промпт 131 описывает базовый набор; R3-022 добавляет frame-семантику и оптимизацию.

**Проблема.**
Оконные функции (ROW_NUMBER, RANK, LAG, LEAD, NTILE, агрегаты OVER) — стандарт SQL:2003, без них невозможно писать top-N-per-group, running totals, time-series аналитику.

**Задача.**
1. Базовый набор: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`.
2. Навигационные: `LAG(col, n, default)`, `LEAD(col, n, default)`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`.
3. Агрегаты OVER: `SUM/AVG/COUNT/MIN/MAX(col) OVER (PARTITION BY ... ORDER BY ...)`.
4. Frame-спецификации: `ROWS BETWEEN N PRECEDING AND N FOLLOWING`, `RANGE BETWEEN ... `, `UNBOUNDED PRECEDING/FOLLOWING`.
5. Оптимизация: сортировка один раз на partition, обход window-by-window.

**Ключевые файлы.**
- `diesel/window/WindowFunction.java` (новый, базовый интерфейс)
- `diesel/window/WindowFrameEvaluator.java` (новый)
- `diesel/window/WindowSpec.java` (новый, parse PARTITION BY / ORDER BY / frame)
- `diesel/window/RowNumberFunction.java`, `RankFunction.java`, `LagFunction.java`, etc. (новые)
- `diesel/QueryParser.java` (синтаксис OVER)
- `diesel/QueryExecutor.java` (window pipeline)

**Критерии приёмки.**
- [ ] Тест `WindowFunctionsTest`: 20 функций × 5 frame-вариантов = 100 случаев, все green.
- [ ] `ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)` даёт top-N внутри dept.
- [ ] `SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` = running total.
- [ ] Производительность на TPC-H Q1 (с оконными функциями) не хуже PG × 2.

---

### 23. FULL OUTER JOIN, LATERAL JOIN, CROSS APPLY (R3-023)

**Категория:** G. SQL coverage
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 2.

**Проблема.**
Без FULL OUTER JOIN нельзя объединять таблицы с preservation строк с обеих сторон. LATERAL / CROSS APPLY нужны для коррелированных подзапросов в FROM.

**Задача.**
1. `FULL OUTER JOIN`: left + right + unmatched с обеих сторон (NULL-fill).
2. `LEFT / RIGHT OUTER JOIN` уже есть — проверить корректность NULL-fill.
3. `CROSS JOIN LATERAL (subquery)` — коррелированный подзапрос в FROM, с доступом к колонкам outer.
4. `CROSS APPLY` / `OUTER APPLY` (T-SQL совместимый синтаксис).
5. Оптимизация: LATERAL → эквивалентный nested-loop join.

**Ключевые файлы.**
- `diesel/join/FullOuterJoinExecutor.java` (новый)
- `diesel/join/LateralJoinExecutor.java` (новый)
- `diesel/QueryParser.java` (синтаксис FULL OUTER, LATERAL, APPLY)

**Критерии приёмки.**
- [ ] Тест `FullOuterJoinTest`: 100 строк + 80 строк, overlap 50 → 130 строк в результате.
- [ ] Тест `LateralJoinTest`: коррелированный подзапрос возвращает N строк per outer row.
- [ ] Тест `OuterApplyTest`: outer apply сохраняет unmatched outer строки с NULL.

---

### 24. UNION / INTERSECT / EXCEPT (R3-024)

**Категория:** G. SQL coverage
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** уточняет и поглощает Промпт 115.

**Проблема.**
Set operations нужны для слияния результатов нескольких SELECT. Сейчас не реализованы.

**Задача.**
1. `UNION` (с удалением дубликатов через hash/sort distinct).
2. `UNION ALL` (без удаления дубликатов — потоковая обработка).
3. `INTERSECT` (общие строки двух запросов).
4. `EXCEPT` (строки первого запроса минус строки второго).
5. Цепочки: `SELECT ... UNION SELECT ... INTERSECT SELECT ...` с правильным приоритетом.
6. Скобки для управления порядком: `(SELECT ... UNION SELECT ...) EXCEPT SELECT ...`.

**Ключевые файлы.**
- `diesel/setop/UnionQuery.java` (новый)
- `diesel/setop/IntersectQuery.java` (новый)
- `diesel/setop/ExceptQuery.java` (новый)
- `diesel/setop/SetOperationExecutor.java` (новый, streaming-merge для UNION ALL)
- `diesel/QueryParser.java` (синтаксис)

**Критерии приёмки.**
- [ ] `UNION` корректно удаляет дубликаты (1M строк → ~500k уникальных).
- [ ] `UNION ALL` не удаляет дубликаты, throughput ≥ 1M rows/sec.
- [ ] `INTERSECT` и `EXCEPT` корректно считают NULL-семантику (NULL = NULL при set ops).
- [ ] Скобки управляют порядком операций.

---

### 25. Foreign Keys с CASCADE (R3-025)

**Категория:** G. SQL coverage
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** промпт 10 (online alter для добавления constraint онлайн)
**Связь с prompt3.md:** уточняет и поглощает Промпт 128 (Foreign Keys с каскадными операциями). R3-025 добавляет online constraint addition и multi-level cascade.

**Проблема.**
Referential integrity — основа реляционной БД. Сейчас FK нет, любой INSERT/UPDATE может нарушить целостность.

**Задача.**
1. `FOREIGN KEY (col) REFERENCES parent(id)` при CREATE TABLE / ALTER TABLE.
2. `ON DELETE CASCADE` / `ON DELETE SET NULL` / `ON DELETE RESTRICT` / `ON DELETE NO ACTION`.
3. `ON UPDATE CASCADE` / `ON UPDATE SET NULL` / `ON UPDATE RESTRICT`.
4. Multi-level cascade (родитель → ребёнок → внук) с защитой от бесконечной рекурсии.
5. Проверка referential integrity на INSERT/UPDATE в child (lookup parent).
6. Online constraint addition: `ALTER TABLE ... ADD CONSTRAINT ... NOT VALID` + фоновая валидация.

**Ключевые файлы.**
- `diesel/constraint/ForeignKeyConstraint.java` (новый)
- `diesel/constraint/CascadeDeleteQuery.java` (новый)
- `diesel/constraint/CascadeUpdateQuery.java` (новый)
- `diesel/constraint/CascadeExecutor.java` (новый, с защитой от циклов)
- `diesel/QueryParser.java` (синтаксис REFERENCES)
- `diesel/constraint/ConstraintValidator.java` (новый, online validation)

**Критерии приёмки.**
- [ ] Тест `ForeignKeyCascadeTest`: DELETE parent → cascade DELETE детей и внуков (3 уровня).
- [ ] `ON DELETE SET NULL` корректно выставляет NULL в child.
- [ ] `ON DELETE RESTRICT` выбрасывает исключение при существующем child.
- [ ] Online ADD CONSTRAINT не блокирует writers > 100 ms.
- [ ] Защита от циклов: cascade depth limit, exception при бесконечной рекурсии.

---

### 26. CHECK constraints, NOT NULL, DEFAULT (R3-026)

**Категория:** G. SQL coverage
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** уточняет и поглощает Промпт 129 (CHECK Constraints). R3-026 добавляет NOT NULL enforcement, DEFAULT expressions, named constraints.

**Проблема.**
CHECK constraints нужны для domain integrity. NOT NULL и DEFAULT — основа schema design.

**Задача.**
1. `CHECK (condition)` при CREATE TABLE / ALTER TABLE.
2. Валидация при INSERT/UPDATE: вычисление condition на каждой изменённой строке.
3. Составные условия: `CHECK (age > 0 AND age < 150 AND email LIKE '%@%')`.
4. Named constraints: `CONSTRAINT name CHECK (...)` для управления (drop by name).
5. `NOT NULL` enforcement на уровне колонки.
6. `DEFAULT` expressions: literals, `NOW()`, `CURRENT_USER`, `NEXTVAL(seq)`, deterministic expressions.

**Ключевые файлы.**
- `diesel/constraint/CheckConstraint.java` (новый)
- `diesel/constraint/NotNullConstraint.java` (новый)
- `diesel/constraint/DefaultExpression.java` (новый)
- `diesel/constraint/NamedConstraint.java` (новый)
- `diesel/QueryParser.java` (синтаксис CHECK / CONSTRAINT / DEFAULT)
- `diesel/constraint/ConstraintRegistry.java` (новый, lookup by name для DROP)

**Критерии приёмки.**
- [ ] Тест `CheckConstraintTest`: 10 различных CHECK conditions, все violation cases ловятся.
- [ ] `NOT NULL` enforcement работает на INSERT и UPDATE.
- [ ] `DEFAULT` выражения вычисляются корректно (literal, function, sequence).
- [ ] Named constraints можно DROP по имени: `ALTER TABLE ... DROP CONSTRAINT name`.

---

### 27. Materialized Views + refresh (R3-027)

**Категория:** G. SQL coverage
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** уточняет и поглощает Промпт 127 (Materialized Views). R3-027 добавляет refresh strategies и query rewrite.

**Проблема.**
Materialized views позволяют кэшировать дорогие запросы. Без них OLAP-нагрузки на DieselDB будут медленными.

**Задача.**
1. `CREATE MATERIALIZED VIEW name AS SELECT ...` — физическое хранение результата.
2. Refresh strategies: `REFRESH MANUAL`, `REFRESH ON COMMIT`, `REFRESH EVERY interval`.
3. `REFRESH MATERIALIZED VIEW name [CONCURRENTLY]` — concurrent без блокировки readers.
4. Query rewrite: оптимизатор автоматически использует MV при совпадении запроса.
5. Incremental refresh: только изменившиеся строки (через delta-tables).
6. Indexes на materialized view (как на обычной таблице).

**Ключевые файлы.**
- `diesel/materializedview/CreateMaterializedViewQuery.java` (новый)
- `diesel/materializedview/MaterializedViewManager.java` (новый)
- `diesel/materializedview/MaterializedViewRefresher.java` (новый)
- `diesel/materializedview/IncrementalRefresher.java` (новый)
- `diesel/materializedview/QueryRewriter.java` (новый, в CBO)
- `diesel/QueryParser.java` (синтаксис)

**Критерии приёмки.**
- [ ] `CREATE MATERIALIZED VIEW` на 1M-строчной базе — < 30 сек.
- [ ] `REFRESH CONCURRENTLY` не блокирует readers во время refresh.
- [ ] Query rewrite: `SELECT count(*) FROM big_table` автоматически использует MV если есть.
- [ ] Incremental refresh после 1 % изменений — < 5 % от full refresh time.

---

### 28. Triggers BEFORE/AFTER (R3-028)

**Категория:** G. SQL coverage
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** промпт 1 (MVCC)
**Связь с prompt3.md:** новое.

**Проблема.**
Triggers нужны для audit logging, derived columns, cross-table consistency checks.

**Задача.**
1. `CREATE TRIGGER name BEFORE/AFTER INSERT/UPDATE/DELETE ON table FOR EACH ROW EXECUTE ...`.
2. BEFORE triggers могут модифицировать NEW.row (изменить значения перед INSERT).
3. AFTER triggers — для side effects (audit log, cascade to other tables).
4. Statement-level triggers: `FOR EACH STATEMENT` (один раз на запрос, не на строку).
5. Trigger body — SQL-only (без PL/pgSQL): один SELECT/INSERT/UPDATE/DELETE.
6. Multiple triggers на одну таблицу: упорядочение по priority.

**Ключевые файлы.**
- `diesel/trigger/Trigger.java` (новый)
- `diesel/trigger/TriggerManager.java` (новый)
- `diesel/trigger/TriggerExecutor.java` (новый)
- `diesel/QueryParser.java` (синтаксис CREATE TRIGGER)

**Критерии приёмки.**
- [ ] Тест `BeforeInsertTriggerTest`: trigger модифицирует `created_at = NOW()` перед INSERT.
- [ ] Тест `AfterDeleteTriggerTest`: trigger пишет в audit_log после DELETE.
- [ ] Statement-level trigger вызывается ровно 1 раз на `DELETE FROM table` (даже если 1000 строк удалено).
- [ ] Multiple triggers: упорядочение по priority field.

---

### 29. VIEW (non-materialized) (R3-029)

**Категория:** G. SQL coverage
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** новое.

**Проблема.**
Non-materialized views — это сохранённые SQL-запросы с имененем. Нужны для абстракции schema, security (ограничение видимых колонок), упрощения сложных запросов.

**Задача.**
1. `CREATE VIEW name AS SELECT ...` — сохранение SQL-текста.
2. `SELECT * FROM view_name` — подстановка SQL-текста view в запрос.
3. Updatable views: `UPDATE view SET ...` → переписывается в `UPDATE underlying_table SET ... WHERE view-condition`.
4. `CHECK OPTION`: INSERT/UPDATE через updatable view должен удовлетворять WHERE условию view.
5. `DROP VIEW name`, `CREATE OR REPLACE VIEW`.
6. Зависимости: DROP TABLE с зависимым view → error или CASCADE.

**Ключевые файлы.**
- `diesel/view/CreateViewQuery.java` (новый)
- `diesel/view/ViewRegistry.java` (новый)
- `diesel/view/ViewExpander.java` (новый, в query rewriter)
- `diesel/view/UpdatableViewChecker.java` (новый)
- `diesel/QueryParser.java` (синтаксис CREATE VIEW)

**Критерии приёмки.**
- [ ] Тест `ViewTest`: SELECT * FROM view возвращает те же данные, что и исходный SELECT.
- [ ] Updatable view: INSERT через view создаёт строку в underlying table.
- [ ] `CHECK OPTION`: INSERT violates view WHERE → exception.
- [ ] DROP TABLE с зависимым view без CASCADE → exception.

---

### 30. Full-text search (R3-030)

**Категория:** G. SQL coverage
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** промпт 18 (CBO)
**Связь с prompt3.md:** уточняет и поглощает Промпт 130 (Full Text Search). R3-030 добавляет GiST/GIN-аналоги и relevance scoring.

**Проблема.**
Без FTS невозможно эффективно искать по тексту (LIKE '%word%' — full scan).

**Задача.**
1. Инвертированный индекс (GIN-аналог) для text колонок.
2. Tokenization: разбиение текста на токены, lowercase, удаление стоп-слов.
3. Stemming (Snowball-аналог) для русского и английского.
4. `MATCH(col) AGAINST('word1 word2')` синтаксис или `col @@ 'word1 & word2'`.
5. Relevance scoring: TF-IDF, BM25.
6. Ranking, highlight, snippet extraction.

**Ключевые файлы.**
- `diesel/fts/FullTextIndex.java` (новый, GIN-структура)
- `diesel/fts/Tokenizer.java` (новый)
- `diesel/fts/Stemmer.java` (новый, Snowball-порт)
- `diesel/fts/RelevanceScorer.java` (новый, BM25)
- `diesel/fts/Highlighter.java` (новый)
- `diesel/QueryParser.java` (синтаксис MATCH/AGAINST)

**Критерии приёмки.**
- [ ] Тест `FtsTest`: 1M документов, поиск по слову → < 50 ms, top-10 результатов.
- [ ] Relevance scoring корректно ранжирует (точные совпадения выше частичных).
- [ ] Stemming: «бегущий» и «беги» матчатся как один токен (русский).
- [ ] Highlight: возвращается snippet с подсвеченными терминами.

---

### 31. Расширенные типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT (R3-031)

**Категория:** G. SQL coverage
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** новое.

**Проблема.**
PostgreSQL-совместимость требует расширенных типов. Без JSONB нельзя хранить полуструктурированные данные; без UUID — нельзя распределённые ID; без ARRAY — нельзя хранить списки.

**Задача.**
1. `JSONB`: бинарное хранение JSON, индексация по пути (`col->'key'->'subkey'`), операторы `@>`, `?`, `?|`, `?&`.
2. `UUID`: RFC 4122 v4/v7, генерация `gen_random_uuid()`, индексация.
3. `ARRAY`: `INTEGER[]`, `TEXT[][]`, операторы `&&` (overlap), `@>` (contains), индекс GIN.
4. `ENUM`: `CREATE TYPE color AS ENUM ('red', 'green', 'blue')`, типобезопасность.
5. `INTERVAL`: `INTERVAL '1 day 2 hours'`, арифметика с TIMESTAMP.
6. `INET` / `CIDR`: IPv4/IPv6 адреса, операторы `<<` (subnet), `>>`.
7. `BIT` / `BIT VARYING`: битовые строки, побитовые операции.

**Ключевые файлы.**
- `diesel/types/JsonbType.java` (новый)
- `diesel/types/UuidType.java` (новый)
- `diesel/types/ArrayType.java` (новый)
- `diesel/types/EnumType.java` (новый)
- `diesel/types/IntervalType.java` (новый)
- `diesel/types/InetType.java` (новый)
- `diesel/types/BitType.java` (новый)
- `diesel/QueryParser.java` (парсинг литералов и операторов)

**Критерии приёмки.**
- [ ] JSONB: `col->'key'` возвращает sub-document; `col @> '{"key":"value"}'` работает.
- [ ] UUID: `gen_random_uuid()` возвращает v4 UUID; index on UUID уникален.
- [ ] ARRAY: `'{1,2,3}'::INTEGER[]` хранится, `col && '{3,4,5}'` возвращает true.
- [ ] ENUM: INSERT значения не из enum → exception.
- [ ] INTERVAL: `TIMESTAMP '2024-01-01' + INTERVAL '1 day'` = `2024-01-02`.
- [ ] INET: `'192.168.1.5' << '192.168.0.0/16'` = true.

---

### 32. Хранимые функции (SQL-only, без PL/pgSQL) (R3-032)

**Категория:** G. SQL coverage
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** новое.

**Проблема.**
Хранимые функции позволяют инкапсулировать бизнес-логику в БД. Без PL/pgSQL DieselDB всё равно может предоставить SQL-only функции (как PostgreSQL `LANGUAGE SQL`).

**Задача.**
1. `CREATE FUNCTION name(arg1 TYPE, ...) RETURNS TYPE LANGUAGE SQL AS $$ SELECT ... $$`.
2. Иммутабельные / стабильные / волатильные функции (`IMMUTABLE`, `STABLE`, `VOLATILE`).
3. Рекурсивные функции (с глубиной limit).
4. Параметры по умолчанию, named arguments.
5. TABLE-returning функции: `RETURNS TABLE(col1 TYPE, col2 TYPE)`.
6. Inline оптимизация: IMMUTABLE функции inlining в запросы.

**Ключевые файлы.**
- `diesel/function/CreateFunctionQuery.java` (новый)
- `diesel/function/SqlFunction.java` (новый)
- `diesel/function/FunctionRegistry.java` (новый)
- `diesel/function/FunctionInliningRewriter.java` (новый)
- `diesel/QueryParser.java` (синтаксис CREATE FUNCTION)

**Критерии приёмки.**
- [ ] `CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b $$` работает.
- [ ] IMMUTABLE функция инлайнится: `SELECT add(1, 2)` → `SELECT 3` (по EXPLAIN).
- [ ] TABLE-returning функция: `SELECT * FROM generate_series(1, 5)` возвращает 5 строк.
- [ ] Named arguments: `SELECT add(b := 2, a := 1)` работает.

---

### 33. Шардинг + distributed query planner (R3-033)

**Категория:** E. Sharding & Partitioning
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** промпт 16 (replication), промпт 17 (partition)
**Связь с prompt3.md:** новое, `Roadmap.md` Этап 5.

**Проблема.**
При росте данных на одну ноду (vertical scaling упирается в CPU/RAM) нужен horizontal scaling — шардинг.

**Задача.**
1. `CREATE SHARDED TABLE ...` с sharding key (hash/range).
2. Shard placement: какие шарды на каких нодах (shard map).
3. Distributed query planner: push-down filters к шардам, fan-out SELECT, merge results.
4. Distributed joins: co-located join (если обе таблицы пошардинжены по одному ключу).
5. Cross-shard join: broadcast / repartition.
6. Distributed transactions: 2PC для multi-shard writes.

**Ключевые файлы.**
- `diesel/sharding/ShardManager.java` (новый)
- `diesel/sharding/ShardMap.java` (новый)
- `diesel/sharding/DistributedQueryPlanner.java` (новый)
- `diesel/sharding/DistributedJoinExecutor.java` (новый)
- `diesel/QueryParser.java` (синтаксис SHARDED)

**Критерии приёмки.**
- [ ] Тест `ShardingTest`: 4 ноды × 1M строк каждая, `SELECT count(*) WHERE shard_key=X` → routed to 1 shard.
- [ ] Co-located join: 2 таблицы пошардинжены по user_id, join без repartition.
- [ ] Cross-shard join через broadcast для small dim table.
- [ ] Distributed transaction: INSERT на 2 шарда → 2PC commit/rollback.

---

### 34. 2PC distributed transactions (R3-034)

**Категория:** D. Replication & HA + E. Sharding
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** промпт 16 (replication)
**Связь с prompt3.md:** новое.

**Проблема.**
Multi-shard writes требуют distributed transactions. Без 2PC атомарность невозможна.

**Задача.**
1. Two-Phase Commit: PREPARE phase (все участники голосуют) + COMMIT/ABORT phase.
2. Coordinator: `TransactionCoordinator` управляет 2PC для multi-shard transactions.
3. Participants: каждый shard — participant, готовит local transaction.
4. Recovery: при краше coordinator между phases — нужно разрешить (heuristic decisions).
5. Timeout: если participant не отвечает за N секунд → abort.
6. Logging: coordinator state machine в persistent log для crash recovery.

**Ключевые файлы.**
- `diesel/tx/TwoPhaseCommitCoordinator.java` (новый)
- `diesel/tx/TransactionParticipant.java` (новый)
- `diesel/tx/CoordinatorLog.java` (новый, persistent)
- `diesel/tx/HeuristicException.java` (новый)

**Критерии приёмки.**
- [ ] Тест `TwoPcCommitTest`: 3 shards, COMMIT на всех → данные видны на всех.
- [ ] Тест `TwoPcAbortTest`: 1 из 3 голосует ABORT → rollback на всех.
- [ ] Тест `TwoPcCrashRecoveryTest`: kill coordinator между PREPARE и COMMIT → recovery по log.
- [ ] Timeout: participant не отвечает 30 сек → abort.

---

### 35. Row-Level Security (RLS) (R3-035)

**Категория:** F. Security
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** промпт 8 (RBAC)
**Связь с prompt3.md:** новое.

**Проблема.**
Multi-tenant системы требуют, чтобы каждый tenant видел только свои строки. Без RLS это делается в приложении (и часто с ошибками).

**Задача.**
1. `CREATE POLICY name ON table FOR SELECT/INSERT/UPDATE/DELETE USING (condition)`.
2. `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`.
3. Policy evaluation: на каждый запрос автоматически добавляется WHERE condition.
4. Multiple policies: combined через OR (permissive) или AND (restrictive).
5. `BYPASSRLS` роль для admin (аналог PostgreSQL `BYPASSRLS`).
6. Tenant isolation: `current_tenant()` функция в policy condition.

**Ключевые файлы.**
- `diesel/security/rls/RowLevelSecurityPolicy.java` (новый)
- `diesel/security/rls/RlsApplier.java` (новый, в query rewriter)
- `diesel/security/rls/PolicyRegistry.java` (новый)
- `diesel/QueryParser.java` (синтаксис CREATE POLICY)

**Критерии приёмки.**
- [ ] Тест `RlsTest`: tenant A видит только свои строки (10), tenant B видит свои (10), не видит строки A.
- [ ] `BYPASSRLS` role видит все строки.
- [ ] Multiple policies combined через OR (permissive) и AND (restrictive) корректно.
- [ ] INSERT/UPDATE/DELETE policies применяются корректно.

---

### 36. Column-level privileges (R3-036)

**Категория:** F. Security
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** промпт 8 (RBAC)
**Связь с prompt3.md:** новое.

**Проблема.**
Table-level RBAC недостаточно: часто нужно скрыть отдельные колонки (например, `salary` от роли `intern`).

**Задача.**
1. `GRANT SELECT (col1, col2) ON table TO role` — column-level SELECT privilege.
2. `GRANT UPDATE (col) ON table TO role` — column-level UPDATE.
3. Masking: `CREATE MASKING POLICY name ON table COLUMN col USING (case when current_user() = 'admin' then col else '***' end)`.
4. Сочетание с RLS: masking не заменяет RLS, дополняет.

**Ключевые файлы.**
- `diesel/security/column/ColumnPrivilege.java` (новый)
- `diesel/security/column/ColumnAccessChecker.java` (новый, в query rewriter)
- `diesel/security/column/MaskingPolicy.java` (новый)
- `diesel/QueryParser.java` (расширение GRANT)

**Критерии приёмки.**
- [ ] Тест `ColumnPrivilegeTest`: пользователь без SELECT на `salary` → `SELECT salary FROM users` → AccessDenied.
- [ ] `SELECT (col1, col2) FROM table` для user с правами только на col1, col2 — работает.
- [ ] Masking: `intern` видит `salary = '***'`, `admin` видит реальное значение.

---

### 37. TDE at rest encryption (R3-037)

**Категория:** F. Security
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** промпт 2 (page storage)
**Связь с prompt3.md:** новое.

**Проблема.**
Если диск скомпрометирован (украден, backup попал не туда), данные в открытом виде — утечка. TDE шифрует страницы на диске, расшифровывает в памяти.

**Задача.**
1. Page-level encryption: AES-256-GCM на каждую страницу при записи, расшифровка при чтении.
2. Master key: хранится в external KMS (AWS KMS, HashiCorp Vault, local file с passphrase).
3. Key rotation: перегенерация master key + re-encrypt всех страниц в фоне.
4. Tablespace-level encryption: `CREATE TABLESPACE ... ENCRYPTION = 'AES-256-GCM'`.
5. WAL encryption: WAL сегменты тоже шифруются.
6. Performance: AES-NI hardware acceleration, кэш расшифрованных страниц в buffer pool.

**Ключевые файлы.**
- `diesel/security/tde/TdeCipher.java` (новый, AES-256-GCM)
- `diesel/security/tde/MasterKeyProvider.java` (новый, интерфейс)
- `diesel/security/tde/KmsMasterKeyProvider.java` (новый, AWS KMS / Vault)
- `diesel/security/tde/FileMasterKeyProvider.java` (новый, passphrase)
- `diesel/security/tde/KeyRotationManager.java` (новый)
- `diesel/storage/page/PageCipher.java` (новый, wrapper над Page с шифрованием)

**Критерии приёмки.**
- [ ] Тест `TdeTest`: данные на диске зашифрованы (`hexdump data/page.bin` не показывает readable текст).
- [ ] Key rotation: rotate master key, проверить что данные всё ещё читаются.
- [ ] Performance overhead < 5 % на read-heavy workload (с AES-NI).
- [ ] WAL сегменты зашифрованы тоже.

---

### 38. Vectorized execution (batch) (R3-038)

**Категория:** H. Query optimizer
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** промпт 18 (CBO)
**Связь с prompt3.md:** новое.

**Проблема.**
Row-by-row execution (tuple-at-a-time) — медленно на OLAP. Vectorized (batch of N rows, e.g. 1024) даёт 5-10× speedup за счёт CPU cache locality и SIMD.

**Задача.**
1. `Batch` контейнер: array-of-columns (columnar) вместо list-of-rows (row-wise).
2. Vectorized operators: scan, filter, project, aggregate — все работают с batches.
3. Vectorized expressions: `col + 1`, `col > 5`, `LOWER(col)` — SIMD где возможно.
4. Adapter: vectorized ↔ row-based для интеропа с legacy operators.
5. Config: `executor.mode = row | vectorized`, batch size 1024 default.

**Ключевые файлы.**
- `diesel/executor/vectorized/Batch.java` (новый)
- `diesel/executor/vectorized/VectorizedScan.java` (новый)
- `diesel/executor/vectorized/VectorizedFilter.java` (новый)
- `diesel/executor/vectorized/VectorizedAggregate.java` (новый)
- `diesel/executor/vectorized/VectorizedExpression.java` (новый)
- `diesel/executor/RowBatchAdapter.java` (новый, interop)

**Критерии приёмки.**
- [ ] Тест `VectorizedExecutionTest`: SUM(col) на 10M строк — 3× быстрее row-by-row.
- [ ] SIMD usage: подтверждается через JIT assembly dump или perf counter.
- [ ] Mixed plan: row-based scan + vectorized aggregate — работает через adapter.

---

### 39. Adaptive joins (runtime switching) (R3-039)

**Категория:** H. Query optimizer
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** промпт 18 (CBO)
**Связь с prompt3.md:** новое.

**Проблема.**
CBO может ошибаться в оценке cardinality (например, статистика устарела). Adaptive join: начинается как hash join, если build-side слишком большой — переключается на nested loop.

**Задача.**
1. `AdaptiveJoinExecutor`: до build-phase оценивает cardinality build-side.
2. Если cardinality < threshold (config: `adaptive.join.hash.threshold`) — hash join.
3. Иначе — nested loop join.
4. Runtime statistics: collect во время выполнения, обновлять `pg_stats`-аналог.
5. Plan feedback: запоминать actual cardinality для будущих планирований.

**Ключевые файлы.**
- `diesel/optimizer/AdaptiveJoinExecutor.java` (новый)
- `diesel/optimizer/RuntimeStatisticsCollector.java` (новый)
- `diesel/optimizer/PlanFeedback.java` (новый, persistent feedback)

**Критерии приёмки.**
- [ ] Тест `AdaptiveJoinTest`: малый build-side → hash join; большой → nested loop.
- [ ] Plan feedback: повторный запуск того же запроса использует actual cardinality.
- [ ] Сравнение с non-adaptive: на 10 mismatched-cardinality запросах adaptive быстрее на 30 %.

---

### 40. Parallel query scan + aggregation (R3-040)

**Категория:** H. Query optimizer
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** промпт 18 (CBO)
**Связь с prompt3.md:** уточняет и поглощает Промпты 123 (Parallel Scan) и 124 (Parallel Aggregation). R3-040 добавляет partition-aware parallelism и Gather node.

**Проблема.**
Single-threaded scan на multi-core CPU не утилизирует ресурсы. Parallel scan делит таблицу на ranges, каждый worker сканирует свой range.

**Задача.**
1. Parallel scan: split table на N ranges (по row-id или page-id), N = `max_parallel_workers`.
2. Parallel aggregation: каждый worker локально агрегирует, `Gather` node merge-ит результаты.
3. Partition-aware: для partitioned tables, каждый worker берёт свою партицию.
4. Adaptive parallelism: при маленьких таблицах (cardinality < threshold) — не параллелить.
5. `SET max_parallel_workers = N` для контроля ресурсов.

**Ключевые файлы.**
- `diesel/executor/parallel/ParallelScanExecutor.java` (новый)
- `diesel/executor/parallel/ParallelAggregationExecutor.java` (новый)
- `diesel/executor/parallel/GatherNode.java` (новый, merge results)
- `diesel/executor/parallel/RangeSplitter.java` (новый)
- `diesel/QueryOptimizer.java` (планирование parallel paths)

**Критерии приёмки.**
- [ ] Тест `ParallelScanTest`: SUM на 100M строк, 8 cores → 6-8× speedup vs single-thread.
- [ ] Adaptive: на 10k-строчной таблице parallelism не включается (overhead > gain).
- [ ] Partition-aware: 12 партиций, 12 workers → каждый worker scan одну партицию.

---

### 41. Bitmap indexes (R3-041)

**Категория:** C. Storage engine
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** уточняет и поглощает Промпт 122 (Bitmap Indexes). R3-041 добавляет сжатие WAH/BBC и integration с CBO.

**Проблема.**
Bitmap indexes эффективны для low-cardinality колонок (gender, status, country): одна bitmap на distinct value, быстрые bitwise operations.

**Задача.**
1. `CREATE BITMAP INDEX name ON table(col)`.
2. BitSet per distinct value, формат: WAH (Word-Aligned Hybrid) или BBC (Byte-Aligned Bitmap Compression).
3. Быстрые bitwise operations: AND, OR, NOT для комбинированных WHERE.
4. Поддержка NULL bitmap.
5. Integration с CBO: cost model для bitmap scan vs btree scan.
6. Bitmap → row-id conversion: `BitmapScanExecutor` возвращает row-ids.

**Ключевые файлы.**
- `diesel/index/bitmap/BitmapIndex.java` (новый)
- `diesel/index/bitmap/CompressedBitmap.java` (новый, WAH)
- `diesel/index/bitmap/BitmapScanExecutor.java` (новый)
- `diesel/QueryParser.java` (синтаксис CREATE BITMAP INDEX)

**Критерии приёмки.**
- [ ] Тест `BitmapIndexTest`: low-cardinality (10 distinct values), 10M rows, `WHERE col = 'X'` → < 50 ms.
- [ ] `WHERE col1 = 'A' AND col2 = 'B'` → AND of bitmaps, 10× faster than btree.
- [ ] WAH compression: размер bitmap < 10 % от raw bitmap.
- [ ] CBO выбирает bitmap scan когда cardinality < 100 distinct values.

---

### 42. Covering indexes (INCLUDE) (R3-042)

**Категория:** C. Storage engine
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** —
**Связь с prompt3.md:** новое.

**Проблема.**
Index-only scan требует, чтобы все нужные колонки были в индексе. Без INCLUDE приходится добавлять колонки в index key (ухудшает selectivity).

**Задача.**
1. `CREATE INDEX name ON table(key_col) INCLUDE (col1, col2, ...)`.
2. INCLUDE columns хранятся в leaf pages, не участвуют в tree navigation.
3. Index-only scan: если все нужные колонки в index (key + INCLUDE), не обращаться к heap.
4. Visibility map: для index-only scan нужно знать, видна ли строка в текущем snapshot (через MVCC).

**Ключевые файлы.**
- `diesel/index/CoveringIndex.java` (новый)
- `diesel/index/IndexOnlyScanExecutor.java` (новый)
- `diesel/mvcc/VisibilityMap.java` (новый, для index-only scan)
- `diesel/QueryParser.java` (синтаксис INCLUDE)

**Критерии приёмки.**
- [ ] Тест `CoveringIndexTest`: `SELECT col1 FROM table WHERE key_col = X` → index-only scan, no heap access (по EXPLAIN).
- [ ] INCLUDE columns не влияют на B-tree navigation.
- [ ] Visibility map корректно помечает страницы, где все строки committed.

---

### 43. Parquet storage (нативный) (R3-043)

**Категория:** C. Storage engine + L. Serialization
**Приоритет:** MEDIUM
**Фаза:** 3
**Зависимости:** промпт 2 (page storage)
**Связь с prompt3.md:** новое. `Roadmap_Parquet_Stage1.md` описывает теорию.

**Проблема.**
Parquet — columnar format с compression, эффективен для OLAP. Нативная интеграция (не через AVRO) даёт 5-10× speedup на аналитических запросах.

**Задача.**
1. `CREATE TABLE ... STORED AS PARQUET`.
2. Columnar layout: row groups × column chunks × pages.
3. Compression: Snappy, ZSTD, gzip per column chunk.
4. Predicate pushdown: min/max statistics per column chunk, skip не-матчащих.
5. Projection pushdown: читать только нужные колонки.
6. Vectorized scan: native integration с промптом 38 (vectorized execution).

**Ключевые файлы.**
- `diesel/storage/parquet/ParquetTableStorage.java` (новый)
- `diesel/storage/parquet/ParquetWriter.java` (новый)
- `diesel/storage/parquet/ParquetReader.java` (новый)
- `diesel/storage/parquet/ColumnChunk.java` (новый)
- `diesel/storage/parquet/RowGroup.java` (новый)
- `diesel/storage/parquet/Statistics.java` (новый, min/max per chunk)

**Критерии приёмки.**
- [ ] Тест `ParquetStorageTest`: 10M rows × 20 columns, `SELECT SUM(col1)` — 5× faster than row-based.
- [ ] Predicate pushdown: `WHERE date = '2024-01-15'` skip 90 % row groups.
- [ ] Projection pushdown: `SELECT col1, col2` читает только 2 колонки.
- [ ] Compression: ZSTD даёт 3-5× compression ratio.

---

### 44. TPC-C / TPC-H сертификация (R3-044)

**Категория:** K. CI/CD & quality
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** все остальные промпты
**Связь с prompt3.md:** новое.

**Проблема.**
TPC-C / TPC-H — industry-standard benchmarks. Без них невозможно сравнить DieselDB с PostgreSQL / MySQL / ClickHouse.

**Задача.**
1. TPC-C small (10 warehouses): реализация workload, проверка ACID.
2. TPC-H SF=1: 22 queries, проверка корректности результатов + latency.
3. Сравнение с PostgreSQL на тех же ресурсах (4-core VM, 16 GB RAM).
4. Regression tracking: каждый release прогоняет TPC-C/H, метрики в `analytics/perf_history.csv`.
5. Public benchmark report в `docs/benchmarks/`.

**Ключевые файлы.**
- `benchmarks/tpcc/TpcCWorkload.java` (новый)
- `benchmarks/tpch/TpcHQueries.java` (новый, 22 queries)
- `benchmarks/tpch/TpcHResultsVerifier.java` (новый, проверка результатов)
- `benchmarks/BenchmarkRunner.java` (новый)
- `.github/workflows/perf.yml` (расширение для TPC)

**Критерии приёмки.**
- [ ] TPC-C: 10 warehouses, ACID properties passed, throughput > 100 tpmC.
- [ ] TPC-H: 22 queries, результаты совпадают с эталоном.
- [ ] Latency не хуже PG × 2 на TPC-H SF=1.
- [ ] Benchmark report опубликован.

---

### 45. GUI админ-панель (аналог pgAdmin) (R3-045)

**Категория:** — (tooling)
**Приоритет:** LOW
**Фаза:** 3
**Зависимости:** промпт 12 (metrics)
**Связь с prompt3.md:** новое.

**Проблема.**
Без GUI admin-панели разработчикам неудобно: psql-only теряет аудиторию, привыкшую к pgAdmin/DBeaver.

**Задача.**
1. Web UI (Next.js / Vue / Svelte): dashboard с метриками, список таблиц, schema viewer.
2. SQL editor: query input, results table, EXPLAIN visualizer.
3. User management: CRUD users/roles (интеграция с промптом 8).
4. Backup / Restore UI (интеграция с промптом 11).
5. Replication monitor (интеграция с промптом 16).
6. OpenTelemetry tracing viewer (интеграция с промптом 12).

**Ключевые файлы.**
- `admin-ui/` (новый module, separate subproject)
- `admin-ui/src/pages/Dashboard.tsx`
- `admin-ui/src/pages/Tables.tsx`
- `admin-ui/src/pages/QueryEditor.tsx`
- `admin-ui/src/pages/Users.tsx`
- `admin-ui/src/pages/Backups.tsx`
- `admin-ui/src/pages/Replication.tsx`

**Критерии приёмки.**
- [ ] Dashboard показывает QPS, latency, active connections из metrics endpoint.
- [ ] SQL editor: `SELECT * FROM users LIMIT 10` → table with results.
- [ ] Schema viewer показывает список таблиц, колонок, индексов.
- [ ] Backup UI: кнопка "Backup now" запускает `diesel_backup` и показывает прогресс.

---

## Доборочные промпты из `prompt3.md` (без дублирования ROADMAP3)

Промпты ниже перенесены из `prompt3.md` (с #97 и далее), за исключением тех, функционал которых уже покрыт карточками ROADMAP3 (см. таблицу дедупликации в начале документа). Сохранена формулировка оригинала для совместимости с историей планирования; нумерация продолжена.

### 46. Lock — deadlock prevention стратегии (из Промпт 111)

**Приоритет:** MEDIUM (concurrency)
**Зависимости:** промпт 6 (Savepoint + Deadlock + Lock timeout)

**Задача:**
1. Wait-die схема предотвращения deadlock (старшая транзакция ждёт, младшая умирает).
2. Wound-wait схема (старшая убивает младшую, младшая ждёт).
3. No-wait схема с immediate abort при первом конфликте.
4. Конфигурация стратегии через `lock.prevention.policy = wait_die | wound_wait | no_wait`.

**Файлы:**
- `diesel/concurrency/DeadlockPreventionPolicy.java` (новый, интерфейс)
- `diesel/concurrency/WaitDiePolicy.java` (новый)
- `diesel/concurrency/WoundWaitPolicy.java` (новый)
- `diesel/concurrency/NoWaitPolicy.java` (новый)

**Критерии приёмки.**
- [ ] Тест `WaitDieTest`: старая tx ждёт, младшая abort-ится с `TransactionAbortedException`.
- [ ] Тест `WoundWaitTest`: старая tx убивает младшую, младшая abort-ится.
- [ ] Тест `NoWaitTest`: при первом конфликте — immediate abort.
- [ ] Сравнение с detection-based deadlock handling (промпт 6): latency ниже на workload с малым числом конфликтов.

---

### 47. ALTER TABLE ADD COLUMN — базовый SQL (из Промпт 113)

**Приоритет:** HIGH
**Зависимости:** — (является prerequisite для промпта 10 — Online schema changes)

**Задача:**
1. Реализуй `ALTER TABLE table_name ADD COLUMN column_name data_type`.
2. Добавление колонки со значением по умолчанию (`ADD COLUMN col INT DEFAULT 0`).
3. Обновление метаданных таблицы (CatalogTable).
4. Обратная совместимость со старыми данными: существующие строки получают default value для новой колонки.
5. Сериализация обновлённой схемы в catalog.

**Файлы:**
- `diesel/AlterTableAddColumnQuery.java` (новый)
- `diesel/Table.java` (расширение метода addColumn)
- `diesel/CatalogTable.java` (обновление metadata)

**Критерии приёмки.**
- [ ] Тест `AlterAddColumnTest`: ADD COLUMN без default → NULL для существующих строк.
- [ ] ADD COLUMN с DEFAULT 0 → 0 для существующих строк.
- [ ] После restart сервера — колонка сохранена (схема persisted).
- [ ] ADD COLUMN после которой сразу INSERT → INSERT видит новую колонку.

---

### 48. ALTER TABLE DROP COLUMN — базовый SQL (из Промпт 114)

**Приоритет:** MEDIUM
**Зависимости:** промпт 47 (ALTER ADD COLUMN — для симметрии API)

**Задача:**
1. Реализуй `ALTER TABLE table_name DROP COLUMN column_name`.
2. Физическое удаление данных из строк (или lazy deletion через tombstone с последующим compaction).
3. Обновление индексов: удаление записей, указывающих на удалённую колонку.
4. Зависимости: если есть CHECK constraint / foreign key / view — запретить DROP без CASCADE.
5. DROP COLUMN с CASCADE — каскадно удалить зависимые объекты.

**Файлы:**
- `diesel/AlterTableDropColumnQuery.java` (новый)
- `diesel/Table.java` (метод dropColumn)
- `diesel/CatalogTable.java` (обновление metadata)
- `diesel/DependencyChecker.java` (новый, проверка зависимостей)

**Критерии приёмки.**
- [ ] Тест `AlterDropColumnTest`: DROP COLUMN → SELECT * больше не возвращает эту колонку.
- [ ] DROP COLUMN с зависимым CHECK constraint → exception без CASCADE.
- [ ] DROP COLUMN CASCADE → constraint тоже удалён.
- [ ] После restart — колонка не возвращается (persisted).

---

### 49. DROP INDEX (из Промпт 116)

**Приоритет:** MEDIUM
**Зависимости:** —

**Задача:**
1. Реализуй `DROP INDEX index_name ON table_name` (и `DROP INDEX index_name` для schema-level).
2. Удаление структуры индекса из памяти и диска.
3. Освобождение ресурсов: file handles, memory buffers.
4. Обновление метаданных: catalog больше не содержит index.
5. Транзакционность: DROP INDEX в транзакции → rollback восстанавливает index.

**Файлы:**
- `diesel/DropIndexQuery.java` (новый)
- `diesel/IndexManager.java` (метод dropIndex)
- `diesel/CatalogTable.java` (обновление metadata)

**Критерии приёмки.**
- [ ] Тест `DropIndexTest`: CREATE INDEX → DROP INDEX → SELECT не использует индекс (по EXPLAIN).
- [ ] DROP INDEX в транзакции, ROLLBACK → индекс восстановлен.
- [ ] DROP несуществующего index → exception с понятным сообщением.
- [ ] DROP INDEX освобождает память (heap usage до/после — в метриках).

---

### 50. TRUNCATE TABLE (из Промпт 117)

**Приоритет:** HIGH
**Зависимости:** промпт 3 (WAL для minimal logging)

**Задача:**
1. Реализуй `TRUNCATE TABLE table_name`.
2. Быстрое удаление всех данных (без построчного удаления) — обнуление указателя на data pages.
3. Сброс auto-increment counters (если есть SEQUENCE на этой таблице).
4. Минимальное WAL logging: одна запись TRUNCATE вместо N row-records.
5. TRUNCATE в транзакции: rollback восстанавливает данные (через WAL undo).
6. `TRUNCATE TABLE a, b, c` — несколько таблиц за раз.

**Файлы:**
- `diesel/TruncateTableQuery.java` (новый)
- `diesel/Table.java` (метод truncate)
- `diesel/wal/WALManager.java` (запись TRUNCATE record)

**Критерии приёмки.**
- [ ] Тест `TruncateTableTest`: 1M rows → TRUNCATE → 0 rows, < 50 ms.
- [ ] TRUNCATE в транзакции, ROLLBACK → данные восстановлены.
- [ ] TRUNCATE a, b, c → все три пустые за одну операцию.
- [ ] Auto-increment counter сброшен (если есть).

---

### 51. CREATE SEQUENCE (из Промпт 118)

**Приоритет:** MEDIUM
**Зависимости:** —

**Задача:**
1. `CREATE SEQUENCE seq_name START WITH n INCREMENT BY m`.
2. Опциональные параметры: `MINVALUE`, `MAXVALUE`, `CYCLE` / `NO CYCLE`, `CACHE`.
3. Хранение текущего значения в `diesel_sequences` (системная таблица).
4. `NEXTVAL(seq_name)` — следующее значение.
5. `CURRVAL(seq_name)` — текущее (только после NEXTVAL в этой сессии).
6. Кэширование: выдача по N значений за раз (уменьшает contention).
7. Persistence: текущее значение survive restart.

**Файлы:**
- `diesel/CreateSequenceQuery.java` (новый)
- `diesel/SequenceManager.java` (новый/обновлённый)
- `diesel/sequence/Sequence.java` (новый)
- `diesel/sequence/SequenceCache.java` (новый)
- `diesel/QueryParser.java` (синтаксис CREATE SEQUENCE, NEXTVAL, CURRVAL)

**Критерии приёмки.**
- [ ] Тест `CreateSequenceTest`: CREATE SEQUENCE seq START 10 INCREMENT 5 → NEXTVAL возвращает 10, 15, 20, ...
- [ ] CACHE 100: 1000 NEXTVAL → 10 disk reads вместо 1000.
- [ ] `CYCLE`: после MAXVALUE — снова START.
- [ ] После restart — sequence продолжается с последнего persisted значения.

---

### 52. DROP SEQUENCE (из Промпт 119)

**Приоритет:** LOW
**Зависимости:** промпт 51 (CREATE SEQUENCE)

**Задача:**
1. `DROP SEQUENCE sequence_name`.
2. Очистка ресурсов: удаление из `SequenceManager`, удаление persisted state.
3. Проверка зависимостей: если есть DEFAULT NEXTVAL(seq) на колонке → запретить DROP без CASCADE.
4. DROP SEQUENCE CASCADE — каскадно удалить DEFAULT из колонок.

**Файлы:**
- `diesel/DropSequenceQuery.java` (новый)
- `diesel/SequenceManager.java` (метод dropSequence)
- `diesel/DependencyChecker.java` (проверка DEFAULT nextval)

**Критерии приёмки.**
- [ ] Тест `DropSequenceTest`: CREATE → DROP → NEXTVAL → exception.
- [ ] DROP с зависимым DEFAULT → exception без CASCADE.
- [ ] DROP CASCADE → DEFAULT в колонке удалён, column остаётся.
- [ ] Persistence: после restart — sequence не восстановлен.

---

### 53. Query Result Cache (из Промпт 120)

**Приоритет:** MEDIUM (performance)
**Зависимости:** —

**Задача:**
1. Кэширование результатов SELECT запросов в памяти.
2. Ключ: normalized SQL + bind variables hash.
3. TTL-based инвалидация (default 60 sec, configurable).
4. Automatic invalidation при INSERT/UPDATE/DELETE на table — помечать cache entries зависящие от этой таблицы как stale.
5. `cache.size`, `cache.ttl.seconds`, `cache.enabled` конфигурация.
6. `EXPLAIN` показывает hit/miss.
7. Manual flush: `FLUSH CACHE` / `FLUSH CACHE table_name`.

**Файлы:**
- `diesel/cache/QueryResultCache.java` (новый)
- `diesel/cache/CachedResult.java` (новый)
- `diesel/cache/CacheInvalidator.java` (новый, listens to writes)
- `diesel/cache/CacheKey.java` (новый, normalized SQL hash)
- `diesel/QueryExecutor.java` (lookup cache before execution)

**Критерии приёмки.**
- [ ] Тест `QueryResultCacheTest`: SELECT cache hit → повторный запрос < 1 ms.
- [ ] INSERT на table → cache entries этой table помечены stale.
- [ ] TTL истёк → cache miss, новый execution.
- [ ] `EXPLAIN` показывает cache hit/miss.
- [ ] Cache size limit: LRU eviction при достижении `cache.size`.

---

### 54. Bulk Insert / Copy API (из Промпт 121)

**Приоритет:** HIGH (performance)
**Зависимости:** промпт 3 (WAL), промпт 2 (page storage — для batched writes)

**Задача:**
1. `BULK INSERT INTO table_name FROM 'file.csv'` (или `BULK INSERT ... FROM 'file.avro'`).
2. Поддержка форматов: CSV, TSV, JSONL, AVRO.
3. `BATCH SIZE N` — батчинг вставок (default 1000).
4. Опционально: отключение индексов на время загрузки, rebuild после.
5. `COPY FROM` / `COPY TO` PostgreSQL-совместимый синтаксис.
6. Single transaction (по умолчанию) или autocommit каждые N строк.
7. Прогресс репортинга: сколько строк загружено, errors count.

**Файлы:**
- `diesel/BulkInsertQuery.java` (новый)
- `diesel/BulkLoader.java` (новый)
- `diesel/bulk/CopyFromExecutor.java` (новый)
- `diesel/bulk/CopyToExecutor.java` (новый)
- `diesel/QueryParser.java` (синтаксис BULK INSERT / COPY)

**Критерии приёмки.**
- [ ] Тест `BulkInsertTest`: 1M rows CSV → < 30 sec (vs 5 min построчно).
- [ ] Batch size 10000 → оптимальный throughput.
- [ ] Ошибка в строке 5000 (bad format) → отчёт `file:line:column`, остальные строки загружены (если `--on-error=continue`).
- [ ] COPY FROM совместим с `psql \copy`.

---

### 55. Virtual Threads для concurrency (из Промпт 125)

**Приоритет:** LOW (future)
**Зависимости:** — (требует Java 21+)

**Задача:**
1. Интеграция Java Virtual Threads (Project Loom, JEP 444 — Java 21).
2. Замена thread pool (`Executors.newFixedThreadPool`) на `Executors.newVirtualThreadPerTaskExecutor`.
3. Adapt IO-bound operations (network, file) на virtual threads (высокий concurrency, низкая память).
4. CPU-bound operations остаются на platform threads (virtual threads не дают benefit для CPU-bound).
5. Benchmark: virtual threads vs platform threads на 10000 concurrent connections.
6. Config: `concurrency.mode = platform | virtual | hybrid`.

**Файлы:**
- `diesel/concurrent/VirtualThreadScheduler.java` (новый)
- `diesel/concurrent/ThreadModeSelector.java` (новый, decides virtual vs platform per task)
- `diesel/DatabaseServer.java` (использование virtual threads для client handlers)
- `pom.xml` (требование Java 21+)

**Критерии приёмки.**
- [ ] Тест `VirtualThreadConcurrencyTest`: 10000 concurrent SELECT — работает (на platform threads — OOM или thread starvation).
- [ ] Memory: 10000 virtual threads < 100 MB heap (vs 10000 platform threads = OOM).
- [ ] Config switch: можно отключить virtual threads (`concurrency.mode = platform`).
- [ ] CPU-bound queries (агрегации) остаются на platform threads.

---

### 56. Record Patterns для чистоты кода (из Промпт 126)

**Приоритет:** LOW (code quality)
**Зависимости:** — (требует Java 21+)

**Задача:**
1. Refactoring существующего кода с использованием record patterns (JEP 440, Java 21).
2. Pattern matching for switch (JEP 441) — замена каскадов `if instanceof` на `case Type(var a, var b) ->`.
3. Снижение boilerplate в парсере AST nodes: `Query`, `Expression`, `Predicate` и т.д.
4. Замена `instanceof + cast` на `switch (obj) { case Foo foo -> ...; case Bar bar -> ...; }`.
5. Документация: code style guide для новых contributions.

**Файлы:**
- `diesel/Query.java` (и подклассы — refactoring to records where appropriate)
- `diesel/Expression.java` (record patterns)
- `diesel/Predicate.java` (record patterns)
- `diesel/QueryOptimizer.java` (pattern matching switch)
- `docs/code-style-guide.md` (новый)

**Критерии приёмки.**
- [ ] 5+ классов переработаны на records / record patterns (видно в diff).
- [ ] Pattern matching switch в `QueryOptimizer` — заменяет каскад if-instanceof.
- [ ] Все тесты green после refactoring (без изменения поведения).
- [ ] Code style guide опубликован, contributions следуют ему.

---

## Сводная таблица промптов prompt4.md

| # | ID ROADMAP3 / источник | Заголовок | Фаза | Приоритет |
|---|------------------------|-----------|------|-----------|
| 1 | R3-001 | MVCC через версионность строк | 1 | CRITICAL |
| 2 | R3-002 | Page-based storage + buffer pool (LRU) | 1 | CRITICAL |
| 3 | R3-003 | WAL + group commit | 1 | CRITICAL |
| 4 | R3-004 | ARIES Recovery Manager | 1 | CRITICAL |
| 5 | R3-005 | Background writer / flusher | 1 | HIGH |
| 6 | R3-006 | Savepoint + Deadlock + Lock timeout | 1 | HIGH |
| 7 | R3-007 | Замена Java Object Serialization в net-протоколе | 1 | CRITICAL |
| 8 | R3-008 | RBAC + Audit log | 1 | HIGH |
| 9 | R3-009 | SSL/TLS transport | 1 | HIGH |
| 10 | R3-010 | Online schema changes (ALTER без блокировки) | 1 | HIGH |
| 11 | R3-011 | Backup / Restore (logical + physical) | 1 | HIGH |
| 12 | R3-012 | Metrics + Prometheus + Health check | 1 | HIGH |
| 13 | R3-013 | Fix blocking bugs из problems.md | 1 | CRITICAL |
| 14 | R3-014 | Checkpoint + Checksummed pages (CRC32C) | 2 | HIGH |
| 15 | R3-015 | libpq + JDBC/Python/Node/Go/Rust drivers | 2 | HIGH |
| 16 | R3-016 | Replication (logical + physical + Raft) | 2 | HIGH |
| 17 | R3-017 | Partitioning (range/list/hash) | 2 | MEDIUM |
| 18 | R3-018 | Cost-Based Optimizer + статистика | 2 | HIGH |
| 19 | R3-019 | UPSERT / RETURNING / ON CONFLICT | 2 | HIGH |
| 20 | R3-020 | CI/CD v2 — coverage, matrix, releases | 2 | HIGH |
| 21 | R3-021 | CTE + рекурсивные CTE | 3 | MEDIUM |
| 22 | R3-022 | Оконные функции | 3 | MEDIUM |
| 23 | R3-023 | FULL OUTER JOIN, LATERAL, CROSS APPLY | 3 | MEDIUM |
| 24 | R3-024 | UNION / INTERSECT / EXCEPT | 3 | MEDIUM |
| 25 | R3-025 | Foreign Keys с CASCADE | 3 | MEDIUM |
| 26 | R3-026 | CHECK constraints, NOT NULL, DEFAULT | 3 | MEDIUM |
| 27 | R3-027 | Materialized Views + refresh | 3 | LOW |
| 28 | R3-028 | Triggers BEFORE/AFTER | 3 | LOW |
| 29 | R3-029 | VIEW (non-materialized) | 3 | LOW |
| 30 | R3-030 | Full-text search | 3 | LOW |
| 31 | R3-031 | Расширенные типы (JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT) | 3 | MEDIUM |
| 32 | R3-032 | Хранимые функции (SQL-only) | 3 | LOW |
| 33 | R3-033 | Шардинг + distributed query planner | 3 | MEDIUM |
| 34 | R3-034 | 2PC distributed transactions | 3 | LOW |
| 35 | R3-035 | Row-Level Security (RLS) | 3 | MEDIUM |
| 36 | R3-036 | Column-level privileges | 3 | LOW |
| 37 | R3-037 | TDE at rest encryption | 3 | MEDIUM |
| 38 | R3-038 | Vectorized execution (batch) | 3 | MEDIUM |
| 39 | R3-039 | Adaptive joins (runtime switching) | 3 | LOW |
| 40 | R3-040 | Parallel query scan + aggregation | 3 | MEDIUM |
| 41 | R3-041 | Bitmap indexes | 3 | LOW |
| 42 | R3-042 | Covering indexes (INCLUDE) | 3 | LOW |
| 43 | R3-043 | Parquet storage (нативный) | 3 | MEDIUM |
| 44 | R3-044 | TPC-C / TPC-H сертификация | 3 | LOW |
| 45 | R3-045 | GUI админ-панель | 3 | LOW |
| 46 | Промпт 111 | Lock — deadlock prevention стратегии | — | MEDIUM |
| 47 | Промпт 113 | ALTER TABLE ADD COLUMN — базовый SQL | — | HIGH |
| 48 | Промпт 114 | ALTER TABLE DROP COLUMN — базовый SQL | — | MEDIUM |
| 49 | Промпт 116 | DROP INDEX | — | MEDIUM |
| 50 | Промпт 117 | TRUNCATE TABLE | — | HIGH |
| 51 | Промпт 118 | CREATE SEQUENCE | — | MEDIUM |
| 52 | Промпт 119 | DROP SEQUENCE | — | LOW |
| 53 | Промпт 120 | Query Result Cache | — | MEDIUM |
| 54 | Промпт 121 | Bulk Insert / Copy API | — | HIGH |
| 55 | Промпт 125 | Virtual Threads для concurrency | — | LOW |
| 56 | Промпт 126 | Record Patterns для чистоты кода | — | LOW |

---

## Рекомендуемый порядок выполнения

### Sprint 1 (недели 1-4): фундамент
- 2 (Page storage + buffer pool)
- 7 (Замена Java-сериализации в протоколе)
- 13b, 13c, 13d — быстрые фиксы парсера

### Sprint 2 (недели 5-8): MVCC + WAL
- 1 (MVCC) — большой кусок, 4 недели
- 3 (WAL + group commit) — параллельно
- 13a (стабильные row-id) — часть промпта 2

### Sprint 3 (недели 9-12): recovery + observability
- 4 (ARIES recovery)
- 5 (Background writer)
- 12 (Metrics + Prometheus) — параллельно

### Sprint 4 (недели 13-16): security + ops
- 6 (Savepoint + Deadlock + Lock timeout)
- 8 (RBAC + audit)
- 9 (TLS transport)

### Sprint 5 (недели 17-20): polish
- 10 (Online schema changes)
- 11 (Backup / restore)
- 13 — закрытие всех оставшихся багов
- 47, 48 (базовый ALTER) — prereq для 10
- Performance testing, bug bash, документация

### Sprint 6 (недели 21-24): buffer
- Регрессии, edge cases, hardening
- 46 (deadlock prevention), 50 (TRUNCATE), 54 (Bulk Insert) — полезные дополнения
- Подготовка release notes
- Migration guide для существующих пользователей

### Фаза 2 (6-12 мес): multi-tenant production
- 14, 15, 16, 17, 18, 19, 20

### Фаза 3 (12-24+ мес): PostgreSQL-parity
- 21-45 (по приоритету внутри фазы)
- 49, 51, 52, 53, 55, 56 — по мере необходимости

---

## Примечания

1. **Параллелизм.** Промпты 1, 2, 3, 7 — фундаментальные, могут делаться параллельно при наличии 2+ разработчиков.
2. **Документация.** После завершения каждой задачи: обновление `KNOWN_LIMITATIONS.md`, `PERSISTENCE_README.md`, `README.md`.
3. **Версионирование.** Каждый промпт — отдельный feature branch → PR → squash merge.
4. **Совместимость.** Весь Фаза 1 поддерживает legacy format через feature flags.
5. **Benchmarks.** До/после каждого промпта — измерение на 3 scenarios: (a) 1k rows, (b) 100k rows, (c) 1M rows; сохранение в `analytics/performance_history.csv`.
6. **Backward-compat тесты.** Перед закрытием PR: все существующие тесты green; regression > 10 % latency — блокирующий merge.
