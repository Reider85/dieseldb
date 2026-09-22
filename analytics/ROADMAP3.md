# ROADMAP3.md — DieselDB Production-Readiness Roadmap (полная картина)

> **Документ:** ROADMAP3.md  
> **Версия:** 1.0  
> **Дата:** 2026-09-19  
> **Горизонт:** Фаза 1 — 3-6 месяцев (critical production-ready), Фаза 2 — 6-12 месяцев (production-grade), Фаза 3 — 12-24+ месяцев (PostgreSQL-parity)  
> **Принцип приоритизации:** по критичности (CRITICAL → HIGH → MEDIUM → LOW); внутри приоритета — по топологии зависимостей (фундамент раньше верхушек)  
> **Связь с другими документами:** дополнение к `prompt3.md` (131 промпт), `Roadmap.md` (5 этапов), `Roadmap_Parquet_Stage1.md`, `resilence.md`, `replication.md`, `monitoring.md`, `cicd.md`, `future.md`, `problems.md`, `PERSISTENCE_README.md`, `KNOWN_LIMITATIONS.md`.

---

## 0. Назначение и границы

Этот документ покрывает **пробелы**, не отражённые ни в `prompt3.md`, ни в `Roadmap.md`, ни в `Roadmap_Parquet_Stage1.md` — и систематизирует их как production-readiness roadmap. Задачи, уже описанные в перечисленных документах, здесь **не дублируются**, но на них даются ссылки в секции 5 «Связь с существующими промптами».

**Что считается production-ready в этом документе:**
1. **Durability** — данные не теряются при краше питания, восстановление за < 30 с.
2. **Concurrency** — десятки параллельных транзакций без деградации, deadlock-безопасность.
3. **Security** — аутентификация, шифрование на transport и at-rest, аудит, RBAC.
4. **Operability** — мониторинг, бэкапы, online schema changes, graceful shutdown.
5. **Correctness** — отсутствие тихой порчи данных, известные баги из `problems.md` устранены.

**Что НЕ входит в scope этого roadmap** (сознательно отложено):
- хранимые процедуры, PL/pgSQL-подобный язык;
- GUI типа pgAdmin (отдельный проект);
- полная parity с PostgreSQL по SQL:99/SQL:2023.

---

## 1. Сводная таблица готовности по категориям

Категории A-M из предыдущего анализа. Текущее значение — оценка по коду и `Roadmap.md`; целевое — к концу Фазы 1 (3-6 мес), Фазы 2 (6-12 мес), Фазы 3 (12-24+ мес).

| # | Категория | Текущее | После Фазы 1 | После Фазы 2 | После Фазы 3 |
|---|-----------|---------|--------------|--------------|--------------|
| A | Durability & Recovery | 10% | 70% | 90% | 95% |
| B | Concurrency & Locking | 25% | 60% | 85% | 95% |
| C | Storage engine | 20% | 50% | 75% | 90% |
| D | Replication & HA | 0% | 10% | 50% | 85% |
| E | Sharding & Partitioning | 0% | 0% | 25% | 70% |
| F | Security | 10% | 50% | 80% | 90% |
| G | SQL coverage | 35% | 50% | 70% | 85% |
| H | Query optimizer | 15% | 30% | 60% | 85% |
| I | Network protocol & drivers | 15% | 25% | 60% | 85% |
| J | Observability & operations | 20% | 60% | 80% | 90% |
| K | CI/CD & quality | 25% | 70% | 85% | 90% |
| L | Serialization formats | 55% | 70% | 85% | 95% |
| M | Correctness / blocking bugs | 50% | 85% | 95% | 99% |
| **Общая production-readiness** | **~22%** | **~55%** | **~75%** | **~90%** |

> Метрика 22 % как «стартовая точка» согласуется с самооценкой проекта в `Roadmap.md` (25-30 %). После Фазы 1 проект становится «минимально production-пригодным» для low-stakes single-tenant сценариев; Фаза 2 — для multi-tenant production; Фаза 3 — для high-load production.

---

## 2. Граф зависимостей задач

```
                    ┌────────────────────────────┐
                    │ R3-001 MVCC versions       │ ──┐
                    │ R3-002 Page storage + LRU  │   │
                    │ R3-003 WAL + group commit  │   │
                    └─────────────┬──────────────┘   │
                                  │                  │
                                  ▼                  ▼
                    ┌────────────────────────────┐
                    │ R3-004 ARIES Recovery      │ ← depends on R3-003
                    │ R3-005 Background writer   │ ← depends on R3-002
                    │ R3-006 Savepoint/Deadlock  │ ← depends on R3-001
                    │ R3-007 Replace Java serial │ (parallel)
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ R3-008 RBAC + Audit        │
                    │ R3-009 SSL/TLS transport   │
                    │ R3-010 Online schema       │ ← depends on R3-007
                    │ R3-011 Backup / Restore    │ ← depends on R3-003
                    │ R3-012 Metrics + Prometheus│
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ R3-013 Logical replication │ ← depends on R3-003
                    │ R3-014 libpq protocol      │ ← depends on R3-007
                    │ R3-015 CBO + statistics    │ ← depends on R3-001
                    │ R3-016 Partitioning       │ ← depends on R3-002
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Phase 3: full SQL parity   │
                    │ (CTE, Window, RLS, 2PC...) │
                    └────────────────────────────┘
```

---

## 3. Карточка задачи — формат

Каждая задача ниже описана карточкой:

- **ID**: R3-NNN (сквозная нумерация внутри этого документа, не продолжение prompt3.md).
- **Категория**: A-M.
- **Приоритет**: CRITICAL / HIGH / MEDIUM / LOW.
- **Фаза**: 1 / 2 / 3.
- **Зависимости**: от каких R3-NNN.
- **Связь с prompt3.md**: «дополняет Промпт X» / «новое, не покрыто» / «уточняет Промпт X».
- **Проблема**: кратко, в чём дыра.
- **Задача**: что конкретно сделать.
- **Ключевые файлы**: какие классы создать/изменить.
- **Критерии приёмки (DoD)**: чек-лист «готово когда…».

---

## 4. Фаза 1 — Critical Production-Readiness (3-6 месяцев)

**Цель фазы:** закрыть 10 главных дыр, без которых DieselDB **нельзя** назвать production-ready даже для single-tenant low-stakes применений. По завершении — метрика готовности ~55 %, всё CRITICAL закрыто.

### R3-001: MVCC через версионность строк

**Категория:** B. Concurrency  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости:** — (фундамент)  
**Связь с prompt3.md:** новoe, не покрыто. В `Roadmap.md` упомянуто как «уже есть», но фактически уровни изоляции реализованы через глубокое клонирование таблиц сериализацией — это не MVCC. `problems.md` §Транзакции-1 описывает проблему.

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

### R3-002: Page-based storage + buffer pool (LRU)

**Категория:** C. Storage engine  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости:** — (фундамент, но концептуально не блокирует R3-001; можно делать параллельно)  
**Связь с prompt3.md:** новoe, не покрыто. Упоминается в `problems.md` (Map-per-row ~48 байт на entry).

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

### R3-003: WAL + group commit

**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости:** —  
**Связь с prompt3.md:** уточняет Промпт 97 (WAL базовая реализация). Промпт 97 не покрывает group commit — без этого latency коммита недопустимая на проде.

**Проблема.**  
Сейчас на COMMIT происходит `saveTablesToDisk()` — сериализация всех таблиц полностью. Для таблицы 100k строк это секунды. Fsync на каждый commit убьёт throughput (десятки commits/sec максимум). Аварийное отключение теряет все изменения с последнего `saveTablesToDisk()`. Group commit не упоминается ни в одном документе проекта.

**Задача.**  
1. Реализовать WAL как сегментированный бинарный лог (`wal-0001.log`, `wal-0002.log`, ...), каждый entry — LSN + txid + operation + before/after image + CRC32C.
2. На COMMIT: добавить commit record в WAL, **один** fsync для группы транзакций (group commit, оконo 5-10 ms или 64 транзакций — что раньше).
3. Concurrent writers в WAL — single-writer thread + queue (`WALWriter`), readers — все.
4. WAL segment rotation: при достижении 64 MB или `wal.segment.max.age.ms` = 5 min.
5. Config: `wal.fsync.policy = always | group | everysec | none` (default `group`).
6. Backward compat: текущая сериализация `.table` остаётся для cold backup, но не для durability.

**Ключевые файлы.**  
- `diesel/wal/WALManager.java` (новый)
- `diesel/wal/WALEntry.java` (новый)
- `diesel/wal/WALWriter.java` (новый, single-writer)
- `diesel/wal/WALSegment.java` (новый)
- `diesel/wal/GroupCommitCoordinator.java` (новый)
- `diesel/Transaction.java` (изменение пути COMMIT)

**Критерии приёмки.**  
- [ ] Throughput COMMIT > 5000/sec на 4-core VM (сейчас < 50/sec на 100k-строчной таблице).
- [ ] p99 commit latency < 20 ms в режиме `group`, < 1 ms в режиме `none` (для dev).
- [ ] После `kill -9` процесса и рестарта — все закоммиченные транзакции на месте, ни одной потери.
- [ ] Незакоммиченная транзакция полностью откатывается (см. R3-004).
- [ ] Тест `WALCrashRecoveryTest`: 10k коммитов, kill, restart, проверка целостности.

---

### R3-004: ARIES Recovery Manager

**Категория:** A. Durability & Recovery  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости:** R3-003 (WAL)  
**Связь с prompt3.md:** уточняет Промпт 99 (ARIES Recovery Manager). В `prompt3.md` описан алгоритм, но не детализованы integration-точки с MVCC и page storage.

**Проблема.**  
Без recovery WAL бесполезен: после краша нужно replay журнала. Промпт 99 даёт концепцию, но integration с MVCC (R3-001) и page storage (R3-002) не описан — это и есть пробел ROADMAP3.

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
- `diesel/DatabaseServer.java` (изменение startup sequence)

**Критерии приёмки.**  
- [ ] Recovery после краша на WAL 1 GB — < 30 секунд.
- [ ] Тест `RecoveryIntegrationTest`: 100 смешанных транзакций, 50 commit / 50 no-commit, kill, restart → 50 видны, 50 откачены.
- [ ] Тест `RecoveryWithLongTransactionTest`: 1 длинная транзакция на 1M insert + kill посередине → все её изменения откачены.
- [ ] Метрика `recovery.duration.ms` экспортируется в metrics endpoint.

---

### R3-005: Background writer / flusher

**Категория:** A. Durability & Recovery  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости:** R3-002 (page storage), R3-003 (WAL)  
**Связь с prompt3.md:** новoe, не покрыто. Упоминается в `resilence.md`, но **не попал** в `prompt3.md` и `Roadmap.md`.

**Проблема.**  
Если dirty pages пишутся на диск только на checkpoint, восстановление после краша будет долгим (много WAL нужно replay). Нужен фоновый writer, который мягко флашит dirty pages в фоне, не блокируя writers.

**Задача.**  
1. Background thread `BufferPoolFlusher`: каждые N ms сканирует buffer pool на dirty pages.
2. Адаптивная стратегия: если dirty pages > 25 % буфера — flush агрессивнее; если < 5 % — реже.
3. Запись dirty page: атомарно (temp file + rename), через `PageManager.writePage()`.
4. Не флашить страницы, чей LSN > lastWALFlushLSN (иначе нарушение WAL rule).
5. Checkpoint (каждые 5 min или N MB WAL): sync flush всех dirty + запись checkpoint record в WAL.

**Ключевые файлы.**  
- `diesel/storage/page/BufferPoolFlusher.java` (новый)
- `diesel/storage/page/CheckpointManager.java` (новой, интегрирован с WAL)
- `diesel/ConfigLoader.java` (новые ключи: `bufferpool.flush.interval.ms`, `bufferpool.dirty.threshold`)

**Критерии приёмки.**  
- [ ] Recovery после краша при active workload — < 10 секунд (мало WAL нужно replay).
- [ ] Throughput writers не падает больше чем на 10 % при включённом flusher.
- [ ] Тест `BackgroundFlusherTest`: 100k inserts без COMMIT, kill, restart → все inserts либо закоммичены (если были commit records), либо откачены.

---

### R3-006: Savepoint + Deadlock detector + Lock timeout

**Категория:** B. Concurrency & Locking  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости:** R3-001 (MVCC), R3-003 (WAL для savepoint records)  
**Связь с prompt3.md:** уточняет Промпты 104 (Deadlock), 105 (Lock timeout), 106-107 (Savepoint). Промпты описывают задачи, но без интеграции с MVCC.

**Проблема.**  
С появлением MVCC (R3-001) нужны: deadlock detection для wait-for graph, lock timeout (иначе вечное ожидание), savepoints (частичный откат транзакции). В коде нет ни одного из этих классов.

**Задача.**  
1. `LockManager`: гранулярные блокировки (table-level / row-level), совместимость по матрице режимов (S/X/IS/IX).
2. `WaitForGraph`: периодическая проверка (каждые 500 ms) на циклы; при обнаружении — жертва = транзакция с наименьшим txid.
3. `LockTimeout`: настраиваемый `lock.timeout.ms` (default 30000); по истечении — `LockTimeoutException`.
4. `SavepointManager`: `SAVEPOINT name`, `ROLLBACK TO name`, `RELEASE name` — частичный undo через undo log (R3-001).
5. WAL records для savepoint create/release.

**Ключевые файлы.**  
- `diesel/concurrency/LockManager.java` (новый)
- `diesel/concurrency/Lock.java` (новый)
- `diesel/concurrency/WaitForGraph.java` (новый)
- `diesel/concurrency/DeadlockDetector.java` (новый)
- `diesel/concurrency/SavepointManager.java` (новый)
- `diesel/concurrency/LockTimeoutException.java` (новый)
- `diesel/QueryParser.java` (парсинг SAVEPOINT / ROLLBACK TO)

**Критерии приёмки.**  
- [ ] Тест `DeadlockTest`: 2 транзакции в цикле → одна убита `DeadlockVictimException`.
- [ ] Тест `LockTimeoutTest`: транзакция ждёт > `lock.timeout.ms` → `LockTimeoutException`.
- [ ] Тест `SavepointTest`: SAVEPOINT + частичный ROLLBACK TO восстанавливает состояние до savepoint, не теряя изменения после savepoint.
- [ ] Метрики `deadlock.count`, `lock.timeout.count`, `savepoint.count` экспортируются.

---

### R3-007: Замена Java Object Serialization в сетевом протоколе

**Категория:** F. Security + L. Serialization  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости:** — (можно делать параллельно с R3-001/002/003)  
**Связь с prompt3.md:** новoe, не покрыто. `problems.md` §Сериализация-6 указывает проблему, но ни в одном плане её нет. Это **RCE-риск** — Java deserialization gadgets.

**Проблема.**  
`DatabaseClient` / `DatabaseServer` общаются через `ObjectInputStream.readObject()` — это известная RCE-уязвимость (deserialization gadgets: commons-collections, spring, etc.). Любой клиент может послать сериализованный payload и получить RCE на сервере. Для production это **blocker**.

**Задача.**  
1. Спроектировать бинарный wire-протокол (TLS-like handshake → length-prefixed messages → CRC32 footer).
2. Каждый message тип — явный класс `XxxMessage` с методами `writeTo(DataOutput)` / `readFrom(DataInput)`.
3. Никакого `ObjectInputStream` — только явные поля.
4. Опционально: сжатие сообщений > 4 KB через ZSTD (использует существующий `CompressionCodec`).
5. Совместимость: новый протокол version 2; старые клиенты подключаются к порту legacy (deprecation warning); удаление legacy протокола — Фаза 2.
6. Раздельный message types: `QueryMessage`, `QueryResultMessage`, `PrepareMessage`, `ExecutePreparedMessage`, `BatchQueryMessage`, `HealthCheckMessage`, `CompressionHandshakeMessage`.

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

### R3-008: RBAC + Audit log

**Категория:** F. Security  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости:** — (R3-007 для transport; R3-001 для session-scoped role)  
**Связь с prompt3.md:** новoe. В `Roadmap.md` Этап 5 упоминается RBAC, но без деталей и слишком поздно (6-12 мес). Для production нужно раньше.

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

---

### R3-009: SSL/TLS transport

**Категория:** F. Security  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости:** R3-007 (новый wire protocol)  
**Связь с prompt3.md:** новoe, частично в `Roadmap.md` Этап 5. Нужно раньше — без TLS нельзя пускать трафик через сеть.

**Проблема.**  
Даже с новым wire-протоколом (R3-007) данные идут в открытом виде. SQL-запросы, результаты, пароли на handshake — всё читаемо в MITM.

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

### R3-010: Online schema changes (ALTER без блокировки)

**Категория:** C. Storage engine + G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости:** R3-002 (page storage — для in-place schema), R3-007 (для coordinated schema version broadcast)  
**Связь с prompt3.md:** новoe. Промпты 113-114 (ALTER TABLE ADD/DROP COLUMN) описывают SQL-парсинг, но не online-семантику — без неё ALTER блокирует writers.

**Проблема.**  
Production БД должна менять схему без даунтайма. Текущий `ALTER TABLE` (когда будет реализован в Промптах 113-114) заблокирует таблицу на время операции.

**Задача.**  
1. ALGORITHM=cop y: новая версия таблицы создаётся в фоне, writers дублируются в старую и новую, по окончании — atomic rename.
2. ALGORITHM=inplace: где возможно (ADD COLUMN nullable, DROP COLUMN через tombstone), без копирования.
3. LOCK=NONE / SHARED / EXCLUSIVE — уровень блокировки во время ALTER.
4. Schema version в catalog: readers видят consistent snapshot своей версии schema.
5. Не блокирует readers (на R3-001 MVCC + на schema version).
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

---

### R3-011: Backup / Restore (logical + physical)

**Категория:** J. Observability & operations  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости:** R3-003 (WAL — для consistent snapshot), R3-004 (Recovery — для restore)  
**Связь с prompt3.md:** новoe. В `Roadmap.md` Этап 4 упоминается «аналог pg_dump/pg_restore», но без деталей.

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
- `diesel/CliRepl.java` (новые команды `BACKUP`, `RESTORE`)

**Критерии приёмки.**  
- [ ] `diesel_dump` на 1M-строчной базе — < 30 с, файл < 100 MB.
- [ ] `diesel_restore` из дампа — данные идентичны исходным (10 тестовых таблиц сравниваются row-by-row).
- [ ] Physical backup работающей базы (writers active) — consistent на момент backup start.
- [ ] PITR: restore на момент «5 минут назад» — точно соответствует состоянию БД в тот момент.

---

### R3-012: Metrics + Prometheus + Health check

**Категория:** J. Observability  
**Приоритет:** HIGH  
**Фаза:** 1  
**Зависимости:** — (можно параллельно с остальным)  
**Связь с prompt3.md:** новoe. В `monitoring.md` и `Roadmap.md` Этап 4 описано, но без конкретики.

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

### R3-013: Fix всех blocking bugs из `problems.md` и `PERSISTENCE_README.md`

**Категория:** M. Correctness  
**Приоритет:** CRITICAL  
**Фаза:** 1  
**Зависимости:** — (можно параллельно)  
**Связь с prompt3.md:** уточняет Промпт 25 (стабильные row-id) и косвенно Промпт 35 (батчинг rebuild). Остальные баги (TRUE, регистр строковых литералов, `indexDefinitions` сериализация) — новoe, не покрыто.

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
Закрыть все 7 багов. R3-001 решает (2) и (6). R3-005 решает (5). Остальные — отдельные фиксы.
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

## 5. Связь с существующими промптами

Таблица: что из ROADMAP3 дополняет / уточняет / заменяет существующие промпты из `prompt3.md` или этапы из `Roadmap.md`.

| ROADMAP3 ID | Связано с | Тип связи | Комментарий |
|-------------|-----------|-----------|-------------|
| R3-001 | `Roadmap.md` Этап 1 (Savepoints / MVCC) | уточняет | Roadmap считал MVCC уже есть; реально — нет. |
| R3-002 | — | новoe | Page storage не упоминался. |
| R3-003 | Промпт 97 (WAL базовая) | уточняет | Добавлены group commit + segment rotation (Промпт 108). |
| R3-004 | Промпт 99 (ARIES) | уточняет | Промпт 99 описан концептуально, добавлены integration точки с MVCC/pages. |
| R3-005 | `resilence.md` | новoe | Background writer упомянут в resilience, но не попал в prompt3/Roadmap. |
| R3-006 | Промпты 104-107 | уточняет | Конкретизирует интеграцию deadlock/lock/savepoint с MVCC. |
| R3-007 | `problems.md` §Сер-6 | новoe | Замена Java-сериализации в сети не покрыта нигде. |
| R3-008 | `Roadmap.md` Этап 5 (RBAC) | ускоряет | Roadmap планировал RBAC на 6-12 мес — слишком поздно. |
| R3-009 | `Roadmap.md` Этап 5 (SSL) | ускоряет | То же. |
| R3-010 | Промпты 113-114 (ALTER) | уточняет | Промпты описывают SQL-парсинг; добавлена online-семантика. |
| R3-011 | `Roadmap.md` Этап 4 (pg_dump analog) | уточняет | Добавлены physical/incremental/PITR. |
| R3-012 | `monitoring.md`, `Roadmap.md` Этап 4 | уточняет | Конкретизация endpoints и метрик. |
| R3-013 | Промпт 25, Промпт 35 | уточняет | Промпты описаны, но не сделаны; добавлены 4 новых бага. |
| R3-014 (Фаза 2) | Промпты 100-103 (Checkpoint, Checksum) | ускоряет | Включение в Фазу 2 вместо раздельных задач. |
| R3-015 (Фаза 2) | `Roadmap.md` Этап 4 (libpq, drivers) | уточняет | Порядок реализации. |
| R3-016 (Фаза 2) | `Roadmap.md` Этап 5 (Replication) | уточняет | Добавлены logical + quorum. |
| R3-017 (Фаза 3) | `Roadmap.md` Этап 5 (Sharding) | уточняет | Конкретизация подхода. |
| R3-018 (Фаза 3) | `Roadmap.md` Этап 2 (CBO) | уточняет | CBO требует MVCC + статистики. |
| R3-019 (Фаза 3) | Промпт 131 (Window functions) | уточняет | Запланирован в Фазу 3. |
| R3-020 (Фаза 3) | `Roadmap.md` Этап 2 (CTE, recursion, FULL JOIN, LATERAL) | уточняет | Фаза 3. |

---

## 6. Фаза 2 — Production-Grade (6-12 месяцев)

**Цель фазы:** закрыть HIGH-приоритетные пробелы, без которых DieselDB не подходит для multi-tenant production. По завершении — метрика готовности ~75 %.

### R3-014: Checkpoint + Checksummed pages (CRC32C)

**Категория:** A. Durability  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости:** R3-003, R3-004, R3-005  
**Связь с prompt3.md:** уточняет Промпты 100-103.

**Проблема.**  
Fuzzy checkpoint нужен для ограничения WAL replay; checksummed pages — для детекции silent corruption (bit rot). Промпты описаны, но интегрировать с R3-001/R3-002/R3-003 нужно явно.

**Задача.**  
1. Fuzzy checkpoint: не ждать quiescent state, а делать consistent snapshot с активными транзакциями.
2. CRC32C на каждой странице (header + payload).
3. На чтение страницы: verify checksum, при провале — `PageCorruptedException`, попытка восстановить из WAL redo.
4. Background scrubber: раз в N дней — все страницы verify, репорт corrupted.

**Ключевые файлы.**  
- `diesel/storage/page/PageChecksum.java` (новый, CRC32C)
- `diesel/recovery/FuzzyCheckpoint.java` (новый)
- `diesel/storage/page/PageScrubber.java` (новый)

**Критерии приёмки.**  
- [ ] Тест `CorruptionDetectionTest`: битый байт в странице → `PageCorruptedException`.
- [ ] Recovery после fuzzy checkpoint — без replay всего WAL.
- [ ] Scrubber на 100 GB базе — < 1 часа, не блокирует writers.

---

### R3-015: libpq protocol + JDBC + Python + Node.js + Go + Rust drivers

**Категория:** I. Network & drivers  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости:** R3-007 (wire protocol)  
**Связь с prompt3.md:** новoe, частично в `Roadmap.md` Этап 4.

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

---

### R3-016: Replication (logical + physical + quorum-based)

**Категория:** D. Replication & HA  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости:** R3-003 (WAL), R3-004 (Recovery), R3-007 (transport)  
**Связь с prompt3.md:** новoe. `Roadmap.md` Этап 5 упоминает, но без деталей. `replication.md` описывает теорию.

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

**Критерии приёмки.**  
- [ ] Streaming replication: lag slave-behind-master < 100 ms на workload 1000 inserts/sec.
- [ ] Synchronous: COMMIT подтверждён только после ack от 1 sync standby.
- [ ] Failover: при `kill -9` мастера, новый лидер выбран < 5 сек.
- [ ] Logical replication: INSERT на мастере → INSERT на подписчике < 1 сек.
- [ ] Replication slot: при offline standby > 5 min — WAL не удаляется, standby может догнать.

---

### R3-017: Partitioning (range / list / hash)

**Категория:** E. Sharding & Partitioning  
**Приоритет:** MEDIUM  
**Фаза:** 2  
**Зависимости:** R3-002 (page storage), R3-010 (online schema для partition management)  
**Связь с prompt3.md:** новoe. `Roadmap.md` Этап 5 упоминает.

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

---

### R3-018: Cost-Based Optimizer + статистика

**Категория:** H. Query optimizer  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости:** R3-001 (MVCC для snapshot статистики), R3-013a (row-id для выборки)  
**Связь с prompt3.md:** новoe, косвенно Промпт 14 (статистика). `Roadmap.md` Этап 2.

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

---

### R3-019: UPSERT / RETURNING / UPSERT-on-conflict

**Категория:** G. SQL coverage  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости:** —  
**Связь с prompt3.md:** новoe.

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

---

### R3-020: CI/CD v2 — coverage, matrix, releases

**Категория:** K. CI/CD  
**Приоритет:** HIGH  
**Фаза:** 2  
**Зависимости:** —  
**Связь с prompt3.md:** новoe. `cicd.md` P0/P1 описано, но не сделано.

**Проблема.**  
Сейчас: только 2 теста из ~100 запускаются в CI; нет coverage; нет release pipeline.

**Задача.**  
1. Surefire pattern fix: `**/*Test.java, **/*Tests.java, **/Test*.java`.
2. JaCoCo + Codecov, coverage threshold 60 %, master > 70 %.
3. Matrix build: JDK 17 / 21 / 25 (LTS), Windows / Linux / macOS.
4. SonarQube Quality Gate блокирует merge при bugs > 0 / critical > 0.
5. Release pipeline: tag → Maven Central + GitHub Release + Docker image.
6. Performance regression: TPC-C small (10 warehouses) и TPC-H small (SF=1) д weekly.
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

## 7. Фаза 3 — PostgreSQL-Parity (12-24+ месяцев)

**Цель фазы:** достичь ~90 % production-readiness и ~85 % PostgreSQL SQL coverage. Краткое описание задач без карточек (детализация — при переходе к Фазе 3).

| ID | Название | Категория | Приоритет | Зависимости |
|----|----------|-----------|-----------|-------------|
| R3-021 | CTE + рекурсивные CTE | G | MEDIUM | R3-018 (CBO) |
| R3-022 | Оконные функции (ROW_NUMBER, RANK, LAG, LEAD, NTILE) | G | MEDIUM | — |
| R3-023 | FULL OUTER JOIN, LATERAL JOIN, CROSS APPLY | G | MEDIUM | — |
| R3-024 | UNION / INTERSECT / EXCEPT | G | MEDIUM | — |
| R3-025 | Foreign Keys с CASCADE | G | MEDIUM | R3-010 (online alter) |
| R3-026 | CHECK constraints, NOT NULL, DEFAULT | G | MEDIUM | — |
| R3-027 | Materialized Views + refresh | G | LOW | — |
| R3-028 | Triggers BEFORE/AFTER | G | LOW | R3-001 (MVCC) |
| R3-029 | VIEW (non-materialized) | G | LOW | — |
| R3-030 | Full-text search (GiST/GIN-аналоги) | G | LOW | R3-018 (CBO) |
| R3-031 | Типы: JSONB, UUID, ARRAY, ENUM, INTERVAL, INET, BIT | G | MEDIUM | — |
| R3-032 | Хранимые функции (SQL-only, без PL/pgSQL) | G | LOW | — |
| R3-033 | Шардинг + distributed query planner | E | MEDIUM | R3-016 (replication), R3-017 (partition) |
| R3-034 | 2PC distributed transactions | D, E | LOW | R3-016 |
| R3-035 | Row-Level Security (RLS) | F | MEDIUM | R3-008 (RBAC) |
| R3-036 | Column-level privileges | F | LOW | R3-008 |
| R3-037 | TDE at rest encryption | F | MEDIUM | R3-002 (page) |
| R3-038 | Vectorized execution (batch) | H | MEDIUM | R3-018 |
| R3-039 | Adaptive joins (runtime switching) | H | LOW | R3-018 |
| R3-040 | Parallel query scan + aggregation | H | MEDIUM | R3-018 |
| R3-041 | Bitmap indexes | C | LOW | — |
| R3-042 | Covering indexes (INCLUDE) | C | LOW | — |
| R3-043 | Parquet storage (нативный, не через AVRO) | C, L | MEDIUM | R3-002 |
| R3-044 | TPC-C / TPC-H сертификация | K | LOW | Все остальное |
| R3-045 | GUI админ-панель (аналог pgAdmin) | — | LOW | R3-012 (metrics) |

---

## 8. Риски и митигация

| Риск | Вероятность | Влияние | Митигация |
|------|-------------|---------|-----------|
| R3-001 MVCC ломает backward compat с `.table` файлами | Средняя | Высокое | Миграционный скрипт `diesel_migrate_v2`; старый формат `.table` читается, пишется новый; версия формата 2. |
| R3-002 Page storage меняет фундамент storage layer | Высокая | Очень высокое | Параллельная разработка с R3-001; feature flag `storage.engine = legacy | page`; legacy поддерживается всю Фазу 1, удаляется в Фазе 2. |
| R3-003 Group commit может дать latency spikes при низкой нагрузке | Средняя | Среднее | Adaptive: при QPS < 50 — переключение в `always` (без group); при QPS > 100 — `group` с окном 5 ms. |
| R3-007 Новый wire протокол ломает существующих клиентов | Высокая | Высокое | Legacy порт в течение всей Фазы 1; deprecation warning; документация миграции. |
| R3-008 RBAC — оператор забывает пароль admin | Средняя | Высокое | `--reset-admin` CLI флаг для локального сброса (только с filesystem access). |
| R3-010 Online ALTER может конфликтовать с long-running queries | Средняя | Среднее | Timeout для old schema version readers; после N минут — killing oldest. |
| R3-016 Репликация несовместима с текущим MVCC | Средняя | Высокое | Logical decoding работает с WAL (R3-003), не затрагивая MVCC. |
| R3-018 CBO даёт регрессии на нестандартных запросах | Высокая | Среднее | `enable_cbo = true | false`; feedback loop — собирать план на slow queries и фиксировать регрессии. |
| Текущая разработка solo/ИИ — не хватит ресурсов на 6 месяцев | Высокая | Очень высокое | Жёсткий скоуп Фазы 1 — только 13 CRITICAL/HIGH задач; остальное переносится. |
| Тесты на больших данных дорогие в CI | Средняя | Низкое | `@LargeTest` аннотация (Промпт 17); в CI только small, nightly — large. |

---

## 9. Метрики приёмки фаз

Фаза считается завершённой, когда **все** её задачи имеют green тесты и выполнены **все** phase-level KPI.

### Фаза 1 (3-6 месяцев)

| Метрика | Целевое значение |
|---------|------------------|
| BEGIN TRANSACTION latency на 100k-строчной таблице | < 1 ms (R3-001) |
| Throughput COMMIT (mixed workload, 4-core VM) | > 5000/sec (R3-003) |
| Recovery после краша на WAL 1 GB | < 30 s (R3-004) |
| Бэкап 1M-строчной базы | < 30 s, файл < 100 MB (R3-011) |
| Время до accepting connections при старте | < 60 s (с recovery) |
| Метрика production-readiness (по таблице §1) | ≥ 55 % |
| Тест-coverage для `diesel/` core | > 60 % |
| SonarQube bugs / critical | 0 / 0 |
| Все blocking bugs из `problems.md` | закрыты |

### Фаза 2 (6-12 месяцев)

| Метрика | Целевое значение |
|---------|------------------|
| Replication lag (streaming, 1000 inserts/sec) | < 100 ms (R3-016) |
| Failover time при kill мастера | < 5 s (R3-016) |
| CBO выбирает hash join на 1k+1M таблицах | 100 % (R3-018) |
| psql подключение и базовые запросы работают | 100 % (R3-015) |
| Partition pruning на 12 партициях | 1 scan вместо 12 (R3-017) |
| Docker image pull + run | < 30 s до ready (R3-020) |
| Метрика production-readiness | ≥ 75 % |

### Фаза 3 (12-24+ месяцев)

| Метрика | Целевое значение |
|---------|------------------|
| SQL coverage vs PostgreSQL | ≥ 85 % |
| TPC-C throughput (10 warehouses) | сопоставимо с PG на тех же ресурсах |
| TPC-H (SF=1) latency | не хуже PG × 1.5 |
| Метрика production-readiness | ≥ 90 % |

---

## 10. Порядок выполнения внутри Фазы 1 (рекомендация)

С учётом зависимостей из §2:

**Спринт 1 (недели 1-4): фундамент**
- R3-002 (page storage + buffer pool) — критическая зависимость
- R3-007 (замена Java-сериализации в протоколе) — параллельно
- R3-013b, R3-013c, R3-013d — быстрые фиксы парсера

**Спринт 2 (недели 5-8): MVCC + WAL**
- R3-001 (MVCC) — большой кусок, 4 недели
- R3-003 (WAL + group commit) — параллельно
- R3-013a (стабильные row-id) — часть R3-002

**Спринт 3 (недели 9-12): recovery + observability**
- R3-004 (ARIES recovery)
- R3-005 (background writer)
- R3-012 (metrics + Prometheus) — параллельно

**Спринт 4 (недели 13-16): security + ops**
- R3-006 (savepoint + deadlock + lock timeout)
- R3-008 (RBAC + audit)
- R3-009 (TLS transport)

**Спринт 5 (недели 17-20): polish**
- R3-010 (online schema changes)
- R3-011 (backup / restore)
- R3-013 — закрытие всех оставшихся багов
- Performance testing, bug bash, документация

**Спринт 6 (недели 21-24): buffer**
- Регрессии, edge cases, hardening
- Подготовка release notes
- Migration guide для существующих пользователей

---

## 11. Примечания

1. **Параллелизм.** Задачи R3-001, R3-002, R3-003, R3-007 — фундаментальные и могут делаться параллельно при наличии 2+ разработчиков. Solo-разработка — последовательное выполнение по §10.
2. **Документация.** После завершения каждой задачи: обновление `KNOWN_LIMITATIONS.md` (снять limit), `PERSISTENCE_README.md` (если касается storage), `README.md` (новые SQL команды).
3. **Версионирование.** Каждая задача — отдельный feature branch → PR → squash merge. Минимум 1 review (даже self-review с чек-листом DoD).
4. **Совместимость.** Весь Фаза 1 поддерживает legacy format через feature flags. Удаление legacy — Фаза 2.
5. **Benchmarks.** До/после каждой задачи — измерение на 3 scenarios: (a) small 1k rows, (b) medium 100k rows, (c) large 1M rows; сохранение в `analytics/performance_history.csv`.
6. **Backward-compat тесты.** Перед закрытием PR: `mvn -Ptest test` — все существующие тесты green; при regression > 10 % latency — блокирующий merge.

---

## 12. Заключение

ROADMAP3.md — это **скелет** перехода DieselDB от экспериментального проекта (~25-30 % готовности) до минимально production-пригодной БД (~55 % за 6 месяцев) и далее до полноценного production (~75-90 % за 12-24 месяца).

Из 13 категорий production-ready требований к концу Фазы 1 будут закрыты CRITICAL-дыры в A (durability), B (concurrency), F (security), M (correctness); HIGH — частично в C (storage), J (operations), K (CI/CD). Полностью останутся открытыми D (replication) и E (sharding) — они в Фазе 2/3.

Документ не заменяет `prompt3.md` и `Roadmap.md`, а **дополняет** их: задаёт порядок выполнения, явные зависимости, метрики приёмки и явные риски. Каждая задача в ROADMAP3.md либо **новая** (не покрыта существующими планами), либо **уточняющая** (детализирует существующий промпт до production-grade уровня).

**Следующие шаги:**  
1. Коммит ROADMAP3.md в репозиторий.  
2. Создание GitHub issues для каждой R3-задачи Фазы 1 (13 задач × 1 issue с labels: phase-1, priority:critical/high, category:A-M).  
3. Создание milestone `Phase 1 — Critical Production-Readiness` с дедлайном +6 месяцев.  
4. Старт Спринта 1 (R3-002 + R3-007 + R3-013b/c/d).
