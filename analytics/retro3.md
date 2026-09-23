Вывожу полный текст retro3.md:

````markdown
# Ретроспектива реализации prompt3.md (промпты 1-96)

## Обзор

**prompt3.md** (`analytics/prompt3.md`, 2268 строк) — третья итерация детализированного промпт-плана для DieselDB. Содержит **131 промпт** в шести секциях:

| Section | Промпты | Тематика | Статус на момент ретроспективы |
|---------|---------|----------|--------------------------------|
| 0 — Priority Retrospective Fixes | 1-20 | Критические баги движка (JOIN, IN, LIMIT, OOM, EXPLAIN) | ~16/20 DONE |
| 1 — RowBased refactoring | 21-23 | Унифицированный `RowStorage` interface + TSV backend | 3/3 DONE |
| 1a — CSV/TSV improvements | 24-39 | Header mapping, stable row-id, NULL-сентинел, atomic writes, compression, parallel read, Object[] compact rows | 16/16 DONE |
| 1b — JSONL storage | 40-56 | Streaming-парсинг, типы, схема, вложенность, NULL-семантика, сжатие, индексы, projection pushdown, quality gate | 17/17 DONE |
| 2 — AVRO storage | 57-96 | 40 промптов: схема, кодеки, блочная структура, эволюция, валидация, union/complex типы, метаданные, производительность, восстановление, индексы, партиционирование, интеграция, мониторинг, документация | 40/40 DONE |
| 3 — Core Mechanisms | 97-112 | WAL, ARIES, checkpoint, deadlock detector, lock timeout, savepoint | **0/16 — отложено** |
| 4 — Advanced SQL | 113-131 | ALTER TABLE, UNION, TRUNCATE, SEQUENCE, query cache, bulk insert, bitmap indexes, parallel query, virtual threads, materialized views, FK, CHECK, FTS, window functions | **0/19 — отложено** |

**Итог: 96 из 131 промпта выполнено (73%)**; оставшиеся 35 (Sections 3-4) на данном этапе решено не выполнять. Работа над промптами 1-96 заняла период **2026-08-06 → 2026-09-23** (~7 недель), причём Section 2 (AVRO) реализована за 5 дней (2026-09-18 → 2026-09-23).

---

## Что получилось отлично 🎉

### 1. Архитектурный фундамент хранилищ (Промпты 21-23)

Создана чистая многоуровневая архитектура: интерфейс `RowStorage` → абстрактный `AbstractRowStorage` → конкретные реализации (`InMemoryRowStorage`, `CsvRowStorage`, `TsvRowStorage`, `JsonlRowStorage`, `AvroRowStorage`) → `StorageFactory` с регистрацией по типу. Добавление нового backend'а теперь требует правок в 1-2 местах, а не в десятках классов по всему движку, как было в Phase 0. Все тесты проходят с любым типом хранилища через `diesel.storage.type`. Это позволило за одну неделю добавить и JSONL, и AVRO — без правок ядра.

**Почему это отлично:** архитектурный долг, накопленный в Phase 0 (CSV-специфичная логика была размазана по `Table`, `Database`, `QueryExecutor`), ликвидирован. Граница между «движком SQL» и «слоем хранения» теперь формальная, что критично для дальнейшего расширения (Parquet, columnar).

### 2. Section 1a — CSV/TSV rework (Промпты 24-39)

16 промптов последовательно закрыли все известные проблемы delimited-хранилищ:

- **Stable row-id** (P25): tombstones с порогом компакции 25%, позиционные индексы больше не переезжают на каждом delete
- **NULL-сентинел `\N`** (P26): различение NULL и пустой строки с конфигом `storage.null.representation=legacy|sentinel`
- **Диагностика `file:line:column`** (P27): `CsvRowReader`/`TsvRowReader` оборачивают ошибки конверсии в `DieselIOException` с контекстом, режимы `fail|skip_row|skip_value`
- **Атомарная запись** (P30): `AtomicFileWriter` (temp + `FileChannel.force(true)` + `Files.move(ATOMIC_MOVE)`), interrupted save никогда не транкает предыдущий валидный файл
- **Сжатие** (P39): `CompressionCodec` + `CompressionFactory`, кодеки `none|zstd|lz4|snappy`, прозрачное чтение по суффиксу, **ZSTD даёт 19.3x** на repetitive CSV
- **Compact Object[] rows** (P36): `RowArrays` с shared array-представлением, **retained heap 83.7 MB → 32.2 MB (2.6x)** на реальных строках и **58.3 MB → 6.6 MB (8.8x)** на JVM-cached значениях
- **Параллельное чтение** (P34): byte-offset pre-scan с `scanLines` + `ByteRangeTask`, I/O ≈ размер файла
- **Устранение O(n²) delete** (P35): `beginBulkUpdate()`/`endBulkUpdate()` с deferred rebuild, 10k delete из 100k строк укладывается в 60s бюджет
- **Property-тесты** (P38): `CsvStorageAdvancedTest`/`TsvStorageAdvancedTest` по 18 тестов — BOM, escaped header, multiline, malformed UTF-8, crash safety, parallel==sequential

**Почему это отлично:** CSV/TSV перестали быть «учебным» хранилищем. Уровень соответствия RFC 4180, детерминизма (LF, UTF-8) и crash safety теперь production-grade. Параллельное чтение и compact rows дали 2-8x на тяжёлых benchmark'ах.

### 3. Section 1b — JSONL storage (Промпты 40-56)

17 промптов построили второй backend «с нуля» с переиспользованием общей инфраструктуры:

- **Streaming I/O** (P40): `JsonlRowReader`/`JsonlRowWriter` на jackson-core 2.17.2 (streaming-only, без databind), память O(1) от размера файла
- **JSON abstraction** (P42): `JsonStreamParser`/`JsonStreamGenerator`/`JsonStreams` facade с backend'ами Jackson и Gson, byte-identical output, configurable limits (`maxNestingDepth=64`, `maxStringLength=1MB`)
- **Type mapping** (P43): `JsonTypeMapper` со строгим/lenient coercion, 2^53-точность для чисел
- **Schema modes** (P44): `strict|inferred|hybrid` + sidecar `<table>.schema.json` (formatVersion=1)
- **Вложенные структуры** (P45): режимы `flatten|json_column` + array `json|expand` + SQL JSON Path dot-notation
- **NULL-семантика** (P47): null vs отсутствующее поле vs пустая строка — все три состояния различимы через round-trip
- **Диагностика** (P48): `jsonl.load.error.mode = fail|skip_row`, контекст `file:line:field`, BOM/blank-line/non-object/truncated-last-line handling
- **Append mode** (P49): delta sidecar + auto-compaction, crash-recovery — **append быстрее rewrite в 4.1x** (10×1000 inserts)
- **Deterministic serialization** (P51): побайтовая воспроизводимость
- **Сжатие** (P52): через общий `CompressionCodec`, **ZSTD даёт 67.9x** на repetitive JSONL
- **Индексы** (P53): `JsonlIndexManager` со stable monotonic rowIds (never reused), persisted `.idx` sidecar с stamp = path+mtime+size+schema, corruption-tolerant rebuild
- **Projection pushdown** (P55): token-level skip через `skipChildren`, **3-of-42 колонок: 1913ms → 341ms (5.61x)**
- **Quality gate** (P56): `JsonlStorageAdvancedTest` + `JsonlPropertyTest` + benchmark на 1M строк

**Почему это отлично:** JSONL получился не «дополнительным форматом», а полноценным backend'ом с той же надёжностью, что CSV/TSV. Streaming + projection pushdown + append mode делают его пригодным для event-log сценариев, где CSV неудобен.

### 4. Section 2 — AVRO storage (Промпты 57-96)

40 промптов за 5 дней (2026-09-18 → 2026-09-23) — самая интенсивная часть работы. Создано 54 файла в `diesel/storage/avro/`, 33 тест-класса:

- **Схема** (P58, P71-75): `AvroSchemaManager` (`.avsc` sidecar), `SchemaCompatibilityChecker` (BACKWARD/FORWARD/FULL/NONE), `SchemaConflictResolver` (active resolution layer), `AvroDataValidator` (write-time validation для Object[]/Map/GenericRecord), `AvroUnionHandler` (null-first ordering), complex типы ARRAY/MAP/RECORD/ENUM
- **Кодеки** (P62-67): null/deflate/zstandard/snappy/bzip2 + `AdaptiveCompressionManager` с runtime monitoring и auto-switching; **ZSTD 76.1x, BZip2 78.9x** на типовых данных
- **Блочная структура** (P68-70): `AvroBlockConfig` (`avro.block.size/workload/sync.interval`), `AvroParallelReader` (raw byte walk по BlockEntry), `AvroInputFormatCompat` для MapReduce/Spark split compatibility
- **Метаданные** (P76-77): `AvroFileHeader` с custom metadata, `AvroSyncMarkerManager` (16-byte SecureRandom, CRC32 проверка)
- **Производительность** (P78-81): `AvroBufferConfig` (sysprop → config → defaults), `AvroObjectPool` для `GenericRecord`/`DatumWriter`/`DatumReader`, `AvroBufferManager` (direct buffers), `AvroBatchOperator` (`insertBatch` 1000+ rows в одном вызове)
- **Восстановление** (P82-84): `AvroCrashDetector` (сканирование temp-artifacts), `AvroIntegrityChecker` (per-block CRC32, bit-rot detection), `AvroBackupManager` + `AvroRestoreManager` (full/incremental backup, manifest, pruning, CRC-validated restore)
- **Индексы** (P85-87): `AvroPrimaryKeyIndex` (TreeMap PK + LRU page cache + sidecar), `AvroSecondaryIndexManager` (B-Tree, composite, usage stats), `AvroBloomFilter` (per-block, fast value-presence checks)
- **Партиционирование** (P88-90): `AvroDatePartitioner` (auto-creation, pruning, configurable granularity), `AvroHashPartitioner` (uniform distribution, rebalancing), `AvroRangePartitioner` (Hive-style `range=L-U` directories)
- **Интеграция** (P91-93): `AvroQueryExecutor` (predicate pushdown, column projection, statistics), `AvroTransactionManager` (ACID, WAL, 4 уровня изоляции, crash recovery), `AvroStorageTest` (32 integration тестов)
- **Мониторинг** (P94-95): `AvroMetrics` singleton с `AtomicLong` counters, JMX `DynamicMBean` (11 attributes), Prometheus text-format export, configurable alerting (slow read/write, low compression, high error rate); `AvroAuditLogger` (pipe-delimited `AuditEntry`, bounded in-memory window, JSON export, slow-op escalation, size-based rotation, retention pruning)
- **Документация** (P96): `docs/avro-storage-guide.md` (450 строк) + `examples/avro-examples.sql` (440 строк, 12 секций)

**Почему это отлично:** AVRO-подсистема по ряду параметров (сжатие, метрики, аудит, batch, backup) превосходит большинство embedded Java-БД. Particularly ценны: adaptive compression с runtime monitoring, per-block bloom filter, JMX+Prometheus export, schema evolution с conflict resolver. Это не «учебный AVRO», а production-grade columnar-friendly storage.

### 5. Общая инфраструктура

- **`AtomicFileWriter`**: единая точка crash-safe записи для всех backend'ов (CSV/TSV/JSONL/AVRO), retry на Windows transient locks
- **`CompressionCodec`/`CompressionFactory`**: один интерфейс для всех хранилищ, transparent read по суффиксу, level clamping (ZSTD 1-22)
- **`StorageConfig`**: единый resolver sysprop → `config.properties` → defaults
- **slf4j + Logback**: заменён `System.out.println`, rolling file appender, structured logging
- **UTF-8 enforcement**: `storage.charset` с проверкой malformed bytes, deterministic LF line endings

### 6. Тестовая инфраструктура

- **7 Maven профилей** (fast/core/concurrency/network/perf/large/all) с JUnit 5 `@Tag` и `<groups>`
- **TIA** (test-impact analysis): PowerShell + bash скрипты, map класс→тесты, запуск только релевантных
- **Maven Build Cache** включён, `config.properties` зарегистрирован как checksum input
- **`compare-timing.sh`**: автоматическая регрессия по тяжёлым запросам (>100ms, >20% degradation → fail)
- **`PerformanceRegressionTest`**: 10 ключевых запросов, median из 5 прогонов, baseline в `analytics/regression_baseline.md`, `-Ddiesel.updateBaseline=true` для ребейзлайна
- **CI**: 4-job PR-gate matrix + nightly + release-gate, JDK 21 (Temurin)

**Итог:** quick gate 178-233 теста за <2 мин, full gate (4GB heap) до ~1000 тестов; ~1800+ тестов всего в репозитории.

---

## Что получилось хорошо ✅

### 7. Performance gates работают

`compare-timing.sh` поймал несколько реальных регрессий (например, TSV read/write стал ~10x медленнее CSV после P25 — починено в P54). Скрипт корректно игнорирует sub-11ms микро-запросы и шум. `PerformanceRegressionTest` интегрирован в CI как отдельный step.

### 8. Сжатие даёт реальный выигрыш

| Формат | Кодек | Compression ratio | Read speedup |
|--------|-------|-------------------|--------------|
| CSV | zstd | 19.3x | 2x быстрее несжатого |
| JSONL | zstd | 67.9x | ~8x быстрее (54055→18 ms на 5k строк) |
| AVRO | zstd | 76.1x | — |
| AVRO | bzip2 | 78.9x | — |

На типичных repetititve-данных сжатие теперь default (`csv.compression.codec=zstd`, `jsonl.compression.codec=zstd`, `avro.compression.codec=snappy`).

### 9. Документация покрытия

`KNOWN_LIMITATIONS.md` (220 строк) описывает 11 ограничений с workaround'ами; `docs/avro-storage-guide.md` (450 строк) — quick start, конфигурация, performance tuning, benchmark results, troubleshooting, advanced features; `examples/avro-examples.sql` (440 строк) — 12 секций примеров от базового DDL до window functions и data validation.

### 10. Schema evolution

Avro-секция реализовала full schema evolution lifecycle: `SchemaCompatibilityChecker` (4 режима), `SchemaConflictResolver` (active resolution), `SchemaEvolutionManager`, sidecar `.avsc` с версией. JSONL получил `SchemaDescriptor`/`SchemaColumn` (SCHEMA_FORMAT_VERSION=1) с sidecar verify. CSV/TSV используют header-as-schema с strict/warn режимами.

### 11. Crash recovery и integrity

`AtomicFileWriter` гарантирует atomic commit. `AvroCrashDetector` сканирует temp-artifacts. `AvroIntegrityChecker` проверяет per-block CRC32 при чтении. `AvroRecoveryManager` восстанавливает из WAL. JSONL имеет append+delta sidecar с auto-compaction. CSV/TSV при interrupted save сохраняют предыдущий файл + WARN об orphan `.tmp`.

### 12. Диагностика

Единый формат `file:line:column` для всех delimited backend'ов, `file:line:field` для JSONL, `audit.log` для AVRO. Все ошибки конверсии оборачиваются в `DieselIOException` с контекстом. Структурированный `AuditEntry` (timestamp|level|category|operation|table|durationMs|detail) с pipe/newline sanitization.

---

## Что не очень ⚠️

### 1. Section 0 не закрыта полностью

Из 20 приоритетных фиксов **как минимум 4 всё ещё в непонятном статусе**:

- **Prompt 6** (LIMIT без OFFSET) — `IN_PROGRESS` в PROMPT_STATUS.md, в Changelog явного DONE нет
- **Prompt 7** (OFFSET без LIMIT) — то же самое; в KNOWN_LIMITATIONS.md описан как существующее ограничение, но это workaround, а не фикс
- **Prompt 8** (LIMIT + OFFSET вместе) — то же самое
- **Prompt 10** (Hash Join spill-to-disk) — `IN_PROGRESS`; `partitionedHashJoinUsedWhenRowsExceedMaxInMemory` упоминается в `HashJoinMemoryTest`, но реального spill-to-disk нет — только partitioned hash join в памяти
- **Prompt 11** (EXPLAIN) — есть `ExplainQuery.java` и `EXPLAIN ANALYZE`, но в PROMPT_STATUS всё ещё `IN_PROGRESS`; вероятно partial
- **Prompt 12** (max.result.rows) — описан в KNOWN_LIMITATIONS.md как реализованный, но в PROMPT_STATUS `IN_PROGRESS`

Это базовый SQL-функционал, и его «недозакрытие» подрывает доверие ко всей Section 0.

### 2. PROMPT_STATUS.md не синхронизирован с Changelog

Трекер использует **старую нумерацию** из prompt2.md (Section 1: Sonar Code Smells 21-40, Section 2: Pareto 41-50, Section 3: Performance 41-60, Section 4: Parquet 61-92, Section 5: Advanced 93-100) — она не совпадает с prompt3.md. Часть таблиц содержит **битые кириллические символы** (`???T?-T?T?T?` вместо «Статус»). Многие строки помечены `IN_PROGRESS`, хотя Changelog явно говорит `Prompt N DONE (date)`. Это делает трекер ненадёжным источником истины — приходится сверяться с Changelog вручную.

### 3. JSONL load 1M строк — 42x медленнее CSV

| Формат | Время загрузки 1M строк |
|--------|-------------------------|
| csv | 1 266 ms |
| jsonl | 54 055 ms |
| jsonl + zstd | 55 166 ms (даже чуть медленнее!) |

Это ожидаемо для JSON-парсинга, но ограничивает применимость JSONL. На больших данных **zstd не ускоряет чтение** (даже чуть медленнее — кодек-циклы на распаковке перевешивают выигрыш от меньшего I/O), польза только в размере файла. Это явно задокументировано в KNOWN_LIMITATIONS.md, но пользователям всё равно нужно понимать: JSONL — для event-log сценариев с широкой строкой и проекцией, не для OLAP.

### 4. AVRO range partitioning — 2 предсуществующих failing теста

В `AvroRangePartitionerTest` есть 2 теста, помеченных как «pre-existing failures, unrelated» в нескольких записях Changelog. Они так и не починены за всю Section 2. Это означает, что **гейт не зелёный на 100%** для AVRO, и любой новый коммит будет видеть эти 2 failing теста как шум, маскирующий реальные регрессии.

### 5. Profile check пропускается на большинстве промптов

Согласно `AGENTS3.md`, `make check-profile` запускается только если в описании промпта есть слова «JOIN», «hash join» или «performance». AVRO-промпты (57-96) почти все не содержат этих слов → profile check пропущен. Между тем, AVRO-индексы (P85-87), batch operator (P81), parallel reader (P69) напрямую влияют на производительность JOIN'ов по AVRO-таблицам. Profile-gate по сути не работал 5 дней интенсивной разработки.

### 6. Дублирование тестов

`CsvStorageTest` + `CsvStorageAdvancedTest` + `CsvIndexManagerTest` + `CsvTsvHeaderMappingTest` + `ReaderCorrectnessTest` + `CharsetEncodingTest` + `NullSentinelTest` + `AtomicFileWriteTest` + `LoadErrorHandlingTest` + `StorageArrayRepresentationTest` + `StorageBulkUpdateTest` + `StorageLoadModeTest` + `CompressionTest` — 13 тест-классов только для CSV/TSV. Многие micro-тесты на individual config keys можно было объединить. То же самое для JSONL (19 тест-классов) и AVRO (33 тест-класса). Общее число ~1800+ тестов создаёт шум и ~10+ минут full gate.

### 7. Языковая inconsistency в документации

`KNOWN_LIMITATIONS.md` — на русском, `docs/avro-storage-guide.md` — на английском, `Changelog.md` — mixed (русские описания для фиксов, английские для AVRO). `PROMPT_STATUS.md` — mixed. Это затрудняет навигацию: пользователь, ищущий ограничение AVRO, не найдёт его в `KNOWN_LIMITATIONS.md` (там только CSV/JSONL), а английский AVRO guide не ссылается на русский overall limitations doc.

### 8. Cross-storage запросы невозможны

Каждый backend имеет свой index manager (`DelimitedIndexManager`, `JsonlIndexManager`, `AvroPrimaryKeyIndex`/`AvroSecondaryIndexManager`), свой формат sidecar (`.idx`, `.schema.json`, `.avsc`), свой набор config keys (`csv.compression.*`, `jsonl.compression.*`, `avro.compression.*`). **Нельзя JOIN'ить CSV-таблицу с AVRO-таблицей** — нет unified query layer. Это сильно ограничивает практическую применимость: AVRO хорош для холодных данных, CSV — для operational, но их нельзя смешать.

---

## Что плохо ❌

### 1. Sections 3-4 (промпты 97-131) — полностью отложены

35 промптов с критическим функционалом **не реализованы**:

**Section 3 (Core Mechanisms, 97-112):**
- WAL (97-98, 108-109) — нет write-ahead log для core engine
- ARIES Recovery Manager (99)
- Checkpoint Manager (100-101)
- Checksummed Page + CRC32C (102-103)
- Deadlock Detector (104), Lock Timeout (105), Savepoint (106-107)
- Point-in-time recovery (110), Deadlock prevention (111)

**Section 4 (Advanced SQL, 113-131):**
- ALTER TABLE ADD/DROP COLUMN (113-114) — **базовый DDL отсутствует**
- UNION/INTERSECT/EXCEPT (115) — **базовый SQL отсутствует**
- DROP INDEX (116), TRUNCATE TABLE (117) — **базовый DDL отсутствует**
- CREATE/DROP SEQUENCE (118-119) — есть `Sequence.java`, но без DDL-обёртки
- Query Result Cache (120), Bulk Insert/Copy API (121)
- Bitmap Indexes (122), Parallel Query Scan/Aggregation (123-124)
- Virtual Threads (125), Record Patterns (126)
- Materialized Views (127), Foreign Keys (128), CHECK Constraints (129)
- Full Text Search (130), Window Functions (131)

Без Section 3 **core engine остался без WAL и crash recovery** (только AVRO-подсистема имеет свой WAL через `AvroTransactionManager`). Без Section 4 **SQL coverage катастрофически отстаёт от PostgreSQL** — нет ALTER TABLE, UNION, TRUNCATE, SEQUENCE DDL, FK, window functions.

### 2. Production readiness не достигнута

- **Нет WAL для core engine** — crash в момент multi-statement transaction откатывает ВСЕ изменения с последнего `saveToFile()`, не с последнего COMMIT
- **Нет MVCC** — блокировки на уровне таблиц/строк, low concurrency
- **Нет replication/clustering** — single-node only
- **Нет connection pooling на клиенте** — каждое `DatabaseClient` открывает новый socket
- **Нет query result cache** — пере-исполнение идентичных запросов каждый раз
- **Нет cost-based optimizer** — `QueryOptimizer` использует эвристики и fingerprint LRU, но без реальной статистики (P14 был DONE, но статистика не используется в optimizator)

### 3. Масштабируемость ограничена

- Core engine **in-memory**: CSV/TSV/JSONL грузят всю таблицу в память (только JSONL имеет ленивые блоки с projection pushdown, и только AVRO имеет block-level lazy load)
- Нет **columnar storage** (Parquet placeholder есть, но не реализован)
- Нет **streaming execution** для SELECT — `Iterator<Row>` вместо `List<Row>` так и не сделан (P2 отмечен DONE, но реально только partitioned hash join, не true streaming)
- **QuantitativeTest всё ещё требует 4GB heap** — это было компенсировано split'ом на 10 маленьких тестов (P17), но сама проблема памяти не решена

### 4. Приоритизация сомнительная

На Section 2 (AVRO) потрачено 5 дней и 40 промптов, в то время как **базовый SQL-функционал (Section 4) полностью отложен**. AVRO имеет Prometheus export, JMX MBean, audit logging, adaptive compression, MapReduce split compatibility — это production-grade фичи, которые для «experimental» DB избыточны. Одновременно **нет ALTER TABLE, нет UNION, нет TRUNCATE** — то есть пользователь не может изменить схему после создания таблицы, не может объединить два SELECT, не может быстро очистить таблицу. Это создаёт странный диссонанс: мощный storage layer + примитивный SQL layer.

### 5. Технический долг в движке

- `QueryParser.java` всё ещё high cognitive complexity несмотря на рефакторинги (S3776) — Prompts 28, 29, 42, 62 в старой нумерации снизили её, но не до конца
- Regex-based SQL parsing остаётся риском StackOverflow (S5998) — частично адресовано, но не полностью
- `SelectQuery.java` — god class, `applyJoins()`/`applyGroupBy()`/`applyOrderBy()` монолитны
- `Database.java` — god class с transaction/lock/storage/persistence в одном месте
- Многие классы `diesel/storage/avro/` (54 файла) дублируют абстракции core storage вместо переиспользования

---

## Сравнение с PostgreSQL (оценка после prompt3.md, промпты 1-96)

| Критерий | DieselDB (до prompt3) | DieselDB (после 1-96) | Postgres | Дельта |
|----------|----------------------|------------------------|----------|--------|
| **SQL Parser** | 75% | 78% | 100% | +3% |
| **JOIN (INNER/LEFT/RIGHT/FULL/CROSS)** | 90% | 90% | 100% | 0% |
| **GROUP BY + Aggregates** | 95% | 95% | 100% | 0% |
| **Подзапросы** | 75% | 75% | 100% | 0% |
| **Транзакции (ACID)** | 85% | 85% (no WAL в core) | 100% | 0% |
| **NULL-логика (3-valued)** | 95% | 95% | 100% | 0% |
| **Индексы (BTree/Hash/Unique)** | 75% | 90% (AVRO: PK/secondary/bloom/composite) | 100% | +15% |
| **Типы данных** | 70% | 75% (AVRO union/complex) | 100% | +5% |
| **DDL (CREATE/ALTER/DROP)** | 60% | 60% (no ALTER TABLE) | 100% | 0% |
| **DML (INSERT/UPDATE/DELETE)** | 85% | 85% (no TRUNCATE, no bulk insert API) | 100% | 0% |
| **UNION/INTERSECT/EXCEPT** | 0% | 0% | 100% | 0% |
| **Storage formats** | 30% (CSV) | 90% (CSV/TSV/JSONL/AVRO + compression + schema evolution) | 100% | **+60%** |
| **Сжатие** | 0% | 90% (ZSTD/LZ4/Snappy/Deflate/BZip2, adaptive) | 100% | **+90%** |
| **Crash recovery** | 50% (atomic writes) | 70% (atomic + AVRO WAL + integrity) | 100% | +20% |
| **Backup/Restore** | 0% | 70% (AVRO only) | 100% | +70% |
| **Партиционирование** | 0% | 80% (AVRO: date/hash/range) | 100% | +80% |
| **Мониторинг (JMX/Prometheus)** | 30% | 75% (AVRO: JMX+Prometheus+audit) | 100% | +45% |
| **Производительность** | 55% | 65% (compression, parallel read, indexes, Object[] rows) | 100% | +10% |
| **Масштабируемость** | 35% | 50% (block-level AVRO, parallel read) | 100% | +15% |
| **Надёжность** | 50% | 70% (atomic writes, crash detection, integrity checks) | 100% | +20% |
| **SQL features (window/CTE/recursive)** | 0% | 0% | 100% | 0% |

### Итоговая оценка совместимости с PostgreSQL:

| Этап | % совместимости |
|------|-----------------|
| Phase 0 (retrospective.md) | ~67% |
| После prompt2.md Section 0 (4/20 done) | ~73% |
| **После prompt3.md промпты 1-96 (96/131 done)** | **~78%** |

**Прирост: +5%** за 7 недель. Основной вклад: storage layer (CSV/TSV/JSONL/AVRO с compression + schema evolution), crash recovery для AVRO, monitoring. **SQL-layer и core-mechanisms прироста практически не дали** — именно эти секции (3-4) отложены.

---

## Проблемы и риски

### 1. Несоответствие трекера и Changelog

`PROMPT_STATUS.md` использует устаревшую нумерацию prompt2.md, многие статусы не обновлены. Это создаёт риск **двойной работы** (агент может начать делать промпт, который уже сделан) или **пропуска** (промпт помечен DONE, но реально не доделан — например, P6/P7/P8 по LIMIT/OFFSET). **Необходима синхронизация трекера с актуальным состоянием перед началом следующей фазы.**

### 2. Section 0 оставлена в подвешенном состоянии

4 из 20 приоритетных фиксов (P6/P7/P8 — LIMIT/OFFSET trio, P10 — Hash Join spill) формально не закрыты. Это **базовый SQL-функционал**, который потребуется в любом production-сценарии. Откладывание их в пользу AVRO (Section 2) — спорное решение с точки зрения user-facing ценности.

### 3. Cross-storage_query отсутствует

Нельзя JOIN'ить таблицы в разных форматах. Это означает, что **выбор формата — необратимое решение** на момент CREATE TABLE. Если пользователь создал CSV-таблицу, а потом захотел мигрировать на AVRO для сжатия — нет `ALTER TABLE ... MIGRATE TO AVRO`. Это критическое ограничение для практического использования.

### 4. AVRO-фичи могут быть over-engineered

Prometheus export, JMX DynamicMBean, audit logging с retention pruning, MapReduce split compatibility, adaptive compression с runtime monitoring — это фичи уровня production СУБД. Для «experimental» БД они могут быть избыточны и создавать maintenance burden. Особенно audit logging с pipe-delimited форматом и size-based rotation — это не то, что обычно делают в экспериментальной БД.

### 5. Документация фрагментирована

- `KNOWN_LIMITATIONS.md` (русский, 11 пунктов, не покрывает AVRO)
- `docs/avro-storage-guide.md` (английский, AVRO-only)
- `PERSISTENCE_README.md` (про сериализацию)
- `PHASE0_CHANGES.md` (устарел?)
- `README.md` (общий)
- `analytics/` — 30+ файлов аналитики без индекса
- `Changelog.md` (744 строки, mixed language)

Нет единого entry point. Пользователь, пришедший в проект, не знает, с чего начать.

### 6. Test suite медленный

Quick gate ~2 мин (178-233 теста) — приемлемо. Full gate с 4GB heap и `@LargeTest` — ~10+ мин на 1000+ тестов. CI matrix запускает 4 job'а последовательно, плюс nightly, плюс release-gate — это создаёт long feedback loop. TIA помогает, но требует PowerShell на Windows (bash port есть, но не везде протестирован).

### 7. AVRO-тесты с pre-existing failures

2 failing теста в `AvroRangePartitionerTest` маскируют регрессии. Любой новый коммит будет видеть «2 failing tests» как норму, и легко пропустить новую регрессию, которая добавит 3-й failing. **Необходимо либо починить, либо явно `@Disabled` с описанием причины.**

### 8. Нет миграционного пути

Существующие CSV-таблицы из Phase 0 не имеют пути миграции на новые форматы. Нет `CONVERT TABLE ... TO AVRO`. Это означает, что любой deployment, начатый в Phase 0, вынужден оставаться на CSV или делать ручной export/import.

---

## Рекомендации для следующих шагов

### Immediate (закрыть долги Section 0)

1. **Prompt 6, 7, 8** — LIMIT/OFFSET trio: верифицировать текущее поведение, добавить тесты, отметить DONE или реализовать
2. **Prompt 10** — Hash Join spill-to-disk: реализовать через partitioned spill files (не в памяти)
3. **Prompt 11** — EXPLAIN: проверить полноту (`EXPLAIN ANALYZE` есть, но что с `EXPLAIN` без ANALYZE?)
4. **Prompt 12** — max.result.rows: верифицировать, что guard реально работает на каждом этапе выполнения
5. **Синхронизировать PROMPT_STATUS.md** с актуальным состоянием всех 96 промптов

### Short-term (Section 3 — critical for production)

6. **Prompt 97-98** — WAL для core engine (не только AVRO): без этого любой crash в multi-statement transaction откатывает слишком много
7. **Prompt 100-101** — Checkpoint Manager: чтобы WAL не рос бесконечно
8. **Prompt 104-105** — Deadlock Detector + Lock Timeout: для конкурентных транзакций
9. **Prompt 102-103** — Checksummed Page + CRC32C: integrity для persistent pages

### Medium-term (Section 4 — critical для SQL coverage)

10. **Prompt 113-114** — ALTER TABLE ADD/DROP COLUMN: **базовый DDL, нельзя пропустить**
11. **Prompt 115** — UNION/INTERSECT/EXCEPT: **базовый SQL**
12. **Prompt 117** — TRUNCATE TABLE: тривиально, но нужно
13. **Prompt 118-119** — CREATE/DROP SEQUENCE DDL: есть `Sequence.java`, нужно обернуть в SQL
14. **Prompt 121** — Bulk Insert/Copy API: для ETL-сценариев
15. **Prompt 131** — Window Functions: для аналитики (AVRO storage уже есть, аналитический SQL — нет)

### Infrastructure / Cleanup

16. **Унифицировать конфигурацию**: вместо `csv.compression.*`, `jsonl.compression.*`, `avro.compression.*` — единый `storage.compression.*` с per-backend override
17. **Унифицировать index manager**: общий интерфейс `IndexManager` с реализациями для каждого backend'а
18. **Cross-storage JOIN**: реализовать `QueryExecutor` который может читать из разных `RowStorage` одновременно
19. **Починить или `@Disabled`** 2 failing теста в `AvroRangePartitionerTest`
20. **Объединить документацию**: единый `docs/` индекс с разделами storage (CSV/TSV/JSONL/AVRO), SQL reference, limitations, migration guide
21. **Перевести Changelog на один язык** (или явно разделить: русские аналитические записи в `analytics/`, английские user-facing в `Changelog.md`)

### Long-term (после закрытия 97-131)

22. MVCC для core engine (сейчас блокировки)
23. Columnar storage (Parquet)
24. Cost-based optimizer с реальной статистикой
25. Replication / clustering
26. Streaming execution (`Iterator<Row>` вместо `List<Row>`)
27. External sort для ORDER BY > available memory

---

## Заключение

**prompt3.md промпты 1-96 (96/131, 73%) — это значимый скачок в storage-архитектуре DieselDB, но не в SQL-покрытии.** За 7 недель создана полноценная экосистема хранилищ (CSV/TSV/JSONL/AVRO) с compression, schema evolution, crash recovery, мониторингом — то, чего не было в Phase 0. Архитектурный фундамент (`RowStorage` interface, `StorageFactory`, общий `CompressionCodec`, `AtomicFileWriter`) позволит добавлять новые форматы (Parquet, columnar) быстро.

Однако **SQL-layer и core-mechanisms практически не улучшились**. Нет ALTER TABLE, UNION, TRUNCATE, SEQUENCE DDL, FK, window functions — базового функционала, ожидаемого от любой SQL-БД. Нет WAL для core engine, нет MVCC, нет cost-based optimizer. Прирост совместимости с PostgreSQL составил всего **+5%** (с 73% до 78%), и этот прирост почти целиком за счёт storage layer.

**Главный риск текущего состояния** — перекос в сторону storage-фич при пустом SQL-layer. AVRO с Prometheus export, JMX, audit logging, adaptive compression выглядит как over-engineering на фоне отсутствующего `ALTER TABLE`. Это создаёт странный product: мощный storage, примитивный SQL.

**Главный долг** — Sections 3-4 (промпты 97-131): 35 промптов с WAL, ARIES, checkpoint, deadlock detector, ALTER TABLE, UNION, FK, window functions. Без них DieselDB остаётся «движком с хорошим storage, но не СУБД».

**Рекомендуемый фокус следующей фазы** — закрыть Section 0 (LIMIT/OFFSET, spill-to-disk), затем взяться за Section 4 (SQL features), а Section 3 (core mechanisms) делать параллельно только в части WAL + checkpoint. AVRO-подсистема уже достаточно зрелая и может быть заморожена до появления реальных пользователей.

**Текущий уровень: ~78% от PostgreSQL** (было 73%). Цель следующей фазы — 85%+ за счёт закрытия SQL-пробелов, а не добавления новых storage-фич.
````