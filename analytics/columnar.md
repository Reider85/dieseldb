
## Раздел 3: Parquet Storage и Query Cache (20 промптов)

### Промпт 86: Интеграция Apache Parquet библиотеки
```
Добавь зависимость Apache Parquet в pom.xml:
```xml
<dependency>
    <groupId>org.apache.parquet</groupId>
    <artifactId>parquet-column</artifactId>
    <version>1.13.1</version>
</dependency>
```

Реализуй базовый ParquetWriter для записи таблиц в Parquet формат.
Файлы: pom.xml, diesel/ParquetWriter.java (новый)
Приоритет: HIGH
```

### Промпт 87: ParquetReader для чтения данных
```
Реализуй ParquetReader который:
1. Читает Parquet файлы в Row объекты
2. Поддерживает projection pushdown (читай только нужные колонки)
3. Поддерживает predicate pushdown (фильтруй при чтении)

Файлы: diesel/ParquetReader.java (новый)
Приоритет: HIGH
```

### Промпт 88: Columnar storage для аналитических запросов
```
Для таблиц >1M строк предлагай columnar storage (Parquet):
1. Конвертация row-based ? columnar (async background job)
2. Dual storage: row-based для OLTP, columnar для OLAP
3. Auto-switch: оптимизатор выбирает storage based on query type

Файлы: diesel/TableStorage.java, diesel/QueryOptimizer.java
Приоритет: MEDIUM
```

### Промпт 89: Schema evolution для Parquet
```
Поддержи эволюцию схемы Parquet файлов:
1. Добавление новых колонок (nullable)
2. Удаление колонок (игнорируй при чтении старых файлов)
3. Изменение типов (safe casts only)

Файлы: diesel/ParquetSchemaManager.java (новый)
Приоритет: MEDIUM
```

### Промпт 90: Partitioning для Parquet таблиц
```
Реализуй partitioning по дате/категории:
1. Directory structure: /table/date=2024-01-01/data.parquet
2. Partition pruning: skip partitions не подходящие под WHERE
3. Dynamic partition creation при INSERT

Файлы: diesel/PartitionedTable.java (новый)
Приоритет: MEDIUM
```

### Промпт 91: Compression codecs для Parquet
```
Поддержи разные codecs:
1. UNCOMPRESSED - быстро, большой размер
2. SNAPPY - баланс скорость/размер (default)
3. GZIP - лучше сжатие, медленнее
4. ZSTD - лучшее сжатие, средняя скорость

Конфигурируемо на уровне таблицы.
Файлы: diesel/ParquetWriter.java
Приоритет: LOW
```

### Промпт 92: Statistics в Parquet metadata
```
Используй встроенную статистику Parquet:
1. Min/max значения для каждой колонки в row group
2. Count null values
3. Skip row groups где min/max не удовлетворяют WHERE условию

Файлы: diesel/ParquetReader.java
Приоритет: MEDIUM
```

### Промпт 93: Bloom filters для Parquet
```
Добавь bloom filters для fast lookup:
1. Bloom filter на первичный ключ
2. Проверка наличия значения перед чтением row group
3. False positive rate configurable (default 1%)

Файлы: diesel/ParquetWriter.java, diesel/ParquetReader.java
Приоритет: LOW
```

### Промпт 94: QueryCache архитектура
```
Создай QueryCache с:
1. Ключ: normalized SQL + parameter types
2. Значение: List<Row> результат
3. TTL: 5 минут (configurable)
4. Max size: 1000 entries (LRU eviction)

Файлы: diesel/QueryCache.java (новый)
Приоритет: HIGH
```

### Промпт 95: Cache invalidation??
```
Реализуй инвалидацию кэша:
1. INSERT/UPDATE/DELETE ? invalidate cache для этой таблицы
2. DDL (ALTER TABLE) ? invalidate все запросы к таблице
3. Time-based: TTL expiry
4. Manual: CLEAR CACHE команда

Файлы: diesel/QueryCache.java, diesel/Table.java
Приоритет: HIGH
```

### Промпт 96: Интеграция QueryCache в SelectQuery.java
```
Модифицируй SelectQuery.execute():
1. Перед выполнением: check cache
2. При cache hit: верни из кэша
3. При cache miss: выполни ? сохрани в кэш ? верни

Добавь метрики: hit rate, miss rate, avg latency saved.
Файлы: diesel/SelectQuery.java
Приоритет: HIGH
```

### Промпт 97: Инвалидация кэша при INSERT
```
При INSERT в таблицу:
1. Найди все cached queries для этой таблицы
2. Invalidate их (remove from cache)
3. Логгируй: "Invalidated 5 cache entries for table USERS"

Файлы: diesel/InsertQuery.java, diesel/QueryCache.java
Приоритет: HIGH
```

### Промпт 98: Инвалидация кэша при UPDATE
```
При UPDATE таблицы:
1. Invalidate все cached SELECT queries к этой таблице
2. Для UPDATE с WHERE: попробуй partial invalidation (сложно)
3. По умолчанию: full invalidation для безопасности

Файлы: diesel/UpdateQuery.java, diesel/QueryCache.java
Приоритет: HIGH
```

### Промпт 99: Инвалидация кэша при DELETE
```
При DELETE из таблицы:
1. Invalidate все cached queries к этой таблице
2. Аналогично UPDATE - conservative approach

Файлы: diesel/DeleteQuery.java, diesel/QueryCache.java
Приоритет: HIGH
```

### Промпт 100: Инвалидация кэша при DDL операциях
```
При DDL (CREATE TABLE, ALTER TABLE, DROP TABLE):
1. CREATE TABLE: no invalidation needed (новая таблица)
2. ALTER TABLE: invalidate все запросы к изменённой таблице
3. DROP TABLE: invalidate + cleanup cache entries

Файлы: diesel/CreateTableQuery.java, diesel/QueryCache.java
Приоритет: MEDIUM
```

### Промпт 101: Мониторинг QueryCache
```
Добавь JMX metrics для QueryCache:
1. Cache size (entries count)
2. Hit rate (%)
3. Miss rate (%)
4. Eviction count (LRU, TTL)
5. Average latency for hit vs miss

Файлы: diesel/QueryCacheMXBean.java (новый)
Приоритет: LOW
```

### Промпт 102: Тестирование Parquet storage
```
Напиши тесты для Parquet integration:
1. Write table to Parquet ? read back ? compare data
2. Test projection pushdown (read 2 columns from 10)
3. Test predicate pushdown (WHERE age > 50)
4. Test partition pruning (skip irrelevant partitions)

Файлы: diesel/ParquetStorageTest.java (новый)
Приоритет: HIGH
```

### Промпт 103: Тестирование QueryCache
```
Напиши тесты для QueryCache:
1. Cache hit: одинаковый запрос ? cache hit
2. Cache miss: разный запрос ? cache miss
3. Invalidation: INSERT ? cache invalidated
4. TTL expiry: wait 5 min ? cache expired
5. LRU eviction: fill cache ? oldest evicted

Файлы: diesel/QueryCacheTest.java (новый)
Приоритет: HIGH
```

### Промпт 104: Integration test Parquet + Cache
```
Комплексный тест:
1. Создай таблицу с 1M строк
2. Запиши в Parquet
3. Выполни SELECT ? cache miss ? read from Parquet
4. Повтори SELECT ? cache hit ? return from cache
5. Сделай INSERT ? cache invalidated
6. Повтори SELECT ? cache miss ? read updated data

Файлы: diesel/ParquetCacheIntegrationTest.java (новый)
Приоритет: MEDIUM
```

### Промпт 105: Документация Parquet формата
```
Создай документацию:
1. Как включить Parquet storage (config)
2. Преимущества columnar storage для аналитики
3. Ограничения: не подходит для частых UPDATE
4. Best practices: когда использовать Parquet vs row-based

Файлы: analytics/PARQUET_GUIDE.md (новый)
Приоритет: MEDIUM
```

## Раздел 4: Дополнительные улучшения (20 промптов)

### Промпт 106: Конфигурация Parquet на уровне таблицы
```
Добавь возможность настройки Parquet per table:
```sql
CREATE TABLE analytics (
    id INT,
    event_date DATE,
    ...
) WITH (
    storage_format = 'PARQUET',
    compression = 'ZSTD',
    partition_by = 'event_date'
)
```

Файлы: diesel/CreateTableQuery.java, diesel/TableOptions.java (новый)
Приоритет: MEDIUM
```

### Промпт 107: Lazy загрузка Parquet файлов
```
Не загружай все Parquet файлы в память:
1. Открывай файл только когда нужны данные из него
2. Закрывай после использования (resource management)
3. Кэшируй metadata (schema, statistics) но не данные

Файлы: diesel/ParquetReader.java
Приоритет: MEDIUM
```

### Промпт 108: Predicate pushdown для Parquet
```
Передавай WHERE условия в ParquetReader:
1. Parquet filter row groups по statistics (min/max)
2. Filter individual rows внутри row group
3. Минимизируй I/O: читай только matching данные

Файлы: diesel/ParquetReader.java, diesel/SelectQuery.java
Приоритет: HIGH
```

### Промпт 109: Параллельное чтение Parquet
```
Читай?? Parquet файлы параллельно:
1. Один файл ? один поток (или один row group ? один поток)
2. ForkJoinPool для управления потоками
3. Merge результатов (concatenation для UNION, sorted merge для ORDER BY)

Файлы: diesel/ParquetReader.java, diesel/SelectQuery.java
Приоритет: MEDIUM
```

### Промпт 110: Статистика использования кэша
```
Собирай detailed статистику:
1. Per-query cache performance (hit/miss/ttl)
2. Per-table invalidation frequency
3. Recommendation: "Table USERS is modified frequently, consider reducing cache TTL"

Файлы: diesel/QueryCache.java, diesel/CacheStatistics.java (новый)
Приоритет: LOW
```

### Промпт 111: Настройка Database.java для Parquet by default
```
Для новых таблиц по умолчанию используй Parquet если:
1. Таблица >100K строк (estimated)
2. Query pattern: больше SELECT чем INSERT/UPDATE
3. Columns >10 (columnar эффективнее)

Иначе используй row-based storage.
Файлы: diesel/Database.java, diesel/TableFactory.java (новый)
Приоритет: LOW
```

### Промпт 112: Обработка ошибок при миграции
```
При конвертации table ? Parquet:
1. Валидируй данные перед записью (no nulls в NOT NULL columns)
2. Rollback при ошибке: оставь original table intact
3. Логгируй прогресс: "Migrated 50% of rows..."

Файлы: diesel/ParquetMigrationJob.java (новый)
Приоритет: MEDIUM
```

### Промпт 113: Поддержка partitioned tables в Parquet
```
Расширь поддержку partitioning:
1. Multi-level partitioning: date=.../category=.../data.parquet
2. Dynamic partition discovery: scan directory structure
3. Partition maintenance: DROP OLD PARTITIONS command

Файлы: diesel/PartitionedTable.java
Приоритет: LOW
```

### Промпт 114: Оптимизация Dictionary encoding для строк
```
Для string колонок с low cardinality:
1. Dictionary encoding: map strings ? integers
2. Храни dictionary в metadata
3. Decode только при необходимости (projection)

Файлы: diesel/ParquetWriter.java
Приоритет: LOW
```

### Промпт 115: Compression tuning (ZSTD levels)
```
ZSTD поддерживает уровни сжатия 1-22:
1. Level 1-3: fast compression, good ratio (default)
2. Level 4-9: balanced
3. Level 10-22: slow compression, best ratio (archival)

Добавь настройку уровня в table options.
Файлы: diesel/ParquetWriter.java
Приоритет: LOW
```

### Промпт 116: Row group size tuning
```
Parquet row group size влияет на performance:
1. Small groups (1MB): better pruning, more overhead
2. Large groups (128MB): less overhead, worse pruning
3. Default: 128MB, configurable per table

Файлы: diesel/ParquetWriter.java
Приоритет: LOW
```

### Промпт 117: Column statistics в Parquet metadata
```
Включи сбор статистики для всех колонок:
1. Min, max, null count, distinct count (approximate)
2. Используй для query optimization
3. Обновляй statistics при дописке данных

Файлы: diesel/ParquetWriter.java
Приоритет: MEDIUM
```

### Промпт 118: Bloom filters для Parquet
```
Добавь bloom filters для быстрого lookup:
1. Один bloom filter на колонку на row group
2. Fast check: value possibly present / definitely absent
3. Особенно полезно для JOIN conditions

Файлы: diesel/ParquetWriter.java, diesel/ParquetReader.java
Приоритет: LOW
```

### Промпт 119: Query Cache warm-up strategy
```
При старте сервера:
1. Load frequently used queries from persistent cache
2. Execute them proactively to warm up cache
3. Configurable list of "important" queries

Файлы: diesel/DatabaseServer.java, diesel/CacheWarmer.java (новый)
Приоритет: LOW
```

### Промпт 120: Adaptive TTL для кэша
```
Dynamic TTL based on table activity:
1. High write frequency ? shorter TTL
2. Read-only tables ? longer TTL (or infinite)
3. Learn from access patterns (ML-based?)

Файлы: diesel/QueryCache.java
Приоритет: LOW
```

### Промпт 121: Query normalization improvements
```
Улучши нормализацию SQL для cache key:
1. Ignore whitespace differences
2. Normalize identifier case (uppercase)
3. Replace literals with placeholders: WHERE age = 25 ? WHERE age = ?
4. Sort IN-list values: IN(3,1,2) ? IN(1,2,3)

Файлы: diesel/QueryNormalizer.java (новый)
Приоритет: MEDIUM
```

### Промпт 122: Parameterized query caching
```
Кэшируй параметризованные запросы:
1. Key: normalized SQL с placeholders
2. Value: Map<parameter_values, result>
3. Ограничь количество parameter combinations (max 100 per query)

Файлы: diesel/QueryCache.java
Приоритет: MEDIUM
```

### Промпт 123: Multi-level cache (L1/L2)
```
Двухуровневый кэш:
1. L1: in-memory, fast, small (100 entries, TTL 1 min)
2. L2: off-heap/disk, slower, large (10000 entries, TTL 1 hour)
3. L1 miss ? check L2 ? execute query

Файлы: diesel/QueryCache.java, diesel/OffHeapCache.java (новый)
Приоритет: LOW
```

### Промпт 124: Cache persistence across restarts
```
Персистентный кэш:
1. Save cache to disk on graceful shutdown
2. Load cache on startup
3. Check validity: table schema unchanged?

Файлы: diesel/QueryCache.java, diesel/CachePersistence.java (новый)
Приоритет: LOW
```

### Промпт 125: Final integration testing and documentation
```
Финальные задачи:
1. Full regression test suite (все тесты проходят)
2. Performance benchmark: сравнение before/after оптимизаций
3. Documentation update: новые фичи, ограничения, best practices
4. Release notes для версии 3.0

Файлы: CHANGELOG.md, README.md, analytics/PERFORMANCE_REPORT.md
Приоритет: HIGH