# Prompt Status Tracker

## Priority Queue (Pareto 20% - Critical First)

| № | Статус | Приоритет | Файлы | Проблема |
|---|--------|-----------|-------|----------|
| 1 | ❌ TODO | CRITICAL | SelectQuery.java, QueryParser.java | JOIN OR → OOM (декартово произведение) |
| 5 | ❌ TODO | CRITICAL | QueryParser.java | IN + AND игнорируется (критичная фильтрация) |
| 3 | ❌ TODO | HIGH | SelectQuery.java | GROUP BY по уникальным значениям → 1 строка вместо N |
| 4 | ❌ TODO | HIGH | QueryParser.java | IN со списком значений не работает |
| 22 | ❌ TODO | HIGH | Multiple (13 мест) | Null Pointer Dereference |
| 17 | ❌ TODO | MEDIUM | pom.xml, tests | Разделить тесты на @LargeTest для CI скорости |
| 29 | ❌ TODO | MEDIUM | SelectQuery.java | Refactor execute() complexity=59 |
| 28 | ❌ TODO | MEDIUM | QueryParser.java | Cognitive Complexity оптимизация |

## Full Status (Prompts 1-100)

### Section 0: Priority Retrospective Fixes (1-20)

| № | Статус | Название | Приоритет |
|---|--------|----------|-----------|
| 1 | ❌ TODO | JOIN с OR в условии (OOM) | CRITICAL |
| 2 | ❌ TODO | Оптимизация памяти Cross Join (streaming) | HIGH |
| 3 | ❌ TODO | GROUP BY с уникальными значениями | HIGH |
| 4 | ❌ TODO | IN со списком значений | HIGH |
| 5 | ❌ TODO | IN с дополнительными условиями (AND/OR) | CRITICAL |
| 6 | ❌ TODO | LIMIT без OFFSET | MEDIUM |
| 7 | ❌ TODO | OFFSET без LIMIT | MEDIUM |
| 8 | ❌ TODO | LIMIT + OFFSET вместе | MEDIUM |
| 9 | ❌ TODO | LIMIT в подзапросах | MEDIUM |
| 10 | ❌ TODO | Hash Join оптимизация | MEDIUM |
| 11 | ❌ TODO | EXPLAIN план выполнения | LOW |
| 12 | ❌ TODO | Лимит на макс. количество строк | MEDIUM |
| 13 | ❌ TODO | Улучшение ошибок OOM | MEDIUM |
| 14 | ❌ TODO | Автоматическая статистика | LOW |
| 15 | ❌ TODO | Индексы для JOIN | MEDIUM |
| 16 | ❌ TODO | Кэширование планов | LOW |
| 17 | ❌ TODO | Уменьшение heap для тестов | MEDIUM |
| 18 | ✔ DONE (2026-08-16) | Профилировщик производительности | LOW |
| 19 | ❌ TODO | Тесты на регрессию | MEDIUM |
| 20 | ✔ DONE (2026-08-16) | Документация ограничений | LOW |

### Section 1: Sonar Code Smells (21-40)

| № | Статус | Название | Приоритет |
|---|--------|----------|-----------|
| 21 | ❌ TODO | StackOverflow в regex (S5998) | HIGH |
| 22 | ❌ TODO | Null Pointer Dereference (S2259) | HIGH |
| 23 | ❌ TODO | Удаление мёртвого кода (S2583, S108, S1144, S1068) | LOW |
| 24 | ❌ TODO | Double Brace Initialization (S3599) | LOW |
| 25 | ❌ TODO | Игнорирование возвращаемых значений (S899) | MEDIUM |
| 26 | ❌ TODO | Regex grouping (S5850) | MEDIUM |
| 27 | ❌ TODO | Regex repeated patterns (S5842) | MEDIUM |
| 28 | ❌ TODO | Cognitive Complexity QueryParser (S3776) | MEDIUM |
| 29 | ❌ TODO | Refactor SelectQuery.execute() (complexity=59) | HIGH |
| 30 | ❌ TODO | Оптимизация regex (S5869, S6353) | LOW |
| 31 | ❌ TODO | String literals в константы (S1192) | LOW |
| 32 | ❌ TODO | Параметры методов (S107) | LOW |
| 33 | ❌ TODO | Boolean null (S2447) | MEDIUM |
| 34 | ❌ TODO | Serializable поля (S1948) | LOW |
| 35 | ❌ TODO | Logger вместо System.out (S106) | LOW |
| 36 | ❌ TODO | Специфичные исключения (S112) | LOW |
| 37 | ❌ TODO | Обработка исключений (S2139, S1141) | LOW |
| 38 | ❌ TODO | Unused параметры/переменные/импорты | LOW |
| 39 | ❌ TODO | Упрощение условий | LOW |
| 40 | ❌ TODO | Финальная очистка CODE_SMELL | LOW |

### Section 2: Performance Optimizations (41-60)

| № | Статус | Название | Приоритет |
|---|--------|----------|-----------|
| 41 | ❌ TODO | updateIndicesAfterInsert O(n×m×log n) | HIGH |
| 42 | ❌ TODO | Nested Loop → Hash Join | HIGH |
| 43 | ❌ TODO | Индексы после DELETE | MEDIUM |
| 44 | ❌ TODO | Кластеризованный индекс создание | MEDIUM |
| 45 | ❌ TODO | Пакетная вставка при загрузке | MEDIUM |
| 46 | ❌ TODO | Индексы для WHERE условий | MEDIUM |
| 47 | ❌ TODO | Массовый UPDATE | MEDIUM |
| 48 | ❌ TODO | indexDefinitions сериализация | MEDIUM |
| 49 | ❌ TODO | Состояние индексов в сериализации | MEDIUM |
| 50 | ❌ TODO | Copy-on-Write для транзакций | HIGH |
| 51 | ❌ TODO | Параллельное выполнение запросов | LOW |
| 52 | ❌ TODO | Асинхронный I/O | LOW |
| 53 | ❌ TODO | Compression для сети | LOW |
| 54 | ❌ TODO | Prepared Statements caching | MEDIUM |
| 55 | ❌ TODO | Batch execution | MEDIUM |
| 56 | ❌ TODO | Pagination результатов | MEDIUM |
| 57 | ❌ TODO | Adaptive query execution | LOW |
| 58 | ❌ TODO | Index-only scans | LOW |
| 59 | ❌ TODO | Parallel index scan | LOW |
| 60 | ❌ TODO | SIMD векторизация агрегатов | LOW |

### Section 3: Parquet Integration (61-92)

| № | Статус | Название | Приоритет |
|---|--------|----------|-----------|
| 61 | ❌ TODO | Apache Parquet интеграция | HIGH |
| 62 | ❌ TODO | ParquetReader | HIGH |
| 63 | ❌ TODO | Columnar storage | HIGH |
| 64 | ❌ TODO | Schema evolution | MEDIUM |
| 65 | ❌ TODO | Partitioning для Parquet | MEDIUM |
| 66 | ❌ TODO | Compression codecs | MEDIUM |
| 67 | ❌ TODO | Statistics metadata | MEDIUM |
| 68 | ❌ TODO | Bloom filters | MEDIUM |
| 69 | ❌ TODO | QueryCache архитектура | MEDIUM |
| 70 | ❌ TODO | Cache invalidation стратегия | MEDIUM |
| 71 | ❌ TODO | QueryCache в SelectQuery | MEDIUM |
| 72 | ❌ TODO | Инвалидация при INSERT | MEDIUM |
| 73 | ❌ TODO | Инвалидация при UPDATE | MEDIUM |
| 74 | ❌ TODO | Инвалидация при DELETE | MEDIUM |
| 75 | ❌ TODO | Инвалидация при DDL | MEDIUM |
| 76 | ❌ TODO | Мониторинг QueryCache | LOW |
| 77 | ❌ TODO | Тестирование Parquet | HIGH |
| 78 | ❌ TODO | Тестирование QueryCache | MEDIUM |
| 79 | ❌ TODO | Integration test Parquet+Cache | HIGH |
| 80 | ❌ TODO | Документация Parquet | LOW |
| 81 | ❌ TODO | Конфигурация Parquet на уровне таблицы | MEDIUM |
| 82 | ❌ TODO | Lazy загрузка Parquet | MEDIUM |
| 83 | ❌ TODO | Predicate pushdown | HIGH |
| 84 | ❌ TODO | Параллельное чтение Parquet | MEDIUM |
| 85 | ❌ TODO | Статистика использования кэша | LOW |
| 86 | ❌ TODO | Database.java для Parquet default | MEDIUM |
| 87 | ❌ TODO | Обработка ошибок миграции | MEDIUM |
| 88 | ❌ TODO | Partitioned tables в Parquet | MEDIUM |
| 89 | ❌ TODO | Dictionary encoding для строк | LOW |
| 90 | ❌ TODO | Compression tuning (ZSTD) | LOW |
| 91 | ❌ TODO | Row group size tuning | LOW |
| 92 | ❌ TODO | Column statistics metadata | LOW |

### Section 4: Advanced Features (93-100)

| № | Статус | Название | Приоритет |
|---|--------|----------|-----------|
| 93 | ❌ TODO | Bloom filters для Parquet | MEDIUM |
| 94 | ❌ TODO | Cache warm-up strategy | LOW |
| 95 | ❌ TODO | Adaptive TTL для кэша | LOW |
| 96 | ❌ TODO | Query normalization improvements | MEDIUM |
| 97 | ❌ TODO | Parameterized query caching | MEDIUM |
| 98 | ❌ TODO | Multi-level cache (L1/L2) | LOW |
| 99 | ❌ TODO | Cache persistence across restarts | LOW |
| 100 | ❌ TODO | Final integration testing & docs | HIGH |

## Legend

- ❌ TODO - Not started
- 🔄 IN_PROGRESS - Currently working
- ✅ DONE - Completed
- ⚠️ BLOCKED - Blocked by dependency

## How to Update

1. Choose next prompt from Priority Queue (top of table)
2. Change status to 🔄 IN_PROGRESS
3. After implementation and tests pass, change to ✅ DONE
4. Add date completed in format `✅ DONE (2025-01-15)`
