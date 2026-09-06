# SonarQube Fix Prompts - DieselDB

**Дата создания:** 2026-09-06  
**Проект:** dieseldb  
**Источник данных:** sonar7.md, sonaranalytics7.md  
**Цель:** 40 промптов для устранения топ-20 правил Парето (80% проблем)

---

## Промпты по правилам (40 промптов)

### java:S5869 — Remove duplicates in character class (102 проблемы)

**Промпт 1:** Найди в `diesel/SubqueryParser.java:27` regex с дубликатами в character class `[...]`. Удали повторяющиеся символы. Примени ко всем 102 случаям.

**Промпт 2:** Массовое исправление S5869: для каждого `Pattern.compile("...[...duplicates...]...")` удали дубликаты символов в квадратных скобках.

### java:S3776 — Reduce Cognitive Complexity (86 проблем)

**Промпт 3:** Рефакторинг `diesel/DeleteQuery.java:56` (сложность 65): разбей на методы validateCondition(), prepareDelete(), executeDelete(), updateIndexes().

**Промпт 4:** Исправь `diesel/SqlLexer.java:108` (сложность 80): примени State Machine pattern с методами handleIdentifier(), handleNumber(), handleString().

**Промпт 5:** Для методов со сложностью > 40 (Table.java:1890, QueryParser.java:3055) примени Early Return и выдели минимум 3 подметода.

**Промпт 6:** Автоматический рефакторинг S3776: каждый метод > 15 сложности декомпозируй на подметоды по Single Responsibility.

### java:S1192 — Define constant for duplicated literal (25 проблем)

**Промпт 7:** В `diesel/QueryParser.java` литерал `"(?i)^("` дублируется 9 раз. Создай константу CASE_INSENSITIVE_START_PATTERN.

**Промпт 8:** Строка `"Client is not connected: call connect() first"` (8 раз в DatabaseClient.java) → константа ERROR_NOT_CONNECTED.

**Промпт 9:** Для всех литералов 3+ повторений создай класс MessageConstants с константами.

**Промпт 10:** Regex-литералы `"(?i)(SELECT"`, `"(?i)FROM\\s+"` вынеси в константы SELECT_PATTERN, FROM_PATTERN.

**Промпт 11:** Литерал `"Column "` (6 раз в Table.java) → константа COLUMN_PREFIX.

### java:S5843 — Simplify regex complexity (17 проблем)

**Промпт 12:** Упрости regex в `diesel/SubqueryParser.java:74` (сложность 46): разбей на composition из простых паттернов.

**Промпт 13:** Для regex со сложностью > 30 используй possessive quantifiers `++` для уменьшения backtracking.

**Промпт 14:** Проверь 17 regex из S5843 на замену String methods (startsWith, contains).

**Промпт 15:** Добавь комментарии к каждому сложному regex с объяснением частей паттерна.

### java:S2925 — Remove Thread.sleep() (15 проблем)

**Промпт 16:** Замени `Thread.sleep(1000)` в тестах на `Awaitility.await().atMost(5, SECONDS).until(condition)`.

**Промпт 17:** Создай TestWaitHelper.waitForCondition(Supplier<Boolean>, Duration) для всех 15 случаев sleep.

**Промпт 18:** В OomHandlingTest.java и PerformanceTest.java замени sleep на executor.awaitTermination().

**Промпт 19:** Для эмуляции задержки используй @Timeout аннотацию вместо Thread.sleep().

**Промпт 20:** Аудит тестов: классифицируй sleep (race condition / эмуляция) и примени synchronization primitives или Mockito delay.

### java:S3008 — Rename static field (13 проблем)

**Промпт 21:** Переименуй PARALLEL_INDEX_SCAN_THRESHOLD → parallelIndexScanThreshold (camelCase).

**Промпт 22:** POOL_SIZE, QUEUE_CAPACITY → poolSize, queueCapacity в DatabaseServer.java.

**Промпт 23:** MAX_IN_MEMORY_ROWS, MAX_HASH_TABLE_SIZE_BYTES → maxInMemoryRows, maxHashTableSizeBytes.

### java:S3457 — Use format strings correctly (13 проблем)

**Промпт 24:** Исправь String.format("Error:", message) → String.format("Error: %s", message).

**Промпт 25:** Проверь соответствие спецификаторов (%s, %d) количеству аргументов во всех 13 случаях.

**Промпт 26:** Замени конкатенацию `"Error: " + var` на `String.format("Error: %s", var)`.

### java:S1068 — Remove unused private field (12 проблем)

**Промпт 27:** Удали поле socketTimeout в DatabaseServer.java:218, обнови конструктор.

**Промпт 28:** Удали lastJoinEstimatedRows, lastJoinActualRows из SelectQuery.java.

### java:S112 — Define dedicated exception (12 проблем)

**Промпт 29:** Создай иерархию: DieselException → QueryParseException, IndexCorruptionException.

**Промпт 30:** Замени `throw new RuntimeException()` на QuerySyntaxException(sql, reason).

### java:S6541 — Brain Method refactor (12 проблем)

**Промпт 31:** DeleteQuery.java:56 (91 строка) → разбей на validateInput(), acquireLock(), performDelete(), releaseLock().

**Промпт 32:** SqlLexer.java:108 (116 строк) → tokenizeIdentifier(), tokenizeLiteral(), tokenizeOperator().

### java:S135 — Reduce break/continue in loop (12 проблем)

**Промпт 33:** Циклы с >1 break/continue → примени Early Return или Extract Method.

**Промпт 34:** Замени цикл с continue на Stream.filter().forEach().

### java:S3358 — Extract nested ternary (11 проблем)

**Промпт 35:** Замени `a ? b ? c : d : e` на метод evaluateNestedCondition().

**Промпт 36:** Максимальная глубина тернарных операторов — 1 уровень.

### java:S2259 — NullPointerException risk (11 проблем, BUG)

**Промпт 37:** Добавь null-check: `if (unquoted == null) throw new ParsingException(...)`.

**Промпт 38:** Оберни nullable вызовы в Optional.ofNullable(...).orElse(defaultValue).

**Промпт 39:** Используй Objects.requireNonNull(param, "message") для параметров.

### java:S1948 — Make field transient/serializable (10 проблем)

**Промпт 40:** В Serializable классах добавь transient полям: keys, rowIndices, children, params.

---

## Сводная таблица

| # | Правило | Проблем | Промптов | Приоритет |
|---|---------|---------|----------|-----------|
| 1 | S5869 | 102 | 2 | 🔥 Высокий |
| 2 | S3776 | 86 | 4 | 🔥 Критический |
| 3 | S1192 | 25 | 5 | 🔥 Высокий |
| 4 | S5843 | 17 | 4 | Средний |
| 5 | S2925 | 15 | 5 | Средний |
| 6 | S3008 | 13 | 3 | Низкий |
| 7 | S3457 | 13 | 3 | Средний |
| 8 | S1068 | 12 | 2 | Низкий |
| 9 | S112 | 12 | 2 | Средний |
| 10 | S6541 | 12 | 2 | Средний |
| 11 | S135 | 12 | 2 | Низкий |
| 12 | S3358 | 11 | 2 | Средний |
| 13 | S2259 | 11 | 3 | 🔥 Критический (BUG) |
| 14 | S1948 | 10 | 1 | Средний |

**Итого:** 40 промптов для устранения 363 проблем (68.6%)

---

## Рекомендации

1. Начни с топ-3 правил (S5869, S3776, S1192) — 40% результата
2. Группируй однотипные исправления
3. Тестируй после каждого правила
4. Один commit = одно правило Sonar

*Документ создан на основе sonar7.md и sonaranalytics7.md*
