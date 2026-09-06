# 16 Нереализованных приоритетных промптов (Section 0 из prompt2.md)

> Статус на 06.09.2026: **4 из 20 выполнено** (Prompt 1, 3, 5, 81).  
> Ниже — **16 оставшихся** промптов из раздела "Priority Retrospective Fixes" (промпты 2, 4, 6–20).

---

### Промпт 2: Оптимизация памяти для Cross Join (streaming)
**Приоритет: HIGH (масштабируемость)**

**Проблема:** QuantitativeTest требует 4GB heap из-за хранения всех результатов в памяти.

**Задача:**
1. Реализуй streaming для SELECT результатов (Iterator<Row> вместо List<Row>)
2. Добавь external sort для ORDER BY когда результат > available memory
3. Используй File-based temporary storage для больших промежуточных результатов
4. Добавь конфиг: max.inmemory.rows = 10000 (превышение → spill to disk)

**Файлы:** diesel/SelectQuery.java, diesel/Table.java  
**Конфиг:** diesel.properties

---

### Промпт 4: Исправление IN со списком значений
**Приоритет: HIGH (некорректная фильтрация)**

**Проблема:** WHERE AGE IN (50, 51, 52) возвращает 2 строки вместо 21.

**Задача:**
1. Проверь парсинг списка значений в QueryParser.parseInList()
2. Убедись что все значения из списка корректно добавляются в Condition
3. Проверь фильтрацию: row.value IN (list) должно проверять все элементы
4. Добавь тесты: IN с 1, 3, 10, 100 значениями; IN с NULL в списке

**Файлы:** diesel/QueryParser.java, diesel/SelectQuery.java  
**Тесты:** InTest (расширить покрытие)

---

### Промпт 6: Исправление LIMIT без OFFSET
**Приоритет: HIGH (некорректное ограничение результата)**

**Проблема:** LIMIT 10 возвращает некорректное количество строк.

**Задача:**
1. Проверь применение limit в SelectQuery.execute() после всех операций
2. Убедись что limit применяется ПОСЛЕ сортировки (ORDER BY ... LIMIT)
3. Тесты: LIMIT 1, LIMIT 10, LIMIT 100, LIMIT больше чем всего строк
4. Проверь взаимодействие LIMIT с GROUP BY и агрегатами

**Файлы:** diesel/SelectQuery.java  
**Тесты:** LimitOffsetTest (создать новый файл)

---

### Промпт 7: Исправление OFFSET без LIMIT
**Приоритет: MEDIUM**

**Проблема:** OFFSET 5 без LIMIT пропускает первые 5 строк но может вернуть 0.

**Задача:**
1. Проверь что offset применяется после сортировки
2. Если offset > total rows → верни пустой результат (а не ошибку)
3. Тесты: OFFSET 0, OFFSET 5, OFFSET больше чем всего строк
4. Добавь предупреждение: "OFFSET without LIMIT may be inefficient"

**Файлы:** diesel/SelectQuery.java  
**Тесты:** LimitOffsetTest

---

### Промпт 8: Исправление LIMIT + OFFSET вместе
**Приоритет: HIGH**

**Проблема:** LIMIT 10 OFFSET 5 возвращает 0 строк вместо ожидаемых.

**Задача:**
1. Проверь порядок применения: сначала ORDER BY, потом OFFSET, потом LIMIT
2. Формула: result.slice(offset, offset + limit)
3. Тесты: LIMIT 10 OFFSET 5, LIMIT 1 OFFSET 99, LIMIT 100 OFFSET 0
4. Проверь edge cases: offset=0, limit=0, offset+limit > total

**Файлы:** diesel/SelectQuery.java  
**Тесты:** LimitOffsetTest

---

### Промпт 9: Исправление LIMIT в подзапросах
**Приоритет: HIGH**

**Проблема:** В подзапросах LIMIT игнорируется (возвращает 600 строк вместо 10).

**Задача:**
1. Проверь выполнение подзапросов в SubqueryParser или SelectQuery
2. Убедись что LIMIT из подзапроса применяется к результату подзапроса
3. Тесты: SELECT * FROM (SELECT ... LIMIT 10) AS subq
4. Проверь вложенные подзапросы (2+ уровня)

**Файлы:** diesel/SubqueryParser.java, diesel/SelectQuery.java  
**Тесты:** SubqueriesTest

---

### Промпт 10: Оптимизация Hash Join для больших таблиц
**Приоритет: MEDIUM (профилактика OOM)**

**Проблема:** Hash Join создаёт хеш-таблицу в памяти которая может вызвать OOM.

**Задача:**
1. Добавь оценку размера хеш-таблицы до начала построения
2. Если estimated size > max.inmemory.rows → fallback на Block Nested Loop Join
3. Реализуй partitioned hash join для таблиц > memory (spill to disk)
4. Добавь метрики: hash table size, build time, probe time

**Файлы:** diesel/SelectQuery.java  
**Конфиг:** max.hash.table.size.mb = 512

---

### Промпт 11: Добавление EXPLAIN для анализа плана выполнения
**Приоритет: MEDIUM (диагностика)**

**Проблема:** Нет способа понять почему запрос медленный или потребляет много памяти.

**Задача:**
1. Реализуй команду EXPLAIN SELECT/INSERT/UPDATE/DELETE
2. Выводи: тип JOIN (Hash/Nested Loop), estimated rows, используемые индексы
3. Формат: текстовое дерево плана выполнения
4. EXPLAIN ANALYZE: выполни запрос и покажи фактические метрики

**Файлы:** diesel/ExplainQuery.java (новый), diesel/SelectQuery.java  
**Тесты:** ExplainTest (новый)

---

### Промпт 12: Лимит на максимальное количество строк в результате
**Приоритет: HIGH (защита от crash)**

**Проблема:** Нет защиты от accidental cross join который генерирует миллиарды строк.

**Задача:**
1. Добавь конфиг: max.result.rows = 1000000 (1 миллион)
2. Если результат превышает лимит → выброси exception с понятным сообщением
3. Добавь hint: /* MAX_ROWS=10000 */ для override на уровне запроса
4. Логгируй предупреждение при достижении 80% лимита

**Файлы:** diesel/SelectQuery.java, diesel/Database.java  
**Конфиг:** max.result.rows

---

### Промпт 13: Улучшение ошибок OutOfMemoryError
**Приоритет: MEDIUM (debuggability)**

**Проблема:** OOM падает без полезной информации о причине.

**Задача:**
1. Перехватывай OutOfMemoryError в DatabaseServer.ClientHandler
2. Логируй контекст: какой запрос, сколько строк, сколько памяти выделено
3. Отправляй клиенту: "Error: Query exceeded memory limit. Consider adding LIMIT or indexes."
4. Добавь метрику: peak.memory.usage.per.query

**Файлы:** diesel/DatabaseServer.java, diesel/SelectQuery.java

---

### Промпт 14: Автоматическая статистика по таблицам
**Приоритет: MEDIUM (основа для оптимизатора)**

**Проблема:** Оптимизатор не знает размер таблиц для выбора плана выполнения.

**Задача:**
1. Храни в Table: rowCount, avgRowSize, lastAnalyzed timestamp
2. Обновляй статистику после INSERT/DELETE (асинхронно)
3. Добавь команду: ANALYZE TABLE name (принудительный пересчёт)
4. Используй статистику для выбора Hash Join vs Nested Loop

**Файлы:** diesel/Table.java, diesel/Database.java

---

### Промпт 15: Индексы для ускорения JOIN условий
**Приоритет: MEDIUM (производительность)**

**Проблема:** JOIN без индексов требует полного сканирования обеих таблиц.

**Задача:**
1. Авто-создавай индекс на колонках JOIN условия если его нет
2. Предупреждай: "Consider creating index on TABLE.COLUMN for faster JOIN"
3. Для FOREIGN KEY автоматически создавай индекс (как в PostgreSQL)
4. Добавь тест: JOIN с индексом vs без (benchmark)

**Файлы:** diesel/SelectQuery.java, diesel/CreateIndexQuery.java

---

### Промпт 16: Кэширование планов выполнения запросов
**Приоритет: LOW (оптимизация)**

**Проблема:** Парсинг и планирование выполняется заново для каждого запроса.

**Задача:**
1. Кэшируй AST + план выполнения для параметризованных запросов
2. Ключ кэша: normalized SQL (без литералов, только структура)
3. Инвалидация: при DDL или изменении статистики таблиц
4. Метрики: cache hit rate, average parse time saved

**Файлы:** diesel/QueryCache.java (новый), diesel/QueryParser.java

---

### Промпт 17: Уменьшение heap requirement для тестов
**Приоритет: HIGH (CI/CD эффективность)**

**Проблема:** QuantitativeTest требует 4GB heap что медленно и дорого в CI.

**Задача:**
1. Раздели QuantitativeTest на маленькие тесты по 50MB каждый
2. Добавь @LargeTest аннотацию для тестов требующих >1GB (skip в CI по умолчанию)
3. Оптимизируй тестовые данные: меньше строк, более репрезентативные выборки
4. Цель: полный набор тестов запускается с -Xmx512m за <5 минут

**Файлы:** diesel/QuantitativeTest.java, pom.xml

---

### Промпт 18: Профилировщик производительности запросов
**Приоритет: MEDIUM (диагностика)**

**Проблема:** Неясно какая часть запроса consumes больше всего времени.

**Задача:**
1. Добавь профилирование: parse time, plan time, execute time, sort time
2. Вывод в лог для медленных запросов (>1s): "Slow query breakdown: ..."
3. Метрики в JMX/SLF4J для мониторинга
4. Флаг: -Ddiesel.profile.slow.threshold.ms=1000

**Файлы:** diesel/SelectQuery.java, diesel/QueryParser.java

---

### Промпт 19: Тесты на регрессию производительности
**Приоритет: MEDIUM (quality gate)**

**Проблема:** Нет автоматической детекции деградации производительности.

**Задача:**
1. Сохраняй baseline timing для ключевых запросов (timing60.md)
2. В CI сравнивай текущее время с baseline (допустимо ±20%)
3. При деградации >20% → fail build с отчётом
4. Храни историю производительности в analytics/performance_history.csv

**Файлы:** .github/workflows/ci.yml (новый step), diesel/PerformanceRegressionTest.java

---

### Промпт 20: Документация известных ограничений и workaround
**Приоритет: LOW (user experience)**

**Проблема:** Пользователи не знают о ограничениях DieselDB.

**Задача:**
1. Создай KNOWN_LIMITATIONS.md в корне проекта
2. Опиши: макс. размер результата, ограничения JOIN с OR, требования к памяти
3. Для каждого ограничения предложи workaround (например "используй LIMIT")
4. Обнови README.md ссылкой на этот документ

**Файлы:** KNOWN_LIMITATIONS.md (новый), README.md


#### 1.1 ALTER TABLE семейство запросов
```sql
-- PostgreSQL/MySQL поддерживают, DieselDB НЕТ:
ALTER TABLE table_name ADD COLUMN column_name data_type;
ALTER TABLE table_name DROP COLUMN column_name;
ALTER TABLE table_name ALTER COLUMN column_name TYPE new_type;
ALTER TABLE table_name ALTER COLUMN column_name SET DEFAULT value;
ALTER TABLE table_name ALTER COLUMN column_name DROP DEFAULT;
ALTER TABLE table_name ADD CONSTRAINT constraint_name PRIMARY KEY (column);
ALTER TABLE table_name DROP CONSTRAINT constraint_name;
ALTER TABLE table_name RENAME TO new_name;
ALTER TABLE table_name RENAME COLUMN old_name TO new_name;
```

#### 1.2 DROP семейство запросов
```sql
-- Частично реализовано только DROP TABLE
DROP INDEX index_name ON table_name;        -- НЕТ
DROP VIEW view_name;                         -- НЕТ
DROP SEQUENCE sequence_name;                 -- НЕТ
TRUNCATE TABLE table_name;                   -- НЕТ

#### 1.5 Последовательности (Sequences)
```sql
-- Базовая поддержка есть в CreateTableQuery, но нет отдельных DDL:
CREATE SEQUENCE seq_name START WITH 1 INCREMENT BY 1;
ALTER SEQUENCE seq_name RESTART WITH 100;
DROP SEQUENCE seq_name;
SELECT NEXTVAL('seq_name');
SELECT CURRVAL('seq_name');

### 3. **Query Result Cache (из Oracle)**
**Что это:** Кэширование результатов идентичных запросов.

**Превосходство над PostgreSQL:**
- Oracle кэширует результаты на уровне SQL текста + bind variables
- Автоматическая инвалидация при изменении таблиц
- Работает для deterministic функций

**Почему 80/20:**
- Минимальные изменения в Query Executor
- HashMap<String, CachedResult> на уровне Database
- TTL-based или invalidation-based

**Реализация:**
```java
class QueryCache {
    Map<String, CachedResult> cache; // key = normalized SQL + params
    // Invalidating на INSERT/UPDATE/DELETE
}
```

**Ожидаемый результат:** +500-1000% для read-heavy workloads с повторяющимися запросами

### 4. **Bulk Insert/Copy API (из SQL Server BULK INSERT, Oracle SQL*Loader, DB2 LOAD)**
**Что что:** Массовая загрузка данных из файлов.

**Превосходство над PostgreSQL:**
- SQL Server: BULK INSERT с минимальным logging
- Oracle: Direct Path Load обходит buffer cache
- Teradata: MultiLoad для параллельной загрузки

**Почему 80/20:**
- Простой парсинг CSV/TXT
- Batch insert уже существует - добавить оптимизацию
- Отключить индексы на время загрузки, перестроить после

**Реализация:**
```sql
BULK INSERT table_name FROM 'file.csv' 
WITH (FORMAT='CSV', BATCHSIZE=10000, SORTED_DATA=false)
```

**Ожидаемый результат:** +1000% скорость массовой загрузки данных

### 5. **Bitmap Indexes (из Oracle, Teradata)**
**Что это:** Индексы для колонок с низкой кардинальностью.

**Превосходство над PostgreSQL:**
- Идеально для gender, status, category (мало уникальных значений)
- Bitmap AND/OR операции очень быстрые
- Сжатие лучше B-Tree в 10 раз для таких данных

**Почему 80/20:**
- Альтернатива B-Tree, не заменяет его
- Простая структура: bitset per distinct value
- Быстрые bitwise операции для WHERE clause

**Реализация:**
```java
class BitmapIndex {
    Map<Object, BitSet> bitmaps; // value -> bitmap rows
    // WHERE status='ACTIVE' -> bitmap lookup
}
```

**Ожидаемый результат:** +200-500% для WHERE с low-cardinality columns

### 9. **Parallel Query Execution (из Oracle, DB2, Teradata, SAP HANA)**
**Что это:** Один запрос выполняется несколькими потоками.

**Превосходство над PostgreSQL:**
- Oracle: Parallel Query для scan, join, aggregation
- Teradata: MPP архитектура - все запросы параллельны
- SAP HANA: Параллелизм на уровне column operations

**Почему средний приоритет:**
- Требует thread-safe итераторов
- Но существующая Java concurrency помогает

**Реализация:**
- Parallel Table Scan (разделить диапазоны строк)
- Parallel Aggregation (map-reduce стиль)

**Ожидаемый результат:** +200-800% для тяжелых аналитических запросов

#### 1. Virtual Threads (Project Loom)
- **Описание**: Легковесные потоки для высоконагруженных приложений
- **Преимущества**:
    - Увеличение пропускной способности в 10-100 раз для I/O операций
    - Упрощение кода (нет необходимости в реактивных фреймворках)
    - Снижение потребления памяти на поток
- **Прирост производительности**: до 100x для concurrent workloads
- **Критичность**: 🔴 Высокая — конкурентное преимущество

#### 3. Record Patterns & Pattern Matching for Switch
- **Описание**: Улучшенная работа с данными
- **Преимущества**:
    - Снижение boilerplate кода на 30-40%
    - Улучшенная читаемость и maintainability
    - Type-safe обработка данных
- **Критичность**: 🟡 Средняя — developer productivity

#### 7. **UNION/INTERSECT/EXCEPT**
```sql
SELECT name FROM users WHERE age > 30
UNION
SELECT name FROM customers WHERE active = true
INTERSECT
SELECT name FROM vip_members;
```

**Ожидаемый эффект:** +10% к полноте SQL

## Сводная таблица статуса Section 0

| # | Промпт | Статус | Приоритет |
|---|--------|--------|-----------|
| 1 | JOIN с OR в условии (OOM) | ✅ **DONE** | CRITICAL |
| 2 | Cross Join streaming | ⬜ TODO | HIGH |
| 3 | GROUP BY unique values | ✅ **DONE** | HIGH |
| 4 | IN со списком значений | ⬜ TODO | HIGH |
| 5 | IN + AND/OR | ✅ **DONE** | CRITICAL |
| 6 | LIMIT без OFFSET | ⬜ TODO | HIGH |
| 7 | OFFSET без LIMIT | ⬜ TODO | MEDIUM |
| 8 | LIMIT + OFFSET вместе | ⬜ TODO | HIGH |
| 9 | LIMIT в подзапросах | ⬜ TODO | HIGH |
| 10 | Hash Join оптимизация | ⬜ TODO | MEDIUM |
| 11 | EXPLAIN | ⬜ TODO | MEDIUM |
| 12 | max.result.rows guard | ⬜ TODO | HIGH |
| 13 | OOM error handling | ⬜ TODO | MEDIUM |
| 14 | Table statistics | ⬜ TODO | MEDIUM |
| 15 | Auto-indexes для JOIN | ⬜ TODO | MEDIUM |
| 16 | Query plan cache | ⬜ TODO | LOW |
| 17 | Reduce test heap | ⬜ TODO | HIGH |
| 18 | Query profiler | ⬜ TODO | MEDIUM |
| 19 | Performance regression tests | ⬜ TODO | MEDIUM |
| 20 | KNOWN_LIMITATIONS.md | ⬜ TODO | LOW |

---

## Рекомендуемый порядок выполнения (по Pareto)

1. **Prompt 2** — Streaming/External Sort (снимет 4GB heap requirement)
2. **Prompt 4** — IN list fix (базовая корректность)
3. **Prompt 6–9** — LIMIT/OFFSET (базовый функционал пагинации)
4. **Prompt 10** — Partitioned Hash Join (spill to disk)
5. **Prompt 11** — EXPLAIN ANALYZE (видимость планов)
6. **Prompt 12** — max.result.rows (защита от runaway queries)
7. **Prompt 14** — Table statistics (основа для оптимизатора)
8. **Prompt 15** — Auto-indexes для JOIN
9. **Prompt 17** — Split QuantitativeTest / @LargeTest
10. **Prompt 19** — CI performance regression gate