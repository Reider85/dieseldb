# 80 Промптов для улучшения DieselDB

## Раздел 1: Исправление ошибок SonarQube (20 промптов)
*На основе документа sonaranalitics.md - приоритет P0 и P1*

### Промпт 1: Исправление StackOverflow в регулярных выражениях (java:S5998)
```
Проанализируй файлы QueryParser.java, SubqueryParser.java и SqlLexer.java. Найди 57 регулярных выражений, которые могут вызвать StackOverflowError (правило java:S5998). Для каждого problematic regex:
1. Определи причину (чрезмерная вложенность, catastrophic backtracking)
2. Предложи упрощённую версию с possessive quantifiers (?>...)
3. Разбей сложные regex на несколько простых паттернов
4. Добавь тесты на SQL-запросы глубиной 100+ уровней вложенности

Приоритет: CRITICAL (BUG, может вызвать падение production)
Файлы: diesel/QueryParser.java, diesel/SubqueryParser.java, diesel/SqlLexer.java
```

### Промпт 2: Исправление Null Pointer Dereference (java:S2259)
```
Найди 13 мест в коде где возможен NullPointerException (правило java:S2259). Для каждого случая:
1. Добавь проверку null перед использованием объекта
2. Используй Optional<T> где это уместно (Java 8+)
3. Добавь @NotNull/@Nullable аннотации для документирования
4. Напиши unit test для проверки edge cases

Примеры мест: Database.getTableForQuery(), Transaction.cloneTable(), SelectQuery.execute()
Приоритет: CRITICAL (BUG, вызывает падение сервера)
```

### Промпт 3: Удаление мёртвого кода (java:S2583, S108, S1144, S1068)
```
Найди и удали весь мёртвый код в проекте:
1. Conditionally executed code that is never reachable (3 места, java:S2583)
2. Empty nested blocks (10 мест, java:S108)
3. Unused private methods (3 места, java:S1144)
4. Unused private fields (4 места, java:S1068)

Используй IntelliJ: Analyze → Run Inspection by Name → "Dead code"
Добавь test что удалённый код действительно не использовался.
Приоритет: LOW но БЫСТРО (-20 проблем за 30 минут)
```

### Промпт 4: Исправление Double Brace Initialization (java:S3599)
```
Найди 2 случая Double Brace Initialization и замени на нормальную инициализацию:

БЫЛО:
```java
Map<String, Object> map = new HashMap<>() {{
    put("key", "value");
}};
```

СТАЛО:
```java
Map<String, Object> map = new HashMap<>();
map.put("key", "value");
```

ИЛИ используй Map.of() для immutable maps:
```java
Map<String, Object> map = Map.of("key", "value");
```

Приоритет: BUG (утечки памяти, проблемы с сериализацией)
```

### Промпт 5: Исправление игнорирования возвращаемых значений (java:S899)
```
Найди 2 места где игнорируются возвращаемые значения важных методов:

БЫЛО:
```java
stringBuilder.append("value"); // return value ignored
stream.forEach(...); // stream result ignored
```

СТАЛО:
```java
StringBuilder result = stringBuilder.append("value");
// ИЛИ явно игнорируй с комментарием
stream.forEach(...); // Intentionally ignoring result
```

Приоритет: BUG (может скрывать ошибки)
```

### Промпт 6: Исправление проблем с regex grouping (java:S5850)
```
Найди 3 случая где alternations в regex должны быть сгруппированы:

БЫЛО: `SELECT|INSERT|UPDATE FROM table`
НЕПРАВИЛЬНО: матчит "SELECT" ИЛИ "INSERT FROM table" ИЛИ "UPDATE FROM table"

СТАЛО: `(?:SELECT|INSERT|UPDATE) FROM table`
ПРАВИЛЬНО: матчит любой keyword followed by "FROM table"

Добавь тесты на парсинг разных SQL statements.
Приоритет: BUG (некорректная работа парсера)
```

### Промпт 7: Исправление regex с repeated patterns (java:S5842)
```
Найди 1 случай где repeated pattern в regex match empty string:

БЫЛО: `(a*)*` или `([^"]*")*`
ПРОБЛЕМА: может вызвать infinite loop или catastrophic backtracking

СТАЛО: `(?:a+)*` или `([^"]*+")*`
РЕШЕНИЕ: используем possessive quantifiers или atomic groups

Приоритет: BUG (бесконечные циклы в парсере)
```

### Промпт 8: Оптимизация Cognitive Complexity в QueryParser.java (java:S3776)
```
Проанализируй QueryParser.java (363 проблемы, cognitive complexity 37-45).

Разбей brain methods (строки 974, 1114, 1971, 2678, 2793) на smaller methods:
1. Выдели parseWhereClause() из main parse method
2. Выдели parseJoinClause() 
3. Выдели parseSubquery()
4. Выдели parseOrderBy()
5. Выдели parseGroupBy()

Цель: снизить complexity с 37-45 до <20 для каждого метода.
Добавь unit tests для каждого выделенного метода.
Приоритет: HIGH (68% всех проблем в этом файле)
```

### Промпт 9: Рефакторинг SelectQuery.execute() (complexity=59)
```
Метод execute() в SelectQuery.java имеет complexity=59 (при норме ~15).

Раздели на методы:
1. executeWhereClause(rows) - фильтрация по WHERE
2. executeGroupBy(rows) - группировка
3. executeHaving(rows) - фильтрация групп
4. executeOrderBy(rows) - сортировка
5. executeJoins(rows) - JOIN обработка
6. executeSelectColumns(rows) - проекция колонок

Каждый метод должен иметь complexity <20.
Добавь integration tests что результат тот же.
Приоритет: CRITICAL (главный источник багов)
```

### Промпт 10: Оптимизация регулярных выражений (java:S5869, S6353)
```
Исправь 360 проблем с regex в парсере:

1. java:S5869 (228 issues) - удали дубликаты символов:
   - `[a-zA-Z0-9_]` → `\w`
   - `[0-9]` → `\d`
   - `[ \t\n\r\f]` → `\s`
   - `[a-z]` → использовать case-insensitive flag

2. java:S6353 (119 issues) - используй краткие формы:
   - Автоматическая замена через IDE (Find/Replace)

3. java:S5843 (13 issues) - упрости сложные regex:
   - Разбей на несколько простых паттернов
   - Используй named groups для читаемости

Benchmark: парсинг 1000 SQL запросов до/после.
Приоритет: HIGH (+30-50% производительности парсера)
```

### Промпт 11: Вынос строковых литералов в константы (java:S1192)
```
Найди 34 случая дублирования строковых литералов и вынеси в константы:

БЫЛО:
```java
if (token.equals("SELECT")) { ... }
if (keyword.equals("SELECT")) { ... }
```

СТАЛО:
```java
public static final String KEYWORD_SELECT = "SELECT";
if (token.equals(KEYWORD_SELECT)) { ... }
if (keyword.equals(KEYWORD_SELECT)) { ... }
```

Создай класс SqlKeywords.java с всеми SQL keywords:
```java
public final class SqlKeywords {
    public static final String SELECT = "SELECT";
    public static final String FROM = "FROM";
    public static final String WHERE = "WHERE";
    public static final String JOIN = "JOIN";
    // ... все keywords
}
```

Приоритет: MEDIUM (упрощение рефакторинга)
```

### Промпт 12: Уменьшение количества параметров методов (java:S107)
```
Найди 20 методов с >7 параметрами и примени refactoring:

БЫЛО:
```java
public void process(String name, int age, String email, 
                   boolean active, Date created, String role,
                   int department, String manager) { ... }
```

СТАЛО - используем Parameter Object:
```java
public class UserContext {
    String name;
    int age;
    String email;
    boolean active;
    Date created;
    String role;
    int department;
    String manager;
}

public void process(UserContext context) { ... }
```

ИЛИ Builder pattern:
```java
public void process(UserContext.Builder builder) { ... }
```

Приоритет: MEDIUM (нарушение инкапсуляции)
```

### Промпт 13: Исправление null из Boolean методов (java:S2447)
```
Найди 8 мест где Boolean методы возвращают null:

БЫЛО:
```java
public Boolean isValid() {
    if (condition) return true;
    if (otherCondition) return false;
    return null; // PROBLEM!
}

// Usage:
if (obj.isValid()) { ... } // NullPointerException!
```

СТАЛО:
```java
public Boolean isValid() {
    if (condition) return true;
    return false; // Never return null
}

// ИЛИ используем Optional:
public Optional<Boolean> isValid() {
    if (condition) return Optional.of(true);
    if (otherCondition) return Optional.of(false);
    return Optional.empty();
}
```

Приоритет: HIGH (NullPointerException в условиях)
```

### Промпт 14: Исправление Serializable полей (java:S1948)
```
Найди 8 не-serializable полей в Serializable классах:

БЫЛО:
```java
class Table implements Serializable {
    private transient Logger logger = Logger.getLogger(...); // OK
    private Connection connection; // PROBLEM! Not serializable
}
```

СТАЛО:
```java
class Table implements Serializable {
    private transient Logger logger = Logger.getLogger(...);
    private transient Connection connection; // Mark as transient
    
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.connection = createNewConnection(); // Reinitialize after deserialization
    }
}
```

Приоритет: HIGH (проблемы кластеризации/репликации)
```

### Промпт 15: Замена System.out.println на Logger (java:S106)
```
Замени 8 случаев System.out.println на proper logging:

БЫЛО:
```java
System.out.println("Query executed: " + sql);
System.out.println("Error: " + e.getMessage());
```

СТАЛО:
```java
private static final Logger LOGGER = Logger.getLogger(ClassName.class.getName());

LOGGER.info("Query executed: " + sql);
LOGGER.severe("Error: " + e.getMessage(), e);
```

Настрой log levels:
- INFO для обычных операций
- WARNING для recoverable errors
- SEVERE для critical errors

Приоритет: MEDIUM (proper logging для production)
```

### Промпт 16: Использование специфичных исключений (java:S112)
```
Замени 8 случаев generic exceptions на specific:

БЫЛО:
```java
throw new Exception("Something went wrong");
throw new RuntimeException("Error");
```

СТАЛО:
```java
throw new IllegalArgumentException("Invalid parameter: " + paramName);
throw new IllegalStateException("Database not initialized");
throw new SQLException("Failed to execute query", cause);
throw new FileNotFoundException("Table file not found: " + tableName);
```

Создай custom exceptions если нужно:
```java
public class QueryParseException extends RuntimeException { ... }
public class ConstraintViolationException extends RuntimeException { ... }
```

Приоритет: MEDIUM (лучшая обработка ошибок)
```

### Промпт 17: Упрощение обработки исключений (java:S2139, S1141)
```
Исправь проблемы с exception handling:

1. java:S2139 (8 issues) - не логировать И выбрасывать:
БЫЛО:
```java
try {
    ...
} catch (Exception e) {
    logger.error("Error", e);
    throw e; // Both logging AND rethrowing
}
```

СТАЛО:
```java
try {
    ...
} catch (Exception e) {
    logger.error("Error during operation", e);
    throw new CustomException("Operation failed", e); // Or just rethrow without logging
}
```

2. java:S1141 (8 issues) - упростить nested try-catch:
Вынести inner try-catch в отдельный метод.

Приоритет: MEDIUM (cleaner error handling)
```

### Промпт 18: Удаление unused параметров, переменных, импортов
```
Удали весь unused code:
1. java:S1172 (20 issues) - unused method parameters
2. java:S1854 (15 issues) - unused assignments  
3. java:S1481 (13 issues) - unused local variables
4. java:S1128 (24 issues) - unnecessary imports

Используй IntelliJ: Code → Optimize Imports
Code → Analyze Code → Run Inspection → "Unused declaration"

Приоритет: LOW но ОЧЕНЬ БЫСТРО (-72 проблемы за 1 час)
```

### Промпт 19: Упрощение условий и тернарных операторов
```
Исправь:
1. java:S3358 (7 issues) - nested ternary operators:
БЫЛО: `a ? b : c ? d : e ? f : g`
СТАЛО: if-else ladder или extract method

2. java:S2589 (4 issues) - gratuitous boolean expressions:
БЫЛО: `if (flag == true)`
СТАЛО: `if (flag)`

3. java:S1066 (3 issues) - mergeable if statements:
БЫЛО:
```java
if (condition1) {
    if (condition2) { ... }
}
```
СТАЛО: `if (condition1 && condition2) { ... }`

Приоритет: LOW (улучшение читаемости)
```

### Промпт 20: Финальная очистка (остальные CODE_SMELL)
```
Исправь оставшиеся minor issues:
1. java:S127 (8) - for loop stop conditions invariant
2. java:S2925 (7) - Thread.sleep in tests (используй Awaitility)
3. java:S125 (3) - commented out code (удалить)
4. java:S3457 (16) - incorrect format strings
5. java:S5786 (16) - JUnit5 public visibility (remove public)
6. java:S6201 (49) - pattern matching for instanceof (Java 16+)

Цель: снизить общее количество проблем с 908 до <450.
Приоритет: LOW (косметические улучшения)
```

---

## Раздел 2: Исправление проблем производительности и архитектуры (20 промптов)
*На основе документа problems.md - O(n²) проблемы и архитектура транзакций*

### Промпт 21: Оптимизация updateIndicesAfterInsert (O(n × m × log n))
```
Проанализируй метод Table.updateIndicesAfterInsert() (строки 476-512).

Проблема: Вложенные циклы обновляют все индексы для всех последующих строк после вставки.
Сложность: O(n × m × log n) где n - строки, m - индексы

Предложи и реализуй оптимизации:
1. Используй lazy index updates - отложи обновление до момента чтения
2. ИЛИ используй delta buffering - накапливай изменения и применяй батчами
3. ИЛИ измени структуру данных на skip list / tree-based storage для эффективных вставок в середину

Добавь benchmark: вставка 10K строк в середину таблицы с 5 индексами.
Цель: ускорение с O(n²) до O(n log n) или лучше.
```

### Промпт 22: Замена Nested Loop Join на Hash Join
```
Проанализируй SelectQuery.java (строки 186-218) где реализован Nested Loop Join.

Проблема: O(n × m) сложность для JOIN без индексов

Реализуй Hash Join:
1. Detect equi-joins (JOIN ... ON a.id = b.id)
2. Build hash table из меньшей таблицы
3. Probe hash table для большей таблицы
4. Сложность: O(n + m) вместо O(n × m)

Добавь эвристику выбора алгоритма:
- Если размер таблиц < 1000 строк → Nested Loop (быстрее для малых данных)
- Если есть equi-join condition → Hash Join
- Если данные отсортированы по join key → Merge Join

Benchmark: JOIN двух таблиц по 10K строк каждая.
Цель: ускорение с 100 секунд до <10 секунд.
```

### Промпт 23: Оптимизация обновления индексов после DELETE
```
Проанализируй DeleteQuery.java (строки 95-112).

Проблема: После удаления строк индексы перестраиваются для ВСЕХ оставшихся строк.
Сложность: O(n × m × k)

Реализуй оптимизации:
1. Track offset shifts вместо полного перестроения
2. Используй logical delete (flag) вместо physical delete
3. Periodic vacuum для физического удаления помеченных строк

Добавь benchmark: удаление 1K строк из таблицы с 1M строк и 5 индексами.
Цель: ускорение с минут до секунд.
```

### Промпт 24: Оптимизация создания кластеризованного индекса
```
Проанализируй Table.createUniqueClusteredIndex() (строки 172-218).

Проблема: O(n × m) сложность при создании индекса на существующей таблице

Оптимизируй:
1. Bulk index build - строй индекс из отсортированных данных за O(n)
2. Отложи построение не-кластеризованных индексов на потом
3. Используй parallel sort для больших таблиц

Benchmark: создание индекса на таблице с 1M строк.
Цель: сокращение времени с часов до минут.
```

### Промпт 25: Пакетная вставка индексов при загрузке (readObject)
```
Проанализируй Table.readObject() (строки 297-322).

Проблема: При десериализации каждый insert в индекс имеет сложность O(log n), общая сложность O(n × m × log n)

Реализуй batch index build:
1. Собери все ключи в список
2. Отсортируй ключи
3. Построй сбалансированное B-дерево за один проход O(n)

Это даст ускорение с O(n log n) до O(n) для каждого индекса.
Benchmark: загрузка таблицы с 100K строк и 5 индексами.
```

### Промпт 26: Индексы для покрытия WHERE условий
```
Проанализируй SelectQuery.evaluateConditions() (строки 57-62, 223-229).

Проблема: Полный скан таблицы O(n × c) при отсутствии подходящего индекса

Реализуй:
1. Composite indexes для multi-column WHERE условий
2. Range scan через B-tree индексы (>, <, BETWEEN)
3. Index-only scan когда все нужные колонки в индексе

Добавь query planner который выбирает оптимальный индекс:
- Statistics по кардинальности колонок
- Cost-based optimization

Benchmark: WHERE с 3 условиями на таблице 1M строк.
Цель: ускорение с секунд до миллисекунд.
```

### Промпт 27: Оптимизация массового UPDATE
```
Проанализируй UpdateQuery.java (строки 27-32).

Проблема: O(n × c) для поиска + O(u × m) для обновления индексов

Реализуй:
1. Index-based update - найди строки через индекс вместо full scan
2. Batch index updates - обновляй индексы батчами
3. Deferred index maintenance - отложи обновление индексов

Добавь синтаксис:
UPDATE users SET status='ACTIVE' WHERE id BETWEEN 1 AND 10000
WITH (BATCH_SIZE=1000, DEFER_INDEX_UPDATE=true);

Benchmark: обновление 10K строк в таблице 1M строк.
```

### Промпт 28: Исправление потери indexDefinitions при сериализации
```
Проанализируй проблему #1 в problems.md (сериализация Table.java).

Проблема: Поле indexDefinitions может потерять данные при сериализации/десериализации

Исправь:
1. Явно сериализуй indexDefinitions в writeObject()
2. Корректно восстанавливай в readObject()
3. Добавь тест на serialize → deserialize → проверка целостности индексов
```

### Промпт 29: Сохранение состояния индексов в сериализованном виде
```
Вместо перестройки индексов при десериализации (проблема #2 в problems.md):

Реализуй сохранение состояния индексов:
1. Сериализуй внутреннюю структуру B-tree (узлы, ключи, указатели)
2. При десериализации восстанавливай структуру без перестройки
3. Для HashIndex сериализуй HashMap напрямую

Это сократит время загрузки с O(n × m × log n) до O(n).
Benchmark: загрузка таблицы 100K строк с 5 индексами.
Цель: сокращение с минут до секунд.
```

### Промпт 30: Замена сериализации на Copy-on-Write для транзакций
```
Проанализируй Transaction.cloneTable() (строки 76-93).

Проблема: Полная сериализация/десериализация для каждого снимка таблицы.
Сложность: O(n × m × log n) для каждого клонирования

Реализуй Copy-on-Write:
1. Shared data - неизмененные строки разделяются между snapshot'ами
2. Copy on modify - копируются только измененные строки
3. Reference counting для управления памятью

ИЛИ реализуй MVCC (Multi-Version Concurrency Control):
1. Каждая строка имеет версию (transaction ID)
2. Чтение видит snapshot на момент начала транзакции
3. Запись создает новую версию строки

Benchmark: начало 100 транзакций на базе с 10 таблицами по 10K строк.
Цель: ускорение с секунд до миллисекунд.
```

### Промпт 31: Исправление READ_UNCOMMITTED dirty reads
```
Проанализируй Database.getTableForQuery() (строки 176-195).

Проблема: READ_UNCOMMITTED позволяет читать грязные данные других транзакций

Исправь семантику уровней изоляции:
1. READ_UNCOMMITTED: используй snapshot на момент начала транзакции (никаких dirty reads)
2. READ_COMMITTED: читай только закоммиченные данные
3. REPEATABLE_READ: гарантируй повторную читаемость
4. SERIALIZABLE: полная сериализуемость

Добавь проверку конфликтов при коммите:
- Optimistic locking (version check)
- Pessimistic locking (row-level locks)

Напиши тесты на concurrency для каждого уровня изоляции.
```

### Промпт 32: Реализация Write-Ahead Log (WAL) для атомарности коммита
```
Проанализируй проблему #3 в problems.md (отсутствие атомарности при коммите).

Реализуй WAL:
1. Перед применением изменений запиши их в WAL
2. WAL имеет формат: [LSN][TransactionID][Operation][Data][Checksum]
3. При коммите: сначала flush WAL to disk, потом apply changes
4. При восстановлении: replay WAL с последнего checkpoint

Структура WAL файла:
[Header: magic_number, version, created_at]
[Record 1: LSN=1, TX=100, OP=INSERT, table=users, data=..., checksum=...]
[Commit: LSN=3, TX=100, status=COMMITTED, checksum=...]

Benchmark: краш сервера во время коммита → восстановление без потери данных.
```

### Промпт 33: Исправление утечки памяти в длинных транзакциях
```
Проанализируй проблему #4 в problems.md (утечка памяти при длинных транзакциях).

Проблема: originalTables и modifiedTables хранятся в памяти до завершения транзакции

Реализуй:
1. Lazy loading snapshot'ов - загружай с диска по требованию
2. Delta storage - храни только дельты изменений
3. Disk spillout - выгружай старые версии на диск при нехватке памяти
4. Transaction timeout - лимит на длительность транзакции
5. GC для завершенных транзакций - освобождай память сразу после commit/rollback

Добавь мониторинг:
- Memory usage per transaction
- Transaction duration tracking
- Alert при превышении лимитов
```

### Промпт 34: Исправление race condition в activeTransactions
```
Проанализируй проблему #5 в problems.md (race condition при доступе к activeTransactions).

Проблема: Итерация по ConcurrentHashMap.values() может привести к ConcurrentModificationException

Исправь:
1. Используй ReentrantReadWriteLock для критических секций
2. Гарантируй атомарность "добавить таблицу + уведомить транзакции"
3. ИЛИ используй StampedLock для оптимистичного чтения

Напиши stress test с 100 потоками создающими/удаляющими таблицы.
```

### Промпт 35: Реализация каскадных откатов транзакций
```
Проанализируй проблему #6 в problems.md (отсутствие обработки каскадных откатов).

Проблема: Если транзакция B прочитала данные из транзакции A, и A делает rollback, B продолжает работать с несуществующими данными

Реализуй:
1. Dependency graph - отслеживай зависимости между транзакциями
2. При rollback проверяй зависимые транзакции
3. Реализуй cascading rollback ИЛИ блокировку зависимых транзакций

Добавь тест:
BEGIN TRANSACTION A; UPDATE users SET x=10 WHERE id=1;
BEGIN TRANSACTION B; SELECT x FROM users WHERE id=1;
ROLLBACK A;
-- Транзакция B должна быть откачена ИЛИ заблокирована
```

### Промпт 36: Fine-grained versioning вместо копирования таблиц
```
Проанализируй проблему #7 в problems.md (неэффективная работа с большими таблицами).

Проблема: При изменении одной строки клонируется вся таблица

Реализуй row-level versioning:
1. Каждая строка имеет metadata: [version_id, created_by_tx, deleted_by_tx]
2. modifiedTables хранит только измененные строки (дельты)
3. originalTables хранит snapshot только для неизмененных строк

Это сократит память с O(n) до O(k) где k - количество измененных строк.
```

### Промпт 37: Реализация gap locking для предотвращения phantom reads
```
Проанализируй проблему #8 в problems.md (фантомное чтение не решено).

Проблема: Даже при SERIALIZABLE нет блокировки диапазонов

Реализуй:
1. Gap locking - блокировка диапазона ключей между существующими строками
2. Predicate locking - блокировка по условию (WHERE x > 10)
3. ИЛИ используй Serializable Snapshot Isolation (SSI) алгоритм

Напиши тесты на phantom read prevention.
```

### Промпт 38: Implicit transaction rollback при ошибке
```
Проанализируй проблему #9 в problems.md (implicit transaction не откатывается при ошибке).

Проблема: При exception в autoCommit режиме транзакция не откатывается явно

Исправь используя try-with-resources:
try (AutoRollbackTransaction tx = database.beginImplicitTransaction()) {
    parsedQuery.execute(table);
    tx.commit();
} catch (Exception e) {
    // Автоматический rollback в AutoRollbackTransaction.close()
    throw e;
}

Реализуй try-with-resources для автоматического управления транзакцией.
```

### Промпт 39: Замена ConcurrentHashMap на HashMap в индексах
```
Проанализируй проблему #4 в problems.md (сериализация ConcurrentHashMap в HashIndex).

Проблема: ConcurrentHashMap неэффективен для сериализации

Исправь:
1. Замени ConcurrentHashMap на обычный HashMap в HashIndex
2. Table сам управляет блокировками на уровне таблицы
3. Это упростит сериализацию и улучшит производительность

Benchmark: сериализация/десериализация индекса с 100K ключей.
```

### Промпт 40: Плоское представление B-tree для сериализации
```
Проанализируй проблему #7 в problems.md (сериализация внутренней структуры Node в B-tree).

Проблема: Сериализация древовидной структуры неэффективна

Реализуй flat representation:
1. Пронумеруй узлы в порядке обхода (level-order)
2. Сериализуй как массив: [node0, node1, node2, ...]
3. Каждый узел хранит индексы детей в массиве
4. При десериализации воссоздавай дерево из массива за O(n)

Benchmark: сериализация/десериализация B-tree с 1M ключей.
Цель: сокращение с минут до секунд.
```

---

## Раздел 3: Внедрение Варианта 2 (Parquet + Query Cache) - 40 промптов
*На основе документа Roadmap_Parquet_Stage1.md, Вариант 2*

### Промпт 41: Добавление Maven зависимостей для Parquet
```
Добавь в pom.xml зависимости Apache Parquet:

<dependencies>
    <!-- Parquet Core -->
    <dependency>
        <groupId>org.apache.parquet</groupId>
        <artifactId>parquet-common</artifactId>
        <version>1.13.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.parquet</groupId>
        <artifactId>parquet-column</artifactId>
        <version>1.13.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.parquet</groupId>
        <artifactId>parquet-hadoop</artifactId>
        <version>1.13.1</version>
    </dependency>
    
    <!-- Compression codecs -->
    <dependency>
        <groupId>org.xerial.snappy</groupId>
        <artifactId>snappy-java</artifactId>
        <version>1.1.10.4</version>
    </dependency>
    <dependency>
        <groupId>com.github.luben</groupId>
        <artifactId>zstd-jni</artifactId>
        <version>1.5.5-5</version>
    </dependency>
    
    <!-- JSON для метаданных -->
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>2.15.2</version>
    </dependency>
</dependencies>

Проверь что зависимости разрешаются: mvn dependency:tree
```

### Промпт 42: Создание класса ParquetTableStorage.java
```
Создай файл diesel/ParquetTableStorage.java с методами:
- save(Table table, Path parquetPath) - сохранение таблицы в Parquet
- load(Path parquetPath, Path metadataPath) - загрузка из Parquet
- convertToParquetSchema(TableSchema schema) - конвертация схемы
- saveMetadata(Table table, Path metadataPath) - сохранение метаданных JSON
- loadMetadata(Path metadataPath) - загрузка метаданных

Используй ZSTD compression, 128MB row groups, 8KB pages, dictionary encoding.
```

### Промпт 43: Реализация конвертации схемы Table → Parquet
```
Реализуй метод convertToParquetSchema() который маппит типы DieselDB на типы Parquet:
- INT/INTEGER → INT32
- BIGINT/LONG → INT64
- FLOAT/REAL → FLOAT
- DOUBLE → DOUBLE
- STRING/VARCHAR/TEXT → BINARY (UTF8)
- BOOLEAN → BOOLEAN
- DATE → INT32 (DATE)
- TIMESTAMP → INT96

Используй Types.primitive() builder pattern для создания MessageType.
```

### Промпт 44: Реализация сохранения метаданных в JSON
```
Реализуй saveMetadata() который сохраняет в JSON:
- tableName
- format: "PARQUET"
- schema (колонки с типами)
- indexes (имя, тип, колонки)
- sequences (имя, текущее значение)
- createdAt, lastModifiedAt timestamps

Используй Jackson ObjectMapper для сериализации.
```

### Промпт 45: Реализация загрузки метаданных из JSON
```
Реализуй loadMetadata() который читает JSON и создаёт TableMetadata объект.
Добавь version field для future compatibility.
Обработай missing fields gracefully (backward compatibility).
```

### Промпт 46: Модификация Table.java для выбора формата хранения
```
Измени Table.java:
1. Добавь enum StorageFormat { CSV, PARQUET }
2. Добавь поле storageFormat (default: PARQUET)
3. Модифицируй saveToFile() - пишет в Parquet если format=PARQUET
4. Модифицируй loadFromFile() - автодетект формата по наличию data.parquet
5. Добавь fallback на CSV если Parquet не найден
```

### Промпт 47: Создание утилиты миграции CSV → Parquet
```
Создай ParquetMigrationTool.java с методами:
- migrateAllTables() - миграция всех таблиц
- migrateTable(tableName) - миграция одной таблицы
- validateTable(original, migrated) - валидация после миграции

Features:
- Backup CSV файлов перед удалением
- Progress reporting
- Error handling с продолжением миграции остальных таблиц
- CLI интерфейс: java ParquetMigrationTool [tableName]
```

### Промпт 48: Benchmark Parquet vs CSV
```
Создай StorageBenchmark.java который сравнивает:
- Write performance: CSV vs Parquet
- Read performance: CSV vs Parquet
- File size: compression ratio
- Memory usage during read/write

Таблица: 100K строк с разными типами данных.
Выведи результаты в формате Markdown для docs/BENCHMARK_RESULTS.md
```

### Промпт 49: Создание QueryCache.java
```
Создай diesel/cache/QueryCache.java:
- put(normalizedSql, result) - кэширование результата
- get(normalizedSql) - возврат из кэша или null
- invalidate(tableName) - инвалидация по таблице
- cleanupExpired() - фоновая очистка по TTL

Features:
- LRU eviction при достижении maxSize
- TTL-based expiration (default 5 минут)
- Thread-safe (ConcurrentHashMap)
- Statistics: hitCount, missCount, hitRatio
```

### Промпт 50: Конфигурация QueryCache
```
Создай CacheConfig.java с параметрами:
- maxSize (default 1000 queries)
- ttlMillis (default 300000 = 5 min)
- enabled (default true)

Добавь загрузку/сохранение из config/cache.properties
Пример:
cache.maxSize=1000
cache.ttlMillis=300000
cache.enabled=true
```

### Промпт 51: Интеграция QueryCache в SelectQuery.java
```
Измени SelectQuery.execute():
1. Нормализация SQL (upper case, trim, sort conditions)
2. Проверка кэша перед выполнением
3. При cache hit - возврат результата (<1ms)
4. При cache miss - выполнение и сохранение в кэш
5. Не кэшировать non-deterministic запросы (NOW(), RANDOM())
```

### Промпт 52: Инвалидация кэша при INSERT
```
Измени InsertQuery.execute():
После успешного INSERT вызови SelectQuery.getQueryCache().invalidate(tableName)
Добавь debug логирование: "Cache invalidated for table: users"
```

### Промпт 53: Инвалидация кэша при UPDATE
```
Измени UpdateQuery.execute():
После успешного UPDATE вызови инвалидацию кэша.
Опционально: selective invalidation только если UPDATE затрагивает колонки используемые в закэшированных SELECT.
```

### Промпт 54: Инвалидация кэша при DELETE
```
Измени DeleteQuery.execute():
После успешного DELETE вызови инвалидацию кэша.
```

### Промпт 55: Инвалидация кэша при DDL операциях
```
Измени Database.java:
- CREATE TABLE → invalidate("*") (весь кэш)
- DROP TABLE → invalidate(tableName)
- ALTER TABLE → invalidate(tableName)
- CREATE INDEX → invalidate(tableName)
```

### Промпт 56: Мониторинг QueryCache
```
Добавь команду SQL: SHOW CACHE STATS
Возвращает:
- cache_size
- max_size
- utilization_percent
- hit_ratio
- average_speedup

Используй AtomicLong для hitCount/missCount counters.
```

### Промпт 57: Тестирование Parquet storage
```
Создай ParquetStorageTest.java:
- testSaveAndLoad() - roundtrip тест
- testCompressionRatio() - проверка 4x сжатия
- testSchemaPreservation() - типы данных сохраняются
- testIndexMetadata() - индексы восстанавливаются

Запуск: mvn test -Dtest=ParquetStorageTest
```

### Промпт 58: Тестирование QueryCache
```
Создай QueryCacheTest.java:
- testPutAndGet() - basic functionality
- testCacheMiss() - return null для отсутствующих
- testTtlExpiration() - entry expire после TTL
- testInvalidate() - очистка по таблице
- testMaxSizeEviction() - LRU eviction

Запуск: mvn test -Dtest=QueryCacheTest
```

### Промпт 59: Integration test Parquet + Cache
```
Создай ParquetCacheIntegrationTest.java:
1. CREATE TABLE с PARQUET storage
2. INSERT данные
3. Первый SELECT (cache miss) - замер времени
4. Второй SELECT (cache hit) - проверка 10x ускорения
5. INSERT → проверка инвалидации кэша
6. Третий SELECT → проверка новых данных
7. Restart DB → проверка persistence

Запуск: mvn test -Dtest=ParquetCacheIntegrationTest
```

### Промпт 60: Документация Parquet формата
```
Создай docs/PARQUET_FORMAT.md:
- Преимущества Parquet над CSV
- Структура файлов (data.parquet + metadata.json)
- Пример metadata.json
- Параметры сжатия (ZSTD, row group size)
- Миграция с CSV
- Benchmark результаты
- Совместимость со Spark/Hive/Presto
```

### Промпт 61: Конфигурация Parquet на уровне таблицы
```
Добавь поддержку WITH clause при CREATE TABLE:
CREATE TABLE users (...) WITH (
    storage_format='PARQUET',
    compression='ZSTD',
    row_group_size='128MB'
);

Измени CreateTableQuery.java для парсинга опций.
```

### Промпт 62: Lazy загрузка Parquet файлов
```
Реализуй LazyParquetTable которая:
- Не загружает все данные в память сразу
- Читает row groups on-demand
- Поддерживает seek к конкретным строкам
- Позволяет работать с таблицами > размера RAM
```

### Промпт 63: Predicate pushdown для Parquet
```
Реализуй predicate pushdown в SelectQuery:
1. Extract WHERE conditions
2. Convert to Parquet FilterPredicate
3. Create ParquetReader.withFilter(predicate)
4. Читать только matching row groups

Benchmark: WHERE id=100 на 1M строк → чтение 1 row group вместо всей таблицы.
```

### Промпт 64: Параллельное чтение Parquet
```
Реализуй ParallelParquetReader:
1. Распредели row groups across threads
2. Каждый thread читает свои groups
3. Собери результаты вместе

Benchmark: чтение 1GB файла с 4 threads → 3-4x ускорение.
```

### Промпт 65: Статистика использования кэша
```
Добавь детальную статистику в QueryCache:
- hitCount, missCount, hitRatio
- evictionCount, invalidationCount
- averageQueryTime (cached vs non-cached)
- averageSpeedup factor

Вывод в логи каждые 5 минут:
"Cache stats: hits=1234, misses=56, ratio=95.7%, speedup=42x"
```

### Промпт 66: Настройка Database.java для Parquet by default
```
Измени Database constructor:
- По умолчанию использовать StorageFormat.PARQUET
- Добавить конфигурационный файл db.properties
- Параметр: default.storage.format=PARQUET
```

### Промпт 67: Обработка ошибок при миграции
```
Добавь в ParquetMigrationTool:
- Rollback при ошибке миграции
- Логирование ошибок в migration.log
- Отчёт: successful/failed tables
- Recovery mode: продолжить с места падения
```

### Промпт 68: Поддержка partitioned tables в Parquet
```
Реализуй partitioning:
CREATE TABLE logs (...) PARTITION BY (year, month)
WITH (storage_format='PARQUET');

Структура:
logs/
  year=2024/
    month=01/data.parquet
    month=02/data.parquet
  year=2025/
    month=01/data.parquet
```

### Промпт 69: Оптимизация Dictionary encoding для строк
```
Включи dictionary encoding в ParquetWriter:
- Для STRING колонок автоматически
- Для low-cardinality колонок (status, gender, etc.)

Это даст дополнительное сжатие 2-3x для строковых данных.
```

### Промпт 70: Compression tuning (ZSTD levels)
```
Добавь настройку уровня сжатия ZSTD:
- Level 1: fastest, lower compression
- Level 3: balanced (default)
- Level 9: best compression, slower

Benchmark разные уровни для разных workload.
```

### Промпт 71: Row group size tuning
```
Добавь настройку row group size:
- Small (32MB): better for random access
- Medium (128MB): balanced (default)
- Large (512MB): better for sequential scan

Benchmark для разных patterns доступа.
```

### Промпт 72: Column statistics в Parquet metadata
```
Включи min/max statistics для каждой колонки:
- Используется для predicate pushdown
- Bloom filters для fast lookups

Это ускорит WHERE queries на 10-100x.
```

### Промпт 73: Bloom filters для Parquet
```
Добавь bloom filters для high-cardinality колонок:
CREATE INDEX idx_email ON users(email) 
WITH (bloom_filter=true);

Bloom filter хранится в Parquet metadata.
Ускоряет point lookups (WHERE email='...').
```

### Промпт 74: Query Cache warm-up strategy
```
Реализуй cache warm-up при старте DB:
- Load frequent queries from history
- Execute them to populate cache
- Особенно полезно после restart

Конфигурация:
cache.warmup.enabled=true
cache.warmup.queries=SELECT * FROM users WHERE id=?,...
```

### Промпт 75: Adaptive TTL для кэша
```
Реализуй adaptive TTL:
- Frequent queries → longer TTL
- Rare queries → shorter TTL
- Based on access pattern analysis

Это увеличит hit ratio на 10-20%.
```

### Промпт 76: Query normalization improvements
```
Улучши нормализацию SQL для лучшего cache hit:
- Remove extra whitespace
- Normalize keywords (UPPER)
- Sort AND/OR conditions (commutative)
- Normalize literals (1 vs 1.0)

Пример:
"SELECT * FROM users WHERE id=1 AND name='John'"
"SELECT * FROM users WHERE name='John' AND id=1"
→ одинаковый normalized key
```

### Промпт 77: Parameterized query caching
```
Поддержи parameterized queries в кэше:
- Ключ: normalized SQL с placeholders
- Значение: map of params → result

Пример:
SELECT * FROM users WHERE id=?
params={1: 100} → result1
params={1: 200} → result2

Это увеличит hit ratio для prepared statements.
```

### Промпт 78: Multi-level cache (L1/L2)
```
Реализуй two-level cache:
- L1: in-memory, small (100 entries), very fast
- L2: disk-backed, large (10000 entries), slower

Hot queries stay in L1, cold queries evicted to L2.
```

### Промпт 79: Cache persistence across restarts
```
Сохраняй кэш на диск при shutdown:
- Serialize cache entries to cache.dat
- Load on startup

Это избежит cache cold-start problem.
```

### Промпт 80: Final integration testing and documentation
```
Проведи comprehensive testing:
1. Unit tests для ParquetTableStorage
2. Unit tests для QueryCache
3. Integration tests (Parquet + Cache together)
4. Performance benchmarks (before/after)
5. Stress tests (concurrent queries)

Обнови документацию:
- README.md с новыми features
- MIGRATION_GUIDE.md для пользователей CSV
- PERFORMANCE_TUNING.md с best practices
```

---

## Итого: 80 промптов
- Раздел 1: 20 промптов (SonarQube fixes)
- Раздел 2: 20 промптов (Performance & Architecture fixes)
- Раздел 3: 40 промптов (Parquet + Query Cache implementation)

Ожидаемый эффект после выполнения всех промптов:
- Количество проблем SonarQube: 908 → <200 (-78%)
- Производительность чтения: +300-500%
- Производительность записи: +50-100% (для bulk operations)
- Размер файлов: -75% (сжатие Parquet)
- Cache hit ratio: 80-95% для read-heavy workload
- Общая оценка: 80% improvement при 20% effort (принцип Парето)
