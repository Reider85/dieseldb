# 100 Промптов для улучшения DieselDB

## Раздел 0: Приоритетные исправления по итогам ретроспективы (20 промптов)
*Принцип Парето: 20% усилий исправят 80% проблем. На основе retrospective.md*

### Промпт 1: Исправление JOIN с OR в условии (критично - OOM)
```
Проблема: Запросы JOIN ... ON ... OR ... создают декартово произведение (360000 строк) и вызывают OutOfMemoryError.

Задача:
1. Добавь детекцию OR в JOIN条件 и принудительно используй Hash Join вместо Nested Loop
2. Реализуй early termination при превышении лимита строк (например 100K)
3. Добавь предупреждение в лог: "WARNING: JOIN with OR condition may produce large result set"
4. Для тестов OrderByTest #27, #28 добавь проверку на отсутствие OOM

Файлы: diesel/SelectQuery.java, diesel/QueryParser.java
Тесты: OrderByTest, JoinTest
Приоритет: CRITICAL (падает production)
```

### Промпт 2: Оптимизация памяти для Cross Join (streaming)
```
Проблема: QuantitativeTest требует 4GB heap из-за хранения всех результатов в памяти.

Задача:
1. Реализуй streaming для SELECT результатов (Iterator<Row> вместо List<Row>)
2. Добавь external sort для ORDER BY когда результат > available memory
3. Используй File-based temporary storage для больших промежуточных результатов
4. Добавь конфиг: max.inmemory.rows = 10000 (превышение → spill to disk)

Файлы: diesel/SelectQuery.java, diesel/Table.java
Конфиг: diesel.properties
Приоритет: HIGH (масштабируемость)
```

### Промпт 3: Исправление GROUP BY с уникальными значениями
```
Проблема: При группировке по столбцу с уникальными значениями возвращается 1 строка вместо N групп.

Задача:
1. Проверь логику группировки в SelectQuery.execute() - секция GROUP BY
2. Убедись что каждая уникальная комбинация GROUP BY ключей создаёт новую группу
3. Добавь тест: GROUP BY по первичному ключу → должно вернуть N строк
4. Проверь работу агрегатных функций внутри каждой группы

Файлы: diesel/SelectQuery.java
Тесты: GroupByTest (добавить тест на unique column grouping)
Приоритет: HIGH (некорректные результаты)
```

### Промпт 4: Исправление IN со списком значений
```
Проблема: WHERE AGE IN (50, 51, 52) возвращает 2 строки вместо 21.

Задача:
1. Проверь парсинг списка значений в QueryParser.parseInList()
2. Убедись что все значения из списка корректно добавляются в Condition
3. Проверь фильтрацию: row.value IN (list) должно проверять все элементы
4. Добавь тесты: IN с 1, 3, 10, 100 значениями; IN с NULL в списке

Файлы: diesel/QueryParser.java, diesel/SelectQuery.java
Тесты: InTest (расширить покрытие)
Приоритет: HIGH (некорректная фильтрация)
```

### Промпт 5: Исправление IN с дополнительными условиями (AND/OR)
```
Проблема: WHERE NAME IN (...) AND BALANCE > 5000 игнорируется, возвращаются все 600 строк.

Задача:
1. Проверь построение AST для комбинированных условий (IN + AND + сравнение)
2. Убедись что все части WHERE условия выполняются (short-circuit evaluation)
3. Добавь логирование: "Evaluating WHERE: condition1 AND condition2"
4. Тесты: IN+AND, IN+OR, IN+AND+OR, NOT IN+AND

Файлы: diesel/QueryParser.java, diesel/SelectQuery.java
Тесты: InTest, AdvancedTest
Приоритет: CRITICAL (полностью игнорируется фильтрация)
```

### Промпт 6: Исправление LIMIT без OFFSET
```
Проблема: LIMIT 10 возвращает некорректное количество строк.

Задача:
1. Проверь применение limit в SelectQuery.execute() после всех операций
2. Убедись что limit применяется ПОСЛЕ сортировки (ORDER BY ... LIMIT)
3. Тесты: LIMIT 1, LIMIT 10, LIMIT 100, LIMIT больше чем всего строк
4. Проверь взаимодействие LIMIT с GROUP BY и агрегатами

Файлы: diesel/SelectQuery.java
Тесты: LimitOffsetTest (создать новый файл)
Приоритет: HIGH (некорректное ограничение результата)
```

### Промпт 7: Исправление OFFSET без LIMIT
```
Проблема: OFFSET 5 без LIMIT пропускает первые 5 строк но может вернуть 0.

Задача:
1. Проверь что offset применяется после сортировки
2. Если offset > total rows → верни пустой результат (а не ошибку)
3. Тесты: OFFSET 0, OFFSET 5, OFFSET больше чем всего строк
4. Добавь предупреждение: "OFFSET without LIMIT may be inefficient"

Файлы: diesel/SelectQuery.java
Тесты: LimitOffsetTest
Приоритет: MEDIUM
```

### Промпт 8: Исправление LIMIT + OFFSET вместе
```
Проблема: LIMIT 10 OFFSET 5 возвращает 0 строк вместо ожидаемых.

Задача:
1. Проверь порядок применения: сначала ORDER BY, потом OFFSET, потом LIMIT
2. Формула: result.slice(offset, offset + limit)
3. Тесты: LIMIT 10 OFFSET 5, LIMIT 1 OFFSET 99, LIMIT 100 OFFSET 0
4. Проверь edge cases: offset=0, limit=0, offset+limit > total

Файлы: diesel/SelectQuery.java
Тесты: LimitOffsetTest
Приоритет: HIGH
```

### Промпт 9: Исправление LIMIT в подзапросах
```
Проблема: В подзапросах LIMIT игнорируется (возвращает 600 строк вместо 10).

Задача:
1. Проверь выполнение подзапросов в SubqueryParser или SelectQuery
2. Убедись что LIMIT из подзапроса применяется к результату подзапроса
3. Тесты: SELECT * FROM (SELECT ... LIMIT 10) AS subq
4. Проверь вложенные подзапросы (2+ уровня)

Файлы: diesel/SubqueryParser.java, diesel/SelectQuery.java
Тесты: SubqueriesTest
Приоритет: HIGH
```

### Промпт 10: Оптимизация Hash Join для больших таблиц
```
Проблема: Hash Join создаёт хеш-таблицу в памяти которая может вызвать OOM.

Задача:
1. Добавь оценку размера хеш-таблицы до начала построения
2. Если estimated size > max.inmemory.rows → fallback на Block Nested Loop Join
3. Реализуй partitioned hash join для таблиц > memory (spill to disk)
4. Добавь метрики: hash table size, build time, probe time

Файлы: diesel/SelectQuery.java
Конфиг: max.hash.table.size.mb = 512
Приоритет: MEDIUM (профилактика OOM)
```

### Промпт 11: Добавление EXPLAIN для анализа плана выполнения
```
Проблема: Нет способа понять почему запрос медленный или потребляет много памяти.

Задача:
1. Реализуй команду EXPLAIN SELECT/INSERT/UPDATE/DELETE
2. Выводи: тип JOIN (Hash/Nested Loop), estimated rows, используемые индексы
3. Формат: текстовое дерево плана выполнения
4. EXPLAIN ANALYZE: выполни запрос и покажи фактические метрики

Файлы: diesel/ExplainQuery.java (новый), diesel/SelectQuery.java
Тесты: ExplainTest (новый)
Приоритет: MEDIUM (диагностика)
```

### Промпт 12: Лимит на максимальное количество строк в результате
```
Проблема: Нет защиты от accidental cross join который генерирует миллиарды строк.

Задача:
1. Добавь конфиг: max.result.rows = 1000000 (1 миллион)
2. Если результат превышает лимит → выброси exception с понятным сообщением
3. Добавь hint: /* MAX_ROWS=10000 */ для override на уровне запроса
4. Логгируй предупреждение при достижении 80% лимита

Файлы: diesel/SelectQuery.java, diesel/Database.java
Конфиг: max.result.rows
Приоритет: HIGH (защита от crash)
```

### Промпт 13: Улучшение ошибок OutOfMemoryError
```
Проблема: OOM падает без полезной информации о причине.

Задача:
1. Перехватывай OutOfMemoryError в DatabaseServer.ClientHandler
2. Логируй контекст: какой запрос, сколько строк, сколько памяти выделено
3. Отправляй клиенту: "Error: Query exceeded memory limit. Consider adding LIMIT or indexes."
4. Добавь метрику: peak.memory.usage.per.query

Файлы: diesel/DatabaseServer.java, diesel/SelectQuery.java
Приоритет: MEDIUM (debuggability)
```

### Промпт 14: Автоматическая статистика по таблицам
```
Проблема: Оптимизатор не знает размер таблиц для выбора плана выполнения.

Задача:
1. Храни в Table: rowCount, avgRowSize, lastAnalyzed timestamp
2. Обновляй статистику после INSERT/DELETE (асинхронно)
3. Добавь команду: ANALYZE TABLE name (принудительный пересчёт)
4. Используй статистику для выбора Hash Join vs Nested Loop

Файлы: diesel/Table.java, diesel/Database.java
Приоритет: MEDIUM (основа для оптимизатора)
```

### Промпт 15: Индексы для ускорения JOIN условий
```
Проблема: JOIN без индексов требует полного сканирования обеих таблиц.

Задача:
1. Авто-создавай индекс на колонках JOIN условия если его нет
2. Предупреждай: "Consider creating index on TABLE.COLUMN for faster JOIN"
3. Для FOREIGN KEY автоматически создавай индекс (как в PostgreSQL)
4. Добавь тест: JOIN с индексом vs без (benchmark)

Файлы: diesel/SelectQuery.java, diesel/CreateIndexQuery.java
Приоритет: MEDIUM (производительность)
```

### Промпт 16: Кэширование планов выполнения запросов
```
Проблема: Парсинг и планирование выполняется заново для каждого запроса.

Задача:
1. Кэшируй AST + план выполнения для параметризованных запросов
2. Ключ кэша: normalized SQL (без литералов, только структура)
3. Invalidация: при DDL или изменении статистики таблиц
4. Метрики: cache hit rate, average parse time saved

Файлы: diesel/QueryCache.java (новый), diesel/QueryParser.java
Приоритет: LOW (оптимизация)
```

### Промпт 17: Уменьшение heap requirement для тестов
```
Проблема: QuantitativeTest требует 4GB heap что медленно и дорого в CI.

Задача:
1. Раздели QuantitativeTest на маленькие тесты по 50MB каждый
2. Добавь @LargeTest аннотацию для тестов требующих >1GB (skip в CI по умолчанию)
3. Оптимизируй тестовые данные: меньше строк, более репрезентативные выборки
4. Цель: полный набор тестов запускается с -Xmx512m за <5 минут

Файлы: diesel/QuantitativeTest.java, pom.xml
Приоритет: HIGH (CI/CD эффективность)
```

### Промпт 18: Профилировщик производительности запросов
```
Проблема: Неясно какая часть запроса consumes больше всего времени.

Задача:
1. Добавь профилирование: parse time, plan time, execute time, sort time
2. Вывод в лог для медленных запросов (>1s): "Slow query breakdown: ..."
3. Метрики в JMX/SLF4J для мониторинга
4. Флаг: -Ddiesel.profile.slow.threshold.ms=1000

Файлы: diesel/SelectQuery.java, diesel/QueryParser.java
Приоритет: MEDIUM (диагностика)
```

### Промпт 19: Тесты на регрессию производительности
```
Проблема: Нет автоматической детекции деградации производительности.

Задача:
1. Сохраняй baseline timing для ключевых запросов (timing60.md)
2. В CI сравнивай текущее время с baseline (допустимо ±20%)
3. При деградации >20% → fail build с отчётом
4. Храни историю производительности в analytics/performance_history.csv

Файлы: .github/workflows/ci.yml (новый step), diesel/PerformanceRegressionTest.java
Приоритет: MEDIUM (quality gate)
```

### Промпт 20: Документация известных ограничений и workaround
```
Проблема: Пользователи не знают о ограничениях DieselDB.

Задача:
1. Создай KNOWN_LIMITATIONS.md в корне проекта
2. Опиши: макс. размер результата, ограничения JOIN с OR, требования к памяти
3. Для каждого ограничения предложи workaround (например "используй LIMIT")
4. Обнови README.md ссылкой на этот документ

Файлы: KNOWN_LIMITATIONS.md (новый), README.md
Приоритет: LOW (user experience)
```

## Раздел 1: Исправление ошибок SonarQube (20 промптов)
*На основе документа sonaranalitics.md - приоритет P0 и P1*

### Промпт 21: Исправление StackOverflow в регулярных выражениях (java:S5998)
```
Проанализируй файлы QueryParser.java, SubqueryParser.java и SqlLexer.java. Найди 57 регулярных выражений, которые могут вызвать StackOverflowError (правило java:S5998). Для каждого problematic regex:
1. Определи причину (чрезмерная вложенность, catastrophic backtracking)
2. Предложи упрощённую версию с possessive quantifiers (?>...)
3. Разбей сложные regex на несколько простых паттернов
4. Добавь тесты на SQL-запросы глубиной 100+ уровней вложенности

Приоритет: CRITICAL (BUG, может вызвать падение production)
Файлы: diesel/QueryParser.java, diesel/SubqueryParser.java, diesel/SqlLexer.java
```

### Промпт 22: Исправление Null Pointer Dereference (java:S2259)
```
Найди 13 мест в коде где возможен NullPointerException (правило java:S2259). Для каждого случая:
1. Добавь проверку null перед использованием объекта
2. Используйте Optional<T> где это уместно (Java 8+)
3. Добавь @NotNull/@Nullable аннотации для документирования
4. Напиши unit test для проверки edge cases

Примеры мест: Database.getTableForQuery(), Transaction.cloneTable(), SelectQuery.execute()
Приоритет: CRITICAL (BUG, вызывает падение сервера)
```

### Промпт 23: Удаление мёртвого кода (java:S2583, S108, S1144, S1068)
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

### Промпт 24: Исправление Double Brace Initialization (java:S3599)
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

### Промпт 25: Исправление игнорирования возвращаемых значений (java:S899)
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

### Промпт 26: Исправление проблем с regex grouping (java:S5850)
```
Найди 3 случая где alternations в regex должны быть сгруппированы:

БЫЛО: `SELECT|INSERT|UPDATE FROM table`
НЕПРАВИЛЬНО: матчит "SELECT" ИЛИ "INSERT FROM table" ИЛИ "UPDATE FROM table"

СТАЛО: `(?:SELECT|INSERT|UPDATE) FROM table`
ПРАВИЛЬНО: матчит любой keyword followed by "FROM table"

Добавь тесты на парсинг разных SQL statements.
Приоритет: BUG (некорректная работа парсера)
```

### Промпт 27: Исправление regex с repeated patterns (java:S5842)
```
Найди 4 regex с repeated patterns которые работают медленно:

Проблемные паттерны:
- `(a+)+` - exponential backtracking
- `(.*?)+` - nested quantifiers
- `[a-z]+[a-z]+` - redundant character classes

Оптимизируй с помощью possessive quantifiers и atomic groups.
Добавь бенчмарк на парсинг 10K SQL запросов.
Приоритет: PERFORMANCE
```

### Промпт 28: Оптимизация Cognitive Complexity в QueryParser.java (java:S3776)
```
QueryParser.java имеет cognitive complexity >200 (порог 15).

Задача:
1. Вынеси парсинг SELECT в отдельный метод parseSelect()
2. Вынеси парсинг JOIN в parseJoins()
3. Вынеси парсинг WHERE условий в parseConditions()
4. Используй Strategy pattern для разных типов запросов

Цель: снизить complexity каждого метода до <20.
Добавь test что рефакторинг не сломал парсинг.
Приоритет: MAINTAINABILITY
```

### Промпт 29: Рефакторинг SelectQuery.execute() (complexity=59)
```
SelectQuery.execute() имеет complexity 59 (порог 15).

Разбей на методы:
1. executeSelect() - основной flow
2. applyJoins() - обработка JOIN
3. applyWhereFilter() - фильтрация WHERE
4. applyGroupBy() - группировка
5. applyOrderBy() - сортировка
6. applyLimitOffset() - лимиты

Каждый метод <20 complexity, покрыт unit tests.
Приоритет: MAINTAINABILITY
```

### Промпт 30: Оптимизация регулярных выражений (java:S5869, S6353)
```
Найди regex которые можно заменить на строковые операции:

БЫЛО: `Pattern.matches("\\d+", str)`
СТАЛО: `str.chars().allMatch(Character::isDigit)`

БЫЛО: `Pattern.compile("[A-Za-z]+")`
СТАЛО: простой цикл или Character.isLetter()

Для простых паттернов строковые операции быстрее в 10 раз.
Добавь бенчмарк comparing old vs new.
Приоритет: PERFORMANCE
```

### Промпт 31: Вынос строковых литералов в константы (java:S1192)
```
Найди 133 строковых литерала которые повторяются 3+ раз:

Примеры:
- "SELECT", "INSERT", "UPDATE", "DELETE"
- "WHERE", "GROUP BY", "ORDER BY", "LIMIT"
- "INNER JOIN", "LEFT JOIN", "RIGHT JOIN"
- "NULL", "TRUE", "FALSE"

Вынеси в класс SqlKeywords:
```java
public final class SqlKeywords {
    public static final String SELECT = "SELECT";
    // ...
}
```

Приоритет: MAINTAINABILITY (быстро)
```

### Промпт 32: Уменьшение количества параметров методов (java:S107)
```
Найди 10 методов с >7 параметрами:

Рефакторинг варианты:
1. Объедини параметры в объект (Parameter Object pattern)
2. Используй Builder pattern для сложных объектов
3. Разбей метод на несколько с меньшим числом параметров

Пример:
```java
// БЫЛО
executeQuery(String sql, Connection conn, Transaction tx, 
             boolean autoCommit, int timeout, Logger log)

// СТАЛО
executeQuery(QueryContext context) // context содержит все параметры
```

Приоритет: MAINTAINABILITY
```

### Промпт 33: Исправление null из Boolean методов (java:S2447)
```
Найди методы возвращающие Boolean которые могут вернуть null:

БЫЛО:
```java
public Boolean evaluate(...) {
    if (condition) return true;
    // missing else → returns null
}
```

СТАЛО:
```java
public Boolean evaluate(...) {
    if (condition) return true;
    return false; // явное значение
}
```

ИЛИ используй three-valued logic enum: TRUE, FALSE, UNKNOWN
Приоритет: BUG (NullPointerException)
```

### Промпт 34: Исправление Serializable полей (java:S1948)
```
Найди 2 класса с не-serializable полями:

Проблема:
```java
class Transaction implements Serializable {
    transient Logger logger = LoggerFactory.getLogger(...); // OK
    Map<String, Object> cache = new HashMap<>(); // СЕРИАЛИЗУЕТСЯ
}
```

Решение:
1. Добавь transient для не-serializable полей
2. ИЛИ сделай поле static final
3. ИЛИ реализуй custom writeObject/readObject

Приоритет: BUG (SerializationException)
```

### Промпт 35: Замена System.out.println на Logger (java:S106)
```
Найди 8 случаев System.out.println и замени на SLF4J logger:

БЫЛО:
```java
System.out.println("Debug info: " + value);
```

СТАЛО:
```java
logger.debug("Debug info: {}", value);
```

Преимущества:
- Можно отключить в production
- Форматированный вывод
- Rolling files
- Разные уровни (DEBUG, INFO, WARN, ERROR)

Приоритет: MAINTAINABILITY
```

### Промпт 36: Использование специфичных исключений (java:S112)
```
Найди 10 мест где бросается generic Exception:

БЫЛО:
```java
throw new Exception("Table not found");
```

СТАЛО:
```java
throw new TableNotFoundException("Table " + tableName + " not found");
```

Создай hierarchy исключений:
- DieselException (base)
- TableNotFoundException
- ColumnNotFoundException
- SyntaxErrorException
- TransactionException

Приоритет: MAINTAINABILITY
```

### Промпт 37: Упрощение обработки исключений (java:S2139, S1141)
```
Найди 15 мест с избыточной обработкой исключений:

БЫЛО:
```java
try {
    method();
} catch (IOException e) {
    throw new RuntimeException(e);
}
```

СТАЛО:
```java
try {
    method();
} catch (IOException e) {
    throw new DieselIOException("Failed to ...", e);
}
```

ИЛИ используй try-with-resources для AutoCloseable.
Приоритет: MAINTAINABILITY
```

### Промпт 38: Удаление unused параметров, переменных, импортов
```
Найди и удали:
1. Unused parameters методов (12 мест)
2. Unused local variables (8 мест)
3. Unused imports (20+ файлов)

IntelliJ: Code → Cleanup → Optimize Imports + Remove unused
Проверь что тесты проходят после удаления.
Приоритет: LOW (быстро)
```

### Промпт 39: Упрощение условий и тернарных операторов
```
Найди 20 сложных условий которые можно упростить:

БЫЛО:
```java
if (flag == true) { ... }
if (x != null && x.equals("value")) { ... }
boolean result = condition ? true : false;
```

СТАЛО:
```java
if (flag) { ... }
if ("value".equals(x)) { ... } // null-safe
boolean result = condition;
```

Используй Objects.equals(), Objects.isNull().
Приоритет: MAINTAINABILITY
```

### Промпт 40: Финальная очистка (остальные CODE_SMELL)
```
Запусти SonarQube analysis и исправь оставшиеся CODE_SMELL:
- Дублированный код (DRY violation)
- Слишком длинные методы (>50 строк)
- Слишком большие классы (>500 строк)
- Missing default в switch
- Primitive obsession (замени на объекты)

Цель: снизить Technical Debt Ratio до <3%.
Приоритет: MAINTAINABILITY
```

## Раздел 2: Исправление топ-10 проблем SonarQube по принципу Парето (10 промптов)
*На основе документа sonaranalytics2.md - эти 10 правил устранят 652 проблемы из 813 (80.2%)*

### Промпт 41: Замена '[A-Za-z0-9_]' на '\w' в регулярных выражениях (java:S6353, 119 проблем)
```
Проблема: В коде используются избыточные character classes '[A-Za-z0-9_]' вместо краткой формы '\w'.

Задача:
1. Найди все regex паттерны с '[A-Za-z0-9_]' в QueryParser.java, SubqueryParser.java, SqlLexer.java
2. Замени '[A-Za-z0-9_]' на '\w' во всех регулярных выражениях
3. Проверь что замена не меняет семантику regex (класс \w включает [a-zA-Z0-9_])
4. Добавь тесты на парсинг идентификаторов с цифрами и подчёркиваниями

Пример замены:
БЫЛО: Pattern.compile("[A-Za-z0-9_]+")
СТАЛО: Pattern.compile("\\w+")

Файлы: diesel/QueryParser.java, diesel/SubqueryParser.java, diesel/SqlLexer.java
Приоритет: HIGH (119 проблем, самая частая проблема в проекте)
```

### Промпт 42: Рефакторинг методов с высокой Cognitive Complexity (java:S3776, 92 проблемы)
```
Проблема: Методы имеют Cognitive Complexity >15 (порог), некоторые >50.

Задача:
1. Найди методы с complexity >20 в QueryParser.java и SelectQuery.java
2. Вынеси крупные блоки кода в отдельные методы:
   - parseSelectStatement() - парсинг SELECT запросов
   - parseJoinClause() - обработка JOIN условий
   - parseWhereCondition() - парсинг WHERE условий
   - parseGroupByClause() - обработка GROUP BY
   - parseOrderByClause() - обработка ORDER BY
3. Используй early return для уменьшения вложенности
4. Примени Strategy pattern для разных типов запросов

Цель: каждый метод должен иметь complexity <20.
Файлы: diesel/QueryParser.java, diesel/SelectQuery.java, diesel/SubqueryParser.java
Приоритет: CRITICAL (92 проблемы, влияет на поддерживаемость)
```

### Промпт 43: Использование pattern matching для instanceof (java:S6201, 84 проблемы)
```
Проблема: Устаревший паттерн instanceof + cast вместо pattern matching (Java 16+).

Задача:
1. Найди все конструкции вида:
   if (obj instanceof Query) {
       Query q = (Query) obj;
       ...
   }
2. Замени на pattern matching syntax (Java 16+):
   if (obj instanceof Query q) {
       // q уже приведён к типу Query
       ...
   }
3. Проверь что версия Java в pom.xml установлена на 16 или выше
4. Добавь тесты что рефакторинг не изменил логику

Файлы: diesel/QueryParser.java, diesel/SelectQuery.java, diesel/SubqueryParser.java
Приоритет: MEDIUM (84 проблемы, улучшает читаемость кода)
```

### Промпт 44: Устранение рекурсивных паттернов в regex (java:S5998, 57 проблем)
```
Проблема: Регулярные выражения могут вызвать StackOverflowError при большой вложенности.

Задача:
1. Найди regex с вложенными квантификаторами: (.*?)+, ([a-z]+)+, (.*)* 
2. Перепиши с possessive квантификаторами: (?>...), .*+, .++
3. Разбей сложные regex на несколько простых паттернов
4. Добавь тесты на SQL с глубиной вложенности 100+ уровней

Пример:
БЫЛО: Pattern.compile("((SELECT|INSERT).*?)+")
СТАЛО: Pattern.compile("(?>SELECT|INSERT)(?:.*?(?>SELECT|INSERT))*")

Файлы: diesel/QueryParser.java, diesel/SubqueryParser.java, diesel/SqlLexer.java
Приоритет: CRITICAL (BUG - риск падения production при больших запросах)
```

### Промпт 45: Вынос строковых литералов в константы (java:S1192, 41 проблема)
```
Проблема: Строковые литералы дублируются в коде 3+ раз.

Задача:
1. Найди повторяющиеся литералы:
   - " does not exist", "already exists"
   - "SELECT", "INSERT", "UPDATE", "DELETE"
   - "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "OFFSET"
   - "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN"
   - "NULL", "TRUE", "FALSE", "AND", "OR", "NOT"
2. Создай класс SqlConstants или используй существующий SqlKeywords
3. Замени все литералы на константы

Пример:
БЫЛО: throw new Exception("Table " + name + " does not exist");
СТАЛО: throw new Exception("Table " + name + TABLE_DOES_NOT_EXIST);

Файлы: diesel/SqlKeywords.java (создать или обновить), diesel/*.java
Приоритет: HIGH (41 проблема, улучшает maintainability)
```

### Промпт 46: Удаление неиспользуемых импортов (java:S1128, 36 проблем)
```
Проблема: В файлах присутствуют неиспользуемые import statements.

Задача:
1. Найди все unused imports (IntelliJ: Code → Optimize Imports)
2. Особенно проверь импорты:
   - diesel.ThreeValuedLogic.TRUE/FALSE/UNKNOWN
   - java.util.* которые не используются
   - duplicate импорты одного класса
3. Удали неиспользуемые импорты
4. Настрой pre-commit hook для авто-очистки импортов

Файлы: Все .java файлы в diesel/
Приоритет: LOW но БЫСТРО (36 проблем, чистится за 10 минут)
```

### Промпт 47: Ограничение break/continue в циклах (java:S135, 30 проблем)
```
Проблема: В циклах используется более одного break/continue statement.

Задача:
1. Найди циклы с множественными break/continue
2. Рефакторинг варианты:
   - Вынеси цикл в отдельный метод с early return
   - Используй boolean flag вместо break
   - Примени Guard Clauses pattern
3. Цель: максимум один break/continue на цикл

Пример рефакторинга:
БЫЛО:
for (Row row : rows) {
    if (condition1) break;
    if (condition2) continue;
    if (condition3) break;
}

СТАЛО:
for (Row row : rows) {
    if (!condition1 && !condition3) {
        if (!condition2) {
            process(row);
        }
    } else {
        break; // или return из вынесенного метода
    }
}

Файлы: diesel/SelectQuery.java, diesel/QueryParser.java, diesel/Table.java
Приоритет: MEDIUM (30 проблем, улучшает читаемость)
```

### Промпт 48: Удаление неиспользуемых параметров методов (java:S1172, 28 проблем)
```
Проблема: Методы имеют параметры которые не используются в теле метода.

Задача:
1. Найди методы с unused parameters (SonarQube покажет locations)
2. Варианты решения:
   - Удалить параметр если он действительно не нужен
   - Использовать параметр если он должен использоваться (bug?)
   - Закомментировать имя параметра: methodName(Type unusedParam)
3. Проверь все вызовы метода перед удалением параметра
4. Обнови документацию (JavaDoc) если параметр удалён

Файлы: diesel/*.java (где SonarQube укажет проблемы)
Приоритет: MEDIUM (28 проблем, улучшает API clarity)
```

### Промпт 49: Заполнение или удаление пустых блоков кода (java:S108, 28 проблем)
```
Проблема: В коде присутствуют пустые блоки else, catch, или просто {}.

Задача:
1. Найди все пустые блоки кода
2. Для каждого случая:
   - Если блок должен быть пустым → добавь комментарий "// intentionally empty"
   - Если блок забыли заполнить → реализуй логику
   - Если блок не нужен → удали пустой блок и упрости условие
3. Особое внимание: пустые catch блоки (скрывают ошибки!)

Пример:
БЫЛО: } catch (Exception e) { }
СТАЛО: } catch (Exception e) { log.warn("Ignored exception", e); }

Файлы: diesel/*.java (где SonarQube укажет проблемы)
Приоритет: HIGH (28 проблем, некоторые могут скрывать баги)
```

### Промпт 50: Удаление использования deprecated setScale() (java:S1874, 28 проблем)
```
Проблема: Используется deprecated метод BigDecimal.setScale() без rounding mode.

Задача:
1. Найди все вызовы setScale() без второго параметра
2. Замени на setScale(scale, RoundingMode.HALF_UP) или другой подходящий режим
3. Варианты RoundingMode:
   - HALF_UP - округление до ближайшего (стандартное)
   - HALF_EVEN - banker's rounding (для финансов)
   - DOWN - усечение (отбрасывание дробной части)
   - UP - округление вверх

Пример:
БЫЛО: bigDecimal.setScale(2)
СТАЛО: bigDecimal.setScale(2, RoundingMode.HALF_UP)

Файлы: diesel/*.java (где используются BigDecimal операции)
Приоритет: MEDIUM (28 проблем, предотвращает UnexpectedResultException)
```

## Раздел 3: Оптимизация производительности (20 промптов)

### Промпт 51: Оптимизация updateIndicesAfterInsert (O(n × m × log n))
```
Проблема: После каждой вставки обновляются все индексы (O(m × log n)).

Решение:
1. Пакетное обновление индексов после bulk insert
2. Отложенное обновление (async queue)
3. Bulk load режим: disable indices → insert all → rebuild indices

Файлы: diesel/Table.java, diesel/BTreeIndex.java
Приоритет: HIGH
```

### Промпт 52: Замена Nested Loop Join на Hash Join
```
Проблема: Nested Loop Join имеет сложность O(n × m).

Реализуй Hash Join:
1. Построй хеш-таблицу по меньшей таблице (O(m))
2. Probe по большей таблице (O(n))
3. Итого: O(n + m) вместо O(n × m)

Добавь эвристику выбора: если smaller table < 1000 строк → Hash Join.
Файлы: diesel/SelectQuery.java
Приоритет: CRITICAL
```

### Промпт 53: Оптимизация обновления индексов после DELETE
```
Проблема: Удаление строки требует обновления всех индексов.

Решение:
1. Пакетное удаление из индексов
2. Bloom filter для быстрой проверки наличия в индексе
3. Lazy deletion: помечай строки как deleted, физическое удаление позже

Файлы: diesel/Table.java, diesel/Index.java
Приоритет: MEDIUM
```

### Промпт 54: Оптимизация создания кластеризованного индекса
```
Проблема: Создание кластеризованного индекса требует полной пересортировки таблицы.

Решение:
1. Incremental build: строй индекс порциями (не блокируя таблицу)
2. Parallel build: используй ForkJoinPool для параллельной сортировки
3. Online index creation: разрешай reads во время build

Файлы: diesel/BTreeClusteredIndex.java
Приоритет: MEDIUM
```

### Промпт 55: Пакетная вставка индексов при загрузке (readObject)
```
Проблема: При десериализации таблицы индексы строятся по одной записи.

Решение:
1. Прочитай все данные → отсортируй → построй индекс за один проход
2. Используй bulk load API индекса (addBatch())
3. Пропусти балансировку дерева до конца вставки

Файлы: diesel/TableStorage.java, diesel/BTreeIndex.java
Приоритет: MEDIUM
```

### Промпт 56: Индексы для покрытия WHERE условий
```
Проблема: WHERE условия сканируют всю таблицу.

Решение:
1. Авто-рекомендация индексов на основе частых WHERE условий
2. Composite индексы для multi-column WHERE
3. Covering index: включает все колонки из SELECT чтобы избежать lookup

Файлы: diesel/SelectQuery.java, diesel/CreateIndexQuery.java
Приоритет: HIGH
```

### Промпт 57: Оптимизация массового UPDATE
```
Проблема: UPDATE каждой строки отдельно неэффективен.

Решение:
1. Пакетный UPDATE: собери все изменения → примени одним проходом
2. Индекс-ассистированный UPDATE: найди строки через индекс → обнови
3. Bulk update режим: disable indices → update all → rebuild

Файлы: diesel/UpdateQuery.java, diesel/Table.java
Приоритет: HIGH
```

### Промпт 58: Исправление потери indexDefinitions при сериализации
```
Проблема: После сериализации/десериализации пропадают определения индексов.

Решение:
1. Добавь indexDefinitions в serializable состояние Table
2. Реализуй custom writeObject/readObject для сохранения индексов
3. Тест: serialize → deserialize → проверь что индексы работают

Файлы: diesel/Table.java, diesel/TableStorage.java
Приоритет: BUG
```

### Промпт 59: Сохранение состояния индексов в сериализованном виде
```
Проблема: Индексы не сохраняются между перезапусками.

Решение:
1. Сериализуй структуру индекса (BTree nodes)
2. При загрузке восстанови индексы из сериализованных данных
3. Валидация: checksum индекса после загрузки

Файлы: diesel/BTreeIndex.java, diesel/TableStorage.java
Приоритет: BUG
```

### Промпт 60: Замена сериализации на Copy-on-Write для транзакций
```
Проблема: Сериализация всей таблицы для транзакций медленная.

Решение:
1. Copy-on-Write: создавай snapshot только изменённых страниц
2. MVCC: храни версии строк вместо копирования таблиц
3. Delta storage: сохраняй только разницу (insert/delete log)

Файлы: diesel/Transaction.java, diesel/Table.java
Приоритет: HIGH
```

## Раздел 2.1: Исправление ошибок SonarQube по принципу Парето (15 промптов)
*На основе документа sonaranalytics4.md - топ-15 правил устранят 80% проблем*

### Промпт 61: Замена [A-Za-z0-9_] на \w в регулярных выражениях (java:S6353 - 119 проблем)
```
Проблема: В коде используются избыточные character классы [A-Za-z0-9_] вместо краткой формы \w.
Это нарушает правило java:S6353 и встречается в 119 местах.

Задача:
1. Найди все regex паттерны с [A-Za-z0-9_] в QueryParser.java, SubqueryParser.java, SqlLexer.java
2. Замени [A-Za-z0-9_] на \w во всех регулярных выражениях
3. Проверь что экранирование обратного слэша корректно: "\\w" в Java строках
4. Добавь тесты на парсинг идентификаторов с цифрами и подчёркиваниями

Пример:
БЫЛО: Pattern.compile("[A-Za-z0-9_]+")
СТАЛО: Pattern.compile("\\w+")

Файлы: diesel/QueryParser.java, diesel/SubqueryParser.java, diesel/SqlLexer.java
Тесты: ParserTest, SqlLexerTest
Приоритет: HIGH (#1 в Pareto - 119 проблем, 14.5% от всех)
```

### Промпт 62: Рефакторинг методов с высокой Cognitive Complexity (java:S3776 - 94 проблемы)
```
Проблема: Методы имеют Cognitive Complexity > 15 (порог), некоторые достигают 38.
Это нарушает правило java:S3776 и встречается в 94 местах.

Задача:
1. Найди методы с complexity > 15 через SonarQube или IntelliJ
2. Примени рефакторинг:
   - Вынеси nested if/else в отдельные методы
   - Используй guard clauses для ранних возвратов
   - Замени сложные условия на именованные boolean переменные
   - Применяй Strategy pattern для вариативной логики
3. Особое внимание: QueryParser.parseSelect(), SelectQuery.execute()

Цель: Каждый метод имеет complexity ≤ 15
Добавь test что рефакторинг не изменил поведение.

Файлы: diesel/QueryParser.java, diesel/SelectQuery.java, diesel/SubqueryParser.java
Тесты: ParserTest, SelectQueryTest
Приоритет: CRITICAL (#2 в Pareto - 94 проблемы, 11.5% от всех)
```

### Промпт 63: Использование instanceof pattern matching (Java 16+) (java:S6201 - 84 проблемы)
```
Проблема: Используется устаревший паттерн instanceof + cast вместо pattern matching.
Это нарушает правило java:S6201 и встречается в 84 местах.

Задача:
1. Найди все случаи instanceof с последующим cast
2. Замени на pattern matching (требуется Java 16+):

БЫЛО:
if (obj instanceof Query) {
    Query q = (Query) obj;
    q.execute();
}

СТАЛО:
if (obj instanceof Query q) {
    q.execute();
}

3. Обнови pom.xml: <maven.compiler.release>16</maven.compiler.release>
4. Проверь совместимость с целевой JVM

Файлы: diesel/*.java (по всему проекту)
Тесты: Все существующие тесты должны проходить
Приоритет: HIGH (#3 в Pareto - 84 проблемы, 10.2% от всех)
```

### Промпт 64: Устранение рекурсивных паттернов в regex (java:S5998 - 57 проблем)
```
Проблема: Регулярные выражения с чрезмерной вложенностью могут вызвать StackOverflowError.
Это нарушает правило java:S5998 (BUG) и встречается в 57 местах.

Задача:
1. Найди regex с вложенными квантификаторами: (a+)+, (.*?)+, (.*)* 
2. Замени на:
   - Possessive quantifiers: a++, .*+, .?+
   - Atomic groups: (?>...)
   - Явные лимиты повторений: {1,100} вместо +
3. Для парсинга вложенных структур используй ручной парсер вместо regex

Пример:
БЫЛО: Pattern.compile("(\\([^)]*\\))+")  // может вызвать SO
СТАЛО: Pattern.compile("(?>\\([^()]*\\))+")  // atomic group

Файлы: diesel/QueryParser.java, diesel/SubqueryParser.java
Тесты: ParserTest (добавь тесты с глубиной вложенности 100+)
Приоритет: CRITICAL (#4 в Pareto - BUG, риск падения production)
```

### Промпт 65: Вынос дублирующихся строковых литералов в константы (java:S1192 - 43 проблемы)
```
Проблема: Строковые литералы повторяются 3+ раз в коде.
Это нарушает правило java:S1192 и встречается в 43 местах.

Задача:
1. Найди повторяющиеся литералы через SonarQube inspection
2. Вынеси в класс SqlConstants:
   - SQL keywords: "SELECT", "INSERT", "WHERE", "JOIN"
   - Операторы: "=", "<>", "LIKE", "IN"
   - Типы данных: "INTEGER", "VARCHAR", "BOOLEAN"
   - Системные имена: "null", "true", "false"

Пример:
БЫЛО: if (token.equals("SELECT")) { ... }
СТАЛО: if (token.equals(SqlKeywords.SELECT)) { ... }

Файлы: diesel/SqlConstants.java (новый), diesel/QueryParser.java
Тесты: ParserTest
Приоритет: CRITICAL (#5 в Pareto - 43 проблемы, улучшает maintainability)
```

### Промпт 66: Удаление неиспользуемых импортов (java:S1128 - 36 проблем)
```
Проблема: В файлах присутствуют unused import statements.
Это нарушает правило java:S1128 и встречается в 36 местах.

Задача:
1. Запусти IntelliJ: Code → Optimize Imports (Ctrl+Alt+O)
2. Или mvn clean compile для детекции через Maven
3. Удали все неиспользуемые импорты
4. Особое внимание: diesel.ThreeValuedLogic.FALSE (упомянуто в отчете)

Автоматизация:
mvn org.apache.maven.plugins:maven-checkstyle-plugin:check -Dcheckstyle.config.location=google_checks.xml

Файлы: Все .java файлы проекта
Тесты: Не требуются (не меняет логику)
Приоритет: MEDIUM (#6 в Pareto - 36 проблем, быстрая победа)
```

### Промпт 67: Удаление неиспользуемых параметров методов (java:S1172 - 30 проблем)
```
Проблема: Методы имеют параметры которые не используются в теле метода.
Это нарушает правило java:S1172 и встречается в 30 местах.

Задача:
1. Найди методы с unused parameters через SonarQube
2. Если параметр действительно не нужен - удали его
3. Если параметр нужен для интерфейса/наследования - добавь @SuppressWarnings("unused")
4. Особое внимание: параметр "not" (упомянут в отчете)

Пример:
БЫЛО: public void process(String data, boolean debug) { ... } // debug не используется
СТАЛО: public void process(String data) { ... }

Файлы: diesel/*.java (по всему проекту)
Тесты: Все тесты должны проходить после удаления параметров
Приоритет: MAJOR (#7 в Pareto - 30 проблем)
```

### Промпт 68: Уменьшение break/continue в циклах (java:S135 - 30 проблем)
```
Проблема: Циклы содержат множественные break/continue statements.
Это нарушает правило java:S135 и встречается в 30 местах.

Задача:
1. Найди циклы с >1 break/continue
2. Рефакторинг:
   - Замени break на boolean flag с проверкой в условии цикла
   - Вынеси тело цикла в отдельный метод с ранним return
   - Используй Stream API где уместно

Пример:
БЫЛО:
for (Row row : rows) {
    if (!condition1) continue;
    if (!condition2) break;
    process(row);
}

СТАЛО:
for (Row row : rows) {
    if (condition1 && condition2) {
        process(row);
    }
}

Файлы: diesel/SelectQuery.java, diesel/QueryParser.java
Тесты: ParserTest, SelectQueryTest
Приоритет: MINOR (#8 в Pareto - 30 проблем, улучшает читаемость)
```

### Промпт 69: Заполнение или удаление пустых блоков кода (java:S108 - 28 проблем)
```
Проблема: В коде присутствуют пустые блоки else, catch, finally.
Это нарушает правило java:S108 и встречается в 28 местах.

Задача:
1. Найди пустые блоки через SonarQube inspection
2. Варианты решения:
   - Удали блок если он действительно не нужен
   - Добавь комментарий // intentionally empty
   - Выброси исключение: throw new UnsupportedOperationException("Not implemented")
   - Добавь логирование: logger.warn("Empty catch block for exception: {}", e)

Пример:
БЫЛО:
try {
    riskyOperation();
} catch (Exception e) {
}

СТАЛО:
try {
    riskyOperation();
} catch (Exception e) {
    logger.warn("Ignored exception during operation: {}", e.getMessage());
}

Файлы: diesel/*.java (по всему проекту)
Тесты: Проверить что обработка ошибок работает корректно
Приоритет: MAJOR (#9 в Pareto - 28 проблем, скрывает ошибки)
```

### Промпт 70: Удаление использования deprecated setScale() (java:S1874 - 28 проблем)
```
Проблема: Используется deprecated метод BigDecimal.setScale().
Это нарушает правило java:S1874 и встречается в 28 местах.

Задача:
1. Найди все вызовы setScale() без второго аргумента
2. Замени на setScale(int scale, RoundingMode mode):

БЫЛО: bigDecimal.setScale(2)
СТАЛО: bigDecimal.setScale(2, RoundingMode.HALF_UP)

3. Импортируй: import java.math.RoundingMode;
4. Выбери подходящий режим округления (обычно HALF_UP для финансов)

Файлы: diesel/*.java (поиск по setScale)
Тесты: ArithmeticTest, агрегатные функции с decimal
Приоритет: MINOR (#10 в Pareto - 28 проблем, future-proofing)
```

### Промпт 71: Удаление неиспользуемых локальных переменных (java:S1481 - 26 проблем)
```
Проблема: Объявлены локальные переменные которые не используются.
Это нарушает правило java:S1481 и встречается в 26 местах.

Задача:
1. Найди unused local variables через SonarQube
2. Удали объявления неиспользуемых переменных
3. Особое внимание: переменная "ck2" (упомянута в отчете)

Пример:
БЫЛО:
int count = 0;
String ck2 = checkKey();
return count;

СТАЛО:
int count = 0;
return count;

Файлы: diesel/*.java (по всему проекту)
Тесты: Не требуются (не меняет логику)
Приоритет: MINOR (#11 в Pareto - 26 проблем, чистота кода)
```

### Промпт 72: Удаление бесполезных присваиваний (java:S1854 - 23 проблемы)
```
Проблема: Переменным присваиваются значения которые никогда не читаются.
Это нарушает правило java:S1854 и встречается в 23 местах.

Задача:
1. Найди useless assignments через SonarQube
2. Удали лишние присваивания
3. Особое внимание: переменная "joins" (упомянута в отчете)

Пример:
БЫЛО:
List<String> joins = new ArrayList<>();
joins = getJoins(); // первое присваивание бесполезно

СТАЛО:
List<String> joins = getJoins();

Файлы: diesel/SelectQuery.java, diesel/QueryParser.java
Тесты: JoinTest, ParserTest
Приоритет: MAJOR (#12 в Pareto - 23 проблемы, wasted computation)
```

### Промпт 73: Исправление неиспользуемых первых аргументов методов (java:S3457 - 23 проблемы)
```
Проблема: Первый аргумент методов не используется в теле метода.
Это нарушает правило java:S3457 и встречается в 23 местах.

Задача:
1. Найди методы где первый параметр игнорируется
2. Варианты решения:
   - Удали параметр если он не нужен
   - Используй параметр в логике метода
   - Если это интерфейс - оставь с @SuppressWarnings

Пример:
БЫЛО: public Result execute(Connection conn, Query q) { return q.run(); }
СТАЛО: public Result execute(Query q) { return q.run(); }

Файлы: diesel/*.java (по всему проекту)
Тесты: Все тесты должны проходить
Приоритет: MAJOR (#13 в Pareto - 23 проблемы)
```

### Промпт 74: Уменьшение количества параметров конструктора (java:S107 - 22 проблемы)
```
Проблема: Конструкторы имеют > 7 параметров (порог), некоторые достигают 14.
Это нарушает правило java:S107 и встречается в 22 местах.

Задача:
1. Найди конструкторы с большим количеством параметров
2. Примени рефакторинг:
   - Builder pattern для сложных объектов
   - Parameter Object pattern (объедини связанные параметры в класс)
   - Методы setter вместо конструктора

Пример (Builder):
БЫЛО: new SelectQuery(table, columns, where, joins, groupBy, orderBy, limit, offset, ...)
СТАЛО:
new SelectQuery.Builder()
    .table(table)
    .columns(columns)
    .where(where)
    .limit(limit)
    .build();

Файлы: diesel/SelectQuery.java, diesel/QueryParser.java
Тесты: Все тесты должны проходить
Приоритет: MAJOR (#14 в Pareto - 22 проблемы, улучшает API)
```

### Промпт 75: Упрощение сложных регулярных выражений (java:S5843 - 21 проблема)
```
Проблема: Регулярные выражения имеют complexity > 20 (порог), некоторые достигают 23.
Это нарушает правило java:S5843 и встречается в 21 местах.

Задача:
1. Найди regex с complexity > 20 через SonarQube
2. Оптимизируй:
   - Упрости alternations: (a|b|c) → [abc] где возможно
   - Убери избыточные группы: (?:...) вместо (...)
   - Разбей сложный regex на несколько простых
   - Используй possessive quantifiers для предотвращения backtracking

Пример:
БЫЛО: Pattern.compile("(\\d+)|(\\w+)|([A-Z][a-z]+)")
СТАЛО: Pattern.compile("\\d+|\\w+|[A-Z][a-z]+")

Файлы: diesel/QueryParser.java, diesel/SqlLexer.java
Тесты: ParserTest (добавь тесты на сложные SQL паттерны)
Приоритет: MAJOR (#15 в Pareto - 21 проблема, performance + maintainability)
```

### Промпт 76: Параллельное выполнение независимых запросов
```
Проблема: Запросы выполняются последовательно даже если независимы.

Решение:
1. Детекция независимых запросов (разные таблицы)
2. Параллельное выполнение через ExecutorService
3. Merge результатов в конце

Файлы: diesel/DatabaseServer.java, diesel/QueryExecutor.java
Приоритет: MEDIUM
```

### Промпт 77: Асинхронный I/O для сетевых операций
```
Проблема: Синхронный I/O блокирует потоки на чтение/запись в сокет.

Решение:
1. Java NIO: Selector + SocketChannel для non-blocking I/O
2. Event loop architecture вместо thread-per-connection
3. Zero-copy transfer: FileChannel.transferTo()

Файлы: diesel/DatabaseServer.java
Приоритет: MEDIUM
```

### Промпт 78: Compression для сетевых ответов
```
Проблема: Большие результаты передаются без сжатия.

Решение:
1. GZIP compression для результатов >1KB
2. Negotiate compression level с клиентом
3. Метрики: compression ratio, CPU overhead

Файлы: diesel/DatabaseClient.java, diesel/DatabaseServer.java
Приоритет: LOW
```

### Промпт 79: Prepared Statements caching
```
Проблема: Каждый запрос парсится заново.

Решение:
1. Кэшируй parsed AST для prepared statements
2. Bind parameters at execution time
3. LRU cache с maxSize=1000

Файлы: diesel/PreparedStatement.java (новый), diesel/QueryParser.java
Приоритет: HIGH
```

### Промпт 80: Batch execution support
```
Проблема: Нет пакетного выполнения запросов.

Решение:
1. Добавь BEGIN BATCH ... END BATCH синтаксис
2. Выполняй запросы в батче одной транзакцией
3. Оптимизируй: отложенная запись индексов до конца батча

Файлы: diesel/BatchQuery.java (новый), diesel/Transaction.java
Приоритет: MEDIUM
```

### Промпт 81: Query result pagination
```
Проблема: Клиент получает весь результат сразу (OOM risk).

Решение:
1. Server-side cursors: клиент запрашивает по N строк за раз
2. Keyset pagination: WHERE id > last_seen_id LIMIT N
3. Stateless pagination: OFFSET/LIMIT с кэшированием

Файлы: diesel/Cursor.java (новый), diesel/SelectQuery.java
Приоритет: MEDIUM
```

### Промпт 82: Adaptive query execution
```
Проблема: План выполнения выбирается один раз и не меняется.

Решение:
1. Мониторь фактическое количество строк во время выполнения
2. Если estimate != actual → перепланируй остаток запроса
3. Learning optimizer: запоминай лучшие планы для похожих запросов

Файлы: diesel/SelectQuery.java, diesel/QueryOptimizer.java (новый)
Приоритет: LOW
```

### Промпт 83: Index-only scans
```
Проблема: Даже при наличии индекса читается вся таблица.

Решение:
1. Если индекс покрывает все колонки SELECT → не читай таблицу
2. Covering index detection в оптимизаторе
3. Метрики: index-only scan ratio

Файлы: diesel/SelectQuery.java, diesel/Index.java
Приоритет: MEDIUM
```

### Промпт 84: Parallel index scan
```
Проблема: Сканирование индекса однопоточное.

Решение:
1. Разбей индекс на ranges
2. Параллельное сканирование ranges через ForkJoinPool
3. Merge результатов (sorted merge для ordered index)

Файлы: diesel/BTreeIndex.java, diesel/SelectQuery.java
Приоритет: LOW
```

### Промпт 85: SIMD векторизация для агрегатов
```
Проблема: Агрегатные функции (SUM, AVG) обрабатывают строки по одной.

Решение:
1. Используй Vector API (Java 16+) для SIMD операций
2. Пакетная обработка: 8-16 значений за одну инструкцию CPU
3. Benchmark: сравнение scalar vs vectorized

Файлы: diesel/AggregateFunctions.java (новый)
Приоритет: LOW (experimental)
```

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
1. Конвертация row-based → columnar (async background job)
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

### Промпт 95: Cache invalidation策略
```
Реализуй инвалидацию кэша:
1. INSERT/UPDATE/DELETE → invalidate cache для этой таблицы
2. DDL (ALTER TABLE) → invalidate все запросы к таблице
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
3. При cache miss: выполни → сохрани в кэш → верни

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
1. Write table to Parquet → read back → compare data
2. Test projection pushdown (read 2 columns from 10)
3. Test predicate pushdown (WHERE age > 50)
4. Test partition pruning (skip irrelevant partitions)

Файлы: diesel/ParquetStorageTest.java (новый)
Приоритет: HIGH
```

### Промпт 103: Тестирование QueryCache
```
Напиши тесты для QueryCache:
1. Cache hit: одинаковый запрос → cache hit
2. Cache miss: разный запрос → cache miss
3. Invalidation: INSERT → cache invalidated
4. TTL expiry: wait 5 min → cache expired
5. LRU eviction: fill cache → oldest evicted

Файлы: diesel/QueryCacheTest.java (новый)
Приоритет: HIGH
```

### Промпт 104: Integration test Parquet + Cache
```
Комплексный тест:
1. Создай таблицу с 1M строк
2. Запиши в Parquet
3. Выполни SELECT → cache miss → read from Parquet
4. Повтори SELECT → cache hit → return from cache
5. Сделай INSERT → cache invalidated
6. Повтори SELECT → cache miss → read updated data

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
Читай多个 Parquet файлы параллельно:
1. Один файл → один поток (или один row group → один поток)
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
При конвертации table → Parquet:
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
1. Dictionary encoding: map strings → integers
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
1. High write frequency → shorter TTL
2. Read-only tables → longer TTL (or infinite)
3. Learn from access patterns (ML-based?)

Файлы: diesel/QueryCache.java
Приоритет: LOW
```

### Промпт 121: Query normalization improvements
```
Улучши нормализацию SQL для cache key:
1. Ignore whitespace differences
2. Normalize identifier case (uppercase)
3. Replace literals with placeholders: WHERE age = 25 → WHERE age = ?
4. Sort IN-list values: IN(3,1,2) → IN(1,2,3)

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
3. L1 miss → check L2 → execute query

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
```
