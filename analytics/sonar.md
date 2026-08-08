# Анализ проекта dieseldb (SonarQube)

- Инструмент: SonarQube 10.7.0 (Community Edition), SonarScanner через Maven
- Дата анализа: 2026-08-08
- Профиль: Sonar way (Java), версия Java: 17
- Объём кода (ncloc): 8309 строк в 34 файлах

## Итоговая сводка

| Показатель | Значение |
|---|---|
| Всего проблем | **908** |
| Ошибки (BUG) | 81 |
| Запахи кода (CODE_SMELL) | 827 |
| Уязвимости (VULNERABILITY) | 0 |
| CRITICAL | 116 |
| MAJOR | 486 |
| MINOR | 276 |
| INFO | 30 |

## Правила по убыванию количества проблем

| Кол-во | Правило | Название | Severity | Тип |
|---|---|---|---|---|
| 16 | java:S5786 | JUnit5 test classes and methods should have default package visibility | INFO | CODE_SMELL |
| 12 | java:S6541 | Methods should not perform too many tasks (aka Brain method) | INFO | CODE_SMELL |
| 2 | java:S6208 | Comma-separated labels should be used in Switch with colon case | INFO | CODE_SMELL |
| 119 | java:S6353 | Regular expression quantifiers and character classes should be used concisely | MINOR | CODE_SMELL |
| 49 | java:S6201 | Pattern Matching for "instanceof" operator should be used instead of simple "instanceof" + cast | MINOR | CODE_SMELL |
| 28 | java:S1874 | "@Deprecated" code should not be used | MINOR | CODE_SMELL |
| 24 | java:S135 | Loops should not contain more than a single "break" or "continue" statement | MINOR | CODE_SMELL |
| 24 | java:S1128 | Unnecessary imports should be removed | MINOR | CODE_SMELL |
| 13 | java:S1481 | Unused local variables should be removed | MINOR | CODE_SMELL |
| 5 | java:S5857 | Character classes should be preferred over reluctant quantifiers in regular expressions | MINOR | CODE_SMELL |
| 4 | java:S3626 | Jump statements should not be redundant | MINOR | CODE_SMELL |
| 2 | java:S3599 | Double Brace Initialization should not be used | MINOR | BUG |
| 2 | java:S2293 | The diamond operator ("<>") should be used | MINOR | CODE_SMELL |
| 2 | java:S899 | Return values should not be ignored when they contain the operation status code | MINOR | BUG |
| 1 | java:S1157 | Case insensitive string comparisons should be made without intermediate upper or lower casing | MINOR | CODE_SMELL |
| 1 | java:S1155 | "Collection.isEmpty()" should be used to test for emptiness | MINOR | CODE_SMELL |
| 1 | java:S1488 | Local variables should not be declared and then immediately returned or thrown | MINOR | CODE_SMELL |
| 1 | java:S5842 | Repeated patterns in regular expressions should not match the empty string | MINOR | BUG |
| 228 | java:S5869 | Character classes in regular expressions should not contain the same character twice | MAJOR | CODE_SMELL |
| 57 | java:S5998 | Regular expressions should not overflow the stack | MAJOR | BUG |
| 20 | java:S1172 | Unused method parameters should be removed | MAJOR | CODE_SMELL |
| 20 | java:S107 | Methods should not have too many parameters | MAJOR | CODE_SMELL |
| 16 | java:S3457 | Format strings should be used correctly | MAJOR | CODE_SMELL |
| 15 | java:S1854 | Unused assignments should be removed | MAJOR | CODE_SMELL |
| 13 | java:S2259 | Null pointers should not be dereferenced | MAJOR | BUG |
| 13 | java:S5843 | Regular expressions should not be too complicated | MAJOR | CODE_SMELL |
| 10 | java:S108 | Nested blocks of code should not be left empty | MAJOR | CODE_SMELL |
| 8 | java:S127 | "for" loop stop conditions should be invariant | MAJOR | CODE_SMELL |
| 8 | java:S1141 | Try-catch blocks should not be nested | MAJOR | CODE_SMELL |
| 8 | java:S2139 | Exceptions should be either logged or rethrown but not both | MAJOR | CODE_SMELL |
| 8 | java:S106 | Standard outputs should not be used directly to log anything | MAJOR | CODE_SMELL |
| 8 | java:S112 | Generic exceptions should never be thrown | MAJOR | CODE_SMELL |
| 7 | java:S3358 | Ternary operators should not be nested | MAJOR | CODE_SMELL |
| 7 | java:S2925 | "Thread.sleep" should not be used in tests | MAJOR | CODE_SMELL |
| 4 | java:S2589 | Boolean expressions should not be gratuitous | MAJOR | CODE_SMELL |
| 4 | java:S1068 | Unused "private" fields should be removed | MAJOR | CODE_SMELL |
| 4 | java:S6204 | "Stream.toList()" method should be used instead of "collectors" when unmodifiable list needed | MAJOR | CODE_SMELL |
| 3 | java:S5850 | Alternatives in regular expressions should be grouped when used with anchors | MAJOR | BUG |
| 3 | java:S1144 | Unused "private" methods should be removed | MAJOR | CODE_SMELL |
| 3 | java:S125 | Sections of code should not be commented out | MAJOR | CODE_SMELL |
| 3 | java:S1066 | Mergeable "if" statements should be combined | MAJOR | CODE_SMELL |
| 3 | java:S2583 | Conditionally executed code should be reachable | MAJOR | BUG |
| 2 | java:S2864 | "entrySet()" should be iterated when both the key and value are needed | MAJOR | CODE_SMELL |
| 2 | java:S1168 | Empty arrays and collections should be returned instead of null | MAJOR | CODE_SMELL |
| 2 | java:S4042 | "java.nio.Files#delete" should be preferred | MAJOR | CODE_SMELL |
| 2 | java:S1171 | Only static class initializers should be used | MAJOR | CODE_SMELL |
| 1 | java:S6397 | Character classes in regular expressions should not contain only one character | MAJOR | CODE_SMELL |
| 1 | java:S3824 | "Map.get" and value test should be replaced with single method call | MAJOR | CODE_SMELL |
| 1 | java:S5785 | JUnit assertTrue/assertFalse should be simplified to the corresponding dedicated assertion | MAJOR | CODE_SMELL |
| 1 | java:S2629 | "Preconditions" and logging arguments should not require evaluation | MAJOR | CODE_SMELL |
| 1 | java:S5961 | Test methods should not contain too many assertions | MAJOR | CODE_SMELL |
| 63 | java:S3776 | Cognitive Complexity of methods should not be too high | CRITICAL | CODE_SMELL |
| 34 | java:S1192 | String literals should not be duplicated | CRITICAL | CODE_SMELL |
| 8 | java:S1948 | Fields in a "Serializable" class should either be transient or serializable | CRITICAL | CODE_SMELL |
| 8 | java:S2447 | "null" should not be returned from a "Boolean" method | CRITICAL | CODE_SMELL |
| 2 | java:S1452 | Generic wildcard types should not be used in return types | CRITICAL | CODE_SMELL |
| 1 | java:S2093 | Try-with-resources should be used | CRITICAL | CODE_SMELL |

## Проблемы по файлам (топ-20)

| Файл | Проблем |
|---|---|
| diesel/QueryParser.java | 363 |
| diesel/SubqueryParser.java | 255 |
| diesel/SelectQuery.java | 51 |
| diesel/Database.java | 25 |
| diesel/DeleteQuery.java | 22 |
| diesel/UpdateQuery.java | 19 |
| diesel/Table.java | 16 |
| diesel/DatabaseClient.java | 16 |
| src/test/java/diesel/PerformanceTest.java | 14 |
| src/test/java/diesel/ServerConnectionLimitTest.java | 13 |
| diesel/SqlLexer.java | 10 |
| diesel/DatabaseServer.java | 9 |
| src/test/java/diesel/AliasesTest.java | 7 |
| src/test/java/diesel/AllTestsSampleTest.java | 7 |
| src/test/java/diesel/OrderByTest.java | 6 |
| src/test/java/diesel/QuantitativeTest.java | 6 |
| src/test/java/diesel/GroupByTest.java | 6 |
| src/test/java/diesel/AdvancedTest.java | 6 |
| src/test/java/diesel/JoinTest.java | 5 |
| src/test/java/diesel/SubqueriesTest.java | 5 |

## Все проблемы и рекомендации по исправлению

> Каждый блок — одно правило: что не так, как исправить и полный список мест (файл:строка — сообщение).

### java:S5786 — JUnit5 test classes and methods should have default package visibility

**Severity:** INFO | **Тип:** CODE_SMELL | **Найдено:** 16

Проблема: JUnit5 тест-классы и методы объявлены public. Рекомендация: в JUnit5 классы и методы могут быть package-private — уберите модификатор public.

Места:

- `src/test/java/diesel/AdvancedTest.java`:15 — Remove this 'public' modifier.
- `src/test/java/diesel/AliasesTest.java`:12 — Remove this 'public' modifier.
- `src/test/java/diesel/AllTestsSampleTest.java`:34 — Remove this 'public' modifier.
- `src/test/java/diesel/GracefulShutdownTest.java`:22 — Remove this 'public' modifier.
- `src/test/java/diesel/GroupByTest.java`:15 — Remove this 'public' modifier.
- `src/test/java/diesel/InTest.java`:13 — Remove this 'public' modifier.
- `src/test/java/diesel/JoinTest.java`:12 — Remove this 'public' modifier.
- `src/test/java/diesel/LikeTest.java`:13 — Remove this 'public' modifier.
- `src/test/java/diesel/OrderByTest.java`:16 — Remove this 'public' modifier.
- `src/test/java/diesel/PerformanceTest.java`:19 — Remove this 'public' modifier.
- `src/test/java/diesel/PerformanceTest.java`:52 — Remove this 'public' modifier.
- `src/test/java/diesel/PersistenceTest.java`:23 — Remove this 'public' modifier.
- `src/test/java/diesel/QuantitativeTest.java`:29 — Remove this 'public' modifier.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:24 — Remove this 'public' modifier.
- `src/test/java/diesel/SocketTimeoutTest.java`:21 — Remove this 'public' modifier.
- `src/test/java/diesel/SubqueriesTest.java`:12 — Remove this 'public' modifier.

### java:S6541 — Methods should not perform too many tasks (aka Brain method)

**Severity:** INFO | **Тип:** CODE_SMELL | **Найдено:** 12

Проблема: метод выполняет слишком много задач (brain method). Рекомендация: разделите на методы с одной ответственностью.

Места:

- `diesel/Database.java`:31 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 139 to 64, Complexity from 45 to 14, Nesting Level from 5 to 2, Number of Variables from 30 to 6.
- `diesel/DeleteQuery.java`:19 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 97 to 64, Complexity from 39 to 14, Nesting Level from 5 to 2, Number of Variables from 34 to 6.
- `diesel/InsertQuery.java`:22 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 107 to 64, Complexity from 32 to 14, Nesting Level from 4 to 2, Number of Variables from 17 to 6.
- `diesel/QueryParser.java`:974 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 134 to 64, Complexity from 37 to 14, Nesting Level from 3 to 2, Number of Variables from 53 to 6.
- `diesel/QueryParser.java`:1114 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 115 to 64, Complexity from 25 to 14, Nesting Level from 4 to 2, Number of Variables from 37 to 6.
- `diesel/QueryParser.java`:1971 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 127 to 64, Complexity from 19 to 14, Nesting Level from 5 to 2, Number of Variables from 23 to 6.
- `diesel/QueryParser.java`:2678 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 108 to 64, Complexity from 38 to 14, Nesting Level from 6 to 2, Number of Variables from 27 to 6.
- `diesel/QueryParser.java`:2793 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 95 to 64, Complexity from 29 to 14, Nesting Level from 5 to 2, Number of Variables from 38 to 6.
- `diesel/SelectQuery.java`:78 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 215 to 64, Complexity from 59 to 14, Nesting Level from 8 to 2, Number of Variables from 79 to 6.
- `diesel/SqlLexer.java`:79 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 116 to 64, Complexity from 34 to 14, Nesting Level from 5 to 2, Number of Variables from 18 to 6.
- `diesel/SubqueryParser.java`:1466 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 105 to 64, Complexity from 34 to 14, Nesting Level from 4 to 2, Number of Variables from 41 to 6.
- `diesel/Table.java`:327 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 125 to 64, Complexity from 46 to 14, Nesting Level from 4 to 2, Number of Variables from 21 to 6.

### java:S6208 — Comma-separated labels should be used in Switch with colon case

**Severity:** INFO | **Тип:** CODE_SMELL | **Найдено:** 2

Проблема: switch с двоеточием содержит только одну метку на case. Рекомендация: объедините метки через запятую: case A, B:.

Места:

- `diesel/QueryParser.java`:765 — Merge the previous cases into this one using comma-separated label.
- `diesel/QueryParser.java`:2879 — Merge the previous cases into this one using comma-separated label.

### java:S6353 — Regular expression quantifiers and character classes should be used concisely

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 119

Проблема: громоздкий character-класс вместо краткой формы. Рекомендация: используйте встроенные классы — [A-Za-z0-9_] -> \w, [0-9] -> \d, [ \\t\\n\\r\\f] -> \s (учтите, что \w в Java включает и символы Unicode, если нужно строго ASCII — проверьте требование).

Места:

- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:60 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:1997 — Use concise character class syntax '\\d' instead of '[0-9]'.
- `diesel/QueryParser.java`:1997 — Use concise character class syntax '\\d' instead of '[0-9]'.
- `diesel/SelectQuery.java`:846 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SelectQuery.java`:910 — Use concise character class syntax '\\w' instead of '[A-Za-z0-9_]'.
- `diesel/SelectQuery.java`:910 — Use concise character class syntax '\\w' instead of '[A-Za-z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:15 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/SubqueryParser.java`:841 — Use concise character class syntax '\\d' instead of '[0-9]'.
- `diesel/SubqueryParser.java`:841 — Use concise character class syntax '\\d' instead of '[0-9]'.

### java:S6201 — Pattern Matching for "instanceof" operator should be used instead of simple "instanceof" + cast

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 49

Проблема: паттерн 'instanceof' + отдельное приведение типа. Рекомендация: используйте pattern matching for instanceof (Java 16+): if (x instanceof Foo f) { ... f.method() ... } — это убирает отдельный cast и связанные с ним ошибки.

Места:

- `diesel/Database.java`:41 — Replace this instanceof check and cast with 'instanceof SetIsolationLevelQuery isolationQuery'
- `diesel/Database.java`:45 — Replace this instanceof check and cast with 'instanceof SetAutoCommitQuery autoCommitQuery'
- `diesel/Database.java`:49 — Replace this instanceof check and cast with 'instanceof BeginTransactionQuery begintransactionquery'
- `diesel/Database.java`:49 — Replace this instanceof check and cast with 'instanceof BeginTransactionQuery begintransactionquery'
- `diesel/Database.java`:90 — Replace this instanceof check and cast with 'instanceof CreateTableQuery createQuery'
- `diesel/Database.java`:98 — Replace this instanceof check and cast with 'instanceof CreateIndexQuery indexQuery'
- `diesel/Database.java`:106 — Replace this instanceof check and cast with 'instanceof CreateHashIndexQuery indexQuery'
- `diesel/Database.java`:114 — Replace this instanceof check and cast with 'instanceof CreateUniqueIndexQuery indexQuery'
- `diesel/Database.java`:122 — Replace this instanceof check and cast with 'instanceof CreateUniqueClusteredIndexQuery indexQuery'
- `diesel/DatabaseClient.java`:44 — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:44 — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:46 — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:46 — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:50 — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:50 — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DeleteQuery.java`:35 — Replace this instanceof check and cast with 'instanceof BTreeIndex btreeindex'
- `diesel/DeleteQuery.java`:181 — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/DeleteQuery.java`:183 — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/DeleteQuery.java`:185 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/DeleteQuery.java`:224 — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/DeleteQuery.java`:226 — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/DeleteQuery.java`:228 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/DeleteQuery.java`:306 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/DeleteQuery.java`:307 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/DeleteQuery.java`:310 — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/DeleteQuery.java`:313 — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/SelectQuery.java`:589 — Replace this instanceof check and cast with 'instanceof BTreeIndex bTreeIndex'
- `diesel/SelectQuery.java`:617 — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/SelectQuery.java`:619 — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/SelectQuery.java`:621 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/SelectQuery.java`:797 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/SelectQuery.java`:803 — Replace this instanceof check and cast with 'instanceof LocalDate localdate'
- `diesel/SelectQuery.java`:805 — Replace this instanceof check and cast with 'instanceof LocalDateTime localdatetime'
- `diesel/SelectQuery.java`:807 — Replace this instanceof check and cast with 'instanceof Boolean boolean'
- `diesel/SelectQuery.java`:809 — Replace this instanceof check and cast with 'instanceof UUID uuid'
- `diesel/SelectQuery.java`:811 — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/SelectQuery.java`:813 — Replace this instanceof check and cast with 'instanceof Character character'
- `diesel/SelectQuery.java`:941 — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/Table.java`:558 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/UpdateQuery.java`:131 — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/UpdateQuery.java`:133 — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/UpdateQuery.java`:135 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/UpdateQuery.java`:174 — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/UpdateQuery.java`:176 — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/UpdateQuery.java`:178 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/UpdateQuery.java`:256 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/UpdateQuery.java`:257 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/UpdateQuery.java`:260 — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/UpdateQuery.java`:263 — Replace this instanceof check and cast with 'instanceof Double double'

### java:S1874 — "@Deprecated" code should not be used

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 28

Проблема: используется устаревший (@Deprecated) API. Рекомендация: перейдите на новую версию API; например BigDecimal.setScale(int, int)/ROUND_HALF_UP -> setScale(int, RoundingMode.HALF_UP).

Места:

- `diesel/SelectQuery.java`:428 — Remove this use of "divide"; it is deprecated.
- `diesel/SelectQuery.java`:428 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AdvancedTest.java`:38 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/AdvancedTest.java`:38 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AliasesTest.java`:74 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AliasesTest.java`:74 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/AliasesTest.java`:87 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/AliasesTest.java`:87 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AllTestsSampleTest.java`:173 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AllTestsSampleTest.java`:173 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/AllTestsSampleTest.java`:187 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AllTestsSampleTest.java`:187 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/GroupByTest.java`:87 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/GroupByTest.java`:87 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/InTest.java`:35 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/InTest.java`:35 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/JoinTest.java`:68 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/JoinTest.java`:68 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/LikeTest.java`:35 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/LikeTest.java`:35 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/OrderByTest.java`:90 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/OrderByTest.java`:90 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/QuantitativeTest.java`:119 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/QuantitativeTest.java`:119 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/QuantitativeTest.java`:133 — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/QuantitativeTest.java`:133 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/SubqueriesTest.java`:66 — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/SubqueriesTest.java`:66 — Remove this use of "setScale"; it is deprecated.

### java:S135 — Loops should not contain more than a single "break" or "continue" statement

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 24

Проблема: в цикле больше одного break/continue — запутанное управление потоком. Рекомендация: оставьте максимум один jump на цикл: вынесите тело в метод с ранним return, используйте флаги или условия.

Места:

- `diesel/DatabaseServer.java`:178 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/DeleteQuery.java`:137 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:582 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:1601 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:2017 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:2151 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:2415 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:2584 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:2691 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:3049 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SelectQuery.java`:188 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SelectQuery.java`:655 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SqlLexer.java`:87 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SqlLexer.java`:100 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:152 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:414 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:738 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:914 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1046 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1100 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1270 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1401 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1476 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/UpdateQuery.java`:87 — Reduce the total number of break and continue statements in this loop to use at most one.

### java:S1128 — Unnecessary imports should be removed

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 24

Проблема: неиспользуемые import-операторы. Рекомендация: удалите их (в IDE: Optimize Imports / Ctrl+Alt+O).

Места:

- `diesel/BeginTransactionQuery.java`:2 — Remove this unnecessary import: same package classes are always implicitly imported.
- `diesel/BeginTransactionQuery.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `diesel/BeginTransactionQuery.java`:4 — Remove this unnecessary import: same package classes are always implicitly imported.
- `diesel/Query.java`:2 — Remove this unused import 'java.util'.
- `src/test/java/diesel/AdvancedTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/AliasesTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/AllTestsSampleTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/DatabaseSmokeTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/DatabaseSmokeTest.java`:12 — Remove this unused import 'org.junit.jupiter.api.Assertions.assertTrue'.
- `src/test/java/diesel/GroupByTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/InTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/InTest.java`:11 — Remove this unused import 'org.junit.jupiter.api.Assertions.assertThrows'.
- `src/test/java/diesel/JoinTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/LikeTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/LikeTest.java`:11 — Remove this unused import 'org.junit.jupiter.api.Assertions.assertThrows'.
- `src/test/java/diesel/OrderByTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/PerformanceTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/PerformanceTest.java`:14 — Remove this unused import 'java.util.stream.Collectors'.
- `src/test/java/diesel/PerformanceTest.java`:15 — Remove this unused import 'java.util.stream.IntStream'.
- `src/test/java/diesel/PersistenceTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/QuantitativeTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:15 — Remove this unused import 'java.util.concurrent.CountDownLatch'.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:16 — Remove this unused import 'java.util.concurrent.TimeUnit'.
- `src/test/java/diesel/SubqueriesTest.java`:3 — Remove this unnecessary import: same package classes are always implicitly imported.

### java:S1481 — Unused local variables should be removed

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 13

Проблема: локальная переменная не используется. Рекомендация: удалите её.

Места:

- `diesel/QueryParser.java`:634 — Remove this unused "indexPart" local variable.
- `diesel/QueryParser.java`:646 — Remove this unused "indexPart" local variable.
- `diesel/QueryParser.java`:658 — Remove this unused "indexPart" local variable.
- `diesel/QueryParser.java`:670 — Remove this unused "indexPart" local variable.
- `diesel/QueryParser.java`:1954 — Remove this unused "conditions" local variable.
- `diesel/QueryParser.java`:2014 — Remove this unused "inQuotes" local variable.
- `diesel/QueryParser.java`:2015 — Remove this unused "currentToken" local variable.
- `diesel/QueryParser.java`:2090 — Remove this unused "column" local variable.
- `diesel/SubqueryParser.java`:495 — Remove this unused "startPos" local variable.
- `src/test/java/diesel/AdvancedTest.java`:34 — Remove this unused "random" local variable.
- `src/test/java/diesel/PerformanceTest.java`:140 — Remove this unused "columns" local variable.
- `src/test/java/diesel/PerformanceTest.java`:141 — Remove this unused "random" local variable.
- `src/test/java/diesel/PerformanceTest.java`:285 — Remove this unused "random" local variable.

### java:S5857 — Character classes should be preferred over reluctant quantifiers in regular expressions

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 5

Проблема: нежадный квантификатор можно заменить character-классом. Рекомендация: перепишите паттерн, например используйте класс вместо reluctant-квантификатора где возможно.

Места:

- `diesel/QueryParser.java`:1560 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/SubqueryParser.java`:233 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/SubqueryParser.java`:667 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/SubqueryParser.java`:702 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/SubqueryParser.java`:1532 — Replace this use of a reluctant quantifier with "[^\\)]*+".

### java:S3626 — Jump statements should not be redundant

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 4

Проблема: лишний jump-оператор (break/continue/return) в конце ветки — недостижим или избыточен. Рекомендация: удалите его.

Места:

- `diesel/QueryParser.java`:2597 — Remove this redundant jump.
- `diesel/QueryParser.java`:2603 — Remove this redundant jump.
- `diesel/SubqueryParser.java`:1279 — Remove this redundant jump.
- `diesel/SubqueryParser.java`:1282 — Remove this redundant jump.

### java:S3599 — Double Brace Initialization should not be used

**Severity:** MINOR | **Тип:** BUG | **Найдено:** 2

Проблема: Double Brace Initialization ({{ }}) — неэффективно и утекает ссылку на this. Рекомендация: используйте фабричные методы (List.of(), Arrays.asList(), Stream), или обычную инициализацию.

Места:

- `diesel/SelectQuery.java`:108 — Use another way to initialize this instance.
- `diesel/Table.java`:248 — Use another way to initialize this instance.

### java:S2293 — The diamond operator ("<>") should be used

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 2

Проблема: явно указанные generic-типы при создании объекта. Рекомендация: используйте diamond-оператор: new HashMap<>().

Места:

- `diesel/Database.java`:18 — Replace the type specification in this constructor call with the diamond operator ("<>").
- `diesel/Table.java`:584 — Replace the type specification in this constructor call with the diamond operator ("<>").

### java:S899 — Return values should not be ignored when they contain the operation status code

**Severity:** MINOR | **Тип:** BUG | **Найдено:** 2

Проблема: возвращаемое значение (содержащее код операции/ошибки) игнорируется. Рекомендация: обработайте результат и проверьте код ошибки.

Места:

- `diesel/Database.java`:338 — Do something with the "boolean" value returned by "delete".
- `diesel/Database.java`:339 — Do something with the "boolean" value returned by "delete".

### java:S1157 — Case insensitive string comparisons should be made without intermediate upper or lower casing

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 1

Проблема: сравнение без учёта регистра через toLowerCase()/toUpperCase(). Рекомендация: используйте equalsIgnoreCase().

Места:

- `diesel/SubqueryParser.java`:395 — Replace these toUpperCase()/toLowerCase() and equals() calls with a single equalsIgnoreCase() call.

### java:S1155 — "Collection.isEmpty()" should be used to test for emptiness

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 1

Проблема: проверка collection.size() == 0. Рекомендация: используйте collection.isEmpty().

Места:

- `diesel/Table.java`:349 — Use isEmpty() to check whether the collection is empty or not.

### java:S1488 — Local variables should not be declared and then immediately returned or thrown

**Severity:** MINOR | **Тип:** CODE_SMELL | **Найдено:** 1

Проблема: локальная переменная объявлена и сразу возвращается. Рекомендация: верните выражение напрямую: return <expr>;.

Места:

- `diesel/Database.java`:168 — Immediately return this expression instead of assigning it to the temporary variable "result".

### java:S5842 — Repeated patterns in regular expressions should not match the empty string

**Severity:** MINOR | **Тип:** BUG | **Найдено:** 1

Проблема: повторяющийся элемент регэкспа может совпасть с пустой строкой. Рекомендация: упростите паттерн, чтобы избежать неоднозначности.

Места:

- `diesel/SubqueryParser.java`:1182 — Rework this part of the regex to not match the empty string.

### java:S5869 — Character classes in regular expressions should not contain the same character twice

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 228

Проблема: символ повторяется внутри character-класса регэкспа (например [0-90-9]) — лишние ветки, регулярка работает медленнее и сбивает с толку. Рекомендация: убрать дубликаты символов в классе; для проверки достаточно каждого символа один раз.

Места:

- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/QueryParser.java`:60 — Remove duplicates in this character class.
- `diesel/SelectQuery.java`:910 — Remove duplicates in this character class.
- `diesel/SelectQuery.java`:910 — Remove duplicates in this character class.
- `diesel/SelectQuery.java`:910 — Remove duplicates in this character class.
- `diesel/SelectQuery.java`:910 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:15 — Remove duplicates in this character class.

### java:S5998 — Regular expressions should not overflow the stack

**Severity:** MAJOR | **Тип:** BUG | **Найдено:** 57

Проблема: квантификатор повторения в регэкспе ((...)+, (...)*, (...){n}) без защиты может переполнить стек для больших входов (ReDoS / stack overflow). Рекомендация: замените на атомарные группы (?>...) или possessive-квантификаторы (*+, ++), либо перепишите участок парсинга без регулярного выражения.

Места:

- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:62 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:793 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:820 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:1358 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:1465 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:1735 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:1798 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:1975 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:2269 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:17 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:65 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:144 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:573 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:828 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:829 — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:1584 — Refactor this repetition that can lead to a stack overflow for large inputs.

### java:S1172 — Unused method parameters should be removed

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 20

Проблема: параметр метода не используется. Рекомендация: удалите параметр и обновите вызовы; если метод реализует интерфейс — это может быть нарушением контракта, в таком случае оставьте с Javadoc-пояснением или пометкой, но лучше согласовать сигнатуру.

Места:

- `diesel/DeleteQuery.java`:254 — Remove this unused method parameter "columnTypes".
- `diesel/QueryParser.java`:677 — Remove this unused method parameter "normalized".
- `diesel/QueryParser.java`:925 — Remove this unused method parameter "normalized".
- `diesel/QueryParser.java`:2362 — Remove these unused method parameters "originalQuery", "not".
- `diesel/QueryParser.java`:2469 — Remove these unused method parameters "database", "originalQuery", "columnAliases".
- `diesel/QueryParser.java`:2506 — Remove this unused method parameter "conditionStr".
- `diesel/QueryParser.java`:2634 — Remove this unused method parameter "tableAliases".
- `diesel/SelectQuery.java`:50 — Remove this unused method parameter "columnTypes".
- `diesel/SelectQuery.java`:530 — Remove this unused method parameter "combinedColumnTypes".
- `diesel/SelectQuery.java`:560 — Remove this unused method parameter "combinedColumnTypes".
- `diesel/SubqueryParser.java`:104 — Remove this unused method parameter "normalizedQuery".
- `diesel/SubqueryParser.java`:662 — Remove this unused method parameter "columnAliases".
- `diesel/SubqueryParser.java`:699 — Remove this unused method parameter "columnAliases".
- `diesel/SubqueryParser.java`:976 — Remove these unused method parameters "database", "originalQuery", "columnAliases", "not".
- `diesel/SubqueryParser.java`:1081 — Remove these unused method parameters "database", "originalQuery", "columnAliases".
- `diesel/SubqueryParser.java`:1173 — Remove these unused method parameters "database", "originalQuery", "columnAliases".
- `diesel/SubqueryParser.java`:1300 — Remove this unused method parameter "tableAliases".
- `diesel/SubqueryParser.java`:1467 — Remove these unused method parameters "originalQuery", "columnAliases".
- `diesel/UpdateQuery.java`:204 — Remove this unused method parameter "columnTypes".
- `src/test/java/diesel/PerformanceTest.java`:97 — Remove this unused method parameter "random".

### java:S107 — Methods should not have too many parameters

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 20

Проблема: у метода слишком много параметров (>7) — тяжело вызывать и читать. Рекомендация: сгруппируйте связанные параметры в record/класс-параметров или используйте Builder.

Места:

- `diesel/QueryParser.java`:1263 — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:1938 — Method has 8 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:2144 — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:2288 — Method has 11 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:2341 — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:2362 — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:2469 — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:2504 — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:2678 — Method has 8 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:2793 — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/SelectQuery.java`:33 — Constructor has 15 parameters, which is greater than 7 authorized.
- `diesel/SelectQuery.java`:44 — Constructor has 16 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:466 — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:778 — Method has 8 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:905 — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:976 — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:1081 — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:1173 — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:1390 — Method has 8 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:1466 — Method has 10 parameters, which is greater than 7 authorized.

### java:S3457 — Format strings should be used correctly

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 16

Проблема: в логировании используется конкатенация строк — значение вычисляется даже когда уровень логирования выключен. Рекомендация: используйте форматные спецификаторы или Supplier/лямбду, чтобы строка собиралась только при активном уровне.

Места:

- `diesel/DatabaseClient.java`:51 — first argument is not used.
- `diesel/QueryParser.java`:2068 — first argument is not used.
- `diesel/QueryParser.java`:2958 — String contains no format specifiers.
- `diesel/QueryParser.java`:2994 — 5th argument is not used.
- `diesel/QueryParser.java`:2994 — 6th argument is not used.
- `diesel/SubqueryParser.java`:873 — 4th argument is not used.
- `diesel/SubqueryParser.java`:873 — 2nd argument is not used.
- `diesel/SubqueryParser.java`:878 — 3rd argument is not used.
- `diesel/Table.java`:350 — first argument is not used.
- `diesel/Table.java`:412 — first argument is not used.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:76 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:87 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:104 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:107 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:115 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/SocketTimeoutTest.java`:81 — Format specifiers or lambda should be used instead of string concatenation.

### java:S1854 — Unused assignments should be removed

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 15

Проблема: переменной присваивается значение, которое больше не используется (dead store). Рекомендация: удалите присваивание или переработайте логику так, чтобы значение использовалось.

Места:

- `diesel/QueryParser.java`:634 — Remove this useless assignment to local variable "indexPart".
- `diesel/QueryParser.java`:646 — Remove this useless assignment to local variable "indexPart".
- `diesel/QueryParser.java`:658 — Remove this useless assignment to local variable "indexPart".
- `diesel/QueryParser.java`:670 — Remove this useless assignment to local variable "indexPart".
- `diesel/QueryParser.java`:1168 — Remove this useless assignment to local variable "onClausePart".
- `diesel/QueryParser.java`:1954 — Remove this useless assignment to local variable "conditions".
- `diesel/QueryParser.java`:2015 — Remove this useless assignment to local variable "currentToken".
- `diesel/QueryParser.java`:2040 — Remove this useless assignment to local variable "matchedPatternName".
- `diesel/QueryParser.java`:2042 — Remove this useless assignment to local variable "matched".
- `diesel/QueryParser.java`:2090 — Remove this useless assignment to local variable "column".
- `diesel/SubqueryParser.java`:495 — Remove this useless assignment to local variable "startPos".
- `src/test/java/diesel/AdvancedTest.java`:34 — Remove this useless assignment to local variable "random".
- `src/test/java/diesel/PerformanceTest.java`:140 — Remove this useless assignment to local variable "columns".
- `src/test/java/diesel/PerformanceTest.java`:141 — Remove this useless assignment to local variable "random".
- `src/test/java/diesel/PerformanceTest.java`:285 — Remove this useless assignment to local variable "random".

### java:S2259 — Null pointers should not be dereferenced

**Severity:** MAJOR | **Тип:** BUG | **Найдено:** 13

Проблема: возможно обращение к null (NullPointerException). Рекомендация: проверяйте значение на null перед разыменованием или используйте Optional/защитные проверки.

Места:

- `diesel/QueryParser.java`:503 — A "NullPointerException" could be thrown; "normalized" is nullable here.
- `diesel/QueryParser.java`:937 — A "NullPointerException" could be thrown; "original" is nullable here.
- `diesel/QueryParser.java`:1308 — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- `diesel/QueryParser.java`:1317 — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- `diesel/QueryParser.java`:1343 — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- `diesel/QueryParser.java`:2322 — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- `diesel/QueryParser.java`:2374 — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- `diesel/QueryParser.java`:2455 — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- `diesel/QueryParser.java`:2520 — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- `diesel/QueryParser.java`:2668 — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- `diesel/QueryParser.java`:3099 — A "NullPointerException" could be thrown; "normalized" is nullable here.
- `diesel/SubqueryParser.java`:1380 — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- `diesel/Table.java`:66 — A "NullPointerException" could be thrown; "sequences" is nullable here.

### java:S5843 — Regular expressions should not be too complicated

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 13

Проблема: регулярное выражение слишком сложное — трудно понять и поддерживать. Рекомендация: разбейте на несколько простых регэкспов/шагов, используйте именованные группы, либо перепишите парсинг на ручной разбор.

Места:

- `diesel/QueryParser.java`:2009 — Simplify this regular expression to reduce its complexity from 35 to the 20 allowed.
- `diesel/QueryParser.java`:2270 — Simplify this regular expression to reduce its complexity from 22 to the 20 allowed.
- `diesel/QueryParser.java`:2967 — Simplify this regular expression to reduce its complexity from 24 to the 20 allowed.
- `diesel/SubqueryParser.java`:65 — Simplify this regular expression to reduce its complexity from 46 to the 20 allowed.
- `diesel/SubqueryParser.java`:97 — Simplify this regular expression to reduce its complexity from 26 to the 20 allowed.
- `diesel/SubqueryParser.java`:233 — Simplify this regular expression to reduce its complexity from 24 to the 20 allowed.
- `diesel/SubqueryParser.java`:667 — Simplify this regular expression to reduce its complexity from 29 to the 20 allowed.
- `diesel/SubqueryParser.java`:831 — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- `diesel/SubqueryParser.java`:833 — Simplify this regular expression to reduce its complexity from 34 to the 20 allowed.
- `diesel/SubqueryParser.java`:835 — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- `diesel/SubqueryParser.java`:841 — Simplify this regular expression to reduce its complexity from 26 to the 20 allowed.
- `diesel/SubqueryParser.java`:980 — Simplify this regular expression to reduce its complexity from 48 to the 20 allowed.
- `diesel/SubqueryParser.java`:1532 — Simplify this regular expression to reduce its complexity from 21 to the 20 allowed.

### java:S108 — Nested blocks of code should not be left empty

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 10

Проблема: пустой вложенный блок кода. Рекомендация: реализуйте его или удалите; если пустота осознанная — добавьте комментарий с объяснением.

Места:

- `src/test/java/diesel/AliasesTest.java`:45 — Either remove or fill this block of code.
- `src/test/java/diesel/GracefulShutdownTest.java`:47 — Either remove or fill this block of code.
- `src/test/java/diesel/GroupByTest.java`:49 — Either remove or fill this block of code.
- `src/test/java/diesel/GroupByTest.java`:56 — Either remove or fill this block of code.
- `src/test/java/diesel/JoinTest.java`:40 — Either remove or fill this block of code.
- `src/test/java/diesel/OrderByTest.java`:50 — Either remove or fill this block of code.
- `src/test/java/diesel/OrderByTest.java`:57 — Either remove or fill this block of code.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:45 — Either remove or fill this block of code.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:110 — Either remove or fill this block of code.
- `src/test/java/diesel/SubqueriesTest.java`:37 — Either remove or fill this block of code.

### java:S127 — "for" loop stop conditions should be invariant

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 8

Проблема: условие продолжения цикла for использует переменную, которая меняется в теле цикла, — поведение неочевидно. Рекомендация: вычислите границу до цикла в локальную переменную или перепишите на while с явным условием.

Места:

- `diesel/QueryParser.java`:2752 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:2757 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:2763 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SqlLexer.java`:68 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SqlLexer.java`:71 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:397 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:1442 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:1448 — Refactor the code in order to not assign to this loop counter from within the loop body.

### java:S1141 — Try-catch blocks should not be nested

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 8

Проблема: try-catch вложены друг в друга. Рекомендация: вынесите внутренний блок в отдельный метод с собственным try-catch, чтобы уменьшить вложенность.

Места:

- `diesel/DatabaseClient.java`:82 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:104 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:107 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:112 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:180 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:201 — Extract this nested try block into a separate method.
- `diesel/QueryParser.java`:39 — Extract this nested try block into a separate method.
- `diesel/QueryParser.java`:1889 — Extract this nested try block into a separate method.

### java:S2139 — Exceptions should be either logged or rethrown but not both

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 8

Проблема: в одном catch исключение и логируется, и пробрасывается дальше. Рекомендация: оставьте что-то одно — либо логируйте, либо пробрасывайте; не делайте оба действия, чтобы не дублировать информацию и не терять контекст.

Места:

- `diesel/Database.java`:170 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/DatabaseClient.java`:32 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/DatabaseClient.java`:56 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/InsertQuery.java`:123 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/QueryParser.java`:564 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/QueryParser.java`:1931 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/SubqueryParser.java`:77 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/SubqueryParser.java`:801 — Either log this exception and handle it, or rethrow it with some contextual information.

### java:S106 — Standard outputs should not be used directly to log anything

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 8

Проблема: вывод в System.out/System.err вместо логгера. Рекомендация: используйте логгер (SLF4J) для диагностики; для CLI-вывода централизуйте вывод в одном месте.

Места:

- `diesel/DatabaseClient.java`:114 — Replace this use of System.out by a logger.
- `diesel/DatabaseClient.java`:116 — Replace this use of System.out by a logger.
- `diesel/DieselDatabase.java`:78 — Replace this use of System.out by a logger.
- `diesel/DieselDatabase.java`:80 — Replace this use of System.out by a logger.
- `diesel/SqlLexer.java`:217 — Replace this use of System.out by a logger.
- `diesel/SqlLexer.java`:220 — Replace this use of System.out by a logger.
- `diesel/SqlLexer.java`:223 — Replace this use of System.out by a logger.
- `diesel/SqlLexer.java`:225 — Replace this use of System.out by a logger.

### java:S112 — Generic exceptions should never be thrown

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 8

Проблема: бросается обобщённое исключение (Exception, Throwable, Error, RuntimeException). Рекомендация: выбрасывайте конкретные типы исключений (IllegalArgumentException, IllegalStateException, собственные).

Места:

- `diesel/Database.java`:172 — Define and throw a dedicated exception instead of using a generic one.
- `diesel/DatabaseClient.java`:34 — Define and throw a dedicated exception instead of using a generic one.
- `diesel/DatabaseClient.java`:52 — Define and throw a dedicated exception instead of using a generic one.
- `diesel/DatabaseClient.java`:59 — Define and throw a dedicated exception instead of using a generic one.
- `diesel/Table.java`:543 — Define and throw a dedicated exception instead of using a generic one.
- `diesel/Table.java`:574 — Define and throw a dedicated exception instead of using a generic one.
- `diesel/Transaction.java`:50 — Define and throw a dedicated exception instead of using a generic one.
- `diesel/Transaction.java`:60 — Define and throw a dedicated exception instead of using a generic one.

### java:S3358 — Ternary operators should not be nested

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 7

Проблема: тернарные операторы вложены. Рекомендация: замените на if/else или промежуточные переменные — читаемость важнее краткости.

Места:

- `diesel/DeleteQuery.java`:51 — Extract this nested ternary operation into an independent statement.
- `diesel/DeleteQuery.java`:302 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:327 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:2870 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:793 — Extract this nested ternary operation into an independent statement.
- `diesel/SubqueryParser.java`:1555 — Extract this nested ternary operation into an independent statement.
- `diesel/UpdateQuery.java`:252 — Extract this nested ternary operation into an independent statement.

### java:S2925 — "Thread.sleep" should not be used in tests

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 7

Проблема: Thread.sleep() в тестах — причина нестабильности (flaky). Рекомендация: ожидайте реальное событие (CountDownLatch, CompletableFuture, awaitility, опрос с таймаутом).

Места:

- `src/test/java/diesel/GracefulShutdownTest.java`:98 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:308 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:347 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:63 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:82 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:86 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/SocketTimeoutTest.java`:57 — Remove this use of "Thread.sleep()".

### java:S2589 — Boolean expressions should not be gratuitous

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 4

Проблема: логическое выражение всегда true или всегда false. Рекомендация: упростите условие — лишние части можно убрать.

Места:

- `diesel/DatabaseClient.java`:68 — Remove this expression which always evaluates to "true"
- `diesel/QueryParser.java`:1384 — Remove this expression which always evaluates to "true"
- `diesel/QueryParser.java`:1388 — Remove this expression which always evaluates to "true"
- `diesel/QueryParser.java`:1397 — Remove this expression which always evaluates to "true"

### java:S1068 — Unused "private" fields should be removed

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 4

Проблема: неиспользуемое private-поле. Рекомендация: удалите его (проверив, что оно не используется через рефлексию/сериализацию).

Места:

- `diesel/DatabaseServer.java`:155 — Remove this unused "socketTimeout" private field.
- `diesel/QueryParser.java`:63 — Remove this unused "originalQuery" private field.
- `diesel/QueryParser.java`:64 — Remove this unused "OPERATORS" private field.
- `diesel/SelectQuery.java`:27 — Remove this unused "subQueries" private field.

### java:S6204 — "Stream.toList()" method should be used instead of "collectors" when unmodifiable list needed

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 4

Проблема: stream().collect(Collectors.toList()). Рекомендация: используйте stream().toList() (Java 16+), если нужен неизменяемый список.

Места:

- `diesel/QueryParser.java`:1716 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure that the list is unmodified.
- `diesel/SelectQuery.java`:239 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure that the list is unmodified.
- `diesel/SelectQuery.java`:413 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure that the list is unmodified.
- `diesel/SelectQuery.java`:450 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure that the list is unmodified.

### java:S5850 — Alternatives in regular expressions should be grouped when used with anchors

**Severity:** MAJOR | **Тип:** BUG | **Найдено:** 3

Проблема: альтернативы в регэкспе с якорями могут дать неоднозначное поведение (например ^a|b$). Рекомендация: сгруппируйте альтернативы: ^(?:a|b)$.

Места:

- `diesel/QueryParser.java`:1319 — Group parts of the regex together to make the intended operator precedence explicit.
- `diesel/SubqueryParser.java`:556 — Group parts of the regex together to make the intended operator precedence explicit.
- `diesel/SubqueryParser.java`:561 — Group parts of the regex together to make the intended operator precedence explicit.

### java:S1144 — Unused "private" methods should be removed

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 3

Проблема: неиспользуемый private-метод. Рекомендация: удалите его (проверив рефлексию/тесты).

Места:

- `diesel/QueryParser.java`:1595 — Remove this unused private "splitOrderByClause" method.
- `diesel/QueryParser.java`:1637 — Remove this unused private "parseLimitClause" method.
- `diesel/QueryParser.java`:2951 — Remove this unused private "areSubQueriesEquivalent" method.

### java:S125 — Sections of code should not be commented out

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 3

Проблема: закомментированный код. Рекомендация: удалите его — исходники комментариями не хранят; при необходимости заведите задачу/документацию.

Места:

- `diesel/QueryParser.java`:886 — This block of commented-out lines of code should be removed.
- `diesel/QueryParser.java`:1405 — This block of commented-out lines of code should be removed.
- `src/test/java/diesel/AllTestsSampleTest.java`:281 — This block of commented-out lines of code should be removed.

### java:S1066 — Mergeable "if" statements should be combined

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 3

Проблема: вложенные if можно объединить. Рекомендация: объедините условия через &&, если ветки совпадают.

Места:

- `diesel/BTreeClusteredIndex.java`:294 — Merge this if statement with the enclosing one.
- `diesel/SelectQuery.java`:271 — Merge this if statement with the enclosing one.
- `diesel/Table.java`:349 — Merge this if statement with the enclosing one.

### java:S2583 — Conditionally executed code should be reachable

**Severity:** MAJOR | **Тип:** BUG | **Найдено:** 3

Проблема: код недостижим или условие всегда истинно из-за предыдущих проверок. Рекомендация: упростите логику, уберите мёртвые ветки.

Места:

- `diesel/SubqueryParser.java`:874 — Change this condition so that it does not always evaluate to "true"
- `diesel/SubqueryParser.java`:879 — Change this condition so that it does not always evaluate to "true"
- `diesel/SubqueryParser.java`:893 — Change this condition so that it does not always evaluate to "true"

### java:S2864 — "entrySet()" should be iterated when both the key and value are needed

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 2

Проблема: цикл по keySet() с обращением к get() по ключу. Рекомендация: итерируйте entrySet(), чтобы получать и ключ, и значение за один проход.

Места:

- `diesel/SelectQuery.java`:242 — Iterate over the "entrySet" instead of the "keySet".
- `diesel/SelectQuery.java`:496 — Iterate over the "entrySet" instead of the "keySet".

### java:S1168 — Empty arrays and collections should be returned instead of null

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 2

Проблема: метод возвращает null вместо пустой коллекции/массива. Рекомендация: возвращайте Collections.emptyList()/пустой массив — это избавит вызывающий код от проверок на null.

Места:

- `diesel/SelectQuery.java`:562 — Return an empty collection instead of null.
- `diesel/SelectQuery.java`:610 — Return an empty collection instead of null.

### java:S4042 — "java.nio.Files#delete" should be preferred

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 2

Проблема: File.delete() возвращает boolean и не бросает исключение при ошибке. Рекомендация: используйте java.nio.file.Files.delete(path) — он бросает IOException, и ошибку нельзя молча проигнорировать.

Места:

- `diesel/Database.java`:338 — Use "java.nio.file.Files#delete" here for better messages on error conditions.
- `diesel/Database.java`:339 — Use "java.nio.file.Files#delete" here for better messages on error conditions.

### java:S1171 — Only static class initializers should be used

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 2

Проблема: присваивание не-статическому полю в статическом инициализаторе. Рекомендация: перенесите инициализацию в instance-контекст или сделайте поле статическим.

Места:

- `diesel/SelectQuery.java`:108 — Move the contents of this initializer to a standard constructor or to field initializers.
- `diesel/Table.java`:248 — Move the contents of this initializer to a standard constructor or to field initializers.

### java:S6397 — Character classes in regular expressions should not contain only one character

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 1

Проблема: character-класс регэкспа содержит только один символ (например [a]). Рекомендация: напишите символ без класса: 'a'.

Места:

- `diesel/SubqueryParser.java`:841 — Replace this character class by the character itself.

### java:S3824 — "Map.get" and value test should be replaced with single method call

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 1

Проблема: связка map.get(key) + проверка + map.put(). Рекомендация: используйте computeIfAbsent()/getOrDefault()/putIfAbsent().

Места:

- `diesel/SelectQuery.java`:377 — Replace this "Map.containsKey()" with a call to "Map.computeIfAbsent()".

### java:S5785 — JUnit assertTrue/assertFalse should be simplified to the corresponding dedicated assertion

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 1

Проблема: assertTrue/assertFalse используются вместо профильных ассертов. Рекомендация: используйте assertEquals(a, b), assertInstanceOf(T.class, x), assertNull и т.п.

Места:

- `src/test/java/diesel/PersistenceTest.java`:70 — Use assertSame instead.

### java:S2629 — "Preconditions" and logging arguments should not require evaluation

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 1

Проблема: аргументы логирования требуют вычисления даже когда уровень выключен. Рекомендация: используйте форматные спецификаторы или передавайте Supplier/лямбду.

Места:

- `diesel/QueryParser.java`:43 — Use the built-in formatting to construct this argument.

### java:S5961 — Test methods should not contain too many assertions

**Severity:** MAJOR | **Тип:** CODE_SMELL | **Найдено:** 1

Проблема: в тесте слишком много assert. Рекомендация: разбейте тест на несколько независимых тестов с одной проверкой/поведением.

Места:

- `src/test/java/diesel/PersistenceTest.java`:75 — Refactor this method to reduce the number of assertions from 26 to less than 25.

### java:S3776 — Cognitive Complexity of methods should not be too high

**Severity:** CRITICAL | **Тип:** CODE_SMELL | **Найдено:** 63

Проблема: когнитивная сложность метода выше порога (15) — метод трудно читать и поддерживать. Рекомендация: разбейте метод на несколько приватных методов по одной ответственности, вынесите вложенные условия и циклы в отдельные методы, уменьшите глубину вложенности.

Места:

- `diesel/BTreeClusteredIndex.java`:125 — Refactor this method to reduce its Cognitive Complexity from 26 to the 15 allowed.
- `diesel/BTreeIndex.java`:129 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/BTreeIndex.java`:167 — Refactor this method to reduce its Cognitive Complexity from 20 to the 15 allowed.
- `diesel/Database.java`:31 — Refactor this method to reduce its Cognitive Complexity from 63 to the 15 allowed.
- `diesel/Database.java`:176 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/Database.java`:197 — Refactor this method to reduce its Cognitive Complexity from 31 to the 15 allowed.
- `diesel/DatabaseServer.java`:173 — Refactor this method to reduce its Cognitive Complexity from 17 to the 15 allowed.
- `diesel/DeleteQuery.java`:19 — Refactor this method to reduce its Cognitive Complexity from 64 to the 15 allowed.
- `diesel/DeleteQuery.java`:155 — Refactor this method to reduce its Cognitive Complexity from 53 to the 15 allowed.
- `diesel/DeleteQuery.java`:254 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `diesel/DeleteQuery.java`:300 — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- `diesel/InsertQuery.java`:22 — Refactor this method to reduce its Cognitive Complexity from 65 to the 15 allowed.
- `diesel/QueryParser.java`:212 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- `diesel/QueryParser.java`:499 — Refactor this method to reduce its Cognitive Complexity from 31 to the 15 allowed.
- `diesel/QueryParser.java`:677 — Refactor this method to reduce its Cognitive Complexity from 38 to the 15 allowed.
- `diesel/QueryParser.java`:813 — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- `diesel/QueryParser.java`:974 — Refactor this method to reduce its Cognitive Complexity from 100 to the 15 allowed.
- `diesel/QueryParser.java`:1114 — Refactor this method to reduce its Cognitive Complexity from 49 to the 15 allowed.
- `diesel/QueryParser.java`:1263 — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- `diesel/QueryParser.java`:1351 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `diesel/QueryParser.java`:1478 — Refactor this method to reduce its Cognitive Complexity from 58 to the 15 allowed.
- `diesel/QueryParser.java`:1555 — Refactor this method to reduce its Cognitive Complexity from 22 to the 15 allowed.
- `diesel/QueryParser.java`:1595 — Refactor this method to reduce its Cognitive Complexity from 16 to the 15 allowed.
- `diesel/QueryParser.java`:1674 — Refactor this method to reduce its Cognitive Complexity from 21 to the 15 allowed.
- `diesel/QueryParser.java`:1860 — Refactor this method to reduce its Cognitive Complexity from 52 to the 15 allowed.
- `diesel/QueryParser.java`:1971 — Refactor this method to reduce its Cognitive Complexity from 43 to the 15 allowed.
- `diesel/QueryParser.java`:2144 — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- `diesel/QueryParser.java`:2504 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/QueryParser.java`:2579 — Refactor this method to reduce its Cognitive Complexity from 42 to the 15 allowed.
- `diesel/QueryParser.java`:2678 — Refactor this method to reduce its Cognitive Complexity from 73 to the 15 allowed.
- `diesel/QueryParser.java`:2793 — Refactor this method to reduce its Cognitive Complexity from 47 to the 15 allowed.
- `diesel/QueryParser.java`:2901 — Refactor this method to reduce its Cognitive Complexity from 22 to the 15 allowed.
- `diesel/QueryParser.java`:3004 — Refactor this method to reduce its Cognitive Complexity from 44 to the 15 allowed.
- `diesel/SelectQuery.java`:78 — Refactor this method to reduce its Cognitive Complexity from 157 to the 15 allowed.
- `diesel/SelectQuery.java`:382 — Refactor this method to reduce its Cognitive Complexity from 47 to the 15 allowed.
- `diesel/SelectQuery.java`:480 — Refactor this method to reduce its Cognitive Complexity from 40 to the 15 allowed.
- `diesel/SelectQuery.java`:560 — Refactor this method to reduce its Cognitive Complexity from 45 to the 15 allowed.
- `diesel/SelectQuery.java`:673 — Refactor this method to reduce its Cognitive Complexity from 37 to the 15 allowed.
- `diesel/SelectQuery.java`:791 — Refactor this method to reduce its Cognitive Complexity from 25 to the 15 allowed.
- `diesel/SelectQuery.java`:830 — Refactor this method to reduce its Cognitive Complexity from 29 to the 15 allowed.
- `diesel/SqlLexer.java`:79 — Refactor this method to reduce its Cognitive Complexity from 80 to the 15 allowed.
- `diesel/SubqueryParser.java`:143 — Refactor this method to reduce its Cognitive Complexity from 29 to the 15 allowed.
- `diesel/SubqueryParser.java`:224 — Refactor this method to reduce its Cognitive Complexity from 31 to the 15 allowed.
- `diesel/SubqueryParser.java`:285 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/SubqueryParser.java`:437 — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- `diesel/SubqueryParser.java`:466 — Refactor this method to reduce its Cognitive Complexity from 36 to the 15 allowed.
- `diesel/SubqueryParser.java`:572 — Refactor this method to reduce its Cognitive Complexity from 29 to the 15 allowed.
- `diesel/SubqueryParser.java`:731 — Refactor this method to reduce its Cognitive Complexity from 20 to the 15 allowed.
- `diesel/SubqueryParser.java`:821 — Refactor this method to reduce its Cognitive Complexity from 32 to the 15 allowed.
- `diesel/SubqueryParser.java`:905 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `diesel/SubqueryParser.java`:1040 — Refactor this method to reduce its Cognitive Complexity from 16 to the 15 allowed.
- `diesel/SubqueryParser.java`:1142 — Refactor this method to reduce its Cognitive Complexity from 16 to the 15 allowed.
- `diesel/SubqueryParser.java`:1173 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/SubqueryParser.java`:1267 — Refactor this method to reduce its Cognitive Complexity from 20 to the 15 allowed.
- `diesel/SubqueryParser.java`:1312 — Refactor this method to reduce its Cognitive Complexity from 21 to the 15 allowed.
- `diesel/SubqueryParser.java`:1390 — Refactor this method to reduce its Cognitive Complexity from 40 to the 15 allowed.
- `diesel/SubqueryParser.java`:1466 — Refactor this method to reduce its Cognitive Complexity from 63 to the 15 allowed.
- `diesel/Table.java`:277 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/Table.java`:327 — Refactor this method to reduce its Cognitive Complexity from 67 to the 15 allowed.
- `diesel/UpdateQuery.java`:20 — Refactor this method to reduce its Cognitive Complexity from 26 to the 15 allowed.
- `diesel/UpdateQuery.java`:105 — Refactor this method to reduce its Cognitive Complexity from 53 to the 15 allowed.
- `diesel/UpdateQuery.java`:204 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `diesel/UpdateQuery.java`:250 — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.

### java:S1192 — String literals should not be duplicated

**Severity:** CRITICAL | **Тип:** CODE_SMELL | **Найдено:** 34

Проблема: строковый литерал дублируется много раз. Рекомендация: вынесите литерал в константу private static final String и используйте её во всех местах.

Места:

- `diesel/Database.java`:27 — Define a constant instead of duplicating this literal "Table " 4 times.
- `diesel/Database.java`:137 — Define a constant instead of duplicating this literal " does not exist" 3 times.
- `diesel/Database.java`:210 — Define a constant instead of duplicating this literal "Table part after split: {0}" 5 times.
- `diesel/Database.java`:322 — Define a constant instead of duplicating this literal ".table" 3 times.
- `diesel/QueryParser.java`:64 — Define a constant instead of duplicating this literal "NOT LIKE" 7 times.
- `diesel/QueryParser.java`:511 — Define a constant instead of duplicating this literal "SELECT" 4 times.
- `diesel/QueryParser.java`:515 — Define a constant instead of duplicating this literal "UPDATE" 3 times.
- `diesel/QueryParser.java`:821 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- `diesel/QueryParser.java`:855 — Define a constant instead of duplicating this literal "quotedString" 3 times.
- `diesel/QueryParser.java`:863 — Define a constant instead of duplicating this literal "openParen" 3 times.
- `diesel/QueryParser.java`:867 — Define a constant instead of duplicating this literal "closeParen" 3 times.
- `diesel/QueryParser.java`:981 — Define a constant instead of duplicating this literal "|\\(.*\\))\\s*\\)(?:\\s+(?:AS\\s+)?(" 5 times.
- `diesel/QueryParser.java`:986 — Define a constant instead of duplicating this literal "(?i)^(" 9 times.
- `diesel/QueryParser.java`:1006 — Define a constant instead of duplicating this literal "COUNT" 3 times.
- `diesel/QueryParser.java`:1281 — Define a constant instead of duplicating this literal "LIMIT" 3 times.
- `diesel/QueryParser.java`:1720 — Define a constant instead of duplicating this literal "Table not found: " 3 times.
- `diesel/QueryParser.java`:1744 — Define a constant instead of duplicating this literal "Unknown column: " 4 times.
- `diesel/QueryParser.java`:1880 — Define a constant instead of duplicating this literal "' does not match column type: " 4 times.
- `diesel/QueryParser.java`:1896 — Define a constant instead of duplicating this literal "Numeric value '" 5 times.
- `diesel/QueryParser.java`:1975 — Define a constant instead of duplicating this literal "Quoted String" 3 times.
- `diesel/QueryParser.java`:1983 — Define a constant instead of duplicating this literal "(?i)(" 7 times.
- `diesel/QueryParser.java`:2216 — Define a constant instead of duplicating this literal "SELECT " 3 times.
- `diesel/QueryParser.java`:2594 — Define a constant instead of duplicating this literal "(SELECT" 5 times.
- `diesel/QueryParser.java`:2986 — Define a constant instead of duplicating this literal "ID=U.ID" 4 times.
- `diesel/QueryParser.java`:2986 — Define a constant instead of duplicating this literal "ID = U.ID" 4 times.
- `diesel/SubqueryParser.java`:62 — Define a constant instead of duplicating this literal "SELECT" 10 times.
- `diesel/SubqueryParser.java`:145 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- `diesel/SubqueryParser.java`:231 — Define a constant instead of duplicating this literal "(?i)^(" 8 times.
- `diesel/SubqueryParser.java`:249 — Define a constant instead of duplicating this literal "SUBQUERY_" 3 times.
- `diesel/SubqueryParser.java`:831 — Define a constant instead of duplicating this literal "(?i)(" 6 times.
- `diesel/SubqueryParser.java`:874 — Define a constant instead of duplicating this literal "<end>" 3 times.
- `diesel/SubqueryParser.java`:1119 — Define a constant instead of duplicating this literal "Unbalanced parentheses in subquery: " 3 times.
- `diesel/Table.java`:60 — Define a constant instead of duplicating this literal " does not exist" 5 times.
- `diesel/Table.java`:82 — Define a constant instead of duplicating this literal "Column " 5 times.

### java:S1948 — Fields in a "Serializable" class should either be transient or serializable

**Severity:** CRITICAL | **Тип:** CODE_SMELL | **Найдено:** 8

Проблема: поле Serializable-класса не является сериализуемым (или изменяемым/не transient). Рекомендация: сделайте поле transient, либо убедитесь, что его тип сериализуем.

Места:

- `diesel/BTreeClusteredIndex.java`:14 — Make "keys" transient or serializable.
- `diesel/BTreeClusteredIndex.java`:15 — Make "rowIndices" private or transient.
- `diesel/BTreeClusteredIndex.java`:16 — Make "children" private or transient.
- `diesel/BTreeIndex.java`:14 — Make "keys" transient or serializable.
- `diesel/BTreeIndex.java`:15 — Make "rowIndices" private or transient.
- `diesel/BTreeIndex.java`:16 — Make "children" private or transient.
- `diesel/HashIndex.java`:9 — Make "indexMap" transient or serializable.
- `diesel/UniqueIndex.java`:8 — Make "indexMap" transient or serializable.

### java:S2447 — "null" should not be returned from a "Boolean" method

**Severity:** CRITICAL | **Тип:** CODE_SMELL | **Найдено:** 8

Проблема: метод с типом возврата Boolean возвращает null. Рекомендация: возвращайте Boolean.TRUE/FALSE, используйте примитивный boolean или Optional<Boolean>.

Места:

- `diesel/DeleteQuery.java`:173 — Null is returned but a "Boolean" is expected.
- `diesel/DeleteQuery.java`:204 — Null is returned but a "Boolean" is expected.
- `diesel/SelectQuery.java`:692 — Null is returned but a "Boolean" is expected.
- `diesel/SelectQuery.java`:757 — Null is returned but a "Boolean" is expected.
- `diesel/ThreeValuedLogic.java`:53 — Null is returned but a "Boolean" is expected.
- `diesel/ThreeValuedLogic.java`:72 — Null is returned but a "Boolean" is expected.
- `diesel/UpdateQuery.java`:123 — Null is returned but a "Boolean" is expected.
- `diesel/UpdateQuery.java`:154 — Null is returned but a "Boolean" is expected.

### java:S1452 — Generic wildcard types should not be used in return types

**Severity:** CRITICAL | **Тип:** CODE_SMELL | **Найдено:** 2

Проблема: generic wildcard в возвращаемом типе (List<? extends X>). Рекомендация: возвращайте конкретный параметризованный тип, чтобы API был предсказуемым для вызывающего кода.

Места:

- `diesel/QueryParser.java`:499 — Remove usage of generic wildcard type.
- `diesel/SubqueryParser.java`:51 — Remove usage of generic wildcard type.

### java:S2093 — Try-with-resources should be used

**Severity:** CRITICAL | **Тип:** CODE_SMELL | **Найдено:** 1

Проблема: ресурсы (Closeable/AutoCloseable) закрываются вручную. Рекомендация: используйте try-with-resources — ресурсы закроются гарантированно.

Места:

- `diesel/DatabaseServer.java`:174 — Change this "try" to a try-with-resources.

---
Сгенерировано: SonarQube 10.7.0.96327, API /api/issues/search (проект dieseldb).

