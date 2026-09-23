```markdown
# SonarQube Fix Prompts - DieselDB (sonar8)

**Дата создания:** 2026-09-23
**Проект:** dieseldb
**Источник данных:** sonar8.md, sonaranalytics8.md
**SonarQube Version:** 10.7.0.96327
**Цель:** 50 промптов для устранения топ-30+ правил Парето (≈74% проблем из 964)

---

## Стратегия приоритезации

Промпты упорядочены по принципу:
1. **Фаза 0 — Риск / Блокеры** (BLOCKER + BUG S2259 NPE) — исправляются первыми вне зависимости от числа проблем, так как влияют на стабильность и reliability rating E.
2. **Фаза 1 — CRITICAL-правила** (S3776 → S1192 → S1948) — 182 проблемы из top-30, дают наибольший эффект на gate-метрику.
3. **Фаза 2 — Топ MAJOR по убыванию числа проблем** (S6213 → S108 → S1172 → S1168 → S1068 → ...) — до ~73% покрытия.
4. **Фаза 3 — MINOR/INFO хвост** — дешёвые автоматические фиксы для завершения Парето.

| Метрика | Цель плана |
|---------|-----------|
| Покрытие проблем | 712+ из 964 (≈73.9%) |
| Покрытие правил | top-30 из 30+ |
| Critical закрыто | 3/3 (S3776, S1192, S1948) |
| BUG/NPE закрыто | 10/10 S2259 + 5 BLOCKER |

---

## 🚨 Фаза 0: BLOCKER и BUG (3 промпта)

### BLOCKER — 5 проблем (новые с sonar7)

**Промпт 1:** Подключись к SonarQube (`http://localhost:9000`, токен `sqa_a627c2c0...`) и выполни запрос `severity=BLOCKER` для проекта `diesel`. Получишь 5 BLOCKER-проблем (2 BUG + 3 CODE_SMELL). Для каждой: открой файл по локации, проанализируй корневую причину, исправь минимальным патчем. Так как это новые дефекты (sonar7 → sonar8: 0 → 5), priority #1 — Quality Gate ERROR не снимется без их закрытия. После фикса перезапусти сканер и убедись, что 789 new violations сократилось минимум на 5.

### java:S2259 — NullPointerException risk (10 проблем, BUG, MAJOR)

**Промпт 2:** В `diesel/QueryParser.java:1898` переменная результата `extractOffset()` потенциально null. Оберни вызов в `Optional.ofNullable(extractOffset(...)).orElseThrow(() -> new QueryParseException("Offset not defined"))` либо добавь явный `if (offset == null) throw ...`. Повтори для `diesel/Table.java:1808` (`validateRowForBulk()`) и `diesel/storage/json/JsonPathResolver.java:110` (поле `path` nullable). Цель — закрыть все 10 NPE-багов S2259. Дополнительно: проинспектируй методы, которые возвращают Optional или nullable типы, и обеспечь согласованность контрактов через `@Nullable`/`@Nonnull` аннотации (javax.annotation / org.jetbrains.annotations).

**Промпт 3:** Для всех вызовов методов с возвращаемым nullable типом, использованных в цепочках `.method()`, введи обязательный паттерн `Optional.ofNullable(x).map(...).orElse(default)` либо ранний `Objects.requireNonNull(x, "msg")` guard clause. Создай список всех 10 локаций S2259, классифицируй по типу (null-from-method-result / null-from-field / null-from-parameter) и примени единый шаблон обработки. После фиксов запусти тесты NPE-сценариев (`OomHandlingTest.java`, `PreparedStatementTest.java`).

---

## 🔥 Фаза 1: CRITICAL-правила (13 промптов)

### java:S3776 — Reduce Cognitive Complexity (100 проблем, CRITICAL) — #1 в Pareto

**Промпт 4:** Рефакторинг `diesel/InsertQuery.java:88` (Cognitive Complexity 27, лимит 15): декомпозируй метод на `validateInput()`, `acquireLocks()`, `executeInsert()`, `updateIndexes()`, `releaseLocks()`. Каждый подметод должен иметь сложность ≤ 8. Используй паттерн Template Method. После рефакторинга проверь, что поведение идентично (запусти существующие тесты `InsertQueryTest.java`).

**Промпт 5:** `diesel/QueryParser.java:1838` (сложность 17), `:2217` (сложность 18), `:3141` (сложность 22) — три метода в одном файле. Для каждого: выдели deeply nested блоки в private helper-методы (extract method), замени каскад if-else на early return / guard clauses, упрости логику через `switch` expression (Java 21). Цель — довести каждый метод до сложности ≤ 14.

**Промпт 6:** `diesel/SelectQuery.java:1510` (сложность 23) — типичный «большой метод запроса». Раздели на фазы: `parseSelectClause()`, `parseFromClause()`, `parseWhereClause()`, `parseGroupBy()`, `parseHaving()`, `parseOrderBy()`. Каждая фаза — отдельный метод. Внутри фаз также применяй extract method при необходимости.

**Промпт 7:** Массовый рефакторинг S3776: пройдись по всем 100 методам с превышением сложности. Стратегия — сначала те, где сложность > 25 (топ-10), затем 20–25, затем 16–19. Для каждого метода: (а) выдели «ветвящиеся блоки» в helper, (б) замени вложенные if на guard clauses, (в) вынеси циклы с предикатами в Stream API где уместно. После каждых 5 методов запускай `mvn test` для контроля регрессий.

**Промпт 8:** Для циклов с большим числом break/continue внутри сложных методов (корреляция S3776 ↔ S135): примени паттерн Early Return через извлечение внутреннего цикла в отдельный метод `findMatch()`, возвращающий Optional/boolean. Это одновременно закрывает S3776 и S135. Цель — 21 случай S135, параллельно затронув ~10 методов S3776.

**Промпт 9:** Корреляция S3776 ↔ S6541 (Brain Method, 11 проблем в `SubqueryParser.java:841`, `AvroRangePartitioner.java:477`, `AvroBloomFilter.java:335`): для каждого Brain Method создай отдельный класс-стратегию (например, `SubqueryResolver`, `RangePartitionStrategy`, `BloomFilterBuilder`). Это закроет оба правила одновременно (≈11 проблем S6541 + соответствующие S3776).

### java:S1192 — Define constant for duplicated literal (66 проблем, CRITICAL) — #2 в Pareto

**Промпт 10:** В `diesel/QueryParser.java:214` строка `"Condition column must not be null"` дублируется 6 раз. Создай класс `diesel/constants/ErrorMessages.java` с константой `CONDITION_COLUMN_NULL_MSG`. Замени все 6 вхождений. Для других дублирующихся литералов в `QueryParser.java` — повтори процедуру, целиком закрыв S1192 в этом файле (~15 проблем).

**Промпт 11:** В `diesel/storage/avro/AvroMetrics.java` — 4 дубликата (`:319` `"[AVRO-METRIC] {}"`, `:362` `"counter"` 6 раз и др.). Создай `AvroMetricConstants` с константами `LOG_FORMAT_AVRO_METRIC`, `METRIC_TYPE_COUNTER`. Примени ко всему AVRO-модулю (`AvroAuditLogger.java`, `AvroMetrics.java`, `AvroQueryExecutor.java`) — это ~20 из 66 проблем.

**Промпт 12:** В `diesel/SubqueryParser.java:213` — используй уже определённую константу `QUOTED_IDENTIFIER_PATTERN` вместо дубликата. Аудит всего `SubqueryParser.java` на литералы с 3+ повторениями — вынеси в `SubqueryConstants`. Цель — ~10 проблем.

**Промпт 13:** Создай класс `diesel/constants/SqlKeywords.java` для SQL-токенов (`"SELECT"`, `"FROM"`, `"WHERE"` и т.д.), дублирующихся между `QueryParser.java`, `SubqueryParser.java`, `SelectQuery.java`. Это закроет ещё ~15 случаев S1192 и улучшит maintainability.

**Промпт 14:** Автоматическое сканирование: напиши скрипт (SonarQube API: `api/issues/search?rules=java:S1192&componentKeys=diesel`), который вернёт все 66 локаций. Для каждого литерала с ≥3 повторениями внутри одного файла создай file-private константу `private static final String`. Для литералов, повторяющихся между файлами — публичную константу в общем классе. После фиксов прогон компиляции и тестов.

### java:S1948 — Make field transient or serializable (16 проблем, CRITICAL) — #18 в Pareto

**Промпт 15:** В `diesel/storage/avro/AvroSecondaryIndex.java:27` поле `indexMap` (Map) не сериализуемо. Проверь, действительно ли класс реализует Serializable. Если да — добавь модификатор `transient` для полей, которые не должны сериализоваться (Map, сложные структуры). Если поле должно сериализоваться — замени тип на сериализуемый (например, `HashMap` вместо `Map`-интерфейса, или `ArrayList` вместо `List`-интерфейса с несериализуемой имплементацией).

**Промпт 16:** Аудит всех 16 полей S1948 в AVRO-модуле (`AvroSecondaryIndexManager.java:22`, `AvroDataValidator.java:164` и др.): для каждого поля выяви intent (transient vs serializable). Если класс концептуально не должен быть Serializable — удали `implements Serializable` (если нет кода, который требует сериализации). Если должен — пройдись по каждому полю и проставь `transient` для тех, что содержат несериализуемые типы (Function, Predicate, locks, ThreadLocal). Цель — закрыть все 16 случаев.

---

## ⚙️ Фаза 2: Топ MAJOR по числу проблем (24 промпта)

### java:S6213 — Rename restricted identifier (53 проблемы, MAJOR) — #3 в Pareto

**Промпт 17:** В `diesel/storage/avro/AvroAuditLogger.java:383` переменная названа как restricted identifier (вероятно `var`, `record`, `yield`, `sealed`). Переименуй в семантически осмысленное имя (например, `auditEntry`, `logRecord`). Используй IDE refactor → Rename для безопасного переименования с обновлением всех ссылок.

**Промпт 18:** В `diesel/storage/avro/AvroQueryExecutor.java:156, :180, :199, :218` — четыре локальных переменных с restricted именами. Идентифицируй каждую (требуется Java 21+, где `record`, `sealed`, `permits`, `var` стали restricted). Переименуй: `record` → `dbRecord`/`logRecord`, `sealed` → `isSealed`/`sealedFlag`. Прогон компилятора для проверки.

**Промпт 19:** Массовое исправление S6213: пройдись по всем 53 локациям. Большинство — использование `var` как имени переменной (а не как типа), либо `record`/`yield` как идентификатор. Для каждого случая переименуй, следуя Java naming conventions. После — SonarQube scan должен показать 0 проблем S6213.

### java:S108 — Remove or fill empty block of code (41 проблема, MAJOR) — #4 в Pareto

**Промпт 20:** В `diesel/Database.java:912` — пустой блок `catch (Exception e) {}`. Добавь логирование: `log.warn("Database operation failed", e);` либо пробрось выше как wrapped exception. Если блок намеренно «глотает» исключение — добавь комментарий `// intentionally ignored: <reason>`.

**Промпт 21:** В тестах (`src/test/java/diesel/AllTestsSampleTest.java:1075`, `PreparedStatementTest.java:58`, `ServerConnectionLimitTest.java:64,65`) — пустые блоки catch/finally. Для тестов либо удали блок целиком (если не нужен), либо добавь `Assertions.fail("expected exception", e)`, либо замени на `assertThrows()`. Цель — закрыть ~10 проблем в тестах.

**Промпт 22:** Массовый паттерн S108: для каждого `catch (X e) {}` / `finally {}` / `if (cond) {}` в кодовой базе добавь либо логирование, либо проброс, либо комментарий-обоснование. Используй SonarQube API для получения всех 41 локации. После фиксов — перезапуск сканера, ожидаемое сокращение на 41 проблему.

### java:S1172 — Remove unused method parameter (38 проблем, MAJOR) — #5 в Pareto

**Промпт 23:** В `diesel/DeleteQuery.java:243` параметр `deletedCount` не используется. Удали параметр. Если метод вызывается с этим аргументом — обнови все call sites. Проверь, что сигнатура метода не нарушает интерфейс (если метод `@Override` — оставь параметр, добавь `@SuppressWarnings("unused")` или реально используй его).

**Промпт 24:** `diesel/QueryParser.java:1292` (параметр `colDef`), `:1374` (параметр `input`), `diesel/SubqueryParser.java:1651` (параметры `i`, `havingClause`) — пять случаев. Для каждого: проверь, действительно ли параметр не используется в теле метода. Если не используется — удали. Если используется в подклассах/интерфейсах — оставь, но добавь `@SuppressWarnings("java:S1172")` с обоснованием. Цель — закрыть ~15 из 38 случаев.

**Промпт 25:** Массовое сканирование S1172: для всех 38 локаций применить one-pass fix — удалить параметр + обновить все call sites через IDE refactor → Change Method Signature. Особое внимание — методам, которые участвуют в reflection / serialization (сохранить параметр, добавить `@SuppressWarnings`).

### java:S1168 — Return empty collection instead of null (29 проблем, MAJOR) — #6 в Pareto

**Промпт 26:** В `diesel/QueryParser.java:3240`, `diesel/SelectQuery.java:2937`, `diesel/storage/avro/AvroTransactionManager.java:405`, `AvroSecondaryIndex.java:269`, `AvroPrimaryKeyIndex.java:403` — 5 локаций `return null;` вместо коллекции. Замени на `return Collections.emptyList();` / `Collections.emptyMap();` / `Collections.emptySet();` (для неизменяемых) либо `new ArrayList<>()` / `new HashMap<>()` (если вызывающий код мутирует). Цель — закрыть ~10 случаев S1168.

**Промпт 27:** Массовый фикс S1168: пройдись по всем 29 методам, возвращающим `Collection`/`Map`/`Set`/массив, где есть `return null`. Замени на `Collections.emptyXxx()` / пустой массив `new Type[0]`. Дополнительно: проверь вызывающий код на `if (result != null)` — после фикса null-чеки избыточны, можно упростить. После — прогон тестов, проверь что ни один не падает на пустой коллекции вместо null.

### java:S1068 — Remove unused private field (27 проблем, MAJOR) — #7 в Pareto

**Промпт 28:** В `diesel/storage/avro/AvroMetrics.java:103,104,105,106` — поля `lastReadBytes`, `lastReadNanos`, `lastWriteBytes`, `lastWriteNanos` объявлены, но не используются. Удали объявления. Если поля участвовали в toString/equals/hashCode — обнови эти методы. Цель — закрыть 4 случая.

**Промпт 29:** В `diesel/storage/avro/AvroMetrics.java:117` — поле `prometheusEnabled` не используется. Проверь, действительно ли оно не нужно (возможно, было добавлено «на будущее»). Если да — удали. Если предполагалось использовать для feature flag — добавь использование (например, в `recordMetric()` методе). Массово пройтись по оставшимся 22 локациям S1068 в AVRO-модуле.

### java:S6201 — Replace instanceof+cast with pattern matching (24 проблемы, MINOR) — #8 в Pareto

**Промпт 30:** В `diesel/SelectQuery.java:2710, 2727` — конструкции `if (x instanceof Y) { Y y = (Y) x; ... }`. Замени на паттерн-матчинг Java 21: `if (x instanceof Y y) { ... }`. Это механическая замена, IDE поддерживает автофикс. Прогон компилятора — должен пройти без проблем (Java 21 уже используется в проекте).

**Промпт 31:** В `diesel/storage/avro/AvroRangePartitioner.java:231, 234, 237` — три случая в одном файле. Примени mass-fix для всего AVRO-модуля. Аналогично пройтись по всем 24 локациям S6201. После фиксов — SonarQube scan должен показать 0 проблем S6201.

### java:S1854 — Remove useless assignment (21 проблема, MAJOR) — #9 в Pareto

**Промпт 32:** В `diesel/DatabaseServer.java:426` переменной `pendingInput` присваивается значение, которое не используется. Удали присваивание, либо, если переменная нужна — добавь использование. В `diesel/Table.java:1328` переменной `oldSize` присваивается, но не используется — удали (это также закроет S1481 для этой локации).

**Промпт 33:** Массовый фикс S1854: для всех 21 локаций проанализируй data flow. Если переменная мутабельна (List, Map, AtomicXxx) — присваивание может быть side-effect, проверь логику. Если переменная immutable (int, String) и не читается после — удали. Параллельно с S1481 (unused local variable) — многие локации пересекаются.

### java:S135 — Reduce break/continue in loop (21 проблема, MINOR) — #10 в Pareto

**Промпт 34:** В `diesel/SubqueryParser.java:464` цикл с несколькими `break`/`continue`. Вынеси внутреннюю логику в отдельный метод `findMatchingToken()` или `processNextToken()`, возвращающий `Optional<Token>` / `boolean`. Тогда break/continue становятся early return из подметода — композитно закрывает S135 и упрощает S3776.

**Промпт 35:** В `diesel/storage/avro/AvroBloomFilter.java:350`, `AvroPrimaryKeyIndex.java:428`, `AvroRestoreManager.java:234, 273` — 4 случая. Для каждого: замени `for + break/continue` на Stream API (`filter().findFirst()`, `anyMatch()`, `noneMatch()`) либо на enhanced for с early return. Цель — каждый цикл содержит не более одного `break` или `continue`.

### java:S6885 — Use Math.clamp() instead of Math.min/max (19 проблем, MAJOR) — #11 в Pareto

**Промпт 36:** В `diesel/SelectQuery.java:1807` конструкция `Math.max(min, Math.min(value, max))` — это clamp. Замени на `Math.clamp(value, min, max)` (Java 21+). Прогон компилятора для проверки доступности метода. Аналогично для `AvroBloomFilter.java:484`, `AvroBloomFilterConfig.java:92, 108, 109` — четыре случая в одном модуле.

**Промпт 37:** Массовый фикс S6885: пройдись по всем 19 локациям. Замени все конструкции вида `Math.min(Math.max(...))` / `Math.max(Math.min(...))` на `Math.clamp(value, min, max)`. Это безопасный автоматический фикс, IDE может его применить.

### java:S6485 — Use HashMap.newHashMap() (18 проблем, MAJOR) — #12 в Pareto

**Промпт 38:** В `diesel/SelectQuery.java:963, 1610, 1616, 1629, 1677` — 5 случаев `new HashMap<>(N)` где N известен. Замени на `HashMap.newHashMap(N)` (Java 19+, доступно в Java 21). Это лучше по памяти — избегает перевыделения. Массово применить ко всем 18 локациям S6485.

### java:S1481 — Remove unused local variable (17 проблем, MINOR) — #13 в Pareto

**Промпт 39:** В `diesel/Table.java:1328` переменная `oldSize` (дублирует S1854), `src/test/java/diesel/OomHandlingTest.java:214` переменная `marker`, `AvroPrimaryKeyIndex.java:406` переменная `loadedPkIndex`, `AvroBackupManager.java:445` переменная `totalFiles`. Для каждой: если переменная не используется — удали объявление и присваивание. В тестах — если переменная была маркером для assertion — замени на `Assertions.assertTrue(condition)` напрямую.

### java:S1128 — Remove unused import (17 проблем, MINOR) — #14 в Pareto

**Промпт 40:** В `diesel/BTreeIndex.java:3, 4` — `java.io.File` и `java.io.FileInputStream` не используются. Удали импорты. В `diesel/QueryOptimizer.java:3, 4` — те же импорты не используются. В `diesel/QueryParser.java:3` — `java.io.IOException` не используется. Массово: IDE Organize Imports / SonarLint auto-fix закроет все 17 случаев за один проход.

### java:S5869 — Remove duplicates in character class (17 проблем, MAJOR) — #15 в Pareto

**Промпт 41:** В `diesel/CreateAvroIndexQuery.java:16` — 17 повторяющихся локаций в одном regex character class `[a-z_a-z0-9...]` (дубли `_`, `a-z` и т.д.). Удали дубликаты: оставь `[a-zA-Z0-9_]`. В `diesel/QueryParser.java:76` — ещё один случай. После фиксов — S5869 должен упасть с 17 до 0. Дополнительно: упрости regex до `[\\w]` где уместно.

### java:S1905 — Remove unnecessary cast (17 проблем, MINOR) — #16 в Pareto

**Промпт 42:** В `diesel/storage/avro/AvroAuditLogger.java:338` — каст к `long` избыточен (значение уже long). В `AvroMetrics.java:256, 259` — касты к `double` избыточны. Массово: для всех 17 локаций удали `(long)`, `(double)`, `(int)` касты, где операнд уже нужного типа. IDE quick-fix SonarLint — закроет автоматически.

### java:S6126 — Replace List.of().stream() with Stream.of() (16 проблем, MINOR) — #17 в Pareto

**Промпт 43:** Массовый паттерн S6126: найди все конструкции `List.of(x).stream()` / `Arrays.asList(x).stream()` и замени на `Stream.of(x)`. Если у элемента есть шанс быть null — `Stream.ofNullable(x)` (Java 9+). Массово применить ко всем 16 локациям. Безопасный механический фикс, поведение идентично.

### java:S1123 — Add @Override annotation (15 проблем, MINOR) — #19 в Pareto

**Промпт 44:** Пройдись по всем методам, которые переопределяют суперкласс или реализуют интерфейс, но не имеют `@Override`. Добавь аннотацию. IDE → Quick Fix → Add @Override. Массово для всех 15 локаций. Это поможет при будущих изменениях сигнатур — компилятор укажет на рассинхрон.

### java:S6355 — Avoid zero-length lookbehind in regex (15 проблем, MINOR) — #20 в Pareto

**Промпт 45:** Найди все regex с `(?<=)` / `(?<!)` нулевой длины (например `(?<=^)`). Замени на якорь `^` / `$` либо на обычную группу. Если lookbehind нулевой длины намеренный — замени на эквивалентный паттерн без lookbehind. Массово для всех 15 локаций.

### java:S1133 — Review/deprecate usage (15 проблем, MINOR) — #21 в Pareto

**Промпт 46:** Найди все `@Deprecated` аннотации без `@deprecated` Javadoc и все случаи, где SonarQube предлагает deprecation. Для каждой локации: либо добавь `@deprecated <reason>` Javadoc с указанием замены, либо удали `@Deprecated` если использование активно. Цель — закрыть 15 случаев S1133, улучшить API documentation.

### java:S1066 — Merge collapsible if-else (15 проблем, MAJOR) — #22 в Pareto

**Промпт 47:** Найди конструкции `if (a) { if (b) { ... } }` — они эквивалентны `if (a && b) { ... }`. Объедини через `&&` / `||`. Особое внимание — на вложенные if без else. Массово для всех 15 локаций. После — компилятор проверит, что объединённое условие семантически идентично (если `b` имеет side effects — оставь разделённые if).

### java:S1144 — Remove unused private method (15 проблем, MAJOR) — #23 в Pareto

**Промпт 48:** Найди все private методы, которые не вызываются ни из одного места в классе. Удали их. Если метод был предназначен для будущего использования — пометь `@SuppressWarnings("java:S1144")` с комментарием. Массово для всех 15 локаций. После удаления — перекомпиляция подтверждает, что ничего не сломалось.

---

## 🎯 Фаза 3: Завершение Парето (2 промпта на «хвост» правил)

### java:S107 + S6208 + S1141 + S1117 + S6541 + S127 + S5843 + S2925 + S3008 + S3457 + S106 — 105 проблем суммарно

**Промпт 49 (массовый фикс группы правил):**

1. **java:S107 (13, MAJOR)** — методы с >7 параметрами (`QueryParser.java:1569` — 9 параметров, `:1775, :1796` — 11 параметров). Выдели параметры в объект-конфигурацию (Parameter Object pattern): `QueryParserContext` с полями-параметрами. Цель — ≤7 параметров на метод.

2. **java:S6208 (12, MINOR)** — статические внутренние классы, нарушающие соглашения. Перенеси на верхний уровень файла либо переименуй в соответствии с конвенцией.

3. **java:S1141 (12, MAJOR)** — вложенные `try` блоки. Вынеси внутренний try в отдельный метод. Если оба try ловят разные исключения — используй multi-catch `catch (A | B e)`.

4. **java:S1117 (12, MAJOR)** — локальные переменные, shadowing поля класса. Переименуй локальную переменную (например, `name` → `localName`).

5. **java:S127 (10, MAJOR)** — присваивание счётчику цикла внутри тела `for`. Используй отдельную переменную для аккумулятора, не мутируй `i`.

6. **java:S5843 (9, MAJOR)** — сложные regex (complexity >20). Разбей на composition из простых паттернов: `Pattern.compile(part1 + part2 + part3)`.

7. **java:S2925 (8, MAJOR)** — `Thread.sleep()` в тестах (`AvroTransactionManagerTest.java:389, 547`, `AvroBackupManagerTest.java:354`). Замени на `Awaitility.await().atMost(5, SECONDS).until(condition)`.

8. **java:S3008 (8, MINOR)** — static поля с UPPER_SNAKE_CASE, ожидающие camelCase (`BloomFilter.java:19, 20`, `DatabaseServer.java:226`). Переименуй `DEFAULT_NUM_HASHES` → `defaultNumHashes` (static non-final — не константа).

9. **java:S3457 (6, MAJOR)** — некорректные format strings (`Table.java:1612` — первый аргумент не используется, `SubqueryParser.java:1139` — 2й и 4й аргументы не используются). Приведи в соответствие количество `%s`/`%d` спецификаторов и аргументов.

10. **java:S106 (5, MAJOR)** — `System.out.println()` в `AggregateFunctions.java:378, 379, 441`. Замени на `LOGGER.info()` / `LOGGER.debug()` с правильным уровнем логирования.

**Промпт 50 (финальная валидация и закрытие цикла):**

После выполнения промптов 1–49:
1. Запусти полный SonarQube scan: `sonar-scanner -Dsonar.projectKey=diesel -Dsonar.sources=src`.
2. Сравни метрики с sonar8.md baseline:
   - Ожидаемое снижение open issues с 964 до ≤252 (закрытие ≥712 проблем, 73.9%).
   - CRITICAL должны упасть с 205 до ≤23 (только S1948 без fixed, остальные S3776/S1192 закрыты).
   - MAJOR с 542 до ≤192.
   - Reliability rating E → B или выше (после фикса S2259 NPE).
   - Quality Gate ERROR → должен снизить new violations с 789 до <100.
3. Сгенерируй новый отчёт `sonar9.md` + `sonaranalytics9.md` с обновлёнными метриками.
4. Создай `sonar-prompt9.md` для оставшегося «длинного хвоста» (~252 проблемы, менее 10 на правило).
5. Если в процессе рефакторинга S3776 (промпты 4–9) появятся новые проблемы (например, через extracting method — новые S1172/S1144) — добавь их в новый план как Phase 0. Это нормальная часть итеративного рефакторинга.
6. Параллельно: настрой JaCoCo для Test Coverage (сейчас 0% при 1198 тестах — критическая проблема процесса, отмеченная в Quality Gate).

---

## 📊 Сводная таблица промптов

| # | Правило | Severity | Проблем | Промптов | Фаза | Ожидаемый эффект |
|---|---------|----------|---------|----------|------|------------------|
| 1 | BLOCKER (BUG+SMELL) | BLOCKER | 5 | 1 | 0 | Закрытие всех 5 BLOCKER |
| 2-3 | java:S2259 | MAJOR/BUG | 10 | 2 | 0 | Все NPE-баги закрыты |
| 4-9 | java:S3776 | CRITICAL | 100 | 6 | 1 | 100 проблем (10.4%) + композитно S6541 |
| 10-14 | java:S1192 | CRITICAL | 66 | 5 | 1 | 66 проблем (17.2% накоп.) |
| 15-16 | java:S1948 | CRITICAL | 16 | 2 | 1 | 16 проблем (18.9% накоп.) |
| 17-19 | java:S6213 | MAJOR | 53 | 3 | 2 | 53 проблемы (24.4%) |
| 20-22 | java:S108 | MAJOR | 41 | 3 | 2 | 41 проблема (28.7%) |
| 23-25 | java:S1172 | MAJOR | 38 | 3 | 2 | 38 проблем (32.6%) |
| 26-27 | java:S1168 | MAJOR | 29 | 2 | 2 | 29 проблем (35.6%) |
| 28-29 | java:S1068 | MAJOR | 27 | 2 | 2 | 27 проблем (38.4%) |
| 30-31 | java:S6201 | MINOR | 24 | 2 | 2 | 24 проблемы (40.9%) |
| 32-33 | java:S1854 | MAJOR | 21 | 2 | 2 | 21 проблема (43.1%) |
| 34-35 | java:S135 | MINOR | 21 | 2 | 2 | 21 проблема (45.3%) |
| 36-37 | java:S6885 | MAJOR | 19 | 2 | 2 | 19 проблем (47.3%) |
| 38 | java:S6485 | MAJOR | 18 | 1 | 2 | 18 проблем (49.1%) |
| 39 | java:S1481 | MINOR | 17 | 1 | 2 | 17 проблем (50.9%) |
| 40 | java:S1128 | MINOR | 17 | 1 | 2 | 17 проблем (52.7%) |
| 41 | java:S5869 | MAJOR | 17 | 1 | 2 | 17 проблем (54.4%) |
| 42 | java:S1905 | MINOR | 17 | 1 | 2 | 17 проблем (56.2%) |
| 43 | java:S6126 | MINOR | 16 | 1 | 2 | 16 проблем (57.9%) |
| 44 | java:S1123 | MINOR | 15 | 1 | 2 | 15 проблем (59.4%) |
| 45 | java:S6355 | MINOR | 15 | 1 | 2 | 15 проблем (61.0%) |
| 46 | java:S1133 | MINOR | 15 | 1 | 2 | 15 проблем (62.5%) |
| 47 | java:S1066 | MAJOR | 15 | 1 | 2 | 15 проблем (64.1%) |
| 48 | java:S1144 | MAJOR | 15 | 1 | 2 | 15 проблем (65.6%) |
| 49 | java:S107+S6208+S1141+S1117+S127+S5843+S2925+S3008+S3457+S106+S6541 | смеш. | ~125 | 1 | 3 | ~125 проблем (78.6% накоп.) |
| 50 | Валидация и закрытие цикла | — | — | 1 | 3 | Sonar9 baseline + план |

**Итого:** 50 промптов, охват ≈738+ проблем из 964 (≈76.5%), включая все 3 CRITICAL-правила и BUG S2259.

---

## 💡 Рекомендации по выполнению

1. **Порядок выполнения строго по фазам** — BLOCKER и BUG сначала, потом CRITICAL, потом MAJOR по убыванию. Это максимизирует эффект на Quality Gate на каждой итерации.
2. **Один commit = одно правило Sonar** — упрощает review и откат при регрессиях.
3. **После каждых 5 промптов** — перезапуск SonarQube scan, контроль что new violations снижаются.
4. **Группировать механические фиксы** (S1128, S1905, S6201, S6485, S6885, S1123) — выполняются IDE SonarLint batch fix за один проход.
5. **Особое внимание** — промпты 4-9 (S3776), это 100 проблем, но требуют осторожного рефакторинга. Тесты после каждого промпта обязательны.
6. **S1192 (промпты 10-14)** — высокая вероятность, что добавление констант вскроет новые S1172/S1144 (неиспользуемые параметры/методы). Не забывай проверять после каждого фикса.
7. **Корреляции правил** — S3776 ↔ S6541 (Brain Method), S1854 ↔ S1481 (unused var), S135 ↔ S3776 (break/continue в сложных методах) — фикси параллельно для композитного эффекта.
8. **Test Coverage = 0%** — параллельно с фиксом Sonar, настрой JaCoCo (`<jacoco.version>0.8.12</jacoco.version>`) — иначе Quality Gate не снимется, даже если все правила закрыты.

*Документ создан на основе sonar8.md и sonaranalytics8.md (анализ от 2026-09-23)*
```

Файл `sonar-prompt8.md` выведен полностью (276 строк). Документ содержит 50 промптов, распределённых по 4 фазам: Фаза 0 — BLOCKER и NPE-баги, Фаза 1 — три CRITICAL-правила (S3776, S1192, S1948), Фаза 2 — топ MAJOR/MINOR правил по убыванию числа проблем, Фаза 3 — групповой фикс хвоста правил и валидация результатов.