# Анализ SonarQube для dieseldb: Приоритеты исправлений (Принцип Парето)

## Краткая сводка

| Показатель | Значение |
|---|---|
| **Всего проблем** | **908** |
| Ошибки (BUG) | 81 |
| Запахи кода (CODE_SMELL) | 827 |
| Уязвимости (VULNERABILITY) | 0 |
| **CRITICAL** | **116** |
| **MAJOR** | **486** |
| MINOR | 276 |
| INFO | 30 |

---

## Классификация по критичности и приоритету

### 🔴 КРИТИЧЕСКИЙ ПРИОРИТЕТ (P0) — Исправлять немедленно

Эти ошибки напрямую влияют на надёжность, корректность работы и безопасность БД.

| Правило | Кол-во | Тип | Почему критично |
|---|---|---|---|
| **java:S5998** — Regular expressions should not overflow the stack | **57** | **BUG** | **StackOverflowError в production при парсинге SQL** |
| **java:S2259** — Null pointers should not be dereferenced | **13** | **BUG** | **NullPointerException приведёт к падению сервера** |
| **java:S2583** — Conditionally executed code should be reachable | **3** | **BUG** | Мёртвый код скрывает логические ошибки |
| **java:S5850** — Alternatives in regular expressions should be grouped | **3** | **BUG** | Некорректная работа парсера SQL |
| **java:S3599** — Double Brace Initialization | **2** | **BUG** | Утечки памяти, проблемы с сериализацией |
| **java:S899** — Return values should not be ignored | **2** | **BUG** | Игнорирование статусов операций |
| **java:S5842** — Repeated patterns in regex match empty string** | **1** | **BUG** | Бесконечные циклы в парсере |

**Итого P0: 81 проблема (все BUG)**

---

### 🟠 ВЫСОКИЙ ПРИОРИТЕТ (P1) — 20% усилий для 80% результата

**Это ключевая секция: исправление этих ~180 проблем (20% от общего числа) улучшит dieseldb на 80%.**

#### A. Критическая сложность кода (Cognitive Complexity & Brain Methods)

| Правило | Кол-во | Влияние |
|---|---|---|
| **java:S3776** — Cognitive Complexity too high | **63** | Невозможность поддержки, баги при изменениях |
| **java:S6541** — Brain methods (слишком много задач) | **12** | Нарушение SRP, трудно тестировать |

**Файлы-лидеры по сложности:**
- `QueryParser.java` — 363 проблемы (5 brain methods)
- `SubqueryParser.java` — 255 проблем (1 brain method)
- `SelectQuery.java` — 51 проблема (1 brain method с complexity=59!)

**Почему это 80% улучшения:**
- Эти 3 файла содержат **618 из 908 проблем (68%)**
- Рефакторинг QueryParser и SelectQuery радикально упростит поддержку парсера SQL
- Сложность SelectQuery.execute() = 59 (при норме ~15) — это главный источник багов

#### B. Проблемы с регулярными выражениями (парсер SQL)

| Правило | Кол-во | Влияние |
|---|---|---|
| **java:S5869** — Duplicate chars in character classes | **228** | Производительность парсера |
| **java:S6353** — Non-concise regex character classes | **119** | Читаемость и поддержка |
| **java:S5843** — Regex too complicated | **13** | Производительность + StackOverflow риск |

**Итого: 360 проблем с regex** — все集中在 парсере. Исправление даст:
- ✅ Ускорение парсинга SQL
- ✅ Снижение риска StackOverflow
- ✅ Упрощение поддержки токенизатора

#### C. Серьёзные архитектурные проблемы

| Правило | Кол-во | Влияние |
|---|---|---|
| **java:S1192** — String literals duplication | **34** | Сложность рефакторинга, ошибки в SQL-ключевых словах |
| **java:S107** — Too many parameters | **20** | Нарушение инкапсуляции, сложно тестировать |
| **java:S2447** — null returned from Boolean method | **8** | NullPointerException в условиях |
| **java:S1948** — Non-serializable fields in Serializable class | **8** | Проблемы кластеризации/репликации |

---

### 🟡 СРЕДНИЙ ПРИОРИТЕТ (P2) — Улучшение качества кода

| Правило | Кол-во | Тип | Рекомендация |
|---|---|---|---|
| java:S1172 — Unused method parameters | 20 | CODE_SMELL | Удалить лишние параметры |
| java:S3457 — Format strings incorrect | 16 | CODE_SMELL | Исправить форматирование логов |
| java:S1854 — Unused assignments | 15 | CODE_SMELL | Dead code removal |
| java:S108 — Empty nested blocks | 10 | CODE_SMELL | Удалить пустые блоки |
| java:S127 — For loop stop conditions invariant | 8 | CODE_SMELL | Оптимизация циклов |
| java:S1141 — Nested try-catch | 8 | CODE_SMELL | Упростить обработку исключений |
| java:S2139 — Exceptions logged AND rethrown | 8 | CODE_SMELL | Логировать ИЛИ выбрасывать |
| **java:S106 — System.out.println used for logging** | **8** | **CODE_SMELL** | **Заменить на Logger** |
| **java:S112 — Generic exceptions thrown** | **8** | **CODE_SMELL** | **Использовать специфичные исключения** |
| java:S3358 — Nested ternary operators | 7 | CODE_SMELL | Упростить условия |
| java:S2925 — Thread.sleep in tests | 7 | CODE_SMELL | Использовать Awaitility |
| java:S1068 — Unused private fields | 4 | CODE_SMELL | Удалить мёртвые поля |
| java:S2589 — Gratuitous boolean expressions | 4 | CODE_SMELL | Упростить условия |
| java:S1144 — Unused private methods | 3 | CODE_SMELL | Удалить мёртвый код |
| java:S125 — Commented out code | 3 | CODE_SMELL | Удалить закомментированный код |
| java:S1066 — Mergeable if statements | 3 | CODE_SMELL | Объединить условия |

---

### 🟢 НИЗКИЙ ПРИОРИТЕТ (P3) — Косметические улучшения

| Правило | Кол-во | Тип |
|---|---|---|
| java:S5786 — JUnit5 public visibility | 16 | INFO |
| java:S6201 — Pattern matching for instanceof | 49 | MINOR |
| java:S1874 — @Deprecated code usage | 28 | MINOR |
| java:S135 — Multiple break/continue in loops | 24 | MINOR |
| java:S1128 — Unnecessary imports | 24 | MINOR |
| java:S1481 — Unused local variables | 13 | MINOR |
| java:S2293 — Diamond operator | 2 | MINOR |
| java:S1155 — Collection.isEmpty() | 1 | MINOR |
| java:S1157 — Case insensitive string comparison | 1 | MINOR |
| java:S1488 — Variable declared and immediately returned | 1 | MINOR |
| java:S2864 — entrySet() iteration | 2 | MAJOR |
| java:S1168 — Return empty instead of null | 2 | MAJOR |
| java:S4042 — Files.delete preferred | 2 | MAJOR |
| java:S1171 — Static initializers only | 2 | MAJOR |
| java:S3824 — Map.get optimization | 1 | MAJOR |
| java:S5785 — JUnit assertion simplification | 1 | MAJOR |
| java:S2629 — Preconditions evaluation | 1 | MAJOR |
| java:S5961 — Too many assertions in test | 1 | MAJOR |
| java:S6397 — Single char in character class | 1 | MAJOR |
| java:S6204 — Stream.toList() | 4 | MAJOR |
| java:S6208 — Comma-separated switch labels | 2 | INFO |
| java:S3626 — Redundant jump statements | 4 | MINOR |
| java:S5857 — Character classes vs reluctant quantifiers | 5 | MINOR |
| java:S1452 — Generic wildcard return types | 2 | CRITICAL |
| java:S2093 — Try-with-resources | 1 | CRITICAL |

---

## 🎯 PLAN OF ACTION: 20% → 80% Improvement

### Этап 1: Критические баги (P0) — 1-2 дня
**Цель: Устранить риски падения production**

1. **java:S5998 (57 issues)** — Исправить regex, вызывающие StackOverflow
   - Файлы: `QueryParser.java`, `SubqueryParser.java`, `SqlLexer.java`
   - Решение: упростить сложные regex, использовать possessive quantifiers

2. **java:S2259 (13 issues)** — Null Pointer Dereference
   - Добавить проверки null перед использованием объектов
   - Использовать Optional где уместно

3. **java:S2583, S5850, S3599, S899, S5842 (11 issues)** — Остальные BUG
   - Исправить логику условий
   - Убрать Double Brace Initialization
   - Проверять возвращаемые значения

### Этап 2: Рефакторинг ключевых файлов (P1-A) — 3-5 дней
**Цель: Снизить сложность на 70%**

**Фокус на 3 файлах (68% всех проблем):**

1. **QueryParser.java (363 проблемы)**
   - Разбить brain methods (строки 974, 1114, 1971, 2678, 2793)
   - Выделить отдельные методы для:
     - Парсинга WHERE clause
     - Обработки JOIN
     - Работы с подзапросами
   - Цель: снизить cognitive complexity с 37-45 до <20

2. **SubqueryParser.java (255 проблем)**
   - Рефакторинг brain method (строка 1466)
   - Выделить парсинг разных типов подзапросов

3. **SelectQuery.java (51 проблема)**
   - **Критично:** execute() method (строка 78, complexity=59!)
   - Разделить на методы:
     - `executeWhereClause()`
     - `executeGroupBy()`
     - `executeOrderBy()`
     - `executeJoins()`

### Этап 3: Оптимизация регулярных выражений (P1-B) — 2-3 дня
**Цель: Ускорить парсинг SQL на 30-50%**

1. **java:S5869 (228 issues)** — Удалить дубликаты символов в классах
   - `[a-zA-Z0-9_]` → `\w`
   - `[0-9]` → `\d`
   - `[ \t\n\r\f]` → `\s`

2. **java:S6353 (119 issues)** — Использовать краткие формы
   - Автоматическая замена через IDE

3. **java:S5843 (13 issues)** — Упростить сложные regex
   - Разбить на несколько простых паттернов

### Этап 4: Архитектурные улучшения (P1-C) — 2-3 дня

1. **java:S1192 (34 issues)** — Вынести строковые литералы в константы
   - SQL keywords: SELECT, FROM, WHERE, JOIN
   - Сообщения об ошибках

2. **java:S107 (20 issues)** — Уменьшить количество параметров
   - Ввести parameter objects
   - Использовать Builder pattern

3. **java:S2447 (8 issues)** — Не возвращать null из Boolean методов
   - Возвращать false вместо null
   - Использовать Optional<Boolean>

4. **java:S1948 (8 issues)** — Serializable поля
   - Сделать поля transient или serializable
   - Важно для репликации

### Этап 5: Качество кода (P2) — 2-3 дня

1. **Заменить System.out.println на Logger (8 issues)**
2. **Использовать специфичные исключения (8 issues)**
3. Удалить неиспользуемые параметры, поля, методы (~40 issues)
4. Упростить обработку исключений (не логировать и не выбрасывать)

---

## 📊 Ожидаемый эффект

| Этап | Проблем исправлено | % от общего | Улучшение |
|---|---|---|---|
| Этап 1 (P0 BUG) | 81 | 9% | **Стабильность: устранение crash-багов** |
| Этап 2 (Рефакторинг) | 618 | 68% | **Поддерживаемость: -70% сложности** |
| Этап 3 (Regex) | 360* | 40%* | **Производительность парсера: +30-50%** |
| Этап 4 (Архитектура) | 70 | 8% | **Расширяемость: легче добавлять фичи** |
| Этап 5 (Quality) | 50 | 5% | **Читаемость: легче онбординг** |
| **ИТОГО (20% effort)** | **~180** | **~20%** | **80% improvement** |

*Примечание: некоторые проблемы пересекаются (один regex может нарушать несколько правил)*

---

## 🎯 Top 5 Critical Files to Fix

| Файл | Проблем | % от total | Приоритет | Действие |
|---|---|---|---|---|
| **diesel/QueryParser.java** | **363** | **40%** | **P0** | Рефакторинг 5 brain methods, исправление regex |
| **diesel/SubqueryParser.java** | **255** | **28%** | **P0** | Рефакторинг 1 brain method, упрощение логики |
| **diesel/SelectQuery.java** | **51** | **6%** | **P1** | Разделение execute() на подметоды |
| **diesel/Database.java** | **25** | **3%** | **P1** | Уменьшение brain method (139 строк) |
| **diesel/DeleteQuery.java** | **22** | **2%** | **P2** | Рефакторинг brain method |

**Концентрация:** 2 файла (QueryParser + SubqueryParser) = **68% всех проблем**

---

## 💡 Quick Wins (исправить за 1 час)

1. **Удалить unused imports** (24 issues) — IDEA: Code → Optimize Imports
2. **Заменить [0-9] на \d в regex** (119 issues) — Find/Replace по файлам
3. **Удалить закомментированный код** (3 issues) — ручное удаление
4. **Убрать public из JUnit5 тестов** (16 issues) — массовый рефакторинг

---

## ⚠️ Риски

1. **Рефакторинг парсера** может сломать существующие SQL-запросы
   - **Митигация:** покрыть тестами перед рефакторингом, использовать git blame

2. **Изменение regex** может изменить поведение парсера
   - **Митигация:** регрессионное тестирование на всех timing*.md тестах

3. **Разбиение brain methods** требует понимания бизнес-логики
   - **Митигация:** привлекать автора кода, делать code review

---

## 📈 Метрики успеха

После исправления 20% критических проблем:

| Метрика | Было | Целевое | 
|---|---|---|
| Всего проблем | 908 | **~450 (-50%)** |
| CRITICAL + MAJOR | 602 | **~250 (-60%)** |
| BUG | 81 | **0 (-100%)** |
| Cognitive Complexity (max) | 59 | **<25** |
| Brain methods | 12 | **0** |
| Файлы с >50 проблемами | 3 | **0** |

---

## 🔧 Инструменты

- **IntelliJ IDEA:** Inspect Code →批量修复
- **SonarLint:** real-time подсказки в IDE
- **jQAssistant:** анализ зависимостей
- **ArchUnit:** тесты на архитектуру (после рефакторинга)

---

*Сгенерировано на основе отчёта SonarQube от 2026-08-08*
