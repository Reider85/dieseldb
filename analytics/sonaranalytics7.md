# SonarQube Analytics Report - DieselDB (Pareto Analysis)

**Дата анализа:** 2026-09-06  
**Проект:** dieseldb  
**Источник данных:** sonar7.md  
**SonarQube Version:** 10.7.0.96327

---

## 📊 Executive Summary

| Метрика | Значение |
|---------|----------|
| Всего правил с проблемами | 30+ |
| Общее количество проблем | 529 |
| Lines of Code | 15,209 |
| Файлов | 71 |
| Классов | 120 |
| Функций | 915 |
| Duplicated Lines Density | 3.5% |
| Comment Lines Density | 14.8% |
| Test Coverage | 0.0% |

---

## 🎯 Принцип Парето: 20% усилий дают 80% результата

### Топ-6 правил (20% от всех правил) для устранения 80% проблем

Исправление этих **6 правил из 30** устранит **258 проблем из 529 (48.8%)**:

| # | Правило | Проблем | Критичность | Описание |
|---|---------|---------|-------------|----------|
| 1 | java:S5869 | 102 | MAJOR | Remove duplicates in character class (regex) |
| 2 | java:S3776 | 86 | **CRITICAL** | Refactor method to reduce Cognitive Complexity from N to 15 |
| 3 | java:S1192 | 25 | **CRITICAL** | Define a constant instead of duplicating literal |
| 4 | java:S5843 | 17 | **MAJOR** | Simplify regex to reduce complexity from N to 20 |
| 5 | java:S2925 | 15 | **MAJOR** | Remove Thread.sleep() calls |
| 6 | java:S3008 | 13 | MINOR | Rename static field to match regex '^[a-z][a-zA-Z0-9]*$' |

**Накопительный итог:** 258 проблем (48.8% от всех)

### Расширенный топ-15 правил (50% от всех правил) для устранения ~70% проблем

| # | Правило | Проблем | Критичность | Описание | Накопительный % |
|---|---------|---------|-------------|----------|-----------------|
| 1 | java:S5869 | 102 | MAJOR | Remove duplicates in character class | 19.3% |
| 2 | java:S3776 | 86 | **CRITICAL** | Reduce Cognitive Complexity | 35.5% |
| 3 | java:S1192 | 25 | **CRITICAL** | Define constant for duplicated literal | 40.3% |
| 4 | java:S5843 | 17 | **MAJOR** | Simplify regex complexity | 43.5% |
| 5 | java:S2925 | 15 | **MAJOR** | Remove Thread.sleep() | 46.3% |
| 6 | java:S3008 | 13 | MINOR | Rename static field | 48.8% |
| 7 | java:S3457 | 13 | **MAJOR** | Format strings used correctly | 51.2% |
| 8 | java:S1068 | 12 | **MAJOR** | Remove unused private field | 53.5% |
| 9 | java:S112 | 12 | **MAJOR** | Define dedicated exception | 55.8% |
| 10 | java:S6541 | 12 | INFO | Brain Method detected - refactor | 58.0% |
| 11 | java:S135 | 12 | MINOR | Reduce break/continue in loop | 60.3% |
| 12 | java:S3358 | 11 | **MAJOR** | Extract nested ternary operation | 62.4% |
| 13 | java:S2259 | 11 | **BUG** | NullPointerException could be thrown | 64.5% |
| 14 | java:S6213 | 10 | **MAJOR** | Rename variable/method (restricted identifier) | 66.3% |
| 15 | java:S1948 | 10 | **CRITICAL** | Make field transient or serializable | 68.2% |

**Накопительный итог:** 363 проблемы (68.6% от всех)

> **Примечание:** Для достижения 80% требуется исправить ~20 правил из 30. Топ-6 правил дают максимальный эффект при минимальных усилиях.

---

## 🚨 Баги по уровню критичности

### CRITICAL (4 правила, 133 проблемы - 25.1%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S3776 | 86 | Refactor method to reduce Cognitive Complexity | 🔥 **#2 в Pareto** |
| java:S1192 | 25 | Define constant instead of duplicating literal | 🔥 **#3 в Pareto** |
| java:S1948 | 10 | Make field private or transient | #15 в Pareto |
| java:S1452 | 7 | Avoid cycles in dependency graph | #22 |
| java:S127 | 10 | Don't assign to loop counter from within loop | #16 |

**Рекомендация:** Начать с java:S3776 и java:S1192 — они входят в топ-3 правил Парето!

### MAJOR (19 правил, 311 проблем - 58.8%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S5869 | 102 | Remove duplicates in character class | 🔥 **#1 в Pareto** |
| java:S5843 | 17 | Simplify regex complexity | 🔥 **#4 в Pareto** |
| java:S2925 | 15 | Remove Thread.sleep() | 🔥 **#5 в Pareto** |
| java:S3457 | 13 | Format strings used correctly | #7 в Pareto |
| java:S1068 | 12 | Remove unused private field | #8 в Pareto |
| java:S112 | 12 | Define dedicated exception | #9 в Pareto |
| java:S3358 | 11 | Extract nested ternary operation | #12 в Pareto |
| java:S2259 | 11 | NullPointerException could be thrown | #13 в Pareto (**BUG**) |
| java:S6213 | 10 | Rename restricted identifier | #14 в Pareto |
| java:S127 | 10 | Don't assign to loop counter | #16 |
| java:S1172 | 9 | Remove unused method parameter | #17 |
| java:S1168 | 9 | Return empty collection instead of null | #18 |
| java:S2629 | 9 | Avoid multiple negations in condition | #19 |
| java:S1141 | 8 | Extract nested try block | #20 |
| java:S108 | 5 | Remove or fill empty block of code | #28 |
| java:S106 | 5 | Replace System.out with logger | #30 |
| java:S6204 | 5 | Remove redundant type check | - |
| java:S6485 | 4 | Use HashMap.newHashMap() | - |
| java:S2139 | 4 | Log exception or rethrow with context | - |

**Рекомендация:** 9 из 19 MAJOR правил уже входят в топ-15 Парето!

### MINOR (5 правил, 68 проблем - 12.9%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S3008 | 13 | Rename static field | 🔥 **#6 в Pareto** |
| java:S135 | 12 | Reduce break/continue statements | #11 в Pareto |
| java:S1905 | 8 | Remove unnecessary cast | - |
| java:S1128 | 5 | Remove unused import | - |
| java:S1450 | 5 | Remove unused private field | - |

**Рекомендация:** java:S3008 и java:S135 входят в топ-15 Парето!

### INFO (2 правила, 17 проблем - 3.2%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S6541 | 12 | Brain Method detected - refactor | #10 в Pareto |
| java:S5786 | 5 | Remove 'public' modifier on interface methods | - |

**Рекомендация:** java:S6541 входит в топ-15 Парето по количеству проблем!

---

## 📈 Распределение проблем по типам

| Тип проблемы | Количество | % |
|--------------|------------|---|
| CODE_SMELL | 510 | 96.4% |
| BUG | 19 | 3.6% |
| VULNERABILITY | 0 | 0% |
| SECURITY_HOTSPOT | 0 | 0% |

**BUG правила (критичные для стабильности):**
- java:S2259 (11 проблем) — возможный NullPointerException
- Остальные BUG распределены по другим правилам

---

## 🏆 Топ-файлы по количеству проблем

| Файл | Проблем | % от всех |
|------|---------|-----------|
| diesel/QueryParser.java | 126 | 23.8% |
| diesel/SubqueryParser.java | 110 | 20.8% |
| diesel/SelectQuery.java | 73 | 13.8% |
| diesel/Table.java | 28 | 5.3% |
| diesel/Database.java | 21 | 4.0% |
| diesel/DatabaseServer.java | 20 | 3.8% |
| diesel/BTreeIndex.java | 16 | 3.0% |
| diesel/QueryOptimizer.java | 10 | 1.9% |
| diesel/AggregateFunctions.java | 9 | 1.7% |
| diesel/QueryExecutor.java | 8 | 1.5% |

**Вывод:** 3 файла (QueryParser.java, SubqueryParser.java, SelectQuery.java) содержат **58.4%** всех проблем!

---

## 🎯 План действий (Roadmap)

### Фаза 1: Быстрые победы (Week 1-2)
Исправить топ-3 правила Парето — **213 проблем (40.3%)**:
1. ✅ java:S5869 (102) — удалить дубликаты в символьных классах regex
2. ✅ java:S3776 (86) — рефакторинг методов с высокой когнитивной сложностью
3. ✅ java:S1192 (25) — вынести дублирующиеся литералы в константы

### Фаза 2: Стабилизация (Week 3-4)
Исправить правила #4-6 — **+45 проблем (48.8% всего)**:
- java:S5843 (17) — упростить сложные regex
- java:S2925 (15) — убрать Thread.sleep() из тестов
- java:S3008 (13) — переименовать static поля по convention

### Фаза 3: Оптимизация (Week 5-6)
Исправить правила #7-11 — **+60 проблем (60.3% всего)**:
- java:S3457, java:S1068, java:S112, java:S6541, java:S135

### Фаза 4: Качество кода (Week 7-8)
Исправить правила #12-20 — **+85 проблем (76.4% всего)**:
- java:S3358, java:S2259, java:S6213, java:S1948, java:S127, java:S1172, java:S1168, java:S2629, java:S1141

### Фаза 5: Полировка (Week 9-10)
Оставшиеся правила — **+126 проблем (100%)**

---

## 💡 Ключевые инсайты

1. **80% проблем集中在 40% правил** — фокус на топ-15 правил даст ~70% эффекта
2. **Топ-3 правила дают 40% результата** — java:S5869 (102), java:S3776 (86), java:S1192 (25)
3. **3 файла — 58.4% проблем** — QueryParser.java (23.8%), SubqueryParser.java (20.8%), SelectQuery.java (13.8%) требуют рефакторинга
4. **CRITICAL уже в топе** — java:S3776 (#2) и java:S1192 (#3) входят в Pareto-топ
5. **BUG риски** — java:S2259 (11 проблем NPE) требует внимания
6. **java:S5869 (102 проблемы)** — наибольшее количество, требует массового исправления regex в SubqueryParser.java:27
7. **Код вырос на 19.1%** — ncloc 12771 → 15209, проблемы выросли на 15.5% (458 → 529)
8. **Test Coverage = 0%** — критическая проблема качества

---

## 📋 Детальный список правил для исправления

### Топ-20 правил по приоритету (для достижения 80% результата)

| # | Правило | Проблем | Severity | Type | Описание |
|---|---------|---------|----------|------|----------|
| 1 | java:S5869 | 102 | MAJOR | CODE_SMELL | Remove duplicates in character class |
| 2 | java:S3776 | 86 | CRITICAL | CODE_SMELL | Reduce Cognitive Complexity to 15 |
| 3 | java:S1192 | 25 | CRITICAL | CODE_SMELL | Define constant for duplicated literal |
| 4 | java:S5843 | 17 | MAJOR | CODE_SMELL | Simplify regex complexity to 20 |
| 5 | java:S2925 | 15 | MAJOR | CODE_SMELL | Remove Thread.sleep() |
| 6 | java:S3008 | 13 | MINOR | CODE_SMELL | Rename static field |
| 7 | java:S3457 | 13 | MAJOR | CODE_SMELL | Use format strings correctly |
| 8 | java:S1068 | 12 | MAJOR | CODE_SMELL | Remove unused private field |
| 9 | java:S112 | 12 | MAJOR | CODE_SMELL | Define dedicated exception |
| 10 | java:S6541 | 12 | INFO | CODE_SMELL | Brain Method - refactor |
| 11 | java:S135 | 12 | MINOR | CODE_SMELL | Reduce break/continue in loop |
| 12 | java:S3358 | 11 | MAJOR | CODE_SMELL | Extract nested ternary |
| 13 | java:S2259 | 11 | MAJOR | BUG | NullPointerException risk |
| 14 | java:S6213 | 10 | MAJOR | CODE_SMELL | Rename restricted identifier |
| 15 | java:S1948 | 10 | CRITICAL | CODE_SMELL | Make field transient/serializable |
| 16 | java:S127 | 10 | MAJOR | CODE_SMELL | Don't modify loop counter |
| 17 | java:S1172 | 9 | MAJOR | CODE_SMELL | Remove unused parameter |
| 18 | java:S1168 | 9 | MAJOR | CODE_SMELL | Return empty collection |
| 19 | java:S2629 | 9 | MAJOR | CODE_SMELL | Avoid multiple negations |
| 20 | java:S1141 | 8 | MAJOR | CODE_SMELL | Extract nested try block |

---

*Отчет сгенерирован на основе данных sonar7.md*
