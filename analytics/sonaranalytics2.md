# SonarQube Analytics Report - DieselDB (Pareto Analysis)

**Дата анализа:** 2026-08-18  
**Проект:** dieseldb  
**Источник данных:** sonar2.md  
**SonarQube Version:** 10.7.0.96327

---

## 📊 Executive Summary

| Метрика | Значение |
|---------|----------|
| Всего правил с проблемами | 29 |
| Общее количество проблем | 813 |
| Lines of Code | 10,876 |
| Файлов | 50 |
| Классов | 82 |
| Функций | 604 |

---

## 🎯 Принцип Парето: 20% усилий дают 80% результата

### Топ-15 правил (52% от всех правил) для устранения 80% проблем

Исправление этих **15 правил из 29** устранит **652 проблемы из 813 (80.2%)**:

| # | Правило | Проблем | Критичность | Описание |
|---|---------|---------|-------------|----------|
| 1 | java:S6353 | 119 | MINOR | Use concise character class syntax '\w' instead of '[A-Za-z0-9_]' |
| 2 | java:S3776 | 92 | **CRITICAL** | Refactor method to reduce Cognitive Complexity from 25 to 15 |
| 3 | java:S6201 | 84 | MINOR | Replace instanceof check and cast with 'instanceof SelectQuery selectquery' |
| 4 | java:S5998 | 57 | **MAJOR** | Refactor repetition that can lead to stack overflow for large inputs |
| 5 | java:S1192 | 41 | **CRITICAL** | Define a constant instead of duplicating literal " does not exist" |
| 6 | java:S1128 | 36 | MINOR | Remove unused import 'diesel.ThreeValuedLogic.TRUE' |
| 7 | java:S135 | 30 | MINOR | Reduce break/continue statements in loop to at most one |
| 8 | java:S1172 | 28 | **MAJOR** | Remove unused method parameter "normalized" |
| 9 | java:S108 | 28 | **MAJOR** | Either remove or fill this block of code |
| 10 | java:S1874 | 28 | MINOR | Remove deprecated use of "setScale" |
| 11 | java:S3457 | 23 | **MAJOR** | First argument is not used |
| 12 | java:S1854 | 23 | **MAJOR** | Remove useless assignment to local variable "joins" |
| 13 | java:S107 | 22 | **MAJOR** | Constructor has 14 parameters (>7 authorized) |
| 14 | java:S5843 | 21 | **MAJOR** | Simplify regex to reduce complexity from 23 to 20 |
| 15 | java:S2925 | 20 | **MAJOR** | Remove use of "Thread.sleep()" |

**Накопительный итог:** 652 проблемы (80.2% от всех)

---

## 🚨 Баги по уровню критичности

### CRITICAL (4 правила, 149 проблем - 18.3%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S3776 | 92 | Refactor method to reduce Cognitive Complexity | 🔥 **#2 в Pareto** |
| java:S1192 | 41 | Define constant instead of duplicating literal | 🔥 **#5 в Pareto** |
| java:S1948 | 8 | Make "rowIndices" private or transient | #25 |
| java:S2447 | 8 | Null is returned but a "Boolean" is expected | #27 |

**Рекомендация:** Начать с java:S3776 и java:S1192 — они входят в топ-20% правил Парето!

---

### MAJOR (17 правил, 313 проблем - 38.5%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S5998 | 57 | Refactor repetition (stack overflow risk) | 🔥 **#4 в Pareto** |
| java:S1172 | 28 | Remove unused method parameter | 🔥 **#8 в Pareto** |
| java:S108 | 28 | Remove or fill empty block of code | 🔥 **#9 в Pareto** |
| java:S3457 | 23 | First argument is not used | 🔥 **#11 в Pareto** |
| java:S1854 | 23 | Remove useless assignment | 🔥 **#12 в Pareto** |
| java:S107 | 22 | Constructor has too many parameters | 🔥 **#13 в Pareto** |
| java:S5843 | 21 | Simplify regex complexity | 🔥 **#14 в Pareto** |
| java:S2925 | 20 | Remove Thread.sleep() | 🔥 **#15 в Pareto** |
| java:S2259 | 18 | NullPointerException could be thrown | #18 |
| java:S3358 | 13 | Extract nested ternary operation | #20 |
| java:S127 | 11 | Don't assign to loop counter from within loop | #21 |
| java:S6213 | 9 | Rename method (restricted identifier) | #22 |
| java:S112 | 8 | Define dedicated exception | #23 |
| java:S2139 | 8 | Log exception or rethrow with context | #24 |
| java:S106 | 8 | Replace System.out with logger | #26 |
| java:S6485 | 8 | Use HashMap.newHashMap() | #28 |
| java:S1141 | 8 | Extract nested try block | #29 |

**Рекомендация:** 8 из 17 MAJOR правил уже входят в топ-20% Парето!

---

### MINOR (6 правил, 316 проблем - 38.9%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S6353 | 119 | Use '\w' instead of '[A-Za-z0-9_]' | 🔥 **#1 в Pareto** |
| java:S6201 | 84 | Use pattern matching for instanceof | 🔥 **#3 в Pareto** |
| java:S1128 | 36 | Remove unused import | 🔥 **#6 в Pareto** |
| java:S135 | 30 | Reduce break/continue statements | 🔥 **#7 в Pareto** |
| java:S1874 | 28 | Remove deprecated setScale() | 🔥 **#10 в Pareto** |
| java:S1481 | 19 | Remove unused local variable | #19 |

**Рекомендация:** Хотя это MINOR, 5 из 6 правил входят в топ-20% Парето по количеству!

---

### INFO (2 правила, 35 проблем - 4.3%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S6541 | 19 | Brain Method detected - refactor | #17 |
| java:S5786 | 16 | Remove 'public' modifier | #16 |

**Рекомендация:** Низкий приоритет, не входят в топ-20% Парето.

---

## 📈 Распределение проблем по типам

| Тип проблемы | Количество | % |
|--------------|------------|---|
| CODE_SMELL | 778 | 95.7% |
| BUG | 35 | 4.3% |

**BUG правила (критичные для стабильности):**
- java:S5998 (57 проблем) — риск stack overflow
- java:S2259 (18 проблем) — возможный NullPointerException

---

## 🏆 Топ-файлы по количеству проблем

| Файл | Проблем | % от всех |
|------|---------|-----------|
| diesel/QueryParser.java | 396 | 48.7% |
| diesel/SubqueryParser.java | 277 | 34.1% |
| diesel/SelectQuery.java | 113 | 13.9% |
| diesel/Database.java | 45 | 5.5% |
| diesel/DeleteQuery.java | 27 | 3.3% |

**Вывод:** 3 файла (QueryParser, SubqueryParser, SelectQuery) содержат **96.7%** всех проблем!

---

## 🎯 План действий (Roadmap)

### Фаза 1: Быстрые победы (Week 1-2)
Исправить топ-5 правил Парето — **380 проблем (46.7%)**:
1. ✅ java:S6353 (119) — заменить [A-Za-z0-9_] на \w в regex
2. ✅ java:S3776 (92) — рефакторинг сложных методов
3. ✅ java:S6201 (84) — использовать pattern matching (Java 16+)
4. ✅ java:S5998 (57) — устранить рекурсивные паттерны
5. ✅ java:S1192 (41) — вынести дублирующиеся литералы в константы

### Фаза 2: Стабилизация (Week 3-4)
Исправить правила #6-10 — **+145 проблем (64.6% всего)**:
- java:S1128, java:S135, java:S1172, java:S108, java:S1874

### Фаза 3: Оптимизация (Week 5-6)
Исправить правила #11-15 — **+107 проблем (80.2% всего)**:
- java:S3457, java:S1854, java:S107, java:S5843, java:S2925

### Фаза 4: Полировка (Week 7-8)
Оставшиеся 14 правил — **161 проблема (100%)**

---

## 💡 Ключевые инсайты

1. **80% проблем集中在 20% правил** — фокус на топ-15 правил даст максимальный эффект
2. **MINOR ≠ неважно** — топ-1 и топ-3 проблемы имеют статус MINOR, но дают 203 исправления
3. **3 файла — 96.7% проблем** — QueryParser.java и SubqueryParser.java требуют рефакторинга
4. **CRITICAL уже в топе** — java:S3776 (#2) и java:S1192 (#5) входят в Pareto-топ
5. **BUG риски** — java:S5998 (stack overflow) и java:S2259 (NPE) требуют внимания

---

*Отчет сгенерирован на основе данных sonar2.md*
