# SonarQube Analytics Report - DieselDB (Pareto Analysis)

**Дата анализа:** 2026-09-23
**Проект:** dieseldb
**Источник данных:** sonar8.md
**SonarQube Version:** 10.7.0.96327

---

## 📊 Executive Summary

| Метрика | Значение |
|---------|----------|
| Всего правил с проблемами | 30+ (детализировано топ-30) |
| Общее количество проблем | 964 |
| Lines of Code | 40,723 |
| Файлов | 177 |
| Классов | 380 |
| Функций | 3,291 |
| Duplicated Lines Density | 5.4% |
| Comment Lines Density | 18.5% |
| Test Coverage | 0.0% |
| Tests | 1,198 |
| Remediation Effort | 8,834 min (147.2 h) |
| Quality Gate | **ERROR** (789 new violations) |

---

## 🎯 Принцип Парето: 20% усилий дают 80% результата

### Топ-6 правил (20% от всех правил) для устранения 80% проблем

Исправление этих **6 правил из 30** устранит **327 проблем из 964 (33.9%)**:

| # | Правило | Проблем | Критичность | Описание |
|---|---------|---------|-------------|----------|
| 1 | java:S3776 | 100 | **CRITICAL** | Refactor method to reduce Cognitive Complexity from N to 15 |
| 2 | java:S1192 | 66 | **CRITICAL** | Define a constant instead of duplicating literal |
| 3 | java:S6213 | 53 | **MAJOR** | Rename variable/method (restricted identifier) |
| 4 | java:S108 | 41 | **MAJOR** | Remove or fill empty block of code |
| 5 | java:S1172 | 38 | **MAJOR** | Remove unused method parameter |
| 6 | java:S1168 | 29 | **MAJOR** | Return empty collection instead of null |

**Накопительный итог:** 327 проблем (33.9% от всех)

### Расширенный топ-15 правил (50% от всех правил) для устранения ~53% проблем

| # | Правило | Проблем | Критичность | Описание | Накопительный % |
|---|---------|---------|-------------|----------|-----------------|
| 1 | java:S3776 | 100 | **CRITICAL** | Reduce Cognitive Complexity | 10.4% |
| 2 | java:S1192 | 66 | **CRITICAL** | Define constant for duplicated literal | 17.2% |
| 3 | java:S6213 | 53 | **MAJOR** | Rename restricted identifier | 22.7% |
| 4 | java:S108 | 41 | **MAJOR** | Remove/fill empty block | 27.0% |
| 5 | java:S1172 | 38 | **MAJOR** | Remove unused method parameter | 30.9% |
| 6 | java:S1168 | 29 | **MAJOR** | Return empty collection instead of null | 33.9% |
| 7 | java:S1068 | 27 | **MAJOR** | Remove unused private field | 36.7% |
| 8 | java:S6201 | 24 | MINOR | Replace instanceof+cast with pattern matching | 39.2% |
| 9 | java:S1854 | 21 | **MAJOR** | Remove useless assignment | 41.4% |
| 10 | java:S135 | 21 | MINOR | Reduce break/continue in loop | 43.6% |
| 11 | java:S6885 | 19 | **MAJOR** | Use Math.clamp() instead of min/max | 45.5% |
| 12 | java:S6485 | 18 | **MAJOR** | Use HashMap.newHashMap() | 47.4% |
| 13 | java:S1481 | 17 | MINOR | Remove unused local variable | 49.2% |
| 14 | java:S1128 | 17 | MINOR | Remove unused import | 50.9% |
| 15 | java:S5869 | 17 | **MAJOR** | Remove duplicates in character class | 52.7% |

**Накопительный итог:** 508 проблем (52.7% от всех)

> **Примечание:** В отличие от sonar7, проблемы в sonar8 распределены более равномерно — ни одно правило не даёт «серебряной пули». Для достижения ~74% достаточно исправить все топ-30 правил (712 из 964). Максимальный эффект на единицу усилий дают правила #1-#7 (354 проблемы, 36.7%).

---

## 🚨 Баги по уровню критичности

### BLOCKER (5 проблем - 0.5%)

| Категория | Проблем | Описание | Приоритет |
|-----------|---------|----------|-----------|
| BUG | 2 | Новые дефекты уровня BLOCKER (в sonar7 было 0) | ⚠️ Ручной разбор в SonarQube |
| CODE_SMELL | 3 | Новые code smells уровня BLOCKER | ⚠️ Ручной разбор в SonarQube |

**Рекомендация:** Правила с BLOCKER-проблемами не представлены в детализации top-30 (каждое <10 проблем) — получить список можно напрямую из SonarQube (`severity=BLOCKER`). Исправлять первыми: это новые проблемы с момента sonar7 (0 → 5).

### CRITICAL (3 правила, 182 проблемы в top-30 / 205 всего - 21.3%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S3776 | 100 | Refactor method to reduce Cognitive Complexity | 🔥 **#1 в Pareto** |
| java:S1192 | 66 | Define constant instead of duplicating literal | 🔥 **#2 в Pareto** |
| java:S1948 | 16 | Make field transient or serializable | #18 в Pareto |

**Рекомендация:** Два главных CRITICAL-правила — одновременно #1 и #2 всего рейтинга Парето! Их исправление даёт 166 проблем (17.2%) сразу.

### MAJOR (12 правил, 350 проблем в top-30 / 542 всего - 56.2%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S6213 | 53 | Rename restricted identifier | 🔥 **#3 в Pareto** |
| java:S108 | 41 | Remove or fill empty block of code | 🔥 **#4 в Pareto** |
| java:S1172 | 38 | Remove unused method parameter | 🔥 **#5 в Pareto** |
| java:S1168 | 29 | Return empty collection instead of null | 🔥 **#6 в Pareto** |
| java:S1068 | 27 | Remove unused private field | 🔥 **#7 в Pareto** |
| java:S1854 | 21 | Remove useless assignment | #9 в Pareto |
| java:S6885 | 19 | Use Math.clamp() | #11 в Pareto |
| java:S6485 | 18 | Use HashMap.newHashMap() | #12 в Pareto |
| java:S5869 | 17 | Remove duplicates in character class | #15 в Pareto |
| java:S1066 | 15 | Merge collapsible if-else statements | #22 в Pareto |
| java:S1144 | 15 | Remove unused private method | #23 в Pareto |
| java:S107 | 13 | Reduce number of method parameters | #24 в Pareto |
| java:S1141 | 12 | Extract nested try block | #26 в Pareto |
| java:S1117 | 12 | Rename shadowed local variable | #27 в Pareto |
| java:S2259 | 10 | NullPointerException could be thrown (**BUG**) | #29 в Pareto |
| java:S127 | 10 | Don't assign to loop counter | #30 в Pareto |

**Рекомендация:** Практически все MAJOR-правила из top-30 уже упорядочены по Парето; начинать с java:S6213 и java:S108 — механические, легко автоматизируются (IDE rename / шаблоны).

### MINOR (10 правил, 169 проблем в top-30 / 167 всего - 17.5%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S6201 | 24 | Replace instanceof+cast with pattern matching | 🔥 **#8 в Pareto** |
| java:S135 | 21 | Reduce break/continue statements | #10 в Pareto |
| java:S1481 | 17 | Remove unused local variable | #13 в Pareto |
| java:S1128 | 17 | Remove unused import | #14 в Pareto |
| java:S1905 | 17 | Remove unnecessary cast | #16 в Pareto |
| java:S6126 | 16 | Replace List.of().stream() with Stream.of() | #17 в Pareto |
| java:S1123 | 15 | Add @Override annotation | #19 в Pareto |
| java:S6355 | 15 | Avoid zero-length lookbehind in regex | #20 в Pareto |
| java:S1133 | 15 | Review/deprecate usage (do-nothing annotations) | #21 в Pareto |
| java:S6208 | 12 | Inner classes should not be static… sort/order | #25 в Pareto |
| java:S3008 | 8 | Rename static field | вне top-30 |

**Рекомендация:** java:S6201, java:S135, java:S1481, java:S1128 входят в топ-15 Парето и исправляются одной настройкой IDE/автоформаттера — дешёвые победы.

### INFO (1 правило, 11 проблем в top-30 / 45 всего - 4.7%)

| Правило | Проблем | Описание | Приоритет |
|---------|---------|----------|-----------|
| java:S6541 | 11 | Brain Method detected - refactor | #28 в Pareto |

**Рекомендация:** java:S6541 коррелирует с java:S3776 — рефакторинг методов высокой сложности закрывает оба правила одновременно.

---

## 📈 Распределение проблем по типам

| Тип проблемы | Количество | % |
|--------------|------------|---|
| CODE_SMELL | 919 | 95.3% |
| BUG | 45 | 4.7% |
| VULNERABILITY | 0 | 0% |
| SECURITY_HOTSPOT | 0 (но 21 hotspot открыто, 0 reviewed) | 0% |

**BUG правила (критичные для стабильности, reliability rating = E):**
- java:S2259 (10 проблем) — возможный NullPointerException (`QueryParser.java:1898`, `Table.java:1808`, `JsonPathResolver.java:110`)
- 2 бага уровня BLOCKER (новые с момента sonar7)
- Остальные 33 бага распределены по правилам вне top-30 (<10 проблем каждое)

---

## 🏆 Топ-файлы по количеству проблем

| Файл | Проблем | % от всех |
|------|---------|-----------|
| diesel/SelectQuery.java | 76 | 7.9% |
| diesel/QueryParser.java | 67 | 6.9% |
| diesel/SubqueryParser.java | 41 | 4.3% |
| diesel/storage/JsonlRowReader.java | 26 | 2.7% |
| diesel/Table.java | 25 | 2.6% |
| diesel/storage/avro/AvroRangePartitioner.java | 23 | 2.4% |
| diesel/storage/avro/SnappyOptimizedCodec.java | 23 | 2.4% |
| diesel/storage/DelimitedIndexManager.java | 22 | 2.3% |
| diesel/storage/avro/AvroMetrics.java | 22 | 2.3% |
| diesel/storage/avro/AvroRowStorage.java | 19 | 2.0% |

**Вывод:** 3 файла (SelectQuery.java, QueryParser.java, SubqueryParser.java) содержат **184 проблемы (19.1%)**. Новый AVRO-модуль (`diesel/storage/avro/*`) суммарно даёт ~170+ проблем — при сканировании только main-кода он был бы главным источником долга.

---

## 🎯 План действий (Roadmap)

### Фаза 1: Быстрые победы (Week 1-2)
Исправить топ-3 правила Парето — **219 проблем (22.7%)**:
1. ✅ java:S3776 (100) — рефакторинг методов с высокой когнитивной сложностью (InsertQuery.java:88, QueryParser.java:1838/2217/3141, SelectQuery.java:1510)
2. ✅ java:S1192 (66) — вынести дублирующиеся литералы в константы (AvroMetrics.java, QueryParser.java:214)
3. ✅ java:S6213 (53) — переименовать restricted identifiers (AvroQueryExecutor.java, AvroAuditLogger.java)

### Фаза 2: Стабилизация (Week 3-4)
Исправить правила #4-7 — **+135 проблем (36.7% всего)**:
- java:S108 (41) — убрать/заполнить пустые блоки
- java:S1172 (38) — удалить неиспользуемые параметры
- java:S1168 (29) — возвращать пустые коллекции вместо null
- java:S1068 (27) — удалить неиспользуемые private поля (AvroMetrics.java)

### Фаза 3: Оптимизация (Week 5-6)
Исправить правила #8-15 — **+172 проблемы (52.7% всего)**:
- java:S6201, java:S1854, java:S135, java:S6885, java:S6485, java:S1481, java:S1128, java:S5869
  (большинство — автоматические fix'ы в IDE: pattern matching, Math.clamp, HashMap.newHashMap, unused imports/vars)

### Фаза 4: Качество кода (Week 7-8)
Исправить правила #16-23 — **+116 проблем (65.6% всего)**:
- java:S1905, java:S6126, java:S1948, java:S1123, java:S6355, java:S1133, java:S1066, java:S1144

### Фаза 5: Полировка + Баги (Week 9-10)
- Все BLOCKER-проблемы (5) и баги java:S2259 (10 NPE-рисков) — **вне очереди, но в приоритете по риску**
- Правила #24-30 и хвост — доведение до 100% (оставшиеся ~252 проблемы)
- Review 21 security hotspot (сейчас 0%)

---

## 💡 Ключевые инсайты

1. **Распределение стало «длинным хвостом»** — в sonar7 топ-6 правил давали 48.8% проблем, в sonar8 только 33.9%. Ни одно правило не является доминирующим; стратегия «исправить 1-2 правила» больше не работает
2. **Топ-2 правила дают 17.2%** — java:S3776 (100) и java:S1192 (66), оба CRITICAL — фокус рефакторинга именно здесь
3. **3 файла — 19.1% проблем** — SelectQuery.java (7.9%), QueryParser.java (6.9%), SubqueryParser.java (4.3%); их доля снизилась с 58.4% до 19.1% из-за роста проекта
4. **Новый AVRO-модуль — главный источник свежего долга** — ~10 файлов × 9-23 проблемы; правила S6213/S1192/S1068/S6201 сконцентрированы именно там
5. **BUG риски** — reliability rating E: 45 багов, включая 10 потенциальных NPE (java:S2259) и 2 BLOCKER
6. **Дешёвые автоматические победы** — S6201+S6885+S6485+S1128+S1481+S1905 (~113 проблем) исправляются массовыми intent-фиксами IDE за дни
7. **Код вырос на 167.8%** — ncloc 15,209 → 40,723; проблемы выросли на 82.2% (529 → 964), т.е. плотность проблем упала с 34.8 до 23.7 на 1K LOC — качество на строку улучшилось, абсолютный долг растёт
8. **Test Coverage = 0%** (при 1198 тестах — тесты не инструментированы JaCoCo) + Quality Gate ERROR (789 new violations) — критическая проблема процесса
9. **Улучшения с sonar7:** java:S5869 102→17, java:S5843 17→9, java:S2925 15→8, java:S3457 13→6 — прошлый Pareto-фокус по regex сработал

---

## 📋 Детальный список правил для исправления

### Топ-20 правил по приоритету (для достижения максимального результата при минимальных усилиях)

| # | Правило | Проблем | Severity | Type | Описание |
|---|---------|---------|----------|------|----------|
| 1 | java:S3776 | 100 | CRITICAL | CODE_SMELL | Reduce Cognitive Complexity to 15 |
| 2 | java:S1192 | 66 | CRITICAL | CODE_SMELL | Define constant for duplicated literal |
| 3 | java:S6213 | 53 | MAJOR | CODE_SMELL | Rename restricted identifier |
| 4 | java:S108 | 41 | MAJOR | CODE_SMELL | Remove/fill empty block |
| 5 | java:S1172 | 38 | MAJOR | CODE_SMELL | Remove unused parameter |
| 6 | java:S1168 | 29 | MAJOR | CODE_SMELL | Return empty collection |
| 7 | java:S1068 | 27 | MAJOR | CODE_SMELL | Remove unused private field |
| 8 | java:S6201 | 24 | MINOR | CODE_SMELL | instanceof pattern matching |
| 9 | java:S1854 | 21 | MAJOR | CODE_SMELL | Remove useless assignment |
| 10 | java:S135 | 21 | MINOR | CODE_SMELL | Reduce break/continue in loop |
| 11 | java:S6885 | 19 | MAJOR | CODE_SMELL | Use Math.clamp() |
| 12 | java:S6485 | 18 | MAJOR | CODE_SMELL | Use HashMap.newHashMap() |
| 13 | java:S1481 | 17 | MINOR | CODE_SMELL | Remove unused local variable |
| 14 | java:S1128 | 17 | MINOR | CODE_SMELL | Remove unused import |
| 15 | java:S5869 | 17 | MAJOR | CODE_SMELL | Remove duplicates in character class |
| 16 | java:S1905 | 17 | MINOR | CODE_SMELL | Remove unnecessary cast |
| 17 | java:S6126 | 16 | MINOR | CODE_SMELL | Replace List.of().stream() with Stream.of() |
| 18 | java:S1948 | 16 | CRITICAL | CODE_SMELL | Make field transient/serializable |
| 19 | java:S1123 | 15 | MINOR | CODE_SMELL | Add @Override annotation |
| 20 | java:S6355 | 15 | MINOR | CODE_SMELL | Avoid zero-length lookbehind |

**Итого топ-20:** 587 проблем (60.9% от всех 964)

---

## ➕ Сценарий «+20 проблем»: сколько закроется при расширении охвата на 20 дополнительных проблем

**Постановка:** базовый план (топ-20 Парето) закрывает 587 проблем (60.9%). Рассматриваем два варианта расширения плана **на +20 проблем**:
- **Вариант А (рекомендуемый):** берём из хвоста ровно 20 самых «жирных» проблем — top-3 правила (#21–#23: S1133+S1066+S1144 = 45 ≥ 20), закрывая их целиком;
- **Вариант Б (максимальный охват):** идём дальше по хвосту и набираем +20 проблем за счёт 10 мелких правил (#26–#35), чтобы закрыть как можно больше разных правил.

### Правила #21–#30 (детализированы в sonar8.md)

| # | Правило | Проблем | Severity | Накопительный % (от 964) |
|---|---------|---------|----------|--------------------------|
| 21 | java:S1133 | 15 | MINOR | 62.4% |
| 22 | java:S1066 | 15 | MAJOR | 64.0% |
| 23 | java:S1144 | 15 | MAJOR | 65.6% |
| 24 | java:S107 | 13 | MAJOR | 66.9% |
| 25 | java:S6208 | 12 | MINOR | 68.2% |
| 26 | java:S1141 | 12 | MAJOR | 69.4% |
| 27 | java:S1117 | 12 | MAJOR | 70.7% |
| 28 | java:S6541 | 11 | INFO | 71.8% |
| 29 | java:S2259 | 10 | MAJOR (**BUG**) | 72.9% |
| 30 | java:S127 | 10 | MAJOR | 73.9% |

**Итого #21–#30:** +125 проблем → накопительно **712 (73.9%)**

### Правила #31–#40 (из детализации sonar8.md + остаток BLOCKER-багов)

| # | Правило | Проблем | Severity | Накопительный % (от 964) |
|---|---------|---------|----------|--------------------------|
| 31 | java:S5843 | 9 | MAJOR | 74.8% |
| 32 | java:S2925 | 8 | MAJOR | 75.7% |
| 33 | java:S3008 | 8 | MINOR | 76.5% |
| 34 | java:S3457 | 6 | MAJOR | 77.2% |
| 35 | java:S106 | 5 | MAJOR | 77.7% |
| 36 | BLOCKER-дефекты (правила вне top-30) | 5 | **BLOCKER** | 78.2% |
| 37–40 | Хвост: 4 правила по ~4 проблемы (оценка) | ~16 | MINOR/INFO | ~79.9% |

**Итого #31–#40:** +57 проблем (из них ~41 точно по данным sonar8.md + 5 BLOCKER + ~16 оценка хвоста)

### 📌 Ответ: сколько закроется

| Сценарий | Правил | Проблем закрыто | Прирост | % от 964 |
|----------|--------|-----------------|---------|----------|
| База: топ-20 Парето | 20 | 587 | — | 60.9% |
| **А: база + 20 проблем из хвоста (#21–#23)** | 23 (+3) | **632** | **+45** | **65.6%** |
| **Б: база + 20 проблем мелкими правилами (#26–#35)** | 30 (+10) | **607** | **+20** | **63.0%** |
| Референс: полный top-30 | 30 | 712 | +125 | 73.9% |
| Референс: все детализированные #1–#35 | 35 | 748 | +161 | 77.6% |

**Расчёт варианта А:** 587 + S1133 (15) + S1066 (15) + S1144 (15) = **632 (65.6%)**. Три правила целиком закрывают норму «+20 проблем» с запасом (+45), остаток плана: 332 проблемы (34.4%).

**Расчёт варианта Б:** суммарный «выхлоп» правил #26–#35 = 12+12+11+10+10+9+8+8+6+5 = **91 проблема** — много больше нормы +20. Если строго ограничить план 20 проблемами (например, по 2 проблемы из каждого из 10 правил), закрыто **607 (63.0%)**, при этом охвачено на 10 правил больше (long-tail сокращается на ~14% по числу затронутых правил). Но если закрывать эти правила **целиком** (что эффективнее на практике — фиксится всё правило сразу), то план «топ-20 + 20 проблем из хвоста мелкими правилами» фактически превращается в топ-35: **748 проблем (77.6%), +161**.

**Вывод:** каждые **+20 проблем** к плану — это всего **+2.1 percentage point** покрытия (60.9% → 63.0–65.6%), что подтверждает «длинный хвост» sonar8: предельная отдача от расширения плана быстро убывает. Чтобы добраться до порога Парето ~80%, нужно закрыть не «+20 проблем», а **все 35 детализированных правил (+161 проблема → 748, 77.6%)** плюс BLOCKER-хвост. Оптимальная тактика — вариант А: 3 крупных правила дают в 2.25× больше эффекта, чем 10 мелких при тех же «+20 проблем».

**Дополнительные эффекты сценария +20:**
- Вариант А закрывает **java:S1066/S1144 (MAJOR, 30 проблем)** — дешёвые IDE-фиксы (merge collapsible if, remove unused private method)
- BUG java:S2259 (10 NPE-рисков) и 5 BLOCKER-проблем остаются за рамками обоих вариантов — их нужно добавлять в план **отдельно по риску, а не по объёму**
- Средний «выхлоп» на правило падает: топ-20 даёт 29.4 проблемы/правило, #21–#23 — 15.0, #26–#35 — 9.5; каждая следующая проблема обходится дороже по числу затрагиваемых файлов

---

*Отчет сгенерирован на основе данных sonar8.md*
