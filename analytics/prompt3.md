# Промпты для DieselDB - Полная нумерация

> Статус на 06.09.2026: **4 из 20 выполнено** (Prompt 1, 3, 5, 112).  
> Ниже — полная нумерация всех промптов включая новые секции.  
> Дополнено 11.09.2026: добавлена Section 1a (Промпты 24-38) — улучшения CSV/TSV-хранилищ по результатам код-ревью; нумерация последующих промптов сдвинута на +15.
> Дополнено 11.09.2026 (2): добавлен Промпт 39 — сжатие CSV/TSV (ZSTD/LZ4/Snappy, настраиваемые кодек и уровень сжатия, вкл/выкл) в Section 1a; нумерация последующих промптов сдвинута на +1.
> Дополнено 11.09.2026 (3): расширена Section 1b (JSONL) — усилены Промпты 40-41 и добавлены Промпты 42-56: абстракции JSON-парсинга и маппинга типов, схема/вложенность, надёжность (NULL-семантика, диагностика ошибок, атомарность и режимы записи, режимы загрузки, детерминированная сериализация) и производительность (сжатие ZSTD/LZ4/Snappy через общий CompressionCodec, индексы со стабильными row-id, параллельное чтение, ленивые блоки с projection pushdown, тесты и бенчмарки); нумерация последующих промптов сдвинута на +15. Детальный анализ пробелов — в analysis.md.

---

## Section 0: Priority Retrospective Fixes (Промпты 1-20)

### Промпт 1: Исправление JOIN с OR в условии (OOM)
**Приоритет: CRITICAL**
**Статус: ✅ ВЫПОЛНЕН**

**Проблема:** JOIN с OR условием вызывает OutOfMemoryError.

**Решение:** Реализовано streaming выполнение JOIN с ограничением памяти.

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

### Промпт 3: GROUP BY unique values
**Приоритет: HIGH**
**Статус: ✅ ВЫПОЛНЕН**

**Проблема:** GROUP BY некорректно считал уникальные значения.

**Решение:** Исправлена логика агрегации и группировки.

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

### Промпт 5: Исправление IN + AND/OR
**Приоритет: CRITICAL**
**Статус: ✅ ВЫПОЛНЕН**

**Проблема:** IN в сочетании с AND/OR условиями работал некорректно.

**Решение:** Исправлена логика вычисления условий с трехзначной логикой.

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
**Статус: ✅ ВЫПОЛНЕН**

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

---

## Section 1: Рефакторинг RowBased хранилищ (Промпты 21-23)

### Промпт 21: Рефакторинг RowBased хранилищ - единый интерфейс
**Приоритет: CRITICAL (архитектура)**

**Проблема:** Новые RowBased хранилища требуют изменений во множестве мест кода.

**Задача:**
1. Создай интерфейс RowStorage с методами: open(), close(), scan(), insert(), update(), delete()
2. Реализуй абстрактный класс AbstractRowStorage с базовой функциональностью
3. Все существующие хранилища должны наследовать от AbstractRowStorage
4. Добавь фабрику StorageFactory для создания хранилищ по типу
5. Минимизируй изменения в Table и Database при добавлении новых хранилищ

**Файлы:** 
- diesel/storage/RowStorage.java (новый интерфейс)
- diesel/storage/AbstractRowStorage.java (новый абстрактный класс)
- diesel/storage/StorageFactory.java (новая фабрика)
- diesel/storage/InMemoryRowStorage.java (новая реализация)
- diesel/storage/FileBasedRowStorage.java (новая реализация)

**Критерии готовности:**
- Новое хранилище добавляется изменением в 1-2 местах
- Все тесты проходят с любым типом хранилища
- Конфигурация типа хранилища через diesel.properties

---

### Промпт 22: TSV хранилище - базовая реализация
**Приоритет: HIGH**

**Задача:**
1. Реализуй TsvRowStorage для хранения данных в TSV формате
2. Поддержка чтения/записи строк с разделителями табуляции
3. Обработка экранирования специальных символов
4. Потоковое чтение больших файлов

**Файлы:** 
- diesel/storage/TsvRowStorage.java (новый)
- diesel/storage/TsvRowReader.java (новый)
- diesel/storage/TsvRowWriter.java (новый)

---

### Промпт 23: TSV хранилище - индексация и поиск
**Приоритет: MEDIUM**

**Задача:**
1. Добавь поддержку индексов для TSV хранилища
2. Реализуй быстрый поиск по первичному ключу
3. Добавь кэширование часто используемых блоков данных
4. Поддержка параллельного чтения несколькими потоками

**Файлы:** diesel/storage/TsvIndexManager.java (новый)

---

## Section 1a: Улучшения CSV/TSV-хранилищ (Промпты 24-39)

> Добавлено 11.09.2026 по результатам код-ревью пакета `diesel/storage/` (CSV/TSV backends, DelimitedIndexManager).

### Промпт 24: CSV/TSV чтение - маппинг колонок по заголовку файла
**Приоритет: CRITICAL (тихая порча данных)**

**Проблема:** `CsvRowReader.readHeader()` и `TsvRowReader.readHeader()` парсят заголовок файла, но выбрасывают его. Данные раскладываются по порядку колонок из схемы (`columns`), а не по порядку из файла. Если в файле колонки переставлены или названы иначе — значения молча попадают не в свои колонки; при несовпадении количества колонок пишется только WARNING. Для СУБД это тихая порча данных.

**Задача:**
1. В `readHeader()` сохрани распарсенный заголовок файла (List<String> fileHeader) и строго сверяй его со схемой
2. Построй маппинг fileHeaderIndex -> schemaColumn и разбирай данные по нему, а не по позиции в схеме
3. При несовпадении имён заголовка со схемой бросай IOException с понятным сообщением (fail-fast); добавь конфиг-флаг `storage.header.mismatch.mode = fail | warn` (по умолчанию fail)
4. Обрабатывай UTF-8 BOM в начале заголовка (срезай \uFEFF), иначе первая колонка не матчится со схемой
5. Добавь тесты: перестановка колонок, лишняя колонка, отсутствующая колонка, BOM

**Файлы:**
- diesel/storage/CsvRowReader.java
- diesel/storage/TsvRowReader.java
- diesel/storage/DelimitedIndexManager.java (параллельный путь использует те же readers)

**Конфиг:** storage.header.mismatch.mode (default: fail)

**Критерии готовности:**
- Загрузка файла с переставленными колонками даёт корректные данные
- Файл с BOM загружается без потери первой колонки
- Тесты зелёные

---

### Промпт 25: Стабильные row-id вместо позиционных индексов в DelimitedIndexManager
**Приоритет: CRITICAL (некорректный поиск после insertAt)**

**Проблема:** `DelimitedIndexManager` хранит индексы как key -> rowIndex. `AbstractRowStorage.insertAt()` — основной путь вставки при кластерном PK (`Table.insertIntoClusteredPosition`) — добавляет новую запись, но НЕ сдвигает смещения существующих записей >= rowIndex. После вставки в середину `searchByPrimaryKey()`/`search()` возвращают неверные позиции строк. Для delete проблема «решена» полным rebuild (O(n log n) на каждый delete), для insertAt — нет.

**Задача:**
1. Введи стабильный идентификатор строки (rowId, long, монотонный) внутри DelimitedIndexManager: индексы key -> rowId, отдельная мапа rowId -> текущая позиция
2. insertAt: обнови только rowId -> позиция (сдвиг диапазона позиций), индексы key -> rowId не трогаются — O(log n) вместо rebuild
3. delete: пометь rowId удалённым и обнови позиции; полный rebuild только по достижении порога «мусора» (компакция)
4. Сохрани обратную совместимость публичного API: searchByPrimaryKey/search/rangeSearch по-прежнему возвращают позиции строк
5. Тесты: кластерная вставка в середину -> searchByPrimaryKey корректен; массовое удаление не деградирует до O(n^2)

**Файлы:**
- diesel/storage/DelimitedIndexManager.java
- diesel/storage/AbstractRowStorage.java

**Критерии готовности:**
- Баг воспроизводится тестом до фикса и зелёный после
- Бенчмарк массовых insertAt/delete не деградирует квадратично

---

### Промпт 26: Различение NULL и пустой строки (сентинел \N)
**Приоритет: HIGH (потеря данных при round-trip)**

**Проблема:** null записывается как пустое поле и читается обратно как null; пустая строка "" тоже пишется как пустое поле — различие теряется безвозвратно (`convertValue`: raw.isEmpty() -> null в обоих reader'ах).

**Задача:**
1. TSV: null -> `\N` (конвенция MySQL/ClickHouse), пустая строка -> пустое поле; `TsvRowReader.unescape` распознаёт `\N` как null, текстовое значение `\N` экранируется (`\\N`)
2. CSV: null -> незакавыченное пустое поле, пустая строка -> закавыченное `""` (RFC 4180 это различает); парсер должен сохранять признак закавыченности поля
3. Обратная совместимость: конфиг `storage.null.representation = legacy | sentinel` (по умолчанию legacy — старое поведение; sentinel — новое), задокументируй миграцию существующих файлов
4. Тесты round-trip: null, "", " \t ", значение "\N", значение с кавычками и переносами

**Файлы:**
- diesel/storage/TsvRowWriter.java, diesel/storage/TsvRowReader.java
- diesel/storage/CsvRowWriter.java, diesel/storage/CsvRowReader.java

**Конфиг:** storage.null.representation = legacy | sentinel (default: legacy)

---

### Промпт 27: Обработка ошибок загрузки и диагностика (файл:строка:колонка)
**Приоритет: HIGH**

**Проблема:** `loadCsv`/`loadTsv` ловят только IOException и глотают его (WARNING) — при обрыве посреди файла таблица молча остаётся усечённой (rows частично заполнены). NumberFormatException/DateTimeParseException из convertValue вообще не перехвачены и улетают наверх без указания файла, строки и колонки.

**Задача:**
1. Добавь в `DelimitedRowReader` счётчик текущей строки (getLineNumber())
2. Все ошибки конвертации оборачивай в DieselIOException с контекстом: `users.csv:142: column 'age': cannot parse "abc" as Integer`
3. Политика ошибок: конфиг `storage.load.error.mode = fail | skip_row | skip_value` (default: fail); skip-режимы логируют и продолжают
4. При IOException посреди чтения не оставляй частично загруженные строки: откат к предыдущему состоянию или rethrow как DieselIOException
5. Тесты: битый тип в середине файла, обрыв файла (усечённая последняя строка), политика skip_row

**Файлы:**
- diesel/storage/DelimitedRowReader.java
- diesel/storage/CsvRowReader.java, diesel/storage/TsvRowReader.java
- diesel/storage/CsvRowStorage.java, diesel/storage/TsvRowStorage.java

**Конфиг:** storage.load.error.mode = fail | skip_row | skip_value (default: fail)

---

### Промпт 28: Экранирование заголовка при записи
**Приоритет: MEDIUM**

**Проблема:** `CsvRowWriter.writeHeader()` использует `String.join(",", columns)` без экранирования; `TsvRowWriter.writeHeader()` — `String.join("\t", columns)`. Имя колонки с запятой/кавычкой/табом молча портит заголовок файла.

**Задача:**
1. Экранируй каждое имя колонки тем же `escapeValue()` (CSV — RFC 4180, TSV — backslash-escaping)
2. `readHeader` должен валидировать имена после unescape (стыкуется с Промптом 24)
3. Тест round-trip: колонки `price, rub` и `a\tb`

**Файлы:**
- diesel/storage/CsvRowWriter.java
- diesel/storage/TsvRowWriter.java

---

### Промпт 29: Детерминированная кодировка и переводы строк
**Приоритет: HIGH (переносимость между платформами)**

**Проблема:** `FileReader`/`FileWriter` используют платформенную кодировку по умолчанию, `newLine()` — платформенный сепаратор. Байты файлов недетерминированы между ОС/настройками JVM — ломаются чексуммы, репликация, побайтовые diff-тесты. UTF-8 BOM не срезается (обрабатывается в Промпте 24).

**Задача:**
1. Замени все FileReader/FileWriter в пакете diesel/storage на `Files.newBufferedReader`/`Files.newBufferedWriter` с явным StandardCharsets.UTF_8
2. Фиксируй перевод строки `\n` вместо `newLine()`
3. Добавь конфиг `storage.charset` (default: UTF-8), reader/writer берут кодировку из него
4. Тест: файл, записанный с одной настройкой `file.encoding`, читается идентично при другой; побайтовое сравнение с эталоном

**Файлы:**
- diesel/storage/*.java (все места с FileReader/FileWriter: CsvRowStorage, TsvRowStorage, DelimitedIndexManager, CsvIndexManager)

**Конфиг:** storage.charset (default: UTF-8)

---

### Промпт 30: Атомарная запись файлов + fsync
**Приоритет: CRITICAL (crash-устойчивость)**

**Проблема:** `saveCsv`/`saveTsv`/`saveSerialized` пишут в целевой файл через `new FileWriter(fileName, false)` — файл усекается ДО записи. Краш/kill -9 посреди сохранения = порванный .csv/.tsv/.table без возможности восстановиться. Для СУБД недопустимо.

**Задача:**
1. Паттерн temp+rename: запись в `<имя>.tmp`, затем `FileChannel.force(true)` (fsync), затем `Files.move(ATOMIC_MOVE, REPLACE_EXISTING)`
2. Один общий хелпер `diesel/storage/AtomicFileWriter` — используй его в CsvRowStorage, TsvRowStorage (saveCsv/saveTsv/saveSerialized) и в Table.writeLegacyCsv/saveToSerializedFile
3. При загрузке: если целевой файл отсутствует, но есть `.tmp` — логируй WARNING о незавершённой записи
4. Тест: имитация обрыва (IOException посреди записи) — целевой файл остаётся предыдущей валидной версией

**Файлы:**
- diesel/storage/AtomicFileWriter.java (новый)
- diesel/storage/CsvRowStorage.java, diesel/storage/TsvRowStorage.java
- diesel/Table.java

**Критерии готовности:**
- Краш во время save не приводит к потере предыдущей валидной версии файла
- Тест обрыва записи зелёный

---

### Промпт 31: Мелкие исправления корректности reader'ов
**Приоритет: LOW**

**Задача:**
1. Boolean: строгий парсинг значений true/false/1/0/yes/no/t/f вместо молчаливого `Boolean.parseBoolean` -> false для произвольных строк
2. Лишние поля в строке данных (больше, чем колонок): WARN с номером строки, не чаще одного раза на файл
3. `CsvRowStorage.insert`/`TsvRowStorage.insert`: убери лишний `rows.get(rows.size()-1)` — сохрани ссылку на скопированную мапу до add
4. Тесты на каждый пункт

**Файлы:**
- diesel/storage/CsvRowReader.java, diesel/storage/TsvRowReader.java
- diesel/storage/CsvRowStorage.java, diesel/storage/TsvRowStorage.java

---

### Промпт 32: Режим загрузки .table vs delimited-файла - отдельные настройки для CSV и TSV
**Приоритет: HIGH (двойной I/O при сохранении, мёртвый код)**

**Проблема:** `saveToFile()` в CsvRowStorage/TsvRowStorage пишет и delimited-файл (.csv/.tsv), и Java-сериализованный .table, но `loadFromFile` всегда читает только delimited-файл. Метод `loadSerialized` — мёртвый код. Итог: двойной расход I/O на каждое сохранение без выгоды.

**Решение: два режима загрузки с отдельной настройкой для каждого формата (csv и tsv настраиваются независимо).**

**Задача:**
1. Добавь в config.properties две независимые настройки:
   - `csv.load.mode` — режим загрузки для CSV-хранилищ
   - `tsv.load.mode` — режим загрузки для TSV-хранилищ
   Допустимые значения (одинаковые для обеих настроек):
   - `file` (синонимы: `csv`, `tsv`) — всегда загружать delimited-файл (.csv для csv.load.mode, .tsv для tsv.load.mode) с проверкой консистентности. Режим по умолчанию.
   - `auto_mtime` — при загрузке выбирать источник по mtime: если .table существует и свежее delimited-файла — быстрый путь через Java-сериализацию, иначе delimited-файл; в обоих случаях обязательна проверка консистентности
2. Проверка консистентности (обязательна в обоих режимах, после загрузки из любого источника):
   - formatVersion .table не выше CURRENT_FORMAT_VERSION
   - заголовок delimited-файла совпадает со схемой (имена и порядок колонок, см. Промпт 24)
   - количество строк соответствует метаданным .table
   - схема колонок/типов .table совпадает с текущей схемой таблицы
   - при провале проверки: отбросить результат быстрого пути, загрузить из delimited-файла, логировать WARNING
3. Реализуй общий хелпер в AbstractRowStorage: `resolveLoadSource(delimitedFile, tableFile, loadMode)` -> источник; CsvRowStorage.loadFromFile/loadTsv и TsvRowStorage.loadFromFile/loadTsv используют его
4. Поддержка переопределения через system property: `-Dcsv.load.mode=auto_mtime`, `-Dtsv.load.mode=file` (приоритет над config.properties)
5. `loadSerialized`: превратить из мёртвого кода в рабочий путь быстрой загрузки (режим auto_mtime); сравнение mtime с точностью не хуже миллисекунды, при равенстве mtime предпочитать delimited-файл
6. Опционально: в режиме `file` зеркальную запись .table при saveToFile можно отключить настройкой `csv.table.mirror = on | off` / `tsv.table.mirror = on | off` (по умолчанию on — совместимость); off экономит ~50% I/O при сохранении
7. Тесты: (a) режим file — .table игнорируется, даже если свежее; (b) auto_mtime — свежий .table грузится быстрым путём и проходит консистентность; (c) auto_mtime — протухший .table (delimited новее) -> загрузка из delimited; (d) битый .table -> автоматический fallback на delimited; (e) настройки csv.load.mode и tsv.load.mode независимы друг от друга

**Файлы:**
- diesel/storage/AbstractRowStorage.java
- diesel/storage/CsvRowStorage.java, diesel/storage/TsvRowStorage.java
- diesel/ConfigLoader.java

**Конфиг:**
- csv.load.mode = file | auto_mtime (default: file)
- tsv.load.mode = file | auto_mtime (default: file)
- csv.table.mirror = on | off, tsv.table.mirror = on | off (default: on, опционально)

**Критерии готовности:**
- Режим file: поведение как сейчас, но с обязательной проверкой консистентности
- Режим auto_mtime: быстрый путь из .table при свежем mtime, fallback на delimited при провале консистентности
- Настройки csv и tsv полностью независимы

---

### Промпт 33: Блочный кэш - реальный I/O или удаление слоя
**Приоритет: MEDIUM**

**Проблема:** данные уже полностью в памяти (rows в storage), поэтому `getBlock()` кэширует срезы `rows.subList` (копии ссылок), а `loadAllBlocksParallel()` «параллельно» копирует списки — I/O нет, слой абстракции без нагрузки (~150 строк поддерживаемого, но бесполезного кода).

**Задача (вариант A предпочтителен, он готовит дорогу к Parquet):**
A. Сделать кэш настоящим:
1. Храни byte-офсеты блоков файла (из пре-скана Промпта 34)
2. getBlock() читает блок с диска (FileChannel.position(offset)) и парсит только свои строки; LRU-вытеснение становится осмысленным
3. Опционально: ленивая загрузка строк по блокам вместо полного in-memory (`csv.lazy.blocks = true | false`, `tsv.lazy.blocks`)
B. Удалить слой:
1. Убери getBlock/loadAllBlocksParallel/block cache из DelimitedIndexManager
2. Оставь API-заглушки с логированием DEPRECATED, чтобы не ломать CsvIndexManagerTest

**Файлы:**
- diesel/storage/DelimitedIndexManager.java
- diesel/storage/CsvRowStorage.java, diesel/storage/TsvRowStorage.java

**Конфиг:** csv.lazy.blocks, tsv.lazy.blocks (для варианта A)

---

### Промпт 34: Параллельное чтение через byte-offset pre-scan
**Приоритет: HIGH (производительность загрузки больших файлов)**

**Проблема:** `ReadRangeTask` переоткрывает файл и пропускает range.start строк полным `next()` с построением HashMap — суммарно O(n*P/2) прочитанных строк. Плюс `countDataLines` (полный скан) и `mayContainMultiLineRows` для CSV (ещё один полный скан): два пре-скана + повторное чтение с начала файла в каждой партиции.

**Задача:**
1. Один пре-скан: прочитай файл как байты, запиши офсеты начала каждой строки; заодно детектируй multiline-строки CSV (незакрытые кавычки) — это заменяет и countDataLines, и mayContainMultiLineRows
2. Партиции = диапазоны офсетов (не строк): каждая задача открывает FileChannel, делает position(офсет первой строки своей партиции) и читает только до офсета конца партиции
3. Опционально: добавь в DelimitedRowReader метод `skip(long rows)` с дешёвой реализацией (парсинг без построения Map) — для случаев, когда офсеты недоступны
4. Кэшируй результат пре-скана до изменения файла (проверка mtime/size)
5. Property-тест: parallel-загрузка == последовательной по содержимому на файлах 10k+ строк (кавычки, переносы внутри полей, BOM, пустая последняя строка)

**Файлы:**
- diesel/storage/DelimitedIndexManager.java
- diesel/storage/DelimitedRowReader.java, CsvRowReader.java, TsvRowReader.java

**Критерии готовности:**
- Загрузка 1M-строчного файла быстрее последовательной на >= 2 ядрах
- Суммарный объём прочитанных байтов не превышает размер файла заметно

---

### Промпт 35: Устранение O(n^2) при delete (батчинг rebuild)
**Приоритет: HIGH (деградация массовых удалений)**

**Проблема:** `AbstractRowStorage.syncIndexDelete` вызывает полный rebuild индексов на КАЖДЫЙ delete: O(n log n) за операцию, O(n^2) на массовое удаление. Тот же rebuild дёргается из setRows.

**Задача:**
1. Введи deferred-режим: `beginBulkUpdate()`/`endBulkUpdate()` — внутри синхронные rebuild не выполняются, накапливается dirty-флаг
2. endBulkUpdate: один rebuild, если dirty
3. Подключи к путям массового изменения в Table (DELETE WHERE, compaction, setRows) и к copyForTransaction (вместо построчного insertAt-зеркалирования — один setRows + один rebuild)
4. Стратегическая альтернатива — Промпт 25 (row-id); этот промпт даёт быстрый эффект без смены структур
5. Тест: удаление 10k строк из 100k укладывается в бюджет времени; корректность поиска после endBulkUpdate

**Файлы:**
- diesel/storage/AbstractRowStorage.java, diesel/storage/DelimitedIndexManager.java
- diesel/Table.java

---

### Промпт 36: Компактное представление строк внутри storage (Object[])
**Приоритет: MEDIUM (память, дорога к колонковым форматам)**

**Проблема:** HashMap<String,Object> на строку — ~48+ байт на entry + автобоксинг; накладные расходы в 5-10 раз превышают полезные данные. Table.rows, storage.rows и DelimitedIndexManager.rows делят одни и те же ссылки, но Map-per-row всё равно доминирует в памяти.

**Задача:**
1. Внутри CsvRowStorage/TsvRowStorage храни строки как Object[] (позиция = индекс колонки), мапа column -> индекс — одна на таблицу
2. Map<String,Object> строй только на границе API (scan/insert/update) — внешние контракты RowStorage не меняются
3. Reader'ы наполняют Object[] напрямую (без промежуточного HashMap)
4. Измерь и задокументируй экономию памяти и скорость загрузки (JOL/JMH, до/после)
5. Поведение индексов и кэша не меняется

**Файлы:**
- diesel/storage/CsvRowStorage.java, diesel/storage/TsvRowStorage.java
- diesel/storage/CsvRowReader.java, diesel/storage/TsvRowReader.java

**Критерии готовности:**
- Память на таблицу снижается >= 3x (замер задокументирован)
- Все существующие тесты зелёные

---

### Промпт 37: Сопутствующие исправления инфраструктуры storage
**Приоритет: LOW**

**Задача:**
1. Замени java.util.logging на slf4j (logback уже в проекте) во всех классах пакета diesel/storage — сейчас два конкурирующих логгера
2. `Table.saveToFile` держит `tableLock.readLock()` — два параллельных вызова могут одновременно писать в один файл; используй writeLock для сохранения (или single-writer guard)
3. Задокументируй или исправь потокобезопасность структур DelimitedIndexManager (primaryKeyIndex/secondaryIndexes не synchronized): контракт «вызов под tableLock» либо явная синхронизация
4. Тест: два параллельных saveToFile не портят файл (в связке с Промптом 30 — атомарная запись)

**Файлы:**
- diesel/storage/*.java
- diesel/Table.java

---

### Промпт 38: Тесты негативных сценариев и property-тесты CSV/TSV
**Приоритет: HIGH (quality gate для хранилищ)**

**Проблема:** текущее покрытие базовое (round-trip, спецсимволы, null) — баги из Промптов 24-31 существующими тестами не ловятся.

**Задача:**
1. Заголовок: перестановка колонок, лишняя/отсутствующая колонка, BOM, несовпадение имён со схемой (Промпт 24)
2. Значения: null vs пустая строка vs пробельные (Промпт 26); невалидное значение типизированной колонки — сообщение с файлом/строкой/колонкой (Промпт 27)
3. Crash-устойчивость: обрыв записи (Промпт 30), .tmp после краша, битый .table с fallback (Промпт 32)
4. Индексы: searchByPrimaryKey после кластерной вставки в середину (Промпт 25); массовый delete (Промпт 35)
5. Property-тесты (jqwik или ручной random): parallel == sequential по содержимому на файлах 10k+ (Промпт 34); случайные значения (табы, переносы, обратные слэши, кавычки, запятые, юникод) round-trip
6. Файлы в других кодировках (KOI8-R, Windows-1251) — ожидаемое поведение: fail с понятной ошибкой (Промпт 29)

**Файлы:**
- src/test/java/diesel/CsvStorageAdvancedTest.java (новый)
- src/test/java/diesel/TsvStorageAdvancedTest.java (новый)

**Критерии готовности:**
- Каждый промпт 24-31 имеет минимум один закрепляющий тест
- mvn test -Ptest зелёный

---

### Промпт 39: Сжатие CSV/TSV файлов - абстракция кодеков и быстрые алгоритмы (ZSTD/LZ4/Snappy)
**Приоритет: HIGH (дисковый I/O и сеть - главное узкое место текстовых форматов)**

**Проблема:** CSV и TSV - текстовые форматы, они занимают много места, и узким местом почти всегда становится дисковый I/O или сеть. При этом reader'ы и writer'ы работают с файлами напрямую: единой точки включения компрессии нет, и без общей абстракции кодек пришлось бы вшивать в каждый класс хранения отдельно.

**Задача:**
1. Введи абстракцию сжатия для CSV и TSV: интерфейс CompressionCodec (wrapOutputStream/wrapInputStream + имя кодека) и фабрику CompressionFactory, разрешающую кодек по имени из настроек; подключи её в CsvRowStorage/TsvRowStorage (запись) и CsvRowReader/TsvRowReader (чтение) так, чтобы остальной код storage не знал о компрессии
2. Поддержи кодеки, которые быстро распаковываются на лету: ZSTD (оптимальный баланс скорость/степень сжатия - кодек по умолчанию), LZ4 и Snappy; избегай gzip для аналитических запросов - он требует много CPU и не поддерживает случайное чтение (random access), поэтому в списке допустимых значений его нет
3. Изменение формата сжатия в настройках: ключи csv.compression.codec и tsv.compression.codec = none | zstd | lz4 | snappy (CSV и TSV настраиваются независимо), переопределение через system property с приоритетом над config.properties (как в Промпте 32)
4. Изменение степени сжатия в настройках: csv.compression.level и tsv.compression.level; валидируй допустимый диапазон для каждого кодека (ZSTD 1-22; для LZ4/Snappy уровень игнорируется или маппируется на acceleration), значения по умолчанию - по кодеку
5. Возможность включить/отключить сжатие: значение none означает плоский текст как сейчас; reader обязан прозрачно читать и сжатые, и несжатые файлы - детект по расширению (.csv.zst/.tsv.zst, .csv.lz4, .csv.snappy и т.п.), при неоднозначности - по конфигу
6. Смена кодека или уровня влияет только на новые записи: существующие файлы читаются по своему фактическому формату, миграция старых данных не требуется
7. Тесты: round-trip для всех кодеков; чтение несжатого файла при включённом сжатии и наоборот; независимость настроек csv/tsv; спецсимволы, multiline-поля и null-сентинел \N (Промпт 26) корректны после сжатия/распаковки; замер размера файла и скорости записи/чтения до/после (результаты зафиксируй в README или KNOWN_LIMITATIONS)

**Файлы:**
- diesel/storage/CompressionCodec.java, diesel/storage/CompressionFactory.java (новые)
- diesel/storage/CsvRowStorage.java, diesel/storage/TsvRowStorage.java
- diesel/storage/DelimitedRowReader.java, CsvRowReader.java, TsvRowReader.java
- diesel/ConfigLoader.java
- pom.xml (зависимости: com.github.luben:zstd-jni, org.lz4:lz4-java, org.xerial.snappy:snappy-java - если ещё не подключены)

**Конфиг:**
- csv.compression.codec = none | zstd | lz4 | snappy (default: zstd); tsv.compression.codec - аналогично
- csv.compression.level, tsv.compression.level (default: по кодеку, для ZSTD - 3)
- Отключение сжатия: csv.compression.codec = none / tsv.compression.codec = none

**Критерии готовности:**
- Кодек, уровень и само сжатие включаются/меняются только через настройки, без изменения кода
- compression = none возвращает прежнее поведение (формат файла идентичен текущему)
- Сжатие даёт существенное сокращение размера (>= 3x на типичных данных) без деградации скорости последовательного чтения (I/O-bound сценарий ускоряется)
- Все существующие тесты зелёные, round-trip проходит для всех кодеков

---

## Section 1b: JSONL хранилище (Промпты 40-56)

> Дополнено 11.09.2026 (3): Промпты 40-41 переписаны в формате код-ревью (проблема, конфиги, критерии готовности) и добавлены Промпты 42-56 — полный паритет с улучшениями CSV/TSV (Промпты 24-39) плюс темы, уникальные для JSONL: абстракция streaming-парсера, маппинг типов JSON, вложенность/dot-path, append-режим записи, projection pushdown. Детальный разбор пробелов и таблица соответствия — в analysis.md.

### Промпт 40: JSONL хранилище - базовая реализация
**Приоритет: HIGH (новый формат хранения)**

**Проблема:** В наборе RowBased-хранилищ есть CSV и TSV, но нет JSONL — де-факто стандарта для логов, экспортов и стриминговых пайплайнов. Данные с вложенными структурами в CSV/TSV приходится сплющивать вручную, а текстовые форматы без экранирования теряют вложенность безвозвратно.

**Задача:**
1. Реализуй JsonlRowStorage для хранения данных в JSON Lines формате: одна строка таблицы = один JSON-объект на строке файла, кодировка UTF-8, разделитель `\n`
2. Реализуй JsonlRowReader/JsonlRowWriter: потоковое построчное чтение/запись, большие файлы без полной загрузки в память
3. Поля JSON-объекта мапятся в колонки схемы по имени (формат самописный, заголовка нет — контроль схемы вынесен в Промпт 44)
4. Поддержка вложенных структур и массивов: в базовой реализации вложенность сериализуется в JSON-колонку (правила flatten/json_column — в Промпте 45)
5. Валидация JSON при записи: невалидная строка не может попасть в файл; парсинг — только streaming-API (никакого DOM, см. Промпт 42)
6. Подключи хранилище к StorageFactory (Промпт 21): тип из конфигурации, без правок Table/Database

**Файлы:**
- diesel/storage/JsonlRowStorage.java (новый)
- diesel/storage/JsonlRowReader.java (новый)
- diesel/storage/JsonlRowWriter.java (новый)
- diesel/storage/StorageFactory.java (регистрация типа jsonl)

**Конфиг:** тип хранилища jsonl в общем списке StorageFactory (diesel.properties)

**Критерии готовности:**
- Round-trip (запись → чтение) сохраняет значения плоских колонок и вложенные структуры
- Файл 100k строк читается потоково, потребление памяти не растёт линейно с размером файла
- Таблица в JSONL-формате работает через общий API RowStorage без special-casing в Table

---

### Промпт 41: JSONL хранилище - расширенные возможности
**Приоритет: MEDIUM**

**Проблема:** Базовая реализация (Промпт 40) делает только плоский round-trip. Для аналитических сценариев нужны контроль типов по схеме, проекция полей и адресация вложенных полей из SQL. Сжатие из первоначальной редакции этого промпта (gzip, lz4) перенесено в Промпт 52: оно должно идти через общий CompressionCodec (Промпт 39) с кодеками ZSTD/LZ4/Snappy, а не вшиваться в JSONL отдельно.

**Задача:**
1. Добавь схему JSON с валидацией типов: значение каждого поля сверяется с типом колонки при чтении и записи; несоответствие — ошибка с координатами файл:строка:поле (правила конверсии — Промпт 43, диагностика — Промпт 48)
2. Реализуй проекцию полей: чтение только запрошенных полей из каждой JSON-строки без построения полной мапы — база для projection pushdown (Промпт 55)
3. Поддержка JSON Path для сложных запросов: адресация вложенных полей dot-нотацией (user.address.city) в предикатах и проекции; правила хранения вложенности — Промпт 45
4. JsonlSchemaManager: единый владелец схемы, валидации и sidecar-файла схемы (вывод и эволюция схемы — Промпт 44)

**Файлы:**
- diesel/storage/JsonlSchemaManager.java (новый)
- diesel/storage/JsonlRowReader.java
- diesel/storage/JsonlRowWriter.java

**Критерии готовности:**
- Значение не того типа в типизированной колонке роняет загрузку с диагностикой файл:строка:поле
- Проекция трёх полей из широкого JSONL парсит только нужные поля (замер времени/аллокаций зафиксирован)

---

### Промпт 42: JSONL - абстракция JSON-парсинга (единая точка streaming-парсера)
**Приоритет: CRITICAL (фундамент производительности и заменяемости)**

**Проблема:** Если каждый класс JSONL-хранилища вызовет JSON-библиотеку напрямую, смена библиотеки или оптимизация парсинга потребуют правок по всему пакету storage. DOM-парсинг (построение дерева всей строки) — типичная причина медленной загрузки и лишних аллокаций: JSONL достаточно потокового API уровня токенов. Вдобавок библиотеки различаются дефолтами: Gson в lenient-режиме молча принимает NaN/Infinity и некавыченные имена, Jackson — строже; без единой точки конфигурации поведение недетерминировано.

**Задача:**
1. Введи интерфейсы JsonStreamParser/JsonStreamGenerator (события: startObject, fieldName, scalarValue, endObject и т.п.) — единственное место в пакете storage, знающее о JSON-библиотеке
2. Реализация по умолчанию — Jackson streaming API (JsonParser/JsonGenerator); альтернативный бэкенд на Gson (JsonReader/JsonWriter) допустим за тем же интерфейсом, библиотека выбирается в одном месте
3. Запрети DOM-парсинг в reader'ах/writer'ах JSONL: код ниже абстракции не импортирует классы JSON-библиотек (архитектурный тест, например ArchUnit)
4. Вынеси общую конфигурацию в JsonParserConfig: max nesting depth (default: 64), max string length, lenient-режим выключен (NaN/Infinity/комментарии — ошибка), поведение на дубликатах ключей — по Промпту 43
5. Тесты: замена бэкенда (Jackson ↔ Gson) не меняет поведение reader'а/writer'а; превышение глубины вложенности и лимитов — ошибка с координатами строки

**Файлы:**
- diesel/storage/json/JsonStreamParser.java, JsonStreamGenerator.java, JsonParserConfig.java (новые)
- diesel/storage/json/JacksonStreamParser.java, JacksonStreamGenerator.java (новые)
- diesel/storage/JsonlRowReader.java, diesel/storage/JsonlRowWriter.java

**Критерии готовности:**
- В пакете diesel/storage нет прямых импортов классов JSON-библиотек вне подпакета json (архитектурный тест зелёный)
- Загрузка 1M строк не создаёт DOM-объектов на строку (замер аллокаций/profiler)

---

### Промпт 43: JSONL - маппинг типов JSON ↔ типы СУБД (JsonTypeMapper)
**Приоритет: CRITICAL (тихая порча данных при конверсии)**

**Проблема:** В JSON всего шесть типов значений (null, boolean, string, number, object, array), а схема таблицы — типизированные колонки. Без единой точки конверсии каждый reader конвертирует по-своему: 1.0 превращается то в Integer, то в Double; целые больше 2^53 молча теряют точность в double; строка "2026-01-01" в колонке DATE парсится или нет — лотерея.

**Задача:**
1. Введи JsonTypeMapper: конверсия JSON-значения в тип колонки схемы и обратно; единая точка для reader'а, writer'а и валидации схемы (стыкуется с Промптом 41)
2. Правила чисел: целое без точки → Integer/Long по диапазону; дробное → Double/BigDecimal по типу колонки; целые с абсолютным значением > 2^53 в DOUBLE-колонке запрещены (ошибка, не молчаливая потеря точности); научная нотация при чтении поддерживается
3. Правила строк: ISO-8601 строки конвертируются в Date/Time типы типизированной колонки; в strict-режиме не-ISO строка в дату — ошибка
4. Режим коэрции: конфиг jsonl.type.coercion = strict | lenient (default: strict) — strict запрещает "1" → 1 (строка в числе); lenient разрешает с WARNING
5. Дубликаты ключей в одном JSON-объекте: конфиг jsonl.duplicate.keys = fail | last_wins (default: fail); молчаливое поведение по умолчанию запрещено
6. NaN/Infinity: JSON их не поддерживает — при записи ошибка или текстовое кодирование по конфигу, при чтении lenient-значения отклоняются (стыкуется с Промптом 42)
7. Тесты round-trip: границы Integer/Long/Double, BigDecimal, 2^53±1, unicode-escape, вложенный null

**Файлы:**
- diesel/storage/json/JsonTypeMapper.java (новый)
- diesel/storage/JsonlRowReader.java, diesel/storage/JsonlRowWriter.java

**Конфиг:**
- jsonl.type.coercion = strict | lenient (default: strict)
- jsonl.duplicate.keys = fail | last_wins (default: fail)

**Критерии готовности:**
- Значение 9007199254740993 (2^53+1) в LONG-колонке читается точно, в DOUBLE-колонке — ошибка с внятным сообщением
- Round-trip граничных типов зелёный


---

### Промпт 44: JSONL - схема: вывод, sidecar-файл, эволюция (аналог маппинга заголовка из Промпта 24)
**Приоритет: CRITICAL (JSONL самописный: без контроля схемы набор полей «плывёт»)**

**Проблема:** У CSV/TSV есть заголовок, который строго сверяется со схемой (Промпт 24). У JSONL заголовка нет: каждая строка несёт собственный набор полей. Наивные реализации либо молча игнорируют лишние/недостающие поля, либо требуют жёсткого совпадения — и то и другое неприемлемо: один файл пишется с полем middle_name, второй без него, третий с опечаткой middel_name — и всё это молча попадает в таблицу по-разному.

**Задача:**
1. Режимы соответствия схеме: конфиг jsonl.schema.mode = strict | inferred | hybrid (default: hybrid)
   - strict: набор полей каждой строки обязан совпадать со схемой таблицы (лишнее поле — ошибка; отсутствующее — по политике Промпта 47)
   - inferred: схема выводится из данных при первой загрузке (пре-скан N строк или всего файла) и сохраняется в sidecar
   - hybrid: поля схемы обязательны и типизируются; лишние поля строк игнорируются с WARNING (один раз на файл, как в Промпте 31), но остаются доступны через dot-path (Промпт 45)
2. Sidecar-файл `<имя>.schema.json` рядом с данными: выведенная схема, версия формата, mtime/size данных на момент вывода; при загрузке сверяй актуальность и перевыводи схему при изменении данных
3. Эволюция схемы: новое поле в данных → расширение схемы (режимы hybrid/inferred) с обновлением sidecar; смена типа поля между строками (number → string) — ошибка fail-fast с файл:строка:поле, не молчаливая конверсия
4. Имя поля сопоставляется колонке по имени (не по позиции); опечатка в имени поля — ошибка с подсказкой ближайшего имени колонки (edit distance)
5. Тесты: строка с новым полем, строка без поля, опечатка в имени, смена типа между строками, перевывод sidecar после изменения данных, BOM (стыкуется с Промптом 48)

**Файлы:**
- diesel/storage/JsonlSchemaManager.java
- diesel/storage/json/JsonSchemaInference.java (новый)
- diesel/storage/JsonlRowStorage.java

**Конфиг:** jsonl.schema.mode = strict | inferred | hybrid (default: hybrid)

**Критерии готовности:**
- Файл, где у половины строк есть дополнительное поле, загружается корректно (hybrid), поле доступно в запросах через dot-path
- Смена типа поля между строками роняет загрузку с диагностикой файл:строка:поле

---

### Промпт 45: JSONL - вложенные структуры и массивы: правила хранения, dot-path, round-trip
**Приоритет: HIGH (главное преимущество JSONL и главная ловушка)**

**Проблема:** Реляционная таблица плоская, а JSON — нет. Без явных правил вложенный объект либо молча сериализуется строкой-JSON в колонку (нельзя фильтровать), либо разворачивается в колонки ad-hoc (структура теряется при записи обратно). Массивы усугубляют: [{"sku":"A","qty":2}] в CSV не выразить в принципе. Без round-trip-гарантий запись после чтения портит данные.

**Задача:**
1. Режимы хранения вложенности: конфиг jsonl.nested.mode = flatten | json_column (default: flatten)
   - flatten: поле user.address.city → колонка user.address.city (dot-нотация); конфликт имён (колонки user и user.address одновременно) — ошибка схемы
   - json_column: вложенный объект/массив хранится целиком в колонке с типом JSON (TEXT), доступ через JSON Path
2. Dot-пути в SQL-слое: WHERE/SELECT по user.address.city работают в обоих режимах (в json_column — вычисляемый JSON Path на чтении); проекция спускается в reader до нужных полей (стыкуется с Промптами 41 и 55)
3. Round-trip вложенности: чтение → запись сохраняет структуру; в flatten-режиме — реконструкция вложенных объектов из колонок; порядок ключей при реконструкции детерминирован (стыкуется с Промптом 51)
4. Массивы: массив объектов — JSON-колонка (авто-фоллбек в flatten-режиме); массив скаляров — JSON-колонка или колонки arr[0], arr[1] по конфигу jsonl.array.columns (зафиксируй выбор и задокументируй)
5. Тесты: глубина 3+ уровней, массив объектов, массив скаляров, конфликт имён колонок, round-trip обоих режимов, смешанные данные (часть полей плоская, часть вложенная)

**Файлы:**
- diesel/storage/json/JsonPathResolver.java (новый)
- diesel/storage/JsonlRowReader.java, diesel/storage/JsonlRowWriter.java
- diesel/storage/JsonlSchemaManager.java

**Конфиг:**
- jsonl.nested.mode = flatten | json_column (default: flatten)
- jsonl.array.columns = json | expand (default: json)

**Критерии готовности:**
- Запрос WHERE user.address.city = 'Москва' находит строки в обоих режимах
- Round-trip вложенной структуры (3 уровня + массив объектов) не теряет данные в обоих режимах

---

### Промпт 46: JSONL - интеграция в архитектуру storage и наследование общей инфраструктуры
**Приоритет: HIGH (не дублировать «долги», уже исправленные для CSV/TSV)**

**Проблема:** CSV/TSV прошли большой путь исправлений (Промпты 24-39): атомарная запись, стабильные row-id, deferred rebuild, компактное представление строк. JsonlRowStorage, написанный с нуля без оглядки на этот список, с высокой вероятностью повторяет те же ошибки — и каждый долг придётся чинить второй раз.

**Задача:**
1. Подключи JsonlRowStorage к StorageFactory (Промпт 21): создание по типу из конфигурации без изменений в Table/Database; все общие тесты контракта AbstractRowStorage проходят с JSONL-бэкендом без правок тестов
2. Атомарная запись через общий AtomicFileWriter (Промпт 30): temp+rename+fsync; никакого усечения целевого файла до успешной записи
3. Компактное представление строк Object[] (Промпт 36) с самого начала: HashMap<String,Object> только на границе API scan/insert/update
4. Отложенные перестройки индексов: beginBulkUpdate()/endBulkUpdate() (Промпт 35) подключены к путям массовых изменений
5. Инфраструктура: slf4j вместо java.util.logging (Промпт 37), UTF-8 и `\n` зафиксированы (Промпт 29), запись под tableLock.writeLock (Промпт 37)
6. Составь чек-лист «наследуемых долгов» в javadoc класса: каждый пункт ссылается на промпт из 24-39 и закрыт тестом

**Файлы:**
- diesel/storage/StorageFactory.java
- diesel/storage/JsonlRowStorage.java
- diesel/storage/AtomicFileWriter.java (переиспользование, без изменений)

**Критерии готовности:**
- Общие тесты контракта RowStorage проходят с JsonlRowStorage
- Краш посреди сохранения не портит предыдущую валидную версию .jsonl (тест обрыва записи)

---

### Промпт 47: JSONL - семантика NULL: null vs отсутствующее поле vs пустая строка
**Приоритет: HIGH (три разных состояния — потеря различий недопустима)**

**Проблема:** В JSONL null, отсутствие ключа и "" — три разных состояния, в отличие от CSV/TSV, где Промпт 26 разделяет только два. Наивный маппинг сводит все три к null: round-trip теряет информацию, а «поле отсутствует» и «поле = null» имеют разную бизнес-семантику (неизвестно vs заведомо пусто). Читатель, пишущий все поля подряд, и читатель, пропускающий пустые, разойдутся в результатах.

**Задача:**
1. Зафиксируй семантику записи: null → JSON null; пустая строка → ""; отсутствующее значение → ключ не пишется
2. Зафиксируй семантику чтения: null → null; "" → пустая строка; отсутствующий ключ → политика jsonl.missing.field = null | error | default (default: null); в strict-режиме схемы (Промпт 44) допустим error
3. Контракт scan(): отсутствие ключа передаётся явным отсутствием записи в Map (не null-значением); задокументируй контракт для Query Executor
4. Обратная совместимость: старые файлы, где null писался отсутствием ключа, читаются идентично (отсутствие → null по умолчанию); миграция не требуется
5. Тесты round-trip: null, "", отсутствие ключа, вложенный объект с null внутри (user.phone = null), массив с null-элементами, три состояния различимы после round-trip

**Файлы:**
- diesel/storage/JsonlRowReader.java, diesel/storage/JsonlRowWriter.java
- diesel/storage/json/JsonTypeMapper.java

**Конфиг:** jsonl.missing.field = null | error | default (default: null)

**Критерии готовности:**
- Три состояния различимы после round-trip (тест на структуру)
- Файл, где ключ есть не во всех строках, загружается без ошибок (default: null)

---

### Промпт 48: JSONL - диагностика ошибок загрузки и толерантность к мусору (файл:строка:JSON-путь)
**Приоритет: HIGH**

**Проблема:** Реальный JSONL содержит пустые строки в конце файла, BOM, битые строки посреди файла (обрыв записи), не-объектные строки (массив целиком, скаляр). Без явной политики загрузка падает с непонятным stack trace без координат или молча теряет строки — прямое повторение проблем Промпта 27 в новом формате.

**Задача:**
1. Добавь в JsonlRowReader счётчик текущей строки (getLineNumber()); все ошибки оборачивай в DieselIOException с координатами и путём поля в dot-нотации: `users.jsonl:142: field 'age': cannot parse "abc" as Integer`
2. Политика ошибок: конфиг jsonl.load.error.mode = fail | skip_row (default: fail); skip_row логирует координаты и причину, продолжает; итоговый WARNING с числом пропущенных строк
3. Толерантность: пустые и whitespace-only строки пропускаются; UTF-8 BOM в начале файла срезается (стыкуется с Промптом 44); строка, не являющаяся JSON-объектом (массив, скаляр), — ошибка с номером строки или skip_row по политике
4. Обрыв JSON посреди строки (незакрытая скобка в последней строке): трактуется как усечённая запись — fail с сообщением о возможном обрыве записи (стыкуется с Промптом 49)
5. Тесты: битая строка посреди файла, пустые строки, BOM, массив вместо объекта, обрыв последней строки, политика skip_row, диагностика содержит файл:строку:поле

**Файлы:**
- diesel/storage/JsonlRowReader.java
- diesel/storage/JsonlRowStorage.java

**Конфиг:** jsonl.load.error.mode = fail | skip_row (default: fail)

---

### Промпт 49: JSONL - режимы записи: полный rewrite, append, компакция
**Приоритет: CRITICAL (crash-устойчивость + производительность записи)**

**Проблема:** CsvRowStorage/TsvRowStorage перезаписывают файл целиком при каждом сохранении (после Промпта 30 — атомарно). Для JSONL-таблиц с миллионами строк полный rewrite на каждый insert — O(n) на операцию. Наивный append решает производительность, но рождает проблемы: незавершённая строка при краше, рассинхронизация с .table, удалённые строки «живут» в файле.

**Задача:**
1. Режим записи: конфиг jsonl.write.mode = rewrite | append (default: rewrite)
   - rewrite: полный атомарный rewrite через общий AtomicFileWriter (Промпт 30) — базовый режим, поведение идентично CSV/TSV
   - append: новые/изменённые строки дозаписываются в конец; удаления и перестановки накапливаются в дельта-метаданных и применяются при загрузке; компакция (полный rewrite) по порогу мусора или по команде
2. Атомарность append: дозапись батчами с fsync-барьером; при краше незавершённая последняя строка отбрасывается при загрузке (JSONL уникально устойчив: битая последняя строка = усечение, предыдущие строки целы)
3. Загрузка append-файла: применяет дельту (удаления/обновления) после чтения; консистентность с метаданными обязательна (стыкуется с Промптом 50)
4. Компакция: ручная команда и автоматическая по порогу jsonl.compaction.threshold (доля удалённых/обновлённых строк); компакция атомарна (temp+rename) и сбрасывает дельту
5. Тесты: краш посреди append → файл читается без последней строки, данные целы; массовые insert в append-режиме быстрее rewrite; компакция возвращает файл к каноническому виду и валидному состоянию

**Файлы:**
- diesel/storage/JsonlRowStorage.java
- diesel/storage/JsonlDeltaManager.java (новый — дельта удалений/обновлений)
- diesel/storage/AtomicFileWriter.java (переиспользование)

**Конфиг:**
- jsonl.write.mode = rewrite | append (default: rewrite)
- jsonl.compaction.threshold (default: 0.3 — доля мусора для автокомпакции)

**Критерии готовности:**
- 10k последовательных insert в append-режиме быстрее rewrite минимум на порядок
- Краш посреди дозаписки не теряет ранее записанные данные (тест обрыва)


---

### Промпт 50: JSONL - режим загрузки .table vs .jsonl (паритет с Промптом 32)
**Приоритет: HIGH (двойной I/O при сохранении, быстрый путь загрузки)**

**Проблема:** Для CSV/TSV Промпт 32 разделил режимы загрузки и устранил двойной I/O. Если JsonlRowStorage скопирует старое поведение (писать и .jsonl, и Java-сериализованный .table без настройки), проблемы воспроизведутся: двойной расход I/O на каждое сохранение и мёртвый код быстрого пути загрузки.

**Задача:**
1. Настройка jsonl.load.mode = file | auto_mtime (default: file), переопределение через system property `-Djsonl.load.mode=...` с приоритетом над config.properties — семантика идентична Промпту 32
2. Быстрый путь auto_mtime: свежий .table грузится через Java-сериализацию, протухший — из .jsonl; сравнение mtime с точностью не хуже миллисекунды, при равенстве предпочитай .jsonl
3. Обязательная проверка консистентности после загрузки из любого источника: formatVersion .table не выше CURRENT_FORMAT_VERSION, схема соответствует, число строк соответствует метаданным; при провале — fallback на .jsonl с WARNING
4. Опционально jsonl.table.mirror = on | off (default: on); off отключает зеркальную запись .table и экономит ~50% I/O при сохранении
5. Используй общий хелпер resolveLoadSource из AbstractRowStorage (Промпт 32) — не дублируй логику выбора источника
6. Тесты: пять сценариев из Промпта 32 (file игнорирует .table даже свежий; auto_mtime быстрый путь; протухший .table → fallback; битый .table → fallback; настройка независима от csv/tsv)

**Файлы:**
- diesel/storage/JsonlRowStorage.java
- diesel/storage/AbstractRowStorage.java (расширение хелпера, если требуется)
- diesel/ConfigLoader.java

**Конфиг:**
- jsonl.load.mode = file | auto_mtime (default: file)
- jsonl.table.mirror = on | off (default: on, опционально)

**Критерии готовности:**
- Режим file: поведение как сейчас, но с обязательной проверкой консистентности
- Режим auto_mtime: быстрый путь при свежем mtime, автоматический fallback при провале проверки

---

### Промпт 51: JSONL - детерминированная сериализация (побайтовая воспроизводимость)
**Приоритет: MEDIUM (переносимость, репликация, diff-тесты)**

**Проблема:** JSON-сериализаторы по умолчанию недетерминированы: порядок ключей зависит от хеш-мапы, не-ASCII символы пишутся то в UTF-8, то в \uXXXX-escape, числа сериализуются по-разному (1.0 vs 1, 1E2 vs 100). Побайтовые diff-тесты, чексуммы и репликация ломаются между запусками и платформами — аналог Промпта 29 для JSON.

**Задача:**
1. Порядок ключей: порядок колонок схемы (не хеш-порядок мапы); в hybrid-режиме (Промпт 44) лишние поля — в алфавитном порядке после полей схемы
2. Числа: целые без дробной части и экспоненты; double — shortest round-trip представление (Double.toString); BigDecimal — plain-строка; научная нотация при записи не используется (при чтении поддерживается)
3. Строки: UTF-8 без \uXXXX-escape для не-ASCII; экранирование только управляющих символов (< 0x20 — \uXXXX), кавычек и обратного слэша
4. Разделитель `\n` (не платформенный сепаратор); завершающий `\n` в конце файла; при записи BOM не пишется, при чтении срезается
5. Тест: побайтовое сравнение с эталоном между запусками JVM с разными file.encoding и на разных ОС; property-тест round-trip на случайных значениях (юникод, эмодзи, управляющие символы, числа на границах)

**Файлы:**
- diesel/storage/JsonlRowWriter.java
- diesel/storage/json/JacksonStreamGenerator.java (настройки сериализации)

**Критерии готовности:**
- Одинаковые данные → побайтово одинаковый файл на любой платформе
- Тест побайтового эталона зелёный

---

### Промпт 52: JSONL - сжатие через общий CompressionCodec (ZSTD/LZ4/Snappy)
**Приоритет: HIGH (дисковый I/O — главное узкое место текстовых форматов)**

**Проблема:** Промпт 39 дал общую абстракцию CompressionCodec для CSV/TSV, но первоначальная редакция Промпта 41 упоминала gzip — CPU-тяжёлый кодек без random access. JSONL ещё объёмнее CSV: имена полей дублируются в каждой строке (30-50% оверхеда), поэтому JSONL обязан подключиться к общему кодеку, а не строить отдельный путь.

**Задача:**
1. Подключи общий CompressionCodec/CompressionFactory (Промпт 39) в JsonlRowStorage (запись) и JsonlRowReader (чтение) — код хранения не знает о компрессии
2. Ключи jsonl.compression.codec = none | zstd | lz4 | snappy (default: zstd) и jsonl.compression.level — семантика идентична Промпту 39; gzip в списке допустимых значений отсутствует
3. Прозрачное чтение сжатых и несжатых файлов: детект по расширению (.jsonl.zst, .jsonl.lz4, .jsonl.snappy), при неоднозначности — по конфигу
4. Смена кодека/уровня влияет только на новые записи; существующие файлы читаются по своему фактическому формату, миграция не требуется
5. Тесты: round-trip всех кодеков; чтение несжатого файла при включённом сжатии и наоборот; спецсимволы, управляющие символы в строках (Промпт 51) и null-семантика (Промпт 47) корректны после сжатия/распаковки; замер размера и скорости записи/чтения до/после — зафиксируй в README или KNOWN_LIMITATIONS

**Файлы:**
- diesel/storage/JsonlRowStorage.java, JsonlRowReader.java, JsonlRowWriter.java
- diesel/storage/CompressionFactory.java (переиспользование; расширение, если нужны суффиксы расширений)

**Конфиг:**
- jsonl.compression.codec = none | zstd | lz4 | snappy (default: zstd)
- jsonl.compression.level (default: по кодеку, для ZSTD — 3)

**Критерии готовности:**
- Сжатие даёт >= 4x сокращение размера на типичных данных (дублирование имён полей сжимается лучше CSV)
- I/O-bound последовательное чтение с ZSTD не медленнее несжатого
- compression = none возвращает формат, идентичный текущему

---

### Промпт 53: JSONL - индексация: стабильные row-id и JsonlIndexManager
**Приоритет: HIGH (поиск без полного скана)**

**Проблема:** Без индексов каждый поиск по PK — полный парсинг всего файла. Копировать подход DelimitedIndexManager до Промпта 25 (key -> rowIndex) значит унаследовать баг с insertAt — надо сразу строить по целевой архитектуре. Отдельные сложности JSONL: поиск по вложенным полям (user.id) и append-режим записи (Промпт 49), смещающий позиции.

**Задача:**
1. JsonlIndexManager: PK-индекс, вторичные индексы, range search — сразу по архитектуре Промпта 25: key -> rowId (стабильный, монотонный), отдельная мапа rowId -> текущая позиция
2. Индексация по вложенным полям через dot-path (стыкуется с Промптом 45): индекс по user.id работает и в flatten, и в json_column режимах
3. Совместимость с append-режимом (Промпт 49): новые строки получают rowId и добавляются в индекс инкрементально без rebuild; удаления — через дельту
4. Persist индекса: sidecar-файл `<имя>.idx` рядом с данными; инвалидация по mtime/size данных; rebuild при несовпадении; sidecar пишется атомарно (Промпт 30)
5. Deferred rebuild: массовые изменения через beginBulkUpdate()/endBulkUpdate() (Промпт 35)
6. Тесты: поиск после кластерной вставки в середину; массовый delete без деградации до O(n^2); индекс по вложенному полю; persist/инвалидация индекса; краш посреди записи sidecar не ломает данные

**Файлы:**
- diesel/storage/JsonlIndexManager.java (новый)
- diesel/storage/JsonlRowStorage.java

**Критерии готовности:**
- searchByPrimaryKey корректен после вставки в середину (тест до/после фикса аналогичен Промпту 25)
- Массовые insertAt/delete не деградируют квадратично

---

### Промпт 54: JSONL - параллельное чтение через byte-offset pre-scan
**Приоритет: HIGH (производительность загрузки больших файлов)**

**Проблема:** Параллельная загрузка CSV/TSV (Промпт 34) усложнена multiline-полями: пре-скан обязан отслеживать незакрытые кавычки. JSONL структурно проще — строка данных = строка файла, вложенность заперта внутри строки, — и это надо использовать: сплит по байтовым офсетам начала строк без анализа содержимого полей.

**Задача:**
1. Пре-скан: один проход по байтам файла, сбор офсетов начала каждой строки; заодно детект пустых/битых строк и BOM; кэш результата пре-скана до изменения файла (проверка mtime/size) — как в Промпте 34
2. Партиции = диапазоны офсетов: каждая задача открывает FileChannel, позиционируется на офсет первой строки своей партиции и читает до офсета конца партиции; строки не пересекают границы партиций (в отличие от CSV)
3. Сжатые файлы (Промпт 52): параллельное чтение через независимые кадры/блоки ZSTD, где возможно, иначе фоллбек на последовательное чтение — зафиксируй поведение и задокументируй
4. Сборка результатов партиций в порядке файла: детерминированный порядок строк не зависит от числа потоков
5. Property-тест: parallel == sequential по содержимому на файлах 1M+ строк (юникод, пустые строки, BOM, сжатые и несжатые варианты)

**Файлы:**
- diesel/storage/JsonlParallelLoader.java (новый)
- diesel/storage/JsonlIndexManager.java
- diesel/storage/JsonlRowReader.java

**Критерии готовности:**
- Загрузка 1M-строчного файла быстрее последовательной на >= 2 ядрах
- Порядок строк результата детерминирован при любом числе потоков

---

### Промпт 55: JSONL - ленивая загрузка по блокам и projection pushdown
**Приоритет: MEDIUM (аналитические запросы по широким JSONL)**

**Проблема:** Полная загрузка всех полей всех строк расточительна, когда запрос берёт 3 поля из 40. Для CSV проекция на уровне чтения бессмысленна (строку надо распарсить целиком, чтобы дойти до нужной колонки), но JSON-структура позволяет пропустить значения чужих полей почти без аллокаций. Урок Промпта 33 обязателен: слой кэша без реального I/O бесполезен — блоки JSONL должны быть настоящими байтовыми диапазонами.

**Задача:**
1. Блоки по офсетам (из пре-скана Промпта 54): блок = N строк (jsonl.block.rows, default: 1000); getBlock читает с диска только свой диапазон и парсит его; LRU-вытеснение становится осмысленным
2. Projection pushdown: в reader передаётся множество нужных путей (колонки + dot-пути из запроса); парсер пропускает значения чужих полей на уровне токенов (стыкуется с Промптами 41 и 45); замерь экономию времени и аллокаций
3. Режим jsonl.lazy.blocks = true | false (default: false): true — строки грузятся блоками по требованию; схема колонок известна из sidecar (Промпт 44); инвалидация блоков при дозаписи и компакции (Промпт 49) обязательна
4. Опционально: статистика блоков (min/max по типизированным колонкам) — каркас для будущих zone map и predicate pushdown
5. Тесты: ленивый режим возвращает те же результаты, что и полный; projection на широких строках в 2-5 раз быстрее полной загрузки; инвалидация блоков после append/compaction не выдаёт устаревшие данные

**Файлы:**
- diesel/storage/JsonlBlockManager.java (новый)
- diesel/storage/JsonlRowReader.java
- diesel/storage/JsonlIndexManager.java

**Конфиг:**
- jsonl.lazy.blocks = true | false (default: false)
- jsonl.block.rows (default: 1000)

**Критерии готовности:**
- SELECT трёх колонок из 40-колоночного JSONL быстрее полной загрузки >= 2x
- Ленивый и полный режимы дают идентичные результаты

---

### Промпт 56: JSONL - тесты негативных сценариев, property-тесты и бенчмарки (quality gate)
**Приоритет: HIGH (quality gate для JSONL-хранилища)**

**Проблема:** Как и для CSV/TSV (Промпт 38), базовое покрытие (round-trip) не ловит баги перечисленных промптов. JSONL добавляет собственные классы багов: вложенность, дубликаты ключей, вариативность полей между строками, три состояния null, недетерминированная сериализация.

**Задача:**
1. Негативные сценарии: битая строка, не-объектная строка, дубликаты ключей, целые > 2^53, смена типа между строками, обрыв файла (Промпты 43, 44, 48)
2. NULL-семантика: null / "" / отсутствие ключа различимы после round-trip (Промпт 47)
3. Вложенность: round-trip 3+ уровней и массивов в обоих режимах (Промпт 45); dot-path запросы по вложенным полям
4. Property-тесты (jqwik или ручной random): parallel == sequential (Промпт 54); случайные значения (юникод, эмодзи, управляющие символы, числа на границах, вложенные структуры) round-trip; побайтовая детерминированность записи (Промпт 51)
5. Crash-устойчивость: обрыв rewrite (temp+rename, Промпт 49), обрыв append, .tmp после краша, битый .table с fallback (Промпт 50)
6. Бенчмарки (JMH или замер в тесте): загрузка 1M строк CSV vs JSONL vs JSONL+zstd; память на строку; projection на широких строках; append vs rewrite на 10k вставках — результаты зафиксируй в README или KNOWN_LIMITATIONS

**Файлы:**
- src/test/java/diesel/JsonlStorageAdvancedTest.java (новый)
- src/test/java/diesel/JsonlPropertyTest.java (новый)

**Критерии готовности:**
- Каждый промпт 40-55 имеет минимум один закрепляющий тест
- mvn test -Ptest зелёный
- Бенчмарки задокументированы

---

## Section 2: AVRO хранилище (Промпты 57-96)

### Промпт 57: AVRO хранилище - базовая настройка проекта
**Приоритет: HIGH**

**Задача:**
1. Добавь зависимость org.apache.avro:avro в pom.xml
2. Настрой Maven plugin для генерации классов из .avsc схем
3. Создай базовую структуру пакетов diesel/storage/avro/
4. Добавь конфигурацию paths к AVRO файлам в diesel.properties

**Файлы:** 
- pom.xml (обновление)
- diesel.properties (добавить avro.* настройки)
- diesel/storage/avro/ (новый пакет)

---

### Промпт 58: AVRO хранилище - схема и типы данных
**Приоритет: HIGH**

**Задача:**
1. Определи映射 SQL типов DieselDB в типы AVRO
2. Создай AvroSchemaManager для управления схемами
3. Реализуй конвертацию Schema -> org.apache.avro.Schema
4. Поддержка complex типов: records, arrays, maps, unions, enums

**Файлы:** 
- diesel/storage/avro/AvroSchemaManager.java (новый)
- diesel/storage/avro/AvroTypeMapper.java (новый)
- src/main/resources/avro/schemas/ (новая директория)

---

### Промпт 59: AVRO хранилище - AvroRowStorage базовый класс
**Приоритет: HIGH**

**Задача:**
1. Реализуй AvroRowStorage extends AbstractRowStorage
2. Методы: open(), close(), scan(), insert(), update(), delete()
3. Интеграция с AvroSchemaManager
4. Базовая сериализация/десериализация Row <-> GenericRecord

**Файлы:** diesel/storage/avro/AvroRowStorage.java (новый)

---

### Промпт 60: AVRO хранилище - запись данных
**Приоритет: HIGH**

**Задача:**
1. Реализуй AvroDataFileWriter для эффективной записи
2. Поддержка sync markers для восстановления
3. Буферизация записи для производительности
4. Обработка ошибок записи с откатом

**Файлы:** 
- diesel/storage/avro/AvroDataFileWriter.java (новый)
- diesel/storage/avro/AvroWriteBuffer.java (новый)

---

### Промпт 61: AVRO хранилище - чтение данных
**Приоритет: HIGH**

**Задача:**
1. Реализуй AvroDataFileReader для чтения
2. Поддержка seek по sync markers
3. Потоковое чтение больших файлов
4. Projection pushdown (чтение только нужных полей)

**Файлы:** 
- diesel/storage/avro/AvroDataFileReader.java (новый)
- diesel/storage/avro/AvroReadIterator.java (новый)

---

### Промпт 62: AVRO сжатие - настройка кодеков
**Приоритет: HIGH**

**Задача:**
1. Добавь поддержку codec: null, deflate, snappy, zstandard, bzip2
2. Конфигурация уровня сжатия через diesel.properties
3. Бенчмарк различных кодеков на тестовых данных
4. Авто-выбор кодека по размеру данных

**Файлы:** 
- diesel/storage/avro/AvroCompressionConfig.java (новый)
- diesel/storage/avro/AvroCodecFactory.java (новый)
- diesel.properties (добавить avro.compression.codec)

---

### Промпт 63: AVRO сжатие - ZStandard кодек
**Приоритет: MEDIUM**

**Задача:**
1. Добавь зависимость com.github.luben:zstd-jni
2. Реализуй ZStandardCodec для Avro
3. Настройка уровня сжатия (1-22)
4. Бенчмарк ZSTD vs Snappy vs Deflate

**Файлы:** diesel/storage/avro/ZStandardCodec.java (новый)

---

### Промпт 64: AVRO сжатие - Snappy оптимизация
**Приоритет: MEDIUM**

**Задача:**
1. Добавь зависимость org.xerial.snappy:snappy-java
2. Оптимизируй буферы для Snappy
3. Benchmark на разных размерах блоков
4. Рекомендации по выбору размера блока

**Файлы:** diesel/storage/avro/SnappyOptimizedCodec.java (новый)

---

### Промпт 65: AVRO сжатие - Deflate уровни
**Приоритет: LOW**

**Задача:**
1. Настройка уровней сжатия Deflate (1-9)
2. Trade-off между скоростью и степенью сжатия
3. Адаптивный выбор уровня по типу данных
4. Кэширование compressor/decompressor

**Файлы:** diesel/storage/avro/DeflateLevelConfig.java (новый)

---

### Промпт 66: AVRO сжатие - BZip2 для холодных данных
**Приоритет: LOW**

**Задача:**
1. Добавь поддержку BZip2 codec
2. Используй для архивных/холодных данных
3. Настройка blockSize (100-900 KB)
4. Интеграция с storage tiering

**Файлы:** diesel/storage/avro/BZip2Codec.java (новый)

---

### Промпт 67: AVRO сжатие - адаптивное сжатие
**Приоритет: MEDIUM**

**Задача:**
1. Мониторинг compression ratio в runtime
2. Авто-переключение кодека при изменении паттерна данных
3. Метрики: compressed size, compression time, decompression time
4. Recommendations engine для выбора кодека

**Файлы:** diesel/storage/avro/AdaptiveCompressionManager.java (новый)

---

### Промпт 68: AVRO блочная структура - настройка блока
**Приоритет: HIGH**

**Задача:**
1. Конфигурация размера блока (по умолчанию 64KB)
2. Оптимальный размер блока для разных workload
3. Sync marker каждые N байт
4. Метаданные блока: count, size, checksum

**Файлы:** 
- diesel/storage/avro/AvroBlockConfig.java (новый)
- diesel/storage/avro/AvroBlockManager.java (новый)

---

### Промпт 69: AVRO блочная структура - параллельное чтение
**Приоритет: MEDIUM**

**Задача:**
1. Чтение нескольких блоков параллельно
2. Распределение блоков между потоками
3. Синхронизация результатов
4. Load balancing между блоками разного размера

**Файлы:** diesel/storage/avro/AvroParallelReader.java (новый)

---

### Промпт 70: AVRO блочная структура - split для MapReduce
**Приоритет: LOW**

**Задача:**
1. Поддержка AvroInputFormat для Hadoop/MapReduce
2. Correct splitting по sync markers
3. Интеграция с Spark через AvroFileFormat
4. Примеры использования в документации

**Файлы:** diesel/storage/avro/AvroInputFormatCompat.java (новый)

---

### Промпт 71: AVRO схема - эволюция схемы
**Приоритет: HIGH**

**Задача:**
1. Поддержка backward compatibility
2. Поддержка forward compatibility
3. full compatibility проверки
4. Версионирование схем

**Файлы:** 
- diesel/storage/avro/SchemaEvolutionManager.java (новый)
- diesel/storage/avro/SchemaCompatibilityChecker.java (новый)

---

### Промпт 72: AVRO схема - разрешение конфликтов
**Приоритет: MEDIUM**

**Задача:**
1. Правила разрешения конфликтов при эволюции
2. Default values для новых полей
3. Игнорирование удаленных полей
4. Renaming fields с aliases

**Файлы:** diesel/storage/avro/SchemaConflictResolver.java (новый)

---

### Промпт 73: AVRO схема - валидация данных
**Приоритет: HIGH**

**Задача:**
1. Валидация данных против схемы при записи
2. Строгий режим vs permissive режим
3. Логи невалидных записей
4. Статистика валидации

**Файлы:** diesel/storage/avro/AvroDataValidator.java (новый)

---

### Промпт 74: AVRO схема - union типы
**Приоритет: MEDIUM**

**Задача:**
1. Поддержка union типов из SQL NULLable колонок
2. Правильный порядок типов в union (null первый)
3. Сериализация/десериализация union значений
4. Оптимизация для common case (не-null значения)

**Файлы:** diesel/storage/avro/AvroUnionHandler.java (новый)

---

### Промпт 75: AVRO схема - complex типы
**Приоритет: MEDIUM**

**Задача:**
1. Поддержка ARRAY типов из SQL
2. Поддержка MAP типов
3. Поддержка RECORD (nested structures)
4. ENUM типы для constrained columns

**Файлы:** 
- diesel/storage/avro/AvroArrayHandler.java (новый)
- diesel/storage/avro/AvroMapHandler.java (новый)
- diesel/storage/avro/AvroRecordHandler.java (новый)

---

### Промпт 76: AVRO мета-данные - заголовок файла
**Приоритет: MEDIUM**

**Задача:**
1. Запись meta данных в заголовок Avro файла
2. Информация о схеме, версии, времени создания
3. Custom metadata: database, table, compression
4. Чтение и валидация заголовка

**Файлы:** diesel/storage/avro/AvroFileHeader.java (новый)

---

### Промпт 77: AVRO мета-данные - синхронизация маркеров
**Приоритет: HIGH**

**Задача:**
1. Генерация случайных sync markers (16 bytes)
2. Запись маркеров между блоками
3. Использование для восстановления после crash
4. Проверка целостности по маркерам

**Файлы:** diesel/storage/avro/AvroSyncMarkerManager.java (новый)

---

### Промпт 78: AVRO производительность - буферизация
**Приоритет: HIGH**

**Задача:**
1. Настройка размера буфера записи
2. Настройка размера буфера чтения
3. Flush策略: по размеру, по времени, принудительно
4. Zero-copy оптимизации где возможно

**Файлы:** 
- diesel/storage/avro/AvroBufferConfig.java (новый)
- diesel/storage/avro/AvroBufferManager.java (новый)

---

### Промпт 79: AVRO производительность - пул объектов
**Приоритет: MEDIUM**

**Задача:**
1. Object pool для GenericRecord
2. Object pool для DatumWriter/DatumReader
3. Снижение GC pressure
4. Метрики: allocation rate, GC time

**Файлы:** diesel/storage/avro/AvroObjectPool.java (новый)

---

### Промпт 80: AVRO производительность - direct buffers
**Приоритет: MEDIUM**

**Задача:**
1. Использование ByteBuffer.allocateDirect()
2. Off-heap хранение для больших блоков
3. Управление жизненным циклом direct buffer
4. Benchmark heap vs off-heap

**Файлы:** diesel/storage/avro/AvroDirectBufferManager.java (новый)

---

### Промпт 81: AVRO производительность - batch операции
**Приоритет: HIGH**

**Задача:**
1. Batch insert: 1000+ записей за один вызов
2. Batch read с предсказанием размера
3. Transaction batching для атомарности
4. Статистика batch операций

**Файлы:** diesel/storage/avro/AvroBatchOperator.java (новый)

---

### Промпт 82: AVRO восстановление - crash recovery
**Приоритет: CRITICAL**

**Задача:**
1. Обнаружение incomplete блоков по sync markers
2. Откат незавершенных транзакций
3. Восстановление до последнего consistent состояния
4. Логирование recovery процесса

**Файлы:** 
- diesel/storage/avro/AvroRecoveryManager.java (новый)
- diesel/storage/avro/AvroCrashDetector.java (новый)

---

### Промпт 83: AVRO восстановление - проверка целостности
**Приоритет: HIGH**

**Задача:**
1. CRC checksum для каждого блока
2. Валидация при чтении
3. Обнаружение bit rot / corruption
4. Статистика integrity checks

**Файлы:** diesel/storage/avro/AvroIntegrityChecker.java (новый)

---

### Промпт 84: AVRO восстановление - backup и restore
**Приоритет: MEDIUM**

**Задача:**
1. Online backup без блокировки записи
2. Point-in-time recovery
3. Incremental backup
4. Restore из backup с валидацией

**Файлы:** 
- diesel/storage/avro/AvroBackupManager.java (новый)
- diesel/storage/avro/AvroRestoreManager.java (новый)

---

### Промпт 85: AVRO индексация - первичный ключ
**Приоритет: HIGH**

**Задача:**
1. Индекс на первичный ключ поверх AVRO файла
2. Быстрый lookup по PK
3. Поддержание индекса при insert/update/delete
4. Кэширование горячих страниц индекса

**Файлы:** diesel/storage/avro/AvroPrimaryKeyIndex.java (новый)

---

### Промпт 86: AVRO индексация - вторичные индексы
**Приоритет: MEDIUM**

**Задача:**
1. Создание secondary indexes на любые колонки
2. B-Tree индекс поверх AVRO данных
3. Composite индексы
4. Статистика использования индексов

**Файлы:** diesel/storage/avro/AvroSecondaryIndex.java (новый)

---

### Промпт 87: AVRO индексация - bloom filter
**Приоритет: MEDIUM**

**Задача:**
1. Bloom filter для каждого блока
2. Быстрая проверка наличия значения в блоке
3. Снижение I/O при point lookups
4. Настройка false positive rate

**Файлы:** diesel/storage/avro/AvroBloomFilter.java (новый)

---

### Промпт 88: AVRO партиционирование - по дате
**Приоритет: HIGH**

**Задача:**
1. Партиционирование по дате/времени
2. Автоматическое создание новых партиций
3. Pruning партиций при query
4. Конфигурация гранулярности (день/месяц/год)

**Файлы:** diesel/storage/avro/AvroDatePartitioner.java (новый)

---

### Промпт 89: AVRO партиционирование - по хешу
**Приоритет: MEDIUM**

**Задача:**
1. Hash partitioning для равномерного распределения
2. Выбор колонки для hashing
3. Количество партиций (конфигурируемое)
4. Rebalancing при изменении количества партиций

**Файлы:** diesel/storage/avro/AvroHashPartitioner.java (новый)

---

### Промпт 90: AVRO партиционирование - range partitioning
**Приоритет: MEDIUM**

**Задача:**
1. Range partitioning по числовым колонкам
2. Определение границ диапазонов
3. Dynamic range adjustment
4. Query pruning по ranges

**Файлы:** diesel/storage/avro/AvroRangePartitioner.java (новый)

---

### Промпт 91: AVRO интеграция - Query Executor
**Приоритет: CRITICAL**

**Задача:**
1. Интеграция AvroRowStorage с QueryExecutor
2. Pushdown predicates в Avro reader
3. Column projection для уменьшения I/O
4. Statistics для cost-based optimization

**Файлы:** diesel/storage/avro/AvroQueryExecutor.java (новый)

---

### Промпт 92: AVRO интеграция - Transaction Manager
**Приоритет: CRITICAL**

**Задача:**
1. ACID транзакции для AVRO хранилища
2. Write-ahead logging для атомарности
3. Isolation levels support
4. Recovery после crash транзакций

**Файлы:** diesel/storage/avro/AvroTransactionManager.java (новый)

---

### Промпт 93: AVRO интеграция - Test Suite
**Приоритет: HIGH**

**Задача:**
1. Unit тесты для всех компонентов
2. Integration тесты с Database
3. Performance тесты с различными codecs
4. Stress тесты с большими объемами

**Файлы:** 
- diesel/storage/avro/AvroStorageTest.java (новый)
- diesel/storage/avro/AvroCompressionTest.java (новый)
- diesel/storage/avro/AvroRecoveryTest.java (новый)

---

### Промпт 94: AVRO мониторинг - метрики
**Приоритет: MEDIUM**

**Задача:**
1. Метрики: read/write throughput, compression ratio
2. JMX integration для мониторинга
3. Prometheus metrics export
4. Alerting на аномалии

**Файлы:** diesel/storage/avro/AvroMetrics.java (новый)

---

### Промпт 95: AVRO мониторинг - логирование
**Приоритет: LOW**

**Задача:**
1. Structured logging для всех операций
2. Audit log для compliance
3. Performance tracing
4. Log rotation и архивация

**Файлы:** diesel/storage/avro/AvroAuditLogger.java (новый)

---

### Промпт 96: AVRO документация и примеры
**Приоритет: LOW**

**Задача:**
1. Руководство по настройке AVRO хранилища
2. Примеры использования в SQL queries
3. Benchmark результаты для разных конфигураций
4. Troubleshooting guide

**Файлы:** 
- docs/avro-storage-guide.md (новый)
- examples/avro-examples.sql (новый)

---

## Section 3: DieselDB Core Mechanisms (Промпты 97-112)

### Промпт 97: WAL - Write Ahead Log базовая реализация
**Приоритет: CRITICAL (durability)**

**Задача:**
1. Создай WALManager для управления журналом предзаписи
2. Реализуй WALEntry для представления записей журнала
3. WALSegment для управления сегментами файлов WAL
4. Базовая запись: begin, commit, abort транзакций

**Файлы:**
- diesel/wal/WALManager.java (новый)
- diesel/wal/WALEntry.java (новый)
- diesel/wal/WALSegment.java (новый)
- diesel/wal/WALConfig.java (новый)

---

### Промпт 98: WAL - восстановление из журнала
**Приоритет: CRITICAL (recovery)**

**Задача:**
1. Реализуй WALRecoveryManager для replay журнала
2. REDO committed транзакций
3. UNDO uncommitted транзакций
4. Обработка partial записей

**Файлы:** diesel/wal/WALRecoveryManager.java (новый)

---

### Промпт 99: ARIES Recovery Manager
**Приоритет: CRITICAL (recovery)**

**Задача:**
1. Реализуй алгоритм ARIES для crash recovery
2. Analysis phase: определение dirty pages
3. Redo phase: повтор всех записей
4. Undo phase: откат незавершенных транзакций

**Файлы:**
- diesel/recovery/CrashRecoveryManager.java (новый)
- diesel/recovery/ARIESAlgorithm.java (новый)

---

### Промпт 100: Checkpoint Manager
**Приоритет: HIGH (performance)**

**Задача:**
1. Реализуй CheckpointManager для периодических checkpoint
2. Fuzzy checkpoint для минимизации пауз
3. Запись CheckpointRecord в WAL
4. Очистка старых WAL сегментов после checkpoint

**Файлы:**
- diesel/recovery/CheckpointManager.java (новый)
- diesel/recovery/CheckpointRecord.java (новый)

---

### Промпт 101: Checkpoint типы - fuzzy vs sharp
**Приоритет: MEDIUM**

**Задача:**
1. Fuzzy checkpoint: без блокировки записи
2. Sharp checkpoint: с полной остановкой записи
3. Конфигурация типа checkpoint
4. Метрики: checkpoint duration, WAL size

**Файлы:** diesel/recovery/CheckpointStrategy.java (новый)

---

### Промпт 102: Checksummed Page - страница с контрольной суммой
**Приоритет: HIGH (integrity)**

**Задача:**
1. Реализуй ChecksummedPage для хранения страниц с checksum
2. Вычисление checksum при записи страницы
3. Валидация checksum при чтении
4. Обнаружение corruption данных

**Файлы:**
- diesel/checksum/ChecksummedPage.java (новый)
- diesel/checksum/PageValidator.java (новый)

---

### Промпт 103: CRC32C алгоритм checksum
**Приоритет: HIGH (integrity)**

**Задача:**
1. Реализуй CRC32C алгоритм (Castagnoli polynomial)
2. Оптимизация через hardware инструкции (SSE4.2)
3. Fallback на software реализацию
4. Benchmark производительности

**Файлы:**
- diesel/checksum/CRC32C.java (новый)
- diesel/checksum/CRC32CNative.java (новый, optional JNI)

---

### Промпт 104: Deadlock Detector - обнаружение взаимных блокировок
**Приоритет: HIGH (concurrency)**

**Задача:**
1. Реализуй DeadlockDetector для обнаружения deadlock
2. Построение wait-for graph
3. DFS/BFS поиск циклов в графе
4. Выбор victim транзакции для отката

**Файлы:**
- diesel/lock/DeadlockDetector.java (новый)
- diesel/lock/WaitForGraph.java (новый)

---

### Промпт 105: Lock Timeout Manager - таймауты блокировок
**Приоритет: HIGH (concurrency)**

**Задача:**
1. Реализуй LockTimeoutManager для управления таймаутами
2. Конфигурация timeout (по умолчанию 30 секунд)
3. Callback при истечении таймаута
4. Метрики: timeout count, wait time distribution

**Файлы:**
- diesel/lock/LockTimeoutManager.java (новый)
- diesel/lock/LockTimeoutException.java (новый)

---

### Промпт 106: Savepoint Manager - управление точками сохранения
**Приоритет: MEDIUM (transactions)**

**Задача:**
1. Реализуй SavepointManager для управления savepoint
2. Создание именованных точек сохранения
3. Rollback до savepoint
4. Release savepoint для освобождения ресурсов

**Файлы:**
- diesel/savepoint/SavepointManager.java (новый)
- diesel/savepoint/Savepoint.java (новый)

---

### Промпт 107: Savepoint - вложенные точки сохранения
**Приоритет: LOW (advanced)**

**Задача:**
1. Поддержка вложенных savepoint
2. Иерархия точек сохранения
3. Rollback к любому уровню
4. Очистка вложенных при release

**Файлы:** diesel/savepoint/NestedSavepointStack.java (новый)

---

### Промпт 108: WAL - сегмент ротация и архивация
**Приоритет: HIGH (maintenance)**

**Задача:**
1. Автоматическая ротация WAL сегментов по размеру
2. Архивация старых сегментов (gzip)
3. Очистка сегментов после checkpoint
4. Конфигурация retention policy

**Файлы:** diesel/wal/WALArchiver.java (новый)

---

### Промпт 109: WAL - async запись для производительности
**Приоритет: MEDIUM (performance)**

**Задача:**
1. Асинхронная запись WAL в background потоке
2. Group commit для batching записей
3. Настройка flush frequency
4. Trade-off: durability vs performance

**Файлы:** diesel/wal/AsyncWALWriter.java (новый)

---

### Промпт 110: Recovery - point-in-time recovery
**Приоритет: MEDIUM (disaster recovery)**

**Задача:**
1. Восстановление до конкретного timestamp
2. Поиск позиции в WAL по времени
3. Replay до нужной точки
4. Валидация восстановленного состояния

**Файлы:** diesel/recovery/PointInTimeRecovery.java (новый)

---

### Промпт 111: Lock - deadlock prevention стратегии
**Приоритет: MEDIUM (concurrency)**

**Задача:**
1. Wait-die схема предотвращения deadlock
2. Wound-wait схема
3. No-wait схема с immediate abort
4. Конфигурация стратегии

**Файлы:** diesel/lock/DeadlockPreventionPolicy.java (новый)

---

### Промпт 112: JOIN с OR условием
**Приоритет: CRITICAL**
**Статус: ✅ ВЫПОЛНЕН**

---

## Section 4: Дополнительные SQL возможности (Промпты 113-131)

### Промпт 113: ALTER TABLE ADD COLUMN
**Приоритет: HIGH**

**Задача:**
1. Реализуй ALTER TABLE table_name ADD COLUMN column_name data_type
2. Добавление колонки со значением по умолчанию
3. Обновление метаданных таблицы
4. Обратная совместимость со старыми данными

**Файлы:** diesel/AlterTableAddColumnQuery.java (новый)

---

### Промпт 114: ALTER TABLE DROP COLUMN
**Приоритет: MEDIUM**

**Задача:**
1. Реализуй ALTER TABLE table_name DROP COLUMN column_name
2. Физическое удаление данных (или lazy deletion)
3. Обновление индексов
4. Зависимости: CHECK constraints, foreign keys

**Файлы:** diesel/AlterTableDropColumnQuery.java (новый)

---

### Промпт 115: UNION/INTERSECT/EXCEPT операторы
**Приоритет: MEDIUM**

**Задача:**
1. Реализуй UNION (с удалением дубликатов)
2. Реализуй UNION ALL (без удаления дубликатов)
3. INTERSECT: общие строки двух запросов
4. EXCEPT: строки первого запроса минус строки второго

**Файлы:** 
- diesel/UnionQuery.java (новый)
- diesel/IntersectQuery.java (новый)
- diesel/ExceptQuery.java (новый)

---

### Промпт 116: DROP INDEX
**Приоритет: MEDIUM**

**Задача:**
1. Реализуй DROP INDEX index_name ON table_name
2. Удаление структуры индекса
3. Освобождение памяти/диска
4. Обновление метаданных

**Файлы:** diesel/DropIndexQuery.java (новый)

---

### Промпт 117: TRUNCATE TABLE
**Приоритет: HIGH**

**Задача:**
1. Реализуй TRUNCATE TABLE table_name
2. Быстрое удаление всех данных (без逐条删除)
3. Сброс auto-increment counters
4. Минимальное WAL logging

**Файлы:** diesel/TruncateTableQuery.java (новый)

---

### Промпт 118: CREATE SEQUENCE
**Приоритет: MEDIUM**

**Задача:**
1. CREATE SEQUENCE seq_name START WITH n INCREMENT BY m
2. Хранение текущего значения последовательности
3. NEXTVAL, CURRVAL функции
4. Кэширование значений для производительности

**Файлы:** 
- diesel/CreateSequenceQuery.java (новый)
- diesel/SequenceManager.java (обновление)

---

### Промпт 119: DROP SEQUENCE
**Приоритет: LOW**

**Задача:**
1. DROP SEQUENCE sequence_name
2. Очистка ресурсов
3. Проверка зависимостей

**Файлы:** diesel/DropSequenceQuery.java (новый)

---

### Промпт 120: Query Result Cache
**Приоритет: MEDIUM (performance)**

**Задача:**
1. Кэширование результатов SELECT запросов
2. Ключ: normalized SQL + bind variables
3. TTL-based инвалидация
4. Automatic invalidation при INSERT/UPDATE/DELETE

**Файлы:** 
- diesel/cache/QueryResultCache.java (новый)
- diesel/cache/CachedResult.java (новый)

---

### Промпт 121: Bulk Insert/Copy API
**Приоритет: HIGH (performance)**

**Задача:**
1. BULK INSERT table_name FROM 'file.csv'
2. Поддержка форматов: CSV, TSV, AVRO
3. BATCH SIZE конфигурация
4. Отключение индексов на время загрузки

**Файлы:** 
- diesel/BulkInsertQuery.java (новый)
- diesel/BulkLoader.java (новый)

---

### Промпт 122: Bitmap Indexes
**Приоритет: LOW (specialized)**

**Задача:**
1. Bitmap индекс для low-cardinality колонок
2. BitSet per distinct value
3. Быстрые bitwise операции для WHERE
4. Сжатие bitmap (WAH, BBC)

**Файлы:** 
- diesel/BitmapIndex.java (новый)
- diesel/BitmapCompressor.java (новый)

---

### Промпт 123: Parallel Query Execution - Scan
**Приоритет: MEDIUM (performance)**

**Задача:**
1. Parallel table scan с разделением диапазонов
2. Распределение между thread pool
3. Сбор результатов из多个 потоков
4. Load balancing

**Файлы:** diesel/executor/ParallelScanExecutor.java (новый)

---

### Промпт 124: Parallel Query Execution - Aggregation
**Приоритет: MEDIUM (performance)**

**Задача:**
1. Map-reduce стиль aggregation
2. Local aggregation per thread
3. Global merge результатов
4. Thread-safe агрегатные функции

**Файлы:** diesel/executor/ParallelAggregationExecutor.java (новый)

---

### Промпт 125: Virtual Threads для concurrency
**Приоритет: LOW (future)**

**Задача:**
1. Интеграция Java Virtual Threads (Project Loom)
2. Замена thread pool на virtual threads
3. Benchmark производительности
4. Требования: Java 21+

**Файлы:** diesel/concurrent/VirtualThreadScheduler.java (новый)

---

### Промпт 126: Record Patterns для чистоты кода
**Приоритет: LOW (code quality)**

**Задача:**
1. Refactoring с использованием record patterns
2. Pattern matching for switch
3. Снижение boilerplate
4. Требования: Java 21+

**Файлы:** Various (refactoring)

---

### Промпт 127: Materialized Views
**Приоритет: LOW (advanced)**

**Задача:**
1. CREATE MATERIALIZED VIEW name AS SELECT ...
2. Хранение результатов view физически
3. Refresh strategies: manual, periodic, incremental
4. Query rewrite для использования materialized views

**Файлы:** 
- diesel/CreateMaterializedViewQuery.java (новый)
- diesel/MaterializedViewManager.java (новый)

---

### Промпт 128: Foreign Keys с каскадными операциями
**Приоритет: MEDIUM (integrity)**

**Задача:**
1. FOREIGN KEY с ON DELETE CASCADE
2. ON UPDATE CASCADE
3. SET NULL при удалении родителя
4. Проверка referential integrity

**Файлы:** 
- diesel/ForeignKeyConstraint.java (новый)
- diesel/CascadeDeleteQuery.java (новый)

---

### Промпт 129: CHECK Constraints
**Приоритет: MEDIUM (integrity)**

**Задача:**
1. CHECK (condition) при CREATE TABLE
2. Валидация при INSERT/UPDATE
3. Составные условия CHECK
4. Named constraints

**Файлы:** diesel/CheckConstraint.java (новый)

---

### Промпт 130: Full Text Search
**Приоритет: LOW (specialized)**

**Задача:**
1. Инвертированный индекс для text колонок
2. Tokenization и stemming
3. Поиск по ключевым словам
4. Relevance scoring

**Файлы:** 
- diesel/FullTextIndex.java (новый)
- diesel/Tokenizer.java (новый)

---

### Промпт 131: Window Functions
**Приоритет: LOW (advanced SQL)**

**Задача:**
1. ROW_NUMBER(), RANK(), DENSE_RANK()
2. NTILE(n) для разделения на группы
3. LAG/LEAD для доступа к соседним строкам
4. OVER (PARTITION BY ... ORDER BY ...)

**Файлы:** 
- diesel/WindowFunction.java (новый)
- diesel/WindowFrameEvaluator.java (новый)

---

## Сводная таблица всех промптов

| # | Промпт | Раздел | Статус | Приоритет |
|---|--------|--------|--------|-----------|
| 1 | JOIN с OR в условии (OOM) | Section 0 | ✅ DONE | CRITICAL |
| 2 | Cross Join streaming | Section 0 | ⬜ TODO | HIGH |
| 3 | GROUP BY unique values | Section 0 | ✅ DONE | HIGH |
| 4 | IN со списком значений | Section 0 | ⬜ TODO | HIGH |
| 5 | IN + AND/OR | Section 0 | ✅ DONE | CRITICAL |
| 6-9 | LIMIT/OFFSET family | Section 0 | ⬜ TODO | HIGH |
| 10 | Hash Join оптимизация | Section 0 | ⬜ TODO | MEDIUM |
| 11 | EXPLAIN | Section 0 | ⬜ TODO | MEDIUM |
| 12 | max.result.rows guard | Section 0 | ⬜ TODO | HIGH |
| 13 | OOM error handling | Section 0 | ⬜ TODO | MEDIUM |
| 14 | Table statistics | Section 0 | ⬜ TODO | MEDIUM |
| 15 | Auto-indexes для JOIN | Section 0 | ⬜ TODO | MEDIUM |
| 16 | Query plan cache | Section 0 | ⬜ TODO | LOW |
| 17 | Reduce test heap | Section 0 | ⬜ TODO | HIGH |
| 18 | Query profiler | Section 0 | ⬜ TODO | MEDIUM |
| 19 | Performance regression tests | Section 0 | ⬜ TODO | MEDIUM |
| 20 | KNOWN_LIMITATIONS.md | Section 0 | ⬜ TODO | LOW |
| 21 | RowBased хранилищ рефакторинг | Section 1 | ⬜ TODO | CRITICAL |
| 22-23 | TSV хранилище | Section 1 | ⬜ TODO | HIGH |
| 24-39 | Улучшения CSV/TSV-хранилищ (код-ревью) | Section 1a | ⬜ TODO | HIGH |
| 40-56 | JSONL хранилище (база, абстракции, надёжность, скорость) | Section 1b | ⬜ TODO | HIGH |
| 57-96 | AVRO хранилище (40 промптов) | Section 2 | ⬜ TODO | HIGH |
| 97-112 | Diesel mechanisms (WAL, recovery, etc.) | Section 3 | ⬜ TODO | CRITICAL |
| 113-131 | Дополнительные SQL возможности | Section 4 | ⬜ TODO | MEDIUM |

---

## Рекомендуемый порядок выполнения (Pareto Principle)

### Фаза 1: Критическая инфраструктура (Промпты 21, 97-99, 102-103)
1. **Промпт 21** - Рефакторинг RowBased хранилищ (база для TSV/JSONL/AVRO)
2. **Промпт 97** - WAL базовая реализация (durability)
3. **Промпт 98** - WAL восстановление
4. **Промпт 99** - ARIES Recovery Manager
5. **Промпт 102** - Checksummed Page (integrity)
6. **Промпт 103** - CRC32C алгоритм

### Фаза 2: RowBased хранилища (Промпты 22-23, 24-39, 40-56)
7. **Промпт 22** - TSV базовая реализация
8. **Промпт 40** - JSONL базовая реализация
9. **Промпты 24-39** - Улучшения CSV/TSV-хранилищ (код-ревью 11.09.2026)
10. **Промпты 41-56** - JSONL: абстракции, надёжность, скорость (анализ 11.09.2026)

### Фаза 3: AVRO хранилище база (Промпты 57-61, 91-93)
11. **Промпт 57** - AVRO настройка проекта
12. **Промпт 58** - AVRO схема и типы
13. **Промпт 59** - AvroRowStorage базовый класс
14. **Промпт 60** - AVRO запись данных
15. **Промпт 61** - AVRO чтение данных
16. **Промпт 91** - AVRO интеграция с Query Executor
17. **Промпт 92** - AVRO интеграция с Transaction Manager
18. **Промпт 93** - AVRO Test Suite

### Фаза 4: AVRO продвинутые возможности (Промпты 62-90, 94-96)
19-50. Промпты 62-90, 94-96 (сжатие, партиционирование, оптимизация)

### Фаза 5: Concurrency и Recovery (Промпты 100-101, 104-111)
51-60. Deadlock detection, lock timeouts, savepoints

### Фаза 6: SQL расширения (Промпты 113-131)
61-79. ALTER TABLE, UNION, bulk operations, etc.
