# Промпты для DieselDB - Полная нумерация

> Статус на 06.09.2026: **4 из 20 выполнено** (Prompt 1, 3, 5, 96).  
> Ниже — полная нумерация всех промптов включая новые секции.  
> Дополнено 11.09.2026: добавлена Section 1a (Промпты 24-38) — улучшения CSV/TSV-хранилищ по результатам код-ревью; нумерация последующих промптов сдвинута на +15.

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

## Section 1a: Улучшения CSV/TSV-хранилищ (Промпты 24-38)

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

## Section 1b: JSONL хранилище (Промпты 39-40)

### Промпт 39: JSONL хранилище - базовая реализация
**Приоритет: HIGH**

**Задача:**
1. Реализуй JsonlRowStorage для хранения данных в JSON Lines формате
2. Каждая строка - отдельный JSON объект
3. Поддержка вложенных структур и массивов
4. Валидация JSON при записи

**Файлы:** 
- diesel/storage/JsonlRowStorage.java (новый)
- diesel/storage/JsonlRowReader.java (новый)
- diesel/storage/JsonlRowWriter.java (новый)

---

### Промпт 40: JSONL хранилище - расширенные возможности
**Приоритет: MEDIUM**

**Задача:**
1. Добавь схему JSON с валидацией типов
2. Реализуй проекцию полей (чтение только нужных полей из JSON)
3. Поддержка JSON Path для сложных запросов
4. Сжатие JSONL файлов (gzip, lz4)

**Файлы:** diesel/storage/JsonlSchemaManager.java (новый)

---

## Section 2: AVRO хранилище (Промпты 41-80)

### Промпт 41: AVRO хранилище - базовая настройка проекта
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

### Промпт 42: AVRO хранилище - схема и типы данных
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

### Промпт 43: AVRO хранилище - AvroRowStorage базовый класс
**Приоритет: HIGH**

**Задача:**
1. Реализуй AvroRowStorage extends AbstractRowStorage
2. Методы: open(), close(), scan(), insert(), update(), delete()
3. Интеграция с AvroSchemaManager
4. Базовая сериализация/десериализация Row <-> GenericRecord

**Файлы:** diesel/storage/avro/AvroRowStorage.java (новый)

---

### Промпт 44: AVRO хранилище - запись данных
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

### Промпт 45: AVRO хранилище - чтение данных
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

### Промпт 46: AVRO сжатие - настройка кодеков
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

### Промпт 47: AVRO сжатие - ZStandard кодек
**Приоритет: MEDIUM**

**Задача:**
1. Добавь зависимость com.github.luben:zstd-jni
2. Реализуй ZStandardCodec для Avro
3. Настройка уровня сжатия (1-22)
4. Бенчмарк ZSTD vs Snappy vs Deflate

**Файлы:** diesel/storage/avro/ZStandardCodec.java (новый)

---

### Промпт 48: AVRO сжатие - Snappy оптимизация
**Приоритет: MEDIUM**

**Задача:**
1. Добавь зависимость org.xerial.snappy:snappy-java
2. Оптимизируй буферы для Snappy
3. Benchmark на разных размерах блоков
4. Рекомендации по выбору размера блока

**Файлы:** diesel/storage/avro/SnappyOptimizedCodec.java (новый)

---

### Промпт 49: AVRO сжатие - Deflate уровни
**Приоритет: LOW**

**Задача:**
1. Настройка уровней сжатия Deflate (1-9)
2. Trade-off между скоростью и степенью сжатия
3. Адаптивный выбор уровня по типу данных
4. Кэширование compressor/decompressor

**Файлы:** diesel/storage/avro/DeflateLevelConfig.java (новый)

---

### Промпт 50: AVRO сжатие - BZip2 для холодных данных
**Приоритет: LOW**

**Задача:**
1. Добавь поддержку BZip2 codec
2. Используй для архивных/холодных данных
3. Настройка blockSize (100-900 KB)
4. Интеграция с storage tiering

**Файлы:** diesel/storage/avro/BZip2Codec.java (новый)

---

### Промпт 51: AVRO сжатие - адаптивное сжатие
**Приоритет: MEDIUM**

**Задача:**
1. Мониторинг compression ratio в runtime
2. Авто-переключение кодека при изменении паттерна данных
3. Метрики: compressed size, compression time, decompression time
4. Recommendations engine для выбора кодека

**Файлы:** diesel/storage/avro/AdaptiveCompressionManager.java (новый)

---

### Промпт 52: AVRO блочная структура - настройка блока
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

### Промпт 53: AVRO блочная структура - параллельное чтение
**Приоритет: MEDIUM**

**Задача:**
1. Чтение нескольких блоков параллельно
2. Распределение блоков между потоками
3. Синхронизация результатов
4. Load balancing между блоками разного размера

**Файлы:** diesel/storage/avro/AvroParallelReader.java (новый)

---

### Промпт 54: AVRO блочная структура - split для MapReduce
**Приоритет: LOW**

**Задача:**
1. Поддержка AvroInputFormat для Hadoop/MapReduce
2. Correct splitting по sync markers
3. Интеграция с Spark через AvroFileFormat
4. Примеры использования в документации

**Файлы:** diesel/storage/avro/AvroInputFormatCompat.java (новый)

---

### Промпт 55: AVRO схема - эволюция схемы
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

### Промпт 56: AVRO схема - разрешение конфликтов
**Приоритет: MEDIUM**

**Задача:**
1. Правила разрешения конфликтов при эволюции
2. Default values для новых полей
3. Игнорирование удаленных полей
4. Renaming fields с aliases

**Файлы:** diesel/storage/avro/SchemaConflictResolver.java (новый)

---

### Промпт 57: AVRO схема - валидация данных
**Приоритет: HIGH**

**Задача:**
1. Валидация данных против схемы при записи
2. Строгий режим vs permissive режим
3. Логи невалидных записей
4. Статистика валидации

**Файлы:** diesel/storage/avro/AvroDataValidator.java (новый)

---

### Промпт 58: AVRO схема - union типы
**Приоритет: MEDIUM**

**Задача:**
1. Поддержка union типов из SQL NULLable колонок
2. Правильный порядок типов в union (null первый)
3. Сериализация/десериализация union значений
4. Оптимизация для common case (не-null значения)

**Файлы:** diesel/storage/avro/AvroUnionHandler.java (новый)

---

### Промпт 59: AVRO схема - complex типы
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

### Промпт 60: AVRO мета-данные - заголовок файла
**Приоритет: MEDIUM**

**Задача:**
1. Запись meta данных в заголовок Avro файла
2. Информация о схеме, версии, времени создания
3. Custom metadata: database, table, compression
4. Чтение и валидация заголовка

**Файлы:** diesel/storage/avro/AvroFileHeader.java (новый)

---

### Промпт 61: AVRO мета-данные - синхронизация маркеров
**Приоритет: HIGH**

**Задача:**
1. Генерация случайных sync markers (16 bytes)
2. Запись маркеров между блоками
3. Использование для восстановления после crash
4. Проверка целостности по маркерам

**Файлы:** diesel/storage/avro/AvroSyncMarkerManager.java (новый)

---

### Промпт 62: AVRO производительность - буферизация
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

### Промпт 63: AVRO производительность - пул объектов
**Приоритет: MEDIUM**

**Задача:**
1. Object pool для GenericRecord
2. Object pool для DatumWriter/DatumReader
3. Снижение GC pressure
4. Метрики: allocation rate, GC time

**Файлы:** diesel/storage/avro/AvroObjectPool.java (новый)

---

### Промпт 64: AVRO производительность - direct buffers
**Приоритет: MEDIUM**

**Задача:**
1. Использование ByteBuffer.allocateDirect()
2. Off-heap хранение для больших блоков
3. Управление жизненным циклом direct buffer
4. Benchmark heap vs off-heap

**Файлы:** diesel/storage/avro/AvroDirectBufferManager.java (новый)

---

### Промпт 65: AVRO производительность - batch операции
**Приоритет: HIGH**

**Задача:**
1. Batch insert: 1000+ записей за один вызов
2. Batch read с предсказанием размера
3. Transaction batching для атомарности
4. Статистика batch операций

**Файлы:** diesel/storage/avro/AvroBatchOperator.java (новый)

---

### Промпт 66: AVRO восстановление - crash recovery
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

### Промпт 67: AVRO восстановление - проверка целостности
**Приоритет: HIGH**

**Задача:**
1. CRC checksum для каждого блока
2. Валидация при чтении
3. Обнаружение bit rot / corruption
4. Статистика integrity checks

**Файлы:** diesel/storage/avro/AvroIntegrityChecker.java (новый)

---

### Промпт 68: AVRO восстановление - backup и restore
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

### Промпт 69: AVRO индексация - первичный ключ
**Приоритет: HIGH**

**Задача:**
1. Индекс на первичный ключ поверх AVRO файла
2. Быстрый lookup по PK
3. Поддержание индекса при insert/update/delete
4. Кэширование горячих страниц индекса

**Файлы:** diesel/storage/avro/AvroPrimaryKeyIndex.java (новый)

---

### Промпт 70: AVRO индексация - вторичные индексы
**Приоритет: MEDIUM**

**Задача:**
1. Создание secondary indexes на любые колонки
2. B-Tree индекс поверх AVRO данных
3. Composite индексы
4. Статистика использования индексов

**Файлы:** diesel/storage/avro/AvroSecondaryIndex.java (новый)

---

### Промпт 71: AVRO индексация - bloom filter
**Приоритет: MEDIUM**

**Задача:**
1. Bloom filter для каждого блока
2. Быстрая проверка наличия значения в блоке
3. Снижение I/O при point lookups
4. Настройка false positive rate

**Файлы:** diesel/storage/avro/AvroBloomFilter.java (новый)

---

### Промпт 72: AVRO партиционирование - по дате
**Приоритет: HIGH**

**Задача:**
1. Партиционирование по дате/времени
2. Автоматическое создание новых партиций
3. Pruning партиций при query
4. Конфигурация гранулярности (день/месяц/год)

**Файлы:** diesel/storage/avro/AvroDatePartitioner.java (новый)

---

### Промпт 73: AVRO партиционирование - по хешу
**Приоритет: MEDIUM**

**Задача:**
1. Hash partitioning для равномерного распределения
2. Выбор колонки для hashing
3. Количество партиций (конфигурируемое)
4. Rebalancing при изменении количества партиций

**Файлы:** diesel/storage/avro/AvroHashPartitioner.java (новый)

---

### Промпт 74: AVRO партиционирование - range partitioning
**Приоритет: MEDIUM**

**Задача:**
1. Range partitioning по числовым колонкам
2. Определение границ диапазонов
3. Dynamic range adjustment
4. Query pruning по ranges

**Файлы:** diesel/storage/avro/AvroRangePartitioner.java (новый)

---

### Промпт 75: AVRO интеграция - Query Executor
**Приоритет: CRITICAL**

**Задача:**
1. Интеграция AvroRowStorage с QueryExecutor
2. Pushdown predicates в Avro reader
3. Column projection для уменьшения I/O
4. Statistics для cost-based optimization

**Файлы:** diesel/storage/avro/AvroQueryExecutor.java (новый)

---

### Промпт 76: AVRO интеграция - Transaction Manager
**Приоритет: CRITICAL**

**Задача:**
1. ACID транзакции для AVRO хранилища
2. Write-ahead logging для атомарности
3. Isolation levels support
4. Recovery после crash транзакций

**Файлы:** diesel/storage/avro/AvroTransactionManager.java (новый)

---

### Промпт 77: AVRO интеграция - Test Suite
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

### Промпт 78: AVRO мониторинг - метрики
**Приоритет: MEDIUM**

**Задача:**
1. Метрики: read/write throughput, compression ratio
2. JMX integration для мониторинга
3. Prometheus metrics export
4. Alerting на аномалии

**Файлы:** diesel/storage/avro/AvroMetrics.java (новый)

---

### Промпт 79: AVRO мониторинг - логирование
**Приоритет: LOW**

**Задача:**
1. Structured logging для всех операций
2. Audit log для compliance
3. Performance tracing
4. Log rotation и архивация

**Файлы:** diesel/storage/avro/AvroAuditLogger.java (новый)

---

### Промпт 80: AVRO документация и примеры
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

## Section 3: DieselDB Core Mechanisms (Промпты 81-96)

### Промпт 81: WAL - Write Ahead Log базовая реализация
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

### Промпт 82: WAL - восстановление из журнала
**Приоритет: CRITICAL (recovery)**

**Задача:**
1. Реализуй WALRecoveryManager для replay журнала
2. REDO committed транзакций
3. UNDO uncommitted транзакций
4. Обработка partial записей

**Файлы:** diesel/wal/WALRecoveryManager.java (новый)

---

### Промпт 83: ARIES Recovery Manager
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

### Промпт 84: Checkpoint Manager
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

### Промпт 85: Checkpoint типы - fuzzy vs sharp
**Приоритет: MEDIUM**

**Задача:**
1. Fuzzy checkpoint: без блокировки записи
2. Sharp checkpoint: с полной остановкой записи
3. Конфигурация типа checkpoint
4. Метрики: checkpoint duration, WAL size

**Файлы:** diesel/recovery/CheckpointStrategy.java (новый)

---

### Промпт 86: Checksummed Page - страница с контрольной суммой
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

### Промпт 87: CRC32C алгоритм checksum
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

### Промпт 88: Deadlock Detector - обнаружение взаимных блокировок
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

### Промпт 89: Lock Timeout Manager - таймауты блокировок
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

### Промпт 90: Savepoint Manager - управление точками сохранения
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

### Промпт 91: Savepoint - вложенные точки сохранения
**Приоритет: LOW (advanced)**

**Задача:**
1. Поддержка вложенных savepoint
2. Иерархия точек сохранения
3. Rollback к любому уровню
4. Очистка вложенных при release

**Файлы:** diesel/savepoint/NestedSavepointStack.java (новый)

---

### Промпт 92: WAL - сегмент ротация и архивация
**Приоритет: HIGH (maintenance)**

**Задача:**
1. Автоматическая ротация WAL сегментов по размеру
2. Архивация старых сегментов (gzip)
3. Очистка сегментов после checkpoint
4. Конфигурация retention policy

**Файлы:** diesel/wal/WALArchiver.java (новый)

---

### Промпт 93: WAL - async запись для производительности
**Приоритет: MEDIUM (performance)**

**Задача:**
1. Асинхронная запись WAL в background потоке
2. Group commit для batching записей
3. Настройка flush frequency
4. Trade-off: durability vs performance

**Файлы:** diesel/wal/AsyncWALWriter.java (новый)

---

### Промпт 94: Recovery - point-in-time recovery
**Приоритет: MEDIUM (disaster recovery)**

**Задача:**
1. Восстановление до конкретного timestamp
2. Поиск позиции в WAL по времени
3. Replay до нужной точки
4. Валидация восстановленного состояния

**Файлы:** diesel/recovery/PointInTimeRecovery.java (новый)

---

### Промпт 95: Lock - deadlock prevention стратегии
**Приоритет: MEDIUM (concurrency)**

**Задача:**
1. Wait-die схема предотвращения deadlock
2. Wound-wait схема
3. No-wait схема с immediate abort
4. Конфигурация стратегии

**Файлы:** diesel/lock/DeadlockPreventionPolicy.java (новый)

---

### Промпт 96: JOIN с OR условием
**Приоритет: CRITICAL**
**Статус: ✅ ВЫПОЛНЕН**

---

## Section 4: Дополнительные SQL возможности (Промпты 97-115)

### Промпт 97: ALTER TABLE ADD COLUMN
**Приоритет: HIGH**

**Задача:**
1. Реализуй ALTER TABLE table_name ADD COLUMN column_name data_type
2. Добавление колонки со значением по умолчанию
3. Обновление метаданных таблицы
4. Обратная совместимость со старыми данными

**Файлы:** diesel/AlterTableAddColumnQuery.java (новый)

---

### Промпт 98: ALTER TABLE DROP COLUMN
**Приоритет: MEDIUM**

**Задача:**
1. Реализуй ALTER TABLE table_name DROP COLUMN column_name
2. Физическое удаление данных (или lazy deletion)
3. Обновление индексов
4. Зависимости: CHECK constraints, foreign keys

**Файлы:** diesel/AlterTableDropColumnQuery.java (новый)

---

### Промпт 99: UNION/INTERSECT/EXCEPT операторы
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

### Промпт 100: DROP INDEX
**Приоритет: MEDIUM**

**Задача:**
1. Реализуй DROP INDEX index_name ON table_name
2. Удаление структуры индекса
3. Освобождение памяти/диска
4. Обновление метаданных

**Файлы:** diesel/DropIndexQuery.java (новый)

---

### Промпт 101: TRUNCATE TABLE
**Приоритет: HIGH**

**Задача:**
1. Реализуй TRUNCATE TABLE table_name
2. Быстрое удаление всех данных (без逐条删除)
3. Сброс auto-increment counters
4. Минимальное WAL logging

**Файлы:** diesel/TruncateTableQuery.java (новый)

---

### Промпт 102: CREATE SEQUENCE
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

### Промпт 103: DROP SEQUENCE
**Приоритет: LOW**

**Задача:**
1. DROP SEQUENCE sequence_name
2. Очистка ресурсов
3. Проверка зависимостей

**Файлы:** diesel/DropSequenceQuery.java (новый)

---

### Промпт 104: Query Result Cache
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

### Промпт 105: Bulk Insert/Copy API
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

### Промпт 106: Bitmap Indexes
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

### Промпт 107: Parallel Query Execution - Scan
**Приоритет: MEDIUM (performance)**

**Задача:**
1. Parallel table scan с разделением диапазонов
2. Распределение между thread pool
3. Сбор результатов из多个 потоков
4. Load balancing

**Файлы:** diesel/executor/ParallelScanExecutor.java (новый)

---

### Промпт 108: Parallel Query Execution - Aggregation
**Приоритет: MEDIUM (performance)**

**Задача:**
1. Map-reduce стиль aggregation
2. Local aggregation per thread
3. Global merge результатов
4. Thread-safe агрегатные функции

**Файлы:** diesel/executor/ParallelAggregationExecutor.java (новый)

---

### Промпт 109: Virtual Threads для concurrency
**Приоритет: LOW (future)**

**Задача:**
1. Интеграция Java Virtual Threads (Project Loom)
2. Замена thread pool на virtual threads
3. Benchmark производительности
4. Требования: Java 21+

**Файлы:** diesel/concurrent/VirtualThreadScheduler.java (новый)

---

### Промпт 110: Record Patterns для чистоты кода
**Приоритет: LOW (code quality)**

**Задача:**
1. Refactoring с использованием record patterns
2. Pattern matching for switch
3. Снижение boilerplate
4. Требования: Java 21+

**Файлы:** Various (refactoring)

---

### Промпт 111: Materialized Views
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

### Промпт 112: Foreign Keys с каскадными операциями
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

### Промпт 113: CHECK Constraints
**Приоритет: MEDIUM (integrity)**

**Задача:**
1. CHECK (condition) при CREATE TABLE
2. Валидация при INSERT/UPDATE
3. Составные условия CHECK
4. Named constraints

**Файлы:** diesel/CheckConstraint.java (новый)

---

### Промпт 114: Full Text Search
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

### Промпт 115: Window Functions
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
| 24-38 | Улучшения CSV/TSV-хранилищ (код-ревью) | Section 1a | ⬜ TODO | HIGH |
| 39-40 | JSONL хранилище | Section 1b | ⬜ TODO | HIGH |
| 41-80 | AVRO хранилище (40 промптов) | Section 2 | ⬜ TODO | HIGH |
| 81-96 | Diesel mechanisms (WAL, recovery, etc.) | Section 3 | ⬜ TODO | CRITICAL |
| 97-115 | Дополнительные SQL возможности | Section 4 | ⬜ TODO | MEDIUM |

---

## Рекомендуемый порядок выполнения (Pareto Principle)

### Фаза 1: Критическая инфраструктура (Промпты 21, 81-83, 86-87)
1. **Промпт 21** - Рефакторинг RowBased хранилищ (база для TSV/JSONL/AVRO)
2. **Промпт 81** - WAL базовая реализация (durability)
3. **Промпт 82** - WAL восстановление
4. **Промпт 83** - ARIES Recovery Manager
5. **Промпт 86** - Checksummed Page (integrity)
6. **Промпт 87** - CRC32C алгоритм

### Фаза 2: RowBased хранилища (Промпты 22-23, 24-38, 39-40)
7. **Промпт 22** - TSV базовая реализация
8. **Промпт 39** - JSONL базовая реализация
9. **Промпты 24-38** - Улучшения CSV/TSV-хранилищ (код-ревью 11.09.2026)

### Фаза 3: AVRO хранилище база (Промпты 41-45, 75-77)
10. **Промпт 41** - AVRO настройка проекта
11. **Промпт 42** - AVRO схема и типы
12. **Промпт 43** - AvroRowStorage базовый класс
13. **Промпт 44** - AVRO запись данных
14. **Промпт 45** - AVRO чтение данных
15. **Промпт 75** - AVRO интеграция с Query Executor
16. **Промпт 76** - AVRO интеграция с Transaction Manager
17. **Промпт 77** - AVRO Test Suite

### Фаза 4: AVRO продвинутые возможности (Промпты 46-74, 78-80)
18-57. Промпты 46-74, 78-80 (сжатие, партиционирование, оптимизация)

### Фаза 5: Concurrency и Recovery (Промпты 84-85, 88-95)
57-64. Deadlock detection, lock timeouts, savepoints

### Фаза 6: SQL расширения (Промпты 97-115)
65-83. ALTER TABLE, UNION, bulk operations, etc.