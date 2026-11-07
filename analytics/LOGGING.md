# LOGGING.md — Переключатели режима и асинхронное логирование в DieselDB

Документ описывает дизайн единого переключателя уровня логирования, помодульных
переключателей с приоритетом над глобальным, асинхронного режима и список
устранений в существующей системе логирования DieselDB. Рекомендация после
аудита репозитория — **использовать уже подключённые SLF4J + logback**, не
строить собственный JUL-based менеджер. Документ — дизайн без Java-кода;
пять промптов в конце содержат всё необходимое для реализации по фазам.

---

## 1. Контекст и текущее состояние

Аудит репозитория выявил, что проект **уже частично мигрирован на SLF4J + logback**,
но миграция не завершена и централизованное управление отсутствует.

### 1.1 Зависимости (pom.xml)

| GAV | Версия | Scope | Назначение |
| --- | --- | --- | --- |
| `org.slf4j:slf4j-api` | 2.0.12 | compile (production) | SLF4J фасад — то, что импортирует код |
| `ch.qos.logback:logback-classic` | 1.5.6 | compile (production) | SLF4J-binding + appender framework |
| `org.junit.jupiter:junit-jupiter` | 5.10.0 | test | Тесты |
| `org.awaitility:awaitility` | 4.2.0 | test | Async-тесты |

**SLF4J и logback уже в production-classpath.** Любой код может писать через
`org.slf4j.Logger` / `org.slf4j.LoggerFactory` без добавления новых зависимостей.

### 1.2 Конфигурация logback (уже существует)

Файл `src/main/resources/logback.xml`:

```xml
<configuration>
    <property name="LOG_DIR"  value="${user.dir}/logs"/>
    <property name="LOG_FILE" value="${LOG_DIR}/diesel.log"/>

    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder><pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern></encoder>
    </appender>

    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${LOG_FILE}</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">
            <fileNamePattern>${LOG_DIR}/diesel.%d{yyyy-MM-dd}.%i.log.gz</fileNamePattern>
            <maxFileSize>10MB</maxFileSize>
            <maxHistory>30</maxHistory>
            <totalSizeCap>100MB</totalSizeCap>
        </rollingPolicy>
        <encoder><pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern></encoder>
    </appender>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
        <appender-ref ref="FILE"/>
    </root>
    <logger name="diesel" level="INFO"/>
</configuration>
```

Файл `src/test/resources/logback-large.xml` — quiet-профиль (root WARN, только
console) для тестов `large`/`perf`/`timing-quick`; активируется через
`logback.configurationFile` в surefire `systemPropertyVariables`.

### 1.3 Кодовая база — гибридный JUL+SLF4J

| Категория | Где | Что используют |
| --- | --- | --- |
| **Уже на SLF4J** (~30 классов) | `diesel/wal/*` (WALWriter, WALSegment, WALArchiver, AsyncWALWriter, GroupCommitCoordinator, WALManager, WALSegmentRotator, WALConfig), `diesel/net/*` (SniSelector, SslContextFactory, CompressionHandshakeHandler, TlsHandshakeHandler, CipherSuiteWhitelist), `diesel/DieselDatabase`, `diesel/VacuumManager`, `diesel/DatabaseClient`, `diesel/CliRepl`, `diesel/storage/*` (Jsonl/Tsv/Csv RowReader/Writer/Storage, JsonlBlockManager, JsonlDeltaManager), `diesel/storage/avro/*` (AvroStatistics, AvroBufferManager, AvroIntegrityChecker, AvroSyncMarkerManager, AvroMetrics) | `org.slf4j.Logger` / `LoggerFactory` |
| **Legacy на голом JUL** (~15 классов) | `diesel/DatabaseServer`, `diesel/Database`, `diesel/Table`, `diesel/BTreeIndex`, `diesel/BTreeClusteredIndex`, `diesel/QueryParser` (с собственным static-init), `diesel/SelectQuery`, `diesel/InsertQuery`, `diesel/UpdateQuery`, `diesel/DeleteQuery`, `diesel/QueryExecutor`, `diesel/QueryOptimizer`, `diesel/RecoveryManager`, `diesel/ReplicationCoordinator`, `diesel/security/AuditLogger`, `diesel/storage/avro/AvroAuditLogger` | `java.util.logging.Logger` |
| **Тесты** | `src/test/java/diesel/Slf4jLogCapture.java` (ListAppender helper), `src/test/resources/logback-large.xml` | SLF4J + logback `ListAppender` |

### 1.4 Конкретные проблемы

| Файл / место | Проблема |
| --- | --- |
| `diesel/QueryParser.java` строки 46–81 | Static-init сам регистрирует отдельный `java.util.logging.ConsoleHandler`, отключает родительские хендлеры, читает `logging.level.diesel` из sysprop или `config.properties`. Это **единственный класс с собственной логикой настройки JUL** — если другие классы сделают так же, будет дублирование вывода и хаос приоритетов. На фоне существующего logback.xml этот код создаёт **третью** параллельную конфигурацию. |
| `config.properties` строки 3–7 | `java.util.logging.ConsoleHandler.level = INFO`, `logging.level.diesel=INFO`, `diesel.SelectQueryParser.level = INFO`, `diesel.QueryParser.level=INFO`, `diesel.ConditionParser.level = INFO` — **мёртвые**: JUL `LogManager` читает их только из `logging.properties`, а не из `config.properties`; `logging.level.diesel` читает только `QueryParser`. |
| `diesel/DatabaseServer.java` метод `logConfig` (строки 182–190) | Печатает ВСЕ ключи `config.properties` на INFO: `config.forEach((k,v) → LOGGER.log(Level.INFO, "Config: {0} = {1}", …))`. **Утечка секретов**: `tls.keystore.password = changeit` (строка 579), `tls.sni.*.keystore.password`, `tls.truststore.password` попадают в stdout / `logs/diesel.log` на старте сервера. Комментарий в `config.properties:575` прямо требует «Keystore password (never logged)» — реализация нарушает это правило. |
| `src/main/resources/logback.xml` | Нет async appender — `LOGGER.info` в горячих путях (`WALWriter.append`, `BufferPoolFlusher.flush`) синхронно форматирует и пишет в Console + File. Нет JSON-формата для Loki/ELK. Нет MDC-полей в pattern (нет traceId). Нет redaction-фильтра. Помодульные уровни не параметризованы из `config.properties`. |

### 1.5 Вывод

SLF4J + logback — **уже подключены и частично используются**. Дальнейший путь —
завершить миграцию legacy-хвоста на SLF4J и централизовать управление уровнями
через `config.properties` (программное применение к `LoggerContext` logback).
Строить собственный JUL-based `DieselLogManager` бессмысленно: logback уже даёт
AsyncAppender, MDC, RollingFileAppender, TurboFilter (sampling + redaction) —
всё что нужно, из коробки.

---

## 2. Лицензионный анализ

| Компонент | Версия | Лицензия | Источник | Риски для коммерческого использования |
| --- | --- | --- | --- | --- |
| **SLF4J API** | 2.0.12 | **MIT** | [slf4j.org/license.html](https://www.slf4j.org/license.html), © 2004-2025 QOS.ch | **Нулевые.** MIT совместим со всеми лицензиями — коммерческое, GPL, proprietary. Единственное требование — сохранить copyright notice в распространяемых копиях (включая NOTICE-файл). |
| **logback-classic** | 1.5.6 | **Dual EPL v2.0 / LGPL 2.1** (по выбору лицензиата) | [logback.qos.ch/license.html](https://logback.qos.ch/license.html) | **Минимальные.** Обе лицензии разрешают использование как dependency в закрытом коммерческом ПО без раскрытия собственного исходного кода. Ограничение: модификации *самого logback* должны оставаться под EPL/LGPL — на пользователя logback как библиотеки не распространяется. EPL 2.0 имеет secondary-license provision (переход на GPL совместим). |

### 2.1 Лицензионные риски — детально

**SLF4J (MIT).** Стандартная MIT-формулировка: «Permission is hereby granted,
free of charge, to any person obtaining a copy of this software … to use, copy,
modify, merge, publish, distribute, sublicense, and/or sell copies». Никаких
copyleft-требований, никаких патентных рисков сверх явного патентного гранта
(в MIT нет явного патентного гранта, но и нет патентных претензий — это
исторически безопасная лицензия). DieselDB как коммерческая/проприетарная БД
может использовать SLF4J без ограничений.

**logback (EPL 2.0 / LGPL 2.1 dual).** Ключевые моменты:
1. **Использование как library — разрешено в коммерческом ПО.** Обе лицензии
   явно допускают link/distribute без传染 на код пользователя.
2. **Модификации самого logback** (если вы форкаете и меняете исходники
   logback) — должны оставаться под EPL 2.0 или LGPL 2.1. Для DieselDB не
   актуально: мы используем logback как binary-dependency, не модифицируем.
3. **EPL 2.0 secondary license provision** — позволяет комбинировать с GPL-
   лицензированным кодом. Для проприетарного кода не имеет значения.
4. **LGPL 2.1** — позволяет динамическую/статическую линковку без раскрытия
   кода использующей программы; требует только предоставить возможность
   обновить logback (что Maven dependency даёт автоматически).
5. **На странице logback-license есть явная фраза**: «Upon request, we may
   exempt open-source projects from LGPL and EPL's reciprocity clauses» — то
   есть QOS.ch идёт навстречу даже open-source проектам с другими лицензиями.
   Для closed-source это не нужно.

**Итог.** Лицензионных рисков от добавления/использования SLF4J + logback в
DieselDB нет. Это одна из самых распространённых связок в Java-индустрии
(Spring, Hibernate, Kafka, Cassandra — все используют). NOTICES-файл должен
упоминать обе библиотеки с их лицензиями.

### 2.2 Сравнение с альтернативой «свой JUL-менеджер»

| Критерий | SLF4J + logback | Свой DieselLogManager на JUL |
| --- | --- | --- |
| Лицензия | MIT + EPL/LGPL (library use safe) | Нет зависимости (но придётся писать ~500 строк своего кода и поддерживать) |
| Зависимости | Уже в pom.xml | Не добавляется |
| MDC | `org.slf4j.MDC` из коробки | ThreadLocal + custom Formatter |
| Async appender | `ch.qos.logback.classic.AsyncAppender` (queueSize, neverBlock, discardingThreshold) | Свой bounded queue + daemon-thread (~150 строк, см. `AuditLogger.java:90–189` как образец) |
| Rolling file appender | `RollingFileAppender` + `SizeAndTimeBasedRollingPolicy` (уже настроен в существующем `logback.xml`) | Своя обёртка над `java.util.logging.FileHandler` |
| Sampling | `TurboFilter` | Свой `Filter` + AtomicLong per logger |
| Redaction | `TurboFilter` | Свой `Filter` |
| JSON-формат | `LogstashEncoder` (Apache 2.0) или собственный `Layout` | Свой `Formatter` |
| Runtime-переключение уровней | `LoggerContext.getLogger("diesel.wal").setLevel(Level.DEBUG)` | Свой reapply() |
| Идиоматичность | Стандарт индустрии | Самописная реализация |
| Объём кода | ~200 строк конфигурации + `DieselLogConfig` ~150 строк | ~500–700 строк своего кода |
| Согласованность с существующей кодовой базой | 30 классов уже на SLF4J; миграция оставшихся 15 — естественное завершение | Раздвоение: 30 на SLF4J, 15+ на JUL, свой менеджер |

**Рекомендация.** Использовать SLF4J + logback. Не строить собственный JUL-менеджер.

---

## 3. Цели и принципы

**Цели**

1. Один источник правды для уровня логирования — `config.properties`, применяемый
   программно к `LoggerContext` logback при старте сервера.
2. Помодульные переопределения **с приоритетом над глобальным** уровнем.
3. Опциональный асинхронный режим через `ch.qos.logback.classic.AsyncAppender`.
4. Помодульные переключатели приведены в `config.properties` как закомментированный
   шаблон — НЕ активны по умолчанию.
5. Завершить миграцию legacy-хвоста (15 классов) с JUL на SLF4J — унификация API.
6. Устранить утечку секретов в `DatabaseServer.logConfig`.
7. Обратная совместимость: `-Dlogging.level.diesel=<…>` принимается как
   deprecated alias (используется тестовыми профилями `large/perf/timing-quick`,
   см. комментарий в `QueryParser.java:56-60`).

**Принципы**

- **Один класс-настройщик** — `DieselLogConfig` (новый). Никакой другой класс не
  регистрирует себе хендлеры. `QueryParser` теряет собственный static-init.
- **`config.properties` как единственный источник**, чтение через существующий
  `ConfigLoader.load()`. Базовая конфигурация остаётся в `logback.xml`
  (appender’ы, pattern, файл ротации); `DieselLogConfig` применяет только
  динамические части — уровни и фильтры — из `config.properties` к
  `LoggerContext` после загрузки logback.
- **Sysprop всегда выигрывает** у `config.properties` — для тестовых профилей и
  ad-hoc debug-сессий.
- **Помодульные ключи по умолчанию закомментированы** — оператор включает их явно.

---

## 4. Единый переключатель уровня

### Ключ

```properties
logging.level = INFO
```

Допустимые значения (case-insensitive): `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG`,
`TRACE`, `ALL`. Также принимаются JUL-имена для обратной совместимости:
`SEVERE`→`ERROR`, `WARNING`→`WARN`, `CONFIG`→`INFO`, `FINE`→`DEBUG`,
`FINER`→`TRACE`, `FINEST`→`TRACE`.

### Приоритет разрешения уровня

```
1. -Dlogging.level=<…>           (sysprop, высший приоритет)
2. logging.level в config.properties
3. <root level="INFO">           (из logback.xml, default)
```

### Применение

`DieselLogConfig.init()` вызывается **первой строкой** `DatabaseServer.start()`
(после `loadConfig()`, до `logConfig`, до `loadCatalog`, до `runRecovery`). Он:

1. Считывает уровень из приоритетной цепочки.
2. Получает `LoggerContext` через `(LoggerContext) LoggerFactory.getILoggerFactory()`.
3. Устанавливает уровень root-логгера: `loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.toLevel(resolved))`.
4. Устанавливает уровень пакетного логгера `diesel` в то же значение —
   чтобы логгеры, созданные после init, наследовали корректный уровень.
5. Не трогает appender’ы — они уже настроены `logback.xml`.

### Устаревший alias

`-Dlogging.level.diesel=<…>` принимается как alias к `-Dlogging.level`. При
обнаружении логируется ОДНО WARNING через SLF4J:
`"Deprecated sysprop -Dlogging.level.diesel used; use -Dlogging.level instead"`.
Это сохраняет работоспособность тестовых профилей `large/perf/timing-quick`
(см. комментарий в `QueryParser.java:56-60`) и даёт явный сигнал к миграции.

---

## 5. Помодульные переключатели с приоритетом

### Ключ

```properties
logging.level.<module> = <level>
```

`<module>` — короткое имя, маппится на имя SLF4J-логгера (по имени пакета или
класса). Таблица модулей (согласована с фактической структурой пакетов репозитория):

| module | SLF4J logger name (prefix) | Затронутые классы (примеры) |
| --- | --- | --- |
| `parser` | `diesel.QueryParser`, `diesel.SelectQueryParser`, `diesel.SubqueryParser`, `diesel.ConditionParser`, `diesel.ParseContext`, `diesel.SqlParsingUtils` | Парсинг SQL |
| `query` | `diesel.SelectQuery`, `diesel.InsertQuery`, `diesel.UpdateQuery`, `diesel.DeleteQuery`, `diesel.QueryExecutor`, `diesel.QueryOptimizer`, `diesel.PreparedStatement` | Исполнение запросов |
| `wal` | `diesel.wal` | `WALWriter`, `AsyncWALWriter`, `WALSegment`, `GroupCommitCoordinator`, `WALArchiver`, `WALSegmentRotator`, `WALManager` |
| `recovery` | `diesel.RecoveryManager`, `diesel.recovery`, `diesel.DatabaseRecoverySink` | ARIES analysis/redo/undo |
| `replication` | `diesel.ReplicationCoordinator`, `diesel.SyncReplicationCoordinator`, `diesel.replication` | WalSender/WalReceiver/StreamingApplier |
| `storage` | `diesel.storage` | RowStorage, PageManager, BufferPool, Avro*, Jsonl/Tsv/Csv |
| `bufferpool` | `diesel.storage.page.BufferPool`, `diesel.storage.page.BufferPoolFlusher`, `diesel.storage.page.PageFlusher`, `diesel.PersistFlusher` | Диск/кэш страниц |
| `net` | `diesel.net`, `diesel.LegacyProtocolAdapter`, `diesel.LibpqClientHandler` | TLS, libpq, codec |
| `security` | `diesel.security` | Auth, RBAC, AuditLogger |
| `concurrency` | `diesel.concurrency` | LockManager, DeadlockDetector, ConflictDetector |
| `backup` | `diesel.backup`, `diesel.DatabasePitrSink` | Physical/Logical/PITR backup |
| `observability` | `diesel.observability` | Metrics, Health, Prometheus |
| `schema` | `diesel.schema`, `diesel.AlterTableCopyAlgorithm`, `diesel.AlterTableAddColumnQuery`, `diesel.AlterTableDropColumnQuery` | Online ALTER, schema versions |
| `server` | `diesel.DatabaseServer`, `diesel.DieselDatabase`, `diesel.Database` | Server lifecycle, catalog |

### Приоритет над глобальным

Правило: **помодульный ключ выигрывает у глобального `logging.level`** — вверх
(`DEBUG` при global `INFO`) или вниз (`WARN` при global `DEBUG`).

Примеры:

| `logging.level` | `logging.level.wal` | Эффект |
| --- | --- | --- |
| `INFO` | (не задан) | Все модули — INFO. |
| `INFO` | `DEBUG` | WAL-логгеры — DEBUG; остальные — INFO. Оператор дебажит WAL не поднимая шума во всём движке. |
| `DEBUG` | `WARN` | Все модули — DEBUG, КРОМЕ WAL — WARN. Оператор включил глобальный DEBUG, но WAL шумит — от него только предупреждения. |
| `OFF` | `WARN` | Движок молчит, но `diesel.wal` всё равно пишет WARN+. Production: тишина + аварийные сигналы. |

### Алгоритм применения

`DieselLogConfig.applyModuleLevels(Properties config)`:

1. Считать все ключи вида `logging.level.<module>` из `config` (и из sysprop
   `-Dlogging.level.<module>=…`, если задан — sysprop приоритетнее).
2. Для каждого ключа разрешить `<module>` → SLF4J-логгер (один или список) по
   таблице выше. Неизвестный модуль → WARNING, ключ игнорируется.
3. Для каждого логгера вызвать `loggerContext.getLogger(name).setLevel(Level.toLevel(moduleLevel))`.
4. Установить уровень промежуточного родителя (например, `diesel.wal`) в
   moduleLevel, чтобы логгеры, созданные после init, унаследовали корректный
   уровень.
5. Вызывается из `init()` **после** установки глобального уровня.

### Runtime-переключение

`DieselLogConfig.reapply()` — public-метод. Перечитывает `config.properties` (или
overlay из тела запроса) и повторно применяет правила. Доступ через существующий
`MetricsHttpServer` admin endpoint (`POST /admin/logging` с JSON-телом) или через
JMX. Без runtime-переключения оператор вынужден перезапускать сервер для
диагностики — неприемлемо для production DB.

---

## 6. Асинхронный режим

### Ключи

```properties
logging.async.enabled         = false
logging.async.queue.size      = 4096
logging.async.discarding.threshold = 0     # 0 = никогда не discard INFO; logback default 20
logging.async.never.block     = true        # true = drop on full (production); false = block producer
logging.async.include.caller.data = false   # false = не генерировать stack trace (дорого)
```

По умолчанию **выключен** — синхронный вывод сохраняется, чтобы не менять
поведение тестовых профилей и сохранить простоту дебага в dev.

### Реализация (дизайн)

Используется встроенный `ch.qos.logback.classic.AsyncAppender` — собственная
реализация не пишется.

В `DieselLogConfig.init()`:

1. Если `logging.async.enabled=false` — ничего не делаем, appender’ы из
   `logback.xml` остаются синхронными (текущее поведение).
2. Если `logging.async.enabled=true`:
   - Создать `AsyncAppender async = new AsyncAppender();`
   - `async.setQueueSize(queueSize);` (default 4096).
   - `async.setDiscardingThreshold(discardingThreshold);` — при `0` logback
     никогда не отбрасывает INFO-записи при заполнении очереди (важно для
     DB audit-трейса).
   - `async.setNeverBlock(neverBlock);` — при `true` (default в нашем конфиге)
     `async.append()` никогда не блокирует вызывающий поток, drop-ы идут в
     счётчик.
   - `async.setIncludeCallerData(includeCallerData);` — `false` по умолчанию,
     генерация caller-data дорогая, для DB не нужно.
   - Взять существующие appender’ы root-логгера (`CONSOLE`, `FILE`) и
     **перенести** их в `async` как вложенные: `async.addAppender(console); async.addAppender(file);`.
   - Очистить appender’ы root-логгера и добавить только `async`:
     `root.detachAndStopAllAppenders(); root.addAppender(async);`.
   - `async.start();`
3. Если глобальный уровень = `OFF` — async не запускается (ранний return).

### Drop-метрики

logback `AsyncAppender` сам по себе не экспортирует счётчик дропов в
`MetricsRegistry`. В `DieselLogConfig.init()` после создания async добавить
`AsyncAppenderStatusListener` (через `async.addStatusListener(...)`) который
по событию `WARN` типа «discarded N events» инкрементит gauge
`diesel.logging.dropped.count` в существующем `diesel.observability.MetricsRegistry`.

### Когда включать

| Сценарий | Рекомендация |
| --- | --- |
| Production OLTP с `logging.level=INFO` | `logging.async.enabled=true` — убирает задержку `LOGGER.info` на горячих путях (WAL writes, buffer pool flush). |
| Dev / debug с `logging.level=DEBUG` | `logging.async.enabled=false` — синхронный вывод читаем, sync stack-traces точные. |
| Benchmarks (`timing/`, `PerformanceRegressionTest`) | `logging.async.enabled=true` + `logging.level=WARN` (через `logback-large.xml`) — минимальное влияние на измерения. |

### Альтернатива: async через logback.xml (без программной настройки)

Можно сделать всё статически в `logback.xml`:

```xml
<appender name="ASYNC" class="ch.qos.logback.classic.AsyncAppender">
    <queueSize>4096</queueSize>
    <discardingThreshold>0</discardingThreshold>
    <neverBlock>true</neverBlock>
    <includeCallerData>false</includeCallerData>
    <appender-ref ref="CONSOLE"/>
    <appender-ref ref="FILE"/>
</appender>
<root level="INFO">
    <appender-ref ref="ASYNC"/>
</root>
```

Но тогда нельзя включать/выключать через `config.properties` (требование
пользователя). Поэтому `DieselLogConfig` делает это программно на основе
`logging.async.enabled` из `config.properties`, и базовый `logback.xml` остаётся
синхронным (appender’ы `CONSOLE` + `FILE` прямо на root). `DieselLogConfig`
оборачивает их в `AsyncAppender` при включённом флаге.

---

## 7. Изменения `config.properties`

Конкретный diff-блок для секции 1–7 файла `config.properties` (строки 3–7
существующего файла):

```diff
- java.util.logging.ConsoleHandler.level = INFO
- logging.level.diesel=INFO
- diesel.SelectQueryParser.level = INFO
- diesel.QueryParser.level=INFO
- diesel.ConditionParser.level = INFO

+ ## ─── Logging (LOGGING.md) ─────
+ ## Global logging level for SLF4J/logback. Accepted (case-insensitive):
+ ## OFF, ERROR, WARN, INFO, DEBUG, TRACE, ALL.
+ ## JUL aliases also accepted: SEVERE=ERROR, WARNING=WARN, FINE=DEBUG,
+ ## FINER=TRACE, FINEST=TRACE. Default: INFO (from logback.xml <root level>).
+ ## Sysprop -Dlogging.level=<…> wins over this key.
+ ## Backend: SLF4J 2.0.12 + logback-classic 1.5.6 (both already in pom.xml).
+ logging.level = INFO
+
+ ## Async logging pipeline (default off; turn on in production OLTP).
+ ## When enabled, DieselLogConfig wraps CONSOLE+FILE appenders from logback.xml
+ ## into a single ch.qos.logback.classic.AsyncAppender drained by a daemon
+ ## 'logback-async' thread. See LOGGING.md §6.
+ logging.async.enabled              = false
+ logging.async.queue.size           = 4096
+ logging.async.discarding.threshold = 0       # 0 = never discard INFO/WARN
+ logging.async.never.block          = true     # true = drop on full (prod)
+ logging.async.include.caller.data  = false    # false = no stack-trace generation
+
+ ## MDC / per-query correlation ID. When true, DieselLogConfig registers a
+ ## turbo filter that injects a per-thread traceId via org.slf4j.MDC and the
+ ## logback pattern includes [%X{traceId:-}] in the output.
+ ## Sources of traceId: libpq startup 'trace_id' parameter, else UUID short.
+ logging.trace.enabled = false
+
+ ## Optional JSON format for Loki/ELK. text (default) | json.
+ ## When json, DieselLogConfig swaps the <encoder> of CONSOLE+FILE appenders
+ ## to a JSON encoder emitting one line per record with fields:
+ ## ts, level, logger, thread, traceId, msg, args.
+ # logging.format = json
+
+ ## Secrets redaction (default ON). A TurboFilter matches record messages
+ ## against patterns covering tls.keystore.password, *.secret, *.token and
+ ## replaces the value with <redacted>. Disable only in trusted dev envs.
+ logging.redaction.enabled = true
+
+ ## Per-module overrides — UNCOMMENT TO ENABLE.
+ ## Module keys take PRIORITY over the global logging.level above:
+ ## a module can be set to a more verbose level (DEBUG while global is INFO)
+ ## OR to a less verbose one (WARN while global is DEBUG).
+ ## See LOGGING.md §5 for the full module → SLF4J-logger-name mapping.
+ # logging.level.parser        = DEBUG
+ # logging.level.query         = DEBUG
+ # logging.level.wal           = DEBUG
+ # logging.level.recovery      = DEBUG
+ # logging.level.replication   = DEBUG
+ # logging.level.storage       = DEBUG
+ # logging.level.bufferpool    = DEBUG
+ # logging.level.net           = DEBUG
+ # logging.level.security      = DEBUG
+ # logging.level.concurrency   = DEBUG
+ # logging.level.backup        = DEBUG
+ # logging.level.observability = DEBUG
+ # logging.level.schema        = DEBUG
+ # logging.level.server        = DEBUG
+
+ ## Sampling for hot paths (1 record out of N). Useful when global=DEBUG
+ ## would flood WAL writes / streaming applier / buffer-pool flusher.
+ ## A TurboFilter drops (N-1) of N records BEFORE formatting (cost saver).
+ # logging.sampling.wal         = 100
+ # logging.sampling.bufferpool  = 100
+ # logging.sampling.replication = 50
+
+ ## Deprecated alias — kept for backward compat with test profiles
+ ## large/perf/timing-quick that pass -Dlogging.level.diesel=WARNING.
+ ## Resolved by DieselLogConfig as alias for logging.level; emits one
+ ## WARNING on startup advising migration.
+ # logging.level.diesel = INFO
```

Ключевое: **помодульные переключатели (`# logging.level.<module>=DEBUG`) приведены
закомментированными** — рабочий конфиг по умолчанию имеет один глобальный
`logging.level = INFO` и не включает ни одного помодульного override. Это явное
требование пользователя и принцип из §3.

Дополнительно, существующий `src/main/resources/logback.xml` остаётся как
базовая конфигурация appender’ов (CONSOLE + FILE + rolling policy). `DieselLogConfig`
не переписывает appender’ы — он только применяет уровни и фильтры к
`LoggerContext` поверх того, что уже настроил logback.

---

## 8. Что улучшить или устранить в системе логирования

Помимо самих переключателей, аудит выявил шесть проблем. Каждая — отдельный
пункт в Phase 4 промпта ниже.

### 8.1 Утечка секретов в `DatabaseServer.logConfig`

**Симптом.** Метод `DatabaseServer.logConfig` (строки 182–190) на старте сервера
печатает все ключи `config.properties` на INFO:
`config.forEach((k,v) → LOGGER.log(Level.INFO, "Config: {0} = {1}", …))`. В
`config.properties` есть `tls.keystore.password = changeit` (строка 579),
`tls.sni.*.keystore.password`, `tls.truststore.password`. Все они попадают в
stdout / `logs/diesel.log`. Комментарий в `config.properties:575` прямо требует
«Keystore password (never logged)» — реализация нарушает это правило.

**Решение (SLF4J + logback).** Внедрить `DieselRedactionTurboFilter extends
ch.qos.logback.classic.turbo.TurboFilter`. Регистрируется на `LoggerContext`
в `DieselLogConfig.init()` при `logging.redaction.enabled=true` (по умолчанию
ON). Фильтр срабатывает на всех уровнях, для каждого `ILoggingEvent`:

1. Если `marker` или `formattedMessage` содержит паттерн вида
   `key = value` где key подходит под regex
   `.*(password|passwd|secret|token|privateKey|keystore\.password|truststore\.password).*`
   — заменить value на `<redacted>`.
2. Для `DatabaseServer.logConfig` специфически — переписать метод так, чтобы
   он печатал только whitelisted-ключи (`storage.*`, `server.*`, `page.*`,
   `bufferpool.*`, `deadlock.*`, `lock.*`, `query.cache.*`, `libpq.*`). Это
   безопаснее, чем regex: whitelist не промахнётся.

`TurboFilter` вызывается **до** форматирования — то есть секрет не попадёт в
encoder даже если фильтр не нашёл совпадения (потому что `logConfig`
переписан whitelist’ом), а фильтр защищает от случайных будущих регрессий.

### 8.2 Завершить миграцию JUL → SLF4J

**Симптом.** 15 legacy-классов (`DatabaseServer`, `Database`, `Table`,
`BTreeIndex`, `BTreeClusteredIndex`, `QueryParser` с собственным static-init,
`SelectQuery`, `InsertQuery`, `UpdateQuery`, `DeleteQuery`, `QueryExecutor`,
`QueryOptimizer`, `RecoveryManager`, `ReplicationCoordinator`, `AuditLogger`,
`AvroAuditLogger`) используют `java.util.logging.Logger` напрямую. Это создаёт
**две параллельные системы логирования**: 30 классов на SLF4J → logback, 15 на
JUL → тоже logback (потому что slf4j-jdk14 не подключён, а JUL-LogManager
пишет в ConsoleAppender минуя logback-appender’ы). Итог: логи legacy-классов
не попадают в `logs/diesel.log`, не ротируются, не подчиняются уровням logback.

**Решение.** Завершить миграцию — заменить во всех 15 файлах:

```java
// Было:
import java.util.logging.Logger;
import java.util.logging.Level;
private static final Logger LOGGER = Logger.getLogger(MyClass.class.getName());
// ...
LOGGER.log(Level.INFO, "msg {0}", arg);
LOGGER.log(Level.SEVERE, "msg", exception);

// Стало:
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
private static final Logger LOGGER = LoggerFactory.getLogger(MyClass.class);
// ...
LOGGER.info("msg {}", arg);
LOGGER.error("msg", exception);
```

Особо важно:

- **`QueryParser.java`** (строки 46–81): удалить весь static-init с
  ConsoleHandler и чтением `logging.level.diesel`. Заменить на одну строку
  `private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);`.
- **`DatabaseServer.java`**: `import org.slf4j.Logger;` заменить JUL на SLF4J.
  `logConfig` переписать на whitelist (см. §8.1).
- **`AuditLogger.java`** и **`AvroAuditLogger.java`**: это собственные
  async-audit подсистемы (bounded queue, gzip archive, retention). Их
  **diagnostic**-логи (warning’и про dropped records, retention failures)
  мигрируем на SLF4J; сами audit-записи остаются в их собственных
  backend’ах (`DIESEL_AUDIT_LOG` таблица / avro-файлы). НЕ трогать
  architecture audit-подсистем, только LOGGER-поле.

### 8.3 MDC / correlation ID

**Симптом.** В логе движка нет способа связать записи одного SQL-запроса: парсер,
executor, WAL-write, lock-acquire, flush — все пишут независимо. При
расследовании инцидента невозможно отличить два параллельных UPDATE.

**Решение (SLF4J MDC — встроен).** Использовать `org.slf4j.MDC`:

```java
import org.slf4j.MDC;
// На accept-е ClientHandler:
String traceId = libpqStartupParam("trace_id");
if (traceId == null) traceId = UUID.randomUUID().toString().substring(0, 8);
MDC.put("traceId", traceId);
try {
    handleQuery(...);
} finally {
    MDC.clear();  // важно для thread pool reuse
}
```

В `logback.xml` обновить pattern (один раз, глобально):

```xml
<pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] [%X{traceId:-}] %-5level %logger{36} - %msg%n</pattern>
```

`%X{traceId:-}` вставляет значение из MDC, или пустую строку если ключа нет.
Никаких собственных ThreadLocal + Formatter — SLF4J MDC уже это делает, и
logback автоматически прокидывает MDC в `AsyncAppender` через
`MDCAdapter` (важно для async-режима: MDC-контекст переносится в
consumer-thread).

Ключ `logging.trace.enabled = false` (default off — extra cost на MDC.put/clear
per request). Для libpq admin / debug — включается явно.

### 8.4 Расширенный file rotation

**Симптом.** Существующий `logback.xml` уже имеет `RollingFileAppender` с
`SizeAndTimeBasedRollingPolicy` (10MB / 30 days / 100MB total). Это базовая
настройка — для production DB желательно больше контроля: путь, размер,
retention, archive compression должны браться из `config.properties`, чтобы
operator менял без редактирования `logback.xml` (который в jar).

**Решение.** В `DieselLogConfig.init()` после загрузки `LoggerContext`:

1. Найти appender `FILE` (`RollingFileAppender`) в root-логгере.
2. Если `logging.file.path` задан в `config.properties` — переопределить
   `((FileAppender) fileAppender).setFile(path);`
3. Достать `rollingPolicy` (`SizeAndTimeBasedRollingPolicy`):
   - `setMaxFileSize(FileSize.valueOf(maxSizeMb + "MB"));`
   - `setMaxHistory(retentionDays);`
   - `setTotalSizeCap(FileSize.valueOf(totalCapMb + "MB"));`
   - `setFileNamePattern(path + ".%d{yyyy-MM-dd}.%i.log.gz");`
4. `rollingPolicy.start();` (перезапуск policy с новыми параметрами).

Ключи в `config.properties`:

```properties
# logging.file.path          = data/log/diesel.log
# logging.file.max.size.mb   = 50
# logging.file.retention.days = 30
# logging.file.total.cap.mb  = 500
```

По умолчанию все ключи закомментированы — используются значения из
`logback.xml` (10MB / 30 days / 100MB). Operator раскомментирует для
production-tuning.

### 8.5 Structured / JSON формат

**Симптом.** logback `<pattern>` выдаёт free-text (`2025-12-12 16:33:21.123
[main] [ab12cd34] INFO  diesel.DatabaseServer - Configuration parameters loaded`).
Это удобно для grep, но неудобно для интеграции с Loki/ELK/CloudWatch, где
нужны structured fields.

**Решение (logback encoder).** Использовать `LogstashEncoder` из
`net.logstash.logback:logstash-logback-encoder` (Apache 2.0, дополнительная
зависимость) или собственный `Layout` (без новой зависимости). Рекомендация —
начать с собственного layout (минимум зависимостей), мигрировать на
LogstashEncoder если потребуется полнофункциональный JSON.

В `DieselLogConfig.init()` при `logging.format=json`:

1. Найти appender’ы `CONSOLE` и `FILE`.
2. Заменить их `<encoder>` на `JsonLayout` (или `LogstashEncoder`):
   ```java
   LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
   JsonLayout layout = new JsonLayout();
   layout.setAppendLineSeparator(true);
   layout.setContext(ctx);
   layout.start();
   ((ConsoleAppender<ILoggingEvent>) console).setLayout(layout);
   ```
3. JSON-строка на запись: `{"ts":"2025-12-12T16:33:21.123+03:00",
   "level":"INFO","logger":"diesel.DatabaseServer","thread":"main",
   "traceId":"ab12cd34","msg":"...","args":[...]}`.

### 8.6 Sampling hot-path

**Симптом.** Если operator включает `logging.level.wal=DEBUG` на production,
WAL пишет десятки тысяч записей в секунду (`WALWriter.append` вызывается на
каждый INSERT). Это убивает throughput и переполняет очередь AsyncAppender.
Аналогично — `BufferPoolFlusher.flush`, `StreamingApplier.apply`.

**Решение (TurboFilter).** Создать `DieselSamplingTurboFilter extends
ch.qos.logback.classic.turbo.TurboFilter`:

- Конфигурация: `Map<String, AtomicLong> counters` keyed by logger name;
  `Map<String, Integer> sampleRates` из `logging.sampling.<module>=N`.
- `decide(Marker marker, Logger logger, Level level, String format,
  Object[] params, Throwable t)`:
  - Найти подходящий module для `logger.getName()` (по префиксу).
  - Если sampling не задан для этого module → `FilterReply.NEUTRAL`.
  - Иначе: `if (counter.incrementAndGet() % N == 0) NEUTRAL; else DENY;`.
  - DENY означает «отбросить ДО форматирования» — экономия на encoder/pattern.

`TurboFilter` регистрируется на `LoggerContext` в `DieselLogConfig.init()`.
По умолчанию sampling выключен (строки `# logging.sampling.*` в §7).

---

## 9. Промпты для реализации

Пять промптов, разбитых по фазам. Каждый — самодостаточный: контекст, ключи
конфига, чек-лист приёмки. Прогонять по порядку; каждая фаза должна
заканчиваться зелёным `mvn test` перед переходом к следующей.

---

### Phase 0 — Завершить миграцию JUL → SLF4J в legacy-классах

```text
КОНТЕКСТ. Репозиторий github.com/Reider85/dieseldb, ветка main. Java 21, Maven.
Зависимости в pom.xml: slf4j-api 2.0.12, logback-classic 1.5.6 — оба в
production scope. logback.xml уже существует в src/main/resources/.
30+ классов уже используют org.slf4j.Logger/LoggerFactory.
15 legacy-классов всё ещё на java.util.logging.Logger:
  diesel/DatabaseServer.java, diesel/Database.java, diesel/Table.java,
  diesel/BTreeIndex.java, diesel/BTreeClusteredIndex.java,
  diesel/QueryParser.java (строки 46-81 — собственный static-init с
    ConsoleHandler, БЕЗ него), diesel/SelectQuery.java, diesel/InsertQuery.java,
  diesel/UpdateQuery.java, diesel/DeleteQuery.java, diesel/QueryExecutor.java,
  diesel/QueryOptimizer.java, diesel/RecoveryManager.java,
  diesel/ReplicationCoordinator.java, diesel/security/AuditLogger.java,
  diesel/storage/avro/AvroAuditLogger.java.

ЗАДАЧА. Заменить во всех перечисленных файлах JUL на SLF4J:
- import java.util.logging.Logger; → import org.slf4j.Logger;
- import java.util.logging.Level; → удалить (использовать методы logger.info/warn/error)
- private static final Logger LOGGER = Logger.getLogger(X.class.getName());
    → private static final Logger LOGGER = LoggerFactory.getLogger(X.class);
- LOGGER.log(Level.INFO, "msg {0}", arg); → LOGGER.info("msg {}", arg);
- LOGGER.log(Level.WARNING, "msg", e); → LOGGER.warn("msg", e);
- LOGGER.log(Level.SEVERE, "msg", e); → LOGGER.error("msg", e);
- LOGGER.log(Level.FINE, "msg"); → LOGGER.debug("msg");
- LOGGER.log(Level.FINER, "msg"); → LOGGER.trace("msg");
- LOGGER.info("msg " + arg); → LOGGER.info("msg {}", arg); (parameterized)
- LOGGER.isLoggable(Level.FINE) → LOGGER.isDebugEnabled();

СПЕЦИФИЧЕСКИЕ ФАЙЛЫ:
- QueryParser.java строки 46-81: удалить ВЕСЬ static-блок с ConsoleHandler и
  чтением logging.level.diesel. Заменить на:
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);
- AuditLogger.java и AvroAuditLogger.java: это async audit subsystems
  (bounded queue + gzip retention). МИГРИРУЕМ ТОЛЬКО LOGGER-поле (диагностические
  warning'и про dropped records, retention failures). Сама audit-queue,
  consumer-thread, backend, retention — НЕ ТРОГАТЬ. AuditLogger.java строки
  1-46: импорты и LOGGER-поле — заменить JUL на SLF4J. Строки 90-189 (create,
  close, consumeLoop) — без изменений логики, только LOGGER.log(...) → LOGGER.warn(...).
- DatabaseServer.java logConfig (строки 182-190): пока НЕ переписывать на
  whitelist — это Phase 4. Сейчас просто заменить JUL на SLF4J с тем же
  forEach-циклом (sec-фильтр добавим в Phase 4).

РЕГРЕССИЯ. После миграции все 15 классов должны писать логи через logback
(т.е. в logs/diesel.log), не через отдельный JUL ConsoleAppender. Verify:
 1. mvn test — зелёный.
 2. grep 'java.util.logging' diesel/ — должен вернуть 0 совпадений в
    перечисленных 15 файлах (проверить rg).
 3. В логах PerformanceRegressionTest нет дублей: ранее QueryParser писал
    через свой ConsoleHandler + DatabaseServer через JUL-root ConsoleHandler,
    плюс logback-CONSOLE. После миграции — только logback-CONSOLE.

ЧЕК-ЛИСТ ПРИЁМКИ.
- [ ] mvn test зелёный.
- [ ] rg 'java.util.logging.Logger' diesel/ возвращает 0 в 15 перечисленных файлах.
- [ ] QueryParser.java: статический блок 46-81 удалён, осталась одна строка
      LOGGER = LoggerFactory.getLogger(...).
- [ ] Slf4jLogCapture продолжает работать (тесты используют его без изменений).
- [ ] logback-large.xml (test profile) продолжает подавлять INFO — verify
      PerformanceRegressionTest зелёный.
```

---

### Phase 1 — Единый переключатель уровня через DieselLogConfig

```text
КОНТЕКСТ. После Phase 0 все классы используют SLF4J+logback. logback.xml
настраивает appender'ы (CONSOLE + FILE с rolling policy), root level=INFO.
config.properties содержит только устаревшие строки 3-7 (мёртвые) — их
надо удалить.

ЗАДАЧА. Создать класс diesel/logging/DieselLogConfig.java:
- public static void init() — вызывается первой строкой в
  DatabaseServer.start() (строка ~223), ПОСЛЕ loadConfig() и ДО logConfig.
- Читает уровень из приоритетной цепочки:
  1) sysprop -Dlogging.level=<…>
  2) logging.level из config.properties (через ConfigLoader.load())
  3) default INFO (из logback.xml <root level>)
- Принимает alias-ы: SEVERE=ERROR, WARNING=WARN, CONFIG=INFO, FINE=DEBUG,
  FINER=TRACE, FINEST=TRACE (case-insensitive). Неизвестное значение →
  WARNING + default INFO.
- Получает LoggerContext: (LoggerContext) LoggerFactory.getILoggerFactory().
- Устанавливает уровень root-логгера:
    loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.toLevel(resolved));
- Устанавливает уровень пакетного логгера 'diesel' в то же значение.
- Не трогает appender'ы (они из logback.xml).
- Backward compat: sysprop -Dlogging.level.diesel=<…> принимается как alias
  к -Dlogging.level. При обнаружении — ОДНО WARNING через SLF4J:
    LoggerFactory.getLogger(DieselLogConfig.class)
      .warn("Deprecated sysprop -Dlogging.level.diesel used; use -Dlogging.level instead");

КОНФИГ. В config.properties удалить строки 3-7 (старые мёртвые JUL-ключи),
добавить:

  logging.level = INFO

  ## Deprecated alias — kept for test profiles large/perf/timing-quick.
  # logging.level.diesel = INFO

ТЕСТЫ. Создать src/test/java/diesel/logging/DieselLogConfigTest.java:
- globalLevelFromConfigProperties — logging.level=DEBUG в test-config →
  root logger level DEBUG, logger 'diesel' level DEBUG.
- syspropOverridesConfig — -Dlogging.level=WARN + logging.level=DEBUG в
  config → root level WARN.
- aliasAccepted — logging.level=ERROR → root level ERROR (logback Level.ERROR).
- julAliasesAccepted — logging.level=FINE → root level DEBUG; logging.level=SEVERE → ERROR.
- unknownValueDefaultsToInfoWithWarning — logging.level=NOPE → INFO +
  перехваченное через Slf4jLogCapture WARNING.
- deprecatedAliasEmitsWarning — -Dlogging.level.diesel=DEBUG → root level
  DEBUG + ОДНО WARNING про deprecated (через Slf4jLogCapture).
- existingLoggingLevelDieselSyspropStillWorks — regression: 
  -Dlogging.level.diesel=WARNING подавляет INFO-вывод QueryParser
  (воспроизвести сценарий из комментария QueryParser.java:56-60 —
  проверить что INFO-сообщения парсера не попадают в Slf4jLogCapture).

ЧЕК-ЛИСТ ПРИЁМКИ.
- [ ] mvn test зелёный.
- [ ] При logging.level=OFF движок не форматирует сообщения (verify via
      MetricsRecordingOverheadTest — overhead не должен вырасти).
- [ ] Удалены строки 3-7 старого config; logging.level=INFO работает.
- [ ] -Dlogging.level.diesel=WARNING для timing-quick профилей продолжает
      подавлять INFO (без этого сломается PerformanceRegressionTest).
- [ ] rg 'logging.level.diesel' diesel/ — только deprecated alias в
      DieselLogConfig, нигде больше.
```

---

### Phase 2 — Помодульные переключатели с приоритетом

```text
КОНТЕКСТ. После Phase 1 есть diesel/logging/DieselLogConfig.java с единым
logging.level. Нужно добавить помодульные переключатели с ПРИОРИТЕТОМ над
глобальным.

ЗАДАЧА. Расширить DieselLogConfig:
- Новый public static void applyModuleLevels(Properties config).
  Вызывается из init() ПОСЛЕ установки глобального уровня.
- Парсит все ключи вида logging.level.<module>=<level> из config и из sysprop
  (-Dlogging.level.<module>=…, sysprop приоритетнее).
- Таблица module → SLF4J logger name (зашить в DieselLogConfig как static Map
 <String, List<String>>):

    parser       → [diesel.QueryParser, diesel.SelectQueryParser,
                    diesel.SubqueryParser, diesel.ConditionParser,
                    diesel.ParseContext, diesel.SqlParsingUtils]
    query        → [diesel.SelectQuery, diesel.InsertQuery, diesel.UpdateQuery,
                    diesel.DeleteQuery, diesel.QueryExecutor, diesel.QueryOptimizer,
                    diesel.PreparedStatement]
    wal          → [diesel.wal]
    recovery     → [diesel.RecoveryManager, diesel.recovery, diesel.DatabaseRecoverySink]
    replication  → [diesel.ReplicationCoordinator,
                    diesel.SyncReplicationCoordinator, diesel.replication]
    storage      → [diesel.storage]
    bufferpool   → [diesel.storage.page.BufferPool,
                    diesel.storage.page.BufferPoolFlusher,
                    diesel.storage.page.PageFlusher, diesel.PersistFlusher]
    net          → [diesel.net, diesel.LegacyProtocolAdapter,
                    diesel.LibpqClientHandler]
    security     → [diesel.security]
    concurrency  → [diesel.concurrency]
    backup       → [diesel.backup, diesel.DatabasePitrSink]
    observability→ [diesel.observability]
    schema       → [diesel.schema, diesel.AlterTableCopyAlgorithm,
                    diesel.AlterTableAddColumnQuery, diesel.AlterTableDropColumnQuery]
    server       → [diesel.DatabaseServer, diesel.DieselDatabase, diesel.Database]

- Для каждого module-key:
  1. resolve module → list of SLF4J logger names.
  2. resolve level (chain: sysprop → config → no override → skip).
  3. Для каждого logger name:
     loggerContext.getLogger(name).setLevel(Level.toLevel(moduleLevel));
  4. Если name — пакетный префикс (например 'diesel.wal'), он же будет
     родителем для будущих логгеров этого пакета — level унаследуется.

- Неизвестный module → WARNING через SLF4J, ключ игнорируется (не падать).
- public static void reapply() — перечитывает config + sysprop, повторно
  применяет глобальный + помодульный уровни. Runtime-переключение.

ПРИОРИТЕТ. Module key ПОБЕЖДАЕТ global logging.level — вверх (DEBUG при
global INFO) или вниз (WARN при global DEBUG). Это ключевое требование.

КОНФИГ. В config.properties (после logging.level = INFO из Phase 1) добавить
закомментированный блок:

  ## Per-module overrides — UNCOMMENT TO ENABLE.
  ## Module keys take PRIORITY over the global logging.level above.
  ## See LOGGING.md §5 for the full module → SLF4J-logger mapping.
  # logging.level.parser        = DEBUG
  # logging.level.query         = DEBUG
  # logging.level.wal           = DEBUG
  # logging.level.recovery      = DEBUG
  # logging.level.replication   = DEBUG
  # logging.level.storage       = DEBUG
  # logging.level.bufferpool    = DEBUG
  # logging.level.net           = DEBUG
  # logging.level.security      = DEBUG
  # logging.level.concurrency   = DEBUG
  # logging.level.backup        = DEBUG
  # logging.level.observability = DEBUG
  # logging.level.schema        = DEBUG
  # logging.level.server        = DEBUG

Важно: ни одна строка не раскомментирована по умолчанию.

ТЕСТЫ. Добавить в DieselLogConfigTest:
- moduleOverridesGlobalUpward — logging.level=INFO +
  logging.level.wal=DEBUG → logger 'diesel.wal.WALWriter' level DEBUG,
  logger 'diesel.DatabaseServer' level INFO.
- moduleOverridesGlobalDownward — logging.level=DEBUG +
  logging.level.wal=WARN → 'diesel.wal.WALWriter' level WARN,
  'diesel.DatabaseServer' level DEBUG.
- moduleSyspropOverridesConfig — logging.level.wal=INFO в config +
  -Dlogging.level.wal=DEBUG → WAL level DEBUG.
- unknownModuleLogsWarningAndIgnored — logging.level.foobar=DEBUG → WARNING,
  глобальный уровень не меняется.
- moduleAffectsLoggersCreatedAfterInit — получить logger
  'diesel.wal.NewClass' ПОСЛЕ init, его effective level = moduleLevel
  (через inheritance от diesel.wal).
- reapplyChangesLevelAtRuntime — init с INFO, затем reapply с config где
  logging.level.wal=DEBUG → WAL logger переключился на DEBUG без рестарта JVM.

ЧЕК-ЛИСТ ПРИЁМКИ.
- [ ] mvn test зелёный.
- [ ] В config.properties блок помодульных ключей полностью закомментирован.
- [ ] При раскомментировании # logging.level.wal=DEBUG и logging.level=INFO
      только WAL-логгеры переходят на DEBUG (verify через
      Slf4jLogCapture: diesel.wal.WALWriter DEBUG-сообщения проходят,
      diesel.DatabaseServer INFO-сообщения проходят, DEBUG-сообщения
      DatabaseServer не проходят).
- [ ] При logging.level=OFF + logging.level.security=WARN security-логгер
      всё равно пишет WARN+.
- [ ] Runtime reapply работает через POST /admin/logging (если endpoint есть)
      или через JMX-вызов DieselLogConfig.reapply().
```

---

### Phase 3 — Асинхронный режим через AsyncAppender

```text
КОНТЕКСТ. После Phase 2 есть DieselLogConfig с глобальным + помодульными
уровнями. Сейчас весь logback-вывод синхронный — LOGGER.info в
WALWriter.append и BufferPoolFlusher.flush блокирует вызывающий поток на
форматирование + write в CONSOLE + FILE. Нужно добавить опциональный
async-режим через встроенный ch.qos.logback.classic.AsyncAppender.

ЗАДАЧА. В DieselLogConfig.init() после установки уровней:
- Прочитать logging.async.enabled (default false). Если false — ничего не
  делать, appender'ы из logback.xml остаются синхронными (текущее поведение).
- Если true:
  1. Получить root logger (ch.qos.logback.classic.Logger):
        Logger root = (Logger) loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
  2. Получить текущие appender'ы root:
        ConsoleAppender<ILoggingEvent> console = (ConsoleAppender)
            root.getAppender("CONSOLE");
        RollingFileAppender<ILoggingEvent> file = (RollingFileAppender)
            root.getAppender("FILE");
  3. Создать AsyncAppender:
        AsyncAppender async = new AsyncAppender();
        async.setContext(loggerContext);
        async.setQueueSize(queueSize);             // default 4096
        async.setDiscardingThreshold(discardingThreshold);  // default 0
        async.setNeverBlock(neverBlock);           // default true
        async.setIncludeCallerData(includeCallerData); // default false
  4. Перенести CONSOLE и FILE в async как вложенные:
        async.addAppender(console);
        async.addAppender(file);
  5. Очистить appender'ы root и добавить только async:
        root.detachAppender(console);
        root.detachAppender(file);
        root.addAppender(async);
  6. async.start();
  7. Зарегистрировать status listener для подсчёта дропов:
        async.addStatusListener(new OnPrintStatusListenerBase() {
          @Override public void addStatusEvent(Status status) {
            if (status.getLevel() == Status.WARN &&
                status.getMessage().contains("discarded")) {
              MetricsRegistry.gauge("diesel.logging.dropped.count")
                .incrementAndGet();
            }
          }
        });
        // или проще — PeriodicQueueDrain с регулярным опросом async.getNumberOfDiscardedEvents()
        // в MetricsHttpServer.

- Если глобальный уровень = OFF — async не запускается (early return).

КОНФИГ. В config.properties (после блока из Phase 2) добавить:

  logging.async.enabled              = false
  logging.async.queue.size           = 4096
  logging.async.discarding.threshold = 0       # 0 = never discard INFO/WARN
  logging.async.never.block          = true     # true = drop on full (prod)
  logging.async.include.caller.data  = false    # false = no stack-trace gen

ТЕСТЫ. Создать DieselAsyncAppenderTest.java (через Slf4jLogCapture +
ListAppender под AsyncAppender):
- asyncDisabledByDefault — logging.async.enabled не задан → root appender'ы
  остались CONSOLE и FILE напрямую, без AsyncAppender wrapper.
- asyncEnabledWrapsConsoleAndFile — logging.async.enabled=true → root имеет
  один appender AsyncAppender, в нём вложены CONSOLE и FILE.
- recordsAreDeliveredAsync — пушим 100 записей, ждём drain через
  awaitility (await().atMost(500, MS).until(...)) — ListAppender под async
  получил все 100 в порядке FIFO.
- neverBlockDropsOnFull — queue.size=10, neverBlock=true, пушим 100 →
  dropped count > 0, ListAppender получил ≤ queue.size+buffer.
- offLevelDoesNotStartAsyncThread — logging.level=OFF +
  logging.async.enabled=true → AsyncAppender не создан, поток
  'logback-async' не запущен (verify via ThreadMXBean или jstack-like check).
- metricsRegistryExposesDroppedCount — после дропов gauge
  'diesel.logging.dropped.count' > 0 в MetricsRegistry (verify через
  existing MetricsRegistryTest).
- shutdownFlushesQueue — shutdown LoggerContext (loggerContext.stop()) →
  все queued записи дошли до downstream ListAppender.

ЧЕК-ЛИСТ ПРИЁМКИ.
- [ ] mvn test зелёный.
- [ ] При logging.async.enabled=false поведение идентично Phase 2
      (синхронный вывод).
- [ ] При logging.async.enabled=true + logging.level=DEBUG throughput
      WALWriter.append не падает более чем на 5% относительно
      logging.level=INFO (verify: WALWriterThroughputTest зелёный).
- [ ] GracefulShutdownTest остаётся зелёным (записи в очереди не теряются
      при stop()).
- [ ] В MetricsRegistry появился gauge 'diesel.logging.dropped.count'.
```

---

### Phase 4 — Фиксы системы логирования (5 подзадач)

```text
КОНТЕКСТ. После Phase 3 есть DieselLogConfig (глобальный + помодульный +
async) на SLF4J+logback. Аудит выявил пять проблем сверх переключателей:

  8.1 DatabaseServer.logConfig печатает tls.keystore.password на INFO.
  8.3 Нет MDC/correlation ID — записи одного запроса нельзя связать.
  8.4 File rotation параметризован только в logback.xml, не из config.properties.
  8.5 Нет JSON-формата для Loki/ELK.
  8.6 На hot-path DEBUG-логи захлебывают очередь (WAL/BufferPoolFlusher).

(Phase 0 уже закрыл проблему 8.2 — миграцию JUL→SLF4J.)

ЗАДАЧА. Пять независимых подзадач, каждая — отдельный коммит.

4.1 REDACTION TURBO FILTER. Создать diesel/logging/DieselRedactionTurboFilter.java
    (extends ch.qos.logback.classic.turbo.TurboFilter). Регистрируется на
    LoggerContext в DieselLogConfig.init() при logging.redaction.enabled=true
    (default ON). decide() проверяет formattedMessage и args:
      - regex по ключу config: .*\.(password|passwd|secret|token|privateKey).*
      - regex по полной строке: tls\..*password, audit\.archive\.dir
      - при совпадении — вернуть FilterReply.DENY (запись не попадает в
        encoder). Это radicial: лучше потерять запись, чем слить секрет.
      - Для записей DatabaseServer.logConfig (формат "Config: {0} = {1}")
        ТОЛЬКО заменить value на "<redacted>" (не DENY): для этого
        TurboFilter должен создать новый ILoggingEvent с замаскированным
        arg[1] и вернуть NEUTRAL. Реализация через
        LoggingEvent.deepCopy() или создание нового LoggingEvent.
    Дополнительно: переписать DatabaseServer.logConfig (строки 182-190) на
    whitelist ключей: печатать только storage.*, server.*, page.*,
    bufferpool.*, deadlock.*, lock.*, query.cache.*, libpq.*. Остальные
    пропускать. Это безопаснее regex: whitelist не промахнётся.
    Тест DieselRedactionTurboFilterTest:
      - tls.keystore.password=changeit → в выводе "<redacted>" или запись
        отсутствует (verify через Slf4jLogCapture).
      - tls.sni.tenant1.example.com.keystore.password=secret → redacted.
      - server.socket.timeout=30000 → печатается как есть.
      - audit.archive.dir → не печатается (whitelist).
      - при logging.redaction.enabled=false — секреты попадают в лог
        (для dev/тестов redaction off).

4.2 MDC / TRACE ID. Создать diesel/logging/DieselTraceContext.java:
      - static String current() — MDC.get("traceId").
      - static void set(String id) — MDC.put("traceId", id).
      - static void clear() — MDC.remove("traceId"); (или MDC.clear() если
        других ключей нет).
      - static String generate() — UUID.randomUUID().toString().substring(0, 8).
    Источники traceId:
      - LibpqProtocol.java startup message: если клиент прислал параметр
        'trace_id' или 'ApplicationName', использовать его; иначе generate().
      - DatabaseServer.ClientHandler.run() — установить traceId на accept,
        clear() в finally.
      - QueryExecutor.execute() — НЕ устанавливать (наследуется от
        ClientHandler thread).
    В logback.xml обновить pattern (один раз):
      %d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] [%X{traceId:-}] %-5level %logger{36} - %msg%n
    Ключ logging.trace.enabled = false (default off — MDC.put/clear per request
    стоит на OLTP).
    При logging.trace.enabled=false pattern оставить без traceId (DieselLogConfig
    может программно подменить encoder pattern, ИЛИ всегда держать [%X{traceId:-}]
    в pattern — если MDC пуст, выводится пустая строка []; это дешевле чем
    переключать encoder).
    Рекомендация: ВТОРОЙ вариант — всегда [%X{traceId:-}] в pattern, и
    DieselTraceContext.set() вызывается только при logging.trace.enabled=true.
    Тест DieselTraceContextTest:
      - traceId присутствует в лог-записи только если DieselTraceContext.set()
        был вызван (verify через Slf4jLogCapture с pattern содержащим %X{traceId}).
      - После ClientHandler.run() finally — MDC.get("traceId") == null.
      - libpq клиент с trace_id="custom" → лог содержит [custom].

4.3 FILE ROTATION FROM CONFIG. В DieselLogConfig.init() после загрузки
    LoggerContext:
      - Найти appender "FILE" (RollingFileAppender) в root-логгере.
      - Если logging.file.path задан и непустой:
          ((FileAppender) fileAppender).setFile(path);
      - Достать rollingPolicy (SizeAndTimeBasedRollingPolicy):
          if (logging.file.max.size.mb задан):
            rollingPolicy.setMaxFileSize(FileSize.valueOf(maxSizeMb + "MB"));
          if (logging.file.retention.days задан):
            rollingPolicy.setMaxHistory(retentionDays);
          if (logging.file.total.cap.mb задан):
            rollingPolicy.setTotalSizeCap(FileSize.valueOf(totalCapMb + "MB"));
          rollingPolicy.setFileNamePattern(path + ".%d{yyyy-MM-dd}.%i.log.gz");
          rollingPolicy.start();  // restart with new params
      - Если путь не задан — использовать значения из logback.xml (10MB / 30 days / 100MB).
    Тест DieselFileRotationConfigTest:
      - logging.file.path=/tmp/diesel-test.log, max.size.mb=1 → после
        записи > 1MB файл ротируется в /tmp/diesel-test.log.yyyy-MM-dd.0.gz.
      - logging.file.path пустой → используется default из logback.xml.
      - retention.days=5 → после 5 дней старые архивы удаляются.

4.4 JSON FORMAT. Создать diesel/logging/DieselJsonLayout.java (extends
    ch.qos.logback.core.LayoutBase<ILoggingEvent>):
      - doLayout(ILoggingEvent event) → JSON line:
        {"ts":"<ISO-8601 with zone>","level":"<LEVEL>","logger":"<name>",
         "thread":"<threadName>","traceId":"<MDC traceId or empty>",
         "msg":"<escaped message>","args":[<escaped args>]}
      - Экранирование: \ → \\, " → \", newline → \n, control chars → \uXXXX.
    В DieselLogConfig.init() при logging.format=json:
      - Найти appender'ы CONSOLE и FILE в root-логгере.
      - Заменить их <encoder>/<pattern> на DieselJsonLayout:
          ConsoleAppender<ILoggingEvent> console = ...;
          DieselJsonLayout layout = new DieselJsonLayout();
          layout.setContext(loggerContext);
          layout.start();
          console.setLayout(layout);  // Layout-based instead of encoder
      - При logging.format=text (default) — не трогать, использовать pattern
        из logback.xml.
    Тест DieselJsonLayoutTest:
      - валидный JSON на выходе (parse через org.json.JSONObject или
        Jackson ObjectMapper — зависимости уже есть в проекте через Avro).
      - все требуемые поля присутствуют (ts, level, logger, thread, traceId, msg, args).
      - special chars в msg экранированы (verify parse не падает).
      - traceId попадает в JSON если MDC заполнен.
      - при logging.format=text pattern из logback.xml остаётся.

4.5 SAMPLING TURBO FILTER. Создать diesel/logging/DieselSamplingTurboFilter.java
    (extends ch.qos.logback.classic.turbo.TurboFilter):
      - Map<String, AtomicLong> counters keyed by logger name.
      - Map<String, Integer> sampleRates из logging.sampling.<module>=<N>.
      - decide(Marker, Logger, Level, String, Object[], Throwable):
        1. Найти module по logger.getName() (по префиксу из таблицы Phase 2).
        2. Если sampling не задан для module → NEUTRAL.
        3. Integer N = sampleRates.get(module);
           if (N == null || N <= 1) return NEUTRAL;
        4. long c = counters.computeIfAbsent(logger.getName(),
              k -> new AtomicLong()).incrementAndGet();
           if (c % N == 0) return NEUTRAL; else return DENY;
      - DENY = отбросить ДО форматирования (экономия на encoder).
    Регистрируется в DieselLogConfig.init() при наличии хотя бы одного
    logging.sampling.<module> ключа.
    Тест DieselSamplingTurboFilterTest:
      - sampleRate=100 → ровно 1 из 100 записей проходит (verify через
        Slf4jLogCapture: 100 LOGGER.debug() вызовов → 1 в ListAppender).
      - sampleRate=0 или 1 → все записи проходят.
      - sampling вычисляется ДО форматирования (verify: msg Supplier
        считающий calls — calls == 1 при sampleRate=100 и 100 вызовов).
      - module без sampling не затронут (diesel.DatabaseServer всегда
        проходит).

КОНФИГ. Добавить в config.properties (после блока из Phase 3) закомментированный
блок sampling + trace + format:

  logging.trace.enabled   = false
  logging.redaction.enabled = true

  # logging.file.path           = data/log/diesel.log
  # logging.file.max.size.mb    = 50
  # logging.file.retention.days = 30
  # logging.file.total.cap.mb   = 500

  # logging.format = json

  # logging.sampling.wal         = 100
  # logging.sampling.bufferpool  = 100
  # logging.sampling.replication = 50

ТЕСТЫ. Один regression-тест DieselLoggingIntegrationTest:
  - Поднимает DatabaseServer с logging.level=INFO + logging.trace.enabled=true
    + logging.format=json + logging.file.path=<tmp>.
  - Выполняет INSERT через libpq-клиент с trace_id="test-123".
  - Проверяет что в лог-файле: (1) JSON-строки валидны, (2) traceId=test-123
    присутствует во всех записях этого INSERT, (3) нет строк с
    tls.keystore.password или changeit.

ЧЕК-ЛИСТ ПРИЁМКИ.
- [ ] mvn test зелёный.
- [ ] В config.properties НЕТ ни одного раскомментированного per-module /
      per-sampling / file / json ключа — все под #.
- [ ] При раскомментировании logging.format=json лог становится JSON-валидным.
- [ ] DatabaseServer.logConfig не печатает tls.keystore.password — verify
      grep по выводу mvn test -Dtest=DatabaseSmokeTest не находит "changeit".
- [ ] DieselLoggingIntegrationTest зелёный.
- [ ] Redaction работает для всех известных секретных ключей в config.properties.
- [ ] GracefulShutdownTest остаётся зелёным (file handler корректно закрывается).
```

---

## 10. Чек-лист приёмки (итоговый по всем фазам)

| # | Критерий | Где проверить |
| --- | --- | --- |
| 1 | Все 15 legacy-классов мигрированы с JUL на SLF4J | `rg 'java.util.logging.Logger' diesel/` → 0 совпадений |
| 2 | `QueryParser.java` static-init 46–81 удалён | visual diff |
| 3 | Единый ключ `logging.level` управляет уровнем root + `diesel` логгеров | `DieselLogConfigTest#globalLevelFromConfigProperties` |
| 4 | Помодульный ключ `logging.level.<module>` имеет приоритет над глобальным — вверх и вниз | `DieselLogConfigTest#moduleOverridesGlobalUpward` / `#moduleOverridesGlobalDownward` |
| 5 | Помодульные переключатели в `config.properties` полностью закомментированы | visual diff `config.properties` |
| 6 | Асинхронный режим через `AsyncAppender` включается отдельно и не запускается при `logging.level=OFF` | `DieselAsyncAppenderTest#offLevelDoesNotStartAsyncThread` |
| 7 | На shutdown `AsyncAppender` корректно drain’ит очередь | `DieselAsyncAppenderTest#shutdownFlushesQueue` |
| 8 | Секреты (`tls.keystore.password`, `*.secret`, `*.token`) не попадают в логи — `TurboFilter` + whitelist в `logConfig` | `DieselRedactionTurboFilterTest` + `DieselLoggingIntegrationTest` |
| 9 | Мёртвые `diesel.*.level=INFO` строки удалены из `config.properties` | visual diff `config.properties` |
| 10 | MDC traceId присутствует в лог-записях при `logging.trace.enabled=true` | `DieselTraceContextTest` |
| 11 | File rotation параметризуется из `config.properties` при заданном `logging.file.path` | `DieselFileRotationConfigTest` |
| 12 | JSON-формат валиден при `logging.format=json` | `DieselJsonLayoutTest` |
| 13 | Sampling 1/N применяется до форматирования (DENY в TurboFilter) | `DieselSamplingTurboFilterTest` |
| 14 | Backward compat: `-Dlogging.level.diesel=WARNING` подавляет INFO (timing-quick профили) | `DieselLogConfigTest#existingLoggingLevelDieselSyspropStillWorks` |
| 15 | Все тесты зелёные: `mvn test` | CI |
| 16 | PerformanceRegressionTest не деградировал более чем на 5% | CI timing job |
| 17 | `Slf4jLogCapture` (существующий test helper) работает без изменений | existing tests using it |
| 18 | NOTICES-файл упоминает SLF4J (MIT) и logback (EPL 2.0/LGPL 2.1) | visual check of `NOTICES` / `LICENSE-3RD-PARTY` |

---

## Ссылки на файлы репозитория

| Файл | Строки | Роль в дизайне |
| --- | --- | --- |
| `pom.xml` | 30–33, 41–44 | `slf4j-api 2.0.12`, `logback-classic 1.5.6` — уже в production scope |
| `src/main/resources/logback.xml` | 1–32 | Базовая конфигурация appender’ов; обновить pattern с `%X{traceId:-}` (Phase 4.2) |
| `src/test/resources/logback-large.xml` | 1–21 | Test-профиль `large/perf/timing-quick`; не трогать |
| `config.properties` | 3–7 | Мёртвые строки → удалить (Phase 1) |
| `config.properties` | 556–567 | SQL audit block → оставить как есть (отдельная подсистема) |
| `config.properties` | 569–627 | TLS block → источник redaction-паттернов (Phase 4.1) |
| `diesel/QueryParser.java` | 46–81 | Static-init со своим ConsoleHandler → удалить (Phase 0) |
| `diesel/DatabaseServer.java` | 182–190 | `logConfig` печатает секреты → переписать через whitelist (Phase 4.1) |
| `diesel/DatabaseServer.java` | 223 | `start()` — точка вызова `DieselLogConfig.init()` (Phase 1) |
| `diesel/security/AuditLogger.java` | 1–415 | Async audit subsystem; мигрировать только LOGGER-поле (Phase 0); queue/consumer/retention не трогать |
| `diesel/storage/avro/AvroAuditLogger.java` | — | Параллельная avro-audit-подсистема; мигрировать только LOGGER-поле (Phase 0) |
| `src/test/java/diesel/Slf4jLogCapture.java` | 1–63 | Существующий test helper — НЕ ТРОГАТЬ, новые тесты используют его |
| `diesel/observability/MetricsRegistry.java` | — | Куда экспортировать `diesel.logging.dropped.count` (Phase 3) |

---

## Лицензионная сводка для NOTICES-файла

После реализации добавить (или обновить) `NOTICES` / `LICENSE-3RD-PARTY.md`:

```text
This product includes software developed by:
  - QOS.ch (SLF4J API, https://www.slf4j.org/) — licensed under MIT License
    Copyright (c) 2004-2025 QOS.ch. All rights reserved.
  - QOS.ch (logback, https://logback.qos.ch/) — dual-licensed under
    Eclipse Public License v2.0 and GNU Lesser General Public License v2.1.
    Copyright (c) 1999-2025 QOS.ch. All rights reserved.

Both libraries are used as binary dependencies without modification;
no reciprocity obligations apply to the DieselDB source code.
```
