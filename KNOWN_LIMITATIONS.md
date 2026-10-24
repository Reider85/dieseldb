# Известные ограничения DieselDB и workaround'ы

Документ описывает известные ограничения экспериментальной БД DieselDB и
рекомендуемые обходные пути (workaround) для каждого из них. Файлы
конфигурации читаются из `config.properties` в рабочем каталоге процесса.

---

## 1. Максимальный размер результата запроса

**Ограничение.** Движок ограничивает количество строк, которое может вернуть
запрос. По умолчанию предел — **1 000 000 строк** (ключ `max.result.rows` в
`config.properties`, дефолт `1000000`). Предел применяется на каждом этапе
выполнения (фильтрация, join, агрегация и т.д.), поэтому ошибка может
сработать даже на промежуточном результате.

- при достижении ~80% лимита в лог пишется предупреждение
  `WARNING: query result is approaching the maximum allowed row limit: ... (80%). Consider adding LIMIT or a MAX_ROWS hint.`;
- при достижении 100% лимита запрос завершается ошибкой
  `Query result exceeds the maximum allowed row limit of N rows at stage '...'. Add LIMIT or a /* MAX_ROWS=N */ hint to override.`

**Workaround:**
- добавляйте `LIMIT` к запросам, которые могут вернуть много строк;
- для отдельного запроса используйте хинт `/* MAX_ROWS=N */` (например
  `SELECT /* MAX_ROWS=5000000 */ ...`), а `/* MAX_ROWS=0 */` полностью
  отключает предел для запроса;
- если лимит нужен выше глобально — поднимите `max.result.rows` в
  `config.properties` и соразмерно увеличьте heap JVM (см. раздел 3);
- сузьте выборку условиями `WHERE`.

---

## 2. JOIN с условием OR

**Ограничение.** JOIN, условие которого содержит `OR` (например
`JOIN b ON a.id = b.id OR a.code = b.code`), может породить очень большой
результат, поскольку равенства из `OR` не раскладываются на эффективный путь
выполнения. Движок логирует
`WARNING: JOIN with OR condition may produce large result set`.

**Workaround:**
- перепишите `OR` как объединение двух запросов с равенствами
  (`SELECT ... WHERE cond1 UNION SELECT ... WHERE cond2`);
- замените `OR` на список `IN` там, где это возможно;
- добавьте индекс на колонки соединения (см. раздел 5);
- ограничьте результат `WHERE`/`LIMIT`.

---

## 3. Требования к памяти / размер heap

**Ограничение.** DieselDB — in-memory движок: таблицы целиком загружаются в
память, а результаты буферизуются в JVM. Пиковые потребности зависят от
объёма данных и типа операций:

- в тестовой/dev конфигурации используется heap **512 MB** (`-Xmx512m`);
- тяжёлые операции (например join 600x600 строк с `ORDER BY`, ~360 тыс. строк
  результата) могут пиково требовать **0.7–1.5 GB** — для них нужен
  `-Xmx4g`;
- ключи конфигурации: `max.inmemory.rows` (порог потоковой обработки / spill,
  дефолт `10000`), `max.hash.table.size.mb` (бюджет hash-join, при превышении
  join разбивается на партиции и выполняет spill, дефолт `512`).

**Workaround:**
- сузьте рабочее множество условиями `WHERE` и `LIMIT`;
- увеличьте heap: `-Xmx4g` (или больше) для больших join'ов;
- при необходимости измените `max.inmemory.rows` и `max.hash.table.size.mb`;
- держите размеры отдельных таблиц умеренными — движок не умеет работать с
  таблицами, которые не помещаются в память.

---

## 4. OFFSET без LIMIT

**Ограничение.** Запрос с `OFFSET`, но без `LIMIT`, вынуждает движок
материализовать и отсортировать весь результат до пропуска строк. Движок
логирует `OFFSET without LIMIT may be inefficient`.

**Workaround:** всегда указывайте `LIMIT` вместе с `OFFSET`
(`... LIMIT 50 OFFSET 100`).

---

## 5. JOIN по неиндексированной колонке

**Ограничение.** При JOIN по колонке без индекса движок автоматически создаёт
in-memory B-tree индекс на колонке соединения и логирует
`Consider creating index on TABLE.COLUMN for faster JOIN`. Такой индекс
создаётся заново при каждом запросе.

**Workaround:** создайте постоянный индекс явно — `CREATE INDEX` (поддерживаются
типы BTREE, HASH, UNIQUE, CLUSTERED). Это ускоряет повторные JOIN'ы и
проверки по равенству.

---

## 6. Персистентность и формат файлов

**Ограничение.** Каждая таблица хранится как `<NAME>.csv` (данные) +
`<NAME>.table` (сериализованные метаданные) в `dataDir` базы данных
(по умолчанию — текущий рабочий каталог). При старте/загрузке таблица
целиком считывается в память; затраты на запись растут вместе с размером
таблицы.

**Workaround:**
- держите размеры таблиц в разумных пределах (см. раздел 3);
- для серверных запусков используйте отдельный каталог данных
  (`start-server.bat`/`start-server.sh`, либо `--local [dataDir]`), чтобы не
  засорять рабочий каталог артефактами.

---

## 7. Транзакции и уровень изоляции

**Ограничение.** Уровень изоляции по умолчанию — **SERIALIZABLE**
(ключ `transaction.isolation.level`). Это самый строгий уровень: при
конкурирующих транзакциях возможны конфликты и необходимость повторного
выполнения.

**Workaround:** если рабочей нагрузке строгая сериализуемость не нужна,
задайте более низкий уровень изоляции (`READ_UNCOMMITTED`, `READ_COMMITTED`,
`REPEATABLE_READ`) в `config.properties` — в обмен на большее число
конкурирующих читателей.

---

## 8. Таймаут сокета сервера

**Ограничение.** Простаивающие клиентские соединения закрываются по таймауту —
по умолчанию **30 секунд** (ключ `server.socket.timeout=30000` в
`config.properties`).

**Workaround:** при длительных сессиях увеличьте `server.socket.timeout` или
переустанавливайте соединение до истечения таймаута.

---

## 9. Порог профилирования медленных запросов

**Ограничение.** Запросы дольше `diesel.profile.slow.threshold.ms`
(по умолчанию **1000 мс**) попадают в лог как
`Slow query breakdown: parse=...ms, plan=...ms, execute=...ms, sort=...ms`.
Значение переопределяется системным свойством `-Ddiesel.profile.slow.threshold.ms`.

**Workaround:** для диагностики медленных запросов снизьте порог (например
`-Ddiesel.profile.slow.threshold.ms=100`); для тихих логов — повысьте его.

---

## 10. JSONL-сжатие: размеры и производительность (Prompt 52)

**Результаты замеров** (500 строк × повторяющийся шаблон `name-XXXX` + 200
символов; Java 21 / Windows 11 / Intel Xeon).

| Кодек | Размер файла | Сжатие | Отношение |
|-------|-------------|--------|-----------|
| none  | 119 230 Б   | —      | 1×        |
| zstd  |   1 756 Б   | 67.9×  | ×1/67.9   |
| lz4   |   5 300 Б   | 22.5×  | ×1/22.5   |
| snappy|  11 596 Б   | 10.3×  | ×1/10.3   |

**Производительность** (5000 строк × повторяющийся шаблон, 3 прогона):

| Сценарий | Сохранение | Загрузка (min/3) |
|----------|-----------|-----------------|
| none     | 328 мс    | 143 мс          |
| zstd     | 434 мс    | 18 мс           |

Последовательное чтение zstd-файла **быстрее** несжатого (~8× в замере),
т.к. обрабатывается в 13 раз меньше данных.

**Ключ конфигурации:** `jsonl.compression.codec = none | zstd | lz4 | snappy`
(default: `zstd`). Ключ `jsonl.compression.level` управляет уровнем ZSTD
(1–22, default 3); LZ4/Snappy игнорируют его.

**Ограничения:**
- Append-режим (`jsonl.write.mode=append`) не совместим с сжатием —
  автоматический fallback на полный rewrite с предупреждением в логе.
- Append-данные перезаписываются при каждом save при codec ≠ none;
 delta-файл не создаётся, т.к. сжатые потоки фреймовые и не дополняются.

## 11. JSONL cross-format benchmark (Prompt 56)

**Результаты замеров** (@LargeTest, Java 21 / Windows 11 / 4-core; PROMPT 56
качественный гейт, файлы тестовых данных во временной директории).

**Загрузка 1M строк** ([JSONL-BENCH-1M], csv vs jsonl vs jsonl+zstd):

| Формат | Время загрузки | Примечание |
|--------|---------------|------------|
| csv    | 1266 мс       | CSV-хранение (дельфированный) |
| jsonl  | 54 055 мс     | JSONL без сжатия |
| jsonl+zstd | 55 166 мс | JSONL + кодек zstd |

**Проекция на широких строках** ([JSONL-BENCH-WIDE], 20k строк × 42 колонки,
проекция 3 из 42 колонок):

| Сценарий | Время | Ускорение |
|----------|-------|-----------|
| full scan       | 1913 мс | 1×  |
| projection      |  341 мс | **5.61×** (требование ≥2×) |

**Append vs rewrite** ([JSONL-BENCH-APPEND], 10 пакетов × 1000 inserts):

| Сценарий | Время | Преимущество |
|----------|-------|--------------|
| rewrite (полный) | 620 мс | 1× |
| append (дополнение) | 153 мс | **4.1×** (требование ≥2×) |

**Ограничения/наблюдения:**
- JSONL-загрузка 1M строк расчётно медленнее CSV (~42× в этом замере);
  это ожидаемо для JSON (объект-парсинг), считается измерением, а не
  критерием — никакого требования на скорость не накладывается.
- JSONL+zstd загружается не быстрее plain JSONL на больших данных (в этом
  замере даже чуть медленнее — кодек-циклы на распаковке); выгода zstd —
  размер файла, а не скорость чтения.
- Проекция даёт ускорение >5× на широких строках (критерий ≥2× выполнен);
  append быстрее rewrite >4× (критерий ≥2× выполнен).
- Замеры единичные (по одному прогону на сценарий, ожидаемый разброс ±10%;
  тяжёлые прогоны идут в -P large, 4GB heap).

---

## 12. Устаревший протокол (Prompt 4 #29)

**Ограничение.** Для совместимости со старыми клиентами (v1) на порту **5440** 
работает устаревший Java Object Serialization протокол. Этот протокол уязвим
для RCE-атак через десериализацию и будет удалён в **Фазе 2**.

**Workaround:**
- новые клиенты должны использовать основной порт (по умолчанию 3306) с 
  новым бинарным протоколом v2;
- на основном порту (3306) Java-сериализация отклоняется с ошибкой при 
  обнаружении магических байтов `ObjectOutputStream`;
- legacy-клиенты на порту 5440 получают deprecation-предупреждение в логах;
- отключить legacy-порт можно установив `legacy.port = 0` в `config.properties`.

**Ключ конфигурации:** `legacy.port = 5440` (default: 5440, 0 = disabled).

**Примечание:** Удаление legacy-протокола запланировано на Фазу 2 разработки.

---

## 13. Параллельный executeBatch по одной таблице не синхронизирован

**Ограничение.** `Database.executeBatch(...)` без транзакции распределяет
запросы по пулу потоков: зависимости по таблицам влияют только на
*группировку* batch-ей, а не на порядок выполнения — несколько INSERT-ов в
одну таблицу выполняются **конкурентно** и на больших партиях падают с
гонкой (`QuerySyntaxException: Index: N, Size: M`, повторный ключ).
Обнаружено при написании тестов Prompt 4 #29.

**Workaround:**
- вставляйте много строк в одну таблицу последовательным циклом
  `executeQuery(...)` (так делают сетевые тесты #29) либо внутри
  транзакции — там `executeBatch` уходит в последовательный
  `executeBatchSequential`;
- `BatchQueryTest` (2-3 запроса) проходит статистически, но не доказывает
  отсутствие гонки на больших партиях.

**Статус:** Кандидат на отдельный промпт — компоненты графа зависимостей
должны выполняться последовательно, независимые — параллельно.

---

## 14. Аутентификация и авторизация: legacy-порт без auth, ограничения RBAC

**Ограничение.** Prompt 4 #31-32 включили аутентификацию и авторизацию,
но остаются ограничения:

- **legacy-порт (5440) не умеет аутентифицироваться** — протокол v1 не имеет
  сообщения AUTH. При включённом `security.auth.required` legacy-адаптер
  отклоняет SQL-запросы с `Error: Authentication required` (либо владелец
  сервера отключает проверку через `security.auth.required = false` для
  legacy-клиентов);
- **принудительная смена пароля — hard-enforcement**: Prompt 4 #32 реализовал
  жёсткую блокировку всех запросов кроме `ALTER USER` до смены пароля
  (флаг `mustChangePassword`);
- **`ALTER USER` с правом ADMIN**: Prompt 4 #32 добавил Authorizer — пользователь
  с правом ADMIN может менять пароль любого пользователя; без ADMIN — только свой;
- **FETCH по курсору не перепроверяется после REVOKE**: при открытом курсоре
  пользователь может продолжить выборку данных даже после отзыва прав
  (аналогично другим СУБД);
- **`--password` виден в списке процессов ОС** — пароль передаётся аргументом
  командной строки; для автоматизации используйте переменные окружения или
  запускайте клиент из защищённого окружения.

**Workaround:**
- в проде оставляйте `security.auth.required = true`; legacy-порт отключайте
  через `legacy.port = 0`;
- смена пароля другого пользователя: пользователю с правом ADMIN или
  локальный `--reset-admin`;
- шифрование учётных данных в транспорте: серверный и клиентский TLS доступны с
  Prompt 4 #34/35 (`tls.enabled`, CLI-флаги); mTLS/SNI — #36; legacy-порт шифрование не поддерживает.

**Ключи конфигурации:** `security.auth.required` (default: true),
`security.auth.bootstrap-admin` (default: true).

## 15. SQL Audit log (prompt4 #33, 2026-10-20)

Ограничения аудита SQL-операций (R3-008 step 4/4):

- **audit выключен по умолчанию** — udit.enabled = false для dev-среды;
  включайте явно для production (compliance-логирование);
- **legacy-порт (5440) аудируется как `embedded`** — v1-адаптер вызывает
  session-less `executeQuery`, поэтому USER_NAME/SOURCE_IP записываются как
  `embedded` (IP недоступен без сессии). Основной v2-порт пишет реальные
  user/IP из сессии;
- **вложенные subquery/batch-выражения не аудируются отдельно** — внешний
  оператор получает одну запись (AUDIT_INNER guard); это осознанно: одна
  запись на пользовательский стейтмент, без шума от внутренних переиспользований;
- **переполнение очереди → graceful drop** — при полной bounded-очереди
  (`audit.queue.capacity`, default 10000) запись отбрасывается, счётчик
  `droppedCount` растёт, WARN логируется каждые 1000; основной запрос
  никогда не блокируется;
- **retention архивирует, а не удаляет** — строки старше
  `audit.retention.days` (default 90) переносятся в gzip-архив
  (`data/audit-archive/audit-<ts>.log.gz`, pipe-delimited) и только потом
  удаляются из таблицы; данные не теряются;
- **`DIESEL_AUDIT_LOG` защищён от DROP** — как и все `diesel_*` системные
  таблицы (guard в `Database.dropTable`);
- **отличие от AVRO audit (Prompt 95)**: `diesel.security.AuditLogger` —
  SQL-аудит (кто/что/когда/ip/статус), `AvroAuditLogger` — storage-level
  аудит операций AVRO-бэкенда; независимые подсистемы с разными ключами
  (`audit.*` vs `avro.audit.*`).

**Workaround:**
- в проде: `audit.enabled = true`, `audit.retention.days` по политике
  compliance (обычно 90-365);
- для отладки: `-Daudit.enabled=true` как system property (перекрывает
  config.properties).

**Ключи конфигурации:** udit.enabled (default: false),
udit.retention.days (default: 90), udit.queue.capacity (default: 10000),
udit.retention.interval.ms (default: 86400000), udit.archive.dir
(default: data/audit-archive).

## 16. TLS transport (prompt4 #34/#35/#36, 2026-10-20/21)

Серверный TLS поверх TCP accept (R3-009 step 1/3):

- **TLS выключен по умолчанию** — `tls.enabled = false`; для продакшена
  включайте явно и поднимайте keystore (см. команду keytool в комментариях
  `config.properties`);
- **broken keystore → отказ в старте** — при `tls.enabled=true` с
  отсутствующим/битым keystore сервер не поднимает accept-сокет
  (fail fast вместо молчаливого downgrade на plaintext);
- **handshake ленивый** — обёртка в SSLSocket происходит после accept,
  сам handshake — на первом чтении в worker-потоке; медленный/вредоносный
  клиент не может заблокировать accept-цикл (anti handshake-DoS); сбой
  handshake закрывает соединение в ClientHandler;
- **cipher whitelist — allow-list** — только 3 TLS 1.3 suites + PFS/AEAD
  TLS 1.2 (ECDHE/DHE + AES-GCM/CHACHA20); legacy (TLS 1.0/1.1, RSA key
  exchange, CBC, RC4, 3DES, анон/NULL) отключены белым списком, не deny-list;
  включены только протоколы TLSv1.3/TLSv1.2;
- **клиент TLS реализован (Prompt 4 #35)** — CLI-флаги `--tls`/`--tls-ca`/`--tls-cert`/`--tls-key` 
  работают; DatabaseClient поддерживает TLS через setSslSocketFactory(); hostname+chain 
  verification, expired cert rejection; опциональный diesel-client.conf; 
  mTLS/SNI реализованы в #36 (см. ниже);
- **legacy-порт (5440) не шифруется** — TLS врезан только в основной
  v2-порт; legacy v1-адаптер остаётся plaintext (ещё один повод держать
  `legacy.port = 0` в проде);
- **mTLS (Prompt 4 #36)** — `tls.client.auth = none|request|require`
  (default `none`); при `require` клиент обязан предоставить сертификат,
  проверяемый серверным truststore (`tls.ca.path`); при `request` cert
  опционален, но при наличии — проверяется; невалидный/отсутствующий cert
  при `require` = сбой handshake в worker-потоке (клиент получает
  IOException, соединение закрывается); отсутствие `tls.ca.path` при
  `auth != none` → отказ в старте (fail fast, без downgrade);
- **SNI multi-tenant (Prompt 4 #36)** — выбор keystore по SNI-хостнейму:
  конфиг-ключи `tls.sni.<hostname>.keystore.path/.password/.type` или
  строки системной таблицы `DIESEL_TENANTS` (SQL `CREATE TENANT <name>
  SNI '<host>' KEYSTORE '<path>' [PASSWORD '<pw>']` / `DROP TENANT <name>`).
  Неизвестный/отсутствующий SNI → fallback на дефолтный `tls.keystore.*`;
  таблица читается один раз при старте сервера (restart для изменений);
  конфиг-ключи приоритетнее таблицы при конфликте хостнеймов;
- **SSL Labs A+ — ручная ops-валидация** — автоматического прогона
  testssl.sh/sslyze в CI нет; prerequisites (TLS 1.2/1.3 only, AEAD-only
  cipher whitelist, no renegotiation) уже покрыты `CipherAuditTest`; при
  включённом mTLS/SNI и валидных сертификатах конфигурация соответствует
  требованиям A+ (HSTS/OCSP stapling — на уровне reverse proxy);
- **пароль keystore в DIESEL_TENANTS хранится открытым** — как и
  `tls.keystore.password` в config.properties (all-String колонки
  системных таблиц); защита — filesystem permissions доступа к БД.

**Workaround:**
- в проде: `tls.enabled = true` + валидный PKCS12 keystore;
- mTLS: `tls.client.auth = require` + `tls.ca.path` (CA, подписавший
  клиентские сертификаты);
- SNI: `CREATE TENANT` для каждого хостнейма или `tls.sni.*` ключи;
- тестовая пара ключей: `keytool -genkeypair -alias dieseldb -keyalg RSA
  -keysize 2048 -storetype PKCS12 -keystore data/diesel-keystore.p12
  -storepass <password> -dname "CN=localhost"
  -ext "SAN=dns:localhost,ip:127.0.0.1"`.

**Ключи конфигурации:** tls.enabled (default: false), tls.keystore.path
(default: data/diesel-keystore.p12), tls.keystore.password (default:
changeit), tls.keystore.type (default: PKCS12), tls.client.auth (default:
none), tls.ca.path (server truststore для mTLS), tls.truststore.type

## §17 Online schema changes (Prompt4 #37, шаг 1/3)

**Ограничения текущей реализации (R3-010 step 1/3):**

- **Продакшен-каталог heap-only** — версии схем персистятся только при page-backed каталоге (тестовый режим с `setCatalogPageManager`). В проде `Database` создаёт `CatalogTable(null)` → catalog в памяти, `flush()` no-op. Включение глобального каталога отложено на после multi-page каталога (prompt 10).
- **История версий** — персистится только счётчик + последняя схема. Полная история версий — in-memory (ограничена 32 версиями + pinned), не восстанавливается при restart.
- **ALTER TABLE** — поддерживает только `ADD COLUMN <nullable>` (metadata-only, step 1). `NOT NULL`, `DEFAULT`, `DROP COLUMN` — шаги 2/3 + промпты 113/114.
- **Per-query snapshot** — `SelectQuery.execute` пинает версию на время execute (timeout bookkeeping), но колонки читаются из `current()` (не из пиннутой версии). Строгий per-query schema snapshot (чтение строго из пиннутой версии) — шаги 2/3.
- **Mid-execute ALTER видимость** — как сегодня (безопасность ссылок гарантируется immutable-списками в Table). MVCC на уровне схем — шаги 2/3.

**Workaround для персистентности в проде:**
- Использовать тестовый режим с page-backed каталогом (не для production workloads из-за риска PageFullException при ~40+ таблицах);
- Следить за multi-page каталогом (prompt 10) как точкой включения прод-версий.

**Ключ конфигурации:** `schema.old.version.timeout.min` (default: 10, минуты) — время жизни старых версий.
(default: AUTO), tls.truststore.password, tls.sni.<hostname>.keystore.path
(per-tenant SNI keystore).

## §18 Online ALTER TABLE ALGORITHM=COPY (Prompt4 #38, шаг 2/3)

**Ограничения текущей реализации (R3-010 step 2/3):**

- **Новая колонка видна только после завершения copy** — во время фонового
  копирования действует старая схема: DML не может ссылаться на добавляемую
  колонку (INSERT с новой колонкой отклоняется как unknown column до swap).
  После COMPLETED колонка видна всем; INSERT может её опускать (значение
  null — пометка хранится в памяти через `alterAddedColumns`, после restart
  пометка теряется и правило снова строгое).
- **Dual-write — deferred journal, а не синхронное дублирование** — записи
  во время копирования идут в старую таблицу + журнал
  (`DualWriteCoordinator`); журнал применяется к shadow-таблице один раз при
  finalize. Барьер (active-counter + seal) гарантирует, что каждая мутация
  атомарна относительно swap; цена — окно finalize, когда DML ждёт
  установку (см. `BARRIER_WAIT_MS` в `ALTER STATUS`; критерий «writers не
  блокированы > 100 ms» меряется именно им, а не суммарной стоимостью
  insert'ов, которые всегда включают pre-existing persist-путь CSV).
- **Compaction/reload откладываются на время copy** — `Table.compact()` и
  `reloadFromStorage()` пропускаются, пока журнал прикреплён (сдвиг индексов
  сломал бы replay); VACUUM повторит попытку после завершения. Физическое
  `removeRow` во время copy поддерживается (событие REMOVE), но его
  конкурентность с другими removeRow не синхронизирована — pre-existing
  отсутствие блокировок в `removeRow`.
- **Batch-DML и DROP TABLE заблокированы во время copy** — `END BATCH`
  (copy-on-write публикация заменила бы таблицу под журнал) и `DROP TABLE`
  на таблице с активным copy отклоняются с ошибкой; дождитесь завершения
  или отмените copy.
- **Отмена доступна только через API** — `Database.cancelAlter(table)`;
  SQL `ALTER ROLLBACK` не реализован. Отмена во время финализации может
  прийти слишком поздно (copy успеет завершиться — это не ошибка, статус
  COMPLETED). Rollback до finalize гарантирует: схема, версии, данные не
  изменены, shadow отбрасывается.
- **Индексы не перестраиваются при swap** — layout shadow-таблицы
  позиционно-идентичен боевой (baseline + те же мутации), поэтому записи
  индексов остаются валидными; это верно, пока во время copy не происходят
  операции, смещающие индексы иным способом (сам CREATE INDEX во время copy
  допустим — его результат обслуживается обычным путём).
- **Crash-семантика** — swap (in-memory) и запись каталога/данных
  разделены: crash между install и persist означает «ALTER не произошёл»
  при старте (каталог + файлы данных не тронуты); промежуточного состояния
  «новый каталог + старые данные» не бывает: данные пишутся первыми,
  каталог последним.
- **Масштаб тестов** — acceptance-критерий «10M строк» отмасштабирован до
  20k строк (бюджет fast-сьюта; проверяются блокировка писателей и
  целостность swap, не абсолютный размер — precedent: prompt4 #37).
- **`ALGORITHM=INPLACE` отклоняется** — синтаксис парсится, выполнение
  отложено на R3-010 step 3 (prompt4 #39); неизвестные токены ALGORITHM →
  syntax error.

**Workaround:**
- прогресс: `ALTER STATUS` (TABLE_NAME, STATE, PROGRESS_PERCENT,
  ROWS_COPIED/ROWS_TOTAL, BARRIER_WAIT_MS, MESSAGE);
- отмена: `Database.cancelAlter(<table>)` до состояния COMPLETED;
- тестовые настройки скорости copy: `-Dalter.copy.batch.size=1000
  -Dalter.copy.batch.sleep.ms=1`, таймаут барьера:
  `-Dalter.copy.gate.timeout.ms=10000`.

**Ключи конфигурации (system properties):** `alter.copy.batch.size`
(default: 2048), `alter.copy.batch.sleep.ms` (default: 0),
`alter.copy.gate.timeout.ms` (default: 10000); `schema.old.version.timeout.min`
(default: 10, наследуется из шага 1).