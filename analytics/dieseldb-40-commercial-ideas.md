# DieselDB — 40 идей для коммерческого успеха (итоговый синтез)

## Диагноз

DieselDB технически силён (ARIES/WAL, MVCC/SSI, libpq-протокол, 5 драйверов, репликация, PITR),
но коммерчески мёртв по нефункциональным причинам: нет v1.0, нет артефактов в реестрах,
один мейнтейнер, нулевая известность, и позиционирование «ещё одна OLTP-база как Postgres».
Стратегия: не догонять Postgres, а занять нишу «встраиваемой аналитико-транзакционной БД
для JVM с PostgreSQL-протоколом и AI-поиском», и монетизировать через open-core.

## Волна 1 — Упаковка (месяцы 0–3), почти без нового кода

1. **Релиз v1.0 + публикация артефактов** (Maven Central, Docker Hub, GitHub Releases). Без этого всё остальное бессмысленно; pipeline для драйверов уже готов.
2. **Позиционирование «embedded-БД для Java»** — наследник H2/HSQLDB/Derby, которые морально устарели. Движок уже работает in-process.
3. **Тестовая замена Postgres в CI** — in-memory режим + libpq + psycopg2/pg-драйверы: запуск за 50 мс без Docker (Testcontainers-light).
4. **Spring Boot starter + GraalVM native image + официальный Docker-образ.** Без этого Java-мир не возьмёт.
5. **Open-core лицензионная модель** — Apache-2.0 ядро + платные модули (HA, multi-tenancy, аудит, governor, поддержка).
6. **Веб-консоль / GUI-клиент** (pgAdmin-аналог) — снижает порог входа, флагманский «wow» на демо.
7. **Публичные бенчмарки** (TPC-C/H + сравнение с H2/Postgres/DuckDB) — валюта доверия на рынке БД.
8. **MCP-сервер и AI-агент интеграции** — в 2026 году БД без MCP-подключения проигрывает в новых проектах.
9. **Документационный портал**: quickstart, tutorials, Discussions, SemVer-обещание.

## Волна 2 — Дифференциация поверх готового кода (месяцы 3–9)

10. **Database branching** — инстант-клон базы от любой LSN/времени за миллисекунды (Neon/PlanetScale-модель) как следствие готового MVCC.
11. **Per-transaction durability** — выбор политики fsync на транзакцию: latency как у Redis, durability как у Postgres.
12. **Serverless CLI-режим**: `diesel query "SELECT..." s3://bucket/*.parquet` без сервера и каталога (ИКР: БД исчезает).
13. **Нативное чтение Parquet/Arrow** (roadmap Roadmap_Parquet_Stage1.md уже есть) — SQL над data lake.
14. **Векторный поиск HNSV/HNSW + pgvector-совместимость** — embedded vector store для JVM/RAG.
15. **Гибридный поиск BM25 + vector из коробки** — то, за что платят разработчики LLM-приложений.
16. **CDC-пакет**: Kafka connector + subscriptions поверх уже реализованного LogicalDecoder (Debezium-lite без Kafka-инфры).
17. **K8s-оператор + Helm** — health-probes уже готовы, нужен операторный слой.
18. **Turnkey DR**: PITR + инкрементальный бэкап + scheduler как один платный модуль «восстановление без DBA».

## Волна 3 — Развитие ядра (месяцы 6–24)

19. **Колоночное хранение + параллельное сканирование** — честный DuckDB-конкурент в JVM (SIMD-агрегаты уже есть).
20. **HTAP: dual row/column storage** с синхронизацией через WAL — транзакции и аналитика без двух систем.
21. **JSONB-тип с бинарным хранением и индексами** (roadmap Phase 3) — multi-model поверх готового JSONL-опыта.
22. **Time-series пакет**: columnar compression, retention, continuous aggregates, chunking.
23. **Авто-тиринг hot→warm→cold** по «температуре данных» с выводом метрик в Prometheus.
24. **Self-driving тюнинг**: авто-индексы по workload, авто-ANALYZE, детектор аномалий по p95/p99 (метрики уже есть).
25. **Per-tenant resource governor**: квоты RU/с, приоритеты IO, отмена запросов, изоляция буфер-пуллов (поверх SNI multi-tenancy).
26. **Edge↔cloud синхронизация**: WAL-репликация на встраиваемые инстансы (рынок IIoT, альтернатива LiteFS).
27. **CRDT-колонки** для multi-master eventual consistency (счётчики, множества) — edge-write без конфликтов.
28. **Auto-failover (Patroni-эквивалент)** — платный enterprise-модуль поверх готовой sync standby + slots.
29. **Connection pooling (PgBouncer-эквивалент)** — server-side, в roadmap Phase 6.
30. **Полный Postgres-диалект SQL** (CTE, window functions, FK, EXISTS, UNION, RETURNING...) — для совместимости, не для маркетинга.
31. **MySQL wire protocol** как второй фронт миграции legacy-приложений.

## Волна 4 — Стратегические ставки (месяцы 12–36)

32. **Морфинг одним бинарником**: embedded → сервер → реплики → шардинг без переустановки и миграций.
33. **Консолидация стека**: OLTP+OLAP+time-series+vector+full-text в одной СУБД — аргумент TCO («уберите 4 лицензии»).
34. **Tamper-evident аудит** (hash-chained, anchored) — продукт для финтеха/медицины/госсектора.
35. **In-database ML/инференс** (ONNX runtime внутри) — scoring рядом с данными, sticky-эффект.
36. **Database-as-a-function** — идеальный конечный результат: результат без системы, оплата за обработанные данные.
37. **In-database ETL/процедурная платформа** — СУБД становится местом исполнения бизнес-логики, а не хранения.
38. **Миграционный тулкит Oracle/MySQL → DieselDB**: assessment-сканер SQL, конвертер диалектов, совместимость.
39. **Edge RDB для устройств**: низкий RAM-footprint, offline-first, опционально без WAL для read-only аналитики на устройстве.
40. **Managed cloud (dieseldb.cloud)** с usage-based pricing и автомасштабированием — конечная точка монетизации.

## Приоритетезация (топ-7 по соотношению эффект/стоимость)

| # | Идея | Почему первой |
|---|------|----------------|
| 1 | v1.0 + реестры | Блокер всего остального, недели работы |
| 2 | Позиционирование embedded Java | Самый короткий путь к пользователям |
| 10 | Database branching | Почти бесплатно из готового MVCC, сильный wow-эффект |
| 12 | Serverless CLI | ИКР + вирусный потенциал, как у DuckDB |
| 14 | Векторный поиск | Горячий спрос 2024–2026, одна фича — новая категория |
| 5 | Open-core модель | Превращает фичи в деньги |
| 19 | Колоночный storage | Ядро аналитической дифференциации |

## Итоговая формула

Не писать много нового кода, а резко поднять идеальность существующих 112 тыс. строк:
MVCC → branching, WAL → CDC/edge-sync, SNI → governor, файловые бэкенды → serverless.
Позиционирование: «встраиваемая DuckDB + vector для JVM с PostgreSQL-протоколом».
Монетизация: open-core + managed cloud.
