DieselDB - An experimental database built on Java 17 & 21 (LTS), developed by Reider85 using AI tools.
Supports simple SELECT, UPDATE, INSERT, DELETE queries,
queries with IN, LIKE, LEFT, RIGHT, INNER, OUTER, CROSS JOIN, queries with LIKE, queries with aliases, queries with subqueries,
GROUP BY, ORDER BY, BTREE and HASH indexes, clustered indexes, transactions with isolation levels:
READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE.
Future plans include implementing mathematical functions in queries, a visual interface similar to PG_ADMIN, and advanced DDL queries

DieselDB - Экспериментальная база данных на Java 17 и 21 (LTS), разработанная Reider85 с использованием ИИ средств.
Поддерживает простые SELECT, UPDATE, INSERT, DELETE запросы,
запросы с IN, LIKE, LEFT,RIGHT,INNER,OUTER,CROSS JOIN,запросы с LIKE,запросы с алиасами, запросы с подзапросами
GROUP BY,ORDER BY, BTREE и HASH индексы, кластерные индексы, транзакции с уровнями изоляции
READ_UNCOMMITTED,READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
В планах реализация математических функций в запросах,визуального интерфейса аналогичного PG_ADMIN, продвинуты[ DDL запросов

DieselDB – Eine experimentelle Datenbank auf Basis von Java 17 & 21 (LTS), entwickelt von Reider85 unter Einsatz von KI-Tools.

Unterstützt werden einfache SELECT-, UPDATE-, INSERT- und DELETE-Abfragen,
Abfragen mit IN, LIKE, LEFT, RIGHT, INNER, OUTER und CROSS JOIN,
Abfragen mit LIKE, Abfragen mit Aliasen, Abfragen mit Unterabfragen,
GROUP BY, ORDER BY, BTREE- und HASH-Indizes, geclusterte Indizes sowie Transaktionen mit den Isolationsleveln
READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ und SERIALIZABLE.
Geplant sind die Implementierung mathematischer Funktionen in Abfragen, eine grafische Benutzeroberfläche ähnlich wie pgAdmin sowie erweiterte DDL-Abfragen.

DieselDB: una base de datos experimental en Java 17 y 21 (LTS), desarrollada por Reider85 con la ayuda de herramientas de IA.
Soporta consultas simples SELECT, UPDATE, INSERT, DELETE; consultas con IN, LIKE; JOIN (LEFT, RIGHT, INNER, OUTER, CROSS); consultas con LIKE, alias y subconsultas; GROUP BY, ORDER BY; índices BTREE y HASH, índices clusterizados; transacciones con niveles de aislamiento READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE.
Entre los planes futuros se incluye la implementación de funciones matemáticas en consultas, una interfaz visual similar a PG_ADMIN y consultas DDL avanzadas.

DieselDB - 实验性数据库，基于 Java 17 和 21 (LTS) 开发，由 Reider85 借助 AI 工具创建。

支持简单的 SELECT、UPDATE、INSERT、DELETE 查询，
支持带有 IN、LIKE 的查询，支持 LEFT、RIGHT、INNER、OUTER、CROSS JOIN，
支持带有 LIKE 的查询，支持别名查询，支持子查询，
支持 GROUP BY、ORDER BY，支持 BTREE 和 HASH 索引，支持聚簇索引，
支持事务及隔离级别：
READ_UNCOMMITTED、READ_COMMITTED、REPEATABLE_READ、SERIALIZABLE

未来计划实现查询中的数学函数、类似于 PG_ADMIN 的可视化界面，以及高级 DDL 查询。

## Known limitations / Известные ограничения

Known limitations and workarounds are documented in [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md).

Известные ограничения и обходные пути описаны в [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md).

## Build / Сборка

Requirements:
- JDK 17 or newer (verified with JDK 17 and JDK 21 LTS)
- Maven 3.9+ (or use the Maven bundled with IntelliJ IDEA)

Quick checks (runs only AllTestsSampleTest and QuantitativeTest):
```
mvn test
```

Full test suite (all test classes):
```
mvn -Ptest test
```

Build the jar:
```
mvn package
```

Install to the local Maven repository:
```
mvn install
```

Требования:
- JDK 17 или новее (проверено с JDK 17 и JDK 21 LTS)
- Maven 3.9+ (или встроенный Maven из IntelliJ IDEA)

Быстрая проверка (запускает только AllTestsSampleTest и QuantitativeTest):
```
mvn test
```

Полный набор тестов (все тест-классы):
```
mvn -Ptest test
```

Сборка jar:
```
mvn package
```

Установка в локальный Maven-репозиторий:
```
mvn install
```

## CSV/TSV compression (Prompt 39)

CSV and TSV files can be compressed with ZSTD (default), LZ4 or Snappy via the
`csv.compression.codec` / `tsv.compression.codec` config keys
(`none | zstd | lz4 | snappy`; resolution: system property → `config.properties` →
code default `zstd`). `csv.compression.level` / `tsv.compression.level` set the
ZSTD level (valid 1–22, default 3, clamped; LZ4/Snappy ignore the level). The
compression applies only to newly written files: `none` keeps the previous
plain format byte-identical, and readers transparently detect the actual format
of existing files by suffix (`.csv.zst`, `.csv.lz4`, `.csv.snappy`, …), so
files written under an earlier codec stay readable after a codec change.
Compressed files are read sequentially (the byte-offset parallel pre-scan
cannot address compressed bytes).

Measured on 3000 repetitive rows (plain file 166,552 bytes; size ratio = ≥3x is
the acceptance criterion):

| Codec | Size (bytes) | Ratio | Write (20k rows) | Read (20k rows) |
|-------|-------------|-------|------------------|-----------------|
| none (plain) | 166,552 | 1.00x | 73.8 ms | 127.9 ms |
| zstd | 8,638 | 19.28x | 188.7 ms | 62.5 ms |
| lz4 | 26,016 | 6.40x | — | — |
| snappy | 25,785 | 6.46x | — | — |

Compressed sequential reads are ~2× faster than plain (I/O-bound scenario
accelerates as expected); writes are ~2.5× slower because of compression CPU.