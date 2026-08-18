# AGENTS.md

DieselDB: an experimental file-persisted SQL database in Java (package-private engine, ~38 classes in `diesel/`), driven prompt-by-prompt from `prompt2.md` (stage-1, 100 prompts). Each prompt ends with a Changelog entry + commit + push. Remote: `github.com/Reider85/dieseldb.git`.

## Build & run (Windows)

- Maven is NOT on `PATH`. Use the IntelliJ-bundled Maven:
  `& "C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2023.3.1\plugins\maven\lib\maven3\bin\mvn.cmd" <args>`
  (copy also at `C:\tools\apache-maven-3.9.6\bin\mvn.cmd`).
- `$env:JAVA_HOME` defaults to JDK 11 and breaks the build; `pom.xml` sets `maven.compiler.source/target=21`, so **JDK 21 is required** (JDK 17 no longer compiles Ч the README's "17 or newer" is stale). Set it inline:
  `$env:JAVA_HOME = "C:\Program Files\Axiom\AxiomJDK-21"` (javac/java likewise from there).
- `mvn package` produces a jar with no usable `Main-Class` (`-Pci` sets it to `com.dieseldb.DatabaseServer`, the wrong package Ч the real class is `diesel.DatabaseServer`), so `java -jar` fails. Launch the engine via class `diesel.DatabaseServer`, or use `start-server.bat/.sh`.
- CI (`.github/workflows/ci.yml`) still targets **JDK 17** and is currently failing to compile for the same reason; don't trust/extend CI Ч build and test locally on JDK 21 with `-Ptest`.
- `pom.xml`: `sourceDirectory=diesel`, `testSourceDirectory=src/test/java`. Surefire default heap is `-Xmx512m` via the `test.heap` Maven property (override: `-Dtest.heap=4g`). Tests needing >1GB heap carry the `@LargeTest` annotation and are skipped unless `-Ddiesel.largeTests=true` is forwarded to the test JVM (`pom.xml` `systemPropertyVariables`).

## Tests

- `mvn test` runs ONLY `AllTestsSampleTest` + `QuantitativeTest` (the acceptance gate) Ч **not** the full suite. The gate runs at `-Xmx512m` (default); the >1GB `@LargeTest` ORDER BY joins are skipped.
- Full suite (~4.5 min): `mvn -Ptest test` (512m, large skipped). With the >1GB joins: `mvn -Ptest -Ddiesel.largeTests=true -Dtest.heap=4g test`. Single class: `mvn -Dtest=ClassName test` (overrides includes).
- Tests live in `package diesel` (not `com.dieseldb`); engine classes are package-private, so new test files MUST also be `package diesel`.
- `AllTestsSampleTest`/`QuantitativeTest` are split into per-group `@Test` methods (`@TestInstance(PER_CLASS)` + `@TestMethodOrder(OrderAnnotation)`, one method per functional group, each well under 50MB heap); the two 600x600 ORDER BY joins (360k rows, peak ~0.7Ц1.5GB) live in a separate `@LargeTest` method in each class. Pass criterion: every group logs "N passed, 0 failed" and surefire shows `Failures: 0, Errors: 0` (default runs also show `Skipped: 1` per class for the heavy join). Their `RECORD_COUNT = 600`; expected row counts are calibrated for 600 rows Ч never change it.
- `@LargeTest` = `diesel/LargeTest.java`: composed annotation (`@Test` + `@Tag("large")` + `@EnabledIfSystemProperty(diesel.largeTests=true)`); skipped by default and in CI.
- `Database.executeQuery` wraps all parse/exec errors in `RuntimeException("Query execution failed: " + msg, cause)` Ч assert via `getMessage().contains(...)`, not exception type.
- Tests create/delete `*.csv`/`*.table` artifacts in the repo root (e.g. `USERS.csv`, `TXN*.csv`) and sometimes delete tracked files (e.g. `NULL_TEST.table`). Always `git status` before committing.

## Timing regression check (required per prompt; user insists timings must not degrade)

- `mvn test` makes `AllTestsSampleTest` write `timingN.md` (auto-numbered) in the repo root. The default run (large joins skipped) records ~138 queries; to compare against the 140-query `timing.md` baseline INCLUDING the two 600x600 ORDER BY joins, run the timing with the large joins enabled: `mvn -Ddiesel.largeTests=true -Dtest.heap=4g test`. `timing.md` is the kept baseline Ч never delete it.
- Compare: `& "C:\Users\user\AppData\Local\Temp\opencode\compare-timing.ps1" -BaseFile timing.md -NewFile timingN.md`
- Pass: aggregate ratio ? ~1.2 AND every >20% degraded row is a sub-11ms micro-query. Machine noise band is ~0.5xЦ2.4x aggregate; the three >100ms 600x600 join/subquery queries are the real gauge. If the run catches heavy load (aggregate >1.2, many degraded), rerun and use the clean run.

## Profile check (per prompt)

- Driver (outside the repo): `C:\Users\user\AppData\Local\Temp\opencode\diesel\ProfileMain.java`. Compile against `target/classes`, run `-Xmx4g` from the repo root (reads `config.properties` + `classpath.txt`). Compare its two 360k-row joins to the previous prompt's numbers (2.9.17: 2184/1939 ms).
- Complexity sign-off is part of every commit: "no new O(n^2)/O(n!)".

## Commit conventions

- Append ONE long single-paragraph entry to `Changelog.md` (entries are appended, not sorted), e.g. `2.9.15 prompt 15 ...`: what changed, per-class test results, timing summary (aggregate + degraded/improved/stable + noise justification), complexity check. Next number: increment the latest 2.9.N (currently 2.9.15).
- Commit message = the changelog text (use `git commit -F <file>` for the long message on Windows). Push to `origin/main`.
- Stage only intended files with explicit `git add <paths>` Ч NEVER `git add -A`/`.`.

## .gitignore is broken

`.gitignore` contains a CI YAML, not ignore patterns. Everything untracked shows in `git status`. Never stage: `target/`, `.idea/`, `.kilo/`, `.qwen/`, `.codebuddy/`, `.mimocode/`, `*.csv`, `*.table`, `timing*.md`, `timing/`, `run*.log`, `classpath.txt`, `config.properties.bak`, `baseline_run.log`.
`benchmark_report.md` IS tracked and auto-regenerated by `PerformanceTest` on each `-Ptest` run; it is committed per convention as "test artifact only".

## Architecture (not obvious from filenames)

- Entrypoints: `diesel/Database.java` (`executeQuery(String, UUID txnId)` dispatch), `DatabaseServer.java`, `DatabaseClient.java`, `CliRepl.java` (`CliRepl [host] [port]` / `--local [dataDir]`), root `start-server.bat/.sh`, `start-client.bat/.sh`.
- Parsing chain: `QueryParser` (main) + `SqlLexer` + `SubqueryParser` (derived tables/subqueries); `ExplainQuery` (EXPLAIN / EXPLAIN ANALYZE); `AnalyzeTableQuery` (ANALYZE TABLE, 2.9.14). New statement types implement `Query<T>` in `diesel/`.
- Persistence: each table saved as `<NAME>.csv` (data) + `<NAME>.table` (serialized metadata) in the Database `dataDir` (default `.` = repo root). `config.properties` is read from the CWD.
- Config keys (`config.properties`, tracked): `max.inmemory.rows=10000` (streaming/spill threshold), `max.hash.table.size.mb=512` (hash-join budget ? partitioned spill join), `max.result.rows=1000000` (result-row limit, prompt 12), `server.socket.timeout=30000`, `transaction.isolation.level=SERIALIZABLE`.
- Join strategy: byte/row budget estimate + (since 2.9.14) table statistics (`preferNestedLoopByStatistics`, ~10-row tables ? nested loop, larger ? hash join). Index types: B-tree, hash, unique, clustered.

## Task specs

`prompt2.md` (stage-1, Russian, "‘айлы:"/"“есты:" hints), `analytics/prompt-stage0.md` (stage-0), `Roadmap_Parquet_Stage1.md` (roadmap). `Changelog.md` is the authoritative per-prompt record; `testfail.md` records failed runs; `PHASE0_CHANGES.md`/`PERSISTENCE_README.md` document the persistence layer.
