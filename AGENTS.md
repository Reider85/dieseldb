# AGENTS.md

DieselDB: an experimental file-persisted SQL database in Java (package-private engine, ~39 classes in `diesel/`), driven prompt-by-prompt from `prompt3.md` (stage-1, 100 prompts).  
Each prompt ends with a Changelog entry + commit + push. Remote: `github.com/Reider85/dieseldb.git`.

---

## AI Agent Quick Start (opencode/kilocode)

**Workflow for each prompt:**

0. **Pre-flight check (MANDATORY, ~15s, saves 10+ min):**
   ```powershell
   git fetch origin main
   git log --oneline -8
   git log --oneline origin/main..HEAD   # local-only commits (should be empty)
   git log --oneline HEAD..origin/main   # remote-only commits (must rebase first)
   Select-String -Path PROMPT_STATUS.md -Pattern "<prompt-id>"
   ```
   If the prompt is already DONE (in `PROMPT_STATUS.md` or the last 8 commits) — **do not re-implement and do not ask the user**; pick the next TODO instead. Never burn a session re-verifying completed work.
   If `origin/main` is ahead — rebase **before** implementing, not at push time (avoids mid-release version collisions).

1. Read `PROMPT_STATUS.md` → select next TODO with highest priority.
2. Read the prompt source (`prompt3.md` / `prompt4.md` / `o2-prompts.md` — whichever owns the task) → find detailed description.
3. Implement changes.
4. **Choose test depth based on change type:**

   | Change type | Step 4 (fast gate) | Step 5 (acceptance) |
   |-------------|--------------------|---------------------|
   | Sonar fix, rename, constant extract, comment-only | `make compile` → `make test-incr` | Skip → go to step 7 |
   | New feature, query logic, bugfix | `make test` | `make timing-quick` |
   | JOIN, hash join, performance, schema change | `make test` | `make timing-quick` + `make check-profile` |
   | WAL / MVCC / recovery / checkpoint / storage-format | `make test` (+ targeted profile) | **SKIP `make timing-quick`** — not on query path (see Timing Policy) |
   | Test infrastructure, build target, quarantine (TESTS-*) | `make compile` → `make test-incr` | the prompt's own acceptance checks (report counts, opt-in target); **SKIP `make timing-quick`** |

5. Run full acceptance gate with `make timing-quick` (alias: `make timing`) **only if step 4 requires it** (see Timing Policy below — WAL/MVCC/recovery work skips it by default). Use `make timing-full` (full `large` profile, 10-30 min) only for schema change / hash-join refactor / `large` profile work.
6. **Profile check (Strict Condition):** Look at the current task description in `prompt2.md`. If the description does **NOT** contain the words "JOIN", "hash join", or "performance" — **SKIP this step entirely**. Otherwise, run:
   ```bash
   make check-profile
   ```
   *Check exit code only: 1 = failure, 0 = success.*

7. Update `PROMPT_STATUS.md` → mark as DONE. Write this **before** running
   `make release` so it lands in the same commit — never as a separate
   "Update PROMPT_STATUS.md" commit. The entry must **not** contain a
   `Committed as X.Y.Z (hash)` line (the commit does not exist yet);
   `Changelog.md` + git history are the authoritative version record.
8. Create changelog + commit (one command):
   ```bash
   make release DESC="short description of changes"
   ```
   This auto-increments the version, appends to `Changelog.md`, commits, and pushes.
   Use `make release-local DESC="..."` if you need to commit without pushing.
   The release target stages `PROMPT_STATUS.md` explicitly along with everything else.

---

## Fast-Path Workflow (for simple changes)

For Sonar fixes, renames, constant extraction, comment-only changes:

1. `make compile` — verify syntax (3-5s)
2. `make test-incr` — fast profile, incremental (6-13 min on this machine; same suite as `make test` since TESTS-1)
3. Update `PROMPT_STATUS.md` → mark as DONE (same rule: no `Committed as ...` line).
4. `make release DESC="..."` — changelog + commit + push

Skip `make timing` and `make check-profile` for non-performance changes.

## Debugging Workflow (for test failures)

1. Isolate the test: `make test-one T=TestClassName#methodName`
2. Fix the code
3. Re-run isolated: `make test-one T=TestClassName#methodName`
4. Full gate (only after isolated passes): `make test`

**Batch before you full-gate:** a full `make test` costs 6-13 min. If the run
surfaced 2+ independent failures (typical for parallel-race work), fix ALL of
them first, verify each with `test-one` (parallel bash calls), and only then
spend ONE full-gate run. TESTS-3 re-ran the full suite after every single fix
→ 9 red runs before first green; batching the same fixes would have been ~3 runs.

**Isolation cost notes:**
- `@Tag("large")` classes (e.g. VacuumTest): isolation = full `-P large` run with 1M-row tests (~10 min). Before re-running, read `target/surefire-reports/TEST-*.xml` suppressed-exceptions + stack trace — the root cause is usually visible without a re-run.
- If you need to verify **2+ independent** isolated tests, run them as **parallel bash tool calls** in one message (they are independent Maven invocations).
- **Do not read a full `make test` stdout** — it is 50-150 KB of per-INSERT INFO
  logs. The summary you need is always in the last ~30 lines: `Tests run: N,
  Failures: X, Errors: Y`, the `EXIT_CODE`/`WALL_SEC` tail, and
  `target/surefire-reports/TEST-*.xml` for stack traces. Grep the saved
  tool-output file instead of scrolling (`Select-String -Path <tool-output>
  -Pattern "FAILURE|ERROR|Tests run:"`).

### Parallel-race pre-flight sweep (MANDATORY before first `make test` after touching JUnit parallelism)

The TESTS-3 session cost **9 red full-suite runs (~55 min)** because shared-state
races surfaced one-per-run: each fix required a full re-run to discover the next
race. Do the sweep ONCE upfront instead — it takes ~2 minutes and finds every
pattern below:

```powershell
# 1. System-property keys mutated by 2+ classes (race: one class clears in
#    @AfterEach what another still needs) -> @ResourceLock per key
Select-String -Path "src\test\java\**\*.java" -Pattern "System\.(set|clear)Property" -List |
  ForEach-Object { Split-Path $_.Path -Leaf }

# 2. Shared static @TempDir in a base class (race: parallel subclasses overwrite
#    each other's injected path -> NoSuchFileException on *.tmp in @AfterAll).
#    Fix = @ResourceLock on EVERY subclass. Do NOT make it non-static: with
#    @TestInstance(PER_CLASS) an instance @TempDir is still null in a non-static
#    @BeforeAll (NPE in setupCommonTables - verified empirically TESTS-3).
Select-String -Path "src\test\java\**\*.java" -Pattern "static\s+Path\s+tempDir"

# 3. Wall-clock / microsecond / throughput assertions (starve under 4-way class
#    parallelism on this 4-core box) -> @Isolated BEFORE the first run:
Select-String -Path "src\test\java\**\*.java" -Pattern `
  "ms, expected|took .*ms|expected < 100|< 200 ms|< 500 ms|p99|at some point"
# Known set (all already @Isolated since TESTS-3): PasswordHasherTest,
# WALRecoverLsnIsOLogNTest, WALFindSegmentIsLogNTest, PersistFlusherTest,
# CopyAlgorithmTest, WALBackpressureTest, FindNewIndexAfterCompactIsO1Test,
# GroupCommitThroughputTest

# 4. Handlers on GLOBAL java.util.Logger loggers (race: capture foreign threads'
#    records). Fix = filter by logRecord.getThreadID() == owner thread.
Select-String -Path "src\test\java\**\*.java" -Pattern "addHandler"

# 5. Fixed Thread.sleep + assert timing (race: scheduler delayed under load).
#    Fix = Awaitility (org.awaitility is already a dependency):
#    await().atMost(2, SECONDS).until(() -> condition)
Select-String -Path "src\test\java\**\*.java" -Pattern "Thread\.sleep\(\d+\);\s*$" -Context 0,3
```

**Acceptance for the sweep:** every hit above either (a) carries
`@ResourceLock`/`@Isolated`, (b) filters by thread, or (c) uses Awaitility —
BEFORE the first `make test`. A missed pattern costs one full 7-13 min red run.

**junit-platform.properties gotchas (TESTS-3):**
- `junit.jupiter.execution.parallel.mode.default=same` is **invalid** in JUnit
  5.10 — it logs `Invalid parallel execution mode 'SAME'` and silently falls
  back. Use `same_thread` (lowercase enum name).
- The file applies to ALL profiles. `concurrency`/`network` force
  class-parallelism OFF via `junit.jupiter.execution.parallel.enabled=false`
  in their surefire `systemPropertyVariables` (TESTS-8: `reuseForks=true`,
  ONE shared serial JVM per profile — the system property beats the
  properties file; @Isolated/@ResourceLock are mutual-exclusion no-ops there
  and only become active if parallelism is re-enabled). `perf`/`large` are
  safe from cross-class parallelism because they use `reuseForks=false`
  (one class per fork), but `fast`/`core`/`all`/`ci` get real parallelism.
- Instance `@TempDir` + `@TestInstance(PER_CLASS)` + non-static `@BeforeAll`
  = null field (injection happens after `@BeforeAll`) — keep `static` and
  serialize subclasses with `@ResourceLock`.

---

**Priority Queue (Pareto 20% → 80% results):**
| ¹ | Priority | Problem | Files |
|---|----------|---------|-------|
| 1 | CRITICAL | JOIN OR – OOM | SelectQuery.java, QueryParser.java |
| 2 | CRITICAL | IN+AND ignored | QueryParser.java |
| 3 | HIGH | GROUP BY unique – 1 row | SelectQuery.java |

---

## Build & run (Windows)

- **RULE:** NEVER call `mvn` directly without setting `JAVA_HOME` first. Always prefer `make` commands to avoid path issues.
- Maven is NOT on `PATH`. If you absolutely must call `mvn` manually, use this exact prefix:  
  `$env:JAVA_HOME = "C:\Program Files\Axiom\AxiomJDK-21"; & "C:\tools\apache-maven-3.9.6\bin\mvn.cmd" <args>`  
  (JDK 17 no longer compiles – pom.xml requires 21).
  **Alternative (if AxiomJDK not installed):**
  `$env:JAVA_HOME = "C:\tools\jdk-21.0.12+8"; & "C:\tools\apache-maven-3.9.9\bin\mvn.cmd" <args>`
- `mvn package` produces a jar without a usable `Main-Class`; launch the engine via `diesel.DatabaseServer` or use `start-server.bat/.sh`.
- Build: `make build` (or `mvn package -DskipTests`).

### PowerShell Quick Reference (for agents that can't use make)

**`make` is NOT on PATH.** Use `scripts\make.ps1` as a direct replacement:

| Task | Command |
|------|---------|
| Compile check | `.\scripts\make.ps1 compile` |
| Run one test | `.\scripts\make.ps1 test-one -T "QueryParserTest#testSelect"` |
| Fast test suite | `.\scripts\make.ps1 test` |
| Core test suite | `.\scripts\make.ps1 test-core` |
| Build JAR | `.\scripts\make.ps1 build` |
| Full release | `.\scripts\make.ps1 release -Desc "description"` |
| Sync pom.xml version | `.\scripts\make.ps1 sync-version` |

**Or use `scripts\mvn.ps1` for direct Maven calls (auto-sets JAVA_HOME):**

| Task | Command |
|------|---------|
| Compile check | `.\scripts\mvn.ps1 -B compile -q` |
| Run one test | `.\scripts\mvn.ps1 -B test -P fast -Dtest="ClassName#method"` |
| Fast test suite | `.\scripts\mvn.ps1 -B clean test -P fast` |
| Core test suite | `.\scripts\mvn.ps1 -B clean test -P core` |
| Build JAR | `.\scripts\mvn.ps1 -B package -DskipTests` |
| AVRO storage tests | `.\scripts\mvn.ps1 -B clean test -P core -Ddiesel.storage.type=avro` |

**Git helpers** — use `scripts/git-helpers.ps1` for safe git operations:

| Task | Command |
|------|---------|
| Push (with retry, 3 attempts) | `.\scripts\git-helpers.ps1 push` |
| Fix stale git lock files | `.\scripts\git-helpers.ps1 fix-lock` |
| Commit + push (safe) | `.\scripts\git-helpers.ps1 commit -m "description"` |
| Short status | `.\scripts\git-helpers.ps1 status` |

## Git Rules

**NEVER commit these directories/files** (they are in `.gitignore`):
- `target/` — compiled `.class` files, build output
- `data/` — runtime database files (`.csv`, `.table`, `.parquet`)
- `logs/` — application logs (`.log`, `.log.gz`)
- `timing/` — benchmark timing files

If you accidentally stage them, run: `git rm -r --cached target/ data/ logs/ timing/`

**Git push best practices:**
- **Always push to `origin main`** — never use `origin master` (the branch is `main`)
- If push fails, run `.\scripts\git-helpers.ps1 fix-lock` first, then retry
- Never force-push. If history diverges, ask the user.
- The `make release` target handles push automatically with the correct branch
- **Pre-release fetch (multi-agent safety):** before `make release`, run `git fetch origin main`. If `origin/main` moved since your session started (another agent/user pushed), **rebase first** (`git rebase origin/main`), then run release. This avoids the push-rejected → rebase → Changelog conflict → renumber cascade that cost ~5 min on 3.3.20.
- **Version collision recipe** (push rejected "fetch first", remote has the same version you just committed):
  1. `git fetch origin main && git rebase origin/main`
  2. Resolve `Changelog.md` / `changelog_entry.txt` conflicts: keep the remote entry, renumber YOUR entry to the next free version (e.g. remote took 3.3.19 → yours becomes 3.3.20).
  3. Update `pom.xml` `<version>` to the new number (or `make sync-version DESC=X.Y.Z`).
  4. `git add` resolved files → `git -c core.editor=true rebase --continue`
  5. Amend the commit message if its version prefix is now wrong: `git commit --amend -m "X.Y.Z ..."`.
  6. `git push origin main`.

## Tests

### Quick Test Reference

| Goal | Command | Time |
|------|---------|------|
| Check it compiles | `make compile` | 3-5s |
| Run one test (fast profile only) | `make test-one T=QueryParserTest#testSelect` | 1-2 min (includes Maven startup + test-compile) |
| Fast suite (incremental) | `make test-incr` | 6-13 min |
| Fast suite | `make test` | 6-13 min (no clean since TESTS-1) |
| Core suite | `make test-core` | 6-8 min |
| Quarantined flakes (opt-in, 5 tests) | `make test-flaky` | ~7-10 min (4 invocations, no clean, +1 isolated retry on red: GroupCommit → 1M-бенчмарк → FlusherTest → WALReadAll-бенчмарк) |
| Timing gate (query-path default) | `make timing-quick` (alias `make timing`) | 3-5 min |
| Timing gate (full large profile) | `make timing-full` | 10-30 min |

**Measured 2026-10-24 (TESTS-3 session, 4-core machine):** fast = 546s baseline / 389-407s after JUnit parallelism, core = 381-407s. The fast floor is dominated by `OldVersionTimeoutTest` (~280s reaper waits) + `AutoJoinIndexTest` benchmark (~75-180s) + `WALRecoverLsnIsOLogNTest` (10-170s) — these run in every fast pass, so **any estimate below ~6 min for `make test` is wrong**. Old table values (~15-30s) were from a different machine/era and caused agents to set 10-min timeouts that killed healthy runs.

**`make test-one` is fast-profile only** (`-Dtest=... -P fast`). For a test tagged `storage`/`query-full` (core profile) it runs **0 tests** and `assert-tests-ran` fails the build. Use instead:
```powershell
.\scripts\mvn.ps1 -B test -P core -Dtest="SomeCoreTest#someMethod"
```
**Quarantined `@Tag("flaky")` tests run through neither command** — every gate excludes
them (`surefire.excludedGroups` ends in `,flaky`). Run them only via `make test-flaky`
(profile `flaky` sets `diesel.runFlaky=true`), see Known Flakes.

### Guard: "no tests ran" fails the build

The default surefire `<configuration>` in `pom.xml` excludes **every** source file
(`<exclude>**/*.java</exclude>`). A run without `-P <profile>` therefore selects
zero tests and still prints `BUILD SUCCESS`. A commit that did not compile once
passed the local gate this way.

Every test target now asserts that surefire actually wrote at least one report
newer than the moment Maven was invoked:

- PowerShell: `scripts/native.ps1` → `Assert-TestsRan` (used by `make.ps1` and `tia.ps1`)
- Make: `scripts/assert-tests-ran.sh` (used by the `run-tests` macros)

A successful run prints `Verified: N surefire report(s) written by profile 'fast'.`
If you see `ERROR: no surefire reports were written ...` the profile was lost from
the Maven command line — treat it as a hard failure, not a pass.

**Do not add a `param()` block to `scripts/mvn.ps1`.** A `param([string[]]$MvnArgs)`
with `ValueFromRemainingArguments` silently drops `-P <profile>` and every `-D`
property, which is exactly the failure this guard exists to catch. The wrapper
relies on the automatic `$args` variable, which preserves them.

- **Fast profile** – `make test` (= `mvn -B test -P fast`, tags: smoke, query, index; **no clean** since TESTS-1) runs only fast unit tests. **Use this as a first filter** before the heavy acceptance gate.
- **Other profiles:** `make test-core` (query-full, storage), `make test-concurrency` (concurrency), `make test-network` (network), `make test-perf` (perf), `make large-test` (large, 4GB heap).
- **Format-specific storage profiles:** When `storage.type` is set in `config.properties` (or via `-Ddiesel.storage.type=`), only tests matching that format run. The `@StorageType` annotation gates format-specific test classes.

| Profile | Command | What it runs |
|---------|---------|-------------|
| `storage-csv` | `make test-storage-csv` | Only CSV storage tests |
| `storage-tsv` | `make test-storage-tsv` | Only TSV storage tests |
| `storage-jsonl` | `make test-storage-jsonl` | Only JSONL storage tests |
| `storage-avro` | `make test-storage-avro` | Only AVRO storage tests |

**How it works:** Each profile sets `diesel.storage.type` as a system property. JUnit 5's `@StorageType` annotation (custom, in `diesel/StorageType.java`) checks this property at test-discovery time and disables classes whose declared type doesn't match. For example, `@StorageType("avro")` on `AvroCompressionTest` means that test only runs when `diesel.storage.type=avro`.

**Manually overriding storage.type for tests:**
```bash
# Run only AVRO storage tests (bypasses Makefile):
$env:JAVA_HOME = "C:\Program Files\Axiom\AxiomJDK-21"; & "C:\tools\apache-maven-3.9.6\bin\mvn.cmd" -B clean test -P core -Ddiesel.storage.type=avro

# Run only CSV storage tests:
$env:JAVA_HOME = "C:\Program Files\Axiom\AxiomJDK-21"; & "C:\tools\apache-maven-3.9.6\bin\mvn.cmd" -B clean test -P core -Ddiesel.storage.type=csv
```

**Test class annotations:**
- `@StorageType("avro")` — Avro tests (4 classes): AvroCompressionTest, AvroDataFileReaderTest, AvroRowStorageTest, AvroSchemaTest
- `@StorageType("jsonl")` — JSONL tests (17 classes): all `Jsonl*Test` files
- `@StorageType("csv")` — CSV tests (4 classes): CsvIndexManagerTest, CsvLargeFileStressTest, CsvStorageAdvancedTest, CsvStorageTest
- `@StorageType("tsv")` — TSV tests (2 classes): TsvStorageAdvancedTest, TsvStorageTest
- `@StorageType({"jsonl","csv"})` — Cross-format tests (2 classes): JsonlLoadModeTest, StorageLoadModeTest
- `@StorageType({"csv","tsv"})` — Delimited tests (4 classes): CharsetEncodingTest, CompressionTest, AtomicFileWriteTest, CsvTsvHeaderMappingTest
- `@StorageType({"avro","csv","tsv","jsonl"})` — Codec/cross tests (5 classes): SnappyOptimizedCodecTest, SnappyOptimizationBenchmark, DelimitedByteParserTest, ReaderCorrectnessTest, JsonStreamAbstractionTest
- **Full release gate** – `make all-tests` = ONE `mvn clean verify -P all` invocation (the `all` profile aggregates the fast+core+concurrency+network tag groups; perf and large are excluded by design). `make all-tests-large` = `all-tests` + `perf` + `large` — the complete 6-profile gate, **recommended for tag-pushes**.
- **Perf test isolation** – Performance tests (`perf` profile) run with `forkCount=1` and no parallel execution to avoid CPU contention. The `all` profile excludes `perf` to prevent interference with other test types. Run performance regression checks with `make test-perf` or `make check-profile` (JOINs only).
- **TIA** – `make tia` (recommend profiles) / `make tia-run` (recommend + run).
- **Isolation Rule for Failures:** If `make timing` fails, DO NOT immediately re-run `make timing`. Find the exact failing test name in the log. Fix the code and run ONLY that specific test:
  ```bash
  make test-one T=TestClassName#methodName
  ```
  Re-run `make timing` **ONLY AFTER** the isolated test passes. This saves minutes on heavy workloads.
- **Database.close() hard rule:** Any test that constructs `Database` MUST close it (`@AfterEach`/`@AfterAll` null-guarded `database.close()`). A `@TempDir` delete error when a `Database` was created is **not a flake** — it is a missing-close bug: the persist-flusher daemon and open WAL/file handles block JUnit temp-dir cleanup. On the FIRST such failure: add `close()`, do not "re-run to see if it passes". If the same pattern appears in multiple classes, **sweep in batch**: `Select-String -Path src/test/java/**/*.java -Pattern "new Database\("` and add close() to every offender in one pass — do not fix one class per release (precedent: 3.3.16 → 3.3.18 → 3.3.19 → 3.3.20 each fixed a different subset).
- The gate expects `Failures: 0, Errors: 0`. The script `compare-timing.sh` / `scripts/compare-timing.ps1` will automatically ignore rows with baseline **< 2.0s** (machine noise — TESTS-6 measured ±2× run-to-run jitter on sub-2s methods on this machine, so the old 11ms floor could never hold a ±20% band) and only treat degradation >20% on **heavy (≥2s)** queries as a failure. If heavy queries are stable, the script returns exit code 0.
- **Large-output discipline:** the `large`, `perf` and `timing-quick` profiles are **quiet since TESTS-12** (logback root WARN + JUL `.level=WARNING` + `-Dlogging.level.diesel=WARNING` honored by QueryParser's JUL ConsoleHandler — measured 2026-10-26: `large-test` stdout **140.9 MB → 0.01 MB**, wall **19:38 → 5:39**). The spam is NOT gone everywhere: `make test-core` still produces **~30 MB** and `make test-incr` **~2.7 MB** (QueryParser JUL INFO per INSERT; FlusherTest ~10k inserts). When output is truncated, do **not** re-run blindly: grep the saved tool-output file for `EXIT_|Tests run:|BUILD|FAILURE`. For diagnosis of failures prefer `target/surefire-reports/TEST-*.xml` (suppressed exceptions + stack traces) over scrolling stdout. Candidate follow-up: the TESTS-12 quiet-logging config extended to the `core`/`flaky` profiles.

### Test Cache Cleaning

| Command | What it clears | When to use |
|---------|---------------|-------------|
| `make clean-cache` | `~/.m2/build-cache/v1/com.dieseldb/dieseldb/` (this project's build cache only) | When build cache returns stale results after config changes |
| `make clean-test-cache` | `target/.cache`, `target/surefire-reports/`, `target/test-classes/` | Stale test results, flaky test debugging, before re-running specific tests |
| `make clean` | Everything above + `target/` + `data/*.csv` + `data/*.table` + logs + build cache | Full rebuild, before release |

**Note:** `mvn clean` also automatically cleans the build cache for this project via `maven-clean-plugin` fileset configuration in `pom.xml`.

**Cost of a clean run (TESTS-5):** every `clean` Maven invocation (`make all-tests` / `make all-tests-large` start with `clean verify -P all`, any `-clean` target) wipes `~/.m2/build-cache/v1/com.dieseldb/dieseldb/`, so the *next* build pays a **full recompile ≈ 1.5–2.5 min** on top of the run itself — and that javac blast ALSO contaminates any load-sensitive test that runs immediately afterwards (it pushed the group-commit p99 from ~5–13ms to 42ms). Budget this; do NOT add `clean` to routine dev targets (that was exactly TESTS-1's point), and never lead a p99/timing assertion with a clean run.

**Tip:** Use `make clean-test-cache` before `make test` if you suspect stale test cache is causing false passes/failures. It's ~10x faster than `make clean`.

---

## Test-Impact Analysis (TIA) — test mapping script

`scripts/tia.ps1` + `scripts/tia-mapping.txt` map changed source files (via `git diff`) to the JUnit 5 `@Tag` buckets they impact, then recommend the Maven profiles to run. Use it to avoid re-running the whole suite after a small change.

**How to run (from repo root, Windows):**

| Command | What it does |
|---|---|
| `make tia` | Dry run: prints changed files, recommended tags and profiles |
| `make tia-run` | TIA, then sequentially runs each recommended profile (`mvn -B clean test -P <profile>`) |
| `powershell -ExecutionPolicy Bypass -File ./scripts/tia.ps1` | Direct call, compares against `origin/main...HEAD` |
| `powershell -ExecutionPolicy Bypass -File ./scripts/tia.ps1 HEAD~1` | Compare against the previous commit |
| `powershell -ExecutionPolicy Bypass -File ./scripts/tia.ps1 main..feature` | Compare two branches |
| `powershell -ExecutionPolicy Bypass -File ./scripts/tia.ps1 --tags` | Print recommended tags only (for CI integration) |
| `powershell -ExecutionPolicy Bypass -File ./scripts/tia.ps1 --run HEAD~1` | Recommend, then execute the impacted profiles |

- **Exit codes:** `0` = success (or nothing to run), `1` = mapping file missing or a recommended profile failed.
- **Requires:** `git` (the Makefile targets invoke PowerShell: `powershell -ExecutionPolicy Bypass -File ./scripts/tia.ps1`; on Linux use `pwsh` or port the script to bash).
- The default ref `origin/main...HEAD` requires `origin/main` to be fetched: `git fetch origin main` if the diff comes back empty.

**Mapping file (`scripts/tia-mapping.txt`):** one rule per line, TAB-separated:

```
<source path glob> <TAB> <tag>[,<tag>...]
```

- Glob examples: `diesel/storage/*.java`, `diesel/*Message.java`
- Tags are the JUnit 5 tags: `smoke, query, index` → `fast`; `query-full, storage` → `core`; `concurrency` → `concurrency`; `network` → `network`; `perf` → `perf`
- Special values: `pom.xml` → `all` (every profile); tag `none` = no tests
- **Keep it up to date:** when you add a new source file under `diesel/`, add a mapping line for it, otherwise TIA will report "no tests to run" for changes in that file.
- **CI-enforced (TESTS-10):** the `lint-tia-mapping` job (name `tia-mapping-drift` in `.github/workflows/ci.yml`) runs `scripts/check-tia-mapping.sh` on **every PR and push to `main`** and fails the build if any `diesel/*.java` is not covered by a mapping line (exact path or glob in column 1; `*/generated/*`, `*Test.java` and `Main.java` are excluded — a class with no tests gets `<TAB>none`). Local check (bash is NOT on PATH on Windows): `& "C:\Program Files\Git\bin\bash.exe" scripts/check-tia-mapping.sh` (exit 0 = covered, exit 1 = lists the missing files).

**Caution:** do NOT run `mvn test -Dgroups="..."` without a profile — the default surefire config excludes all tests, so 0 tests execute. Always use the profile commands the script prints (`mvn -B clean test -P <profile>`) or run via `make tia-run`.

**Recommended workflow:** implement changes → `make tia` → run the recommended profiles (or `make tia-run`) → then continue with the normal acceptance gate.

---

## Timing regression check (fully automated)

Read the **Timing Policy** section above first — most storage-engine prompts skip this gate.

- `make timing` (alias — routes to `timing-quick` since TESTS-6) / `make timing-quick`:
  1. Wipes `target/surefire-reports/` (the collector sweeps the WHOLE directory and surefire never deletes stale reports from earlier profiles), then runs **only `@Tag("timing")` tests** via the `timing-quick` profile (sequential, 4GB heap) — ~3-5 min. Covers the `timing/timing.md` baseline rows **except `DelimitedIoPerfTest`** (326s+324s), which only `timing-full` re-measures.
  2. Collector builds `timing/timingN.md` from surefire reports: `scripts/collect-timing.py` (Makefile) or `scripts/collect-timing.ps1` (make.ps1 — `python` on Windows may be a dead WindowsApps stub, exit 9009).
  3. Executes `compare-timing.sh timing/timing.md timing/timingN.md` – this script:
  - Compares each query’s time.
  - **Ignores** any query whose baseline is <2.0 s (machine noise) — `MinBaseline=2.0` in both `scripts/compare-timing.ps1` and `compare-timing.sh` (TESTS-6 raised the old 11ms floor: sub-2s methods jitter ±2× run-to-run on this machine, so a ±20% band below 2s can never hold; sub-2s query latency stays gated by `PerformanceRegressionTest` in the `perf` profile with its own 11ms floor).
  - Flags a regression only if a query with baseline ≥2.0 s degrades by >20%.
  - Exits with code `1` if any such regression exists, otherwise `0`.
- `make timing-full`: the pre-TESTS-6 behavior — entire `large` profile (10-30 min) + the same collect/compare. Reserve for schema change / hash-join refactor / `large`-profile work.
- You never need to interpret the output manually – just check the exit code. Follow the red-gate steps in **Timing Policy** (re-run once, then commit-with-noise-note if unrelated — never A/B git-stash without user request).
- Manual compare (PowerShell): `.\scripts\compare-timing.ps1 -Base timing\timing.md -New timing\timingN.md`
- `timing/timing.md` is the tracked baseline – never delete it.

---

## Profile check (fully automated – conditionally skipped)

- Driver: `ProfileMain.java` (outside repo) runs the two critical 360k-row joins. It writes structured results to `profile_results.json`.
- `make check-profile`:
  1. Compiles `ProfileMain.java` against `target/classes`.
  2. Runs it with `-Xmx4g`.
  3. Parses the last profile numbers from `Changelog.md` (looks for the previous prompt’s entry).
  4. Compares the new numbers with the old ones.
  5. Exits with code `1` if any of the two joins degrades by >10%, otherwise `0`.
- **Reminder:** These joins are already executed during `make timing`. Running `make check-profile` repeats that heavy work. Refer to Step 6 in the Quick Start – skip this unless the task explicitly mentions JOINs or performance.

## Changelog automation

- Use `make release DESC="your change description"` – the one-command workflow:
  ```bash
  make release DESC="Fix JOIN OR OOM by implementing hash join spilling"
  ```
  This auto-appends the versioned entry to `Changelog.md`, **syncs `pom.xml` project version** to the same value, commits, and pushes. Version prefix is auto-calculated from the last commit (e.g. `3.1.32` → `3.1.33`); when patch reaches 99 it rolls over — `X.Y.99` → `X.(Y+1).0`, `X.99.99` → `(X+1).0.0`.
  **PROMPT_STATUS.md joins this commit:** update it before running `make release`
  (step 7 of the workflow); the release target stages it explicitly, so a DONE
  entry never becomes its own commit. Do not write `Committed as ...` into
  PROMPT_STATUS.md — the commit does not exist yet when the entry is written.

- Use `make release-local DESC="..."` if you need to commit without pushing.
- Use `make changelog DESC="..."` to only create the entry (no commit/push).

### pom.xml version sync

`pom.xml` project version (`<artifactId>dieseldb</artifactId>` + `<version>`) must match the latest Changelog entry and git commit version prefix. It is **synced automatically** inside `make release` / `release-local` via `scripts/sync-pom-version.ps1` (called from `commit-and-changelog.ps1` with the new version), so agents never need to hand-edit `<version>` during a prompt.

| Command | What it does |
|---|---|
| `make sync-version` | Sync pom.xml with the last Changelog entry |
| `make sync-version DESC=check` | Check-only: exit 1 if pom.xml ≠ Changelog version (no write) |
| `make sync-version DESC=X.Y.Z` | Sync pom.xml to an explicit version |
| `.\scripts\make.ps1 sync-version` | Same as `make sync-version` (Windows) |
| `.\scripts\make.ps1 sync-version -Desc check` | Same as `DESC=check` |
| `powershell -ExecutionPolicy Bypass -File scripts/sync-pom-version.ps1` | Direct script call (default source: Changelog) |
| `powershell -ExecutionPolicy Bypass -File scripts/sync-pom-version.ps1 -Source Commit` | Source version from last commit message |
| `powershell -ExecutionPolicy Bypass -File scripts/sync-pom-version.ps1 -CheckOnly` | Direct check-only mode |

- **Exit codes:** `0` = in sync / updated; `1` = error or check mismatch.
- **What it touches:** only the project `<version>` after `<artifactId>dieseldb</artifactId>`. Dependency and plugin versions are never modified.
- **Agents:** do **not** manually edit the project `<version>` in `pom.xml` during prompt work — `make release` keeps it aligned. If you must set it outside release (e.g. after a bulk history rewrite), use `make sync-version DESC=X.Y.Z`.

---

## Anti-Hang Guards

- **MANDATORY bash timeouts on every test/gate command** — never leave `timeout` unset for Maven/make invocations. The opencode bash tool timeout parameter is the *only* hard stop; a hung `test-one` once burned 7 hours:

  | Command class | `timeout` (ms) | Notes |
  |---|---|---|
  | `make compile` / single tiny test | 300_000 (5 min) | normally 5–10s, **BUT a `pom.xml`/profile edit invalidates the maven build-cache → full recompile of 374 main + 279 test sources ≈ 1.5–2.5 min** (TESTS-5 killed a healthy compile at the old 120s cap and paid a wasted re-run) |
  | `make test-one T=...` | 300_000 (5 min) | includes Maven startup + test-compile; on a build-cache miss it recompiles everything and lands at ~2–3 min — 300s is the floor, not 120s. 0 tests executed = profile lost (see guard) |
  | `make test-incr` / `test` | 1_200_000 (20 min) | fast suite runs 6–13 min on this machine (see Quick Test Reference) — the old 10-min cap killed healthy TESTS-3/TESTS-4 runs |
  | `make test-core` | 1_200_000 (20 min) | core suite runs 6–8 min; with parallel-race triage up to ~10 min |
  | `make test-flaky` | 1_800_000 (30 min) | FOUR mvn invocations (NO clean): group-commit p99 + ONE isolated retry on red (~15s each) + JSONL 1M benchmark (~3–5 min) + FlusherTest 5k-insert (~20s) + WALReadAll microbenchmark (~15s). Continue-on-error — a red flake never stops the remaining invocations. TESTS-5's first attempt ran under the 900s `large-test` cap with `clean` and burned 14.8 min (recompile blast contaminated the very p99 it measured) |
  | `make test-concurrency` / `test-network` | 600_000 (10 min) | |
  | `make test-perf` / `large-test` | 900_000 (15 min) | |
  | `make timing-quick` / `make timing` (alias) | 1_200_000 (20 min) | expected 3–5 min + up to ~2.5 min recompile after a pom/profile edit (TESTS-6) |
  | `make timing-full` | 1_800_000 (30 min) | absolute cap (full `large` profile) |
  | `make all-tests` | 2_400_000 (40 min) | absolute cap |
  | `make all-tests-large` | 3_600_000 (60 min) | all-tests + perf + large; tag-push gate |
  | ProfileMain / `check-profile` | 900_000 (15 min) | |

  **Gate-time budget (TESTS-5 measured, 2026-10-25):** a test-infrastructure prompt costs
  **40–60 min of wall time** on this machine — fast 9:40 (+2:07 for the known-flake isolated
  re-run), core 7:13, compile-after-pom-edit 1.5–2.5 min *each time*, `test-flaky` 6–8 min,
  plus one wasted 14.8-min run for every design/timeout mistake. Plan the anti-hang caps from
  these numbers, not from the "compile 3-5s" folklore.

  If a command exceeds its timeout: kill it, run `make clean-test-cache`, retry **once**; if it hangs again, isolate the test class and check the Known-Flakes list below before debugging.

- **Max fix attempts: 3** — if quick tests or isolated tests fail 3 times in a row, stop and report to the user
- **Never run from agent context** — these commands block forever:
  - `start-server.bat` / `start-server.sh` (long-running TCP server)
  - `start-client.bat` / `start-client.sh` (interactive REPL)
- **Isolation rule cap** — max 3 attempts on the isolated test; if still failing, stop and report
- **Missing make targets** — `make changelog`, `make check-profile` may not exist in the Makefile. If `make` fails, fall back to raw Maven commands as shown in the Tests section above. `make quick-test` has been removed — use `make test`.

## Timing Policy (skip by default for storage-engine work)

`make timing` (alias for `make timing-quick` since TESTS-6) is **3–5 min**: only `@Tag("timing")` tests, sequential, covering the `timing/timing.md` baseline rows except `DelimitedIoPerfTest` (326s×2, gated by `make timing-full` alone). `make timing-full` is the pre-TESTS-6 behavior — entire `large` profile, 10–30 min. Both are **flaky on this Windows machine** (background services, CPU drift). Skip unless the change can actually move query latency:

- **SKIP `make timing-quick` when the prompt touches:** WAL, MVCC, recovery/ARIES, checkpoint, buffer-pool flusher, page storage internals, row-storage plumbing, config/JMX — anything not on the SELECT/JOIN execution path. Precedent: WAL-4/6/8/9, MVCC-3, Prompt 4 #21 all skipped timing with zero regressions.
- **RUN `make timing-quick` when:** JOIN / hash join / GROUP BY / ORDER BY / index / planner / SELECT hot-path code, or the prompt text says "performance". Use `make timing-full` instead for schema change / hash-join refactor / `large` profile changes (it re-measures `DelimitedIoPerfTest` and the M-`large` tests `timing-quick` skips).
- **On a red timing gate (compare exit 1):**
  1. **TESTS-14 retry marker:** a red run (test failure or compare-red) drops `target/.timing-retry-done` — the next timing invocation fails instantly with `ERROR: timing already retried` instead of burning another 10–30 min. Reset it only after addressing the red: `make clean-test-cache` clears the marker (Windows: `.\scripts\make.ps1 clean-test-cache`), then re-run `make timing-quick` **once** only (noise filter). Both `timing-quick` and `timing-full` share one marker; a green run clears it automatically.
  2. If still red: identify the failing query; if your change is not on that path (or isolated query times are unchanged), **commit with a red gate** and note "timing noise, user-approved precedent 3.2.81/3.2.86" — do **not** block the prompt on machine drift. Run `make clean-test-cache` after such a commit so the leftover marker doesn't block the NEXT query-path prompt's timing run.
  3. **Do NOT** run A/B `git stash` proofs against clean HEAD unless the user explicitly asks — they double the cost of an already-failing gate.
  4. Re-seed the baseline (`timing/timing.md` ← latest good `timingN.md`) only after two consecutive clean runs, or when the user approves it.
- **Triage: test-error during `make timing` (different from compare-red):** If the timing run aborts because a test **errored/threw** (surefire BUILD FAILURE with Errors > 0 — e.g. @TempDir teardown, NPE), that is NOT a compare regression:
  1. Fix the root cause (usually Database.close() — see hard rule above).
  2. Isolate only the failing class (`make test-one` or profile-scoped `-Dtest=`).
  3. If the fix touched **only test code / teardown** and no prod query-path code: the full `make timing` re-run is **OPTIONAL** — commit with a note "timing aborted on test error X; isolated re-run green; fix is teardown-only". A full re-run is required only when a prod query-path test errored or prod code changed in the fix.
  4. This prevents wasting a second 10–30 min timing run on a teardown-only incident (precedent: VacuumTest 3.3.20 cost ~40 min across two full timing runs).

## Known Flakes (do not re-debug from scratch)

Failures below are **pre-existing / environmental** on this machine. When one appears: re-run the isolated test once (`make test-one`); if it fails again and your change is unrelated, note it in the changelog and move on. Do **not** spend a session A/B-proving pre-existence.

**Quarantined (TESTS-5):** five documented flakes are out of every gate — they carry `@Tag("flaky")` + `@EnabledIfSystemProperty(named = "diesel.runFlaky", matches = "true")` and every profile's `surefire.excludedGroups` ends in `,flaky`:

- `FlusherTest#testMixedCommittedUncommitted`
- `JsonlStorageAdvancedTest#benchmarkOneMillionRowsCsvVsJsonlVsJsonlZstd`
- `GroupCommitThroughputTest#testAcceptanceThroughputAndP99`
- `GroupCommitThroughputTest#testAcceptanceNonePolicyP99`
- `WALReadAllIsAlreadySortedTest#readAllIsAtLeastAsFastAsTheOldSortPath` (quarantined 3.3.50 — two strikes: red after the 3.2.99 best-of-median fix, red again at 594µs vs 434µs×1.1 under -P all load)

Gates never execute them (no false-red core/perf/timing from these five). Run them on demand with **`make test-flaky`** — it runs **four separate Maven invocations, in this order, with NO `clean`**: (1) `-P flaky -Dtest=GroupCommitThroughputTest` first, with **ONE isolated retry on red** (the p99 assertion is load-sensitive; a clean+full-recompile right before it measured 42ms), (2) `-P flaky-jsonl` (the 1M-row benchmark alone — it needs `diesel.storage.type=jsonl` to pass its `@StorageType` gate; a jsonl engine makes FlusherTest's insert loop take hours, hence the split), (3) `-P flaky -Dtest=FlusherTest` (timing-insensitive), (4) `-P flaky -Dtest=WALReadAllIsAlreadySortedTest` (sub-ms microbenchmark, last). The target is **continue-on-error**: all four always run, the exit code aggregates at the end. Profile `flaky` sets `diesel.runFlaky=true`, `diesel.largeTests=true` and turns class-parallelism off. CI runs all four nightly in the `nightly-flaky` job. A red `make test-flaky`/nightly = the flake got worse — that is the signal, investigate, do not "re-run until green". If a future fix removes a flake, delete its two annotations and the test rejoins the gate automatically.

| Symptom | Class / area | Notes |
|---|---|---|
| Timing compare flags a heavy query, isolated times unchanged | `make timing` overall | CPU/background-service drift; see Timing Policy |
| `FlusherTest#testMixedCommittedUncommitted` O(N²)/slow or flaky | storage/page flusher | pre-existing insert path; thread-dump proven unrelated to MVCC work. **Quarantined (TESTS-5)** — run via `make test-flaky` |
| `JsonlStorageAdvancedTest` 1M-row benchmark exceeds bound | jsonl storage | A/B-proven pre-existing (~70s on clean HEAD). **Quarantined (TESTS-5)** — run via `make test-flaky` |
| Microbenchmarks (e.g. `WALReadAllIsAlreadySortedTest`, Snappy*Benchmark) timing-sensitive | perf | min-of-3 / threshold noise; fix criterion, not code, if red |
| `WALReadAllIsAlreadySortedTest#readAllIsAtLeastAsFastAsTheOldSortPath` "best median" assert fails | WAL readAll benchmark | sub-ms medians starve under 4-way class parallelism; new path measured first (old path gets warm file-cache) + `isLsnAscending` assert runs under `-ea` only on the new path. Two strikes (3.2.99 fix didn't hold). **Quarantined (3.3.50)** — run via `make test-flaky` |
| `CheckpointDurationTest#checkpointDurationEmptyDatabase` "within a few ms" assert fails | storage/checkpoint | NOT a flake source: the outer timer wraps all of `checkpointNow()` while the recorded duration stops before WAL-archiver `runOnce()` + INFO log + write-gate unlock — a structural tail of >10ms on loaded Windows. Bound raised 10→100ms (3.3.50); still catches a broken recorder |
| `GroupCommitThroughputTest#testAcceptanceThroughputAndP99` p99 > 20ms under full-suite load | WAL group commit | load-sensitive timing; passes in isolation p99≈5ms (observed 2026-10-20). **Quarantined (TESTS-5)** together with `#testAcceptanceNonePolicyP99` — run via `make test-flaky` (class stays `@Isolated` for its remaining methods). A p99 measured right after another heavy class in the SAME fork is load contamination, not a regression (TESTS-5 saw 105ms 0.5s after FlusherTest's 50k-row disk writes) — that is why `test-flaky` runs it first, in its own invocation |
| Wall-clock/microsecond proof tests red under full-suite load | PasswordHasherTest, WALRecoverLsnIsOLogNTest, WALFindSegmentIsLogNTest, PersistFlusherTest, CopyAlgorithmTest, WALBackpressureTest, FindNewIndexAfterCompactIsO1Test, GroupCommitThroughputTest | all **@Isolated since 3.3.38** (TESTS-3): their hard thresholds starve under 4-way class parallelism on this 4-core box. Isolated re-run is green → note and move on; do NOT relax the assertion |
| `@TempDir` delete errors under parallel Windows runs | any | **not a flake when Database was created** — missing `close()` (see Database.close() hard rule in Tests); fix close(), never "retry to see"; classes with `Database(tempDir)` must call `db.close()` in `@AfterEach` (fixed in CopyOnWriteIsolationTest/ConcurrentConflictTest/RollbackTest/UndoLogSpillTest/IsolationSemanticsTest/MvccDeleteTest/MvccUpdateTest/LazyDeleteTest/VacuumTest 2026-10-20). Pure-logic races (no Database) → retry isolated once |
| ru-RU locale breaks "0.30" string asserts | any toString/format | use `Locale.ROOT` (past bug in AdaptiveFlushStrategy) |
| `WALBackpressureTest#queueFullBlocksProducerAndLosesNoData` "queue nearly full" assert fails | storage/WAL backpressure | load-sensitive timing; passes in isolation (observed 2026-10-19) |

## Subagent Discipline

- **Always pass `timeout_ms`** when spawning subagents — never leave it unset:
  - `explore` subagent: `timeout_ms: 300000` (5 min)
  - `general` subagent: `timeout_ms: 600000` (10 min)
- **On stall notification** (>60s no activity): cancel immediately, fall back to manual grep/glob/read
- **On UnknownError**: don't retry the same subagent — do the work directly with grep/glob/read
- **Never wait more than 2 min** for a stalled subagent — cancel and proceed manually

## Plan-Mode Diagnostic Workflow

When plan mode blocks a file write you need for diagnostics (profiling scripts, test scripts, benchmarks):

1. **Option A — Inline bash**: Run profiling commands directly without writing a file:
   ```bash
   python -c "import sqlite3; conn = sqlite3.connect('...'); ..."
   ```
2. **Option B — Defer to plan**: Document the diagnostic command in the plan file, get approval, then write+run after `plan_exit`
3. **Option C — Read-only analysis**: Use grep/glob/read to analyze existing data; document findings in the plan

**Never get stuck in analysis paralysis.** If you can't write a diagnostic script, use read-only tools and move on.

## Windows Command Reference (PowerShell)

This project runs on Windows. Do NOT use Unix commands — use PowerShell equivalents:

| Unix | PowerShell | Notes |
|------|-----------|-------|
| `wc -l file` | `(Get-Content file).Count` | Count lines |
| `wc -w file` | `(Get-Content file \| Measure-Object -Word).Words` | Count words |
| `head -n 10 file` | `Get-Content file -Head 10` | First N lines |
| `tail -n 10 file` | `Get-Content file -Tail 10` | Last N lines |
| `grep "pattern" file` | `Select-String -Path file -Pattern "pattern"` | Search in file |
| `cat file` | `Get-Content file` | Read file |
| `touch file` | `New-Item -ItemType File -Path file -Force` | Create file |
| `rm file` | `Remove-Item file` | Delete file |
| `cp src dst` | `Copy-Item src dst` | Copy file |
| `mv src dst` | `Move-Item src dst` | Move/rename |
| `find . -name "*.java"` | `Get-ChildItem -Recurse -Filter "*.java"` | Find files |
| `sort file` | `Get-Content file \| Sort-Object` | Sort lines |
| `uniq` | `Select-Object -Unique` | Deduplicate |
| `diff a b` | `Compare-Object (Get-Content a) (Get-Content b)` | Compare files |

**BOM trap (cost a compile cycle in TESTS-3):** PowerShell 5.1
`Set-Content -Encoding UTF8` / `Out-File -Encoding UTF8` write a UTF-8 **BOM**
(`\ufeff`) — javac then fails with `illegal character: '\ufeff'` at line 1.
When batch-editing `.java` via PowerShell, write without BOM:
```powershell
[System.IO.File]::WriteAllText($path, $text, (New-Object System.Text.UTF8Encoding($false)))
# strip BOM already written to a file:
$c=[System.IO.File]::ReadAllText($path); [System.IO.File]::WriteAllText($path,$c,(New-Object System.Text.UTF8Encoding($false)))
```

**Inverse trap — non-ASCII in BOM-less `.ps1` (cost a test-flaky run in TESTS-5):**
PowerShell 5.1 *reads* a UTF-8 file **without BOM** using the ANSI codepage, so a
single em-dash (`U+2014`) inside `scripts/*.ps1` breaks the parser with a cascade of
`UnexpectedToken` / `Expected final ')'` errors. Keep `.ps1` files **ASCII-only**
(dash `-` instead of `—`); if you must use non-ASCII, add a UTF-8 BOM first.
```