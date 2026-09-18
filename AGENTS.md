# AGENTS.md

DieselDB: an experimental file-persisted SQL database in Java (package-private engine, ~39 classes in `diesel/`), driven prompt-by-prompt from `prompt3.md` (stage-1, 100 prompts).  
Each prompt ends with a Changelog entry + commit + push. Remote: `github.com/Reider85/dieseldb.git`.

---

## AI Agent Quick Start (opencode/kilocode)

**Workflow for each prompt:**

1. Read `PROMPT_STATUS.md` → select next TODO with highest priority.
2. Read `prompt3.md` → find detailed prompt description.
3. Implement changes.
4. **Run quick (fast) tests first** – this catches trivial errors early, saving time on the heavy suite:
   ```bash
   make test
   ```
   (This runs the **fast** profile: `mvn -B clean test -P fast` – smoke + index + query tags, <30s.)  
   If it fails, fix and repeat until it passes **before** moving to the full acceptance gate. **Max 3 fix attempts** — if still failing, stop and report.

5. Run **full acceptance gate** (includes heavy joins) with `make timing` – this automatically:
  - Builds the project
  - Runs the **large** profile (`@LargeTest`, 600x600 joins, 4GB heap)
  - Collects per-test times from surefire reports into `timing/timingN.md`
  - Compares it against the baseline `timing/timing.md` **and fails if real degradation** (see below)
  - For a full release run of ALL profiles use `make all-tests`.

6. **Profile check (Strict Condition):** Look at the current task description in `prompt2.md`. If the description does **NOT** contain the words "JOIN", "hash join", or "performance" — **SKIP this step entirely**. Otherwise, run:
   ```bash
   make check-profile
   ```
   *Check exit code only: 1 = failure, 0 = success.*

7. Create changelog entry: `make changelog "short description of changes"` – this script auto-appends test results, timing summary, and profile numbers (if profile was run). You only provide the one-sentence description.

8. Commit with `git commit -F changelog_entry.txt` (the script creates this file).

9. Update `PROMPT_STATUS.md` → mark as DONE.

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
- Build: `make build` (or `mvn package -DskipTests`.

## Git Rules

**NEVER commit these directories/files** (they are in `.gitignore`):
- `target/` — compiled `.class` files, build output
- `data/` — runtime database files (`.csv`, `.table`, `.parquet`)
- `logs/` — application logs (`.log`, `.log.gz`)
- `timing/` — benchmark timing files

If you accidentally stage them, run: `git rm -r --cached target/ data/ logs/ timing/`

## Tests

- **Fast profile** – `make test` (= `mvn -B clean test -P fast`, tags: smoke, query, index) runs only fast unit tests. **Use this as a first filter** before the heavy acceptance gate.
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
- **Full release gate** – `make all-tests` runs all 6 profiles sequentially. This is the **required** gate before commit.
- **TIA** – `make tia` (recommend profiles) / `make tia-run` (recommend + run).
- **Isolation Rule for Failures:** If `make timing` fails, DO NOT immediately re-run `make timing`. Find the exact failing test name in the log. Fix the code and run ONLY that specific test:
  ```bash
  $env:JAVA_HOME = "C:\Program Files\Axiom\AxiomJDK-21"; mvn test -P <profile> -Dtest=TestClassName#methodName
  ```
  Re-run `make timing` **ONLY AFTER** the isolated test passes. This saves minutes on heavy workloads.
- The gate expects `Failures: 0, Errors: 0`. The script `compare-timing.sh` will automatically ignore sub-11ms micro-queries and only treat degradation >20% on **heavy (>100ms)** queries as a failure. If heavy queries are stable, the script returns exit code 0.

### Test Cache Cleaning

| Command | What it clears | When to use |
|---------|---------------|-------------|
| `make clean-cache` | `~/.m2/build-cache/v1/com.dieseldb/dieseldb/` (this project's build cache only) | When build cache returns stale results after config changes |
| `make clean-test-cache` | `target/.cache`, `target/surefire-reports/`, `target/test-classes/` | Stale test results, flaky test debugging, before re-running specific tests |
| `make clean` | Everything above + `target/` + `data/*.csv` + `data/*.table` + logs + build cache | Full rebuild, before release |

**Note:** `mvn clean` also automatically cleans the build cache for this project via `maven-clean-plugin` fileset configuration in `pom.xml`.

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

**Caution:** do NOT run `mvn test -Dgroups="..."` without a profile — the default surefire config excludes all tests, so 0 tests execute. Always use the profile commands the script prints (`mvn -B clean test -P <profile>`) or run via `make tia-run`.

**Recommended workflow:** implement changes → `make tia` → run the recommended profiles (or `make tia-run`) → then continue with the normal acceptance gate.

---

## Timing regression check (fully automated)

- `make timing`:
  1. Runs the large profile (including two 600x600 ORDER BY joins).
  2. `scripts/collect-timing.py` builds `timing/timingN.md` from surefire reports.
  3. Executes `compare-timing.sh timing/timing.md timing/timingN.md` – this script:
  - Compares each query’s time.
  - **Ignores** any query whose baseline is <11 ms (machine noise).
  - Flags a regression only if a query with baseline ≥11 ms degrades by >20%.
  - Exits with code `1` if any such regression exists, otherwise `0`.
- You never need to interpret the output manually – just check the exit code. If the script fails, rerun `make timing` once (to rule out random noise) and if it still fails, investigate.
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

- Use `make changelog "your change description"` – for example:
  ```bash
  make changelog "Fix JOIN OR OOM by implementing hash join spilling"
  ```
  This script auto-appends test results, timing summary, and profile numbers (if profile was run) to `Changelog.md` and creates `changelog_entry.txt` for the commit message.

---

## Anti-Hang Guards

- **Max fix attempts: 3** — if quick tests or isolated tests fail 3 times in a row, stop and report to the user
- **Timeouts on long commands** — always use `bash` timeout parameter:
  - Quick tests (`make test`): 5 min max
  - `make timing` (large, 4GB): 30 min max
  - `make all-tests`: 40 min max
  - ProfileMain: 15 min max
- **Never run from agent context** — these commands block forever:
  - `start-server.bat` / `start-server.sh` (long-running TCP server)
  - `start-client.bat` / `start-client.sh` (interactive REPL)
- **Isolation rule cap** — max 3 attempts on the isolated test; if still failing, stop and report
- **Missing make targets** — `make changelog`, `make check-profile` may not exist in the Makefile. If `make` fails, fall back to raw Maven commands as shown in the Tests section above. `make quick-test` has been removed — use `make test`.

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
```