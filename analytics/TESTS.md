# TESTS.md — Prompts for Test Workflow Speedup

Sequence of self-contained prompts for the AGENTS.md prompt-driven workflow. Each prompt follows the standard pre-flight → implement → test gate → `PROMPT_STATUS.md` → `make release DESC="..."` cycle. Pick the next TODO from `PROMPT_STATUS.md` by priority, read the matching section here, implement, run the **Test plan**, verify the **Acceptance criteria**, then `make release`.

## Priority matrix

| # | ID | Title | Effort | Saving | Risk | Pre-req |
|---|----|-------|--------|--------|------|---------|
| 1 | TESTS-1 | Drop `mvn clean` from dev/CI gates | 1h | 30-90s/run | low | — |
| 2 | TESTS-2 | Cache Maven build-cache in GitHub Actions | 30min | 10-30s/CI run | low | TESTS-1 |
| 3 | TESTS-4 | Replace `SimpleDateFormat` + stack-walk in fixtures | 1h | ~1-2s/run | low | — |
| 4 | TESTS-3 | Enable JUnit 5 parallel execution | 1.5h | 20-40% on fast | medium | TESTS-4 |
| 5 | TESTS-5 | Quarantine known flakes | 1h | 30-90s/core gate | low | — |
| 6 | TESTS-6 | Split `make timing` into quick / full | 2h | 5-25min/prompt | medium | — |
| 7 | TESTS-7 | Consolidate `make all-tests` | 1h | 30-50s/release | low | — |
| 8 | TESTS-8 | `reuseForks=true` + `@Isolated` (net/conc) | 2h | 30-90s/profile | medium | — |
| 9 | TESTS-9 | Bulk-load fixtures in `AbstractDieselTest` | 4h | 30-60s/fast | medium | — |
| 10 | TESTS-10 | CI guard for `tia-mapping.txt` drift | 2h | preventive | low | — |

Optional follow-ups: TESTS-11..15 (smaller wins, see end of file).

Total estimate: ~12-15h. Combined wall-time savings: 30-50% on query-path prompts, 20-30% on storage-path prompts, 15-25% per CI run.

---

## TESTS-1: Drop `mvn clean` from dev/CI test gates

**Goal:** Cut 30-90s per `make test` and per CI job by not wiping `target/` on every test invocation.

**Background:** The `run-tests` macro in `Makefile:49-54` hardcodes `mvn -B clean test -P <profile>`. The same pattern appears in `.github/workflows/ci.yml` (`pr-gate`, `pr-gate-tia`, `manual`). Since `maven-build-cache-extension` 1.0.0 is enabled (`pom.xml:85-91`), every `clean` wipes the cache input and forces a full recompile of `diesel/` (~150 classes) + `src/test/java` (~238 classes).

**Tasks:**
1. In `Makefile`, change the `run-tests` macro to omit `clean`, and add a `run-tests-clean` variant for release:
```makefile
define run-tests
@stamp=$$(mktemp); \
echo "Running profile '$(1)'..."; \
$(MVN) -B test -P $(1) $(2) || exit 1; \
$(ASSERT_TESTS) "$$stamp"
endef

define run-tests-clean   # release-gate only
@stamp=$$(mktemp); \
echo "Running profile '$(1)' (clean)..."; \
$(MVN) -B clean test -P $(1) $(2) || exit 1; \
$(ASSERT_TESTS) "$$stamp"
endef
```
2. Update `all-tests` target — keep `clean` only on the first profile to start from a known state:
```makefile
all-tests:
	@first=1; \
	for p in fast core concurrency network perf large; do \
		echo ""; echo "--- $$p ---"; \
		if [ $$first -eq 1 ]; then \
			$(call run-tests-clean,$$p) || exit 1; \
			first=0; \
		else \
			$(call run-tests-incr,$$p) || exit 1; \
		fi; \
	done
```
3. In `.github/workflows/ci.yml`, remove `clean` from `pr-gate`, `pr-gate-tia`, `manual` jobs. Keep `clean` only in `release-gate` (`mvn -B clean verify -P all`).
4. Add a CI step that detects orphaned `.class` files (deleted `.java` left a stale `.class`):
```yaml
- name: Detect orphaned test classes
  run: |
    src_count=$(git ls-files 'src/test/java/**/*.java' | wc -l)
    cls_count=$(find target/test-classes -name '*.class' 2>/dev/null | wc -l)
    if [ "$$cls_count" -gt "$$src_count" ]; then
      echo "ERROR: orphaned test .class files (deleted .java left .class behind)"; exit 1
    fi
```
This step fails fast before `mvn test` if a cleanup is needed.

**Change type (per AGENTS.md table):** config-only / constant extract
- Step 4 (fast gate): `make compile` → `make test-incr`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `make compile` (verify syntax, 3-5s)
- `make test-incr` (fast profile, no clean, ~15s expected)
- `make test` (clean variant — sanity, ~30s expected)
- Run `make test-incr` twice in a row — second run must be faster than the first (cache warm)
- Verify `assert-tests-ran.sh` still triggers on a missing `-P` argument (run `mvn -B test` without `-P` once, expect the assertion to fail)
- `make test-core` (verify core still passes after the change)

**Acceptance criteria:**
- [ ] `make test-incr` writes ≥1 surefire report (verified by `assert-tests-ran.sh`)
- [ ] No "ERROR: no surefire reports were written" in CI logs
- [ ] `make test` wall-time decreases vs baseline (compare with `git stash` of the Makefile change)
- [ ] `make all-tests` wall-time decreases by ≥30s vs baseline
- [ ] `make clean-test-cache` still works as escape hatch

**Risk mitigation:** If a deleted `.java` file leaves an orphaned `.class` in `target/test-classes/`, surefire may try to run it. The new CI step (task 4) catches this. Run `make clean-test-cache` once per release branch sync as defense-in-depth. If a flake appears from stale state, the fix is one `make clean-test-cache` away — not a code bug.

**Commit message:**
```
make release DESC="Tests: drop 'mvn clean' from dev/CI gates — let build-cache hit, saves 30-90s per run"
```

**Estimated saving:** 30-90s per `make test`; ~90s on `make all-tests`; 15-30s per CI job.

---

## TESTS-2: Cache Maven build-cache in GitHub Actions

**Goal:** Persist the `maven-build-cache-extension` cache across CI runs so unchanged modules skip recompile.

**Background:** `actions/setup-java@v4 cache: maven` (in `ci.yml:39,84,113,141,165`) caches only `~/.m2/repository` (dependency JARs). The build-cache lives at `~/.m2/build-cache/v1/com.dieseldb/dieseldb/` (per `pom.xml:131-137`) and is currently lost between runs. Without this, TESTS-1 saves time locally but CI still pays full recompile on every push.

**Tasks:**
1. In `.github/workflows/ci.yml`, add an explicit cache step to all jobs that run Maven (place it BEFORE `Set up JDK 21` so `setup-java` doesn't fight over `~/.m2/repository`):
```yaml
- name: Cache Maven build-cache
  uses: actions/cache@v4
  with:
    path: |
      ~/.m2/build-cache/v1/com.dieseldb/dieseldb
    key: buildcache-${{ runner.os }}-${{ matrix.profile || inputs.profile || 'release' }}-${{ hashFiles('pom.xml', 'diesel/**/*.java', 'src/test/java/**/*.java', 'src/main/avro/**/*.avsc') }}
    restore-keys: |
      buildcache-${{ runner.os }}-${{ matrix.profile || inputs.profile || 'release' }}-
      buildcache-${{ runner.os }}-
```
2. Add `if: always()` to the cache step so it saves on test failure too (the next run still benefits from the partial compile).
3. Do NOT include `~/.m2/repository` in this cache step — `setup-java`'s `cache: maven` already handles it. Two caches touching the same dir causes conflicts.
4. Add a CI summary step that prints the cache hit/miss ratio:
```yaml
- name: Cache hit summary
  if: always()
  run: |
    if [ -f ~/.m2/build-cache/v1/com.dieseldb/dieseldb/.cache-hit ]; then
      echo "✓ Build-cache HIT"
    else
      echo "✗ Build-cache MISS — first run for this key"
    fi
```

**Change type:** CI infrastructure / config-only
- Step 4 (fast gate): `make compile`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- Push a small change (e.g., add a comment in `diesel/Database.java`) and trigger CI
- First run: cache miss expected — `mvn test-compile` does full work
- Push another small change — second run should show `restore-keys` partial hit on `buildcache-${{ runner.os }}-` prefix
- Verify no test profile regressed (same number of surefire reports)

**Acceptance criteria:**
- [ ] First push after change: cache miss (expected), tests pass
- [ ] Second push with same `diesel/` content: cache hit, `mvn test-compile` skips unchanged modules
- [ ] Cache size in CI summary ≤ 100 MB (otherwise the cache key is too coarse)
- [ ] No "Could not restore cache" warnings
- [ ] No conflict with `setup-java`'s `cache: maven`

**Risk mitigation:** Cache key uses `hashFiles` — if any source edits, it's a hard miss. `restore-keys` provides partial fallback (last successful cache for the same OS/profile). The `if: always()` save-on-failure prevents a successful-but-slow run from poisoning the cache with bad state. If a CI run becomes flaky after this change, drop the cache key prefix and start fresh.

**Commit message:**
```
make release DESC="CI: cache maven-build-cache directory across runs — saves 10-30s per CI job on hot commits"
```

**Estimated saving:** 10-30s per CI run on hot commits; up to 60s when only test files changed.

---

## TESTS-4: Replace `SimpleDateFormat` + stack-walk in test fixtures (PRE-REQ for TESTS-3)

**Goal:** Pre-req for TESTS-3 (thread-safety under parallel classes). Also eliminates ~5-50ms per `@BeforeEach` from `Thread.currentThread().getStackTrace()`.

**Background:** Two thread-safety / performance issues in current test fixtures:
1. `AbstractDieselTest.java:29` — `static SimpleDateFormat DATE_FORMATTER`. `SimpleDateFormat` is **not thread-safe**. Under `parallel=classes` (TESTS-3) this causes either data races (silent date corruption) or sync overhead (synchronized internal `Calendar`).
2. `FlusherTest.java:47-60` — `getTestMethodName()` walks `Thread.currentThread().getStackTrace()` and loops over every frame to find the calling test method. Costs ~5-50ms per `@BeforeEach`, and is brittle (relies on the call-stack pattern).

**Tasks:**
1. In `AbstractDieselTest.java`, replace the date formatter:
```java
// Replace:
import java.text.SimpleDateFormat;
import java.util.Date;
protected static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
// ...
String date = DATE_FORMATTER.format(new Date(System.currentTimeMillis() - (i * 24L * 60 * 60 * 1000)));

// With:
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
protected static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
// ...
String date = LocalDate.now().minusDays(i).format(DATE_FORMATTER);
```
2. In `FlusherTest.java`, delete `getTestMethodName()` and use JUnit 5 `TestInfo`:
```java
import org.junit.jupiter.api.TestInfo;

@BeforeEach
void setUp(TestInfo info) throws IOException {
    cleanupTestDir();
    String methodName = info.getTestMethod()
        .orElseThrow(() -> new IllegalStateException("no test method context"))
        .getName();
    walDir = TEST_DIR + "_wal_" + methodName;
    database = new Database(TEST_DIR, createWALConfig());
    database.executeQuery("CREATE TABLE " + TABLE_NAME + " (ID LONG PRIMARY KEY, NAME STRING)", null);
    database.getTable(TABLE_NAME).saveToFile(TABLE_NAME);
    database.getTable(TABLE_NAME).saveToSerializedFile(TABLE_NAME);
}
```
3. Sweep other test classes for the same patterns:
```bash
grep -rn "new SimpleDateFormat" src/test/java
grep -rn "getStackTrace" src/test/java
```
For each hit, apply the equivalent fix.

**Change type (per AGENTS.md table):** rename / refactor
- Step 4 (fast gate): `make compile` → `make test-incr`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `make compile` (verify imports resolve)
- `make test-incr` (fast profile — verify `AbstractDieselTest` subclasses still pass with new date format)
- `make test-core` (FlusherTest is in core profile — verify WAL dir naming unchanged)
- Run `make test-incr` 3 times — verify no flake from date formatting

**Acceptance criteria:**
- [ ] `make test-incr` passes (all `AbstractDieselTest` subclasses)
- [ ] No `NumberFormatException` or `ParseException` in surefire reports
- [ ] `grep -rn "new SimpleDateFormat" src/test/java` returns 0 hits
- [ ] `grep -rn "getStackTrace" src/test/java` returns 0 hits (or only legitimate non-test-method-name uses)
- [ ] `FlusherTest` WAL directories still follow the `data_flusher_test_wal_<methodName>` pattern

**Risk mitigation:** `DateTimeFormatter` is a strict functional superset of `SimpleDateFormat` for ISO date formatting — no behavior change for `yyyy-MM-dd` pattern. `TestInfo` is the JUnit 5 idiomatic way to get test method context — supported since JUnit 5.0. Both changes are pure refactors with no logic change.

**Commit message:**
```
make release DESC="Tests: replace SimpleDateFormat (thread-unsafe) with DateTimeFormatter; replace stack-walk with TestInfo — unblocks parallel classes"
```

**Estimated saving:** ~1-2s per `make test` run (mostly from removing `getStackTrace` per `@BeforeEach`); enables TESTS-3 (the bigger win).

---

## TESTS-3: Enable JUnit 5 parallel execution at the platform level

**Goal:** Reduce `fast` profile wall-time by 20-40% by letting JUnit 5 actually run classes in parallel.

**Background:** Surefire is configured with `<parallel>classes</parallel>` and `<threadCountClasses>4</threadCountClasses>` in `pom.xml:179-180` (fast profile). But JUnit 5 also needs a `junit-platform.properties` file to enable parallel execution at the platform level. Without it, surefire's `parallel=classes` setting may not engage fully — classes can still run sequentially in the single forked JVM.

**Prerequisites:** TESTS-4 must be merged first (removes the static `SimpleDateFormat` thread-safety violation).

**Tasks:**
1. Create `src/test/resources/junit-platform.properties`:
```properties
# Enable JUnit 5 parallel execution at the platform level.
# Surefire's <parallel>classes</parallel> alone is not enough — JUnit 5
# also needs these flags to actually schedule classes concurrently.
junit.jupiter.execution.parallel.enabled=true
# Methods within a class run sequentially (most tests hold a Database
# instance field — no need for method-level parallelism).
junit.jupiter.execution.parallel.mode.default=same
# Classes run in parallel — this is the win.
junit.jupiter.execution.parallel.mode.classes.default=concurrent
# Fixed parallelism — match surefire's threadCountClasses=4 for fast profile.
junit.jupiter.execution.parallel.config.strategy=fixed
junit.jupiter.execution.parallel.config.fixed.parallelism=4
```
2. Run a smoke test to find any class with shared static mutable state:
```bash
make test
# Look in target/surefire-reports/TEST-*.txt for:
#   - SQLException "table USERS not found" (cross-class DB sharing)
#   - ConcurrentModificationException
#   - "File already locked" / IOException on tempDir cleanup
```
3. If smoke test reveals conflicts, add JUnit 5 `@ResourceLock` annotations:
```java
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.ResourceAccessMode;

@ResourceLock(value = "diesel.Database.globalConfig", mode = ResourceAccessMode.READ)
@Tag("query")
public class MyTest { ... }
```
Most `AbstractDieselTest` subclasses should be conflict-free (each has its own `@TempDir` + `Database` instance).

4. For the `core` profile (`pom.xml:204-207`), raise `threadCountClasses` from 2 to 4 to match the JUnit 5 `fixed.parallelism=4`:
```xml
<threadCountClasses>4</threadCountClasses>
```

**Change type (per AGENTS.md table):** test infrastructure / config
- Step 4 (fast gate): `make test`
- Step 5 (acceptance): SKIP (no perf regression — only speedup expected)
- Step 6 (profile): SKIP

**Test plan:**
- `make test` (fast profile, 4 parallel classes — verify passes)
- `make test-core` (core profile, 4 parallel classes — verify passes)
- Repeat `make test` 3 times to detect flakes
- Check `target/surefire-reports/` report count matches baseline
- Run `make timing-quick` (after TESTS-6) — verify no regression on heavy queries

**Acceptance criteria:**
- [ ] `make test` wall-time decreases by ≥20% vs baseline (measure with `time make test`)
- [ ] No new flake across 3 consecutive runs
- [ ] No `IllegalStateException` or `ConcurrentModificationException` in surefire reports
- [ ] `target/surefire-reports/` contains the same number of `TEST-*.xml` files as before
- [ ] All `AbstractDieselTest` subclasses still pass

**Risk mitigation:** `mode.default=same` keeps methods within a class sequential — most tests hold a `Database` instance field. `mode.classes.default=concurrent` parallelizes across classes only. The `@TempDir` per-class injection (see comment at `AbstractDieselTest.java:31-40`) ensures each class writes to its own directory, preventing the file-stomp issue that previously hit `PerformanceRegressionTest`. If a specific class breaks under parallel, mark it `@Isolated` (JUnit 5 will run it in a dedicated fork) — opt-in is safer than opt-out.

**Commit message:**
```
make release DESC="Tests: enable JUnit 5 parallel classes via junit-platform.properties — 20-40% faster fast profile"
```

**Estimated saving:** 20-40% on `fast` profile wall-time; 15-25% on `core` profile.

---

## TESTS-5: Quarantine known flakes via `@EnabledIfSystemProperty`

**Goal:** Stop running 3 documented flaky tests in every `core`/`large` gate. Saves 30-90s per gate run + eliminates agent confusion from false-red `make timing`.

**Background:** AGENTS.md "Known Flakes" lists three pre-existing/environmental flakes:
- `FlusherTest#testMixedCommittedUncommitted` (storage/page flusher, O(N²)/slow — pre-existing insert path)
- `JsonlStorageAdvancedTest` 1M-row benchmark (A/B-proven pre-existing ~70s on clean HEAD)
- `GroupCommitThroughputTest#testAcceptanceThroughputAndP99` (WAL group commit, load-sensitive p99 > 20ms under full-suite load; passes in isolation p99≈5ms)

These are not bugs to fix — they're environmental. Currently they consume 30-90s per `core`/`large` gate and trigger false-negative `make timing` red gates that the agent then has to re-run.

**Tasks:**
1. Add `@Tag("flaky")` and `@EnabledIfSystemProperty` to each of the three flaky test methods/classes:

   **In `FlusherTest.java`:**
   ```java
   import org.junit.jupiter.api.Tag;
   import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
   
   @Test
   @Tag("flaky")
   @EnabledIfSystemProperty(named = "diesel.runFlaky", matches = "true")
   void testMixedCommittedUncommitted() { ... }
   ```

   **In `JsonlStorageAdvancedTest.java`:** apply the same annotations to the 1M-row benchmark method.

   **In `GroupCommitThroughputTest.java`:** apply to `testAcceptanceThroughputAndP99`.

2. In `pom.xml`, add `flaky` to the default `excludedGroups`:
```xml
<!-- In <properties>: -->
<surefire.excludedGroups>large,flaky</surefire.excludedGroups>

<!-- All profile <configuration> blocks already inherit ${surefire.excludedGroups},
     so the change in <properties> propagates everywhere. -->
```

3. Add a `test-flaky` Makefile target for nightly opt-in:
```makefile
.PHONY: test-flaky

## Run quarantined flaky tests (opt-in via -Ddiesel.runFlaky=true)
test-flaky:
	@stamp=$$(mktemp); \
	echo "Running flaky tests (diesel.runFlaky=true)..."; \
	$(MVN) -B clean test -P core \
		-Ddiesel.runFlaky=true \
		-Dgroups=flaky \
		-Dsurefire.excludedGroups= \
		|| exit 1; \
	$(ASSERT_TESTS) "$$stamp"
```

4. Add a `nightly-flaky` job to `.github/workflows/ci.yml`:
```yaml
nightly-flaky:
  name: flaky
  runs-on: ubuntu-24.04
  if: github.event_name == 'schedule'
  steps:
    - uses: actions/checkout@v4
    - name: Set up JDK 21
      uses: actions/setup-java@v4
      with: { distribution: temurin, java-version: 21, cache: maven }
    - name: Run flaky tests
      run: mvn -B clean test -P core -Ddiesel.runFlaky=true -Dgroups=flaky -Dsurefire.excludedGroups=
    - uses: actions/upload-artifact@v4
      if: always()
      with:
        name: nightly-flaky-reports
        path: target/surefire-reports/
        retention-days: 30
```

5. Update AGENTS.md "Known Flakes" section: add a note that these are now in quarantine — gates run without them; `make test-flaky` runs them on demand.

**Change type (per AGENTS.md table):** test infrastructure
- Step 4 (fast gate): `make compile` → `make test-incr`
- Step 5 (acceptance): SKIP (these tests no longer gate)
- Step 6 (profile): SKIP

**Test plan:**
- `make test-incr` (verify fast profile still passes — should be unaffected)
- `make test-core` (verify core no longer runs the 3 flaky tests)
- `make test-flaky` (verify the 3 tests run when explicitly opted in)
- Verify `make timing` no longer reds on the 3 flaky tests
- Verify the same total number of test methods in `make all-tests` minus exactly 3

**Acceptance criteria:**
- [ ] `make test-core` no longer runs `FlusherTest#testMixedCommittedUncommitted` (check surefire report list)
- [ ] `make test-core` no longer runs `JsonlStorageAdvancedTest#test1MRowBenchmark`
- [ ] `make test-core` no longer runs `GroupCommitThroughputTest#testAcceptanceThroughputAndP99`
- [ ] `make test-flaky` runs all 3 (with `diesel.runFlaky=true`)
- [ ] Coverage: same number of test methods in `make all-tests` minus 3
- [ ] `make timing` no longer produces false-red on these tests

**Risk mitigation:** Tests are still in the codebase — they just don't gate. `make test-flaky` runs them on demand. If a future fix removes the flake, remove the `@EnabledIfSystemProperty` annotation and the test rejoins the gate. The flaky-test count (3) is small enough that the daily `nightly-flaky` job will catch any regression introduced into them.

**Commit message:**
```
make release DESC="Tests: quarantine 3 known flakes (@EnabledIfSystemProperty diesel.runFlaky) — gates run 30-90s faster, less false-red timing"
```

**Estimated saving:** 30-90s on each `core`/`large` gate; eliminates false-red `make timing` from CPU drift (saves up to one 10-30min retry per query-path prompt).

---

## TESTS-6: Split `make timing` into `timing-quick` / `timing-full`

**Goal:** Reduce `make timing` from 10-30 min to 3-5 min for most query-path prompts.

**Background:** Per AGENTS.md "Timing Policy", `make timing` (`Makefile:169-178`) runs the entire `large` profile (1M-row, 4GB heap) + 2× 600×600 ORDER BY joins. AGENTS.md says "RUN `make timing` when: JOIN / hash join / GROUP BY / ORDER BY / index / planner / SELECT hot-path code" — but most of those changes only need the heavy timing-relevant queries, not the 1M-row inserts. Storage-engine prompts already skip `make timing` (per Timing Policy). This prompt gives query-path prompts a faster default.

**Tasks:**
1. Tag the timing-relevant test methods/classes with `@Tag("timing")`:
   - `PerformanceRegressionTest` — all methods (this IS the timing regression test)
   - `JoinTest` — only `*OrderBy*`, `*HashJoin*`, `*JoinOr*` methods (use `@Tag` per method)
   - `OrderByTest` — all methods
   - `GroupByTest` — all methods
   - Any other class with `@Tag("query-full")` that runs >100ms queries — review `timing/timing.md` for the existing baseline list

2. In `Makefile`, add `timing-quick` and rename `timing` to `timing-full` (keep `timing` as alias):
```makefile
.PHONY: timing timing-quick timing-full

## Fast timing gate — only @Tag("timing") tests (~3-5 min, default for query-path)
timing-quick:
	@stamp=$$(mktemp); \
	echo "Running timing-quick (only @Tag('timing') tests)..."; \
	$(MVN) -B clean test -P core -Dgroups=timing -DexcludedGroups=large,flaky || exit 1; \
	$(ASSERT_TESTS) "$$stamp"
	$(PY) scripts/collect-timing.py
	@if [ -f timing/timing.md ]; then \
		./compare-timing.sh timing/timing.md timing/timingN.md; \
	else \
		cp timing/timingN.md timing/timing.md; \
		echo "Baseline created. Re-run 'make timing-quick' to compare against it."; \
	fi

## Full timing gate — entire large profile (10-30 min, schema change / hash-join refactor)
timing-full:
	$(call run-tests,large)
	$(PY) scripts/collect-timing.py
	@if [ -f timing/timing.md ]; then \
		./compare-timing.sh timing/timing.md timing/timingN.md; \
	else \
		cp timing/timingN.md timing/timing.md; \
		echo "Baseline created. Re-run 'make timing-full' to compare against it."; \
	fi

## Backward-compat alias — defaults to quick
timing: timing-quick
```

3. Update AGENTS.md:
   - "Quick Test Reference" table: add `make timing-quick` row (3-5 min), keep `make timing` (=quick) and add `make timing-full` (10-30 min).
   - Step 5 of the AI Agent Quick Start workflow: change default from `make timing` to `make timing-quick`. Add note: "Use `make timing-full` only for schema change / hash-join refactor / `large` profile work."
   - "Timing Policy" section: clarify that `timing-quick` is the new default for query-path prompts; `timing-full` is reserved for `large` profile changes.

4. The `compare-timing.sh` script already ignores <11ms queries — make sure `timing-quick` produces a `timingN.md` with the same heavy queries as the existing `timing/timing.md` baseline. If the query set differs, regenerate the baseline after 2 clean runs (per AGENTS.md re-seed policy).

**Change type (per AGENTS.md table):** test infrastructure / build target
- Step 4 (fast gate): `make test`
- Step 5 (acceptance): `make timing-quick` (was `make timing`)
- Step 6 (profile): SKIP (unless JOIN / hash join — then `make check-profile`)

**Test plan:**
- `make timing-quick` (verify it runs only `@Tag("timing")` tests, ~3-5 min expected)
- Compare `timing/timingN.md` to `timing/timing.md` — no regression (`compare-timing.sh` exit 0)
- `make timing-full` (verify the old behavior still works, ~10-30 min)
- Run `make timing-quick` 2x — verify deterministic timing on the heavy queries

**Acceptance criteria:**
- [ ] `make timing-quick` wall-time ≤ 5 min
- [ ] `make timing-quick` produces a `timingN.md` containing the heavy queries from `timing.md`
- [ ] `compare-timing.sh timing/timing.md timing/timingN.md` exit code = 0 (no regression)
- [ ] `make timing-full` unchanged behavior (still runs the full `large` profile)
- [ ] `make timing` (alias) routes to `timing-quick`

**Risk mitigation:** If `timing-quick` produces a different set of queries than `timing.md` baseline (because some `@Tag("timing")` tests aren't in the baseline), the compare will flag false-regressions. Fix: regenerate baseline `cp timing/timingN.md timing/timing.md` after 2 consecutive clean runs (per AGENTS.md re-seed policy). The AGENTS.md "red timing gate" procedure still applies — re-run once, then commit-with-note if the change is unrelated.

**Commit message:**
```
make release DESC="Tests: split 'make timing' into timing-quick (3-5min, @Tag('timing') only) + timing-full (current behavior)"
```

**Estimated saving:** 5-25 min on each query-path prompt (default was 10-30 min, now 3-5 min for `timing-quick`).

---

## TESTS-7: Consolidate `make all-tests` into a single Maven verify

**Goal:** Cut ~30-50s of Maven startup overhead from the release gate.

**Background:** `Makefile:153-167` (`all-tests`) runs 6 profiles sequentially via a shell loop — 6 separate `mvn` invocations, each with its own JVM startup (~5-10s), plugin classpath resolution, surefire init. The existing `all` profile (`pom.xml:325-348`) already aggregates the right groups with `parallel=classes` — a single Maven invocation can do the same work in one shot.

**Tasks:**
1. Replace the `all-tests` loop with a single Maven invocation:
```makefile
## Run ALL profiles in one Maven invocation (release gate)
all-tests:
	@stamp=$$(mktemp); \
	echo "Running ALL profiles via 'all' profile (single Maven invocation)..."; \
	$(MVN) -B clean verify -P all || exit 1; \
	$(ASSERT_TESTS) "$$stamp"
	@echo ""
	@echo "All profiles passed."
```
2. The `all` profile already excludes `large,perf` (per `pom.xml:329`) — keep `large-test` and `test-perf` as separate optional targets (unchanged).
3. Add a new composite target for the FULL release gate (including `large` and `perf`):
```makefile
## Full release gate — all profiles + large + perf
all-tests-large: all-tests
	$(call run-tests-incr,large)
	$(call run-tests-incr,perf)
```
4. Update AGENTS.md "Full release gate" reference (`make all-tests`) to mention that for tag-pushes the recommended gate is `make all-tests-large`.

**Change type (per AGENTS.md table):** build target / config
- Step 4 (fast gate): `make compile`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `make all-tests` (verify it runs all 5 non-large profiles in one Maven invocation, single `BUILD SUCCESS`)
- Verify surefire report count matches baseline (sum of profile reports — count `TEST-*.xml` in `target/surefire-reports/`)
- `make all-tests-large` (verify `large` + `perf` still work)
- Time `make all-tests` vs old loop — expect ≥30s faster

**Acceptance criteria:**
- [ ] `make all-tests` wall-time decreases by ≥30s vs old loop
- [ ] Single Maven invocation in output (one `BUILD SUCCESS`, one `Tests run: X` summary)
- [ ] Same total number of `TEST-*.xml` reports in `target/surefire-reports/` as before
- [ ] `make all-tests-large` still passes (release gate intact for tag-pushes)
- [ ] Per-profile targets (`make test-core`, `make test-concurrency`, etc.) still available for debugging

**Risk mitigation:** If a test fails in one profile, the single-Maven invocation stops at the first failure — you lose the "all profiles run, then aggregate failures" behavior. For debugging, fall back to per-profile targets (`make test-core`, `make test-concurrency`, etc.) which remain available. The single-Maven behavior matches the existing `release-gate` job in `ci.yml` (`mvn -B clean verify -P all`).

**Commit message:**
```
make release DESC="Tests: consolidate 'make all-tests' into single 'mvn verify -P all' — saves 30-50s Maven startup overhead"
```

**Estimated saving:** 30-50s on the release gate (`make all-tests`).

---

## TESTS-8: Switch `network` and `concurrency` profiles to `reuseForks=true` + `@Isolated`

**Goal:** Save 30-90s on `make test-network` + `make test-concurrency` by avoiding fresh JVM per test class.

**Background:** `pom.xml:231-233` (concurrency profile) and `:257` (network profile) set `forkCount=1, reuseForks=false`. Each of ~17 network + ~10 concurrency test classes spawns a fresh JVM (~1-3s each). With `parallel=classes` disabled in these profiles (intentional for thread isolation), all classes run serially — so JVM startup is pure overhead. JUnit 5 `@Isolated` annotation is the proper tool for "this test needs exclusive JVM access" — it forces surefire to run the class in a dedicated fork, while letting others share.

**Tasks:**
1. In `pom.xml`, change both profiles:
```xml
<!-- concurrency profile -->
<configuration>
    <excludes combine.self="override"/>
    <groups>${surefire.groups}</groups>
    <excludedGroups>${surefire.excludedGroups}</excludedGroups>
    <forkCount>1</forkCount>
    <reuseForks>true</reuseForks>   <!-- was: false -->
    <argLine>-Xmx1g --add-modules=jdk.incubator.vector</argLine>
</configuration>

<!-- network profile: same change (reuseForks true) -->
```

2. Identify tests that genuinely need a fresh JVM:
```bash
# Tests that touch process-level resources
grep -rln "@Tag(\"network\")\|@Tag(\"concurrency\")" src/test/java | \
    xargs grep -l "ServerSocket\|new Thread\|Runtime.getRuntime\|System.exit\|ProcessBuilder"
```
Likely candidates: `GracefulShutdownTest`, `MtlsTest`, `PageManagerCrashTest`, `CliTlsTest`. Add `@Isolated` to each:
```java
import org.junit.jupiter.api.parallel.Isolated;

@Tag("network")
@Isolated
public class GracefulShutdownTest { ... }
```

3. For tests that share JVM-global resources (system properties, fixed ports), add `@ResourceLock`:
```java
@ResourceLock(value = "system.properties", mode = ResourceAccessMode.READ_WRITE)
public class CliTlsTest { ... }
```

4. Add a CI smoke job that runs `make test-network` + `make test-concurrency` 3 times in a row to catch flakes:
```yaml
profile-stability:
  name: stability-${{ matrix.profile }}
  runs-on: ubuntu-24.04
  if: github.event_name == 'schedule'
  strategy:
    matrix:
      profile: [network, concurrency]
  steps:
    - uses: actions/checkout@v4
    - name: Set up JDK 21
      uses: actions/setup-java@v4
      with: { distribution: temurin, java-version: 21, cache: maven }
    - name: Run ${{ matrix.profile }} 3x
      run: |
        for i in 1 2 3; do
          echo "--- run $i ---"
          mvn -B clean test -P ${{ matrix.profile }} || exit 1
        done
```

**Change type (per AGENTS.md table):** test infrastructure
- Step 4 (fast gate): `make test-network` (after change)
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `make test-concurrency` (verify passes with `reuseForks=true`)
- `make test-network` (verify passes with `reuseForks=true`)
- Run each 3 times — check for flakes
- Verify `@Isolated` classes still get their own fork (check surefire log: "Forking command line: [...]" lines — count should match `@Isolated` count + 1 shared fork)
- Time both — expect ≥30s reduction

**Acceptance criteria:**
- [ ] `make test-network` wall-time decreases by ≥30s
- [ ] `make test-concurrency` wall-time decreases by ≥30s
- [ ] No port-binding errors (`BindException: Address already in use`) across 3 consecutive runs
- [ ] `@Isolated` classes still run in dedicated forks (verify via surefire log)
- [ ] Same number of test methods run as before

**Risk mitigation:** `@Isolated` is JUnit 5's official mechanism for "this test needs exclusive JVM access" — supported since JUnit 5.0. If a flake appears in an un-isolated class, add `@Isolated` to it too — opt-in is safer than opt-out. The nightly `profile-stability` job catches any regression introduced by the change. If a single test class is fundamentally incompatible with `reuseForks=true`, isolate it and revert the profile setting back to `false` — but this should be the last resort.

**Commit message:**
```
make release DESC="Tests: switch network/concurrency profiles to reuseForks=true + @Isolated — saves 30-90s per profile"
```

**Estimated saving:** 30-90s on `make test-network` + `make test-concurrency`; up to 120s on `make all-tests-large`.

---

## TESTS-9: Bulk-load fixtures in `AbstractDieselTest`

**Goal:** Cut 30-60s from `make test` by replacing 4400 single-INSERT parses + 44 file-flushes per fast run with 4 bulk loads.

**Background:** `AbstractDieselTest.java:55-103` — each of ~11 subclasses runs `@BeforeAll setupCommonTables()`:
- 4× CREATE TABLE + 4× CREATE INDEX (16 statements)
- 100 × 4 = 400 INSERT via `String.format` + `database.executeQuery(sql, null)` — full SQL parse per call
- 4× `saveToFile` (flush to disk)

For 11 subclasses, that's 4400 SQL parses + 44 file writes per `make test`. SQL parsing dominates the cost.

**Tasks:**
1. Generate fixture CSVs once and commit them:
   - Create `scripts/generate-fixtures.py` that emits deterministic 100-row fixtures:
     - `src/test/resources/fixtures/users.csv` — columns: `ID,USER_CODE,NAME,AGE,BALANCE,DATE_FIELD,ACTIVE,PRECISION`
     - `src/test/resources/fixtures/profiles.csv`
     - `src/test/resources/fixtures/transactions.csv`
     - `src/test/resources/fixtures/user_details.csv`
   - The script must produce the SAME row data as `AbstractDieselTest.setupCommonTables()` currently generates (use `LocalDate.now().minusDays(i)` for dates, `100 + (i % 9000)` for balance, etc.) — so test assertions still pass.
   - Run the script once and commit the CSVs.

2. Add a `bulkLoadCSV` method to `Database` (or a test-only helper `diesel/TestDataLoader.java`):
```java
// In diesel/Database.java (package-private method, test-only):
/**
 * Bulk-load rows from a CSV file directly into the table's RowStorage,
 * bypassing the SQL parser. Used by tests for fast fixture setup.
 * Format: header row with column names, then one row per line.
 */
void bulkLoadCSV(String tableName, Path csvPath) throws IOException {
    Table table = getTable(tableName);
    try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
        String header = reader.readLine();
        String[] cols = header.split(",");
        String line;
        int rowNum = 0;
        while ((line = reader.readLine()) != null) {
            String[] vals = line.split(",");
            table.insertFromCsv(cols, vals, rowNum++);  // new package-private method on Table
        }
    }
    table.rebuildIndexes();   // ensure indexes reflect loaded rows
}
```

3. In `diesel/Table.java` (or wherever the Row storage is), add the package-private `insertFromCsv` method that bypasses the SQL parser:
```java
/** Test-only: insert a row from CSV column names + values, no SQL parsing. */
void insertFromCsv(String[] colNames, String[] values, int rowNum) {
    Row row = new Row();
    for (int i = 0; i < colNames.length; i++) {
        row.set(colNames[i], parseColumnValue(colNames[i], values[i]));
    }
    rows.add(row);
}

private Object parseColumnValue(String colName, String value) {
    ColumnSchema col = schema.getColumn(colName);
    return switch (col.type()) {
        case LONG -> Long.parseLong(value);
        case INTEGER -> Integer.parseInt(value);
        case BIGDECIMAL -> new BigDecimal(value);
        case DOUBLE -> Double.parseDouble(value);
        case BOOLEAN -> Boolean.parseBoolean(value);
        case DATE -> LocalDate.parse(value, DATE_FORMATTER);
        case STRING -> value;
    };
}
```

4. Replace `setupCommonTables()` in `AbstractDieselTest.java`:
```java
private static final Path FIXTURES = Path.of("src/test/resources/fixtures");

@BeforeAll
void setupCommonTables() {
    database.setDataDir(tempDir.toString());
    dropTable("USERS");
    dropTable("PROFILES");
    dropTable("TRANSACTIONS");
    dropTable("USER_DETAILS");

    database.executeQuery("CREATE TABLE USERS (...)", null);
    database.executeQuery("CREATE UNIQUE INDEX ON USERS (ID)", null);
    // ... other CREATE statements unchanged ...

    // Was: 400 INSERT via executeQuery (full SQL parse each)
    // Now: 4 bulk loads (single file read each, no SQL parse)
    try {
        database.bulkLoadCSV("USERS", FIXTURES.resolve("users.csv"));
        database.bulkLoadCSV("PROFILES", FIXTURES.resolve("profiles.csv"));
        database.bulkLoadCSV("TRANSACTIONS", FIXTURES.resolve("transactions.csv"));
        database.bulkLoadCSV("USER_DETAILS", FIXTURES.resolve("user_details.csv"));
    } catch (IOException e) {
        throw new UncheckedIOException("Failed to load test fixtures", e);
    }

    database.getTable("USERS").saveToFile("USERS");
    database.getTable("PROFILES").saveToFile("PROFILES");
    database.getTable("TRANSACTIONS").saveToFile("TRANSACTIONS");
    database.getTable("USER_DETAILS").saveToFile("USER_DETAILS");

    LOGGER.log(Level.INFO, "Setup completed: {0} records per table (bulk-loaded)", recordCount());
}
```

5. For subclasses that override `recordCount()` (e.g., `PerformanceRegressionTest` with 1000 rows), either:
   - Generate larger fixtures (`users_1000.csv`) and load conditionally based on `recordCount()`, OR
   - Use the 100-row fixture and accept the smaller record count if the test logic permits (verify each subclass).

6. Add an equivalence test that verifies bulk-load produces the same rows as INSERT:
```java
// In src/test/java/diesel/BulkLoadEquivalenceTest.java
@Tag("smoke")
public class BulkLoadEquivalenceTest {
    @Test
    void bulkLoadMatchesInsert() {
        // Setup two databases, one with INSERT, one with bulkLoadCSV
        // Assert getTable("USERS").getRows() are element-wise equal
    }
}
```
This catches any future regression in the bulk-load path.

**Change type (per AGENTS.md table):** new feature (test infrastructure)
- Step 4 (fast gate): `make test`
- Step 5 (acceptance): SKIP (no query-path change)
- Step 6 (profile): SKIP

**Test plan:**
- `make test` (fast profile — verify all 11 `AbstractDieselTest` subclasses still pass)
- `make test-core` (core profile — verify storage tests still pass with the new path)
- `make timing-quick` (after TESTS-6) — verify no regression on heavy queries (`recordCount` unchanged → baseline should match)
- Verify fixtures are deterministic (run `make test` twice, `diff` the `target/surefire-reports/TEST-*.txt` — should be identical)
- Run `scripts/generate-fixtures.py` twice — verify the same output (deterministic given same `LocalDate.now()` seed)

**Acceptance criteria:**
- [ ] `make test` wall-time decreases by ≥30s
- [ ] All 11 subclasses of `AbstractDieselTest` pass unchanged
- [ ] `PerformanceRegressionTest#*` baseline times unchanged (`compare-timing.sh` exit 0)
- [ ] Fixture CSVs committed to `src/test/resources/fixtures/`
- [ ] `scripts/generate-fixtures.py` reproduces the committed CSVs deterministically
- [ ] `BulkLoadEquivalenceTest#bulkLoadMatchesInsert` passes

**Risk mitigation:** If `bulkLoadCSV` skips SQL parsing, it must produce the same `Row` objects as `INSERT INTO ... VALUES (...)`. The `BulkLoadEquivalenceTest` (task 6) catches any divergence — tag it `@Tag("smoke")` so it runs in the `fast` profile, gating every PR. The `parseColumnValue` switch must handle every column type the schema supports — if a new type is added later, this method must be updated (add a unit test for each column type). For `PerformanceRegressionTest` with 1000 rows, generate a `users_1000.csv` fixture — do NOT silently reduce the record count, as that would change timing baseline.

**Commit message:**
```
make release DESC="Tests: bulk-load fixtures in AbstractDieselTest — replaces 4400 single-INSERTs per fast run with 4 CSV loads"
```

**Estimated saving:** 30-60s on `make test`; 60-120s on `make test-core`.

---

## TESTS-10: CI guard for `scripts/tia-mapping.txt` drift

**Goal:** Prevent silent coverage loss when new `diesel/*.java` files aren't added to TIA mapping.

**Background:** AGENTS.md "Test-Impact Analysis" warns: "Keep it up to date: when you add a new source file under `diesel/`, add a mapping line for it, otherwise TIA will report 'no tests to run' for changes in that file." This is a manual maintenance burden — the TIA workflow's whole point is to skip running tests, but if it skips too aggressively, PRs merge without verification. The current `scripts/tia-mapping.txt` is hand-maintained and drift is invisible until a regression escapes.

**Tasks:**
1. Add a CI lint script `scripts/check-tia-mapping.sh`:
```bash
#!/usr/bin/env bash
# Fail if any diesel/*.java source file (excluding generated Avro code
# and test files) is not covered by scripts/tia-mapping.txt — either
# by an exact path match or by a glob pattern in the first column.
set -euo pipefail

MAPPING="scripts/tia-mapping.txt"
[ -f "$MAPPING" ] || { echo "ERROR: $MAPPING not found"; exit 1; }

# Build a list of globs from the mapping file's first column (TAB-separated).
mapfile -t GLOBS < <(awk -F'\t' '/^[^#]/ && NF>=2 {print $1}' "$MAPPING")

MISSING=0
for src in $(find diesel -name '*.java' \
                -not -path '*/generated/*' \
                -not -name '*Test.java' \
                -not -name 'Main.java'); do
    rel="${src#./}"
    matched=0
    for g in "${GLOBS[@]}"; do
        # Use bash's [[ ]] pattern matching (extglob)
        if [[ "$rel" == $g ]]; then
            matched=1
            break
        fi
    done
    if [ $matched -eq 0 ]; then
        echo "MISSING from tia-mapping.txt: $rel"
        MISSING=1
    fi
done

if [ $MISSING -ne 0 ]; then
    echo ""
    echo "Either add an exact path or a glob pattern (e.g. 'diesel/storage/*.java')"
    echo "to scripts/tia-mapping.txt for each missing file."
    exit 1
fi
echo "All diesel/*.java files are covered by tia-mapping.txt."
exit 0
```

2. Make it executable: `chmod +x scripts/check-tia-mapping.sh`

3. Add a new CI job `lint-tia-mapping` in `.github/workflows/ci.yml`:
```yaml
lint-tia-mapping:
  name: tia-mapping-drift
  runs-on: ubuntu-24.04
  if: github.event_name == 'pull_request'
  steps:
    - uses: actions/checkout@v4
      with: { fetch-depth: 0 }
    - name: Verify tia-mapping.txt covers all diesel sources
      run: ./scripts/check-tia-mapping.sh
```

4. Update AGENTS.md "Test-Impact Analysis" section: add a note that the mapping is now CI-enforced — adding a new `diesel/*.java` file without updating `tia-mapping.txt` will fail the `tia-mapping-drift` job.

5. Verify the script passes on current HEAD — if not, fix the existing drift first:
```bash
./scripts/check-tia-mapping.sh
# If it fails, add the missing mapping lines and commit them in this same release.
```

**Change type (per AGENTS.md table):** test infrastructure / CI
- Step 4 (fast gate): `make compile`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- Run `./scripts/check-tia-mapping.sh` locally — should pass on current HEAD
- Add a dummy `diesel/Dummy.java` without updating mapping — script should fail and list `diesel/Dummy.java` as missing
- Add the mapping line `diesel/Dummy.java<TAB>smoke,query` — script should pass
- Delete the dummy file — script should pass again

**Acceptance criteria:**
- [ ] `./scripts/check-tia-mapping.sh` exits 0 on current HEAD (fix any drift first)
- [ ] Script fails when a new `diesel/*.java` is added without mapping
- [ ] Script correctly evaluates glob patterns (e.g., `diesel/storage/*.java` covers all storage sources)
- [ ] CI job `lint-tia-mapping` runs on PRs and fails the build on drift
- [ ] No false positives on existing files

**Risk mitigation:** Allow glob patterns (e.g., `diesel/storage/*.java`) to cover multiple files — the script uses bash's `[[ ]]` pattern matching which evaluates globs correctly. The script excludes `*/generated/*` (Avro plugin output) and `*Test.java` (test files don't need TIA mapping). If a legitimate source file doesn't have any tests (e.g., a new utility class), add it to the mapping with the special value `none` (per AGENTS.md): `diesel/NewUtil.java<TAB>none` — the script will accept this.

**Commit message:**
```
make release DESC="Tests: CI guard for tia-mapping.txt drift — fails PR if a diesel/*.java is not mapped"
```

**Estimated saving:** preventive — saves whole sessions where TIA under-runs tests on a new file (precedent: silent coverage loss until a regression escapes to production).

---

## Optional follow-up prompts

Smaller wins, implement as time permits. Each follows the same prompt structure as TESTS-1..10.

### TESTS-11: Reduce `FlusherTest.ROW_COUNT` from 50k to 5k

**Goal:** Cut 5-15s per `FlusherTest` method.

**Background:** `FlusherTest.java:26` sets `ROW_COUNT = 50000` with a comment "50k for efficiency, test runs faster" — but 50k SQL parses per `@BeforeEach` is the opposite of efficient. The comment is from when each insert was slower; with batch insert paths, 5k rows is enough.

**Tasks:**
1. In `FlusherTest.java:26`, change `ROW_COUNT = 50000` to `5000`.
2. Verify buffer pool flush threshold — check `BufferPool.java` for `PAGE_SIZE × capacity`. If 5k rows × ~80 bytes < 4 pages, raise to 10k. The exact threshold depends on `BufferPool.DEFAULT_CAPACITY`.
3. Make it configurable: `ROW_COUNT = Integer.getInteger("diesel.flusher.rowCount", 5000);` — so `large` profile can override with `-Ddiesel.flusher.rowCount=50000`.

**Test plan:**
- `make test-one T=FlusherTest#testFlusherCrashRecovery` (verify it still passes)
- `make test-core` (verify all FlusherTest methods still pass)

**Acceptance:**
- [ ] `FlusherTest#testFlusherCrashRecovery` still asserts 0 rows after WAL recovery (uncommitted inserts rolled back)
- [ ] Buffer pool flush actually triggered (check log for "Flusher running")
- [ ] `make test-core` passes

**Commit message:**
```
make release DESC="Tests: reduce FlusherTest.ROW_COUNT from 50k to 5k — saves 5-15s per test method"
```

**Estimated saving:** 5-15s per `FlusherTest` method (4-6 methods = 20-90s on `make test-core`).

---

### TESTS-12: Suppress INFO log in `large` profile runs

**Goal:** Cut ~1-2 min I/O on `make timing` (150MB output → ~5MB).

**Background:** AGENTS.md "Large-output discipline": `make timing` / `large-test` runs produce ~150 MB of tool output — `QueryParser` logs every INSERT at INFO level. When output is truncated, agents re-run blindly. The logs are not needed for timing comparison — `surefire-reports` already capture assertion failures.

**Tasks:**
1. Create `src/test/resources/logback-large.xml`:
```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder><pattern>%-5level %logger{20} - %msg%n</pattern></encoder>
    </appender>
    <logger name="diesel.QueryParser" level="WARN"/>
    <logger name="diesel.InsertExecutor" level="WARN"/>
    <logger name="diesel.Flusher" level="WARN"/>
    <root level="WARN"/>
</configuration>
```
2. In `pom.xml` `large` profile, add to `<systemPropertyVariables>`:
```xml
<systemPropertyVariables>
    <diesel.largeTests>${diesel.largeTests}</diesel.largeTests>
    <diesel.runAllTestsSample>true</diesel.runAllTestsSample>
    <logback.configurationFile>src/test/resources/logback-large.xml</logback.configurationFile>
</systemPropertyVariables>
```
3. Also apply to `perf` profile (similar INFO spam).
4. Verify surefire reports still capture assertion failures — they go to `target/surefire-reports/TEST-*.txt`, separate from stdout.

**Test plan:**
- `make large-test` — verify output is ≤10 MB (was 150 MB)
- `make timing` — verify `compare-timing.sh` still works (timing data is in surefire reports, not stdout)
- Run a failing test (e.g., temporarily break an assertion) — verify the failure message appears in the surefire report

**Acceptance:**
- [ ] `make large-test` stdout ≤ 10 MB
- [ ] Assertion failures still appear in `target/surefire-reports/TEST-*.txt`
- [ ] `compare-timing.sh` exit code unchanged vs baseline

**Commit message:**
```
make release DESC="Tests: suppress INFO log in large/perf profiles — cuts 150MB output to ~5MB, saves 1-2min I/O per timing run"
```

**Estimated saving:** 1-2 min I/O per `make timing` / `large-test` run; ~60s agent parsing time.

---

### TESTS-13: Surefire `redirectTestOutputToFile=true`

**Goal:** Stop tests' stdout from blocking Maven.

**Background:** Tests like `FlusherTest` call `System.out.println("Inserted " + i + " rows...")` for progress — this goes through Maven's stdout, blocking on slow terminals. Surefire's `redirectTestOutputToFile=true` writes test stdout to `target/surefire-reports/TEST-*.txt` instead, non-blocking.

**Tasks:**
1. In `pom.xml` surefire `<configuration>` (the default one, not profile-specific), add:
```xml
<redirectTestOutputToFile>true</redirectTestOutputToFile>
```
2. Sweep `src/test/java` for `System.setOut(...)` calls — verify none depend on capturing Maven's stdout (none expected):
```bash
grep -rn "System.setOut\|System.setErr" src/test/java
```
3. If any test relies on capturing stdout (e.g., to verify a log message), use `System.setOut(new PrintStream(baos))` in a `@BeforeEach`/`@AfterEach` pair — Surefire's redirect doesn't break this, but verify.

**Test plan:**
- `make test` (verify stdout is empty in Maven output, but `target/surefire-reports/TEST-FlusherTest.txt` contains the progress messages)
- `make test-core` (verify all core tests pass)

**Acceptance:**
- [ ] Maven stdout during `make test` shows no `System.out.println` output
- [ ] `target/surefire-reports/TEST-*.txt` files contain the redirected stdout
- [ ] No test breaks due to lost stdout capture

**Commit message:**
```
make release DESC="Tests: surefire redirectTestOutputToFile=true — non-blocking test stdout, smaller Maven log"
```

**Estimated saving:** ~5-15s per `make test` (faster Maven output flushing).

---

### TESTS-14: `make timing` retry marker

**Goal:** Enforce the "retry once" rule from AGENTS.md Timing Policy.

**Background:** AGENTS.md "On a red timing gate" says "Re-run `make timing` **once** only (noise filter)." But this rule is in the doc, not the Makefile — agents can forget and burn 10-30 min on a second retry. A file marker enforces it.

**Tasks:**
1. In `Makefile` `timing` target, add a marker check:
```makefile
timing: timing-quick   # or timing-full, depending on TESTS-6 outcome

timing-quick:
	@if [ -f target/.timing-retry-done ]; then \
		echo "ERROR: timing already retried. Fix the test, then 'make clean-test-cache' to reset."; \
		exit 1; \
	fi
	@stamp=$$(mktemp); \
	echo "Running timing-quick..."; \
	$(MVN) -B clean test -P core -Dgroups=timing -DexcludedGroups=large,flaky || { \
	    echo "Timing failed. One retry permitted — re-run 'make timing-quick'."; \
	    touch target/.timing-retry-done; \
	    exit 1; \
	}; \
	$(ASSERT_TESTS) "$$stamp"
	$(PY) scripts/collect-timing.py
	@if [ -f timing/timing.md ]; then \
		./compare-timing.sh timing/timing.md timing/timingN.md || { \
		    echo "Timing regression. One retry permitted — re-run 'make timing-quick'."; \
		    touch target/.timing-retry-done; \
		    exit 1; \
		}; \
	else \
		cp timing/timingN.md timing/timing.md; \
		echo "Baseline created."; \
	fi
	@rm -f target/.timing-retry-done   # success — clear marker
```
2. `target/.timing-retry-done` is already covered by `.gitignore` (the `target/` rule).

**Test plan:**
- Run `make timing-quick` — should pass, marker not created
- Simulate a failure (e.g., break a baseline query) — first run fails, marker created
- Run `make timing-quick` again — should fail with "ERROR: timing already retried"
- Run `make clean-test-cache` — marker removed
- Run `make timing-quick` — should pass (after fixing the baseline)

**Acceptance:**
- [ ] First `make timing-quick` failure creates `target/.timing-retry-done`
- [ ] Second `make timing-quick` invocation fails immediately with the marker error
- [ ] `make clean-test-cache` removes the marker
- [ ] Successful `make timing-quick` removes the marker

**Commit message:**
```
make release DESC="Tests: enforce 'timing retry once' via marker file — prevents accidental 3rd 10-30min run"
```

**Estimated saving:** 10-30 min per agent mistake (prevents accidental 3rd retry).

---

### TESTS-15: Gate `--add-modules=jdk.incubator.vector` to codec-only profiles

**Goal:** Save ~200-500ms JVM startup on profiles that don't use vector API.

**Background:** `--add-modules=jdk.incubator.vector` appears in every profile's `argLine` (e.g., `pom.xml:116,181,208,233,258,287,313,343,370,396,431,459,487,515`). The vector API is only used in storage-codec code (Snappy, LZ4 codecs). Loading the module adds ~200-500ms to JVM startup — irrelevant for one run, but multiplied by `forkCount × profiles` it adds up.

**Tasks:**
1. Verify the assumption — check where vector API is actually used:
```bash
rg "jdk.incubator.vector" diesel/
rg "import jdk.incubator.vector" diesel/
```
2. If only storage-codec code uses it, move the flag from global surefire `<argLine>` to a new `vector` profile or to the `storage-*` profiles only:
```xml
<!-- Global default argLine (no vector): -->
<argLine>-Xmx${test.heap}</argLine>

<!-- storage-* profiles add the flag: -->
<argLine>-Xmx1g --add-modules=jdk.incubator.vector</argLine>

<!-- fast, core, concurrency, network, all, ci: no vector flag -->
```
3. Add a CI guard: run `make test` (fast profile) and verify no `NoClassDefFoundError` for vector classes. If any test in `fast` profile needs vector (unlikely), revert and document.

**Test plan:**
- `make test` (fast profile — verify passes without vector flag)
- `make test-core` (verify passes — storage tests in core profile still have the flag)
- `make test-storage-csv` / `storage-avro` (verify they pass with the flag)

**Acceptance:**
- [ ] `rg "import jdk.incubator.vector" diesel/` shows only codec/storage code
- [ ] `make test` passes without the vector flag in `fast` profile
- [ ] Storage tests still pass with the flag in `storage-*` profiles
- [ ] No `NoClassDefFoundError` in surefire reports

**Commit message:**
```
make release DESC="Tests: gate --add-modules=jdk.incubator.vector to storage profiles only — saves ~300ms JVM startup on fast/core"
```

**Estimated saving:** ~300ms × forkCount × profile invocations per `make all-tests` ≈ 5-15s on the release gate.

---

## How to consume this file

1. **Bootstrap:** Add all 15 entries to `PROMPT_STATUS.md` as TODOs with their priority:
   ```
   - [ ] TESTS-1: Drop `mvn clean` from dev/CI gates — 1h, 30-90s/run — P1
   - [ ] TESTS-2: Cache Maven build-cache in GitHub Actions — 30min, 10-30s/CI run — P1
   - [ ] TESTS-3: Enable JUnit 5 parallel execution — 1.5h, 20-40% on fast — P2 (needs TESTS-4)
   - [ ] TESTS-4: Replace SimpleDateFormat + stack-walk — 1h, ~1-2s/run — P1 (pre-req for TESTS-3)
   - [ ] TESTS-5: Quarantine known flakes — 1h, 30-90s/core gate — P1
   - [ ] TESTS-6: Split `make timing` into quick/full — 2h, 5-25min/prompt — P2
   - [ ] TESTS-7: Consolidate `make all-tests` — 1h, 30-50s/release — P2
   - [ ] TESTS-8: reuseForks=true + @Isolated — 2h, 30-90s/profile — P2
   - [ ] TESTS-9: Bulk-load fixtures in AbstractDieselTest — 4h, 30-60s/fast — P3
   - [ ] TESTS-10: CI guard for tia-mapping.txt drift — 2h, preventive — P2
   - [ ] TESTS-11..15: optional follow-ups — variable effort — P3
   ```
2. **Pick next TODO:** Highest priority that has its pre-reqs DONE. Read the matching section above.
3. **Implement** per the Tasks list.
4. **Run the Test plan** for that prompt — verify every checkbox in Acceptance criteria.
5. **Update `PROMPT_STATUS.md`** to mark the entry DONE (before `make release`, per AGENTS.md step 7).
6. **`make release DESC="..."`** with the suggested commit message — auto-increments version, appends Changelog, commits, pushes.

### Pre-req chain

- TESTS-4 → TESTS-3 (thread-safety pre-req)
- TESTS-1 → TESTS-2 (build-cache hit only matters without `clean`)
- TESTS-6 → optional benefit from TESTS-5 (flaky tests would false-red `timing-quick`)

All other prompts are independent — pick by priority.
