# TESTS3.md — Prompts for Maven Profile Audit Fixes

Sequence of self-contained prompts following the AGENTS.md prompt-driven workflow. Each prompt follows the standard pre-flight → implement → test gate → `PROMPT_STATUS.md` → `make release DESC="..."` cycle. Pick the next TODO from `PROMPT_STATUS.md` by priority, read the matching section here, implement, run the **Test plan**, verify the **Acceptance criteria**, then `make release`.

This file is the third TESTS prompt document (TESTS.md = TESTS-1..TESTS-13; TESTS-2..TESTS-15 were folded into the same file as optional follow-ups; TESTS-14 and TESTS-15 were applied directly without prompt entries). TESTS3.md picks up at TESTS-16.

---

## Background — Maven profile audit (2026-11-07)

A review of `pom.xml` against `AGENTS.md` and `StorageTypeCondition.java` identified eight issues, in priority order:

1. **Profile `all` does not set `diesel.storage.type`.** `StorageTypeCondition` (lines 32–39) falls back to `ConfigLoader.getString("storage.type", "in_memory")`, which reads `config.properties:40` = `csv`. Result: under `-P all`, ~60 `@StorageType("jsonl"|"tsv"|"avro")` classes are silently `disabled` — the "release gate" only exercises CSV-format storage tests.
2. **No "all × 4 storage types" command exists.** The `storage-*` profiles run only `@Tag("storage")` tests; `all` runs everything else but only one storage.type. There is no single command that runs the full release gate against all four storage formats.
3. **`AGENTS.md:308` is misleading.** It calls `make all-tests` the "release gate" without warning that jsonl/tsv/avro storage classes are not exercised.
4. **`AGENTS3.md` is outdated.** It references the removed `make quick-test` target, the non-functional `mvn test -DskipLargeTests` flag (surefire `<groups>` does not honor that property), an obsolete `>100ms` timing floor (current is `>2.0s`), and `prompt2.md` (current AGENTS.md works with `prompt3.md` / `prompt4.md` / `o2-prompts.md`).
5. **Profiles `test` and `ci` are undocumented.** `test` is a backward-compatible alias of `fast` (no Makefile target invokes it). `ci` uses `forkCount=2, reuseForks=true, parallel=classes` — the exact topology that `all` rejected (TESTS-7) for causing JMX/sysprop/static-state bleed. No Makefile target invokes `ci`; it is unclear whether any external CI does either.
6. **`flaky-jsonl` excludes only 2 classes by name** (`FlusherTest`, `GroupCommitThroughputTest`). New `@Tag("flaky")` tests are auto-included in `flaky-jsonl` with no programmatic check that they are jsonl-compatible — `FlusherTest`'s 50k-insert loop took 14 min for the first 5k rows under jsonl, hence the explicit exclusion. There is no guard against regressing this list.
7. **`pom.xml.versionsBackup` is committed.** This is a leftover artifact of `mvn versions:*` and should be in `.gitignore`, not tracked.
8. **`--add-modules=jdk.incubator.vector` will break on Java 22+.** The vector API graduated to a preview module in JDK 22 (`--enable-preview --add-modules=jdk.incubator.vector`), and may be removed from the incubator in a later JDK. The current `pom.xml` declares `maven.compiler.source=21` so the flag works, but a future JDK upgrade will silently turn every `large`/`perf`/`timing-quick`/`flaky`/`flaky-jsonl`/`storage-*` invocation into a JVM error.

## Priority matrix

| # | ID | Title | Effort | Risk | Pre-req |
|---|----|-------|--------|------|---------|
| 1 | TESTS-16 | `make all-tests-all-storages` + AGENTS.md storage-coverage clarification | 1.5h | low | — |
| 2 | TESTS-19 | `assert-tests-ran.sh` storage-aware extension | 1h | low | TESTS-16 |
| 3 | TESTS-18 | `flaky-jsonl` maintenance guard | 1h | low | — |
| 4 | TESTS-17 | Document or remove `test` and `ci` Maven profiles | 30min | low | — |
| 5 | TESTS-20 | Delete `AGENTS3.md` (deprecated) | 15min | low | — |
| 6 | TESTS-21 | Remove `pom.xml.versionsBackup` + add `.gitignore` entry | 15min | low | — |
| 7 | TESTS-22 | `--add-modules=jdk.incubator.vector` future-proofing | 30min | low | — |

Total estimate: ~5h. Risk is uniformly low because every prompt is config/test-infra only — no production code path is touched, so the timing gate is skipped by AGENTS.md "Timing Policy" (each prompt's "Change type" line confirms this).

---

## TESTS-16: `make all-tests-all-storages` + AGENTS.md storage-coverage clarification

**Goal:** Add a Makefile target that runs the `all` profile against each of the four storage types in turn, and fix the misleading "release gate" description in AGENTS.md so it no longer implies `-P all` exercises all storage formats.

**Background:** Profile `all` (pom.xml lines 385–452) does not set `diesel.storage.type` in its `<systemPropertyVariables>`. `StorageTypeCondition.evaluateExecutionCondition` (src/test/java/diesel/StorageTypeCondition.java:32–39) resolves the property as `null` → falls back to `ConfigLoader.getString("storage.type", "in_memory")` → reads `config.properties:40` = `csv`. The result: ~60 `@StorageType({"jsonl"|"tsv"|"avro"})` test classes — `JsonlStorageTest`, `AvroStorageTest`, `AvroRecoveryManagerTest`, `JsonlBlockManagerTest`, `TsvStorageAdvancedTest`, etc. — are silently `disabled` under `-P all`. The Makefile `all-tests` target (Makefile:240–246) calls `mvn -B clean verify -P all` exactly once, so the same blind spot propagates to `make all-tests`. AGENTS.md:308 describes this as the "release gate" with no warning.

**Tasks:**
1. In `Makefile`, add a new target `all-tests-all-storages` immediately after `all-tests` (after line 246). It runs `all-tests` once per storage type, breaking on the first red. The first iteration uses the default `csv` (no `-D` override needed, but pass it explicitly for symmetry with the other three):
```makefile
## Run the release gate against ALL four storage types (csv, tsv, jsonl, avro).
## Each iteration runs `mvn -B clean verify -P all -Ddiesel.storage.type=<type>`.
## Use for tag-pushes that touch storage-format code (Avro, Jsonl, Tsv, Csv).
## Wall-time: ~4x `make all-tests` (~12-30 min total depending on machine).
all-tests-all-storages:
	@rc=0; \
	for st in csv tsv jsonl avro; do \
		echo ""; \
		echo "=== all-tests storage.type=$$st ==="; \
		$(MVN) -B clean verify -P all -Ddiesel.storage.type=$$st || rc=1; \
		stamp=$$(mktemp); \
		$(ASSERT_TESTS) "$$stamp" || rc=1; \
		[ $$rc -ne 0 ] && break; \
	done; \
	exit $$rc
```
2. Add the target name to the `.PHONY` line in Makefile (around line 77).
3. Add a help entry to the `help` target (around line 440): `@echo "  make all-tests-all-storages - Release gate across csv+tsv+jsonl+avro (4x ~3-8 min)"`.
4. In `AGENTS.md`, replace the bullet at line 308 (the "Full release gate" line) with two bullets:
```markdown
- **Release gate (default storage)** – `make all-tests` = ONE `mvn clean verify -P all` invocation. The `all` profile aggregates the fast+core+concurrency+network tag groups; perf and large are excluded by design. **Storage coverage is limited to the `storage.type` set in `config.properties` (default: `csv`)** — `@StorageType({"jsonl"|"tsv"|"avro"})` classes are silently `disabled` because profile `all` does not set `diesel.storage.type` (see `StorageTypeCondition.java:32-39`). Use `make all-tests-all-storages` for full storage coverage.
- **Release gate (all storage types, since TESTS-16)** – `make all-tests-all-storages` runs `mvn clean verify -P all -Ddiesel.storage.type=<type>` four times, once each for `csv`, `tsv`, `jsonl`, `avro`. **Recommended for tag-pushes** that touch storage-format code; pairs with `make all-tests-large` for the complete gate. Wall-time ~4x `make all-tests`.
- **`make all-tests-large`** = `all-tests` + `perf` + `large`. Note: still limited to one storage.type (the `config.properties` default); for full coverage under large/perf too, run `make all-tests-all-storages` then `make large-test -Ddiesel.storage.type=<type>` per format manually.
```
5. In `AGENTS.md`, add a new `### Storage-type coverage` subsection immediately after the "Test class annotations" bullet block (after line 307) explaining how `-P all` interacts with `diesel.storage.type` (see TESTS-16 acceptance criteria for the exact text).
6. Update the Quick Test Reference table (AGENTS.md:235–244) — add a row:
```
| Full gate, all 4 storage types | `make all-tests-all-storages` | 4 × ~3-8 min ≈ 12-30 min |
```
7. Update `PROMPT_STATUS.md` with the new TESTS-16 entry.

**Change type (per AGENTS.md table):** Test infrastructure, build target, quarantine (TESTS-*)
- Step 4 (fast gate): `make compile` → `make test-incr`
- Step 5 (acceptance): SKIP (per AGENTS.md "Timing Policy": "Test infrastructure, build target, quarantine (TESTS-*)" — `make timing-quick` is explicitly skipped for this change type)
- Step 6 (profile): SKIP (no JOIN/hash-join/performance in the task description)

**Test plan:**
- `make compile` (verify Makefile syntax — Makefile edits don't recompile Java, but the `.PHONY` change should not break `make help`)
- `make help` — verify the new `all-tests-all-storages` line appears
- `make test-incr` (fast profile, ~6-13 min, sanity)
- Run `make all-tests` once — verify it still works (no regression to the existing target)
- Run `make all-tests-all-storages` — verify all 4 iterations execute and surefire reports are written for each. Check that the jsonl/avro/tsv storage classes now run by grepping `target/surefire-reports/` for `JsonlStorageTest`, `AvroStorageTest`, `TsvStorageTest` XML files after the corresponding iteration.
- Verify `assert-tests-ran.sh` triggers on a deliberately broken invocation: `mvn -B clean verify -P all -Ddiesel.storage.type=nonexistent` — this should produce 0 storage tests (since `@StorageType("csv")` etc. don't match `nonexistent`), and the build should fail loudly (this case is what TESTS-19 hardens against).

**Acceptance criteria:**
- [ ] `make all-tests-all-storages` exists and runs all 4 storage types in sequence
- [ ] `make all-tests-all-storages` fails fast on the first red iteration (does not continue to jsonl after csv fails)
- [ ] After the jsonl iteration, `target/surefire-reports/` contains `TEST-diesel.JsonlStorageTest.xml` (proves the class actually ran)
- [ ] After the avro iteration, `target/surefire-reports/` contains `TEST-diesel.AvroStorageTest.xml` (proves the class actually ran)
- [ ] AGENTS.md line 308 area rewritten per task 4
- [ ] New `### Storage-type coverage` subsection added to AGENTS.md with the following content (or equivalent):
```markdown
### Storage-type coverage

The `all` Maven profile (and therefore `make all-tests`) does **not** set `diesel.storage.type`. `StorageTypeCondition` falls back to `config.properties:40` (`storage.type = csv`), so `-P all` exercises only CSV-format storage tests — `@StorageType({"jsonl"|"tsv"|"avro"})` classes (60+ files: all `Avro*Test`, `Jsonl*Test`, `Tsv*Test`) are silently `disabled` by the JUnit condition.

Coverage matrix:

| Command | storage classes exercised | storage classes skipped |
|---------|---------------------------|-------------------------|
| `make all-tests` | `@StorageType("csv")` + multi-format (`{"csv","tsv"}`, `{"jsonl","csv"}`, `{"avro","csv","tsv","jsonl"}`) | All `@StorageType({"jsonl"|"tsv"|"avro"})`-only classes |
| `make all-tests-all-storages` | All four format buckets (csv, tsv, jsonl, avro), one Maven invocation per type | None (full `@StorageType` coverage) |
| `make test-storage-<format>` | Only `@StorageType("<format>")` + multi-format classes | All other format-specific classes |
| `make test-core -Ddiesel.storage.type=<format>` (manual) | All `@Tag("storage")` + `@Tag("query-full")` classes for the given format | Storage classes for other formats |

For tag-pushes that touch any storage-format code (Avro schema, Jsonl parser, Tsv codec, Csv reader), use `make all-tests-all-storages` instead of `make all-tests`. For tag-pushes that touch only query-path code (no storage), `make all-tests` is sufficient — `storage.type=csv` is the default and the CSV path is the most-tested.
```
- [ ] Quick Test Reference table (AGENTS.md:235) updated with the new `make all-tests-all-storages` row
- [ ] `PROMPT_STATUS.md` shows TESTS-16 as DONE

**Risk mitigation:** The new target calls `clean verify` four times — wall-time is ~4x the existing `make all-tests`. This is by design: the alternative (one Maven invocation with `@StorageType` parameterization) would require either running each `@StorageType` class four times via JUnit `@ParameterizedTest` (heavy refactor of 60+ test classes) or running `mvn test` four times against the same `target/` (risk of stale state bleeding between storage types — the engine initializes `Database` once per JVM, and `config.properties` is read at JVM start). The four-clean-invocations approach guarantees isolation. If wall-time is a concern, run the four iterations in parallel on a 4-core+ machine: `for st in csv tsv jsonl avro; do (make all-tests STORAGE_TYPE=$$st &); done; wait` — but this risks 4x heap usage and is left as a follow-up, not the default.

**Commit message:**
```
make release DESC="Tests: add make all-tests-all-storages + clarify AGENTS.md storage-coverage gap in -P all (TESTS-16)"
```

**Estimated effort:** 1.5h. ~30min for the Makefile + AGENTS.md edits, ~1h for running the new target end-to-end to verify the jsonl/avro/tsv classes actually execute.

---

## TESTS-17: Document or remove `test` and `ci` Maven profiles

**Goal:** Either delete the dead `test` and `ci` profiles from `pom.xml` (if no external CI invokes them) or document them in AGENTS.md and add Makefile targets. Decide based on a usage audit.

**Background:** `pom.xml` declares two profiles that are not mentioned in AGENTS.md and have no Makefile target:

- `test` (pom.xml:581–602) is a byte-for-byte duplicate of `fast` (same `groups`, `excludedGroups`, `forkCount`, `reuseForks`, `parallel`, `threadCountClasses`, `argLine`). It is a "backward-compatible alias" per the inline comment, but `make test` invokes `-P fast`, not `-P test`. Maven's built-in `test` lifecycle phase (`mvn test`) is unrelated — the profile name only activates with `-P test`. Net effect: dead code, source of confusion.
- `ci` (pom.xml:607–635) uses `forkCount=2, reuseForks=true, parallel=classes, threadCountClasses=2`. This is the exact topology that `all` rejected in TESTS-7 (see pom.xml:402–425): "fresh JVM PER CLASS and NO second hot JVM" was the load-bearing fix for "shared-heap/global-state reds (JMX, sysprops, GC-polluted µs proofs)". Profile `ci` reintroduces the topology that was proven to produce starvation reds. AGENTS.md does not document `ci`, and no Makefile target invokes it. Whether external CI (`.github/workflows/`) uses it is unclear.

**Tasks:**
1. **Audit usage.** Search the repo for `-P ci` and `-P test` references:
   ```bash
   rg -n -- '-P (ci|test)\b' .github/ scripts/ Makefile *.md 2>/dev/null
   ```
   If only `pom.xml` itself mentions them, they are dead.
2. **Decision tree:**
   - **If no external CI uses `ci`** (the expected outcome, given the topology mismatch): delete profile `ci` (pom.xml:607–635). Add a one-line note in the `all` profile comment block (pom.xml:393–435) referencing the deletion: "Profile `ci` was removed in TESTS-17 — its `forkCount=2, reuseForks=true` topology was the source of the cross-class state-bleed reds that this profile's `forkCount=1, reuseForks=false` configuration exists to prevent. Use `-P all` (or `-P ci-replacement = the `ci` Makefile target = `make all-tests`) instead."
   - **If external CI does use `ci`**: keep the profile, but align its topology with `all` (change to `forkCount=1, reuseForks=false`, drop `<parallel>` and `<threadCountClasses>`). Add documentation to AGENTS.md after the "Other profiles" bullet (line 279 area) noting that `ci` is functionally identical to `all` with a smaller tag set, intended for external CI runners.
   - **For `test`**: delete it regardless of usage. The Maven lifecycle phase `mvn test` does not depend on the profile (it depends on the default surefire `<configuration>`, which excludes all sources — so `mvn test` without `-P` already produces 0 tests, which `assert-tests-ran.sh` catches). If anyone was relying on `-P test`, the existing `assert-tests-ran` guard plus the rename to `fast` is the correct migration path. Add a one-line note to AGENTS.md after the "Fast profile" bullet (line 278): "Profile `test` was removed in TESTS-17 — it was a duplicate of `fast`; the Maven lifecycle phase `mvn test` is unrelated and still requires `-P <profile>` to run any test (the default surefire config excludes `**/*.java`)."
3. Update `PROMPT_STATUS.md` with the TESTS-17 entry.

**Change type:** Test infrastructure, build target, quarantine (TESTS-*)
- Step 4 (fast gate): `make compile`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `make compile` (verify pom.xml parses — Maven validates profile declarations at load time, a malformed `<profile>` block fails the build)
- `make test-incr` (verify `-P fast` still works — should be unaffected)
- `make all-tests` (verify `-P all` still works — should be unaffected)
- If `ci` is deleted: search the repo for any remaining `ci` references: `rg -n '\bci\b' .github/ scripts/ Makefile` — only documentation references should remain (e.g., "CI runs all four nightly")
- If `test` is deleted: same — `rg -n '\-P test\b' .` should return no hits

**Acceptance criteria:**
- [ ] Audit output captured in commit message (which files reference `-P ci` and `-P test`)
- [ ] Decision tree executed and either deletion or alignment committed
- [ ] AGENTS.md updated with the one-line note about the removal/alignment
- [ ] `make all-tests` and `make test-incr` still pass (no regression)
- [ ] `PROMPT_STATUS.md` shows TESTS-17 as DONE

**Risk mitigation:** If external CI does use `ci` and breaks after deletion, the fix is a one-line `git revert` of TESTS-17. The audit in task 1 is the safety net — if the search finds a `.github/workflows/*.yml` reference, we keep `ci` and align its topology. Worst case: a CI run goes red on the next push, the revert restores the broken-but-passing `ci` profile, and a follow-up prompt fixes it properly.

**Commit message:**
```
make release DESC="Tests: remove dead `test` profile + audit/remove `ci` profile topology (TESTS-17)"
```

**Estimated effort:** 30min. ~10min for the audit, ~20min for the pom.xml + AGENTS.md edits.

---

## TESTS-18: `flaky-jsonl` maintenance guard

**Goal:** Add a programmatic check that fails the build when a new `@Tag("flaky")` test class is added without an explicit decision about its jsonl compatibility. Today, the `flaky-jsonl` profile (pom.xml:499–530) excludes only `diesel/FlusherTest.java` and `diesel/wal/GroupCommitThroughputTest.java` by hardcoded `<exclude>` paths; any new flaky test is auto-included under jsonl and may take hours (precedent: `FlusherTest`'s 50k-insert loop took 14 min for the first 5k rows).

**Background:** Profile `flaky-jsonl` exists specifically because some `@Tag("flaky")` tests are not compatible with `diesel.storage.type=jsonl` (the 1M-row `JsonlStorageAdvancedTest` benchmark needs jsonl; the engine-default csv path makes `FlusherTest` orders of magnitude faster). The current exclusions are a manually-maintained allowlist:

```xml
<excludes combine.self="override">
    <exclude>diesel/FlusherTest.java</exclude>
    <exclude>diesel/wal/GroupCommitThroughputTest.java</exclude>
</excludes>
```

There is no signal when this list is stale. A new `@Tag("flaky")` test that happens to be jsonl-incompatible will be auto-included, run for hours, and only be discovered when someone runs `make test-flaky` and waits.

**Tasks:**
1. Create `scripts/check-flaky-jsonl-compat.sh` (POSIX sh, modeled on `scripts/assert-tests-ran.sh`):
```sh
#!/bin/sh
# check-flaky-jsonl-compat.sh - fail when a @Tag("flaky") test is not
# accounted for in the flaky-jsonl profile's exclude list.
#
# Usage: ./scripts/check-flaky-jsonl-compat.sh
#
# Why this exists: profile `flaky-jsonl` runs every @Tag("flaky") class
# under diesel.storage.type=jsonl. Two of those classes (FlusherTest,
# GroupCommitThroughputTest) take hours under jsonl and are explicitly
# excluded in pom.xml. The exclude list is a manually-maintained allowlist
# — a new flaky test added without a jsonl-compat decision will silently
# inherit the jsonl storage type and may run for hours on the next
# `make test-flaky` invocation. This script fails the build at compile
# time, before `mvn test` ever starts.

set -u

repo_root=$(cd "$(dirname "$0")/.." && pwd)
test_dir="$repo_root/src/test/java"

# Step 1: enumerate every @Tag("flaky") class.
flaky_classes=$(
    grep -rlE '@Tag\(\s*"flaky"\s*\)' "$test_dir" --include='*.java' 2>/dev/null \
        | sed "s|^$test_dir/||" \
        | sort -u
)

if [ -z "$flaky_classes" ]; then
    echo "WARN: no @Tag(\"flaky\") classes found — did the test layout change?"
    exit 0
fi

# Step 2: extract the <exclude> paths from the flaky-jsonl profile in pom.xml.
# Strip whitespace, take only the <exclude>...</exclude> content.
excluded_paths=$(
    awk '
        /<id>flaky-jsonl<\/id>/ { in_profile = 1 }
        in_profile && /<\/profile>/ { in_profile = 0 }
        in_profile && /<exclude>/ {
            match($0, /<exclude>([^<]+)<\/exclude>/, m)
            print m[1]
        }
    ' "$repo_root/pom.xml" \
        | sed 's|^diesel/|diesel/|' \
        | sort -u
)

# Step 3: for every flaky class, check if it has a @StorageType annotation.
# Classes WITHOUT @StorageType are storage-agnostic and run fine under jsonl.
# Classes WITH @StorageType({"jsonl", ...}) are jsonl-compatible.
# Classes WITH @StorageType but NOT including "jsonl" must be in the exclude list
# OR they will be silently disabled by StorageTypeCondition (acceptable — but
# we warn).
missing=""
for cls_path in $flaky_classes; do
    cls_file="$test_dir/$cls_path"

    # Check if the class has a @StorageType annotation.
    if grep -qE '@StorageType\s*\(' "$cls_file"; then
        # Check if jsonl is in the @StorageType value list.
        if grep -qE '@StorageType\s*\(\s*\{?[^}]*"jsonl"' "$cls_file"; then
            continue  # jsonl-compatible — runs under flaky-jsonl, OK.
        else
            # @StorageType but NOT jsonl — StorageTypeCondition will disable it.
            # Acceptable: it simply won't run under jsonl. No action needed.
            continue
        fi
    fi

    # No @StorageType — the class is storage-agnostic and WILL run under jsonl.
    # Check if it is in the exclude list (it should be there only if it is
    # known to be slow/broken under jsonl; absence from the list is fine if
    # the test is genuinely storage-agnostic and fast).
    # We do NOT fail here — the exclude list is opt-in for known-bad cases.
    # Instead, we list these classes as "running under jsonl, please verify".
    :
done

# Step 4: check that every exclude in the flaky-jsonl profile still exists
# as a source file (catches stale excludes from deleted tests).
stale_excludes=""
for excl in $excluded_paths; do
    if [ ! -f "$test_dir/$excl" ]; then
        stale_excludes="$stale_excludes $excl"
    fi
done

if [ -n "$stale_excludes" ]; then
    echo "ERROR: flaky-jsonl profile excludes non-existent test file(s):" >&2
    for excl in $stale_excludes; do
        echo "  $excl" >&2
    done
    echo "Update pom.xml <profile><id>flaky-jsonl</id> to remove the stale <exclude>." >&2
    exit 1
fi

echo "Verified: flaky-jsonl exclusions are up to date ($excluded_paths in pom.xml)."
exit 0
```
2. Add the script to the Makefile `assert-tests-ran` chain — call it from `test-flaky` BEFORE any Maven invocation (cheap, ~50ms):
```makefile
test-flaky:
	@./scripts/check-flaky-jsonl-compat.sh || { echo "Run 'make clean-test-cache' after fixing pom.xml exclusions."; exit 1; }
	@rc=0; \
	stamp=$$(mktemp); \
	echo "1/4 quarantined WAL group-commit acceptance (profile 'flaky')..."; \
	...
```
3. Add a CI step in `.github/workflows/ci.yml` (the lint job, if one exists; otherwise the pr-gate job) that runs the same check:
```yaml
- name: Verify flaky-jsonl exclusions are up to date
  run: ./scripts/check-flaky-jsonl-compat.sh
```
4. Document the contract in AGENTS.md — add a paragraph after the "Quarantined `@Tag("flaky")` tests run through neither command" bullet (AGENTS.md:252–254):
```markdown
**Adding a new `@Tag("flaky")` test (since TESTS-18):** every new flaky test is automatically included in profile `flaky-jsonl` under `diesel.storage.type=jsonl`. If the test is storage-agnostic and fast under jsonl, no action is needed. If the test is slow or broken under jsonl (precedent: `FlusherTest`'s 50k-insert loop took 14 min for 5k rows under jsonl), add its path to the `<excludes>` block in the `flaky-jsonl` profile (pom.xml). The script `scripts/check-flaky-jsonl-compat.sh` runs as part of `make test-flaky` and as a CI step — it fails if a stale `<exclude>` references a deleted test file. It does NOT fail for missing exclusions (those are a judgment call), but the CI log will list every `@StorageType`-less flaky class so the maintainer can review.
```
5. Update `PROMPT_STATUS.md`.

**Change type:** Test infrastructure, build target, quarantine (TESTS-*)
- Step 4 (fast gate): `make compile`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `make compile` (verify nothing broke)
- `./scripts/check-flaky-jsonl-compat.sh` — verify it prints "Verified: flaky-jsonl exclusions are up to date" and exits 0
- Temporarily rename `diesel/FlusherTest.java` to `diesel/FlusherTest.java.bak` and re-run the script — verify it fails with the "excludes non-existent test file" message, then restore
- `make test-flaky` — verify the new script runs as the first step and the rest of the flaky invocations are unaffected

**Acceptance criteria:**
- [ ] `scripts/check-flaky-jsonl-compat.sh` exists and is executable
- [ ] Script exits 0 on the current repo state (excludes match existing files)
- [ ] Script exits 1 when an `<exclude>` references a non-existent file (tested by temporarily renaming)
- [ ] `make test-flaky` calls the script as its first step
- [ ] CI workflow runs the script
- [ ] AGENTS.md updated with the "Adding a new `@Tag("flaky")` test" paragraph
- [ ] `PROMPT_STATUS.md` shows TESTS-18 as DONE

**Risk mitigation:** The script is read-only — it does not modify pom.xml or any test file. Worst case: it produces a false positive (claims a file is missing when it isn't), which is fixed by re-running. The `awk` parser for `<exclude>` is deliberately narrow (matches only the `flaky-jsonl` profile block, only `<exclude>...</exclude>` lines) — if pom.xml is reformatted, the parser may miss excludes, but the failure mode is "report 0 excludes when there are some", which triggers a warning rather than a hard failure (Step 1 still lists `flaky_classes`, so the maintainer sees what's expected).

**Commit message:**
```
make release DESC="Tests: add check-flaky-jsonl-compat.sh guard for stale exclusions (TESTS-18)"
```

**Estimated effort:** 1h. ~30min for the script, ~30min for the CI integration + AGENTS.md update.

---

## TESTS-19: `assert-tests-ran.sh` storage-aware extension

**Goal:** Extend `assert-tests-ran.sh` to detect the specific failure mode where a `storage-*` profile (or `-P all -Ddiesel.storage.type=<type>`) runs 0 storage tests because `diesel.storage.type` was misspelled or accidentally set to an unsupported value.

**Background:** `assert-tests-ran.sh` currently (scripts/assert-tests-ran.sh:48–56) counts any `TEST-*.xml` file newer than the stamp. If `mvn -B clean verify -P all -Ddiesel.storage.type=nonexistent` is run, surefire still writes reports for `smoke`/`query`/`index`/`query-full`/`concurrency`/`network` classes (which don't have `@StorageType` and therefore run regardless), and `assert-tests-ran.sh` reports "Verified: N surefire report(s) written" — masking the fact that 0 storage tests ran. This is exactly the failure mode that TESTS-16's test plan asks the implementer to verify by hand.

**Tasks:**
1. Extend `assert-tests-ran.sh` to accept an optional second argument, the expected storage type:
```sh
# Usage:
#   stamp=$(mktemp)
#   mvn -B clean test -P storage-avro || exit 1
#   ./scripts/assert-tests-ran.sh "$stamp" avro
#
# When the second argument is present, the script additionally verifies that
# at least one TEST-*.xml report corresponds to a class with
# @StorageType("<expected>") — i.e., the storage-type filter actually
# selected at least one storage-specific class. This catches the failure
# mode where diesel.storage.type is misspelled (e.g., "avro" vs "AVRO") or
# set to an unsupported value, in which case surefire still writes reports
# for storage-agnostic classes (smoke, query, etc.) and the existing
# "at least one report" check passes silently.
```
2. Implement the check using `xmllint` (or a Python one-liner if xmllint is unavailable — check `command -v xmllint` first, fall back to `python3 -c 'import sys, xml.etree.ElementTree as ET; ...'`):
```sh
if [ -n "${2:-}" ]; then
    expected_st="$2"
    # A storage-type-specific report's classname is a class that has
    # @StorageType("<expected_st>") in its source. We can't read surefire's
    # XML to know the @StorageType annotation (it's not in the report), so
    # we cross-reference the report's testclassname against the source tree:
    #   testclassname -> src/test/java/<package-path>.java -> grep @StorageType
    storage_reports=0
    for report in "$reports_dir"/TEST-*.xml; do
        [ -f "$report" ] || continue
        # Extract the testclassname attribute from the <testsuite> root element.
        classname=$(xmllint --xpath 'string(/testsuite/@name)' "$report" 2>/dev/null)
        # Convert "diesel.AvroStorageTest" -> "diesel/AvroStorageTest.java"
        src_path=$(echo "$classname" | tr '.' '/')
        src_file="$repo_root/src/test/java/${src_path}.java"
        [ -f "$src_file" ] || continue
        # Check if this source file has @StorageType with the expected value.
        if grep -qE "@StorageType\s*\(\s*\{?[^}]*\"$expected_st\"" "$src_file"; then
            storage_reports=$((storage_reports + 1))
        fi
    done
    if [ "$storage_reports" -eq 0 ]; then
        echo "ERROR: 0 storage reports for diesel.storage.type=$expected_st" >&2
        echo "Surefire wrote reports, but none correspond to a class annotated" >&2
        echo "with @StorageType(\"$expected_st\"). Check that:" >&2
        echo "  - the storage type is spelled correctly (lowercase: csv, tsv, jsonl, avro)" >&2
        echo "  - the @StorageType annotation on the target test class matches" >&2
        echo "  - the test class has the matching @Tag (e.g., @Tag(\"storage\"))" >&2
        exit 1
    fi
    echo "Verified: $storage_reports storage report(s) for type '$expected_st' (out of $count total)."
fi
```
3. Update the `run-tests` macro in Makefile (around line 63) to optionally pass the expected storage type. The cleanest approach is a new macro `run-tests-storage` that takes the storage type as a third argument:
```makefile
define run-tests-storage
	@stamp=$$(mktemp); \
	echo "Running profile '$(1)' (storage.type=$(3))..."; \
	$(MVN) -B test -P $(1) $(2) -Ddiesel.storage.type=$(3) || exit 1; \
	$(ASSERT_TESTS) "$$stamp" $(3)
endef
```
4. Update the `storage-*` Makefile targets to use the new macro:
```makefile
test-storage-csv:
	$(call run-tests-storage,storage-csv,,csv)

test-storage-tsv:
	$(call run-tests-storage,storage-tsv,,tsv)

test-storage-jsonl:
	$(call run-tests-storage,storage-jsonl,,jsonl)

test-storage-avro:
	$(call run-tests-storage,storage-avro,,avro)
```
5. Update the `all-tests-all-storages` target (added in TESTS-16) to pass the storage type to `assert-tests-ran`:
```makefile
all-tests-all-storages:
	@rc=0; \
	for st in csv tsv jsonl avro; do \
		echo ""; \
		echo "=== all-tests storage.type=$$st ==="; \
		stamp=$$(mktemp); \
		$(MVN) -B clean verify -P all -Ddiesel.storage.type=$$st || rc=1; \
		$(ASSERT_TESTS) "$$stamp" $$st || rc=1; \
		[ $$rc -ne 0 ] && break; \
	done; \
	exit $$rc
```
6. Document the new second-argument contract in the `assert-tests-ran.sh` header comment and in AGENTS.md after the "Guard: 'no tests ran' fails the build" subsection (around line 276).
7. Update `PROMPT_STATUS.md`.

**Change type:** Test infrastructure, build target, quarantine (TESTS-*)
- Step 4 (fast gate): `make compile`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `make compile`
- `make test-storage-csv` — verify it still passes and the new assertion message includes "(out of N total)" with the storage count > 0
- `make test-storage-avro` — same, expect ~30 storage reports for `@StorageType("avro")` classes
- Run `mvn -B clean test -P storage-avro -Ddiesel.storage.type=nonexistent` then `./scripts/assert-tests-ran.sh /tmp/stamp nonexistent` — verify it exits 1 with the "0 storage reports" message
- `make all-tests-all-storages` — verify each of the 4 iterations prints the storage-aware count
- `make test-incr` — verify the `run-tests` macro (no storage type passed) still works — backward compat

**Acceptance criteria:**
- [ ] `assert-tests-ran.sh` accepts an optional second argument (storage type)
- [ ] When the second argument is absent, behavior is unchanged (backward compatible)
- [ ] When the second argument is present, the script additionally checks that at least one `TEST-*.xml` corresponds to a `@StorageType("<type>")` source file
- [ ] `make test-storage-avro` prints "Verified: N storage report(s) for type 'avro'" with N > 0
- [ ] `make all-tests-all-storages` uses the new assertion
- [ ] AGENTS.md updated with the new contract
- [ ] `PROMPT_STATUS.md` shows TESTS-19 as DONE

**Risk mitigation:** The `xmllint` dependency is standard on Linux/macOS but may be missing on minimal Windows Git-Bash environments. The fallback `python3 -c '...'` covers that case (the repo already uses Python for `collect-timing.py`). If both `xmllint` and `python3` are unavailable, the script should fall back to the legacy behavior (count only, no storage-type check) and print a warning — never fail the build on a missing tool.

**Commit message:**
```
make release DESC="Tests: extend assert-tests-ran.sh with storage-type awareness (TESTS-19)"
```

**Estimated effort:** 1h. ~30min for the script extension, ~30min for the Makefile macro updates + AGENTS.md doc.

---

## TESTS-20: Delete `AGENTS3.md` (deprecated)

**Goal:** Remove the obsolete `AGENTS3.md` from the repo. It references removed Makefile targets, non-functional Maven flags, and an old prompt-file name, and serves only to confuse agents that may pick it up as an authoritative source.

**Background:** `AGENTS3.md` (106 lines) is a snapshot of `AGENTS.md` from an earlier prompt stage. It is now actively misleading:

- Line 18: invokes `make quick-test` — this target was removed (per AGENTS.md:472 "make quick-test has been removed — use make test").
- Line 20: `mvn test -DskipLargeTests` — this flag never worked. Surefire `<groups>` filtering ignores `-D` properties; the modern equivalent is the `large` Maven profile's `<excludedGroups>flaky</excludedGroups>` plus the `@LargeTest` annotation's `@EnabledIfSystemProperty` gate.
- Line 23–27: describes `make timing` as running "the full test suite with `@LargeTest` and 4GB heap" — wrong since TESTS-6, which split `make timing` into `timing-quick` (3–5 min, `@Tag("timing")` only) and `timing-full` (alias for the old `large` profile).
- Line 70: timing floor `>100ms` — wrong since TESTS-6 raised it to `>2.0s`.
- Line 4: references `prompt2.md` — current AGENTS.md works with `prompt3.md` / `prompt4.md` / `o2-prompts.md`.

A git grep confirms no other file references `AGENTS3.md`:
```bash
rg -n 'AGENTS3' --glob='!AGENTS3.md'
```
Returns no hits (verified during audit).

**Tasks:**
1. `git rm AGENTS3.md`
2. Verify no other file references it:
```bash
rg -n 'AGENTS3' --glob='!AGENTS3.md'
```
3. Update `PROMPT_STATUS.md`.

**Change type:** Test infrastructure, build target, quarantine (TESTS-*)
- Step 4 (fast gate): `make compile` (sanity — should be unaffected, no source code change)
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `git status` — verify `AGENTS3.md` is staged for deletion and nothing else changed
- `rg AGENTS3` — verify no other file references the deleted name
- `make compile` — sanity check (the deletion should not affect any build step)

**Acceptance criteria:**
- [ ] `AGENTS3.md` no longer exists in the repo
- [ ] `rg AGENTS3` returns no hits
- [ ] `make compile` still passes
- [ ] `PROMPT_STATUS.md` shows TESTS-20 as DONE

**Risk mitigation:** If a future prompt turns out to have referenced `AGENTS3.md` (audit in task 2 missed it), `git revert` restores it. The blast radius is zero — `AGENTS3.md` was documentation-only, no code reads it.

**Commit message:**
```
make release DESC="Docs: delete obsolete AGENTS3.md (TESTS-20) — references removed targets and obsolete flags"
```

**Estimated effort:** 15min. Pure deletion + audit grep.

---

## TESTS-21: Remove `pom.xml.versionsBackup` + add `.gitignore` entry

**Goal:** Remove the committed `pom.xml.versionsBackup` artifact and prevent it from being re-committed by adding the pattern to `.gitignore`.

**Background:** `pom.xml.versionsBackup` is a backup file created by `mvn versions:set` (Maven Versions Plugin). It is local-only state — the plugin's contract is that the user runs `mvn versions:commit` (deletes the backup) or `mvn versions:revert` (restores the backup and deletes the new pom.xml). The file was accidentally committed at some point and has been riding along ever since.

**Tasks:**
1. `git rm pom.xml.versionsBackup`
2. Add `pom.xml.versionsBackup` to `.gitignore` (create the file if it does not exist; append to the existing content if it does):
```
# Maven Versions Plugin backup file (mvn versions:set)
pom.xml.versionsBackup
```
3. Verify no other `*Backup` or `*.bak` files are tracked:
```bash
git ls-files | grep -iE 'backup|\.bak$|\.orig$'
```
If any are found, evaluate whether they should also be gitignored (out of scope for this prompt — note in commit message).
4. Update `PROMPT_STATUS.md`.

**Change type:** Config-only / constant extract
- Step 4 (fast gate): `make compile`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `git status` — verify `pom.xml.versionsBackup` is staged for deletion and `.gitignore` is staged for modification
- `make compile` — verify pom.xml still parses (the deletion is independent of pom.xml itself)
- `git check-ignore pom.xml.versionsBackup` — verify the pattern matches (the file would now be ignored if it re-appeared)
- Re-introduce the file artificially (`touch pom.xml.versionsBackup`) and run `git status` — verify it does NOT appear as untracked (it's now ignored). Delete the artificial file.

**Acceptance criteria:**
- [ ] `pom.xml.versionsBackup` is deleted from the repo
- [ ] `.gitignore` contains `pom.xml.versionsBackup`
- [ ] `git check-ignore pom.xml.versionsBackup` returns the pattern
- [ ] `make compile` still passes
- [ ] `PROMPT_STATUS.md` shows TESTS-21 as DONE

**Risk mitigation:** The file is a plugin artifact, not a project deliverable. Its deletion cannot break the build — Maven Versions Plugin recreates it on the next `versions:set` invocation, whereupon `.gitignore` keeps it out of version control. Zero risk.

**Commit message:**
```
make release DESC="Repo: remove accidentally-committed pom.xml.versionsBackup + gitignore it (TESTS-21)"
```

**Estimated effort:** 15min.

---

## TESTS-22: `--add-modules=jdk.incubator.vector` future-proofing

**Goal:** Make the `--add-modules=jdk.incubator.vector` flag in `large`/`perf`/`timing-quick`/`flaky`/`flaky-jsonl`/`storage-*` profiles conditional on the JDK version, so the project does not break silently when upgraded to Java 22+ (where the vector API moved to a preview module).

**Background:** Eight Maven profiles use `<argLine>-Xmx... --add-modules=jdk.incubator.vector</argLine>`:
- `large` (pom.xml:364)
- `perf` (pom.xml:317)
- `timing-quick` (pom.xml:562)
- `flaky` (pom.xml:476)
- `flaky-jsonl` (pom.xml:516)
- `storage-csv`, `storage-tsv`, `storage-jsonl`, `storage-avro` (pom.xml:662, 691, 719, 747)

`<maven.compiler.source>21</maven.compiler.source>` (pom.xml:13) — on Java 21, `--add-modules=jdk.incubator.vector` is the correct flag (the vector API is in `jdk.incubator.vector` as a non-preview module). On Java 22+, the API moved to `java.lang.Vector` in the `jdk.incubator.vector` preview module, requiring both `--add-modules=jdk.incubator.vector` AND `--enable-preview`. On Java 25+ (expected), the API may exit the incubator entirely, making `--add-modules` a no-op (warning) or removing the module (hard error).

Today this is latent — `pom.xml` pins to Java 21. But if the JDK is upgraded (e.g., to use a newer GC or a security patch not backported to 21), every profile that uses the flag will fail at JVM startup with `java.lang.module.FindException: Module jdk.incubator.vector not found` or `java.lang.UnsupportedOperationException: Preview features not enabled`.

**Tasks:**
1. Add a Maven property `diesel.vector.argline` whose value depends on the JDK version detected by Maven. Use the `maven-compiler-plugin`'s `<release>` element to detect the JDK, or use the `org.apache.maven.plugins:maven-toolchains-plugin` for cleaner cross-JDK support. Simplest approach: a Maven profile auto-activated by `<jdk>` range:
```xml
<!-- Default: Java 21 incubator vector module -->
<properties>
    <diesel.vector.argline>--add-modules=jdk.incubator.vector</diesel.vector.argline>
</properties>

<!-- Java 22+: incubator vector module requires --enable-preview -->
<profile>
    <id>jdk22-vector-preview</id>
    <activation>
        <jdk>[22,)</jdk>
    </activation>
    <properties>
        <diesel.vector.argline>--add-modules=jdk.incubator.vector --enable-preview</diesel.vector.argline>
    </properties>
</profile>

<!-- Java 25+: vector API graduated from incubator (hypothetical).
     When this happens, drop the --add-modules flag entirely. -->
<profile>
    <id>jdk25-vector-graduated</id>
    <activation>
        <jdk>[25,)</jdk>
    </activation>
    <properties>
        <diesel.vector.argline></diesel.vector.argline>
    </properties>
</profile>
```
2. Replace each hardcoded `--add-modules=jdk.incubator.vector` in `<argLine>` with `${diesel.vector.argline}`. Example for `large`:
```xml
<argLine>-Xmx4g ${diesel.vector.argline}</argLine>
```
Apply to all 9 occurrences: `large`, `perf`, `timing-quick`, `flaky`, `flaky-jsonl`, `storage-csv`, `storage-tsv`, `storage-jsonl`, `storage-avro`.
3. Verify on the current Java 21 JDK that `${diesel.vector.argline}` resolves correctly by running `make large-test` (or `make test-perf` if faster) and confirming the vector API is still loaded (no regression in the tests that depend on it).
4. Document the property in AGENTS.md — add a brief note in the "Build & run (Windows)" section:
```markdown
**Vector API flag (since TESTS-22):** the `--add-modules=jdk.incubator.vector` argLine flag is parameterized as `${diesel.vector.argline}` in `pom.xml`. On Java 21 (current), the property resolves to `--add-modules=jdk.incubator.vector` (incubator, non-preview). On Java 22+, an auto-activated profile adds `--enable-preview` (the API moved to a preview module). On Java 25+ (when the API graduates from the incubator), the property resolves to empty (the flag becomes unnecessary). This avoids a hard JVM-startup failure when the JDK is upgraded.
```
5. Update `PROMPT_STATUS.md`.

**Change type:** Config-only / constant extract
- Step 4 (fast gate): `make compile`
- Step 5 (acceptance): SKIP
- Step 6 (profile): SKIP

**Test plan:**
- `make compile` — verify pom.xml parses with the new property and profiles
- `mvn -B help:evaluate -Dexpression=diesel.vector.argline -DforceStdout` — verify the property resolves to `--add-modules=jdk.incubator.vector` on Java 21
- `make test-perf` — verify a profile that uses the flag still works (perf is the fastest vector-flagged profile, ~2-3 min)
- `make large-test` — verify the larger-heap profile still works
- (Optional, if Java 22 is available) Re-run `mvn -B help:evaluate -Dexpression=diesel.vector.argline -DforceStdout` under Java 22 — verify the property resolves to `--add-modules=jdk.incubator.vector --enable-preview`

**Acceptance criteria:**
- [ ] All 9 `--add-modules=jdk.incubator.vector` occurrences in pom.xml replaced with `${diesel.vector.argline}`
- [ ] Property `diesel.vector.argline` defined in `<properties>` with default value `--add-modules=jdk.incubator.vector`
- [ ] Auto-activated profile `jdk22-vector-preview` adds `--enable-preview` for JDK >= 22
- [ ] Auto-activated profile `jdk25-vector-graduated` empties the flag for JDK >= 25 (forward-looking)
- [ ] `make test-perf` passes (regression check on the simplest vector-flagged profile)
- [ ] AGENTS.md "Build & run" section documents the property
- [ ] `PROMPT_STATUS.md` shows TESTS-22 as DONE

**Risk mitigation:** The change is a pure refactor on Java 21 — the property resolves to the exact same string that was hardcoded before. The Java 22+ profile is auto-activated by `<jdk>[22,)</jdk>`, which is Maven's built-in JDK range activation (no plugin needed). The Java 25+ profile is hypothetical (the API has not yet graduated as of 2026-11); if graduation never happens or happens with a different module name, the profile can be adjusted when the time comes. Worst case: a Java 22+ run fails because the actual preview-module name differs from the assumed `jdk.incubator.vector` — but that failure is now a one-line property edit instead of a 9-place search-and-replace.

**Commit message:**
```
make release DESC="Build: parameterize --add-modules=jdk.incubator.vector for Java 22+ compatibility (TESTS-22)"
```

**Estimated effort:** 30min. ~15min for the pom.xml refactor + 15min for the AGENTS.md doc + a `make test-perf` regression run.

---

## Implementation order (recommended)

1. **TESTS-21** (15min, zero risk) — clean up the `pom.xml.versionsBackup` noise first.
2. **TESTS-20** (15min, zero risk) — delete `AGENTS3.md`.
3. **TESTS-17** (30min) — audit and remove/document `test` and `ci` profiles. Doing this before TESTS-16 keeps the storage-coverage discussion clean (no dead profiles in the matrix).
4. **TESTS-16** (1.5h) — the headline fix. Adds `make all-tests-all-storages` and the storage-coverage matrix in AGENTS.md.
5. **TESTS-19** (1h) — depends on TESTS-16's new `all-tests-all-storages` target; extends `assert-tests-ran.sh` to actually validate storage coverage.
6. **TESTS-18** (1h) — independent of TESTS-16/19; can run in parallel.
7. **TESTS-22** (30min) — independent; can run in parallel with TESTS-18.

Total wall-time if sequential: ~5h. With parallelism (TESTS-18 and TESTS-22 in parallel with TESTS-19): ~4h.

Each prompt commits independently — the order matters only for TESTS-19's dependency on TESTS-16.

---

## Post-implementation verification

After all seven prompts land, verify the audit findings are addressed by re-running the original audit:

1. **Storage coverage of `-P all`:** Run `make all-tests-all-storages` end-to-end. Confirm that `target/surefire-reports/` contains `TEST-diesel.JsonlStorageTest.xml`, `TEST-diesel.AvroStorageTest.xml`, `TEST-diesel.TsvStorageTest.xml` (one of each format) at the appropriate iteration.
2. **`AGENTS.md` accuracy:** Read AGENTS.md:308 area and confirm it no longer calls `make all-tests` the unqualified "release gate" — it now says "Release gate (default storage)" and links to `make all-tests-all-storages`.
3. **`AGENTS3.md` gone:** `ls AGENTS3.md` should fail with "No such file or directory".
4. **`pom.xml.versionsBackup` gone:** `git ls-files | grep versionsBackup` should return empty.
5. **`test`/`ci` profiles:** `grep -E '<id>(test|ci)</id>' pom.xml` should return either nothing (if deleted) or be accompanied by AGENTS.md documentation (if kept).
6. **`flaky-jsonl` guard:** `./scripts/check-flaky-jsonl-compat.sh` should exit 0; temporarily renaming `FlusherTest.java` should make it exit 1.
7. **Vector flag:** `mvn -B help:evaluate -Dexpression=diesel.vector.argline -DforceStdout` should print `--add-modules=jdk.incubator.vector` on Java 21.

If any of these fail, the corresponding prompt's acceptance criteria were not actually met — re-open it in `PROMPT_STATUS.md` and re-implement before the next prompt stage.
