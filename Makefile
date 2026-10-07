#!/bin/bash

# Makefile for DieselDB Development
# Usage: make [target]

JAVA_HOME ?= /usr/lib/jvm/java-21-openjdk-amd64

# Must be exported, not just assigned: a variable set only inside a Makefile is
# not visible to the environment of the recipe's child processes, so Maven never
# saw the JAVA_HOME defined two lines above.
export JAVA_HOME

# Resolve Maven once, JAVA_HOME before PATH. An explicit MVN=... on the command
# line still wins. The previous "MVN_PATH ?= $(shell which mvn)" was declared
# and then never referenced, so every target actually executed a bare "mvn" and
# failed on machines where Maven is not on PATH.
MVN ?= $(if $(wildcard $(JAVA_HOME)/bin/mvn),$(JAVA_HOME)/bin/mvn,$(shell command -v mvn 2>/dev/null || echo mvn))

PY ?= python3

ifeq ($(OS),Windows_NT)
PS = powershell -ExecutionPolicy Bypass -File
# git-helpers.ps1 adds retry + stale-lock cleanup, so use it where it can run.
GIT_PUSH = $(PS) ./scripts/git-helpers.ps1 push
# commit-and-changelog.ps1 has no POSIX counterpart, so on Linux fall back to
# pwsh (the cross-platform PowerShell). Fail loudly rather than silently
# dropping the changelog if it is not installed.
CHANGELOG = pwsh -File ./scripts/commit-and-changelog.ps1
else
# git push is the portable primitive; retry is a nicety, not a requirement.
GIT_PUSH = git push origin main
CHANGELOG = pwsh -File ./scripts/commit-and-changelog.ps1
endif

# tia.ps1 is the Windows entry point, tia.sh the POSIX one. Both exist in
# scripts/; picking the right one keeps `make tia` working on Linux.
ifeq ($(OS),Windows_NT)
TIA = $(PS) ./scripts/tia.ps1
else
TIA = ./scripts/tia.sh
endif

ASSERT_TESTS = ./scripts/assert-tests-ran.sh

# ---------------------------------------------------------------------------
# The only two ways to run tests.
#
# Usage: $(call run-tests,fast)
#        $(call run-tests-incr,fast,-Dtest=JoinTest#joinKeys)
#
# Every test target routes through these macros on purpose. Each target used to
# spell out its own "$(MVN) -B clean test -P <profile>" line, and a broken
# argument passthrough in mvn.ps1 once made all of them run zero tests while
# still reporting BUILD SUCCESS. Centralising the invocation means the
# "-P <profile>" argument has exactly one owner, and the post-run assertion that
# tests actually executed is never forgotten.
# ---------------------------------------------------------------------------
define run-tests
	@stamp=$$(mktemp); \
	echo "Running profile '$(1)'..."; \
	$(MVN) -B clean test -P $(1) $(2) || exit 1; \
	$(ASSERT_TESTS) "$$stamp"
endef

define run-tests-incr
	@stamp=$$(mktemp); \
	echo "Running profile '$(1)' (no clean)..."; \
	$(MVN) -B test -P $(1) $(2) || exit 1; \
	$(ASSERT_TESTS) "$$stamp"
endef

.PHONY: all build compile test test-one test-incr test-core test-network test-concurrency test-perf large-test all-tests timing profile clean clean-cache clean-test-cache help check-timing compare-timing tia tia-run changelog release release-local setup doctor test-storage-csv test-storage-tsv test-storage-jsonl test-storage-avro sync-version

# Default target
all: build

## Build the project
build:
	@echo "Building DieselDB..."
	$(MVN) -B package -DskipTests

## Quick compile check (3-5s, no tests — use for syntax verification)
compile:
	@echo "Compiling (no tests)..."
	$(MVN) -B compile -q

## Run single test (usage: make test-one T=QueryParserTest\#testSelect)
## Note: in make, '#' starts a comment, so it MUST be escaped as '\#'.
test-one:
	@if [ -z "$(T)" ]; then \
		echo "Usage: make test-one T=ClassName\\#methodName"; \
		echo "       The hash must be escaped for make: T=ClassName\\#methodName"; \
		exit 1; \
	fi
	$(call run-tests-incr,fast,-Dtest=$(T))

## Fast incremental test without clean
test-incr:
	$(call run-tests-incr,fast)

## Create changelog entry with auto-incrementing version prefix
## Usage: make changelog DESC="short description"
changelog:
	@if [ -z "$(DESC)" ]; then \
		echo "Usage: make changelog DESC=\"description\""; \
		exit 1; \
	fi
	$(CHANGELOG) "$(DESC)"

## Sync pom.xml project version with Changelog/commit
## Usage: make sync-version              - sync from last Changelog entry
##        make sync-version DESC=check   - check only (exit 1 on mismatch)
##        make sync-version DESC=X.Y.Z   - sync to explicit version
sync-version:
	$(PS) ./scripts/sync-pom-version.ps1 $(if $(filter check,$(DESC)),-CheckOnly,$(if $(DESC),-Version $(DESC),))

## Changelog + commit + push in one step (usage: make release DESC="fix: ...")
## PROMPT_STATUS.md must be updated BEFORE this target runs (AGENTS.md workflow
## step 7); it is staged explicitly so the DONE entry joins this commit.
release:
	@if [ -z "$(DESC)" ]; then echo "Usage: make release DESC=\"description\""; exit 1; fi
	$(CHANGELOG) "$(DESC)"
	git add PROMPT_STATUS.md
	@if git diff --cached --name-only | grep -q 'PROMPT_STATUS\.md'; then \
		echo "PROMPT_STATUS.md staged - its DONE entry joins this commit."; \
	else \
		echo "PROMPT_STATUS.md unchanged - if a prompt was completed, update it BEFORE make release."; \
	fi
	git add -A
	git commit -F changelog_entry.txt
	$(GIT_PUSH)

## Changelog + commit only, no push (usage: make release-local DESC="fix: ...")
release-local:
	@if [ -z "$(DESC)" ]; then echo "Usage: make release-local DESC=\"description\""; exit 1; fi
	$(CHANGELOG) "$(DESC)"
	git add PROMPT_STATUS.md
	@if git diff --cached --name-only | grep -q 'PROMPT_STATUS\.md'; then \
		echo "PROMPT_STATUS.md staged - its DONE entry joins this commit."; \
	else \
		echo "PROMPT_STATUS.md unchanged - if a prompt was completed, update it BEFORE make release."; \
	fi
	git add -A
	git commit -F changelog_entry.txt

## Run fast profile: smoke + index + query
test:
	$(call run-tests,fast)

## Run core profile: full query + storage (2-4 min)
test-core:
	$(call run-tests,core)

## Run network profile: server + sockets
test-network:
	$(call run-tests,network)

## Run concurrency profile: txn + threads
test-concurrency:
	$(call run-tests,concurrency)

## Run perf profile: benchmarks
test-perf:
	$(call run-tests,perf)

## Run large profile: @LargeTest (4GB heap)
large-test:
	$(call run-tests,large)

## Run format-specific storage tests (only tests matching diesel.storage.type run)
test-storage-csv:
	$(call run-tests,storage-csv)

test-storage-tsv:
	$(call run-tests,storage-tsv)

test-storage-jsonl:
	$(call run-tests,storage-jsonl)

test-storage-avro:
	$(call run-tests,storage-avro)

## Run ALL profiles sequentially (release gate)
all-tests:
	@echo "Running ALL profiles sequentially (release gate)..."
	@for p in fast core concurrency network perf large; do \
		echo ""; \
		echo "--- $$p ---"; \
		$(call run-tests,$$p) || exit 1; \
	done
	@echo ""
	@echo "All profiles passed."

## Full acceptance gate: large profile (4GB heap) + timing compare vs baseline
timing:
	$(call run-tests,large)
	$(PY) scripts/collect-timing.py
	@if [ -f timing/timing.md ]; then \
		./compare-timing.sh timing/timing.md timing/timingN.md; \
	else \
		echo "Baseline timing/timing.md not found - creating it from this run."; \
		cp timing/timingN.md timing/timing.md; \
		echo "Baseline created. Re-run 'make timing' to compare against it."; \
	fi

## Compare timing results (usage: make compare-timing BASE=timing/timing.md NEW=timing/timingN.md)
compare-timing:
	@if [ -z "$(BASE)" ] || [ -z "$(NEW)" ]; then \
		echo "Usage: make compare-timing BASE=timing/timing.md NEW=timing/timingN.md"; \
		exit 1; \
	fi
	./compare-timing.sh $(BASE) $(NEW)

## Profile main application
profile:
	@echo "Compiling profiler..."
	javac -cp target/classes ProfileMain.java
	@echo "Running profiler..."
	java -Xmx4g -cp target/classes:. ProfileMain

## Clean test cache only (surefire reports, build cache, test classes)
clean-test-cache:
	@echo "Cleaning test cache..."
	rm -rf target/.cache target/surefire-reports target/test-classes

## Clean Maven build cache
clean-cache:
	@echo "Cleaning build cache..."
	rm -rf $(HOME)/.m2/build-cache/v1/com.dieseldb/dieseldb

## Clean build artifacts
clean: clean-cache
	@echo "Cleaning..."
	$(MVN) clean
	rm -rf target/.cache
	rm -f data/*.csv data/*.table *.log timing/timingN.md classpath.txt

## Check timing regression (fail if degradation > 20%)
check-timing:
	@if [ ! -f timing/timing.md ] || [ ! -f timing/timingN.md ]; then \
		echo "Error: timing/timing.md or timing/timingN.md not found"; \
		exit 1; \
	fi
	@echo "Checking for timing regressions (>20% is failure)..."
	@awk 'NR==FNR {if(NR>1) base[$$1]=$$2; next} \
	FNR>1 { \
		if($$1 in base) { \
			ratio=$$2/base[$$1]; \
			if(ratio>1.2) { \
				printf "REGRESSION: %s %.2fx (baseline: %.3f, new: %.3f)\n", $$1, ratio, base[$$1], $$2; \
				fail=1; \
			} else if(ratio<0.8) { \
				printf "IMPROVEMENT: %s %.2fx faster\n", $$1, 1/ratio; \
			} \
		} \
	} \
	END {if(fail) exit 1}' timing/timing.md timing/timingN.md
	@echo "Timing check passed (no regressions >20%)"

## Test-impact analysis: show recommended profiles
tia:
	@echo "Running test-impact analysis..."
	@$(TIA)

## TIA + auto-run recommended profiles
tia-run:
	@echo "Running TIA + impacted tests..."
	@$(TIA) --run

## Verify build environment: JAVA_HOME, mvn, java (exit 1 if broken)
setup:
	@echo "Checking build environment..."
	@echo "MVN=$(MVN)"
	@java -version 2>&1 | head -1
	@$(MVN) -version 2>&1 | head -1
	@echo "Build environment OK."

## Full environment diagnostic
doctor:
	@echo "=== DieselDB Doctor ==="
	@echo ""
	@echo "--- Java ---"
	@java -version 2>&1 || echo "ERROR: java not found on PATH"
	@echo ""
	@echo "--- JAVA_HOME ---"
	@if [ -n "$$JAVA_HOME" ]; then echo "JAVA_HOME=$$JAVA_HOME"; else echo "WARNING: JAVA_HOME not set"; fi
	@echo ""
	@echo "--- Maven ---"
	@echo "resolved MVN: $(MVN)"
	@$(MVN) -version 2>&1 || echo "ERROR: mvn not found (set MVN=/path/to/mvn)"
	@echo ""
	@echo "--- Git ---"
	@git --version 2>&1 || echo "ERROR: git not found"
	@git remote -v 2>&1 | head -2
	@git branch --show-current 2>&1
	@echo ""
	@echo "--- Python ---"
	@$(PY) --version 2>&1 || echo "ERROR: python not found"
	@echo ""
	@echo "--- Lock files ---"
	@find .git -name "*.lock" 2>/dev/null | head -5 || echo "  (none)"
	@echo ""
	@echo "--- Disk space ---"
	@df -h . 2>/dev/null || echo "  (N/A on Windows)"
	@echo ""
	@echo "=== Doctor done ==="

## Show help
help:
	@echo "DieselDB Makefile - Quick Reference"
	@echo ""
	@echo "Quick workflow (simple changes):"
	@echo "  make compile            - Compile only, no tests (3-5s)"
	@echo "  make test-incr          - Fast profile without clean"
	@echo "  make test-one T=X\\#m    - Run a single test method (hash must be escaped: \\#)"
	@echo "  make release DESC=\"...\"  - Changelog + commit + push"
	@echo ""
	@echo "Targets:"
	@echo "  make build              - Build project (package, skip tests)"
	@echo "  make changelog          - Create changelog entry with auto version prefix"
	@echo "  make sync-version       - Sync pom.xml version with Changelog/commit"
	@echo "  make test               - Fast profile: smoke + index + query"
	@echo "  make test-core          - Core profile: query + storage (2-4 min)"
	@echo "  make test-network       - Network profile: server + sockets (1-2 min)"
	@echo "  make test-concurrency   - Concurrency profile: txn + threads (<30s)"
	@echo "  make test-perf          - Performance profile: benchmarks (2-3 min)"
	@echo "  make large-test         - Large profile: @LargeTest tests (3-8 min, 4GB heap)"
	@echo "  make all-tests          - Run ALL profiles sequentially (release gate)"
	@echo "  make tia                - Test-impact analysis: recommend profiles"
	@echo "  make tia-run            - TIA + run recommended profiles"
	@echo "  make timing             - Run timing tests + compare"
	@echo "  make check-timing       - Check for regressions (>20% fail)"
	@echo "  make profile            - Run profiler"
	@echo "  make clean              - Remove build artifacts"
	@echo "  make clean-test-cache   - Clean test cache only (surefire, build cache)"
	@echo "  make setup              - Verify JAVA_HOME, mvn, java are available"
	@echo "  make doctor             - Full environment diagnostic"
	@echo "  make help               - Show this help"
	@echo ""
	@echo "Every test target asserts that surefire produced reports and fails if"
	@echo "Maven reported success without running a single test."
	@echo ""
	@echo "PowerShell (Windows agents):"
	@echo "  .\scripts\make.ps1 <target>         - Full Makefile equivalent"
	@echo "  .\scripts\mvn.ps1 <args>            - Maven with auto JAVA_HOME"
	@echo "  .\scripts\git-helpers.ps1 push      - Git push with retry (origin main)"
	@echo "  .\scripts\git-helpers.ps1 fix-lock  - Remove stale git lock files"
	@echo "  .\scripts\compare-timing.ps1        - Compare timing (PowerShell)"
	@echo ""
	@echo "Variables:"
	@echo "  JAVA_HOME=/path/to/java"
	@echo "  MVN=/path/to/mvn"
	@echo "  T=ClassName\\#method   for test-one"
	@echo "  DESC=\"...\"            for changelog, release, release-local"
	@echo "  BASE=/NEW=            for compare-timing"
