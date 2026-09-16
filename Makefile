#!/bin/bash

# Makefile for DieselDB Development
# Usage: make [target]

JAVA_HOME ?= /usr/lib/jvm/java-21-openjdk-amd64
MVN ?= mvn
MVN_PATH ?= $(shell which mvn)
PY ?= python3

ifeq ($(OS),Windows_NT)
TIA = powershell -ExecutionPolicy Bypass -File ./scripts/tia.ps1
else
TIA = ./scripts/tia.sh
endif

.PHONY: all build test test-core test-network test-concurrency test-perf large-test all-tests timing profile clean help check-timing tia tia-run

# Default target
all: build

## Build the project
build:
	@echo "Building DieselDB..."
	$(MVN) -B package -DskipTests

## Run fast profile: smoke + index + query (<30s)
test:
	@echo "Running fast profile (smoke + index + query)..."
	$(MVN) -B clean test -P fast

## Run core profile: full query + storage (2-4 min)
test-core:
	@echo "Running core profile..."
	$(MVN) -B clean test -P core

## Run network profile: server + sockets
test-network:
	@echo "Running network profile..."
	$(MVN) -B clean test -P network

## Run concurrency profile: txn + threads
test-concurrency:
	@echo "Running concurrency profile..."
	$(MVN) -B clean test -P concurrency

## Run perf profile: benchmarks
test-perf:
	@echo "Running perf profile..."
	$(MVN) -B clean test -P perf

## Run large profile: @LargeTest (4GB heap)
large-test:
	@echo "Running large profile (@LargeTest)..."
	$(MVN) -B clean test -P large

## Run ALL profiles sequentially (release gate)
all-tests:
	@echo "Running ALL profiles sequentially..."
	for p in fast core concurrency network perf large; do \
		echo "--- $$p ---"; \
		$(MVN) -B clean test -P $$p || exit 1; \
	done

## Full acceptance gate: large profile (4GB heap) + timing compare vs baseline
timing:
	@echo "Running acceptance gate: large profile (600x600 joins, 4GB heap)..."
	$(MVN) -B clean test -P large
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

## Clean build artifacts
clean:
	@echo "Cleaning..."
	$(MVN) clean
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

## Show help
help:
	@echo "DieselDB Makefile - Quick Reference"
	@echo ""
	@echo "Targets:"
	@echo "  make build              - Build project (package, skip tests)"
	@echo "  make test               - Fast profile: smoke + index + query (<30s)"
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
	@echo "  make help               - Show this help"
	@echo ""
	@echo "Variables:"
	@echo "  JAVA_HOME=/path/to/java"
	@echo "  MVN=/path/to/mvn"
