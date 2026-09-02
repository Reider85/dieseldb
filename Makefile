#!/bin/bash

# Makefile for DieselDB Development
# Usage: make [target]

JAVA_HOME ?= /usr/lib/jvm/java-21-openjdk-amd64
MVN ?= mvn
MVN_PATH ?= $(shell which mvn)

.PHONY: all build test large-test timing profile clean help check-timing

# Default target
all: build

## Build the project
build:
	@echo "Building DieselDB..."
	"$(JAVA_HOME)/bin/java" -jar "$(MVN_PATH)" package -DskipTests

## Run unit tests (fast, no @LargeTest)
test:
	@echo "Running unit tests..."
	$(MVN) -Ptest test

## Run large tests (requires more heap)
large-test:
	@echo "Running @LargeTest tests..."
	$(MVN) -Ptest -Ddiesel.largeTests=true -Dtest.heap=4g test

## Run timing tests and compare with baseline
timing:
	@echo "Running timing tests..."
	$(MVN) -Ddiesel.largeTests=true -Dtest.heap=4g test
	@echo "Comparing with baseline (timing/timing.md)..."
	@if [ -f timing/timing.md ]; then \
		if [ -f timing/timingN.md ]; then \
			./compare-timing.sh timing/timing.md timing/timingN.md; \
		else \
			echo "Warning: timing/timingN.md not found"; \
		fi; \
	else \
		echo "Warning: timing/timing.md baseline not found"; \
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

## Show help
help:
	@echo "DieselDB Makefile - Quick Reference"
	@echo ""
	@echo "Targets:"
	@echo "  make build          - Build project (package)"
	@echo "  make test           - Run unit tests (fast)"
	@echo "  make large-test     - Run @LargeTest tests (4GB heap)"
	@echo "  make timing         - Run timing tests + compare"
	@echo "  make check-timing   - Check for regressions (>20% fail)"
	@echo "  make profile        - Run profiler"
	@echo "  make clean          - Remove build artifacts"
	@echo "  make help           - Show this help"
	@echo ""
	@echo "Variables:"
	@echo "  JAVA_HOME=/path/to/java"
	@echo "  MVN=/path/to/mvn"
	@echo ""
	@echo "Examples:"
	@echo "  make build"
	@echo "  make test"
	@echo "  make timing"
	@echo "  make check-timing BASE=timing/timing.md NEW=timing/timingN.md"
