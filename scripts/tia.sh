#!/usr/bin/env bash
# tia.sh - Test-Impact Analysis for DieselDB (bash port of tia.ps1).
# Maps changed sources (git diff) to JUnit 5 @Tag buckets and Maven profiles.
#
# Usage:
#   ./scripts/tia.sh                    # show recommendations vs origin/main...HEAD
#   ./scripts/tia.sh HEAD~1             # compare with previous commit
#   ./scripts/tia.sh main..feature      # compare two refs
#   ./scripts/tia.sh --tags             # print tags only (comma-separated)
#   ./scripts/tia.sh --profiles         # print profile names only (space-separated)
#   ./scripts/tia.sh --run [ref]        # run recommended profiles sequentially

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

MAPPING_FILE="$REPO_ROOT/scripts/tia-mapping.txt"
if [[ ! -f "$MAPPING_FILE" ]]; then
    echo "Error: mapping file not found: $MAPPING_FILE" >&2
    exit 1
fi

REF="origin/main...HEAD"
MODE="show"
for arg in "$@"; do
    case "$arg" in
        --run)      MODE="run" ;;
        --tags)     MODE="tags" ;;
        --profiles) MODE="profiles" ;;
        *)          REF="$arg" ;;
    esac
done

CHANGED=$(git diff --name-only "$REF" 2>/dev/null | sort -u || true)
if [[ -z "$CHANGED" ]]; then
    echo "No changes detected against $REF"
    exit 0
fi

if [[ "$MODE" == "show" ]]; then
    echo "=== Changed files (vs $REF) ==="
    echo "$CHANGED" | sed 's/^/  /'
    echo ""
fi

declare -A TAGS_SET
while IFS= read -r FILE; do
    [[ -z "$FILE" ]] && continue
    while read -r GLOB TAGS; do
        [[ -z "${GLOB:-}" || -z "${TAGS:-}" || "$GLOB" == \#* ]] && continue
        if [[ "$FILE" == $GLOB ]]; then
            IFS=',' read -ra ARR <<< "$TAGS"
            for T in "${ARR[@]}"; do
                T="$(echo "$T" | xargs)"
                [[ -n "$T" && "$T" != "none" ]] && TAGS_SET["$T"]=1
            done
        fi
    done < "$MAPPING_FILE"
done <<< "$CHANGED"

if echo "$CHANGED" | grep -qE "^pom\.xml$"; then
    TAGS_SET["all"]=1
fi

if [[ -z "${TAGS_SET[*]+x}" ]]; then
    echo "No test buckets impacted by changes."
    exit 0
fi

if [[ -n "${TAGS_SET[all]:-}" ]]; then
    case "$MODE" in
        tags)     echo "all" ;;
        profiles) echo "fast core concurrency network perf large" ;;
        *)
            echo "=== pom.xml changed - run every profile ==="
            for P in fast core concurrency network perf large; do
                echo "  mvn -B clean test -P $P"
            done
            ;;
    esac
    exit 0
fi

TAGS_SORTED=$(printf "%s\n" "${!TAGS_SET[@]}" | sort -u | paste -sd, -)

PROFILES=""
for T in "${!TAGS_SET[@]}"; do
    case "$T" in
        smoke|query|index)  PROFILES="$PROFILES fast" ;;
        query-full|storage) PROFILES="$PROFILES core" ;;
        concurrency)        PROFILES="$PROFILES concurrency" ;;
        network)            PROFILES="$PROFILES network" ;;
        perf)               PROFILES="$PROFILES perf" ;;
    esac
done
PROFILES=$(echo "$PROFILES" | tr ' ' '\n' | sort -u | grep -v '^$' | paste -sd' ' -)

case "$MODE" in
    tags)
        echo "$TAGS_SORTED"
        ;;
    profiles)
        echo "$PROFILES"
        ;;
    show)
        echo "=== Recommended test tags ==="
        echo "$TAGS_SORTED" | tr ',' '\n' | sed 's/^/  - /'
        echo ""
        echo "=== Recommended Maven profiles ==="
        echo "$PROFILES" | tr ' ' '\n' | sed 's/^/  - /'
        echo ""
        echo "=== Commands ==="
        for P in $PROFILES; do
            echo "  mvn -B clean test -P $P"
        done
        echo ""
        echo "NOTE: 'mvn -Dgroups=...' without -P runs 0 tests (default surefire excludes all)."
        ;;
    run)
        RC=0
        for P in $PROFILES; do
            echo ""
            echo "--- $P ---"
            if ! mvn -B clean test -P "$P"; then
                echo "FAILED: $P"
                RC=1
                break
            fi
        done
        exit $RC
        ;;
esac
