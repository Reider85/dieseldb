#!/bin/bash

# Timing Regression Check Script
# Compares timing/timingN.md against baseline timing/timing.md
# Fails if any test shows >20% degradation
# Usage: ./compare-timing.sh [baseline.md] [new.md]

BASELINE="${1:-timing/timing.md}"
NEW="${2:-timing/timingN.md}"
THRESHOLD=1.2  # 20% degradation threshold

if [ ! -f "$BASELINE" ]; then
    echo "Error: Baseline file '$BASELINE' not found"
    exit 1
fi

if [ ! -f "$NEW" ]; then
    echo "Error: New timing file '$NEW' not found"
    exit 1
fi

echo "Comparing $NEW against $BASELINE (threshold: ${THRESHOLD}x)"
echo "============================================================"

REGRESSIONS=0
IMPROVEMENTS=0
UNCHANGED=0

# Read baseline into associative array
declare -A BASE_TIMES
while IFS=' ' read -r test_name time rest; do
    if [[ "$test_name" != "Test" && -n "$test_name" && -n "$time" ]]; then
        BASE_TIMES["$test_name"]="$time"
    fi
done < "$BASELINE"

# Compare new times against baseline
while IFS=' ' read -r test_name time rest; do
    if [[ "$test_name" == "Test" || -z "$test_name" || -z "$time" ]]; then
        continue
    fi
    
    if [[ -v BASE_TIMES["$test_name"] ]]; then
        base_time="${BASE_TIMES[$test_name]}"
        
        # Calculate ratio using awk for floating point
        ratio=$(awk "BEGIN {printf \"%.3f\", $time / $base_time}")
        inv_ratio=$(awk "BEGIN {printf \"%.2f\", 1 / $ratio}")
        
        is_regression=$(awk "BEGIN {print ($ratio > $THRESHOLD) ? 1 : 0}")
        is_improvement=$(awk "BEGIN {print ($ratio < 0.8) ? 1 : 0}")
        
        if [ "$is_regression" -eq 1 ]; then
            printf "🔴 REGRESSION: %-40s %sx (was: %.3fs, now: %.3fs)\n" "$test_name" "$ratio" "$base_time" "$time"
            ((REGRESSIONS++))
        elif [ "$is_improvement" -eq 1 ]; then
            printf "🟢 IMPROVEMENT: %-39s %sx faster (was: %.3fs, now: %.3fs)\n" "$test_name" "$inv_ratio" "$base_time" "$time"
            ((IMPROVEMENTS++))
        else
            ((UNCHANGED++))
        fi
    else
        echo "⚪ NEW TEST: $test_name = ${time}s (no baseline)"
    fi
done < "$NEW"

echo "============================================================"
echo "Summary:"
echo "  Regressions (>20%): $REGRESSIONS"
echo "  Improvements (>20%): $IMPROVEMENTS"
echo "  Unchanged: $UNCHANGED"

if [ $REGRESSIONS -gt 0 ]; then
    echo ""
    echo "❌ FAILED: $REGRESSIONS test(s) show performance regression"
    exit 1
else
    echo ""
    echo "✅ PASSED: No significant regressions detected"
    exit 0
fi
