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
