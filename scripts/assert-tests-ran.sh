#!/bin/sh
# assert-tests-ran.sh - fail when Maven reported success without running a test
#
# Usage:
#   stamp=$(mktemp)
#   mvn -B clean test -P fast || exit 1
#   ./scripts/assert-tests-ran.sh "$stamp"
# #
# # With storage type validation (TESTS-19):
#   stamp=$(mktemp)
#   mvn -B clean test -P storage-avro || exit 1
#   ./scripts/assert-tests-ran.sh "$stamp" avro
#
# The stamp MUST be created before Maven starts. Only reports newer than the
# stamp count, because `test-incr` runs without `clean` and would otherwise be
# satisfied by reports left over from an earlier run.
#
# Why this exists: the default surefire <configuration> in pom.xml excludes every
# source file, so a run without -P <profile> selects zero tests and still prints
# BUILD SUCCESS. A commit that did not compile once passed the local gate that way.
#
# Storage-aware extension (TESTS-19): when a second argument (storage type) is
# provided, the script additionally verifies that at least one TEST-*.xml report
# corresponds to a class with @StorageType("<expected>") — i.e., the storage-type
# filter actually selected at least one storage-specific class. This catches the
# failure mode where diesel.storage.type is misspelled (e.g., "avro" vs "AVRO")
# or set to an unsupported value, in which case surefire still writes reports
# for storage-agnostic classes (smoke, query, etc.) and the existing "at least
# one report" check passes silently.
#
# Known caveat: under WSL the repo lives on a 9p/DrvFs mount with attribute
# caching (cache=5). A report created within ~5s of the stamp can be reported as
# not-newer, producing a spurious failure. On native Linux, where this Makefile
# is meant to run, /tmp and target/ are both on ext4 and the comparison is exact.

set -u

stamp="${1:-}"
if [ -z "$stamp" ] || [ ! -f "$stamp" ]; then
    echo "ERROR: usage: $0 <stamp-file>" >&2
    echo "       The stamp must be an existing file created before Maven ran." >&2
    exit 1
fi

# Portable approach for dirname
case "$0" in
    */*) script_dir="${0%/*}" ;;
    *) script_dir="." ;;
esac
repo_root=$(cd "$script_dir/.." && pwd)
reports_dir="$repo_root/target/surefire-reports"

if [ ! -d "$reports_dir" ]; then
    echo "" >&2
    echo "ERROR: $reports_dir does not exist - no tests were executed." >&2
    echo "" >&2
    echo "Maven reported success without running a single test. In this project" >&2
    echo "that happens whenever the surefire profile is missing: the default" >&2
    echo "<configuration> in pom.xml excludes every source file, so" >&2
    echo "  mvn -B clean test          (no -P)  ->  0 tests, BUILD SUCCESS" >&2
    echo "  mvn -B clean test -P fast          ->  tests run" >&2
    echo "" >&2
    echo "Check that the Maven command line actually contains '-P <profile>'." >&2
    exit 1
fi

count=$(find "$reports_dir" -name 'TEST-*.xml' -newer "$stamp" 2>/dev/null | wc -l | tr -d ' ')

if [ "$count" -eq 0 ]; then
    echo "" >&2
    echo "ERROR: no surefire reports newer than $stamp - no tests were executed." >&2
    echo "Maven reported success without running a single test." >&2
    echo "Check that the Maven command line actually contains '-P <profile>'." >&2
    exit 1
fi

# Storage-aware check (TESTS-19): if a storage type is provided, verify that
# at least one report corresponds to a @StorageType("<storage_type>") class.
if [ -n "${2:-}" ]; then
    expected_st="$2"
    storage_reports=0
    
    # Check for xmllint availability, fallback to Python if needed
    if command -v xmllint >/dev/null 2>&1; then
        # Use xmllint to extract testclassname from XML reports
        for report in "$reports_dir"/TEST-*.xml; do
            [ -f "$report" ] || continue
            # Extract the testclassname attribute from the <testsuite> root element.
            classname=$(xmllint --xpath 'string(/testsuite/@name)' "$report" 2>/dev/null)
            [ -z "$classname" ] && continue
            # Convert "diesel.AvroStorageTest" -> "diesel/AvroStorageTest.java"
            src_path=$(echo "$classname" | tr '.' '/')
            src_file="$repo_root/src/test/java/${src_path}.java"
            [ -f "$src_file" ] || continue
            # Check if this source file has @StorageType with the expected value.
            if grep -qE "@StorageType\s*\(\s*\{?[^}]*\"$expected_st\"" "$src_file"; then
                storage_reports=$((storage_reports + 1))
            fi
        done
    else
        # Fallback to Python if xmllint is not available
        if command -v python3 >/dev/null 2>&1; then
            for report in "$reports_dir"/TEST-*.xml; do
                [ -f "$report" ] || continue
                classname=$(python3 -c "
import xml.etree.ElementTree as ET
try:
    tree = ET.parse('$report')
    root = tree.getroot()
    print(root.get('name', ''))
except:
    pass
" 2>/dev/null)
                [ -z "$classname" ] && continue
                # Convert "diesel.AvroStorageTest" -> "diesel/AvroStorageTest.java"
                src_path=$(echo "$classname" | tr '.' '/')
                src_file="$repo_root/src/test/java/${src_path}.java"
                [ -f "$src_file" ] || continue
                # Check if this source file has @StorageType with the expected value.
                if python3 -c "
import re
with open('$src_file', 'r') as f:
    content = f.read()
if re.search(r'@StorageType\s*\(\\s*\\{?[^}]*\"$expected_st\"', content):
    exit(0)
else:
    exit(1)
" 2>/dev/null; then
                    storage_reports=$((storage_reports + 1))
                fi
            done
        else
            # If neither xmllint nor Python is available, warn and fall back to legacy behavior
            echo "WARNING: Neither xmllint nor python3 available for storage-type validation. Using legacy count-only behavior." >&2
            storage_reports="$count"
        fi
    fi
    
    if [ "$storage_reports" -eq 0 ]; then
        echo "" >&2
        echo "ERROR: 0 storage reports for diesel.storage.type=$expected_st" >&2
        echo "Surefire wrote reports, but none correspond to a class annotated" >&2
        echo "with @StorageType(\"$expected_st\"). Check that:" >&2
        echo "  - the storage type is spelled correctly (lowercase: csv, tsv, jsonl, avro)" >&2
        echo "  - the @StorageType annotation on the target test class matches" >&2
        echo "  - the test class has the matching @Tag (e.g., @Tag(\"storage\"))" >&2
        exit 1
    fi
    echo "Verified: $storage_reports storage report(s) for type '$expected_st' (out of $count total)."
else
    echo "Verified: $count surefire report(s) written."
fi

exit 0
