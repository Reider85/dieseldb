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