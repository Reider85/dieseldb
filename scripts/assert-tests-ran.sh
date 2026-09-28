#!/bin/sh
# assert-tests-ran.sh - fail when Maven reported success without running a test
#
# Usage:
#   stamp=$(mktemp)
#   mvn -B clean test -P fast || exit 1
#   ./scripts/assert-tests-ran.sh "$stamp"
#
# The stamp MUST be created before Maven starts. Only reports newer than the
# stamp count, because `test-incr` runs without `clean` and would otherwise be
# satisfied by reports left over from an earlier run.
#
# Why this exists: the default surefire <configuration> in pom.xml excludes every
# source file, so a run without -P <profile> selects zero tests and still prints
# BUILD SUCCESS. A commit that did not compile once passed the local gate that way.
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

repo_root=$(cd "$(dirname "$0")/.." && pwd)
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

echo "Verified: $count surefire report(s) written."
exit 0
