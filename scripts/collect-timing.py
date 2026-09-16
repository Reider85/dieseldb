#!/usr/bin/env python3
# collect-timing.py - collect per-test wall times from surefire XML reports
# Output: timing/timingN.md in the format consumed by compare-timing.sh:
#   <test_name> <seconds>
# Usage: python3 scripts/collect-timing.py   (run from repo root after mvn test)

import glob
import os
import xml.etree.ElementTree as ET

ROWS = []
for path in glob.glob("target/surefire-reports/TEST-*.xml"):
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as e:
        print("WARN: cannot parse %s: %s" % (path, e))
        continue
    for tc in root.iter("testcase"):
        cls = (tc.get("classname") or "").split(".")[-1]
        name = "%s.%s" % (cls, tc.get("name") or "")
        try:
            secs = float(tc.get("time") or 0.0)
        except ValueError:
            secs = 0.0
        ROWS.append((name, secs))

ROWS.sort(key=lambda r: -r[1])

os.makedirs("timing", exist_ok=True)
with open("timing/timingN.md", "w", encoding="utf-8") as out:
    out.write("Test Time(s)\n")
    for name, secs in ROWS:
        out.write("%s %.3f\n" % (name, secs))

print("Wrote %d rows to timing/timingN.md" % len(ROWS))
