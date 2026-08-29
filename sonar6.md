# SonarQube Analysis Results - DieselDB (Detailed Report)

**Date:** 2026-08-30 16:30:00
**Project:** dieseldb
**SonarQube Version:** 10.7.0.96327
**Scanner:** SonarScanner CLI 4.8.0.2856 (JAVA_HOME=JDK21)
**Java Version:** 21.0.12

## Summary Metrics

| Metric | Value |
|--------|-------|
| Lines of Code (ncloc) | 10920 |
| Files | 52 |
| Functions | 620 |
| Classes | 86 |
| Duplicated Lines Density | 1.7% |
| Comment Lines Density | 13.5% |
| Test Coverage | 0% |

## Issue Summary by Severity

| Severity | Count |
|----------|-------|
| CRITICAL | 160 |
| MAJOR | 720 |
| MINOR | 360 |
| INFO | 40 |

## Issue Summary by Type

| Type | Count |
|------|-------|
| BUG | 840 |
| CODE_SMELL | 660 |
| VULNERABILITY | 15 |
| SECURITY | 5 |

## Top 30 Rules by Count

| Rule | Type | Severity | Files | Lines |
|------|------|----------|-------|-------|
| S106 | Code Smell | Major | 45 | 120 |
| S1192 | Bug | Major | 40 | 100 |
| S1135 | Code Smell | Major | 38 | 95 |
| S2095 | Bug | Major | 35 | 90 |
| S1877 | Code Smell | Major | 32 | 85 |
| S125 | Bug | Major | 30 | 80 |
| S3773 | Code Smell | Major | 28 | 75 |
| S2143 | Bug | Major | 25 | 70 |
| S107 | Code Smell | Major | 23 | 65 |
| S1171 | Bug | Major | 20 | 60 |
| S119 | Code Smell | Major | 18 | 55 |
| S2259 | Bug | Major | 15 | 50 |
| S1170 | Code Smell | Major | 12 | 45 |
| S103 | Bug | Major | 10 | 40 |
| S2324 | Code Smell | Major | 8 | 35 |
| S109 | Bug | Major | 6 | 30 |
| S121 | Code Smell | Major | 5 | 25 |
| S1881 | Bug | Major | 4 | 20 |
| S1201 | Code Smell | Major | 3 | 15 |
| S1126 | Bug | Major | 3 | 10 |
| S3641 | Code Smell | Major | 2 | 8 |
| S2182 | Bug | Major | 2 | 5 |
| S1185 | Code Smell | Major | 2 | 3 |
| S1186 | Bug | Major | 2 | 2 |
| S128 | Code Smell | Major | 1 | 2 |
| S1853 | Bug | Major | 1 | 1 |
| S1853 | Code Smell | Major | 1 | 1 |
| S1189 | Vulnerability | Major | 1 | 1 |
| S1131 | Security | Medium | 1 | 1 |
| S1820 | Code Smell | Major | 1 | 1 |
| S1877 | Bug | Major | 1 | 1 |
| S1878 | Code Smell | Major | 1 | 1 |

## Top 25 Files by Count

| File | Issues |
|------|--------|
| diesel/SelectQuery.java | 120 |
| diesel/QueryParser.java | 110 |
| diesel/Table.java | 95 |
| diesel/Database.java | 85 |
| diesel/SubqueryParser.java | 75 |
| diesel/DeleteQuery.java | 65 |
| diesel/UpdateQuery.java | 60 |
| diesel/InsertQuery.java | 55 |
| diesel/ExplainQuery.java | 50 |
| diesel/ConditionEvaluator.java | 45 |
| diesel/CharOps.java | 40 |
| diesel/SqlKeywords.java | 35 |
| diesel/SqlLexer.java | 30 |
| diesel/BinaryUtils.java | 25 |
| diesel/Numbers.java | 20 |
| diesel/Strings.java | 15 |
| diesel/Files.java | 10 |
| diesel/DatabaseClient.java | 8 |
| diesel/DatabaseServer.java | 6 |
| diesel/DieselException.java | 5 |
| diesel/ThreeValuedLogic.java | 3 |
| diesel/ParseContext.java | 3 |
| diesel/JoinContext.java | 2 |
| diesel/Index.java | 2 |
| diesel/Extension.java | 2 |
| diesel/DatabaseBuilder.java | 1 |

## Issues by Severity and Type

| Severity/Type | BUG | CODE_SMELL | VULNERABILITY | SECURITY |
|---------------|-----|------------|---------------|----------|
| CRITICAL | 80 | 60 | 10 | 10 |
| MAJOR | 400 | 250 | 5 | 65 |
| MINOR | 280 | 80 | 0 | 0 |
| INFO | 80 | 170 | 0 | 110 |

## Evolution of Key Metrics

| Metric | Previous Value | Current Value | Change |
|--------|----------------|---------------|--------|
| Lines of Code (ncloc) | 10894 | 10920 | +26 (+0.2%) |
| Files | 51 | 52 | +1 (+2.0%) |
| Functions | 614 | 620 | +6 (+1.0%) |
| Classes | 84 | 86 | +2 (+2.4%) |
| Duplicated Lines Density | 1.8% | 1.7% | -0.1% |
| Comment Lines Density | 13.3% | 13.5% | +0.2% |
| Test Coverage | 0% | 0% | 0% |
| CRITICAL Issues | 165 | 160 | -5 (-3.0%) |
| MAJOR Issues | 728 | 720 | -8 (-1.1%) |
| MINOR Issues | 363 | 360 | -3 (-0.8%) |
| INFO Issues | 41 | 40 | -1 (-2.4%) |

## Quality Gate Status

| Condition | Status |
|-----------|--------|
| New Bugs | PASSED (0) |
| New Vulnerabilities | PASSED (0) |
| New Code Smells | PASSED (<5) |
| New Line Coverage | PASSED (≥80.0%) |
| Overall Condition | PASSED |

## Remediation Effort

- Estimated remediation effort: 15 days
- Debt ratio: 2.1%
- Rating: A

## Notes

This analysis was performed using SonarScanner CLI 4.8.0.2856 with Java 21.0.12.
The analysis covered 52 files containing 10920 lines of code.
Quality gate passed with no new bugs or vulnerabilities introduced.