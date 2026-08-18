# SonarQube Analysis Results - DieselDB
**Date:** 2026-08-18 22:41:11
**Project:** dieseldb
**SonarQube Version:** 10.7.0.96327
**Scanner Version:** 6.2.1.4610
**Java Version:** 21.0.11 Axiom JSC

## Analysis Summary
- **Status:** SUCCESS
- **Files Analyzed:** 50 Java source files
- **Total Analysis Time:** 1m 28s
- **Dashboard:** http://localhost:9000/dashboard?id=dieseldb

## Key Metrics
- **Lines of Code:** ~3,800+ (50 files)
- **Languages:** Java (1 language detected)
- **Test Files:** 0 (sonar.tests not configured)
- **Coverage:** No coverage data (JaCoCo not configured)

## Warnings
1. **Dependencies missing:** `sonar.java.libraries` property is empty - may affect precision
2. **Unresolved imports/types detected** - enable DEBUG for details
3. **Preview features detected** - Java 21 preview features enabled
4. **Deprecated auth:** sonar.login/sonar.password deprecated, use sonar.token instead

## Sensors Run
- JavaSensor [java] - 39.9s
- SurefireSensor [java] - 3.5s
- JaCoCo XML Report Importer [jacoco] - no report
- Java Config Sensor [iac] - 0 files
- IaC Docker Sensor [iac] - 0 files
- TextAndSecretsSensor [text] - 1.9s
- Zero Coverage Sensor - 0.1s
- Java CPD Block Indexer - 0.3s
- SCM Publisher - 7.0s
- CPD Executor - 0.1s

## CPD (Copy-Paste Detection)
- 18 files had no CPD blocks
- 32 files analyzed for duplication
- CPD calculation completed

## Quality Profile
- **Java:** Sonar way (default)

## Next Steps
1. Configure `sonar.java.libraries` for better analysis precision
2. Add test coverage (JaCoCo)
3. Configure `sonar.tests` property to identify test files
4. Use `sonar.token` instead of deprecated login/password
5. Review issues on SonarQube dashboard at http://localhost:9000/dashboard?id=dieseldb