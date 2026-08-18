# Plan: Test Profile Table Cleanup

## Goal
Before each test profile run (default, `-Ptest`, `-Ptest-light`), completely clear all table files (.csv and .table) that tests create, ensuring a clean slate and preventing cross-test contamination.

## Current State Analysis

### Tables Created by Tests
| Table | Used By |
|-------|---------|
| USERS | AdvancedTest, OrderByTest, AliasesTest, LikeTest, GroupByTest, JoinTest, InTest, QuantitativeTest, AllTestsSampleTest, Phase0IntegrationTest, DatabaseSmokeTest, SubqueriesTest |
| PROFILES | OrderByTest, GroupByTest, QuantitativeTest, AllTestsSampleTest |
| TRANSACTIONS | AliasesTest, QuantitativeTest, AllTestsSampleTest |
| USER_DETAILS | JoinTest, QuantitativeTest, AllTestsSampleTest |
| PERSIST_TEST | PersistenceTest, QuantitativeTest, AllTestsSampleTest |
| PERSIST_TEST2 | PersistenceTest |
| NULL_TEST | QuantitativeTest |
| AGG_TEST | QuantitativeTest |
| TXN_TEST | QuantitativeTest |
| CASE_TEST | QuantitativeTest |
| MyTable | QuantitativeTest |
| BOOL_TEST | QuantitativeTest |
| TXN66_TEST | QuantitativeTest |
| TXN67_TEST | QuantitativeTest |
| TXN68_TEST | QuantitativeTest |
| PROMPT70_TEST | QuantitativeTest (uses temp dir) |
| SMOKE | DatabaseSmokeTest |
| USERS_PERF | (referenced in file listing, not in test code) |

### Files to Clean
Each table creates two files:
- `{TABLE}.csv` - data file
- `{TABLE}.table` - serialized table metadata

### Current Cleanup Approaches (Inconsistent)
- **QuantitativeTest/AllTestsSampleTest**: `dropTable()` in `setup()` drops tables via `database.dropTable()` which deletes files
- **PersistenceTest**: Has `@BeforeEach`/`@AfterEach` `cleanup()` deleting `.csv` and `.table` files explicitly
- **OrderByTest/GroupByTest/JoinTest/SubqueriesTest/AdvancedTest/AliasesTest**: Call `dropTable()` or `dropTables()` in `@BeforeEach`
- **Other tests**: No explicit cleanup (rely on `dropTable` in setup or nothing)

### Problem
- Files persist on disk between test runs
- Different tests use different cleanup strategies
- If a test crashes or is interrupted, files remain
- Running `-Ptest` after default profile (or vice versa) can have leftover files
- No centralized cleanup before profile execution

## Design Decisions

### 1. Where to Implement Cleanup
**Decision**: Maven `maven-antrun-plugin` in `pom.xml` bound to `pre-test` phase (runs before surefire).

Rationale:
- Runs once per test execution (not per test class)
- Applies to all profiles (default, `-Ptest`, `-Ptest-light`)
- Can delete files in project base directory where tables are created
- Declarative, no test code changes needed

### 2. Cleanup Strategy
**Decision**: Delete all known test table files (.csv and .table) from project base directory.

Files to delete (from analysis):
```
USERS.csv, USERS.table
PROFILES.csv, PROFILES.table
TRANSACTIONS.csv, TRANSACTIONS.table
USER_DETAILS.csv, USER_DETAILS.table
PERSIST_TEST.csv, PERSIST_TEST.table
PERSIST_TEST2.csv, PERSIST_TEST2.table
NULL_TEST.csv, NULL_TEST.table
AGG_TEST.csv, AGG_TEST.table
TXN_TEST.csv, TXN_TEST.table
CASE_TEST.csv, CASE_TEST.table
MyTable.csv, MyTable.table
BOOL_TEST.csv, BOOL_TEST.table
TXN66_TEST.csv, TXN66_TEST.table
TXN67_TEST.csv, TXN67_TEST.table
TXN68_TEST.csv, TXN68_TEST.table
PROMPT70_TEST.csv, PROMPT70_TEST.table
SMOKE.csv, SMOKE.table
USERS_PERF.csv, USERS_PERF.table (if exists)
```

Use wildcard patterns for simplicity: `*_TEST.csv`, `*_TEST.table`, plus specific known names.

### 3. Implementation Approach
Add `maven-antrun-plugin` to `<build><plugins>` (not profile-specific) so it runs for all test executions:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-antrun-plugin</artifactId>
    <version>3.1.0</version>
    <executions>
        <execution>
            <id>clean-test-tables</id>
            <phase>pre-test</phase>
            <goals>
                <goal>run</goal>
            </goals>
            <configuration>
                <target>
                    <delete>
                        <fileset dir="${project.basedir}" includes="*.csv,*.table" 
                                 excludes="config.properties,**/target/**"/>
                    </delete>
                </target>
            </configuration>
        </execution>
    </executions>
</plugin>
```

Note: Use `excludes` to avoid deleting `config.properties` and anything in `target/`.

### 4. Alternative: JUnit Extension (Future)
Could also create a JUnit 5 `@BeforeAll` extension for per-test-class cleanup, but Maven-level is simpler and covers all profiles uniformly.

## Implementation Tasks

1. **Add maven-antrun-plugin to pom.xml**
   - Place in `<build><plugins>` (outside profiles)
   - Configure execution at `pre-test` phase
   - Delete `*.csv` and `*.table` files from `${project.basedir}`
   - Exclude `config.properties` and `target/**`

2. **Verify Cleanup Works**
   - Run `mvn test` (default profile) → confirm files deleted before test
   - Run `mvn test -Ptest` → confirm files deleted before test
   - Run `mvn test -Ptest-light` → confirm files deleted before test
   - Check that test data is properly recreated during test execution

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Deletes user's config.properties | Explicitly exclude `config.properties` |
| Deletes target/ build output | Exclude `target/**` |
| Files created in subdirectories | Current tests create files in project root; if subdirs used, adjust fileset |
| Performance overhead | Minimal - only deletes ~30 small files |
| Tests that need persistent data | No test currently relies on cross-run persistence |

## Validation Plan

1. Create some test table files manually: `touch USERS.csv USERS.table`
2. Run `mvn test` - verify files are deleted before test starts (check logs)
3. Run `mvn test -Ptest` - same
4. Run `mvn test -Ptest-light` - same
5. Verify all tests still pass (351 tests for -Ptest/-Ptest-light, 2 for default)
6. Verify config.properties and target/ are untouched

## Open Questions

1. Should cleanup also happen after test (`post-test`) for extra safety? (Current: pre-test only)
2. Are there any tables created in subdirectories? (Current: all in project root)
3. Should USERS_PERF be added to explicit list? (Only in file listing, not in test code)

---
Plan ready for implementation.