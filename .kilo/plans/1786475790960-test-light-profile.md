# Plan: test-light Maven Profile

## Goal
Create a separate Maven profile `test-light` that runs ALL tests in under 60 seconds by reducing record counts, with a hard minimum of 5 records per table (0 not allowed).

---

## Current State Analysis

### Test Categories

| Test Class | RECORD_COUNT | Data Setup |
|------------|--------------|------------|
| **Lightweight (10 records)** | | |
| AdvancedTest | 10 | @BeforeEach creates USERS |
| AliasesTest | 10 | @BeforeEach creates USERS, TRANSACTIONS |
| JoinTest | 10 | @BeforeEach creates USERS, USER_DETAILS |
| OrderByTest | 10 | @BeforeEach creates USERS, PROFILES |
| LikeTest | 10 | @BeforeEach creates USERS |
| SubqueriesTest | 10 | @BeforeEach creates USERS |
| InTest | 10 | @BeforeEach creates USERS |
| GroupByTest | 10 | @BeforeEach creates USERS, PROFILES |
| PerformanceTest | 10 | Creates USERS with 16 columns |
| **Heavy (600 records)** | | |
| QuantitativeTest | 600 | setup() creates USERS, PROFILES, TRANSACTIONS, USER_DETAILS |
| AllTestsSampleTest | 600 | setup() creates USERS, PROFILES, TRANSACTIONS, USER_DETAILS |

### Maven Profiles (pom.xml:56-73)
- **Default**: runs only `AllTestsSampleTest`, `QuantitativeTest`
- **`-Ptest`**: runs `**/*Test.java` (all 17 test classes)

### Assertions Dependent on RECORD_COUNT
- **OrderByTest**: expects `RECORD_COUNT` (simple) or `RECORD_COUNT * RECORD_COUNT` (join with OR)
- **GroupByTest**: expects `RECORD_COUNT`
- **AliasesTest**: expects `RECORD_COUNT`
- **QuantitativeTest/AllTestsSampleTest**: many assertions expect `RECORD_COUNT` or `RECORD_COUNT * RECORD_COUNT`

### No Current Mechanism
- No system property or config for record count
- Each test class hardcodes its own `RECORD_COUNT` constant

---

## Design Decisions

### 1. Configuration Mechanism: System Property
**Decision**: Use `-Dtest.record.count=<N>` system property read by all test classes.
- Clean, standard Maven approach
- Can be set in profile configuration
- Tests fall back to hardcoded default if property not set

### 2. Centralized Config Class
**Decision**: Create `TestConfig.java` in `src/test/java/diesel/` with:
```java
public final class TestConfig {
    public static final int RECORD_COUNT = Integer.getInteger("test.record.count", 600);
    private TestConfig() {}
}
```
- All test classes reference `TestConfig.RECORD_COUNT`
- Default 600 preserves current behavior for default and `-Ptest` profiles

### 3. Test-Light Profile Configuration
**Decision**: Add profile `test-light` in pom.xml that:
- Sets `-Dtest.record.count=5` via surefire `argLine`
- Runs all tests (`**/*Test.java` like `-Ptest`)
- Uses same JVM settings (`-Xmx4g`)

### 4. Assertion Updates
For tests with `RECORD_COUNT * RECORD_COUNT` expectations (join with OR producing cross-product):
- These assertions **must be updated** to use `TestConfig.RECORD_COUNT * TestConfig.RECORD_COUNT`
- At count=5: expects 25 rows (not 360000)
- This is correct behavior - the test validates the logic, not the absolute number

---

## Implementation Tasks

### Task 1: Create TestConfig.java
**File**: `src/test/java/diesel/TestConfig.java`
- Public final class with static `RECORD_COUNT` reading `test.record.count` system property
- Default value: 600

### Task 2: Update Lightweight Test Classes (9 files)
**Files**: AdvancedTest, AliasesTest, JoinTest, OrderByTest, LikeTest, SubqueriesTest, InTest, GroupByTest, PerformanceTest
- Replace `private static final int RECORD_COUNT = 10;` with `TestConfig.RECORD_COUNT`
- Remove local constant declaration

### Task 3: Update Heavy Test Classes (2 files)
**Files**: QuantitativeTest, AllTestsSampleTest
- Replace `private static final int RECORD_COUNT = 600;` with `TestConfig.RECORD_COUNT`
- Verify all assertions using `RECORD_COUNT` or `RECORD_COUNT * RECORD_COUNT` work with dynamic value

### Task 4: Add test-light Profile to pom.xml
**File**: pom.xml (after line 72, before `</profiles>`)
```xml
<profile>
    <id>test-light</id>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <includes>
                        <include>**/*Test.java</include>
                    </includes>
                    <argLine>-Xmx4g -Dtest.record.count=5</argLine>
                </configuration>
            </plugin>
        </plugins>
    </build>
</profile>
```

### Task 5: Validate
- Run `mvn test -Ptest-light` → must complete in < 60s, all tests pass
- Run `mvn test -Ptest` → must still pass (default 600 records)
- Run `mvn test` (default) → must still pass (only 2 test classes, 600 records)

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Join OR-condition assertions fail at low count | Assertions use `TestConfig.RECORD_COUNT * TestConfig.RECORD_COUNT` - will correctly expect 25 at count=5 |
| PerformanceTest writes benchmark report | It uses its own RECORD_COUNT for report labels; update to `TestConfig.RECORD_COUNT` |
| AllTestsSampleTest writes timing report | Uses `timingEntries` with query names; no count dependency in report format |
| .table files loaded by persistence tests | PersistenceTest uses fixed inserts (Alice, Bob), not RECORD_COUNT - no change needed |

---

## Validation Commands

```bash
# Test light profile (should complete in < 60s)
mvn.cmd test -Ptest-light

# Verify full profile still works
mvn.cmd test -Ptest

# Verify default profile still works
mvn.cmd test
```

---

## Out of Scope
- Changing .table file contents (they're for persistence tests with fixed data)
- Modifying timing comparison logic (compare-timing.ps1)
- Changing Changelog (done on commit by user)