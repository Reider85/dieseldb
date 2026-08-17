package diesel;

import diesel.Database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SelectQueryRefactorTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        createUsers();
        insertUsers();
        createProfiles();
        insertProfiles();
    }

    private void createUsers() {
        database.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)", null);
    }

    private void createProfiles() {
        database.executeQuery("CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(p_seq 1 1), USER_ID LONG, PROFILE_NAME STRING, PROFILE_AGE INTEGER)", null);
    }

    private void insertUsers() {
        for (int i = 1; i <= 7; i++) {
            String query = String.format(Locale.US,
                    "INSERT INTO USERS (NAME, AGE, BALANCE) VALUES ('User%d', %d, %d)",
                    i, i * 5, 100 + i);
            database.executeQuery(query, null);
        }
    }

    private void insertProfiles() {
        for (int i = 1; i <= 6; i++) {
            String query = String.format(Locale.US,
                    "INSERT INTO PROFILES (USER_ID, PROFILE_NAME, PROFILE_AGE) VALUES (%d, 'Prof%d', %d)",
                    i, i, i * 5);
            database.executeQuery(query, null);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runSelect(String query) {
        return (List<Map<String, Object>>) database.executeQuery(query, null);
    }

    @Test
    void executeSelectReturnsAllRows() {
        List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS");
        assertEquals(7, rows.size(), "executeSelect must return all rows");
    }

    @Test
    void applyWhereFilterFiltersRows() {
        List<Map<String, Object>> rows = runSelect("SELECT ID, NAME FROM USERS WHERE AGE > 10");
        assertEquals(5, rows.size(), "WHERE AGE > 10 must keep ages 15,20,25,30,35");
        rows = runSelect("SELECT ID, NAME FROM USERS WHERE AGE > 10 AND AGE < 30");
        assertEquals(3, rows.size(), "WHERE AGE > 10 AND AGE < 30 must keep ages 15,20,25");
    }

    @Test
    void applyJoinsInnerHashJoinReturnsMatches() {
        List<Map<String, Object>> rows = runSelect("SELECT ID, PROFILE_NAME FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID");
        assertEquals(6, rows.size(), "INNER JOIN must match the 6 profiled users");
        assertEquals("Prof1", rows.get(0).get("PROFILE_NAME"), "first joined row must be Prof1");
    }

    @Test
    void applyJoinsLeftJoinRunsNestedLoop() {
        List<Map<String, Object>> rows = runSelect("SELECT ID, PROFILE_NAME FROM USERS LEFT JOIN PROFILES ON USERS.ID = PROFILES.USER_ID");
        assertEquals(6, rows.size(), "LEFT JOIN must return the 6 matched rows");
    }

    @Test
    void applyJoinsCrossJoinProducesProduct() {
        List<Map<String, Object>> rows = runSelect("SELECT ID, PROFILE_ID FROM USERS CROSS JOIN PROFILES");
        assertEquals(42, rows.size(), "CROSS JOIN must produce 7 x 6 rows");
    }

    @Test
    void applyJoinsOrInOnConditionProducesWarningPath() {
        List<Map<String, Object>> rows = runSelect("SELECT COUNT(*) AS CNT FROM USERS u JOIN PROFILES p ON u.ID = p.USER_ID OR u.ID = p.PROFILE_ID");
        assertEquals(1, rows.size(), "OR-in-ON JOIN must return one aggregate row");
        assertEquals(6L, ((Number) rows.get(0).get("CNT")).longValue(), "OR-in-ON JOIN must match one profile per user");
    }

    @Test
    void applyGroupByReturnsNUniqueGroups() {
        List<Map<String, Object>> rows = runSelect("SELECT NAME, COUNT(*) AS CNT FROM USERS GROUP BY NAME");
        assertEquals(7, rows.size(), "GROUP BY unique NAME must return one row per group");
        for (Map<String, Object> row : rows) {
            assertEquals(1L, ((Number) row.get("CNT")).longValue(), "each unique NAME group must aggregate exactly one row");
        }
    }

    @Test
    void applyOrderBySortsRows() {
        List<Map<String, Object>> rows = runSelect("SELECT ID, AGE FROM USERS ORDER BY AGE DESC");
        assertEquals(7, rows.size(), "ORDER BY must return all rows");
        assertEquals(7L, ((Number) rows.get(0).get("ID")).longValue(), "oldest user (age 35) must be first");
        assertEquals(1L, ((Number) rows.get(6).get("ID")).longValue(), "youngest user (age 5) must be last");
    }

    @Test
    void applyLimitOffsetAppliesLimits() {
        List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS ORDER BY ID LIMIT 3 OFFSET 2");
        assertEquals(3, rows.size(), "LIMIT 3 OFFSET 2 must return three rows");
        assertEquals(3L, ((Number) rows.get(0).get("ID")).longValue(), "first returned row must be ID 3");
        assertEquals(5L, ((Number) rows.get(2).get("ID")).longValue(), "last returned row must be ID 5");
    }

    @Test
    void aggregateWithoutGroupByWithLimit() {
        List<Map<String, Object>> rows = runSelect("SELECT COUNT(*) AS CNT FROM USERS LIMIT 1");
        assertEquals(1, rows.size(), "aggregate without GROUP BY must return one row");
        assertEquals(7L, ((Number) rows.get(0).get("CNT")).longValue(), "COUNT(*) must cover all rows before LIMIT");
    }

    @Test
    void combinedPipelineJoinWhereGroupByOrderByLimit() {
        List<Map<String, Object>> rows = runSelect("SELECT NAME, COUNT(*) AS CNT FROM USERS JOIN PROFILES ON USERS.ID = PROFILES.USER_ID WHERE AGE > 10 GROUP BY NAME ORDER BY NAME LIMIT 3");
        assertEquals(3, rows.size(), "combined pipeline must return three groups");
        assertEquals("User3", rows.get(0).get("NAME"), "first group sorted by name must be User3");
        for (Map<String, Object> row : rows) {
            assertEquals(1L, ((Number) row.get("CNT")).longValue(), "each group must aggregate exactly one row");
        }
    }
}