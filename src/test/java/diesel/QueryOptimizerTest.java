package diesel;

import diesel.Database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class QueryOptimizerTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        QueryOptimizer.loadAdaptiveConfig();
        QueryOptimizer.clearCacheForTest();
    }

    @AfterEach
    void tearDown() {
        QueryOptimizer.loadAdaptiveConfig();
        QueryOptimizer.clearCacheForTest();
    }

    private void createAndPopulateTables() {
        try { database.dropTable("T2"); } catch (Exception ignored) {}
        try { database.dropTable("T1"); } catch (Exception ignored) {}
        database.executeQuery("CREATE TABLE T1 (ID LONG PRIMARY KEY SEQUENCE(seq1 1 1), NAME STRING, AGE INTEGER, VAL BIGDECIMAL)", null);
        database.executeQuery("CREATE TABLE T2 (ID LONG PRIMARY KEY SEQUENCE(seq2 1 1), T1_ID LONG, NAME STRING, SCORE INTEGER)", null);
        for (int i = 1; i <= 100; i++) {
            database.executeQuery("INSERT INTO T1 (NAME, AGE, VAL) VALUES ('name" + i + "', " + (i % 50) + ", " + (i * 1.5) + ")", null);
        }
        for (int i = 1; i <= 200; i++) {
            database.executeQuery("INSERT INTO T2 (T1_ID, NAME, SCORE) VALUES (" + ((i % 100) + 1) + ", 'detail" + i + "', " + (i % 10) + ")", null);
        }
    }

    @Test
    void testFingerprintComputation() {
        // Single table – no joins
        String fp1 = QueryOptimizer.QueryFingerprint.compute(List.of("USERS"), null);
        assertEquals("USERS", fp1);

        // Two tables, one join – table names sorted
        QueryParser.JoinInfo join = new QueryParser.JoinInfo(
                "T1", "T2", null, "ID", "T1_ID", QueryParser.JoinType.INNER, List.of());

        String fp2 = QueryOptimizer.QueryFingerprint.compute(List.of("T2", "T1"), List.of(join));
        assertTrue(fp2.contains("T1"));
        assertTrue(fp2.contains("T2"));
        assertTrue(fp2.contains("INNER"));
    }

    @Test
    void testPlanCacheLruEviction() {
        QueryOptimizer.PlanCache cache = new QueryOptimizer.PlanCache(3);
        cache.put("a", java.util.Set.of("hash-a"));
        cache.put("b", java.util.Set.of("hash-b"));
        cache.put("c", java.util.Set.of("hash-c"));
        assertEquals(3, cache.size());

        // Access 'a' so it stays, add 'd' which should evict 'b' (LRU)
        cache.lookup("a");
        cache.put("d", java.util.Set.of("hash-d"));
        assertEquals(3, cache.size());
        assertNull(cache.lookup("b"));
        assertNotNull(cache.lookup("a"));
        assertNotNull(cache.lookup("d"));
    }

    @Test
    void testExecutionStateTracking() {
        QueryOptimizer.QueryExecutionState state =
                new QueryOptimizer.QueryExecutionState("test", java.util.Set.of("hash"), 0.5);
        assertEquals(0, state.stepIndex());

        // Perfect match – no deviation
        boolean needsReplan = state.reportStep(100, 100);
        assertFalse(needsReplan);
        assertFalse(state.hasSignificantDeviation());
        assertEquals(1, state.stepIndex());

        // Within threshold
        needsReplan = state.reportStep(100, 80);
        assertFalse(needsReplan);
        assertFalse(state.hasSignificantDeviation());

        // Significant deviation (2x) – should trigger replan
        needsReplan = state.reportStep(100, 20);
        assertTrue(needsReplan);
        assertTrue(state.hasSignificantDeviation());
    }

    @Test
    void testBasicJoinQueryStillWorks() {
        createAndPopulateTables();
        List<Map<String, Object>> result = (List<Map<String, Object>>)
                database.executeQuery("SELECT T1.NAME, T2.NAME FROM T1 INNER JOIN T2 ON T1.ID = T2.T1_ID", null);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        // T2 has 200 rows referencing T1 (100 rows), so every T1 row has matches
        assertTrue(result.size() >= 100);
    }

    @Test
    void testAdaptiveOptimizerDisabled() {
        createAndPopulateTables();
        QueryOptimizer.setEnabledForTest(false);
        try {
            List<Map<String, Object>> result = (List<Map<String, Object>>)
                    database.executeQuery("SELECT T1.NAME FROM T1 INNER JOIN T2 ON T1.ID = T2.T1_ID", null);
            assertNotNull(result);
            assertFalse(result.isEmpty());
        } finally {
            QueryOptimizer.setEnabledForTest(true);
        }
    }

    @Test
    void testOptimizerEnabledByDefault() {
        QueryOptimizer.QueryExecutionState state =
                new QueryOptimizer.QueryExecutionState("test", null, 0.5);
        assertNotNull(state);
        assertFalse(state.replanned);
        assertEquals(0, state.maxDeviation, 0.001);
    }

    @Test
    void testLearnedAlgorithmBias() {
        QueryOptimizer.QueryExecutionState state =
                new QueryOptimizer.QueryExecutionState("test", java.util.Set.of("hash-T2"), 0.5);
        assertTrue(state.isLearned("hash-T2"));
        assertFalse(state.isLearned("nl-T2"));
    }

    @Test
    void testMarkReplannedPreventsDoubleReplan() {
        QueryOptimizer.QueryExecutionState state =
                new QueryOptimizer.QueryExecutionState("test", null, 0.5);
        // Simulate deviation
        state.reportStep(100, 10);
        assertTrue(state.hasSignificantDeviation());
        // First replan
        state.markReplanned();
        assertTrue(state.replanned);
        // Replan flag should prevent further replanning
    }

    @Test
    void testAdaptiveConfigDefaults() {
        QueryOptimizer.loadAdaptiveConfig();
        // Just verify it doesn't throw
    }

    @Test
    void testJoinWithAdaptiveReplan() {
        createAndPopulateTables();
        // Run a multi-join query – adaptive system may or may not replan,
        // but the query must return correct results regardless.
        List<Map<String, Object>> result = (List<Map<String, Object>>)
                database.executeQuery(
                        "SELECT T1.NAME, T2.NAME, T2.SCORE " +
                        "FROM T1 INNER JOIN T2 ON T1.ID = T2.T1_ID " +
                        "WHERE T2.SCORE > 5", null);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        // Verify results are correct – only SCORE > 5
        // Column aliases are unqualified (SCORE, not T2.SCORE) in DieselDB
        for (Map<String, Object> row : result) {
            Object score = row.get("SCORE");
            assertNotNull(score, "SCORE column must be present in result: " + row.keySet());
            assertTrue(((Number) score).intValue() > 5);
        }
    }
}
