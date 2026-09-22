package diesel;

import diesel.storage.avro.AvroQueryConfig;
import diesel.storage.avro.AvroQueryExecutor;
import diesel.storage.avro.AvroRowStorage;
import diesel.storage.avro.AvroStatistics;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link AvroQueryExecutor}, {@link AvroQueryConfig} and
 * {@link AvroStatistics} (Prompt 91).
 */
@Tag("storage")
@StorageType("avro")
class AvroQueryExecutorTest {

    @TempDir
    Path tempDir;

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE", "ACTIVE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("ACTIVE", Boolean.class);
        return t;
    }

    private static Map<String, Object> row(Object... vals) {
        Map<String, Object> r = new LinkedHashMap<>();
        List<String> c = cols();
        for (int i = 0; i < vals.length; i++) {
            r.put(c.get(i), vals[i]);
        }
        return r;
    }

    private AvroRowStorage createStorage(String tableName) {
        AvroRowStorage s = new AvroRowStorage(tableName, cols(), types());
        s.setDataDir(tempDir.toString());
        return s;
    }

    private void insertRows(AvroRowStorage s, int count) {
        for (int i = 1; i <= count; i++) {
            s.insert(row((long) i, "User" + i, 20 + (i % 50), i % 2 == 0));
        }
        s.saveToFile(s.getTableName());
    }

    /** Helper: builds an equality predicate on a named column. */
    private static Predicate<GenericRecord> eqPredicate(String col, Object value) {
        return rec -> {
            Object v = rec.get(col);
            // Convert Avro Utf8 to String for comparison
            if (v instanceof org.apache.avro.util.Utf8 utf8) v = utf8.toString();
            if (v == null && value == null) return true;
            if (v == null || value == null) return false;
            if (v instanceof Number && value instanceof Number) {
                return ((Number) v).doubleValue() == ((Number) value).doubleValue();
            }
            return v.equals(value);
        };
    }

    @BeforeEach
    void resetCaches() {
        AvroQueryExecutor.resetCache();
    }

    // ─── Config resolution ──────────────────────────────────────────

    @Test
    void testConfigDefaults() {
        AvroQueryConfig config = AvroQueryConfig.resolve();
        assertTrue(config.pushdownEnabled());
        assertTrue(config.projectionEnabled());
        assertEquals(10000, config.parallelThreshold());
    }

    @Test
    void testConfigSyspropOverride() {
        try {
            System.setProperty("avro.query.pushdown.enabled", "false");
            System.setProperty("avro.query.projection.enabled", "off");
            System.setProperty("avro.query.parallel.threshold", "500");
            AvroQueryConfig config = AvroQueryConfig.resolve();
            assertFalse(config.pushdownEnabled());
            assertFalse(config.projectionEnabled());
            assertEquals(500, config.parallelThreshold());
        } finally {
            System.clearProperty("avro.query.pushdown.enabled");
            System.clearProperty("avro.query.projection.enabled");
            System.clearProperty("avro.query.parallel.threshold");
        }
    }

    @Test
    void testConfigDisabled() {
        try {
            System.setProperty("avro.query.pushdown.enabled", "false");
            AvroQueryConfig config = AvroQueryConfig.resolve();
            assertFalse(config.pushdownEnabled());
        } finally {
            System.clearProperty("avro.query.pushdown.enabled");
        }
    }

    // ─── Full scan ──────────────────────────────────────────────────

    @Test
    void testFullScanReturnsAllRows() throws Exception {
        AvroRowStorage s = createStorage("fullscan_test");
        insertRows(s, 50);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, null, new LinkedHashSet<>(cols()), cols(), types(), null);

        assertEquals(50, result.rows().size());
        assertEquals("User1", result.rows().get(0).get("NAME"));
        assertEquals(50L, result.rows().get(49).get("ID"));
    }

    // ─── Column projection ──────────────────────────────────────────

    @Test
    void testProjectionReturnsOnlyRequestedColumns() throws Exception {
        AvroRowStorage s = createStorage("proj_test");
        insertRows(s, 10);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        Set<String> projected = new LinkedHashSet<>(List.of("ID", "NAME"));
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, null, projected, cols(), types(), null);

        assertEquals(10, result.rows().size());
        for (Map<String, Object> r : result.rows()) {
            assertEquals(2, r.size());
            assertNotNull(r.get("ID"));
            assertNotNull(r.get("NAME"));
            assertNull(r.get("AGE"));
            assertNull(r.get("ACTIVE"));
        }
    }

    @Test
    void testProjectionStarReturnsAllColumns() throws Exception {
        AvroRowStorage s = createStorage("proj_star_test");
        insertRows(s, 5);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, null, new LinkedHashSet<>(cols()), cols(), types(), null);

        assertEquals(5, result.rows().size());
        for (Map<String, Object> r : result.rows()) {
            assertEquals(4, r.size());
        }
    }

    // ─── Predicate pushdown: EQUALS ─────────────────────────────────

    @Test
    void testPushdownEquals() throws Exception {
        AvroRowStorage s = createStorage("eq_test");
        insertRows(s, 100);

        Predicate<GenericRecord> predicate = eqPredicate("AGE", 30);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, predicate, new LinkedHashSet<>(cols()), cols(), types(), null);

        assertFalse(result.rows().isEmpty());
        for (Map<String, Object> r : result.rows()) {
            assertEquals(30, r.get("AGE"));
        }
    }

    @Test
    void testPushdownEqualsNull() throws Exception {
        AvroRowStorage s = createStorage("eq_null_test");
        s.insert(row(1L, "Alice", 30, true));
        s.insert(row(2L, null, 25, false));
        s.saveToFile(s.getTableName());

        Predicate<GenericRecord> predicate = rec -> rec.get("NAME") == null;

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, predicate, new LinkedHashSet<>(cols()), cols(), types(), null);

        assertEquals(1, result.rows().size());
        assertNull(result.rows().get(0).get("NAME"));
    }

    // ─── Predicate pushdown: range ──────────────────────────────────

    @Test
    void testPushdownLessThan() throws Exception {
        AvroRowStorage s = createStorage("lt_test");
        insertRows(s, 100);

        Predicate<GenericRecord> predicate = rec -> {
            Object val = rec.get("AGE");
            return val instanceof Number n && n.intValue() < 25;
        };

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, predicate, new LinkedHashSet<>(cols()), cols(), types(), null);

        assertFalse(result.rows().isEmpty());
        for (Map<String, Object> r : result.rows()) {
            assertTrue(((Number) r.get("AGE")).intValue() < 25);
        }
    }

    @Test
    void testPushdownGreaterThan() throws Exception {
        AvroRowStorage s = createStorage("gt_test");
        insertRows(s, 100);

        Predicate<GenericRecord> predicate = rec -> {
            Object val = rec.get("AGE");
            return val instanceof Number n && n.intValue() > 40;
        };

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, predicate, new LinkedHashSet<>(cols()), cols(), types(), null);

        assertFalse(result.rows().isEmpty());
        for (Map<String, Object> r : result.rows()) {
            assertTrue(((Number) r.get("AGE")).intValue() > 40);
        }
    }

    // ─── Predicate + projection combined ─────────────────────────────

    @Test
    void testPushdownWithProjection() throws Exception {
        AvroRowStorage s = createStorage("combined_test");
        insertRows(s, 100);

        Predicate<GenericRecord> predicate = eqPredicate("ID", 10L);
        // Projection must include columns used by predicate + result columns
        Set<String> projected = new LinkedHashSet<>(List.of("ID", "NAME"));

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, predicate, projected, cols(), types(), null);

        assertFalse(result.rows().isEmpty());
        assertEquals(1, result.rows().size());
        assertEquals(10L, result.rows().get(0).get("ID"));
    }

    // ─── LIMIT ──────────────────────────────────────────────────────

    @Test
    void testLimitApplied() throws Exception {
        AvroRowStorage s = createStorage("limit_test");
        insertRows(s, 100);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, null, new LinkedHashSet<>(cols()), cols(), types(), 5);

        assertEquals(5, result.rows().size());
    }

    @Test
    void testLimitWithPushdown() throws Exception {
        AvroRowStorage s = createStorage("limit_push_test");
        insertRows(s, 100);

        Predicate<GenericRecord> predicate = eqPredicate("ACTIVE", true);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, predicate, new LinkedHashSet<>(cols()), cols(), types(), 3);

        assertTrue(result.rows().size() <= 3);
    }

    // ─── Empty table ────────────────────────────────────────────────

    @Test
    void testEmptyTable() throws Exception {
        AvroRowStorage s = createStorage("empty_test");

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, null, new LinkedHashSet<>(cols()), cols(), types(), null);

        assertEquals(0, result.rows().size());
    }

    // ─── Statistics ─────────────────────────────────────────────────

    @Test
    void testStatisticsCollected() throws Exception {
        AvroRowStorage s = createStorage("stats_test");
        insertRows(s, 25);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, null, new LinkedHashSet<>(cols()), cols(), types(), null);

        assertNotNull(result.statistics());
        assertTrue(result.statistics().getRowCount() > 0);
        assertTrue(result.statistics().getFileSize() > 0);
    }

    @Test
    void testStatisticsEstimateScanCost() throws Exception {
        AvroRowStorage s = new AvroRowStorage("cost_test", cols(), types());
        s.setDataDir(tempDir.toString());
        insertRows(s, 100);

        File avroFile = s.resolveAvroFile();
        assertNotNull(avroFile);

        AvroStatistics stats = AvroStatistics.collectFromFile(avroFile, cols(), types());
        assertTrue(stats.estimateScanCost() > 0);
    }

    @Test
    void testStatisticsFilterSelectivity() throws Exception {
        AvroRowStorage s = new AvroRowStorage("sel_test", cols(), types());
        s.setDataDir(tempDir.toString());
        insertRows(s, 50);

        File avroFile = s.resolveAvroFile();
        assertNotNull(avroFile);

        AvroStatistics stats = AvroStatistics.collectFromFile(avroFile, cols(), types());
        stats.collectColumnStatistics(avroFile, cols(), types());
        double sel = stats.estimateFilterSelectivity("AGE");
        assertTrue(sel > 0.0 && sel <= 1.0);
    }

    // ─── scanWithProjection convenience ──────────────────────────────

    @Test
    void testScanWithProjection() throws Exception {
        AvroRowStorage s = createStorage("scanproj_test");
        insertRows(s, 10);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        List<Map<String, Object>> result = executor.scanWithProjection(s, List.of("ID"));

        assertEquals(10, result.size());
        for (Map<String, Object> r : result) {
            assertEquals(1, r.size());
            assertNotNull(r.get("ID"));
        }
    }

    // ─── No predicate pushdown (null predicate) ─────────────────────

    @Test
    void testNullPredicateReturnsAllRows() throws Exception {
        AvroRowStorage s = createStorage("null_pred_test");
        insertRows(s, 20);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, null, new LinkedHashSet<>(cols()), cols(), types(), null);

        assertEquals(20, result.rows().size());
    }

    // ─── Projection-only (no predicate) ──────────────────────────────

    @Test
    void testProjectionOnlyWithoutPredicate() throws Exception {
        AvroRowStorage s = createStorage("proj_only_test");
        insertRows(s, 30);

        AvroQueryExecutor executor = new AvroQueryExecutor();
        Set<String> projected = new LinkedHashSet<>(List.of("ID"));
        AvroQueryExecutor.QueryResult result = executor.executeQuery(
                s, null, projected, cols(), types(), null);

        assertEquals(30, result.rows().size());
        for (Map<String, Object> r : result.rows()) {
            assertEquals(1, r.size());
            assertNotNull(r.get("ID"));
        }
    }
}
