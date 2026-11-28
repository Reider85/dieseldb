package diesel.optimizer;

import diesel.Database;
import diesel.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("query-full")
class StatsTest {
    @TempDir
    Path tempDir;
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
            database = null;
        }
    }

    private Table createTable(String name) {
        Map<String, Class<?>> types = new HashMap<>();
        types.put("ID", Long.class);
        types.put("NAME", String.class);
        types.put("SCORE", Integer.class);
        database.createTable(name, List.of("ID", "NAME", "SCORE"), types, null);
        return database.getTable(name);
    }

    @Test
    void analyzeCollectsPerColumnStatistics() {
        Table table = createTable("STATS_BASIC");
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 1; i <= 12; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", (long) i);
            row.put("NAME", i <= 5 ? "A" : i <= 8 ? "B" : i <= 10 ? "C" : null);
            row.put("SCORE", i <= 2 ? null : i % 5);
            rows.add(row);
        }
        table.bulkInsert(rows);

        Object result = database.executeQuery("ANALYZE TABLE STATS_BASIC", null);
        assertTrue(result instanceof String);
        assertTrue(((String) result).contains("3 columns"), (String) result);

        Map<String, ColumnStatistics> stats = table.getColumnStatistics();
        assertEquals(3, stats.size());
        ColumnStatistics name = stats.get("NAME");
        assertEquals(12, name.getRowCount());
        assertEquals(2, name.getNullCount());
        assertEquals(2.0 / 12.0, name.getNullFraction(), 1e-9);
        assertEquals(3, name.getDistinctCount());
        assertEquals("A", name.getMostCommonValues().get(0).getValue());
        assertEquals(5, name.getMostCommonValues().get(0).getCount());
        assertEquals(5.0 / 12.0, name.getMostCommonValues().get(0).getFrequency(), 1e-9);
        assertEquals(4, name.getHistogramBounds().size());
        ColumnStatistics score = stats.get("SCORE");
        assertEquals(2, score.getNullCount());
        assertEquals(5, score.getDistinctCount());

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> statRows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM DIESEL_STATS WHERE TABLE_NAME = 'STATS_BASIC'", null);
        assertEquals(3, statRows.size());
        ColumnStatistics restored = ColumnStatistics.fromRow(statRows.get(0));
        assertEquals(12, restored.getRowCount());
        assertEquals(stats.get(restored.getColumnName()).getDistinctCount(), restored.getDistinctCount());
    }

    @Test
    @Timeout(180)
    void analyzeOneMillionRowsCompletesWithinFiveSeconds() {
        Table table = createTable("STATS_MILLION");
        for (int batch = 0; batch < 10; batch++) {
            List<Map<String, Object>> rows = new ArrayList<>(100_000);
            for (int i = batch * 100_000; i < (batch + 1) * 100_000; i++) {
                rows.add(Map.of("ID", (long) i, "NAME", "N" + (i % 1000), "SCORE", i % 100));
            }
            table.bulkInsert(rows);
        }

        long start = System.nanoTime();
        database.executeQuery("ANALYZE TABLE STATS_MILLION", null);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMillis < 5000, "ANALYZE on 1M rows took " + elapsedMillis + " ms");

        ColumnStatistics name = table.getColumnStatistics().get("NAME");
        assertEquals(1_000_000, name.getRowCount());
        assertEquals(1000, name.getDistinctCount());
        assertEquals(10, name.getMostCommonValues().size());
        assertTrue(name.getHistogramBounds().size() <= 101);
    }

    @Test
    void analyzeAcceptsTableNameWithoutTableKeyword() {
        Table table = createTable("STATS_SHORT");
        table.bulkInsert(List.of(Map.of("ID", 1L, "NAME", "A", "SCORE", 1)));
        Object result = database.executeQuery("ANALYZE STATS_SHORT", null);
        assertTrue(((String) result).contains("1 rows"), (String) result);
    }
}
