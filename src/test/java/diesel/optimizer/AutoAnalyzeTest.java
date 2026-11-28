package diesel.optimizer;

import diesel.Database;
import diesel.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("query-full")
@Isolated
class AutoAnalyzeTest {
    @TempDir
    Path tempDir;
    private Database database;

    @BeforeEach
    void setUp() {
        StatisticsCollector.setThresholdForTest(0.2);
        database = new Database(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
            database = null;
        }
        StatisticsCollector.setThresholdForTest(null);
    }

    private Table createTableWithRows(String name, int count) {
        Map<String, Class<?>> types = new HashMap<>();
        types.put("ID", Long.class);
        types.put("NAME", String.class);
        types.put("SCORE", Integer.class);
        database.createTable(name, List.of("ID", "NAME", "SCORE"), types, null);
        Table table = database.getTable(name);
        List<Map<String, Object>> rows = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            rows.add(Map.of("ID", (long) i, "NAME", "N" + i, "SCORE", i % 10));
        }
        table.bulkInsert(rows);
        return table;
    }

    private void insertRows(String table, int from, int to) {
        for (int i = from; i <= to; i++) {
            database.executeQuery("INSERT INTO " + table + " (ID, NAME, SCORE) VALUES (" + i + ", 'X', 1)", null);
        }
    }

    @Test
    void deleteAndInsertOfThirtyPercentTriggersAutoAnalyze() {
        Table table = createTableWithRows("AUTO_STATS_TRIGGER", 1000);
        database.executeQuery("ANALYZE TABLE AUTO_STATS_TRIGGER", null);
        long baseline = table.getColumnStatistics().get("ID").getAnalyzedAtMillis();
        assertEquals(0, table.getRowsChangedSinceAnalyze());

        database.executeQuery("DELETE FROM AUTO_STATS_TRIGGER WHERE ID < 300", null);
        insertRows("AUTO_STATS_TRIGGER", 1001, 1300);

        await().atMost(10, SECONDS).until(() -> table.getColumnStatistics().get("ID").getAnalyzedAtMillis() > baseline);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> statRows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM DIESEL_STATS WHERE TABLE_NAME = 'AUTO_STATS_TRIGGER'", null);
        assertEquals(3, statRows.size());
    }

    @Test
    void changesBelowThresholdDoNotTriggerAutoAnalyze() {
        Table table = createTableWithRows("AUTO_STATS_BELOW", 1000);
        database.executeQuery("ANALYZE TABLE AUTO_STATS_BELOW", null);
        long baseline = table.getColumnStatistics().get("ID").getAnalyzedAtMillis();

        insertRows("AUTO_STATS_BELOW", 1001, 1050);

        assertEquals(50, table.getRowsChangedSinceAnalyze());
        assertEquals(baseline, table.getColumnStatistics().get("ID").getAnalyzedAtMillis());
    }

    @Test
    void zeroThresholdDisablesAutoAnalyze() {
        StatisticsCollector.setThresholdForTest(0.0);
        Table table = createTableWithRows("AUTO_STATS_DISABLED", 1000);
        database.executeQuery("ANALYZE TABLE AUTO_STATS_DISABLED", null);

        insertRows("AUTO_STATS_DISABLED", 1001, 1300);

        assertEquals(300, table.getRowsChangedSinceAnalyze());
    }
}
