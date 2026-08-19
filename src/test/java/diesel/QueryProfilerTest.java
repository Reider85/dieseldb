package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the per-query phase profiler (Prompt 18): the breakdown counters,
 * the slow-query threshold behaviour, the JMX MBean exposure and that the
 * ORDER BY sort phase is only charged to queries that actually sort.
 */
public class QueryProfilerTest {

    private Database database;
    private QueryProfiler profiler;

    @BeforeEach
    void setUp() {
        database = new Database();
        dropTable();
        database.executeQuery("CREATE TABLE PROFILER_TEST (ID LONG PRIMARY KEY, NAME STRING, AGE INTEGER)", null);
        profiler = QueryProfiler.getInstance();
        profiler.resetForTest();
        profiler.setSlowThresholdMsForTest(QueryProfiler.DEFAULT_SLOW_THRESHOLD_MS);
    }

    @AfterEach
    void tearDown() {
        profiler.resetForTest();
        profiler.setSlowThresholdMsForTest(QueryProfiler.DEFAULT_SLOW_THRESHOLD_MS);
        dropTable();
    }

    private void dropTable() {
        try {
            database.dropTable("PROFILER_TEST");
        } catch (TableNotFoundException ignored) {
            // Ignore: table may not have been created
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> rows(Object result) {
        return (List<Map<String, Object>>) result;
    }

    @Test
    void everyQueryRecordsCountersAndBreakdown() {
        database.executeQuery("INSERT INTO PROFILER_TEST (ID, NAME, AGE) VALUES (1, 'alpha', 20)", null);
        Object result = database.executeQuery("SELECT ID, NAME FROM PROFILER_TEST WHERE AGE > 10", null);

        assertNotNull(result);
        assertEquals(1, rows(result).size());
        assertEquals(2, profiler.getTotalQueries(), "INSERT and SELECT must both be profiled");
        assertEquals(0, profiler.getSlowQueryCount(), "fast queries must not trip the default threshold");
        assertTrue(profiler.getTotalExecuteMs() >= 0);
        assertTrue(profiler.getTotalParseMs() >= 0);
        assertTrue(profiler.getLastTotalMs() >= 0, "the last breakdown must reflect the SELECT");
        assertEquals("", profiler.getLastSlowQuery(), "no slow query must be remembered at the default threshold");
    }

    @Test
    void thresholdZeroMarksEveryQuerySlow() {
        profiler.setSlowThresholdMsForTest(0);

        database.executeQuery("INSERT INTO PROFILER_TEST (ID, NAME, AGE) VALUES (1, 'alpha', 20)", null);
        Object result = database.executeQuery("SELECT ID FROM PROFILER_TEST", null);

        assertNotNull(result);
        assertEquals(2, profiler.getSlowQueryCount(), "every query is slow at threshold 0");
        assertTrue(profiler.getLastSlowQuery().contains("SELECT ID FROM PROFILER_TEST"),
                "the most recent slow query must be recorded");
        assertEquals(profiler.getLastTotalMs(), profiler.getLastSlowTotalMs());
    }

    @Test
    void orderBySortPhaseIsMeasuredOnlyWhenSorting() {
        Table table = database.getTable("PROFILER_TEST");
        for (int i = 1; i <= 20000; i++) {
            Map<String, Object> row = new java.util.HashMap<>();
            row.put("ID", (long) i);
            row.put("NAME", "user" + i);
            row.put("AGE", i % 100);
            table.addRow(row);
        }

        profiler.resetForTest();
        database.executeQuery("SELECT ID, NAME FROM PROFILER_TEST WHERE AGE > 10", null);
        assertEquals(0, profiler.getLastSortMs(), "a query without ORDER BY must not charge the sort phase");

        database.executeQuery("SELECT ID, NAME FROM PROFILER_TEST WHERE AGE > 10 ORDER BY NAME", null);
        assertTrue(profiler.getLastSortMs() > 0,
                "sorting 20000 rows must charge a measurable sort phase");
        assertTrue(profiler.getTotalSortMs() > 0);
        assertEquals(0, profiler.getSlowQueryCount(),
                "a 20000-row scan plus sort stays well under the 1000ms threshold");
    }

    @Test
    void parseTimeIsZeroOnQueryCacheHit() {
        database.executeQuery("INSERT INTO PROFILER_TEST (ID, NAME, AGE) VALUES (1, 'alpha', 20)", null);

        Object first = database.executeQuery("SELECT * FROM PROFILER_TEST WHERE NAME = 'alpha'", null);
        Object second = database.executeQuery("SELECT * FROM PROFILER_TEST WHERE NAME = 'alpha'", null);

        assertEquals(1, rows(first).size());
        assertEquals(first, second);
        assertEquals(1, database.getQueryCache().getHitCount(), "the repeated SELECT must hit the cache");
        assertEquals(0, profiler.getLastParseMs(),
                "a cached plan is not re-parsed, so the last parse phase must be zero");
    }

    @Test
    void jmxMBeanExposesLiveMetrics() throws Exception {
        profiler.setSlowThresholdMsForTest(0);
        database.executeQuery("SELECT ID FROM PROFILER_TEST", null);

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName(QueryProfiler.OBJECT_NAME);
        assertTrue(server.isRegistered(name), "the profiler MBean must be registered on the platform MBean server");

        assertEquals(1L, server.getAttribute(name, "TotalQueries"));
        assertEquals(1L, server.getAttribute(name, "SlowQueryCount"));
        assertEquals("SELECT ID FROM PROFILER_TEST", server.getAttribute(name, "LastSlowQuery"));
        assertEquals((long) profiler.getTotalExecuteMs(), server.getAttribute(name, "TotalExecuteMs"));
        assertTrue((long) server.getAttribute(name, "MaxTotalMs") >= 0);
        assertTrue((long) server.getAttribute(name, "ThresholdMs") >= 0);
    }

    @Test
    void thresholdSettableToZero() {
        profiler.setSlowThresholdMsForTest(0);
        assertEquals(0, profiler.getSlowThresholdMs());

        database.executeQuery("SELECT ID FROM PROFILER_TEST", null);
        assertEquals(1, profiler.getSlowQueryCount(), "threshold 0 makes the SELECT slow");
    }
}
