package diesel;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fast tests for prompt4.md step 3 (MVCC Vacuum Manager): VACUUM SQL
 * parsing/dispatch, dead-row reclamation (tombstones and aborted inserts),
 * JMX metrics and the auto-vacuum schedule. The 1M-row acceptance workload
 * lives in {@link VacuumTest}.
 */
@Tag("smoke")
@Tag("concurrency")
class VacuumManagerTest {

    @TempDir
    static Path tempDir;

    private static int tableSeq = 0;

    @AfterEach
    void clearProperties() {
        System.clearProperty(VacuumManager.INTERVAL_PROPERTY);
        System.clearProperty(VacuumManager.BATCH_SIZE_PROPERTY);
    }

    // ─── Parsing ────────────────────────────────────────────────────

    @Test
    void vacuumQueryParsesAllSupportedForms() {
        Database db = new Database();
        QueryParser parser = new QueryParser();

        Query<?> all = parser.parse("VACUUM", db);
        VacuumQuery allVacuum = assertInstanceOf(VacuumQuery.class, all);
        assertNull(allVacuum.getTableName());

        Query<?> bare = parser.parse("VACUUM VAC_PARSE_A", db);
        assertEquals("VAC_PARSE_A", ((VacuumQuery) bare).getTableName());

        Query<?> keyword = parser.parse("vacuum table vac_parse_b", db);
        assertEquals("VAC_PARSE_B", ((VacuumQuery) keyword).getTableName());

        Query<?> semicolon = parser.parse("VACUUM VAC_PARSE_C;", db);
        assertEquals("VAC_PARSE_C", ((VacuumQuery) semicolon).getTableName());
    }

    @Test
    void vacuumQueryRejectsMalformedInput() {
        Database db = new Database();
        QueryParser parser = new QueryParser();

        assertThrows(IllegalArgumentException.class, () -> parser.parse("VACUUM TABLE", db));
        assertThrows(IllegalArgumentException.class, () -> parser.parse("VACUUM a b", db));
        assertThrows(IllegalArgumentException.class, () -> parser.parse("VACUUM (t)", db));
    }

    // ─── Reclamation ────────────────────────────────────────────────

    @Test
    void vacuumRemovesTombstonedRowsAndReportsMetricsViaJmx() throws Exception {
        Database db = newDatabase();
        String table = nextTableName("VAC_TOMB");
        db.executeQuery("CREATE TABLE " + table
                + " (ID LONG PRIMARY KEY SEQUENCE(" + table.toLowerCase() + "_seq 1 1), VAL STRING)", null);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("VAL", "v" + i);
            rows.add(row);
        }
        db.getTable(table).bulkInsert(rows);

        db.executeQuery("DELETE FROM " + table + " WHERE ID < 3", null);
        assertEquals(20, db.getTable(table).getRawRowCount(),
                "tombstones keep the rows physically present before VACUUM");
        assertEquals(2, db.getTable(table).getDeletedCount());

        String result = (String) db.executeQuery("VACUUM " + table, null);
        assertTrue(result.startsWith("VACUUM " + table + ": 2 dead tuples removed"),
                "unexpected VACUUM result: " + result);

        assertEquals(18, db.getTable(table).getRawRowCount(),
                "VACUUM must physically remove the tombstoned rows");
        assertEquals(0, db.getTable(table).getDeletedCount());

        ObjectName mbean = db.getVacuumManager().getObjectName();
        assertNotNull(mbean, "VACUUM must register the metrics MBean");
        Object removed = ManagementFactory.getPlatformMBeanServer()
                .getAttribute(mbean, VacuumManager.ATTR_DEAD_TUPLES);
        assertEquals(2L, removed, "JMX vacuum.dead_tuples_removed must count the reclaimed rows");
        Object runs = ManagementFactory.getPlatformMBeanServer()
                .getAttribute(mbean, VacuumManager.ATTR_RUNS);
        assertEquals(1L, runs, "JMX vacuum.runs must count the completed run");
        Object duration = ManagementFactory.getPlatformMBeanServer()
                .getAttribute(mbean, VacuumManager.ATTR_DURATION_MS);
        assertTrue(((Long) duration) >= 0, "JMX vacuum.duration.ms must be readable");

        // Live rows stay queryable after the compaction.
        Object remaining = db.executeQuery("SELECT ID FROM " + table, null);
        assertInstanceOf(List.class, remaining);
        assertEquals(18, ((List<?>) remaining).size());
    }

    @Test
    void vacuumRemovesAbortedInsertRows() {
        Database db = newDatabase();
        String table = nextTableName("VAC_ABORT");
        db.executeQuery("CREATE TABLE " + table
                + " (ID LONG PRIMARY KEY SEQUENCE(" + table.toLowerCase() + "_seq 1 1), VAL STRING)", null);
        for (int i = 0; i < 3; i++) {
            db.executeQuery("INSERT INTO " + table + " (VAL) VALUES ('live" + i + "')", null);
        }

        String begin = (String) db.executeQuery("BEGIN TRANSACTION", null);
        UUID txId = UUID.fromString(begin.split(": ")[1]);
        db.executeQuery("INSERT INTO " + table + " (VAL) VALUES ('doomed')", txId);
        db.executeQuery("ROLLBACK", txId);

        Table target = db.getTable(table);
        assertEquals(4, target.getRawRowCount(),
                "the rolled-back insert stays physically present until VACUUM");

        String result = (String) db.executeQuery("VACUUM TABLE " + table, null);
        assertTrue(result.contains("1 dead tuples removed"), "unexpected VACUUM result: " + result);
        assertEquals(3, target.getRawRowCount(), "VACUUM must reclaim the aborted row version");
        assertEquals(0, target.getDeletedCount());

        Object visible = db.executeQuery("SELECT VAL FROM " + table, null);
        assertEquals(3, ((List<?>) visible).size(), "live rows are unaffected by the vacuum");
    }

    @Test
    void bareVacuumCoversEveryTable() {
        Database db = newDatabase();
        String first = nextTableName("VAC_ALL_A");
        String second = nextTableName("VAC_ALL_B");
        for (String name : new String[]{first, second}) {
            db.executeQuery("CREATE TABLE " + name
                    + " (ID LONG PRIMARY KEY SEQUENCE(" + name.toLowerCase() + "_seq 1 1), VAL STRING)", null);
            List<Map<String, Object>> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Map<String, Object> row = new HashMap<>();
                row.put("VAL", "v" + i);
                rows.add(row);
            }
            db.getTable(name).bulkInsert(rows);
            db.executeQuery("DELETE FROM " + name + " WHERE ID < 3", null);
        }

        String result = (String) db.executeQuery("VACUUM", null);
        assertTrue(result.startsWith("VACUUM: 2 table(s), 4 dead tuples removed"),
                "unexpected VACUUM result: " + result);
        assertEquals(8, db.getTable(first).getRawRowCount());
        assertEquals(8, db.getTable(second).getRawRowCount());
    }

    @Test
    void vacuumOfUnknownTableFails() {
        Database db = newDatabase();
        assertThrows(TableNotFoundException.class, () -> db.executeQuery("VACUUM NO_SUCH_TABLE_XYZ", null));
    }

    // ─── Auto-vacuum schedule ───────────────────────────────────────

    @Test
    void autoVacuumRunsOnScheduleAndStops() throws Exception {
        System.setProperty(VacuumManager.INTERVAL_PROPERTY, "40");
        Database db = newDatabase();
        String table = nextTableName("VAC_AUTO");
        db.executeQuery("CREATE TABLE " + table
                + " (ID LONG PRIMARY KEY SEQUENCE(" + table.toLowerCase() + "_seq 1 1), VAL STRING)", null);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("VAL", "v" + i);
            rows.add(row);
        }
        db.getTable(table).bulkInsert(rows);
        db.executeQuery("DELETE FROM " + table + " WHERE ID < 3", null);

        VacuumManager manager = db.getVacuumManager();
        assertEquals(40, manager.getIntervalMs(), "the system property must override the schedule");
        manager.startAutoVacuum();
        manager.startAutoVacuum(); // second call is a no-op
        assertTrue(manager.isAutoVacuumRunning());

        long deadline = System.currentTimeMillis() + 5_000;
        while (manager.getVacuumRuns() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(manager.getVacuumRuns() >= 1,
                "auto-vacuum must run at least once within 5s of a 40ms schedule");
        assertEquals(8, db.getTable(table).getRawRowCount(),
                "the scheduled vacuum must have reclaimed the tombstones");

        manager.stop();
        assertFalse(manager.isAutoVacuumRunning());
        long runsAtStop = manager.getVacuumRuns();
        Thread.sleep(120);
        assertEquals(runsAtStop, manager.getVacuumRuns(), "no runs may happen after stop()");
        assertNull(manager.getObjectName(), "stop() must unregister the MBean");
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private static Database newDatabase() {
        Database db = new Database();
        db.setDataDir(tempDir.toString());
        return db;
    }

    private static String nextTableName(String prefix) {
        return prefix + "_" + (++tableSeq);
    }
}
