package diesel;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 JSONL-8 (mirror of o2 CSV-7 / EngineBatchInsertIsONTest): the engine
 * must funnel multi-row {@code INSERT} statements through the deferred
 * storage bulk-update window on the JSONL backend too.
 *
 * <p><b>Acceptance gate, 0 prod changes</b> (CSV-2/CSV-3/Buffer-8
 * precedent): the wiring from o2 CSV-7 (3.3.71) is storage-agnostic —
 * {@link InsertQuery#executeMultiRow} wraps batches at or above
 * {@code diesel.bulkInsert.threshold} in
 * {@code Table.beginBulkUpdate()}/{@code endBulkUpdate()}, and
 * {@code JsonlRowStorage.beginBulkUpdate/endBulkUpdate} delegate into the
 * {@code JsonlIndexManager} bulk window (o2 JSONL-3), so a 100k-row batch
 * costs n tail appends plus one deferred reindex instead of an O(n)
 * position shift per row. This class pins that contract against the JSONL
 * storage selected via {@code diesel.storage.type=jsonl}.
 *
 * <p>What is <em>not</em> claimed: the sequence-primary-key and
 * parser-level contracts are engine-wide and already pinned by
 * {@code EngineBatchInsertIsONTest} on the delimited backend; this class
 * covers the JSONL-specific wall bound plus correctness of the row mirror,
 * clustered layout and secondary index after the window closes.
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlEngineBatchInsertIsONTest {

    @TempDir
    Path tempDir;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
            database = null;
        }
    }

    // ─── O(n) wall-bound: 10⁵ tuples in ONE SQL statement ──────────

    /**
     * The prompt's JsonlEngineBatchInsertIsONTest criterion: a single
     * {@code INSERT INTO t VALUES (...), (...), ..., (...)} with 10⁵ rows
     * completes in O(n), not O(n²). IDs arrive shuffled (a modular
     * permutation), so without the clustered-key sort every row would land
     * mid-table: the old shape paid an engine {@code ArrayList.add(int)}
     * shift and an {@code JsonlIndexManager} position shift per insert —
     * minutes, which @Timeout(60) catches. With the sort plus the bulk
     * window the batch is n tail appends plus one deferred reindex. The
     * 15000ms bound mirrors the calibrated delimited sibling
     * (EngineBatchInsertIsONTest cold-JIT: 7425ms); JSONL adds per-row
     * {@code fromMap} boxing and row-presence bookkeeping on the same order.
     */
    @Test
    @Timeout(60)
    void multiValuesInsertOf100kShuffledRowsCompletesWithinWallBound() {
        final int rows = 100_000;
        final long boundMillis = 15_000;
        database.executeQuery(
                "CREATE TABLE JMBULK (ID LONG PRIMARY KEY, NAME STRING, AGE INTEGER)", null);
        database.executeQuery("CREATE INDEX ON JMBULK (AGE)", null);

        StringBuilder sql = new StringBuilder(rows * 24);
        sql.append("INSERT INTO JMBULK (ID, NAME, AGE) VALUES ");
        for (int i = 0; i < rows; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            // 7919 is prime => (i * 7919) % 100000 is a permutation: the
            // tuples arrive in shuffled clustered-key order on purpose.
            long id = (long) ((i * 7919L) % rows);
            sql.append('(').append(id).append(", 'n").append(id).append("', ")
               .append(id % 10).append(')');
        }

        long start = System.nanoTime();
        database.executeQuery(sql.toString(), null);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        Table table = database.getTable("JMBULK");
        assertEquals(rows, table.getLiveRowCount(), "every tuple of the batch is inserted");
        assertTrue(elapsedMillis < boundMillis,
                "single multi-VALUES INSERT of " + rows + " rows took " + elapsedMillis
                        + " ms, expected < " + boundMillis
                        + " ms (bulk window + clustered-key sort on jsonl, o2 JSONL-8)");

        // Clustered layout: the shuffled batch landed in ascending key order.
        assertEquals(List.of(0L), idsOf(database.executeQuery("SELECT ID FROM JMBULK WHERE ID = 0", null)));
        assertEquals(List.of(54_321L), idsOf(database.executeQuery("SELECT ID FROM JMBULK WHERE ID = 54321", null)));
        assertEquals(List.of(99_999L), idsOf(database.executeQuery("SELECT ID FROM JMBULK WHERE ID = 99999", null)));
        // Secondary AGE index survived the window: ids with id%10==3.
        Object ageHits = database.executeQuery("SELECT ID FROM JMBULK WHERE AGE = 3", null);
        assertEquals(rows / 10, ((List<?>) ageHits).size(),
                "AGE secondary index covers the whole batch after the window closes");
    }

    // ─── Small batch: per-row path keeps VALUES semantics ───────────

    /**
     * Batches below the bulk threshold keep the pre-existing per-row path on
     * JSONL exactly as on delimited: VALUES order drives sequence assignment,
     * quote-aware tuple splitting accepts commas/parentheses inside strings,
     * and a clustered table lands rows in key order regardless of VALUES
     * order (clustered contract, not a batch side effect).
     */
    @Test
    void smallMultiValuesBatchKeepsValuesSemanticsAndClusteredOrder() {
        database.executeQuery(
                "CREATE TABLE JMSMALL (ID LONG PRIMARY KEY, NAME STRING, AGE INTEGER)", null);

        database.executeQuery(
                "INSERT INTO JMSMALL (ID, NAME, AGE) VALUES "
                        + "(3, 'c', 30), (1, 'a,b(x)', 10), (2, 'c', 20)", null);

        Table table = database.getTable("JMSMALL");
        assertEquals(3, table.getLiveRowCount(), "all three tuples inserted");
        // Physical (clustered) order is ascending by ID despite VALUES order.
        assertEquals(List.of(1L, 2L, 3L),
                idsOf(database.executeQuery("SELECT ID FROM JMSMALL", null)));
        // The quoted comma/paren tuple parsed and landed intact.
        assertEquals(List.of("a,b(x)"),
                namesOf(database.executeQuery("SELECT NAME FROM JMSMALL WHERE ID = 1", null)));
    }

    // ─── Duplicate clustered key inside a batch ─────────────────────

    /**
     * A duplicate primary key inside the batch throws IllegalStateException
     * at the offending tuple on the JSONL backend too; without
     * statement-level rollback the tuples inserted before the failure remain
     * (the documented engine contract for multi-row VALUES outside an MVCC
     * transaction).
     */
    @Test
    void duplicateKeyInMultiValuesBatchThrowsAndKeepsEarlierTuples() {
        database.executeQuery(
                "CREATE TABLE JMDUP (ID LONG PRIMARY KEY, NAME STRING)", null);

        assertThrows(IllegalStateException.class, () -> database.executeQuery(
                "INSERT INTO JMDUP (ID, NAME) VALUES "
                        + "(1, 'a'), (2, 'b'), (2, 'dup'), (3, 'c')", null),
                "duplicate clustered key inside the batch must fail");

        Table table = database.getTable("JMDUP");
        assertEquals(2, table.getLiveRowCount(),
                "tuples before the failing one stay inserted (no statement rollback)");
        assertEquals(List.of(1L, 2L), idsOf(database.executeQuery("SELECT ID FROM JMDUP", null)));

        // The table stays usable after the failed batch.
        database.executeQuery("INSERT INTO JMDUP (ID, NAME) VALUES (3, 'c')", null);
        assertEquals(3, table.getLiveRowCount(), "insert after the failed batch succeeds");
    }

    // ─── Helpers ────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static List<Long> idsOf(Object queryResult) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) queryResult;
        return rows.stream().map(r -> (Long) r.get("ID")).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<String> namesOf(Object queryResult) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) queryResult;
        return rows.stream().map(r -> (String) r.get("NAME")).toList();
    }
}
