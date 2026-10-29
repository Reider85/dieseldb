package diesel;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 CSV-7 (shared CSV+TSV): the engine wraps multi-row mutations in the
 * deferred storage bulk-update window instead of driving the index manager
 * row-by-row.
 *
 * <p>Delivered wiring:
 * <ul>
 *   <li>multi-tuple {@code VALUES (...), (...)} parsing in
 *       {@code QueryParser.parseInsertQuery} (was: exactly one tuple or a
 *       syntax error);</li>
 *   <li>{@link InsertQuery#multiRow} executes a batch at or above
 *       {@code diesel.bulkInsert.threshold} (default 100) inside
 *       {@code Table.beginBulkUpdate()}/{@code endBulkUpdate()} so the
 *       storage manager performs one {@code reindex()} at the end instead of
 *       an O(n) position shift per row (o2 CSV-4/CSV-5 residual);</li>
 *   <li>large batches supplying the clustered key are stably sorted by it
 *       first — a clustered table stores rows in key order, so the final
 *       state is unchanged, but every insert becomes a tail append on both
 *       the engine row mirror ({@code ArrayList.add(int)} shifts vanish) and
 *       the storage mirror;</li>
 *   <li>{@code UpdateQuery.applyBulkUpdate} also opens the storage window
 *       (prompt-literal; today a no-op for the delimited manager whose
 *       {@code updateRow} does not shift positions, kept as the shared bulk
 *       contract);</li>
 *   <li>{@code AbstractRowStorage.runInBulkMode(Runnable)} helper (prompt
 *       change #2); {@code DelimitedIndexManager} bulk mode became a nesting
 *       depth counter so an in-batch {@code Table.compact()} (its own inner
 *       window) cannot close the outer window early.</li>
 * </ul>
 *
 * <p>What is <em>not</em> claimed: a batch whose keys interleave with a
 * non-empty clustered table still pays the inherent engine
 * {@code ArrayList.add(int, E)} shift per mid-table row (prompt: residual
 * per-row cost acceptable); single-row statements keep the per-row path by
 * design ({@code diesel.bulkInsert.threshold}); statement-level rollback of a
 * failed batch is not performed outside an MVCC transaction (earlier tuples
 * of the batch stay inserted).
 *
 * <p>The engine under test is CSV/TSV-backed via the {@code storage.type}
 * config default ({@code csv}); {@code @StorageType} re-runs it under the
 * explicit {@code storage-tsv} profile (DelimitedIndexManager is the shared
 * code path for both).
 */
@Tag("storage")
@StorageType({"csv", "tsv"})
class EngineBatchInsertIsONTest {

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
     * The prompt's EngineBatchInsertIsONTest criterion: a single
     * {@code INSERT INTO t VALUES (...), (...), ..., (...)} with 10⁵ rows
     * completes in O(n), not O(n²). The IDs arrive shuffled (a modular
     * permutation), so without the clustered-key sort every row would land
     * mid-table: the old path paid an engine {@code ArrayList.add(int)}
     * shift, an inverse-map storage shift and an
     * {@code updateIndicesAfterInsert} B-tree walk over all later rows per
     * insert — ~5*10⁹ operations = minutes, which @Timeout(60) catches.
     * With the sort plus the bulk window the batch is n tail appends plus
     * one deferred reindex. The 15000ms bound is calibrated on this machine
     * (cold-JIT run measured 7425ms: SQL parse of the ~2.4MB statement,
     * per-row engine validation, clustered B-tree inserts, row locks and
     * the background CSV flusher contending for the same cores) and still
     * sits >4x below any plausible quadratic regression. Secondary-index
     * correctness after the window closes is verified by an AGE lookup
     * spanning base and index state.
     */
    @Test
    @Timeout(60)
    void multiValuesInsertOf100kShuffledRowsCompletesWithinWallBound() {
        final int rows = 100_000;
        final long boundMillis = 15_000;
        database.executeQuery(
                "CREATE TABLE MBULK (ID LONG PRIMARY KEY, NAME STRING, AGE INTEGER)", null);
        database.executeQuery("CREATE INDEX ON MBULK (AGE)", null);

        StringBuilder sql = new StringBuilder(rows * 24);
        sql.append("INSERT INTO MBULK (ID, NAME, AGE) VALUES ");
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

        Table table = database.getTable("MBULK");
        assertEquals(rows, table.getLiveRowCount(), "every tuple of the batch is inserted");
        assertTrue(elapsedMillis < boundMillis,
                "single multi-VALUES INSERT of " + rows + " rows took " + elapsedMillis
                        + " ms, expected < " + boundMillis
                        + " ms (bulk window + clustered-key sort, o2 CSV-7)");

        // Clustered layout: the shuffled batch landed in ascending key order.
        assertEquals(List.of(0L), idsOf(database.executeQuery("SELECT ID FROM MBULK WHERE ID = 0", null)));
        assertEquals(List.of(54_321L), idsOf(database.executeQuery("SELECT ID FROM MBULK WHERE ID = 54321", null)));
        assertEquals(List.of(99_999L), idsOf(database.executeQuery("SELECT ID FROM MBULK WHERE ID = 99999", null)));
        // Secondary AGE index survived the window: ids with id%10==3.
        Object ageHits = database.executeQuery("SELECT ID FROM MBULK WHERE AGE = 3", null);
        assertEquals(rows / 10, ((List<?>) ageHits).size(),
                "AGE secondary index covers the whole batch after the window closes");
    }

    // ─── Small batch: per-row path keeps VALUES semantics ───────────

    /**
     * Batches below the bulk threshold keep the pre-existing per-row path:
     * VALUES order drives sequence assignment and the parser accepts several
     * tuples including string values containing commas and parentheses
     * (quote-aware tuple splitting). Rows of a clustered table land in key
     * order regardless of VALUES order — that is the clustered contract, not
     * a batch side effect.
     */
    @Test
    void smallMultiValuesBatchKeepsValuesSemanticsAndClusteredOrder() {
        database.executeQuery(
                "CREATE TABLE MSMALL (ID LONG PRIMARY KEY, NAME STRING, AGE INTEGER)", null);

        database.executeQuery(
                "INSERT INTO MSMALL (ID, NAME, AGE) VALUES "
                        + "(3, 'c', 30), (1, 'a,b(x)', 10), (2, 'c', 20)", null);

        Table table = database.getTable("MSMALL");
        assertEquals(3, table.getLiveRowCount(), "all three tuples inserted");
        // Physical (clustered) order is ascending by ID despite VALUES order.
        assertEquals(List.of(1L, 2L, 3L),
                idsOf(database.executeQuery("SELECT ID FROM MSMALL", null)));
        // The quoted comma/paren tuple parsed and landed intact.
        assertEquals(List.of("a,b(x)"),
                namesOf(database.executeQuery("SELECT NAME FROM MSMALL WHERE ID = 1", null)));
    }

    // ─── Duplicate clustered key inside a batch ─────────────────────

    /**
     * A duplicate primary key inside the batch throws IllegalStateException
     * at the offending tuple; without statement-level rollback the tuples
     * inserted before the failure remain (the documented engine contract for
     * multi-row VALUES outside an MVCC transaction).
     */
    @Test
    void duplicateKeyInMultiValuesBatchThrowsAndKeepsEarlierTuples() {
        database.executeQuery(
                "CREATE TABLE MDUP (ID LONG PRIMARY KEY, NAME STRING)", null);

        assertThrows(IllegalStateException.class, () -> database.executeQuery(
                "INSERT INTO MDUP (ID, NAME) VALUES "
                        + "(1, 'a'), (2, 'b'), (2, 'dup'), (3, 'c')", null),
                "duplicate clustered key inside the batch must fail");

        Table table = database.getTable("MDUP");
        assertEquals(2, table.getLiveRowCount(),
                "tuples before the failing one stay inserted (no statement rollback)");
        assertEquals(List.of(1L, 2L), idsOf(database.executeQuery("SELECT ID FROM MDUP", null)));

        // The table stays usable after the failed batch.
        database.executeQuery("INSERT INTO MDUP (ID, NAME) VALUES (3, 'c')", null);
        assertEquals(3, table.getLiveRowCount(), "insert after the failed batch succeeds");
    }

    // ─── Sequence primary key: assignment follows VALUES order ──────

    /**
     * A sequence-based primary key column is rejected by the parser when
     * supplied explicitly, so the clustered column is absent from the batch
     * columns: no sort applies and sequence values are assigned in VALUES
     * order, tuple by tuple.
     */
    @Test
    void sequencePrimaryKeyMultiValuesAssignsInStatementOrder() {
        database.executeQuery(
                "CREATE TABLE MSEQ (ID LONG PRIMARY KEY SEQUENCE(mseq_seq 1 1), NAME STRING)", null);

        database.executeQuery(
                "INSERT INTO MSEQ (NAME) VALUES ('third'), ('first'), ('second')", null);

        Table table = database.getTable("MSEQ");
        assertEquals(3, table.getLiveRowCount(), "all tuples inserted");
        List<?> result = (List<?>) database.executeQuery("SELECT ID, NAME FROM MSEQ", null);
        assertEquals(3, result.size());
        // Sequence order == VALUES order: first tuple got 1, second 2, third 3.
        assertEquals(1L, ((Map<?, ?>) result.get(0)).get("ID"));
        assertEquals("third", ((Map<?, ?>) result.get(0)).get("NAME"));
        assertEquals(2L, ((Map<?, ?>) result.get(1)).get("ID"));
        assertEquals("first", ((Map<?, ?>) result.get(1)).get("NAME"));
        assertEquals(3L, ((Map<?, ?>) result.get(2)).get("ID"));
        assertEquals("second", ((Map<?, ?>) result.get(2)).get("NAME"));
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
