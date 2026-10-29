package diesel;

import diesel.storage.CsvIndexManager;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #47 (R3-013a): stable row-id acceptance — random interleaved
 * mid-table insertAt/deleteRow never desynchronizes the PK/secondary indexes
 * from physical storage positions.
 *
 * <p>The production fix shipped long ago as prompt3 Prompt 25 (3.0.51) plus
 * o2 CSV-1 (3.3.64): DelimitedIndexManager assigns a monotonically increasing
 * {@code nextRowId} per row, indexes map key→rowId (stable identity), and a
 * dual {@code rowIdToPosition}/{@code positionToRowIdMap} keeps physical
 * positions in sync under mid-table shifts. This class closes the residual
 * acceptance gap with a model-based property test the earlier unit tests did
 * not cover: 10⁴ random interleaved mid-inserts/mid-deletes verified against
 * an independent {@code ArrayList} model, plus a 10⁶-append stress run.
 *
 * <p>Deviations from the literal prompt text (agreed in plan mode):
 * <ul>
 *   <li>{@code searchByPrimaryKey} still returns physical positions, not
 *       row-ids — Prompt 25 criterion 4 (public API stability); Table /
 *       SelectQuery / MVCC consume positions, and the bug being fixed was
 *       <em>wrong positions after insertAt</em>, not the return type.</li>
 *   <li>No {@code diesel/RowId} long wrapper — bare {@code long} is the
 *       wire representation everywhere; a wrapper would only add allocation
 *       pressure.</li>
 *   <li>No {@code Table.getRow(rowId)} — Table is positional by design;
 *       row-ids live only inside the index manager.</li>
 *   <li>The old failing {@code PerformanceTest} criterion (WHERE ... = TRUE
 *       parsed as a column) is Prompt 48 (R3-013b), out of scope here.</li>
 * </ul>
 *
 * <p>The manager under test is the format-agnostic DelimitedIndexManager via
 * its only concrete CSV instantiation (TsvIndexManager: 7-line subclass, zero
 * overrides — identical shared code path for TSV).
 */
@Tag("storage")
@StorageType({"csv", "tsv"})
class RowIdStabilityTest {

    private static final List<String> COLS = List.of("ID", "NAME", "AGE");

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        return t;
    }

    private static Map<String, Object> row(long id, String name, int age) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", name);
        r.put("AGE", age);
        return r;
    }

    private static CsvIndexManager manager(int rowCount, String pk) {
        CsvIndexManager m = new CsvIndexManager("T", COLS, types());
        List<Map<String, Object>> rows = new ArrayList<>(Math.max(rowCount, 1));
        for (int i = 0; i < rowCount; i++) {
            rows.add(row(i, "n" + i, i % 10));
        }
        m.buildIndexes(rows, pk);
        return m;
    }

    /**
     * Independent positional model: model.get(pos) is the PK value stored at
     * physical position {@code pos}. Every mid-insert/mid-delete is mirrored
     * here, then the manager's {@code searchByPrimaryKey} is asserted against
     * this list for every live key.
     */
    private static void assertMatchesModel(CsvIndexManager m, List<Long> model, String context) {
        assertEquals(model.size(), m.getRowCount(), "row count vs model " + context);
        for (int pos = 0; pos < model.size(); pos++) {
            long pk = model.get(pos);
            assertEquals(List.of(pos), m.searchByPrimaryKey(pk),
                    "PK " + pk + " must sit at position " + pos + " (" + context + ")");
        }
    }

    private static void assertSecondaryMatchesModel(CsvIndexManager m, List<Long> model, String context) {
        // AGE = pk % 10 in every fixture row; rebuild expected posting lists
        // from the model and compare as sorted position lists.
        for (int age = 0; age < 10; age++) {
            List<Integer> expected = new ArrayList<>();
            for (int pos = 0; pos < model.size(); pos++) {
                if (model.get(pos) % 10 == age) {
                    expected.add(pos);
                }
            }
            List<Integer> actual = new ArrayList<>(m.search("AGE", age));
            actual.sort(Integer::compareTo);
            assertEquals(expected, actual,
                    "AGE=" + age + " posting positions must match model (" + context + ")");
        }
    }

    // ─── Acceptance: 10⁴ random mid-insert / mid-delete churn ────────

    /**
     * Prompt4 #47 primary criterion: 10 000 random interleaved mid-table
     * insertAt/deleteAt operations; after every batch of mutations
     * {@code searchByPrimaryKey} (PK) and {@code search} (secondary AGE)
     * return exactly the positions the independent model predicts.
     *
     * <p>Correctness-only: no wall-bound (mid-table single-row ops are O(n)
     * by design per o2 CSV-4/CSV-7 — this test guards the inverse-map
     * shift walks, not asymptotics). 10⁴ ops over a ~5k-row table finishes
     * in a few seconds on this machine.
     */
    @Test
    @Timeout(120)
    void tenThousandRandomMidInsertsAndDeletesKeepIndexesCorrect() {
        final int initialRows = 2_000;
        final int operations = 10_000;
        final int verifyEvery = 500;
        CsvIndexManager m = manager(initialRows, "ID");
        assertTrue(m.createIndex("AGE"));

        List<Long> model = new ArrayList<>(initialRows);
        for (long i = 0; i < initialRows; i++) {
            model.add(i);
        }

        // Deterministic seed: a red run must be reproducible from the log.
        Random rnd = new Random(47L);
        long nextKey = initialRows;

        for (int op = 0; op < operations; op++) {
            int choice = rnd.nextInt(100);
            if (choice < 45 || model.isEmpty()) {
                // Mid/tail insertAt: any position 0..size inclusive.
                int pos = model.isEmpty() ? 0 : rnd.nextInt(model.size() + 1);
                long key = nextKey++;
                m.insertAt(pos, row(key, "x" + key, (int) (key % 10)));
                model.add(pos, key);
            } else if (choice < 90) {
                // Mid-table deleteRow (biased away from pure-tail so the
                // shift loop actually runs most of the time).
                int pos = rnd.nextInt(model.size());
                long removed = model.remove(pos);
                m.deleteRow(pos);
                assertTrue(m.searchByPrimaryKey(removed).isEmpty(),
                        "deleted PK " + removed + " must disappear (op " + op + ")");
            } else {
                // Tail delete — exercises the empty-shift fast path.
                int pos = model.size() - 1;
                long removed = model.remove(pos);
                m.deleteRow(pos);
                assertTrue(m.searchByPrimaryKey(removed).isEmpty(),
                        "tail-deleted PK " + removed + " must disappear (op " + op + ")");
            }

            if ((op + 1) % verifyEvery == 0) {
                assertMatchesModel(m, model, "after op " + (op + 1));
            }
        }

        assertMatchesModel(m, model, "final state");
        assertSecondaryMatchesModel(m, model, "final state");
    }

    // ─── Compaction boundary during random churn ─────────────────────

    /**
     * Deleting past the 25% tombstone threshold fires mid-flight reindex
     * (both maps rebuilt, rowIds reassigned). The model only tracks PK
     * values — compaction must be invisible to search results. A storm of
     * deletes interleaved with inserts forces several compaction rounds.
     */
    @Test
    @Timeout(60)
    void compactionDuringRandomChurnKeepsSearchCorrect() {
        final int initialRows = 100;
        final int operations = 2_000;
        CsvIndexManager m = manager(initialRows, "ID");

        List<Long> model = new ArrayList<>(initialRows);
        for (long i = 0; i < initialRows; i++) {
            model.add(i);
        }

        Random rnd = new Random(47013L);
        long nextKey = initialRows;
        int compactionsSeen = 0;
        int lastPending = 0;

        for (int op = 0; op < operations; op++) {
            int choice = rnd.nextInt(100);
            if (choice < 30 || model.isEmpty()) {
                int pos = model.isEmpty() ? 0 : rnd.nextInt(model.size() + 1);
                long key = nextKey++;
                m.insertAt(pos, row(key, "x" + key, (int) (key % 10)));
                model.add(pos, key);
            } else {
                int pos = rnd.nextInt(model.size());
                long removed = model.remove(pos);
                m.deleteRow(pos);
                assertTrue(m.searchByPrimaryKey(removed).isEmpty(),
                        "deleted PK " + removed + " must disappear (op " + op + ")");
            }

            // Pending tombstones drop back to ~0 after each auto-compaction;
            // count rising edges to prove the compaction path actually ran.
            int pending = m.getDeletedCount();
            if (pending < lastPending) {
                compactionsSeen++;
            }
            lastPending = pending;

            if ((op + 1) % 200 == 0) {
                assertMatchesModel(m, model, "after op " + (op + 1));
            }
        }

        assertMatchesModel(m, model, "final state");
        assertTrue(compactionsSeen >= 1,
                "expected at least one auto-compaction during " + operations
                        + " ops over a " + initialRows + "-row table, saw " + compactionsSeen);
    }

    // ─── Stress: 10⁶ appends + 10⁵ deletes without mismatches ────────

    /**
     * Prompt4 #47 stress criterion, feasible interpretation: 10⁶ tail
     * appends (O(1) each) followed by 10⁵ deletes whose positions sit in a
     * bounded window near the tail (shift ≤ 1000 per call — a literal 10⁵
     * uniformly-mid deletes over a 10⁶-row table would be ~5·10¹⁰ shift
     * steps, O(n²) by the documented per-row design of o2 CSV-4/CSV-7).
     * Correctness is the gate: every sampled surviving PK must resolve to
     * its model position, every removed key must stay absent, and the
     * secondary AGE posting-list sizes must match the live population.
     */
    @Test
    @Timeout(180)
    void stressOneMillionAppendsAndHundredThousandDeletesWithoutMismatch() {
        final int inserts = 1_000_000;
        final int deletes = 100_000;
        final int midWindow = 1_000;
        CsvIndexManager m = manager(0, "ID");
        assertTrue(m.createIndex("AGE"));

        // Positional model of PK values (boxed Long: ~16 MB, fine under the
        // storage-profile heap; keeps the delete-loop key oracle trivially
        // correct without re-deriving sliding-window arithmetic).
        List<Long> model = new ArrayList<>(inserts);

        long start = System.nanoTime();
        for (int i = 0; i < inserts; i++) {
            m.appendIndexedRow(row(i, "n" + i, i % 10), i);
            model.add((long) i);
        }
        long insertMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertEquals(inserts, m.getRowCount());
        assertEquals(List.of(0), m.searchByPrimaryKey(0L));
        assertEquals(List.of(inserts - 1), m.searchByPrimaryKey(inserts - 1L));

        start = System.nanoTime();
        for (int i = 0; i < deletes; i++) {
            // Bounded mid-window near the tail: every delete shifts at most
            // midWindow-1 later rows (real mid-table shift path, controlled
            // cost), and the removed key is read straight from the model.
            int pos = model.size() - 1 - (i % midWindow);
            long removed = model.remove(pos);
            m.deleteRow(pos);
            assertTrue(m.searchByPrimaryKey(removed).isEmpty(),
                    "deleted PK " + removed + " must disappear (delete " + i + ")");
        }
        long deleteMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertEquals(model.size(), m.getRowCount());
        assertTrue(insertMillis < 60_000,
                inserts + " appends took " + insertMillis + " ms, expected < 60000 ms");
        assertTrue(deleteMillis < 60_000,
                deletes + " bounded-window deletes took " + deleteMillis
                        + " ms, expected < 60000 ms");

        // Sampled key-level verification: every 997th live row plus the
        // first/last model entries. A full 10⁶-point loop would double the
        // wall time for no extra bug class (the invariant is positional
        // shift math, already covered at full fidelity by the 10k churn test).
        int checked = 0;
        for (int pos = 0; pos < model.size(); pos += 997) {
            long pk = model.get(pos);
            assertEquals(List.of(pos), m.searchByPrimaryKey(pk),
                    "live PK " + pk + " must sit at model position " + pos);
            checked++;
        }
        checked += 2; // first + last
        assertEquals(List.of(0), m.searchByPrimaryKey(model.get(0)));
        assertEquals(List.of(model.size() - 1), m.searchByPrimaryKey(model.get(model.size() - 1)));
        assertTrue(checked > 900, "expected >900 sampled live keys, checked " + checked);

        // Secondary AGE index still covers the surviving population.
        for (int age = 0; age < 10; age++) {
            long expectedCount = 0;
            for (Long pk : model) {
                if (pk % 10 == age) {
                    expectedCount++;
                }
            }
            assertEquals(expectedCount, m.search("AGE", age).size(),
                    "AGE=" + age + " posting-list size must match live population");
        }
    }
}
