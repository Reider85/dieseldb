package diesel;

import diesel.storage.CsvIndexManager;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 CSV-2 (shared CSV+TSV): {@code createIndex(String)} must be
 * O(n log n), not O(n²). The O(n²) came from {@code positionToRowId(i)}
 * linearly scanning the rowIdToPosition entry set per row; o2 CSV-1 made
 * that lookup O(1) via the inverse map, so this class is the acceptance
 * guard keeping createIndex on the O(n log n) TreeMap-insert path
 * (DelimitedIndexManager.createIndex).
 *
 * <p>Consolidates the prompt-named CreateIndexIsONLogNTest with the
 * correctness cross-checks that prove the O(1) lookup and the new
 * LinkedHashSet posting lists (o2 CSV-6) build the same index the
 * maintained path does, following the CatalogLookupIsO1Test /
 * PositionToRowIdInverseMapTest single-file precedent. The manager under
 * test is the format-agnostic DelimitedIndexManager via its only concrete
 * CSV instantiation (TsvIndexManager is a 7-line subclass with zero
 * overrides, so the shared code path is identical for TSV).
 */
@Tag("storage")
@StorageType({"csv", "tsv"})
class CreateIndexIsONLogNTest {

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

    private static Object[] array(long id, String name, int age) {
        return new Object[] {id, name, age};
    }

    private static CsvIndexManager manager(int rowCount, String pk, IntFunction<Integer> ageOf) {
        CsvIndexManager m = new CsvIndexManager("T", COLS, types());
        List<Map<String, Object>> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(row(i, "n" + i, ageOf.apply(i)));
        }
        m.buildIndexes(rows, pk);
        return m;
    }

    /** Expected ascending positions of rows whose AGE lies in [lo, hi]. */
    private static List<Integer> agePositions(int rowCount, int lo, int hi) {
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            int age = i; // unique-AGE fixtures use ageOf = identity
            if (age >= lo && age <= hi) {
                out.add(i);
            }
        }
        return out;
    }

    // ─── O(n log n) wall-bound ──────────────────────────────────────

    /**
     * createIndex over 100k rows: the old per-row O(n) positionToRowId
     * scan cost sum(i) ~= 5*10^9 entry comparisons (tens of seconds); the
     * O(1) inverse-map lookup plus TreeMap inserts run in tens of
     * milliseconds. The 2000ms bound sits ~40x above the expected time so
     * the test fails a regression without being load-sensitive
     * (PositionToRowIdInverseMapTest bound style); @Timeout catches a hang
     * even faster.
     */
    @Test
    @Timeout(60)
    void createIndexOver100kRowsCompletesWithinWallBound() {
        final int tableRows = 100_000;
        final long boundMillis = 2_000;
        CsvIndexManager m = manager(tableRows, "ID", i -> i);

        long start = System.nanoTime();
        boolean ok = m.createIndex("AGE");
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(ok, "index must be available after createIndex");
        assertTrue(elapsedMillis < boundMillis,
                "createIndex over " + tableRows + " rows took " + elapsedMillis
                        + " ms, expected < " + boundMillis
                        + " ms (O(n log n) createIndex, o2 CSV-2)");
        // The freshly built index is functional: unique AGE keys map 1:1 to
        // positions (build order preserved by reindex).
        assertEquals(List.of(12_345), m.search("AGE", 12_345));
        assertEquals(List.of(99_999), m.search("AGE", 99_999));
        assertEquals(agePositions(tableRows, 100, 104), m.rangeSearch("AGE", 100, 104));
        assertTrue(m.search("AGE", -1).isEmpty(), "absent key stays absent");
    }

    // ─── Duplicate-key posting lists ────────────────────────────────

    /**
     * With duplicate AGE values the posting lists hold many rowIds per key
     * (the O(n) shape CSV-6's O(1) remove defends against). createIndex must
     * collect all of them, and rangeSearch must read the LinkedHashSet
     * posting lists back in ascending position order.
     */
    @Test
    void createIndexOnDuplicateKeysBuildsCompletePostingLists() {
        final int rowCount = 2_000;
        final int distinctAges = 7;
        CsvIndexManager m = manager(rowCount, "ID", i -> i % distinctAges);
        assertTrue(m.createIndex("AGE"));

        for (int age = 0; age < distinctAges; age++) {
            List<Integer> expected = new ArrayList<>();
            for (int i = age; i < rowCount; i += distinctAges) {
                expected.add(i);
            }
            List<Integer> hits = m.search("AGE", age);
            assertEquals(expected.size(), hits.size(), "posting list for AGE=" + age);
            assertEquals(expected, hits, "ascending positions for AGE=" + age);
        }
        // Range over values 2..4 collects the union of their posting lists.
        List<Integer> expectedRange = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            int age = i % distinctAges;
            if (age >= 2 && age <= 4) {
                expectedRange.add(i);
            }
        }
        assertEquals(expectedRange, m.rangeSearch("AGE", 2, 4));
    }

    // ─── Equivalence with the maintained index path ─────────────────

    /**
     * The build path (createIndex) and the maintained path (updateRow ->
     * removeIndexedRow + insertIndexedRow, o2 CSV-6's LinkedHashSet sets)
     * must produce identical lookups: after rewriting every row's AGE from
     * a unique value to i%10, search/rangeSearch must match a freshly
     * computed expectation, and the vacated unique keys must be dropped
     * (empty posting list removal).
     */
    @Test
    void createIndexMatchesMaintainedIndexAfterChurn() {
        final int rowCount = 5_000;
        final int distinctAges = 10;
        CsvIndexManager m = manager(rowCount, "ID", i -> i);
        assertTrue(m.createIndex("AGE"));

        // Rewrite every row: AGE i -> i%10. updateRow disassociates the old
        // unique key (posting list empties -> key dropped) and joins the new
        // low-cardinality posting list.
        for (int i = 0; i < rowCount; i++) {
            m.updateRow(array(i, "n" + i, i), i, array(i, "n" + i, i % distinctAges));
        }

        for (int age = 0; age < distinctAges; age++) {
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < rowCount; i++) {
                if (i % distinctAges == age) {
                    expected.add(i);
                }
            }
            assertEquals(expected, m.search("AGE", age), "maintained index for AGE=" + age);
        }
        // Vacated unique keys (>= distinctAges) were dropped entirely.
        assertTrue(m.search("AGE", 15).isEmpty(), "churned-away key dropped");
        assertTrue(m.search("AGE", 4_999).isEmpty(), "churned-away key dropped");
        // PK lookups survive the churn untouched.
        assertEquals(List.of(0), m.searchByPrimaryKey(0L));
        assertEquals(List.of(4_999), m.searchByPrimaryKey(4_999L));
    }
}
