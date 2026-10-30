package diesel;

import diesel.storage.avro.AvroSecondaryIndex;
import diesel.storage.avro.AvroSecondaryIndexManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * o2 Avro-13 acceptance: {@code rebuildAllIndexes} must cost
 * O(rows x indexes x log columns), not O(rows x indexes x columns).
 *
 * <p>The discriminator is the <b>composite-width swing</b> (w=5 vs w=50 covered
 * columns of ONE composite index) at fixed row count and fixed row width (50
 * cols). Rows carry <b>case-mismatched keys</b> (lowercase row keys vs uppercase
 * schema columns) so every covered lookup takes the case-insensitive path.
 * Old shape: per covered column an O(row_size) entrySet scan -> time scales
 * ~linearly with w (~10x). New shape: ONE lazy CI view per row (w-independent,
 * amortized) + O(log columns) lookups -> ~2-3x. A bound of 4x passes the new
 * shape and fails the old one.</p>
 *
 * <p><b>Deviation (measured, first attempt was red):</b> the natural
 * index-count axis cannot discriminate here — the inherent
 * O(rows x indexes) {@code index.insert} cost is index-count linear for BOTH
 * shapes and dominates the lookup delta (measured new-shape ratio
 * t(idx=50)/t(idx=5)=8.09 vs the old shape's ~10: indistinguishable, bound 4
 * failed). Width scaling does not multiply the insert count (one insert per row
 * per index), so the lookup-shape delta stays visible.</p>
 */
@Tag("storage")
@StorageType("avro")
class RebuildAllIndexesIsORowsTimesIndexesTest {

    private static final int COLS = 50;
    private static final int ROWS = 12_000;
    private static final int W_SMALL = 5;
    private static final int W_LARGE = 50;

    /** Expected upper bound for t(w=50)/t(w=5); old scan shape ~10x. */
    private static final double MAX_RATIO = 4.0;

    private static final String[] LOWER_KEYS = lowerKeys();
    private static final String[] UPPER_COLS = upperCols();

    private static String[] lowerKeys() {
        String[] keys = new String[COLS];
        for (int i = 0; i < COLS; i++) {
            keys[i] = "col_" + i;
        }
        return keys;
    }

    private static String[] upperCols() {
        String[] cols = new String[COLS];
        for (int i = 0; i < COLS; i++) {
            cols[i] = "COL_" + i;
        }
        return cols;
    }

    /** Unique per (column, row) so search() results are position-exact. */
    private static String value(int col, int row) {
        return col + ":" + row;
    }

    private static List<String> columns() {
        List<String> cols = new ArrayList<>(COLS);
        cols.addAll(List.of(UPPER_COLS));
        return cols;
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> types = new LinkedHashMap<>();
        for (String col : UPPER_COLS) {
            types.put(col, String.class);
        }
        return types;
    }

    /** Manager with ONE composite index over the first {@code width} columns. */
    private static AvroSecondaryIndexManager managerWithComposite(int width) {
        AvroSecondaryIndexManager manager =
                new AvroSecondaryIndexManager("AVRO_REBUILD_T", columns(), types());
        manager.createCompositeIndex("CX", columns().subList(0, width));
        return manager;
    }

    private static AvroSecondaryIndexManager managerWithIndexes(int indexCount) {
        AvroSecondaryIndexManager manager =
                new AvroSecondaryIndexManager("AVRO_REBUILD_T", columns(), types());
        for (int i = 0; i < indexCount; i++) {
            manager.createIndex("IDX_" + i, UPPER_COLS[i]);
        }
        return manager;
    }

    /** Lowercase row keys vs uppercase schema columns — forces the CI path. */
    private static List<Map<String, Object>> ciMismatchRows() {
        List<Map<String, Object>> rows = new ArrayList<>(ROWS);
        for (int r = 0; r < ROWS; r++) {
            rows.add(ciMismatchRow(r));
        }
        return rows;
    }

    private static Map<String, Object> ciMismatchRow(int r) {
        Map<String, Object> row = new HashMap<>();
        for (int c = 0; c < COLS; c++) {
            row.put(LOWER_KEYS[c], value(c, r));
        }
        return row;
    }

    /** Unmeasured warmup run so JIT compilation is not billed to either shape. */
    private static void warmup(List<Map<String, Object>> rows) {
        managerWithComposite(W_SMALL).rebuildAllIndexes(rows);
        managerWithComposite(W_LARGE).rebuildAllIndexes(rows);
    }

    /** Small-of-2: fastest of two timed rebuilds; manager setup stays outside the timer. */
    private static long timedRebuild(int width, List<Map<String, Object>> rows) {
        AvroSecondaryIndexManager manager = managerWithComposite(width);
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 2; i++) {
            long start = System.nanoTime();
            manager.rebuildAllIndexes(rows);
            best = Math.min(best, System.nanoTime() - start);
        }
        return best;
    }

    @Test
    @Timeout(60)
    void rebuildCostScalesWithLogOfCoveredWidthNotWithWidthTimesScan() {
        List<Map<String, Object>> rows = ciMismatchRows();

        warmup(rows);

        long small = timedRebuild(W_SMALL, rows);
        long big = timedRebuild(W_LARGE, rows);
        double ratio = (double) big / Math.max(1L, small);

        assertTrue(ratio <= MAX_RATIO,
                String.format(Locale.ROOT,
                        "rebuildAllIndexes ratio t(width=%d)/t(width=%d)=%.2f must be <= %.1f "
                                + "(old per-covered-column scan shape is ~10x; new = one CI view per "
                                + "row + O(log columns) lookups, o2 Avro-13). small="
                                + "%.1f ms, big=%.1f ms",
                        W_LARGE, W_SMALL, ratio, MAX_RATIO, small / 1e6, big / 1e6));

        // Sanity wall-bound: 12k rows x 50 cols over a 50-wide composite stays far under this.
        assertTrue(big < 5_000_000_000L,
                "rebuild of 12k x 50-col rows over a 50-wide composite must stay under 5s, was "
                        + String.format(Locale.ROOT, "%.0f ms", big / 1e6));
    }

    @Test
    void ciMismatchedAndExactRowsIndexIdentically() {
        List<Map<String, Object>> ciRows = new ArrayList<>(200);
        List<Map<String, Object>> exactRows = new ArrayList<>(200);
        for (int r = 0; r < 200; r++) {
            ciRows.add(ciMismatchRow(r));
            Map<String, Object> exact = new HashMap<>();
            for (int c = 0; c < COLS; c++) {
                exact.put(UPPER_COLS[c], value(c, r));
            }
            exactRows.add(exact);
        }

        AvroSecondaryIndexManager ciManager = managerWithIndexes(3);
        ciManager.rebuildAllIndexes(ciRows);

        AvroSecondaryIndexManager exactManager = managerWithIndexes(3);
        exactManager.rebuildAllIndexes(exactRows);

        for (int r = 0; r < 200; r++) {
            for (int idx = 0; idx < 3; idx++) {
                AvroSecondaryIndex index = ciManager.getIndex("IDX_" + idx);
                assertEquals(List.of(r), index.search(value(idx, r)),
                        "CI-mismatched rebuild must find row " + r + " in index " + idx);
                assertEquals(List.of(r), exactManager.getIndex("IDX_" + idx).search(value(idx, r)),
                        "exact-match rebuild must find row " + r + " in index " + idx);
            }
        }
        assertEquals(200, ciManager.getIndex("IDX_0").size());
        assertEquals(200, exactManager.getIndex("IDX_0").size());
    }

    @Test
    void compositeAndMissingColumnsAreHandled() {
        AvroSecondaryIndexManager manager = new AvroSecondaryIndexManager("AVRO_REBUILD_T", columns(), types());
        manager.createIndex("SINGLE", "COL_1");
        manager.createCompositeIndex("PAIR", List.of("COL_1", "COL_2"));

        List<Map<String, Object>> rows = new ArrayList<>(100);
        for (int r = 0; r < 100; r++) {
            Map<String, Object> row = new HashMap<>();
            row.put("col_1", value(1, r)); // CI-mismatched keys
            if (r % 2 == 0) {
                row.put("col_2", value(2, r)); // every other row misses COL_2 entirely
            }
            rows.add(row);
        }
        manager.rebuildAllIndexes(rows);

        assertEquals(100, manager.getIndex("SINGLE").size(),
                "every row carries col_1 -> full posting list");
        // A missing composite column yields a null component, not a null key — the row
        // stays indexed exactly as with the pre-Avro-13 scan shape (parity, no skip).
        assertEquals(100, manager.getIndex("PAIR").size(),
                "missing col_2 rows keep a null component in the composite key");
        assertEquals(List.of(7), manager.getIndex("SINGLE").search(value(1, 7)));
        // value(1, r) is unique per row -> exact single-element posting
        assertEquals(1, manager.getIndex("SINGLE").search(value(1, 8)).size());
    }
}
