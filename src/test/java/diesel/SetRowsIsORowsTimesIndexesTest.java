package diesel;

import diesel.storage.avro.AvroRowStorage;
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
 * o2 Avro-19 acceptance: {@code AvroRowStorage.setRows} (the bulk-load /
 * rollback / partitioner delegate) must complete in
 * O(rows x indexes x log columns), not O(rows x indexes x columns).
 *
 * <p>After Avro-13 the setRows rebuild goes through
 * {@code rebuildAllIndexesFromArrays}: positional reads against the schema-level
 * column index, zero Map key scanning. Discriminator = <b>composite-width
 * swing</b> (w=5 vs w=50 covered columns of ONE composite index) over rows with
 * <b>lowercase keys</b> (the old Map path would pay a case-insensitive scan per
 * covered column per row while {@code fromMap}'s own case-insensitive
 * {@code colIndex} still lands the values correctly). Old shape ~5-10x (w x
 * scan), new shape ~2-3x (w x positional lookup on top of the
 * index-count-independent {@code fromMap} floor). Bound 3x separates them.</p>
 *
 * <p><b>Deviation (measured on the sibling rebuild test):</b> an index-count
 * axis cannot discriminate — the inherent index-insert cost is linear in the
 * index count for both shapes (measured new-shape ratio 8.09 vs old ~10) and
 * the {@code fromMap} conversion floor additionally saturates small deltas.
 * Width scaling keeps one insert per row and a fixed floor.</p>
 */
@Tag("storage")
@StorageType("avro")
class SetRowsIsORowsTimesIndexesTest {

    private static final int COLS = 100;
    private static final int ROWS = 5_000;
    private static final int W_SMALL = 5;
    private static final int W_LARGE = 50;

    /** Expected upper bound for t(width=50) / t(width=5); old scan shape ~5x+. */
    private static final double MAX_RATIO = 3.0;

    private static String value(int col, int row) {
        return col + ":" + row;
    }

    private static List<String> columns() {
        List<String> cols = new ArrayList<>(COLS);
        for (int i = 0; i < COLS; i++) {
            cols.add("COL_" + i);
        }
        return cols;
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> types = new LinkedHashMap<>();
        for (String col : columns()) {
            types.put(col, String.class);
        }
        return types;
    }

    /** Lowercase row keys: exercises the case-insensitive colIndex path of fromMap. */
    private static List<Map<String, Object>> lowercaseRows(int rowCount) {
        String[] lowerKeys = new String[COLS];
        for (int i = 0; i < COLS; i++) {
            lowerKeys[i] = "col_" + i;
        }
        List<Map<String, Object>> rows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            Map<String, Object> row = new HashMap<>();
            for (int c = 0; c < COLS; c++) {
                row.put(lowerKeys[c], value(c, r));
            }
            rows.add(row);
        }
        return rows;
    }

    private static AvroRowStorage storageWithComposite(int width) {
        List<String> cols = columns();
        AvroRowStorage storage = new AvroRowStorage("AVRO_SETROWS_T", cols, types());
        storage.createCompositeIndex("CX", cols.subList(0, width));
        return storage;
    }

    private static void warmup(List<Map<String, Object>> rows) {
        storageWithComposite(W_SMALL).setRows(rows);
        storageWithComposite(W_LARGE).setRows(rows);
    }

    private static long timedSetRows(int width, List<Map<String, Object>> rows) {
        long first = runSetRows(width, rows);
        long second = runSetRows(width, rows);
        return Math.min(first, second);
    }

    /** Small-of-2; storage creation + index registration stay outside the timer. */
    private static long runSetRows(int width, List<Map<String, Object>> rows) {
        AvroRowStorage storage = storageWithComposite(width);
        long start = System.nanoTime();
        storage.setRows(rows);
        return System.nanoTime() - start;
    }

    @Test
    @Timeout(60)
    void setRowsCostScalesWithLogOfCoveredWidthNotWithWidthTimesScan() {
        List<Map<String, Object>> rows = lowercaseRows(ROWS);

        warmup(rows);

        long small = timedSetRows(W_SMALL, rows);
        long big = timedSetRows(W_LARGE, rows);
        double ratio = (double) big / Math.max(1L, small);

        assertTrue(ratio <= MAX_RATIO,
                String.format(Locale.ROOT,
                        "setRows ratio t(width=%d)/t(width=%d)=%.2f must be <= %.1f "
                                + "(old Map rebuild = w x row_size CI scan per row; new "
                                + "rebuildAllIndexesFromArrays = positional O(log columns), "
                                + "o2 Avro-19). small=%.1f ms, big=%.1f ms",
                        W_LARGE, W_SMALL, ratio, MAX_RATIO, small / 1e6, big / 1e6));
        assertTrue(big < 5_000_000_000L,
                "setRows of 5k x 100-col rows over a 50-wide composite must stay under 5s, was "
                        + String.format(Locale.ROOT, "%.0f ms", big / 1e6));
    }

    @Test
    void setRowsBuildsSearchableIndexesFromLowercaseMaps() {
        List<String> cols = columns();
        AvroRowStorage storage = new AvroRowStorage("AVRO_SETROWS_T", cols, types());
        storage.createSecondaryIndex("IDX_0", cols.get(0));
        storage.createSecondaryIndex("IDX_1", cols.get(1));
        storage.createCompositeIndex("PAIR", List.of(cols.get(0), cols.get(1)));

        List<Map<String, Object>> rows = lowercaseRows(500);
        storage.setRows(rows);

        assertEquals(500, storage.scan().size(), "all rows must be stored");
        assertEquals(500, storage.getSecondaryIndexManager().getIndex("IDX_0").size());
        assertEquals(500, storage.getSecondaryIndexManager().getIndex("PAIR").size());

        for (int r : new int[]{0, 1, 7, 250, 499}) {
            assertEquals(List.of(r),
                    storage.getSecondaryIndexManager().getIndex("IDX_0").search(value(0, r)),
                    "row " + r + " must be reachable through the array-built index");
        }

        // rows replaced wholesale: a second setRows must not duplicate postings
        storage.setRows(lowercaseRows(100));
        assertEquals(100, storage.scan().size());
        assertEquals(100, storage.getSecondaryIndexManager().getIndex("IDX_0").size());
        assertEquals(List.of(3), storage.getSecondaryIndexManager().getIndex("IDX_0").search(value(0, 3)));
        assertTrue(storage.getSecondaryIndexManager().getIndex("IDX_0").search(value(0, 400)).isEmpty(),
                "stale postings from the replaced row set must be gone");
    }
}
