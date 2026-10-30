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
 * o2 Avro-14 acceptance: {@code syncOnInsert} (and the sibling syncOn* paths)
 * must not multiply a case-insensitive entrySet scan by the number of covered
 * columns.
 *
 * <p>Discriminator = <b>composite-width swing</b> (w=5 vs w=50 covered columns
 * of ONE composite index) on inserts of a <b>case-mismatched row</b>. Old shape:
 * per insert cost = w x scan(row_size) -> ~10x swing. New shape: one lazy
 * {@code RowCiView} per insert call (width-independent build, amortized over the
 * covered lookups) + O(log columns) per covered column -> ~2-3x. Bound 4x passes
 * the new shape and fails the old one.</p>
 *
 * <p><b>Deviation (measured on the sibling rebuild test):</b> an index-count
 * axis cannot discriminate — the inherent O(rows x indexes) insert cost is
 * linear in the index count for both shapes and washes the lookup delta out
 * (measured new-shape ratio 8.09 vs old ~10). Width scaling keeps one insert
 * per row per index, so the lookup-shape delta stays visible.</p>
 *
 * <p>The timing batch intentionally reuses a single row object: the measured
 * work is the per-call view build + index sync, and posting-list growth is
 * identical for both swings. Correctness runs on distinct rows separately.</p>
 */
@Tag("storage")
@StorageType("avro")
class SyncOnInsertIsOIndexesTest {

    private static final int COLS = 50;
    private static final int INSERTS = 8_000;
    private static final int W_SMALL = 5;
    private static final int W_LARGE = 50;

    /** Expected upper bound for t(width=50) / t(width=5); old shape ~10x. */
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
                new AvroSecondaryIndexManager("AVRO_SYNC_T", columns(), types());
        manager.createCompositeIndex("CX", columns().subList(0, width));
        return manager;
    }

    private static AvroSecondaryIndexManager managerWithIndexes(int indexCount) {
        AvroSecondaryIndexManager manager =
                new AvroSecondaryIndexManager("AVRO_SYNC_T", columns(), types());
        for (int i = 0; i < indexCount; i++) {
            manager.createIndex("IDX_" + i, UPPER_COLS[i]);
        }
        return manager;
    }

    /** Single case-mismatched row shared by the whole timing batch (see class javadoc). */
    private static Map<String, Object> ciMismatchRow() {
        Map<String, Object> row = new HashMap<>();
        for (int c = 0; c < COLS; c++) {
            row.put(LOWER_KEYS[c], value(c, 0));
        }
        return row;
    }

    private static void warmup(Map<String, Object> row) {
        syncBatch(W_SMALL, row, 300);
        syncBatch(W_LARGE, row, 300);
    }

    private static long syncBatch(int width, Map<String, Object> row, int inserts) {
        AvroSecondaryIndexManager manager = managerWithComposite(width);
        long start = System.nanoTime();
        for (int r = 0; r < inserts; r++) {
            manager.syncOnInsert(row, r);
        }
        return System.nanoTime() - start;
    }

    /** Small-of-2 over full batches; manager creation stays outside the timer. */
    private static long timedSyncBatch(int width, Map<String, Object> row) {
        long first = syncBatchSetupOutside(width, row);
        long second = syncBatchSetupOutside(width, row);
        return Math.min(first, second);
    }

    private static long syncBatchSetupOutside(int width, Map<String, Object> row) {
        AvroSecondaryIndexManager manager = managerWithComposite(width);
        long start = System.nanoTime();
        for (int r = 0; r < INSERTS; r++) {
            manager.syncOnInsert(row, r);
        }
        return System.nanoTime() - start;
    }

    @Test
    @Timeout(60)
    void syncOnInsertCostScalesWithLogOfCoveredWidthNotWithWidthTimesScan() {
        Map<String, Object> row = ciMismatchRow();

        warmup(row);

        long small = timedSyncBatch(W_SMALL, row);
        long big = timedSyncBatch(W_LARGE, row);
        double ratio = (double) big / Math.max(1L, small);

        assertTrue(ratio <= MAX_RATIO,
                String.format(Locale.ROOT,
                        "syncOnInsert ratio t(width=%d)/t(width=%d)=%.2f must be <= %.1f "
                                + "(old = w x row_size scan per insert ~10x; new = one CI view per "
                                + "call, o2 Avro-14). small=%.1f ms, big=%.1f ms",
                        W_LARGE, W_SMALL, ratio, MAX_RATIO, small / 1e6, big / 1e6));
        assertTrue(big < 5_000_000_000L,
                "8k inserts over a 50-wide composite must stay under 5s, was "
                        + String.format(Locale.ROOT, "%.0f ms", big / 1e6));
    }

    @Test
    void syncLifecycleIsExactForCiMismatchedRows() {
        AvroSecondaryIndexManager manager = managerWithIndexes(3);
        List<Map<String, Object>> rows = new ArrayList<>(500);
        for (int r = 0; r < 500; r++) {
            Map<String, Object> row = new HashMap<>();
            for (int c = 0; c < COLS; c++) {
                row.put(LOWER_KEYS[c], value(c, r));
            }
            rows.add(row);
            manager.syncOnInsert(row, r);
        }

        for (int r = 0; r < 500; r++) {
            assertEquals(List.of(r), manager.getIndex("IDX_0").search(value(0, r)),
                    "inserted row " + r + " must be searchable via the CI path");
        }
        assertEquals(500, manager.getIndex("IDX_0").size());

        // update: value changes -> old posting drops, new posting appears
        Map<String, Object> oldRow = rows.get(42);
        Map<String, Object> newRow = new HashMap<>(oldRow);
        newRow.put(LOWER_KEYS[0], value(0, 999));
        manager.syncOnUpdate(oldRow, newRow, 42);
        assertEquals(List.of(), manager.getIndex("IDX_0").search(value(0, 42)));
        assertEquals(List.of(42), manager.getIndex("IDX_0").search(value(0, 999)));

        // delete: restore the value the index currently holds for row 42
        manager.syncOnDelete(newRow, 42);
        assertEquals(List.of(), manager.getIndex("IDX_0").search(value(0, 999)));
        assertEquals(499, manager.getIndex("IDX_0").size());

        // untouched rows keep working
        assertEquals(List.of(7), manager.getIndex("IDX_1").search(value(1, 7)));
    }
}
