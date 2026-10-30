package diesel;

import diesel.storage.avro.AvroSecondaryIndexManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * o2 Avro-15 acceptance: {@code isCompatible} must scale with
 * O(indexes x covered x log columns), not O(indexes x covered x columns).
 *
 * <p>The discriminator is an <b>index-count swing</b> (10 vs 100 single-column
 * indexes over the LAST columns of a fixed 1000-column table — last columns
 * maximize the old linear {@code findColumnIndex} scan). Old shape:
 * indexes x ~columns per call -> ~9x swing. New shape: the pre-built case-insensitive
 * {@code columnIndex} makes both the positional check and {@code findColumnIndex}
 * O(log columns) — the swing collapses to ~1.1x. Bound 4x separates them.</p>
 *
 * <p>The call loop runs {@link #CALLS} iterations per timed batch so absolute
 * times stay far above timer resolution.</p>
 */
@Tag("storage")
@StorageType("avro")
class IsCompatibleIsOIndexesTimesCoveredTest {

    private static final int COLS = 1_000;
    private static final int CALLS = 1_000;

    /** Expected upper bound for t(indexes=100) / t(indexes=10); old shape ~9x. */
    private static final double MAX_RATIO = 4.0;

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

    /** K indexes over the LAST K columns so the old scan has to walk ~all columns. */
    private static AvroSecondaryIndexManager managerWithIndexes(int indexCount) {
        List<String> cols = columns();
        AvroSecondaryIndexManager manager =
                new AvroSecondaryIndexManager("AVRO_COMPAT_T", cols, types());
        for (int i = 0; i < indexCount; i++) {
            manager.createIndex("IDX_" + i, cols.get(COLS - indexCount + i));
        }
        return manager;
    }

    private static void warmup(List<String> cols, Map<String, Class<?>> types) {
        runCalls(managerWithIndexes(10), cols, types, 100);
        runCalls(managerWithIndexes(100), cols, types, 100);
    }

    private static long runCalls(AvroSecondaryIndexManager manager,
                                 List<String> cols, Map<String, Class<?>> types, int calls) {
        long start = System.nanoTime();
        boolean compatible = false;
        for (int i = 0; i < calls; i++) {
            compatible = manager.isCompatible(cols, types);
        }
        if (!compatible) {
            throw new IllegalStateException("fixture must be compatible");
        }
        return System.nanoTime() - start;
    }

    /** Small-of-2 of {@link #CALLS}-call batches; manager creation outside the timer. */
    private static long timedCompat(int indexCount, List<String> cols, Map<String, Class<?>> types) {
        long first = runCalls(managerWithIndexes(indexCount), cols, types, CALLS);
        long second = runCalls(managerWithIndexes(indexCount), cols, types, CALLS);
        return Math.min(first, second);
    }

    @Test
    @Timeout(60)
    void isCompatibleCostDependsOnIndexCountNotOnIndexesTimesColumns() {
        List<String> cols = columns();
        Map<String, Class<?>> types = types();

        warmup(cols, types);

        long small = timedCompat(10, cols, types);
        long big = timedCompat(100, cols, types);
        double ratio = (double) big / Math.max(1L, small);

        assertTrue(ratio <= MAX_RATIO,
                String.format(Locale.ROOT,
                        "isCompatible ratio t(indexes=100)/t(indexes=10)=%.2f must be <= %.1f "
                                + "(old = indexes x columns linear scan ~9x; new = case-insensitive "
                                + "columnIndex O(log columns), o2 Avro-15). small=%.1f ms, big=%.1f ms",
                        ratio, MAX_RATIO, small / 1e6, big / 1e6));
        assertTrue(big < 5_000_000_000L,
                "1000 isCompatible batches must stay under 5s, was "
                        + String.format(Locale.ROOT, "%.0f ms", big / 1e6));
    }

    @Test
    void compatibilityRejectionsStayExact() {
        List<String> cols = columns();
        Map<String, Class<?>> types = types();
        AvroSecondaryIndexManager manager = managerWithIndexes(10);

        assertTrue(manager.isCompatible(cols, types), "identical schema must be compatible");

        // size mismatch
        assertFalse(manager.isCompatible(cols.subList(0, COLS - 1), types));

        // unknown column in the expected schema (same size)
        List<String> renamed = new ArrayList<>(cols);
        renamed.set(COLS - 1, "NOPE");
        assertFalse(manager.isCompatible(renamed, types), "renamed/unknown column must reject");

        // reordered columns break the positional contract
        List<String> reordered = new ArrayList<>(cols);
        reordered.set(COLS - 2, cols.get(COLS - 1));
        reordered.set(COLS - 1, cols.get(COLS - 2));
        assertFalse(manager.isCompatible(reordered, types), "reordered columns must reject");

        // type mismatch on a covered column (last column is indexed)
        Map<String, Class<?>> wrongTypes = new LinkedHashMap<>(types);
        wrongTypes.put(cols.get(COLS - 1), Integer.class);
        assertFalse(manager.isCompatible(cols, wrongTypes), "covered-column type mismatch must reject");
    }
}
