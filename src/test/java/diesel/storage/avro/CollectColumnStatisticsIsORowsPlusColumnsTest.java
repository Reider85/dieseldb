package diesel.storage.avro;

import diesel.StorageType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 Avro-3 acceptance: {@code AvroStatistics.collectColumnStatistics} must run
 * in O(rows x columns), not O(rows x columns x fields).
 *
 * <p>Pre-optimization the row loop called {@code avroRecord.get(col)} by name
 * and {@code getBaseType(schema, col)} per column per record; the fix hoists
 * both into {@code fieldPositions[]}/{@code baseTypes[]} resolved once against
 * the file schema, so the hot loop does positional {@code get(int)} lookups
 * only.
 *
 * <p><b>Documented deviation:</b> {@code org.apache.avro.Schema.getField} is
 * an O(1) {@code fieldMap} lookup (not the O(fields) linear scan the prompt
 * assumed), so the pre-fix shape was already O(rows x columns) asymptotically -
 * the hoist removes the per-cell constant overhead (name lookup + union branch
 * walk). The width-swing guard below fails a genuinely quadratic per-cell
 * implementation (e.g. a hand-rolled field scan), which is the shape the
 * prompt's acceptance criterion targets; it cannot distinguish the old
 * constant-factor from the new one (that gap is covered by the wall bound).
 */
@Tag("storage")
@StorageType("avro")
class CollectColumnStatisticsIsORowsPlusColumnsTest {

    private static final int ROWS = 10_000;

    @TempDir
    Path tempDir;

    /**
     * Writes {@code rows} x {@code cols} Long-typed columns; cell (i, j)
     * holds {@code i * 1000 + j} so every column has a distinct, checkable
     * min (j at i=0) and max ((rows-1)*1000 + j).
     */
    private File writeFile(int rows, int cols) throws IOException {
        File f = new File(tempDir.toFile(), "stats_" + rows + "x" + cols + ".avro");
        List<String> columns = new ArrayList<>(cols);
        Map<String, Class<?>> types = new LinkedHashMap<>();
        for (int j = 0; j < cols; j++) {
            String col = "C" + j;
            columns.add(col);
            types.put(col, Long.class);
        }
        AvroDataFileWriter w = new AvroDataFileWriter(columns, types, f);
        try {
            for (int i = 0; i < rows; i++) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int j = 0; j < cols; j++) {
                    row.put("C" + j, (long) i * 1000 + j);
                }
                w.writeRow(row);
            }
            w.flush();
        } finally {
            w.close();
        }
        return f;
    }

    private static long collectMillis(File f, List<String> columns,
                                      Map<String, Class<?>> types) throws IOException {
        AvroStatistics stats = new AvroStatistics();
        long start = System.nanoTime();
        stats.collectColumnStatistics(f, columns, types);
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        LAST.set(stats);
        return elapsed;
    }

    /** Last collected stats, so callers can assert contents after timing. */
    private static final ThreadLocal<AvroStatistics> LAST = new ThreadLocal<>();

    private static List<String> cols(int n) {
        List<String> c = new ArrayList<>(n);
        for (int j = 0; j < n; j++) {
            c.add("C" + j);
        }
        return c;
    }

    private static Map<String, Class<?>> longTypes(int n) {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        for (int j = 0; j < n; j++) {
            t.put("C" + j, Long.class);
        }
        return t;
    }

    /**
     * Spec shape (10^4 rows x 10^2 columns): the collection must finish well
     * inside the wall bound. The 3000ms bound sits far above the expected
     * few-hundred-milliseconds (file decode dominates) so it fails a
     * per-cell quadratic regression without being load-sensitive;
     * {@code @Timeout} catches a hang even faster.
     */
    @Test
    @Timeout(120)
    void collectOver10kRowsAnd100ColsCompletesWithinWallBound() throws IOException {
        File f = writeFile(ROWS, 100);
        long elapsed = collectMillis(f, cols(100), longTypes(100));
        AvroStatistics stats = LAST.get();

        assertEquals(ROWS, stats.getRowCount(), "row count");
        assertTrue(elapsed < 3_000,
                "collectColumnStatistics over " + ROWS + "x100 took " + elapsed
                        + " ms, expected < 3000 ms (O(rows x columns), o2 Avro-3)");

        Map<String, AvroStatistics.ColumnStats> all = stats.getColumnStatistics();
        AvroStatistics.ColumnStats c0 = all.get("C0");
        AvroStatistics.ColumnStats c99 = all.get("C99");
        assertEquals(0L, c0.getNullCount(), "no nulls written");
        assertEquals(0L, c99.getNullCount(), "no nulls written");
        assertEquals(0L, ((Number) c0.getMin()).longValue(), "C0 min = cell(0,0)");
        assertEquals((ROWS - 1) * 1000L, ((Number) c0.getMax()).longValue(), "C0 max");
        assertEquals(99L, ((Number) c99.getMin()).longValue(), "C99 min = cell(0,99)");
        assertEquals((ROWS - 1) * 1000L + 99, ((Number) c99.getMax()).longValue(), "C99 max");
    }

    /**
     * Width swing at fixed row count: cost must grow ~linearly with the
     * column count (t(100 cols) / t(10 cols) bounded), because a per-cell
     * quadratic shape (O(rows x columns^2)) grows 100x while a linear shape
     * grows 10x (plus equal-ish file-decode overhead). Bound 30 sits between
     * the two shapes; each side is measured as the best of two runs with an
     * unmeasured warmup to keep sub-second measurements stable.
     */
    @Test
    @Timeout(180)
    void collectionCostGrowsLinearlyWithColumnCount() throws IOException {
        File small = writeFile(ROWS, 10);
        File wide = writeFile(ROWS, 100);

        // Unmeasured warmup on a tiny file (JIT + class loading).
        File tiny = writeFile(200, 10);
        collectMillis(tiny, cols(10), longTypes(10));

        List<String> c10 = cols(10);
        Map<String, Class<?>> t10 = longTypes(10);
        List<String> c100 = cols(100);
        Map<String, Class<?>> t100 = longTypes(100);

        long best10 = Long.MAX_VALUE;
        for (int i = 0; i < 2; i++) {
            best10 = Math.min(best10, collectMillis(small, c10, t10));
        }
        long best100 = Long.MAX_VALUE;
        for (int i = 0; i < 2; i++) {
            best100 = Math.min(best100, collectMillis(wide, c100, t100));
        }

        double ratio = (double) best100 / Math.max(best10, 1);
        assertTrue(ratio <= 30.0,
                "t(100 cols)=" + best100 + "ms / t(10 cols)=" + best10
                        + "ms = " + ratio + "x, expected <= 30x (linear-in-columns"
                        + " shape; O(columns^2) would approach 100x, o2 Avro-3)");
    }
}
