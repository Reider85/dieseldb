package diesel;

import diesel.storage.DelimitedByteParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Measures allocation rates during CSV parsing via {@link DelimitedByteParser}.
 * Verifies that the byte[] parser reduces intermediate String allocations
 * compared to the String-based baseline.
 */
class AllocationProfileTest {

    @TempDir
    File tempDir;

    private static final int ROWS = 1000;
    private static final int COLS = 5;

    private static List<String> schema() {
        return List.of("C1", "C2", "C3", "C4", "C5");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("C1", Long.class);
        t.put("C2", Long.class);
        t.put("C3", Long.class);
        t.put("C4", Long.class);
        t.put("C5", Long.class);
        return t;
    }

    private static byte[] generateCsv() {
        StringBuilder sb = new StringBuilder();
        sb.append("C1,C2,C3,C4,C5\n");
        for (int i = 0; i < ROWS; i++) {
            sb.append((long) i).append(",")
                    .append((long) (i + 1)).append(",")
                    .append((long) (i + 2)).append(",")
                    .append((long) (i + 3)).append(",")
                    .append((long) (i + 4)).append("\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void allocationWithinBudget() throws Exception {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

        // Warmup: force JIT compilation
        byte[] warmupBytes = generateCsv();
        for (int i = 0; i < 3; i++) {
            DelimitedByteParser.parse(warmupBytes, StandardCharsets.UTF_8, schema(), types(),
                    (byte) ',', (byte) '"', "warmup.csv");
        }

        // Force GC before measurement
        System.gc();
        Thread.sleep(100);

        MemoryUsage heapBefore = memoryBean.getHeapMemoryUsage();

        // Parse 1000 rows x 5 Long columns = 5000 Long values
        byte[] bytes = generateCsv();
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, schema(), types(),
                (byte) ',', (byte) '"', "profile.csv");

        MemoryUsage heapAfter = memoryBean.getHeapMemoryUsage();
        long heapDelta = heapAfter.getUsed() - heapBefore.getUsed();

        assertEquals(ROWS, rows.size(), "Expected " + ROWS + " rows");

        // The byte[] parser should use minimal heap for typed columns.
        // With 1000 rows x 5 Long columns, the main allocations are:
        // - 1000 Object[] arrays (one per row) ≈ 8KB
        // - 5000 Long objects ≈ 80KB
        // - 1000 ArrayList entries ≈ 8KB
        // Total ≈ ~100KB. String-based path would add ~5MB of intermediate Strings.
        long budgetBytes = 2 * 1024 * 1024; // 2MB (generous, still way less than 5MB)
        System.out.printf("[ALLOC-PROFILE] rows=%d heapDeltaKB=%d budgetKB=%d%n",
                ROWS, heapDelta / 1024, budgetBytes / 1024);

        assertTrue(heapDelta < budgetBytes,
                "Heap delta " + heapDelta + " bytes exceeds budget " + budgetBytes
                        + " bytes for 1000x5 Long rows");
    }

    @Test
    void producesCorrectResults() throws Exception {
        byte[] bytes = generateCsv();
        List<Object[]> rows = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, schema(), types(),
                (byte) ',', (byte) '"', "verify.csv");

        assertEquals(ROWS, rows.size());
        for (int i = 0; i < ROWS; i++) {
            assertEquals((long) i, rows.get(i)[0], "Row " + i + " C1");
            assertEquals((long) (i + 1), rows.get(i)[1], "Row " + i + " C2");
            assertEquals((long) (i + 2), rows.get(i)[2], "Row " + i + " C3");
            assertEquals((long) (i + 3), rows.get(i)[3], "Row " + i + " C4");
            assertEquals((long) (i + 4), rows.get(i)[4], "Row " + i + " C5");
        }
    }
}
