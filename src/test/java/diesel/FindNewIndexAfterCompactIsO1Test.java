package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MVCC-4 verification test: findNewIndexAfterCompact is O(1) using prefix array.
 * Uses reflection to test the private method with large datasets.
 */
@Tag("query")
@Isolated  // wall-clock timing assertions starve under parallel-class CPU contention
class FindNewIndexAfterCompactIsO1Test {

    @Test
    void findNewIndexAfterCompactIsConstantTime() throws Exception {
        // Create a minimal table instance
        Database db = new Database();
        Table table = new Table(db, "test", List.of("ID", "VAL"), 
                Map.of("ID", Long.class, "VAL", String.class), 
                "ID", null);

        // Set up a large deletedRows BitSet via reflection
        Field deletedRowsField = Table.class.getDeclaredField("deletedRows");
        deletedRowsField.setAccessible(true);
        BitSet deletedRows = new BitSet(200_000);

        // Mark every other row as deleted (50% density)
        for (int i = 0; i < 200_000; i++) {
            if (i % 2 == 0) {
                deletedRows.set(i);
            }
        }
        deletedRowsField.set(table, deletedRows);

        // Get the private method via reflection
        Method findNewIndexMethod = Table.class.getDeclaredMethod(
                "findNewIndexAfterCompact", int.class, int[].class);
        findNewIndexMethod.setAccessible(true);

        // Build the prefix array (what the actual implementation does)
        int[] prefixAlive = buildPrefixAliveForTest(deletedRows, 200_000);

        // Warmup: 5k calls
        int warmupCount = 5_000;
        for (int i = 0; i < warmupCount; i++) {
            int oldIndex = ThreadLocalRandom.current().nextInt(200_000);
            findNewIndexMethod.invoke(table, oldIndex, prefixAlive);
        }

        // Measurement: 20k calls to a deleted row (should return -1)
        int measurementCount = 20_000;
        long totalNanos = 0;
        int testIndex = 199_998; // Deleted index (even)
        
        for (int i = 0; i < measurementCount; i++) {
            long start = System.nanoTime();
            Object result = findNewIndexMethod.invoke(table, testIndex, prefixAlive);
            long end = System.nanoTime();
            totalNanos += (end - start);
            assertEquals(-1, result, "Deleted row should return -1");
        }

        double avgMicros = (totalNanos / measurementCount) / 1000.0;
        assertTrue(avgMicros < 10.0, 
                "Average call time " + avgMicros + "µs, expected < 10µs (O(1) proof)");

        // Also test a non-deleted row for correctness
        int aliveIndex = 199_999; // Should be alive (odd)
        Object result = findNewIndexMethod.invoke(table, aliveIndex, prefixAlive);
        int expectedNewIndex = prefixAlive[aliveIndex];
        assertEquals(expectedNewIndex, result, "Alive row should return prefix value");

        // Verify method signature (2 parameters: int, int[])
        Method[] methods = Table.class.getDeclaredMethods();
        boolean foundMethod = false;
        for (Method m : methods) {
            if (m.getName().equals("findNewIndexAfterCompact") 
                    && m.getParameterCount() == 2
                    && m.getParameterTypes()[0] == int.class
                    && m.getParameterTypes()[1] == int[].class) {
                foundMethod = true;
                break;
            }
        }
        assertTrue(foundMethod, "Method signature should be findNewIndexAfterCompact(int, int[])");
    }

    /**
     * Helper to build prefix array matching the implementation logic.
     * prefixAlive[i+1] = prefixAlive[i] + (isDeleted(i) ? 0 : 1)
     */
    private int[] buildPrefixAliveForTest(BitSet deletedRows, int size) {
        int[] prefixAlive = new int[size + 1];
        for (int i = 0; i < size; i++) {
            prefixAlive[i + 1] = prefixAlive[i] + (deletedRows.get(i) ? 0 : 1);
        }
        return prefixAlive;
    }
}