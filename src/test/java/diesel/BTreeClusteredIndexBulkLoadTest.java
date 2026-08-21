package diesel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BTreeClusteredIndexBulkLoadTest {

    @Test
    void emptyTable_bulkLoad_producesEmptyTree() {
        BTreeClusteredIndex index = new BTreeClusteredIndex(Integer.class);
        index.bulkLoad(List.of(), List.of());
        assertTrue(index.validate());
        assertTrue(index.search(42).isEmpty());
    }

    @Test
    void singleRow_bulkLoad_producesSingleLeaf() {
        BTreeClusteredIndex index = new BTreeClusteredIndex(Integer.class);
        index.bulkLoad(List.of(10), List.of(0));
        assertTrue(index.validate());
        assertEquals(List.of(0), index.search(10));
    }

    @Test
    void exactFullLeaf_bulkLoad_producesCorrectStructure() {
        // 2t-1 = 5 keys fills exactly one leaf
        BTreeClusteredIndex index = new BTreeClusteredIndex(Integer.class);
        List<Object> keys = List.of(1, 2, 3, 4, 5);
        List<Integer> indices = List.of(0, 1, 2, 3, 4);
        index.bulkLoad(keys, indices);
        assertTrue(index.validate());
        for (int i = 0; i < 5; i++) {
            assertEquals(List.of(i), index.search(i + 1));
        }
    }

    @Test
    void multipleLeafLevels_bulkLoad_producesBalancedTree() {
        // 20 keys -> 4 leaves of 5, then 1 internal root with 3 keys + 4 children
        BTreeClusteredIndex index = new BTreeClusteredIndex(Integer.class);
        int n = 20;
        List<Object> keys = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            keys.add(i + 1);
            indices.add(i);
        }
        index.bulkLoad(keys, indices);
        assertTrue(index.validate());
        for (int i = 0; i < n; i++) {
            assertEquals(List.of(i), index.search(i + 1));
        }
    }

    @Test
    void largeDataSet_bulkLoad_allKeysSearchable() {
        int n = 500;
        List<Object> keys = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            keys.add(i * 3 + 1);  // non-consecutive keys
            indices.add(i);
        }

        BTreeClusteredIndex bulkIndex = new BTreeClusteredIndex(Integer.class);
        bulkIndex.bulkLoad(keys, indices);

        assertTrue(bulkIndex.validate());

        // Every key must be searchable (including keys that become separators)
        for (int i = 0; i < n; i++) {
            assertEquals(List.of(i), bulkIndex.search(keys.get(i)),
                    "Key " + keys.get(i) + " should be found at index " + i);
        }
    }

    @Test
    void bulkLoad_withSubsequentInsert_works() {
        BTreeClusteredIndex index = new BTreeClusteredIndex(Integer.class);
        index.bulkLoad(List.of(10, 20, 30), List.of(0, 1, 2));
        assertTrue(index.validate());

        // Insert additional key
        index.insert(25, 3);
        assertTrue(index.validate());
        assertEquals(List.of(3), index.search(25));
        assertEquals(List.of(2), index.search(30));
    }

    @Test
    void bulkLoad_withSubsequentRemove_works() {
        BTreeClusteredIndex index = new BTreeClusteredIndex(Integer.class);
        index.bulkLoad(List.of(10, 20, 30, 40, 50), List.of(0, 1, 2, 3, 4));
        assertTrue(index.validate());

        index.remove(30, 2);
        assertTrue(index.validate());
        assertTrue(index.search(30).isEmpty());
        assertEquals(List.of(1), index.search(20));
        assertEquals(List.of(3), index.search(40));
    }

    @Test
    void bulkLoad_mismatchedSizes_throwsException() {
        BTreeClusteredIndex index = new BTreeClusteredIndex(Integer.class);
        assertThrows(IllegalArgumentException.class,
                () -> index.bulkLoad(List.of(1, 2), List.of(0)));
    }

    @Test
    void largeDataSet_bulkLoad_validatePasses() {
        int n = 10_000;
        List<Object> keys = new ArrayList<>(n);
        List<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(i);
            indices.add(i);
        }
        BTreeClusteredIndex index = new BTreeClusteredIndex(Integer.class);
        index.bulkLoad(keys, indices);
        assertTrue(index.validate());
    }
}
