package diesel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BTreeIndexBulkLoadTest {

    @Test
    void emptyInput_bulkLoad_producesEmptyTree() {
        BTreeIndex index = new BTreeIndex(Integer.class);
        index.bulkLoad(List.of(), List.of());
        assertTrue(index.search(42).isEmpty());
    }

    @Test
    void singleKey_bulkLoad_producesSingleEntry() {
        BTreeIndex index = new BTreeIndex(Integer.class);
        index.bulkLoad(List.of(10), List.of(0));
        assertEquals(List.of(0), index.search(10));
    }

    @Test
    void uniqueKeys_bulkLoad_allKeysSearchable() {
        BTreeIndex index = new BTreeIndex(Integer.class);
        List<Object> keys = List.of(1, 2, 3, 4, 5);
        List<Integer> indices = List.of(0, 1, 2, 3, 4);
        index.bulkLoad(keys, indices);
        for (int i = 0; i < 5; i++) {
            assertEquals(List.of(i), index.search(i + 1));
        }
    }

    @Test
    void duplicateKeys_bulkLoad_mergesRowIndices() {
        BTreeIndex index = new BTreeIndex(Integer.class);
        // Keys: 10, 10, 10, 20, 20 — three rows for key 10, two for key 20
        List<Object> keys = List.of(10, 10, 10, 20, 20);
        List<Integer> indices = List.of(0, 1, 2, 3, 4);
        index.bulkLoad(keys, indices);

        List<Integer> result10 = index.search(10);
        assertEquals(3, result10.size());
        assertTrue(result10.containsAll(List.of(0, 1, 2)));

        List<Integer> result20 = index.search(20);
        assertEquals(2, result20.size());
        assertTrue(result20.containsAll(List.of(3, 4)));
    }

    @Test
    void largeDataSet_bulkLoad_allKeysSearchable() {
        int n = 1000;
        List<Object> keys = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            keys.add(i * 3 + 1);
            indices.add(i);
        }
        BTreeIndex index = new BTreeIndex(Integer.class);
        index.bulkLoad(keys, indices);
        for (int i = 0; i < n; i++) {
            assertEquals(List.of(i), index.search(keys.get(i)),
                    "Key " + keys.get(i) + " should be found at index " + i);
        }
    }

    @Test
    void bulkLoad_withSubsequentInsert_works() {
        BTreeIndex index = new BTreeIndex(Integer.class);
        index.bulkLoad(List.of(10, 20, 30), List.of(0, 1, 2));

        index.insert(25, 3);
        assertEquals(List.of(3), index.search(25));
        assertEquals(List.of(2), index.search(30));
    }

    @Test
    void bulkLoad_withSubsequentRemove_works() {
        BTreeIndex index = new BTreeIndex(Integer.class);
        index.bulkLoad(List.of(10, 20, 30, 40, 50), List.of(0, 1, 2, 3, 4));

        index.remove(30, 2);
        assertTrue(index.search(30).isEmpty());
        assertEquals(List.of(1), index.search(20));
        assertEquals(List.of(3), index.search(40));
    }

    @Test
    void bulkLoad_mismatchedSizes_throwsException() {
        BTreeIndex index = new BTreeIndex(Integer.class);
        assertThrows(IllegalArgumentException.class,
                () -> index.bulkLoad(List.of(1, 2), List.of(0)));
    }

    @Test
    void rangeSearch_afterBulkLoad_returnsCorrectRange() {
        BTreeIndex index = new BTreeIndex(Integer.class);
        List<Object> keys = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            keys.add(i);
            indices.add(i);
        }
        index.bulkLoad(keys, indices);

        List<Integer> range = index.rangeSearch(10, 20);
        assertEquals(11, range.size());
        for (int i = 10; i <= 20; i++) {
            assertTrue(range.contains(i));
        }
    }

    @Test
    void duplicateKeys_rangeSearch_includesAllMatchingRows() {
        BTreeIndex index = new BTreeIndex(Integer.class);
        // 5 rows with key=5, 3 rows with key=15, 2 rows with key=25
        List<Object> keys = List.of(5, 5, 5, 5, 5, 15, 15, 15, 25, 25);
        List<Integer> indices = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        index.bulkLoad(keys, indices);

        List<Integer> range = index.rangeSearch(5, 15);
        assertEquals(8, range.size());
        assertTrue(range.containsAll(List.of(0, 1, 2, 3, 4, 5, 6, 7)));
    }

    @Test
    void largeDataSet_bulkLoad_10000_keys() {
        int n = 10_000;
        List<Object> keys = new ArrayList<>(n);
        List<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(i);
            indices.add(i);
        }
        BTreeIndex index = new BTreeIndex(Integer.class);
        index.bulkLoad(keys, indices);
        // Spot-check a few keys
        assertEquals(List.of(0), index.search(0));
        assertEquals(List.of(5000), index.search(5000));
        assertEquals(List.of(9999), index.search(9999));
    }
}
