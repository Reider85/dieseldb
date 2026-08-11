package diesel;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A hash secondary index mapping keys to the set of row indexes that hold
 * them, backed by a concurrent map. Null keys are ignored.
 *
 * @see Index
 */
class HashIndex implements Index, Serializable {
    private static final long serialVersionUID = 1L;
    private final ConcurrentHashMap<Object, Set<Integer>> indexMap;
    private final Class<?> keyType;

    /**
     * Creates an empty hash index for the given key type.
     *
     * @param keyType the Java type of the indexed keys
     */
    public HashIndex(Class<?> keyType) {
        this.indexMap = new ConcurrentHashMap<>();
        this.keyType = keyType;
    }

    /**
     * Inserts the key, associating it with the given row index. Null keys are
     * ignored.
     *
     * @param key      the key to insert
     * @param rowIndex the row index to associate with the key
     */
    @Override
    public void insert(Object key, int rowIndex) {
        if (key != null) {
            indexMap.computeIfAbsent(key, k -> new HashSet<>()).add(rowIndex);
        }
    }

    /**
     * Removes the association between the key and the given row index. Null
     * keys are ignored.
     *
     * @param key      the key to remove
     * @param rowIndex the row index to remove
     */
    @Override
    public void remove(Object key, int rowIndex) {
        if (key != null) {
            Set<Integer> indices = indexMap.get(key);
            if (indices != null) {
                indices.remove(rowIndex);
                if (indices.isEmpty()) {
                    indexMap.remove(key);
                }
            }
        }
    }

    /**
     * Returns every row index that holds the given key.
     *
     * @param key the key to look up
     * @return the list of matching row indexes, possibly empty
     */
    @Override
    public List<Integer> search(Object key) {
        if (key == null) {
            return new ArrayList<>();
        }
        Set<Integer> indices = indexMap.get(key);
        return indices != null ? new ArrayList<>(indices) : new ArrayList<>();
    }

    /**
     * Returns the Java type of the indexed keys.
     *
     * @return the key type
     */
    @Override
    public Class<?> getKeyType() {
        return keyType;
    }
}