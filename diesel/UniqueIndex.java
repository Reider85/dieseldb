package diesel;

import java.io.Serializable;
import java.util.*;

/**
 * A unique secondary index mapping each key to exactly one row index. Null
 * keys are rejected on insert and duplicate keys raise an
 * {@link IllegalStateException}.
 *
 * @see Index
 */
class UniqueIndex implements Index, Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<Object, Integer> indexMap;
    private final Class<?> keyType;

    /**
     * Creates an empty unique index for the given key type.
     *
     * @param keyType the Java type of the indexed keys
     */
    public UniqueIndex(Class<?> keyType) {
        this.indexMap = new HashMap<>();
        this.keyType = keyType;
    }

    /**
     * Inserts the key, associating it with the given row index.
     *
     * @param key      the key to insert
     * @param rowIndex the row index to associate with the key
     * @throws IllegalArgumentException if the key is null
     * @throws IllegalStateException    if the key already exists
     */
    @Override
    public void insert(Object key, int rowIndex) {
        if (key == null) {
            throw new IllegalArgumentException("Null keys are not allowed in unique index");
        }
        if (indexMap.containsKey(key)) {
            throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_PREFIX + key + ErrorMessages.ALREADY_EXISTS_SUFFIX + " in unique index");
        }
        indexMap.put(key, rowIndex);
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
            Integer storedIndex = indexMap.get(key);
            if (storedIndex != null && storedIndex == rowIndex) {
                indexMap.remove(key);
            }
        }
    }

    /**
     * Returns the row index that holds the given key, or an empty list when
     * the key is absent.
     *
     * @param key the key to look up
     * @return the list with the matching row index, or an empty list
     */
    @Override
    public List<Integer> search(Object key) {
        if (key == null) {
            return new ArrayList<>();
        }
        Integer rowIndex = indexMap.get(key);
        return rowIndex != null ? Collections.singletonList(rowIndex) : new ArrayList<>();
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