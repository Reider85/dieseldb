package diesel;

import java.io.Serializable;
import java.util.*;

class UniqueIndex implements Index, Serializable {
    private final Map<Object, Integer> indexMap;
    private final Class<?> keyType;

    public UniqueIndex(Class<?> keyType) {
        this.indexMap = new HashMap<>();
        this.keyType = keyType;
    }

    @Override
    public void insert(Object key, int rowIndex) {
        if (key == null) {
            throw new IllegalArgumentException("Null keys are not allowed in unique index");
        }
        if (indexMap.containsKey(key)) {
            throw new IllegalStateException("Duplicate key violation: key '" + key + "' already exists in unique index");
        }
        indexMap.put(key, rowIndex);
    }

    @Override
    public void remove(Object key, int rowIndex) {
        if (key != null) {
            Integer storedIndex = indexMap.get(key);
            if (storedIndex != null && storedIndex == rowIndex) {
                indexMap.remove(key);
            }
        }
    }

    @Override
    public List<Integer> search(Object key) {
        if (key == null) {
            return new ArrayList<>();
        }
        Integer rowIndex = indexMap.get(key);
        return rowIndex != null ? Collections.singletonList(rowIndex) : new ArrayList<>();
    }

    @Override
    public Class<?> getKeyType() {
        return keyType;
    }
}