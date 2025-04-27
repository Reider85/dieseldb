package diesel;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class HashIndex implements Index, Serializable {
    private final ConcurrentHashMap<Object, Set<Integer>> indexMap;
    private final Class<?> keyType;

    public HashIndex(Class<?> keyType) {
        this.indexMap = new ConcurrentHashMap<>();
        this.keyType = keyType;
    }

    @Override
    public void insert(Object key, int rowIndex) {
        if (key != null) {
            indexMap.computeIfAbsent(key, k -> new HashSet<>()).add(rowIndex);
        }
    }

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

    @Override
    public List<Integer> search(Object key) {
        if (key == null) {
            return new ArrayList<>();
        }
        Set<Integer> indices = indexMap.get(key);
        return indices != null ? new ArrayList<>(indices) : new ArrayList<>();
    }

    @Override
    public Class<?> getKeyType() {
        return keyType;
    }
}