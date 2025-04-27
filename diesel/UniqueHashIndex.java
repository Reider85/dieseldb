package diesel;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

public class UniqueHashIndex implements Index, Serializable {
    private static final Logger LOGGER = Logger.getLogger(UniqueHashIndex.class.getName());
    private final Class<?> keyType;
    private final Map<Object, Integer> index;

    public UniqueHashIndex(Class<?> keyType) {
        this.keyType = keyType;
        this.index = new HashMap<>();
        LOGGER.log(Level.INFO, "Initialized UniqueHashIndex for key type {0}", keyType.getSimpleName());
    }

    @Override
    public void insert(Object key, int rowIndex) {
        if (key == null) return;
        if (!keyType.isInstance(key)) {
            throw new IllegalArgumentException("Key type mismatch: expected " + keyType.getSimpleName() + ", got " + key.getClass().getSimpleName());
        }
        if (index.containsKey(key)) {
            LOGGER.log(Level.WARNING, "Duplicate key detected: {0} at row {1}", new Object[]{key, index.get(key)});
            throw new IllegalArgumentException("Duplicate key: " + key);
        }
        index.put(key, rowIndex);
        LOGGER.log(Level.FINE, "Inserted unique key {0} at row {1}, index size: {2}",
                new Object[]{key, rowIndex, index.size()});
    }

    @Override
    public void remove(Object key, int rowIndex) {
        if (key == null) return;
        Integer existingRow = index.get(key);
        if (existingRow != null && existingRow.equals(rowIndex)) {
            index.remove(key);
            LOGGER.log(Level.FINE, "Removed key {0} from row {1}", new Object[]{key, rowIndex});
        }
    }

    @Override
    public List<Integer> search(Object key) {
        if (key == null) return new ArrayList<>();
        Integer rowIndex = index.get(key);
        LOGGER.log(Level.FINE, "Searching key {0}, found row: {1}", new Object[]{key, rowIndex});
        return rowIndex != null ? Collections.singletonList(rowIndex) : new ArrayList<>();
    }

    @Override
    public Class<?> getKeyType() {
        return keyType;
    }
}
