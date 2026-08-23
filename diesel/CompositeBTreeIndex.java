package diesel;

import java.io.Serializable;
import java.util.*;

/**
 * A B-tree secondary index mapping composite (multi-column) keys to row
 * indexes.  Keys are {@link List} instances whose elements are compared
 * left-to-right using natural ordering.
 *
 * <p>Supports exact lookup via {@link #search} and prefix lookup via
 * {@link #prefixSearch} for queries that reference only the leading
 * columns of the composite key.
 *
 * @see Index
 */
class CompositeBTreeIndex implements Index, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Composite key wrapper that implements element-by-element comparison.
     */
    record CompositeKey(List<Object> values) implements Comparable<CompositeKey>, Serializable {
        private static final long serialVersionUID = 1L;
        @Override
        public int compareTo(CompositeKey other) {
            int minLen = Math.min(values.size(), other.values.size());
            for (int i = 0; i < minLen; i++) {
                int cmp = compareObjects(values.get(i), other.values.get(i));
                if (cmp != 0) return cmp;
            }
            return Integer.compare(values.size(), other.values.size());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CompositeKey other)) return false;
            return values.equals(other.values);
        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }

        private static int compareObjects(Object k1, Object k2) {
            if (k1 == null && k2 == null) return 0;
            if (k1 == null) return -1;
            if (k2 == null) return 1;
            if (k1 instanceof Comparable c1 && k2 instanceof Comparable c2) {
                return c1.compareTo(c2);
            }
            return String.valueOf(k1).compareTo(String.valueOf(k2));
        }
    }

    private final List<String> columns;
    private final BTreeIndex delegate;

    /**
     * Creates a composite B-tree index over the given columns.
     *
     * @param columns the column names that make up the composite key
     */
    CompositeBTreeIndex(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Composite index requires at least one column");
        }
        this.columns = List.copyOf(columns);
        this.delegate = new BTreeIndex(CompositeKey.class);
    }

    @Override
    public void insert(Object key, int rowIndex) {
        if (key == null) return;
        @SuppressWarnings("unchecked")
        List<Object> parts = (List<Object>) key;
        if (parts.stream().anyMatch(Objects::isNull)) return;
        delegate.insert(new CompositeKey(parts), rowIndex);
    }

    @Override
    public void remove(Object key, int rowIndex) {
        if (key == null) return;
        @SuppressWarnings("unchecked")
        List<Object> parts = (List<Object>) key;
        delegate.remove(new CompositeKey(parts), rowIndex);
    }

    @Override
    public List<Integer> search(Object key) {
        if (key == null) return Collections.emptyList();
        @SuppressWarnings("unchecked")
        List<Object> parts = (List<Object>) key;
        return delegate.search(new CompositeKey(parts));
    }

    @Override
    public Class<?> getKeyType() {
        return CompositeKey.class;
    }

    /**
     * Searches for all rows whose composite key starts with the given prefix.
     * For example, on index (A, B), a prefix of [1] returns all rows where A=1
     * regardless of B.
     *
     * @param prefix the leading column values to match
     * @return the matching row indexes
     */
    public List<Integer> prefixSearch(List<Object> prefix) {
        if (prefix == null || prefix.isEmpty()) return Collections.emptyList();
        if (prefix.stream().anyMatch(Objects::isNull)) return Collections.emptyList();

        CompositeKey lower = new CompositeKey(prefix);
        CompositeKey upper = buildUpperBound(prefix);
        if (upper != null) {
            return delegate.rangeSearch(lower, upper);
        }
        return delegate.search(lower);
    }

    /**
     * Builds an upper-bound key for prefix range search. Returns null if the
     * prefix already represents the maximum possible value.
     */
    private CompositeKey buildUpperBound(List<Object> prefix) {
        List<Object> upper = new ArrayList<>(prefix);
        int lastIdx = upper.size() - 1;
        Object lastVal = upper.get(lastIdx);
        if (lastVal instanceof Number n) {
            if (n instanceof Long v) upper.set(lastIdx, v + 1);
            else if (n instanceof Integer v) upper.set(lastIdx, v + 1);
            else if (n instanceof Double v) upper.set(lastIdx, v + 1);
            else if (n instanceof Float v) upper.set(lastIdx, (double) (v + 1));
            else if (n instanceof Short v) upper.set(lastIdx, (int) (v + 1));
            else if (n instanceof Byte v) upper.set(lastIdx, (int) (v + 1));
            else return null;
        } else if (lastVal instanceof String s) {
            upper.set(lastIdx, s + Character.MAX_VALUE);
        } else if (lastVal instanceof Comparable c) {
            // Attempt to increment — fall back to appending max char to string form
            upper.set(lastIdx, String.valueOf(c) + Character.MAX_VALUE);
        } else {
            return null;
        }
        return new CompositeKey(upper);
    }

    /**
     * Bulk-loads the index from pre-sorted composite keys.
     *
     * @param sortedKeys sorted list of composite key lists
     * @param sortedRowIdx corresponding row indexes
     */
    public void bulkLoad(List<List<Object>> sortedKeys, List<Integer> sortedRowIdx) {
        List<Object> compositeKeys = new ArrayList<>(sortedKeys.size());
        for (List<Object> key : sortedKeys) {
            compositeKeys.add(new CompositeKey(key));
        }
        delegate.bulkLoad(compositeKeys, sortedRowIdx);
    }

    /**
     * Returns the column names that make up the composite key.
     */
    public List<String> getColumns() {
        return columns;
    }
}
