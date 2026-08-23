package diesel;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * A B-tree secondary index mapping keys to the row indexes that hold them.
 * Supports exact lookup via {@link #search} and ordered range lookups via
 * {@link #rangeSearch}. Null keys are ignored on insert and remove.
 *
 * @see Index
 */
class BTreeIndex implements Index, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(BTreeIndex.class.getName());

    private static class Node implements Serializable {
        private static final long serialVersionUID = 1L;
        List<Object> keys;
        List<List<Integer>> rowIndices; // For leaf nodes, each key maps to a list of row indices
        List<Node> children; // For internal nodes
        boolean isLeaf;

        Node(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.keys = new ArrayList<>();
            this.rowIndices = isLeaf ? new ArrayList<>() : null;
            this.children = isLeaf ? null : new ArrayList<>();
        }
    }

    private Node root;
    private final int t; // Minimum degree (defines the range for number of keys)
    private final Class<?> keyType;

    /**
     * Creates an empty B-tree index for the given key type.
     *
     * @param keyType the Java type of the indexed keys
     */
    public BTreeIndex(Class<?> keyType) {
        this.t = 3; // Minimum degree, can be adjusted
        this.root = new Node(true);
        this.keyType = keyType;
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

    /**
     * Inserts the key, associating it with the given row index. Null keys are
     * ignored.
     *
     * @param key      the key to insert
     * @param rowIndex the row index to associate with the key
     */
    @Override
    public void insert(Object key, int rowIndex) {
        if (key == null) {
            LOGGER.log(Level.WARNING, "Attempted to insert null key");
            return;
        }
        Node r = root;
        if (r.keys.size() == (2 * t - 1)) {
            Node s = new Node(false);
            root = s;
            s.children.add(r);
            splitChild(s, 0);
            insertNonFull(s, key, rowIndex);
        } else {
            insertNonFull(r, key, rowIndex);
        }
    }

    private void insertNonFull(Node x, Object key, int rowIndex) {
        int i = 0;
        while (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
            i++;
        }
        if (x.isLeaf) {
            if (i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0) {
                x.rowIndices.get(i).add(rowIndex);
                LOGGER.log(Level.FINE, "Appended rowIndex {0} to key {1} at position {2}", new Object[]{rowIndex, key, i});
            } else {
                x.keys.add(i, key);
                x.rowIndices.add(i, new ArrayList<>(Collections.singletonList(rowIndex)));
                LOGGER.log(Level.FINE, "Inserted new key {0} with rowIndex {1} at position {2}", new Object[]{key, rowIndex, i});
            }
        } else {
            if (x.children.get(i).keys.size() == (2 * t - 1)) {
                splitChild(x, i);
                if (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
                    i++;
                }
            }
            insertNonFull(x.children.get(i), key, rowIndex);
        }
        validateNode(x);
    }

    private void splitChild(Node x, int i) {
        Node z = new Node(x.children.get(i).isLeaf);
        Node y = x.children.get(i);
        int mid = t - 1;

        if (y.isLeaf) {
            Object promotedKey = y.keys.get(mid);
            z.keys.addAll(y.keys.subList(mid + 1, y.keys.size()));
            z.rowIndices.addAll(y.rowIndices.subList(mid + 1, y.rowIndices.size()));

            x.keys.add(i, promotedKey);
            x.children.add(i + 1, z);

            y.keys.subList(mid + 1, y.keys.size()).clear();
            y.rowIndices.subList(mid + 1, y.rowIndices.size()).clear();
        } else {
            z.keys.addAll(y.keys.subList(mid + 1, y.keys.size()));
            z.children.addAll(y.children.subList(mid + 1, y.children.size()));

            x.keys.add(i, y.keys.get(mid));
            x.children.add(i + 1, z);

            y.keys.subList(mid, y.keys.size()).clear();
            y.children.subList(mid + 1, y.children.size()).clear();
        }

        validateNode(x);
        validateNode(y);
        validateNode(z);
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
        if (key == null) {
            LOGGER.log(Level.WARNING, "Attempted to remove null key");
            return;
        }
        remove(root, key, rowIndex);
        if (root.keys.isEmpty() && !root.isLeaf) {
            root = root.children.get(0);
        }
    }

    private void remove(Node x, Object key, int rowIndex) {
        validateNode(x);
        int i = 0;
        while (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
            i++;
        }

        LOGGER.log(Level.FINE, "Removing key={0}, rowIndex={1}, node keys={2}, isLeaf={3}, i={4}",
                new Object[]{key, rowIndex, x.keys, x.isLeaf, i});

        if (x.isLeaf) {
            for (int j = 0; j < x.keys.size(); j++) {
                if (compareKeys(key, x.keys.get(j)) == 0) {
                    List<Integer> indices = x.rowIndices.get(j);
                    if (indices.remove(Integer.valueOf(rowIndex))) {
                        LOGGER.log(Level.FINE, "Removed rowIndex {0} for key {1} at position {2}", new Object[]{rowIndex, key, j});
                        if (indices.isEmpty()) {
                            x.keys.remove(j);
                            x.rowIndices.remove(j);
                            LOGGER.log(Level.FINE, "Removed key {0} at position {1} as no indices remain", new Object[]{key, j});
                        }
                        validateNode(x);
                        return;
                    }
                }
            }
            LOGGER.log(Level.FINE, "No matching key={0}, rowIndex={1} found in leaf node", new Object[]{key, rowIndex});
            return;
        }

        if (i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0) {
            // Key matches separator — try left child first (regular-insert
            // convention), then right child (bulk-load convention).
            remove(x.children.get(i), key, rowIndex);
            remove(x.children.get(i + 1), key, rowIndex);
        } else if (i < x.children.size()) {
            remove(x.children.get(i), key, rowIndex);
        } else {
            LOGGER.log(Level.FINE, "No valid child for key={0}, i={1}, children size={2}",
                    new Object[]{key, i, x.children.size()});
        }
    }

    private void validateNode(Node x) {
        if (x.isLeaf) {
            if (x.rowIndices == null || x.rowIndices.size() != x.keys.size()) {
                LOGGER.log(Level.SEVERE, "Invalid leaf node: keys={0}, rowIndices size={1}, rowIndices={2}",
                        new Object[]{x.keys, x.rowIndices != null ? x.rowIndices.size() : null, x.rowIndices});
                throw new IllegalStateException("Leaf node has mismatched keys and rowIndices");
            }
            for (int i = 0; i < x.rowIndices.size(); i++) {
                List<Integer> indices = x.rowIndices.get(i);
                if (indices == null || indices.isEmpty()) {
                    LOGGER.log(Level.SEVERE, "Invalid leaf node: empty or null rowIndices for key {0} at position {1}, keys={2}, rowIndices={3}",
                            new Object[]{x.keys.get(i), i, x.keys, x.rowIndices});
                    throw new IllegalStateException("Leaf node has empty or null rowIndices");
                }
            }
        } else {
            if (x.children == null || x.children.size() != x.keys.size() + 1) {
                LOGGER.log(Level.SEVERE, "Invalid internal node: keys={0}, keys size={1}, children={2}, children size={3}",
                        new Object[]{x.keys, x.keys.size(), x.children, x.children != null ? x.children.size() : null});
                throw new IllegalStateException("Internal node has mismatched keys and children");
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
        return search(root, key);
    }

    private List<Integer> search(Node x, Object key) {
        List<Integer> result = new ArrayList<>();
        if (key == null) {
            return result;
        }
        if (x.isLeaf) {
            for (int i = 0; i < x.keys.size(); i++) {
                if (compareKeys(key, x.keys.get(i)) == 0) {
                    result.addAll(x.rowIndices.get(i));
                }
            }
            return result;
        }
        // Bulk-loaded trees may have the same key as both a separator in this
        // internal node and as data in a child.  When the key matches a
        // separator at position i, the standard B-tree convention is to go
        // right (children[i+1]) because left children hold keys < separator.
        // We therefore search the right child on an exact match.
        int i = 0;
        while (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
            i++;
        }
        if (i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0) {
            // Key matches separator at position i.  Regular-insert trees keep
            // the promoted key in the left child; bulk-loaded trees store it
            // in the right child.  Search both to cover either convention.
            result.addAll(search(x.children.get(i), key));
            result.addAll(search(x.children.get(i + 1), key));
        } else {
            // Key is strictly less than separator at i (or past all separators).
            result.addAll(search(x.children.get(i), key));
        }
        return result;
    }

    /**
     * Returns every row index whose key lies within the inclusive
     * {@code [low, high]} range. Either bound may be {@code null} to indicate
     * an open-ended range ({@code null low} means no lower bound;
     * {@code null high} means no upper bound).
     *
     * @param low  the inclusive lower bound, or {@code null}
     * @param high the inclusive upper bound, or {@code null}
     * @return the list of matching row indexes, possibly empty
     */
    public List<Integer> rangeSearch(Object low, Object high) {
        List<Integer> result = new ArrayList<>();
        rangeSearch(root, low, high, result);
        return result;
    }

    private void rangeSearch(Node x, Object low, Object high, List<Integer> result) {
        if (x.isLeaf) {
            for (int i = 0; i < x.keys.size(); i++) {
                Object key = x.keys.get(i);
                boolean aboveLow = low == null || compareKeys(key, low) >= 0;
                boolean belowHigh = high == null || compareKeys(key, high) <= 0;
                if (aboveLow && belowHigh) {
                    result.addAll(x.rowIndices.get(i));
                }
            }
        } else {
            for (Node child : x.children) {
                rangeSearch(child, low, high, result);
            }
        }
    }

    /**
     * Returns every row index whose key is greater than or equal to
     * {@code low}. Equivalent to {@code rangeSearch(low, null)}.
     */
    public List<Integer> rangeSearchLow(Object low) {
        return rangeSearch(low, null);
    }

    /**
     * Returns every row index whose key is less than or equal to
     * {@code high}. Equivalent to {@code rangeSearch(null, high)}.
     */
    public List<Integer> rangeSearchHigh(Object high) {
        return rangeSearch(null, high);
    }

    /**
     * Bulk-loads the index from pre-sorted data. All key–rowIndex pairs are
     * supplied in ascending key order. Duplicate keys are merged into a single
     * entry with a combined list of row indices. After this call the previous
     * tree is discarded and replaced.
     *
     * @param sortedKeys   sorted keys (ascending)
     * @param sortedRowIdx corresponding row indices (same order as sortedKeys)
     * @throws IllegalArgumentException if lists are empty or have mismatched sizes
     */
    void bulkLoad(List<Object> sortedKeys, List<Integer> sortedRowIdx) {
        if (sortedKeys.size() != sortedRowIdx.size()) {
            throw new IllegalArgumentException("sortedKeys and sortedRowIdx must have the same size");
        }
        int n = sortedKeys.size();
        if (n == 0) {
            this.root = new Node(true);
            return;
        }

        // Merge duplicate keys into single entries with combined row index lists.
        List<Object> mergedKeys = new ArrayList<>();
        List<List<Integer>> mergedIndices = new ArrayList<>();
        Object prevKey = sortedKeys.get(0);
        List<Integer> currentIndices = new ArrayList<>();
        currentIndices.add(sortedRowIdx.get(0));
        for (int i = 1; i < n; i++) {
            Object key = sortedKeys.get(i);
            if (compareKeys(key, prevKey) == 0) {
                currentIndices.add(sortedRowIdx.get(i));
            } else {
                mergedKeys.add(prevKey);
                mergedIndices.add(currentIndices);
                prevKey = key;
                currentIndices = new ArrayList<>();
                currentIndices.add(sortedRowIdx.get(i));
            }
        }
        mergedKeys.add(prevKey);
        mergedIndices.add(currentIndices);

        // Build all leaf nodes from merged data (left to right).
        int leafCapacity = 2 * t - 1;
        List<Node> leaves = new ArrayList<>();
        Node currentLeaf = new Node(true);
        for (int i = 0; i < mergedKeys.size(); i++) {
            currentLeaf.keys.add(mergedKeys.get(i));
            currentLeaf.rowIndices.add(mergedIndices.get(i));
            if (currentLeaf.keys.size() == leafCapacity || i == mergedKeys.size() - 1) {
                leaves.add(currentLeaf);
                if (i < mergedKeys.size() - 1) {
                    currentLeaf = new Node(true);
                }
            }
        }

        // Build internal levels bottom-up.
        List<Node> currentLevel = leaves;
        while (currentLevel.size() > 1) {
            List<Node> nextLevel = new ArrayList<>();
            int i = 0;
            while (i < currentLevel.size()) {
                Node parent = new Node(false);
                parent.children.add(currentLevel.get(i));
                i++;
                while (parent.keys.size() < leafCapacity && i < currentLevel.size()) {
                    parent.keys.add(extractFirstKey(currentLevel.get(i)));
                    parent.children.add(currentLevel.get(i));
                    i++;
                }
                nextLevel.add(parent);
            }
            currentLevel = nextLevel;
        }

        this.root = currentLevel.get(0);
    }

    /**
     * Extracts the leftmost (smallest) key from a subtree.
     */
    private Object extractFirstKey(Node node) {
        if (node.isLeaf) {
            return node.keys.get(0);
        }
        return extractFirstKey(node.children.get(0));
    }

    private int compareKeys(Object k1, Object k2) {
        if (k1 instanceof Comparable ck1 && k2 instanceof Comparable ck2) {
            @SuppressWarnings("unchecked")
            Comparable<Object> c1 = (Comparable<Object>) ck1;
            return c1.compareTo(k2);
        }
        return String.valueOf(k1).compareTo(String.valueOf(k2));
    }
}