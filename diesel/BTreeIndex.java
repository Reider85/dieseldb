package diesel;

import java.io.Serializable;
import java.io.FileInputStream;
import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.stream.Collectors;

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
    
    /**
     * Threshold for using parallel index scan (minimum estimated rows to trigger parallel processing).
     * Below this threshold, sequential scan is used to avoid parallelism overhead.
     */
    private static long PARALLEL_INDEX_SCAN_THRESHOLD = 10000;
    
    /**
     * Shared ForkJoinPool for parallel index scan operations.
     * Uses Runtime.getRuntime().availableProcessors() parallelism.
     * Daemon threads so it does not block JVM exit.
     */
    private static final ForkJoinPool INDEX_SCAN_POOL = new ForkJoinPool(
            Runtime.getRuntime().availableProcessors(),
            pool -> {
                ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setDaemon(true);
                t.setName("diesel-index-scan-" + t.getPoolIndex());
                return t;
            },
            null, true);
    
    static {
        loadParallelIndexScanConfig();
    }

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
    
    /**
     * Extracts the rightmost (largest) key from a subtree.
     */
    private Object extractLastKey(Node node) {
        if (node.isLeaf) {
            return node.keys.get(node.keys.size() - 1);
        }
        return extractLastKey(node.children.get(node.children.size() - 1));
    }
    
    private int compareKeys(Object k1, Object k2) {
        if (k1 instanceof Comparable ck1 && k2 instanceof Comparable) {
            @SuppressWarnings("unchecked")
            Comparable<Object> c1 = (Comparable<Object>) ck1;
            return c1.compareTo(k2);
        }
        return String.valueOf(k1).compareTo(String.valueOf(k2));
    }
    
    /**
     * Loads parallel index scan configuration from config.properties.
     */
    private static void loadParallelIndexScanConfig() {
        long threshold = 10000; // default
        try {
            File configFile = new File(ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                java.util.Properties props = new java.util.Properties();
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
                String raw = props.getProperty("parallel.index.scan.threshold");
                if (raw != null) {
                    threshold = Long.parseLong(raw.trim());
                }
            }
        } catch (Exception ignored) {
            // Keep the default on any config error
            LOGGER.fine("Config error for parallel index scan threshold, using default: " + ignored.getMessage());
        }
        PARALLEL_INDEX_SCAN_THRESHOLD = threshold;
    }
    
    /**
     * Returns every row index whose key lies within the inclusive
     * {@code [low, high]} range using parallel processing when beneficial.
     * Either bound may be {@code null} to indicate an open-ended range.
     *
     * @param low  the inclusive lower bound, or {@code null}
     * @param high the inclusive upper bound, or {@code null}
     * @return the list of matching row indexes, possibly empty
     */
    public List<Integer> rangeSearchParallel(Object low, Object high) {
        // Estimate the number of results to decide whether to use parallel processing
        // For now, we use a heuristic based on tree depth and node sizes
        long estimatedSize = estimateRangeSize(low, high);
        if (estimatedSize < PARALLEL_INDEX_SCAN_THRESHOLD) {
            // Use sequential scan for small ranges
            return rangeSearch(low, high);
        }
        
        // Use parallel scan for large ranges
        return executeParallelRangeSearch(low, high);
    }
    
    /**
     * Returns every row index whose key is greater than or equal to
     * {@code low} using parallel processing when beneficial.
     */
    public List<Integer> rangeSearchLowParallel(Object low) {
        long estimatedSize = estimateRangeSizeLow(low);
        if (estimatedSize < PARALLEL_INDEX_SCAN_THRESHOLD) {
            return rangeSearchLow(low);
        }
        return executeParallelRangeSearchLow(low);
    }
    
    /**
     * Returns every row index whose key is less than or equal to
     * {@code high} using parallel processing when beneficial.
     */
    public List<Integer> rangeSearchHighParallel(Object high) {
        long estimatedSize = estimateRangeSizeHigh(high);
        if (estimatedSize < PARALLEL_INDEX_SCAN_THRESHOLD) {
            return rangeSearchHigh(high);
        }
        return executeParallelRangeSearchHigh(high);
    }
    
    /**
     * Estimates the number of keys in the given range.
     * This is a simplified estimation - we count keys in the tree.
     */
    private long estimateRangeSize(Object low, Object high) {
        // For null bounds, we scan the entire index
        if (low == null && high == null) {
            return countAllKeys(root);
        }
        
        // For bounded ranges, we estimate by sampling
        return estimateBoundedRangeSize(root, low, high);
    }
    
    /**
     * Estimates the number of keys >= low.
     */
    private long estimateRangeSizeLow(Object low) {
        if (low == null) {
            return countAllKeys(root);
        }
        return countKeysAbove(root, low);
    }
    
    /**
     * Estimates the number of keys <= high.
     */
    private long estimateRangeSizeHigh(Object high) {
        if (high == null) {
            return countAllKeys(root);
        }
        return countKeysBelow(root, high);
    }
    
    /**
     * Counts all keys in the subtree rooted at the given node.
     */
    private long countAllKeys(Node node) {
        if (node == null) return 0;
        long count = node.keys.size();
        if (!node.isLeaf) {
            for (Node child : node.children) {
                count += countAllKeys(child);
            }
        }
        return count;
    }
    
    /**
     * Counts keys that are >= low in the subtree.
     */
    private long countKeysAbove(Node node, Object low) {
        if (node == null) return 0;
        long count = 0;
        
        if (node.isLeaf) {
            for (int i = 0; i < node.keys.size(); i++) {
                if (compareKeys(node.keys.get(i), low) >= 0) {
                    count++;
                }
            }
        } else {
            for (int i = 0; i < node.keys.size(); i++) {
                if (compareKeys(node.keys.get(i), low) >= 0) {
                    // This key and all keys in right subtree are >= low
                    count += countAllKeys(node.children.get(i + 1));
                    count++; // count this key
                } else {
                    // Check left subtree
                    count += countKeysAbove(node.children.get(i), low);
                }
            }
            // Check last child
            count += countKeysAbove(node.children.get(node.children.size() - 1), low);
        }
        return count;
    }
    
    /**
     * Counts keys that are <= high in the subtree.
     */
    private long countKeysBelow(Node node, Object high) {
        if (node == null) return 0;
        long count = 0;
        
        if (node.isLeaf) {
            for (int i = 0; i < node.keys.size(); i++) {
                if (compareKeys(node.keys.get(i), high) <= 0) {
                    count++;
                }
            }
        } else {
            for (int i = 0; i < node.keys.size(); i++) {
                if (compareKeys(node.keys.get(i), high) > 0) {
                    // This key and all keys in right subtree are > high
                    count += countKeysBelow(node.children.get(i), high);
                } else {
                    // This key and all keys in left subtree are <= high
                    count += countAllKeys(node.children.get(i));
                    count++; // count this key
                }
            }
            // Check last child
            count += countKeysBelow(node.children.get(node.children.size() - 1), high);
        }
        return count;
    }
    
    /**
     * Estimates the number of keys in a bounded range [low, high].
     */
    private long estimateBoundedRangeSize(Node node, Object low, Object high) {
        if (node == null) return 0;
        long count = 0;
        
        if (node.isLeaf) {
            for (int i = 0; i < node.keys.size(); i++) {
                Object key = node.keys.get(i);
                boolean aboveLow = low == null || compareKeys(key, low) >= 0;
                boolean belowHigh = high == null || compareKeys(key, high) <= 0;
                if (aboveLow && belowHigh) {
                    count++;
                }
            }
        } else {
            for (int i = 0; i < node.children.size(); i++) {
                // Check if this subtree might contain keys in range
                Object subtreeMin = extractFirstKey(node.children.get(i));
                Object subtreeMax = extractLastKey(node.children.get(i));
                
                boolean minInRange = (low == null || compareKeys(subtreeMin, low) >= 0) &&
                                    (high == null || compareKeys(subtreeMin, high) <= 0);
                boolean maxInRange = (low == null || compareKeys(subtreeMax, low) >= 0) &&
                                    (high == null || compareKeys(subtreeMax, high) <= 0);
                boolean rangeOverlap = compareKeys(subtreeMin, high) <= 0 && 
                                      compareKeys(subtreeMax, low) >= 0;
                
                if (minInRange || maxInRange || rangeOverlap) {
                    count += estimateBoundedRangeSize(node.children.get(i), low, high);
                }
            }
        }
        return count;
    }
    
    /**
     * Executes parallel range search by dividing work among subtrees.
     */
    private List<Integer> executeParallelRangeSearch(Object low, Object high) {
        // Get the root node
        Node root = this.root;
        
        // If the tree is empty or has only a root leaf node, process sequentially
        if (root == null || root.isLeaf) {
            return rangeSearch(low, high);
        }
        
        // Create tasks for each child subtree that might contain keys in range
        List<RangeSearchTask> tasks = new ArrayList<>();
        for (int i = 0; i < root.children.size(); i++) {
            Node child = root.children.get(i);
            // Check if this subtree might contain keys in range
            if (subtreeMayContainInRange(child, low, high)) {
                tasks.add(new RangeSearchTask(child, low, high, this));
            }
        }
        
        // If we only have one task or no tasks, process sequentially
        if (tasks.size() <= 1) {
            return rangeSearch(low, high);
        }
        
        // Execute tasks in parallel and merge results
        List<List<Integer>> results;
        try {
            results = INDEX_SCAN_POOL.invokeAll(tasks).stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return null;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (RuntimeException e) {
            return rangeSearch(low, high); // Fallback to sequential
        }
        
        // Merge results (they should already be in order due to B-tree properties)
        List<Integer> mergedResult = new ArrayList<>();
        for (List<Integer> result : results) {
            mergedResult.addAll(result);
        }
        return mergedResult;
    }
    
    /**
     * Executes parallel range search for keys >= low.
     */
    private List<Integer> executeParallelRangeSearchLow(Object low) {
        Node root = this.root;
        
        if (root == null || root.isLeaf) {
            return rangeSearchLow(low);
        }
        
        List<RangeSearchTask> tasks = new ArrayList<>();
        for (int i = 0; i < root.children.size(); i++) {
            Node child = root.children.get(i);
            if (subtreeMayContainInRange(child, low, null)) {
                tasks.add(new RangeSearchTask(child, low, null, this));
            }
        }
        
        if (tasks.size() <= 1) {
            return rangeSearchLow(low);
        }
        
        List<List<Integer>> results;
        try {
            results = INDEX_SCAN_POOL.invokeAll(tasks).stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return null;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (RuntimeException e) {
            return rangeSearchLow(low);
        }
        
        List<Integer> mergedResult = new ArrayList<>();
        for (List<Integer> result : results) {
            mergedResult.addAll(result);
        }
        return mergedResult;
    }
    
    /**
     * Executes parallel range search for keys <= high.
     */
    private List<Integer> executeParallelRangeSearchHigh(Object high) {
        Node root = this.root;
        
        if (root == null || root.isLeaf) {
            return rangeSearchHigh(high);
        }
        
        List<RangeSearchTask> tasks = new ArrayList<>();
        for (int i = 0; i < root.children.size(); i++) {
            Node child = root.children.get(i);
            if (subtreeMayContainInRange(child, null, high)) {
                tasks.add(new RangeSearchTask(child, null, high, this));
            }
        }
        
        if (tasks.size() <= 1) {
            return rangeSearchHigh(high);
        }
        
        List<List<Integer>> results;
        try {
            results = INDEX_SCAN_POOL.invokeAll(tasks).stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return null;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (RuntimeException e) {
            return rangeSearchHigh(high);
        }
        
        List<Integer> mergedResult = new ArrayList<>();
        for (List<Integer> result : results) {
            mergedResult.addAll(result);
        }
        return mergedResult;
    }
    
    /**
     * Checks if a subtree might contain keys in the specified range.
     */
    private boolean subtreeMayContainInRange(Node node, Object low, Object high) {
        if (node == null) return false;
        
        Object minKey = extractFirstKey(node);
        Object maxKey = extractLastKey(node);
        
        boolean minTooHigh = high != null && compareKeys(minKey, high) > 0;
        boolean maxTooLow = low != null && compareKeys(maxKey, low) < 0;
        
        return !(minTooHigh || maxTooLow);
    }
    
    /**
     * Task for searching a range in a subtree.
     */
    private static class RangeSearchTask implements Callable<List<Integer>> {
        private final Node node;
        private final Object low;
        private final Object high;
        private final BTreeIndex index;
        
        RangeSearchTask(Node node, Object low, Object high, BTreeIndex index) {
            this.node = node;
            this.low = low;
            this.high = high;
            this.index = index;
        }
        
        @Override
        public List<Integer> call() {
            List<Integer> result = new ArrayList<>();
            index.rangeSearch(node, low, high, result);
            return result;
        }
    }
}