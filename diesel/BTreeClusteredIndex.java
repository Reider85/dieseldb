package diesel;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * A unique clustered B-tree index over the table's primary key: each key maps
 * to exactly one row index, and the physical row order follows the key order.
 * Duplicate or null keys are rejected on insert.
 *
 * @see Index
 * @see Table#createUniqueClusteredIndex
 */
class BTreeClusteredIndex implements Index, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(BTreeClusteredIndex.class.getName());

    private static class Node implements Serializable {
        private static final long serialVersionUID = 1L;
        List<Object> keys;
        List<Integer> rowIndices; // For leaf nodes
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
    private final int t; // Minimum degree
    private final Class<?> keyType;

    /**
     * Creates an empty clustered B-tree index for the given key type.
     *
     * @param keyType the Java type of the indexed keys
     */
    public BTreeClusteredIndex(Class<?> keyType) {
        this.t = 3;
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
     * Inserts the key, associating it with the given row index.
     *
     * @param key      the key to insert
     * @param rowIndex the row index to associate with the key
     * @throws IllegalStateException if the key already exists in the index
     */
    @Override
    public void insert(Object key, int rowIndex) {
        // Проверяем уникальность ключа
        List<Integer> existing = search(key);
        if (!existing.isEmpty()) {
            throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_PREFIX + key + ErrorMessages.ALREADY_EXISTS_SUFFIX);
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
        int i = x.keys.size() - 1;
        if (x.isLeaf) {
            x.keys.add(null);
            x.rowIndices.add(null);
            while (i >= 0 && compareKeys(key, x.keys.get(i)) < 0) {
                x.keys.set(i + 1, x.keys.get(i));
                x.rowIndices.set(i + 1, x.rowIndices.get(i));
                i--;
            }
            x.keys.set(i + 1, key);
            x.rowIndices.set(i + 1, rowIndex);
        } else {
            while (i >= 0 && compareKeys(key, x.keys.get(i)) < 0) {
                i--;
            }
            i++;
            if (x.children.get(i).keys.size() == (2 * t - 1)) {
                splitChild(x, i);
                if (compareKeys(key, x.keys.get(i)) > 0) {
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

        z.keys.addAll(y.keys.subList(mid + 1, y.keys.size()));
        if (y.isLeaf) {
            z.rowIndices.addAll(y.rowIndices.subList(mid + 1, y.rowIndices.size()));
        } else {
            z.children.addAll(y.children.subList(mid + 1, y.children.size()));
        }

        x.keys.add(i, y.keys.get(mid));
        x.children.add(i + 1, z);

        y.keys.subList(mid, y.keys.size()).clear();
        if (y.isLeaf) {
            y.rowIndices.subList(mid, y.rowIndices.size()).clear();
        } else {
            y.children.subList(mid + 1, y.children.size()).clear();
        }

        validateNode(x);
        validateNode(y);
        validateNode(z);
    }

    /**
     * Removes the association between the key and the given row index.
     *
     * @param key      the key to remove
     * @param rowIndex the row index to remove
     */
    @Override
    public void remove(Object key, int rowIndex) {
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

        if (x.isLeaf) {
            for (int j = 0; j < x.keys.size(); j++) {
                if (compareKeys(key, x.keys.get(j)) == 0 && x.rowIndices.get(j) == rowIndex) {
                    x.keys.remove(j);
                    x.rowIndices.remove(j);
                    LOGGER.log(Level.FINE, "Removed key={0}, rowIndex={1} from leaf node", new Object[]{key, rowIndex});
                    validateNode(x);
                    return;
                }
            }
            return;
        }

        if (i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0) {
            remove(x.children.get(i + 1), key, rowIndex);
        } else {
            if (i < x.children.size()) {
                Node child = x.children.get(i);
                validateNode(child);
                if (child.keys.size() < t) {
                    fillChild(x, i);
                    i = 0;
                    while (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
                        i++;
                    }
                    if (i >= x.children.size()) {
                        throw new IllegalStateException("Invalid child index after filling child");
                    }
                    child = x.children.get(i);
                }
                remove(child, key, rowIndex);
            }
        }
    }

    private void validateNode(Node x) {
        if (x.isLeaf) {
            if (x.rowIndices == null || x.rowIndices.size() != x.keys.size()) {
                LOGGER.log(Level.SEVERE, "Invalid leaf node: keys={0}, rowIndices size={1}, rowIndices={2}",
                        new Object[]{x.keys, x.rowIndices != null ? x.rowIndices.size() : null, x.rowIndices});
                throw new IllegalStateException("Leaf node has mismatched keys and rowIndices");
            }
        } else {
            if (x.children == null || x.children.size() != x.keys.size() + 1) {
                LOGGER.log(Level.SEVERE, "Invalid internal node: keys={0}, keys size={1}, children={2}, children size={3}",
                        new Object[]{x.keys, x.keys.size(), x.children, x.children != null ? x.children.size() : null});
                throw new IllegalStateException("Internal node has mismatched keys and children");
            }
        }
    }

    private void fillChild(Node x, int i) {
        if (i >= x.children.size()) {
            throw new IllegalStateException("Invalid child index in fillChild");
        }
        LOGGER.log(Level.FINE, "Filling child at index={0}, parent keys={1}, children size={2}",
                new Object[]{i, x.keys, x.children.size()});
        if (i > 0 && x.children.get(i - 1).keys.size() >= t) {
            borrowFromPrev(x, i);
            LOGGER.log(Level.FINE, "Borrowed from previous sibling at index={0}", i - 1);
        } else if (i < x.children.size() - 1 && x.children.get(i + 1).keys.size() >= t) {
            borrowFromNext(x, i);
            LOGGER.log(Level.FINE, "Borrowed from next sibling at index={0}", i + 1);
        } else {
            if (i < x.children.size() - 1) {
                LOGGER.log(Level.FINE, "Merging child at index={0} with next sibling", i);
                merge(x, i);
            } else {
                LOGGER.log(Level.FINE, "Merging child at index={0} with previous sibling", i - 1);
                merge(x, i - 1);
            }
        }
        validateNode(x);
    }

    private void borrowFromPrev(Node x, int i) {
        Node child = x.children.get(i);
        Node sibling = x.children.get(i - 1);

        child.keys.add(0, x.keys.get(i - 1));
        if (child.isLeaf) {
            child.rowIndices.add(0, sibling.rowIndices.get(sibling.rowIndices.size() - 1));
        } else {
            child.children.add(0, sibling.children.get(sibling.children.size() - 1));
        }

        x.keys.set(i - 1, sibling.keys.get(sibling.keys.size() - 1));
        sibling.keys.remove(sibling.keys.size() - 1);
        if (sibling.isLeaf) {
            sibling.rowIndices.remove(sibling.rowIndices.size() - 1);
        } else {
            sibling.children.remove(sibling.children.size() - 1);
        }
        validateNode(child);
        validateNode(sibling);
    }

    private void borrowFromNext(Node x, int i) {
        Node child = x.children.get(i);
        Node sibling = x.children.get(i + 1);

        child.keys.add(x.keys.get(i));
        if (child.isLeaf) {
            child.rowIndices.add(sibling.rowIndices.get(0));
        } else {
            child.children.add(sibling.children.get(0));
        }

        x.keys.set(i, sibling.keys.get(0));
        sibling.keys.remove(0);
        if (sibling.isLeaf) {
            sibling.rowIndices.remove(0);
        } else {
            sibling.children.remove(0);
        }
        validateNode(child);
        validateNode(sibling);
    }

    private void merge(Node x, int i) {
        Node child = x.children.get(i);
        Node sibling = x.children.get(i + 1);

        LOGGER.log(Level.FINE, "Merging child at index={0}, isLeaf={1}, child keys={2}, sibling keys={3}, parent key={4}",
                new Object[]{i, child.isLeaf, child.keys, sibling.keys, x.keys.get(i)});

        if (child.isLeaf) {
            // Leaf node merge: append sibling keys and rowIndices (no duplicates due to uniqueness)
            child.keys.addAll(sibling.keys);
            child.rowIndices.addAll(sibling.rowIndices);
            LOGGER.log(Level.FINE, "Merged leaf node: child keys={0}, child rowIndices={1}",
                    new Object[]{child.keys, child.rowIndices});
        } else {
            // Internal node merge: add parent key and sibling keys/children
            child.keys.add(x.keys.get(i));
            child.keys.addAll(sibling.keys);
            child.children.addAll(sibling.children);
            LOGGER.log(Level.FINE, "Merged internal node: added parent key {0}, sibling keys={1}, sibling children size={2}",
                    new Object[]{x.keys.get(i), sibling.keys, sibling.children.size()});
        }

        x.keys.remove(i);
        x.children.remove(i + 1);

        validateNode(child);
        validateNode(x);
        LOGGER.log(Level.FINE, "Merge completed, child keys={0}, child rowIndices={1}, child children size={2}",
                new Object[]{child.keys, child.isLeaf ? child.rowIndices : null, child.isLeaf ? 0 : child.children.size()});
    }

    /**
     * Builds the B-tree from pre-sorted, pre-validated data in O(N) time.
     * The caller MUST guarantee:
     * <ol>
     *   <li>{@code sortedKeys} and {@code rowIndices} are the same length</li>
     *   <li>{@code sortedKeys} are in ascending order per {@link #compareKeys}</li>
     *   <li>No duplicate keys exist</li>
     *   <li>No null keys exist</li>
     * </ol>
     * After this call, the previous tree is discarded and replaced.
     *
     * @param sortedKeys sorted unique keys (ascending)
     * @param rowIndices corresponding row indices
     * @throws IllegalArgumentException if lists are empty or have mismatched sizes
     */
    void bulkLoad(List<Object> sortedKeys, List<Integer> rowIndices) {
        if (sortedKeys.size() != rowIndices.size()) {
            throw new IllegalArgumentException("sortedKeys and rowIndices must have the same size");
        }
        int n = sortedKeys.size();
        if (n == 0) {
            this.root = new Node(true);
            return;
        }

        int leafCapacity = 2 * t - 1;

        // Phase 1: Build all leaf nodes from sorted data (left to right)
        List<Node> leaves = new ArrayList<>();
        Node currentLeaf = new Node(true);
        for (int i = 0; i < n; i++) {
            currentLeaf.keys.add(sortedKeys.get(i));
            currentLeaf.rowIndices.add(rowIndices.get(i));
            if (currentLeaf.keys.size() == leafCapacity || i == n - 1) {
                leaves.add(currentLeaf);
                if (i < n - 1) {
                    currentLeaf = new Node(true);
                }
            }
        }

        // Phase 2: Build internal levels bottom-up
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
     * Validates the entire B-tree satisfies all structural invariants:
     * key ordering, node sizes, leaf depth, and children/rowIndices consistency.
     *
     * @return {@code true} if the tree is valid
     */
    public boolean validate() {
        if (root.keys.isEmpty() && root.isLeaf) return true;
        return validateRecursive(root, null, null);
    }

    private boolean validateRecursive(Node node, Object minKey, Object maxKey) {
        for (int i = 1; i < node.keys.size(); i++) {
            if (compareKeys(node.keys.get(i - 1), node.keys.get(i)) >= 0) return false;
        }
        if (minKey != null && !node.keys.isEmpty() && compareKeys(node.keys.get(0), minKey) < 0) return false;
        if (maxKey != null && !node.keys.isEmpty() && compareKeys(node.keys.get(node.keys.size() - 1), maxKey) > 0) return false;
        if (node.keys.size() > 2 * t - 1) return false;
        if (node.isLeaf && node.rowIndices.size() != node.keys.size()) return false;
        if (!node.isLeaf) {
            if (node.children.size() != node.keys.size() + 1) return false;
            for (int i = 0; i < node.children.size(); i++) {
                Object childMin = (i == 0) ? minKey : node.keys.get(i - 1);
                Object childMax = (i == node.keys.size()) ? maxKey : node.keys.get(i);
                if (!validateRecursive(node.children.get(i), childMin, childMax)) return false;
            }
        }
        return true;
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
        return search(root, key);
    }

    private List<Integer> search(Node x, Object key) {
        List<Integer> result = new ArrayList<>();
        int i = 0;
        while (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
            i++;
        }
        if (x.isLeaf) {
            if (i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0) {
                result.add(x.rowIndices.get(i));
            }
        } else {
            // Separator keys in internal nodes are the minimum of the right
            // subtree, so when the key matches a separator, search child[i+1].
            int childIdx = (i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0) ? i + 1 : i;
            result.addAll(search(x.children.get(childIdx), key));
        }
        return result;
    }

    private int compareKeys(Object k1, Object k2) {
        if (k1 instanceof Comparable ck1 && k2 instanceof Comparable) {
            @SuppressWarnings("unchecked")
            Comparable<Object> c1 = (Comparable<Object>) ck1;
            return c1.compareTo(k2);
        }
        return String.valueOf(k1).compareTo(String.valueOf(k2));
    }
}