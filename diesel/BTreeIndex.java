package diesel;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

class BTreeIndex implements Index, Serializable {
    private static final Logger LOGGER = Logger.getLogger(BTreeIndex.class.getName());

    private static class Node implements Serializable {
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

    public BTreeIndex(Class<?> keyType) {
        this.t = 3; // Minimum degree, can be adjusted
        this.root = new Node(true);
        this.keyType = keyType;
    }

    @Override
    public Class<?> getKeyType() {
        return keyType;
    }

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

        if (i < x.children.size()) {
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
        int i = 0;
        while (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
            i++;
        }
        result.addAll(search(x.children.get(i), key));
        return result;
    }

    public List<Integer> rangeSearch(Object low, Object high) {
        List<Integer> result = new ArrayList<>();
        if (low == null || high == null) {
            return result;
        }
        rangeSearch(root, low, high, result);
        return result;
    }

    private void rangeSearch(Node x, Object low, Object high, List<Integer> result) {
        if (x.isLeaf) {
            for (int i = 0; i < x.keys.size(); i++) {
                if (compareKeys(x.keys.get(i), low) >= 0 && compareKeys(x.keys.get(i), high) <= 0) {
                    result.addAll(x.rowIndices.get(i));
                }
            }
        } else {
            for (Node child : x.children) {
                rangeSearch(child, low, high, result);
            }
        }
    }

    private int compareKeys(Object k1, Object k2) {
        if (k1 instanceof Comparable && k2 instanceof Comparable) {
            return ((Comparable<Object>) k1).compareTo(k2);
        }
        return String.valueOf(k1).compareTo(String.valueOf(k2));
    }
}