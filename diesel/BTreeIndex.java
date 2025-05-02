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
        int i = x.keys.size() - 1;
        if (x.isLeaf) {
            while (i >= 0 && compareKeys(key, x.keys.get(i)) < 0) {
                i--;
            }
            i++;
            if (i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0) {
                x.rowIndices.get(i).add(rowIndex);
                LOGGER.log(Level.FINE, "Appended rowIndex {0} to key {1} at position {2}", new Object[]{rowIndex, key, i});
            } else {
                x.keys.add(i, key);
                x.rowIndices.add(i, new ArrayList<>(Collections.singletonList(rowIndex)));
                LOGGER.log(Level.FINE, "Inserted new key {0} with rowIndex {1} at position {2}", new Object[]{key, rowIndex, i});
            }
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
            Node child = x.children.get(i + 1);
            validateNode(child);
            if (child.keys.size() < t) {
                LOGGER.log(Level.FINE, "Filling child at index={0}, child keys={1}", new Object[]{i + 1, child.keys});
                fillChild(x, i + 1);
                i = 0;
                while (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
                    i++;
                }
                if (i >= x.children.size()) {
                    LOGGER.log(Level.SEVERE, "Invalid child index after fill: {0}, children size={1}, node keys={2}",
                            new Object[]{i, x.children.size(), x.keys});
                    throw new IllegalStateException("Invalid child index after filling child");
                }
            }
            remove(x.children.get(i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0 ? i + 1 : i), key, rowIndex);
        } else {
            if (i < x.children.size()) {
                Node child = x.children.get(i);
                validateNode(child);
                if (child.keys.size() < t) {
                    LOGGER.log(Level.FINE, "Filling child at index={0}, child keys={1}", new Object[]{i, child.keys});
                    fillChild(x, i);
                    i = 0;
                    while (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
                        i++; // Fixed: Increment i to find the correct key position
                    }
                    if (i >= x.children.size()) {
                        LOGGER.log(Level.SEVERE, "Invalid child index after fill: {0}, children size={1}, node keys={2}",
                                new Object[]{i, x.children.size(), x.keys});
                        throw new IllegalStateException("Invalid child index after filling child");
                    }
                }
                remove(x.children.get(i), key, rowIndex);
            } else {
                LOGGER.log(Level.FINE, "No valid child for key={0}, i={1}, children size={2}",
                        new Object[]{key, i, x.children.size()});
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

    private void fillChild(Node x, int i) {
        validateNode(x);
        LOGGER.log(Level.FINE, "Filling child at index={0}, parent keys={1}, children size={2}",
                new Object[]{i, x.keys, x.children.size()});
        if (i >= x.children.size()) {
            LOGGER.log(Level.SEVERE, "Invalid child index: {0}, children size={1}", new Object[]{i, x.children.size()});
            throw new IllegalStateException("Invalid child index in fillChild");
        }

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
            child.rowIndices.add(0, new ArrayList<>(sibling.rowIndices.get(sibling.rowIndices.size() - 1)));
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
            child.rowIndices.add(new ArrayList<>(sibling.rowIndices.get(0)));
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
            // Leaf node merge: combine keys and rowIndices, handling duplicates
            for (int j = 0; j < sibling.keys.size(); j++) {
                Object siblingKey = sibling.keys.get(j);
                List<Integer> siblingIndices = sibling.rowIndices.get(j);
                int pos = -1;
                for (int k = 0; k < child.keys.size(); k++) {
                    if (compareKeys(siblingKey, child.keys.get(k)) == 0) {
                        pos = k;
                        break;
                    }
                }
                if (pos >= 0) {
                    // Duplicate key found, merge rowIndices
                    child.rowIndices.get(pos).addAll(siblingIndices);
                    LOGGER.log(Level.FINE, "Merged rowIndices for key {0} at child position {1}: {2}",
                            new Object[]{siblingKey, pos, child.rowIndices.get(pos)});
                } else {
                    // New key, add to child in sorted order
                    int insertPos = child.keys.size();
                    for (int k = 0; k < child.keys.size(); k++) {
                        if (compareKeys(siblingKey, child.keys.get(k)) < 0) {
                            insertPos = k;
                            break;
                        }
                    }
                    child.keys.add(insertPos, siblingKey);
                    child.rowIndices.add(insertPos, new ArrayList<>(siblingIndices));
                    LOGGER.log(Level.FINE, "Added key {0} with rowIndices {1} at child position {2}",
                            new Object[]{siblingKey, siblingIndices, insertPos});
                }
            }
        } else {
            // Internal node merge: add parent key and sibling keys/children
            child.keys.add(x.keys.get(i));
            child.keys.addAll(sibling.keys);
            child.children.addAll(sibling.children);
            LOGGER.log(Level.FINE, "Merged internal node: added parent key {0}, sibling keys {1}, sibling children size={2}",
                    new Object[]{x.keys.get(i), sibling.keys, sibling.children.size()});
        }

        x.keys.remove(i);
        x.children.remove(i + 1);

        validateNode(child);
        validateNode(x);
        LOGGER.log(Level.FINE, "Merge completed, child keys={0}, child rowIndices={1}, child children size={2}",
                new Object[]{child.keys, child.isLeaf ? child.rowIndices : null, child.isLeaf ? 0 : child.children.size()});
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
        int i = 0;
        while (i < x.keys.size() && compareKeys(key, x.keys.get(i)) > 0) {
            i++;
        }
        if (i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0) {
            if (x.isLeaf) {
                result.addAll(x.rowIndices.get(i));
            }
        }
        if (!x.isLeaf) {
            result.addAll(search(x.children.get(i), key));
        }
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
        int i = 0;
        while (i < x.keys.size() && compareKeys(low, x.keys.get(i)) > 0) {
            i++;
        }
        if (x.isLeaf) {
            while (i < x.keys.size() && compareKeys(x.keys.get(i), high) <= 0) {
                result.addAll(x.rowIndices.get(i));
                i++;
            }
        } else {
            while (i < x.keys.size() && compareKeys(x.keys.get(i), high) <= 0) {
                rangeSearch(x.children.get(i), low, high, result);
                if (i < x.keys.size() && compareKeys(x.keys.get(i), low) >= 0) {
                    result.addAll(search(x.keys.get(i)));
                }
                i++;
            }
            if (i < x.children.size()) {
                rangeSearch(x.children.get(i), low, high, result);
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