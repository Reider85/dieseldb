package diesel;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

class BTreeClusteredIndex implements Index, Serializable {
    private static final Logger LOGGER = Logger.getLogger(BTreeClusteredIndex.class.getName());

    private static class Node implements Serializable {
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

    public BTreeClusteredIndex(Class<?> keyType) {
        this.t = 3;
        this.root = new Node(true);
        this.keyType = keyType;
    }

    @Override
    public Class<?> getKeyType() {
        return keyType;
    }

    @Override
    public void insert(Object key, int rowIndex) {
        // Проверяем уникальность ключа
        List<Integer> existing = search(key);
        if (!existing.isEmpty()) {
            throw new IllegalStateException("Duplicate key violation: key '" + key + "' already exists");
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
                throw new IllegalStateException("Leaf node has mismatched keys and rowIndices");
            }
        } else {
            if (x.children == null || x.children.size() != x.keys.size() + 1) {
                throw new IllegalStateException("Internal node has mismatched keys and children");
            }
        }
    }

    private void fillChild(Node x, int i) {
        if (i >= x.children.size()) {
            throw new IllegalStateException("Invalid child index in fillChild");
        }
        if (i > 0 && x.children.get(i - 1).keys.size() >= t) {
            borrowFromPrev(x, i);
        } else if (i < x.children.size() - 1 && x.children.get(i + 1).keys.size() >= t) {
            borrowFromNext(x, i);
        } else {
            if (i < x.children.size() - 1) {
                merge(x, i);
            } else {
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
    }

    private void merge(Node x, int i) {
        Node child = x.children.get(i);
        Node sibling = x.children.get(i + 1);

        child.keys.add(x.keys.get(i));
        child.keys.addAll(sibling.keys);
        if (child.isLeaf) {
            child.rowIndices.addAll(sibling.rowIndices);
        } else {
            child.children.addAll(sibling.children);
        }

        x.keys.remove(i);
        x.children.remove(i + 1);
    }

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
        if (i < x.keys.size() && compareKeys(key, x.keys.get(i)) == 0) {
            if (x.isLeaf) {
                result.add(x.rowIndices.get(i));
            }
        }
        if (!x.isLeaf) {
            result.addAll(search(x.children.get(i), key));
        }
        return result;
    }

    private int compareKeys(Object k1, Object k2) {
        if (k1 instanceof Comparable && k2 instanceof Comparable) {
            return ((Comparable<Object>) k1).compareTo(k2);
        }
        return String.valueOf(k1).compareTo(String.valueOf(k2));
    }
}