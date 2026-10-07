package diesel.storage.page;

import java.util.HashMap;
import java.util.function.Predicate;

/**
 * LRU recency order for {@link BufferPool} (prompt4.md step 7, R3-002).
 *
 * <p>Explicit doubly-linked list + hash map for O(1) {@link #add},
 * {@link #touch}, {@link #remove} and O(1) typical victim selection (the LRU
 * end is unpinned in the common case; a full scan degrades to O(frames) only
 * when every candidate is pinned).
 *
 * <p>Ordering semantics: recency is updated on <em>pin</em> (the access);
 * {@code unpin} does not touch the order, so a page released long ago remains
 * an older candidate than one pinned recently — classic LRU, not MRU-on-free.
 *
 * <p>Not thread-safe by design: it is a private structure guarded by the
 * {@code BufferPool} lock (all callers run inside that critical section).
 */
final class LruEvictionPolicy {

    /** Doubly-linked recency node. */
    private static final class Node {
        private final PageId pageId;
        private Node prev;
        private Node next;

        Node(PageId pageId) {
            this.pageId = pageId;
        }
    }

    /** Page id → recency node. */
    private final HashMap<PageId, Node> nodes = new HashMap<>();
    /** Least recently used end (eviction side). */
    private Node head;
    /** Most recently used end. */
    private Node tail;

    /**
     * Adds {@code pageId} at the most-recently-used end. Adding an id that is
     * already tracked is equivalent to {@link #touch(PageId)}.
     *
     * @param pageId page identifier to track (must not be {@code null})
     */
    void add(PageId pageId) {
        Node existing = nodes.get(pageId);
        if (existing != null) {
            moveToTail(existing);
            return;
        }
        Node node = new Node(pageId);
        nodes.put(pageId, node);
        appendTail(node);
    }

    /**
     * Marks {@code pageId} as the most recently used. No-op when the id is
     * not tracked (defensive: callers always add first).
     *
     * @param pageId page identifier to mark as recently used
     */
    void touch(PageId pageId) {
        Node node = nodes.get(pageId);
        if (node != null) {
            moveToTail(node);
        }
    }

    /**
     * Removes {@code pageId} from the order entirely (frame gone).
     *
     * @param pageId page identifier to forget
     */
    void remove(PageId pageId) {
        Node node = nodes.remove(pageId);
        if (node != null) {
            detach(node);
        }
    }

    /**
     * Selects the least-recently-used page accepted by {@code evictable},
     * removing it from the order (the caller removes the frame itself).
     *
     * @param evictable predicate deciding whether a page may be evicted
     *                  (BufferPool tests {@code pinCount == 0} here)
     * @return the victim page id, or {@code null} when no tracked page
     *         qualifies (all pinned)
     */
    PageId pollEvictable(Predicate<PageId> evictable) {
        for (Node node = head; node != null; node = node.next) {
            if (evictable.test(node.pageId)) {
                detach(node);
                nodes.remove(node.pageId);
                return node.pageId;
            }
        }
        return null;
    }

    /** Drops all tracked pages (pool close/clear). */
    void clear() {
        nodes.clear();
        head = null;
        tail = null;
    }

    /** @return number of tracked pages */
    int size() {
        return nodes.size();
    }

    private void appendTail(Node node) {
        node.prev = tail;
        node.next = null;
        if (tail != null) {
            tail.next = node;
        } else {
            head = node;
        }
        tail = node;
    }

    private void detach(Node node) {
        if (node.prev != null) {
            node.prev.next = node.next;
        } else {
            head = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        } else {
            tail = node.prev;
        }
        node.prev = null;
        node.next = null;
    }

    private void moveToTail(Node node) {
        if (node != tail) {
            detach(node);
            appendTail(node);
        }
    }
}
