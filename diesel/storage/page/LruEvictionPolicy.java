package diesel.storage.page;

import java.util.HashMap;
import java.util.function.Predicate;

/**
 * Eviction candidate queue for {@link BufferPool} (prompt4.md step 7, R3-002;
 * o2 Buffer-5).
 *
 * <p>Tracks only <em>unpinned</em> pages: a page enters the order when its
 * pin count drops to zero ({@link #onUnpinned}) and leaves it when it is
 * pinned again ({@link #onPinned}). Explicit doubly-linked list + hash map
 * keep every transition O(1), and {@link #pollEvictable} takes the
 * least-recently-released candidate off the head without ever walking pinned
 * frames — eviction is O(1) under any pin distribution (the previous
 * all-pages scan degraded to O(frames) per eviction when many pinned pages
 * clustered at the recency head, O(frames&#178;) cumulatively).
 *
 * <p>Ordering semantics: the queue is ordered by <em>release time</em>
 * (unpin of the last pin), not by access time. A pin/unpin cycle moves the
 * page to the most-recently-released end, so the common "re-pin as touch"
 * pattern keeps its recency meaning; a page pinned for a long time rejoins
 * the queue at the tail when finally released.
 *
 * <p>Not thread-safe by design: it is a private structure guarded by the
 * {@code BufferPool} lock (all callers run inside that critical section).
 */
final class LruEvictionPolicy {

    /** Doubly-linked queue node. */
    private static final class Node {
        private final PageId pageId;
        private Node prev;
        private Node next;

        Node(PageId pageId) {
            this.pageId = pageId;
        }
    }

    /** Page id → queue node (unpinned pages only). */
    private final HashMap<PageId, Node> nodes = new HashMap<>();
    /** Least-recently-released end (eviction side). */
    private Node head;
    /** Most-recently-released end. */
    private Node tail;

    /**
     * Registers the pin-count 0&#8594;1 transition: the page leaves the
     * eviction queue. No-op when the id is not queued (defensive; callers
     * invoke this exactly on the transition).
     *
     * @param pageId page identifier that became pinned
     */
    void onPinned(PageId pageId) {
        Node node = nodes.remove(pageId);
        if (node != null) {
            detach(node);
        }
    }

    /**
     * Registers the pin-count 1&#8594;0 transition: the page joins the queue
     * at the most-recently-released end. Re-registering an already queued id
     * moves it to that end (defensive; the transition shape makes it a no-op).
     *
     * @param pageId page identifier that became unpinned
     */
    void onUnpinned(PageId pageId) {
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
     * Selects the least-recently-released page accepted by {@code evictable},
     * removing it from the queue (the caller removes the frame itself).
     *
     * <p>Every queued page is unpinned by invariant, so the head qualifies in
     * the common case and the call is O(1); the walk only continues when the
     * predicate rejects a candidate (defensive: missing frame).
     *
     * @param evictable predicate deciding whether a page may be evicted
     *                  (BufferPool tests residency and {@code pinCount == 0})
     * @return the victim page id, or {@code null} when no queued page
     *         qualifies (all resident pages pinned)
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

    /** Drops all queued pages (pool close/clear). */
    void clear() {
        nodes.clear();
        head = null;
        tail = null;
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
