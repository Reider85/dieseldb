package diesel.storage.page;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import static org.junit.jupiter.api.Assertions.*;

/**
 * O2 Buffer-5 verification test: LruEvictionPolicy.pollEvictable() uses an
 * unpinned-page queue for O(1) eviction under any pin distribution, instead
 * of walking the entire LRU list which could be O(F) when many pinned pages
 * are clustered at the head.
 */
@Tag("storage")
@Tag("smoke")
class LruEvictionPolicyUnpinnedQueueTest {

    private LruEvictionPolicy policy;

    @BeforeEach
    void setUp() {
        policy = new LruEvictionPolicy();
    }

    @Test
    void pollEvictableOnlyIteratesUnpinnedPages() {
        // Create several page IDs
        PageId page1 = new PageId(1, 1, 0);
        PageId page2 = new PageId(2, 1, 0);
        PageId page3 = new PageId(3, 1, 0);
        PageId page4 = new PageId(4, 1, 0);
        PageId page5 = new PageId(5, 1, 0);

        // Add pages to the policy (they become unpinned)
        policy.onUnpinned(page1);
        policy.onUnpinned(page2);
        policy.onUnpinned(page3);
        policy.onUnpinned(page4);
        policy.onUnpinned(page5);

        // Pin some pages (they should leave the eviction queue)
        policy.onPinned(page2);
        policy.onPinned(page4);

        // The unpinned queue should only contain unpinned pages
        // We can't directly access the queue, but we can test eviction behavior
        
        // Page1 should be evictable (it's unpinned)
        PageId evicted = policy.pollEvictable(pageId -> true);
        assertEquals(page1, evicted, "Should evict the least recently unpinned page");

        // Page3 should be next (still unpinned)
        evicted = policy.pollEvictable(pageId -> true);
        assertEquals(page3, evicted, "Should evict the next least recently unpinned page");

        // Page5 should be next (still unpinned)
        evicted = policy.pollEvictable(pageId -> true);
        assertEquals(page5, evicted, "Should evict the last unpinned page");

        // No more pages should be evictable
        evicted = policy.pollEvictable(pageId -> true);
        assertNull(evicted, "No more pages should be evictable");
    }

    @Test
    void pollEvictableIgnoresPinnedPages() {
        // Create pages
        PageId page1 = new PageId(1, 1, 0);
        PageId page2 = new PageId(2, 1, 0);
        PageId page3 = new PageId(3, 1, 0);

        // Add pages to the policy
        policy.onUnpinned(page1);
        policy.onUnpinned(page2);
        policy.onUnpinned(page3);

        // Pin page2 (it should not be evictable)
        policy.onPinned(page2);

        // Try to evict - should get page1 (not page2, which is pinned)
        PageId evicted = policy.pollEvictable(pageId -> true);
        assertEquals(page1, evicted, "Should evict unpinned page, not pinned page");

        // Next eviction should be page3 (still unpinned)
        evicted = policy.pollEvictable(pageId -> true);
        assertEquals(page3, evicted, "Should evict the remaining unpinned page");

        // No more pages should be evictable
        evicted = policy.pollEvictable(pageId -> true);
        assertNull(evicted, "No more pages should be evictable");
    }

    @Test
    void pollEvictableWithCustomPredicate() {
        // Create pages
        PageId page1 = new PageId(1, 1, 0);
        PageId page2 = new PageId(2, 1, 0);
        PageId page3 = new PageId(3, 1, 0);

        // Add pages to the policy
        policy.onUnpinned(page1);
        policy.onUnpinned(page2);
        policy.onUnpinned(page3);

        // Use a predicate that only accepts even page numbers (only page2 qualifies)
        PageId evicted = policy.pollEvictable(pageId -> pageId.tablespaceId() % 2 == 0);
        assertEquals(page2, evicted, "Should evict page2 (even tablespace ID)");

        // Next eviction should not find any matching pages
        evicted = policy.pollEvictable(pageId -> pageId.tablespaceId() % 2 == 0);
        assertNull(evicted, "No more even pages should be evictable");

        // Now test that all pages are gone
        evicted = policy.pollEvictable(pageId -> true);
        assertNull(evicted, "No pages should remain after filtering");
    }

    @Test
    void reUnpinMovesToTailOfQueue() {
        // Create pages
        PageId page1 = new PageId(1, 1, 0);
        PageId page2 = new PageId(2, 1, 0);
        PageId page3 = new PageId(3, 1, 0);

        // Add pages to the policy
        policy.onUnpinned(page1);
        policy.onUnpinned(page2);
        policy.onUnpinned(page3);

        // Pin and unpin page2 (should move it to the tail)
        policy.onPinned(page2);
        policy.onUnpinned(page2);

        // Evict all pages - page2 should be last
        PageId evicted = policy.pollEvictable(pageId -> true);
        assertEquals(page1, evicted, "Should evict page1 first");

        evicted = policy.pollEvictable(pageId -> true);
        assertEquals(page3, evicted, "Should evict page3 second");

        evicted = policy.pollEvictable(pageId -> true);
        assertEquals(page2, evicted, "Should evict page2 last (re-pinned)");
    }

    @Test
    void clearRemovesAllPages() {
        // Create pages
        PageId page1 = new PageId(1, 1, 0);
        PageId page2 = new PageId(2, 1, 0);
        PageId page3 = new PageId(3, 1, 0);

        // Add pages to the policy
        policy.onUnpinned(page1);
        policy.onUnpinned(page2);
        policy.onUnpinned(page3);

        // Clear the policy
        policy.clear();

        // No pages should be evictable
        PageId evicted = policy.pollEvictable(pageId -> true);
        assertNull(evicted, "No pages should be evictable after clear");
    }
}