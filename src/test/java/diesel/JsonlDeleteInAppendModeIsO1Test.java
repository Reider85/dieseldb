package diesel;

import diesel.storage.JsonlDeltaManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 JSONL-5 + JSONL-6 ({@code JsonlDeltaManager} append-mode delete path):
 * <ul>
 * <li><b>JSONL-5:</b> {@code findBaseLineForValue} resolves a base row's
 * 0-based file line through an {@code IdentityHashMap<Object[], Integer>}
 * (O(1) hash-lookup) instead of an O(B*m) {@code Arrays.equals} content
 * scan over every non-deleted base row. Identity is valid because the
 * storage never clones rows between {@code onLoad} and a later delete — the
 * same {@code Object[]} instances live in the live row list.</li>
 * <li><b>JSONL-6:</b> the pending-row branch of {@code onDelete} removes a
 * single {@code PendingRow} pair (row + presence) instead of two parallel
 * ArrayList entries. The residual O(P) pair-shift is inherent to the
 * positional pending model (prompt acceptance: "plus the inherent ArrayList
 * shift cost"); the dominant O(B*m) term is what JSONL-5 kills.</li>
 * </ul>
 *
 * <p>Follows the JsonlPositionToRowIdIsO1Test / UpdateRowIsOLogNTest
 * ratio-bound precedent: N-independence of a fixed-size delete batch is
 * measured between a small and a big base fixture (the old scan scaled with
 * B, the new path does not), plus wall bound and correctness regressions.
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlDeleteInAppendModeIsO1Test {

    private static Object[] row(int i) {
        return new Object[] {(long) i, "n" + i, i % 97};
    }

    private static List<Object[]> baseRows(int count) {
        List<Object[]> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(row(i));
        }
        return rows;
    }

    private static List<boolean[]> allPresent(int count, int columns) {
        List<boolean[]> presence = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            presence.add(new boolean[columns]);
        }
        return presence;
    }

    /**
     * Mirrors the engine delete contract: notify the delta manager while the
     * row is still in the live list, then remove it (JsonlRowStorage.delete).
     * Deleting from the tail downwards keeps every earlier index valid.
     */
    private static void deleteAt(JsonlDeltaManager dm, List<Object[]> live, int index) {
        dm.onDelete(index, live);
        live.remove(index);
    }

    /**
     * Times {@code deletes} tail-descending deletes against a base of
     * {@code baseCount} rows. Deleting from the tail keeps the driver's
     * {@code live.remove} an O(1) pop (no ArrayList tail-shift to pollute
     * the ratio — the shift is the engine's inherent rows.remove cost, not
     * the delta manager's), while the old content scan still walks the full
     * unvisited prefix (0..line) on every call, so the old shape measured
     * O(B) per delete and the new identity lookup measures O(1).
     */
    private static long timeDeletes(int baseCount, int deletes) {
        List<Object[]> loaded = baseRows(baseCount);
        JsonlDeltaManager dm = new JsonlDeltaManager(0.5);
        dm.onLoad(loaded, allPresent(baseCount, 3), baseCount);
        List<Object[]> live = new ArrayList<>(loaded);
        long start = System.nanoTime();
        for (int j = 0; j < deletes; j++) {
            deleteAt(dm, live, live.size() - 1);
        }
        return System.nanoTime() - start;
    }

    // ─── O(1) structural proof: cost independent of base size ──────

    /**
     * Two base fixtures (100k vs 2k rows) with an identical 1000-delete
     * batch. The old findBaseLineForValue scanned up to B base rows per
     * delete (big/small ratio ~50x); the identity map makes the cost
     * independent of B (ratio ~1-2x, bound 10x — JsonlPositionToRowIdIsO1Test
     * calibration on the same machine). Absolute wall bound is a secondary
     * catastrophic-regression guard. Unmeasured JIT warmup first
     * (EvictionUnderPinnedHeadTest precedent).
     */
    @Test
    @Timeout(60)
    void baseDeleteBatchIsIndependentOfBaseSize() {
        final int deletes = 1_000;
        final int bigBase = 100_000;
        final int smallBase = 2_000;
        final double maxRatio = 10.0;
        final long absoluteBoundMillis = 2_000;

        // Warmup: hot-compile the delete path on a small fixture so neither
        // measurement pays the JIT compilation.
        timeDeletes(1_000, 200);

        // Small base first: identical call pattern, both with hot code.
        long smallNanos = timeDeletes(smallBase, deletes);
        long bigNanos = timeDeletes(bigBase, deletes);

        long smallMillis = TimeUnit.NANOSECONDS.toMillis(smallNanos);
        long bigMillis = TimeUnit.NANOSECONDS.toMillis(bigNanos);
        double ratio = (double) bigNanos / Math.max(1L, smallNanos);

        assertTrue(ratio <= maxRatio,
                "append-mode base deletes must not scale with base size: big(B=100k)="
                        + bigMillis + " ms vs small(B=2k)=" + smallMillis
                        + " ms, ratio=" + String.format(java.util.Locale.ROOT, "%.1f", ratio)
                        + ", expected <= " + maxRatio + " (o2 JSONL-5 O(1) findBaseLine)");
        assertTrue(bigMillis < absoluteBoundMillis,
                deletes + " deletes against a 100k base took " + bigMillis
                        + " ms, expected < " + absoluteBoundMillis + " ms");
    }

    // ─── Identity precision: duplicate-content rows ─────────────────

    /**
     * Two rows with EQUAL content but DISTINCT array instances must resolve
     * to their own exact lines. The old content scan returned the first
     * non-deleted match, so deleting the second duplicate tombstoned the
     * first one's line (wrong delta, resurrection of the deleted row after
     * reload); the identity map pins the exact instance.
     */
    @Test
    void duplicateContentRowsResolveToTheirOwnLines() {
        List<Object[]> loaded = new ArrayList<>();
        Object[] dupA = new Object[] {1L, "same", 7};
        Object[] dupB = new Object[] {1L, "same", 7};
        loaded.add(row(0));
        loaded.add(dupA);
        loaded.add(row(2));
        loaded.add(row(3));
        loaded.add(dupB);
        JsonlDeltaManager dm = new JsonlDeltaManager(0.5);
        List<Object[]> live = new ArrayList<>(loaded);
        dm.onLoad(live, allPresent(5, 3), 5);

        // Delete the SECOND duplicate (position 4): only line 4 is tombstoned.
        deleteAt(dm, live, 4);
        assertEquals(Set.of(4), dm.getDeletedBaseLines(),
                "the exact instance's line is tombstoned, not the content twin's");

        // Delete the FIRST duplicate (position 1, still live after the tail
        // delete): now line 1 is tombstoned too.
        deleteAt(dm, live, 1);
        assertEquals(Set.of(4, 1), dm.getDeletedBaseLines(),
                "each duplicate resolves to its own line");

        assertEquals(3, live.size(), "live list mirrors both removals");
        assertEquals(3, dm.getTotalRowCount(),
                "total = 5 base - 2 tombstones");
    }

    // ─── Post-save delete guard: onSaveComplete registers rows ─────

    /**
     * A row inserted and persisted (onSaveComplete) must stay resolvable by
     * the identity map: deleting it right after the save tombstones its
     * exact appended line instead of falling to the "delta may be
     * incomplete" warn path (registration beyond the prompt sketch — the
     * map would otherwise lose every just-graduated pending row).
     */
    @Test
    void deleteRightAfterSaveResolvesAppendedLine() {
        List<Object[]> loaded = baseRows(5);
        JsonlDeltaManager dm = new JsonlDeltaManager(0.5);
        List<Object[]> live = new ArrayList<>(loaded);
        dm.onLoad(live, allPresent(5, 3), 5);

        // Insert 3 pending rows, then persist them into the base.
        for (int i = 0; i < 3; i++) {
            Object[] pending = row(100 + i);
            live.add(pending);
            dm.onInsert(pending, new boolean[3]);
        }
        dm.onSaveComplete();
        assertEquals(8, dm.getBaseFileLineCount(), "pending rows graduated into the base");
        assertEquals(0, dm.getDeltaRowCount(), "nothing pending after the save");

        // Delete the first appended row (position 5): line 5 is registered.
        deleteAt(dm, live, 5);
        assertEquals(Set.of(5), dm.getDeletedBaseLines(),
                "post-save delete resolves the appended line via the identity map");
        assertEquals(7, dm.getTotalRowCount(), "total = 8 base - 1 tombstone");
    }

    // ─── JSONL-6: pending pair removal keeps both views in sync ─────

    /**
     * Deleting a pending (never-persisted) row removes its PAIR: both the
     * projected row view and the projected presence view shrink together
     * (one pair-shift instead of two parallel list shifts). Update-in-append
     * mode (onDelete of the base row + onInsert of the replacement) must
     * keep the two delta channels independent.
     */
    @Test
    void pendingDeleteKeepsRowAndPresenceViewsPaired() {
        List<Object[]> loaded = baseRows(5);
        JsonlDeltaManager dm = new JsonlDeltaManager(0.5);
        List<Object[]> live = new ArrayList<>(loaded);
        dm.onLoad(live, allPresent(5, 3), 5);

        for (int i = 0; i < 3; i++) {
            Object[] pending = row(100 + i);
            live.add(pending);
            dm.onInsert(pending, new boolean[] {true, false, true});
        }
        assertEquals(3, dm.getPendingNewRows().size());
        assertEquals(3, dm.getPendingNewPresence().size());

        // Delete the first pending row (position 5 == pendingOffset): the
        // pair goes, the two later pendings survive with aligned views.
        deleteAt(dm, live, 5);
        assertEquals(2, dm.getPendingNewRows().size(), "one pending row removed");
        assertEquals(2, dm.getPendingNewPresence().size(), "presence view shrinks with it");
        assertEquals(2, dm.getDeltaRowCount(), "pending-only delta count");
        assertArrayEquals(row(101), dm.getPendingNewRows().get(0),
                "remaining pending rows keep their order");
        assertTrue(dm.getDeletedBaseLines().isEmpty(), "no base tombstone for a pending delete");

        // Update-in-append (engine contract: onDelete sees the OLD row still
        // at rowIndex, then the storage swaps in the new one and onInserts it).
        Object[] replacement = row(200);
        dm.onDelete(2, live);
        live.set(2, replacement);
        dm.onInsert(replacement, new boolean[3]);
        assertEquals(Set.of(2), dm.getDeletedBaseLines(), "the replaced base line is tombstoned");
        assertEquals(3, dm.getPendingNewRows().size(), "the replacement joins the pending list");
        assertEquals(4, dm.getDeltaRowCount(), "1 tombstone + 3 pending");
    }
}
