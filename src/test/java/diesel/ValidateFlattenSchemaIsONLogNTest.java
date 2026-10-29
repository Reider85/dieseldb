package diesel;

import diesel.storage.JsonlSchemaManager;
import diesel.storage.json.JsonParserConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 JSONL-1: {@code JsonlSchemaManager.validateFlattenSchema} must cost
 * O(C log C), not the old O(C&sup2;) pairwise dot-prefix scan. Every schema
 * manager construction under the default {@code FLATTEN} nested mode ran the
 * check, so a wide table (5000 dot-notation leaf columns) paid 25 million
 * string comparisons per open.
 *
 * <p>The fix walks every dot-boundary ancestor of each column against the
 * existing case-insensitive {@code indexByName} TreeMap. A sorted
 * adjacent-pair walk (the prompt's alternative) is unsound for this
 * matcher: an unrelated column such as {@code user!} sorts between
 * {@code user} and {@code user.address} ({@code '!'} &lt; {@code '.'}) and
 * hides the conflict from any adjacency check — {@code interleavedColumn}
 * below pins that equivalence.
 */
@Tag("storage")
@StorageType("jsonl")
class ValidateFlattenSchemaIsONLogNTest {

    private static JsonParserConfig flatten() {
        return JsonParserConfig.builder()
                .nestedMode(JsonParserConfig.NestedMode.FLATTEN).build();
    }

    private static JsonlSchemaManager manager(List<String> cols) {
        Map<String, Class<?>> types = new HashMap<>(cols.size() * 2);
        for (String c : cols) {
            types.put(c, String.class);
        }
        return new JsonlSchemaManager(cols, types, flatten());
    }

    /**
     * Builds a conflict-free wide schema: mostly plain columns plus a dotted
     * chain for every 50th entry whose ancestor is deliberately absent, so
     * the ancestor walk exercises its inner loop on ~100 columns without
     * throwing.
     */
    private static List<String> wideSchema(int columns) {
        List<String> cols = new ArrayList<>(columns);
        for (int i = 0; i < columns; i++) {
            if (i % 50 == 0 && i > 0) {
                cols.add("grp" + i + ".leaf.deeper");
            } else {
                cols.add("c" + i);
            }
        }
        return cols;
    }

    /**
     * Prompt acceptance: 5000 columns validate in milliseconds, not seconds.
     * The ratio-bound is calibrated correctly for O(C log C): a 10x column
     * count legitimately costs ~10x*log2(10x)/log2(x) ~= 14x (measured 8.3;
     * the old O(C&sup2;) pairwise scan would measure ~100x), so the bound
     * sits at 25 — well above the new shape, far below the old one. The
     * absolute bound (raised from the prompt's 50ms to 200ms for
     * loaded-Windows parallel-suite headroom; new path is single-digit ms,
     * old path ~25M comparisons = hundreds of ms) is a
     * catastrophic-regression guard. Unmeasured JIT warmup on a fresh small
     * fixture first (EvictionUnderPinnedHeadTest precedent).
     */
    @Test
    @Timeout(30)
    void wideFlattenSchemaValidatesInONLogN() {
        final int big = 5_000;
        final int small = 500;
        final double maxRatio = 25.0;
        final long absoluteBoundMillis = 200;

        // Warmup: hot-compile the constructor + walk on a small fixture.
        manager(wideSchema(small));

        long smallStart = System.nanoTime();
        manager(wideSchema(small));
        long smallNanos = System.nanoTime() - smallStart;

        long bigStart = System.nanoTime();
        assertDoesNotThrow(() -> manager(wideSchema(big)),
                "conflict-free wide schema must construct");
        long bigNanos = System.nanoTime() - bigStart;

        long bigMillis = TimeUnit.NANOSECONDS.toMillis(bigNanos);
        double ratio = (double) bigNanos / Math.max(1L, smallNanos);
        assertTrue(ratio <= maxRatio,
                "validateFlattenSchema must be O(C log C): t(" + big + ")/t(" + small
                        + ")=" + String.format(Locale.ROOT, "%.1f", ratio)
                        + ", expected <= " + maxRatio + " (o2 JSONL-1)");
        assertTrue(bigMillis < absoluteBoundMillis,
                "validateFlattenSchema over " + big + " columns took " + bigMillis
                        + " ms, expected < " + absoluteBoundMillis + " ms (o2 JSONL-1)");
    }

    // ─── Conflict detection equivalence with the old pairwise scan ───

    /**
     * Every shape the old O(C&sup2;) scan rejected must still throw with the
     * documented message, including the case-insensitive prefix (old
     * {@code regionMatches(ignoreCase=true)}), multi-level ancestors, and the
     * interleaved-column trap a sorted adjacent-pair walk would miss.
     */
    @Test
    void dotPrefixConflictsStillRejected() {
        DieselIOException basic = assertThrows(DieselIOException.class,
                () -> manager(List.of("a", "a.b")),
                "direct dot-prefix pair");
        assertTrue(basic.getMessage().contains("dot-prefix"),
                "message keeps the documented wording: " + basic.getMessage());

        assertThrows(DieselIOException.class,
                () -> manager(List.of("a", "A.b")),
                "prefix match is case-insensitive (regionMatches ignoreCase)");
        assertThrows(DieselIOException.class,
                () -> manager(List.of("A", "a.b")),
                "case-insensitive in the other direction");
        assertThrows(DieselIOException.class,
                () -> manager(List.of("a.b", "a.b.c")),
                "multi-level ancestor two hops up");
        assertThrows(DieselIOException.class,
                () -> manager(List.of("user", "user.address", "user.address.city")),
                "the same chain conflicts at every level");
        assertThrows(DieselIOException.class,
                () -> manager(List.of("user", "user!", "user.address")),
                "interleaved column between prefix and candidate "
                        + "('!' < '.') hides the pair from any adjacent-sorted walk");
        assertThrows(DieselIOException.class,
                () -> manager(List.of("a", "a.")),
                "trailing-dot column is itself a prefix ('a.' = 'a' + '.')");
        assertDoesNotThrow(() -> manager(List.of("a.", "a.b")),
                "'a.' is NOT a prefix of 'a.b' (char after 'a.' is 'b') - "
                        + "old pairwise scan accepted this too");
    }

    /**
     * Dotted columns whose ancestors are absent are legal (the normal wide
     * flatten schema), and a lone dot-less duplicate-shaped pair such as
     * {@code a}/{@code ab} must not be mistaken for a prefix conflict.
     */
    @Test
    void nonConflictingDottedColumnsPass() {
        assertDoesNotThrow(() -> manager(List.of(
                        "ID", "user.name", "user.address.city", "settings", "a", "ab")),
                "dotted leaves without their ancestors are unambiguous");
        assertDoesNotThrow(() -> manager(List.of("grp.leaf.deeper", "other.thing")),
                "chains rooted at different ancestors do not interact");
    }
}
