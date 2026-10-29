package diesel;

import diesel.storage.JsonlDeltaManager;
import diesel.storage.JsonlRowStorage;
import diesel.storage.json.JsonParserConfig;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 JSONL-7: {@code JsonlRowStorage.applyDelta} must replay a base-file
 * delta in O(R), not O(D&times;R). Every append-mode load replays the kept
 * {@code .jsonl.delta} sidecar; the old path issued one
 * {@code rows.remove(lineNum)} + {@code rowPresence.remove(lineNum)} per
 * tombstone, each shifting the whole trailing tail — quadratic once
 * deletions accumulate across saves.
 *
 * <p>The fix builds a {@code boolean[]} deleted-line mask and compacts both
 * parallel lists with a single two-pointer sweep. The mask additionally
 * de-duplicates repeated line numbers (the old reverse-sorted removes deleted
 * the wrong neighbour on a duplicate) and skips out-of-range entries,
 * negative ones included (the old code threw AIOOBE and aborted the whole
 * application in its catch-all).
 *
 * <p>Fixtures write the {@code .jsonl} base and {@code .jsonl.delta} files
 * directly (both formats are simple line-based JSON — the same bytes
 * {@code writeDeltaFile} emits), and the timing test invokes the private
 * {@code applyDelta} via reflection (WALFindSegmentIsLogNTest precedent)
 * after loading the base, so the measurement isolates the compaction itself
 * without paying the ~10s 100k-row JSON parse of {@code loadFromFile}.
 */
@Tag("storage")
@StorageType("jsonl")
class ApplyDeltaIsORTest {

    private static final List<String> COLS = List.of("ID", "NAME", "AGE");
    private static final Map<String, Class<?>> TYPES = Map.of(
            "ID", Long.class,
            "NAME", String.class,
            "AGE", Integer.class);

    @TempDir
    Path tempDir;

    /** Private {@code JsonlRowStorage.applyDelta(String)} (WALFindSegmentIsLogNTest reflection precedent). */
    private static final Method APPLY_DELTA;
    static {
        try {
            APPLY_DELTA = JsonlRowStorage.class.getDeclaredMethod("applyDelta", String.class);
            APPLY_DELTA.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static JsonParserConfig appendConfig() {
        return new JsonParserConfig.Builder()
                .writeMode(JsonParserConfig.WriteMode.APPEND)
                .compactionThreshold(0.5)
                .build();
    }

    private JsonlRowStorage newStorage(String name) {
        JsonlRowStorage storage = new JsonlRowStorage(name, COLS, TYPES, appendConfig());
        storage.setDataDir(tempDir.toString());
        storage.open();
        return storage;
    }

    private Path jsonlPath(String name) {
        return tempDir.resolve(name + ".jsonl");
    }

    private Path deltaPath(String name) {
        return tempDir.resolve(name + JsonlDeltaManager.DELTA_FILE_SUFFIX);
    }

    private void writeBase(String name, int rowCount) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(jsonlPath(name), StandardCharsets.UTF_8)) {
            for (int i = 0; i < rowCount; i++) {
                w.write("{\"ID\":" + i + ",\"NAME\":\"n" + i + "\",\"AGE\":" + (i % 97) + "}");
                w.newLine();
            }
        }
    }

    private void writeDelta(String name, int baseLines, int... deletions) throws IOException {
        StringBuilder sb = new StringBuilder("{\"baseLineCount\":").append(baseLines)
                .append(",\"deletions\":[");
        for (int i = 0; i < deletions.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(deletions[i]);
        }
        sb.append("]}\n");
        Files.writeString(deltaPath(name), sb.toString(), StandardCharsets.UTF_8);
    }

    /**
     * Times {@code applyDelta} itself via reflection (WALFindSegmentIsLogNTest
     * precedent): the full {@code loadFromFile} path pays a ~10s 100k-row
     * JSON parse on this machine, which both dwarfs the compaction and would
     * let the old O(D*R) shape pass a load-level ratio bound (parse noise
     * cancels nowhere). Loading the base first and replaying only the delta
     * isolates the exact method under change.
     */
    private long timeApplyDelta(int baseRows, int deletes) throws Exception {
        String name = "ADT";
        Files.deleteIfExists(jsonlPath(name));
        Files.deleteIfExists(deltaPath(name));
        writeBase(name, baseRows);
        int[] lines = new int[deletes];
        int stride = Math.max(1, baseRows / Math.max(1, deletes));
        for (int i = 0; i < deletes; i++) {
            lines[i] = Math.min(baseRows - 1, i * stride);
        }

        // Load the base (no delta file yet): rows materialized, delta
        // manager registered the base via onLoad - exactly the state
        // applyDelta sees in production right after gatherAndCommit.
        JsonlRowStorage storage = newStorage(name);
        storage.loadFromFile(name);

        writeDelta(name, baseRows, lines);
        long start = System.nanoTime();
        APPLY_DELTA.invoke(storage, deltaPath(name).toString());
        long nanos = System.nanoTime() - start;
        assertEquals(baseRows - deletes, storage.scan().size(),
                "every tombstoned line is replayed out of the live rows");
        storage.close();
        return nanos;
    }

    // ─── O(R) structural proof: cost independent of delete count ─────

    /**
     * Same 100k-row base, two delta sizes (2k vs 50k tombstones). The old
     * per-tombstone shifts scaled linearly with D (big/small ~15-30x); the
     * mask compaction is one O(R) sweep either way (measured ~1-3x). Ratio
     * bound 10 separates the shapes with margin; the absolute bound is the
     * catastrophic-regression guard (old D=50k = ~2.5e9 element shifts =
     * seconds, new = milliseconds). Unmeasured JIT warmup on a fresh small
     * fixture first.
     */
    @Test
    @Timeout(60)
    void deltaReplayIsIndependentOfDeleteCount() {
        final int baseRows = 100_000;
        final int smallDeletes = 2_000;
        final int bigDeletes = 50_000;
        final double maxRatio = 10.0;
        final long absoluteBoundMillis = 2_000;

        try {
            // Warmup: hot-compile applyDelta on a small fixture.
            timeApplyDelta(2_000, 200);

            long smallNanos = timeApplyDelta(baseRows, smallDeletes);
            long bigNanos = timeApplyDelta(baseRows, bigDeletes);

            long smallMillis = TimeUnit.NANOSECONDS.toMillis(smallNanos);
            long bigMillis = TimeUnit.NANOSECONDS.toMillis(bigNanos);
            double ratio = (double) bigNanos / Math.max(1L, smallNanos);
            assertTrue(ratio <= maxRatio,
                    "applyDelta must be O(R), not O(D*R): big(D=" + bigDeletes + ")="
                            + bigMillis + " ms vs small(D=" + smallDeletes + ")="
                            + smallMillis + " ms, ratio=" + String.format(Locale.ROOT, "%.1f", ratio)
                            + ", expected <= " + maxRatio + " (o2 JSONL-7)");
            assertTrue(bigMillis < absoluteBoundMillis,
                    "delta replay of " + bigDeletes + " deletes over a " + baseRows
                            + "-row base took " + bigMillis + " ms, expected < "
                            + absoluteBoundMillis + " ms");
        } catch (Exception e) {
            throw new AssertionError("applyDelta replay failed", e);
        }
    }

    // ─── Compaction correctness: duplicates, out-of-range, negatives ─

    /**
     * The mask must keep the exact survivors in base order while tolerating
     * the delta shapes the old code mishandled: a duplicated line number
     * (old reverse-sorted removes deleted the WRONG neighbour — remove(3);
     * remove(3) hit what used to be row 4's slot), an out-of-range high line
     * (skipped by both), and a negative line (old code threw AIOOBE and
     * aborted the whole application in its catch-all — the surviving rows
     * before the exception stayed shifted).
     */
    @Test
    void compactionKeepsExactSurvivorsDespiteMalformedLines() throws IOException {
        String name = "ADCOR";
        writeBase(name, 10);
        writeDelta(name, 10, 1, 3, 3, 7, 999, -1);

        JsonlRowStorage storage = newStorage(name);
        storage.loadFromFile(name);
        List<Map<String, Object>> rows = storage.scan();

        // Lines 1, 3 (once), 7 removed; 999 and -1 skipped.
        assertEquals(7, rows.size(), "exactly three distinct in-range lines removed");
        List<Long> ids = rows.stream().map(r -> (Long) r.get("ID")).toList();
        assertEquals(List.of(0L, 2L, 4L, 5L, 6L, 8L, 9L), ids,
                "survivors keep base order; the duplicate removed its line once");
        assertEquals("n6", rows.get(4).get("NAME"), "row contents stay aligned with IDs");
        storage.close();
    }

    /**
     * A delta deleting every line collapses the table to zero rows and keeps
     * both parallel lists in lockstep (the truncation branch, not just the
     * in-place compaction branch).
     */
    @Test
    void deletingEveryLineYieldsEmptyTable() throws IOException {
        String name = "ADALL";
        int baseRows = 8;
        int[] all = new int[baseRows];
        for (int i = 0; i < baseRows; i++) {
            all[i] = i;
        }
        writeBase(name, baseRows);
        writeDelta(name, baseRows, all);

        JsonlRowStorage storage = newStorage(name);
        storage.loadFromFile(name);
        assertTrue(storage.scan().isEmpty(), "all lines tombstoned -> empty table");
        storage.close();
    }
}
