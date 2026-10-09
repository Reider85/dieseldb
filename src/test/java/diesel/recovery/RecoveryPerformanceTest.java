package diesel.recovery;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PageManager;
import diesel.wal.CommitPayload;
import diesel.wal.DmlPayload;
import diesel.wal.WALConfig;
import diesel.wal.WALEntry;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance acceptance test for full ARIES recovery (prompt 4 #19):
 * analysis + redo + undo over a ~1 GiB WAL must complete in under 30 seconds.
 *
 * <p>Builds a WAL dominated by {@code PAGE_IMAGE} records (worst case: every
 * record is decoded and LSN-checked on each of the three passes), plus a
 * committed transaction with a COMMIT payload and a small active transaction
 * with logical DML records so every phase does real work. The target size can
 * be overridden with {@code -Ddiesel.recovery.perf.bytes=N}.
 */
@Tag("perf")
public class RecoveryPerformanceTest {

    private static final long DEFAULT_TARGET_BYTES = 1L << 30; // 1 GiB
    private static final long MAX_RECOVERY_MS = 30_000;
    private static final int PAGE_SIZE = PageManager.PAGE_SIZE;
    private static final int PAGE_COUNT = 64;
    private static final long SEGMENT_SIZE = 64L * 1024 * 1024; // 64 MiB segments
    private static final int ACTIVE_DML_RECORDS = 1_000;

    @Test
    void fullRecoveryOnOneGigabyteWalCompletesUnderThirtySeconds() throws IOException {
        long targetBytes = Long.getLong("diesel.recovery.perf.bytes", DEFAULT_TARGET_BYTES);
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"),
                "recovery-perf-" + UUID.randomUUID());
        Path walDir = tmp.resolve("wal");
        Path pageFile = tmp.resolve("recovery-perf-pages.bin");
        Files.createDirectories(tmp);

        WALManager walManager = new WALManager(WALConfig.of(walDir, SEGMENT_SIZE));
        try (PageManager pages = PageManager.open(pageFile, PAGE_COUNT * 2, PAGE_SIZE)) {
            PageId[] pageIds = new PageId[PAGE_COUNT];
            for (int p = 0; p < PAGE_COUNT; p++) {
                pageIds[p] = pages.allocatePage(0);
            }
            pages.flush();

            byte[][] images = new byte[PAGE_COUNT][];
            for (int p = 0; p < PAGE_COUNT; p++) {
                Page page = new Page(pageIds[p], PAGE_SIZE);
                page.insert(("perf-" + p).getBytes(StandardCharsets.UTF_8));
                ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
                page.writeTo(buffer);
                images[p] = buffer.array();
            }

            // Committed transaction (txid 1): 100 logical inserts + COMMIT payload.
            byte[][] committedInserts = new byte[100][];
            int[] committedIndexes = new int[100];
            for (int i = 0; i < 100; i++) {
                committedIndexes[i] = i;
                committedInserts[i] = DmlPayload.serialize("T", i, Map.of("ID", (long) i));
                walManager.append(1, WALOpcode.INSERT, null, committedInserts[i]);
            }
            byte[] commitPayload = CommitPayload.serialize(1L, 1L,
                    Map.of("T", java.util.Arrays.stream(committedIndexes).boxed()
                            .toList()),
                    Map.of());
            walManager.append(1, WALOpcode.COMMIT, null, commitPayload);

            // Active transaction (txid 2): logical DML records, never committed.
            for (int i = 0; i < ACTIVE_DML_RECORDS; i++) {
                walManager.append(2, WALOpcode.INSERT, null,
                        DmlPayload.serialize("T", 1000 + i, Map.of("ID", (long) (1000 + i))));
            }

            // Fill the rest of the target with PAGE_IMAGE records (txid 0 =
            // system records: analysis skips them, so only txid 2 stays active).
            long written = 0;
            int p = 0;
            while (written < targetBytes) {
                WALEntry entry = walManager.append(0, WALOpcode.PAGE_IMAGE, null, images[p]);
                written += entry.encodedSize();
                p = (p + 1) % PAGE_COUNT;
            }
            assertTrue(walManager.getLastLsn() > 0, "WAL has entries");

            MvccRedoSink redoSink = payload -> { /* counting happens in RedoPhase */ };
            MvccUndoSink undoSink = new MvccUndoSink() {
                @Override
                public void onInsertUndo(long txid, String table, int rowIndex,
                                         Map<String, Object> inserted) {
                    // no-op: measure scan/decode cost only
                }

                @Override
                public void onUpdateUndo(long txid, String table, int rowIndex,
                                         Map<String, Object> before, Map<String, Object> after) {
                    // no-op
                }

                @Override
                public void onDeleteUndo(long txid, String table, int rowIndex,
                                         Map<String, Object> before) {
                    // no-op
                }
            };

            long startNanos = System.nanoTime();
            RecoveryResult result = ARIESAlgorithm.recover(walManager, pages, null,
                    redoSink, undoSink);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            assertEquals(1, result.getCommittedTxidCount(), "txid 1 committed");
            assertEquals(1, result.getActiveTxidCount(), "txid 2 active");
            assertEquals(ACTIVE_DML_RECORDS, result.getUndo().getUndoneInserts(),
                    "every active INSERT record is undone");
            assertEquals(1, result.getRedo().getCommitsReplayed(),
                    "the COMMIT payload is replayed to the redo sink");
            assertTrue(elapsedMs < MAX_RECOVERY_MS,
                    "full recovery of " + written + " WAL bytes took " + elapsedMs
                            + " ms (limit " + MAX_RECOVERY_MS + " ms)");
        } finally {
            walManager.close();
            deleteTree(tmp);
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        Files.walk(root)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore cleanup errors
                    }
                });
    }
}
