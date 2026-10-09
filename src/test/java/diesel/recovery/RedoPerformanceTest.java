package diesel.recovery;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PageManager;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance acceptance test for the ARIES redo phase (prompt 4 #18):
 * replaying a 1 GB WAL must complete in under 20 seconds.
 *
 * <p>Builds a ~1 GiB WAL of {@code PAGE_IMAGE} records (the worst case for
 * redo: every record must be decoded, LSN-checked and installed), then times
 * {@link RedoPhase#redo(WALManager, PageManager, CheckpointRecord, MvccRedoSink)}.
 * The target size can be overridden with {@code -Ddiesel.redo.perf.bytes=N}
 * for quick smoke runs.
 */
@Tag("perf")
public class RedoPerformanceTest {

    private static final long DEFAULT_TARGET_BYTES = 1L << 30; // 1 GiB
    private static final long MAX_REDO_MS = 20_000;
    private static final int PAGE_SIZE = PageManager.PAGE_SIZE;
    private static final int PAGE_COUNT = 64;
    private static final long SEGMENT_SIZE = 64L * 1024 * 1024; // 64 MiB segments

    @Test
    void redoOnOneGigabyteWalCompletesUnderTwentySeconds() throws IOException {
        long targetBytes = Long.getLong("diesel.redo.perf.bytes", DEFAULT_TARGET_BYTES);
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"),
                "redo-perf-" + UUID.randomUUID());
        Path walDir = tmp.resolve("wal");
        Path pageFile = tmp.resolve("redo-perf-pages.bin");
        Files.createDirectories(tmp);

        WALManager walManager = new WALManager(WALConfig.of(walDir, SEGMENT_SIZE));
        try (PageManager pages = PageManager.open(pageFile, PAGE_COUNT * 2, PAGE_SIZE)) {
            // Base state: PAGE_COUNT allocated pages.
            PageId[] pageIds = new PageId[PAGE_COUNT];
            for (int p = 0; p < PAGE_COUNT; p++) {
                pageIds[p] = pages.allocatePage(0);
            }
            pages.flush();

            // Prebuild one page image per page; append re-encodes it each time,
            // so the loop below is pure WAL I/O (LSN allocation, CRC, write).
            byte[][] images = new byte[PAGE_COUNT][];
            for (int p = 0; p < PAGE_COUNT; p++) {
                Page page = new Page(pageIds[p], PAGE_SIZE);
                page.insert(("perf-" + p).getBytes(StandardCharsets.UTF_8));
                ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
                page.writeTo(buffer);
                images[p] = buffer.array();
            }

            long written = 0;
            long entries = 0;
            int p = 0;
            while (written < targetBytes) {
                WALEntry entry = walManager.append(1, WALOpcode.PAGE_IMAGE, null, images[p]);
                written += entry.encodedSize();
                entries++;
                p = (p + 1) % PAGE_COUNT;
            }
            assertTrue(walManager.getLastLsn() > 0, "WAL has entries");

            long startNanos = System.nanoTime();
            RedoResult result = RedoPhase.redo(walManager, pages, null, null);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            assertEquals(entries, result.getApplied(),
                    "every page image must be applied on a cold base state");
            assertEquals(0, result.getSkipped(), "nothing was redone before");
            assertEquals(walManager.getLastLsn(), result.getLastLsn(),
                    "window ends at the log end");
            assertTrue(elapsedMs < MAX_REDO_MS,
                    "redo of " + written + " WAL bytes took " + elapsedMs
                            + " ms (limit " + MAX_REDO_MS + " ms)");
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
