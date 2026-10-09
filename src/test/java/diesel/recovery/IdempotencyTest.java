package diesel.recovery;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PageManager;
import diesel.storage.page.PinnedPage;
import diesel.wal.WALConfig;
import diesel.wal.WALEntry;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Acceptance test for redo idempotency (prompt 4 #18).
 *
 * <p>Core criterion: re-running {@link RedoPhase} over an already-redone page
 * is a no-op — the page LSN check makes every replayed after-image safe to
 * apply again, whether the previous pass reached disk or died before flushing.
 */
@Tag("smoke")
@Tag("storage")
public class IdempotencyTest {

    private static final int PAGE_SIZE = PageManager.PAGE_SIZE;
    private static final int PAGE_COUNT = 10;
    private static final int IMAGES_PER_PAGE = 5;

    private Path walDir;
    private Path pageDir;
    private Path pageFile;
    private WALManager wal;
    private PageManager pages;
    private List<WALEntry> pageEntries;

    @BeforeEach
    void setUp() throws IOException {
        String id = "idempotency-test-" + UUID.randomUUID();
        walDir = Path.of(System.getProperty("java.io.tmpdir"), id + "-wal");
        pageDir = Path.of(System.getProperty("java.io.tmpdir"), id + "-pages");
        pageFile = pageDir.resolve("idempotency-pages.bin");
        Files.createDirectories(pageDir);
        wal = new WALManager(WALConfig.of(walDir, 1024 * 1024));
        pages = PageManager.open(pageFile, 64, PAGE_SIZE);
        pageEntries = writeMixedWal();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (pages != null) {
            pages.close();
        }
        if (wal != null) {
            wal.close();
        }
        deleteTree(walDir);
        deleteTree(pageDir);
    }

    // --- helpers ---

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

    private static byte[] image(PageId pageId, String marker) {
        Page page = new Page(pageId, PAGE_SIZE);
        page.insert(marker.getBytes(StandardCharsets.UTF_8));
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        page.writeTo(buffer);
        return buffer.array();
    }

    /**
     * Writes 50 page-image records (5 per page over 10 pages) interleaved with
     * 5 logical records, and returns the page-image entries in LSN order.
     */
    private List<WALEntry> writeMixedWal() throws IOException {
        PageId[] pageIds = new PageId[PAGE_COUNT];
        for (int p = 0; p < PAGE_COUNT; p++) {
            pageIds[p] = pages.allocatePage(0);
        }
        pages.flush();

        List<WALEntry> entries = new ArrayList<>(PAGE_COUNT * IMAGES_PER_PAGE);
        int counter = 0;
        for (int round = 0; round < IMAGES_PER_PAGE; round++) {
            for (int p = 0; p < PAGE_COUNT; p++) {
                counter++;
                entries.add(wal.append(1, WALOpcode.PAGE_IMAGE, null,
                        image(pageIds[p], "r-" + counter)));
            }
            wal.append(7, WALOpcode.BEGIN, null, null);
            wal.append(7, WALOpcode.INSERT, null, new byte[]{7});
            wal.append(7, WALOpcode.COMMIT, null, null);
            wal.append(8, WALOpcode.ABORT, null, null);
            wal.append(8, WALOpcode.BEGIN, null, null);
        }
        return entries;
    }

    /** Returns marker and LSN of every page for state comparison. */
    private List<String> snapshotState(PageManager manager) throws IOException {
        List<String> state = new ArrayList<>();
        for (int p = 0; p < PAGE_COUNT; p++) {
            try (PinnedPage pinned = manager.readPage(new PageId(0, 1, p))) {
                Page page = pinned.getPage();
                byte[] tuple = page.getSlotCount() > 0 ? page.get(0) : null;
                state.add(page.getLsn() + ":" + (tuple == null ? "-" : new String(tuple, StandardCharsets.UTF_8)));
            }
        }
        return state;
    }

    // --- tests ---

    @Test
    void secondRedoOnAlreadyRedonePagesIsNoOp() throws IOException {
        RedoResult first = RedoPhase.redo(wal, pages);
        assertEquals(50, first.getApplied(), "first pass applies every page image");
        assertEquals(0, first.getSkipped(), "first pass has nothing to skip");
        assertEquals(25, first.getIgnored(), "25 logical records are ignored");
        List<String> stateAfterFirst = snapshotState(pages);

        RedoResult second = RedoPhase.redo(wal, pages);
        assertEquals(0, second.getApplied(), "second pass applies nothing");
        assertEquals(50, second.getSkipped(), "every page image is skipped by the LSN check");
        assertEquals(25, second.getIgnored(), "logical records are ignored again");
        assertEquals(first.getTotalRecords(), second.getTotalRecords(), "same window scanned");
        assertEquals(stateAfterFirst, snapshotState(pages), "page state unchanged by the re-run");

        RedoResult third = RedoPhase.redo(wal, pages);
        assertEquals(0, third.getApplied(), "redo stays a no-op on any number of re-runs");
        assertEquals(50, third.getSkipped(), "still 50 skips");
    }

    @Test
    void redoAfterCrashWithoutFlushReapplies() throws IOException {
        // First recovery attempt applies everything but dies before flushing:
        // the pool holds the new state, disk does not.
        for (WALEntry entry : pageEntries) {
            assertTrue(pages.applyRedo(entry), "entry " + entry.getLsn() + " must apply in memory");
        }
        pages.closeDiscardingDirty(); // crash — nothing persisted

        try (PageManager recovered = PageManager.open(pageFile, 64, PAGE_SIZE)) {
            RedoResult result = RedoPhase.redo(wal, recovered);

            assertEquals(50, result.getApplied(),
                    "disk never saw the images, so the whole window is re-applied");
            assertEquals(0, result.getSkipped(), "nothing durable to skip");

            // Final state: each page shows the last image written for it.
            for (int p = 0; p < PAGE_COUNT; p++) {
                try (PinnedPage pinned = recovered.readPage(new PageId(0, 1, p))) {
                    Page page = pinned.getPage();
                    assertEquals(1, page.getSlotCount(), "page has the marker slot");
                    assertEquals("r-" + (p + 1 + (IMAGES_PER_PAGE - 1) * PAGE_COUNT),
                            new String(page.get(0), StandardCharsets.UTF_8),
                            "page " + p + " shows the last image");
                }
            }

            // And a re-run over this now-durable state is a no-op again.
            RedoResult again = RedoPhase.redo(wal, recovered);
            assertEquals(0, again.getApplied(), "durable state is not re-applied");
            assertEquals(50, again.getSkipped(), "all images skipped");
        }
    }

    @Test
    void applyRedoRejectsNonPageRecords() throws IOException {
        WALEntry begin = wal.append(1, WALOpcode.BEGIN, null, null);
        assertThrows(IllegalArgumentException.class, () -> pages.applyRedo(begin),
                "logical records carry no page after-image");

        WALEntry emptyImage = wal.append(1, WALOpcode.PAGE_IMAGE, null, null);
        assertThrows(IllegalArgumentException.class, () -> pages.applyRedo(emptyImage),
                "page-image record without an after-image is rejected");

        pages.allocatePage(0);
        pages.flush();
        RedoResult result = RedoPhase.redo(wal, pages);
        assertEquals(50, result.getApplied(), "the 50 valid page images from setup still apply");
        assertEquals(0, result.getSkipped(), "nothing was redone before");
        assertEquals(27, result.getIgnored(),
                "25 logical + BEGIN + image-less PAGE_IMAGE are all ignored");
        assertEquals(77, result.getTotalRecords(), "every record lands in exactly one bucket");
    }
}
