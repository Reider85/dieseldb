package diesel.recovery;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PageManager;
import diesel.storage.page.PinnedPage;
import diesel.wal.CommitPayload;
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
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Acceptance test for the ARIES redo phase (prompt 4 #18).
 *
 * <p>Core criterion: after a kill mid-workload, {@link RedoPhase} replays the
 * WAL window and every page matches the last committed after-image (content
 * and page LSN), while records before the checkpoint stay excluded.
 */
@Tag("smoke")
@Tag("storage")
public class RedoTest {

    private static final int PAGE_SIZE = PageManager.PAGE_SIZE;

    private Path walDir;
    private Path pageDir;
    private Path pageFile;
    private WALManager wal;
    private PageManager pages;

    @BeforeEach
    void setUp() throws IOException {
        String id = "redo-test-" + UUID.randomUUID();
        walDir = Path.of(System.getProperty("java.io.tmpdir"), id + "-wal");
        pageDir = Path.of(System.getProperty("java.io.tmpdir"), id + "-pages");
        pageFile = pageDir.resolve("redo-pages.bin");
        Files.createDirectories(pageDir);
        wal = new WALManager(WALConfig.of(walDir, 1024 * 1024));
        pages = PageManager.open(pageFile, 128, PAGE_SIZE);
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

    /** Builds a full page after-image containing {@code marker} in slot 0. */
    private static byte[] image(PageId pageId, String marker) {
        Page page = new Page(pageId, PAGE_SIZE);
        page.insert(marker.getBytes(StandardCharsets.UTF_8));
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        page.writeTo(buffer); // syncs the header into the image
        return buffer.array();
    }

    private WALEntry appendImage(PageId pageId, String marker) throws IOException {
        return wal.append(1, WALOpcode.PAGE_IMAGE, null, image(pageId, marker));
    }

    /** Reads the marker from slot 0 of the given page. */
    private static String marker(PageManager manager, PageId pageId) throws IOException {
        try (PinnedPage pinned = manager.readPage(pageId)) {
            Page page = pinned.getPage();
            assertEquals(1, page.getSlotCount(), "page " + pageId + " must hold the marker slot");
            byte[] tuple = page.get(0);
            assertNotNull(tuple, "page " + pageId + " slot 0 must not be empty");
            return new String(tuple, StandardCharsets.UTF_8);
        }
    }

    // --- tests ---

    @Test
    void thousandMixedOpsAfterKillRedoMatchesLastCommittedState() throws IOException {
        int pageCount = 40;
        PageId[] pageIds = new PageId[pageCount];
        for (int p = 0; p < pageCount; p++) {
            pageIds[p] = pages.allocatePage(0);
        }
        pages.flush();

        // 1000 WAL records: every 10th is a logical record (BEGIN/INSERT/
        // payload-less COMMIT/ABORT), the other 900 are page images cycling
        // over the 40 pages.
        List<WALEntry> pageEntries = new ArrayList<>(900);
        long[] lastOp = new long[pageCount];
        long[] lastLsn = new long[pageCount];
        int pageEntryIdx = 0;
        for (int i = 1; i <= 1000; i++) {
            if (i % 10 == 0) {
                long txid = 5_000 + i;
                switch ((i / 10) % 4) {
                    case 0 -> wal.append(txid, WALOpcode.BEGIN, null, null);
                    case 1 -> wal.append(txid, WALOpcode.INSERT, null, new byte[]{1, 2, 3});
                    case 2 -> wal.append(txid, WALOpcode.COMMIT, null, null);
                    default -> wal.append(txid, WALOpcode.ABORT, null, null);
                }
            } else {
                int p = pageEntryIdx % pageCount;
                WALEntry entry = appendImage(pageIds[p], "op-" + i);
                pageEntries.add(entry);
                lastOp[p] = i;
                lastLsn[p] = entry.getLsn();
                pageEntryIdx++;
            }
        }
        assertEquals(1000, wal.getLastLsn(), "exactly 1000 records");
        assertEquals(900, pageEntries.size(), "900 page-image records");

        // Simulated kill: the first 150 page images reached disk, the next 150
        // were still dirty in the buffer pool when the process died.
        for (int i = 0; i < 150; i++) {
            assertTrue(pages.applyRedo(pageEntries.get(i)), "prefix entry " + i + " must apply");
        }
        pages.flush();
        for (int i = 150; i < 300; i++) {
            assertTrue(pages.applyRedo(pageEntries.get(i)), "pool entry " + i + " must apply");
        }
        pages.closeDiscardingDirty(); // kill — unflushed images are lost

        try (PageManager recovered = PageManager.open(pageFile, 128, PAGE_SIZE)) {
            RedoResult result = RedoPhase.redo(wal, recovered);

            assertEquals(750, result.getApplied(), "only the 750 records lost by the crash are re-applied");
            assertEquals(150, result.getSkipped(), "the durable prefix is skipped by the page LSN check");
            assertEquals(100, result.getIgnored(), "logical records carry no page after-image");
            assertEquals(1000, result.getTotalRecords(), "every record lands in exactly one bucket");
            assertEquals(1000, result.getLastLsn(), "window ends at the log end");

            for (int p = 0; p < pageCount; p++) {
                assertEquals("op-" + lastOp[p], marker(recovered, pageIds[p]),
                        "page " + pageIds[p] + " content must match the last committed image");
                try (PinnedPage pinned = recovered.readPage(pageIds[p])) {
                    assertEquals(lastLsn[p], pinned.getPage().getLsn(),
                            "page " + pageIds[p] + " LSN must be the last committed record");
                }
            }
        }
    }

    @Test
    void redoWindowStartsAfterCheckpoint() throws IOException {
        PageId persistedPage = pages.allocatePage(0);
        PageId neverDurable = pages.allocatePage(0);
        pages.flush();

        // Pre-checkpoint record that is already durable on disk.
        WALEntry durable = appendImage(persistedPage, "before");
        assertTrue(pages.applyRedo(durable));
        pages.flush();

        // Pre-checkpoint record that only exists in the WAL (written just
        // before the checkpoint, never applied — the window must exclude it).
        wal.append(9, WALOpcode.PAGE_IMAGE, null, image(neverDurable, "pre-only"));
        wal.writeCheckpoint(List.of(9L)); // consumes the next LSN

        // Post-checkpoint record lost by the crash.
        WALEntry after = appendImage(persistedPage, "after");

        try (PageManager recovered = PageManager.open(pageFile, 128, PAGE_SIZE)) {
            RedoResult result = RedoPhase.redo(wal, recovered);

            assertEquals(1, result.getApplied(), "only the post-checkpoint record is replayed");
            assertEquals(0, result.getSkipped(), "pre-checkpoint records are outside the window");
            assertEquals(after.getLsn(), result.getLastLsn(), "window ends at the log end");

            assertEquals("after", marker(recovered, persistedPage), "post-checkpoint image wins");
            try (PinnedPage pinned = recovered.readPage(neverDurable)) {
                assertEquals(0, pinned.getPage().getSlotCount(),
                        "pre-checkpoint record must be excluded from the window (not just LSN-skipped)");
                assertEquals(0, pinned.getPage().getLsn(), "page never redone");
            }
        }
    }

    @Test
    void redoCreatesPageBeyondFileEnd() throws IOException {
        PageId allocated = pages.allocatePage(0);
        pages.flush();
        assertEquals(1, pages.getNextPageNum(), "one page allocated");

        PageId far = new PageId(0, 1, 99);
        WALEntry entry = appendImage(far, "far");

        RedoResult result = RedoPhase.redo(wal, pages);

        assertEquals(1, result.getApplied(), "allocation redo creates the missing page");
        assertEquals("far", marker(pages, far), "created page carries the image");
        try (PinnedPage pinned = pages.readPage(far)) {
            assertEquals(entry.getLsn(), pinned.getPage().getLsn(), "page LSN stamped");
        }
        assertEquals(100, pages.getNextPageNum(), "allocator watermark advanced past the redone page");

        PageId next = pages.allocatePage(0);
        assertEquals(100, next.pageNum(), "next allocation must not address the redone page");
        assertEquals(0, markerSlotCount(pages, next), "newly allocated page is empty");
    }

    private static int markerSlotCount(PageManager manager, PageId pageId) throws IOException {
        try (PinnedPage pinned = manager.readPage(pageId)) {
            return pinned.getPage().getSlotCount();
        }
    }

    @Test
    void emptyWalRedoIsNoOp() throws IOException {
        pages.allocatePage(0);
        pages.flush();

        RedoResult result = RedoPhase.redo(wal, pages);

        assertEquals(0, result.getTotalRecords(), "no records to replay");
        assertEquals(0, result.getApplied(), "nothing applied");
        assertEquals(0, result.getLastLsn(), "empty log");
    }

    @Test
    void commitPayloadsAreReplayedToMvccSink() throws IOException {
        wal.append(7, WALOpcode.BEGIN, null, null);
        wal.append(7, WALOpcode.INSERT, null, new byte[]{9, 9});
        byte[] payload = CommitPayload.serialize(7, 42L,
                Map.of("t1", List.of(0, 5)), Map.of("t1", List.of(2)));
        wal.append(7, WALOpcode.COMMIT, null, payload);
        wal.append(8, WALOpcode.COMMIT, null, null); // legacy payload-less commit
        PageId pageId = pages.allocatePage(0);
        wal.append(1, WALOpcode.PAGE_IMAGE, null, image(pageId, "mvcc"));

        List<CommitPayload> received = new ArrayList<>();
        RedoResult result = RedoPhase.redo(wal, pages, null, received::add);

        assertEquals(1, result.getApplied(), "one page image replayed");
        assertEquals(1, result.getCommitsReplayed(), "one COMMIT payload delivered to the MVCC sink");
        assertEquals(3, result.getIgnored(), "BEGIN + INSERT + payload-less COMMIT");
        assertEquals(5, result.getTotalRecords(), "all records accounted for");

        assertEquals(1, received.size(), "sink receives exactly one commit");
        CommitPayload replayed = received.get(0);
        assertEquals(7, replayed.getTxid(), "committed txid restored");
        assertEquals(42L, replayed.getCommitCsn(), "commit CSN restored");
        assertArrayEquals(new int[]{0, 5}, replayed.getModifiedRows().get("t1"),
                "modified row indexes restored");
        assertArrayEquals(new int[]{2}, replayed.getDeletedRows().get("t1"),
                "deleted row indexes restored");
        assertEquals("mvcc", marker(pages, pageId), "physical redo unaffected by MVCC replay");
    }
}
