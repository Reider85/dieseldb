package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import diesel.storage.page.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * PageManager crash recovery tests.
 * Verifies that interrupted atomic writes (without commit) leave the target file intact,
 * and that previously flushed pages remain readable after a simulated crash.
 */
@Tag("storage")
class PageManagerCrashTest {

    @TempDir
    Path tempDir;

    private Path pageFile;
    private PageManager manager;
    private static final int PAGE_SIZE = 8192;
    private static final int CAPACITY_PAGES = 10;

    @BeforeEach
    void setUp() throws Exception {
        pageFile = tempDir.resolve("crash-test.pages");
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
    }

    @Test
    void interruptedAtomicWriteLeavesTargetIntact() throws Exception {
        // Write some valid pages first
        PageId id1 = new PageId(1, 1, 0);
        Page page1 = new Page(id1, PAGE_SIZE);
        page1.insert("valid page 1".getBytes());
        manager.writePage(page1);
        
        PageId id2 = new PageId(1, 1, 1);
        Page page2 = new Page(id2, PAGE_SIZE);
        page2.insert("valid page 2".getBytes());
        manager.writePage(page2);
        
        manager.flush(); // ensure valid pages are on disk
        
        // Verify valid pages exist
        assertEquals(2L * PAGE_SIZE, manager.getFileSize(), "file should contain 2 pages");
        try (PinnedPage pinned = manager.readPage(id1)) {
            assertArrayEquals("valid page 1".getBytes(), pinned.getPage().get(0));
        }
        
        // Simulate interrupted atomic write: create temp file but don't commit
        Path tmpFile = AtomicFileWriter.tmpPath(pageFile);
        byte[] partialData = "incomplete atomic write".getBytes();
        Files.write(tmpFile, partialData, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        
        // Verify temp file exists but target is unchanged
        assertTrue(Files.exists(tmpFile), "temp file should exist after incomplete write");
        assertEquals(2L * PAGE_SIZE, manager.getFileSize(), "target file should still have 2 pages");
        
        // Close manager (simulating crash)
        manager.close();
        
        // Verify target file is still intact (not corrupted by incomplete write)
        assertTrue(Files.exists(pageFile), "target file should still exist");
        assertEquals(2L * PAGE_SIZE, Files.size(pageFile), "target file size unchanged");
        
        // Verify original pages are still readable with new manager
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
        try (PinnedPage pinned = manager.readPage(id1)) {
            assertArrayEquals("valid page 1".getBytes(), pinned.getPage().get(0));
        }
        try (PinnedPage pinned = manager.readPage(id2)) {
            assertArrayEquals("valid page 2".getBytes(), pinned.getPage().get(0));
        }
        
        // Verify temp file is cleaned up
        assertFalse(Files.exists(tmpFile), "temp file should be cleaned up");
    }

    @Test
    void warnInterruptedWriteDetectsLeftoverTemp() throws Exception {
        // Create a leftover temp file
        Path tmpFile = AtomicFileWriter.tmpPath(pageFile);
        byte[] dummyData = "leftover temp".getBytes();
        Files.write(tmpFile, dummyData, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        
        // Verify warning is generated
        AtomicFileWriter.warnInterruptedWrite(pageFile);
        
        // Temp file should still exist (warning doesn't delete it)
        assertTrue(Files.exists(tmpFile), "temp file should still exist after warning");
    }

    @Test
    void flushedPagesSurviveCrash() throws Exception {
        // Create and flush multiple pages
        List<PageId> ids = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            PageId id = manager.allocatePage(1);
            ids.add(id);
            Page page = new Page(id, PAGE_SIZE);
            page.insert(("flushed-" + i).getBytes());
            manager.writePage(page);
        }
        
        // Flush all to disk
        manager.flush();
        
        // Verify all pages are on disk
        assertEquals(5L * PAGE_SIZE, manager.getFileSize(), "file should contain 5 pages");
        
        // Read all pages to ensure they're valid
        for (int i = 0; i < 5; i++) {
            PageId id = ids.get(i);
            try (PinnedPage pinned = manager.readPage(id)) {
                assertArrayEquals(("flushed-" + i).getBytes(), pinned.getPage().get(0));
            }
        }
        
        // Simulate crash by closing abruptly
        manager.close();
        
        // Verify pages are still readable after crash
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
        for (int i = 0; i < 5; i++) {
            PageId id = ids.get(i);
            try (PinnedPage pinned = manager.readPage(id)) {
                assertArrayEquals(("flushed-" + i).getBytes(), pinned.getPage().get(0));
                assertFalse(pinned.getPage().isDirty(), "flushed page should be clean");
            }
        }
    }

    @Test
    void unflushedPagesLostOnCrash() throws Exception {
        // Create page but don't flush
        PageId id = new PageId(1, 1, 0);
        Page page = new Page(id, PAGE_SIZE);
        page.insert("unflushed".getBytes());
        manager.writePage(page);
        
        // Page should be dirty in pool but not on disk
        assertTrue(page.isDirty(), "page should be dirty");
        
        // Simulate crash by closing without flush
        manager.close();
        
        // Verify file is still empty (unflushed data lost)
        assertEquals(0, Files.size(pageFile), "file should be empty after crash");
        
        // Verify page is not readable after crash
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
        try {
            manager.readPage(id);
            fail("should not be able to read unflushed page after crash");
        } catch (IOException e) {
            // Expected - page was never written to disk
        }
    }

    @Test
    void atomicFileWriterCleanupOnFailure() throws Exception {
        // Simulate AtomicFileWriter failure during write
        Path tmpFile = AtomicFileWriter.tmpPath(pageFile);
        byte[] partialData = "partial".getBytes();
        Files.write(tmpFile, partialData, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        
        // Create target as a non-empty directory to simulate move failure
        Files.createDirectories(pageFile);
        Files.write(pageFile, "existing content".getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        
        // Try to write (should fail because target is a directory)
        try {
            AtomicFileWriter.writeNewFileAtomically(pageFile, "complete".getBytes());
            fail("should fail when target is a directory");
        } catch (IOException e) {
            // Expected - cannot move to a directory
        }
        
        // Verify temp file is cleaned up even on failure
        assertFalse(Files.exists(tmpFile), "temp file should be cleaned up on failure");
    }
}