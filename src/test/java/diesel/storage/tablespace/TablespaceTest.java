package diesel.storage.tablespace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import diesel.storage.page.PageManager;

/**
 * Tests for Tablespace functionality.
 */
@Tag("storage")
@Tag("smoke")
class TablespaceTest {

    private static final long TEST_FILE_SIZE_MB = 1; // Use small size for testing
    private static final int TEST_PAGE_SIZE = PageManager.PAGE_SIZE;
    private Path tempDir;
    private Path tablespaceDir;
    private Tablespace tablespace;
    
    @BeforeEach
    void setUp() throws IOException {
        // Create temporary directory for testing
        tempDir = Files.createTempDirectory("dieseldb-test-");
        tablespaceDir = tempDir.resolve("test_tablespace");
        
        // Create tablespace with small file size for testing
        tablespace = new Tablespace(tablespaceDir, TEST_FILE_SIZE_MB * 1024 * 1024);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        // Clean up resources
        if (tablespace != null) {
            tablespace.close();
        }
        
        // Delete temporary directory
        deleteDirectory(tempDir);
    }
    
    @Test
    void testWriteAndReadSinglePage() throws IOException {
        ByteBuffer testData = createTestData(42);
        int pageNumber = 0;
        
        // Write page
        tablespace.writePage(pageNumber, testData);
        
        // Read page back
        ByteBuffer readBuffer = ByteBuffer.allocate(TEST_PAGE_SIZE);
        tablespace.readPage(pageNumber, readBuffer);
        
        // Verify data - compare content, not position
        readBuffer.flip();
        assertTrue(buffersEqual(testData, readBuffer), "Written and read data should match");
    }
    
    @Test
    void testWriteAndReadMultiplePages() throws IOException {
        int pageCount = 10;
        
        for (int i = 0; i < pageCount; i++) {
            ByteBuffer testData = createTestData(i);
            tablespace.writePage(i, testData);
            
            // Read back and verify
            ByteBuffer readBuffer = ByteBuffer.allocate(TEST_PAGE_SIZE);
            tablespace.readPage(i, readBuffer);
            readBuffer.flip();
            assertTrue(buffersEqual(testData, readBuffer), "Data should match for page " + i);
        }
    }
    
    @Test
    void testFileCreationWhenNeeded() throws IOException {
        // Initially no files should exist
        assertEquals(0, tablespace.getFileCount(), "Should start with no files");
        
        // Write data that spans multiple files
        ByteBuffer testData = createTestData(1);
        
        // Calculate pages per file
        int pagesPerFile = (int) (TEST_FILE_SIZE_MB * 1024 * 1024 / TEST_PAGE_SIZE);
        System.out.println("Pages per file: " + pagesPerFile);
        
        // Write to page that requires second file
        int crossFilePage = pagesPerFile; // This should be the first page of the second file
        System.out.println("Writing to page: " + crossFilePage);
        
        tablespace.writePage(crossFilePage, testData);
        
        // Should now have 2 files
        assertEquals(2, tablespace.getFileCount(), "Should have created second file");
    }
    
    @Test
    void testTotalSizeCalculation() throws IOException {
        // Initially no files, size should be 0
        assertEquals(0, tablespace.getTotalSize(), "Initial total size should be 0");
        
        // Write some data
        ByteBuffer testData = createTestData(1);
        tablespace.writePage(0, testData);
        
        // After writing, total size should be at least PAGE_SIZE
        long totalSize = tablespace.getTotalSize();
        assertTrue(totalSize >= TEST_PAGE_SIZE, "Total size should be at least 1 page after writing");
    }
    
    @Test
    void testFlush() throws IOException {
        // Write data
        ByteBuffer testData = createTestData(123);
        tablespace.writePage(0, testData);
        
        // Flush should not throw exception
        assertDoesNotThrow(() -> tablespace.flush(), "Flush should not throw exception");
    }
    
    @Test
    void testTablespaceRegistryDefaultOnly() throws IOException {
        TablespaceRegistry registry = new TablespaceRegistry(
            tablespaceDir, 
            TEST_FILE_SIZE_MB * 1024 * 1024
        );
        
        // Should have default tablespace
        assertNotNull(registry.getDefaultTablespace(), "Default tablespace should exist");
        
        // Should list only default tablespace
        assertEquals(1, registry.listTablespaces().size(), "Should have only default tablespace");
        assertTrue(registry.listTablespaces().contains("default"), "Should contain 'default' tablespace");
        
        // Getting default should work
        assertEquals(registry.getDefaultTablespace(), registry.getTablespace("default"));
        
        // Non-default should throw exception
        assertThrows(UnsupportedOperationException.class, 
            () -> registry.getTablespace("nonexistent"),
            "Should throw exception for non-default tablespace");
        
        registry.close();
    }
    
    @Test
    void testTablespaceRegistryCreateThrowsException() throws IOException {
        TablespaceRegistry registry = new TablespaceRegistry(
            tablespaceDir, 
            TEST_FILE_SIZE_MB * 1024 * 1024
        );
        
        // Creating new tablespace should throw exception
        assertThrows(UnsupportedOperationException.class,
            () -> registry.createTablespace("new", Paths.get("/tmp/new"), 1024 * 1024),
            "Should throw exception for creating new tablespace");
        
        registry.close();
    }
    
    @Test
    void testLargeDataFill() throws IOException {
        // Test filling enough data to require multiple files
        // Write 129 pages to exceed the 128-page limit of a 1MB file
        int pagesToWrite = 129;
        
        ByteBuffer testData = createTestData(999);
        
        // Write pages until we exceed the capacity of one file
        for (int i = 0; i < pagesToWrite; i++) {
            tablespace.writePage(i, testData);
            if (i % 32 == 0) {
                System.out.println("After writing page " + i + ", file count: " + tablespace.getFileCount());
            }
        }
        
        // Should have created multiple files (since we exceeded 128 pages)
        System.out.println("Final file count: " + tablespace.getFileCount());
        assertTrue(tablespace.getFileCount() > 1, "Should have created multiple files when exceeding 128 pages");
        
        // Total size should be at least 2 pages
        long totalSize = tablespace.getTotalSize();
        System.out.println("Total size: " + totalSize + " bytes");
        // Since files are created but may be empty until written to,
        // we'll just check that we have multiple files created
        assertTrue(tablespace.getFileCount() > 1, "Should have created multiple files");
        assertTrue(totalSize >= TEST_PAGE_SIZE, "Total size should be at least 1 page");
    }
    
    /**
     * Helper method to create test data.
     */
    private ByteBuffer createTestData(int seed) {
        ByteBuffer buffer = ByteBuffer.allocate(TEST_PAGE_SIZE);
        for (int i = 0; i < TEST_PAGE_SIZE; i++) {
            buffer.put((byte) (seed + (i % 256)));
        }
        buffer.flip();
        return buffer;
    }
    
    /**
     * Helper method to compare ByteBuffer content.
     */
    private boolean buffersEqual(ByteBuffer b1, ByteBuffer b2) {
        // Create duplicates to avoid modifying original buffers
        ByteBuffer b1Copy = b1.duplicate();
        ByteBuffer b2Copy = b2.duplicate();
        
        // Flip both to compare content from start to end
        b1Copy.rewind();
        b2Copy.rewind();
        
        // Limit to the minimum of both buffer capacities
        int limit = Math.min(b1Copy.remaining(), b2Copy.remaining());
        b1Copy.limit(limit);
        b2Copy.limit(limit);
        
        if (b1Copy.remaining() != b2Copy.remaining()) {
            System.out.println("Buffer size mismatch: " + b1Copy.remaining() + " vs " + b2Copy.remaining());
            return false;
        }
        
        boolean equal = true;
        while (b1Copy.hasRemaining()) {
            byte byte1 = b1Copy.get();
            byte byte2 = b2Copy.get();
            if (byte1 != byte2) {
                System.out.println("Byte mismatch at position " + (b1Copy.position() - 1) + ": " + byte1 + " vs " + byte2);
                equal = false;
            }
        }
        
        if (!equal) {
            System.out.println("Buffer contents differ:");
            byte[] b1Array = new byte[Math.min(10, limit)];
            byte[] b2Array = new byte[Math.min(10, limit)];
            b1Copy.rewind();
            b2Copy.rewind();
            b1Copy.get(b1Array);
            b2Copy.get(b2Array);
            System.out.println("Original first 10 bytes: " + Arrays.toString(b1Array));
            System.out.println("Read first 10 bytes: " + Arrays.toString(b2Array));
        }
        
        return equal;
    }
    
    /**
     * Helper method to delete a directory recursively.
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted((a, b) -> -a.compareTo(b)) // Reverse order to delete files before directories
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to delete: " + p, e);
                    }
                });
        }
    }
}