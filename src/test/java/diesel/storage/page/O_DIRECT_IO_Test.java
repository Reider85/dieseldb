package diesel.storage.page;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Unit tests for O_DIRECT I/O operations.
 */
public class O_DIRECT_IO_Test {

    @Test
    public void testO_DIRECTReadWrite() throws IOException {
        Path testFile = Files.createTempFile("odirect-test", ".dat");
        try {
            int pageSize = 8192;
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            FileChannelIO io = new FileChannelIO(testFile, pageSize);
            
            // Test write
            ByteBuffer writeBuffer = ByteBuffer.allocateDirect(pageSize);
            writeBuffer.putInt(0x12345678);
            writeBuffer.putInt(0xABCDEF00);
            // Fill the rest of the buffer with zeros
            while (writeBuffer.remaining() > 0) {
                writeBuffer.put((byte) 0);
            }
            writeBuffer.flip();
            
            PageId pageId = new PageId(1, 1, 0);
            io.writePage(pageId, writeBuffer);
            
            // Test read
            ByteBuffer readBuffer = io.readPage(pageId);
            assertEquals(0x12345678, readBuffer.getInt());
            assertEquals(0xABCDEF00, readBuffer.getInt());
            
            // Test file size
            assertEquals(pageSize, io.size());
            
            io.close();
        } finally {
            Files.deleteIfExists(testFile);
        }
    }

    @Test
    public void testO_DIRECTMultiplePages() throws IOException {
        Path testFile = Files.createTempFile("odirect-multi-test", ".dat");
        try {
            int pageSize = 4096;
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            FileChannelIO io = new FileChannelIO(testFile, pageSize);
            
            // Write multiple pages
            for (int i = 0; i < 10; i++) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
                buffer.putInt(i); // Page number as marker
                buffer.putInt(0xCAFEBABE); // Magic value
                // Fill the rest of the buffer with zeros
                while (buffer.remaining() > 0) {
                    buffer.put((byte) 0);
                }
                buffer.flip();
                
                PageId pageId = new PageId(1, 1, i);
                io.writePage(pageId, buffer);
            }
            
            // Read back and verify
            for (int i = 0; i < 10; i++) {
                ByteBuffer buffer = io.readPage(new PageId(1, 1, i));
                assertEquals(i, buffer.getInt());
                assertEquals(0xCAFEBABE, buffer.getInt());
            }
            
            // Test file size
            assertEquals(10 * pageSize, io.size());
            
            io.close();
        } finally {
            Files.deleteIfExists(testFile);
        }
    }

    @Test
    public void testO_DIRECTFileExtension() throws IOException {
        Path testFile = Files.createTempFile("odirect-extend-test", ".dat");
        try {
            int pageSize = 8192;
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            FileChannelIO io = new FileChannelIO(testFile, pageSize);
            
            // Write first page
            ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
            buffer.putInt(0x12345678);
            // Fill the rest of the buffer with zeros
            while (buffer.remaining() > 0) {
                buffer.put((byte) 0);
            }
            buffer.flip();
            
            PageId pageId = new PageId(1, 1, 0);
            io.writePage(pageId, buffer);
            
            // Extend file
            long newSize = 3 * pageSize;
            io.extendTo(newSize);
            
            // Verify file size
            assertEquals(newSize, io.size());
            
            // Verify first page still intact
            ByteBuffer readBuffer = io.readPage(pageId);
            assertEquals(0x12345678, readBuffer.getInt());
            
            // Verify new pages are zero-filled
            ByteBuffer zeroBuffer = io.readPage(new PageId(1, 1, 2));
            for (int i = 0; i < pageSize / 4; i++) {
                assertEquals(0, zeroBuffer.getInt());
            }
            
            io.close();
        } finally {
            Files.deleteIfExists(testFile);
        }
    }

    @Test
    public void testO_DIRECTBufferPool() throws IOException {
        int pageSize = 8192;
        int maxBuffers = 5;
        O_DIRECT_BufferPool pool = new O_DIRECT_BufferPool(pageSize, maxBuffers);
        
        // Test buffer acquisition and release
        ByteBuffer buffer1 = pool.getAlignedBuffer(0);
        assertNotNull(buffer1);
        assertEquals(pageSize, buffer1.capacity());
        
        // Buffer should be marked as in-use
        assertNull(pool.getAlignedBuffer(0));
        
        // Release buffer
        pool.releaseAlignedBuffer(0);
        
        // Buffer should be available again
        assertNotNull(pool.getAlignedBuffer(0));
        
        // Test buffer states
        int[] states = pool.getBufferStates();
        assertEquals(maxBuffers, states.length);
        
        pool.getAlignedBuffer(1);
        pool.getAlignedBuffer(2);
        int[] updatedStates = pool.getBufferStates();
        assertEquals(1, updatedStates[0]);
        assertEquals(1, updatedStates[1]);
        assertEquals(1, updatedStates[2]);
    }

    @Test
    public void testO_DIRECTAlignment() throws IOException {
        Path testFile = Files.createTempFile("odirect-align-test", ".dat");
        try {
            int pageSize = 8192;
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            FileChannelIO io = new FileChannelIO(testFile, pageSize);
            
            // Test alignment by writing at specific offsets
            ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
            buffer.putInt(0x12345678);
            buffer.flip();
            
            // Create buffer for each write
            ByteBuffer buffer0 = ByteBuffer.allocateDirect(pageSize);
            buffer0.putInt(0x12345678);
            while (buffer0.remaining() > 0) {
                buffer0.put((byte) 0);
            }
            buffer0.flip();
            
            ByteBuffer buffer1 = ByteBuffer.allocateDirect(pageSize);
            buffer1.putInt(0x12345678);
            while (buffer1.remaining() > 0) {
                buffer1.put((byte) 0);
            }
            buffer1.flip();
            
            // Write at offset 0
            PageId pageId0 = new PageId(1, 1, 0);
            io.writePage(pageId0, buffer0);
            
            // Write at offset 1 (should be page-aligned)
            PageId pageId1 = new PageId(1, 1, 1);
            io.writePage(pageId1, buffer1);
            
            // Verify both pages
            ByteBuffer readBuffer0 = io.readPage(pageId0);
            ByteBuffer readBuffer1 = io.readPage(pageId1);
            assertEquals(0x12345678, readBuffer0.getInt());
            assertEquals(0x12345678, readBuffer1.getInt());
            
            io.close();
        } finally {
            Files.deleteIfExists(testFile);
        }
    }

    @Test
    public void testO_DIRECTErrorHandling() throws IOException {
        Path testFile = Files.createTempFile("odirect-error-test", ".dat");
        try {
            int pageSize = 8192;
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            FileChannelIO io = new FileChannelIO(testFile, pageSize);
            
            // Test null parameters
            assertThrows(NullPointerException.class, () -> io.readPage(null));
            assertThrows(NullPointerException.class, () -> io.writePage(null, ByteBuffer.allocate(pageSize)));
            assertThrows(NullPointerException.class, () -> io.writePage(new PageId(1, 1, 0), null));
            
            // Test buffer size mismatch
            ByteBuffer wrongSize = ByteBuffer.allocate(pageSize / 2);
            assertThrows(IllegalArgumentException.class, () -> io.writePage(new PageId(1, 1, 0), wrongSize));
            
            io.close();
            
            // Test operations on closed IO
            assertThrows(IOException.class, () -> io.readPage(new PageId(1, 1, 0)));
            assertThrows(IOException.class, () -> io.writePage(new PageId(1, 1, 0), ByteBuffer.allocate(pageSize)));
            
        } finally {
            Files.deleteIfExists(testFile);
        }
    }
}