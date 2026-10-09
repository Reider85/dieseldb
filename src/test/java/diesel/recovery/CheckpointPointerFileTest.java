package diesel.recovery;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@Tag("storage")
@Tag("smoke")
class CheckpointPointerFileTest {
    @TempDir
    Path tempDir;

    @Test
    void testWriteAndRead() throws IOException {
        CheckpointPointerFile pointerFile = new CheckpointPointerFile(tempDir);
        long lsn = 12345L;
        
        pointerFile.write(lsn);
        assertEquals(lsn, pointerFile.read());
        assertTrue(Files.exists(pointerFile.getPath()));
    }

    @Test
    void testReadMissingFile() throws IOException {
        CheckpointPointerFile pointerFile = new CheckpointPointerFile(tempDir);
        assertEquals(0, pointerFile.read());
    }

    @Test
    void testReadLegacyFileWrongSize() throws IOException {
        Path ptrFile = tempDir.resolve("checkpoint.ptr");
        // Write a 4-byte legacy file (old lastAppendedLSN format)
        Files.write(ptrFile, new byte[]{0x00, 0x00, 0x30, 0x39}, StandardOpenOption.CREATE);
        
        CheckpointPointerFile pointerFile = new CheckpointPointerFile(tempDir);
        assertEquals(0, pointerFile.read()); // Legacy files return 0 (invalid)
    }

    @Test
    void testReadCorrectSize() throws IOException {
        Path ptrFile = tempDir.resolve("checkpoint.ptr");
        // Write 8-byte correct format
        byte[] bytes = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(54321L);
        Files.write(ptrFile, bytes, StandardOpenOption.CREATE);
        
        CheckpointPointerFile pointerFile = new CheckpointPointerFile(tempDir);
        assertEquals(54321L, pointerFile.read());
    }

    @Test
    void testReadTruncatedFile() throws IOException {
        Path ptrFile = tempDir.resolve("checkpoint.ptr");
        // Write only 4 bytes instead of 8
        Files.write(ptrFile, new byte[]{0x00, 0x00, 0x12, 0x34}, StandardOpenOption.CREATE);
        
        CheckpointPointerFile pointerFile = new CheckpointPointerFile(tempDir);
        assertEquals(0, pointerFile.read()); // Invalid size returns 0
    }
}