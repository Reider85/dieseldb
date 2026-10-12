package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import diesel.wal.WALManager;
import diesel.wal.WALSegment;
import diesel.wal.WALConfig;
import diesel.wal.WALEntry;
import diesel.wal.WALOpcode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("storage")
@Tag("smoke")
class WALReadByLsnTest {

    @TempDir
    Path tempDir;

    @Test
    void readByLsnSingleSegment() throws Exception {
        Path walDir = tempDir.resolve("wal");
        java.nio.file.Files.createDirectories(walDir);
        WALConfig config = WALConfig.of(walDir, 64 * 1024); // 64KB segment
        WALManager walManager = new WALManager(config);
        
        // Write some records
        WALEntry entry1 = walManager.append(1, WALOpcode.INSERT, null, "test1".getBytes());
        WALEntry entry2 = walManager.append(2, WALOpcode.INSERT, null, "test2".getBytes());
        WALEntry entry3 = walManager.append(3, WALOpcode.INSERT, null, "test3".getBytes());
        
        // Test reading by LSN
        WALEntry result1 = walManager.readByLsn(1);
        assertEquals(entry1, result1);
        
        WALEntry result2 = walManager.readByLsn(2);
        assertEquals(entry2, result2);
        
        WALEntry result3 = walManager.readByLsn(3);
        assertEquals(entry3, result3);
        
        walManager.close();
    }

    @Test
    void readByLsnMultipleSegments() throws Exception {
        Path walDir = tempDir.resolve("wal");
        java.nio.file.Files.createDirectories(walDir);
        WALConfig config = WALConfig.of(walDir, 1024); // Small segment to force rotation
        WALManager walManager = new WALManager(config);
        
        // Write enough records to trigger multiple segments
        for (int i = 1; i <= 50; i++) {
            walManager.append(i, WALOpcode.INSERT, null, ("test" + i).getBytes());
        }
        
        // Test reading from different segments
        WALEntry result1 = walManager.readByLsn(1);
        assertNotNull(result1);
        
        WALEntry result25 = walManager.readByLsn(25);
        assertNotNull(result25);
        
        WALEntry result50 = walManager.readByLsn(50);
        assertNotNull(result50);
        
        walManager.close();
    }

    @Test
    void readByLsnMissingLsn() throws Exception {
        Path walDir = tempDir.resolve("wal");
        java.nio.file.Files.createDirectories(walDir);
        WALConfig config = WALConfig.of(walDir, 64 * 1024);
        WALManager walManager = new WALManager(config);
        
        // Write some records
        WALEntry entry1 = walManager.append(1, WALOpcode.INSERT, null, "test1".getBytes());
        WALEntry entry3 = walManager.append(3, WALOpcode.INSERT, null, "test3".getBytes()); // Skip LSN 2
        
        // Test reading existing records
        WALEntry result1 = walManager.readByLsn(1);
        assertEquals(entry1, result1);
        
        WALEntry result3 = walManager.readByLsn(3);
        assertEquals(entry3, result3);
        
        // Test reading missing record
        WALEntry result2 = walManager.readByLsn(2);
        assertNull(result2);
        
        walManager.close();
    }

    @Test
    void walSegmentReadByLsn() throws Exception {
        Path walDir = tempDir.resolve("wal");
        java.nio.file.Files.createDirectories(walDir);
        WALSegment segment = WALSegment.create(walDir, 1);
        
        // Write some records
        WALEntry entry1 = new WALEntry(1, 1, WALOpcode.INSERT, null, "test1".getBytes());
        WALEntry entry2 = new WALEntry(2, 2, WALOpcode.INSERT, null, "test2".getBytes());
        WALEntry entry3 = new WALEntry(3, 3, WALOpcode.INSERT, null, "test3".getBytes());
        
        segment.append(entry1);
        segment.append(entry2);
        segment.append(entry3);
        
        // Test reading by LSN
        WALEntry result1 = segment.readByLsn(1);
        assertEquals(entry1, result1);
        
        WALEntry result2 = segment.readByLsn(2);
        assertEquals(entry2, result2);
        
        WALEntry result3 = segment.readByLsn(3);
        assertEquals(entry3, result3);
        
        // Test reading missing LSN
        WALEntry resultMissing = segment.readByLsn(99);
        assertNull(resultMissing);
        
        segment.close();
    }
}