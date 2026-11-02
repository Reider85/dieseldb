package diesel.replication;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;

import diesel.Database;
import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALArchiver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/**
 * Tests for WAL archiver integration with replication slots (prompt4 #59).
 */
@Tag("wal")
@Tag("replication")
class WALArchiverReplicationSlotTest {

    private Database database;
    private WALConfig walConfig;
    private WALManager walManager;
    private Path walDir;
    private Path archiveDir;
    private Clock testClock;

    @BeforeEach
    void setUp() throws Exception {
        // Create test directories
        walDir = Files.createTempDirectory("wal_test");
        archiveDir = Files.createTempDirectory("archive_test");
        
        // Create test database
        database = new Database("test_data_wal_replication");
        
        // Clear any existing replication slots to ensure clean state
        
        // Create test WAL config
        walConfig = WALConfig.of(walDir, 1024 * 1024); // 1MB minimum segment size
        // Set additional properties via reflection since we can't use Builder
        try {
            var archiveDirField = WALConfig.class.getDeclaredField("archiveDir");
            archiveDirField.setAccessible(true);
            archiveDirField.set(walConfig, archiveDir);
            
            var archiveRetentionDaysField = WALConfig.class.getDeclaredField("archiveRetentionDays");
            archiveRetentionDaysField.setAccessible(true);
            archiveRetentionDaysField.set(walConfig, 1);
            
            var enabledField = WALConfig.class.getDeclaredField("enabled");
            enabledField.setAccessible(true);
            enabledField.set(walConfig, true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set WALConfig properties", e);
        }
        
        // Create test clock
        testClock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        
        // Create WAL manager with database reference
        walManager = new WALManager(walConfig, testClock, database);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (walManager != null) {
            walManager.close();
        }
        if (database != null) {
            database.close();
        }
        
        // Clean up test directories
        deleteDirectory(walDir);
        deleteDirectory(archiveDir);
        new java.io.File("test_data_wal_replication").delete();
    }

    @Test
    void testWALArchiverCreationWithDatabase() {
        WALArchiver archiver = walManager.getArchiver();
        assertNotNull(archiver);
        
        // Verify archiver has access to database
        // This is tested indirectly through the retention policy logic
    }

    @Test
    void testRetentionPolicyWithReplicationSlots() throws Exception {
        // Create some test WAL segments
        createTestWALSegment(walDir, 1, 1000);
        createTestWALSegment(walDir, 2, 2000);
        createTestWALSegment(walDir, 3, 3000);
        
        // Archive the segments
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0001.log"), 1);
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0002.log"), 2);
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0003.log"), 3);
        
        // Create replication slots
        database.createReplicationSlot("slot1", 1500); // Needs segments 1 and 2
        database.createReplicationSlot("slot2", 2500); // Needs segments 1, 2, and 3
        
        // Set clock to make archives old enough for deletion
        testClock = Clock.fixed(Instant.now().plusSeconds(86400 * 2), ZoneId.systemDefault());
        
        // Run retention - should not delete segments needed by replication slots
        walManager.getArchiver().runRetention();
        
        // Verify archives still exist (they're needed by replication slots)
        assertTrue(Files.exists(archiveDir.resolve("wal.0001.log.gz")));
        assertTrue(Files.exists(archiveDir.resolve("wal.0002.log.gz")));
        assertTrue(Files.exists(archiveDir.resolve("wal.0003.log.gz")));
    }

    @Test
    void testRetentionPolicyAfterSlotConfirmation() throws Exception {
        // Create some test WAL segments
        createTestWALSegment(walDir, 1, 1000);
        createTestWALSegment(walDir, 2, 2000);
        createTestWALSegment(walDir, 3, 3000);
        createTestWALSegment(walDir, 4, 4000);
        
        // Archive the segments
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0001.log"), 1);
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0002.log"), 2);
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0003.log"), 3);
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0004.log"), 4);
        
        // Create replication slots
        database.createReplicationSlot("slot1", 1500); // Needs segments 1 and 2
        database.createReplicationSlot("slot2", 2500); // Needs segments 1, 2, and 3
        
        // Simulate slot confirmation - update confirmed LSN to 4000
        database.updateReplicationSlotConfirmedFlushLsn("slot1", 4000);
        database.updateReplicationSlotConfirmedFlushLsn("slot2", 4000);
        
        // Set clock to make archives old enough for deletion
        testClock = Clock.fixed(Instant.now().plusSeconds(86400 * 2), ZoneId.systemDefault());
        
        // Run retention - should delete all archives since all segments are confirmed
        walManager.getArchiver().runRetention();
        
        // Verify all archives are deleted
        assertFalse(Files.exists(archiveDir.resolve("wal.0001.log.gz")));
        assertFalse(Files.exists(archiveDir.resolve("wal.0002.log.gz")));
        assertFalse(Files.exists(archiveDir.resolve("wal.0003.log.gz")));
        assertFalse(Files.exists(archiveDir.resolve("wal.0004.log.gz")));
    }

    @Test
    void testRetentionPolicyPartialDeletion() throws Exception {
        // Create some test WAL segments
        createTestWALSegment(walDir, 1, 1000);
        createTestWALSegment(walDir, 2, 2000);
        createTestWALSegment(walDir, 3, 3000);
        createTestWALSegment(walDir, 4, 4000);
        
        // Archive the segments
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0001.log"), 1);
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0002.log"), 2);
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0003.log"), 3);
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0004.log"), 4);
        
        // Create replication slot that only needs segments 1 and 2
        database.createReplicationSlot("slot1", 2000);
        
        // Set clock to make archives old enough for deletion
        testClock = Clock.fixed(Instant.now().plusSeconds(86400 * 2), ZoneId.systemDefault());
        
        // Run retention - should delete segments 3 and 4 but keep 1 and 2
        walManager.getArchiver().runRetention();
        
        // Verify archives 3 and 4 are deleted
        assertFalse(Files.exists(archiveDir.resolve("wal.0003.log.gz")));
        assertFalse(Files.exists(archiveDir.resolve("wal.0004.log.gz")));
        
        // Verify archives 1 and 2 are still needed
        assertTrue(Files.exists(archiveDir.resolve("wal.0001.log.gz")));
        assertTrue(Files.exists(archiveDir.resolve("wal.0002.log.gz")));
    }

    @Test
    void testRetentionPolicyNoReplicationSlots() throws Exception {
        // Create some test WAL segments
        createTestWALSegment(walDir, 1, 1000);
        createTestWALSegment(walDir, 2, 2000);
        
        // Archive the segments
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0001.log"), 1);
        walManager.getArchiver().archiveSegment(walDir.resolve("wal.0002.log"), 2);
        
        // Set clock to make archives old enough for deletion
        testClock = Clock.fixed(Instant.now().plusSeconds(86400 * 2), ZoneId.systemDefault());
        
        // Run retention - should delete all archives since no slots exist
        walManager.getArchiver().runRetention();
        
        // Verify all archives are deleted
        assertFalse(Files.exists(archiveDir.resolve("wal.0001.log.gz")));
        assertFalse(Files.exists(archiveDir.resolve("wal.0002.log.gz")));
    }

    @Test
    void testReplicationSlotNeedsWALSegmentsBeyond() throws Exception {
        // Initially no slots
        assertFalse(database.replicationSlotsNeedWALSegmentsBeyond(1000));
        
        // Create slot with initial LSN 2000
        database.createReplicationSlot("test_slot", 2000);
        
        // Slot needs segments beyond LSN 1000 but not beyond 2000
        assertTrue(database.replicationSlotsNeedWALSegmentsBeyond(1000));
        assertTrue(database.replicationSlotsNeedWALSegmentsBeyond(1500));
        assertFalse(database.replicationSlotsNeedWALSegmentsBeyond(2000));
        assertFalse(database.replicationSlotsNeedWALSegmentsBeyond(2500));
        
        // Update slot to higher LSN
        database.updateReplicationSlotConfirmedFlushLsn("test_slot", 4000);
        
        // Now slot needs segments beyond 3000 but not beyond 4000
        assertTrue(database.replicationSlotsNeedWALSegmentsBeyond(3000));
        assertFalse(database.replicationSlotsNeedWALSegmentsBeyond(4000));
    }

    @Test
    void testMinimumConfirmedReplicationLsn() throws Exception {
        // Initially no slots
        assertEquals(-1, database.getMinimumConfirmedReplicationLsn());
        
        // Create first slot with initial LSN 1000
        database.createReplicationSlot("slot1", 1000);
        assertEquals(1000, database.getMinimumConfirmedReplicationLsn());
        
        // Create second slot with initial LSN 2000
        database.createReplicationSlot("slot2", 2000);
        assertEquals(1000, database.getMinimumConfirmedReplicationLsn());
        
        // Update second slot to higher LSN
        database.updateReplicationSlotConfirmedFlushLsn("slot2", 3000);
        assertEquals(1000, database.getMinimumConfirmedReplicationLsn());
        
        // Update first slot to higher LSN
        database.updateReplicationSlotConfirmedFlushLsn("slot1", 2500);
        assertEquals(2500, database.getMinimumConfirmedReplicationLsn());
        
        // Drop first slot
        database.dropReplicationSlot("slot1");
        assertEquals(3000, database.getMinimumConfirmedReplicationLsn());
        
        // Drop second slot
        database.dropReplicationSlot("slot2");
        assertEquals(-1, database.getMinimumConfirmedReplicationLsn());
    }

    private void createTestWALSegment(Path dir, int segmentNumber, long lsn) throws Exception {
        String fileName = String.format("wal.%04d.log", segmentNumber);
        Path segmentFile = dir.resolve(fileName);
        
        // Create a minimal WAL segment file
        String content = String.format("LSN:%d\nSEGMENT:%d\n", lsn, segmentNumber);
        Files.write(segmentFile, content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void deleteDirectory(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (Exception e) {
                        // Ignore errors
                    }
                });
        }
    }
}