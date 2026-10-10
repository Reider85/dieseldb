package diesel;

import diesel.concurrency.ConflictDetector;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for proper cleanup of rowReaders index after commits and rollbacks.
 * @Tag("concurrency")
 */
@Tag("concurrency")
public class RowReadersCleanupTest {

    @Test
    public void testCleanupAfterCommit() {
        Database db = new Database();
        ConflictDetector detector = db.getConflictDetector();
        
        long txid1 = 1;
        long txid2 = 2;
        long snapshotCsn1 = 100;
        long snapshotCsn2 = 200;
        long commitCsn = 300;
        
        // Start two transactions that read overlapping rows
        detector.beginTracking(txid1, snapshotCsn1);
        detector.beginTracking(txid2, snapshotCsn2);
        
        // Both read the same row
        detector.noteRead(txid1, "test_table", 1);
        detector.noteRead(txid2, "test_table", 1);
        
        // Verify rowReaders tracks both readers
        assertEquals(1, detector.trackedReaderRowCount());
        
        // Commit txid1 - should cleanup its reader registration
        java.util.Map<String, java.util.Set<Integer>> writeSet1 = new java.util.HashMap<>();
        assertDoesNotThrow(() -> {
            detector.noteCommit(txid1, commitCsn, writeSet1);
        });
        
        // Verify only txid2 remains in rowReaders
        assertEquals(1, detector.trackedReaderRowCount());
        
        // Commit txid2 - should cleanup completely
        java.util.Map<String, java.util.Set<Integer>> writeSet2 = new java.util.HashMap<>();
        assertDoesNotThrow(() -> {
            detector.noteCommit(txid2, commitCsn + 1, writeSet2);
        });
        
        // Verify rowReaders is empty
        assertEquals(0, detector.trackedReaderRowCount());
        assertEquals(0, detector.activeSerializableCount());
    }

    @Test
    public void testCleanupAfterRollback() {
        Database db = new Database();
        ConflictDetector detector = db.getConflictDetector();
        
        long txid1 = 1;
        long txid2 = 2;
        long snapshotCsn1 = 100;
        long snapshotCsn2 = 200;
        
        // Start two transactions that read overlapping rows
        detector.beginTracking(txid1, snapshotCsn1);
        detector.beginTracking(txid2, snapshotCsn2);
        
        // Both read the same row
        detector.noteRead(txid1, "test_table", 1);
        detector.noteRead(txid2, "test_table", 1);
        
        // Verify rowReaders tracks both readers
        assertEquals(1, detector.trackedReaderRowCount());
        
        // Rollback txid1 - should cleanup its reader registration
        detector.noteRollback(txid1);
        
        // Verify only txid2 remains in rowReaders
        assertEquals(1, detector.trackedReaderRowCount());
        
        // Rollback txid2 - should cleanup completely
        detector.noteRollback(txid2);
        
        // Verify rowReaders is empty
        assertEquals(0, detector.trackedReaderRowCount());
        assertEquals(0, detector.activeSerializableCount());
    }

    @Test
    public void testCleanupAfterConflict() {
        Database db = new Database();
        ConflictDetector detector = db.getConflictDetector();
        
        long txid1 = 1;
        long txid2 = 2;
        long snapshotCsn1 = 100;
        long snapshotCsn2 = 200;
        long commitCsn = 300;
        
        // Start two transactions
        detector.beginTracking(txid1, snapshotCsn1);
        detector.beginTracking(txid2, snapshotCsn2);
        
        // Both read the same row
        detector.noteRead(txid1, "test_table", 1);
        detector.noteRead(txid2, "test_table", 1);
        
        // Verify rowReaders tracks both readers
        assertEquals(1, detector.trackedReaderRowCount());
        
        // txid2 tries to commit with write to the row - should fail (victim)
        java.util.Map<String, java.util.Set<Integer>> writeSet = new java.util.HashMap<>();
        writeSet.put("test_table", java.util.Set.of(1));
        
        assertThrows(SerializationFailureException.class, () -> {
            detector.noteCommit(txid2, commitCsn, writeSet);
        });
        
        // Verify both txid1 and txid2 reader registrations still exist (cleanup only on success/rollback)
        assertEquals(1, detector.trackedReaderRowCount());
        
        // txid2 should still be tracked (only cleanup on rollback)
        assertEquals(2, detector.activeSerializableCount());
        
        // txid2 rollback should cleanup its registration
        detector.noteRollback(txid2);
        
        // Verify only txid1 remains
        assertEquals(1, detector.trackedReaderRowCount());
        
        // txid1 can commit successfully
        java.util.Map<String, java.util.Set<Integer>> writeSet1 = new java.util.HashMap<>();
        assertDoesNotThrow(() -> {
            detector.noteCommit(txid1, commitCsn + 1, writeSet1);
        });
        
        // Verify everything is cleaned up
        assertEquals(0, detector.trackedReaderRowCount());
        assertEquals(0, detector.activeSerializableCount());
    }

    @Test
    public void testClearEmptiesRowReaders() {
        Database db = new Database();
        ConflictDetector detector = db.getConflictDetector();
        
        long txid1 = 1;
        long snapshotCsn1 = 100;
        
        // Start transaction and read a row
        detector.beginTracking(txid1, snapshotCsn1);
        detector.noteRead(txid1, "test_table", 1);
        
        // Verify rowReaders has the entry
        assertEquals(1, detector.trackedReaderRowCount());
        
        // Clear everything
        detector.clear();
        
        // Verify rowReaders is empty
        assertEquals(0, detector.trackedReaderRowCount());
        assertEquals(0, detector.activeSerializableCount());
    }
}