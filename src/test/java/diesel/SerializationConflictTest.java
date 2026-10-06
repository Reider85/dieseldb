package diesel;

import diesel.concurrency.ConflictDetector;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for MVCC SERIALIZABLE SSI conflict detection (prompt4.md #5).
 * Tests SerializationFailureException and ConflictDetector functionality.
 */
public class SerializationConflictTest {

    @Test
    public void testSerializationFailureExceptionCreation() {
        SerializationFailureException ex = new SerializationFailureException("Test message");
        assertEquals("Test message", ex.getMessage());
        assertTrue(ex instanceof TransactionException);
    }

    @Test
    public void testConflictDetectorBasicTracking() {
        Database db = new Database();
        ConflictDetector detector = db.getConflictDetector();
        
        // Test beginTracking
        long txid = 1;
        long snapshotCsn = 100;
        detector.beginTracking(txid, snapshotCsn);
        
        // Test noteRead
        detector.noteRead(txid, "test_table", 1);
        
        // Test noteWrite
        detector.noteWrite(txid, "test_table", 1);
        
        // Test active count
        assertEquals(1, detector.activeSerializableCount());
        
        // Test rollback cleanup
        detector.noteRollback(txid);
        assertEquals(0, detector.activeSerializableCount());
    }

    @Test
    public void testConflictDetectorWriteConflict() {
        Database db = new Database();
        ConflictDetector detector = db.getConflictDetector();
        
        long txid1 = 1;
        long txid2 = 2;
        long snapshotCsn1 = 100;
        long snapshotCsn2 = 200;
        
        // Start two transactions
        detector.beginTracking(txid1, snapshotCsn1);
        detector.beginTracking(txid2, snapshotCsn2);
        
        // Transaction 2 writes to a row
        detector.noteWrite(txid2, "test_table", 1);
        
        // Transaction 1 tries to write to the same row - should detect conflict
        assertThrows(SerializationFailureException.class, () -> {
            detector.checkSerializableWriteConflict("test_table", 1, txid1, snapshotCsn1, txid2, 0);
        });
        
        // Cleanup
        detector.noteRollback(txid1);
        detector.noteRollback(txid2);
    }

    @Test
    public void testConflictDetectorStaleSnapshot() {
        Database db = new Database();
        ConflictDetector detector = db.getConflictDetector();
        
        long txid1 = 1;
        long txid2 = 2;
        long snapshotCsn1 = 100;
        long snapshotCsn2 = 200;
        
        // Start two transactions
        detector.beginTracking(txid1, snapshotCsn1);
        detector.beginTracking(txid2, snapshotCsn2);
        
        // Row was committed after txid1's snapshot
        assertThrows(SerializationFailureException.class, () -> {
            detector.checkSerializableWriteConflict("test_table", 1, txid1, snapshotCsn1, 0, 150);
        });
        
        // Cleanup
        detector.noteRollback(txid1);
        detector.noteRollback(txid2);
    }

    @Test
    public void testConflictDetectorRwConflictAtCommit() {
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
        
        // Transaction 1 reads a row
        detector.noteRead(txid1, "test_table", 1);
        
        // Transaction 2 writes to that row and commits
        java.util.Map<String, java.util.Set<Integer>> writeSet = new java.util.HashMap<>();
        writeSet.put("test_table", java.util.Set.of(1));
        
        // This should succeed - no conflict yet
        detector.noteCommit(txid2, commitCsn, writeSet);
        
        // Now transaction 1 tries to commit - should detect rw-conflict
        java.util.Map<String, java.util.Set<Integer>> writeSet1 = new java.util.HashMap<>();
        assertThrows(SerializationFailureException.class, () -> {
            detector.noteCommit(txid1, commitCsn + 1, writeSet1);
        });
        
        // Cleanup
        detector.noteRollback(txid1);
    }

    @Test
    public void testTupleVisibilityHasSerializableWriteConflict() {
        // Test the helper method in TupleVisibility
        assertTrue(TupleVisibility.hasSerializableWriteConflict(2, 0, 1, 100)); // pending foreign change
        assertTrue(TupleVisibility.hasSerializableWriteConflict(0, 150, 1, 100)); // stale snapshot
        assertFalse(TupleVisibility.hasSerializableWriteConflict(1, 0, 1, 100)); // self write
        assertFalse(TupleVisibility.hasSerializableWriteConflict(0, 50, 1, 100)); // no conflict
    }
}