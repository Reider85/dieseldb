package diesel;

import diesel.concurrency.ConflictDetector;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Benchmark test to verify that noteCommit performance is O(W) (flat) regardless of 
 * the number of active readers, eliminating the O(T×W×R) Cartesian complexity.
 * @Tag("concurrency")
 */
@Tag("concurrency")
public class ConflictDetectorCartesianBenchmarkTest {

    private static final int R = 500; // Number of rows each transaction reads
    private static final int W = 10;   // Number of rows the committing transaction writes
    private static final int MAX_T = 1000; // Maximum number of active transactions to test

    @Test
    public void testNoteCommitStaysFlatAsReadersGrow() {
        Database db = new Database();
        ConflictDetector detector = db.getConflictDetector();
        
        long snapshotCsn = 100;
        long commitCsn = 200;
        
        // Test with T=10, T=100, T=500, T=1000 active readers
        int[] tValues = {10, 100, 500, 1000};
        long[] times = new long[tValues.length];
        
        // Create writeSet for the committing transaction (W=10 writes)
        // Use rows 1001-1010 to avoid conflicts with active readers (who read 1-500)
        java.util.Map<String, java.util.Set<Integer>> writeSet = new java.util.HashMap<>();
        for (int i = 1001; i <= 1010; i++) {
            writeSet.put("test_table", java.util.Set.of(i));
        }
        
        // Measure for each T value
        for (int i = 0; i < tValues.length; i++) {
            int t = tValues[i];
            
            // Setup: create T active readers
            createTransactions(detector, t);
            
            // Measure commit time (best of 3 to reduce noise)
            long bestTime = Long.MAX_VALUE;
            for (int attempt = 0; attempt < 3; attempt++) {
                long time = measureCommitTime(detector, writeSet);
                bestTime = Math.min(bestTime, time);
            }
            times[i] = bestTime;
            
            // Cleanup for next iteration
            detector.clear();
            
            System.out.printf("T=%d, time=%dms%n", t, bestTime);
        }
        
        // Assert correctness: non-conflicting commit should succeed
        // (none of the active readers read the written rows)
        createTransactions(detector, 10);
        assertDoesNotThrow(() -> {
            measureCommitTime(detector, writeSet);
        });
        detector.clear();
        
// Assert correctness: conflicting commit should fail
        // Create one transaction that reads a conflicting row
        detector.beginTracking(999, snapshotCsn);
        detector.noteRead(999, "test_table", 100); // Read row 100 (will be written)
        
        java.util.Map<String, java.util.Set<Integer>> conflictingWriteSet = new java.util.HashMap<>();
        conflictingWriteSet.put("test_table", java.util.Set.of(100)); // Write row 100
        
        // Create a separate committing transaction that will conflict with txid 999
        long committingTxid = 2000;
        detector.beginTracking(committingTxid, snapshotCsn);
        
        // The committing transaction must also read the row to create the conflict
        // (This simulates a real scenario where the committer read the row earlier)
        detector.noteRead(committingTxid, "test_table", 100);
        
        assertThrows(diesel.SerializationFailureException.class, () -> {
            detector.noteCommit(committingTxid, commitCsn, conflictingWriteSet);
        });
        detector.clear();
        
        // Performance assertions
        // The key insight: old implementation was O(T×W×R), new implementation is O(W)
        // So time(T=1000) should be roughly similar to time(T=10), not 100x slower
        
        long timeT10 = times[0];    // T=10 baseline
        long timeT100 = times[1];   // T=100 
        long timeT500 = times[2];   // T=500
        long timeT1000 = times[3];  // T=1000
        
        // Assert: T=1000 is not much worse than T=10 (allowing some overhead for more map operations)
        // Using generous bounds: T=1000 should be < 20x T=10 + 50ms slack
        // This would fail spectacularly with old O(T×W×R) implementation
        long maxAcceptableTime = Math.max(20 * timeT10, 50) + 50; // Generous bounds
        assertTrue(timeT1000 < maxAcceptableTime, 
            String.format("T=1000 time %dms exceeds acceptable bound %dms (T=10 was %dms)", 
                timeT1000, maxAcceptableTime, timeT10));
        
        // Also assert that performance doesn't degrade significantly as T grows
        // T=1000 should be roughly similar to T=100 (not 10x worse)
        assertTrue(timeT1000 < 5 * timeT100 + 20,
            String.format("T=1000 time %dms is much worse than T=100 time %dms", 
                timeT1000, timeT100));
        
        System.out.printf("Performance: T=10=%dms, T=100=%dms, T=500=%dms, T=1000=%dms%n", 
            timeT10, timeT100, timeT500, timeT1000);
    }
    
    // Helper to create T transactions, each reading R rows
    private void createTransactions(ConflictDetector detector, int t) {
        long snapshotCsn = 100;
        for (long txid = 1; txid <= t; txid++) {
            detector.beginTracking(txid, snapshotCsn);
            for (int row = 1; row <= R; row++) {
                detector.noteRead(txid, "test_table", row);
            }
        }
    }
    
    // Helper to measure noteCommit time for a given writeSet
    private long measureCommitTime(ConflictDetector detector, java.util.Map<String, java.util.Set<Integer>> writeSet) {
        long startTime = System.nanoTime();
        
        // Create a new transaction that will commit
        long committingTxid = MAX_T + 1;
        detector.beginTracking(committingTxid, 100);
        
        // Add some reads to the committing transaction (doesn't affect the benchmark)
        // Use rows 1001-1010 to avoid conflicts with active readers (who read 1-500)
        for (int row = 1001; row <= 1010; row++) {
            detector.noteRead(committingTxid, "test_table", row);
        }
        
        // Perform the commit operation being benchmarked
        detector.noteCommit(committingTxid, 200, writeSet);
        
        long endTime = System.nanoTime();
        return (endTime - startTime) / 1_000_000; // Convert to milliseconds
    }
}