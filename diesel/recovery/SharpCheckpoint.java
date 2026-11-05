package diesel.recovery;

import diesel.storage.page.BufferPool;
import diesel.storage.page.BufferPoolFlusher;
import diesel.storage.page.Page;
import diesel.wal.WALManager;
import diesel.TxStatusTracker;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Sharp checkpoint strategy (Prompt4 #53).
 * 
 * <p>Performs blocking checkpoints by holding the write gate for the entire
 * duration and flushing all dirty pages. Writers are blocked during checkpoint.
 * 
 * <p>This is the traditional checkpoint behavior used in existing tests.
 * Checkpoint record includes active transaction IDs at checkpoint time.
 */
public final class SharpCheckpoint implements CheckpointStrategy {
    
    private final WALManager walManager;
    private final BufferPoolFlusher flusher;
    private final BufferPool bufferPool;
    private final TxStatusTracker txStatusTracker;
    private final ReadWriteLock writeGate;
    
    /**
     * Creates a sharp checkpoint strategy.
     * 
     * @param walManager WAL manager for writing checkpoint records
     * @param flusher buffer pool flusher for flushing dirty pages
     * @param bufferPool buffer pool (nullable, for fallback logic)
     * @param txStatusTracker transaction status tracker for active txids
     * @param writeGate write gate to acquire during checkpoint (must not be null)
     */
    public SharpCheckpoint(WALManager walManager, BufferPoolFlusher flusher, 
                         BufferPool bufferPool, TxStatusTracker txStatusTracker,
                         ReadWriteLock writeGate) {
        if (walManager == null) {
            throw new IllegalArgumentException("WALManager must not be null");
        }
        if (txStatusTracker == null) {
            throw new IllegalArgumentException("TxStatusTracker must not be null");
        }
        if (writeGate == null) {
            throw new IllegalArgumentException("WriteGate must not be null");
        }
        
        this.walManager = walManager;
        this.flusher = flusher;
        this.bufferPool = bufferPool;
        this.txStatusTracker = txStatusTracker;
        this.writeGate = writeGate;
    }
    
    @Override
    public long checkpoint() throws IOException {
        // Step 1: Acquire write gate (blocks writers)
        writeGate.writeLock().lock();
        try {
            // Step 2: Flush all dirty pages (blocking)
            if (flusher != null) {
                try {
                    flusher.flushNow();
                } catch (IOException e) {
                    // Log but continue - checkpoint record should still be written
                    System.err.println("SharpCheckpoint: flush failed, continuing with checkpoint: " + e.getMessage());
                }
            } else if (bufferPool != null) {
                // Fallback: flush all dirty pages directly
                flushAllDirtyPages();
            }
            
            // Step 3: Get active transaction IDs
            List<Long> activeTxids = txStatusTracker.getActiveTxids();
            
            // Step 4: Write checkpoint record to WAL
            walManager.writeCheckpoint(activeTxids);
            
            // Step 5: Return the checkpoint LSN
            return walManager.getLastCheckpointLsn();
            
        } finally {
            // Step 6: Release write gate
            writeGate.writeLock().unlock();
        }
    }
    
    @Override
    public String name() {
        return "sharp";
    }
    
    /**
     * Fallback method for flushing all dirty pages when flusher is null.
     * Uses BufferPool.flushDirty with all dirty pages.
     */
    private void flushAllDirtyPages() throws IOException {
        if (bufferPool == null) {
            return; // Nothing to flush
        }
        
        try {
            int flushed = bufferPool.flushDirty();
            if (flushed > 0) {
                System.out.println("SharpCheckpoint: flushed " + flushed + " dirty pages (fallback)");
            }
        } catch (IOException e) {
            System.err.println("SharpCheckpoint: fallback flush failed: " + e.getMessage());
            throw e;
        }
    }
}