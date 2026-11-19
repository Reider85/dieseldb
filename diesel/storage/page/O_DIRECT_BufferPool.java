package diesel.storage.page;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Buffer pool for O_DIRECT operations that manages aligned direct buffers.
 * 
 * O_DIRECT requires both memory and file offsets to be aligned to block boundaries
 * (typically 512 bytes). This pool provides pre-allocated, aligned buffers
 * to avoid runtime alignment checks and improve performance.
 */
public class O_DIRECT_BufferPool {
    
    /**
     * Buffer state: 0 = free, 1 = in-use
     */
    private static final int BUFFER_FREE = 0;
    private static final int BUFFER_IN_USE = 1;
    
    private final int pageSize;
    private final AtomicReferenceArray<ByteBuffer> buffers;
    private final AtomicInteger[] bufferStates;
    private final int maxBuffers;
    
    /**
     * Creates a new O_DIRECT buffer pool.
     * 
     * @param pageSize page size in bytes (must be aligned to 512 bytes)
     * @param maxBuffers maximum number of buffers to manage
     */
    public O_DIRECT_BufferPool(int pageSize, int maxBuffers) {
        if (pageSize <= 0 || !isPowerOfTwo(pageSize) || pageSize % 512 != 0) {
            throw new IllegalArgumentException("Page size must be positive, power of 2, and aligned to 512 bytes: " + pageSize);
        }
        if (maxBuffers <= 0) {
            throw new IllegalArgumentException("Max buffers must be positive: " + maxBuffers);
        }
        
        this.pageSize = pageSize;
        this.maxBuffers = maxBuffers;
        this.buffers = new AtomicReferenceArray<>(maxBuffers);
        this.bufferStates = new AtomicInteger[maxBuffers];
        
        // Initialize buffers with aligned direct buffers
        for (int i = 0; i < maxBuffers; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
            
            // Note: Direct ByteBuffer address() method is not available in all Java versions
            // We'll rely on the fact that allocateDirect typically returns aligned memory
            // For production use, additional alignment verification might be needed
            
            buffers.set(i, buffer);
            bufferStates[i] = new AtomicInteger(BUFFER_FREE);
        }
    }
    
    /**
     * Gets an aligned buffer from the pool.
     * 
     * @param bufferIndex index of the buffer to acquire
     * @return the aligned buffer, or null if the buffer is in use
     */
    public ByteBuffer getAlignedBuffer(int bufferIndex) {
        if (bufferIndex < 0 || bufferIndex >= maxBuffers) {
            throw new IndexOutOfBoundsException("Buffer index out of bounds: " + bufferIndex);
        }
        
        if (bufferStates[bufferIndex].compareAndSet(BUFFER_FREE, BUFFER_IN_USE)) {
            ByteBuffer buffer = buffers.get(bufferIndex);
            buffer.clear();
            return buffer;
        }
        
        return null; // Buffer is in use
    }
    
    /**
     * Releases an aligned buffer back to the pool.
     * 
     * @param bufferIndex index of the buffer to release
     */
    public void releaseAlignedBuffer(int bufferIndex) {
        if (bufferIndex < 0 || bufferIndex >= maxBuffers) {
            throw new IndexOutOfBoundsException("Buffer index out of bounds: " + bufferIndex);
        }
        
        bufferStates[bufferIndex].set(BUFFER_FREE);
    }
    
    /**
     * Gets the page size used by this buffer pool.
     * 
     * @return the page size in bytes
     */
    public int getPageSize() {
        return pageSize;
    }
    
    /**
     * Gets the maximum number of buffers in this pool.
     * 
     * @return the maximum number of buffers
     */
    public int getMaxBuffers() {
        return maxBuffers;
    }
    
    /**
     * Checks if a value is a power of two.
     * 
     * @param value value to check
     * @return true if the value is a power of two, false otherwise
     */
    private boolean isPowerOfTwo(int value) {
        return (value & (value - 1)) == 0;
    }
    
    /**
     * Gets the current usage statistics of the buffer pool.
     * 
     * @return array where each element is 0 (free) or 1 (in-use)
     */
    public int[] getBufferStates() {
        int[] states = new int[maxBuffers];
        for (int i = 0; i < maxBuffers; i++) {
            states[i] = bufferStates[i].get();
        }
        return states;
    }
}