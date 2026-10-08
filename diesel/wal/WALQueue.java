package diesel.wal;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded queue for WAL write requests with backpressure tracking (prompt4.md step 13, R3-003 step 3/5).
 *
 * <p>Package-private: used by WALWriter.
 */
public final class WALQueue {

    private final BlockingQueue<WALWriteRequest> queue;
    private final AtomicLong blockedCount = new AtomicLong(0);
    private final AtomicLong maxSizeSeen = new AtomicLong(0);

    /**
     * Creates a WAL queue with the specified capacity.
     *
     * @param capacity the maximum queue size
     */
    public WALQueue(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Puts a write request into the queue, blocking if the queue is full.
     * Increments blockedCount when the queue is full before waiting.
     *
     * @param request the write request to put
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void put(WALWriteRequest request) throws InterruptedException {
        if (queue.size() == queue.remainingCapacity()) {
            blockedCount.incrementAndGet();
        }
        queue.put(request);
        updateMaxSizeSeen();
    }

    /**
     * Takes a write request from the queue, blocking if the queue is empty.
     *
     * @return the next write request
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public WALWriteRequest take() throws InterruptedException {
        return queue.take();
    }

    /**
     * Offers a write request without blocking. Returns {@code false} when the
     * queue is full (callers then rely on the writer draining by itself).
     *
     * @param request the request to offer
     * @return true if the request was enqueued
     */
    public boolean offer(WALWriteRequest request) {
        if (!queue.offer(request)) {
            return false;
        }
        updateMaxSizeSeen();
        return true;
    }

    /**
     * Drains up to {@code max} additional write requests from the queue into
     * {@code target} without blocking. One lock acquisition per batch instead
     * of one per request (prompt4.md step 13 batching).
     *
     * @param target the collection to drain requests into
     * @param max the maximum number of requests to drain
     * @return the number of requests drained
     */
    public int drainTo(List<WALWriteRequest> target, int max) {
        return queue.drainTo(target, max);
    }

    /**
     * Returns the current size of the queue.
     *
     * @return the number of elements in the queue
     */
    public int size() {
        return queue.size();
    }

    /**
     * Returns the capacity of the queue.
     *
     * @return the maximum number of elements the queue can hold
     */
    public int capacity() {
        return queue.size() + queue.remainingCapacity();
    }

    /**
     * Returns the number of times the queue was full and put() had to block.
     *
     * @return the count of blocking events
     */
    public long getBlockedCount() {
        return blockedCount.get();
    }

    /**
     * Returns the maximum size seen since the queue was created or last reset.
     *
     * @return the maximum observed queue size
     */
    public long getMaxSizeSeen() {
        return maxSizeSeen.get();
    }

    /**
     * Returns whether the queue is empty.
     *
     * @return true if the queue is empty, false otherwise
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * Returns whether the queue is full.
     *
     * @return true if the queue is full, false otherwise
     */
    public boolean isFull() {
        return queue.remainingCapacity() == 0;
    }

    /**
     * Updates the maximum size seen if the current size is greater.
     */
    private void updateMaxSizeSeen() {
        int currentSize = queue.size();
        long currentMax = maxSizeSeen.get();
        while (currentSize > currentMax) {
            if (maxSizeSeen.compareAndSet(currentMax, currentSize)) {
                break;
            }
            currentMax = maxSizeSeen.get();
        }
    }
}