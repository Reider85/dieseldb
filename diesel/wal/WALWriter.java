package diesel.wal;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-writer thread for WAL entries with bounded queue and backpressure (prompt4.md step 13, R3-003 step 3/5).
 *
 * <p><b>Design.</b> Producers enqueue {@code WALWriteRequest} objects containing
 * transaction data and a {@code CompletableFuture<WALEntry>} for the result.
 * The writer thread dequeues requests, allocates LSNs at dequeue time (ensuring
 * strictly monotonic LSNs), appends via {@code WALManager}, and completes the future.
 * A FLUSH barrier enqueues a special request that forces {@code WALManager.flush()}.
 *
 * <p><b>Thread safety.</b> The writer thread is the only caller of {@code WALManager.append},
 * {@code WALManager.allocateLsn}, and {@code WALManager.flush}. Producers never touch
 * WALManager file APIs. The queue is thread-safe; the writer loop is single-threaded.
 *
 * <p><b>Metrics.</b> Exposed as a {@code DynamicMBean} ({@code diesel:type=WALWriter,id=N}):
 * queue size, append latency p99, append count/errors, queue blocked count, max queue size seen.
 */
public final class WALWriter implements AutoCloseable, DynamicMBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(WALWriter.class);

    /** JMX object name prefix. */
    static final String OBJECT_NAME_PREFIX = "diesel:type=WALWriter";
    /** JMX attribute names. */
    static final String ATTR_QUEUE_SIZE = "wal.queue.size";
    static final String ATTR_APPEND_LATENCY_P99 = "wal.append.latency.p99";
    static final String ATTR_APPEND_COUNT = "wal.append.count";
    static final String ATTR_APPEND_ERRORS = "wal.append.errors";
    static final String ATTR_QUEUE_BLOCKED = "wal.queue.blocked";
    static final String ATTR_QUEUE_MAX_SIZE = "wal.queue.max.size";

    /** MBean sequence counter for multiple instances. */
    private static final AtomicInteger MBEAN_SEQUENCE = new AtomicInteger();
    /** Ring buffer size for p99 latency calculation (1024 samples ≈ 1MB at 8 bytes each). */
    private static final int LATENCY_RING_SIZE = 1024;
    /** Max requests coalesced into one segment write (bounded to keep batch latency low). */
    private static final int MAX_BATCH_ENTRIES = 256;
    /** Max encoded bytes coalesced into one segment write (bounds the batch buffer). */
    private static final long MAX_BATCH_BYTES = 4L * 1024 * 1024;

    private final WALManager manager;
    private final WALConfig config;
    private final WALQueue queue;
    private final Thread writerThread;
    private final AtomicLong appendCount = new AtomicLong();
    private final AtomicLong appendErrors = new AtomicLong();
    private final AtomicLong queueBlockedCount = new AtomicLong();
    private final AtomicReference<ObjectName> registeredName = new AtomicReference<>();

    /** Ring buffer for append latencies (nanoseconds, single writer ⇒ no contention). */
    private final long[] latencyRing = new long[LATENCY_RING_SIZE];
    private int latencyRingIndex = 0;
    private volatile boolean running = true;

    /**
     * Creates a WAL writer that uses an existing WAL manager.
     *
     * @param manager the WAL manager to use for appending
     * @param config the WAL configuration
     */
    public WALWriter(WALManager manager, WALConfig config) {
        this.manager = manager;
        this.config = config;
        this.queue = new WALQueue((int) Math.min(config.getQueueMaxSize(), Integer.MAX_VALUE));
        this.writerThread = new Thread(this::writerLoop, "diesel-wal-writer");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
        registerMBean();
    }

    /**
     * Creates a WAL writer that owns its WAL manager (convenience for tests).
     *
     * @param config the WAL configuration
     * @return a new WAL writer with an owned manager
     * @throws IOException if the WAL manager cannot be created
     */
    public static WALWriter open(WALConfig config) throws IOException {
        WALManager manager = new WALManager(config);
        return new WALWriter(manager, config);
    }

    /**
     * Returns the WAL manager used by this writer.
     *
     * @return the WAL manager
     */
    public WALManager getManager() {
        return manager;
    }

    /**
     * Appends a WAL entry asynchronously.
     *
     * @param txid the transaction id
     * @param op the operation code
     * @param before the before image (may be null)
     * @param after the after image (may be null)
     * @return a future that completes with the WAL entry when written
     */
    public CompletableFuture<WALEntry> appendAsync(long txid, WALOpcode op, byte[] before, byte[] after) {
        if (!running) {
            CompletableFuture<WALEntry> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("WALWriter is closed"));
            return future;
        }

        CompletableFuture<WALEntry> future = new CompletableFuture<>();
        WALWriteRequest request = new WALWriteRequest(txid, op, before, after, future, System.nanoTime());
        
        try {
            queue.put(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(new CompletionException(e));
            return future;
        }
        
        return future;
    }

    /**
     * Appends a WAL entry synchronously (blocking convenience).
     *
     * @param txid the transaction id
     * @param op the operation code
     * @param before the before image (may be null)
     * @param after the after image (may be null)
     * @return the WAL entry after it is written
     * @throws IOException if the append fails
     */
    public WALEntry append(long txid, WALOpcode op, byte[] before, byte[] after) throws IOException {
        try {
            return appendAsync(txid, op, before, after).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException("Unexpected append failure", cause);
            }
        }
    }

    /**
     * Flushes all pending WAL entries to durable storage.
     *
     * @throws IOException if the flush fails
     */
    public void flush() throws IOException {
        if (!running) {
            throw new IllegalStateException("WALWriter is closed");
        }

        CompletableFuture<Void> flushFuture = new CompletableFuture<>();
        WALWriteRequest flushRequest = WALWriteRequest.createFlushBarrier(flushFuture);
        
        try {
            queue.put(flushRequest);
            flushFuture.get(); // Wait for flush completion
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Flush interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new IOException("Flush failed", cause);
            }
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new IOException("Flush failed", cause);
            }
        }
    }

    /**
     * The writer loop: drains queued requests in batches and writes each batch
     * to the segment with a single channel write.
     *
     * <p>A flush barrier splits the batch: everything queued before it is
     * written first, then {@code WALManager.flush()} runs, then the barrier
     * future completes — preserving the "flush waits for prior appends"
     * contract regardless of batching.
     */
    private void writerLoop() {
        List<WALWriteRequest> drained = new ArrayList<>(MAX_BATCH_ENTRIES);
        List<WALWriteRequest> pendingRequests = new ArrayList<>(MAX_BATCH_ENTRIES);
        List<WALEntry> pendingEntries = new ArrayList<>(MAX_BATCH_ENTRIES);
        try {
            while (running || !queue.isEmpty()) {
                drained.clear();
                drained.add(queue.take());
                queue.drainTo(drained, MAX_BATCH_ENTRIES - 1);

                long pendingBytes = 0;
                for (WALWriteRequest request : drained) {
                    if (request.isFlushBarrier()) {
                        writePending(pendingRequests, pendingEntries);
                        pendingRequests.clear();
                        pendingEntries.clear();
                        pendingBytes = 0;
                        try {
                            manager.flush();
                            request.future().complete(null);
                        } catch (Throwable t) {
                            request.future().completeExceptionally(t);
                            LOGGER.error("Flush barrier failed", t);
                        }
                        continue;
                    }

                    try {
                        long lsn = manager.allocateLsn();
                        WALEntry entry = new WALEntry(lsn, request.txid(), request.op(),
                                request.before(), request.after());
                        pendingEntries.add(entry);
                        pendingRequests.add(request);
                        pendingBytes += entry.encodedSize();
                        if (pendingEntries.size() >= MAX_BATCH_ENTRIES || pendingBytes >= MAX_BATCH_BYTES) {
                            writePending(pendingRequests, pendingEntries);
                            pendingRequests.clear();
                            pendingEntries.clear();
                            pendingBytes = 0;
                        }
                    } catch (Throwable t) {
                        appendErrors.incrementAndGet();
                        request.future().completeExceptionally(t);
                        LOGGER.error("Failed to append WAL entry for txid={}, op={}",
                                request.txid(), request.op(), t);
                    }
                }
                writePending(pendingRequests, pendingEntries);
                pendingRequests.clear();
                pendingEntries.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("WAL writer thread interrupted");
        } catch (Throwable t) {
            LOGGER.error("Uncaught error in WAL writer thread", t);
        } finally {
            unregisterMBean();
        }
    }

    /**
     * Writes all pending entries as one batched segment write and completes
     * their futures. Every future is always completed — either normally after
     * the write, or exceptionally if the batch write fails.
     *
     * @param requests the requests matching {@code entries}, position by position
     * @param entries the entries to write; no-op when empty
     */
    private void writePending(List<WALWriteRequest> requests, List<WALEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        try {
            manager.appendBatch(entries);
            long completedAt = System.nanoTime();
            for (int i = 0; i < entries.size(); i++) {
                recordLatency(completedAt - requests.get(i).enqueueNanos());
                appendCount.incrementAndGet();
                requests.get(i).future().complete(entries.get(i));
            }
        } catch (Throwable t) {
            appendErrors.addAndGet(entries.size());
            for (WALWriteRequest request : requests) {
                request.future().completeExceptionally(t);
            }
            LOGGER.error("Failed to append batch of {} WAL entries", entries.size(), t);
        }
    }

    /**
     * Records an append latency in the ring buffer for p99 calculation.
     */
    private void recordLatency(long latencyNanos) {
        latencyRing[latencyRingIndex] = latencyNanos;
        latencyRingIndex = (latencyRingIndex + 1) % LATENCY_RING_SIZE;
    }

    /**
     * Returns the p99 append latency in microseconds.
     *
     * @return p99 latency in microseconds as long, or 0 if no samples yet
     */
    public long getAppendLatencyP99() {
        if (appendCount.get() == 0) {
            return 0L;
        }

        // Copy the ring buffer to avoid concurrent modification
        long[] samples = new long[LATENCY_RING_SIZE];
        int count = (int) Math.min(appendCount.get(), LATENCY_RING_SIZE);
        synchronized (latencyRing) {
            System.arraycopy(latencyRing, 0, samples, 0, LATENCY_RING_SIZE);
        }

        if (count == 0) {
            return 0L;
        }

        // Sort and find the p99 index
        java.util.Arrays.sort(samples, 0, count);
        int p99Index = (int) Math.ceil(0.99 * count) - 1;
        return samples[p99Index] / 1000L; // Convert to microseconds
    }

    /**
     * Returns the current queue size.
     */
    public int getQueueSize() {
        return queue.size();
    }

    /**
     * Returns the number of times the queue was full and put() had to block.
     */
    public long getQueueBlockedCount() {
        return queue.getBlockedCount();
    }

    /**
     * Returns the maximum queue size seen.
     */
    public long getMaxQueueSizeSeen() {
        return queue.getMaxSizeSeen();
    }

    /**
     * Returns the number of successful appends.
     */
    public long getAppendCount() {
        return appendCount.get();
    }

    /**
     * Returns the number of failed appends.
     */
    public long getAppendErrors() {
        return appendErrors.get();
    }

    /**
     * Returns the configured maximum queue size.
     */
    public long getQueueMaxSize() {
        return config.getQueueMaxSize();
    }

    /**
     * Closes the writer and shuts down the writer thread.
     */
    @Override
    public void close() throws IOException {
        if (!running) {
            return; // Already closed
        }

        running = false;

        // Wake the writer thread if it is parked in take() on an empty queue:
        // a flush-barrier pill makes it run one final flush and then exit the
        // loop (running == false and queue empty). If the queue is full the
        // writer is draining anyway and will exit on its own.
        queue.offer(WALWriteRequest.createFlushBarrier(new CompletableFuture<Void>()));

        // Wait for the writer thread to finish with a timeout
        try {
            writerThread.join(30_000); // 30 second timeout
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for writer thread to shut down");
        }
        
        // If still alive after timeout, force interrupt and wait a bit more
        if (writerThread.isAlive()) {
            LOGGER.warn("WAL writer thread did not shut down gracefully, interrupting");
            writerThread.interrupt();
            try {
                writerThread.join(5_000); // Additional 5 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted during final writer thread shutdown");
            }
        }

        if (manager != null) {
            manager.close();
        }
    }

    /**
     * Returns the JMX object name this writer is registered under, or null.
     */
    public ObjectName getObjectName() {
        return registeredName.get();
    }

    // ─── DynamicMBean implementation ─────────────────────────────────────

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
        switch (attribute) {
            case ATTR_QUEUE_SIZE:
                return getQueueSize();
            case ATTR_APPEND_LATENCY_P99:
                return getAppendLatencyP99();
            case ATTR_APPEND_COUNT:
                return getAppendCount();
            case ATTR_APPEND_ERRORS:
                return getAppendErrors();
            case ATTR_QUEUE_BLOCKED:
                return getQueueBlockedCount();
            case ATTR_QUEUE_MAX_SIZE:
                return getQueueMaxSize();
            default:
                throw new AttributeNotFoundException("Unknown attribute: " + attribute);
        }
    }

    @Override
    @SuppressWarnings("unused")
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("WALWriter attributes are read-only");
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList result = new AttributeList();
        for (String attribute : attributes) {
            try {
                result.add(new Attribute(attribute, getAttribute(attribute)));
            } catch (Exception ignored) {
                // Skip attributes that cannot be read.
            }
        }
        return result;
    }

    @Override
    @SuppressWarnings("unused")
    public AttributeList setAttributes(AttributeList attributes) {
        return new AttributeList();
    }

    @Override
    @SuppressWarnings("unused")
    public Object invoke(String actionName, Object[] params, String[] signature)
            throws MBeanException, ReflectionException {
        throw new ReflectionException(new UnsupportedOperationException(
                "No operations on WALWriter; attributes are read-only"));
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        String[] names = {
            ATTR_QUEUE_SIZE, ATTR_APPEND_LATENCY_P99, ATTR_APPEND_COUNT,
            ATTR_APPEND_ERRORS, ATTR_QUEUE_BLOCKED, ATTR_QUEUE_MAX_SIZE
        };
        String[] descriptions = {
            "Current number of requests in the WAL queue",
            "99th percentile append latency in microseconds",
            "Total number of successfully appended WAL entries",
            "Total number of failed append operations",
            "Number of times the queue was full and put() had to block",
            "Maximum configured queue size"
        };
        MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[names.length];
        for (int i = 0; i < names.length; i++) {
            attributes[i] = new MBeanAttributeInfo(names[i], "long", descriptions[i],
                    true, false, false);
        }
        return new MBeanInfo(WALWriter.class.getName(),
                "DieselDB WAL writer (single-threaded queue consumer)",
                attributes, null, null, null);
    }

    // ─── JMX registration ───────────────────────────────────────────────

    /**
     * Registers the JMX MBean.
     */
    private void registerMBean() {
        if (registeredName.get() != null) {
            return;
        }
        
        synchronized (this) {
            if (registeredName.get() != null) {
                return;
            }
            
            try {
                ObjectName name = new ObjectName(
                    OBJECT_NAME_PREFIX + ",id=" + MBEAN_SEQUENCE.incrementAndGet());
                ManagementFactory.getPlatformMBeanServer().registerMBean(this, name);
                registeredName.set(name);
            } catch (Exception e) {
                LOGGER.warn("Failed to register WALWriter MBean: {}", e.toString());
            }
        }
    }

    /**
     * Unregisters the JMX MBean.
     */
    private void unregisterMBean() {
        ObjectName name = registeredName.getAndSet(null);
        if (name != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
            } catch (Exception ignored) {
                // Already gone; nothing to clean up.
            }
        }
    }
}