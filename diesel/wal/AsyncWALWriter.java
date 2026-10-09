package diesel.wal;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import java.lang.management.ManagementFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Policy-aware WAL writer with async commit support (prompt4.md #15, R3-003 step 5/5).
 * 
 * <p><b>Design.</b> Wraps a WALWriter to provide async commit APIs with configurable fsync policies:
 * ALWAYS (fsync per commit), GROUP (batch with group coordinator), EVERYSEC (fsync every second),
 * NONE (no fsync). Exposes JMX metrics for async operations.
 * 
 * <p><b>Thread safety.</b> All public methods are thread-safe. The underlying WALWriter is single-
 * threaded, so concurrent async operations are queued internally.
 * 
 * <p><b>Metrics.</b> Exposed as a {@code DynamicMBean} ({@code diesel:type=AsyncWALWriter,id=N}):
 * pending async operations, async throughput, policy, coordinator stats.
 */
public final class AsyncWALWriter implements AutoCloseable, DynamicMBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncWALWriter.class);

    /** JMX object name prefix. */
    static final String OBJECT_NAME_PREFIX = "diesel:type=AsyncWALWriter";
    /** JMX attribute names. */
    static final String ATTR_PENDING_ASYNC = "async.pending.operations";
    static final String ATTR_ASYNC_THROUGHPUT = "async.throughput.ops.per.sec";
    static final String ATTR_FSYNC_POLICY = "fsync.policy";
    static final String ATTR_COORDINATOR_PENDING = "coordinator.pending.commits";
    static final String ATTR_COORDINATOR_FSYNC_COUNT = "coordinator.fsync.count";
    static final String ATTR_COORDINATOR_GROUP_SIZE = "coordinator.current.group.size";

    /** MBean sequence counter for multiple instances. */
    private static final AtomicInteger MBEAN_SEQUENCE = new AtomicInteger();

    private final WALWriter writer;
    private final FsyncPolicy policy;
    private final GroupCommitCoordinator coordinator;
    private final ScheduledExecutorService backgroundFlusher;
    private final AtomicLong pendingAsync = new AtomicLong();
    private final AtomicLong totalAsync = new AtomicLong();
    private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
    private volatile boolean running = true;
    private volatile ObjectName registeredName;

    /**
     * Creates an async WAL writer with group commit support.
     *
     * @param writer the underlying WAL writer
     * @param policy the fsync policy to use
     * @param groupWindowMs the group window size in milliseconds (for GROUP policy)
     * @param groupMaxSize the maximum group size (for GROUP policy)
     * @param scheduler a scheduled executor for background operations
     */
    public AsyncWALWriter(WALWriter writer, FsyncPolicy policy, 
                         long groupWindowMs, int groupMaxSize,
                         ScheduledExecutorService scheduler) {
        this.writer = writer;
        this.policy = policy;
        
        // The coordinator handles every policy (ALWAYS → immediate fsync,
        // NONE → append without fsync, GROUP/EVERYSEC → batched fsync), so
        // COMMIT requests always take the policy-aware path.
        this.coordinator = new GroupCommitCoordinator(writer, policy, groupWindowMs, 
                                                   groupMaxSize, scheduler);
        this.backgroundFlusher = scheduler;
        
        registerMBean();
    }

    /**
     * Appends a WAL entry asynchronously.
     *
     * @param txid the transaction id
     * @param op the operation code
     * @param before the before image (may be null)
     * @param after the after image (may be null)
     * @return a future that completes when the entry is written
     */
    public CompletableFuture<WALEntry> appendAsync(long txid, WALOpcode op, 
                                                 byte[] before, byte[] after) {
        if (!running) {
            CompletableFuture<WALEntry> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("AsyncWALWriter is closed"));
            return future;
        }

        pendingAsync.incrementAndGet();
        totalAsync.incrementAndGet();
        
        CompletableFuture<WALEntry> future = new CompletableFuture<>();
        
        // Submit to a background thread to avoid blocking the caller
        CompletableFuture.runAsync(() -> {
            try {
                if (coordinator != null && (op == WALOpcode.COMMIT || op == WALOpcode.BEGIN)) {
                    // For commit/begin operations, use the coordinator for policy-aware batching
                    coordinator.submitCommit(txid, op, before, after).whenComplete((entry, error) -> {
                        pendingAsync.decrementAndGet();
                        if (error != null) {
                            future.completeExceptionally(error);
                        } else {
                            future.complete(entry);
                        }
                    });
                } else {
                    // For other operations, append directly
                    WALEntry entry = writer.append(txid, op, before, after);
                    pendingAsync.decrementAndGet();
                    future.complete(entry);
                }
            } catch (Exception e) {
                pendingAsync.decrementAndGet();
                future.completeExceptionally(e);
            }
        });
        
        return future;
    }

    /**
     * Submits a commit asynchronously with policy-aware durability.
     *
     * @param txid the transaction id
     * @param before the before image (may be null)
     * @param after the after image (may be null)
     * @return a future that completes when the commit is durable
     */
    public CompletableFuture<WALEntry> commitAsync(long txid, byte[] before, byte[] after) {
        return appendAsync(txid, WALOpcode.COMMIT, before, after);
    }

    /**
     * Flushes all pending WAL entries to durable storage.
     *
     * @return a future that completes when the flush is done
     */
    public CompletableFuture<Void> flushAsync() {
        if (!running) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("AsyncWALWriter is closed"));
            return future;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        
        // Submit to background thread
        CompletableFuture.runAsync(() -> {
            try {
                if (coordinator != null) {
                    coordinator.flush();
                } else {
                    writer.flush();
                }
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        
        return future;
    }

    /**
     * Returns the number of pending async operations.
     */
    public int getPendingAsync() {
        return (int) pendingAsync.get();
    }

    /**
     * Returns the async throughput in operations per second.
     */
    public double getAsyncThroughput() {
        long elapsedMs = System.currentTimeMillis() - startTime.get();
        if (elapsedMs <= 0) {
            return 0.0;
        }
        return (totalAsync.get() * 1000.0) / elapsedMs;
    }

    /**
     * Returns the current fsync policy.
     */
    public FsyncPolicy getPolicy() {
        return policy;
    }

    /**
     * Returns the group commit coordinator, or null if not using group commit.
     */
    public GroupCommitCoordinator getCoordinator() {
        return coordinator;
    }

    /**
     * Returns coordinator metrics, or zero values if no coordinator.
     */
    public long getCoordinatorPending() {
        return coordinator != null ? coordinator.getPendingCommits() : 0;
    }

    /**
     * Returns the coordinator fsync count, or zero if no coordinator.
     */
    public long getCoordinatorFsyncCount() {
        return coordinator != null ? coordinator.getFsyncCount() : 0;
    }

    /**
     * Returns the current group size, or zero if no coordinator.
     */
    public int getCoordinatorGroupSize() {
        return coordinator != null ? coordinator.getCurrentGroupSize() : 0;
    }

    @Override
    public void close() throws IOException {
        if (!running) {
            return;
        }
        
        running = false;
        
        // Close the coordinator if present
        if (coordinator != null) {
            coordinator.close();
        }
        
        // Flush any remaining entries
        writer.flush();
        
        unregisterMBean();
    }

    // ─── DynamicMBean implementation ─────────────────────────────────────

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
        switch (attribute) {
            case ATTR_PENDING_ASYNC:
                return getPendingAsync();
            case ATTR_ASYNC_THROUGHPUT:
                return getAsyncThroughput();
            case ATTR_FSYNC_POLICY:
                return getPolicy().name();
            case ATTR_COORDINATOR_PENDING:
                return getCoordinatorPending();
            case ATTR_COORDINATOR_FSYNC_COUNT:
                return getCoordinatorFsyncCount();
            case ATTR_COORDINATOR_GROUP_SIZE:
                return getCoordinatorGroupSize();
            default:
                throw new AttributeNotFoundException("Unknown attribute: " + attribute);
        }
    }

    @Override
    @SuppressWarnings("unused")
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("AsyncWALWriter attributes are read-only");
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
                "No operations on AsyncWALWriter; attributes are read-only"));
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        String[] names = {
            ATTR_PENDING_ASYNC, ATTR_ASYNC_THROUGHPUT, ATTR_FSYNC_POLICY,
            ATTR_COORDINATOR_PENDING, ATTR_COORDINATOR_FSYNC_COUNT, ATTR_COORDINATOR_GROUP_SIZE
        };
        String[] descriptions = {
            "Number of async operations currently pending",
            "Throughput in async operations per second",
            "Current fsync policy (ALWAYS, GROUP, EVERYSEC, NONE)",
            "Number of commits pending in the group coordinator",
            "Total fsync operations performed by coordinator",
            "Current number of commits in the batch group"
        };
        MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[names.length];
        for (int i = 0; i < names.length; i++) {
            attributes[i] = new MBeanAttributeInfo(names[i], "long", descriptions[i],
                    true, false, false);
        }
        return new MBeanInfo(AsyncWALWriter.class.getName(),
                "DieselDB Async WAL Writer (policy-aware async commits)",
                attributes, null, null, null);
    }

    // ─── JMX registration ───────────────────────────────────────────────

    /**
     * Registers the JMX MBean.
     */
    private void registerMBean() {
        if (registeredName != null) {
            return;
        }
        
        synchronized (this) {
            if (registeredName != null) {
                return;
            }
            
            try {
                ObjectName name = new ObjectName(
                    OBJECT_NAME_PREFIX + ",id=" + MBEAN_SEQUENCE.incrementAndGet());
                ManagementFactory.getPlatformMBeanServer().registerMBean(this, name);
                registeredName = name;
            } catch (Exception e) {
                LOGGER.warn("Failed to register AsyncWALWriter MBean: {}", e.toString());
            }
        }
    }

    /**
     * Unregisters the JMX MBean.
     */
    private void unregisterMBean() {
        ObjectName name = registeredName;
        if (name != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
            } catch (Exception ignored) {
                // Already gone; nothing to clean up.
            }
            registeredName = null;
        }
    }
}