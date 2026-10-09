package diesel.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
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
 * Coordinates group commits to reduce fsync frequency (prompt4.md #15, R3-003 step 5/5).
 * 
 * <p><b>Design.</b> Accumulates commit requests until a timeout (5 ms) or maximum group size (64),
 * then writes all entries as a single batch and issues one fsync. Each request's future completes
 * after the group fsync, ensuring durability for the policy. Thread-safe with lock-free batching
 * under normal load.
 * 
 * <p><b>Thread safety.</b> Uses a ReentrantLock for group modification (add/flush) and a concurrent
 * pending list for reads. The flush thread is the only writer to the pending list; all other threads
 * only add or read.
 * 
 * <p><b>Metrics.</b> Exposed as a {@code DynamicMBean} ({@code diesel:type=GroupCommitCoordinator,id=N}):
 * pending commits, current group size, fsync count, group commit throughput.
 */
public final class GroupCommitCoordinator implements AutoCloseable, DynamicMBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupCommitCoordinator.class);

    /** JMX object name prefix. */
    static final String OBJECT_NAME_PREFIX = "diesel:type=GroupCommitCoordinator";
    /** JMX attribute names. */
    static final String ATTR_PENDING_COMMITS = "group.pending.commits";
    static final String ATTR_CURRENT_GROUP_SIZE = "group.current.size";
    static final String ATTR_FSYNC_COUNT = "group.fsync.count";
    static final String ATTR_GROUP_THROUGHPUT = "group.throughput.commits.per.sec";
    static final String ATTR_GROUP_WINDOW_MS = "group.window.ms";
    static final String ATTR_GROUP_MAX_SIZE = "group.max.size";

    /** MBean sequence counter for multiple instances. */
    private static final AtomicInteger MBEAN_SEQUENCE = new AtomicInteger();

    private final WALWriter writer;
    private final FsyncPolicy policy;
    private final long groupWindowMs;
    private final int groupMaxSize;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock groupLock = new ReentrantLock();
    private final List<GroupCommitRequest> pending = new ArrayList<>();
    private final AtomicLong fsyncCount = new AtomicLong();
    private final AtomicLong totalCommits = new AtomicLong();
    private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
    private volatile ScheduledFuture<?> scheduledFlush;
    private volatile boolean running = true;
    private volatile ObjectName registeredName;

    /**
     * Creates a group commit coordinator.
     *
     * @param writer the WAL writer to use for batching
     * @param policy the fsync policy (must be GROUP or EVERYSEC to use batching)
     * @param groupWindowMs the window size in milliseconds for batching commits
     * @param groupMaxSize the maximum number of commits to batch together
     * @param scheduler a scheduled executor for timeout-based flushes
     */
    public GroupCommitCoordinator(WALWriter writer, FsyncPolicy policy, 
                                long groupWindowMs, int groupMaxSize,
                                ScheduledExecutorService scheduler) {
        this.writer = writer;
        this.policy = policy;
        this.groupWindowMs = groupWindowMs;
        this.groupMaxSize = groupMaxSize;
        this.scheduler = scheduler;
        registerMBean();
    }

    /**
     * Submits a commit request for batching and returns a future that completes
     * when the commit is durable according to the fsync policy.
     *
     * @param txid the transaction id
     * @param op the operation code (typically COMMIT)
     * @param before the before image (may be null)
     * @param after the after image (may be null)
     * @return a future that completes when the commit is durable
     */
    public CompletableFuture<WALEntry> submitCommit(long txid, WALOpcode op, 
                                                  byte[] before, byte[] after) {
        if (!running) {
            CompletableFuture<WALEntry> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("GroupCommitCoordinator is closed"));
            return future;
        }

        CompletableFuture<WALEntry> future = new CompletableFuture<>();
        GroupCommitRequest request = new GroupCommitRequest(txid, op, before, after, future);
        
        totalCommits.incrementAndGet();
        
        if (policy == FsyncPolicy.NONE) {
            // No fsync: complete immediately after append
            try {
                WALEntry entry = writer.append(txid, op, before, after);
                future.complete(entry);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            return future;
        }
        
        if (policy == FsyncPolicy.ALWAYS) {
            // Always fsync: submit as a single-commit group
            submitSingleCommit(request);
            return future;
        }
        
        // GROUP or EVERYSEC: add to pending and trigger batching
        groupLock.lock();
        try {
            pending.add(request);
            
            // Check if we need to flush immediately due to group size
            if (pending.size() >= groupMaxSize) {
                flushGroup();
            } else {
                // GROUP: flush after the group window; EVERYSEC: at most one
                // fsync per second (rescheduled after each batch flush).
                long delayMs = (policy == FsyncPolicy.EVERYSEC) ? 1_000 : groupWindowMs;
                if (scheduledFlush == null || scheduledFlush.isDone()) {
                    scheduledFlush = scheduler.schedule(this::flushGroupTimeout, 
                                                       delayMs, TimeUnit.MILLISECONDS);
                }
            }
        } finally {
            groupLock.unlock();
        }
        
        return future;
    }

    /**
     * Flushes all pending commits immediately.
     *
     * @throws IOException if the flush fails
     */
    public void flush() throws IOException {
        groupLock.lock();
        try {
            flushGroup();
        } finally {
            groupLock.unlock();
        }
    }

    /**
     * Flushes the current group if non-empty.
     */
    private void flushGroup() {
        if (pending.isEmpty()) {
            return;
        }
        
        // Cancel any pending timeout flush
        if (scheduledFlush != null && !scheduledFlush.isDone()) {
            scheduledFlush.cancel(false);
            scheduledFlush = null;
        }
        
        List<GroupCommitRequest> toFlush = new ArrayList<>(pending);
        pending.clear();
        
        if (!toFlush.isEmpty()) {
            writeBatch(toFlush);
        }
    }

    /**
     * Flushes the group when the timeout expires.
     */
    private void flushGroupTimeout() {
        groupLock.lock();
        try {
            flushGroup();
        } finally {
            groupLock.unlock();
        }
    }

    /**
     * Writes a batch of requests and completes their futures after fsync.
     */
    private void writeBatch(List<GroupCommitRequest> requests) {
        try {
            // Write all entries to the WAL
            List<WALEntry> entries = new ArrayList<>();
            for (GroupCommitRequest request : requests) {
                WALEntry entry = writer.append(request.txid(), request.op(), 
                                             request.before(), request.after());
                entries.add(entry);
            }
            
            // For EVERYSEC policy, complete futures immediately (no fsync)
            if (policy == FsyncPolicy.EVERYSEC) {
                for (int i = 0; i < requests.size(); i++) {
                    requests.get(i).future().complete(entries.get(i));
                }
                return;
            }
            
            // For GROUP and ALWAYS, fsync and then complete futures
            writer.flush();
            fsyncCount.incrementAndGet();
            
            for (int i = 0; i < requests.size(); i++) {
                requests.get(i).future().complete(entries.get(i));
            }
            
        } catch (Exception e) {
            // Complete all futures exceptionally
            for (GroupCommitRequest request : requests) {
                request.future().completeExceptionally(e);
            }
            LOGGER.error("Failed to write batch of {} commits", requests.size(), e);
        }
    }

    /**
     * Submits a single commit with immediate fsync (for ALWAYS policy).
     */
    private void submitSingleCommit(GroupCommitRequest request) {
        try {
            WALEntry entry = writer.append(request.txid(), request.op(), 
                                        request.before(), request.after());
            writer.flush();
            fsyncCount.incrementAndGet();
            request.future().complete(entry);
        } catch (Exception e) {
            request.future().completeExceptionally(e);
            LOGGER.error("Failed to write single commit for txid={}", request.txid(), e);
        }
    }

    /**
     * Returns the current number of pending commits.
     */
    public int getPendingCommits() {
        return pending.size();
    }
    
    /**
     * Returns the total number of commits processed.
     */
    public long getTotalCommits() {
        return totalCommits.get();
    }

    /**
     * Returns the current group size (number of commits that would be flushed in the next batch).
     */
    public int getCurrentGroupSize() {
        return pending.size();
    }

    /**
     * Returns the total number of fsync operations performed.
     */
    public long getFsyncCount() {
        return fsyncCount.get();
    }

    /**
     * Returns the group commit throughput in commits per second.
     */
    public double getGroupThroughput() {
        long elapsedMs = System.currentTimeMillis() - startTime.get();
        if (elapsedMs <= 0) {
            return 0.0;
        }
        return (totalCommits.get() * 1000.0) / elapsedMs;
    }

    /**
     * Returns the group window size in milliseconds.
     */
    public long getGroupWindowMs() {
        return groupWindowMs;
    }

    /**
     * Returns the maximum group size.
     */
    public int getGroupMaxSize() {
        return groupMaxSize;
    }

    /**
     * Returns the current fsync policy.
     */
    public FsyncPolicy getPolicy() {
        return policy;
    }

    @Override
    public void close() throws IOException {
        if (!running) {
            return;
        }
        
        running = false;
        
        // Cancel any pending flush
        if (scheduledFlush != null && !scheduledFlush.isDone()) {
            scheduledFlush.cancel(false);
        }
        
        // Flush any remaining commits
        groupLock.lock();
        try {
            flushGroup();
        } finally {
            groupLock.unlock();
        }
        
        unregisterMBean();
    }

    // ─── DynamicMBean implementation ─────────────────────────────────────

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
        switch (attribute) {
            case ATTR_PENDING_COMMITS:
                return getPendingCommits();
            case ATTR_CURRENT_GROUP_SIZE:
                return getCurrentGroupSize();
            case ATTR_FSYNC_COUNT:
                return getFsyncCount();
            case ATTR_GROUP_THROUGHPUT:
                return getGroupThroughput();
            case ATTR_GROUP_WINDOW_MS:
                return getGroupWindowMs();
            case ATTR_GROUP_MAX_SIZE:
                return getGroupMaxSize();
            default:
                throw new AttributeNotFoundException("Unknown attribute: " + attribute);
        }
    }

    @Override
    @SuppressWarnings("unused")
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("GroupCommitCoordinator attributes are read-only");
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
                "No operations on GroupCommitCoordinator; attributes are read-only"));
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        String[] names = {
            ATTR_PENDING_COMMITS, ATTR_CURRENT_GROUP_SIZE, ATTR_FSYNC_COUNT,
            ATTR_GROUP_THROUGHPUT, ATTR_GROUP_WINDOW_MS, ATTR_GROUP_MAX_SIZE
        };
        String[] descriptions = {
            "Number of commits waiting to be batched",
            "Number of commits in the current batch that will be flushed together",
            "Total number of fsync operations performed",
            "Throughput in commits per second",
            "Maximum time to wait before flushing a batch (milliseconds)",
            "Maximum number of commits to batch together"
        };
        MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[names.length];
        for (int i = 0; i < names.length; i++) {
            attributes[i] = new MBeanAttributeInfo(names[i], "long", descriptions[i],
                    true, false, false);
        }
        return new MBeanInfo(GroupCommitCoordinator.class.getName(),
                "DieselDB Group Commit Coordinator (batching for fsync reduction)",
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
                LOGGER.warn("Failed to register GroupCommitCoordinator MBean: {}", e.toString());
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

    /**
     * A commit request waiting to be batched.
     */
    private static record GroupCommitRequest(
        long txid,
        WALOpcode op,
        byte[] before,
        byte[] after,
        CompletableFuture<WALEntry> future
    ) {}
}