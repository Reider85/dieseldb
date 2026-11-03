package diesel;

import diesel.SyncAck;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Synchronous replication coordinator (prompt4 #60, R3-016 step 3/5).
 * 
 * <p>Coordinates synchronous replication by waiting for acknowledgments from
 * configured standbys before allowing a COMMIT to proceed to the client.
 * 
 * <p>Configuration:
 * <ul>
 *   <li>{@code synchronous_commit} - off, local, on (default off)</li>
 *   <li>{@code synchronous_standby_names} - comma-separated list of standby names</li>
 *   <li>{@code synchronous_commit.timeout.ms} - timeout in milliseconds (default 5000)</li>
 * </ul>
 */
public final class SyncReplicationCoordinator {
    
    private static final Logger LOGGER = Logger.getLogger(SyncReplicationCoordinator.class.getName());
    
    /** Synchronous commit modes. */
    public enum SyncMode {
        OFF,      // Don't wait for any standby acknowledgments
        LOCAL,    // Wait only for local disk write (no standby wait)
        ON        // Wait for N standby acknowledgments
    }
    
    /** Default timeout for synchronous commit (5 seconds). */
    private static final long DEFAULT_TIMEOUT_MS = 5000;
    
    /** Database instance for WAL access. */
    private final Database database;
    /** Current synchronous commit mode. */
    private final SyncMode syncMode;
    /** Names of standbys that must acknowledge commits. */
    private final Set<String> requiredStandbyNames;
    /** Timeout for waiting on acknowledgments. */
    private final long timeoutMs;
    /** Active ACK collectors (one per commit). */
    private final Map<Long, AckCollector> activeCollectors = new ConcurrentHashMap<>();
    /** Whether this coordinator is running. */
    private final AtomicBoolean running = new AtomicBoolean(true);
    
    /**
     * Creates a new synchronous replication coordinator.
     * 
     * @param database the database instance
     */
    public SyncReplicationCoordinator(Database database) {
        this.database = database;
        this.syncMode = parseSyncMode();
        this.requiredStandbyNames = parseRequiredStandbyNames();
        this.timeoutMs = ConfigLoader.getLong("synchronous_commit.timeout.ms", DEFAULT_TIMEOUT_MS);
        
        LOGGER.log(Level.INFO, "SyncReplicationCoordinator initialized: mode={0}, standbys={1}, timeout={2}ms",
                new Object[]{syncMode, requiredStandbyNames, timeoutMs});
    }
    
    /**
     * Submits a commit for synchronous replication processing.
     * 
     * @param transaction the transaction being committed
     * @param commitLsn the LSN of the commit record
     * @return true if the commit can proceed, false if it should be aborted
     */
    public boolean submitCommit(Transaction transaction, long commitLsn) {
        if (!running.get()) {
            LOGGER.warning("SyncReplicationCoordinator is not running");
            return true; // Allow commit if coordinator is stopped
        }
        
        if (syncMode == SyncMode.OFF) {
            LOGGER.fine("Synchronous replication disabled: commit proceeding without standby wait");
            return true;
        }
        
        if (requiredStandbyNames.isEmpty()) {
            LOGGER.warning("Synchronous replication enabled but no standbys configured");
            return true; // No standbys to wait for
        }
        
        if (syncMode == SyncMode.LOCAL) {
            LOGGER.fine("Synchronous replication LOCAL mode: commit proceeding after local write");
            return true;
        }
        
        // ON mode: wait for standby acknowledgments
        return waitForStandbyAcks(transaction, commitLsn);
    }
    
    /**
     * Processes an acknowledgment from a standby.
     * 
     * @param standbyName the name of the standby
     * @param appliedLsn the LSN the standby has applied
     */
    public void processAck(String standbyName, long appliedLsn) {
        if (!running.get()) {
            return;
        }
        
        // Find active collectors waiting for this standby
        activeCollectors.values().forEach(collector -> {
            if (collector.requiresAck(standbyName)) {
                collector.addAck(new SyncAck(standbyName, appliedLsn));
            }
        });
    }
    
    /**
     * Stops the coordinator and cleans up resources.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            // Wake up any waiting collectors with an exception
            activeCollectors.values().forEach(collector -> {
                collector.abort(new IllegalStateException("Coordinator stopped"));
            });
            activeCollectors.clear();
            LOGGER.info("SyncReplicationCoordinator stopped");
        }
    }
    
    /**
     * Waits for acknowledgments from required standbys.
     */
    private boolean waitForStandbyAcks(Transaction transaction, long commitLsn) {
        AckCollector collector = new AckCollector(transaction, commitLsn, requiredStandbyNames);
        activeCollectors.put(commitLsn, collector);
        
        try {
            LOGGER.log(Level.FINE, "Waiting for {0} standby ACKs for commit LSN {1}",
                    new Object[]{requiredStandbyNames.size(), commitLsn});
            
            boolean success = collector.waitForAcks(timeoutMs);
            
            if (success) {
                LOGGER.log(Level.INFO, "Commit LSN {0} confirmed by all required standbys", commitLsn);
            } else {
                LOGGER.log(Level.WARNING, "Commit LSN {0} timed out waiting for standby ACKs", commitLsn);
            }
            
            return success;
        } finally {
            activeCollectors.remove(commitLsn);
        }
    }
    
    /**
     * Parses the synchronous commit mode from configuration.
     */
    private SyncMode parseSyncMode() {
        String modeStr = ConfigLoader.getString("synchronous_commit", "off").trim().toLowerCase();
        try {
            return SyncMode.valueOf(modeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Invalid synchronous_commit mode '{0}', using default 'off'", modeStr);
            return SyncMode.OFF;
        }
    }
    
    /**
     * Parses the required standby names from configuration.
     */
    private Set<String> parseRequiredStandbyNames() {
        String namesStr = ConfigLoader.getString("synchronous_standby_names", "").trim();
        if (namesStr.isEmpty()) {
            return Collections.emptySet();
        }
        
        return Arrays.stream(namesStr.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toSet());
    }
    
    /**
     * Collector for acknowledgments during a single commit.
     */
    private class AckCollector {
        private final Transaction transaction;
        private final long commitLsn;
        private final Set<String> requiredStandbyNames;
        private final List<SyncAck> receivedAcks = new CopyOnWriteArrayList<>();
        private final AtomicLong lastReceivedAck = new AtomicLong(-1);
        private final AtomicBoolean aborted = new AtomicBoolean(false);
        private final CountDownLatch completionLatch = new CountDownLatch(1);
        
        public AckCollector(Transaction transaction, long commitLsn, Set<String> requiredStandbyNames) {
            this.transaction = transaction;
            this.commitLsn = commitLsn;
            this.requiredStandbyNames = requiredStandbyNames;
        }
        
        public boolean requiresAck(String standbyName) {
            return !aborted.get() && requiredStandbyNames.contains(standbyName);
        }
        
        public void addAck(SyncAck ack) {
            if (aborted.get()) {
                return;
            }
            
            receivedAcks.add(ack);
            lastReceivedAck.set(Math.max(lastReceivedAck.get(), ack.getAppliedLsn()));
            
            // Check if we have all required ACKs
            if (receivedAcks.size() >= requiredStandbyNames.size()) {
                completionLatch.countDown();
            }
        }
        
        public boolean waitForAcks(long timeoutMs) {
            try {
                boolean completed = completionLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
                
                if (completed) {
                    // Verify all ACKs are up-to-date
                    boolean allUpToDate = receivedAcks.stream()
                            .allMatch(ack -> ack.isUpToDate(commitLsn));
                    
                    if (!allUpToDate) {
                        LOGGER.log(Level.WARNING, "Some standbys were not up-to-date for commit LSN {0}", commitLsn);
                        return false;
                    }
                    
                    return true;
                }
                
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Interrupted waiting for standby ACKs for commit LSN {0}", commitLsn);
                return false;
            }
        }
        
        public void abort(Throwable reason) {
            if (aborted.compareAndSet(false, true)) {
                completionLatch.countDown();
                LOGGER.log(Level.WARNING, "Aborting ACK collection for commit LSN {0}: {1}",
                        new Object[]{commitLsn, reason.getMessage()});
            }
        }
    }
}