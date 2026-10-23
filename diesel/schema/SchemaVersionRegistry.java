package diesel.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import diesel.DieselException;

/**
 * Manages multiple schema versions for a single table with bounded history and timeout-based cleanup.
 * 
 * <p>Key features:
 * - Monotonic version counter starting from 1
 * - Bounded history: retains current + last MAX_RETAINED versions + all pinned versions
 * - Timeout-based killing: old versions beyond timeout are removed
 * - Pin-based protection: pinned versions never expire
 * - Thread-safe operations with minimal locking
 * 
 * @see SchemaVersion
 * @see PinnedSchema
 */
public final class SchemaVersionRegistry {
    
    /**
     * Handle for pinning a schema version. Must be closed when no longer needed.
     * Closing releases the pin and allows the version to be garbage collected if not current.
     */
    public static final class PinnedSchema implements AutoCloseable {
        private final SchemaVersionRegistry registry;
        private final long version;
        private final long pinNanos;
        private boolean closed = false;
        
        PinnedSchema(SchemaVersionRegistry registry, long version, long pinNanos) {
            this.registry = registry;
            this.version = version;
            this.pinNanos = pinNanos;
        }
        
        public long getVersion() {
            return version;
        }
        
        public long getPinNanos() {
            return pinNanos;
        }
        
        /**
         * Releases the pin. Idempotent - safe to call multiple times.
         * After closing, this pinned version may be subject to timeout-based removal
         * if it's not the current version and not retained by other pins.
         */
        @Override
        public void close() {
            if (!closed) {
                registry.releasePin(this);
                closed = true;
            }
        }
    }
    
    private static final int MAX_RETAINED = 32;
    private static final long DEFAULT_TIMEOUT_MINUTES = 10;
    
    private final AtomicLong versionCounter;
    private final long timeoutNanos;
    private final ScheduledExecutorService reaperService;
    private ScheduledFuture<?> reaperTask;
    
    // Thread-safe storage: LinkedHashMap preserves insertion order for bounded history
    // ConcurrentHashMap for concurrent access + synchronized for compound operations
    private final Map<Long, SchemaVersion> versions = Collections.synchronizedMap(
        new LinkedHashMap<Long, SchemaVersion>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, SchemaVersion> eldest) {
                // Keep: current + last MAX_RETAINED + pinned versions
                if (size() <= MAX_RETAINED + getPinnedCount()) {
                    return false;
                }
                // Contract: pinned versions never expire — skip bounded-history
                // eviction while the eldest entry is pinned (the map may grow
                // past the bound for the duration of the pin; stale pins are
                // reaped by timeout, which releases the bound again).
                return !isVersionPinned(eldest.getKey());
            }
        }
    );
    
    // Track pinned versions to prevent removal from bounded history
    private final Map<Long, Long> pinnedVersions = Collections.synchronizedMap(
        new LinkedHashMap<Long, Long>()
    );
    
    // Test hooks
    private volatile long testClockNanos = System.nanoTime();
    private volatile long testTimeoutMinutes = -1;
    
    /**
     * Creates a new registry with the initial schema version 1.
     * 
     * @param initialColumns    Initial column names
     * @param initialTypes      Initial column name to type mapping
     * @param description        Initial description (e.g. "CREATE TABLE")
     * @param timeoutMinutes     Timeout for old versions in minutes (<=0 for default)
     */
    public SchemaVersionRegistry(List<String> initialColumns, Map<String, Class<?>> initialTypes, 
                                String description, long timeoutMinutes) {
        this.versionCounter = new AtomicLong(1);
        this.timeoutNanos = calculateTimeoutNanos(timeoutMinutes);
        this.reaperService = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread t = new Thread(r, "SchemaVersionReaper-" + System.identityHashCode(this));
                t.setDaemon(true);
                return t;
            }
        );
        
        // Start reaper with period = min(timeout, 60s) to avoid too frequent checks
        long periodNanos = Math.min(timeoutNanos, TimeUnit.MINUTES.toNanos(1));
        this.reaperTask = reaperService.scheduleAtFixedRate(
            () -> reapStalePins(System.nanoTime()), periodNanos, periodNanos, TimeUnit.NANOSECONDS
        );
        
        // Create initial version
        SchemaVersion initial = new SchemaVersion(
            1, 
            getCurrentTimeNanos(), 
            initialColumns, 
            initialTypes, 
            description
        );
        versions.put(1L, initial);
    }
    
    /**
     * Gets the current (latest) schema version.
     * 
     * @return Current version (never null)
     * @throws IllegalStateException if registry is empty (should not happen with initial version)
     */
    public SchemaVersion current() {
        synchronized (versions) {
            if (versions.isEmpty()) {
                throw new IllegalStateException("Registry has no versions");
            }
            return versions.get(versionCounter.get());
        }
    }
    
    /**
     * Gets the current version number.
     * 
     * @return Current version number (starts at 1)
     */
    public long currentVersion() {
        return versionCounter.get();
    }
    
    /**
     * Creates a new schema version with the specified columns and types.
     * The new version becomes the current version.
     * 
     * @param newColumns    Updated column list (must be superset of current for step 1)
     * @param newTypes      Updated column type mapping
     * @param description   Description of the change
     * @return New schema version
     */
    public SchemaVersion createNewVersion(List<String> newColumns, Map<String, Class<?>> newTypes, 
                                         String description) {
        long newVersion = versionCounter.incrementAndGet();
        long now = getCurrentTimeNanos();
        
        SchemaVersion version = new SchemaVersion(
            newVersion, 
            now, 
            newColumns, 
            newTypes, 
            description
        );
        
        synchronized (versions) {
            versions.put(newVersion, version);
        }
        
        return version;
    }
    
    /**
     * Pins the current schema version to prevent timeout-based removal.
     * Must be closed when no longer needed.
     * 
     * @return Pinned handle for the current version
     */
    public PinnedSchema pinCurrent() {
        long currentVersion = currentVersion();
        long pinNanos = getCurrentTimeNanos();
        
        synchronized (pinnedVersions) {
            pinnedVersions.put(currentVersion, pinNanos);
        }
        
        return new PinnedSchema(this, currentVersion, pinNanos);
    }
    
    /**
     * Resolves a schema version by number.
     * 
     * @param versionNumber Version number to resolve
     * @return The schema version
     * @throws SchemaVersionTimeoutException if version has been killed due to timeout
     * @throws IllegalArgumentException if version doesn't exist
     */
    public SchemaVersion resolve(long versionNumber) {
        // Check if this version has been killed (removed from versions map)
        if (!isVersionAlive(versionNumber)) {
            throw new SchemaVersionTimeoutException(versionNumber, 
                "Schema version " + versionNumber + " has been killed due to timeout");
        }
        
        SchemaVersion version = versions.get(versionNumber);
        if (version == null) {
            throw new IllegalArgumentException("Schema version " + versionNumber + " not found");
        }
        
        return version;
    }
    
    /**
     * Gets the number of currently pinned versions.
     * 
     * @return Count of pinned versions
     */
    public int getPinnedCount() {
        synchronized (pinnedVersions) {
            return pinnedVersions.size();
        }
    }
    
    /**
     * Gets the number of total versions in the registry (including those not yet reaped).
     * 
     * @return Total version count
     */
    public int getVersionCount() {
        synchronized (versions) {
            return versions.size();
        }
    }
    
    // Package-private for testing
    void reapStalePins(long currentTimeNanos) {
        synchronized (pinnedVersions) {
            // Remove pins that are older than timeout and not for current version
            pinnedVersions.entrySet().removeIf(entry -> {
                long version = entry.getKey();
                long pinTime = entry.getValue();
                
                // Never remove current version pins
                if (version == currentVersion()) {
                    return false;
                }
                
                // Remove old pins
                return (currentTimeNanos - pinTime) > timeoutNanos;
            });
        }
        
        synchronized (versions) {
            // Remove versions that are not current, not pinned, and older than timeout
            versions.entrySet().removeIf(entry -> {
                long version = entry.getKey();
                SchemaVersion schemaVersion = entry.getValue();
                
                // Keep current version
                if (version == currentVersion()) {
                    return false;
                }
                
                // Keep pinned versions
                if (isVersionPinned(version)) {
                    return false;
                }
                
                // Remove old versions
                return (currentTimeNanos - schemaVersion.getCreatedAtMs()) > timeoutNanos;
            });
        }
    }
    
    // Package-private for testing
    void releasePin(PinnedSchema pin) {
        synchronized (pinnedVersions) {
            pinnedVersions.remove(pin.getVersion(), pin.getPinNanos());
        }
    }
    
    // Package-private for testing
    boolean isVersionAlive(long version) {
        synchronized (versions) {
            return versions.containsKey(version);
        }
    }
    
    // Package-private for testing
    boolean isVersionPinned(long version) {
        synchronized (pinnedVersions) {
            return pinnedVersions.containsKey(version);
        }
    }
    
    // Package-private for testing
    void setTimeoutForTest(long timeoutMinutes) {
        this.testTimeoutMinutes = timeoutMinutes;
    }
    
    // Package-private for testing
    void setClockForTest(long testClockNanos) {
        this.testClockNanos = testClockNanos;
    }
    
    // Package-private for testing
    void resetForTest() {
        this.testTimeoutMinutes = -1;
        this.testClockNanos = System.nanoTime();
    }
    
    private long calculateTimeoutNanos(long timeoutMinutes) {
        long effectiveMinutes = timeoutMinutes;
        if (effectiveMinutes <= 0) {
            effectiveMinutes = testTimeoutMinutes > 0 ? testTimeoutMinutes : DEFAULT_TIMEOUT_MINUTES;
        }
        return TimeUnit.MINUTES.toNanos(effectiveMinutes);
    }
    
    private long getCurrentTimeNanos() {
        return testClockNanos;
    }
    
    /**
     * Closes the registry and shuts down the reaper service.
     * Should be called when the table is no longer in use.
     */
    public void close() {
        if (reaperTask != null) {
            reaperTask.cancel(false);
        }
        reaperService.shutdownNow();
    }
}