package diesel.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.Marker;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * TurboFilter for sampling log messages from hot-path modules.
 * 
 * <p>Reduces log volume in high-frequency logging scenarios by dropping
 * (N-1) of N records before formatting. This is particularly useful for
 * modules like WAL, bufferpool, and replication that can generate thousands
 * of log messages per second.
 * 
 * <p>Sampling is configured via properties like:
 * - logging.sampling.wal=100 (1 record out of 100)
 * - logging.sampling.bufferpool=50 (1 record out of 50)
 * - logging.sampling.replication=10 (1 record out of 10)
 */
public class DieselSamplingTurboFilter extends TurboFilter {
    
    private static final Map<String, Integer> MODULE_MAPPING = Map.of(
        "wal", "diesel.wal",
        "bufferpool", "diesel.storage.page.BufferPool",
        "bufferpoolflusher", "diesel.storage.page.BufferPoolFlusher",
        "pageflusher", "diesel.storage.page.PageFlusher",
        "persistflusher", "diesel.PersistFlusher",
        "replication", "diesel.replication",
        "replicationcoordinator", "diesel.ReplicationCoordinator",
        "syncreplicationcoordinator", "diesel.SyncReplicationCoordinator"
    );
    
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, Integer> sampleRates = new ConcurrentHashMap<>();
    private boolean enabled = true;
    
    @Override
    public void start() {
        super.start();
        this.enabled = true;
    }
    
    @Override
    public void stop() {
        super.stop();
        this.enabled = false;
        counters.clear();
        sampleRates.clear();
    }
    
    @Override
    public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] params, Throwable t) {
        if (!enabled) {
            return FilterReply.NEUTRAL;
        }
        
        String loggerName = logger.getName();
        String module = findModuleForLogger(loggerName);
        
        if (module == null) {
            return FilterReply.NEUTRAL; // Not a sampled module
        }
        
        Integer sampleRate = sampleRates.get(module);
        if (sampleRate == null || sampleRate <= 1) {
            return FilterReply.NEUTRAL; // No sampling or sample all
        }
        
        AtomicLong counter = counters.computeIfAbsent(loggerName, k -> new AtomicLong(0));
        long count = counter.incrementAndGet();
        
        // Only allow 1 out of N records to pass through
        if (count % sampleRate == 0) {
            return FilterReply.NEUTRAL;
        } else {
            return FilterReply.DENY; // Drop this record
        }
    }
    
    /**
     * Updates sample rates from configuration.
     * 
     * @param config configuration properties
     */
    public void updateSampleRates(Map<String, String> config) {
        sampleRates.clear();
        
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            
            if (key.startsWith("logging.sampling.")) {
                String module = key.substring("logging.sampling.".length());
                try {
                    int rate = Integer.parseInt(value);
                    if (rate > 0) {
                        sampleRates.put(module, rate);
                    }
                } catch (NumberFormatException e) {
                    // Invalid rate, ignore
                }
            }
        }
        
        // Reset counters when sample rates change
        counters.clear();
    }
    
    /**
     * Gets the current sample rate for a module.
     * 
     * @param module module name
     * @return sample rate, or null if not configured
     */
    public Integer getSampleRate(String module) {
        return sampleRates.get(module);
    }
    
    /**
     * Gets the current count for a logger.
     * 
     * @param loggerName logger name
     * @return current count
     */
    public long getCount(String loggerName) {
        AtomicLong counter = counters.get(loggerName);
        return counter != null ? counter.get() : 0;
    }
    
    /**
     * Resets all counters.
     */
    public void resetCounters() {
        counters.clear();
    }
    
    /**
     * Finds the module for a given logger name.
     * 
     * @param loggerName logger name to check
     * @return module name, or null if not found
     */
    private String findModuleForLogger(String loggerName) {
        for (Map.Entry<String, String> entry : MODULE_MAPPING.entrySet()) {
            String module = entry.getKey();
            String loggerPrefix = entry.getValue();
            
            if (loggerName.startsWith(loggerPrefix)) {
                return module;
            }
        }
        
        return null;
    }
    
    /**
     * Checks if sampling is enabled for a module.
     * 
     * @param module module name
     * @return true if sampling is enabled
     */
    public boolean isSamplingEnabled(String module) {
        Integer rate = sampleRates.get(module);
        return rate != null && rate > 1;
    }
    
    /**
     * Enables or disables the sampling filter.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    /**
     * Checks if the filter is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }
}