package diesel.logging;

import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.Marker;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
    
    /**
     * Checks if sampling is enabled for a specific module.
     */
    public boolean isSamplingEnabled(String module) {
        return sampleRates.containsKey(module) && sampleRates.get(module) > 0;
    }
    
    /**
     * Gets the current count for a module.
     */
    public long getCount(String module) {
        return counters.getOrDefault(module, new AtomicLong(0)).get();
    }
    
    /**
     * Resets all counters.
     */
    public void resetCounters() {
        counters.clear();
    }
    
    /**
     * Gets the sample rate for a module.
     */
    public int getSampleRate(String module) {
        return sampleRates.getOrDefault(module, 0);
    }
    
    // TODO: Implement full filtering logic when needed
    // For now, just return neutral to allow compilation
    @Override
    public FilterReply decide(Marker marker, ch.qos.logback.classic.Logger logger, Level level, String format, Object[] params, Throwable t) {
        return FilterReply.NEUTRAL;
    }
}