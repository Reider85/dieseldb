package diesel.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for DieselSamplingTurboFilter functionality.
 */
@Tag("logging")
public class DieselSamplingTurboFilterTest {
    
    private DieselSamplingTurboFilter filter;
    private Logger logger;
    
    @BeforeEach
    void setUp() {
        filter = new DieselSamplingTurboFilter();
        filter.start();
        
        // Create a mock logger context
        logger = new Logger("diesel.wal.WALWriter", null, null);
    }
    
    @Test
    void testSamplingRate100() {
        // Configure sampling rate of 100 (1 out of 100)
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.wal", "100");
        filter.updateSampleRates(config);
        
        // Test that only 1 out of 100 messages passes through
        int passed = 0;
        for (int i = 0; i < 100; i++) {
            FilterReply reply = filter.decide(null, logger, Level.INFO, "Test message", null, null);
            if (reply == FilterReply.NEUTRAL) {
                passed++;
            }
        }
        
        // Should be exactly 1 message passed through
        assertEquals(1, passed);
    }
    
    @Test
    void testSamplingRate50() {
        // Configure sampling rate of 50 (1 out of 50)
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.bufferpool", "50");
        filter.updateSampleRates(config);
        
        // Test that only 1 out of 50 messages passes through
        int passed = 0;
        for (int i = 0; i < 50; i++) {
            FilterReply reply = filter.decide(null, logger, Level.INFO, "Test message", null, null);
            if (reply == FilterReply.NEUTRAL) {
                passed++;
            }
        }
        
        // Should be exactly 1 message passed through
        assertEquals(1, passed);
    }
    
    @Test
    void testSamplingRate10() {
        // Configure sampling rate of 10 (1 out of 10)
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.replication", "10");
        filter.updateSampleRates(config);
        
        // Test that only 1 out of 10 messages passes through
        int passed = 0;
        for (int i = 0; i < 10; i++) {
            FilterReply reply = filter.decide(null, logger, Level.INFO, "Test message", null, null);
            if (reply == FilterReply.NEUTRAL) {
                passed++;
            }
        }
        
        // Should be exactly 1 message passed through
        assertEquals(1, passed);
    }
    
    @Test
    void testNoSamplingForRate1() {
        // Configure sampling rate of 1 (sample all - no filtering)
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.wal", "1");
        filter.updateSampleRates(config);
        
        // Test that all messages pass through
        for (int i = 0; i < 10; i++) {
            FilterReply reply = filter.decide(null, logger, Level.INFO, "Test message", null, null);
            assertEquals(FilterReply.NEUTRAL, reply);
        }
    }
    
    @Test
    void testNoSamplingForRate0() {
        // Configure sampling rate of 0 (no sampling)
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.wal", "0");
        filter.updateSampleRates(config);
        
        // Test that all messages pass through
        for (int i = 0; i < 10; i++) {
            FilterReply reply = filter.decide(null, logger, Level.INFO, "Test message", null, null);
            assertEquals(FilterReply.NEUTRAL, reply);
        }
    }
    
    @Test
    void testNonSampledLogger() {
        // Configure sampling for wal but not for other modules
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.wal", "100");
        filter.updateSampleRates(config);
        
        // Test a non-sampled logger (should pass through)
        Logger otherLogger = new Logger("diesel.DatabaseServer", null, null);
        for (int i = 0; i < 10; i++) {
            FilterReply reply = filter.decide(null, otherLogger, Level.INFO, "Test message", null, null);
            assertEquals(FilterReply.NEUTRAL, reply);
        }
    }
    
    @Test
    void testModuleMapping() {
        // Test different logger names map to correct modules
        Logger walLogger = new Logger("diesel.wal.WALWriter", null, null);
        Logger bufferpoolLogger = new Logger("diesel.storage.page.BufferPool", null, null);
        Logger replicationLogger = new Logger("diesel.replication.Replication", null, null);
        Logger otherLogger = new Logger("diesel.DatabaseServer", null, null);
        
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.wal", "100");
        config.put("logging.sampling.bufferpool", "50");
        config.put("logging.sampling.replication", "10");
        filter.updateSampleRates(config);
        
        // WAL logger should be sampled
        assertTrue(filter.isSamplingEnabled("wal"));
        
        // BufferPool logger should be sampled
        assertTrue(filter.isSamplingEnabled("bufferpool"));
        
        // Replication logger should be sampled
        assertTrue(filter.isSamplingEnabled("replication"));
        
        // Other logger should not be sampled
        assertFalse(filter.isSamplingEnabled("other"));
    }
    
    @Test
    void testCounterReset() {
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.wal", "100");
        filter.updateSampleRates(config);
        
        // Generate some counts
        for (int i = 0; i < 50; i++) {
            filter.decide(null, logger, Level.INFO, "Test message", null, null);
        }
        
        // Should have some counts
        assertTrue(filter.getCount("diesel.wal.WALWriter") > 0);
        
        // Reset counters
        filter.resetCounters();
        
        // Should be zero
        assertEquals(0, filter.getCount("diesel.wal.WALWriter"));
    }
    
    @Test
    void testFilterCanBeDisabled() {
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.wal", "100");
        filter.updateSampleRates(config);
        
        // Disable filter
        filter.setEnabled(false);
        
        // All messages should pass through even with sampling configured
        for (int i = 0; i < 10; i++) {
            FilterReply reply = filter.decide(null, logger, Level.INFO, "Test message", null, null);
            assertEquals(FilterReply.NEUTRAL, reply);
        }
    }
    
    @Test
    void testInvalidSampleRate() {
        // Configure invalid sample rate
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.wal", "invalid");
        filter.updateSampleRates(config);
        
        // Should not crash and should not sample
        for (int i = 0; i < 10; i++) {
            FilterReply reply = filter.decide(null, logger, Level.INFO, "Test message", null, null);
            assertEquals(FilterReply.NEUTRAL, reply);
        }
        
        // Should not have sampling enabled
        assertFalse(filter.isSamplingEnabled("wal"));
    }
    
    @Test
    void testGetSampleRate() {
        Map<String, String> config = new HashMap<>();
        config.put("logging.sampling.wal", "100");
        config.put("logging.sampling.bufferpool", "50");
        filter.updateSampleRates(config);
        
        assertEquals(100, (int) filter.getSampleRate("wal"));
        assertEquals(50, (int) filter.getSampleRate("bufferpool"));
        assertNull(filter.getSampleRate("nonexistent"));
    }
}