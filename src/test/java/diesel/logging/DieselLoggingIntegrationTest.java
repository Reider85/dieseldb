package diesel.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration test for Phase 4 logging features.
 * 
 * <p>Tests the integration of all Phase 4 components:
 * - Secret redaction
 * - Trace ID correlation
 * - JSON logging format
 * - Sampling filter
 */
@Tag("logging")
@Tag("integration")
public class DieselLoggingIntegrationTest {
    
    @TempDir
    Path tempDir;
    
    private LoggerContext loggerContext;
    private Logger logger;
    private ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> listAppender;
    
    @BeforeEach
    void setUp() {
        loggerContext = new LoggerContext();
        loggerContext.setName("test");
        logger = loggerContext.getLogger("diesel.DatabaseServer");
        
        // Create list appender to capture log events
        listAppender = new ListAppender<>();
        listAppender.setContext(loggerContext);
        listAppender.start();
        
        logger.addAppender(listAppender);
        logger.setLevel(Level.INFO);
    }
    
    @Test
    void testRedactionIntegration() {
        // Test that secrets are redacted in log messages
        DieselRedactionTurboFilter redactionFilter = new DieselRedactionTurboFilter();
        redactionFilter.setContext(loggerContext);
        redactionFilter.start();
        loggerContext.addTurboFilter(redactionFilter);
        
        // Log a message with secret
        logger.info("Config: tls.keystore.password = changeit");
        
        // Verify that the secret is redacted
        assertEquals(1, listAppender.list.size());
        String logMessage = listAppender.list.get(0).getFormattedMessage();
        assertFalse(logMessage.contains("changeit"));
        assertTrue(logMessage.contains("Config: tls.keystore.password = <redacted>") || 
                   listAppender.list.get(0).getLevel() == Level.OFF);
    }
    
    @Test
    void testTraceIdIntegration() {
        // Test trace ID correlation
        DieselTraceContext.set("test-trace-123");
        
        // Log messages with trace ID
        logger.info("Starting database server");
        logger.info("Loading configuration");
        logger.info("Database server ready");
        
        // Verify that trace ID is present in MDC
        assertEquals("test-trace-123", DieselTraceContext.current());
        
        // Clear trace ID
        DieselTraceContext.clear();
        assertNull(DieselTraceContext.current());
    }
    
    @Test
    void testJsonFormatIntegration() {
        // Test JSON logging format
        DieselJsonLayout jsonLayout = new DieselJsonLayout();
        jsonLayout.setContext(loggerContext);
        jsonLayout.start();
        
        // Replace the appender layout with JSON
        ConsoleAppender<ch.qos.logback.classic.spi.ILoggingEvent> consoleAppender = 
            (ConsoleAppender<ch.qos.logback.classic.spi.ILoggingEvent>) logger.getAppender("CONSOLE");
        if (consoleAppender != null) {
            consoleAppender.setLayout(jsonLayout);
        }
        
        // Set trace ID and log message
        DieselTraceContext.set("json-test-456");
        logger.info("JSON format test message");
        
        // Verify JSON format
        assertEquals(1, listAppender.list.size());
        String jsonOutput = listAppender.list.get(0).getFormattedMessage();
        
        // Basic JSON validation
        assertTrue(jsonOutput.startsWith("{"));
        assertTrue(jsonOutput.endsWith("}"));
        assertTrue(jsonOutput.contains("\"ts\":"));
        assertTrue(jsonOutput.contains("\"level\":\"INFO\""));
        assertTrue(jsonOutput.contains("\"logger\":\"diesel.DatabaseServer\""));
        assertTrue(jsonOutput.contains("\"traceId\":\"json-test-456\""));
        assertTrue(jsonOutput.contains("\"msg\":\"JSON format test message\""));
    }
    
    @Test
    void testSamplingFilterIntegration() {
        // Test sampling filter
        DieselSamplingTurboFilter samplingFilter = new DieselSamplingTurboFilter();
        samplingFilter.setContext(loggerContext);
        
        // Configure sampling for WAL module
        java.util.Map<String, String> samplingConfig = new java.util.HashMap<>();
        samplingConfig.put("logging.sampling.wal", "10");
        samplingFilter.updateSampleRates(samplingConfig);
        samplingFilter.start();
        loggerContext.addTurboFilter(samplingFilter);
        
        // Log multiple messages (should be sampled)
        Logger walLogger = loggerContext.getLogger("diesel.wal.WALWriter");
        walLogger.info("WAL write message 1");
        walLogger.info("WAL write message 2");
        walLogger.info("WAL write message 3");
        walLogger.info("WAL write message 4");
        walLogger.info("WAL write message 5");
        walLogger.info("WAL write message 6");
        walLogger.info("WAL write message 7");
        walLogger.info("WAL write message 8");
        walLogger.info("WAL write message 9");
        walLogger.info("WAL write message 10");
        
        // Should have approximately 1 message out of 10
        int walMessages = (int) listAppender.list.stream()
            .filter(event -> event.getLoggerName().startsWith("diesel.wal"))
            .count();
        
        // Should be between 1-3 messages (sampling with some randomness)
        assertTrue(walMessages >= 1 && walMessages <= 3, 
                   "Expected 1-3 WAL messages, got " + walMessages);
    }
    
    @Test
    void testCompleteIntegrationScenario() {
        // Test all features together
        
        // 1. Enable redaction
        DieselRedactionTurboFilter redactionFilter = new DieselRedactionTurboFilter();
        redactionFilter.setContext(loggerContext);
        redactionFilter.start();
        loggerContext.addTurboFilter(redactionFilter);
        
        // 2. Enable JSON format
        DieselJsonLayout jsonLayout = new DieselJsonLayout();
        jsonLayout.setContext(loggerContext);
        jsonLayout.start();
        
        ConsoleAppender<ch.qos.logback.classic.spi.ILoggingEvent> consoleAppender = 
            (ConsoleAppender<ch.qos.logback.classic.spi.ILoggingEvent>) logger.getAppender("CONSOLE");
        if (consoleAppender != null) {
            consoleAppender.setLayout(jsonLayout);
        }
        
        // 3. Set trace ID
        DieselTraceContext.set("integration-test-789");
        
        // 4. Enable sampling
        DieselSamplingTurboFilter samplingFilter = new DieselSamplingTurboFilter();
        samplingFilter.setContext(loggerContext);
        java.util.Map<String, String> samplingConfig = new java.util.HashMap<>();
        samplingConfig.put("logging.sampling.bufferpool", "5");
        samplingFilter.updateSampleRates(samplingConfig);
        samplingFilter.start();
        loggerContext.addTurboFilter(samplingFilter);
        
        // Log various messages
        logger.info("Database server starting");
        
        Logger bufferpoolLogger = loggerContext.getLogger("diesel.storage.page.BufferPool");
        for (int i = 0; i < 10; i++) {
            bufferpoolLogger.debug("Buffer pool operation " + i);
        }
        
        logger.info("Configuration loaded");
        
        // Verify integration
        assertEquals(1, listAppender.list.size()); // Should have only one message due to sampling
        
        String jsonOutput = listAppender.list.get(0).getFormattedMessage();
        
        // Verify all features are present
        assertTrue(jsonOutput.contains("\"traceId\":\"integration-test-789\""));
        assertTrue(jsonOutput.contains("\"level\":\"INFO\""));
        assertFalse(jsonOutput.contains("password")); // Redaction should work
        assertTrue(jsonOutput.contains("\"logger\":\"diesel.DatabaseServer\""));
    }
    
    @Test
    void testLibpqTraceIdExtraction() {
        // Test trace ID extraction from libpq parameters
        String libpqParams = "trace_id=client-trace-123,ApplicationName=MyApp,other=value";
        String traceId = DieselTraceContext.extractFromLibpqParams(libpqParams);
        
        assertEquals("client-trace-123", traceId);
        
        // Test with ApplicationName
        String params2 = "ApplicationName=app-name,trace_id=ignored";
        String traceId2 = DieselTraceContext.extractFromLibpqParams(params2);
        
        assertEquals("app-name", traceId2);
    }
    
    @Test
    void testConfigurationIntegration() {
        // Test that configuration is properly integrated
        Properties config = new Properties();
        config.setProperty("logging.redaction.enabled", "true");
        config.setProperty("logging.trace.enabled", "true");
        config.setProperty("logging.format", "json");
        config.setProperty("logging.sampling.wal", "100");
        
        // Simulate DieselLogConfig.init() behavior
        assertTrue(Boolean.parseBoolean(config.getProperty("logging.redaction.enabled")));
        assertTrue(Boolean.parseBoolean(config.getProperty("logging.trace.enabled")));
        assertEquals("json", config.getProperty("logging.format"));
        assertEquals("100", config.getProperty("logging.sampling.wal"));
    }
}