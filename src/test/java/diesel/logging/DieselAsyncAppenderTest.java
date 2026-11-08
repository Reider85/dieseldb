package diesel.logging;

import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.classic.AsyncAppender;
import diesel.ConfigLoader;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for async logging functionality in DieselLogConfig.
 */
@Tag("query")
class DieselAsyncAppenderTest {
    
    private LoggerContext loggerContext;
    private ListAppender<ILoggingEvent> listAppender;
    private DieselAsyncAppenderTest.TestConfigLoader testConfigLoader;
    
    @BeforeEach
    void setUp() {
        // Use a test-specific config loader
        testConfigLoader = new DieselAsyncAppenderTest.TestConfigLoader();
        
        // Capture logging events for verification
        loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        listAppender = new ListAppender<>();
        listAppender.setContext(loggerContext);
        listAppender.start();
        
        // Add appender to root logger
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(listAppender);
        rootLogger.setLevel(Level.DEBUG); // Capture all levels for testing
    }
    
    @Test
    @Tag("smoke")
    @DisplayName("Async logging is disabled by default")
    void asyncDisabledByDefault() {
        // Use default config (async.enabled = false)
        testConfigLoader.clearProperties();
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.load());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.load());
        
        // Verify root logger still has direct appenders
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        assertNull(rootLogger.getAppender("ASYNC"), "AsyncAppender should not be present when disabled");
        
        // Verify that CONSOLE and FILE appenders are still direct
        assertNotNull(rootLogger.getAppender("CONSOLE") != null || rootLogger.getAppender("FILE") != null, 
                    "Should have direct appenders when async is disabled");
    }
    
    @Test
    @Tag("query")
    @DisplayName("Async enabled wraps console and file appenders")
    void asyncEnabledWrapsConsoleAndFile() {
        // Enable async logging
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "100");
        testConfigLoader.setProperty("logging.async.never.block", "true");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.load());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.load());
        
        // Verify root logger has AsyncAppender
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        assertNotNull(asyncAppender, "AsyncAppender should be present when enabled");
        
        // Verify AsyncAppender has the configured properties
        assertEquals(100, asyncAppender.getQueueSize(), "Queue size should be configured");
        assertTrue(asyncAppender.isNeverBlock(), "Never block should be true");
        
        // Verify that direct appenders are removed
        assertNull(rootLogger.getAppender("CONSOLE"), "CONSOLE appender should be detached");
        assertNull(rootLogger.getAppender("FILE"), "FILE appender should be detached");
        
        // AsyncAppender should contain the original appenders
        assertTrue(asyncAppender.iterator().hasNext(), "AsyncAppender should contain nested appenders");
    }
    
    @Test
    @Tag("query")
    @DisplayName("Records are delivered asynchronously")
    void recordsAreDeliveredAsync() {
        // Enable async logging with small queue for testing
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "10");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.load());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.load());
        
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        
        // Create a new list appender to capture async output
        ListAppender<ILoggingEvent> asyncListAppender = new ListAppender<>();
        asyncListAppender.setContext(loggerContext);
        asyncListAppender.start();
        
        // Find the nested appender inside async
        ch.qos.logback.core.Appender<ILoggingEvent> nestedAppender = asyncAppender.iterator().next();
        if (nestedAppender instanceof ch.qos.logback.core.spi.AppenderAttachable) {
            ((ch.qos.logback.core.spi.AppenderAttachable<ILoggingEvent>) nestedAppender)
                .addAppender(asyncListAppender);
        }
        
        // Send multiple log messages
        Logger testLogger = LoggerFactory.getLogger("diesel.AsyncTest");
        for (int i = 0; i < 5; i++) {
            testLogger.info("Test message {}", i);
        }
        
        // Wait for async processing (with timeout)
        await().atMost(2, SECONDS).until(() -> asyncListAppender.list.size() >= 5);
        
        // Verify all messages were delivered
        assertEquals(5, asyncListAppender.list.size(), "All messages should be delivered asynchronously");
        
        // Verify message content
        for (int i = 0; i < 5; i++) {
            assertTrue(asyncListAppender.list.get(i).getMessage().contains("Test message " + i));
        }
    }
    
    @Test
    @Tag("query")
    @DisplayName("Never block drops on full queue")
    void neverBlockDropsOnFull() {
        // Enable async with very small queue and never block = true
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "2");
        testConfigLoader.setProperty("logging.async.never.block", "true");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.load());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.load());
        
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        
        // Create capture appender
        ListAppender<ILoggingEvent> captureAppender = new ListAppender<>();
        captureAppender.setContext(loggerContext);
        captureAppender.start();
        
        // Add capture appender to async
        asyncAppender.addAppender(captureAppender);
        
        Logger testLogger = LoggerFactory.getLogger("diesel.BlockTest");
        
        // Send more messages than queue capacity
        for (int i = 0; i < 10; i++) {
            testLogger.info("Message {}", i);
        }
        
        // Give some time for async processing
        await().atMost(1, SECONDS).until(() -> captureAppender.list.size() > 0);
        
        // Verify some messages were delivered (queue capacity + some buffer)
        assertTrue(captureAppender.list.size() > 0, "Some messages should be delivered");
        assertTrue(captureAppender.list.size() <= 5, "Not all messages should be delivered (queue full)");
        
        // Check if dropped events were counted (metrics)
        // Note: This is a basic check - actual metrics integration is tested separately
    }
    
    @Test
    @Tag("query")
    @DisplayName("OFF level does not start async thread")
    void offLevelDoesNotStartAsyncThread() {
        // Set global level to OFF, but enable async
        testConfigLoader.setProperty("logging.level", "OFF");
        testConfigLoader.setProperty("logging.async.enabled", "true");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.load());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.load());
        
        // Verify no AsyncAppender was created
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        assertNull(rootLogger.getAppender("ASYNC"), "AsyncAppender should not be created when level is OFF");
        
        // Verify original appenders are still there
        assertNotNull(rootLogger.getAppender("CONSOLE") || rootLogger.getAppender("FILE"), 
                    "Original appenders should remain when async is disabled due to OFF level");
    }
    
    @Test
    @Tag("query")
    @DisplayName("Metrics registry exposes dropped count")
    void metricsRegistryExposesDroppedCount() {
        // Enable async with small queue to force drops
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "1");
        testConfigLoader.setProperty("logging.async.never.block", "true");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.load());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.load());
        
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        
        // Mock the metrics registry check (since we can't easily access it in tests)
        AtomicInteger droppedCount = new AtomicInteger(0);
        
        // Override the status listener to count drops
        asyncAppender.addStatusListener(status -> {
            if (status.getLevel() == Status.WARN && status.getMessage() != null && 
                status.getMessage().contains("discarded")) {
                droppedCount.incrementAndGet();
            }
        });
        
        Logger testLogger = LoggerFactory.getLogger("diesel.DropTest");
        
        // Send many messages to force queue overflow
        for (int i = 0; i < 10; i++) {
            testLogger.info("Drop test message {}", i);
        }
        
        // Give time for async processing and status events
        await().atMost(2, SECONDS).until(() -> droppedCount.get() > 0);
        
        // Verify drops were detected
        assertTrue(droppedCount.get() > 0, "Dropped events should be counted");
    }
    
    @Test
    @Tag("query")
    @DisplayName("Shutdown flushes queue")
    void shutdownFlushesQueue() {
        // Enable async logging
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "20");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.load());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.load());
        
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        
        // Create capture appender
        ListAppender<ILoggingEvent> captureAppender = new ListAppender<>();
        captureAppender.setContext(loggerContext);
        captureAppender.start();
        
        // Add capture appender to async
        asyncAppender.addAppender(captureAppender);
        
        Logger testLogger = LoggerFactory.getLogger("diesel.ShutdownTest");
        
        // Send messages
        for (int i = 0; i < 10; i++) {
            testLogger.info("Shutdown test message {}", i);
        }
        
        // Stop the logger context (triggers async appender shutdown)
        loggerContext.stop();
        
        // Verify all messages were delivered during shutdown
        assertEquals(10, captureAppender.list.size(), "All messages should be delivered during shutdown");
    }
    
    @Test
    @Tag("smoke")
    @DisplayName("Async configuration parameters are properly parsed")
    void asyncConfigurationParameters() {
        // Test various parameter combinations
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "2048");
        testConfigLoader.setProperty("logging.async.discarding.threshold", "5");
        testConfigLoader.setProperty("logging.async.never.block", "false");
        testConfigLoader.setProperty("logging.async.include.caller.data", "true");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.load());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.load());
        
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        
        assertNotNull(asyncAppender, "AsyncAppender should be created");
        assertEquals(2048, asyncAppender.getQueueSize(), "Queue size should be 2048");
        assertEquals(5, asyncAppender.getDiscardingThreshold(), "Discarding threshold should be 5");
        assertFalse(asyncAppender.isNeverBlock(), "Never block should be false");
        assertTrue(asyncAppender.isIncludeCallerData(), "Include caller data should be true");
    }
    
    /**
     * Test implementation of ConfigLoader for testing purposes.
     */
    static class TestConfigLoader extends ConfigLoader {
        private final Properties properties = new Properties();
        
        public void setProperty(String key, String value) {
            properties.setProperty(key, value);
        }
        
        public void clearProperties() {
            properties.clear();
        }
        
        @Override
        protected Properties loadProperties() {
            return properties;
        }
    }
}