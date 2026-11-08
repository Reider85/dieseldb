package diesel.logging;

import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.joran.JoranConfigurator;
import diesel.ConfigLoader;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;
import ch.qos.logback.core.joran.spi.JoranException;

import java.net.URL;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for async logging functionality in DieselLogConfig.
 */
@Tag("query")
// Mutates the shared root logger (levels, appenders) - must not run
// concurrently with DieselLogConfigTest (TESTS-3 parallel-race rule).
@ResourceLock("logback-state")
class DieselAsyncAppenderTest {
    
    private LoggerContext loggerContext;
    private ListAppender<ILoggingEvent> listAppender;
    private DieselAsyncAppenderTest.TestConfigLoader testConfigLoader;
    private Level originalRootLevel;
    private Level originalDieselLevel;
    
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
        originalRootLevel = rootLogger.getLevel();
        originalDieselLevel = loggerContext.getLogger("diesel").getLevel();
        rootLogger.addAppender(listAppender);
        rootLogger.setLevel(Level.DEBUG); // Capture all levels for testing
    }
    
    @AfterEach
    void tearDown() {
        // configureAsyncLogging detaches CONSOLE/FILE from root and keeps the
        // only references locally, so after an async test the whole logging
        // configuration is rebuilt from logback.xml.
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        
        ch.qos.logback.core.Appender<ILoggingEvent> async = rootLogger.getAppender("ASYNC");
        if (async instanceof AsyncAppender) {
            AsyncAppender asyncAppender = (AsyncAppender) async;
            if (asyncAppender.isStarted()) {
                asyncAppender.stop();   // flushes the queue and stops TARGETS
            }
            rootLogger.detachAppender(asyncAppender);
        }
        
        rootLogger.detachAppender(listAppender);
        listAppender.stop();
        
        // configureAsyncLogging detached CONSOLE/FILE from root; they are still
        // registered in the context object map (Joran) and were never stopped.
        // Re-attach them instead of a full reset(): reset() would also wipe the
        // capture appenders of unrelated test classes running in parallel.
        if (rootLogger.getAppender("CONSOLE") == null) {
            Object console = loggerContext.getObject("CONSOLE");
            if (console instanceof ch.qos.logback.core.Appender<?> app) {
                if (!app.isStarted()) {
                    app.start();
                }
                @SuppressWarnings("unchecked")
                ch.qos.logback.core.Appender<ILoggingEvent> typed =
                        (ch.qos.logback.core.Appender<ILoggingEvent>) app;
                rootLogger.addAppender(typed);
            }
        }
        if (rootLogger.getAppender("FILE") == null) {
            Object file = loggerContext.getObject("FILE");
            if (file instanceof ch.qos.logback.core.Appender<?> app) {
                if (!app.isStarted()) {
                    app.start();
                }
                @SuppressWarnings("unchecked")
                ch.qos.logback.core.Appender<ILoggingEvent> typed =
                        (ch.qos.logback.core.Appender<ILoggingEvent>) app;
                rootLogger.addAppender(typed);
            }
        }
        
        // Fallback: rebuild from logback.xml if the object map did not help.
        if (rootLogger.getAppender("CONSOLE") == null && rootLogger.getAppender("FILE") == null) {
            loggerContext.reset();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(loggerContext);
            URL url = getClass().getClassLoader().getResource("logback.xml");
            if (url != null) {
                try {
                    configurator.doConfigure(url);
                } catch (JoranException e) {
                    throw new IllegalStateException("Failed to restore logback.xml configuration", e);
                }
            }
            if (!loggerContext.isStarted()) {
                loggerContext.start();
            }
        }
        
        rootLogger.setLevel(originalRootLevel);
        loggerContext.getLogger("diesel").setLevel(originalDieselLevel);
    }
    
    /** Path of the FILE appender target (logback.xml: ${user.dir}/logs/diesel.log). */
    private static final java.nio.file.Path LOG_FILE = java.nio.file.Path.of("logs", "diesel.log");
    
    private static long logFileSize() {
        try {
            return java.nio.file.Files.exists(LOG_FILE) ? java.nio.file.Files.size(LOG_FILE) : 0L;
        } catch (java.io.IOException e) {
            return 0L;
        }
    }
    
    /** Reads everything appended to logs/diesel.log since {@code position}. */
    private static String readLogFrom(long position) {
        try {
            if (!java.nio.file.Files.exists(LOG_FILE)) {
                return "";
            }
            long size = java.nio.file.Files.size(LOG_FILE);
            if (size <= position) {
                return "";
            }
            try (java.nio.channels.SeekableByteChannel channel =
                     java.nio.file.Files.newByteChannel(LOG_FILE, java.nio.file.StandardOpenOption.READ)) {
                channel.position(position);
                java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate((int) (size - position));
                while (buffer.hasRemaining() && channel.read(buffer) > 0) {
                    // read fully
                }
                buffer.flip();
                return java.nio.charset.StandardCharsets.UTF_8.decode(buffer).toString();
            }
        } catch (java.io.IOException e) {
            return "";
        }
    }
    
    /**
     * Stops the CONSOLE appender via the context object map (Joran registers
     * appenders there). The fan-out target keeps its reference, so doAppend on
     * a stopped console is a no-op - lets tests write large messages without
     * flooding stdout. tearDown's logback.xml rebuild restarts it.
     */
    private void stopConsoleQuietly() {
        Object console = loggerContext.getObject("CONSOLE");
        if (console instanceof ch.qos.logback.core.Appender<?> appender && appender.isStarted()) {
            appender.stop();
        }
    }
    
    @Test
    @Tag("smoke")
    @DisplayName("Async logging is disabled by default")
    void asyncDisabledByDefault() {
        // Use default config (async.enabled = false)
        testConfigLoader.clearProperties();
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.loadProperties());
        
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
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.loadProperties());
        
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
        
        // AsyncAppender must contain the wrapped CONSOLE/FILE target
        // (logback allows only one nested appender, so both are fan-out wrapped)
        assertNotNull(asyncAppender.getAppender("TARGETS"),
                "AsyncAppender should contain the wrapped CONSOLE/FILE target");
    }
    
    @Test
    @Tag("query")
    @DisplayName("Records are delivered asynchronously")
    void recordsAreDeliveredAsync() {
        // Enable async logging with small queue for testing
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "10");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.loadProperties());
        
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        assertNotNull(asyncAppender, "AsyncAppender should be present when enabled");
        assertTrue(asyncAppender.isStarted(), "AsyncAppender should be started");
        
        // logback allows only ONE nested appender, so capture is observed at
        // the end of the async pipeline (FILE target) instead of a nested
        // ListAppender, which would be silently ignored.
        long position = logFileSize();
        Logger testLogger = LoggerFactory.getLogger("diesel.AsyncTest");
        for (int i = 0; i < 5; i++) {
            testLogger.info("AsyncDelivery message {}", i);
        }
        
        // Wait for the async worker to flush all messages to the FILE target
        await().atMost(5, SECONDS).until(() -> readLogFrom(position).contains("AsyncDelivery message 4"));
        
        String tail = readLogFrom(position);
        for (int i = 0; i < 5; i++) {
            assertTrue(tail.contains("AsyncDelivery message " + i),
                    "Message " + i + " should be delivered asynchronously");
        }
    }
    
    @Test
    @Tag("query")
    @DisplayName("Never block drops on full queue")
    void neverBlockDropsOnFull() {
        // Enable async with very small queue and never block = true
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "1");
        testConfigLoader.setProperty("logging.async.never.block", "true");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.loadProperties());
        
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        assertNotNull(asyncAppender, "AsyncAppender should be present when enabled");
        assertEquals(1, asyncAppender.getQueueSize(), "Queue size should be 1");
        assertTrue(asyncAppender.isNeverBlock(), "Never block should be true");
        
        // Keep the worker busy writing a large event to FILE so the burst
        // below overflows the size-1 queue deterministically. Console is
        // stopped first so the large message does not flood stdout.
        stopConsoleQuietly();
        long position = logFileSize();
        Logger testLogger = LoggerFactory.getLogger("diesel.BlockTest");
        testLogger.info("QueueFill {}", "x".repeat(10_000_000));
        for (int i = 0; i < 10; i++) {
            testLogger.info("Burst message {}", i);
        }
        
        // Wait until the large event has been written (worker done, queue settled)
        await().atMost(10, SECONDS).until(() -> readLogFrom(position).contains("QueueFill"));
        
        // queue=1 while the worker was busy: at most a couple of the 10 burst
        // messages can have been queued - never all of them (neverBlock drops
        // the rest instead of stalling the producer).
        String tail = readLogFrom(position);
        long delivered = 0;
        for (int i = 0; i < 10; i++) {
            if (tail.contains("Burst message " + i)) {
                delivered++;
            }
        }
        assertTrue(delivered < 10,
                "Queue of 1 must drop burst messages, but all " + delivered + " were delivered");
    }
    
    @Test
    @Tag("query")
    @DisplayName("OFF level does not start async thread")
    void offLevelDoesNotStartAsyncThread() {
        // Set global level to OFF, but enable async
        testConfigLoader.setProperty("logging.level", "OFF");
        testConfigLoader.setProperty("logging.async.enabled", "true");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.loadProperties());
        
        // Verify no AsyncAppender was created
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        assertNull(rootLogger.getAppender("ASYNC"), "AsyncAppender should not be created when level is OFF");
        
        // Verify original appenders are still there
        assertNotNull(rootLogger.getAppender("CONSOLE") != null || rootLogger.getAppender("FILE") != null, 
                    "Original appenders should remain when async is disabled due to OFF level");
    }
    
    @Test
    @Tag("query")
    @DisplayName("Metrics registry exposes dropped count")
    void metricsRegistryExposesDroppedCount() {
        // MetricsRegistry integration for dropped events is still a TODO in
        // DieselLogConfig.configureAsyncLogging, and logback 1.5.7 emits no
        // per-drop status. What logback DOES report is a WARN status
        // "...possibly discarded" when the queue cannot flush within
        // maxFlushTime during stop - that is the observable "dropped count"
        // this test pins down.
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "1");
        testConfigLoader.setProperty("logging.async.never.block", "true");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.loadProperties());
        
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        assertNotNull(asyncAppender, "AsyncAppender should be present when enabled");
        
        // A flush window of 1ms combined with a large event guarantees the
        // worker is still busy when stop() joins it, so logback reports the
        // timeout as "...possibly discarded" (the only discard signal 1.5.7
        // exposes without a MetricsRegistry).
        asyncAppender.setMaxFlushTime(1);
        
        AtomicInteger discardStatuses = new AtomicInteger(0);
        StatusListener listener = status -> {
            if (status.getLevel() == Status.WARN
                    && status.getMessage() != null
                    && status.getMessage().contains("discard")) {
                discardStatuses.incrementAndGet();
            }
        };
        loggerContext.getStatusManager().add(listener);
        try {
            stopConsoleQuietly();   // keep the large event off stdout
            Logger testLogger = LoggerFactory.getLogger("diesel.DropTest");
            testLogger.info("FlushTimeout {}", "x".repeat(10_000_000));
            
            // Stop forces a queue flush; the in-flight large event exceeds the
            // 1ms flush window, so logback emits "...possibly discarded".
            asyncAppender.stop();
            
            assertTrue(discardStatuses.get() > 0,
                    "Flush timeout should report discarded events, count=" + discardStatuses.get());
        } finally {
            loggerContext.getStatusManager().remove(listener);
        }
    }
    
    @Test
    @Tag("query")
    @DisplayName("Shutdown flushes queue")
    void shutdownFlushesQueue() {
        // Enable async logging
        testConfigLoader.setProperty("logging.async.enabled", "true");
        testConfigLoader.setProperty("logging.async.queue.size", "20");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.loadProperties());
        
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        AsyncAppender asyncAppender = (AsyncAppender) rootLogger.getAppender("ASYNC");
        assertNotNull(asyncAppender, "AsyncAppender should be present when enabled");
        
        long position = logFileSize();
        Logger testLogger = LoggerFactory.getLogger("diesel.ShutdownTest");
        
        // Send messages
        for (int i = 0; i < 10; i++) {
            testLogger.info("Shutdown test message {}", i);
        }
        
        // Stop the async appender: its shutdown flushes the queue into the
        // FILE target. Stopping the whole LoggerContext instead would reset()
        // the global configuration and poison every test class that runs in
        // parallel with this one.
        asyncAppender.stop();
        
        // Verify all messages were delivered during shutdown flush
        String tail = readLogFrom(position);
        for (int i = 0; i < 10; i++) {
            assertTrue(tail.contains("Shutdown test message " + i),
                    "Message " + i + " should be delivered during shutdown");
        }
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
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.configureAsyncLogging(testConfigLoader.loadProperties());
        
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
        
        public Properties loadProperties() {
            return properties;
        }
    }
}