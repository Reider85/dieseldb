package diesel.logging;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import diesel.ConfigLoader;

import java.util.Properties;

/**
 * Unit tests for DieselLogConfig functionality.
 */
@Tag("query")
class DieselLogConfigTest {
    
    private LoggerContext loggerContext;
    private ListAppender<ILoggingEvent> listAppender;
    private DieselLogConfigTest.TestConfigLoader testConfigLoader;
    
    @BeforeEach
    void setUp() {
        // Use a test-specific config loader
        testConfigLoader = new DieselLogConfigTest.TestConfigLoader();
        
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
    void globalLevelFromConfigProperties() {
        // Set config with DEBUG level
        testConfigLoader.setProperty("logging.level", "DEBUG");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        
        // Verify root logger level
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        assertEquals(Level.DEBUG, rootLogger.getLevel());
        
        // Verify diesel logger level  
        ch.qos.logback.classic.Logger dieselLogger = loggerContext.getLogger("diesel");
        assertEquals(Level.DEBUG, dieselLogger.getLevel());
    }
    
    @Test
    @Tag("smoke")
    void syspropOverridesConfig() {
        // Set system property with WARN level
        System.setProperty("logging.level", "WARN");
        testConfigLoader.setProperty("logging.level", "DEBUG");
        
        try {
            // Apply configuration
            DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
            
            // Verify root logger level (system property should win)
            ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
            assertEquals(Level.WARN, rootLogger.getLevel());
            
            // Verify diesel logger level
            ch.qos.logback.classic.Logger dieselLogger = loggerContext.getLogger("diesel");
            assertEquals(Level.WARN, dieselLogger.getLevel());
        } finally {
            // Clean up system property
            System.clearProperty("logging.level");
        }
    }
    
    @Test
    @Tag("smoke")
    void aliasAccepted() {
        // Set config with ERROR level using JUL alias
        testConfigLoader.setProperty("logging.level", "SEVERE");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        
        // Verify root logger level (should be ERROR, not SEVERE)
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        assertEquals(Level.ERROR, rootLogger.getLevel());
    }
    
    @Test
    @Tag("smoke")
    void julAliasesAccepted() {
        // Test various JUL aliases
        testConfigLoader.setProperty("logging.level", "FINE");
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        assertEquals(Level.DEBUG, loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).getLevel());
        
        testConfigLoader.setProperty("logging.level", "WARNING");
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        assertEquals(Level.WARN, loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).getLevel());
        
        testConfigLoader.setProperty("logging.level", "CONFIG");
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        assertEquals(Level.INFO, loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).getLevel());
    }
    
    @Test
    @Tag("smoke")
    void unknownValueDefaultsToInfoWithWarning() {
        // Set config with unknown level
        testConfigLoader.setProperty("logging.level", "NOPE");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        
        // Verify root logger level (should default to INFO)
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        assertEquals(Level.INFO, rootLogger.getLevel());
        
        // Verify warning was logged
        assertTrue(listAppender.list.stream()
            .anyMatch(event -> event.getLevel() == Level.WARN && 
                             event.getMessage().contains("Unknown logging level 'NOPE'")));
    }
    
    @Test
    @Tag("smoke")
    void deprecatedAliasEmitsWarning() {
        // Set system property with deprecated alias
        System.setProperty("logging.level.diesel", "DEBUG");
        testConfigLoader.setProperty("logging.level", "INFO");
        
        try {
            // Apply configuration
            DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
            
            // Verify warning was logged
            assertTrue(listAppender.list.stream()
                .anyMatch(event -> event.getLevel() == Level.WARN && 
                                 event.getMessage().contains("Deprecated sysprop -Dlogging.level.diesel used")));
        } finally {
            // Clean up system property
            System.clearProperty("logging.level.diesel");
        }
    }
    
    @Test
    @Tag("smoke")
    void existingLoggingLevelDieselSyspropStillWorks() {
        // Test that -Dlogging.level.diesel still works for backward compatibility
        System.setProperty("logging.level.diesel", "WARNING");
        testConfigLoader.setProperty("logging.level", "INFO");
        
        try {
            // Apply configuration
            DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
            
            // Verify root logger level (should be INFO from config, not WARNING from diesel alias)
            ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
            assertEquals(Level.INFO, rootLogger.getLevel());
            
            // Verify warning was logged about deprecated alias
            assertTrue(listAppender.list.stream()
                .anyMatch(event -> event.getLevel() == Level.WARN && 
                                 event.getMessage().contains("Deprecated sysprop -Dlogging.level.diesel used")));
        } finally {
            // Clean up system property
            System.clearProperty("logging.level.diesel");
        }
    }
    
    @Test
    @Tag("query")
    void moduleOverridesGlobalUpward() {
        // Set global level to INFO, module level to DEBUG
        testConfigLoader.setProperty("logging.level", "INFO");
        testConfigLoader.setProperty("logging.level.wal", "DEBUG");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.applyModuleLevels(testConfigLoader.loadProperties());
        
        // Verify global level is INFO
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        assertEquals(Level.INFO, rootLogger.getLevel());
        
        // Verify WAL module level is DEBUG (overrides global)
        ch.qos.logback.classic.Logger walLogger = loggerContext.getLogger("diesel.wal");
        assertEquals(Level.DEBUG, walLogger.getLevel());
        
        // Verify non-WAL loggers inherit global INFO level
        ch.qos.logback.classic.Logger serverLogger = loggerContext.getLogger("diesel.DatabaseServer");
        assertEquals(Level.INFO, serverLogger.getLevel());
    }
    
    @Test
    @Tag("query")
    void moduleOverridesGlobalDownward() {
        // Set global level to DEBUG, module level to WARN
        testConfigLoader.setProperty("logging.level", "DEBUG");
        testConfigLoader.setProperty("logging.level.wal", "WARN");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.applyModuleLevels(testConfigLoader.loadProperties());
        
        // Verify global level is DEBUG
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        assertEquals(Level.DEBUG, rootLogger.getLevel());
        
        // Verify WAL module level is WARN (overrides global downward)
        ch.qos.logback.classic.Logger walLogger = loggerContext.getLogger("diesel.wal");
        assertEquals(Level.WARN, walLogger.getLevel());
        
        // Verify non-WAL loggers inherit global DEBUG level
        ch.qos.logback.classic.Logger serverLogger = loggerContext.getLogger("diesel.DatabaseServer");
        assertEquals(Level.DEBUG, serverLogger.getLevel());
    }
    
    @Test
    @Tag("query")
    void moduleSyspropOverridesConfig() {
        // Set config wal level to INFO, system property wal level to DEBUG
        System.setProperty("logging.level.wal", "DEBUG");
        testConfigLoader.setProperty("logging.level", "INFO");
        testConfigLoader.setProperty("logging.level.wal", "INFO");
        
        try {
            // Apply configuration
            DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
            DieselLogConfig.applyModuleLevels(testConfigLoader.loadProperties());
            
            // Verify WAL module level is DEBUG (system property wins)
            ch.qos.logback.classic.Logger walLogger = loggerContext.getLogger("diesel.wal");
            assertEquals(Level.DEBUG, walLogger.getLevel());
        } finally {
            // Clean up system property
            System.clearProperty("logging.level.wal");
        }
    }
    
    @Test
    @Tag("query")
    void unknownModuleLogsWarningAndIgnored() {
        // Set config with unknown module
        testConfigLoader.setProperty("logging.level", "INFO");
        testConfigLoader.setProperty("logging.level.foobar", "DEBUG");
        
        // Apply configuration
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.applyModuleLevels(testConfigLoader.loadProperties());
        
        // Verify warning was logged
        assertTrue(listAppender.list.stream()
            .anyMatch(event -> event.getLevel() == Level.WARN && 
                             event.getMessage().contains("Unknown logging level 'foobar'")));
        
        // Verify global level is still applied
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        assertEquals(Level.INFO, rootLogger.getLevel());
    }
    
    @Test
    @Tag("query")
    void moduleAffectsLoggersCreatedAfterInit() {
        // Apply configuration with WAL module level DEBUG
        testConfigLoader.setProperty("logging.level", "INFO");
        testConfigLoader.setProperty("logging.level.wal", "DEBUG");
        
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.applyModuleLevels(testConfigLoader.loadProperties());
        
        // Create a new logger after configuration (should inherit from diesel.wal parent)
        ch.qos.logback.classic.Logger newWalLogger = loggerContext.getLogger("diesel.wal.NewClass");
        
        // Verify it inherits the DEBUG level from diesel.wal
        assertEquals(Level.DEBUG, newWalLogger.getLevel());
    }
    
    @Test
    @Tag("query")
    void reapplyChangesLevelAtRuntime() {
        // Initial configuration
        testConfigLoader.setProperty("logging.level", "INFO");
        testConfigLoader.setProperty("logging.level.wal", "DEBUG");
        
        DieselLogConfig.applyGlobalLevel(testConfigLoader.loadProperties());
        DieselLogConfig.applyModuleLevels(testConfigLoader.loadProperties());
        
        // Verify initial state
        ch.qos.logback.classic.Logger walLogger = loggerContext.getLogger("diesel.wal");
        assertEquals(Level.DEBUG, walLogger.getLevel());
        
        // Change configuration
        testConfigLoader.setProperty("logging.level.wal", "WARN");
        
        // Apply configuration directly using test config loader
        DieselLogConfig.applyModuleLevels(testConfigLoader.loadProperties());
        
        // Verify new state
        assertEquals(Level.WARN, walLogger.getLevel());
    }
    
    /**
     * Test implementation of ConfigLoader for testing purposes.
     */
    static class TestConfigLoader extends ConfigLoader {
        private final Properties properties = new Properties();
        
        public void setProperty(String key, String value) {
            properties.setProperty(key, value);
        }
        
        public Properties loadProperties() {
            return properties;
        }
    }
}