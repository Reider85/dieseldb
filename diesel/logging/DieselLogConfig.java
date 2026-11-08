package diesel.logging;

import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.status.StatusListener;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import diesel.logging.DieselRedactionTurboFilter;
import diesel.logging.DieselJsonLayout;
import diesel.logging.DieselSamplingTurboFilter;

/**
 * Centralized logging configuration manager for DieselDB.
 * 
 * <p>Provides unified control over logging levels through a priority chain:
 * system properties → config.properties → default INFO.
 * 
 * <p>Supports both global logging levels and per-module overrides with
 * priority over global levels.
 */
public class DieselLogConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DieselLogConfig.class);
    
    /**
     * Initializes the logging configuration.
     * 
     * <p>Called as the first line in DatabaseServer.start() after loadConfig().
     * Resolves logging levels from priority chain and applies them to logback.
     * Also configures async logging if enabled.
     */
    public static void init() {
        Properties config = loadConfig();
        applyGlobalLevel(config);
        applyModuleLevels(config);
        configureRedactionFilter(config);
        configureTraceContext(config);
        configureJsonFormat(config);
        configureSamplingFilter(config);
        configureAsyncLogging(config);
    }
    
    /**
     * Configures sampling filter if enabled in configuration.
     */
    private static void configureSamplingFilter(Properties config) {
        // Check if any sampling keys are present
        boolean hasSamplingKeys = false;
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("logging.sampling.")) {
                hasSamplingKeys = true;
                break;
            }
        }
        
        if (!hasSamplingKeys) {
            LOGGER.info("Sampling filter is not configured");
            return;
        }
        
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        DieselSamplingTurboFilter samplingFilter = new DieselSamplingTurboFilter();
        samplingFilter.setContext(loggerContext);
        
        // Convert Properties to Map for sampling filter
        Map<String, String> samplingConfig = new HashMap<>();
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("logging.sampling.")) {
                samplingConfig.put(key, config.getProperty(key));
            }
        }
        
        samplingFilter.updateSampleRates(samplingConfig);
        samplingFilter.start();
        
        // Add filter to logger context
        loggerContext.addTurboFilter(samplingFilter);
        LOGGER.info("Sampling filter enabled for {} modules", samplingConfig.size());
    }
    
    /**
     * Configures JSON logging format if enabled in configuration.
     */
    private static void configureJsonFormat(Properties config) {
        String format = config.getProperty("logging.format", "text");
        
        if (!"json".equalsIgnoreCase(format)) {
            LOGGER.info("Using text logging format (default)");
            return;
        }
        
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        DieselJsonLayout jsonLayout = new DieselJsonLayout();
        jsonLayout.setContext(loggerContext);
        jsonLayout.start();
        
        // Find and replace encoders in CONSOLE and FILE appenders
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        
        // Update CONSOLE appender
        ch.qos.logback.core.Appender<ILoggingEvent> consoleAppender = rootLogger.getAppender("CONSOLE");
        if (consoleAppender instanceof ch.qos.logback.core.ConsoleAppender) {
            ch.qos.logback.core.ConsoleAppender<ILoggingEvent> console = 
                (ch.qos.logback.core.ConsoleAppender<ILoggingEvent>) consoleAppender;
            console.setLayout(jsonLayout);
        }
        
        // Update FILE appender
        ch.qos.logback.core.Appender<ILoggingEvent> fileAppender = rootLogger.getAppender("FILE");
        if (fileAppender instanceof ch.qos.logback.core.rolling.RollingFileAppender) {
            ch.qos.logback.core.rolling.RollingFileAppender<ILoggingEvent> file = 
                (ch.qos.logback.core.rolling.RollingFileAppender<ILoggingEvent>) fileAppender;
            file.setLayout(jsonLayout);
        }
        
        LOGGER.info("JSON logging format enabled");
    }
    
    /**
     * Configures trace context if enabled in configuration.
     */
    private static void configureTraceContext(Properties config) {
        String enabledStr = config.getProperty("logging.trace.enabled", "false");
        boolean enabled = Boolean.parseBoolean(enabledStr);
        
        if (!enabled) {
            LOGGER.info("Trace context is disabled (default behavior)");
            return;
        }
        
        LOGGER.info("Trace context enabled for request correlation");
        // Trace context is used via DieselTraceContext.set() calls
        // No additional configuration needed at this level
    }
    
    /**
     * Configures redaction filter if enabled in configuration.
     */
    private static void configureRedactionFilter(Properties config) {
        String enabledStr = config.getProperty("logging.redaction.enabled", "true");
        boolean enabled = Boolean.parseBoolean(enabledStr);
        
        if (!enabled) {
            LOGGER.info("Redaction filter is disabled");
            return;
        }
        
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        DieselRedactionTurboFilter redactionFilter = new DieselRedactionTurboFilter();
        redactionFilter.setContext(loggerContext);
        redactionFilter.start();
        
        // Add filter to logger context
        loggerContext.addTurboFilter(redactionFilter);
        LOGGER.info("Redaction filter enabled for sensitive information");
    }
    
    /**
     * Loads configuration using reflection to access package-private ConfigLoader.
     */
    private static Properties loadConfig() {
        try {
            // Use reflection to access the package-private ConfigLoader
            Class<?> configLoaderClass = Class.forName("diesel.ConfigLoader");
            java.lang.reflect.Method loadMethod = configLoaderClass.getMethod("loadProperties");
            return (Properties) loadMethod.invoke(null);
        } catch (Exception e) {
            LOGGER.warn("Failed to load config from ConfigLoader, using empty properties", e);
            return new Properties();
        }
    }
    
    /**
     * Applies global logging level from priority chain:
     * 1. System property -Dlogging.level=<level>
     * 2. config.properties logging.level=<level> 
     * 3. Default INFO (from logback.xml)
     */
    public static void applyGlobalLevel(Properties config) {
        String resolvedLevel = resolveGlobalLevel(config);
        
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        
        // Apply to root logger
        loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.toLevel(resolvedLevel));
        
        // Apply to diesel package logger for inheritance
        loggerContext.getLogger("diesel").setLevel(Level.toLevel(resolvedLevel));
        
        LOGGER.info("Global logging level set to: {}", resolvedLevel);
    }
    
    /**
     * Resolves global logging level from priority chain.
     */
    private static String resolveGlobalLevel(Properties config) {
        // 1. Check system property first (highest priority)
        String syspropLevel = System.getProperty("logging.level");
        if (syspropLevel != null && !syspropLevel.trim().isEmpty()) {
            if (System.getProperty("logging.level.diesel") != null) {
                LOGGER.warn("Deprecated sysprop -Dlogging.level.diesel used; use -Dlogging.level instead");
            }
            return normalizeLevel(syspropLevel);
        }
        
        // 2. Check config.properties
        String configLevel = config.getProperty("logging.level");
        if (configLevel != null && !configLevel.trim().isEmpty()) {
            return normalizeLevel(configLevel);
        }
        
        // 3. Default to INFO (from logback.xml)
        return "INFO";
    }
    
    /**
     * Normalizes level string, converting JUL aliases to SLF4J levels.
     * Case-insensitive.
     */
    private static String normalizeLevel(String level) {
        if (level == null || level.trim().isEmpty()) {
            LOGGER.warn("Empty logging level, defaulting to INFO");
            return "INFO";
        }
        
        String upperLevel = level.toUpperCase().trim();
        
        // Convert JUL aliases to SLF4J levels
        switch (upperLevel) {
            case "SEVERE":
                return "ERROR";
            case "WARNING":
                return "WARN";
            case "CONFIG":
            case "INFO":
                return "INFO";
            case "FINE":
                return "DEBUG";
            case "FINER":
            case "FINEST":
                return "TRACE";
            case "OFF":
            case "ERROR":
            case "WARN":
            case "DEBUG":
            case "TRACE":
            case "ALL":
                return upperLevel;
            default:
                LOGGER.warn("Unknown logging level '{}', defaulting to INFO", level);
                return "INFO";
        }
    }
    
    /**
     * Applies per-module logging levels with priority over global level.
     * Called after applyGlobalLevel() so module levels can override.
     */
    public static void applyModuleLevels(Properties config) {
        // Module mapping - can be extended as needed
        Map<String, List<String>> moduleMapping = Map.ofEntries(
            Map.entry("parser", List.of("diesel.QueryParser", "diesel.SelectQueryParser", "diesel.SubqueryParser", 
                                      "diesel.ConditionParser", "diesel.ParseContext", "diesel.SqlParsingUtils")),
            Map.entry("query", List.of("diesel.SelectQuery", "diesel.InsertQuery", "diesel.UpdateQuery", 
                                    "diesel.DeleteQuery", "diesel.QueryExecutor", "diesel.QueryOptimizer", 
                                    "diesel.PreparedStatement")),
            Map.entry("wal", List.of("diesel.wal")),
            Map.entry("recovery", List.of("diesel.RecoveryManager", "diesel.recovery", "diesel.DatabaseRecoverySink")),
            Map.entry("replication", List.of("diesel.ReplicationCoordinator", "diesel.SyncReplicationCoordinator", 
                                          "diesel.replication")),
            Map.entry("storage", List.of("diesel.storage")),
            Map.entry("bufferpool", List.of("diesel.storage.page.BufferPool", "diesel.storage.page.BufferPoolFlusher", 
                                          "diesel.storage.page.PageFlusher", "diesel.PersistFlusher")),
            Map.entry("net", List.of("diesel.net", "diesel.LegacyProtocolAdapter", "diesel.LibpqClientHandler")),
            Map.entry("security", List.of("diesel.security")),
            Map.entry("concurrency", List.of("diesel.concurrency")),
            Map.entry("backup", List.of("diesel.backup", "diesel.DatabasePitrSink")),
            Map.entry("observability", List.of("diesel.observability")),
            Map.entry("schema", List.of("diesel.schema", "diesel.AlterTableCopyAlgorithm", 
                                      "diesel.AlterTableAddColumnQuery", "diesel.AlterTableDropColumnQuery")),
            Map.entry("server", List.of("diesel.DatabaseServer", "diesel.DieselDatabase", "diesel.Database"))
        );
        
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        
        // Apply module levels
        for (var entry : moduleMapping.entrySet()) {
            String module = entry.getKey();
            List<String> loggers = entry.getValue();
            
            // Check system property first (highest priority)
            String moduleLevel = System.getProperty("logging.level." + module);
            if (moduleLevel == null || moduleLevel.trim().isEmpty()) {
                // Check config.properties
                moduleLevel = config.getProperty("logging.level." + module);
            }
            
            if (moduleLevel != null && !moduleLevel.trim().isEmpty()) {
                String normalizedLevel = normalizeLevel(moduleLevel);
                
                // Apply to all loggers in this module
                for (String loggerName : loggers) {
                    ch.qos.logback.classic.Logger logger = loggerContext.getLogger(loggerName);
                    Level level = Level.toLevel(normalizedLevel);
                    logger.setLevel(level);
                    
                    // Set parent level for inheritance (e.g., diesel.wal for future loggers)
                    int lastDot = loggerName.lastIndexOf('.');
                    if (lastDot > 0) {
                        String parentName = loggerName.substring(0, lastDot);
                        ch.qos.logback.classic.Logger parent = loggerContext.getLogger(parentName);
                        if (parent.getLevel() == null) {
                            parent.setLevel(level);
                        }
                    }
                }
                
                LOGGER.info("Module '{}' logging level set to: {}", module, normalizedLevel);
            }
        }
    }
    
    /**
     * Reapplies logging configuration at runtime.
     * Can be called via admin endpoints or JMX for dynamic configuration changes.
     */
    public static void reapply() {
        LOGGER.info("Reapplying logging configuration");
        Properties config = loadConfig();
        applyGlobalLevel(config);
        applyModuleLevels(config);
        configureAsyncLogging(config);
    }
    
    /**
     * Configures async logging if enabled in configuration.
     * 
     * <p>When enabled, wraps CONSOLE and FILE appenders from logback.xml
     * into a single AsyncAppender drained by a daemon 'logback-async' thread.
     */
    public static void configureAsyncLogging(Properties config) {
        // Check if async logging is enabled
        String enabledStr = config.getProperty("logging.async.enabled", "false");
        boolean enabled = Boolean.parseBoolean(enabledStr);
        
        if (!enabled) {
            LOGGER.info("Async logging is disabled (default behavior)");
            return;
        }
        
        // Check global level - if OFF, don't start async
        String globalLevel = resolveGlobalLevel(config);
        if ("OFF".equalsIgnoreCase(globalLevel)) {
            LOGGER.info("Async logging disabled: global logging level is OFF");
            return;
        }
        
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        
        try {
            // Get existing appenders
            ConsoleAppender<ILoggingEvent> consoleAppender = 
                (ConsoleAppender<ILoggingEvent>) rootLogger.getAppender("CONSOLE");
            RollingFileAppender<ILoggingEvent> fileAppender = 
                (RollingFileAppender<ILoggingEvent>) rootLogger.getAppender("FILE");
            
            if (consoleAppender == null && fileAppender == null) {
                LOGGER.warn("No CONSOLE or FILE appenders found, skipping async configuration");
                return;
            }
            
            // Create AsyncAppender
            AsyncAppender asyncAppender = new AsyncAppender();
            asyncAppender.setContext(loggerContext);
            asyncAppender.setName("ASYNC");
            
            // Configure async parameters from config
            String queueSizeStr = config.getProperty("logging.async.queue.size", "4096");
            int queueSize = Integer.parseInt(queueSizeStr);
            asyncAppender.setQueueSize(queueSize);
            
            String discardingThresholdStr = config.getProperty("logging.async.discarding.threshold", "0");
            int discardingThreshold = Integer.parseInt(discardingThresholdStr);
            asyncAppender.setDiscardingThreshold(discardingThreshold);
            
            String neverBlockStr = config.getProperty("logging.async.never.block", "true");
            boolean neverBlock = Boolean.parseBoolean(neverBlockStr);
            asyncAppender.setNeverBlock(neverBlock);
            
            String includeCallerDataStr = config.getProperty("logging.async.include.caller.data", "false");
            boolean includeCallerData = Boolean.parseBoolean(includeCallerDataStr);
            asyncAppender.setIncludeCallerData(includeCallerData);
            
            // Add existing appenders to async appender
            if (consoleAppender != null) {
                asyncAppender.addAppender(consoleAppender);
            }
            if (fileAppender != null) {
                asyncAppender.addAppender(fileAppender);
            }
            
            // Note: Status listener tracking will be added when MetricsRegistry is available
            
            // Start async appender
            asyncAppender.start();
            
            // Replace root appenders with async appender
            rootLogger.detachAndStopAllAppenders();
            if (consoleAppender != null) {
                rootLogger.detachAppender("CONSOLE");
            }
            if (fileAppender != null) {
                rootLogger.detachAppender("FILE");
            }
            rootLogger.addAppender(asyncAppender);
            
            LOGGER.info("Async logging enabled with queueSize={}, discardingThreshold={}, neverBlock={}", 
                       queueSize, discardingThreshold, neverBlock);
            
        } catch (Exception e) {
            LOGGER.error("Failed to configure async logging", e);
        }
    }
    
    /**
     * Status listener for AsyncAppender to track dropped events.
     */
    private static class AsyncAppenderStatusListener implements StatusListener {
        @Override
        public void addStatusEvent(Status status) {
            if (status.getLevel() == Status.WARN && 
                status.getMessage() != null && 
                status.getMessage().contains("discarded")) {
                
                // Extract dropped count from message
                String message = status.getMessage();
                try {
                    // Example message: "AsyncAppender discarded 15 events"
                    if (message.contains("discarded")) {
                        String[] parts = message.split(" ");
                        for (int i = 0; i < parts.length - 1; i++) {
                            if (parts[i].equals("discarded") && i + 1 < parts.length) {
                                int dropped = Integer.parseInt(parts[i + 1]);
                                // TODO: Integrate with MetricsRegistry when available
                                // For now, just log the dropped count
                                LOGGER.debug("AsyncAppender dropped {} events", dropped);
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to parse dropped events from status: {}", message, e);
                }
            }
        }
        
        /**
         * Converts a Properties map to Map<String, String> for compatibility.
         */
        private Map<String, String> convertToStringMap(Properties props) {
            Map<String, String> result = new HashMap<>();
            for (String key : props.stringPropertyNames()) {
                result.put(key, props.getProperty(key));
            }
            return result;
        }
        
    }
}