package diesel.logging;

import java.util.Map;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

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
     */
    public static void init() {
        Properties config = loadConfig();
        applyGlobalLevel(config);
        applyModuleLevels(config);
    }
    
    /**
     * Loads configuration using reflection to access package-private ConfigLoader.
     */
    private static Properties loadConfig() {
        try {
            // Use reflection to access the package-private ConfigLoader
            Class<?> configLoaderClass = Class.forName("diesel.ConfigLoader");
            java.lang.reflect.Method loadMethod = configLoaderClass.getMethod("load");
            return (Properties) loadMethod.invoke(null);
        } catch (Exception e) {
            LOGGER.warn("Failed to load config from ConfigLoader, using empty properties", e);
            return new Properties();
        }
    }
    
    /**
     * Applies global logging level from priority chain:
     * 1. System property -Dlogging.level=&lt;level&gt;
     * 2. config.properties logging.level=&lt;level&gt; 
     * 3. Default INFO (from logback.xml)
     */
    private static void applyGlobalLevel(Properties config) {
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
    private static void applyModuleLevels(Properties config) {
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
    }
}