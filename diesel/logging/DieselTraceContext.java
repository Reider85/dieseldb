package diesel.logging;

import org.slf4j.MDC;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Context management for trace correlation IDs in DieselDB logging.
 * 
 * <p>Provides centralized management of trace IDs using SLF4J MDC
 * (Mapped Diagnostic Context) to correlate log messages from the same
 * SQL request across different components.
 * 
 * <p>Trace IDs are automatically generated or can be provided from
 * external sources (e.g., libpq startup parameters).
 */
public class DieselTraceContext {
    
    /**
     * Gets the current trace ID from MDC.
     * 
     * @return current trace ID, or null if not set
     */
    public static String current() {
        return MDC.get("traceId");
    }
    
    /**
     * Sets the trace ID in MDC.
     * 
     * @param traceId the trace ID to set (can be null to clear)
     */
    public static void set(String traceId) {
        if (traceId == null || traceId.trim().isEmpty()) {
            clear();
        } else {
            MDC.put("traceId", traceId);
        }
    }
    
    /**
     * Clears the trace ID from MDC.
     * 
     * <p>Important: Call this in finally blocks when handling requests
     * to prevent thread pool contamination.
     */
    public static void clear() {
        MDC.remove("traceId");
        // Clear all MDC entries to prevent leakage
        MDC.clear();
    }
    
    /**
     * Generates a new trace ID.
     * 
     * <p>Uses a shortened UUID (8 characters) for compact logging.
     * 
     * @return generated trace ID
     */
    public static String generate() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Extracts trace ID from libpq startup parameters.
     * 
     * <p>Checks for 'trace_id' or 'ApplicationName' parameters from libpq.
     * 
     * @param libpqParams libpq startup parameters
     * @return trace ID from parameters, or null if not found
     */
    public static String extractFromLibpqParams(String libpqParams) {
        if (libpqParams == null || libpqParams.trim().isEmpty()) {
            return null;
        }
        
        // Parse parameters (format: "key1=value1,key2=value2,...")
        String[] params = libpqParams.split(",");
        for (String param : params) {
            String[] keyValue = param.split("=", 2);
            if (keyValue.length == 2) {
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                
                // Check for trace_id parameter
                if ("trace_id".equalsIgnoreCase(key)) {
                    return value;
                }
                
                // Check for ApplicationName parameter
                if ("ApplicationName".equalsIgnoreCase(key)) {
                    return value;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Ensures a trace ID is set, generating one if necessary.
     * 
     * @return current trace ID (existing or newly generated)
     */
    public static String ensureTraceId() {
        String current = current();
        if (current == null || current.trim().isEmpty()) {
            current = generate();
            set(current);
        }
        return current;
    }
    
    /**
     * Formats a trace ID for logging (adds brackets if present).
     * 
     * @param traceId the trace ID to format
     * @return formatted trace ID for logging
     */
    public static String formatForLogging(String traceId) {
        if (traceId == null || traceId.trim().isEmpty()) {
            return "";
        }
        return "[" + traceId + "]";
    }
}