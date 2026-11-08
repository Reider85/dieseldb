package diesel.logging;

import ch.qos.logback.core.LayoutBase;
import ch.qos.logback.classic.spi.ILoggingEvent;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * JSON layout for structured logging in DieselDB.
 * 
 * <p>Formats log events as JSON objects with the following structure:
 * {
 *   "ts": "2025-12-12T16:33:21.123+03:00",
 *   "level": "INFO",
 *   "logger": "diesel.DatabaseServer",
 *   "thread": "main",
 *   "traceId": "abc123",
 *   "msg": "Configuration parameters loaded",
 *   "args": ["param1", "param2"]
 * }
 * 
 * <p>Supports JSON escaping for special characters and includes trace ID
 * when available in MDC.
 */
public class DieselJsonLayout extends LayoutBase<ILoggingEvent> {
    
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                       .withZone(ZoneId.systemDefault());
    
    private boolean includeCallerData = false;
    
    @Override
    public void start() {
        super.start();
    }
    
    @Override
    public void stop() {
        super.stop();
    }
    
    @Override
    public String doLayout(ILoggingEvent event) {
        Map<String, Object> jsonMap = new HashMap<>();
        
        // Add timestamp
        jsonMap.put("ts", formatTimestamp(event.getTimeStamp()));
        
        // Add level
        jsonMap.put("level", event.getLevel().toString());
        
        // Add logger name
        jsonMap.put("logger", event.getLoggerName());
        
        // Add thread name
        jsonMap.put("thread", event.getThreadName());
        
        // Add trace ID from MDC if available
        String traceId = event.getMDCPropertyMap().get("traceId");
        jsonMap.put("traceId", traceId != null ? traceId : "");
        
        // Add message
        jsonMap.put("msg", event.getFormattedMessage());
        
        // Add arguments if available
        Object[] argArray = event.getArgumentArray();
        if (argArray != null && argArray.length > 0) {
            jsonMap.put("args", escapeJsonArray(argArray));
        } else {
            jsonMap.put("args", new Object[0]);
        }
        
        // Include caller data if enabled
        if (includeCallerData && event.hasCallerData()) {
            Map<String, Object> callerData = new HashMap<>();
            callerData.put("className", event.getCallerData()[0].getClassName());
            callerData.put("methodName", event.getCallerData()[0].getMethodName());
            callerData.put("fileName", event.getCallerData()[0].getFileName());
            callerData.put("lineNumber", event.getCallerData()[0].getLineNumber());
            jsonMap.put("caller", callerData);
        }
        
        return toJsonString(jsonMap) + "\n";
    }
    
    /**
     * Formats timestamp to ISO-8601 format with timezone.
     */
    private String formatTimestamp(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return TIMESTAMP_FORMATTER.format(instant);
    }
    
    /**
     * Converts a map to JSON string with proper escaping.
     */
    private String toJsonString(Map<String, Object> map) {
        StringBuilder json = new StringBuilder("{");
        
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                json.append(",");
            }
            first = false;
            
            json.append("\"").append(escapeJsonString(entry.getKey())).append("\":");
            
            Object value = entry.getValue();
            if (value instanceof String) {
                json.append("\"").append(escapeJsonString((String) value)).append("\"");
            } else if (value instanceof Number) {
                json.append(value);
            } else if (value instanceof Boolean) {
                json.append(value);
            } else if (value instanceof Object[]) {
                json.append(escapeJsonArray((Object[]) value));
            } else {
                json.append("\"").append(escapeJsonString(value.toString())).append("\"");
            }
        }
        
        json.append("}");
        return json.toString();
    }
    
    /**
     * Escapes a string for JSON output.
     */
    private String escapeJsonString(String input) {
        if (input == null) {
            return "";
        }
        
        StringBuilder escaped = new StringBuilder();
        for (char c : input.toCharArray()) {
            switch (c) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (c <= '\u001F' || c >= '\u007F' && c <= '\u009F') {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
            }
        }
        return escaped.toString();
    }
    
    /**
     * Escapes an array for JSON output.
     */
    private String escapeJsonArray(Object[] array) {
        StringBuilder json = new StringBuilder("[");
        
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                json.append(",");
            }
            
            Object item = array[i];
            if (item instanceof String) {
                json.append("\"").append(escapeJsonString((String) item)).append("\"");
            } else if (item instanceof Number) {
                json.append(item);
            } else if (item instanceof Boolean) {
                json.append(item);
            } else {
                json.append("\"").append(escapeJsonString(item.toString())).append("\"");
            }
        }
        
        json.append("]");
        return json.toString();
    }
    
    /**
     * Sets whether to include caller data in the JSON output.
     */
    public void setIncludeCallerData(boolean includeCallerData) {
        this.includeCallerData = includeCallerData;
    }
}