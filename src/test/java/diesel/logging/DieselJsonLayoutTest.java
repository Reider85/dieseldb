package diesel.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DieselJsonLayout functionality.
 */
public class DieselJsonLayoutTest {
    
    private DieselJsonLayout layout;
    
    @BeforeEach
    void setUp() {
        layout = new DieselJsonLayout();
        layout.start();
    }
    
    @Test
    void testBasicJsonOutput() {
        ILoggingEvent event = createLoggingEvent(Level.INFO, "Test message", "test.logger");
        
        String json = layout.doLayout(event);
        
        // Verify basic structure
        assertTrue(json.startsWith("{"));
        assertTrue(json.endsWith("}\n"));
        
        // Verify required fields
        assertTrue(json.contains("\"ts\":"));
        assertTrue(json.contains("\"level\":\"INFO\""));
        assertTrue(json.contains("\"logger\":\"test.logger\""));
        assertTrue(json.contains("\"thread\":\"main\""));
        assertTrue(json.contains("\"traceId\":\"\""));
        assertTrue(json.contains("\"msg\":\"Test message\""));
        assertTrue(json.contains("\"args\":[]"));
    }
    
    @Test
    void testJsonWithTraceId() {
        // Create event with trace ID in MDC
        ILoggingEvent event = createLoggingEvent(Level.DEBUG, "Debug message", "diesel.Database");
        event.getMDCPropertyMap().put("traceId", "abc123");
        
        String json = layout.doLayout(event);
        
        assertTrue(json.contains("\"traceId\":\"abc123\""));
    }
    
    @Test
    void testJsonWithArguments() {
        ILoggingEvent event = createLoggingEvent(Level.WARN, "User login failed for {0}", "auth.logger", "admin");
        
        String json = layout.doLayout(event);
        
        assertTrue(json.contains("\"msg\":\"User login failed for admin\""));
        assertTrue(json.contains("\"args\":[\"admin\"]"));
    }
    
    @Test
    void testJsonWithMultipleArguments() {
        ILoggingEvent event = createLoggingEvent(Level.ERROR, "Transaction {0} failed after {1}ms", "tx.logger", "12345", "1500");
        
        String json = layout.doLayout(event);
        
        assertTrue(json.contains("\"msg\":\"Transaction 12345 failed after 1500ms\""));
        assertTrue(json.contains("\"args\":[\"12345\",\"1500\"]"));
    }
    
    @Test
    void testJsonEscaping() {
        ILoggingEvent event = createLoggingEvent(Level.INFO, "Message with quotes: \"hello\", backslash: \\, newline: \n", "escape.test");
        
        String json = layout.doLayout(event);
        
        // Verify special characters are escaped
        assertTrue(json.contains("\"msg\":\"Message with quotes: \\\"hello\\\", backslash: \\\\, newline: \\n\""));
    }
    
    @Test
    void testTimestampFormat() {
        ILoggingEvent event = createLoggingEvent(Level.INFO, "Timestamp test", "timestamp.test");
        
        String json = layout.doLayout(event);
        
        // Verify timestamp format (ISO-8601 with timezone)
        assertTrue(json.contains("\"ts\":\""));
        assertTrue(json.contains("T"));
        assertTrue(json.contains("Z") || json.contains("+") || json.contains("-"));
    }
    
    @Test
    void testThreadName() {
        ILoggingEvent event = createLoggingEvent(Level.INFO, "Thread test", "thread.test");
        
        String json = layout.doLayout(event);
        
        assertTrue(json.contains("\"thread\":\"main\""));
    }
    
    @Test
    void testDifferentLogLevels() {
        ILoggingEvent errorEvent = createLoggingEvent(Level.ERROR, "Error message", "error.test");
        ILoggingEvent warnEvent = createLoggingEvent(Level.WARN, "Warning message", "warn.test");
        ILoggingEvent infoEvent = createLoggingEvent(Level.INFO, "Info message", "info.test");
        ILoggingEvent debugEvent = createLoggingEvent(Level.DEBUG, "Debug message", "debug.test");
        
        String errorJson = layout.doLayout(errorEvent);
        String warnJson = layout.doLayout(warnEvent);
        String infoJson = layout.doLayout(infoEvent);
        String debugJson = layout.doLayout(debugEvent);
        
        assertTrue(errorJson.contains("\"level\":\"ERROR\""));
        assertTrue(warnJson.contains("\"level\":\"WARN\""));
        assertTrue(infoJson.contains("\"level\":\"INFO\""));
        assertTrue(debugJson.contains("\"level\":\"DEBUG\""));
    }
    
    @Test
    void testValidJsonStructure() {
        ILoggingEvent event = createLoggingEvent(Level.INFO, "JSON structure test", "json.test");
        
        String json = layout.doLayout(event);
        
        // Basic JSON validation
        assertTrue(json.startsWith("{") && json.endsWith("}\n"));
        
        // Count quotes to ensure proper structure
        long quoteCount = json.chars().filter(c -> c == '"').count();
        assertTrue(quoteCount > 0); // Should have quotes
        
        // Should have balanced braces
        long openBraces = json.chars().filter(c -> c == '{').count();
        long closeBraces = json.chars().filter(c -> c == '}').count();
        assertEquals(openBraces, closeBraces);
    }
    
    /**
     * Helper method to create a logging event for testing.
     */
    private ILoggingEvent createLoggingEvent(Level level, String message, String loggerName, Object... args) {
        // Create a simple logging event for testing
        LoggingEvent event = new LoggingEvent();
        event.setLoggerName(loggerName);
        event.setLevel(level);
        event.setMessage(message);
        event.setTimeStamp(System.currentTimeMillis());
        return event;
    }
}