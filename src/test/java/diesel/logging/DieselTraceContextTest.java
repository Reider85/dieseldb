package diesel.logging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for DieselTraceContext functionality.
 */
public class DieselTraceContextTest {
    
    @BeforeEach
    void setUp() {
        // Clear any existing MDC state
        DieselTraceContext.clear();
    }
    
    @AfterEach
    void tearDown() {
        // Clear MDC after each test
        DieselTraceContext.clear();
    }
    
    @Test
    void testGenerateTraceId() {
        String traceId = DieselTraceContext.generate();
        assertNotNull(traceId);
        assertEquals(8, traceId.length());
        assertTrue(traceId.matches("[a-f0-9]{8}"));
    }
    
    @Test
    void testSetAndGetTraceId() {
        String traceId = "test-1234";
        DieselTraceContext.set(traceId);
        
        assertEquals(traceId, DieselTraceContext.current());
    }
    
    @Test
    void testClearTraceId() {
        String traceId = "test-1234";
        DieselTraceContext.set(traceId);
        assertEquals(traceId, DieselTraceContext.current());
        
        DieselTraceContext.clear();
        assertNull(DieselTraceContext.current());
    }
    
    @Test
    void testEnsureTraceIdWhenNotSet() {
        String traceId = DieselTraceContext.ensureTraceId();
        assertNotNull(traceId);
        assertEquals(8, traceId.length());
        assertEquals(traceId, DieselTraceContext.current());
    }
    
    @Test
    void testEnsureTraceIdWhenAlreadySet() {
        String existingId = "existing-id";
        DieselTraceContext.set(existingId);
        
        String result = DieselTraceContext.ensureTraceId();
        assertEquals(existingId, result);
    }
    
    @Test
    void testExtractFromLibpqParams() {
        // Test with trace_id parameter
        String params1 = "trace_id=custom-trace-id,ApplicationName=MyApp,other=value";
        assertEquals("custom-trace-id", DieselTraceContext.extractFromLibpqParams(params1));
        
        // Test with ApplicationName parameter
        String params2 = "ApplicationName=app-name,trace_id=ignored";
        assertEquals("app-name", DieselTraceContext.extractFromLibpqParams(params2));
        
        // Test with no trace parameters
        String params3 = "param1=value1,param2=value2";
        assertNull(DieselTraceContext.extractFromLibpqParams(params3));
        
        // Test with empty parameters
        assertNull(DieselTraceContext.extractFromLibpqParams(null));
        assertNull(DieselTraceContext.extractFromLibpqParams(""));
    }
    
    @Test
    void testFormatForLogging() {
        assertEquals("", DieselTraceContext.formatForLogging(null));
        assertEquals("", DieselTraceContext.formatForLogging(""));
        assertEquals("[test-id]", DieselTraceContext.formatForLogging("test-id"));
        assertEquals("[12345678]", DieselTraceContext.formatForLogging("12345678"));
    }
    
    @Test
    void testSetNullTraceId() {
        DieselTraceContext.set("valid-id");
        assertEquals("valid-id", DieselTraceContext.current());
        
        DieselTraceContext.set(null);
        assertNull(DieselTraceContext.current());
        
        DieselTraceContext.set("");
        assertNull(DieselTraceContext.current());
    }
    
    @Test
    void testGenerateMultipleUniqueIds() {
        // Generate multiple IDs and verify they're unique
        String id1 = DieselTraceContext.generate();
        String id2 = DieselTraceContext.generate();
        String id3 = DieselTraceContext.generate();
        
        assertNotEquals(id1, id2);
        assertNotEquals(id2, id3);
        assertNotEquals(id1, id3);
        
        // Verify all are valid UUID prefixes
        assertTrue(id1.matches("[a-f0-9]{8}"));
        assertTrue(id2.matches("[a-f0-9]{8}"));
        assertTrue(id3.matches("[a-f0-9]{8}"));
    }
}