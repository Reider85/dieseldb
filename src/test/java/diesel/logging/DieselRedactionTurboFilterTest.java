package diesel.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;
import org.slf4j.Marker;
import java.util.regex.Pattern;

/**
 * Tests for DieselRedactionTurboFilter functionality.
 */
@Tag("logging")
public class DieselRedactionTurboFilterTest {
    
    private LoggerContext loggerContext;
    private Logger logger;
    private DieselRedactionTurboFilter filter;
    
    @BeforeEach
    void setUp() {
        loggerContext = new LoggerContext();
        loggerContext.setName("test");
        logger = loggerContext.getLogger("test.logger");
        filter = new DieselRedactionTurboFilter();
        filter.setContext(loggerContext);
        filter.start();
    }
    
    @Test
    void testSecretRedactionForPassword() {
        // Test that password values are redacted
        FilterReply reply = filter.decide(null, logger, Level.INFO, "Config: tls.keystore.password = {}", new Object[]{"changeit"}, null);
        
        assertEquals(FilterReply.NEUTRAL, reply); // Message allowed but value should be redacted
        
        // Test direct password mention in message
        reply = filter.decide(null, logger, Level.INFO, "User password is {}", new Object[]{"secret123"}, null);
        
        assertEquals(FilterReply.DENY, reply); // Message denied entirely
    }
    
    @Test
    void testSecretRedactionForToken() {
        // Test that token values are redacted
FilterReply reply = filter.decide(null, logger, Level.INFO, "Config: api.token = {}", new Object[]{"abc123def"}, null);
        
        assertEquals(FilterReply.NEUTRAL, reply); // Message allowed but value should be redacted
        
        // Test direct token mention in message
reply = filter.decide(null, logger, Level.INFO, "Access token {}", new Object[]{"xyz789"}, null);
        
        assertEquals(FilterReply.DENY, reply); // Message denied entirely
    }
    
    @Test
    void testSafeConfigKeysAreLogged() {
        // Test that safe keys are allowed
FilterReply reply = filter.decide(null, logger, Level.INFO, "Config: server.port = {}", new Object[]{"5432"}, null);
        
        assertEquals(FilterReply.NEUTRAL, reply); // Message allowed
        
        // Test safe key with prefix
reply = filter.decide(null, logger, Level.INFO, "Config: storage.data.dir = {}", new Object[]{"/data"}, null);
        
        assertEquals(FilterReply.NEUTRAL, reply); // Message allowed
    }
    
    @Test
    void testKeystorePasswordRedaction() {
        // Test keystore password redaction
FilterReply reply = filter.decide(null, logger, Level.INFO, "Config: tls.keystore.password = {}", new Object[]{"mysecret"}, null);
        
        assertEquals(FilterReply.NEUTRAL, reply); // Message allowed but value redacted
    }
    
    @Test
    void testTruststorePasswordRedaction() {
        // Test truststore password redaction
        FilterReply reply = filter.decide(null, createLoggingEvent(Level.INFO, "Config: tls.truststore.password = anothersecret"), 
                                        "Config: tls.truststore.password = {}", new Object[]{"anothersecret"}, null);
        
        assertEquals(FilterReply.NEUTRAL, reply); // Message allowed but value redacted
    }
    
    @Test
    void testArchiveDirRedaction() {
        // Test archive directory redaction
FilterReply reply = filter.decide(null, logger, Level.INFO, "Config: audit.archive.dir = {}", new Object[]{"/secure/audit"}, null);
        
        assertEquals(FilterReply.DENY, reply); // Message denied entirely
    }
    
    @Test
    void testFilterCanBeDisabled() {
        // Test that filter can be disabled
        filter.setEnabled(false);
        
FilterReply reply = filter.decide(null, logger, Level.INFO, "Config: tls.keystore.password = {}", new Object[]{"changeme"}, null);
        
        assertEquals(FilterReply.NEUTRAL, reply); // Message allowed when disabled
    }
    
    @Test
    void testUnknownPatternsPassThrough() {
        // Test that unknown patterns pass through
FilterReply reply = filter.decide(null, logger, Level.INFO, "Config: server.name = {}", new Object[]{"production"}, null);
        
        assertEquals(FilterReply.NEUTRAL, reply); // Message allowed
    }
    
    /**
     * Helper method to create a logging event for testing.
     */
    private ILoggingEvent createLoggingEvent(Level level, String message) {
        // Create a simple logging event for testing
        return new ILoggingEvent() {
            @Override
            public String getFormattedMessage() {
                return message;
            }
            
            @Override
            public String getMessage() {
                return message;
            }
            
            @Override
            public Object[] getArgumentArray() {
                return new Object[0];
            }
            
            @Override
            public Throwable getThrowable() {
                return null;
            }
            
            @Override
            public LoggerContext getLoggerContext() {
                return loggerContext;
            }
            
            @Override
            public String getLoggerName() {
                return logger.getName();
            }
            
            @Override
            public Level getLevel() {
                return level;
            }
            
            @Override
            public Marker getMarker() {
                return null;
            }
            
            @Override
            public long getTimeStamp() {
                return System.currentTimeMillis();
            }
            
            @Override
            public void prepareForDeferredProcessing() {
            }
        };
    }
}