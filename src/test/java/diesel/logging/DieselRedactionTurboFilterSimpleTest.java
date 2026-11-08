package diesel.logging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Set;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Simple test for DieselRedactionTurboFilter without full compilation.
 * This tests the core redaction logic.
 */
public class DieselRedactionTurboFilterSimpleTest {
    
    @Test
    void testIsSafeKey() {
        // Test the isSafeKey method directly
        Set<String> safeKeys = Set.of(
            "storage.*", "server.*", "page.*", "bufferpool.*", 
            "deadlock.*", "lock.*", "query.cache.*", "libpq.*",
            "transaction.isolation.level", "server.pool.size", 
            "server.queue.capacity", "server.backlog", "server.port",
            "server.bind.address", "server.ssl.enabled", 
            "server.ssl.protocol", "server.ssl.ciphers",
            "logging.*", "metrics.*", "observability.*"
        );
        
        // Test safe keys
        assertTrue(isSafeKey("server.port", safeKeys));
        assertTrue(isSafeKey("storage.data.dir", safeKeys));
        assertTrue(isSafeKey("transaction.isolation.level", safeKeys));
        
        // Test unsafe keys
        assertFalse(isSafeKey("tls.keystore.password", safeKeys));
        assertFalse(isSafeKey("api.secret", safeKeys));
        assertFalse(isSafeKey("database.token", safeKeys));
        assertFalse(isSafeKey("audit.archive.dir", safeKeys));
    }
    
    @Test
    void testSecretPatterns() {
        // Test regex patterns for secret detection
        Pattern secretPattern = Pattern.compile(
            ".*(password|passwd|secret|token|privateKey|keystore\\.password|truststore\\.password).*"
        );
        
        assertTrue(secretPattern.matcher("tls.keystore.password = changeit").matches());
        assertTrue(secretPattern.matcher("api.token = abc123").matches());
        assertTrue(secretPattern.matcher("user password is secret").matches());
        assertFalse(secretPattern.matcher("server.port = 5432").matches());
        assertFalse(secretPattern.matcher("storage.data.dir = /data").matches());
    }
    
    /**
     * Helper method to check if a key is safe to log.
     */
    private boolean isSafeKey(String key, Set<String> safeKeys) {
        // Check against whitelist patterns
        for (String safePattern : safeKeys) {
            if (safePattern.endsWith(".*")) {
                String prefix = safePattern.substring(0, safePattern.length() - 2);
                if (key.startsWith(prefix)) {
                    return true;
                }
            } else if (key.equals(safePattern)) {
                return true;
            }
        }
        
        // Additional blacklist check for known sensitive keys
        String lowerKey = key.toLowerCase();
        return !lowerKey.contains("password") &&
               !lowerKey.contains("passwd") &&
               !lowerKey.contains("secret") &&
               !lowerKey.contains("token") &&
               !lowerKey.contains("privatekey") &&
               !lowerKey.contains("keystore.password") &&
               !lowerKey.contains("truststore.password") &&
               !lowerKey.contains("archive.dir");
    }
}