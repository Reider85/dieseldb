package diesel.net.libpq;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SCRAM-SHA-256 authentication mechanism.
 */
public class ScramSha256Test {
    
    @Test
    void testGenerateNonce() {
        String nonce1 = ScramSha256.generateNonce();
        String nonce2 = ScramSha256.generateNonce();
        
        // Nonce should be 16 bytes represented as base64
        assertEquals(22, nonce1.length()); // 16 bytes = 22 base64 chars with padding
        assertEquals(22, nonce2.length());
        
        // Nonces should be different
        assertNotEquals(nonce1, nonce2);
        
        // Should contain only valid base64 characters
        assertTrue(nonce1.matches("^[A-Za-z0-9+/=]+$"));
        assertTrue(nonce2.matches("^[A-Za-z0-9+/=]+$"));
    }
    
    @Test
    void testGenerateSalt() {
        byte[] salt1 = ScramSha256.generateSalt();
        byte[] salt2 = ScramSha256.generateSalt();
        
        // Salt should be 16 bytes
        assertEquals(16, salt1.length);
        assertEquals(16, salt2.length);
        
        // Salts should be different
        assertNotEquals(salt1, salt2);
    }
    
    @Test
    void testHashPassword() {
        String password = "testpassword123";
        byte[] salt = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        
        String hash1 = ScramSha256.hashPassword(password, salt);
        String hash2 = ScramSha256.hashPassword(password, salt);
        
        // Same password and salt should produce same hash
        assertEquals(hash1, hash2);
        
        // Hash should be base64 encoded
        assertTrue(hash1.matches("^[A-Za-z0-9+/=]+$"));
        
        // Different password should produce different hash
        String hash3 = ScramSha256.hashPassword("differentpassword", salt);
        assertNotEquals(hash1, hash3);
    }
    
    @Test
    void testCreateClientFirstMessage() {
        String username = "testuser";
        String nonce = "rOprNGfwEbeRWgbNEkqO";
        
        String clientFirstMessage = ScramSha256.createClientFirstMessage(username, nonce);
        
        // Should contain gs2-header, authzid, username, and nonce
        assertTrue(clientFirstMessage.startsWith("n,"));
        assertTrue(clientFirstMessage.contains("n=testuser,"));
        assertTrue(clientFirstMessage.contains("r=rOprNGfwEbeRWgbNEkqO"));
    }
    
    @Test
    void testParseServerFirstMessage() {
        String serverResponse = "r=rOprNGfwEbeRWgbNEkqO,s=MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMQ==,i=4096,";
        String nonce = "rOprNGfwEbeRWgbNEkqO";
        
        ScramSha256.ServerFirstMessage serverFirstMessage = ScramSha256.parseServerFirstMessage(serverResponse, nonce);
        
        assertEquals("rOprNGfwEbeRWgbNEkqO", serverFirstMessage.nonce);
        assertNotNull(serverFirstMessage.salt);
        assertEquals(4096, serverFirstMessage.iterations);
    }
    
    @Test
    void testCreateClientFinalMessage() {
        String clientFirstMessage = "n,";
        ScramSha256.ServerFirstMessage serverFirstMessage = new ScramSha256.ServerFirstMessage(
            "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfzFbmY+TXCzEJBB6Wg8MOoW+GZdZ2MT1Nz+I4TKY0h4HGM2KFV+Rg",
            new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
            4096
        );
        String password = "testpassword123";
        
        String clientFinalMessage = ScramSha256.createClientFinalMessage(clientFirstMessage, serverFirstMessage, password);
        
        // Should contain proof
        assertTrue(clientFinalMessage.contains("c=biws"));
        assertTrue(clientFinalMessage.contains("p="));
    }
    
    @Test
    void testParseServerFinalMessage() {
        String serverFinal = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfzFbmY+TXCzEJBB6Wg8MOoW+GZdZ2MT1Nz+I4TKY0h4HGM2KFV+Rg,v=signature";
        
        ScramSha256.ServerFinalMessage serverFinalMessage = ScramSha256.parseServerFinalMessage(serverFinal, "client-final-message");
        
        assertEquals("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfzFbmY+TXCzEJBB6Wg8MOoW+GZdZ2MT1Nz+I4TKY0h4HGM2KFV+Rg,v=signature", serverFinalMessage.payload);
    }
    
    @Test
    void testAuthenticationFlow() {
        String username = "testuser";
        String password = "testpassword123";
        String nonce = ScramSha256.generateNonce();
        
        // Client first message
        String clientFirstMessage = ScramSha256.createClientFirstMessage(username, nonce);
        
        // Server generates challenge
        byte[] salt = ScramSha256.generateSalt();
        String serverChallenge = "r=" + nonce + ",s=" + java.util.Base64.getEncoder().encodeToString(salt) + ",i=4096";
        
        // Parse server challenge
        ScramSha256.ServerFirstMessage serverFirstMessage = ScramSha256.parseServerFirstMessage(serverChallenge, nonce);
        
        // Client computes proof
        String clientFinalMessage = ScramSha256.createClientFinalMessage(clientFirstMessage, serverFirstMessage, password);
        
        // Server verifies proof (this would normally use stored hash)
        String storedHash = ScramSha256.hashPassword(password, salt);
        
        // This test just verifies the flow works without throwing exceptions
        assertNotNull(clientFinalMessage);
        assertNotNull(serverFirstMessage);
        assertNotNull(storedHash);
    }
}