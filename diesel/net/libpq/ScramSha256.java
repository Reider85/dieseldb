package diesel.net.libpq;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

/**
 * SCRAM-SHA-256 authentication implementation for libpq protocol.
 * 
 * RFC 5802: SCRAM-SHA-256 authentication mechanism
 * RFC 7677: SCRAM-SHA-256 and SCRAM-SHA-256-PLUS SASL Mechanisms
 */
public class ScramSha256 {
    
    private static final String SCRAM_MECHANISM = "SCRAM-SHA-256";
    private static final int ITERATION_COUNT = 4096;
    private static final int KEY_LENGTH = 32;
    private static final int SALT_LENGTH = 16;
    
    private static final SecureRandom random = new SecureRandom();
    
    /**
     * Generates a random nonce for SCRAM authentication.
     */
    public static String generateNonce() {
        byte[] nonceBytes = new byte[16];
        random.nextBytes(nonceBytes);
        return Base64.getEncoder().withoutPadding().encodeToString(nonceBytes);
    }
    
    /**
     * Generates a random salt for password storage.
     */
    public static byte[] generateSalt() {
        byte[] salt = new byte[SALT_LENGTH];
        random.nextBytes(salt);
        return salt;
    }
    
    /**
     * Hashes a password with PBKDF2 using the given salt.
     */
    public static String hashPassword(String password, byte[] salt) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, ITERATION_COUNT, KEY_LENGTH * 8);
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            byte[] hash = factory.generateSecret(spec).getEncoded();
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Password hashing failed", e);
        }
    }
    
    /**
     * Creates the client's first message for SCRAM authentication.
     */
    public static String createClientFirstMessage(String username, String nonce) {
        return String.format("n,,n=%s,r=%s", username, nonce);
    }
    
    /**
     * Parses the server's first message in SCRAM authentication.
     */
    public static ServerFirstMessage parseServerFirstMessage(String serverResponse, String clientNonce) {
        if (!serverResponse.startsWith("r=" + clientNonce)) {
            throw new IllegalArgumentException("Server response does not match client nonce");
        }
        
        String nonce = extractValue(serverResponse, "r=");
        String saltBase64 = extractValue(serverResponse, "s=");
        String iterationsStr = extractValue(serverResponse, "i=");
        
        byte[] salt = Base64.getDecoder().decode(saltBase64);
        int iterations = Integer.parseInt(iterationsStr);
        
        return new ServerFirstMessage(nonce, salt, iterations);
    }
    
    /**
     * Creates the client's final message for SCRAM authentication.
     */
    public static String createClientFinalMessage(String clientFirstMessage, ServerFirstMessage serverFirstMessage, String password) {
        // Compute ClientKey
        String storedPassword = hashPassword(password, serverFirstMessage.salt);
        byte[] clientKey = hmacSha256(storedPassword, "Client Key");
        
        // Compute StoredKey
        byte[] storedKey = sha256(clientKey);
        
        // Compute AuthMessage
        String authMessage = String.format("%s,%s,c=biws,r=%s",
                clientFirstMessage, "r=" + serverFirstMessage.nonce + ",s=" + Base64.getEncoder().encodeToString(serverFirstMessage.salt) + ",i=" + serverFirstMessage.iterations,
                "biws");
        byte[] authMessageBytes = authMessage.getBytes();
        byte[] clientSignature = hmacSha256(storedKey, authMessageBytes);
        byte[] clientProof = xorBytes(clientKey, clientSignature);
        
        return String.format("c=biws,r=%s,p=%s", serverFirstMessage.nonce, Base64.getEncoder().encodeToString(clientProof));
    }
    
    /**
     * Parses the server's final message in SCRAM authentication.
     */
    public static ServerFinalMessage parseServerFinalMessage(String serverResponse, String clientFinalMessage) {
        return new ServerFinalMessage(serverResponse);
    }
    
    /**
     * Verifies the server's signature in SCRAM authentication.
     */
    public static boolean verifyServerSignature(ServerFinalMessage serverFinalMessage, String clientFirstMessage, 
                                               ServerFirstMessage serverFirstMessage, String clientFinalMessage, String password) {
        // Compute ServerKey
        String storedPassword = hashPassword(password, serverFirstMessage.salt);
        byte[] serverKey = hmacSha256(storedPassword, "Server Key");
        
        // Compute AuthMessage
        String authMessage = String.format("%s,%s,c=biws,r=%s",
                clientFirstMessage, "r=" + serverFirstMessage.nonce + ",s=" + Base64.getEncoder().encodeToString(serverFirstMessage.salt) + ",i=" + serverFirstMessage.iterations,
                clientFinalMessage);
        byte[] authMessageBytes = authMessage.getBytes();
        byte[] serverSignature = hmacSha256(serverKey, authMessageBytes);
        
        // Verify the signature
        String expectedSignature = Base64.getEncoder().encodeToString(serverSignature);
        return expectedSignature.equals(serverFinalMessage.serverSignature);
    }
    
    /**
     * Extracts a value from a SCRAM message.
     */
    private static String extractValue(String message, String key) {
        int start = message.indexOf(key) + key.length();
        int end = message.indexOf(',', start);
        if (end == -1) {
            end = message.length();
        }
        return message.substring(start, end);
    }
    
    /**
     * Computes HMAC-SHA-256.
     */
    private static byte[] hmacSha256(String key, String data) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(key.getBytes(), "HmacSHA256"));
            return mac.doFinal(data.getBytes());
        } catch (Exception e) {
            throw new RuntimeException("HMAC-SHA-256 computation failed", e);
        }
    }
    
    /**
     * Computes HMAC-SHA-256.
     */
    private static byte[] hmacSha256(byte[] key, byte[] data) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("HMAC-SHA-256 computation failed", e);
        }
    }
    
    /**
     * Computes SHA-256 hash.
     */
    private static byte[] sha256(byte[] data) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            return digest.digest(data);
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 computation failed", e);
        }
    }
    
    /**
     * XORs two byte arrays.
     */
    private static byte[] xorBytes(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Byte arrays must be of equal length");
        }
        
        byte[] result = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            result[i] = (byte) (a[i] ^ b[i]);
        }
        return result;
    }
    
    /**
     * Represents the server's first message in SCRAM authentication.
     */
    public static class ServerFirstMessage {
        public final String nonce;
        public final byte[] salt;
        public final int iterations;
        
        public ServerFirstMessage(String nonce, byte[] salt, int iterations) {
            this.nonce = nonce;
            this.salt = salt;
            this.iterations = iterations;
        }
    }
    
    /**
     * Represents the server's final message in SCRAM authentication.
     */
    public static class ServerFinalMessage {
        public final String payload;
        public final String serverSignature;
        
        public ServerFinalMessage(String payload) {
            this.payload = payload;
            this.serverSignature = extractValue(payload, "v=");
        }
    }
}