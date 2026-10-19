package diesel.security;

import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.util.Locale;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Salted password hashing for RBAC accounts (prompt4 #30, R3-008 step 1/4).
 *
 * <p>Default algorithm is PBKDF2-With-HmacSHA256 from the JDK — no external
 * dependency. Encoded form: {@code pbkdf2_sha256$<iterations>$<saltB64>$<hashB64>}.
 * Argon2 was evaluated as an option and deferred (optional in the prompt;
 * would add a runtime dependency).
 */
public final class PasswordHasher {

    /** Default PBKDF2 iteration count (OWASP 2023 guidance for PBKDF2-HmacSHA256). */
    public static final int DEFAULT_ITERATIONS = 100_000;

    private static final String ALGORITHM = "PBKDF2WithHmacSHA256";
    private static final String PREFIX = "pbkdf2_sha256";
    /** Optional 5th part marking a hash whose account must change its password (#31). */
    public static final String MUST_CHANGE_SUFFIX = "must_change";
    private static final int SALT_BYTES = 16;
    private static final int KEY_BITS = 256;

    private final int iterations;
    private final SecureRandom random;

    /** Creates a hasher with {@link #DEFAULT_ITERATIONS}. */
    public PasswordHasher() {
        this(DEFAULT_ITERATIONS);
    }

    /**
     * @param iterations PBKDF2 iteration count; must be positive
     */
    public PasswordHasher(int iterations) {
        if (iterations <= 0) {
            throw new IllegalArgumentException("PBKDF2 iterations must be positive: " + iterations);
        }
        this.iterations = iterations;
        this.random = new SecureRandom();
    }

    /** Returns the PBKDF2 iteration count this hasher uses. */
    public int getIterations() {
        return iterations;
    }

    /**
     * Hashes the password with a fresh random salt.
     *
     * @param password the plaintext password
     * @return the encoded hash string, safe to store
     */
    public String hash(String password) {
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("Password must not be null or empty");
        }
        byte[] salt = new byte[SALT_BYTES];
        random.nextBytes(salt);
        byte[] hash = pbkdf2(password, salt, iterations);
        Base64.Encoder b64 = Base64.getEncoder();
        return PREFIX + "$" + iterations + "$" + b64.encodeToString(salt) + "$" + b64.encodeToString(hash);
    }

    /**
     * Verifies a password against an encoded hash produced by {@link #hash}.
     *
     * @param password the candidate plaintext password
     * @param encoded  the stored encoded hash
     * @return true when the password matches
     */
    public boolean verify(String password, String encoded) {
        if (password == null || encoded == null) {
            return false;
        }
        String[] parts = splitEncoded(encoded);
        if (parts.length != 4 || !PREFIX.equals(parts[0])) {
            return false;
        }
        int encodedIterations;
        try {
            encodedIterations = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return false;
        }
        try {
            byte[] salt = Base64.getDecoder().decode(parts[2]);
            byte[] expected = Base64.getDecoder().decode(parts[3]);
            byte[] actual = pbkdf2(password, salt, encodedIterations);
            return constantTimeEquals(expected, actual);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Returns the encoded hash with the must-change flag appended
     * (prompt4.md #31). The flag survives restarts without a schema change.
     *
     * @param encoded an encoded hash, with or without the flag
     * @return the hash carrying the {@link #MUST_CHANGE_SUFFIX} marker
     */
    public static String withMustChangeFlag(String encoded) {
        if (encoded == null) {
            throw new IllegalArgumentException("encoded hash must not be null");
        }
        if (requiresPasswordChange(encoded)) {
            return encoded;
        }
        return encoded + "$" + MUST_CHANGE_SUFFIX;
    }

    /**
     * Returns the encoded hash without the must-change flag.
     *
     * @param encoded an encoded hash, with or without the flag
     * @return the hash stripped of the {@link #MUST_CHANGE_SUFFIX} marker
     */
    public static String withoutMustChangeFlag(String encoded) {
        if (encoded == null) {
            throw new IllegalArgumentException("encoded hash must not be null");
        }
        if (!requiresPasswordChange(encoded)) {
            return encoded;
        }
        return encoded.substring(0, encoded.lastIndexOf('$'));
    }

    /** True when the hash is flagged so its account must change the password first (#31). */
    public static boolean requiresPasswordChange(String encoded) {
        if (encoded == null) {
            return false;
        }
        String[] parts = encoded.split("\\$");
        return parts.length == 5 && MUST_CHANGE_SUFFIX.equals(parts[4]);
    }

    /** Splits an encoded hash, tolerating the optional must-change suffix. */
    private static String[] splitEncoded(String encoded) {
        String[] parts = encoded.split("\\$");
        if (parts.length == 5 && MUST_CHANGE_SUFFIX.equals(parts[4])) {
            String[] stripped = new String[4];
            System.arraycopy(parts, 0, stripped, 0, 4);
            return stripped;
        }
        return parts;
    }

    private static byte[] pbkdf2(String password, byte[] salt, int iterations) {
        try {
            KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, KEY_BITS);
            return SecretKeyFactory.getInstance(ALGORITHM).generateSecret(spec).getEncoded();
        } catch (Exception e) {
            throw new IllegalStateException("PBKDF2 hashing unavailable: " + e.getMessage(), e);
        }
    }

    private static boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        int diff = 0;
        for (int i = 0; i < a.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }

    /** True when {@code encoded} is a hash this class can verify. */
    public static boolean isEncodedHash(String encoded) {
        return encoded != null
                && encoded.toLowerCase(Locale.ROOT).startsWith(PREFIX + "$");
    }
}
