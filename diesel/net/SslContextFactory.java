package diesel.net;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the server-side {@link SSLContext} for the TLS transport
 * (prompt4.md #34, R3-009 step 1/3).
 *
 * <p>Loads a PKCS12 (or JKS) keystore holding the server certificate chain and
 * private key, and produces a {@code TLS} {@code SSLContext}. TLS 1.3 is
 * preferred with TLS 1.2 fallback; the exact protocol and cipher-suite
 * restriction is applied per socket by {@link CipherSuiteWhitelist#apply}.
 *
 * <p>Configuration keys (resolved from the server's merged Properties, i.e.
 * {@code config.properties} overlaid with any per-instance override):
 * <ul>
 *   <li>{@code tls.enabled} — {@code false} by default; when {@code true},
 *       {@link #create} must be given a usable keystore or it fails fast</li>
 *   <li>{@code tls.keystore.path} — filesystem path to the keystore file</li>
 *   <li>{@code tls.keystore.password} — keystore password (never logged)</li>
 *   <li>{@code tls.keystore.type} — optional, {@code PKCS12} by default;
 *       {@code JKS} also accepted</li>
 * </ul>
 *
 * <p>Misconfiguration (enabled without a readable keystore) throws instead of
 * silently downgrading the listener to plaintext.
 */
public final class SslContextFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SslContextFactory.class);

    /** Config key: master switch, default off. */
    public static final String ENABLED_KEY = "tls.enabled";

    /** Config key: keystore file path. */
    public static final String KEYSTORE_PATH_KEY = "tls.keystore.path";

    /** Config key: keystore password. Never logged. */
    public static final String KEYSTORE_PASSWORD_KEY = "tls.keystore.password";

    /** Config key: keystore type, PKCS12 by default. */
    public static final String KEYSTORE_TYPE_KEY = "tls.keystore.type";

    /** Default keystore type: PKCS12 (the modern standard, JKS legacy). */
    public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

    private SslContextFactory() {
    }

    /**
     * Whether TLS is switched on in the given configuration.
     *
     * @param config the merged server configuration
     * @return {@code true} when {@code tls.enabled} parses as true
     */
    public static boolean isEnabled(Properties config) {
        return Boolean.parseBoolean(config.getProperty(ENABLED_KEY, "false").trim());
    }

    /**
     * Creates the server {@code SSLContext} from the configuration.
     *
     * @param config the merged server configuration (file + instance overlay)
     * @return a {@code TLS} context initialised with the keystore key managers
     *         and the default trust managers (client-cert trust is wired in
     *         prompt4 #36, mTLS)
     * @throws IOException              if the keystore file is missing/unreadable
     * @throws GeneralSecurityException if the keystore cannot be parsed or the
     *                                  password is wrong
     */
    public static SSLContext create(Properties config) throws IOException, GeneralSecurityException {
        String path = config.getProperty(KEYSTORE_PATH_KEY, "").trim();
        if (path.isEmpty()) {
            throw new IOException("TLS is enabled but " + KEYSTORE_PATH_KEY + " is not set");
        }
        String password = config.getProperty(KEYSTORE_PASSWORD_KEY, "");
        String type = config.getProperty(KEYSTORE_TYPE_KEY, DEFAULT_KEYSTORE_TYPE).trim();

        KeyStore keyStore = loadKeyStore(Path.of(path), type, password.toCharArray());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, password.toCharArray());
        // Trust managers: platform defaults for now. Trusting client certs and
        // the truststore path land with mTLS in prompt4 #36.
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null);

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        LOGGER.info("TLS SSLContext created from keystore {} (type={})", path, type);
        return context;
    }

    private static KeyStore loadKeyStore(Path path, String type, char[] password)
            throws IOException, GeneralSecurityException {
        if (!Files.isRegularFile(path)) {
            throw new IOException("TLS keystore not found: " + path);
        }
        KeyStore keyStore = KeyStore.getInstance(type);
        try (InputStream in = Files.newInputStream(path)) {
            keyStore.load(in, password);
        }
        if (keyStore.size() == 0) {
            throw new GeneralSecurityException("TLS keystore is empty: " + path);
        }
        return keyStore;
    }
}
