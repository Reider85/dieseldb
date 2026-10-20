package diesel.net;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Collection;
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

    // Client-side config keys (prompt4 #35)
    /** Config key: truststore file path for client (PEM or PKCS12). */
    public static final String TRUSTSTORE_PATH_KEY = "tls.ca.path";
    /** Config key: truststore password (for PKCS12 truststores). */
    public static final String TRUSTSTORE_PASSWORD_KEY = "tls.truststore.password";
    /** Config key: truststore type, AUTO by default. */
    public static final String TRUSTSTORE_TYPE_KEY = "tls.truststore.type";
    /** Config key: client certificate file path (PEM or PKCS12). */
    public static final String CLIENT_CERT_PATH_KEY = "tls.cert.path";
    /** Config key: client private key file path (PEM, PKCS#8 or PKCS#1). */
    public static final String CLIENT_KEY_PATH_KEY = "tls.key.path";
    /** Config key: client keystore password (for PKCS12 client identity). */
    public static final String CLIENT_KEYSTORE_PASSWORD_KEY = "tls.keystore.password";

    /** Default truststore type: AUTO (by file extension). */
    public static final String DEFAULT_TRUSTSTORE_TYPE = "AUTO";
    /** Default client keystore type: AUTO (by file extension). */
    public static final String DEFAULT_CLIENT_KEYSTORE_TYPE = "AUTO";

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

    /**
     * Creates the client {@code SSLContext} from the configuration.
     * (prompt4 #35, R3-009 step 2/3).
     *
     * @param config the client configuration (diesel-client.conf or CLI flags merged)
     * @return a {@code TLS} context initialised with optional client identity
     *         and the specified truststore (or platform defaults if none)
     * @throws IOException              if certificate/key files are missing/unreadable
     * @throws GeneralSecurityException if certificates cannot be parsed or passwords are wrong
     */
    public static SSLContext createClient(Properties config) throws IOException, GeneralSecurityException {
        // Load truststore (optional)
        KeyStore trustStore = loadTrustStore(config);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        // Load client identity (optional)
        KeyStore keyStore = loadClientIdentity(config);
        KeyManagerFactory kmf = null;
        if (keyStore != null) {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, getClientKeyPassword(config));
        }

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf == null ? null : kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        LOGGER.info("Client TLS SSLContext created: truststore={}, clientIdentity={}",
                trustStore != null ? "configured" : "platform-defaults",
                keyStore != null ? "configured" : "none");
        return context;
    }

    private static KeyStore loadTrustStore(Properties config) throws IOException, GeneralSecurityException {
        String caPath = config.getProperty(TRUSTSTORE_PATH_KEY, "").trim();
        if (caPath.isEmpty()) {
            // No truststore specified → use platform defaults (JVM cacerts)
            return null;
        }

        String type = config.getProperty(TRUSTSTORE_TYPE_KEY, DEFAULT_TRUSTSTORE_TYPE).trim();
        char[] password = config.getProperty(TRUSTSTORE_PASSWORD_KEY, "").toCharArray();

        if ("AUTO".equalsIgnoreCase(type)) {
            type = detectKeystoreType(Path.of(caPath));
        }

        if ("PEM".equalsIgnoreCase(type) || "DER".equalsIgnoreCase(type)) {
            // Truststore is a PEM/DER certificate file → load into in-memory PKCS12
            return loadPemCertificatesAsTrustStore(Path.of(caPath));
        } else {
            // PKCS12/JKS file
            return loadKeyStore(Path.of(caPath), type, password);
        }
    }

    private static KeyStore loadClientIdentity(Properties config) throws IOException, GeneralSecurityException {
        String certPath = config.getProperty(CLIENT_CERT_PATH_KEY, "").trim();
        if (certPath.isEmpty()) {
            // No client identity specified
            return null;
        }

        String keyPath = config.getProperty(CLIENT_KEY_PATH_KEY, certPath).trim();
        String type = config.getProperty(KEYSTORE_TYPE_KEY, DEFAULT_CLIENT_KEYSTORE_TYPE).trim();
        char[] password = config.getProperty(CLIENT_KEYSTORE_PASSWORD_KEY, "").toCharArray();

        if ("AUTO".equalsIgnoreCase(type)) {
            type = detectKeystoreType(Path.of(certPath));
        }

        if ("PEM".equalsIgnoreCase(type)) {
            // Client identity is PEM files → load into in-memory PKCS12
            return loadPemIdentityAsKeystore(Path.of(certPath), Path.of(keyPath), password);
        } else {
            // PKCS12/JKS file (keyPath ignored if certPath is a PKCS12 containing both)
            return loadKeyStore(Path.of(certPath), type, password);
        }
    }

    private static char[] getClientKeyPassword(Properties config) {
        String password = config.getProperty(CLIENT_KEYSTORE_PASSWORD_KEY, "");
        return password.isEmpty() ? new char[0] : password.toCharArray();
    }

    private static String detectKeystoreType(Path path) {
        String filename = path.getFileName().toString().toLowerCase();
        if (filename.endsWith(".p12") || filename.endsWith(".pfx")) {
            return "PKCS12";
        } else if (filename.endsWith(".jks")) {
            return "JKS";
        } else if (filename.endsWith(".pem") || filename.endsWith(".crt")) {
            return "PEM";
        } else {
            // Default to PKCS12 for unknown extensions
            return "PKCS12";
        }
    }

    private static KeyStore loadPemCertificatesAsTrustStore(Path caPath) throws IOException, GeneralSecurityException {
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        try (InputStream in = Files.newInputStream(caPath)) {
            Collection<? extends Certificate> certs = certFactory.generateCertificates(in);
            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, null); // Initialize empty
            int i = 0;
            for (Certificate cert : certs) {
                trustStore.setCertificateEntry("trust-" + (i++), cert);
            }
            return trustStore;
        }
    }

    private static KeyStore loadPemIdentityAsKeystore(Path certPath, Path keyPath, char[] password)
            throws IOException, GeneralSecurityException {
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        try (InputStream certIn = Files.newInputStream(certPath)) {
            Certificate[] certChain = certFactory.generateCertificates(certIn)
                    .toArray(new Certificate[0]);
            
            PrivateKey privateKey = loadPemPrivateKey(keyPath);
            
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, password); // Initialize with password (empty if not specified)
            keyStore.setKeyEntry("client", privateKey, password, certChain);
            return keyStore;
        }
    }

    private static PrivateKey loadPemPrivateKey(Path keyPath) throws IOException, GeneralSecurityException {
        String keyContent = Files.readString(keyPath);
        keyContent = keyContent.replaceAll("-----.*PRIVATE KEY-----", "").replaceAll("\n", "").trim();
        
        byte[] keyBytes = java.util.Base64.getDecoder().decode(keyContent);
        
        try {
            // Try PKCS#8 first
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(spec);
        } catch (Exception e) {
            // Fallback to PKCS#1 RSA
            // PKCS#1 format: wrapped in X509 format for KeyFactory
            java.security.spec.X509EncodedKeySpec x509Spec = new X509EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(x509Spec);
        }
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
