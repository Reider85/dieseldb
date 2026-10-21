package diesel.net;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.StandardConstants;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509KeyManager;
import diesel.security.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side SNI keystore selection for multi-tenant TLS deployments
 * (prompt4.md #36, R3-009 step 3/3).
 *
 * <p>Tenants come from two sources, merged at startup (config wins on
 * hostname conflicts):
 * <ul>
 *   <li>config keys {@code tls.sni.<hostname>.keystore.path} /
 *       {@code .keystore.password} / {@code .keystore.type}</li>
 *   <li>rows of the {@code DIESEL_TENANTS} system table (see
 *       {@code CREATE TENANT}), passed in by the caller</li>
 * </ul>
 *
 * <p>Selection happens inside the TLS stack during the handshake via a
 * composite {@link X509ExtendedKeyManager} that reads the requested SNI
 * hostname from the handshake session and delegates to the matching tenant's
 * key managers; unknown or absent SNI falls back to the default
 * {@code tls.keystore.*} certificate. The lazy-handshake design of
 * {@link TlsHandshakeHandler} is preserved.
 */
public final class SniSelector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SniSelector.class);

    /** Config key prefix: {@code tls.sni.<hostname>.keystore.path}. */
    static final String SNI_PREFIX = "tls.sni.";

    /** Suffix of the per-tenant keystore path key. */
    static final String KEYSTORE_PATH_SUFFIX = ".keystore.path";
    /** Suffix of the per-tenant keystore password key. */
    static final String KEYSTORE_PASSWORD_SUFFIX = ".keystore.password";
    /** Suffix of the per-tenant keystore type key. */
    static final String KEYSTORE_TYPE_SUFFIX = ".keystore.type";

    /** Normalized hostname → tenant key managers. */
    private final Map<String, javax.net.ssl.KeyManager[]> tenantKeyManagers;
    /** Default key managers from {@code tls.keystore.*} (unknown/absent SNI). */
    private final javax.net.ssl.KeyManager[] defaultKeyManagers;

    private SniSelector(Map<String, javax.net.ssl.KeyManager[]> tenantKeyManagers,
            javax.net.ssl.KeyManager[] defaultKeyManagers) {
        this.tenantKeyManagers = Collections.unmodifiableMap(tenantKeyManagers);
        this.defaultKeyManagers = defaultKeyManagers;
    }

    /** Number of configured tenants (config + table, merged). */
    public int tenantCount() {
        return tenantKeyManagers.size();
    }

    /**
     * Builds the selector from config keys and the {@code DIESEL_TENANTS}
     * rows, or returns {@code null} when no tenants are configured (the
     * caller then keeps the plain single-keystore context of prompt4 #34).
     *
     * @param config       the merged server configuration
     * @param tableTenants tenants loaded from {@code DIESEL_TENANTS};
     *                     {@code null} or empty in config-only setups
     * @return the selector, or {@code null} when there are no tenants
     * @throws IOException              if a tenant keystore is missing/unreadable
     *                                  (fail fast at startup) or the default
     *                                  {@code tls.keystore.*} is unusable
     * @throws GeneralSecurityException if a keystore cannot be parsed
     */
    public static SniSelector fromConfig(Properties config, List<Tenant> tableTenants)
            throws IOException, GeneralSecurityException {
        Map<String, TenantSpec> specs = new HashMap<>();
        // Source 1: tls.sni.<hostname>.keystore.* config keys
        for (String key : config.stringPropertyNames()) {
            if (!key.startsWith(SNI_PREFIX) || !key.endsWith(KEYSTORE_PATH_SUFFIX)) {
                continue;
            }
            String hostname = key.substring(SNI_PREFIX.length(),
                    key.length() - KEYSTORE_PATH_SUFFIX.length());
            if (hostname.isEmpty()) {
                continue;
            }
            String path = config.getProperty(key, "").trim();
            if (path.isEmpty()) {
                continue;
            }
            String base = SNI_PREFIX + hostname;
            String password = config.getProperty(base + KEYSTORE_PASSWORD_SUFFIX, "");
            String type = config.getProperty(base + KEYSTORE_TYPE_SUFFIX,
                    SslContextFactory.DEFAULT_KEYSTORE_TYPE).trim();
            specs.put(hostname.toLowerCase(Locale.ROOT),
                    new TenantSpec(hostname, path, password, type));
        }
        // Source 2: DIESEL_TENANTS rows (config wins on hostname conflicts)
        if (tableTenants != null) {
            for (Tenant tenant : tableTenants) {
                String normalized = tenant.sniHostname().toLowerCase(Locale.ROOT);
                specs.putIfAbsent(normalized,
                        new TenantSpec(tenant.sniHostname(), tenant.keystorePath(),
                                tenant.keystorePassword(), SslContextFactory.DEFAULT_KEYSTORE_TYPE));
            }
        }
        if (specs.isEmpty()) {
            return null;
        }
        // Default keystore for the SNI fallback (unknown/absent hostname)
        String defaultPath = config.getProperty(SslContextFactory.KEYSTORE_PATH_KEY, "").trim();
        if (defaultPath.isEmpty()) {
            throw new IOException("SNI tenants are configured but " + SslContextFactory.KEYSTORE_PATH_KEY
                    + " is not set (needed as the fallback certificate)");
        }
        String defaultPassword = config.getProperty(SslContextFactory.KEYSTORE_PASSWORD_KEY, "");
        String defaultType = config.getProperty(SslContextFactory.KEYSTORE_TYPE_KEY,
                SslContextFactory.DEFAULT_KEYSTORE_TYPE).trim();
        javax.net.ssl.KeyManager[] defaultKms =
                SslContextFactory.serverKeyManagers(Path.of(defaultPath), defaultType, defaultPassword);

        Map<String, javax.net.ssl.KeyManager[]> tenants = new HashMap<>();
        for (Map.Entry<String, TenantSpec> entry : specs.entrySet()) {
            TenantSpec spec = entry.getValue();
            String type = spec.type().equalsIgnoreCase("AUTO")
                    ? SslContextFactory.detectKeystoreType(Path.of(spec.path()))
                    : spec.type();
            tenants.put(entry.getKey(),
                    SslContextFactory.serverKeyManagers(Path.of(spec.path()), type, spec.password()));
        }
        LOGGER.info("SNI TLS enabled: {} tenant(s), fallback keystore {}",
                tenants.size(), defaultPath);
        return new SniSelector(tenants, defaultKms);
    }

    /**
     * Builds the server {@code SSLContext} whose key manager selects the
     * tenant certificate by the client's SNI hostname; trust managers follow
     * the {@code tls.client.auth} logic of {@link SslContextFactory}.
     *
     * @param config the merged server configuration (for trust managers)
     * @return the SNI-aware server SSLContext
     * @throws IOException              if mTLS is on without a truststore
     * @throws GeneralSecurityException if the truststore cannot be parsed
     */
    public SSLContext createServerContext(Properties config)
            throws IOException, GeneralSecurityException {
        javax.net.ssl.KeyManager[] composite = {
            new SniKeyManager(tenantKeyManagers, defaultKeyManagers)
        };
        return SslContextFactory.createWithKeyManagers(config, composite);
    }

    /** Per-tenant spec resolved from config or the tenants table. */
    private record TenantSpec(String hostname, String path, String password, String type) {
    }

    /**
     * Composite key manager: picks the tenant key managers matching the SNI
     * hostname of the in-progress handshake, else the default keystore.
     * Aliases returned to JSSE are prefixed with the tenant hostname so
     * {@code getCertificateChain}/{@code getPrivateKey} can delegate back to
     * the right tenant key manager.
     */
    static final class SniKeyManager extends X509ExtendedKeyManager {

        private final Map<String, javax.net.ssl.KeyManager[]> tenantKeyManagers;
        private final javax.net.ssl.KeyManager[] defaultKeyManagers;
        /** Alias prefix → owning key manager (populated by chooseServerAlias). */
        private final ConcurrentHashMap<String, X509KeyManager> aliasOwners = new ConcurrentHashMap<>();

        SniKeyManager(Map<String, javax.net.ssl.KeyManager[]> tenantKeyManagers,
                javax.net.ssl.KeyManager[] defaultKeyManagers) {
            this.tenantKeyManagers = tenantKeyManagers;
            this.defaultKeyManagers = defaultKeyManagers;
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            return select(keyType, issuers, extractSniHostname(socket));
        }

        @Override
        public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
            String hostname = null;
            if (engine != null) {
                SSLSession session = engine.getHandshakeSession();
                if (session != null) {
                    hostname = hostnameFrom(session);
                }
            }
            return select(keyType, issuers, hostname);
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            X509KeyManager owner = aliasOwners.get(alias);
            if (owner != null) {
                return owner.getCertificateChain(stripPrefix(alias));
            }
            return defaultX509().getCertificateChain(alias);
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            X509KeyManager owner = aliasOwners.get(alias);
            if (owner != null) {
                return owner.getPrivateKey(stripPrefix(alias));
            }
            return defaultX509().getPrivateKey(alias);
        }

        // Client-auth entry points: this is a server endpoint; client aliases
        // are never requested (the client-cert path uses chooseClientAlias on
        // the client side only).
        @Override
        public String chooseClientAlias(String[] keyTypes, Principal[] issuers, Socket socket) {
            return null;
        }

        @Override
        public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
            return null;
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            return null;
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return null;
        }

        private String select(String keyType, Principal[] issuers, String hostname) {
            javax.net.ssl.KeyManager[] candidates = defaultKeyManagers;
            String prefix = "";
            if (hostname != null) {
                javax.net.ssl.KeyManager[] tenant =
                        tenantKeyManagers.get(hostname.toLowerCase(Locale.ROOT));
                if (tenant != null) {
                    candidates = tenant;
                    prefix = hostname.toLowerCase(Locale.ROOT) + ":";
                }
            }
            for (javax.net.ssl.KeyManager km : candidates) {
                String alias = chooseFrom(km, keyType, issuers);
                if (alias != null) {
                    String prefixed = prefix.isEmpty() ? alias : prefix + alias;
                    if (!prefix.isEmpty() && km instanceof X509KeyManager xkm) {
                        aliasOwners.put(prefixed, xkm);
                    }
                    return prefixed;
                }
            }
            return null;
        }

        private static String chooseFrom(javax.net.ssl.KeyManager km, String keyType,
                Principal[] issuers) {
            if (km instanceof X509ExtendedKeyManager extended) {
                return extended.chooseServerAlias(keyType, issuers, (Socket) null);
            }
            if (km instanceof X509KeyManager legacy) {
                return legacy.chooseServerAlias(keyType, issuers, null);
            }
            return null;
        }

        private X509KeyManager defaultX509() {
            for (javax.net.ssl.KeyManager km : defaultKeyManagers) {
                if (km instanceof X509KeyManager xkm) {
                    return xkm;
                }
            }
            throw new IllegalStateException("No default X509 key manager configured");
        }

        private static String stripPrefix(String alias) {
            int idx = alias.indexOf(':');
            return idx >= 0 ? alias.substring(idx + 1) : alias;
        }

        private static String extractSniHostname(Socket socket) {
            if (socket instanceof SSLSocket sslSocket) {
                SSLSession session = sslSocket.getHandshakeSession();
                if (session != null) {
                    return hostnameFrom(session);
                }
            }
            return null;
        }

        private static String hostnameFrom(SSLSession session) {
            if (!(session instanceof ExtendedSSLSession extendedSession)) {
                return null;
            }
            List<SNIServerName> names = extendedSession.getRequestedServerNames();
            for (SNIServerName name : names) {
                if (name.getType() == StandardConstants.SNI_HOST_NAME) {
                    return ((SNIHostName) name).getAsciiName();
                }
            }
            return null;
        }
    }
}
