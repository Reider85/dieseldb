package diesel.net;

import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import diesel.security.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-connection TLS wrapping for the server accept loop (prompt4.md #34,
 * R3-009 step 1/3; mTLS + SNI in #36 step 3/3).
 *
 * <p>Created once per server in {@code DatabaseServer.start()} when
 * {@code tls.enabled=true}; each accepted plain {@link Socket} is wrapped into
 * an {@link SSLSocket} with the {@link CipherSuiteWhitelist} applied and the
 * {@code tls.client.auth} client-certificate policy of
 * {@link MutualTlsHandler}. When SNI tenants are configured
 * ({@link SniSelector}), the context selects the per-tenant keystore during
 * the handshake. The handshake itself is lazy: it runs on the first read/write
 * inside the worker thread ({@code ClientHandler.run}), never on the accept
 * thread, so a slow or malicious peer cannot stall {@code accept()}
 * (handshake-DoS). A failed handshake — including mTLS client-cert
 * rejection — surfaces as an {@link IOException} in the worker and the
 * connection is closed there.
 */
public final class TlsHandshakeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsHandshakeHandler.class);

    private final SSLContext sslContext;
    private final MutualTlsHandler.ClientAuthMode clientAuthMode;

    private TlsHandshakeHandler(SSLContext sslContext, MutualTlsHandler.ClientAuthMode clientAuthMode) {
        this.sslContext = sslContext;
        this.clientAuthMode = clientAuthMode;
    }

    /**
     * Builds the handler from the server configuration, or returns
     * {@code null} when TLS is disabled — the accept loop then passes plain
     * sockets through untouched.
     *
     * @param config       the merged server configuration
     * @param tableTenants tenants from the {@code DIESEL_TENANTS} table
     *                     (may be {@code null}/empty; SNI is then config-only
     *                     or disabled)
     * @return the handler, or {@code null} when {@code tls.enabled=false}
     * @throws IOException              if TLS is enabled but the keystore is
     *                                  missing/unreadable (fail fast, no
     *                                  silent plaintext downgrade), the
     *                                  {@code tls.client.auth} value is
     *                                  invalid, or mTLS is on without truststore
     * @throws GeneralSecurityException if a keystore is malformed or the
     *                                  password is wrong
     */
    public static TlsHandshakeHandler fromConfig(Properties config, List<Tenant> tableTenants)
            throws IOException, GeneralSecurityException {
        if (!SslContextFactory.isEnabled(config)) {
            return null;
        }
        MutualTlsHandler.ClientAuthMode mode = MutualTlsHandler.parse(config);
        SniSelector sniSelector = SniSelector.fromConfig(config, tableTenants);
        SSLContext context = sniSelector != null
                ? sniSelector.createServerContext(config)
                : SslContextFactory.create(config);
        if (sniSelector != null) {
            LOGGER.info("TLS SNI keystore selection active ({} tenant(s))", sniSelector.tenantCount());
        }
        if (mode != MutualTlsHandler.ClientAuthMode.NONE) {
            LOGGER.info("TLS client authentication: {0}", mode);
        }
        return new TlsHandshakeHandler(context, mode);
    }

    /**
     * Wraps an accepted connection into a whitelisted {@link SSLSocket} with
     * the configured client-auth policy. The TLS handshake is not performed
     * here; it happens on the first I/O on the returned socket (worker
     * thread), where mTLS client-cert rejection surfaces as an IOException.
     *
     * @param raw the accepted plain TCP socket
     * @return an SSLSocket with whitelisted protocols and cipher suites
     * @throws IOException if the SSL socket cannot be created or the
     *                     whitelist cannot be applied
     */
    public Socket wrap(Socket raw) throws IOException {
        SSLSocketFactory factory = sslContext.getSocketFactory();
        SSLSocket sslSocket = (SSLSocket) factory.createSocket(raw, null, 0, true);
        // Sockets from an SSLSocketFactory default to client mode; a wrapped
        // accepted socket must be a server-side endpoint.
        sslSocket.setUseClientMode(false);
        try {
            CipherSuiteWhitelist.apply(sslSocket);
            MutualTlsHandler.apply(sslSocket, clientAuthMode);
        } catch (IOException e) {
            try {
                sslSocket.close();
            } catch (IOException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
        return sslSocket;
    }
}
