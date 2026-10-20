package diesel.net;

import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-connection TLS wrapping for the server accept loop (prompt4.md #34,
 * R3-009 step 1/3).
 *
 * <p>Created once per server in {@code DatabaseServer.start()} when
 * {@code tls.enabled=true}; each accepted plain {@link Socket} is wrapped into
 * an {@link SSLSocket} with the {@link CipherSuiteWhitelist} applied. The
 * handshake itself is lazy: it runs on the first read/write inside the worker
 * thread ({@code ClientHandler.run}), never on the accept thread, so a slow or
 * malicious peer cannot stall {@code accept()} (handshake-DoS). A failed
 * handshake surfaces as an {@link IOException} in the worker and the connection
 * is closed there.
 */
public final class TlsHandshakeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsHandshakeHandler.class);

    private final SSLContext sslContext;

    private TlsHandshakeHandler(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    /**
     * Builds the handler from the server configuration, or returns
     * {@code null} when TLS is disabled — the accept loop then passes plain
     * sockets through untouched.
     *
     * @param config the merged server configuration
     * @return the handler, or {@code null} when {@code tls.enabled=false}
     * @throws IOException              if TLS is enabled but the keystore is
     *                                  missing/unreadable (fail fast, no
     *                                  silent plaintext downgrade)
     * @throws GeneralSecurityException if the keystore is malformed or the
     *                                  password is wrong
     */
    public static TlsHandshakeHandler fromConfig(Properties config)
            throws IOException, GeneralSecurityException {
        if (!SslContextFactory.isEnabled(config)) {
            return null;
        }
        return new TlsHandshakeHandler(SslContextFactory.create(config));
    }

    /**
     * Wraps an accepted connection into a whitelisted {@link SSLSocket}. The
     * TLS handshake is not performed here; it happens on the first I/O on the
     * returned socket (worker thread).
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
