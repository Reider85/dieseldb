package diesel.net;

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * Mutual TLS (client-certificate) policy for the server transport
 * (prompt4.md #36, R3-009 step 3/3).
 *
 * <p>Reads {@code tls.client.auth} from the merged server configuration and
 * applies the corresponding {@link SSLParameters} requirement to each wrapped
 * socket before the (lazy) handshake:
 * <ul>
 *   <li>{@code none} (default) — no client certificate requested</li>
 *   <li>{@code request} — certificate optional; when presented it must be
 *       verifiable by the server truststore ({@code tls.ca.path})</li>
 *   <li>{@code require} — certificate mandatory; a cert-less client fails
 *       the handshake</li>
 * </ul>
 *
 * <p>Enforcement happens inside the TLS stack during the handshake (which runs
 * lazily in the worker thread per {@link TlsHandshakeHandler}); a rejected
 * client surfaces as an {@link IOException} there — no accept-thread blocking.
 */
final class MutualTlsHandler {

    /** Client-certificate policy for a TLS server endpoint. */
    enum ClientAuthMode {
        /** No client-certificate verification (default). */
        NONE,
        /** Certificate optional; when presented it must verify. */
        REQUEST,
        /** Certificate mandatory; cert-less clients are rejected. */
        REQUIRE
    }

    private MutualTlsHandler() {
    }

    /**
     * Parses {@code tls.client.auth} from the configuration. An unknown value
     * fails fast (no silent downgrade), consistent with the TLS keystore
     * fail-fast rule of prompt4 #34.
     *
     * @param config the merged server configuration
     * @return the parsed mode, {@link ClientAuthMode#NONE} when unset
     * @throws IOException if the value is not none/request/require
     */
    static ClientAuthMode parse(Properties config) throws IOException {
        String raw = config.getProperty(SslContextFactory.CLIENT_AUTH_KEY,
                SslContextFactory.DEFAULT_CLIENT_AUTH).trim().toLowerCase(Locale.ROOT);
        return switch (raw) {
            case "none" -> ClientAuthMode.NONE;
            case "request" -> ClientAuthMode.REQUEST;
            case "require" -> ClientAuthMode.REQUIRE;
            default -> throw new IOException("Invalid " + SslContextFactory.CLIENT_AUTH_KEY
                    + " value '" + raw + "': expected none, request or require");
        };
    }

    /**
     * Applies the client-auth requirement to a server-side SSLSocket. Must be
     * called before the handshake (i.e. before the first read/write — the lazy
     * handshake of {@link TlsHandshakeHandler} guarantees that).
     *
     * @param socket the server-mode SSLSocket
     * @param mode   the policy to apply
     */
    static void apply(SSLSocket socket, ClientAuthMode mode) {
        switch (mode) {
            case REQUIRE -> {
                SSLParameters params = socket.getSSLParameters();
                params.setNeedClientAuth(true);
                socket.setSSLParameters(params);
            }
            case REQUEST -> {
                SSLParameters params = socket.getSSLParameters();
                params.setWantClientAuth(true);
                socket.setSSLParameters(params);
            }
            case NONE -> {
                // Nothing to do: client-auth stays off.
            }
        }
    }
}
