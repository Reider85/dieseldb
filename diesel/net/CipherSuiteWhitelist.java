package diesel.net;

import java.util.List;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cipher-suite and protocol whitelist for the TLS transport (prompt4.md #34,
 * R3-009 step 1/3).
 *
 * <p>Only modern AEAD suites are enabled: the three TLS 1.3 suites plus the
 * forward-secret (ephemeral ECDHE/DHE) AES-GCM and CHACHA20-POLY1305 suites
 * of TLS 1.2. Everything legacy is left out of the whitelist and therefore
 * disabled: SSLv3/TLS 1.0/1.1 protocols, static-key RSA key exchange,
 * static ECDH, non-forward-secrecy CBC suites, RC4, 3DES and NULL/EXPORT
 * grades. Enabling a suite here is a deliberate allow-list decision, never an
 * intersection with {@code getSupportedCipherSuites()} minus a deny-list.
 */
public final class CipherSuiteWhitelist {

    private static final Logger LOGGER = LoggerFactory.getLogger(CipherSuiteWhitelist.class);

    /** Protocols offered to peers: TLS 1.3 preferred, TLS 1.2 fallback. */
    public static final List<String> ALLOWED_PROTOCOLS = List.of("TLSv1.3", "TLSv1.2");

    /**
     * Allowed cipher suites in server preference order: TLS 1.3 suites first
     * (they are version-negotiated, not listed per-protocol), then the TLS 1.2
     * forward-secret AEAD suites.
     */
    public static final List<String> ALLOWED_SUITES = List.of(
            // TLS 1.3
            "TLS_AES_128_GCM_SHA256",
            "TLS_AES_256_GCM_SHA384",
            "TLS_CHACHA20_POLY1305_SHA256",
            // TLS 1.2, PFS + AEAD
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");

    private CipherSuiteWhitelist() {
    }

    /**
     * Returns the whitelist entries the JVM actually supports, preserving the
     * whitelist preference order.
     *
     * @param supported the socket's supported suites (from
     *                  {@code getSupportedCipherSuites()})
     * @return the intersection, in whitelist order; never null
     */
    public static String[] filter(String[] supported) {
        List<String> supportedSet = List.of(supported);
        return ALLOWED_SUITES.stream()
                .filter(supportedSet::contains)
                .toArray(String[]::new);
    }

    /**
     * Applies the protocol and cipher-suite whitelist to a socket: enables only
     * {@link #ALLOWED_PROTOCOLS} and the intersection of {@link #ALLOWED_SUITES}
     * with the suites the socket supports. Fails fast when the JVM supports
     * none of the whitelisted suites (broken custom JSSE provider) instead of
     * silently continuing with a weaker set.
     *
     * @param socket the TLS socket to configure
     * @throws SSLException if no whitelisted suite or protocol is supported
     */
    public static void apply(SSLSocket socket) throws SSLException {
        String[] enabled = filter(socket.getSupportedCipherSuites());
        if (enabled.length == 0) {
            throw new SSLException("No whitelisted cipher suite is supported by this JSSE provider");
        }
        socket.setEnabledCipherSuites(enabled);
        // Intersect protocols with supported too, so a JVM without TLS 1.3
        // still comes up on TLS 1.2 rather than failing to start.
        List<String> protocols = ALLOWED_PROTOCOLS.stream()
                .filter(p -> List.of(socket.getSupportedProtocols()).contains(p))
                .toList();
        if (protocols.isEmpty()) {
            throw new SSLException("No whitelisted TLS protocol is supported by this JSSE provider");
        }
        socket.setEnabledProtocols(protocols.toArray(String[]::new));
        // Server preference order for TLS 1.2 suite selection.
        SSLParameters params = socket.getSSLParameters();
        params.setUseCipherSuitesOrder(true);
        socket.setSSLParameters(params);
        LOGGER.debug("TLS whitelist applied: protocols={}, suites={}",
                protocols, List.of(enabled));
    }
}
