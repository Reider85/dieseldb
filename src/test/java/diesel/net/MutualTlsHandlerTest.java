package diesel.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Prompt4 #36 unit tests: parsing of {@code tls.client.auth} and application
 * of the client-auth requirement to a server-mode SSLSocket.
 */
@Tag("smoke")
class MutualTlsHandlerTest {

    @Test
    void parseDefaultsToNone() throws Exception {
        assertEquals(MutualTlsHandler.ClientAuthMode.NONE,
                MutualTlsHandler.parse(new Properties()));
    }

    @Test
    void parseAcceptsAllThreeModes() throws Exception {
        for (String value : new String[] {"none", "request", "require"}) {
            Properties props = new Properties();
            props.setProperty("tls.client.auth", value);
            MutualTlsHandler.parse(props);
        }
        Properties upper = new Properties();
        upper.setProperty("tls.client.auth", " REQUIRE ");
        assertEquals(MutualTlsHandler.ClientAuthMode.REQUIRE, MutualTlsHandler.parse(upper));
    }

    @Test
    void parseRejectsUnknownMode() {
        Properties props = new Properties();
        props.setProperty("tls.client.auth", "maybe");
        assertThrows(java.io.IOException.class, () -> MutualTlsHandler.parse(props));
    }

    @Test
    void applySetsNeedOrWantClientAuth() throws Exception {
        SSLContext context = SSLContext.getDefault();
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket()) {
            MutualTlsHandler.apply(socket, MutualTlsHandler.ClientAuthMode.REQUIRE);
            assertTrue(socket.getNeedClientAuth(), "require must set needClientAuth");
            assertFalse(socket.getWantClientAuth(), "require must not also set wantClientAuth");
        }
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket()) {
            MutualTlsHandler.apply(socket, MutualTlsHandler.ClientAuthMode.REQUEST);
            assertTrue(socket.getWantClientAuth(), "request must set wantClientAuth");
            assertFalse(socket.getNeedClientAuth(), "request must not set needClientAuth");
        }
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket()) {
            MutualTlsHandler.apply(socket, MutualTlsHandler.ClientAuthMode.NONE);
            assertFalse(socket.getNeedClientAuth());
            assertFalse(socket.getWantClientAuth());
        }
    }
}
