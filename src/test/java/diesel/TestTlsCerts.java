package diesel;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import java.util.Locale;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * Generates an ephemeral self-signed PKCS12 keystore for TLS network tests
 * (prompt4.md #34) via the JDK's {@code keytool}, and builds a client
 * {@link SSLSocketFactory} that trusts it. Nothing is checked into the repo:
 * the keystore lives in a JUnit temp directory and dies with the test run.
 */
final class TestTlsCerts {

    static final String PASSWORD = "changeit";
    static final String ALIAS = "dieseldb-test";

    /** The generated keystore location and its password. */
    record Keystore(Path path, String password) {
    }

    private TestTlsCerts() {
    }

    /**
     * Runs {@code keytool -genkeypair} to create a PKCS12 keystore with a
     * single RSA-2048 self-signed certificate for {@code CN=localhost}
     * (SAN: dns:localhost, ip:127.0.0.1).
     *
     * @param dir directory that will hold the keystore file
     * @return the keystore path and password
     * @throws IOException          if keytool fails or is not found
     * @throws InterruptedException if the keytool process is interrupted
     */
    static Keystore generate(Path dir) throws IOException, InterruptedException {
        Path keystore = dir.resolve("test-keystore.p12");
        Path keytool = Path.of(System.getProperty("java.home"), "bin",
                isWindows() ? "keytool.exe" : "keytool");
        if (!Files.isExecutable(keytool)) {
            throw new IOException("keytool not found or not executable: " + keytool);
        }
        List<String> command = List.of(
                keytool.toString(),
                "-genkeypair",
                "-alias", ALIAS,
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "30",
                "-storetype", "PKCS12",
                "-keystore", keystore.toString(),
                "-storepass", PASSWORD,
                "-keypass", PASSWORD,
                "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1");
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();
        if (exit != 0 || !Files.isRegularFile(keystore)) {
            throw new IOException("keytool -genkeypair failed (exit " + exit + "): " + output);
        }
        return new Keystore(keystore, PASSWORD);
    }

    /**
     * Builds a client-side {@link SSLSocketFactory} that trusts exactly the
     * given test keystore (the self-signed certificate is its own trust
     * anchor).
     *
     * @param keystore the generated keystore
     * @return a factory producing client sockets that accept the server cert
     * @throws IOException              if the keystore cannot be read
     * @throws GeneralSecurityException if the trust managers cannot be built
     */
    static SSLSocketFactory clientFactory(Keystore keystore)
            throws IOException, GeneralSecurityException {
        KeyStore trust = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystore.path())) {
            trust.load(in, keystore.password().toCharArray());
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, tmf.getTrustManagers(), null);
        return context.getSocketFactory();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }
}
