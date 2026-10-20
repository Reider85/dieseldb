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

    /** Keystore pair for expired-cert tests: server keystore + CA cert PEM. */
    record ExpiredKeystores(Keystore server, Path caPem) {
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

    /**
     * Exports the certificate from a keystore as a PEM file.
     *
     * @param keystore the keystore file
     * @param alias the certificate alias
     * @param password the keystore password
     * @param outputFile the PEM file to write
     * @throws IOException if keytool fails or files cannot be written
     * @throws InterruptedException if the keytool process is interrupted
     */
    static void exportCertificatePem(Path keystore, String alias, String password, Path outputFile)
            throws IOException, InterruptedException {
        Path keytool = Path.of(System.getProperty("java.home"), "bin",
                isWindows() ? "keytool.exe" : "keytool");
        if (!Files.isExecutable(keytool)) {
            throw new IOException("keytool not found or not executable: " + keytool);
        }
        List<String> command = List.of(
                keytool.toString(),
                "-exportcert",
                "-rfc",
                "-alias", alias,
                "-keystore", keystore.toString(),
                "-storepass", password,
                "-file", outputFile.toString());
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();
        if (exit != 0 || !Files.isRegularFile(outputFile)) {
            throw new IOException("keytool -exportcert failed (exit " + exit + "): " + output);
        }
    }

    /**
     * Generates an expired certificate for testing expired cert rejection.
     * Creates a CA, signs a server cert with -startdate in the past and -validity 1,
     * and imports both into a server keystore.
     *
     * @param directory directory that will hold the keystore file
     * @return the keystore path and password
     * @throws IOException if keytool fails or is not found
     * @throws InterruptedException if the keytool process is interrupted
     */
    static ExpiredKeystores generateExpired(Path dir) throws IOException, InterruptedException {
        Path caKeystore = dir.resolve("ca-keystore.p12");
        Path serverKeystore = dir.resolve("expired-server-keystore.p12");
        Path serverCsr = dir.resolve("server.csr");
        Path serverCert = dir.resolve("server.crt");
        Path caCert = dir.resolve("ca.crt");
        
        Path keytool = Path.of(System.getProperty("java.home"), "bin",
                isWindows() ? "keytool.exe" : "keytool");
        if (!Files.isExecutable(keytool)) {
            throw new IOException("keytool not found or not executable: " + keytool);
        }

        // Step 1: Generate CA keystore
        List<String> caCommand = List.of(
                keytool.toString(),
                "-genkeypair",
                "-alias", "ca",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "3650",
                "-storetype", "PKCS12",
                "-keystore", caKeystore.toString(),
                "-storepass", PASSWORD,
                "-keypass", PASSWORD,
                "-dname", "CN=Test CA",
                "-ext", "bc:c");
        runKeytoolCommand(caCommand);

        // Step 2: Generate server keystore with keypair
        List<String> serverKeyCommand = List.of(
                keytool.toString(),
                "-genkeypair",
                "-alias", ALIAS,
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-storetype", "PKCS12",
                "-keystore", serverKeystore.toString(),
                "-storepass", PASSWORD,
                "-keypass", PASSWORD,
                "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1");
        runKeytoolCommand(serverKeyCommand);

        // Step 3: Generate CSR from server keystore
        List<String> csrCommand = List.of(
                keytool.toString(),
                "-certreq",
                "-alias", ALIAS,
                "-keystore", serverKeystore.toString(),
                "-storepass", PASSWORD,
                "-file", serverCsr.toString());
        runKeytoolCommand(csrCommand);

        // Step 4: Sign server cert with CA (expired validity)
        List<String> signCommand = List.of(
                keytool.toString(),
                "-gencert",
                "-alias", "ca",
                "-keystore", caKeystore.toString(),
                "-storepass", PASSWORD,
                "-infile", serverCsr.toString(),
                "-outfile", serverCert.toString(),
                "-startdate", "2020/01/01 00:00:00",
                "-validity", "1",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-rfc");
        runKeytoolCommand(signCommand);

        // Step 5: Export CA cert
        List<String> exportCaCommand = List.of(
                keytool.toString(),
                "-exportcert",
                "-alias", "ca",
                "-keystore", caKeystore.toString(),
                "-storepass", PASSWORD,
                "-file", caCert.toString(),
                "-rfc");
        runKeytoolCommand(exportCaCommand);

        // Step 6: Import CA into server keystore
        List<String> importCaCommand = List.of(
                keytool.toString(),
                "-importcert",
                "-alias", "ca",
                "-keystore", serverKeystore.toString(),
                "-storepass", PASSWORD,
                "-file", caCert.toString(),
                "-noprompt");
        runKeytoolCommand(importCaCommand);

        // Step 7: Import signed server cert
        List<String> importCertCommand = List.of(
                keytool.toString(),
                "-importcert",
                "-alias", ALIAS,
                "-keystore", serverKeystore.toString(),
                "-storepass", PASSWORD,
                "-file", serverCert.toString());
        runKeytoolCommand(importCertCommand);

        return new ExpiredKeystores(new Keystore(serverKeystore, PASSWORD), caCert);
    }

    private static void runKeytoolCommand(List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IOException("keytool command failed (exit " + exit + "): " + output);
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }
}
