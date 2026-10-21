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
import javax.net.ssl.KeyManagerFactory;
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

    /**
     * Full mTLS PKI (prompt4 #36): CA keystore, CA-signed server keystore
     * (CN=localhost), CA-signed client keystore (CN=diesel-client), and the
     * exported CA certificate PEM (for the server's {@code tls.ca.path}).
     */
    record MtlsPki(Keystore server, Keystore client, Keystore ca, Path caPem) {
    }

    /**
     * SNI multi-tenant PKI (prompt4 #36): CA + three CA-signed server
     * keystores with distinct CNs/SANs — default (CN=localhost), tenant1
     * (CN=tenant1.example.com), tenant2 (CN=tenant2.example.com).
     */
    record SniPki(Keystore defaultKs, Keystore tenant1, Keystore tenant2, Keystore ca) {
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
     * Builds a client-side {@link SSLSocketFactory} that trusts the given
     * keystore AND presents the given client identity (mTLS, prompt4 #36).
     *
     * @param trust          keystore whose certificate(s) act as trust anchors
     * @param clientIdentity keystore holding the client certificate + private key
     * @return a factory producing client sockets with a client certificate
     * @throws IOException              if a keystore cannot be read
     * @throws GeneralSecurityException if the managers cannot be built
     */
    static SSLSocketFactory clientFactory(Keystore trust, Keystore clientIdentity)
            throws IOException, GeneralSecurityException {
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(trust.path())) {
            trustStore.load(in, trust.password().toCharArray());
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        KeyStore identityStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(clientIdentity.path())) {
            identityStore.load(in, clientIdentity.password().toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(identityStore, clientIdentity.password().toCharArray());

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return context.getSocketFactory();
    }

    /**
     * Generates a client keystore whose certificate IS signed by the given
     * CA but expired (startdate 2020, validity 1 day) — the client will
     * present it (the issuer is acceptable) and the server must reject it
     * at verification (prompt4 #36 mTLS "presented but invalid" tests).
     *
     * @param dir directory that will hold the keystore file
     * @param ca  the CA keystore that signs the client certificate
     * @return the expired client keystore
     * @throws IOException          if keytool fails or is not found
     * @throws InterruptedException if the keytool process is interrupted
     */
    static Keystore expiredClientKeystore(Path dir, Keystore ca)
            throws IOException, InterruptedException {
        Path keytool = keytool();
        Path keystore = dir.resolve("expired-client.p12");
        Path csr = dir.resolve("expired-client.csr");
        Path cert = dir.resolve("expired-client.crt");
        Path caCert = dir.resolve("expired-client-ca.crt");

        runKeytoolCommand(List.of(keytool.toString(),
                "-genkeypair", "-alias", "client", "-keyalg", "RSA", "-keysize", "2048",
                "-storetype", "PKCS12", "-keystore", keystore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", "CN=expired-client"));
        runKeytoolCommand(List.of(keytool.toString(),
                "-certreq", "-alias", "client", "-keystore", keystore.toString(),
                "-storepass", PASSWORD, "-file", csr.toString()));
        runKeytoolCommand(List.of(keytool.toString(),
                "-gencert", "-alias", "ca", "-keystore", ca.path().toString(),
                "-storepass", ca.password(), "-infile", csr.toString(),
                "-outfile", cert.toString(),
                "-startdate", "2020/01/01 00:00:00", "-validity", "1", "-rfc"));
        runKeytoolCommand(List.of(keytool.toString(),
                "-exportcert", "-alias", "ca", "-keystore", ca.path().toString(),
                "-storepass", ca.password(), "-file", caCert.toString(), "-rfc"));
        runKeytoolCommand(List.of(keytool.toString(),
                "-importcert", "-alias", "ca", "-keystore", keystore.toString(),
                "-storepass", PASSWORD, "-file", caCert.toString(), "-noprompt"));
        runKeytoolCommand(List.of(keytool.toString(),
                "-importcert", "-alias", "client", "-keystore", keystore.toString(),
                "-storepass", PASSWORD, "-file", cert.toString()));
        return new Keystore(keystore, PASSWORD);
    }

    /**
     * Generates a full mTLS PKI (prompt4 #36): a CA, a CA-signed server
     * certificate (CN=localhost), and a CA-signed client certificate
     * (CN=diesel-client), all in PKCS12 keystores, plus the CA cert PEM.
     *
     * @param dir directory that will hold the keystore files
     * @return the server keystore, client keystore, and CA certificate PEM
     * @throws IOException          if keytool fails or is not found
     * @throws InterruptedException if the keytool process is interrupted
     */
    static MtlsPki generateMtlsPki(Path dir) throws IOException, InterruptedException {
        Path keytool = keytool();
        Path caKeystore = dir.resolve("mtls-ca.p12");
        Path caCert = dir.resolve("mtls-ca.crt");
        Path serverKeystore = dir.resolve("mtls-server.p12");
        Path clientKeystore = dir.resolve("mtls-client.p12");

        runKeytoolCommand(List.of(keytool.toString(),
                "-genkeypair", "-alias", "ca", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "3650", "-storetype", "PKCS12",
                "-keystore", caKeystore.toString(), "-storepass", PASSWORD,
                "-keypass", PASSWORD, "-dname", "CN=DieselDB Test CA", "-ext", "bc:c"));

        signedLeaf(keytool, serverKeystore, "server", "CN=localhost",
                "SAN=dns:localhost,ip:127.0.0.1", caKeystore, caCert);
        signedLeaf(keytool, clientKeystore, "client", "CN=diesel-client",
                null, caKeystore, caCert);

        return new MtlsPki(new Keystore(serverKeystore, PASSWORD),
                new Keystore(clientKeystore, PASSWORD),
                new Keystore(caKeystore, PASSWORD), caCert);
    }

    /**
     * Generates an SNI multi-tenant PKI (prompt4 #36): a CA and three
     * CA-signed server keystores with distinct CNs/SANs.
     *
     * @param dir directory that will hold the keystore files
     * @return the default, tenant1, and tenant2 keystores
     * @throws IOException          if keytool fails or is not found
     * @throws InterruptedException if the keytool process is interrupted
     */
    static SniPki generateSniPki(Path dir) throws IOException, InterruptedException {
        Path keytool = keytool();
        Path caKeystore = dir.resolve("sni-ca.p12");
        Path caCert = dir.resolve("sni-ca.crt");

        runKeytoolCommand(List.of(keytool.toString(),
                "-genkeypair", "-alias", "ca", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "3650", "-storetype", "PKCS12",
                "-keystore", caKeystore.toString(), "-storepass", PASSWORD,
                "-keypass", PASSWORD, "-dname", "CN=SNI Test CA", "-ext", "bc:c"));

        Path defaultKs = dir.resolve("sni-default.p12");
        Path tenant1Ks = dir.resolve("sni-tenant1.p12");
        Path tenant2Ks = dir.resolve("sni-tenant2.p12");

        signedLeaf(keytool, defaultKs, "server", "CN=localhost",
                "SAN=dns:localhost,ip:127.0.0.1", caKeystore, caCert);
        signedLeaf(keytool, tenant1Ks, "server", "CN=tenant1.example.com",
                "SAN=dns:tenant1.example.com", caKeystore, caCert);
        signedLeaf(keytool, tenant2Ks, "server", "CN=tenant2.example.com",
                "SAN=dns:tenant2.example.com", caKeystore, caCert);

        return new SniPki(new Keystore(defaultKs, PASSWORD),
                new Keystore(tenant1Ks, PASSWORD), new Keystore(tenant2Ks, PASSWORD),
                new Keystore(caKeystore, PASSWORD));
    }

    /**
     * Generates a keypair in {@code keystorePath}, has the CA sign it, and
     * imports the CA cert + signed certificate back (7-step keytool recipe,
     * same as {@link #generateExpired} but with valid dates).
     */
    private static void signedLeaf(Path keytool, Path keystorePath, String alias,
            String dname, String san, Path caKeystore, Path caCert)
            throws IOException, InterruptedException {
        Path csr = keystorePath.resolveSibling(alias + ".csr");
        Path cert = keystorePath.resolveSibling(alias + ".crt");

        // 1. keypair
        List<String> genkey = new java.util.ArrayList<>(List.of(keytool.toString(),
                "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048",
                "-storetype", "PKCS12", "-keystore", keystorePath.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", dname));
        if (san != null) {
            genkey.add("-ext");
            genkey.add(san);
        }
        runKeytoolCommand(genkey);
        // 2. CSR
        runKeytoolCommand(List.of(keytool.toString(),
                "-certreq", "-alias", alias, "-keystore", keystorePath.toString(),
                "-storepass", PASSWORD, "-file", csr.toString()));
        // 3. CA signs
        List<String> sign = new java.util.ArrayList<>(List.of(keytool.toString(),
                "-gencert", "-alias", "ca", "-keystore", caKeystore.toString(),
                "-storepass", PASSWORD, "-infile", csr.toString(),
                "-outfile", cert.toString(), "-validity", "365", "-rfc"));
        if (san != null) {
            sign.add("-ext");
            sign.add(san);
        }
        runKeytoolCommand(sign);
        // 4. export CA
        Path caCopy = keystorePath.resolveSibling(alias + "-ca.crt");
        runKeytoolCommand(List.of(keytool.toString(),
                "-exportcert", "-alias", "ca", "-keystore", caKeystore.toString(),
                "-storepass", PASSWORD, "-file", caCopy.toString(), "-rfc"));
        Files.copy(caCopy, caCert, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        // 5. import CA into leaf keystore
        runKeytoolCommand(List.of(keytool.toString(),
                "-importcert", "-alias", "ca", "-keystore", keystorePath.toString(),
                "-storepass", PASSWORD, "-file", caCert.toString(), "-noprompt"));
        // 6. import signed cert
        runKeytoolCommand(List.of(keytool.toString(),
                "-importcert", "-alias", alias, "-keystore", keystorePath.toString(),
                "-storepass", PASSWORD, "-file", cert.toString()));
    }

    private static Path keytool() throws IOException {
        Path keytool = Path.of(System.getProperty("java.home"), "bin",
                isWindows() ? "keytool.exe" : "keytool");
        if (!Files.isExecutable(keytool)) {
            throw new IOException("keytool not found or not executable: " + keytool);
        }
        return keytool;
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
