package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import diesel.net.SniSelector;
import diesel.security.Tenant;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Prompt4 #36 unit tests: SniSelector tenant resolution from config keys and
 * DIESEL_TENANTS rows, hostname normalization, config-over-table priority,
 * and fail-fast behaviour on missing keystores.
 */
@Tag("smoke")
class SniSelectorTest {

    private static Path keystoreDir;
    private static TestTlsCerts.Keystore defaultKs;
    private static TestTlsCerts.Keystore tenantKs;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void generateKeystoresOnce() throws Exception {
        keystoreDir = Files.createTempDirectory("dieseldb-sni-unit");
        defaultKs = TestTlsCerts.generate(Files.createDirectories(keystoreDir.resolve("default")));
        tenantKs = TestTlsCerts.generate(Files.createDirectories(keystoreDir.resolve("tenant")));
    }

    @AfterAll
    static void removeKeystoresOnce() {
        deleteRecursively(keystoreDir);
    }

    private Properties baseProps() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.keystore.path", defaultKs.path().toString());
        props.setProperty("tls.keystore.password", defaultKs.password());
        return props;
    }

    @Test
    void noTenantsYieldsNull() throws Exception {
        assertNull(SniSelector.fromConfig(baseProps(), null),
                "without config keys or table rows the plain context is used");
        assertNull(SniSelector.fromConfig(baseProps(), List.of()));
    }

    @Test
    void configKeysBuildTenantSelector() throws Exception {
        Properties props = baseProps();
        props.setProperty("tls.sni.tenant1.example.com.keystore.path",
                tenantKs.path().toString());
        props.setProperty("tls.sni.tenant1.example.com.keystore.password",
                tenantKs.password());
        SniSelector selector = SniSelector.fromConfig(props, null);
        assertNotNull(selector);
        assertEquals(1, selector.tenantCount());
        assertNotNull(selector.createServerContext(props),
                "SNI context builds with the default keystore as fallback");
    }

    @Test
    void tableRowsMergeWithConfigAndConfigWins() throws Exception {
        Properties props = baseProps();
        props.setProperty("tls.sni.shared.example.com.keystore.path",
                defaultKs.path().toString());
        props.setProperty("tls.sni.shared.example.com.keystore.password",
                defaultKs.password());
        Tenant configTenant = tenant("from-config", "shared.example.com",
                tenantKs.path().toString(), tenantKs.password());
        Tenant tableOnly = tenant("table-only", "table.example.com",
                tenantKs.path().toString(), tenantKs.password());
        SniSelector selector = SniSelector.fromConfig(props,
                List.of(configTenant, tableOnly));
        assertNotNull(selector);
        assertEquals(2, selector.tenantCount(),
                "config hostname overrides the table row; table-only row is added");
    }

    @Test
    void hostnameMatchingIsCaseInsensitive() throws Exception {
        Properties props = baseProps();
        props.setProperty("tls.sni.Tenant1.Example.COM.keystore.path",
                tenantKs.path().toString());
        props.setProperty("tls.sni.Tenant1.Example.COM.keystore.password",
                tenantKs.password());
        SniSelector selector = SniSelector.fromConfig(props,
                List.of(tenant("t", "tenant1.example.com", tenantKs.path().toString(), "")));
        assertNotNull(selector);
        assertEquals(1, selector.tenantCount(),
                "same hostname with different case collapses to one tenant");
    }

    @Test
    void tenantsWithoutDefaultKeystoreFailFast() {
        Properties props = new Properties();
        props.setProperty("tls.sni.tenant1.example.com.keystore.path",
                tenantKs.path().toString());
        assertThrows(IOException.class, () -> SniSelector.fromConfig(props, null));
    }

    @Test
    void missingTenantKeystoreFailsFast() {
        Properties props = baseProps();
        props.setProperty("tls.sni.broken.example.com.keystore.path",
                tempDir.resolve("missing.p12").toString());
        assertThrows(IOException.class, () -> SniSelector.fromConfig(props, null));
    }

    private static Tenant tenant(String name, String sni, String path, String password) {
        return new Tenant(UUID.randomUUID(), name, sni, path, password, LocalDateTime.now());
    }

    private static void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // Best-effort temp-dir cleanup.
                }
            });
        } catch (IOException ignored) {
            // Best-effort temp-dir cleanup.
        }
    }
}
