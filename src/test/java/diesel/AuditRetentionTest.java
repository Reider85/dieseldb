package diesel;

import static org.junit.jupiter.api.Assertions.*;

import diesel.security.AuditBackend;
import diesel.security.AuditRecord;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * Engine-level retention acceptance (prompt4 #33): DIESEL_AUDIT_LOG rows
 * older than {@code audit.retention.days} are archived to a gzip cold file
 * and removed from the table — archived, never lost. The row-selection and
 * archive-writing logic is covered at unit level by
 * {@code AuditLoggerTest#retentionArchivesAndDeletesExpiredRows}.
 */
@Tag("smoke")
@ResourceLock("diesel.audit.enabled")  // shares JVM-global AuditLogger.ENABLED_KEY system property
public class AuditRetentionTest {

    @TempDir
    Path tempDir;
    private Database database;

    @BeforeEach
    void setUp() {
        System.setProperty(AuditTestSupport.ENABLED_KEY, "true");
        System.setProperty("audit.retention.days", "1");
        System.setProperty("audit.retention.interval.ms", "0");
        System.setProperty("audit.archive.dir", tempDir.resolve("audit-archive").toString());
        database = new Database();
        database.setDataDir(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        database.close();
        System.clearProperty(AuditTestSupport.ENABLED_KEY);
        System.clearProperty("audit.retention.days");
        System.clearProperty("audit.retention.interval.ms");
        System.clearProperty("audit.archive.dir");
    }

    @Test
    void expiredRowsAreArchivedAndRemovedRecentRowsKept() throws Exception {
        // Touch the logger so DIESEL_AUDIT_LOG exists, then insert rows
        // directly: one 30 days old, one current.
        AuditTestSupport.flush(database);
        var table = database.getTable(AuditBackend.AUDIT);
        table.addRow(new AuditRecord(UUID.randomUUID(),
                LocalDateTime.now().minusDays(30), "old-user", "10.0.0.1",
                "SELECT ancient", "OK", 3).toRow());
        table.addRow(new AuditRecord(UUID.randomUUID(),
                LocalDateTime.now(), "new-user", "10.0.0.2",
                "SELECT fresh", "OK", 4).toRow());
        table.saveToFile(AuditBackend.AUDIT);

        // A live statement wakes the consumer; with interval=0 its next
        // pass runs retention immediately.
        database.executeQuery("CREATE TABLE RET_T (ID INTEGER)", null);
        AuditTestSupport.flush(database);

        // The consumer thread runs retention asynchronously; poll briefly.
        List<Map<String, Object>> rows = null;
        for (int i = 0; i < 20; i++) {
            Thread.sleep(250);
            rows = database.getTable(AuditBackend.AUDIT).getRows();
            if (rows.size() <= 2) { // CREATE TABLE row + fresh audit row
                break;
            }
        }
        assertNotNull(rows);
        boolean oldGone = rows.stream().noneMatch(r -> "old-user".equals(r.get("USER_NAME")));
        boolean freshKept = rows.stream().anyMatch(r -> "new-user".equals(r.get("USER_NAME")));
        assertTrue(oldGone, "the 30-day-old row must be pruned from the table");
        assertTrue(freshKept, "the current row must be kept");

        // ...and it must live on in the gzip cold archive.
        Path archiveDir = tempDir.resolve("audit-archive");
        Path cwdArchive = Path.of("data", "audit-archive");
        Path found = findArchive(archiveDir);
        if (found == null) {
            found = findArchive(cwdArchive);
        }
        assertNotNull(found, "a gzip cold-archive file must exist under " + archiveDir + " or " + cwdArchive);
        assertTrue(Files.size(found) > 0, "the archive file must not be empty");
    }

    private static Path findArchive(Path dir) throws Exception {
        if (!Files.isDirectory(dir)) {
            return null;
        }
        try (var stream = Files.list(dir)) {
            return stream.filter(p -> p.getFileName().toString().startsWith("audit-")
                            && p.getFileName().toString().endsWith(".log.gz"))
                    .findFirst().orElse(null);
        }
    }
}