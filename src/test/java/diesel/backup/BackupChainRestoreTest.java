package diesel.backup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RestoreChain acceptance (prompt4 #42): full backup + 3 incrementals applied
 * in order reproduce the final database state byte-for-byte.
 */
@Tag("storage")
public class BackupChainRestoreTest {

    private static final int PAGE_SIZE = 8192;
    private static final int PAGE_COUNT = 60;

    @TempDir
    Path tempDir;

    private Path databaseDir;
    private Path fullDir;
    private Path inc1;
    private Path inc2;
    private Path inc3;
    private Path restoreDir;
    /** Accumulating per-page mutations, so each version builds on the previous one. */
    private final java.util.Map<Long, String> mutations = new java.util.TreeMap<>();

    @BeforeEach
    void setUp() throws IOException {
        databaseDir = tempDir.resolve("database");
        fullDir = tempDir.resolve("full");
        inc1 = tempDir.resolve("inc1");
        inc2 = tempDir.resolve("inc2");
        inc3 = tempDir.resolve("inc3");
        restoreDir = tempDir.resolve("restored");
        Files.createDirectories(databaseDir);
        // PhysicalBackup requires an existing checkpoint file.
        Files.writeString(databaseDir.resolve("checkpoint.json"),
                "{\"version\": 1, \"lsn\": 1000, \"timestamp\": 0}");
    }

    @Test
    void fullPlusThreeIncrementalsRestoresFinalState() throws Exception {
        // v0: initial state.
        writePagesDb();
        writeTable("users.table", "id,name\n1,alice\n");
        writeTable("orders.table", "id,amount\n1,100\n");

        // Step-2 full backup (file-level; pages.db intentionally not part of it).
        new PhysicalBackup(databaseDir, fullDir).backup();

        // v1: mutate pages 1-9 + users.table.
        mutatePages(1, 10, "v1");
        writePagesDb();
        writeTable("users.table", "id,name\n1,alice\n2,bob\n");
        new IncrementalBackup(databaseDir, inc1, fullDir).backup();

        // v2: mutate page 5 again, delete orders.table, add items.table.
        mutations.put(5L, "v2-page-5");
        writePagesDb();
        Files.delete(databaseDir.resolve("orders.table"));
        writeTable("items.table", "sku,qty\nA1,3\n");
        new IncrementalBackup(databaseDir, inc2, inc1).backup();

        // v3: mutate page 42 + items.table.
        mutations.put(42L, "v3-page-42");
        writePagesDb();
        writeTable("items.table", "sku,qty\nA1,3\nA2,5\n");
        new IncrementalBackup(databaseDir, inc3, inc2).backup();

        // Snapshot the expected final state.
        byte[] expectedPages = Files.readAllBytes(databaseDir.resolve(IncrementalBackup.PAGE_FILE));
        String expectedUsers = Files.readString(databaseDir.resolve("users.table"));
        String expectedItems = Files.readString(databaseDir.resolve("items.table"));

        IncrementalBackup.restoreChain(fullDir, List.of(inc1, inc2, inc3), restoreDir);

        assertArrayEquals(expectedPages,
                Files.readAllBytes(restoreDir.resolve(IncrementalBackup.PAGE_FILE)),
                "restored pages.db must match the final state byte-for-byte");
        assertEquals(expectedUsers, Files.readString(restoreDir.resolve("users.table")));
        assertEquals(expectedItems, Files.readString(restoreDir.resolve("items.table")));
        assertFalse(Files.exists(restoreDir.resolve("orders.table")),
                "file deleted in inc2 must be deleted by the chain");
    }

    @Test
    void pageDeltaApplyIsIdempotent() throws Exception {
        writePagesDb();
        writeTable("users.table", "id\n1\n");
        new PhysicalBackup(databaseDir, fullDir).backup();
        mutatePages(7, 8, "x");
        writePagesDb();
        new IncrementalBackup(databaseDir, inc1, fullDir).backup();

        byte[] expected = Files.readAllBytes(databaseDir.resolve(IncrementalBackup.PAGE_FILE));

        IncrementalBackup.applyIncremental(inc1, restoreDir);
        IncrementalBackup.applyIncremental(inc1, restoreDir); // second apply = no-op overwrite

        assertArrayEquals(expected, Files.readAllBytes(restoreDir.resolve(IncrementalBackup.PAGE_FILE)));
    }

    @Test
    void rejectsNonIncrementalBackupDir() throws IOException {
        writeTable("users.table", "id\n1\n");
        new PhysicalBackup(databaseDir, fullDir).backup();
        assertThrows(IOException.class,
                () -> IncrementalBackup.applyIncremental(fullDir, restoreDir));
    }

    @Test
    void rejectsMissingManifest() {
        assertThrows(IOException.class,
                () -> IncrementalBackup.applyIncremental(tempDir.resolve("nope"), restoreDir));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void mutatePages(int fromInclusive, int toExclusive, String tag) {
        for (int i = fromInclusive; i < toExclusive; i++) {
            mutations.put((long) i, tag + "-page-" + i);
        }
    }

    /** Rewrites pages.db deterministically; mutated pages get an extra tuple + LSN. */
    private void writePagesDb() throws IOException {
        Path pageFile = databaseDir.resolve(IncrementalBackup.PAGE_FILE);
        try (FileChannel ch = FileChannel.open(pageFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
            for (long pageNum = 0; pageNum < PAGE_COUNT; pageNum++) {
                Page page = new Page(new PageId(0, 1, pageNum), PAGE_SIZE);
                page.insert(("base-" + pageNum).getBytes(StandardCharsets.UTF_8));
                String extra = mutations.get(pageNum);
                if (extra != null) {
                    page.insert(extra.getBytes(StandardCharsets.UTF_8));
                    page.setLsn(5000 + pageNum);
                }
                buf.clear();
                page.writeTo(buf);
                buf.flip();
                while (buf.hasRemaining()) {
                    ch.write(buf, pageNum * (long) PAGE_SIZE + buf.position());
                }
            }
        }
    }

    private void writeTable(String name, String content) throws IOException {
        Files.writeString(databaseDir.resolve(name), content);
    }
}
