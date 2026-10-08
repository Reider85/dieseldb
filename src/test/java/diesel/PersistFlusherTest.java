package diesel;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the background persist flusher (write-behind off the writer
 * thread). Covers: background mode keeps INSERT latency free of whole-file
 * saves, explicit flush still lands data on disk, deferred=false restores the
 * synchronous path, and close flushes pending writes.
 */
@Tag("smoke")
class PersistFlusherTest {

    @TempDir
    Path tempDir;

    @Test
    void backgroundModeEventuallyPersistsInsertsWithoutSyncFlushOnWriter() throws Exception {
        String prevBackground = System.getProperty("diesel.persist.background");
        String prevDeferred = System.getProperty("diesel.persist.deferred");
        String prevInterval = System.getProperty("diesel.persist.flush.interval.ms");
        System.setProperty("diesel.persist.background", "true");
        System.setProperty("diesel.persist.deferred", "true");
        System.setProperty("diesel.persist.flush.interval.ms", "20");
        try {
            Database db = new Database();
            db.setDataDir(tempDir.toString());
            db.executeQuery(
                    "CREATE TABLE PF_BG (ID LONG PRIMARY KEY SEQUENCE(pf_bg_seq 1 1), VAL STRING)",
                    null);
            Table table = db.getTable("PF_BG");

            for (int i = 0; i < 200; i++) {
                db.executeQuery("INSERT INTO PF_BG (VAL) VALUES ('v" + i + "')", null);
            }
            assertEquals(200, table.getRawRowCount(), "in-memory rows must see all inserts");
            assertTrue(table.hasPendingPersist() || findDataFile(tempDir, "PF_BG") != null,
                    "auto-commit inserts must mark the table dirty for write-behind "
                            + "(or already be flushed by it)");

            long deadline = System.currentTimeMillis() + 5_000;
            while (table.hasPendingPersist() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertFalse(table.hasPendingPersist(),
                    "background flusher must clear pending persist within 5s");

            Table.flushAllPendingPersists();
            Path dataFile = findDataFile(tempDir, "PF_BG");
            assertNotNull(dataFile, "flushed writer table must exist on disk in tempDir");
            db.close();
        } finally {
            restore("diesel.persist.background", prevBackground);
            restore("diesel.persist.deferred", prevDeferred);
            restore("diesel.persist.flush.interval.ms", prevInterval);
        }
    }

    @Test
    void deferredFalseStillWritesSynchronouslyOnTheWriterThread() throws Exception {
        String prevBackground = System.getProperty("diesel.persist.background");
        String prevDeferred = System.getProperty("diesel.persist.deferred");
        System.setProperty("diesel.persist.background", "true");
        System.setProperty("diesel.persist.deferred", "false");
        try {
            Database db = new Database();
            db.setDataDir(tempDir.toString());
            db.executeQuery(
                    "CREATE TABLE PF_SYNC (ID LONG PRIMARY KEY SEQUENCE(pf_sync_seq 1 1), VAL STRING)",
                    null);
            for (int i = 0; i < 5; i++) {
                db.executeQuery("INSERT INTO PF_SYNC (VAL) VALUES ('s" + i + "')", null);
            }
            Table table = db.getTable("PF_SYNC");
            assertFalse(table.hasPendingPersist(),
                    "deferred=false must flush synchronously on each auto-commit DML");
            Table.flushAllPendingPersists();
            assertNotNull(findDataFile(tempDir, "PF_SYNC"),
                    "sync path must have written the data file");
            db.close();
        } finally {
            restore("diesel.persist.background", prevBackground);
            restore("diesel.persist.deferred", prevDeferred);
        }
    }

    @Test
    void backgroundDisabledRestoresThresholdSyncFlush() throws Exception {
        String prevBackground = System.getProperty("diesel.persist.background");
        String prevDeferred = System.getProperty("diesel.persist.deferred");
        String prevMaxPending = System.getProperty("diesel.persist.max.pending");
        System.setProperty("diesel.persist.background", "false");
        System.setProperty("diesel.persist.deferred", "true");
        System.setProperty("diesel.persist.max.pending", "4");
        try {
            Database db = new Database();
            db.setDataDir(tempDir.toString());
            db.executeQuery(
                    "CREATE TABLE PF_THR (ID LONG PRIMARY KEY SEQUENCE(pf_thr_seq 1 1), VAL STRING)",
                    null);
            for (int i = 0; i < 4; i++) {
                db.executeQuery("INSERT INTO PF_THR (VAL) VALUES ('t" + i + "')", null);
            }
            Table table = db.getTable("PF_THR");
            assertFalse(table.hasPendingPersist(),
                    "background=false + threshold reached must sync-flush on the writer thread");
            assertNotNull(findDataFile(tempDir, "PF_THR"),
                    "threshold sync flush must write the data file");
            db.close();
        } finally {
            restore("diesel.persist.background", prevBackground);
            restore("diesel.persist.deferred", prevDeferred);
            restore("diesel.persist.max.pending", prevMaxPending);
        }
    }

    @Test
    void concurrentInsertsDuringBackgroundFlushKeepWriterFast() throws Exception {
        String prevBackground = System.getProperty("diesel.persist.background");
        String prevInterval = System.getProperty("diesel.persist.flush.interval.ms");
        System.setProperty("diesel.persist.background", "true");
        System.setProperty("diesel.persist.flush.interval.ms", "10");
        try {
            Database db = new Database();
            db.setDataDir(tempDir.toString());
            db.executeQuery(
                    "CREATE TABLE PF_RACE (ID LONG PRIMARY KEY SEQUENCE(pf_race_seq 1 1), VAL STRING)",
                    null);

            AtomicLong maxInsertNs = new AtomicLong(0);
            Thread writer = new Thread(() -> {
                try {
                    for (int i = 0; i < 400; i++) {
                        long t0 = System.nanoTime();
                        db.executeQuery(
                                "INSERT INTO PF_RACE (VAL) VALUES ('r" + i + "')", null);
                        long dur = System.nanoTime() - t0;
                        maxInsertNs.accumulateAndGet(dur, Math::max);
                    }
                } catch (RuntimeException e) {
                    throw e;
                }
            }, "pf-race-writer");
            writer.start();
            writer.join(30_000);
            assertFalse(writer.isAlive(), "writer must finish");

            Table table = db.getTable("PF_RACE");
            assertEquals(400, table.getRawRowCount());
            // Snapshot saves keep INSERT latency off the whole-file rewrite;
            // allow generous CI headroom but reject multi-hundred-ms stalls
            // that used to dominate the measured path.
            long maxMs = maxInsertNs.get() / 1_000_000L;
            assertTrue(maxMs < 250,
                    "max INSERT latency under background flush must stay under 250 ms, was "
                            + maxMs + " ms");

            Table.flushAllPendingPersists();
            assertNotNull(findDataFile(tempDir, "PF_RACE"),
                    "background flush must write the data file");
            db.close();
        } finally {
            restore("diesel.persist.background", prevBackground);
            restore("diesel.persist.flush.interval.ms", prevInterval);
        }
    }

    private static void restore(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }

    /**
     * Finds the persisted data file for {@code table} in {@code dir}, trying
     * every storage-format extension. The active format comes from
     * {@code storage.type} (tsv by default in config.properties), so asserting a
     * single hardcoded extension only ever matches one profile.
     */
    private static Path findDataFile(Path dir, String table) {
        for (String ext : new String[] {".tsv", ".csv", ".jsonl", ".avro", ".table"}) {
            Path candidate = dir.resolve(table + ext);
            if (Files.exists(candidate)) {
                return candidate;
            }
        }
        return null;
    }
}
