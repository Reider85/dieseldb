package diesel.security;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AuditLogger} over an in-memory backend
 * (prompt4 #33): disabled no-op, row round-trip, queue overflow drop
 * metric, non-blocking record, close() final drain, archive encoding.
 */
@Tag("smoke")
public class AuditLoggerTest {

    /** In-memory fake of the DIESEL_AUDIT_LOG table. */
    private static final class MemBackend implements AuditBackend {
        final List<Map<String, Object>> rows = new ArrayList<>();
        volatile CountDownLatch blockAppend;
        volatile int batchCount;

        @Override
        public List<Map<String, Object>> readAll() {
            synchronized (rows) {
                return new ArrayList<>(rows);
            }
        }

        @Override
        public void appendRow(Map<String, Object> row) {
            appendBatch(List.of(row));
        }

        @Override
        public void appendBatch(List<Map<String, Object>> batch) {
            CountDownLatch latch = blockAppend;
            if (latch != null) {
                try {
                    latch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            synchronized (rows) {
                rows.addAll(batch);
            }
            batchCount++;
        }

        @Override
        public void deleteRows(List<String> ids) {
            synchronized (rows) {
                rows.removeIf(row -> ids.contains(String.valueOf(row.get("ID"))));
            }
        }
    }

    @Test
    void disabledLoggerRecordsNothing() {
        MemBackend backend = new MemBackend();
        AuditLogger logger = new AuditLogger(backend, false, 90, 100, 1000,
                Path.of("target", "audit-test"), Clock.systemUTC());
        assertFalse(logger.isEnabled());
        logger.record("admin", "127.0.0.1", "SELECT 1", "OK", 1);
        logger.flushForTest();
        assertTrue(backend.rows.isEmpty(), "disabled logger must not write rows");
        assertEquals(0, logger.getRecordedCount());
        logger.close();
    }

    @Test
    void recordRoundTripsThroughBackend() {
        MemBackend backend = new MemBackend();
        AuditLogger logger = new AuditLogger(backend, true, 90, 100, 1000,
                Path.of("target", "audit-test"), Clock.systemUTC());
        try {
            logger.record("admin", "10.0.0.1", "SELECT * FROM USERS", "OK", 5);
            logger.flushForTest();
            assertEquals(1, backend.rows.size());
            AuditRecord decoded = AuditRecord.fromRow(backend.rows.get(0));
            assertEquals("admin", decoded.getUserName());
            assertEquals("10.0.0.1", decoded.getSourceIp());
            assertEquals("SELECT * FROM USERS", decoded.getQuery());
            assertEquals("OK", decoded.getStatus());
            assertEquals(5, decoded.getDurationMs());
            assertEquals(1, logger.getRecordedCount());
            assertEquals(0, logger.getDroppedCount());
        } finally {
            logger.close();
        }
    }

    @Test
    void queueOverflowDropsGracefullyWithMetric() throws Exception {
        MemBackend backend = new MemBackend();
        CountDownLatch block = new CountDownLatch(1);
        backend.blockAppend = block;
        // capacity 4: the consumer thread picks the first record and blocks
        // in appendBatch, so the queue holds at most 4 more; the rest drop.
        AuditLogger logger = new AuditLogger(backend, true, 90, 4, 60_000,
                Path.of("target", "audit-test"), Clock.systemUTC());
        try {
            for (int i = 0; i < 50; i++) {
                logger.record("u" + i, "ip", "SELECT " + i, "OK", 1);
            }
            assertTrue(logger.getDroppedCount() > 0,
                    "overflow must drop records, dropped=" + logger.getDroppedCount());
            // Unblock the consumer so close() can finish.
            block.countDown();
        } finally {
            block.countDown();
            logger.close();
        }
    }

    @Test
    void recordNeverBlocksOnFullQueue() {
        MemBackend backend = new MemBackend();
        CountDownLatch block = new CountDownLatch(1);
        backend.blockAppend = block;
        AuditLogger logger = new AuditLogger(backend, true, 90, 2, 60_000,
                Path.of("target", "audit-test"), Clock.systemUTC());
        try {
            long start = System.nanoTime();
            for (int i = 0; i < 200; i++) {
                logger.record("u", "ip", "SELECT " + i, "OK", 1);
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs < 500, "producer must never block, took " + elapsedMs + "ms");
        } finally {
            block.countDown();
            logger.close();
        }
    }

    @Test
    void closeDrainsRemainingRecords() {
        MemBackend backend = new MemBackend();
        AuditLogger logger = new AuditLogger(backend, true, 90, 1000, 60_000,
                Path.of("target", "audit-test"), Clock.systemUTC());
        for (int i = 0; i < 10; i++) {
            logger.record("u", "ip", "SELECT " + i, "OK", 1);
        }
        logger.close();
        // close() performs a final synchronous drain on the caller thread
        assertFalse(backend.rows.isEmpty(), "close() must drain queued records");
        assertEquals(10, backend.rows.size());
        // second close is a no-op
        logger.close();
        assertEquals(10, backend.rows.size());
    }

    @Test
    void rowCodecRoundTrips() {
        AuditRecord original = AuditRecord.create("alice", "192.168.1.1",
                "INSERT INTO T VALUES (1)", "ERROR", 42);
        Map<String, Object> row = original.toRow();
        AuditRecord decoded = AuditRecord.fromRow(row);
        assertEquals(original.getId(), decoded.getId());
        assertEquals(original.getEventTime(), decoded.getEventTime());
        assertEquals(original.getUserName(), decoded.getUserName());
        assertEquals(original.getSourceIp(), decoded.getSourceIp());
        assertEquals(original.getQuery(), decoded.getQuery());
        assertEquals(original.getStatus(), decoded.getStatus());
        assertEquals(original.getDurationMs(), decoded.getDurationMs());
    }

    @Test
    void archiveLineEscapesPipesAndNewlines() {
        AuditRecord record = new AuditRecord(
                java.util.UUID.randomUUID(),
                java.time.LocalDateTime.of(2026, 1, 2, 3, 4, 5),
                "u", "ip", "SELECT a|b\nc", "OK", 7);
        String line = new String(AuditLogger.encodeArchiveLine(record),
                java.nio.charset.StandardCharsets.UTF_8);
        assertFalse(line.contains("a|b"), "raw pipe must be escaped");
        assertFalse(line.contains("\nc"), "raw newline must be escaped");
        assertTrue(line.endsWith("\n"));
        assertTrue(line.contains("a\\pb\\nc"));
    }

    @Test
    void retentionArchivesAndDeletesExpiredRows() {
        MemBackend backend = new MemBackend();
        // Fixed clock: now = 2026-10-20, retention 90 days -> cutoff 2026-07-22
        Clock fixed = Clock.fixed(Instant.parse("2026-10-20T12:00:00Z"), ZoneOffset.UTC);
        // Two old rows (2026-01-01) and one recent row (2026-10-19)
        backend.rows.add(new AuditRecord(java.util.UUID.randomUUID(),
                java.time.LocalDateTime.of(2026, 1, 1, 0, 0), "old1", "ip",
                "SELECT 1", "OK", 1).toRow());
        backend.rows.add(new AuditRecord(java.util.UUID.randomUUID(),
                java.time.LocalDateTime.of(2026, 1, 2, 0, 0), "old2", "ip",
                "SELECT 2", "OK", 2).toRow());
        backend.rows.add(new AuditRecord(java.util.UUID.randomUUID(),
                java.time.LocalDateTime.of(2026, 10, 19, 0, 0), "recent", "ip",
                "SELECT 3", "OK", 3).toRow());

        AuditLogger logger = new AuditLogger(backend, true, 90, 100, 60_000,
                Path.of("target", "audit-retention"), fixed);
        try {
            int archived = logger.runRetentionForTest();
            assertEquals(2, archived, "two rows older than 90 days must be archived");
            assertEquals(1, backend.rows.size(), "recent row must remain");
            assertEquals("recent", backend.rows.get(0).get("USER_NAME"));
            // The archive file must exist and be non-empty (gzip)
            Path archiveDir = Path.of("target", "audit-retention");
            java.io.File[] files = archiveDir.toFile()
                    .listFiles((d, n) -> n.startsWith("audit-") && n.endsWith(".log.gz"));
            assertNotNull(files);
            assertEquals(1, files.length);
            assertTrue(files[0].length() > 0, "archive file must not be empty");
        } finally {
            logger.close();
        }
    }

    @Test
    void retentionDisabledWhenDaysNonPositive() {
        MemBackend backend = new MemBackend();
        backend.rows.add(new AuditRecord(java.util.UUID.randomUUID(),
                java.time.LocalDateTime.of(2020, 1, 1, 0, 0), "ancient", "ip",
                "SELECT 1", "OK", 1).toRow());
        AuditLogger logger = new AuditLogger(backend, true, 0, 100, 60_000,
                Path.of("target", "audit-retention-off"), Clock.systemUTC());
        try {
            assertEquals(0, logger.runRetentionForTest());
            assertEquals(1, backend.rows.size(), "retention <= 0 must prune nothing");
        } finally {
            logger.close();
        }
    }
}