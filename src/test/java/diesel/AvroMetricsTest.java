package diesel;

import diesel.storage.avro.AvroMetrics;
import diesel.storage.avro.AvroMetrics.Alert;
import diesel.storage.avro.AvroMetrics.MetricsSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 94 AVRO metrics tests: config resolution (sysprop → config.properties → defaults),
 * read/write recording, compression ratio, snapshot immutability, JMX MBean exposure,
 * Prometheus text-format export, and alerting on anomalies.
 */
@Tag("storage")
@StorageType("avro")
class AvroMetricsTest {

    private static final String[] SYS_PROPS = {
            AvroMetrics.ENABLED_KEY,
            AvroMetrics.JMX_ENABLED_KEY,
            AvroMetrics.PROMETHEUS_ENABLED_KEY,
            AvroMetrics.THROUGHPUT_WINDOW_MS_KEY,
            AvroMetrics.ALERT_SLOW_READ_MS_KEY,
            AvroMetrics.ALERT_SLOW_WRITE_MS_KEY,
            AvroMetrics.ALERT_LOW_COMPRESSION_KEY,
            AvroMetrics.ALERT_HIGH_ERROR_RATE_KEY,
            AvroMetrics.ALERT_MAX_TABLES_KEY,
            AvroMetrics.JMX_OBJECT_NAME_KEY,
            AvroMetrics.CONFIG_FILE_KEY,
    };

    @TempDir
    Path tempDir;

    @AfterEach
    void cleanSysProps() {
        for (String key : SYS_PROPS) {
            System.clearProperty(key);
        }
        AvroMetrics.getInstance().resetMetrics();
    }

    // ─── Config resolution ────────────────────────────────────────────

    @Test
    void configDefaults() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resolveConfig();
        // defaults are applied — no exception means config resolved
        MetricsSnapshot snap = m.snapshot();
        assertNotNull(snap);
        assertEquals(0, snap.totalReads());
    }

    @Test
    void configSyspropOverrides() {
        System.setProperty(AvroMetrics.ALERT_SLOW_READ_MS_KEY, "999");
        AvroMetrics.getInstance().resolveConfig();
        // after resolve, slow read threshold should be 999
        // trigger an alert to verify
        AvroMetrics.getInstance().resetMetrics();
        AvroMetrics.getInstance().recordRead("t", 100, 1_000_000_000L); // 1000ms > 999ms
        List<Alert> alerts = AvroMetrics.getInstance().getRecentAlerts(10);
        boolean found = alerts.stream().anyMatch(a -> "slow_read".equals(a.metric()));
        assertTrue(found, "Expected slow_read alert with sysprop threshold 999");
    }

    @Test
    void configInvalidValueFallsBackToDefault() {
        System.setProperty(AvroMetrics.ALERT_SLOW_READ_MS_KEY, "not_a_number");
        AvroMetrics.getInstance().resolveConfig();
        // should fall back to default (5000) without exception
        AvroMetrics.getInstance().resetMetrics();
        AvroMetrics.getInstance().recordRead("t", 100, 2_000_000_000L); // 2000ms < 5000ms default
        List<Alert> alerts = AvroMetrics.getInstance().getRecentAlerts(10);
        boolean found = alerts.stream().anyMatch(a -> "slow_read".equals(a.metric()));
        assertFalse(found, "Invalid sysprop should fall back to 5000ms default");
    }

    @Test
    void configFileOverrideIsHonored() throws Exception {
        Properties p = new Properties();
        p.setProperty(AvroMetrics.ALERT_SLOW_READ_MS_KEY, "100");
        File cfg = tempDir.resolve("metrics-test.properties").toFile();
        try (var out = Files.newOutputStream(cfg.toPath())) {
            p.store(out, "test");
        }
        System.setProperty(AvroMetrics.CONFIG_FILE_KEY, cfg.getAbsolutePath());
        AvroMetrics.getInstance().resolveConfig();

        AvroMetrics.getInstance().resetMetrics();
        AvroMetrics.getInstance().recordRead("t", 100, 200_000_000L); // 200ms > 100ms
        List<Alert> alerts = AvroMetrics.getInstance().getRecentAlerts(10);
        boolean found = alerts.stream().anyMatch(a -> "slow_read".equals(a.metric()));
        assertTrue(found, "Config file override should set threshold to 100ms");
    }

    // ─── Read/write recording ─────────────────────────────────────────

    @Test
    void recordReadIncrementsCounters() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("users", 1024, 5_000_000L);
        MetricsSnapshot s = m.snapshot();
        assertEquals(1, s.totalReads());
        assertEquals(1024, s.totalBytesRead());
    }

    @Test
    void recordWriteIncrementsCounters() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordWrite("orders", 2048, 10_000_000L);
        MetricsSnapshot s = m.snapshot();
        assertEquals(1, s.totalWrites());
        assertEquals(2048, s.totalBytesWritten());
    }

    @Test
    void multipleReadsAccumulate() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t1", 100, 1_000_000L);
        m.recordRead("t2", 200, 2_000_000L);
        m.recordRead("t1", 300, 3_000_000L);
        MetricsSnapshot s = m.snapshot();
        assertEquals(3, s.totalReads());
        assertEquals(600, s.totalBytesRead());
    }

    @Test
    void readThroughputCalculatedCorrectly() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        // 1_000_000_000 nanos = 1 second, 1024 bytes -> 1024 bytes/sec
        m.recordRead("t", 1024, 1_000_000_000L);
        MetricsSnapshot s = m.snapshot();
        assertTrue(s.readThroughputBytesPerSec() > 1020.0,
                "Expected ~1024 bytes/sec, got " + s.readThroughputBytesPerSec());
    }

    @Test
    void writeThroughputCalculatedCorrectly() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordWrite("t", 2048, 2_000_000_000L); // 2 seconds -> 1024 bytes/sec
        MetricsSnapshot s = m.snapshot();
        assertTrue(s.writeThroughputBytesPerSec() > 510.0,
                "Expected ~1024 bytes/sec, got " + s.writeThroughputBytesPerSec());
    }

    // ─── Compression ratio ────────────────────────────────────────────

    @Test
    void recordCompressionCalculatesRatio() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordCompression("t", 1000, 200); // 0.2 ratio
        MetricsSnapshot s = m.snapshot();
        assertEquals(0.2, s.lastCompressionRatio(), 0.01);
    }

    @Test
    void avgCompressionRatioAcrossMultipleOps() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordCompression("t", 1000, 200);  // 0.2
        m.recordCompression("t", 2000, 400);  // 0.2
        MetricsSnapshot s = m.snapshot();
        assertEquals(0.2, s.avgCompressionRatio(), 0.01);
    }

    @Test
    void compressionRatioZeroDivisionGuard() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordCompression("t", 0, 0);
        MetricsSnapshot s = m.snapshot();
        assertEquals(1.0, s.lastCompressionRatio(), 0.001);
        assertEquals(1.0, s.avgCompressionRatio(), 0.001);
    }

    // ─── Snapshot ─────────────────────────────────────────────────────

    @Test
    void snapshotReturnsCurrentValues() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 512, 1_000_000L);
        m.recordWrite("t", 256, 2_000_000L);
        MetricsSnapshot s = m.snapshot();
        assertEquals(1, s.totalReads());
        assertEquals(1, s.totalWrites());
        assertEquals(512, s.totalBytesRead());
        assertEquals(256, s.totalBytesWritten());
        assertTrue(s.timestampMs() > 0);
    }

    @Test
    void snapshotIsImmutableRecord() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 100, 1_000_000L);
        MetricsSnapshot s1 = m.snapshot();
        MetricsSnapshot s2 = m.snapshot();
        assertEquals(s1.totalReads(), s2.totalReads());
        assertEquals(s1.totalWrites(), s2.totalWrites());
        assertEquals(s1.totalBytesRead(), s2.totalBytesRead());
        assertEquals(s1.readThroughputBytesPerSec(), s2.readThroughputBytesPerSec());
    }

    @Test
    void resetClearsAll() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.recordRead("t", 100, 1_000_000L);
        m.recordWrite("t", 200, 2_000_000L);
        m.recordCompression("t", 500, 100);
        m.recordError("t", "io");
        m.resetMetrics();
        MetricsSnapshot s = m.snapshot();
        assertEquals(0, s.totalReads());
        assertEquals(0, s.totalWrites());
        assertEquals(0, s.totalBytesRead());
        assertEquals(0, s.totalBytesWritten());
        assertEquals(0, s.activeTables());
    }

    @Test
    void activeTablesIncrementDecrement() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.incrementActiveTables();
        m.incrementActiveTables();
        m.incrementActiveTables();
        m.decrementActiveTables();
        MetricsSnapshot s = m.snapshot();
        assertEquals(2, s.activeTables());
    }

    @Test
    void activeTablesDecrementFloorAtZero() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.decrementActiveTables();
        m.decrementActiveTables();
        MetricsSnapshot s = m.snapshot();
        assertEquals(0, s.activeTables());
    }

    @Test
    void errorRateTracked() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 100, 1_000_000L);
        m.recordError("t", "io");
        m.recordError("t", "parse");
        MetricsSnapshot s = m.snapshot();
        assertTrue(s.errorRate() > 0.6, "Expected >0.6 error rate, got " + s.errorRate());
    }

    // ─── JMX MBean ───────────────────────────────────────────────────

    @Test
    void jmxMBeanExposesLiveMetrics() throws Exception {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 1024, 5_000_000L);

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName on = new javax.management.ObjectName(
                System.getProperty(AvroMetrics.JMX_OBJECT_NAME_KEY, AvroMetrics.DEFAULT_JMX_OBJECT_NAME));
        assertTrue(server.isRegistered(on), "AvroMetrics MBean should be registered");

        long totalReads = (long) server.getAttribute(on, "TotalReads");
        assertEquals(1, totalReads);

        long bytesRead = (long) server.getAttribute(on, "TotalBytesRead");
        assertEquals(1024, bytesRead);

        double uptime = ((Number) server.getAttribute(on, "UptimeMs")).doubleValue();
        assertTrue(uptime >= 0, "Uptime should be non-negative");
    }

    @Test
    void jmxMBeanAttributesReadOnly() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        javax.management.ObjectName on = new javax.management.ObjectName(
                System.getProperty(AvroMetrics.JMX_OBJECT_NAME_KEY, AvroMetrics.DEFAULT_JMX_OBJECT_NAME));
        assertTrue(server.isRegistered(on));

        // setAttribute should throw AttributeNotFoundException
        try {
            server.setAttribute(on, new Attribute("TotalReads", 999L));
            assertTrue(false, "Should have thrown");
        } catch (javax.management.AttributeNotFoundException expected) {
            // expected
        }
    }

    // ─── Prometheus export ────────────────────────────────────────────

    @Test
    void prometheusOutputContainsExpectedMetrics() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 512, 1_000_000L);
        m.recordWrite("t", 256, 2_000_000L);
        String prom = m.renderPrometheus();
        assertTrue(prom.contains("avro_total_reads"), "Should contain avro_total_reads");
        assertTrue(prom.contains("avro_total_writes"), "Should contain avro_total_writes");
        assertTrue(prom.contains("avro_total_bytes_read"), "Should contain avro_total_bytes_read");
        assertTrue(prom.contains("avro_total_bytes_written"), "Should contain avro_total_bytes_written");
        assertTrue(prom.contains("avro_compression_ratio"), "Should contain avro_compression_ratio");
        assertTrue(prom.contains("avro_active_tables"), "Should contain avro_active_tables");
        assertTrue(prom.contains("avro_uptime_ms"), "Should contain avro_uptime_ms");
    }

    @Test
    void prometheusCounterFormat() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 100, 1_000_000L);
        m.recordRead("t", 200, 1_000_000L);
        String prom = m.renderPrometheus();
        assertTrue(prom.contains("# TYPE avro_total_reads counter"), "Should declare counter type");
        assertTrue(prom.contains("# HELP avro_total_reads"), "Should declare help text");
        assertTrue(prom.contains("avro_total_reads 2"), "Should contain value 2");
    }

    @Test
    void prometheusGaugeFormat() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.incrementActiveTables();
        String prom = m.renderPrometheus();
        assertTrue(prom.contains("# TYPE avro_active_tables gauge"), "Should declare gauge type");
        assertTrue(prom.contains("avro_active_tables 1"), "Should contain value 1");
    }

    @Test
    void prometheusEmptyState() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        String prom = m.renderPrometheus();
        assertTrue(prom.contains("avro_total_reads 0"), "Empty state should show 0");
        assertTrue(prom.contains("avro_uptime_ms"), "Uptime should still be present");
    }

    // ─── Alerting ─────────────────────────────────────────────────────

    @Test
    void slowReadAlertFires() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        // default threshold 5000ms, send 6000ms read
        m.recordRead("t", 100, 6_000_000_000L);
        List<Alert> alerts = m.getRecentAlerts(10);
        assertTrue(alerts.size() > 0, "Should have fired at least one alert");
        assertTrue(alerts.stream().anyMatch(a -> "slow_read".equals(a.metric())));
    }

    @Test
    void slowWriteAlertFires() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordWrite("t", 100, 6_000_000_000L); // 6000ms > 5000ms
        List<Alert> alerts = m.getRecentAlerts(10);
        assertTrue(alerts.stream().anyMatch(a -> "slow_write".equals(a.metric())));
    }

    @Test
    void noAlertBelowThreshold() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 100, 1_000_000L); // 1ms < 5000ms
        m.recordWrite("t", 100, 2_000_000L); // 2ms < 5000ms
        List<Alert> alerts = m.getRecentAlerts(10);
        assertTrue(alerts.isEmpty(), "No alerts should fire below threshold");
    }

    @Test
    void highErrorRateAlertFires() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        // default threshold 0.1 (10%)
        for (int i = 0; i < 10; i++) {
            m.recordRead("t", 100, 100_000L);
        }
        m.recordError("t", "io");
        m.recordError("t", "io");
        // 2 errors / 12 ops = 0.166 > 0.1
        List<Alert> alerts = m.getRecentAlerts(10);
        assertTrue(alerts.stream().anyMatch(a -> "high_error_rate".equals(a.metric())));
    }

    @Test
    void alertHistoryBounded() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        // fire many slow read alerts with wide window to bypass rate-limiting
        System.setProperty(AvroMetrics.THROUGHPUT_WINDOW_MS_KEY, "0");
        m.resolveConfig();
        for (int i = 0; i < 1100; i++) {
            m.recordRead("t", 100, 6_000_000_000L); // 6s > 5s threshold
        }
        // history capped at 1000
        List<Alert> all = m.getRecentAlerts(2000);
        assertTrue(all.size() <= AvroMetrics.MAX_ALERT_HISTORY,
                "Alert history should be bounded to " + AvroMetrics.MAX_ALERT_HISTORY
                        + " but was " + all.size());
    }

    @Test
    void clearAlertsResetsCount() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 100, 6_000_000_000L);
        assertTrue(m.getRecentAlerts(10).size() > 0);
        m.clearAlerts();
        assertEquals(0, m.getRecentAlerts(10).size());
        assertEquals(0, m.snapshot().alertCount());
    }

    @Test
    void alertSeverityLevels() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 100, 6_000_000_000L); // WARN for slow read
        List<Alert> alerts = m.getRecentAlerts(10);
        assertTrue(alerts.stream().anyMatch(a -> a.severity() == Alert.Severity.WARN));
    }

    // ─── Disabled metrics ─────────────────────────────────────────────

    @Test
    void disabledMetricsSkipsRecording() {
        System.setProperty(AvroMetrics.ENABLED_KEY, "false");
        AvroMetrics m = AvroMetrics.getInstance();
        m.resolveConfig();
        m.recordRead("t", 100, 1_000_000L);
        m.recordWrite("t", 100, 1_000_000L);
        m.recordCompression("t", 100, 10);
        m.recordError("t", "io");
        MetricsSnapshot s = m.snapshot();
        assertEquals(0, s.totalReads());
        assertEquals(0, s.totalWrites());
        // re-enable for other tests
        System.clearProperty(AvroMetrics.ENABLED_KEY);
        m.resolveConfig();
    }

    // ─── Thread safety ────────────────────────────────────────────────

    @Test
    void concurrentRecordingIsSafe() throws Exception {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        int threads = 8;
        int opsPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicLong totalExpected = new AtomicLong();

        for (int t = 0; t < threads; t++) {
            final int idx = t;
            pool.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        m.recordRead("t" + idx, 1, 1_000_000L);
                        totalExpected.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        MetricsSnapshot s = m.snapshot();
        assertEquals(totalExpected.get(), s.totalReads(),
                "Concurrent reads should sum correctly");
    }

    // ─── Edge cases ───────────────────────────────────────────────────

    @Test
    void nullTableNameHandled() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead(null, 100, 1_000_000L);
        m.recordWrite(null, 100, 1_000_000L);
        m.recordError(null, "io");
        MetricsSnapshot s = m.snapshot();
        assertEquals(1, s.totalReads());
        assertEquals(1, s.totalWrites());
    }

    @Test
    void negativeBytesHandledGracefully() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", -100, 1_000_000L);
        MetricsSnapshot s = m.snapshot();
        assertEquals(1, s.totalReads());
        assertEquals(-100, s.totalBytesRead());
    }

    @Test
    void zeroNanosThroughputIsZero() {
        AvroMetrics m = AvroMetrics.getInstance();
        m.resetMetrics();
        m.recordRead("t", 1024, 0);
        MetricsSnapshot s = m.snapshot();
        assertEquals(0.0, s.readThroughputBytesPerSec());
    }
}
