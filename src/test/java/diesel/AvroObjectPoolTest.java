package diesel;

import diesel.storage.avro.AvroObjectPool;
import diesel.storage.avro.AvroObjectPool.Config;
import diesel.storage.avro.AvroObjectPool.MetricsSnapshot;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 79 AVRO object-pool tests: GenericRecord / DatumWriter / DatumReader
 * borrow-return reuse, per-schema capacity enforcement, config resolution
 * (sysprop → config.properties → defaults), metrics (allocation counters,
 * allocation rate, GC pause time) and lifecycle (reset/close/singleton).
 */
@Tag("storage")
@StorageType("avro")
class AvroObjectPoolTest {

    private static final String[] POOL_SYS_PROPS = {
            AvroObjectPool.ENABLED_KEY,
            AvroObjectPool.RECORD_CAPACITY_KEY,
            AvroObjectPool.WRITER_CAPACITY_KEY,
            AvroObjectPool.READER_CAPACITY_KEY,
            "avro.pool.config.file",
    };

    @TempDir
    Path tempDir;

    @AfterEach
    void cleanPoolSysProps() {
        for (String key : POOL_SYS_PROPS) {
            System.clearProperty(key);
        }
    }

    private static Schema schema(String name, String... fields) {
        SchemaBuilder.FieldAssembler<Schema> assembler =
                SchemaBuilder.record(name).namespace("diesel.pool.test").fields();
        for (String field : fields) {
            assembler.name(field).type().unionOf().nullType().and().stringType().endUnion().nullDefault();
        }
        return assembler.endRecord();
    }

    // ─── Config resolution ──────────────────────────────────────────

    @Test
    void configDefaults() {
        Config cfg = Config.resolve();
        assertTrue(cfg.enabled());
        assertEquals(AvroObjectPool.DEFAULT_RECORD_CAPACITY, cfg.recordCapacity());
        assertEquals(AvroObjectPool.DEFAULT_WRITER_CAPACITY, cfg.writerCapacity());
        assertEquals(AvroObjectPool.DEFAULT_READER_CAPACITY, cfg.readerCapacity());
    }

    @Test
    void configSyspropOverrides() {
        System.setProperty(AvroObjectPool.RECORD_CAPACITY_KEY, "7");
        System.setProperty(AvroObjectPool.WRITER_CAPACITY_KEY, "3");
        System.setProperty(AvroObjectPool.READER_CAPACITY_KEY, "2");
        System.setProperty(AvroObjectPool.ENABLED_KEY, "off");
        Config cfg = Config.resolve();
        assertFalse(cfg.enabled());
        assertEquals(7, cfg.recordCapacity());
        assertEquals(3, cfg.writerCapacity());
        assertEquals(2, cfg.readerCapacity());
    }

    @Test
    void configInvalidValueFallsBackToDefault() {
        System.setProperty(AvroObjectPool.RECORD_CAPACITY_KEY, "-5");
        Config cfg = Config.resolve();
        assertEquals(AvroObjectPool.DEFAULT_RECORD_CAPACITY, cfg.recordCapacity());
    }

    @Test
    void configFileOverride() throws IOException {
        Path cfgFile = tempDir.resolve("pool.properties");
        Files.writeString(cfgFile, AvroObjectPool.RECORD_CAPACITY_KEY + "=11\n"
                + AvroObjectPool.WRITER_CAPACITY_KEY + "=4\n");
        System.setProperty("avro.pool.config.file", cfgFile.toString());
        Config cfg = Config.resolve();
        assertEquals(11, cfg.recordCapacity());
        assertEquals(4, cfg.writerCapacity());
        assertEquals(AvroObjectPool.DEFAULT_READER_CAPACITY, cfg.readerCapacity());
    }

    // ─── GenericRecord pool ─────────────────────────────────────────

    @Test
    void recordBorrowReuse() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("R", "a", "b");
        GenericRecord first = pool.borrowRecord(s);
        pool.returnRecord(first);
        assertEquals(1, pool.pooledRecords());
        GenericRecord second = pool.borrowRecord(s);
        assertSame(first, second);
        assertEquals(0, pool.pooledRecords(), "the re-borrowed record is loaned out again");
    }

    @Test
    void recordFieldsClearedOnReturn() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("RC", "a", "b", "c");
        GenericRecord rec = pool.borrowRecord(s);
        rec.put("a", "x");
        rec.put("b", "y");
        rec.put("c", "z");
        pool.returnRecord(rec);
        GenericRecord reused = pool.borrowRecord(s);
        assertNull(reused.get("a"));
        assertNull(reused.get("b"));
        assertNull(reused.get("c"));
    }

    @Test
    void recordCapacityEnforced() {
        AvroObjectPool pool = AvroObjectPool.newPool(config("true", "3", "8", "8"));
        Schema s = schema("CAP", "a");
        GenericRecord[] held = new GenericRecord[5];
        for (int i = 0; i < held.length; i++) {
            held[i] = pool.borrowRecord(s);
        }
        for (GenericRecord record : held) {
            pool.returnRecord(record);
        }
        assertEquals(3, pool.pooledRecords());
    }

    @Test
    void recordSchemaIsolation() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s1 = schema("Isolation1", "a");
        Schema s2 = schema("Isolation2", "b");
        GenericRecord r1 = pool.borrowRecord(s1);
        pool.returnRecord(r1);
        GenericRecord r2 = pool.borrowRecord(s2);
        assertNotSame(r1, r2);
        assertEquals(1, pool.pooledRecords());
    }

    @Test
    void recordBorrowWithoutReturnAllocates() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("Alloc", "a");
        GenericRecord a = pool.borrowRecord(s);
        GenericRecord b = pool.borrowRecord(s);
        GenericRecord c = pool.borrowRecord(s);
        assertNotSame(a, b);
        assertNotSame(b, c);
        assertEquals(3, pool.totalAllocations());
        assertEquals(0, pool.totalPoolHits());
        assertEquals(3, pool.snapshot().activeRecords());
    }

    @Test
    void recordNullSchemaRejected() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        assertThrows(IllegalArgumentException.class, () -> pool.borrowRecord(null));
    }

    // ─── DatumWriter pool ───────────────────────────────────────────

    @Test
    void writerBorrowReuse() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("W", "a");
        DatumWriter<GenericRecord> w1 = pool.borrowWriter(s);
        pool.returnWriter(w1, s);
        assertEquals(1, pool.pooledWriters());
        DatumWriter<GenericRecord> w2 = pool.borrowWriter(s);
        assertSame(w1, w2);
        assertEquals(0, pool.pooledWriters(), "the re-borrowed writer is loaned out again");
    }

    @Test
    void writerCapacityEnforced() {
        AvroObjectPool pool = AvroObjectPool.newPool(config("true", "8", "2", "8"));
        Schema s = schema("WCap", "a");
        DatumWriter<GenericRecord>[] held = new DatumWriter[4];
        for (int i = 0; i < held.length; i++) {
            held[i] = pool.borrowWriter(s);
        }
        for (DatumWriter<GenericRecord> writer : held) {
            pool.returnWriter(writer, s);
        }
        assertEquals(2, pool.pooledWriters());
    }

    @Test
    void writerNullSchemaRejected() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        assertThrows(IllegalArgumentException.class, () -> pool.borrowWriter(null));
    }

    // ─── DatumReader pool ───────────────────────────────────────────

    @Test
    void readerBorrowReuse() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("Rdr", "a");
        GenericDatumReader<GenericRecord> r1 = pool.borrowReader(s, s);
        pool.returnReader(r1, s);
        assertEquals(1, pool.pooledReaders());
        GenericDatumReader<GenericRecord> r2 = pool.borrowReader(s, s);
        assertSame(r1, r2);
        assertEquals(0, pool.pooledReaders(), "the re-borrowed reader is loaned out again");
    }

    @Test
    void readerKeyedByReaderSchema() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema writer = schema("KWriter", "a", "b");
        Schema full = schema("KFull", "a", "b");
        Schema projected = schema("KProj", "a");
        GenericDatumReader<GenericRecord> fullReader = pool.borrowReader(writer, full);
        GenericDatumReader<GenericRecord> projectedReader = pool.borrowReader(writer, projected);
        pool.returnReader(fullReader, writer, full);
        pool.returnReader(projectedReader, writer, projected);
        assertSame(fullReader, pool.borrowReader(writer, full));
        assertSame(projectedReader, pool.borrowReader(writer, projected));
        assertNotSame(fullReader, projectedReader);
    }

    @Test
    void readerCapacityEnforced() {
        AvroObjectPool pool = AvroObjectPool.newPool(config("true", "8", "8", "2"));
        Schema s = schema("RCap", "a");
        GenericDatumReader<GenericRecord>[] held = new GenericDatumReader[4];
        for (int i = 0; i < held.length; i++) {
            held[i] = pool.borrowReader(s, s);
        }
        for (GenericDatumReader<GenericRecord> reader : held) {
            pool.returnReader(reader, s);
        }
        assertEquals(2, pool.pooledReaders());
    }

    @Test
    void readerNullSchemaRejected() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("RN", "a");
        assertThrows(IllegalArgumentException.class, () -> pool.borrowReader(null, s));
        assertThrows(IllegalArgumentException.class, () -> pool.borrowReader(s, null));
    }

    // ─── Disabled pool ──────────────────────────────────────────────

    @Test
    void disabledPoolAlwaysAllocates() {
        AvroObjectPool pool = AvroObjectPool.newPool(config("false", "3", "3", "3"));
        Schema s = schema("D", "a");
        GenericRecord r1 = pool.borrowRecord(s);
        GenericRecord r2 = pool.borrowRecord(s);
        assertNotSame(r1, r2);
        pool.returnRecord(r1);
        assertEquals(0, pool.pooledRecords());
        assertFalse(pool.isEnabled());

        DatumWriter<GenericRecord> w1 = pool.borrowWriter(s);
        DatumWriter<GenericRecord> w2 = pool.borrowWriter(s);
        assertNotSame(w1, w2);

        GenericDatumReader<GenericRecord> rd1 = pool.borrowReader(s, s);
        GenericDatumReader<GenericRecord> rd2 = pool.borrowReader(s, s);
        assertNotSame(rd1, rd2);
    }

    // ─── Metrics ────────────────────────────────────────────────────

    @Test
    void metricsRecordCounters() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("M", "a");
        pool.borrowRecord(s);                     // miss + allocation
        GenericRecord rec = pool.borrowRecord(s); // miss + allocation
        pool.returnRecord(rec);
        GenericRecord reused = pool.borrowRecord(s); // hit
        assertSame(rec, reused);
        MetricsSnapshot snap = pool.snapshot();
        assertEquals(2, snap.recordAllocations());
        assertEquals(1, snap.recordPoolHits());
        assertEquals(2, snap.recordPoolMisses());
        assertEquals(1, snap.recordReturns());
        assertEquals(0, snap.pooledRecords(), "the hit borrow loaned the record out again");
    }

    @Test
    void metricsWriterReaderCounters() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("MW", "a");
        DatumWriter<GenericRecord> w1 = pool.borrowWriter(s);
        pool.returnWriter(w1, s);
        DatumWriter<GenericRecord> w2 = pool.borrowWriter(s);
        assertSame(w1, w2);
        GenericDatumReader<GenericRecord> r1 = pool.borrowReader(s, s);
        pool.returnReader(r1, s);
        GenericDatumReader<GenericRecord> r2 = pool.borrowReader(s, s);
        assertSame(r1, r2);
        MetricsSnapshot snap = pool.snapshot();
        assertEquals(1, snap.writerAllocations());
        assertEquals(1, snap.writerPoolHits());
        assertEquals(1, snap.writerPoolMisses());
        assertEquals(1, snap.writerReturns());
        assertEquals(1, snap.readerAllocations());
        assertEquals(1, snap.readerPoolHits());
        assertEquals(1, snap.readerReturns());
        assertEquals(2, snap.totalPoolHits());
        assertEquals(2, snap.totalAllocations());
    }

    @Test
    void metricsAllocationRate() {
        AvroObjectPool pool = AvroObjectPool.newPool(config("true", "0", "0", "0"));
        Schema s = schema("Rate", "a");
        for (int i = 0; i < 20; i++) {
            pool.returnRecord(pool.borrowRecord(s));
        }
        MetricsSnapshot snap = pool.snapshot();
        assertEquals(20, snap.recordAllocations());
        assertTrue(snap.allocationRatePerSec() > 0.0, "allocation rate must be positive after allocations");
        assertEquals(20, snap.totalAllocations());
        assertTrue(snap.elapsedSeconds() >= 0);
    }

    @Test
    void metricsGcTimeNonNegative() {
        assertTrue(AvroObjectPool.sampleGcTimeMs() >= 0);
        MetricsSnapshot snap = AvroObjectPool.newPool().snapshot();
        assertTrue(snap.gcPauseTimeMs() >= 0);
    }

    @Test
    void metricsResetResetsCounters() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("Reset", "a");
        GenericRecord rec = pool.borrowRecord(s);
        pool.returnRecord(rec);
        assertTrue(pool.totalAllocations() > 0);
        pool.resetMetrics();
        MetricsSnapshot snap = pool.snapshot();
        assertEquals(0, snap.totalAllocations());
        assertEquals(0, snap.totalPoolHits());
        assertEquals(0, snap.totalPoolMisses());
        assertEquals(1, snap.pooledRecords(), "resetMetrics leaves pools untouched");
    }

    @Test
    void metricsActiveTracking() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("Active", "a");
        GenericRecord r1 = pool.borrowRecord(s);
        GenericRecord r2 = pool.borrowRecord(s);
        assertEquals(2, pool.snapshot().activeRecords());
        pool.returnRecord(r1);
        assertEquals(1, pool.snapshot().activeRecords());
        pool.returnRecord(r2);
        assertEquals(0, pool.snapshot().activeRecords());
    }

    // ─── Thread safety ──────────────────────────────────────────────

    @Test
    void concurrentBorrowReturn() throws Exception {
        AvroObjectPool pool = AvroObjectPool.newPool(config("true", "64", "8", "8"));
        Schema s = schema("Conc", "a");
        int threads = 8;
        int iterations = 2000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            executor.execute(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        GenericRecord rec = pool.borrowRecord(s);
                        rec.put("a", "v" + i);
                        assertEquals("v" + i, rec.get("a"));
                        pool.returnRecord(rec);
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
        }
        ready.await(5, TimeUnit.SECONDS);
        start.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "workers must finish");
        assertNull(failure.get(), "no worker may throw");
        long pooled = pool.pooledRecords();
        assertTrue(pooled <= 64, "pooled records must respect capacity, got " + pooled);
        assertEquals(0, pool.snapshot().activeRecords());
        MetricsSnapshot snap = pool.snapshot();
        assertTrue(snap.totalPoolHits() > 0);
        assertTrue(snap.totalAllocations() >= 1);
    }

    // ─── Lifecycle ──────────────────────────────────────────────────

    @Test
    void resetPurgesPools() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("Life", "a");
        pool.returnRecord(pool.borrowRecord(s));
        pool.returnWriter(pool.borrowWriter(s), s);
        pool.returnReader(pool.borrowReader(s, s), s);
        assertTrue(pool.pooledRecords() >= 1);
        assertTrue(pool.pooledWriters() >= 1);
        assertTrue(pool.pooledReaders() >= 1);
        pool.reset();
        assertEquals(0, pool.pooledRecords());
        assertEquals(0, pool.pooledWriters());
        assertEquals(0, pool.pooledReaders());
        assertTrue(pool.isEnabled());
    }

    @Test
    void closeIsIdempotentAndDegrades() {
        AvroObjectPool pool = AvroObjectPool.newPool();
        Schema s = schema("Close", "a");
        pool.returnRecord(pool.borrowRecord(s));
        assertEquals(1, pool.pooledRecords());
        pool.close();
        pool.close();
        assertFalse(pool.isEnabled());
        assertEquals(0, pool.pooledRecords());
        GenericRecord rec = pool.borrowRecord(s);
        assertNotNull(rec, "borrow after close still works (graceful degradation)");
        pool.returnRecord(rec);
        assertEquals(0, pool.pooledRecords(), "return after close is a no-op");
    }

    @Test
    void singletonIsStableAndTransientPoolsAreIndependent() {
        assertSame(AvroObjectPool.instance(), AvroObjectPool.instance());
        assertNotSame(AvroObjectPool.instance(), AvroObjectPool.newPool());
        assertNotSame(AvroObjectPool.newPool(), AvroObjectPool.newPool());
    }

    @Test
    void toStringSummaries() {
        assertNotNull(AvroObjectPool.newPool().toString());
        assertNotNull(AvroObjectPool.Config.resolve().toString());
    }

    private static Config config(String enabled, String recordCap, String writerCap, String readerCap) {
        System.setProperty(AvroObjectPool.ENABLED_KEY, enabled);
        System.setProperty(AvroObjectPool.RECORD_CAPACITY_KEY, recordCap);
        System.setProperty(AvroObjectPool.WRITER_CAPACITY_KEY, writerCap);
        System.setProperty(AvroObjectPool.READER_CAPACITY_KEY, readerCap);
        return Config.resolve();
    }
}