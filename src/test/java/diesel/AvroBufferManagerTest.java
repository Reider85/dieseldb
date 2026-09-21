package diesel;

import diesel.storage.avro.AvroBufferConfig;
import diesel.storage.avro.AvroBufferManager;
import diesel.storage.avro.AvroBufferManager.ManagedWriteBuffer;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroReadIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 78 AVRO buffer manager tests:
 * byte-budget write flushing, size/time/forced strategies,
 * read buffer sizing, zero-copy read/wrap paths, stats and lifecycle.
 */
@Tag("storage")
@StorageType("avro")
class AvroBufferManagerTest {

    private static final String[] PROP_KEYS = {
            "avro.buffer.write.size",
            "avro.buffer.read.size",
            "avro.buffer.flush.strategy",
            "avro.buffer.flush.interval.ms",
            "avro.buffer.zero.copy",
            "avro.buffer.config.file"
    };

    private final Map<String, String> prevProps = new LinkedHashMap<>();

    @TempDir
    Path tempDir;

    @BeforeEach
    void saveConfig() {
        for (String key : PROP_KEYS) {
            prevProps.put(key, System.getProperty(key));
            System.clearProperty(key);
        }
    }

    @AfterEach
    void restoreConfig() {
        for (String key : PROP_KEYS) {
            String prev = prevProps.get(key);
            if (prev != null) {
                System.setProperty(key, prev);
            } else {
                System.clearProperty(key);
            }
        }
    }

    // ─── Test data helpers ─────────────────────────────────────────

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> m = new LinkedHashMap<>();
        m.put("ID", Long.class);
        m.put("NAME", String.class);
        m.put("AGE", Integer.class);
        return m;
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ID", id);
        m.put("NAME", "User-" + id);
        m.put("AGE", (int) (id % 100));
        return m;
    }

    private File newOut(String name) {
        return new File(tempDir.toFile(), name + ".avro");
    }

    /** Reads every row back from a finished Avro file. */
    private static List<Map<String, Object>> readAll(File f) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (AvroDataFileReader reader = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(reader, cols(), types())) {
            while (it.hasNext()) {
                Object[] next = it.next();
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("ID", next[0]);
                m.put("NAME", next[1]);
                m.put("AGE", next[2]);
                rows.add(m);
            }
        }
        return rows;
    }

    // ─── Write side ────────────────────────────────────────────────

    @Test
    void writeRoundTripViaManagedBuffer() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            ManagedWriteBuffer buf = mgr.createWriteBuffer("t", newOut("rt"), cols(), types());
            for (long i = 0; i < 5; i++) {
                buf.writeRow(row(i));
            }
            assertEquals(5, buf.getBufferedRows());
            assertEquals(0, mgr.getTotalFlushes());
            assertEquals(1, mgr.getWriteBufferCount());
            assertTrue(buf.getBufferedBytes() > 0);
            assertSame(buf, mgr.getWriteBuffer("t"));
            buf.close();

            List<Map<String, Object>> rows = readAll(newOut("rt"));
            assertEquals(5, rows.size());
            assertEquals(0L, rows.get(0).get("ID"));
            assertEquals(4L, rows.get(4).get("ID"));
            assertEquals("User-2", rows.get(2).get("NAME"));
            assertTrue(mgr.getTotalFlushes() >= 1);
        }
    }

    @Test
    void sizeStrategyAutoFlushesOnByteBudget() throws IOException {
        System.setProperty("avro.buffer.write.size", "1");
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            ManagedWriteBuffer buf = mgr.createWriteBuffer("t", newOut("size"), cols(), types());
            for (long i = 0; i < 5; i++) {
                buf.writeRow(row(i));
            }
            assertEquals(0, buf.getBufferedRows());
            assertTrue(mgr.getTotalFlushes() >= 5, "every tiny budget write should flush");
            assertTrue(mgr.getTotalRowsFlushed() >= 5);
            buf.close();

            assertEquals(5, readAll(newOut("size")).size());
        }
    }

    @Test
    void forcedStrategyKeepsRowsUntilExplicitFlush() throws IOException {
        System.setProperty("avro.buffer.flush.strategy", "forced");
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            ManagedWriteBuffer buf = mgr.createWriteBuffer("t", newOut("forced"), cols(), types());
            for (long i = 0; i < 10; i++) {
                buf.writeRow(row(i));
            }
            assertEquals(10, buf.getBufferedRows());
            assertEquals(0, mgr.getTotalFlushes());

            mgr.flushAll();
            assertEquals(0, buf.getBufferedRows());
            assertEquals(1, mgr.getTotalFlushes());
            assertEquals(10, mgr.getTotalRowsFlushed());

            mgr.flush("t");
            assertEquals(1, mgr.getTotalFlushes(), "flushing an empty buffer must not count");
            buf.close();
            assertEquals(10, readAll(newOut("forced")).size());
        }
    }

    @Test
    void timeStrategyFlushesAutomatically() throws Exception {
        System.setProperty("avro.buffer.flush.strategy", "time");
        System.setProperty("avro.buffer.flush.interval.ms", "100");
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            ManagedWriteBuffer buf = mgr.createWriteBuffer("t", newOut("time"), cols(), types());
            for (long i = 0; i < 3; i++) {
                buf.writeRow(row(i));
            }
            assertTrue(buf.getBufferedRows() > 0);

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && buf.getBufferedRows() > 0) {
                Thread.sleep(50);
            }
            assertEquals(0, buf.getBufferedRows());
            assertTrue(mgr.getTotalFlushes() > 0, "scheduler should have flushed");
        }
    }

    @Test
    void flushUnknownNameThrows() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            IOException e = assertThrows(IOException.class, () -> mgr.flush("nope"));
            assertTrue(e.getMessage().contains("nope"));
        }
    }

    @Test
    void duplicateBufferNameRejected() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            mgr.createWriteBuffer("dup", newOut("dup"), cols(), types());
            assertThrows(IllegalArgumentException.class,
                    () -> mgr.createWriteBuffer("dup", newOut("dup2"), cols(), types()));
        }
    }

    @Test
    void blankBufferNameRejected() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            assertThrows(IllegalArgumentException.class,
                    () -> mgr.createWriteBuffer("  ", newOut("blank"), cols(), types()));
        }
    }

    @Test
    void closedManagerRejectsCreate() throws IOException {
        AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve());
        mgr.close();
        assertTrue(mgr.isClosed());
        assertThrows(IllegalStateException.class,
                () -> mgr.createWriteBuffer("t", newOut("closed"), cols(), types()));
        mgr.close();
    }

    @Test
    void rollbackDeletesFileAndClosesBuffer() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            ManagedWriteBuffer buf = mgr.createWriteBuffer("t", newOut("rb"), cols(), types());
            buf.writeRow(row(1));
            buf.writeRow(row(2));
            buf.rollback();
            assertFalse(buf.isOpen());
            assertFalse(newOut("rb").exists());
            assertThrows(IOException.class, () -> buf.writeRow(row(3)));
        }
    }

    @Test
    void closeAllWriteBuffersFlushesAll() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            mgr.createWriteBuffer("a", newOut("ca"), cols(), types());
            mgr.createWriteBuffer("b", newOut("cb"), cols(), types());
            mgr.getWriteBuffer("a").writeRow(row(1));
            mgr.getWriteBuffer("b").writeRow(row(2));
            mgr.closeAllWriteBuffers();
            assertEquals(0, mgr.getWriteBufferCount());
            assertEquals(2, mgr.getTotalRowsFlushed());
            assertEquals(1, readAll(newOut("ca")).size());
            assertEquals(1, readAll(newOut("cb")).size());
        }
    }

    @Test
    void statsCountersTrackFlushedPayload() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            ManagedWriteBuffer buf = mgr.createWriteBuffer("t", newOut("stats"), cols(), types());
            for (long i = 0; i < 4; i++) {
                buf.writeRow(row(i));
            }
            long before = mgr.getTotalBytesFlushed();
            mgr.flushAll();
            assertEquals(4, mgr.getTotalRowsFlushed());
            assertTrue(mgr.getTotalBytesFlushed() > before);
            assertTrue(mgr.getTotalBytesFlushed() > 0);
        }
    }

    @Test
    void writeRowOnClosedBufferThrows() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            ManagedWriteBuffer buf = mgr.createWriteBuffer("t", newOut("closedbuf"), cols(), types());
            buf.close();
            assertThrows(IOException.class, () -> buf.writeRow(row(9)));
        }
    }

    @Test
    void codecFactoryWriteVariant() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            ManagedWriteBuffer buf = mgr.createWriteBuffer(
                    "t", newOut("codec"), cols(), types(),
                    org.apache.avro.file.CodecFactory.deflateCodec(6));
            for (long i = 0; i < 3; i++) {
                buf.writeRow(row(i));
            }
            buf.close();
            assertEquals(3, readAll(newOut("codec")).size());
        }
    }

    // ─── Read side ─────────────────────────────────────────────────

    @Test
    void newReadBufferSizesAndAvoidsDoubleWrap() throws IOException {
        Path p = tempDir.resolve("read-buf.avro");
        Files.write(p, new byte[]{1, 2, 3});
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            InputStream raw = new ByteArrayInputStream(new byte[]{1, 2, 3});
            BufferedInputStream bis = mgr.newReadBuffer(raw);
            assertTrue(bis instanceof BufferedInputStream);
            assertSame(bis, mgr.newReadBuffer(bis));

            BufferedInputStream fileStream = mgr.newReadBuffer(p);
            assertNotNull(fileStream);
            fileStream.close();
        }
    }

    @Test
    void readBufferSizeFromConfigApplied() throws IOException {
        System.setProperty("avro.buffer.read.size", "8192");
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            assertEquals(8192, mgr.getConfig().readBufferSize());
            BufferedInputStream bis = mgr.newReadBuffer(new ByteArrayInputStream(new byte[10]));
            assertTrue(bis instanceof BufferedInputStream);
            bis.close();
        }
    }

    @Test
    void zeroCopyDisabledReturnsEmpty() throws IOException {
        System.setProperty("avro.buffer.zero.copy", "false");
        Path f = Files.write(tempDir.resolve("zcoff.txt"), "hello".getBytes(StandardCharsets.UTF_8));
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            Optional<MappedByteBuffer> mapped = mgr.zeroCopyReadBuffer(f);
            assertFalse(mapped.isPresent());
        }
    }

    @Test
    void zeroCopyReadMapsFileContents() throws IOException {
        String content = "zero-copy-payload-1234567890";
        Path f = Files.write(tempDir.resolve("zc.txt"), content.getBytes(StandardCharsets.UTF_8));
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            Optional<MappedByteBuffer> whole = mgr.zeroCopyReadBuffer(f);
            assertTrue(whole.isPresent());
            byte[] out = new byte[whole.get().remaining()];
            whole.get().get(out);
            assertEquals(content, new String(out, StandardCharsets.UTF_8));

            Optional<MappedByteBuffer> slice = mgr.zeroCopyReadBuffer(f, 5, 5);
            assertTrue(slice.isPresent());
            byte[] sliceOut = new byte[slice.get().remaining()];
            slice.get().get(sliceOut);
            assertEquals("copy-", new String(sliceOut, StandardCharsets.UTF_8));
        }
    }

    @Test
    void zeroCopyReadEmptyFileMapsZeroBytes() throws IOException {
        Path f = tempDir.resolve("empty.txt");
        Files.write(f, new byte[0]);
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            Optional<MappedByteBuffer> mapped = mgr.zeroCopyReadBuffer(f);
            assertTrue(mapped.isPresent());
            assertEquals(0, mapped.get().remaining());
        }
    }

    @Test
    void wrapZeroCopySharesBackingArray() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            byte[] data = {1, 2, 3, 4};
            ByteBuffer wrapped = mgr.wrapZeroCopy(data);
            assertArrayEquals(data, wrapped.array());
            assertSame(data, wrapped.array());
        }
    }

    // ─── Misc ──────────────────────────────────────────────────────

    @Test
    void estimateBufferedBytesGrowsWithRows() throws IOException {
        try (AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve())) {
            ManagedWriteBuffer buf = mgr.createWriteBuffer("t", newOut("bytes"), cols(), types());
            buf.writeRow(row(1));
            int afterOne = buf.getBufferedBytes();
            assertTrue(afterOne > 0);
            buf.writeRow(row(2));
            assertTrue(buf.getBufferedBytes() > afterOne);
        }
    }

    @Test
    void nonTimeStrategyCloseIsCleanNoOp() throws IOException {
        AvroBufferManager mgr = new AvroBufferManager(AvroBufferConfig.resolve());
        assertEquals(AvroBufferConfig.FlushStrategy.SIZE, mgr.getConfig().flushStrategy());
        mgr.close();
        assertTrue(mgr.isClosed());
        mgr.close();
    }
}