package diesel;

import diesel.storage.avro.AvroBlockManager;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroReadIterator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.CodecFactory;

import static org.junit.jupiter.api.Assertions.*;

@Tag("storage")
@StorageType("avro")
class AvroWriterBlockMetadataTest {

    private static final String[] PROP_KEYS = {
            "avro.block.size", "avro.block.workload", "avro.block.sync.interval"
    };
    private final Map<String, String> prev = new LinkedHashMap<>();

    @TempDir
    Path tempDir;

    @BeforeEach
    void saveProps() {
        for (String k : PROP_KEYS) {
            prev.put(k, System.getProperty(k));
            System.clearProperty(k);
        }
    }

    @AfterEach
    void restoreProps() {
        for (String k : PROP_KEYS) {
            String v = prev.get(k);
            if (v != null) System.setProperty(k, v);
            else System.clearProperty(k);
        }
    }

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE", "ACTIVE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> m = new LinkedHashMap<>();
        m.put("ID", Long.class);
        m.put("NAME", String.class);
        m.put("AGE", Integer.class);
        m.put("ACTIVE", Boolean.class);
        return m;
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ID", id);
        m.put("NAME", "User" + id);
        m.put("AGE", (int) (id % 100));
        m.put("ACTIVE", id % 2 == 0);
        return m;
    }

    private static void writeRows(AvroDataFileWriter w, int n) throws IOException {
        for (int i = 0; i < n; i++) {
            w.writeRow(row(i));
        }
        w.flush();
    }

    @Test
    void metadataPopulatedAfterClose() throws IOException {
        File f = new File(tempDir.toFile(), "meta.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            writeRows(w, 5000);
        } finally {
            w.close();
        }
        AvroBlockManager m = w.getBlockManager();
        assertNotNull(m, "block manager should be populated after close");
        assertTrue(m.getBlockCount() >= 1, "expected at least one block");
        assertEquals(5000, m.getTotalRecordCount());
        assertTrue(m.getTotalCompressedSize() > 0);
        long sum = m.getBlocks().stream().mapToLong(AvroBlockManager.BlockMetadata::recordCount).sum();
        assertEquals(5000, sum);
    }

    @Test
    void blockCountMatchesReader() throws IOException {
        File f = new File(tempDir.toFile(), "cnt.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            writeRows(w, 5000);
        } finally {
            w.close();
        }
        AvroBlockManager m = w.getBlockManager();
        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            assertEquals(m.getBlockCount(), r.countBlocks());
        }
    }

    @Test
    void rowCountMatchesSequentialRead() throws IOException {
        File f = new File(tempDir.toFile(), "rows.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            writeRows(w, 5000);
        } finally {
            w.close();
        }
        AvroBlockManager m = w.getBlockManager();
        int n = 0;
        try (AvroDataFileReader r = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(r, cols(), types())) {
            while (it.hasNext()) {
                it.next();
                n++;
            }
        }
        assertEquals(5000, n);
        assertEquals(5000, m.getTotalRecordCount());
    }

    @Test
    void crc32MatchesRecomputedPayload() throws IOException {
        File f = new File(tempDir.toFile(), "crc.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            writeRows(w, 5000);
        } finally {
            w.close();
        }
        AvroBlockManager m = w.getBlockManager();
        String codecName;
        try (AvroDataFileReader probe = new AvroDataFileReader(f)) {
            codecName = probe.getCodecName();
        }
        try (RandomAccessFile raf = new RandomAccessFile(f, "r");
             FileChannel ch = raf.getChannel()) {
            for (AvroBlockManager.BlockMetadata meta : m.getBlocks()) {
                byte[] payload = new byte[(int) meta.compressedSize()];
                readFully(ch, meta.startOffset(), payload);
                byte[] data = decompress(codecName, payload);
                assertEquals(meta.checksum(), AvroBlockManager.computeCRC32(data),
                        "block " + meta.blockIndex());
            }
        }
    }

    @Test
    void syncIntervalProducesManyBlocks() throws IOException {
        System.setProperty("avro.block.sync.interval", "1024");
        File f = new File(tempDir.toFile(), "sync.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            writeRows(w, 2000);
        } finally {
            w.close();
        }
        AvroBlockManager m = w.getBlockManager();
        assertTrue(m.getBlockCount() >= 2, "expected multiple blocks with 1024-byte sync interval");
        assertTrue(m.getBlockCount() < 1000, "should not be per-row blocks");
        assertEquals(2000, m.getTotalRecordCount());
    }

    @Test
    void defaultNotBlockPerRow() throws IOException {
        File f = new File(tempDir.toFile(), "def.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            writeRows(w, 2000);
        } finally {
            w.close();
        }
        AvroBlockManager m = w.getBlockManager();
        assertTrue(m.getBlockCount() < 2000, "default config must not create one block per row");
        assertEquals(2000, m.getTotalRecordCount());
    }

    @Test
    void fileSummaryContainsStats() throws IOException {
        File f = new File(tempDir.toFile(), "sum.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            writeRows(w, 100);
        } finally {
            w.close();
        }
        String summary = w.getFileSummary();
        assertNotNull(summary);
        assertTrue(summary.contains("blocks="));
        assertTrue(summary.contains("records="));
        assertTrue(summary.contains("compressed="));
        assertTrue(summary.contains("uncompressed="));
    }

    @Test
    void emptyFileHasNoBlocks() throws IOException {
        File f = new File(tempDir.toFile(), "empty.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        w.close();
        AvroBlockManager m = w.getBlockManager();
        assertNotNull(m);
        assertEquals(0, m.getBlockCount());
        assertEquals(0, m.getTotalRecordCount());
    }

    @Test
    void rollbackDeletesFile() throws IOException {
        File f = new File(tempDir.toFile(), "roll.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            writeRows(w, 10);
        } finally {
            w.rollback();
            w.close();
        }
        assertFalse(f.exists());
    }

    private static byte[] decompress(String codecName, byte[] payload) throws IOException {
        if ("null".equals(codecName)) {
            return payload;
        }
        org.apache.avro.file.Codec codec = createCodec(codecName);
        ByteBuffer in = ByteBuffer.wrap(payload);
        ByteBuffer out = codec.decompress(in);
        byte[] arr = new byte[out.remaining()];
        out.get(arr);
        return arr;
    }

    private static org.apache.avro.file.Codec createCodec(String codecName) throws IOException {
        try {
            Method m = CodecFactory.class.getDeclaredMethod("createInstance");
            m.setAccessible(true);
            return (org.apache.avro.file.Codec) m.invoke(CodecFactory.fromString(codecName));
        } catch (ReflectiveOperationException | SecurityException e) {
            throw new IOException("Unsupported codec '" + codecName + "': " + e.getMessage(), e);
        }
    }

    private static void readFully(FileChannel ch, long pos, byte[] out) throws IOException {
        int off = 0;
        while (off < out.length) {
            int got = ch.read(ByteBuffer.wrap(out, off, out.length - off), pos + off);
            if (got < 0) {
                throw new IOException("truncated at " + pos);
            }
            off += got;
        }
    }
}