package diesel;

import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroReadIterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("storage")
class AvroDataFileReaderTest {

    @TempDir
    Path tempDir;

    private static final int ROWS_TOTAL = 66000;

    private static List<String> simpleCols() {
        return List.of("ID", "NAME", "AGE", "ACTIVE");
    }

    private static Map<String, Class<?>> simpleTypes() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("ACTIVE", Boolean.class);
        return t;
    }

    private static Map<String, Object> simpleRow(long id) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", "User" + id);
        r.put("AGE", (int) (id % 100));
        r.put("ACTIVE", id % 2 == 0);
        return r;
    }

    /** Writes ROWS_TOTAL rows through the production writer; the row-count block
     *  heuristic (64KB/1024 = 65536 rows) guarantees at least two blocks. */
    private File writeSimpleAvro(String tableName) throws IOException {
        File f = new File(tempDir.toFile(), tableName + ".avro");
        AvroDataFileWriter w = new AvroDataFileWriter(simpleCols(), simpleTypes(), f);
        try {
            for (long i = 0; i < ROWS_TOTAL; i++) {
                w.writeRow(simpleRow(i));
            }
            w.flush();
        } finally {
            w.close();
        }
        return f;
    }

    private static int countRecords(AvroDataFileReader reader) {
        int n = 0;
        while (reader.hasNext()) {
            reader.next();
            n++;
        }
        return n;
    }

    private static List<String> fieldNames(Schema schema) {
        List<String> names = new ArrayList<>();
        for (Schema.Field f : schema.getFields()) {
            names.add(f.name());
        }
        return names;
    }

    // ─── Full streaming read ────────────────────────────────────────

    @Test
    void testSequentialFullRead() throws IOException {
        File f = writeSimpleAvro("seq_read");
        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            assertEquals(Schema.Type.RECORD, r.getSchema().getType());
            assertEquals(simpleCols(), fieldNames(r.getSchema()));
            assertEquals(0, r.getNumBlocksRead());
            assertTrue(r.getFileLength() > 0);
            assertEquals(ROWS_TOTAL, countRecords(r));
            assertTrue(r.getNumBlocksRead() >= 2, "expected >=2 blocks after sync");
        }
    }

    @Test
    void testIteratorStreaming() throws IOException {
        File f = writeSimpleAvro("iter_stream");
        try (AvroDataFileReader r = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(r, simpleCols(), simpleTypes())) {
            int n = 0;
            while (it.hasNext()) {
                Object[] row = it.next();
                assertEquals(n, ((Number) row[0]).longValue());
                assertEquals("User" + n, row[1]);
                n++;
            }
            assertEquals(ROWS_TOTAL, n);
        }
    }

    @Test
    void testIteratorBatches() throws IOException {
        File f = writeSimpleAvro("iter_batch");
        try (AvroDataFileReader r = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(r, simpleCols(), simpleTypes())) {
            List<Object[]> all = new ArrayList<>();
            List<Object[]> batch;
            while (!(batch = it.nextBatch(7)).isEmpty()) {
                all.addAll(batch);
            }
            assertEquals(ROWS_TOTAL, all.size());
            assertEquals(0L, ((Number) all.get(0)[0]).longValue());
            assertEquals(ROWS_TOTAL - 1L, ((Number) all.get(all.size() - 1)[0]).longValue());
        }
    }

    // ─── Block counting & sync-marker seeking ───────────────────────

    @Test
    void testCountBlocksAndSeekBySyncMarker() throws IOException {
        File f = writeSimpleAvro("seek_read");
        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            long totalBlocks = r.countBlocks();
            assertTrue(totalBlocks >= 2, "expected >=2 blocks, got " + totalBlocks);

            long block0End = -1;
            long block0Count = -1;
            long seen = 0;
            r.reset();
            while (r.hasNext()) {
                long cnt = r.getCurrentBlockRecordCount();
                if (block0Count < 0) {
                    block0Count = cnt;
                }
                for (long i = 0; i < cnt; i++) {
                    r.next();
                }
                seen += cnt;
                if (block0End < 0) {
                    block0End = r.getPosition();
                }
            }
            assertEquals(ROWS_TOTAL, seen);
            assertTrue(block0Count < ROWS_TOTAL, "block 0 should not hold every row");

            r.reset();
            assertTrue(r.seekToSyncMarker(block0End - AvroDataFileReader.SYNC_SIZE));
            assertEquals(block0End, r.getPosition());
            assertEquals(ROWS_TOTAL - block0Count, countRecords(r));

            r.reset();
            assertFalse(r.seekToSyncMarker(r.getFileLength()));
        }
    }

    // ─── Projection pushdown ────────────────────────────────────────

    @Test
    void testProjectionReadOnlyRequestedFields() throws IOException {
        File f = writeSimpleAvro("proj_read");
        try (AvroDataFileReader r = new AvroDataFileReader(f, List.of("ID", "NAME"))) {
            assertTrue(r.isReaderSchemaCompatible());
            int n = 0;
            while (r.hasNext()) {
                GenericRecord rec = r.next();
                assertEquals(List.of("ID", "NAME"), fieldNames(rec.getSchema()));
                assertNotNull(rec.get("ID"));
                assertNotNull(rec.get("NAME"));
                assertThrows(org.apache.avro.AvroRuntimeException.class, () -> rec.get("AGE"));
                n++;
            }
            assertEquals(ROWS_TOTAL, n);
        }
    }

    @Test
    void testProjectionWithUnknownColumnFallsBackToFullRead() throws IOException {
        File f = writeSimpleAvro("proj_full");
        try (AvroDataFileReader r = new AvroDataFileReader(f, List.of("DOES_NOT_EXIST"))) {
            assertFalse(r.isReaderSchemaCompatible());
            assertNull(r.getReaderSchema());
            assertEquals(ROWS_TOTAL, countRecords(r));
            assertEquals(simpleCols(), fieldNames(r.getSchema()));
        }
    }

    // ─── Complex types through the new reader ───────────────────────

    @Test
    void testComplexTypesRoundTrip() throws IOException {
        File f = writeComplexAvro();
        try (AvroDataFileReader r = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(r, cols(), types())) {
            List<Object[]> rows = new ArrayList<>();
            while (it.hasNext()) {
                rows.add(it.next());
            }
            assertEquals(1, rows.size());
            Object[] row0 = rows.get(0);
            assertEquals(1L, row0[0]);
            assertEquals("Alice", row0[1]);
            assertEquals(30, row0[2]);
            assertEquals(0, new BigDecimal("1000.50").compareTo((BigDecimal) row0[3]));
            assertEquals(LocalDate.of(1994, 5, 15), row0[4]);
            assertEquals(LocalDateTime.of(2024, 1, 10, 14, 30), row0[5]);
            assertEquals(true, row0[6]);
        }
    }

    /** Writes one complex-type row directly with the raw Avro object-container
     *  API (logical-type values pre-converted), independent of the storage write
     *  path, so the reader's decoding of decimal/date/timestamp is verified. */
    private File writeComplexAvro() throws IOException {
        Schema schema = diesel.storage.avro.AvroSchemaManager.buildTableSchema("temp", cols(), types());
        File f = new File(tempDir.toFile(), "complex_reader.avro");
        try (org.apache.avro.file.DataFileWriter<GenericRecord> w =
                     new org.apache.avro.file.DataFileWriter<>(new org.apache.avro.generic.GenericDatumWriter<>(schema))) {
            w.create(schema, f);
            GenericRecord rec = new org.apache.avro.generic.GenericData.Record(schema);
            rec.put("ID", 1L);
            rec.put("NAME", "Alice");
            rec.put("AGE", 30);
            rec.put("BALANCE", java.nio.ByteBuffer.wrap(
                    new BigDecimal("1000.50").setScale(18).unscaledValue().toByteArray()));
            rec.put("BIRTHDATE", (int) LocalDate.of(1994, 5, 15).toEpochDay());
            rec.put("LAST_LOGIN", LocalDateTime.of(2024, 1, 10, 14, 30)
                    .atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli());
            rec.put("ACTIVE", true);
            w.append(rec);
        }
        return f;
    }

    // ─── Corrupt / non-Avro files ───────────────────────────────────

    @Test
    void testRejectsNonAvroFile() throws IOException {
        File f = new File(tempDir.toFile(), "not_an_avro.avro");
        Files.writeString(f.toPath(), "this is definitely not an avro object container file");
        assertThrows(IOException.class, () -> new AvroDataFileReader(f));
    }

    // ─── Column/type helpers (mirror AvroRowStorageTest) ────────────

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE", "BALANCE", "BIRTHDATE", "LAST_LOGIN", "ACTIVE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("BALANCE", BigDecimal.class);
        t.put("BIRTHDATE", LocalDate.class);
        t.put("LAST_LOGIN", LocalDateTime.class);
        t.put("ACTIVE", Boolean.class);
        return t;
    }
}