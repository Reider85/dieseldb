package diesel;

import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroFileHeader;
import diesel.storage.avro.AvroRowStorage;
import org.apache.avro.file.CodecFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("storage")
@StorageType("avro")
class AvroFileHeaderTest {

    @TempDir
    Path tempDir;

    private static List<String> simpleCols() {
        return List.of("ID", "NAME", "AGE");
    }

    private static Map<String, Class<?>> simpleTypes() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        return t;
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", "User" + id);
        r.put("AGE", (int) id);
        return r;
    }

    // ─── Builder defaults ───────────────────────────────────────────

    @Test
    void builderAppliesDefaults() {
        AvroFileHeader h = AvroFileHeader.builder().build();
        assertEquals(AvroFileHeader.ENGINE_NAME, h.engine());
        assertEquals(AvroFileHeader.FORMAT_VERSION, h.formatVersion());
        assertEquals(AvroFileHeader.DEFAULT_SCHEMA_VERSION, h.schemaVersion());
        assertEquals(AvroFileHeader.DEFAULT_COMPRESSION_CODEC, h.compressionCodec());
        assertEquals(AvroFileHeader.DEFAULT_COMPRESSION_LEVEL, h.compressionLevel());
        assertNull(h.database());
        assertNull(h.tableName());
        assertNull(h.creationTimestamp());
    }

    @Test
    void builderSetsFields() {
        Instant ts = Instant.parse("2026-09-21T12:00:00Z");
        AvroFileHeader h = AvroFileHeader.builder()
                .database("analytics")
                .tableName("users")
                .schemaVersion(3)
                .creationTimestamp(ts)
                .compressionCodec("zstandard")
                .compressionLevel(5)
                .engine("DieselDB")
                .build();
        assertEquals("analytics", h.database());
        assertEquals("users", h.tableName());
        assertEquals(3, h.schemaVersion());
        assertEquals(ts, h.creationTimestamp());
        assertEquals("zstandard", h.compressionCodec());
        assertEquals(5, h.compressionLevel());
        assertEquals("DieselDB", h.engine());
        assertTrue(h.validate().isEmpty());
    }

    // ─── MetaMap round-trip ─────────────────────────────────────────

    @Test
    void toMetaMapContainsDieselKeys() {
        AvroFileHeader h = AvroFileHeader.builder()
                .database("db")
                .tableName("t")
                .compressionCodec("deflate")
                .compressionLevel(6)
                .build();
        Map<String, byte[]> meta = h.toMetaMap();
        assertEquals("db", new String(meta.get(AvroFileHeader.KEY_DATABASE), StandardCharsets.UTF_8));
        assertEquals("t", new String(meta.get(AvroFileHeader.KEY_TABLE), StandardCharsets.UTF_8));
        assertEquals("deflate", new String(meta.get(AvroFileHeader.KEY_COMPRESSION_CODEC), StandardCharsets.UTF_8));
        assertEquals("6", new String(meta.get(AvroFileHeader.KEY_COMPRESSION_LEVEL), StandardCharsets.UTF_8));
        assertEquals(AvroFileHeader.ENGINE_NAME, new String(meta.get(AvroFileHeader.KEY_ENGINE), StandardCharsets.UTF_8));
        assertEquals(AvroFileHeader.FORMAT_VERSION, new String(meta.get(AvroFileHeader.KEY_FORMAT_VERSION), StandardCharsets.UTF_8));
        assertTrue(meta.containsKey(AvroFileHeader.KEY_CREATION_TIMESTAMP)
                || h.creationTimestamp() == null);
    }

    @Test
    void metaMapRoundTripIsStable() {
        Instant ts = Instant.parse("2026-09-21T08:30:00Z");
        AvroFileHeader original = AvroFileHeader.builder()
                .database("analytics")
                .tableName("orders")
                .schemaVersion(2)
                .creationTimestamp(ts)
                .compressionCodec("zstandard")
                .compressionLevel(9)
                .build();
        AvroFileHeader parsed = AvroFileHeader.fromMetaMap(original.toMetaMap());
        assertEquals(original, parsed);
        assertTrue(parsed.hasDieselMetadata());
    }

    @Test
    void emptyMetaMapFallsBackToDefaults() {
        AvroFileHeader h = AvroFileHeader.fromMetaMap(new HashMap<>());
        assertFalse(h.hasDieselMetadata());
        assertEquals(AvroFileHeader.ENGINE_NAME, h.engine());
        assertEquals(AvroFileHeader.DEFAULT_SCHEMA_VERSION, h.schemaVersion());
        assertEquals("null", h.compressionCodec());
        assertEquals(-1, h.compressionLevel());
        assertNull(h.database());
        assertNull(h.tableName());
        assertNull(h.creationTimestamp());
        assertTrue(h.validate().isEmpty());
    }

    @Test
    void pre76FileWithOnlyAvroKeysParsesCleanly() {
        Map<String, byte[]> meta = new HashMap<>();
        meta.put("avro.schema", "{\"type\":\"record\",\"name\":\"r\",\"fields\":[]}".getBytes(StandardCharsets.UTF_8));
        meta.put("avro.codec", "null".getBytes(StandardCharsets.UTF_8));
        AvroFileHeader h = AvroFileHeader.fromMetaMap(meta);
        assertFalse(h.hasDieselMetadata());
        assertTrue(h.validate().isEmpty());
    }

    // ─── Malformed metadata fails fast ──────────────────────────────

    @Test
    void malformedSchemaVersionRejected() {
        Map<String, byte[]> meta = new HashMap<>();
        meta.put(AvroFileHeader.KEY_SCHEMA_VERSION, "abc".getBytes(StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> AvroFileHeader.fromMetaMap(meta));
    }

    @Test
    void malformedCompressionLevelRejected() {
        Map<String, byte[]> meta = new HashMap<>();
        meta.put(AvroFileHeader.KEY_COMPRESSION_LEVEL, "high".getBytes(StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> AvroFileHeader.fromMetaMap(meta));
    }

    @Test
    void malformedTimestampRejected() {
        Map<String, byte[]> meta = new HashMap<>();
        meta.put(AvroFileHeader.KEY_CREATION_TIMESTAMP, "not-a-timestamp".getBytes(StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> AvroFileHeader.fromMetaMap(meta));
    }

    // ─── Validation ─────────────────────────────────────────────────

    @Test
    void newerFormatVersionIsRejected() {
        AvroFileHeader h = AvroFileHeader.builder().formatVersion("2.0").build();
        List<String> problems = h.validate();
        assertFalse(problems.isEmpty());
        assertEquals(1, problems.stream().filter(p -> p.contains("Unsupported")).count());
    }

    @Test
    void sameMajorFormatVersionIsAccepted() {
        AvroFileHeader h = AvroFileHeader.builder().formatVersion(
                AvroFileHeader.FORMAT_VERSION).build();
        assertTrue(h.validate().isEmpty());
    }

    @Test
    void malformedFormatVersionIsRejected() {
        AvroFileHeader h = AvroFileHeader.builder().formatVersion("x.y").build();
        assertFalse(h.validate().isEmpty());
    }

    @Test
    void zeroSchemaVersionIsRejected() {
        AvroFileHeader h = AvroFileHeader.builder().schemaVersion(0).build();
        assertFalse(h.validate().isEmpty());
    }

    @Test
    void blankDatabaseAndTableAreRejected() {
        AvroFileHeader h = AvroFileHeader.builder().database(" ").tableName("  ").build();
        List<String> problems = h.validate();
        assertEquals(2, problems.stream()
                .filter(p -> p.contains("Blank")).count());
    }

    @Test
    void requireValidThrowsOnProblems() {
        AvroFileHeader h = AvroFileHeader.builder().formatVersion("9.9").build();
        assertThrows(IllegalArgumentException.class, h::requireValid);
    }

    @Test
    void requireValidAcceptsGoodHeader() {
        AvroFileHeader h = AvroFileHeader.builder().database("d").tableName("t").build();
        assertDoesNotThrow(h::requireValid);
    }

    // ─── Writer / Reader integration ────────────────────────────────

    @Test
    void writerPersistsHeaderMetadata() throws Exception {
        File f = new File(tempDir.toFile(), "meta.avro");
        Instant ts = Instant.parse("2026-09-21T10:00:00Z");
        AvroFileHeader header = AvroFileHeader.builder()
                .database("analytics")
                .tableName("meta_test")
                .creationTimestamp(ts)
                .compressionCodec("null")
                .compressionLevel(-1)
                .build();
        AvroDataFileWriter w = new AvroDataFileWriter(
                simpleCols(), simpleTypes(), f, CodecFactory.nullCodec(), 0, header);
        try {
            w.writeRow(row(1));
            w.writeRow(row(2));
        } finally {
            w.close();
        }

        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            assertEquals("analytics", r.getFileHeader().database());
            assertEquals("meta_test", r.getFileHeader().tableName());
            assertEquals(ts, r.getFileHeader().creationTimestamp());
            assertTrue(r.getFileHeader().hasDieselMetadata());
            assertEquals(2, readCount(r));
        }
    }

    @Test
    void writerWithoutHeaderWritesNoDieselMetadata() throws Exception {
        File f = new File(tempDir.toFile(), "plain.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(simpleCols(), simpleTypes(), f);
        try {
            w.writeRow(row(1));
        } finally {
            w.close();
        }
        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            assertFalse(r.getFileHeader().hasDieselMetadata());
            assertTrue(r.getFileHeader().validate().isEmpty());
        }
    }

    @Test
    void nullCodecLevelRoundTripKeepsCodec() throws Exception {
        File f = new File(tempDir.toFile(), "codec.avro");
        AvroFileHeader header = AvroFileHeader.builder()
                .tableName("c")
                .compressionCodec("zstandard")
                .compressionLevel(7)
                .build();
        AvroDataFileWriter w = new AvroDataFileWriter(
                simpleCols(), simpleTypes(), f, CodecFactory.zstandardCodec(7), 0, header);
        try {
            w.writeRow(row(1));
            w.writeRow(row(2));
        } finally {
            w.close();
        }
        // avro.codec reflects the actual codec; diesel.* records the intent.
        try (AvroDataFileReader r = new AvroDataFileReader(f)) {
            assertEquals("zstandard", r.getFileHeader().compressionCodec());
            assertEquals(7, r.getFileHeader().compressionLevel());
        }
    }

    // ─── Storage integration ────────────────────────────────────────

    @Test
    void rowStorageWritesAndReadsHeader() throws Exception {
        AvroRowStorage s = new AvroRowStorage("hdr_users", simpleCols(), simpleTypes());
        s.setDataDir(tempDir.toString());
        s.insert(row(1));
        s.insert(row(2));
        s.saveToFile("hdr_users");

        File avroFile = new File(tempDir.toFile(), "hdr_users.avro");
        assertTrue(avroFile.isFile());
        try (AvroDataFileReader r = new AvroDataFileReader(avroFile)) {
            AvroFileHeader h = r.getFileHeader();
            assertTrue(h.hasDieselMetadata());
            assertEquals("default", h.database());
            assertEquals("hdr_users", h.tableName());
            assertEquals(AvroFileHeader.ENGINE_NAME, h.engine());
            assertEquals(AvroFileHeader.FORMAT_VERSION, h.formatVersion());
            assertEquals(1, h.schemaVersion());
            assertNotNull(h.creationTimestamp());
            assertTrue(h.validate().isEmpty());
        }

        AvroRowStorage loaded = new AvroRowStorage("hdr_users", simpleCols(), simpleTypes());
        loaded.setDataDir(tempDir.toString());
        loaded.loadFromFile("hdr_users");
        assertEquals(2, loaded.scan().size());
    }

    @Test
    void pre76FileWithoutHeaderLoadsFine() throws Exception {
        File f = new File(tempDir.toFile(), "legacy.avro");
        AvroDataFileWriter w = new AvroDataFileWriter(simpleCols(), simpleTypes(), f);
        try {
            w.writeRow(row(1));
            w.writeRow(row(2));
        } finally {
            w.close();
        }
        AvroRowStorage s = new AvroRowStorage("legacy", simpleCols(), simpleTypes());
        s.setDataDir(tempDir.toString());
        s.loadFromFile("legacy");
        assertEquals(2, s.scan().size());
    }

    private static int readCount(AvroDataFileReader r) {
        int n = 0;
        while (r.hasNext()) {
            r.next();
            n++;
        }
        return n;
    }

    // ─── Object contract ────────────────────────────────────────────

    @Test
    void equalsAndHashCode() {
        AvroFileHeader a = AvroFileHeader.builder().database("d").tableName("t").schemaVersion(2).build();
        AvroFileHeader b = AvroFileHeader.builder().database("d").tableName("t").schemaVersion(2).build();
        AvroFileHeader c = AvroFileHeader.builder().database("d").tableName("t").schemaVersion(3).build();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, null);
        assertNotEquals(a, "other");
    }

    @Test
    void toStringSummarizesHeader() {
        AvroFileHeader h = AvroFileHeader.builder().database("x").tableName("t").build();
        String s = h.toString();
        assertTrue(s.contains("database='x'"));
        assertTrue(s.contains("table='t'"));
    }
}