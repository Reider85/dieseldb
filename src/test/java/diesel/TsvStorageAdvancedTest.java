package diesel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import diesel.storage.AtomicFileWriter;
import diesel.storage.TsvRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 38 (§1a) TSV negative-scenario and property tests (mirror of
 * {@link CsvStorageAdvancedTest} for the tab-separated backend). Anchors the
 * storage-level behaviour of the CSV/TSV prompts: header mapping (24),
 * clustered primary-key search after mid insert (25), null sentinels (26),
 * load-error modes with file:line:column diagnostics (27), escaped header
 * names (28), charset failures (29), crash-safe atomic writes (30), strict
 * boolean parsing and extra-fields warnings (31), broken-.table fallback (32),
 * parallel-vs-sequential read equality (34) and deferred bulk deletes (35).
 */
class TsvStorageAdvancedTest {

    @TempDir
    Path tempDir;

    private static final String[] PROP_KEYS = {
            "storage.null.representation",
            "storage.load.error.mode",
            "storage.header.mismatch.mode",
            "csv.load.mode",
            "csv.table.mirror",
            "csv.parallel.read.threshold",
            "tsv.load.mode",
            "tsv.table.mirror",
            "tsv.parallel.read.threshold",
            "storage.charset"
    };

    private final Map<String, String> prevProps = new LinkedHashMap<>();

    @BeforeEach
    void saveConfig() {
        for (String key : PROP_KEYS) {
            prevProps.put(key, System.getProperty(key));
        }
    }

    @AfterEach
    void restoreConfig() {
        for (Map.Entry<String, String> e : prevProps.entrySet()) {
            if (e.getValue() == null) {
                System.clearProperty(e.getKey());
            } else {
                System.setProperty(e.getKey(), e.getValue());
            }
        }
    }

    // ─── helpers ─────────────────────────────────────────────────────

    private static List<String> cols(String... names) {
        return List.of(names);
    }

    private static Map<String, Class<?>> typed(String... pairs) {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            t.put(pairs[i], switch (pairs[i + 1]) {
                case "Long" -> Long.class;
                case "Integer" -> Integer.class;
                case "Boolean" -> Boolean.class;
                default -> String.class;
            });
        }
        return t;
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private TsvRowStorage storage(String table, List<String> schema, Map<String, Class<?>> types) {
        TsvRowStorage storage = new TsvRowStorage(table, schema, types);
        storage.setDataDir(tempDir.toString());
        storage.open();
        return storage;
    }

    /** Value whose {@code toString()} explodes mid-write, simulating an I/O failure. */
    private static final class Exploding {
        @Override
        public String toString() {
            throw new IllegalStateException("boom");
        }
    }

    // ─── Prompt 24: header mapping at storage level ──────────────────

    @Test
    void headerReorderMapsByColumnNameNotPosition() throws Exception {
        List<String> schema = cols("ID", "NAME", "AGE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "AGE", "Integer");
        Files.writeString(tempDir.resolve("REORDER.tsv"),
                "AGE\tID\tNAME\n30\t1\tAlice\n25\t2\tBob\n", StandardCharsets.UTF_8);

        TsvRowStorage loaded = storage("REORDER", schema, types);
        loaded.loadFromFile("REORDER");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(2, rows.size());
        assertEquals(map("ID", 1L, "NAME", "Alice", "AGE", 30), rows.get(0));
        assertEquals(map("ID", 2L, "NAME", "Bob", "AGE", 25), rows.get(1));
    }

    @Test
    void extraHeaderColumnIgnoredWithSingleExtraFieldsWarning() throws Exception {
        List<String> schema = cols("ID", "NAME", "AGE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "AGE", "Integer");
        Files.writeString(tempDir.resolve("EXTRA.tsv"),
                "ID\tNAME\tAGE\tEMAIL\n1\tAlice\t30\ta@b.com\n", StandardCharsets.UTF_8);

        try (Slf4jLogCapture capture = new Slf4jLogCapture("diesel.storage.TsvRowReader")) {
            TsvRowStorage loaded = storage("EXTRA", schema, types);
            loaded.loadFromFile("EXTRA");

            List<Map<String, Object>> rows = loaded.scan();
            assertEquals(1, rows.size());
            assertFalse(rows.get(0).containsKey("EMAIL"), "extra column must be dropped");
            assertEquals("Alice", rows.get(0).get("NAME"));

            List<ILoggingEvent> warnings = capture.eventsMatching(Level.WARN, "ignoring extra fields");
            assertEquals(1, warnings.size(), "exactly one extra-fields warning expected: " + capture.events());
        }
    }

    @Test
    void missingHeaderColumnFailsAndRollsBack() throws Exception {
        List<String> schema = cols("ID", "NAME", "AGE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "AGE", "Integer");
        Files.writeString(tempDir.resolve("MISSING.tsv"),
                "ID\tNAME\n1\tAlice\n", StandardCharsets.UTF_8);

        TsvRowStorage storage = storage("MISSING", schema, types);
        storage.insert(map("ID", 99L, "NAME", "keep", "AGE", 7));

        DieselIOException ex = assertThrows(DieselIOException.class,
                () -> storage.loadFromFile("MISSING"));

        assertTrue(ex.getMessage().contains(tempDir.resolve("MISSING.tsv").toString()),
                "message must contain the file path: " + ex.getMessage());
        Throwable cause = ex.getCause();
        assertNotNull(cause, "the wrapped header IOException must be preserved");
        assertTrue(cause.getMessage().contains("[AGE]"), "cause must name the missing column: " + cause.getMessage());
        assertTrue(cause.getMessage().contains("missing"), "cause must explain the mismatch: " + cause.getMessage());

        assertEquals(1, storage.scan().size(), "failed load must roll back to the previous rows");
        assertEquals("keep", storage.scan().get(0).get("NAME"));
    }

    @Test
    void missingHeaderColumnWarnsAndReadsNullInWarnMode() throws Exception {
        System.setProperty("storage.header.mismatch.mode", "warn");
        List<String> schema = cols("ID", "NAME", "AGE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "AGE", "Integer");
        Files.writeString(tempDir.resolve("WARN.tsv"),
                "ID\tNAME\n1\tAlice\n", StandardCharsets.UTF_8);

        try (Slf4jLogCapture capture = new Slf4jLogCapture("diesel.storage.TsvRowReader")) {
            TsvRowStorage loaded = storage("WARN", schema, types);
            loaded.loadFromFile("WARN");

            List<Map<String, Object>> rows = loaded.scan();
            assertEquals(1, rows.size());
            assertEquals(1L, rows.get(0).get("ID"));
            assertEquals("Alice", rows.get(0).get("NAME"));
            assertNull(rows.get(0).get("AGE"), "missing column reads as null in warn mode");

            assertFalse(capture.eventsMatching(Level.WARN, "TSV header columns missing from file").isEmpty(),
                    "expected a header-mismatch WARNING: " + capture.events());
        }
    }

    @Test
    void bomAndCaseInsensitiveHeaderMapped() throws Exception {
        List<String> schema = cols("ID", "NAME", "AGE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "AGE", "Integer");
        Files.writeString(tempDir.resolve("BOM.tsv"),
                "\uFEFFid\tname\tage\n7\tMoscow\t31\n", StandardCharsets.UTF_8);

        TsvRowStorage loaded = storage("BOM", schema, types);
        loaded.loadFromFile("BOM");

        assertEquals(1, loaded.scan().size());
        assertEquals(map("ID", 7L, "NAME", "Moscow", "AGE", 31), loaded.scan().get(0));
    }

    // ─── Prompt 28: escaped header column names ──────────────────────

    @Test
    void escapedHeaderColumnRoundTripsThroughStorage() throws Exception {
        String tabColumn = "a\tb";
        List<String> schema = cols("id", tabColumn);
        Map<String, Class<?>> types = typed("id", "Integer", tabColumn, "Integer");

        TsvRowStorage storage = storage("ESCHDR", schema, types);
        storage.insert(map("id", 1, tabColumn, 7));
        storage.saveToFile("ESCHDR");

        String raw = new String(Files.readAllBytes(tempDir.resolve("ESCHDR.tsv")), StandardCharsets.UTF_8);
        assertTrue(raw.startsWith("id\ta\\tb\n"),
                "tab-bearing header column must be backslash-escaped, got: " + raw.replace("\n", "\\n"));

        TsvRowStorage loaded = storage("ESCHDR", schema, types);
        loaded.loadFromFile("ESCHDR");
        assertEquals(1, loaded.scan().size());
        assertEquals(map("id", 1, tabColumn, 7), loaded.scan().get(0));
    }

    // ─── Prompt 26: null vs empty vs whitespace (sentinel) ───────────

    @Test
    void sentinelNullVsEmptyVsWhitespaceAndLiteralBackslashN() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<String> schema = cols("ID", "NAME", "DATA");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "DATA", "String");

        TsvRowStorage storage = storage("SENT", schema, types);
        storage.insert(map("ID", 1L, "NAME", null, "DATA", ""));
        storage.insert(map("ID", 2L, "NAME", "", "DATA", " "));
        storage.insert(map("ID", 3L, "NAME", " ", "DATA", "x"));
        storage.insert(map("ID", 4L, "NAME", "lit", "DATA", "\\N"));
        storage.saveToFile("SENT");

        String raw = new String(Files.readAllBytes(tempDir.resolve("SENT.tsv")), StandardCharsets.UTF_8);
        assertTrue(raw.startsWith("ID\tNAME\tDATA\n1\t\\N\t\n"),
                "null must be the \\N sentinel and the empty string a plain empty field, got: "
                        + raw.replace("\n", "\\n"));
        assertTrue(raw.contains("4\tlit\t\\\\N\n"),
                "a literal backslash-N must be escaped as \\\\N, got: " + raw.replace("\n", "\\n"));

        TsvRowStorage loaded = storage("SENT", schema, types);
        loaded.loadFromFile("SENT");

        List<Map<String, Object>> expected = List.of(
                map("ID", 1L, "NAME", null, "DATA", ""),
                map("ID", 2L, "NAME", "", "DATA", " "),
                map("ID", 3L, "NAME", " ", "DATA", "x"),
                map("ID", 4L, "NAME", "lit", "DATA", "\\N"));
        assertEquals(expected, loaded.scan(), "null, empty, whitespace and literal \\N must stay distinct");
    }

    // ─── Prompt 27: diagnostics and skip modes ───────────────────────

    @Test
    void invalidTypedValueEndToEndReportsLineColumnAndRollsBack() throws Exception {
        List<String> schema = cols("ID", "AGE", "NAME");
        Map<String, Class<?>> types = typed("ID", "Long", "AGE", "Integer", "NAME", "String");
        Files.writeString(tempDir.resolve("BADVAL.tsv"),
                "ID\tAGE\tNAME\n1\t30\tAlice\n2\tx\tBob\n", StandardCharsets.UTF_8);

        TsvRowStorage storage = storage("BADVAL", schema, types);
        storage.insert(map("ID", 99L, "AGE", 7, "NAME", "keep"));

        DieselIOException ex = assertThrows(DieselIOException.class,
                () -> storage.loadFromFile("BADVAL"));

        String msg = ex.getMessage();
        assertTrue(msg.contains(tempDir.resolve("BADVAL.tsv").toString()), "must name the file: " + msg);
        assertTrue(msg.contains("line 3"), "must carry the physical line: " + msg);
        assertTrue(msg.contains("AGE"), "must carry the column name: " + msg);
        assertTrue(msg.contains("x"), "must carry the bad value: " + msg);
        assertTrue(msg.contains("Integer"), "must carry the expected type: " + msg);

        assertEquals(1, storage.scan().size(), "failed load must roll back");
        assertEquals("keep", storage.scan().get(0).get("NAME"));
    }

    @Test
    void skipRowAndSkipValueModesEndToEnd() throws Exception {
        List<String> schema = cols("ID", "AGE");
        Map<String, Class<?>> types = typed("ID", "Long", "AGE", "Integer");
        Files.writeString(tempDir.resolve("SKIPMODE.tsv"),
                "ID\tAGE\n1\t25\n2\tbad\n3\t35\n", StandardCharsets.UTF_8);

        System.setProperty("storage.load.error.mode", "skip_row");
        TsvRowStorage skipRow = storage("SKIPMODE", schema, types);
        skipRow.loadFromFile("SKIPMODE");
        assertEquals(List.of(
                        map("ID", 1L, "AGE", 25),
                        map("ID", 3L, "AGE", 35)),
                skipRow.scan(), "skip_row must drop the bad row");

        System.setProperty("storage.load.error.mode", "skip_value");
        TsvRowStorage skipValue = storage("SKIPMODE", schema, types);
        skipValue.loadFromFile("SKIPMODE");
        List<Map<String, Object>> rows = skipValue.scan();
        assertEquals(3, rows.size(), "skip_value must keep every row");
        assertEquals(25, rows.get(0).get("AGE"));
        assertNull(rows.get(1).get("AGE"), "bad value becomes null");
        assertEquals(35, rows.get(2).get("AGE"));
    }

    // ─── Prompt 30 + 32: crash safety and broken .table ──────────────

    @Test
    void interruptedSaveKeepsPreviousFileAndCleansTemp() throws Exception {
        List<String> schema = cols("ID", "NAME", "AGE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "AGE", "Integer");

        TsvRowStorage storage = storage("CRASH", schema, types);
        storage.insert(map("ID", 1L, "NAME", "Alice", "AGE", 25));
        storage.saveToFile("CRASH");

        Path tsv = tempDir.resolve("CRASH.tsv");
        byte[] v1 = Files.readAllBytes(tsv);

        storage.insert(map("ID", 2L, "NAME", new Exploding(), "AGE", 30));
        assertThrows(RuntimeException.class, () -> storage.saveToFile("CRASH"));

        assertArrayEquals(v1, Files.readAllBytes(tsv), "previous valid file must be untouched");
        assertFalse(Files.exists(AtomicFileWriter.tmpPath(tsv)), "no temp file may linger");
        storage.close();
    }

    @Test
    void orphanTmpWarnsOnLoad() throws Exception {
        Path tsv = tempDir.resolve("ORPHAN.tsv");
        Files.createFile(AtomicFileWriter.tmpPath(tsv));
        List<String> schema = cols("ID", "NAME");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String");

        try (Slf4jLogCapture capture = new Slf4jLogCapture("diesel.storage.AtomicFileWriter")) {
            TsvRowStorage storage = storage("ORPHAN", schema, types);
            storage.loadFromFile("ORPHAN"); // target missing -> must not throw
            assertTrue(storage.scan().isEmpty());
            storage.close();

            assertFalse(capture.eventsMatching(Level.WARN, "Interrupted write").isEmpty(),
                    "expected an Interrupted-write WARNING: " + capture.events());
        }
    }

    @Test
    void brokenTableFallsBackToDelimited() throws Exception {
        System.setProperty("tsv.load.mode", "auto_mtime");
        System.setProperty("tsv.table.mirror", "on");
        List<String> schema = cols("ID", "NAME", "AGE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "AGE", "Integer");

        TsvRowStorage storage = storage("BROKENTBL", schema, types);
        storage.insert(map("ID", 1L, "NAME", "Alice", "AGE", 25));
        storage.insert(map("ID", 2L, "NAME", "Bob", "AGE", 30));
        storage.saveToFile("BROKENTBL");
        storage.close();

        Path table = tempDir.resolve("BROKENTBL.table");
        Files.write(table, new byte[]{1, 2, 3, 4, 5}); // corrupt serialised snapshot
        table.toFile().setLastModified(tempDir.resolve("BROKENTBL.tsv").toFile().lastModified() + 5000);

        TsvRowStorage loaded = storage("BROKENTBL", schema, types);
        loaded.loadFromFile("BROKENTBL");
        assertEquals(2, loaded.scan().size(), "broken .table must fall back to the delimited file");
        assertEquals("Alice", loaded.scan().get(0).get("NAME"));
    }

    // ─── Prompt 25 + 35: indexes ─────────────────────────────────────

    @Test
    void clusteredInsertThenPrimaryKeySearchCorrect() throws Exception {
        List<String> schema = cols("ID", "NAME");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String");

        TsvRowStorage storage = storage("CLUSTER", schema, types);
        storage.setPrimaryKeyColumn("ID");
        for (long id = 101; id <= 105; id++) {
            storage.insert(map("ID", id, "NAME", "row-" + id));
        }
        storage.insertAt(2, map("ID", 999L, "NAME", "MID"));

        assertEquals(List.of(2), storage.searchByPrimaryKey(999L), "inserted row must be found at its position");
        assertEquals(List.of(3), storage.searchByPrimaryKey(103L), "later rows must shift down by one");

        List<Long> ids = new ArrayList<>();
        for (Map<String, Object> row : storage.scan()) {
            ids.add((Long) row.get("ID"));
        }
        assertEquals(List.of(101L, 102L, 999L, 103L, 104L, 105L), ids);
    }

    @Test
    void massBulkDeleteKeepsPrimaryKeyIndexConsistent() throws Exception {
        List<String> schema = cols("ID", "NAME", "VALUE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "VALUE", "Integer");

        TsvRowStorage storage = storage("BULKDEL", schema, types);
        storage.setPrimaryKeyColumn("ID");
        for (int i = 1; i <= 2000; i++) {
            storage.insert(map("ID", (long) i, "NAME", "row-" + i, "VALUE", i));
        }

        storage.beginBulkUpdate();
        assertTrue(storage.getIndexManager().isBulkUpdating());
        for (int idx = 1999; idx >= 0; idx -= 2) {
            storage.delete(idx); // high-to-low removes the even-ID rows (ID = idx + 1)
        }
        assertTrue(storage.getIndexManager().isBulkUpdating(), "window stays open until endBulkUpdate");
        storage.endBulkUpdate();
        assertFalse(storage.getIndexManager().isBulkUpdating());

        assertEquals(1000, storage.getInternalRows().size(), "exactly 1000 rows must survive");
        for (int i = 2; i <= 2000; i += 2) {
            assertTrue(storage.searchByPrimaryKey((long) i).isEmpty(), "deleted ID=" + i + " must be absent");
        }
        for (int i = 1; i <= 2000; i += 2) {
            assertEquals(1, storage.searchByPrimaryKey((long) i).size(), "live ID=" + i + " must be found");
        }
    }

    // ─── Prompt 34: parallel == sequential on 10k+ rows ──────────────

    @Test
    void parallelLoadEqualsSequentialOn10kRows() throws Exception {
        System.setProperty("tsv.parallel.read.threshold", "1");
        List<String> schema = cols("ID", "NAME", "AGE", "SCORE");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String", "AGE", "Integer", "SCORE", "Integer");

        TsvRowStorage writer = storage("PAR10K", schema, types);
        for (int i = 1; i <= 10_000; i++) {
            writer.insert(map("ID", (long) i, "NAME", "Name" + i, "AGE", 18 + (i % 80), "SCORE", i * 3));
        }
        writer.saveToFile("PAR10K");
        writer.close();

        TsvRowStorage par = storage("PAR10K", schema, types);
        par.setPrimaryKeyColumn("ID");
        par.loadFromFile("PAR10K", true);
        TsvRowStorage seq = storage("PAR10K", schema, types);
        seq.setPrimaryKeyColumn("ID");
        seq.loadFromFile("PAR10K", false);

        assertEquals(10_000, par.scan().size());
        assertEquals(10_000, seq.scan().size());
        assertEquals(seq.scan(), par.scan(), "parallel and sequential loads must match in order and content");
        assertEquals(10_000, par.getIndexManager().getRowCount());
        assertEquals(10_000, seq.getIndexManager().getRowCount());
        assertEquals(List.of(0), par.searchByPrimaryKey(1L));
        assertEquals(List.of(4999), par.searchByPrimaryKey(5000L));
        assertEquals(List.of(9999), par.searchByPrimaryKey(10_000L));
    }

    // ─── Prompt 26/34/36: property-style randomized round-trip ───────

    @Test
    void randomRoundTripPreservesDifficultValues() throws Exception {
        System.setProperty("storage.null.representation", "sentinel");
        List<String> schema = cols("ID", "NAME", "DATA", "SCORE", "FLAG");
        Map<String, Class<?>> types = typed(
                "ID", "Long", "NAME", "String", "DATA", "String", "SCORE", "Integer", "FLAG", "Boolean");

        String[] pool = {
                null,
                "",
                " ",
                "plain",
                "line1\nline2",
                "cr\rhere",
                "tab\there",
                "back\\slash",
                "\\N",
                "\u041c\u043e\u0441\u043a\u0432\u0430",
                "\u4e2d\u6587",
                "\ud83d\udca9",
                "x".repeat(60)
        };

        Random rnd = new Random(42);
        List<Map<String, Object>> expected = new ArrayList<>();
        TsvRowStorage storage = storage("RANDPROP", schema, types);
        for (int i = 1; i <= 150; i++) {
            String name = pool[rnd.nextInt(pool.length)];
            String data = pool[rnd.nextInt(pool.length)];
            Integer score = rnd.nextBoolean() ? null : rnd.nextInt(1000);
            Boolean flag = switch (rnd.nextInt(3)) {
                case 0 -> null;
                case 1 -> true;
                default -> false;
            };
            Map<String, Object> row = map("ID", (long) i, "NAME", name, "DATA", data, "SCORE", score, "FLAG", flag);
            expected.add(row);
            storage.insert(row);
        }
        storage.saveToFile("RANDPROP");
        storage.close();

        TsvRowStorage loaded = storage("RANDPROP", schema, types);
        loaded.loadFromFile("RANDPROP");

        List<Map<String, Object>> actual = loaded.scan();
        assertEquals(expected.size(), actual.size(), "row count after round-trip");
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i), "row " + (i + 1) + " differs after round-trip");
        }
    }

    // ─── Prompt 31: strict boolean parsing ───────────────────────────

    @Test
    void strictBooleanAcceptedAndInvalidValueFails() throws Exception {
        List<String> schema = cols("ID", "ACTIVE");
        Map<String, Class<?>> types = typed("ID", "Long", "ACTIVE", "Boolean");
        Files.writeString(tempDir.resolve("BOOL_OK.tsv"),
                "ID\tACTIVE\n1\ttrue\n2\tfalse\n3\tyes\n4\t0\n5\tt\n", StandardCharsets.UTF_8);

        TsvRowStorage ok = storage("BOOL_OK", schema, types);
        ok.loadFromFile("BOOL_OK");
        List<Map<String, Object>> rows = ok.scan();
        assertEquals(Boolean.TRUE, rows.get(0).get("ACTIVE"));
        assertEquals(Boolean.FALSE, rows.get(1).get("ACTIVE"));
        assertEquals(Boolean.TRUE, rows.get(2).get("ACTIVE"));
        assertEquals(Boolean.FALSE, rows.get(3).get("ACTIVE"));
        assertEquals(Boolean.TRUE, rows.get(4).get("ACTIVE"));

        Files.writeString(tempDir.resolve("BOOL_BAD.tsv"),
                "ID\tACTIVE\n9\tsometimes\n", StandardCharsets.UTF_8);
        DieselIOException ex = assertThrows(DieselIOException.class,
                () -> storage("BOOL_BAD", schema, types).loadFromFile("BOOL_BAD"));
        assertTrue(ex.getMessage().contains("Boolean"), "strict parse must reject the value: " + ex.getMessage());
    }

    // ─── Prompt 29: other encodings fail with a clear error ──────────

    @Test
    void cyrillicInKoi8rAndWindows1251FailsWithClearError() throws Exception {
        List<String> schema = cols("ID", "NAME");
        Map<String, Class<?>> types = typed("ID", "Long", "NAME", "String");

        byte[] koi8r = encodeRaw("ID\tNAME\n1\t", "F0 D2 C9 D7 C5 D4", "\n");
        byte[] win1251 = encodeRaw("ID\tNAME\n1\t", "D0 F0 E8 E2 E5 F2", "\n");

        Files.write(tempDir.resolve("KOI8R.tsv"), koi8r);
        Files.write(tempDir.resolve("WIN1251.tsv"), win1251);

        for (String table : new String[]{"KOI8R", "WIN1251"}) {
            TsvRowStorage storage = storage(table, schema, types);
            DieselIOException ex = assertThrows(DieselIOException.class,
                    () -> storage.loadFromFile(table),
                    "legacy 8-bit Cyrillic bytes are malformed UTF-8 and must fail");
            assertTrue(ex.getMessage().contains(tempDir.resolve(table + ".tsv").toString()),
                    "error must name the offending file: " + ex.getMessage());
        }
    }

    /** Assembles a raw byte row: ASCII prefix + the given hex-encoded byte sequence + a trailing newline. */
    private static byte[] encodeRaw(String prefix, String hexBytes, String suffix) {
        String[] parts = hexBytes.trim().split("\\s+");
        byte[] tail = new byte[parts.length];
        for (int i = 0; i < parts.length; i++) {
            tail[i] = (byte) Integer.parseInt(parts[i], 16);
        }
        byte[] head = prefix.getBytes(StandardCharsets.UTF_8);
        byte[] end = suffix.getBytes(StandardCharsets.US_ASCII);
        byte[] out = new byte[head.length + tail.length + end.length];
        System.arraycopy(head, 0, out, 0, head.length);
        System.arraycopy(tail, 0, out, head.length, tail.length);
        System.arraycopy(end, 0, out, head.length + tail.length, end.length);
        return out;
    }
}