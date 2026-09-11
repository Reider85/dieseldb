package diesel;

import diesel.storage.CsvRowStorage;
import diesel.storage.TsvRowStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies deterministic text encoding and line endings for CSV/TSV storage
 * (Prompt 29): files are always written in the configured {@code storage.charset}
 * (default UTF-8) with {@code \n} line separators, regardless of the platform's
 * default charset and line separator.
 */
class CharsetEncodingTest {

    @TempDir
    Path tempDir;

    private static final List<String> SCHEMA = List.of("NAME", "AGE", "CITY");
    private static final Map<String, Class<?>> TYPES;

    static {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        t.put("CITY", String.class);
        TYPES = Map.copyOf(t);
    }

    private static Map<String, Object> unicodeRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("NAME", "Привет Москва \u00fcber 💾");
        row.put("AGE", 30);
        row.put("CITY", "Москва");
        return row;
    }

    @Test
    void csvRoundTripPreservesUnicode() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("ENC_CSV", SCHEMA, TYPES);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(unicodeRow());
        storage.saveToFile("ENC_CSV");
        storage.close();

        CsvRowStorage loaded = new CsvRowStorage("ENC_CSV", SCHEMA, TYPES);
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("ENC_CSV");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(1, rows.size());
        assertEquals("Привет Москва \u00fcber 💾", rows.get(0).get("NAME"));
        assertEquals(30, rows.get(0).get("AGE"));
        assertEquals("Москва", rows.get(0).get("CITY"));
        loaded.close();
    }

    @Test
    void tsvRoundTripPreservesUnicode() throws Exception {
        TsvRowStorage storage = new TsvRowStorage("ENC_TSV", SCHEMA, TYPES);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(unicodeRow());
        storage.saveToFile("ENC_TSV");
        storage.close();

        TsvRowStorage loaded = new TsvRowStorage("ENC_TSV", SCHEMA, TYPES);
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.loadFromFile("ENC_TSV");

        List<Map<String, Object>> rows = loaded.scan();
        assertEquals(1, rows.size());
        assertEquals("Привет Москва \u00fcber 💾", rows.get(0).get("NAME"));
        assertEquals("Москва", rows.get(0).get("CITY"));
        loaded.close();
    }

    @Test
    void csvBytesAreUtf8WithoutCarriageReturns() throws Exception {
        CsvRowStorage storage = new CsvRowStorage("ENC_BYTES_CSV", SCHEMA, TYPES);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(unicodeRow());
        storage.saveToFile("ENC_BYTES_CSV");
        storage.close();

        File csv = new File(tempDir.toString(), "ENC_BYTES_CSV.csv");
        byte[] raw = Files.readAllBytes(csv.toPath());

        // No CR byte (0x0D) means \n (LF) line endings across platforms.
        assertFalse(new String(raw, java.nio.charset.StandardCharsets.ISO_8859_1).contains("\r"));

        // Byte content decodes losslessly as UTF-8 and contains the unicode value.
        String text = new String(raw, java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(text.contains("Привет Москва über 💾"));
        assertTrue(text.contains("Москва"));
    }

    @Test
    void tsvBytesAreUtf8WithoutCarriageReturns() throws Exception {
        TsvRowStorage storage = new TsvRowStorage("ENC_BYTES_TSV", SCHEMA, TYPES);
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.insert(unicodeRow());
        storage.saveToFile("ENC_BYTES_TSV");
        storage.close();

        File tsv = new File(tempDir.toString(), "ENC_BYTES_TSV.tsv");
        byte[] raw = Files.readAllBytes(tsv.toPath());
        String text = new String(raw, java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(text.contains("Привет Москва über 💾"));
        assertTrue(text.contains("Москва"));
        assertFalse(text.contains("\r"), "file must use \\n line endings, not platform separator");
    }

    @Test
    void storageCharsetPropertyIsHonored() throws Exception {
        System.setProperty("storage.charset", "windows-1251");
        try {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("NAME", "Привет Москва");
            row.put("AGE", 30);
            row.put("CITY", "Москва");

            CsvRowStorage storage = new CsvRowStorage("ENC_WIN1251", SCHEMA, TYPES);
            storage.setDataDir(tempDir.toString());
            storage.open();
            storage.insert(row);
            storage.saveToFile("ENC_WIN1251");
            storage.close();

            File csv = new File(tempDir.toString(), "ENC_WIN1251.csv");
            byte[] raw = Files.readAllBytes(csv.toPath());

            // The configured charset is actually used: bytes are windows-1251, not UTF-8.
            String decoded1251 = new String(raw, java.nio.charset.Charset.forName("windows-1251"));
            assertTrue(decoded1251.contains("Привет"));
            assertFalse(new String(raw, java.nio.charset.StandardCharsets.UTF_8).contains("Привет"));

            // Reloading honours the same charset: values round-trip.
            CsvRowStorage loaded = new CsvRowStorage("ENC_WIN1251", SCHEMA, TYPES);
            loaded.setDataDir(tempDir.toString());
            loaded.open();
            loaded.loadFromFile("ENC_WIN1251");
            List<Map<String, Object>> rows = loaded.scan();
            assertEquals(1, rows.size());
            assertEquals("Привет Москва", rows.get(0).get("NAME"));
            loaded.close();
        } finally {
            System.clearProperty("storage.charset");
        }
    }
}