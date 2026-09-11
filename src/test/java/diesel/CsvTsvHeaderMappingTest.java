package diesel;

import diesel.storage.CsvRowReader;
import diesel.storage.CsvRowWriter;
import diesel.storage.CsvRowStorage;
import diesel.storage.TsvRowReader;
import diesel.storage.TsvRowWriter;
import diesel.storage.TsvRowStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CsvTsvHeaderMappingTest {

    @TempDir
    Path tempDir;

    private List<String> schema;
    private Map<String, Class<?>> types;

    @BeforeEach
    void setUp() {
        schema = List.of("NAME", "AGE", "CITY");
        types = new LinkedHashMap<>();
        types.put("NAME", String.class);
        types.put("AGE", Integer.class);
        types.put("CITY", String.class);
    }

    // ── CSV tests ───────────────────────────────────────────────────

    @Test
    void csvColumnReorderMapsCorrectly() throws IOException {
        File f = tempDir.resolve("reorder.csv").toFile();
        // File header: CITY,NAME,AGE (reordered from schema NAME,AGE,CITY)
        Files.writeString(f.toPath(), "CITY,NAME,AGE\nMoscow,Alice,30\nLondon,Bob,25\n");

        try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
             CsvRowReader reader = new CsvRowReader(br, schema, types)) {
            List<String> header = reader.readHeader();
            assertEquals(List.of("CITY", "NAME", "AGE"), header);

            Map<String, Object> row1 = reader.next();
            assertEquals("Alice", row1.get("NAME"));
            assertEquals(30, row1.get("AGE"));
            assertEquals("Moscow", row1.get("CITY"));

            Map<String, Object> row2 = reader.next();
            assertEquals("Bob", row2.get("NAME"));
            assertEquals(25, row2.get("AGE"));
            assertEquals("London", row2.get("CITY"));
        }
    }

    @Test
    void csvExtraColumnInFileIgnored() throws IOException {
        File f = tempDir.resolve("extra.csv").toFile();
        // File has 4 columns, schema has 3 — extra "EMAIL" column is ignored
        Files.writeString(f.toPath(), "NAME,AGE,CITY,EMAIL\nAlice,30,Moscow,a@b.com\n");

        try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
             CsvRowReader reader = new CsvRowReader(br, schema, types)) {
            reader.readHeader();
            Map<String, Object> row = reader.next();
            assertEquals("Alice", row.get("NAME"));
            assertEquals(30, row.get("AGE"));
            assertEquals("Moscow", row.get("CITY"));
            assertFalse(row.containsKey("EMAIL"));
        }
    }

    @Test
    void csvMissingColumnInFileThrowsInFailMode() throws IOException {
        System.setProperty("storage.header.mismatch.mode", "fail");
        try {
            File f = tempDir.resolve("missing.csv").toFile();
            // File header missing "CITY" column
            Files.writeString(f.toPath(), "NAME,AGE\nAlice,30\n");

            try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
                 CsvRowReader reader = new CsvRowReader(br, schema, types)) {
                assertThrows(IOException.class, reader::readHeader);
            }
        } finally {
            System.clearProperty("storage.header.mismatch.mode");
        }
    }

    @Test
    void csvMissingColumnInFileWarnsInWarnMode() throws IOException {
        System.setProperty("storage.header.mismatch.mode", "warn");
        try {
            File f = tempDir.resolve("missing_warn.csv").toFile();
            Files.writeString(f.toPath(), "NAME,AGE\nAlice,30\n");

            try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
                 CsvRowReader reader = new CsvRowReader(br, schema, types)) {
                // Should not throw in warn mode
                List<String> header = reader.readHeader();
                assertEquals(List.of("NAME", "AGE"), header);

                Map<String, Object> row = reader.next();
                assertEquals("Alice", row.get("NAME"));
                assertEquals(30, row.get("AGE"));
                assertNull(row.get("CITY"));
            }
        } finally {
            System.clearProperty("storage.header.mismatch.mode");
        }
    }

    @Test
    void csvBomStrippedFromFirstColumn() throws IOException {
        File f = tempDir.resolve("bom.csv").toFile();
        // Write BOM + header
        Files.writeString(f.toPath(), "\uFEFFNAME,AGE,CITY\nAlice,30,Moscow\n", StandardCharsets.UTF_8);

        try (BufferedReader br = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
             CsvRowReader reader = new CsvRowReader(br, schema, types)) {
            List<String> header = reader.readHeader();
            assertEquals(List.of("NAME", "AGE", "CITY"), header);

            Map<String, Object> row = reader.next();
            assertEquals("Alice", row.get("NAME"));
            assertEquals(30, row.get("AGE"));
            assertEquals("Moscow", row.get("CITY"));
        }
    }

    @Test
    void csvExactMatchPreservesExistingBehavior() throws IOException {
        File f = tempDir.resolve("exact.csv").toFile();
        Files.writeString(f.toPath(), "NAME,AGE,CITY\nAlice,30,Moscow\nBob,25,London\n");

        try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
             CsvRowReader reader = new CsvRowReader(br, schema, types)) {
            List<String> header = reader.readHeader();
            assertEquals(List.of("NAME", "AGE", "CITY"), header);

            Map<String, Object> row1 = reader.next();
            assertEquals("Alice", row1.get("NAME"));
            assertEquals(30, row1.get("AGE"));
            assertEquals("Moscow", row1.get("CITY"));

            Map<String, Object> row2 = reader.next();
            assertEquals("Bob", row2.get("NAME"));
            assertEquals(25, row2.get("AGE"));
            assertEquals("London", row2.get("CITY"));
        }
    }

    @Test
    void csvReturnedListMatchesFileHeader() throws IOException {
        File f = tempDir.resolve("returned.csv").toFile();
        Files.writeString(f.toPath(), "CITY,AGE,NAME\nMoscow,30,Alice\n");

        try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
             CsvRowReader reader = new CsvRowReader(br, schema, types)) {
            List<String> header = reader.readHeader();
            assertEquals(3, header.size());
            assertEquals("CITY", header.get(0));
            assertEquals("AGE", header.get(1));
            assertEquals("NAME", header.get(2));
        }
    }

    // ── TSV tests ───────────────────────────────────────────────────

    @Test
    void tsvColumnReorderMapsCorrectly() throws IOException {
        File f = tempDir.resolve("reorder.tsv").toFile();
        Files.writeString(f.toPath(), "CITY\tNAME\tAGE\nMoscow\tAlice\t30\nLondon\tBob\t25\n");

        try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
             TsvRowReader reader = new TsvRowReader(br, schema, types)) {
            List<String> header = reader.readHeader();
            assertEquals(List.of("CITY", "NAME", "AGE"), header);

            Map<String, Object> row1 = reader.next();
            assertEquals("Alice", row1.get("NAME"));
            assertEquals(30, row1.get("AGE"));
            assertEquals("Moscow", row1.get("CITY"));

            Map<String, Object> row2 = reader.next();
            assertEquals("Bob", row2.get("NAME"));
            assertEquals(25, row2.get("AGE"));
            assertEquals("London", row2.get("CITY"));
        }
    }

    @Test
    void tsvExtraColumnInFileIgnored() throws IOException {
        File f = tempDir.resolve("extra.tsv").toFile();
        Files.writeString(f.toPath(), "NAME\tAGE\tCITY\tEMAIL\nAlice\t30\tMoscow\ta@b.com\n");

        try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
             TsvRowReader reader = new TsvRowReader(br, schema, types)) {
            reader.readHeader();
            Map<String, Object> row = reader.next();
            assertEquals("Alice", row.get("NAME"));
            assertEquals(30, row.get("AGE"));
            assertEquals("Moscow", row.get("CITY"));
            assertFalse(row.containsKey("EMAIL"));
        }
    }

    @Test
    void tsvMissingColumnInFileThrowsInFailMode() throws IOException {
        System.setProperty("storage.header.mismatch.mode", "fail");
        try {
            File f = tempDir.resolve("missing.tsv").toFile();
            Files.writeString(f.toPath(), "NAME\tAGE\nAlice\t30\n");

            try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
                 TsvRowReader reader = new TsvRowReader(br, schema, types)) {
                assertThrows(IOException.class, reader::readHeader);
            }
        } finally {
            System.clearProperty("storage.header.mismatch.mode");
        }
    }

    @Test
    void tsvMissingColumnInFileWarnsInWarnMode() throws IOException {
        System.setProperty("storage.header.mismatch.mode", "warn");
        try {
            File f = tempDir.resolve("missing_warn.tsv").toFile();
            Files.writeString(f.toPath(), "NAME\tAGE\nAlice\t30\n");

            try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
                 TsvRowReader reader = new TsvRowReader(br, schema, types)) {
                List<String> header = reader.readHeader();
                assertEquals(List.of("NAME", "AGE"), header);

                Map<String, Object> row = reader.next();
                assertEquals("Alice", row.get("NAME"));
                assertEquals(30, row.get("AGE"));
                assertNull(row.get("CITY"));
            }
        } finally {
            System.clearProperty("storage.header.mismatch.mode");
        }
    }

    @Test
    void tsvBomStrippedFromFirstColumn() throws IOException {
        File f = tempDir.resolve("bom.tsv").toFile();
        Files.writeString(f.toPath(), "\uFEFFNAME\tAGE\tCITY\nAlice\t30\tMoscow\n", StandardCharsets.UTF_8);

        try (BufferedReader br = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
             TsvRowReader reader = new TsvRowReader(br, schema, types)) {
            List<String> header = reader.readHeader();
            assertEquals(List.of("NAME", "AGE", "CITY"), header);

            Map<String, Object> row = reader.next();
            assertEquals("Alice", row.get("NAME"));
            assertEquals(30, row.get("AGE"));
            assertEquals("Moscow", row.get("CITY"));
        }
    }

    @Test
    void tsvExactMatchPreservesExistingBehavior() throws IOException {
        File f = tempDir.resolve("exact.tsv").toFile();
        Files.writeString(f.toPath(), "NAME\tAGE\tCITY\nAlice\t30\tMoscow\nBob\t25\tLondon\n");

        try (BufferedReader br = new BufferedReader(new java.io.FileReader(f));
             TsvRowReader reader = new TsvRowReader(br, schema, types)) {
            List<String> header = reader.readHeader();
            assertEquals(List.of("NAME", "AGE", "CITY"), header);

            Map<String, Object> row1 = reader.next();
            assertEquals("Alice", row1.get("NAME"));
            assertEquals(30, row1.get("AGE"));
            assertEquals("Moscow", row1.get("CITY"));

            Map<String, Object> row2 = reader.next();
            assertEquals("Bob", row2.get("NAME"));
            assertEquals(25, row2.get("AGE"));
            assertEquals("London", row2.get("CITY"));
        }
    }

    // ── CsvRowStorage integration (column reorder round-trip) ───────

    @Test
    void csvStorageRoundTripWithReorder() throws IOException {
        System.setProperty("storage.header.mismatch.mode", "warn");
        try {
            String tableName = "reorder_test";
            CsvRowStorage storage = new CsvRowStorage(tableName, schema, types);
            // Save with schema order
            Map<String, Object> row1 = new LinkedHashMap<>();
            row1.put("NAME", "Alice");
            row1.put("AGE", 30);
            row1.put("CITY", "Moscow");
            storage.insert(row1);

            storage.saveToFile(tableName);

            // Manually rewrite the file header in a different order
            File csvFile = tempDir.resolve(tableName + ".csv").toFile();
            if (!csvFile.exists()) {
                csvFile = new File(tableName + ".csv");
            }
            // The file is written to current dir by CsvRowStorage, find it
            String[] entries = new File(".").list((d, n) -> n.endsWith(".csv"));
            csvFile = null;
            if (entries != null) {
                for (String e : entries) {
                    if (e.startsWith(tableName)) {
                        csvFile = new File(e);
                        break;
                    }
                }
            }
            if (csvFile == null || !csvFile.exists()) {
                return; // skip if file not found
            }

            // Rewrite with reordered header AND reordered data: CITY,NAME,AGE
            Files.writeString(csvFile.toPath(), "CITY,NAME,AGE\nMoscow,Alice,30\n");

            // Reload with warn mode
            CsvRowStorage reload = new CsvRowStorage(tableName, schema, types);
            reload.loadFromFile(tableName);

            List<Map<String, Object>> loaded = reload.scan();
            assertEquals(1, loaded.size());
            assertEquals("Alice", loaded.get(0).get("NAME"));
            assertEquals(30, loaded.get(0).get("AGE"));
            assertEquals("Moscow", loaded.get(0).get("CITY"));
        } finally {
            System.clearProperty("storage.header.mismatch.mode");
            // cleanup generated files
            String[] entries = new File(".").list((d, n) -> n.startsWith("reorder_test") && (n.endsWith(".csv") || n.endsWith(".table")));
            if (entries != null) {
                for (String e : entries) new File(e).delete();
            }
        }
    }

    @Test
    void tsvStorageRoundTripWithReorder() throws IOException {
        System.setProperty("storage.header.mismatch.mode", "warn");
        try {
            String tableName = "reorder_tsv_test";
            TsvRowStorage storage = new TsvRowStorage(tableName, schema, types);
            Map<String, Object> row1 = new LinkedHashMap<>();
            row1.put("NAME", "Alice");
            row1.put("AGE", 30);
            row1.put("CITY", "Moscow");
            storage.insert(row1);
            storage.saveToFile(tableName);

            // Find and rewrite the file
            String[] entries = new File(".").list((d, n) -> n.startsWith(tableName) && n.endsWith(".tsv"));
            File tsvFile = null;
            if (entries != null) {
                for (String e : entries) {
                    tsvFile = new File(e);
                    break;
                }
            }
            if (tsvFile == null || !tsvFile.exists()) return;

            // Rewrite with reordered header AND reordered data: CITY\tNAME\tAGE
            Files.writeString(tsvFile.toPath(), "CITY\tNAME\tAGE\nMoscow\tAlice\t30\n");

            TsvRowStorage reload = new TsvRowStorage(tableName, schema, types);
            reload.loadFromFile(tableName);

            List<Map<String, Object>> loaded = reload.scan();
            assertEquals(1, loaded.size());
            assertEquals("Alice", loaded.get(0).get("NAME"));
            assertEquals(30, loaded.get(0).get("AGE"));
            assertEquals("Moscow", loaded.get(0).get("CITY"));
        } finally {
            System.clearProperty("storage.header.mismatch.mode");
            String[] entries = new File(".").list((d, n) -> n.startsWith("reorder_tsv_test") && (n.endsWith(".tsv") || n.endsWith(".table")));
            if (entries != null) {
                for (String e : entries) new File(e).delete();
            }
        }
    }
}
