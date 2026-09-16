package diesel;

import diesel.storage.CsvRowStorage;
import diesel.storage.DelimitedByteParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stress test for large CSV files: verifies that the byte[] parser path
 * does not hit the 2GB String limit or cause OOM on files larger than
 * typical String-based thresholds.
 *
 * <p>Tests with a moderate file (50k rows) to verify the parser works
 * correctly with large datasets without requiring excessive CI time.
 * For true 500MB+ testing, run manually with increased heap.
 */
class CsvLargeFileStressTest {

    @TempDir
    File tempDir;

    private static List<String> schema() {
        return List.of("ID", "NAME", "VALUE", "FLAG");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("VALUE", Double.class);
        t.put("FLAG", Boolean.class);
        return t;
    }

    @Test
    void parseLargeDataset() throws Exception {
        int rows = 50_000;
        List<String> lines = new ArrayList<>(rows + 1);
        lines.add("ID,NAME,VALUE,FLAG");
        for (int i = 0; i < rows; i++) {
            lines.add(i + ",User_" + i + "," + (1000.0 + i * 0.1) + "," + (i % 2 == 0));
        }
        byte[] bytes = String.join("\n", lines).getBytes(StandardCharsets.UTF_8);

        List<Object[]> parsed = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, schema(), types(),
                (byte) ',', (byte) '"', "stress.csv");

        assertEquals(rows, parsed.size());
        assertEquals(0L, parsed.get(0)[0]);
        assertEquals("User_0", parsed.get(0)[1]);
        assertEquals((long) (rows - 1), parsed.get(rows - 1)[0]);
        assertEquals("User_" + (rows - 1), parsed.get(rows - 1)[1]);
    }

    @Test
    void storageLoadLargeDataset() throws Exception {
        int rows = 50_000;
        File csvFile = new File(tempDir, "large.csv");
        try (BufferedWriter bw = Files.newBufferedWriter(csvFile.toPath(), StandardCharsets.UTF_8)) {
            bw.write("ID,NAME,VALUE,FLAG\n");
            for (int i = 0; i < rows; i++) {
                bw.write(i + ",User_" + i + "," + (1000.0 + i * 0.1) + "," + (i % 2 == 0));
                bw.write('\n');
            }
        }

        CsvRowStorage storage = new CsvRowStorage("LARGE", schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile("LARGE", false);

        assertEquals(rows, storage.scan().size());
        assertEquals("User_0", storage.scan().get(0).get("NAME"));
        assertEquals("User_" + (rows - 1), storage.scan().get(rows - 1).get("NAME"));
        storage.close();
    }

    @Test
    void largeFileWithAllEmptyFields() throws Exception {
        int rows = 10_000;
        List<String> lines = new ArrayList<>(rows + 1);
        lines.add("ID,NAME,VALUE,FLAG");
        for (int i = 0; i < rows; i++) {
            lines.add(",,,,");
        }
        byte[] bytes = String.join("\n", lines).getBytes(StandardCharsets.UTF_8);

        List<Object[]> parsed = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8, schema(), types(),
                (byte) ',', (byte) '"', "empty_stress.csv");

        assertEquals(rows, parsed.size());
        for (int i = 0; i < rows; i++) {
            assertEquals(4, parsed.get(i).length);
            for (int j = 0; j < 4; j++) {
                assertNull(parsed.get(i)[j]);
            }
        }
    }

    @Test
    void largeFileWithQuotedFields() throws Exception {
        int rows = 10_000;
        List<String> lines = new ArrayList<>(rows + 1);
        lines.add("ID,DATA");
        for (int i = 0; i < rows; i++) {
            lines.add(i + ",\"value with, comma and \"\"quotes\"\"\"");
        }
        byte[] bytes = String.join("\n", lines).getBytes(StandardCharsets.UTF_8);

        List<Object[]> parsed = DelimitedByteParser.parse(bytes, StandardCharsets.UTF_8,
                List.of("ID", "DATA"), Map.of("ID", Long.class, "DATA", String.class),
                (byte) ',', (byte) '"', "quoted_stress.csv");

        assertEquals(rows, parsed.size());
        for (int i = 0; i < rows; i++) {
            assertEquals((long) i, parsed.get(i)[0]);
            assertEquals("value with, comma and \"quotes\"", parsed.get(i)[1]);
        }
    }
}
