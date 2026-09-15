package diesel;

import diesel.storage.CsvRowStorage;
import diesel.storage.CsvRowWriter;
import diesel.storage.TsvRowStorage;
import diesel.storage.TsvRowWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Large I/O performance gate for the CSV/TSV hot path (prompt 43): writes and
 * loads 200k rows through the direct writer path and the storage byte fast
 * path, records the timings as {@code [DELIM-IO]} and guards against
 * regressions with generous absolute ceilings plus an internal sequential
 * versus parallel sanity check. Run only via {@code -Ddiesel.largeTests=true}
 * (full acceptance gate).
 */
class DelimitedIoPerfTest {

    static final int ROWS = 200_000;
    static final long CEILING_MS = 30_000;

    @TempDir
    Path tempDir;

    private static List<String> schema() {
        return List.of("ID", "NAME", "AGE", "BALANCE", "ACTIVE", "TAG");
    }

    private static Map<String, Class<?>> types() {
        return Map.of(
                "ID", Long.class,
                "NAME", String.class,
                "AGE", Integer.class,
                "BALANCE", BigDecimal.class,
                "ACTIVE", Boolean.class,
                "TAG", String.class);
    }

    private static List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            rows.add(new Object[]{
                    (long) i,
                    "Name_" + i,
                    18 + (i % 80),
                    new BigDecimal("12.50"),
                    i % 2 == 0,
                    "tag " + i
            });
        }
        return rows;
    }

    @LargeTest
    void csvTsvIoPerformance() throws Exception {
        List<Object[]> data = rows();
        Path csv = tempDir.resolve("PERF_CSV.csv");
        Path tsv = tempDir.resolve("PERF_TSV.tsv");

        long writeCsv = timeWrite(csv, true, data);
        long writeTsv = timeWrite(tsv, false, data);

        long loadCsvSeq = timeStorageLoad("PERF_CSV", true, false);
        long loadCsvPar = timeStorageLoad("PERF_CSV", true, true);
        long loadTsvSeq = timeStorageLoad("PERF_TSV", false, false);
        long loadTsvPar = timeStorageLoad("PERF_TSV", false, true);

        System.out.printf(Locale.ROOT,
                "[DELIM-IO] writeCsv=%dms writeTsv=%dms loadCsvSeq=%dms loadCsvPar=%dms "
                        + "loadTsvSeq=%dms loadTsvPar=%dms rows=%d%n",
                writeCsv, writeTsv, loadCsvSeq, loadCsvPar, loadTsvSeq, loadTsvPar, ROWS);

        assertTrue(writeCsv < CEILING_MS, "CSV write took " + writeCsv + " ms (ceiling " + CEILING_MS + ")");
        assertTrue(writeTsv < CEILING_MS, "TSV write took " + writeTsv + " ms (ceiling " + CEILING_MS + ")");
        assertTrue(loadCsvSeq < CEILING_MS, "CSV sequential load took " + loadCsvSeq + " ms");
        assertTrue(loadCsvPar < CEILING_MS, "CSV parallel load took " + loadCsvPar + " ms");
        assertTrue(loadTsvSeq < CEILING_MS, "TSV sequential load took " + loadTsvSeq + " ms");
        assertTrue(loadTsvPar < CEILING_MS, "TSV parallel load took " + loadTsvPar + " ms");

        assertTrue(loadCsvSeq <= loadCsvPar * 4 + 500,
                "CSV sequential (" + loadCsvSeq + " ms) must stay within 4x of parallel ("
                        + loadCsvPar + " ms)");
        assertTrue(loadTsvSeq <= loadTsvPar * 4 + 500,
                "TSV sequential (" + loadTsvSeq + " ms) must stay within 4x of parallel ("
                        + loadTsvPar + " ms)");
    }

    private long timeWrite(Path file, boolean csv, List<Object[]> data) throws Exception {
        for (int warmup = 0; warmup < 2; warmup++) {
            writeOnce(file, csv, data);
        }
        long start = System.nanoTime();
        writeOnce(file, csv, data);
        long end = System.nanoTime();
        return (end - start) / 1_000_000;
    }

    private void writeOnce(Path file, boolean csv, List<Object[]> data) throws Exception {
        try (BufferedWriter bw = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            if (csv) {
                try (CsvRowWriter writer = new CsvRowWriter(bw, schema(), types())) {
                    writer.writeHeader();
                    for (Object[] row : data) {
                        writer.writeRow(row);
                    }
                }
            } else {
                try (TsvRowWriter writer = new TsvRowWriter(bw, schema(), types())) {
                    writer.writeHeader();
                    for (Object[] row : data) {
                        writer.writeRow(row);
                    }
                }
            }
        }
    }

    private long timeStorageLoad(String tableName, boolean csv, boolean parallel) throws Exception {
        for (int warmup = 0; warmup < 1; warmup++) {
            loadOnce(tableName, csv, parallel);
        }
        long start = System.nanoTime();
        int count = loadOnce(tableName, csv, parallel);
        long end = System.nanoTime();
        assertEquals(ROWS, count, tableName + " must load all rows");
        return (end - start) / 1_000_000;
    }

    private int loadOnce(String tableName, boolean csv, boolean parallel) throws Exception {
        if (csv) {
            CsvRowStorage storage = new CsvRowStorage(tableName, schema(), types());
            storage.setDataDir(tempDir.toString());
            storage.open();
            storage.loadFromFile(tableName, parallel);
            return storage.scan().size();
        }
        TsvRowStorage storage = new TsvRowStorage(tableName, schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.loadFromFile(tableName, parallel);
        return storage.scan().size();
    }

    @Test
    void writeOutputsRoundTripThroughStorage() throws Exception {
        List<Object[]> data = rows();
        Path csv = tempDir.resolve("ROUND_CSV.csv");
        Path tsv = tempDir.resolve("ROUND_TSV.tsv");
        writeOnce(csv, true, data);
        writeOnce(tsv, false, data);

        CsvRowStorage c = new CsvRowStorage("ROUND_CSV", schema(), types());
        c.setDataDir(tempDir.toString());
        c.open();
        c.loadFromFile("ROUND_CSV", false);
        assertEquals(ROWS, c.scan().size());
        assertEquals("Name_0", c.scan().get(0).get("NAME"));
        assertEquals(0L, c.scan().get(0).get("ID"));
        assertEquals("tag " + (ROWS - 1), c.scan().get(ROWS - 1).get("TAG"));

        TsvRowStorage t = new TsvRowStorage("ROUND_TSV", schema(), types());
        t.setDataDir(tempDir.toString());
        t.open();
        t.loadFromFile("ROUND_TSV", false);
        assertEquals(ROWS, t.scan().size());
        assertEquals("Name_1", t.scan().get(1).get("NAME"));
        assertEquals(1L, t.scan().get(1).get("ID"));
    }
}