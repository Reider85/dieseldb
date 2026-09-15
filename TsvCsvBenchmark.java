import diesel.storage.*;
import java.io.*;
import java.util.*;

/**
 * Micro-benchmark: compare CSV vs TSV read/write for the same data.
 * Tests 10 000 rows x 5 columns.
 */
public class TsvCsvBenchmark {

    static final int ROWS = 10_000;
    static final int WARMUP = 3;
    static final int RUNS = 5;

    public static void main(String[] args) throws Exception {
        List<String> columns = List.of("ID", "NAME", "AGE", "BALANCE", "INFO");
        Map<String, Class<?>> types = Map.of(
                "ID", Long.class, "NAME", String.class,
                "AGE", Integer.class, "BALANCE", Double.class, "INFO", String.class
        );

        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("ID", (long) i);
            row.put("NAME", "User_" + i);
            row.put("AGE", 20 + (i % 60));
            row.put("BALANCE", 1000.0 + i * 0.1);
            row.put("INFO", "Some info string for row " + i);
            data.add(row);
        }

        File csvFile = File.createTempFile("bench", ".csv");
        File tsvFile = File.createTempFile("bench", ".tsv");
        csvFile.deleteOnExit();
        tsvFile.deleteOnExit();

        // === WRITE BENCHMARK ===
        // CSV write
        for (int i = 0; i < WARMUP; i++) writeCsv(csvFile, columns, data);
        long csvWriteTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            writeCsv(csvFile, columns, data);
            csvWriteTotal += System.nanoTime() - t0;
        }
        double csvWriteMs = csvWriteTotal / 1e6 / RUNS;

        // TSV write
        for (int i = 0; i < WARMUP; i++) writeTsv(tsvFile, columns, data);
        long tsvWriteTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            writeTsv(tsvFile, columns, data);
            tsvWriteTotal += System.nanoTime() - t0;
        }
        double tsvWriteMs = tsvWriteTotal / 1e6 / RUNS;

        // === READ BENCHMARK ===
        // CSV read
        for (int i = 0; i < WARMUP; i++) readCsv(csvFile, columns, types);
        long csvReadTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            readCsv(csvFile, columns, types);
            csvReadTotal += System.nanoTime() - t0;
        }
        double csvReadMs = csvReadTotal / 1e6 / RUNS;

        // TSV read
        for (int i = 0; i < WARMUP; i++) readTsv(tsvFile, columns, types);
        long tsvReadTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            readTsv(tsvFile, columns, types);
            tsvReadTotal += System.nanoTime() - t0;
        }
        double tsvReadMs = tsvReadTotal / 1e6 / RUNS;

        System.out.println("=== BENCHMARK: " + ROWS + " rows x " + columns.size() + " cols, avg of " + RUNS + " runs ===");
        System.out.printf("WRITE  CSV: %8.2f ms%n", csvWriteMs);
        System.out.printf("WRITE  TSV: %8.2f ms  (ratio: %.2fx)%n", tsvWriteMs, tsvWriteMs / csvWriteMs);
        System.out.printf("READ   CSV: %8.2f ms%n", csvReadMs);
        System.out.printf("READ   TSV: %8.2f ms  (ratio: %.2fx)%n", tsvReadMs, tsvReadMs / csvReadMs);

        // unescape-only micro-benchmark
        String[] testValues = {"User_12345", "12345", "1000.5", "Some info string for row 42", "tab\there", "back\\slash"};
        System.out.println("\n=== UNESCAPE micro-bench (1M iterations) ===");
        for (String v : testValues) {
            long t0 = System.nanoTime();
            for (int i = 0; i < 1_000_000; i++) {
                TsvRowReader.unescape(v);
            }
            long elapsed = System.nanoTime() - t0;
            boolean hasBackslash = v.contains("\\");
            System.out.printf("  %-30s hasBS=%-5s -> %6.2f ms%n", v, hasBackslash, elapsed / 1e6);
        }

        System.out.println("\n=== escapeValue micro-bench (1M iterations) ===");
        Object[] testObjs = {"User_12345", 12345L, 1000.5, "tab\there", "back\\slash"};
        for (Object v : testObjs) {
            long t0 = System.nanoTime();
            for (int i = 0; i < 1_000_000; i++) {
                CsvRowWriter.escapeValue(v);
            }
            long csvTime = System.nanoTime() - t0;
            t0 = System.nanoTime();
            for (int i = 0; i < 1_000_000; i++) {
                TsvRowWriter.escapeValue(v);
            }
            long tsvTime = System.nanoTime() - t0;
            System.out.printf("  %-20s CSV=%6.2f ms  TSV=%6.2f ms (ratio: %.2fx)%n",
                    String.valueOf(v), csvTime / 1e6, tsvTime / 1e6, (double) tsvTime / csvTime);
        }
    }

    static void writeCsv(File f, List<String> cols, List<Map<String, Object>> data) throws Exception {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(f, false));
             CsvRowWriter w = new CsvRowWriter(bw, cols)) {
            w.writeHeader();
            for (Map<String, Object> row : data) w.writeRow(row);
        }
    }

    static void writeTsv(File f, List<String> cols, List<Map<String, Object>> data) throws Exception {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(f, false));
             TsvRowWriter w = new TsvRowWriter(bw, cols)) {
            w.writeHeader();
            for (Map<String, Object> row : data) w.writeRow(row);
        }
    }

    static List<Map<String, Object>> readCsv(File f, List<String> cols, Map<String, Class<?>> types) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(f));
             CsvRowReader r = new CsvRowReader(br, cols, types)) {
            r.readHeader();
            return r.readAll();
        }
    }

    static List<Map<String, Object>> readTsv(File f, List<String> cols, Map<String, Class<?>> types) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(f));
             TsvRowReader r = new TsvRowReader(br, cols, types)) {
            r.readHeader();
            return r.readAll();
        }
    }
}
