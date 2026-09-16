import diesel.storage.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Micro-benchmark: compare CSV vs TSV read/write for the same data.
 * Tests 200 000 rows x 5 columns.
 *
 * <p>Reports three read paths:
 * <ul>
 *   <li><b>legacy streaming</b> — {@link CsvRowReader#CsvRowReader(BufferedReader, List, Map)} char-by-char</li>
 *   <li><b>byte fast path (3.0.97+)</b> — {@link CsvRowReader#loadFast} via readAllBytes→decode→splitLines→LineSource</li>
 *   <li><b>baseline pre-3.0.97</b> — {@link CompressionFactory#openDelimitedReader} streaming, the production path before the fast-path commits</li>
 * </ul>
 *
 * <p>The write path uses the typed 3-arg {@link CsvRowWriter#CsvRowWriter(BufferedWriter, List, Map)}
 * constructor with {@code Object[]} rows (matching production), so the
 * typed-primitive {@code StringBuilder.append(long/int/double)} path is active.
 */
public class TsvCsvBenchmark {

    static final int ROWS = 200_000;
    static final int WARMUP = 5;
    static final int RUNS = 7;

    public static void main(String[] args) throws Exception {
        List<String> columns = List.of("ID", "NAME", "AGE", "BALANCE", "INFO");
        Map<String, Class<?>> types = Map.of(
                "ID", Long.class, "NAME", String.class,
                "AGE", Integer.class, "BALANCE", Double.class, "INFO", String.class
        );

        List<Object[]> data = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            data.add(new Object[]{
                    (long) i,
                    "User_" + i,
                    20 + (i % 60),
                    1000.0 + i * 0.1,
                    "Some info string for row " + i
            });
        }

        File csvFile = File.createTempFile("bench", ".csv");
        File tsvFile = File.createTempFile("bench", ".tsv");
        csvFile.deleteOnExit();
        tsvFile.deleteOnExit();

        // === WRITE BENCHMARK ===
        for (int i = 0; i < WARMUP; i++) writeCsv(csvFile, columns, types, data);
        long csvWriteTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            writeCsv(csvFile, columns, types, data);
            csvWriteTotal += System.nanoTime() - t0;
        }
        double csvWriteMs = csvWriteTotal / 1e6 / RUNS;

        for (int i = 0; i < WARMUP; i++) writeTsv(tsvFile, columns, types, data);
        long tsvWriteTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            writeTsv(tsvFile, columns, types, data);
            tsvWriteTotal += System.nanoTime() - t0;
        }
        double tsvWriteMs = tsvWriteTotal / 1e6 / RUNS;

        // === READ BENCHMARK ===

        // CSV read — legacy streaming (BufferedReader + CsvRowReader)
        for (int i = 0; i < WARMUP; i++) readCsvStreamingLegacy(csvFile, columns, types);
        long csvReadTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            readCsvStreamingLegacy(csvFile, columns, types);
            csvReadTotal += System.nanoTime() - t0;
        }
        double csvReadLegacyMs = csvReadTotal / 1e6 / RUNS;

        // CSV read — byte fast path (3.0.97+ loadFast)
        for (int i = 0; i < WARMUP; i++) readCsvFast(csvFile, columns, types);
        long csvReadFastTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            readCsvFast(csvFile, columns, types);
            csvReadFastTotal += System.nanoTime() - t0;
        }
        double csvReadFastMs = csvReadFastTotal / 1e6 / RUNS;

        // CSV read — baseline pre-3.0.97 (CompressionFactory.openDelimitedReader streaming)
        for (int i = 0; i < WARMUP; i++) readCsvBaseline8590d8c(csvFile, columns, types);
        long csvBaselineTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            readCsvBaseline8590d8c(csvFile, columns, types);
            csvBaselineTotal += System.nanoTime() - t0;
        }
        double csvBaselineMs = csvBaselineTotal / 1e6 / RUNS;

        // TSV read — legacy streaming
        for (int i = 0; i < WARMUP; i++) readTsvStreamingLegacy(tsvFile, columns, types);
        long tsvReadTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            readTsvStreamingLegacy(tsvFile, columns, types);
            tsvReadTotal += System.nanoTime() - t0;
        }
        double tsvReadLegacyMs = tsvReadTotal / 1e6 / RUNS;

        // TSV read — byte fast path (3.0.97+ loadFast)
        for (int i = 0; i < WARMUP; i++) readTsvFast(tsvFile, columns, types);
        long tsvReadFastTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            readTsvFast(tsvFile, columns, types);
            tsvReadFastTotal += System.nanoTime() - t0;
        }
        double tsvReadFastMs = tsvReadFastTotal / 1e6 / RUNS;

        // TSV read — baseline pre-3.0.97
        for (int i = 0; i < WARMUP; i++) readTsvBaseline8590d8c(tsvFile, columns, types);
        long tsvBaselineTotal = 0;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            readTsvBaseline8590d8c(tsvFile, columns, types);
            tsvBaselineTotal += System.nanoTime() - t0;
        }
        double tsvBaselineMs = tsvBaselineTotal / 1e6 / RUNS;

        System.out.println("=== BENCHMARK: " + ROWS + " rows x " + columns.size() + " cols, avg of " + RUNS + " runs ===");
        System.out.printf("WRITE  CSV (typed 3-arg ctor, Object[]):  %8.2f ms%n", csvWriteMs);
        System.out.printf("WRITE  TSV (typed 3-arg ctor, Object[]):  %8.2f ms  (ratio: %.2fx)%n", tsvWriteMs, tsvWriteMs / csvWriteMs);
        System.out.println();
        System.out.printf("READ   CSV baseline(pre-3.0.97 streaming): %8.2f ms  (not used in production since 3.0.97)%n", csvBaselineMs);
        System.out.printf("READ   CSV streaming(BufferedReader):      %8.2f ms  (legacy, not used in production since 3.0.97)%n", csvReadLegacyMs);
        System.out.printf("READ   CSV fast(loadFast readAllBytes):    %8.2f ms  (production since 3.0.97)%n", csvReadFastMs);
        System.out.printf("         fast vs baseline: %.2fx | fast vs streaming: %.2fx%n",
                csvBaselineMs / csvReadFastMs, csvReadLegacyMs / csvReadFastMs);
        System.out.println();
        System.out.printf("READ   TSV baseline(pre-3.0.97 streaming): %8.2f ms  (not used in production since 3.0.97)%n", tsvBaselineMs);
        System.out.printf("READ   TSV streaming(BufferedReader):      %8.2f ms  (legacy, not used in production since 3.0.97)%n", tsvReadLegacyMs);
        System.out.printf("READ   TSV fast(loadFast readAllBytes):    %8.2f ms  (production since 3.0.97)%n", tsvReadFastMs);
        System.out.printf("         fast vs baseline: %.2fx | fast vs streaming: %.2fx%n",
                tsvBaselineMs / tsvReadFastMs, tsvReadLegacyMs / tsvReadFastMs);

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

    // ─── Write helpers (production-matching: 3-arg ctor with types, Object[] rows) ───

    static void writeCsv(File f, List<String> cols, Map<String, Class<?>> types,
                         List<Object[]> data) throws Exception {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(f, false));
             CsvRowWriter w = new CsvRowWriter(bw, cols, types)) {
            w.writeHeader();
            for (Object[] row : data) w.writeRow(row);
        }
    }

    static void writeTsv(File f, List<String> cols, Map<String, Class<?>> types,
                         List<Object[]> data) throws Exception {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(f, false));
             TsvRowWriter w = new TsvRowWriter(bw, cols, types)) {
            w.writeHeader();
            for (Object[] row : data) w.writeRow(row);
        }
    }

    // ─── Read helpers (3 points of comparison) ───

    /**
     * Legacy streaming read via BufferedReader + CsvRowReader (char-by-char parser).
     * This path has NOT been used in production since prompt 3.0.97.
     */
    static List<Map<String, Object>> readCsvStreamingLegacy(File f, List<String> cols,
                                                            Map<String, Class<?>> types) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(f));
             CsvRowReader r = new CsvRowReader(br, cols, types)) {
            r.readHeader();
            return r.readAll();
        }
    }

    /**
     * Legacy streaming read via BufferedReader + TsvRowReader.
     * NOT used in production since prompt 3.0.97.
     */
    static List<Map<String, Object>> readTsvStreamingLegacy(File f, List<String> cols,
                                                            Map<String, Class<?>> types) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(f));
             TsvRowReader r = new TsvRowReader(br, cols, types)) {
            r.readHeader();
            return r.readAll();
        }
    }

    /** Production fast path (3.0.97+): readAllBytes → decode → splitLines → LineSource. */
    static List<Object[]> readCsvFast(File f, List<String> cols, Map<String, Class<?>> types) throws Exception {
        return CsvRowReader.loadFast(f, cols, types);
    }

    /** Production fast path (3.0.97+): readAllBytes → decode → splitLines → LineSource. */
    static List<Object[]> readTsvFast(File f, List<String> cols, Map<String, Class<?>> types) throws Exception {
        return TsvRowReader.loadFast(f, cols, types);
    }

    /**
     * Baseline pre-3.0.97 read path: CompressionFactory.openDelimitedReader streaming.
     * This is exactly what loadCsv did before commit 3.0.97.
     */
    static List<Map<String, Object>> readCsvBaseline8590d8c(File f, List<String> cols,
                                                            Map<String, Class<?>> types) throws Exception {
        try (BufferedReader br = CompressionFactory.openDelimitedReader(f,
                CompressionFactory.resolveActual(f, "csv.compression.codec").codec(),
                StandardCharsets.UTF_8);
             CsvRowReader r = new CsvRowReader(br, cols, types)) {
            r.readHeader();
            return r.readAll();
        }
    }

    /** Baseline pre-3.0.97 read path for TSV. */
    static List<Map<String, Object>> readTsvBaseline8590d8c(File f, List<String> cols,
                                                            Map<String, Class<?>> types) throws Exception {
        try (BufferedReader br = CompressionFactory.openDelimitedReader(f,
                CompressionFactory.resolveActual(f, "tsv.compression.codec").codec(),
                StandardCharsets.UTF_8);
             TsvRowReader r = new TsvRowReader(br, cols, types)) {
            r.readHeader();
            return r.readAll();
        }
    }
}
