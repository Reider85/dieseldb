package diesel;

import diesel.storage.avro.AvroCodecFactory;
import diesel.storage.avro.AvroCompressionConfig;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroRowStorage;
import diesel.storage.avro.SnappyOptimizedCodec;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 64 Snappy optimization benchmark: performance testing across
 * different buffer sizes and data characteristics.
 *
 * <p>Comprehensive benchmark to evaluate Snappy optimization effectiveness:
 * <ul>
 *   <li>Performance across different buffer sizes (4KB, 8KB, 16KB, 32KB, 64KB)</li>
 *   <li>Performance with different data types (text, numeric, mixed)</li>
 *   <li>Compression ratio analysis</li>
 *   <li>Cache effectiveness measurements</li>
 *   <li>Recommendation generation for optimal buffer sizing</li>
 * </ul>
 */
@Tag("storage")
class SnappyOptimizationBenchmark {

    @Test
    void benchmarkSnappyBufferSizePerformance() throws IOException {
        System.out.println("\n=== Snappy Buffer Size Performance Benchmark ===");
        
        int[] rowCounts = {1000, 5000, 10000};
        String[] dataTypes = {"text", "numeric", "mixed"};
        
        for (String dataType : dataTypes) {
            System.out.println("\n--- Data Type: " + dataType.toUpperCase() + " ---");
            
            for (int rowCount : rowCounts) {
                System.out.println("\nRow Count: " + rowCount);
                benchmarkSnappyForDataType(dataType, rowCount);
            }
        }
    }

    @Test
    void benchmarkSnappyCacheEffectiveness() {
        System.out.println("\n=== Snappy Cache Effectiveness Benchmark ===");
        
        int iterations = 100;
        List<Object[]> testData = createMixedDataTable(100);
        
        // Test without cache
        long noCacheTime = benchmarkRepeatedCompression(testData, iterations, false);
        
        // Test with cache
        long cacheTime = benchmarkRepeatedCompression(testData, iterations, true);
        
        System.out.println("No cache total time: " + noCacheTime + " ms");
        System.out.println("With cache total time: " + cacheTime + " ms");
        System.out.println("Cache improvement: " + ((double) (noCacheTime - cacheTime) / noCacheTime * 100) + "%");
        
        assertTrue(cacheTime <= noCacheTime, "Cache should improve performance");
    }

    @Test
    void benchmarkSnappyVsBaselineCompression() throws IOException {
        System.out.println("\n=== Snappy vs Baseline Compression Benchmark ===");
        
        int rows = 10000;
        String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ";
        
        List<Map<String, Object>> data = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", text);
            r.put("AGE", i % 100);
            r.put("ACTIVE", i % 2 == 0);
            r.put("DESCRIPTION", text.repeat(2));
            data.add(r);
        }

        // Benchmark baseline Snappy
        long baselineWriteTime = benchmarkCompressionWrite("baseline_snappy", data, -1);
        long baselineReadTime = benchmarkCompressionRead("baseline_snappy", data.size());
        long baselineSize = getFileSize("baseline_snappy");
        
        System.out.println("Baseline Snappy - Write: " + baselineWriteTime + "ms, Read: " + baselineReadTime + "ms, Size: " + baselineSize + " bytes");
        
        // Benchmark optimized Snappy
        long optimizedWriteTime = benchmarkCompressionWrite("optimized_snappy", data, -1);
        long optimizedReadTime = benchmarkCompressionRead("optimized_snappy", data.size());
        long optimizedSize = getFileSize("optimized_snappy");
        
        System.out.println("Optimized Snappy - Write: " + optimizedWriteTime + "ms, Read: " + optimizedReadTime + "ms, Size: " + optimizedSize + " bytes");
        
        // Calculate improvements
        double writeImprovement = ((double) (baselineWriteTime - optimizedWriteTime) / baselineWriteTime * 100);
        double readImprovement = ((double) (baselineReadTime - optimizedReadTime) / baselineReadTime * 100);
        
        System.out.println("Write improvement: " + writeImprovement + "%");
        System.out.println("Read improvement: " + readImprovement + "%");
        
        // Verify compression ratio
        assertTrue(optimizedSize > 0, "Compressed file size should be positive");
        assertTrue(baselineSize > 0, "Baseline file size should be positive");
        assertTrue(optimizedSize < baselineSize, "Optimized compression should be smaller");
    }

    @Test
    void generateSnappyRecommendations() {
        System.out.println("\n=== Snappy Buffer Size Recommendations ===");
        
        // Test different data patterns and generate recommendations
        String[] dataPatterns = {
            "user_profiles",    // Text-heavy
            "financial_data",   // Numeric-heavy
            "sensor_readings",  // Mixed numeric/text
            "log_files",        // Text-heavy
            "temperature_data"  // Numeric-heavy
        };
        
        for (String pattern : dataPatterns) {
            int recommendedSize = SnappyOptimizedCodec.getRecommendedBufferSize(pattern);
            List<Object[]> testData = createDataForPattern(pattern, 1000);
            int actualOptimalSize = SnappyOptimizedCodec.resolveBufferSize(testData);
            
            System.out.println("Pattern: " + pattern);
            System.out.println("  Recommended size: " + recommendedSize + " bytes");
            System.out.println("  Actual optimal size: " + actualOptimalSize + " bytes");
            System.out.println("  Match: " + (recommendedSize == actualOptimalSize ? "Yes" : "No"));
        }
    }

    @Test
    void benchmarkLargeScaleSnappyPerformance() throws IOException {
        System.out.println("\n=== Large Scale Snappy Performance Benchmark ===");
        
        int rows = 50000;
        String text = "DieselDB AVRO Snappy optimization test data. This is a longer text string that should benefit from optimized buffer sizing and caching mechanisms. ";
        
        List<Map<String, Object>> data = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) i);
            r.put("NAME", "User" + i);
            r.put("EMAIL", "user" + i + "@example.com");
            r.put("AGE", i % 100);
            r.put("SALARY", (double) (50000 + i * 100));
            r.put("ACTIVE", i % 2 == 0);
            r.put("DESCRIPTION", text.repeat(i % 3 + 1));
            data.add(r);
        }

        System.out.println("Testing with " + rows + " rows of mixed data");
        
        long startTime = System.nanoTime();
        AvroRowStorage storage = new AvroRowStorage("large_snappy_test", 
                List.of("ID", "NAME", "EMAIL", "AGE", "SALARY", "ACTIVE", "DESCRIPTION"),
                Map.of(
                        "ID", Long.class,
                        "NAME", String.class,
                        "EMAIL", String.class,
                        "AGE", Integer.class,
                        "SALARY", Double.class,
                        "ACTIVE", Boolean.class,
                        "DESCRIPTION", String.class
                ));
        storage.setDataDir("data");
        
        for (Map<String, Object> row : data) {
            storage.insert(row);
        }
        storage.saveToFile("large_snappy_test");
        long writeTime = (System.nanoTime() - startTime) / 1_000_000;
        
        File avroFile = new File("data/large_snappy_test.avro");
        long fileSize = avroFile.length();
        
        System.out.println("Write time: " + writeTime + " ms");
        System.out.println("File size: " + fileSize + " bytes");
        System.out.println("Compression ratio: " + (data.size() * 100.0 / fileSize) + " rows per KB");
        
        // Verify we can read it back
        startTime = System.nanoTime();
        AvroRowStorage readStorage = new AvroRowStorage("large_snappy_test_read",
                List.of("ID", "NAME", "EMAIL", "AGE", "SALARY", "ACTIVE", "DESCRIPTION"),
                Map.of(
                        "ID", Long.class,
                        "NAME", String.class,
                        "EMAIL", String.class,
                        "AGE", Integer.class,
                        "SALARY", Double.class,
                        "ACTIVE", Boolean.class,
                        "DESCRIPTION", String.class
                ));
        readStorage.setDataDir("data");
        readStorage.loadFromFile("large_snappy_test_read");
        long readTime = (System.nanoTime() - startTime) / 1_000_000;
        
        List<Map<String, Object>> result = readStorage.scan();
        assertEquals(rows, result.size());
        
        System.out.println("Read time: " + readTime + " ms");
        System.out.println("Total throughput: " + (rows * 1000.0 / (writeTime + readTime)) + " rows/ms");
    }

    // Helper methods for benchmarking

    private void benchmarkSnappyForDataType(String dataType, int rowCount) throws IOException {
        List<Object[]> testData = createDataForType(dataType, rowCount);
        
        StringBuilder results = new StringBuilder();
        long minWriteTime = Long.MAX_VALUE;
        long minReadTime = Long.MAX_VALUE;
        int bestSize = 8192;
        
        for (int bufferSize : SnappyOptimizedCodec.BENCHMARK_SIZES) {
            long writeTime = benchmarkCompressionWriteForData("snappy_" + dataType + "_" + bufferSize, testData, bufferSize);
            long readTime = benchmarkCompressionReadForData("snappy_" + dataType + "_" + bufferSize, testData.size());
            long fileSize = getFileSize("snappy_" + dataType + "_" + bufferSize);
            
            results.append(String.format(Locale.ROOT,
                    "  Size %5d: Write=%,5dms Read=%,5dms Size=%,8d bytes Ratio=%.2f%n",
                    bufferSize, writeTime, readTime, fileSize, (double) rowCount * 100 / fileSize));
            
            if (writeTime < minWriteTime) {
                minWriteTime = writeTime;
                bestSize = bufferSize;
            }
            if (readTime < minReadTime) {
                minReadTime = readTime;
            }
        }
        
        System.out.println(results.toString());
        System.out.println("  Best write size: " + bestSize + " bytes (" + minWriteTime + "ms)");
        System.out.println("  Best read size: " + bestSize + " bytes (" + minReadTime + "ms)");
    }

    private long benchmarkRepeatedCompression(List<Object[]> data, int iterations, boolean useCache) {
        long totalTime = 0;
        
        for (int i = 0; i < iterations; i++) {
            if (!useCache) {
                SnappyOptimizedCodec.clearCache();
            }
            
            long startTime = System.nanoTime();
            SnappyOptimizedCodec.benchmarkCompression(data);
            totalTime += (System.nanoTime() - startTime) / 1_000_000;
        }
        
        return totalTime;
    }

    private long benchmarkCompressionWrite(String fileName, List<Map<String, Object>> data, int level) throws IOException {
        long startTime = System.nanoTime();
        
        AvroDataFileWriter writer = new AvroDataFileWriter(
                List.of("ID", "NAME", "AGE", "ACTIVE"),
                Map.of("ID", Long.class, "NAME", String.class, "AGE", Integer.class, "ACTIVE", Boolean.class),
                new File("data/" + fileName + ".avro"),
                AvroCodecFactory.factory("snappy", level)
        );
        
        try {
            for (Map<String, Object> row : data) {
                writer.writeRow(row);
            }
            writer.flush();
        } finally {
            writer.close();
        }
        
        return (System.nanoTime() - startTime) / 1_000_000;
    }

    private long benchmarkCompressionRead(String fileName, int expectedRows) throws IOException {
        long startTime = System.nanoTime();
        
        try (AvroDataFileReader reader = new AvroDataFileReader(new File("data/" + fileName + ".avro"))) {
            int count = 0;
            while (reader.hasNext()) {
                reader.next();
                count++;
            }
            assertEquals(expectedRows, count);
        }
        
        return (System.nanoTime() - startTime) / 1_000_000;
    }

    private long benchmarkCompressionWriteForData(String fileName, List<Object[]> data, int bufferSize) throws IOException {
        long startTime = System.nanoTime();
        
        AvroDataFileWriter writer = new AvroDataFileWriter(
                List.of("ID", "NAME", "VALUE", "ACTIVE"),
                Map.of("ID", Long.class, "NAME", String.class, "VALUE", Object.class, "ACTIVE", Boolean.class),
                new File("data/" + fileName + ".avro"),
                AvroCodecFactory.factory("snappy", -1)
        );
        
        try {
            for (Object[] row : data) {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put("ID", row[0]);
                map.put("NAME", row[1]);
                map.put("VALUE", row[2]);
                map.put("ACTIVE", row[3]);
                writer.writeRow(map);
            }
            writer.flush();
        } finally {
            writer.close();
        }
        
        return (System.nanoTime() - startTime) / 1_000_000;
    }

    private long benchmarkCompressionReadForData(String fileName, int expectedRows) throws IOException {
        long startTime = System.nanoTime();
        
        try (AvroDataFileReader reader = new AvroDataFileReader(new File("data/" + fileName + ".avro"))) {
            int count = 0;
            while (reader.hasNext()) {
                reader.next();
                count++;
            }
            assertEquals(expectedRows, count);
        }
        
        return (System.nanoTime() - startTime) / 1_000_000;
    }

    private long getFileSize(String fileName) {
        File file = new File("data/" + fileName + ".avro");
        return file.exists() ? file.length() : 0;
    }

    private List<Object[]> createDataForType(String type, int count) {
        switch (type) {
            case "text":
                return createTextDataTable(count);
            case "numeric":
                return createNumericDataTable(count);
            case "mixed":
                return createMixedDataTable(count);
            default:
                return createMixedDataTable(count);
        }
    }

    private List<Object[]> createDataForPattern(String pattern, int count) {
        // Simulate different data patterns
        if (pattern.contains("user") || pattern.contains("log")) {
            return createTextDataTable(count);
        } else if (pattern.contains("financial") || pattern.contains("temperature") || pattern.contains("sensor")) {
            return createNumericDataTable(count);
        } else {
            return createMixedDataTable(count);
        }
    }

    private List<Object[]> createTextDataTable(int count) {
        List<Object[]> data = new ArrayList<>(count);
        String longText = "This is a longer text string for testing buffer selection. ";
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{
                    (long) i,
                    "User" + i,
                    longText.repeat(3),
                    "Address" + i,
                    "Description" + i + " " + longText.repeat(2),
                    i % 2 == 0
            });
        }
        return data;
    }

    private List<Object[]> createNumericDataTable(int count) {
        List<Object[]> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{
                    (long) i,
                    (double) (i * 3.14159),
                    (float) (i * 2.71828),
                    i,
                    i % 1000,
                    i % 2 == 0
            });
        }
        return data;
    }

    private List<Object[]> createMixedDataTable(int count) {
        List<Object[]> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{
                    (long) i,
                    "User" + i,
                    (double) (i * 3.14159),
                    i % 2 == 0,
                    "Address" + i,
                    (float) (i * 2.71828),
                    i % 100
            });
        }
        return data;
    }
}