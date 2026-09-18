package diesel;

import diesel.storage.avro.SnappyOptimizedCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 64 SnappyOptimizedCodec tests: buffer size selection, caching,
 * benchmarking, and recommendations.
 *
 * <p>Tests verify the optimized Snappy codec functionality including:
 * <ul>
 *   <li>Buffer size selection based on data characteristics</li>
 *   <li>Compressor/decompressor caching</li>
 *   <li>Benchmark performance across different buffer sizes</li>
 *   <li>Data type analysis and recommendations</li>
 * </ul>
 */
@Tag("storage")
class SnappyOptimizedCodecTest {

    private static final String[] PROP_KEYS = {
            "avro.snappy.buffer.size",
            "avro.snappy.cache.compressors",
            "avro.snappy.auto.optimize"
    };

    private final List<String> prevProps = new ArrayList<>();

    @BeforeEach
    void saveConfig() {
        for (String key : PROP_KEYS) {
            String prev = System.getProperty(key);
            prevProps.add(prev);
            if (prev != null) {
                System.setProperty(key, prev);
            } else {
                System.clearProperty(key);
            }
        }
    }

    @Test
    void defaultBufferSizeConstants() {
        assertEquals(4096, SnappyOptimizedCodec.MIN_BUFFER_SIZE);
        assertEquals(8192, SnappyOptimizedCodec.DEFAULT_BUFFER_SIZE);
        assertEquals(65536, SnappyOptimizedCodec.MAX_BUFFER_SIZE);
        assertEquals(5, SnappyOptimizedCodec.BENCHMARK_SIZES.length);
        assertEquals(4096, SnappyOptimizedCodec.BENCHMARK_SIZES[0]);
        assertEquals(65536, SnappyOptimizedCodec.BENCHMARK_SIZES[4]);
    }

    @Test
    void resolvesDefaultBufferSizeForEmptyData() {
        List<Object[]> emptyData = new ArrayList<>();
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(emptyData);
        assertEquals(SnappyOptimizedCodec.DEFAULT_BUFFER_SIZE, bufferSize);
    }

    @Test
    void resolvesSmallDataSize() {
        List<Object[]> smallData = createSmallData(10);
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(smallData);
        assertTrue(bufferSize >= SnappyOptimizedCodec.MIN_BUFFER_SIZE);
        assertTrue(bufferSize <= SnappyOptimizedCodec.DEFAULT_BUFFER_SIZE);
    }

    @Test
    void resolvesTextDataWithLargerBuffer() {
        List<Object[]> textData = createTextData(100);
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(textData);
        assertTrue(bufferSize >= 16384, "Text data should use larger buffers");
        assertTrue(bufferSize <= 32768, "Text data buffer should not exceed 32KB");
    }

    @Test
    void resolvesNumericDataWithSmallerBuffer() {
        List<Object[]> numericData = createNumericData(100);
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(numericData);
        assertTrue(bufferSize >= 4096, "Numeric data should use at least 4KB buffers");
        assertTrue(bufferSize <= 8192, "Numeric data buffer should not exceed 8KB");
    }

    @Test
    void resolvesMixedDataWithMediumBuffer() {
        List<Object[]> mixedData = createMixedData(100);
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(mixedData);
        assertTrue(bufferSize >= 8192, "Mixed data should use at least 8KB buffers");
        assertTrue(bufferSize <= 16384, "Mixed data buffer should not exceed 16KB");
    }

    @Test
    void estimatesDataSizeCorrectly() {
        List<Object[]> data = createTextData(50);
        int estimatedSize = SnappyOptimizedCodec.estimateDataSize(data);
        assertTrue(estimatedSize > 0, "Estimated size should be positive");
        assertTrue(estimatedSize < 1000000, "Estimated size should be reasonable");
    }

    @Test
    void clearsCache() {
        // Add something to cache first
        SnappyOptimizedCodec.clearCache();
        String stats = SnappyOptimizedCodec.getCacheStats();
        assertTrue(stats.contains("0 compressors") && stats.contains("0 decompressors"),
                "Cache should be empty after clear");
    }

    @Test
    void getsCacheStats() {
        SnappyOptimizedCodec.clearCache();
        String stats = SnappyOptimizedCodec.getCacheStats();
        assertNotNull(stats);
        assertTrue(stats.contains("Snappy cache"));
    }

    @Test
    void createsCodecFactory() {
        assertNotNull(SnappyOptimizedCodec.newCodec());
        assertTrue(SnappyOptimizedCodec.newCodec().toString().contains("snappy"),
                "Codec factory should be for Snappy");
    }

    @Test
    void benchmarksCompressionWithDefaultSizes() {
        List<Object[]> data = createTextData(100);
        SnappyOptimizedCodec.BenchmarkResult[] results = 
                SnappyOptimizedCodec.benchmarkCompression(data);
        
        assertEquals(SnappyOptimizedCodec.BENCHMARK_SIZES.length, results.length);
        
        for (SnappyOptimizedCodec.BenchmarkResult result : results) {
            assertNotNull(result);
            assertTrue(result.getBufferSize() > 0);
            assertTrue(result.getFactoryTime() >= 0);
            assertTrue(result.getCompressionTime() >= 0);
            assertTrue(result.getDecompressionTime() >= 0);
            assertTrue(result.getDataSize() > 0);
            assertTrue(result.getCompressionRatio() >= 0);
            assertTrue(result.getDecompressionRatio() >= 0);
        }
    }

    @Test
    void benchmarksCompressionWithCustomSize() {
        List<Object[]> data = createNumericData(50);
        int customSize = 16384;
        SnappyOptimizedCodec.BenchmarkResult result = 
                SnappyOptimizedCodec.benchmarkCompression(data, customSize);
        
        assertNotNull(result);
        assertEquals(customSize, result.getBufferSize());
        assertTrue(result.getCompressionTime() >= 0);
        assertTrue(result.getDecompressionTime() >= 0);
    }

    @Test
    void recommendsBufferSizeForTextData() {
        int recommended = SnappyOptimizedCodec.getRecommendedBufferSize("text");
        assertEquals(32768, recommended);
        
        recommended = SnappyOptimizedCodec.getRecommendedBufferSize("string");
        assertEquals(32768, recommended);
    }

    @Test
    void recommendsBufferSizeForNumericData() {
        int recommended = SnappyOptimizedCodec.getRecommendedBufferSize("numeric");
        assertEquals(4096, recommended);
        
        recommended = SnappyOptimizedCodec.getRecommendedBufferSize("int");
        assertEquals(4096, recommended);
        
        recommended = SnappyOptimizedCodec.getRecommendedBufferSize("double");
        assertEquals(4096, recommended);
    }

    @Test
    void recommendsBufferSizeForMixedData() {
        int recommended = SnappyOptimizedCodec.getRecommendedBufferSize("mixed");
        assertEquals(16384, recommended);
        
        recommended = SnappyOptimizedCodec.getRecommendedBufferSize("json");
        assertEquals(16384, recommended);
    }

    @Test
    void recommendsBufferSizeForBinaryData() {
        int recommended = SnappyOptimizedCodec.getRecommendedBufferSize("binary");
        assertEquals(8192, recommended);
        
        recommended = SnappyOptimizedCodec.getRecommendedBufferSize("blob");
        assertEquals(8192, recommended);
    }

    @Test
    void recommendsDefaultBufferSizeForUnknownType() {
        int recommended = SnappyOptimizedCodec.getRecommendedBufferSize("unknown");
        assertEquals(SnappyOptimizedCodec.DEFAULT_BUFFER_SIZE, recommended);
    }

    @Test
    void testCaseInsensitiveDataTypeRecommendations() {
        int recommended = SnappyOptimizedCodec.getRecommendedBufferSize("TEXT");
        assertEquals(32768, recommended);
        
        recommended = SnappyOptimizedCodec.getRecommendedBufferSize("NuMeRiC");
        assertEquals(4096, recommended);
    }

    @Test
    void resolvesBufferSizeWithAutoOptimizationDisabled() {
        System.setProperty("avro.snappy.auto.optimize", "false");
        
        List<Object[]> data = createMixedData(100);
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(data);
        
        // Should still return a valid buffer size even with auto-optimization disabled
        assertTrue(bufferSize >= SnappyOptimizedCodec.MIN_BUFFER_SIZE);
        assertTrue(bufferSize <= SnappyOptimizedCodec.MAX_BUFFER_SIZE);
        
        // Restore default
        System.setProperty("avro.snappy.auto.optimize", "true");
    }

    @Test
    void respectsCustomBufferSizeConfig() {
        System.setProperty("avro.snappy.buffer.size", "16384");
        
        List<Object[]> data = createTextData(100);
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(data);
        
        // Should still work with custom config
        assertTrue(bufferSize >= SnappyOptimizedCodec.MIN_BUFFER_SIZE);
        
        // Restore default
        System.setProperty("avro.snappy.buffer.size", "8192");
    }

    @Test
    void benchmarkResultToStringContainsAllMetrics() {
        List<Object[]> data = createTextData(10);
        SnappyOptimizedCodec.BenchmarkResult result = 
                SnappyOptimizedCodec.benchmarkCompression(data, 8192);
        
        String resultStr = result.toString();
        assertTrue(resultStr.contains("BufferSize"));
        assertTrue(resultStr.contains("Factory"));
        assertTrue(resultStr.contains("Compression"));
        assertTrue(resultStr.contains("Decompression"));
        assertTrue(resultStr.contains("Ratio"));
    }

    // Helper methods to create test data

    private List<Object[]> createSmallData(int count) {
        List<Object[]> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{i, "small"});
        }
        return data;
    }

    private List<Object[]> createTextData(int count) {
        List<Object[]> data = new ArrayList<>(count);
        String longText = "This is a longer text string that should trigger text-based buffer selection. ";
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{
                    i, 
                    "User" + i, 
                    longText.repeat(2), 
                    "Address" + i,
                    "Description" + i + " " + longText.repeat(1)
            });
        }
        return data;
    }

    private List<Object[]> createNumericData(int count) {
        List<Object[]> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{
                    (long) i,
                    (double) (i * 3.14),
                    (float) (i * 2.71),
                    i,
                    i % 1000
            });
        }
        return data;
    }

    private List<Object[]> createMixedData(int count) {
        List<Object[]> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            data.add(new Object[]{
                    (long) i,
                    "User" + i,
                    (double) (i * 3.14),
                    i % 2 == 0,
                    "Address" + i,
                    (float) (i * 2.71)
            });
        }
        return data;
    }
}