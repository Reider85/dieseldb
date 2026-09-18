package diesel;

import diesel.storage.avro.SnappyOptimizedCodec;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to verify Prompt 64 Snappy optimization is working
 */
public class Prompt64VerificationTest {

    @Test
    void testSnappyOptimizedCodecBasicFunctionality() {
        // Test that we can create the codec
        assertNotNull(SnappyOptimizedCodec.newCodec());
        
        // Test buffer size constants
        assertEquals(4096, SnappyOptimizedCodec.MIN_BUFFER_SIZE);
        assertEquals(8192, SnappyOptimizedCodec.DEFAULT_BUFFER_SIZE);
        assertEquals(65536, SnappyOptimizedCodec.MAX_BUFFER_SIZE);
        
        // Test benchmark sizes
        assertEquals(5, SnappyOptimizedCodec.BENCHMARK_SIZES.length);
        assertEquals(4096, SnappyOptimizedCodec.BENCHMARK_SIZES[0]);
        assertEquals(65536, SnappyOptimizedCodec.BENCHMARK_SIZES[4]);
        
        // Test data size estimation
        var testData = List.of(
            new Object[]{1L, "test", 3.14, true},
            new Object[]{2L, "another", 2.71, false}
        );
        
        int estimatedSize = SnappyOptimizedCodec.estimateDataSize(testData);
        assertTrue(estimatedSize > 0, "Estimated size should be positive");
        
        // Test buffer size resolution
        int bufferSize = SnappyOptimizedCodec.resolveBufferSize(testData);
        assertTrue(bufferSize >= 4096, "Buffer size should be at least minimum");
        assertTrue(bufferSize <= 65536, "Buffer size should be at most maximum");
        
        // Test recommendations
        int textRecommended = SnappyOptimizedCodec.getRecommendedBufferSize("text");
        assertEquals(32768, textRecommended);
        
        int numericRecommended = SnappyOptimizedCodec.getRecommendedBufferSize("numeric");
        assertEquals(4096, numericRecommended);
        
        // Test cache functionality
        SnappyOptimizedCodec.clearCache();
        String cacheStats = SnappyOptimizedCodec.getCacheStats();
        assertTrue(cacheStats.contains("0 compressors"));
        assertTrue(cacheStats.contains("0 decompressors"));
        
        // Test benchmark functionality
        var benchmarkResults = SnappyOptimizedCodec.benchmarkCompression(testData);
        assertEquals(5, benchmarkResults.length);
        
        for (var result : benchmarkResults) {
            assertNotNull(result);
            assertTrue(result.getBufferSize() > 0);
            assertTrue(result.getCompressionTime() >= 0);
            assertTrue(result.getDecompressionTime() >= 0);
        }
        
        // Test recommendation generation
        String recommendations = SnappyOptimizedCodec.getDetailedRecommendations(testData);
        assertNotNull(recommendations);
        assertTrue(recommendations.contains("Snappy Buffer Size Recommendations"));
        
        String suggestions = SnappyOptimizedCodec.getOptimizationSuggestions(testData);
        assertNotNull(suggestions);
        assertTrue(suggestions.contains("Snappy Optimization Suggestions"));
        
        String performanceReport = SnappyOptimizedCodec.generatePerformanceReport(testData);
        assertNotNull(performanceReport);
        assertTrue(performanceReport.contains("Snappy Performance Report"));
    }
}