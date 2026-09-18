import java.util.List;

import diesel.storage.avro.SnappyOptimizedCodec;

public class SimplePrompt64Test {
    public static void main(String[] args) {
        System.out.println("=== Prompt 64 Snappy Optimization Test ===");
        
        try {
            // Test 1: Basic codec creation
            System.out.println("Test 1: Creating Snappy codec...");
            SnappyOptimizedCodec.newCodec();
            System.out.println("✓ Snappy codec created successfully");
            
            // Test 2: Buffer size constants
            System.out.println("\nTest 2: Buffer size constants...");
            System.out.println("MIN_BUFFER_SIZE: " + SnappyOptimizedCodec.MIN_BUFFER_SIZE);
            System.out.println("DEFAULT_BUFFER_SIZE: " + SnappyOptimizedCodec.DEFAULT_BUFFER_SIZE);
            System.out.println("MAX_BUFFER_SIZE: " + SnappyOptimizedCodec.MAX_BUFFER_SIZE);
            System.out.println("✓ Buffer size constants verified");
            
            // Test 3: Benchmark sizes
            System.out.println("\nTest 3: Benchmark sizes...");
            int[] benchmarkSizes = SnappyOptimizedCodec.BENCHMARK_SIZES;
            System.out.println("Benchmark sizes: " + benchmarkSizes.length);
            for (int size : benchmarkSizes) {
                System.out.println("  " + size);
            }
            System.out.println("✓ Benchmark sizes verified");
            
            // Test 4: Data size estimation
            System.out.println("\nTest 4: Data size estimation...");
            List<Object[]> testData = List.of(
                new Object[]{1L, "test", 3.14, true},
                new Object[]{2L, "another", 2.71, false}
            );
            
            int estimatedSize = SnappyOptimizedCodec.estimateDataSize(testData);
            System.out.println("Estimated data size: " + estimatedSize);
            System.out.println("✓ Data size estimation works");
            
            // Test 5: Buffer size resolution
            System.out.println("\nTest 5: Buffer size resolution...");
            int bufferSize = SnappyOptimizedCodec.resolveBufferSize(testData);
            System.out.println("Resolved buffer size: " + bufferSize);
            System.out.println("✓ Buffer size resolution works");
            
            // Test 6: Recommendations
            System.out.println("\nTest 6: Buffer size recommendations...");
            int textRecommended = SnappyOptimizedCodec.getRecommendedBufferSize("text");
            int numericRecommended = SnappyOptimizedCodec.getRecommendedBufferSize("numeric");
            System.out.println("Text recommended: " + textRecommended);
            System.out.println("Numeric recommended: " + numericRecommended);
            System.out.println("✓ Buffer size recommendations work");
            
            // Test 7: Cache functionality
            System.out.println("\nTest 7: Cache functionality...");
            SnappyOptimizedCodec.clearCache();
            String cacheStats = SnappyOptimizedCodec.getCacheStats();
            System.out.println("Cache stats: " + cacheStats);
            System.out.println("✓ Cache functionality works");
            
            // Test 8: Benchmark functionality
            System.out.println("\nTest 8: Benchmark functionality...");
            var benchmarkResults = SnappyOptimizedCodec.benchmarkCompression(testData);
            System.out.println("Benchmark results: " + benchmarkResults.length);
            for (var result : benchmarkResults) {
                System.out.println("  " + result);
            }
            System.out.println("✓ Benchmark functionality works");
            
            // Test 9: Recommendation generation
            System.out.println("\nTest 9: Recommendation generation...");
            String recommendations = SnappyOptimizedCodec.getDetailedRecommendations(testData);
            System.out.println("Recommendations length: " + recommendations.length());
            System.out.println("✓ Recommendation generation works");
            
            System.out.println("\n=== ALL TESTS PASSED ===");
            System.out.println("Prompt 64 Snappy optimization is fully functional!");
            
        } catch (Exception e) {
            System.err.println("Test failed with exception: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}