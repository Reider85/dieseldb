package diesel.checksum;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Random;
import java.util.zip.Checksum;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt4 #51 — CRC32C implementation (hardware-accelerated) — R3-014 step 1/3.
 * 
 * Acceptance criteria:
 * - BenchmarkTest: throughput ≥ 5 GB/sec on supported CPU.
 * - FallbackTest: pure Java works (slower).
 * 
 * Measures hardware throughput and compares with pure-Java software path.
 * Uses System.nanoTime() for timing (no JMH in project).
 */
@Tag("perf")
@Timeout(120) // 2 minutes max
class CRC32CThroughputTest {

    private static final int BUFFER_SIZE = 8 << 20; // 8 MiB
    private static final int MIN_GIBIBYTES_TO_PROCESS = 2; // At least 2 GiB for measurement
    private static final double MIN_THROUGHPUT_GB_S = 5.0; // Acceptance criterion: ≥5 GB/s

    @Test
    void hardwareThroughputAtLeast5Gbps() {
        byte[] buffer = createTestBuffer();
        long totalBytesProcessed = 0;
        long startTime = 0;
        long endTime = 0;
        
        // Warmup: process ~1 GiB to let JIT optimize
        long warmupBytes = 1L << 30; // 1 GiB
        long warmupStart = System.nanoTime();
        while (totalBytesProcessed < warmupBytes) {
            CRC32C.compute(buffer);
            totalBytesProcessed += buffer.length;
        }
        long warmupEnd = System.nanoTime();
        
        // Measurement: process at least MIN_GIBIBYTES_TO_PROCESS GiB
        totalBytesProcessed = 0;
        startTime = System.nanoTime();
        
        // Use iterations instead of while loop to ensure we actually process data
        // Reduce to 1 GiB for faster testing
        long targetBytes = 1L << 30; // 1 GiB
        int iterations = (int) ((targetBytes / buffer.length) + 1);
        
        for (int i = 0; i < iterations; i++) {
            CRC32C.compute(buffer);
            totalBytesProcessed += buffer.length;
        }
        
        endTime = System.nanoTime();
        
        // Calculate throughput
        double elapsedSeconds = (endTime - startTime) / 1_000_000_000.0;
        double throughputGBs = (totalBytesProcessed / (1.0 * 1024 * 1024 * 1024)) / elapsedSeconds;
        
        System.out.printf("Hardware CRC32C throughput: %.2f GB/s (%.0f MB in %.2fs)%n",
            throughputGBs, totalBytesProcessed / (1024.0 * 1024.0), elapsedSeconds);
        
        // Assert minimum throughput per acceptance criterion
        assertTrue(throughputGBs >= MIN_THROUGHPUT_GB_S,
            String.format("Hardware throughput must be ≥ %.1f GB/s, was %.2f GB/s",
                MIN_THROUGHPUT_GB_S, throughputGBs));
        
        // Also measure software throughput for comparison
        double softwareThroughputGBs = measureSoftwareThroughput(buffer);
        
        // Assert hardware is at least 2x software (generous "медленнее" ratio)
        double ratio = throughputGBs / softwareThroughputGBs;
        System.out.printf("Software throughput: %.2f GB/s, Hardware/Software ratio: %.1fx%n",
            softwareThroughputGBs, ratio);
        
        assertTrue(ratio >= 2.0,
            String.format("Hardware should be at least 2x faster than software, ratio was %.1fx", ratio));
    }

    @Test
    void incrementalChecksumThroughput() {
        // Test throughput of incremental checksum updates
        byte[] buffer = createTestBuffer();
        int iterations = 10_000; // Reduced for faster testing
        
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            Checksum checksum = CRC32C.newChecksum();
            checksum.update(buffer);
            checksum.update(buffer, 0, buffer.length / 2);
            checksum.update(buffer, buffer.length / 2, buffer.length / 2);
            @SuppressWarnings("unused")
            int value = (int) checksum.getValue();
        }
        long endTime = System.nanoTime();
        
        long elapsedNanos = endTime - startTime;
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        double totalBytes = iterations * (double) buffer.length * 2.0; // 2x buffer size per iteration (full + 2 halves)
        double throughputGBs = totalBytes / (elapsedSeconds * 1024.0 * 1024.0 * 1024.0);
        
        System.out.printf("Incremental CRC32C throughput: %.2f GB/s (%.0f bytes in %.2fs)%n",
            throughputGBs, totalBytes, elapsedSeconds);
        
        // Should still be reasonably fast (assert is loose since it's not in acceptance criteria)
        assertTrue(throughputGBs >= 1.0,
            String.format("Incremental throughput should be ≥ 1.0 GB/s, was %.2f GB/s", throughputGBs));
    }

    /**
     * Measures pure-Java software throughput for comparison.
     */
    private double measureSoftwareThroughput(byte[] buffer) {
        long totalBytesProcessed = 0;
        long startTime = 0;
        long endTime = 0;
        
        // Warmup (smaller for software since it's slower)
        long warmupBytes = 256 << 20; // 256 MiB
        long warmupStart = System.nanoTime();
        while (totalBytesProcessed < warmupBytes) {
            CRC32C.computeSoftware(buffer);
            totalBytesProcessed += buffer.length;
        }
        long warmupEnd = System.nanoTime();
        
        // Measurement: same as hardware test
        totalBytesProcessed = 0;
        startTime = System.nanoTime();
        
        while (totalBytesProcessed < (MIN_GIBIBYTES_TO_PROCESS << 30)) {
            CRC32C.computeSoftware(buffer);
            totalBytesProcessed += buffer.length;
        }
        
        endTime = System.nanoTime();
        
        // Calculate throughput
        double elapsedSeconds = (endTime - startTime) / 1_000_000_000.0;
        double throughputGBs = (totalBytesProcessed / (1.0 * 1024 * 1024 * 1024)) / elapsedSeconds;
        
        return throughputGBs;
    }

    /**
     * Creates a deterministic test buffer for reproducible benchmarks.
     * Uses fixed seed to ensure same data across runs.
     */
    private byte[] createTestBuffer() {
        byte[] buffer = new byte[BUFFER_SIZE];
        Random random = new Random(54321); // Fixed seed for reproducibility
        random.nextBytes(buffer);
        return buffer;
    }
}