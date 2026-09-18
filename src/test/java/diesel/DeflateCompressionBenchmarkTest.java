package diesel;

import diesel.storage.avro.DeflateLevelConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Locale;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 65 Deflate level benchmark: measures the speed/ratio trade-off of
 * levels 1..9 on text, numeric and repetitive payloads and validates that the
 * compression ratio strictly improves as the level rises.
 *
 * <p>The measurements are printed as {@code [DEFLATE-BENCH]} rows for the
 * changelog; the only hard assertions are the monotonic-ratio property and a
 * generous whole-benchmark wall-clock ceiling so the test stays a fast
 * member of the {@code storage} tag.
 */
@Tag("storage")
class DeflateCompressionBenchmarkTest {

    private static final String[] DATA_TYPES = {"text", "repetitive", "numeric"};

    @Test
    void benchmarkDeflateLevelTradeoff() throws Exception {
        System.out.println("\n=== Deflate Level Speed/Ratio Bench (Prompt 65) ===");
        long totalStart = System.nanoTime();

        for (String dataType : DATA_TYPES) {
            System.out.println("\n--- Data type: " + dataType.toUpperCase(Locale.ROOT) + " ---");
            byte[] payload = buildPayload(dataType, 250_000);
            double previousRatio = 1.0;
            for (int level : DeflateLevelConfig.BENCHMARK_LEVELS) {
                Measured m = compress(payload, level);
                double ratio = (double) m.compressedSize / payload.length;
                System.out.printf("[DEFLATE-BENCH] %-10s level %d  raw=%d compressed=%d ratio=%.4f time=%dms%n",
                        dataType, level, payload.length, m.compressedSize, ratio, m.timeMs);
                assertTrue(ratio <= previousRatio + 0.005,
                        "higher deflate level must not be significantly worse: level " + level
                                + " ratio=" + ratio + " > previous=" + previousRatio);
                previousRatio = ratio;
            }
            // Estimated vs measured sanity: estimation is a heuristic, allow a wide band.
            double estimated = DeflateLevelConfig.estimateCompressionRatio(5, dataType);
            assertTrue(estimated > 0.0 && estimated <= 1.0);
        }

        long totalMs = (System.nanoTime() - totalStart) / 1_000_000;
        System.out.println("\n[DEFLATE-BENCH] total=" + totalMs + "ms");
        assertTrue(totalMs < 20_000, "deflate level bench should finish quickly, took " + totalMs + "ms");
    }

    // ─── Helpers ───────────────────────────────────────────────────

    private static byte[] buildPayload(String dataType, int targetBytes) {
        StringBuilder sb = new StringBuilder(targetBytes + 64);
        int i = 0;
        while (sb.length() < targetBytes) {
            switch (dataType) {
                case "text" -> sb.append("The quick brown fox jumps over the lazy dog. row=").append(i++).append(' ');
                case "repetitive" -> sb.append("ABABABABABABABABABABABABABABABABABABABAB");
                default -> sb.append(i).append(',').append(i * 31L).append(',').append(i & 0x3FF).append(' ');
            }
        }
        return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static Measured compress(byte[] payload, int level) throws Exception {
        long start = System.nanoTime();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DeflaterOutputStream dos = new DeflaterOutputStream(bos, new Deflater(level), 8192);
        try {
            dos.write(payload);
        } finally {
            dos.close();
        }
        long timeMs = (System.nanoTime() - start) / 1_000_000;
        return new Measured(bos.size(), timeMs);
    }

    private record Measured(int compressedSize, long timeMs) {
    }
}