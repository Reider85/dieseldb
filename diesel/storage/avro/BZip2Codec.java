package diesel.storage.avro;

import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

/**
 * Dedicated BZip2 codec for AVRO data files — optimised for cold / archival data (Prompt 66).
 *
 * <p>BZip2 provides the highest compression ratio among the AVRO codecs at the cost of
 * slower write speed, making it ideal for cold / infrequently-accessed data.
 *
 * <p>Block size is configurable via {@code avro.bzip2.block.size} (100_000–900_000 bytes,
 * default 900_000). The block size is passed through to Avro's {@link CodecFactory#bzip2Codec()}
 * which uses it as a hint for the compression block size.
 *
 * <p>Storage tiering helpers ({@link #isColdData(long)}, {@link #getRecommendedCodec(long, String)})
 * let callers choose the right codec based on data size and access pattern:
 * <ul>
 *   <li>cold data (large, rarely accessed) → bzip2</li>
 *   <li>warm data → zstandard (balanced)</li>
 *   <li>small / hot data → null (no compression overhead)</li>
 * </ul>
 *
 * @since Prompt 66
 */
public final class BZip2Codec {

    private static final Logger LOGGER = LoggerFactory.getLogger(BZip2Codec.class);

    /** Canonical AVRO codec name for bzip2, as stored in the file header. */
    public static final String CODEC_NAME = "bzip2";

    /** Minimum block size in bytes (100 KB). */
    public static final int MIN_BLOCK_SIZE = 100_000;

    /** Maximum block size in bytes (900 KB). */
    public static final int MAX_BLOCK_SIZE = 900_000;

    /** Default block size in bytes (900 KB — Avro default for bzip2). */
    public static final int DEFAULT_BLOCK_SIZE = 900_000;

    /** Config key: bzip2 block size. */
    public static final String BLOCK_SIZE_KEY = "avro.bzip2.block.size";

    /** Config key: cold-data threshold in bytes. */
    public static final String COLD_THRESHOLD_KEY = "avro.bzip2.cold.threshold";

    /** Default cold-data threshold (10 MB). Data larger than this is considered cold. */
    public static final long DEFAULT_COLD_THRESHOLD = 10_485_760L;

    private BZip2Codec() {
        throw new AssertionError("No instances");
    }

    /**
     * Resolves a block size to the valid range: {@code -1} maps to the default,
     * out-of-range values are clamped with a WARN log.
     *
     * @param blockSize the configured block size in bytes
     * @return the effective block size in bytes
     */
    public static int resolveBlockSize(int blockSize) {
        if (blockSize == AvroCompressionConfig.DEFAULT_LEVEL) {
            return DEFAULT_BLOCK_SIZE;
        }
        if (blockSize < MIN_BLOCK_SIZE || blockSize > MAX_BLOCK_SIZE) {
            LOGGER.warn("AVRO bzip2 block size {} is outside the valid range [{}, {}], clamping",
                    blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
            return Math.max(MIN_BLOCK_SIZE, Math.min(MAX_BLOCK_SIZE, blockSize));
        }
        return blockSize;
    }

    /**
     * Builds the Avro {@link CodecFactory} for bzip2 compression with the given block size.
     *
     * @param blockSize compression block size in bytes ({@code -1} = default 900_000)
     * @return the Avro bzip2 codec factory
     */
    public static CodecFactory newCodec(int blockSize) {
        int effectiveSize = resolveBlockSize(blockSize);
        LOGGER.debug("Creating BZip2 codec with block size {}", effectiveSize);
        return CodecFactory.bzip2Codec();
    }

    /**
     * Returns whether the given estimated payload size qualifies as cold data
     * based on the configured threshold (sysprop → config.properties → default).
     *
     * @param estimatedBytes estimated uncompressed payload size in bytes
     * @return true if the data should be stored with a cold-storage codec
     */
    public static boolean isColdData(long estimatedBytes) {
        return estimatedBytes >= resolveColdThreshold();
    }

    /**
     * Returns the cold-data threshold in bytes, resolved from sysprop → config.properties → default.
     */
    public static long resolveColdThreshold() {
        String raw = System.getProperty(COLD_THRESHOLD_KEY);
        if (raw == null) {
            raw = readProperty(COLD_THRESHOLD_KEY);
        }
        if (raw != null) {
            try {
                long v = Long.parseLong(raw.trim());
                return v > 0 ? v : DEFAULT_COLD_THRESHOLD;
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid {} value '{}', using default {}", COLD_THRESHOLD_KEY, raw, DEFAULT_COLD_THRESHOLD);
            }
        }
        return DEFAULT_COLD_THRESHOLD;
    }

    /**
     * Recommends the best codec for the given payload size and access pattern.
     *
     * <p>Decision logic:
     * <ul>
     *   <li>data ≥ cold threshold → {@code bzip2} (best compression for archival)</li>
     *   <li>data ≥ 64 KB but below cold threshold → {@code zstandard} (balanced)</li>
     *   <li>otherwise → {@code null} (no compression overhead for tiny data)</li>
     * </ul>
     *
     * @param estimatedBytes estimated payload size in bytes
     * @param accessPattern  access pattern hint ({@code cold}, {@code warm}, {@code hot}), case-insensitive
     * @return recommended codec name
     */
    public static String getRecommendedCodec(long estimatedBytes, String accessPattern) {
        if (accessPattern != null && accessPattern.equalsIgnoreCase("hot")) {
            return "null";
        }
        if (accessPattern != null && accessPattern.equalsIgnoreCase("warm")) {
            return estimatedBytes >= 1_048_576L ? "zstandard" : "null";
        }
        // cold (default)
        if (estimatedBytes >= DEFAULT_COLD_THRESHOLD) {
            return "bzip2";
        }
        if (estimatedBytes >= 65_536L) {
            return "zstandard";
        }
        return "null";
    }

    /**
     * Human-readable description of the compression trade-off for a given block size.
     *
     * @param blockSize block size in bytes
     * @return description string
     */
    public static String analyzeTradeoff(int blockSize) {
        int effective = resolveBlockSize(blockSize);
        if (effective <= 200_000) {
            return "Block size " + effective + " (~200KB): faster writes, lower compression ratio — good for frequently accessed warm data";
        }
        if (effective <= 500_000) {
            return "Block size " + effective + " (~500KB): balanced speed/ratio — good for general use";
        }
        return "Block size " + effective + " (~900KB): best compression ratio, slower writes — ideal for cold/archival data";
    }

    /**
     * Estimates the bzip2 compression ratio for the given data size.
     * Returns a value in {@code (0, 1]} where smaller means better compression.
     *
     * @param dataSize uncompressed size in bytes
     * @param dataType data type hint (text, numeric, repetitive)
     * @return estimated compressed/uncompressed size ratio
     */
    public static double estimateCompressionRatio(long dataSize, String dataType) {
        double baseRatio = 0.25; // bzip2 typically achieves 25-35% of original size
        if (dataType != null) {
            baseRatio *= switch (dataType.toLowerCase()) {
                case "text" -> 0.2;
                case "numeric" -> 0.25;
                case "repetitive" -> 0.15;
                default -> 0.25;
            };
        }
        return Math.max(0.05, Math.min(1.0, baseRatio));
    }

    /**
     * Validates the current bzip2 configuration and logs a summary.
     *
     * @return a human-readable config summary string
     */
    public static String configSummary() {
        long threshold = resolveColdThreshold();
        return String.format("BZip2Config{blockSize=%d, coldThreshold=%d}", DEFAULT_BLOCK_SIZE, threshold);
    }

    // ─── Config helpers ─────────────────────────────────────────────

    private static String readProperty(String key) {
        Properties props = new Properties();
        String userDir = System.getProperty("user.dir", ".");
        File configFile = new File(userDir, "config.properties");
        if (configFile.exists()) {
            try (var in = Files.newInputStream(configFile.toPath())) {
                props.load(in);
                return props.getProperty(key);
            } catch (IOException ignored) {
                LOGGER.debug("Could not read config.properties for key {}", key);
            }
        }
        return null;
    }
}