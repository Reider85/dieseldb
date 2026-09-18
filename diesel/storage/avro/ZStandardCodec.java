package diesel.storage.avro;

import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dedicated ZStandard (zstd) codec support for AVRO data files (Prompt 63).
 *
 * <p>Zstd is the linked codec for the AVRO backend: the native library comes
 * from {@code com.github.luben:zstd-jni} (declared in {@code pom.xml}), and
 * Avro's built-in {@link CodecFactory#zstandardCodec(int)} provides both the
 * write-side encoder and the read-side decoder. Reading requires no extra
 * registration — Avro resolves the codec from the file header's codec name, so
 * a {@code zstandard} file created here is decoded transparently by
 * {@link AvroDataFileReader}.
 *
 * <p>The compressible level range for zstd is {@code 1..22} (1 = fastest,
 * 22 = strongest). {@code -1} (or {@link AvroCompressionConfig#DEFAULT_LEVEL})
 * resolves to Avro's built-in default level (3).
 *
 * @since Prompt 63
 */
public final class ZStandardCodec {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZStandardCodec.class);

    /** Canonical AVRO codec name for zstd, as stored in the file header. */
    public static final String CODEC_NAME = "zstandard";

    /** Lowest supported zstd compression level (fastest). */
    public static final int MIN_LEVEL = 1;

    /** Highest supported zstd compression level (strongest). */
    public static final int MAX_LEVEL = 22;

    /** Avro's built-in default zstd level (3). */
    public static final int DEFAULT_LEVEL = CodecFactory.DEFAULT_ZSTANDARD_LEVEL;

    private ZStandardCodec() {
        throw new AssertionError("No instances");
    }

    /**
     * Resolves a zstd level to a valid compressible range: {@code -1} maps to
     * the Avro default, out-of-range values are clamped to {@code [1, 22]}.
     *
     * @param level the configured level
     * @return the effective level
     */
    public static int resolveLevel(int level) {
        if (level == AvroCompressionConfig.DEFAULT_LEVEL) {
            return DEFAULT_LEVEL;
        }
        if (level < MIN_LEVEL || level > MAX_LEVEL) {
            LOGGER.warn("AVRO zstandard level {} is outside the valid range [{}, {}], clamping",
                    level, MIN_LEVEL, MAX_LEVEL);
            return Math.max(MIN_LEVEL, Math.min(MAX_LEVEL, level));
        }
        return level;
    }

    /**
     * Builds the Avro {@link CodecFactory} for writing zstd-compressed data
     * files at the given level (resolved via {@link #resolveLevel(int)}).
     *
     * @param level compression level ({@code -1} = Avro default)
     * @return the Avro zstandard write codec
     */
    public static CodecFactory newCodec(int level) {
        return CodecFactory.zstandardCodec(resolveLevel(level));
    }
}