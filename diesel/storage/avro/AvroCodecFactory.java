package diesel.storage.avro;

import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Maps the configured AVRO codec name and level to a concrete Avro
 * {@link CodecFactory} (Prompt 62).
 *
 * <p>The mapping uses Avro 1.12's public static factory methods, so no Avro
 * internals are reached reflectively:
 * <ul>
 *   <li>{@code null} → {@link CodecFactory#nullCodec()}</li>
 *   <li>{@code deflate} → {@link CodecFactory#deflateCodec(int)} (level clamped 0..9)</li>
 *   <li>{@code snappy} → {@link CodecFactory#snappyCodec()} (no level)</li>
 *   <li>{@code zstandard} → {@link CodecFactory#zstandardCodec(int)} (level clamped 1..22)</li>
 *   <li>{@code bzip2} → {@link CodecFactory#bzip2Codec()} (no level)</li>
 * </ul>
 *
 * <p>A level of {@code -1} (or {@code AvroCompressionConfig#DEFAULT_LEVEL})
 * resolves to the Avro codec's built-in default for the level-bearing codecs
 * (deflate {@code -1}, zstandard {@code 3}).
 *
 * @since Prompt 62
 */
public final class AvroCodecFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroCodecFactory.class);

    private static final int DEFLATE_MIN_LEVEL = 0;
    private static final int DEFLATE_MAX_LEVEL = 9;
    private static final int ZSTD_MIN_LEVEL = 1;
    private static final int ZSTD_MAX_LEVEL = 22;

    private AvroCodecFactory() {
        throw new AssertionError("No instances");
    }

    /**
     * Builds the Avro {@link CodecFactory} for the given codec name and level.
     *
     * @param codec canonical codec name: {@code null}, {@code deflate},
     *              {@code snappy}, {@code zstandard} or {@code bzip2}
     * @param level compression level ({@code -1} = codec default); ignored by
     *              {@code null}, {@code snappy} and {@code bzip2}
     * @return the Avro codec factory for writing
     * @throws IllegalArgumentException when {@code codec} is unsupported
     */
    public static CodecFactory factory(String codec, int level) {
        String name = codec == null ? "" : codec.trim().toLowerCase(Locale.ROOT);
        return switch (name) {
            case "null" -> CodecFactory.nullCodec();
            case "snappy" -> CodecFactory.snappyCodec();
            case "bzip2" -> CodecFactory.bzip2Codec();
            case "deflate" -> CodecFactory.deflateCodec(resolveDeflateLevel(level));
            case "zstandard" -> CodecFactory.zstandardCodec(resolveZstandardLevel(level));
            default -> throw new IllegalArgumentException(
                    "Unknown AVRO compression codec '" + codec + "' (expected: null, deflate, snappy, zstandard, bzip2)");
        };
    }

    /**
     * Resolves the deflate level: {@code -1} maps to the Avro/JDK default,
     * out-of-range values are clamped to {@code [0, 9]}, and unparsable values
     * fall back to {@code -1}.
     */
    static int resolveDeflateLevel(int level) {
        return clamp(level, DEFLATE_MIN_LEVEL, DEFLATE_MAX_LEVEL, "deflate", -1);
    }

    /**
     * Resolves the zstandard level: {@code -1} maps to the Avro default (3),
     * out-of-range values are clamped to {@code [1, 22]}.
     */
    static int resolveZstandardLevel(int level) {
        return clamp(level, ZSTD_MIN_LEVEL, ZSTD_MAX_LEVEL, "zstandard", CodecFactory.DEFAULT_ZSTANDARD_LEVEL);
    }

    private static int clamp(int level, int min, int max, String codec, int defaultValue) {
        if (level == AvroCompressionConfig.DEFAULT_LEVEL) {
            return defaultValue;
        }
        if (level < min || level > max) {
            LOGGER.warn("AVRO {} level {} is outside the valid range [{}, {}], clamping",
                    codec, level, min, max);
            return Math.max(min, Math.min(max, level));
        }
        return level;
    }
}