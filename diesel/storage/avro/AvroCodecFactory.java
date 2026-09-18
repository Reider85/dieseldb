package diesel.storage.avro;

import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Maps the configured AVRO codec name and level to a concrete Avro
 * {@link CodecFactory} (Prompt 62, enhanced in Prompt 65 with deflate level optimization).
 *
 * <p>The mapping uses Avro 1.12's public static factory methods, so no Avro
 * internals are reached reflectively:
 * <ul>
 *   <li>{@code null} → {@link CodecFactory#nullCodec()}</li>
 *   <li>{@code deflate} → {@link DeflateLevelConfig#newCodec(int)} (level clamped 1..9 with adaptive selection)</li>
 *   <li>{@code snappy} → {@link SnappyOptimizedCodec#newCodec()} (optimized with buffer sizing and caching)</li>
 *   <li>{@code zstandard} → {@link ZStandardCodec#newCodec(int)} (level clamped 1..22)</li>
 *   <li>{@code bzip2} → {@link CodecFactory#bzip2Codec()} (no level)</li>
 * </ul>
 *
 * <p>A level of {@code -1} (or {@code AvroCompressionConfig#DEFAULT_LEVEL})
 * resolves to the codec's built-in default:
 * <ul>
 *   <li>deflate: {@link DeflateLevelConfig#DEFAULT_LEVEL} (3, balanced speed/ratio)</li>
 *   <li>zstandard: {@link ZStandardCodec#DEFAULT_LEVEL} (3)</li>
 *   <li>snappy/bzip2/null: no level parameter</li>
 * </ul>
 *
 * <p>Prompt 65 enhances deflate compression with intelligent level selection (1-9 range),
 * compressor caching, and adaptive strategies based on data characteristics.
 *
 * @since Prompt 62 (enhanced in Prompt 65)
 */
public final class AvroCodecFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroCodecFactory.class);

    // Deflate levels use 1-9 range (Prompt 65) instead of 0-9 for clearer semantics

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
            case "snappy" -> SnappyOptimizedCodec.newCodec();
            case "bzip2" -> CodecFactory.bzip2Codec();
            case "deflate" -> DeflateLevelConfig.newCodec(level);
            case "zstandard" -> ZStandardCodec.newCodec(level);
            default -> throw new IllegalArgumentException(
                    "Unknown AVRO compression codec '" + codec + "' (expected: null, deflate, snappy, zstandard, bzip2)");
        };
    }

    /**
     * Resolves the deflate level using DeflateLevelConfig (Prompt 65).
     * Level range is 1-9 with -1 mapping to the default level (3).
     */
    static int resolveDeflateLevel(int level) {
        return DeflateLevelConfig.resolveLevel(level);
    }
}