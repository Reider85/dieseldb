package diesel.format;

import java.util.Collections;
import java.util.Map;

/**
 * Configuration for a format write operation: the desired compression codec,
 * the row-group size for columnar formats, and opaque format-specific
 * options. Formats that do not support a requested codec fall back to their
 * default.
 */
public final class WriteOptions {

    /** Engine defaults. */
    public static final WriteOptions DEFAULT = new WriteOptions(null, null, Map.of());

    private final CompressionCodec compression;
    private final Integer rowGroupSize;
    private final Map<String, String> formatOptions;

    /**
     * Creates write options.
     *
     * @param compression  requested codec, or {@code null} for the format default
     * @param rowGroupSize target row-group size in bytes for columnar formats,
     *                     or {@code null} for the format default
     * @param formatOptions format-specific hints, may be empty
     */
    public WriteOptions(CompressionCodec compression, Integer rowGroupSize,
                        Map<String, String> formatOptions) {
        this.compression = compression;
        this.rowGroupSize = rowGroupSize;
        this.formatOptions = formatOptions == null
                ? Map.of() : Collections.unmodifiableMap(formatOptions);
    }

    /**
     * Creates write options with only a compression codec.
     *
     * @param compression the requested codec (or {@code null})
     * @return new options
     */
    public static WriteOptions withCompression(CompressionCodec compression) {
        return new WriteOptions(compression, null, Map.of());
    }

    public CompressionCodec getCompression() {
        return compression;
    }

    public Integer getRowGroupSize() {
        return rowGroupSize;
    }

    public Map<String, String> getFormatOptions() {
        return formatOptions;
    }
}