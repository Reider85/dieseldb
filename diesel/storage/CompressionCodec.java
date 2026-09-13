package diesel.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Pluggable compression codec for delimited storage files (Prompt 39).
 *
 * <p>{@code none} is the identity codec (no compression). Concrete codecs for
 * {@code zstd}, {@code lz4} and {@code snappy} wrap the raw byte streams at
 * the file boundary only; the rest of the storage layer never branches on the
 * concrete codec, only on {@link #isNone()} when deciding whether a file is
 * byte addressable (parallel reads).
 */
public interface CompressionCodec {

    /**
     * Lower-case canonical codec name ({@code none}, {@code zstd},
     * {@code lz4} or {@code snappy}), also used as the config value.
     */
    String name();

    /**
     * File suffix appended after the base {@code .csv} / {@code .tsv}
     * extension ({@code .zst}, {@code .lz4}, {@code .snappy}, or empty for
     * {@code none}).
     */
    String suffix();

    /**
     * Wraps an output stream with this codec's compressor. For {@code none}
     * the stream is returned untouched.
     */
    OutputStream wrapOutputStream(OutputStream out) throws IOException;

    /**
     * Wraps an input stream with this codec's decompressor. For {@code none}
     * the stream is returned untouched.
     */
    InputStream wrapInputStream(InputStream in) throws IOException;

    /** Returns {@code true} for the identity (uncompressed) codec. */
    default boolean isNone() {
        return false;
    }
}