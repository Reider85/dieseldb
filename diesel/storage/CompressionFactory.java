package diesel.storage;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * Resolution entry point for delimited-file compression (Prompt 39).
 *
 * <p>The codec and its level come <i>exclusively</i> from configuration keys
 * ({@code csv.compression.codec} / {@code tsv.compression.codec} and their
 * {@code .level} counterparts) resolved via {@link StorageConfig}: a system
 * property override wins, then the root {@code config.properties}, then the
 * code-level defaults ({@code zstd}, level {@code 3}). Changing the codec or
 * level affects only newly written files; existing files are read according to
 * their actual format (suffix detection).
 *
 * <p>Compressed files are not byte addressable, so the parallel read path
 * (byte-offset pre-scan) is only used for plain files; compressed inputs
 * always fall back to a single sequential pass.
 */
public final class CompressionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompressionFactory.class);

    /** Code-level default codec, applied only when no config value is present. */
    public static final String DEFAULT_CODEC = "zstd";

    /** ZSTD level bounds (valid range 1..22; spec default 3). */
    public static final int ZSTD_MIN_LEVEL = 1;
    public static final int ZSTD_MAX_LEVEL = 22;
    public static final int DEFAULT_LEVEL = 3;

    private static final CompressionCodec LZ4 = new Lz4Codec();
    private static final CompressionCodec SNAPPY = new SnappyCodec();
    private static final CompressionCodec NONE = new NoneCodec();
    private static final List<CompressionCodec> COMPRESSING =
            List.of(new ZstdCodec(DEFAULT_LEVEL), LZ4, SNAPPY);

    private CompressionFactory() {
        throw new AssertionError("No instances");
    }

    /** The physical delimited file and the codec it was detected with. */
    public record ResolvedDelimitedFile(File file, CompressionCodec codec) {
        public boolean compressed() {
            return !codec.isNone();
        }
    }

    /**
     * Returns the codec for the given config value. Unknown names are rejected.
     */
    public static CompressionCodec forName(String name) {
        String normalized = name == null ? "" : name.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "none" -> NONE;
            case "zstd" -> new ZstdCodec(DEFAULT_LEVEL);
            case "lz4" -> LZ4;
            case "snappy" -> SNAPPY;
            default -> throw new IllegalArgumentException(
                    "Unknown compression codec '" + name + "' (expected: none, zstd, lz4, snappy)");
        };
    }

    /**
     * Resolves the codec for the given configuration key. The ZSTD compression
     * level is read from the sibling {@code .level} key and clamped to the
     * valid range; LZ4 and Snappy ignore the level.
     */
    static CompressionCodec resolveLeveled(String codecKey, String levelKey) {
        CompressionCodec codec = forName(StorageConfig.getString(codecKey, DEFAULT_CODEC));
        return "zstd".equals(codec.name())
                ? new ZstdCodec(resolveLevel(levelKey))
                : codec;
    }

    /**
     * Resolves and validates the configured compression level for ZSTD.
     * Returns the default when unset or unparsable and clamps out-of-range
     * values into {@code [ZSTD_MIN_LEVEL, ZSTD_MAX_LEVEL]}.
     */
    static int resolveLevel(String levelKey) {
        String raw = StorageConfig.getString(levelKey, String.valueOf(DEFAULT_LEVEL));
        int level;
        try {
            level = Integer.parseInt(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn("Invalid {} value '{}', using default level {}", levelKey, raw, DEFAULT_LEVEL);
            return DEFAULT_LEVEL;
        }
        if (level < ZSTD_MIN_LEVEL || level > ZSTD_MAX_LEVEL) {
            LOGGER.warn("{} value {} is outside the valid ZSTD range [{}, {}], clamping",
                    levelKey, level, ZSTD_MIN_LEVEL, ZSTD_MAX_LEVEL);
            return Math.max(ZSTD_MIN_LEVEL, Math.min(ZSTD_MAX_LEVEL, level));
        }
        return level;
    }

    /**
     * Detects the physical delimited file and its codec:
     * <ol>
     *   <li>the configured codec's suffixed successor ({@code .zst} / {@code .lz4}
     *       / {@code .snappy}),</li>
     *   <li>the plain base file (always readable, regardless of the codec),</li>
     *   <li>any other existing compressed successor (a file written under an
     *       earlier codec stays readable after a codec change).</li>
     * </ol>
     * When nothing exists the configured codec's candidate is returned and the
     * caller's existence check decides (missing-file handling stays untouched).
     */
    static ResolvedDelimitedFile resolveActual(File base, String codecKey) {
        for (CompressionCodec candidate : COMPRESSING) {
            if (base.getName().endsWith(candidate.suffix())) {
                return new ResolvedDelimitedFile(base, candidate);
            }
        }
        CompressionCodec configured = forName(StorageConfig.getString(codecKey, DEFAULT_CODEC));
        if (!configured.isNone()) {
            File own = suffixed(base, configured);
            if (own.exists()) {
                return new ResolvedDelimitedFile(own, configured);
            }
        }
        if (base.exists()) {
            return new ResolvedDelimitedFile(base, NONE);
        }
        for (CompressionCodec candidate : COMPRESSING) {
            if (candidate.name().equals(configured.name())) {
                continue;
            }
            File compressed = suffixed(base, candidate);
            if (compressed.exists()) {
                return new ResolvedDelimitedFile(compressed, candidate);
            }
        }
        return new ResolvedDelimitedFile(base, configured.isNone() ? NONE : configured);
    }

    /** Returns the physical file a writer should produce for the given codec. */
    static File delimitedWriteTarget(File base, CompressionCodec codec) {
        return codec.isNone() ? base : suffixed(base, codec);
    }

    /**
     * Opens a buffered reader over a delimited file, transparently
     * decompressing with the given codec when it is not {@code none}. The
     * decoder is configured with {@link CodingErrorAction#REPORT} (matching
     * {@code Files.newBufferedReader}) so malformed input bytes fail loudly
     * instead of being silently replaced.
     */
    static BufferedReader openDelimitedReader(File file, CompressionCodec codec, Charset charset)
            throws IOException {
        InputStream in = Files.newInputStream(file.toPath());
        if (codec != null && !codec.isNone()) {
            in = codec.wrapInputStream(in);
        }
        CharsetDecoder decoder = charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        return new BufferedReader(new InputStreamReader(in, decoder));
    }

    /**
     * Returns a pass-through wrapper whose {@code close()} is a no-op. Used to
     * finish a compressor (writing its frame trailer) while keeping the
     * underlying {@link AtomicFileWriter} channel open until {@code commit()}.
     */
    static OutputStream nonClosing(OutputStream out) {
        return new FilterOutputStream(out) {
            @Override
            public void close() {
                // Keep the AtomicFileWriter channel open; commit() flushes and closes it.
            }
        };
    }

    private static File suffixed(File base, CompressionCodec codec) {
        return new File(base.getPath() + codec.suffix());
    }

    private static final class NoneCodec implements CompressionCodec {
        @Override
        public String name() {
            return "none";
        }

        @Override
        public String suffix() {
            return "";
        }

        @Override
        public OutputStream wrapOutputStream(OutputStream out) {
            return out;
        }

        @Override
        public InputStream wrapInputStream(InputStream in) {
            return in;
        }

        @Override
        public boolean isNone() {
            return true;
        }
    }

    private static final class ZstdCodec implements CompressionCodec {
        private final int level;

        ZstdCodec(int level) {
            this.level = level;
        }

        @Override
        public String name() {
            return "zstd";
        }

        @Override
        public String suffix() {
            return ".zst";
        }

        @Override
        public OutputStream wrapOutputStream(OutputStream out) throws IOException {
            return new ZstdOutputStream(out, level);
        }

        @Override
        public InputStream wrapInputStream(InputStream in) throws IOException {
            return new ZstdInputStream(in);
        }
    }

    private static final class Lz4Codec implements CompressionCodec {
        @Override
        public String name() {
            return "lz4";
        }

        @Override
        public String suffix() {
            return ".lz4";
        }

        @Override
        public OutputStream wrapOutputStream(OutputStream out) throws IOException {
            return new LZ4FrameOutputStream(out);
        }

        @Override
        public InputStream wrapInputStream(InputStream in) throws IOException {
            return new LZ4FrameInputStream(in);
        }
    }

    private static final class SnappyCodec implements CompressionCodec {
        @Override
        public String name() {
            return "snappy";
        }

        @Override
        public String suffix() {
            return ".snappy";
        }

        @Override
        public OutputStream wrapOutputStream(OutputStream out) throws IOException {
            return new SnappyOutputStream(out);
        }

        @Override
        public InputStream wrapInputStream(InputStream in) throws IOException {
            return new SnappyInputStream(in);
        }
    }
}