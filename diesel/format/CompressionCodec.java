package diesel.format;

/**
 * Compression codecs supported by storage formats. Format handlers map these
 * onto their own codec identifiers (Parquet {@code SNAPPY}/{@code GZIP}
 * mappings, ORC {@code SNAPPY}/{@code ZSTD}, and so on); a format that does
 * not support a requested codec falls back to its default.
 */
public enum CompressionCodec {
    /** No compression. */
    NONE,
    /** Snappy — fast, modest ratio, widely supported. */
    SNAPPY,
    /** GZIP — slower, better ratio. */
    GZIP,
    /** ZSTD — very good ratio with fast decompression. */
    ZSTD,
    /** LZ4 — extremely fast. */
    LZ4
}