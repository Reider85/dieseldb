package diesel.format;

/**
 * Immutable capability matrix describing what a {@link TableFormat} can do.
 * The engine inspects these flags to decide how a format may be used: whether
 * analytic scans can push projection/predicates down into the reader, whether
 * hybrid workloads can append to an existing file, whether a columnar layout
 * is present, and so on.
 *
 * <p>Use the static factories {@link #rowBased()} and {@link #columnar()} to
 * build a sensible starting point and refine with the {@code with} methods
 * (each returns a new instance).</p>
 */
public final class FormatCapabilities {

    /** No capabilities. */
    public static final FormatCapabilities NONE = new FormatCapabilities();

    private final boolean supportsProjection;
    private final boolean supportsPredicatePushdown;
    private final boolean supportsRowGroupStats;
    private final boolean supportsPartitionPruning;
    private final boolean supportsCompression;
    private final boolean supportsAppend;
    private final boolean supportsSchemaEvolution;
    private final boolean isColumnar;
    private final boolean isSplittable;
    private final CompressionCodec[] supportedCodecs;

    private FormatCapabilities() {
        this(false, false, false, false, false, false, false, false, false, new CompressionCodec[]{CompressionCodec.NONE});
    }

    private FormatCapabilities(boolean supportsProjection,
                               boolean supportsPredicatePushdown,
                               boolean supportsRowGroupStats,
                               boolean supportsPartitionPruning,
                               boolean supportsCompression,
                               boolean supportsAppend,
                               boolean supportsSchemaEvolution,
                               boolean isColumnar,
                               boolean isSplittable,
                               CompressionCodec[] supportedCodecs) {
        this.supportsProjection = supportsProjection;
        this.supportsPredicatePushdown = supportsPredicatePushdown;
        this.supportsRowGroupStats = supportsRowGroupStats;
        this.supportsPartitionPruning = supportsPartitionPruning;
        this.supportsCompression = supportsCompression;
        this.supportsAppend = supportsAppend;
        this.supportsSchemaEvolution = supportsSchemaEvolution;
        this.isColumnar = isColumnar;
        this.isSplittable = isSplittable;
        this.supportedCodecs = supportedCodecs.clone();
    }

    /**
     * Default capabilities for a row-oriented storage format (legacy CSV,
     * JSON lines, …). These are inherently not splittable without extra
     * machinery and do not push predicates down.
     *
     * @return row-based capability set
     */
    public static FormatCapabilities rowBased() {
        return new FormatCapabilities(false, false, false, false,
                false, true, false, false, false,
                new CompressionCodec[]{CompressionCodec.NONE});
    }

    /**
     * Default capabilities for a columnar storage format (Parquet, ORC, …).
     * Columnar formats support projection and predicate pushdown, row-group
     * statistics, compression, splitting and schema evolution.
     *
     * @return columnar capability set
     */
    public static FormatCapabilities columnar() {
        return new FormatCapabilities(true, true, true, true,
                true, false, true, true, true,
                new CompressionCodec[]{CompressionCodec.SNAPPY, CompressionCodec.GZIP});
    }

    /**
     * Returns a copy with the projection-pushdown flag changed.
     *
     * @param value the new flag value
     * @return a new capability set
     */
    public FormatCapabilities withProjection(boolean value) {
        return new FormatCapabilities(value, supportsPredicatePushdown, supportsRowGroupStats,
                supportsPartitionPruning, supportsCompression, supportsAppend, supportsSchemaEvolution,
                isColumnar, isSplittable, supportedCodecs);
    }

    /**
     * Returns a copy with the predicate-pushdown flag changed.
     *
     * @param value the new flag value
     * @return a new capability set
     */
    public FormatCapabilities withPredicatePushdown(boolean value) {
        return new FormatCapabilities(supportsProjection, value, supportsRowGroupStats,
                supportsPartitionPruning, supportsCompression, supportsAppend, supportsSchemaEvolution,
                isColumnar, isSplittable, supportedCodecs);
    }

    /**
     * Returns a copy with the row-group-statistics flag changed.
     *
     * @param value the new flag value
     * @return a new capability set
     */
    public FormatCapabilities withRowGroupStats(boolean value) {
        return new FormatCapabilities(supportsProjection, supportsPredicatePushdown, value,
                supportsPartitionPruning, supportsCompression, supportsAppend, supportsSchemaEvolution,
                isColumnar, isSplittable, supportedCodecs);
    }

    /**
     * Returns a copy with the partition-pruning flag changed.
     *
     * @param value the new flag value
     * @return a new capability set
     */
    public FormatCapabilities withPartitionPruning(boolean value) {
        return new FormatCapabilities(supportsProjection, supportsPredicatePushdown, supportsRowGroupStats,
                value, supportsCompression, supportsAppend, supportsSchemaEvolution,
                isColumnar, isSplittable, supportedCodecs);
    }

    /**
     * Returns a copy with the compression-support flag and codec list changed.
     *
     * @param value  whether the format supports compression
     * @param codecs the supported codecs
     * @return a new capability set
     */
    public FormatCapabilities withCompression(boolean value, CompressionCodec... codecs) {
        CompressionCodec[] effective = codecs == null || codecs.length == 0
                ? new CompressionCodec[]{CompressionCodec.NONE} : codecs;
        return new FormatCapabilities(supportsProjection, supportsPredicatePushdown, supportsRowGroupStats,
                supportsPartitionPruning, value, supportsAppend, supportsSchemaEvolution,
                isColumnar, isSplittable, effective);
    }

    /**
     * Returns a copy with the append flag changed.
     *
     * @param value the new flag value
     * @return a new capability set
     */
    public FormatCapabilities withAppend(boolean value) {
        return new FormatCapabilities(supportsProjection, supportsPredicatePushdown, supportsRowGroupStats,
                supportsPartitionPruning, supportsCompression, value, supportsSchemaEvolution,
                isColumnar, isSplittable, supportedCodecs);
    }

    /**
     * Returns a copy with the schema-evolution flag changed.
     *
     * @param value the new flag value
     * @return a new capability set
     */
    public FormatCapabilities withSchemaEvolution(boolean value) {
        return new FormatCapabilities(supportsProjection, supportsPredicatePushdown, supportsRowGroupStats,
                supportsPartitionPruning, supportsCompression, supportsAppend, value,
                isColumnar, isSplittable, supportedCodecs);
    }

    /**
     * Returns a copy with the columnar flag changed.
     *
     * @param value the new flag value
     * @return a new capability set
     */
    public FormatCapabilities withColumnar(boolean value) {
        return new FormatCapabilities(supportsProjection, supportsPredicatePushdown, supportsRowGroupStats,
                supportsPartitionPruning, supportsCompression, supportsAppend, supportsSchemaEvolution,
                value, isSplittable, supportedCodecs);
    }

    /**
     * Returns a copy with the splittable flag changed (splitting allows
     * parallel processing of independent segments).
     *
     * @param value the new flag value
     * @return a new capability set
     */
    public FormatCapabilities withSplittable(boolean value) {
        return new FormatCapabilities(supportsProjection, supportsPredicatePushdown, supportsRowGroupStats,
                supportsPartitionPruning, supportsCompression, supportsAppend, supportsSchemaEvolution,
                isColumnar, value, supportedCodecs);
    }

    public boolean supportsProjection() {
        return supportsProjection;
    }

    public boolean supportsPredicatePushdown() {
        return supportsPredicatePushdown;
    }

    public boolean supportsRowGroupStats() {
        return supportsRowGroupStats;
    }

    public boolean supportsPartitionPruning() {
        return supportsPartitionPruning;
    }

    public boolean supportsCompression() {
        return supportsCompression;
    }

    public boolean supportsAppend() {
        return supportsAppend;
    }

    public boolean supportsSchemaEvolution() {
        return supportsSchemaEvolution;
    }

    public boolean isColumnar() {
        return isColumnar;
    }

    public boolean isSplittable() {
        return isSplittable;
    }

    public CompressionCodec[] supportedCodecs() {
        return supportedCodecs.clone();
    }
}