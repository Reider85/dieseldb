package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import diesel.ConfigKeys;

/**
 * File-level statistics for an AVRO data file, used by the cost-based
 * optimizer to estimate scan costs and filter selectivity.
 *
 * <p>Statistics are collected by scanning the Avro file header and the
 * block index without decoding individual records. Column-level
 * statistics (min/max/null-count) require a full read and are computed
 * lazily on first access via {@link #collectColumnStatistics}.
 *
 * <p>Configuration is resolved per call from system property, then
 * {@code config.properties}, then code defaults:
 * <pre>
 * avro.stats.collect.on.query = true   (collect stats on first query)
 * avro.stats.cache.ttl.ms    = 60000  (re-collect after this many ms)
 * </pre>
 *
 * @since Prompt 91
 */
public final class AvroStatistics {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroStatistics.class);

    /** Config key: whether to automatically collect statistics on query. */
    public static final String COLLECT_ON_QUERY_KEY = "avro.stats.collect.on.query";
    /** Config key: time-to-live for cached statistics in milliseconds. */
    public static final String CACHE_TTL_MS_KEY = "avro.stats.cache.ttl.ms";

    public static final boolean DEFAULT_COLLECT_ON_QUERY = true;
    public static final long DEFAULT_CACHE_TTL_MS = 60_000;

    private static final String CONFIG_FILE_KEY = "avro.stats.config.file";

    private long rowCount;
    private long fileSize;
    private int blockCount;
    private long avgBlockSize;
    private String codec;
    private long totalPayloadBytes;
    private long lastCollectedMillis;
    private final Map<String, ColumnStats> columnStatistics = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private boolean columnStatsCollected;

    /**
     * Per-column statistics: min, max, null count and estimated distinct count.
     */
    public static final class ColumnStats {
        private Object min;
        private Object max;
        private long nullCount;
        private long distinctCount;

        ColumnStats() {}

        public Object getMin() { return min; }
        public Object getMax() { return max; }
        public long getNullCount() { return nullCount; }
        public long getDistinctCount() { return distinctCount; }

        @Override
        public String toString() {
            return "ColumnStats{min=" + min + ", max=" + max
                    + ", nullCount=" + nullCount + ", distinctCount=" + distinctCount + '}';
        }
    }

    AvroStatistics() {}

    // ─── Accessors ──────────────────────────────────────────────────

    public long getRowCount() { return rowCount; }
    public long getFileSize() { return fileSize; }
    public int getBlockCount() { return blockCount; }
    public long getAvgBlockSize() { return avgBlockSize; }
    public String getCodec() { return codec; }
    public long getTotalPayloadBytes() { return totalPayloadBytes; }
    public long getLastCollectedMillis() { return lastCollectedMillis; }
    public boolean isColumnStatsCollected() { return columnStatsCollected; }

    public Map<String, ColumnStats> getColumnStatistics() {
        return Map.copyOf(columnStatistics);
    }

    public ColumnStats getColumnStats(String columnName) {
        return columnStatistics.get(columnName);
    }

    // ─── Cost estimation ────────────────────────────────────────────

    /**
     * Estimates the byte cost of a full scan of this file.
     *
     * @return estimated bytes to read
     */
    public double estimateScanCost() {
        return totalPayloadBytes > 0 ? totalPayloadBytes : fileSize * 0.8;
    }

    /**
     * Estimates the selectivity of a predicate on the given column.
     * Returns a value in [0.0, 1.0] where 1.0 means all rows match.
     *
     * @param columnName the column to estimate for
     * @return estimated selectivity
     */
    public double estimateFilterSelectivity(String columnName) {
        ColumnStats stats = columnStatistics.get(columnName);
        if (stats == null || rowCount == 0) {
            return 1.0;
        }
        if (stats.distinctCount <= 1) {
            return 1.0;
        }
        return 1.0 / stats.distinctCount;
    }

    /**
     * Estimates the cost of a filtered scan (full scan * selectivity).
     *
     * @param columnName the filtered column
     * @return estimated bytes to read after filtering
     */
    public double estimateFilteredScanCost(String columnName) {
        return estimateScanCost() * estimateFilterSelectivity(columnName);
    }

    // ─── Collection ─────────────────────────────────────────────────

    /**
     * Collects block-level statistics by scanning the Avro file header
     * and block index (no record decoding). This is a lightweight
     * operation suitable for cost-based optimization decisions.
     *
     * @param avroFile the Avro data file
     * @return the populated statistics object
     * @throws IOException if the file cannot be read
     */
    public static AvroStatistics collectFromFile(File avroFile) throws IOException {
        AvroStatistics stats = new AvroStatistics();
        stats.fileSize = avroFile.length();

        try (AvroDataFileReader reader = new AvroDataFileReader(avroFile)) {
            stats.codec = reader.getCodecName();
            stats.blockCount = (int) reader.countBlocks();
        }

        if (stats.blockCount > 0) {
            stats.avgBlockSize = stats.fileSize / stats.blockCount;
        }
        stats.totalPayloadBytes = stats.fileSize;
        stats.lastCollectedMillis = System.currentTimeMillis();
        LOGGER.debug("Collected Avro statistics for {}: {} rows (est), {} blocks, codec={}",
                avroFile.getName(), stats.rowCount, stats.blockCount, stats.codec);
        return stats;
    }

    /**
     * Collects block-level statistics from the Avro file and also
     * performs a lightweight record scan to count rows.
     *
     * @param avroFile    the Avro data file
     * @param columns     column names
     * @param columnTypes column types
     * @return the populated statistics object
     * @throws IOException if the file cannot be read
     */
    public static AvroStatistics collectFromFile(File avroFile, List<String> columns,
                                                  Map<String, Class<?>> columnTypes) throws IOException {
        AvroStatistics stats = collectFromFile(avroFile);
        try (AvroDataFileReader reader = new AvroDataFileReader(avroFile)) {
            long count = 0;
            while (reader.hasNext()) {
                reader.nextRecord();
                count++;
            }
            stats.rowCount = count;
        }
        stats.lastCollectedMillis = System.currentTimeMillis();
        return stats;
    }

    /**
     * Collects column-level statistics (min/max/null-count/distinct) by
     * reading every record in the file. This is an expensive operation
     * that should only be triggered by ANALYZE TABLE or on-demand.
     *
     * @param avroFile    the Avro data file
     * @param columns     column names
     * @param columnTypes column types
     * @return this (for chaining)
     * @throws IOException if the file cannot be read
     */
    public AvroStatistics collectColumnStatistics(File avroFile, List<String> columns,
                                                   Map<String, Class<?>> columnTypes) throws IOException {
        columnStatistics.clear();
        for (String col : columns) {
            columnStatistics.put(col, new ColumnStats());
        }
        rowCount = 0;

        try (AvroDataFileReader reader = new AvroDataFileReader(avroFile)) {
            while (reader.hasNext()) {
                org.apache.avro.generic.GenericRecord record = reader.nextRecord();
                rowCount++;
                for (String col : columns) {
                    Object value = record.get(col);
                    ColumnStats cs = columnStatistics.get(col);
                    if (value == null || org.apache.avro.Schema.Type.NULL.equals(
                            getBaseType(record.getSchema(), col))) {
                        cs.nullCount++;
                    } else {
                        updateMinMax(cs, value);
                    }
                }
            }
        }
        for (ColumnStats cs : columnStatistics.values()) {
            cs.distinctCount = (cs.nullCount < rowCount) ? Math.max(1, rowCount - cs.nullCount) : 0;
        }
        columnStatsCollected = true;
        lastCollectedMillis = System.currentTimeMillis();
        LOGGER.debug("Collected column statistics for {} columns in {} rows",
                columnStatistics.size(), rowCount);
        return this;
    }

    // ─── Helpers ────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static void updateMinMax(ColumnStats cs, Object value) {
        if (cs.min == null) {
            cs.min = value;
            cs.max = value;
            return;
        }
        if (value instanceof Comparable<?> && cs.min instanceof Comparable<?>) {
            try {
                int cmp = ((Comparable<Object>) cs.min).compareTo(value);
                if (cmp > 0) cs.min = value;
                cmp = ((Comparable<Object>) cs.max).compareTo(value);
                if (cmp < 0) cs.max = value;
            } catch (ClassCastException ignored) {
                // non-comparable types: keep first/last seen
            }
        }
    }

    private static org.apache.avro.Schema.Type getBaseType(org.apache.avro.Schema schema, String field) {
        if (schema == null || schema.getType() != org.apache.avro.Schema.Type.RECORD) {
            return null;
        }
        org.apache.avro.Schema.Field f = schema.getField(field);
        if (f == null) return null;
        org.apache.avro.Schema fs = f.schema();
        if (fs.getType() == org.apache.avro.Schema.Type.UNION) {
            for (org.apache.avro.Schema branch : fs.getTypes()) {
                if (branch.getType() != org.apache.avro.Schema.Type.NULL) {
                    return branch.getType();
                }
            }
        }
        return fs.getType();
    }

    /**
     * Returns {@code true} if the cached statistics are still valid
     * (collected within the configured TTL).
     */
    public boolean isCacheValid() {
        long ttl = getLong(CACHE_TTL_MS_KEY, DEFAULT_CACHE_TTL_MS);
        return (System.currentTimeMillis() - lastCollectedMillis) < ttl;
    }

    @Override
    public String toString() {
        return "AvroStatistics{rowCount=" + rowCount
                + ", fileSize=" + fileSize
                + ", blockCount=" + blockCount
                + ", avgBlockSize=" + avgBlockSize
                + ", codec='" + codec + "'"
                + ", totalPayloadBytes=" + totalPayloadBytes
                + ", columnStatsCollected=" + columnStatsCollected + '}';
    }

    // ─── Config resolution (same pattern as AvroBloomFilterConfig) ──

    private static long getLong(String key, long defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static String getString(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null) {
            return systemValue;
        }
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop;
    }

    private static java.util.Properties rootProps() {
        java.util.Properties props = new java.util.Properties();
        String override = System.getProperty(CONFIG_FILE_KEY);
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, "."), ConfigKeys.CONFIG_FILE);
        if (configFile.exists()) {
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
            }
        }
        return props;
    }
}
