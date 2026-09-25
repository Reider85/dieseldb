package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Predicate;

/**
 * AVRO-optimised query executor (Prompt 91).
 *
 * <p>This class sits between the generic {@link diesel.SelectQuery} and the
 * {@link AvroRowStorage} layer. When the table is backed by Avro, the
 * executor intercepts the scan, applies column projection and predicate
 * pushdown at the Avro binary level, and returns only the rows the query
 * actually needs — eliminating full-table materialisation as a Map.
 *
 * <p>Three optimisations are offered:
 * <ol>
 *   <li><b>Column projection</b> — only columns referenced by the SELECT,
 *       WHERE, JOIN and ORDER BY clauses are read from the Avro file.
 *       The {@link AvroDataFileReader} skips non-projected fields at the
 *       token level via a narrowed reader schema.</li>
 *   <li><b>Predicate pushdown</b> — a caller-supplied
 *       {@code Predicate<GenericRecord>} is evaluated against the raw
 *       {@link GenericRecord} before conversion to {@code Map}, so
 *       non-matching records never cross the Avro→Java boundary.</li>
 *   <li><b>Statistics</b> — {@link AvroStatistics} are collected lazily and
 *       cached for the cost-based optimizer.</li>
 * </ol>
 *
 * <p>Configuration is resolved per call from system property, then
 * {@code config.properties}, then code defaults via {@link AvroQueryConfig}.
 *
 * @since Prompt 91
 */
public final class AvroQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroQueryExecutor.class);

    /** Cached configuration, resolved once per JVM. */
    private static volatile AvroQueryConfig cachedConfig;

    /**
     * Immutable result of an optimised Avro query.
     *
     * @param rows            filtered and projected rows
     * @param totalRowsScanned rows touched before filtering
     * @param statistics      file-level statistics (may be {@code null})
     */
    public record QueryResult(List<Map<String, Object>> rows,
                               long totalRowsScanned,
                               AvroStatistics statistics) {}

    // ─── Public API ─────────────────────────────────────────────────

    /**
     * Executes a query against the given Avro storage with predicate and
     * column pushdown.
     *
     * @param storage          the Avro row storage
     * @param predicate        filter predicate applied at the GenericRecord
     *                         level (may be {@code null} for no filtering)
     * @param requiredColumns  columns needed by the query (projection set)
     * @param allColumns       full column list of the table
     * @param columnTypes      column→type map
     * @param limit            SQL LIMIT value, or {@code null}
     * @return the query result with filtered rows and statistics
     */
    public QueryResult executeQuery(AvroRowStorage storage,
                                     Predicate<GenericRecord> predicate,
                                     Set<String> requiredColumns,
                                     List<String> allColumns,
                                     Map<String, Class<?>> columnTypes,
                                     Integer limit) {
        AvroQueryConfig config = resolveConfig();
        long startNanos = System.nanoTime();

        try {
            File avroFile = storage.resolveAvroFile();
            if (avroFile == null || !avroFile.isFile()) {
                LOGGER.debug("Avro file not found for table {}, falling back to in-memory scan",
                        storage.getTableName());
                return fallbackScan(storage, allColumns);
            }

            List<Map<String, Object>> rows;
            long totalScanned;

            if (config.pushdownEnabled() && predicate != null) {
                rows = readWithPushdown(avroFile, requiredColumns, allColumns,
                        columnTypes, predicate, limit);
            } else if (config.projectionEnabled() && requiredColumns.size() < allColumns.size()) {
                rows = readWithProjection(avroFile, requiredColumns, allColumns, columnTypes, limit);
            } else {
                rows = readFull(avroFile, allColumns, columnTypes, limit);
            }
            totalScanned = estimateTotalRows(avroFile, allColumns, columnTypes);

            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            LOGGER.debug("AvroQueryExecutor: {} rows returned, {} scanned in {} ms (file={})",
                    rows.size(), totalScanned, elapsedMs, avroFile.getName());

            AvroStatistics stats = collectOrReuseStatistics(avroFile, allColumns, columnTypes);
            return new QueryResult(rows, totalScanned, stats);

        } catch (IOException e) {
            LOGGER.warn("Avro pushdown failed, falling back to in-memory scan: {}", e.getMessage());
            return fallbackScan(storage, allColumns);
        }
    }

    /**
     * Convenience overload for queries without LIMIT.
     */
    public QueryResult executeQuery(AvroRowStorage storage,
                                     Predicate<GenericRecord> predicate,
                                     Set<String> requiredColumns,
                                     List<String> allColumns,
                                     Map<String, Class<?>> columnTypes) {
        return executeQuery(storage, predicate, requiredColumns, allColumns, columnTypes, null);
    }

    /**
     * Executes a full scan with projection only (no predicate pushdown).
     */
    public List<Map<String, Object>> scanWithProjection(AvroRowStorage storage,
                                                         Collection<String> projectedColumns) throws IOException {
        File avroFile = storage.resolveAvroFile();
        if (avroFile == null || !avroFile.isFile()) {
            return storage.scan();
        }
        List<String> allColumns = storage.getColumns();
        Map<String, Class<?>> columnTypes = storage.getColumnTypes();
        return readWithProjection(avroFile, new LinkedHashSet<>(projectedColumns),
                allColumns, columnTypes, null);
    }

    // ─── I/O: read with pushdown ────────────────────────────────────

    private List<Map<String, Object>> readWithPushdown(File avroFile,
                                                        Set<String> requiredColumns,
                                                        List<String> allColumns,
                                                        Map<String, Class<?>> columnTypes,
                                                        Predicate<GenericRecord> predicate,
                                                        Integer limit) throws IOException {
        List<String> projectionList = new ArrayList<>(requiredColumns);
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Class<?>> resolvedTypes = buildColumnTypeLookup(columnTypes);

        try (AvroDataFileReader reader = new AvroDataFileReader(avroFile, projectionList)) {
            while (reader.hasNext()) {
                GenericRecord record = reader.nextRecord();
                if (predicate.test(record)) {
                    result.add(convertRecordToMap(record, allColumns, resolvedTypes));
                    if (limit != null && result.size() >= limit) {
                        break;
                    }
                }
            }
        }
        return result;
    }

    // ─── I/O: read with projection only ─────────────────────────────

    private List<Map<String, Object>> readWithProjection(File avroFile,
                                                          Set<String> requiredColumns,
                                                          List<String> allColumns,
                                                          Map<String, Class<?>> columnTypes,
                                                          Integer limit) throws IOException {
        List<String> projectionList = new ArrayList<>(requiredColumns);
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Class<?>> resolvedTypes = buildColumnTypeLookup(columnTypes);

        try (AvroDataFileReader reader = new AvroDataFileReader(avroFile, projectionList)) {
            while (reader.hasNext()) {
                GenericRecord record = reader.nextRecord();
                result.add(convertRecordToMap(record, allColumns, resolvedTypes));
                if (limit != null && result.size() >= limit) {
                    break;
                }
            }
        }
        return result;
    }

    // ─── I/O: full read (fallback) ──────────────────────────────────

    private List<Map<String, Object>> readFull(File avroFile,
                                                List<String> allColumns,
                                                 Map<String, Class<?>> columnTypes,
                                                 Integer limit) throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Class<?>> resolvedTypes = buildColumnTypeLookup(columnTypes);
        try (AvroDataFileReader reader = new AvroDataFileReader(avroFile)) {
            while (reader.hasNext()) {
                GenericRecord record = reader.nextRecord();
                result.add(convertRecordToMap(record, allColumns, resolvedTypes));
                if (limit != null && result.size() >= limit) {
                    break;
                }
            }
        }
        return result;
    }

    // ─── Fallback ───────────────────────────────────────────────────

    private QueryResult fallbackScan(AvroRowStorage storage, List<String> allColumns) {
        List<Map<String, Object>> allRows = storage.scan();
        return new QueryResult(allRows, allRows.size(), null);
    }

    // ─── Record value extraction ────────────────────────────────────

    private static Map<String, Object> convertRecordToMap(GenericRecord record,
                                                           List<String> allColumns,
                                                           Map<String, Class<?>> columnTypes) {
        // Only include columns present in the record's schema (projected subset).
        // Non-projected fields are skipped entirely to keep the map lean.
        Schema recordSchema = record.getSchema();
        Map<String, Object> map = new LinkedHashMap<>(Math.max(recordSchema.getFields().size() * 2, 4));
        for (String col : allColumns) {
            if (recordSchema.getField(col) != null) {
                Object val = record.get(col);
                if (val instanceof org.apache.avro.util.Utf8 utf8) {
                    map.put(col, utf8.toString());
                } else if (val instanceof ByteBuffer bb) {
                    Class<?> targetType = columnTypes.get(col);
                    if (targetType == BigDecimal.class) {
                        int scale = 18; // default
                        Schema.Field field = recordSchema.getField(col);
                        if (field != null) {
                            org.apache.avro.LogicalType lt = field.schema().getLogicalType();
                            if (lt != null) {
                                org.apache.avro.Schema base = field.schema();
                                if (base.getType() == Schema.Type.UNION) {
                                    for (Schema branch : base.getTypes()) {
                                        if (branch.getType() == Schema.Type.BYTES
                                                && branch.getLogicalType() != null) {
                                            base = branch;
                                            break;
                                        }
                                    }
                                }
                                if (base.getLogicalType() instanceof org.apache.avro.LogicalTypes.Decimal d) {
                                    scale = d.getScale();
                                }
                            }
                        }
                        map.put(col, new BigDecimal(new BigInteger(bb.array()), scale).stripTrailingZeros());
                    } else {
                        byte[] arr = new byte[bb.remaining()];
                        bb.get(arr);
                        map.put(col, arr);
                    }
                } else {
                    map.put(col, val);
                }
            }
        }
        return map;
    }

    private static Map<String, Class<?>> buildColumnTypeLookup(Map<String, Class<?>> columnTypes) {
        Map<String, Class<?>> resolved = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (columnTypes == null) {
            return resolved;
        }
        for (Map.Entry<String, Class<?>> entry : columnTypes.entrySet()) {
            if (entry.getKey() != null && entry.getKey().indexOf('.') < 0) {
                resolved.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, Class<?>> entry : columnTypes.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            int separator = entry.getKey().lastIndexOf('.');
            if (separator >= 0 && separator + 1 < entry.getKey().length()) {
                resolved.putIfAbsent(entry.getKey().substring(separator + 1), entry.getValue());
            }
        }
        return resolved;
    }

    // ─── Statistics ─────────────────────────────────────────────────

    private static final Map<String, AvroStatistics> statsCache = new HashMap<>();

    private AvroStatistics collectOrReuseStatistics(File avroFile,
                                                    List<String> allColumns,
                                                    Map<String, Class<?>> columnTypes) {
        String key = avroFile.getAbsolutePath();
        AvroStatistics cached = statsCache.get(key);
        if (cached != null && cached.isCacheValid()) {
            return cached;
        }
        try {
            AvroStatistics stats = AvroStatistics.collectFromFile(avroFile, allColumns, columnTypes);
            statsCache.put(key, stats);
            return stats;
        } catch (IOException e) {
            LOGGER.debug("Could not collect Avro statistics: {}", e.getMessage());
            return null;
        }
    }

    private long estimateTotalRows(File avroFile, List<String> allColumns,
                                    Map<String, Class<?>> columnTypes) throws IOException {
        try (AvroDataFileReader reader = new AvroDataFileReader(avroFile)) {
            long count = 0;
            while (reader.hasNext()) {
                reader.nextRecord();
                count++;
            }
            return count;
        }
    }

    // ─── Config resolution ──────────────────────────────────────────

    private static AvroQueryConfig resolveConfig() {
        AvroQueryConfig config = cachedConfig;
        if (config == null) {
            synchronized (AvroQueryExecutor.class) {
                config = cachedConfig;
                if (config == null) {
                    config = AvroQueryConfig.resolve();
                    cachedConfig = config;
                }
            }
        }
        return config;
    }

    /**
     * Clears the cached configuration and statistics. Intended for tests.
     */
    public static void resetCache() {
        cachedConfig = null;
        statsCache.clear();
    }
}
