package diesel.storage;

import java.util.List;
import java.util.Map;

/**
 * Index, cache and parallel-read manager for TSV-backed tables. Internally it
 * fully reuses the format-agnostic {@link DelimitedIndexManager}: the primary-key
 * index, per-column secondary indexes and parallel file reading differ from the
 * CSV flavour only in the row reader ({@link TsvRowReader}) and the {@code tsv.*}
 * config keys.
 *
 * <p>TSV rows never span multiple physical lines, so the byte-offset pre-scan
 * of prompt 34 always partitions by exact line boundaries.
 *
 * <p>Config keys (from {@code config.properties} or {@code -Dtsv.*}):
 * <ul>
 *   <li>{@code tsv.block.size} &mdash; rows per block (default 1000)</li>
 *   <li>{@code tsv.parallel.read.threshold} &mdash; min rows to enable parallel reads (default 10000)</li>
 * </ul>
 */
public class TsvIndexManager extends DelimitedIndexManager {

    /**
     * @param tableName   the table name
     * @param columns     the ordered column names of the underlying schema
     * @param columnTypes column name to type mapping of the underlying schema
     */
    public TsvIndexManager(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        super(tableName, columns, columnTypes, TsvRowReader::new, "tsv", false);
    }
}