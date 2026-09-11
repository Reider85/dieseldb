package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Index, cache and parallel-read manager for CSV-backed tables. Fully reuses
 * the format-agnostic {@link DelimitedIndexManager} and differs from the TSV
 * flavour only in the row reader ({@link CsvRowReader}) and the {@code csv.*}
 * config keys.
 *
 * <p>Unlike TSV, CSV fields enclosed in double quotes may span multiple
 * physical lines. A cheap pre-scan detects such files via
 * {@link CsvRowReader#endsInsideQuotes}, so the parallel line-range path is
 * only taken when every physical line maps to exactly one row.
 *
 * <p>Config keys (from {@code config.properties} or {@code -Dcsv.*}):
 * <ul>
 *   <li>{@code csv.block.size} &mdash; rows per cache block (default 1000)</li>
 *   <li>{@code csv.cache.max.blocks} &mdash; LRU cache capacity in blocks (default 64)</li>
 *   <li>{@code csv.parallel.read.threshold} &mdash; min rows to enable parallel reads (default 10000)</li>
 * </ul>
 */
public class CsvIndexManager extends DelimitedIndexManager {

    /**
     * @param tableName   the table name
     * @param columns     the ordered column names of the underlying schema
     * @param columnTypes column name to type mapping of the underlying schema
     */
    public CsvIndexManager(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        super(tableName, columns, columnTypes, CsvRowReader::new, "csv", true);
    }

    @Override
    protected boolean mayContainMultiLineRows(File file) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            if (bufferedReader.readLine() == null) {
                return false;
            }
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (CsvRowReader.endsInsideQuotes(line)) {
                    return true;
                }
            }
            return false;
        }
    }
}