package diesel.storage;

import java.io.BufferedReader;
import java.util.List;
import java.util.Map;

/**
 * Factory that opens a {@link DelimitedRowReader} for a storage format.
 * Implementations normally bind to the format's reader constructor, e.g.
 * {@code CsvRowReader::new} or {@code TsvRowReader::new}, keeping
 * {@link DelimitedIndexManager} fully format-agnostic.
 *
 * <p>Adding a new delimited storage type requires only a reader that
 * implements {@link DelimitedRowReader} plus a factory reference to it.
 *
 * @see DelimitedIndexManager
 */
@FunctionalInterface
public interface RowReaderFactory {

    /**
     * @param in          the underlying character-input stream
     * @param columns     the ordered column names of the schema
     * @param columnTypes the column name to type mapping
     * @return a fresh reader positioned before the header line
     */
    DelimitedRowReader create(BufferedReader in, List<String> columns, Map<String, Class<?>> columnTypes);
}