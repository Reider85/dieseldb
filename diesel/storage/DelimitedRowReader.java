package diesel.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Streaming row reader for a delimited storage format, shared by
 * {@link CsvRowReader} and {@link TsvRowReader}. Extending {@link Iterator}
 * allows consumers to skip rows cheaply (used by the parallel line-range
 * readers of {@link DelimitedIndexManager}); extending {@link AutoCloseable}
 * lets callers rely on try-with-resources.
 */
public interface DelimitedRowReader extends Iterator<Map<String, Object>>, AutoCloseable {

    /**
     * Consumes and validates the schema header line of the underlying stream.
     *
     * @return the parsed column names from the file header
     * @throws IOException on I/O errors or header/schema mismatch when in fail mode
     */
    List<String> readHeader() throws IOException;

    /**
     * Reads every remaining row of the underlying stream.
     *
     * @return the decoded rows in file order
     * @throws IOException on I/O errors
     */
    List<Map<String, Object>> readAll() throws IOException;

    /**
     * Returns the 1-based physical line number on which the last consumed row
     * started. The header line is line 1; the first data row starts at line 2.
     * For CSV multi-line quoted fields, the returned line number reflects the
     * start of the logical row. Returns {@code 0} before {@link #readHeader()}
     * has been called.
     *
     * @return the line number of the last consumed row (1-based), or 0
     */
    long getLineNumber();

    @Override
    void close() throws IOException;

    /**
     * Strictly parses a boolean token from a delimited field, accepting the
     * synonyms {@code true/false}, {@code 1/0}, {@code yes/no} and {@code t/f}
     * (case-insensitive, surrounding whitespace ignored). Unlike
     * {@link Boolean#parseBoolean(String)} this never silently treats an
     * arbitrary string as {@code false}.
     *
     * @param raw the raw field value
     * @return the parsed boolean
     * @throws IllegalArgumentException when the value is not a boolean synonym
     */
    static boolean parseBooleanStrict(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("Invalid boolean value: null");
        }
        return switch (raw.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "true", "1", "yes", "t" -> true;
            case "false", "0", "no", "f" -> false;
            default -> throw new IllegalArgumentException("Invalid boolean value: '" + raw + "'");
        };
    }
}