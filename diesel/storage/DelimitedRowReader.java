package diesel.storage;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

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
     * Reads every remaining row of the underlying stream into compact Object[]
     * arrays (the representation the readers produce natively), closing the
     * reader. Returns {@code null} rows are skipped (the
     * {@code storage.load.error.mode = skip_row} policy).
     *
     * @return the decoded rows in file order
     * @throws IOException on I/O errors
     */
    default List<Object[]> readAllArrays() throws IOException {
        List<Object[]> rows = new java.util.ArrayList<>();
        while (hasNext()) {
            Object[] row = nextArray();
            if (row != null) {
                rows.add(row);
            }
        }
        close();
        return rows;
    }

    /**
     * Reads the next row into a compact Object[] whose slot {@code i} holds the
     * value of schema column {@code i}. Returns {@code null} when the whole row
     * was skipped by the {@code storage.load.error.mode} policy.
     */
    Object[] nextArray();

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
     * Returns the header-to-schema column mapping built by
     * {@link #readHeader()}: {@code columnMapping[i]} is the index of the file
     * column feeding schema column {@code i}, or {@code -1} when the schema
     * column is absent from the file. Returns {@code null} before the header
     * has been consumed.
     *
     * @return the mapping array, or {@code null}
     */
    int[] columnMapping();

    /**
     * Prepares this reader to decode data rows directly from a partition that
     * starts at a data-line boundary (byte-offset parallel read, prompt 34).
     * The column mapping must already be known (parsed once from the header);
     * no header line is consumed and line numbering starts at
     * {@code firstDataLine} so error diagnostics keep absolute file positions.
     *
     * @param columnMapping the header-to-schema mapping from {@link #columnMapping()}
     * @param firstDataLine the 1-based physical line of the partition's first row
     */
    void initPartition(int[] columnMapping, long firstDataLine);

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

    /**
     * Returns the value parser for a column type given by its simple name
     * ({@code Long}, {@code Integer}, {@code Double}, {@code Float},
     * {@code BigDecimal}, {@code Boolean}, {@code LocalDate},
     * {@code LocalDateTime}, {@code UUID}), or {@code null} for String/unknown
     * types whose values stay as text. Returning concrete function references
     * lets the readers precompile a per-column converter at construction time,
     * so no per-cell type switch runs on the hot path.
     *
     * @param typeName the column type's simple name, or {@code null}
     * @return the parse function, or {@code null} for text columns
     */
    static Function<String, Object> baseParser(String typeName) {
        return switch (typeName) {
            case "Long" -> Long::parseLong;
            case "Integer" -> Integer::parseInt;
            case "Double" -> Double::parseDouble;
            case "Float" -> Float::parseFloat;
            case "BigDecimal" -> BigDecimal::new;
            case "Boolean" -> DelimitedRowReader::parseBooleanStrict;
            case "LocalDate" -> LocalDate::parse;
            case "LocalDateTime" -> LocalDateTime::parse;
            case "UUID" -> UUID::fromString;
            default -> null;
        };
    }
}