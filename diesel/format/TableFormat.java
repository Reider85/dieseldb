package diesel.format;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Core abstraction for a persistent table storage format. A format
 * implementation owns the file layout of a table: writing {@link TableData}
 * to a file, reading it back (optionally with projection/predicate pushdown),
 * inferring a schema from an existing file and probing whether a file is
 * readable by this format.
 *
 * <p>Formats are decoupled from the engine: they exchange {@link TableData}
 * carriers and never see engine-internal classes, so new formats (Parquet,
 * ORC, JSON, custom) can be plugged in via {@link FormatRegistry} without
 * touching the core engine.</p>
 *
 * @see FormatRegistry
 * @see FormatCapabilities
 */
public interface TableFormat {

    /**
     * Returns the unique format identifier (e.g. {@code "CSV"}, {@code "PARQUET"}).
     *
     * @return the format name, uppercased by convention
     */
    String getName();

    /**
     * Returns a human-readable description of the format.
     *
     * @return the description
     */
    String getDescription();

    /**
     * Returns the file extension including the leading dot (e.g. {@code ".csv"}).
     *
     * @return the extension
     */
    String getFileExtension();

    /**
     * Returns the capabilities of this format.
     *
     * @return the capability matrix
     */
    FormatCapabilities getCapabilities();

    /**
     * Writes the table data to the given file path.
     *
     * @param data     the data to persist
     * @param filePath the destination file
     * @param options  write options (compression, row-group size, format hints)
     * @throws IOException if the file cannot be written
     */
    void write(TableData data, Path filePath, WriteOptions options) throws IOException;

    /**
     * Reads table data from the given file, honoring projection, predicate
     * pushdown and limit from {@code options} when the format supports them.
     * The returned {@link TableData} carries the schema, rows and any
     * dictionary metadata recovered from the file.
     *
     * @param filePath the source file
     * @param options  read options (projection, predicates, limit)
     * @return the parsed data
     * @throws IOException if the file cannot be read
     */
    TableData read(Path filePath, ReadOptions options) throws IOException;

    /**
     * Infers the schema (columns and types) from an existing file, without
     * reading all rows. The returned {@link TableData} has empty rows.
     *
     * @param filePath the source file
     * @return the inferred schema
     * @throws IOException if the file cannot be read
     */
    TableData inferSchema(Path filePath) throws IOException;

    /**
     * Determines whether the file can be read by this format. The default
     * implementation matches the file extension.
     *
     * @param filePath the candidate file
     * @return true when the file appears readable
     */
    default boolean canRead(Path filePath) {
        String fileName = filePath.getFileName() == null ? "" : filePath.getFileName().toString();
        return fileName.toLowerCase().endsWith(getFileExtension());
    }
}