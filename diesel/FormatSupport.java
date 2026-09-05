package diesel;

import diesel.format.TableData;

import java.nio.file.Path;

/**
 * Public bridge between the package-private engine classes and the public
 * {@code diesel.format} handler layer. Format handlers live in a different
 * package and cannot see engine-internal classes, so this facade exposes the
 * Parquet writer/reader through the neutral {@link TableData} carrier.
 */
public final class FormatSupport {

    private FormatSupport() {
    }

    /**
     * Writes {@link TableData} to a single {@code .parquet} file in
     * {@code dataDir} (SNAPPY-compressed columnar layout).
     *
     * @param data      the data to persist
     * @param dataDir   the destination directory, created if missing
     * @param tableName the table name used as the file base name
     */
    public static void writeParquet(TableData data, String dataDir, String tableName) {
        ParquetWriter.writeTableData(data, dataDir, tableName);
    }

    /**
     * Reads a {@code .parquet} file into a {@link TableData} carrier, restoring
     * the engine metadata (schema, sequences, index definitions) from the footer.
     *
     * @param file the Parquet file
     * @return the carrier
     */
    public static TableData readParquet(Path file) {
        return ParquetReader.readTableData(file);
    }
}