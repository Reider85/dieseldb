package diesel.format;

import diesel.FormatSupport;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Apache Parquet columnar format handler. Writes {@link TableData} into a
 * single {@code .parquet} file through the engine's {@link FormatSupport}
 * bridge; the footer stores the whole schema and dictionary metadata so a
 * table can be rebuilt losslessly. Read and infer-schema operations go through
 * the same bridge.
 */
public final class ParquetFormat implements TableFormat {

    /** Format identifier used in {@code storage.format.*} configuration. */
    public static final String NAME = "PARQUET";

    /** File extension including the leading dot. */
    public static final String EXTENSION = ".parquet";

    private static final FormatCapabilities CAPABILITIES = FormatCapabilities.columnar();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getDescription() {
        return "Apache Parquet columnar file (SNAPPY-compressed, schema and dictionary in footer)";
    }

    @Override
    public String getFileExtension() {
        return EXTENSION;
    }

    @Override
    public FormatCapabilities getCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public void write(TableData data, Path filePath, WriteOptions options) throws IOException {
        String dir = filePath.getParent() == null ? "." : filePath.getParent().toString();
        String fileName = filePath.getFileName() == null ? "" : filePath.getFileName().toString();
        String tableName = fileName.toLowerCase(Locale.ROOT).endsWith(EXTENSION)
                ? fileName.substring(0, fileName.length() - EXTENSION.length()) : fileName;
        FormatSupport.writeParquet(data, dir, tableName);
    }

    @Override
    public TableData read(Path filePath, ReadOptions options) throws IOException {
        TableData data = FormatSupport.readParquet(filePath);
        if (options == null || options.getLimit() < 0 || data.getRows().size() <= options.getLimit()) {
            return data;
        }
        return TableData.builder()
                .columns(data.getColumns())
                .columnTypes(data.getColumnTypes())
                .rows(data.getRows().subList(0, (int) options.getLimit()))
                .metadata(data.getMetadata())
                .build();
    }

    @Override
    public TableData inferSchema(Path filePath) throws IOException {
        TableData data = FormatSupport.readParquet(filePath);
        return TableData.builder()
                .columns(data.getColumns())
                .columnTypes(data.getColumnTypes())
                .metadata(data.getMetadata())
                .build();
    }
}