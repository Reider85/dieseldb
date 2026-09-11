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

    @Override
    void close() throws IOException;
}