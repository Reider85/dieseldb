package diesel.storage.avro;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;

/**
 * Streaming {@link Iterator} over an {@link AvroDataFileReader}, converting each
 * {@link GenericRecord} into the {@code Object[]} row format used by
 * {@link AvroRowStorage}. Both {@link Iterator} and {@link Iterable} semantics
 * are provided so callers can use either enhanced-for or explicit iteration;
 * the reader is closed via {@link #close()}.
 *
 * <p>When the underlying reader was opened with a column projection, records
 * only carry the requested fields and {@code AvroRowStorage#fromRecord} lands
 * {@code null} in every non-projected position – callers can then fill in the
 * remaining values themselves.
 *
 * @since Prompt 61
 */
public final class AvroReadIterator implements Iterator<Object[]>, Iterable<Object[]>, Closeable {

    private final AvroDataFileReader reader;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;

    public AvroReadIterator(AvroDataFileReader reader, List<String> columns,
                            Map<String, Class<?>> columnTypes) {
        this.reader = reader;
        this.columns = columns;
        this.columnTypes = columnTypes;
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public Object[] next() {
        GenericRecord record;
        try {
            record = reader.nextRecord();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read row from Avro file: " + reader.getFile(), e);
        }
        return AvroRowStorage.fromRecord(record, columns, columnTypes);
    }

    /**
     * Reads up to {@code maxRecords} rows in one batch. The underlying reader
     * advances block-by-block, so large batches still touch only the blocks that
     * contain the requested rows.
     *
     * @param maxRecords maximum number of rows to fetch
     * @return fetched rows, in file order (never {@code null})
     * @throws IOException if the underlying Avro file is corrupt
     */
    public List<Object[]> nextBatch(int maxRecords) throws IOException {
        List<Object[]> batch = new ArrayList<>(Math.min(maxRecords, 4096));
        for (int i = 0; i < maxRecords && reader.hasNext(); i++) {
            batch.add(AvroRowStorage.fromRecord(reader.nextRecord(), columns, columnTypes));
        }
        return batch;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return this;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}