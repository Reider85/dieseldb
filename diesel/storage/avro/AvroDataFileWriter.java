package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class AvroDataFileWriter {
    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final DatumWriter<GenericRecord> datumWriter;
    private final Schema schema;
    private final File outputFile;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ByteBuffer syncBuffer;
    private final int blockSize;
    private int bytesWritten = 0;

    public AvroDataFileWriter(List<String> columns, Map<String, Class<?>> columnTypes, File outputFile) throws IOException {
        this(columns, columnTypes, outputFile, CodecFactory.nullCodec());
    }

    /**
     * Creates a writer producing an Avro object-container file compressed with
     * the given codec factory ({@link CodecFactory#nullCodec()} disables
     * compression). The codec is recorded in the file header, so the reader
     * decodes the file transparently regardless of this setting.
     *
     * @param codec the Avro codec factory (may be {@code null}; falls back to
     *              no compression)
     */
    public AvroDataFileWriter(List<String> columns, Map<String, Class<?>> columnTypes, File outputFile,
                              CodecFactory codec) throws IOException {
        this.outputFile = outputFile;
        this.schema = AvroSchemaManager.buildTableSchema("temp", columns, columnTypes);
        this.datumWriter = new GenericDatumWriter<>(schema);
        this.dataFileWriter = new DataFileWriter<>(datumWriter);
        this.blockSize = 67108864; // 64MB default

        // Initialize sync buffer
        this.syncBuffer = ByteBuffer.allocate(16);
        syncBuffer.putLong(System.currentTimeMillis());
        syncBuffer.putLong(0); // Sync marker version
        syncBuffer.flip();

        if (codec != null) {
            dataFileWriter.setCodec(codec);
        }
        dataFileWriter.create(schema, outputFile);
    }

    public synchronized void writeRow(Map<String, Object> rowMap) throws IOException {
        if (closed.get()) {
            throw new IOException("DataFileWriter is already closed");
        }

        // Convert map to array for compatibility with existing toRecord method
        Object[] rowArray = new Object[schema.getFields().size()];
        for (int i = 0; i < schema.getFields().size(); i++) {
            String fieldName = schema.getFields().get(i).name();
            rowArray[i] = rowMap.get(fieldName);
        }
        
        GenericRecord record = toRecord(rowArray, schema);
        dataFileWriter.append(record);
        bytesWritten++;
        
        // Write sync marker if block size reached
        if (bytesWritten >= blockSize / 1024) { // Approximate row size
            dataFileWriter.sync();
            bytesWritten = 0;
        }
    }

    private static GenericRecord toRecord(Object[] row, Schema schema) {
        GenericRecord record = new org.apache.avro.generic.GenericData.Record(schema);
        for (int i = 0; i < schema.getFields().size() && i < row.length; i++) {
            Schema.Field field = schema.getFields().get(i);
            record.put(field.name(), row[i]);
        }
        return record;
    }

    public synchronized void flush() throws IOException {
        if (closed.get()) {
            return;
        }
        dataFileWriter.flush();
    }

    public synchronized void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            dataFileWriter.close();
        }
    }

    public synchronized void rollback() throws IOException {
        if (outputFile.exists()) {
            if (!outputFile.delete()) {
                throw new IOException("Failed to rollback file: " + outputFile.getAbsolutePath());
            }
        }
    }

    public File getOutputFile() {
        return outputFile;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getBytesWritten() {
        return bytesWritten;
    }
}