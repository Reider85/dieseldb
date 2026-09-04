package diesel;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Prompt 86: writes a DieselDB {@link Table} into the Apache Parquet columnar
 * format. This is the first integration point with the Parquet ecosystem; a
 * matching reader (Prompt 87) and higher-level features (compression, statistics,
 * partitioning) build on top of this writer.
 *
 * <p>Supported column value types are mapped to Parquet primitive types:
 * <ul>
 *   <li>{@code Integer}/{@code Short}/{@code Byte} → INT32</li>
 *   <li>{@code Long} → INT64</li>
 *   <li>{@code Float} → FLOAT</li>
 *   <li>{@code Double} → DOUBLE</li>
 *   <li>{@code Boolean} → BOOLEAN</li>
 *   <li>{@code String}/{@code UUID} → BINARY (UTF8)</li>
 *   <li>{@code BigDecimal} → FIXED_LEN_BYTE_ARRAY (DECIMAL, precision/scale)</li>
 *   <li>{@code LocalDate} → INT32 (DATE)</li>
 *   <li>{@code LocalDateTime} → INT64 (TIMESTAMP_MILLIS)</li>
 * </ul>
 *
 * <p>The writer writes through a plain {@link FileChannel} via a custom
 * {@link OutputFile} implementation, so it does not depend on the Hadoop file
 * system abstraction (which on Windows requires winutils/ HADOOP_HOME).
 */
class ParquetWriter {

    private static final Logger LOGGER = Logger.getLogger(ParquetWriter.class.getName());
    private static final String PARQUET_EXTENSION = ".parquet";
    private static final int DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024;
    private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;

    private ParquetWriter() {
    }

    /**
     * Builds the Parquet {@link MessageType} schema for the given columns.
     *
     * @param columns     the ordered column names
     * @param columnTypes the column name to Java type mapping
     * @return the generated Parquet schema
     */
    static MessageType buildSchema(List<String> columns, Map<String, Class<?>> columnTypes) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (String column : columns) {
            builder.addField(primitiveField(column, columnTypes.get(column)));
        }
        return builder.named("diesel_table");
    }

    private static Type primitiveField(String name, Class<?> javaType) {
        if (javaType == Integer.class || javaType == Short.class || javaType == Byte.class) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                    .named(name);
        }
        if (javaType == Long.class) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                    .named(name);
        }
        if (javaType == Float.class) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
                    .named(name);
        }
        if (javaType == Double.class) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
                    .named(name);
        }
        if (javaType == Boolean.class) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
                    .named(name);
        }
        if (javaType == LocalDate.class) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                    .as(OriginalType.DATE)
                    .named(name);
        }
        if (javaType == LocalDateTime.class) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                    .as(OriginalType.TIMESTAMP_MILLIS)
                    .named(name);
        }
        // BigDecimal is written as its plain text representation in a UTF8 binary
        // column (lossless for the value); numeric DECIMAL semantics can be added
        // by the reader (Prompt 87) once the schema handles arbitrary scale.
        return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                .as(OriginalType.UTF8)
                .named(name);
    }

    /**
     * Writes the live rows of {@code table} to a {@code .parquet} file in the
     * table's data directory. Tombstoned rows are skipped, matching the CSV and
     * serialized persistence paths.
     *
     * @param table     the table to persist
     * @param tableName the table name used for both the schema name and the file base name
     * @throws DieselIOException if the Parquet file cannot be written
     */
    public static void writeTableToParquet(Table table, String tableName) {
        if (table == null) {
            throw new IllegalArgumentException("Table must not be null");
        }
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name must not be null or blank");
        }

        String dataDir = table.getDatabase() != null ? table.getDatabase().getDataDir() : ".";
        java.io.File dir = new java.io.File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        java.nio.file.Path filePath = java.nio.file.Path.of(dataDir, tableName + PARQUET_EXTENSION);

        List<String> columns = table.getColumns();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        MessageType schema = buildSchema(columns, columnTypes);
        List<Map<String, Object>> rows = table.getLiveRows();

        LOGGER.log(Level.INFO, "Writing table {0} ({1} rows) to Parquet file {2}",
                new Object[]{tableName, rows.size(), filePath});

        CodecFactory codecFactory = null;
        try (FileChannel channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            Configuration conf = new Configuration();
            ByteBufferAllocator allocator = new HeapByteBufferAllocator();
            codecFactory = new CodecFactory(conf, DEFAULT_PAGE_SIZE);
            CodecFactory.BytesCompressor compressor =
                    codecFactory.getCompressor(CompressionCodecName.SNAPPY);

            ParquetFileWriter fileWriter = new ParquetFileWriter(
                    new FileChannelOutputFile(channel), schema,
                    ParquetFileWriter.Mode.OVERWRITE, DEFAULT_ROW_GROUP_SIZE, 0);
            fileWriter.start();

            ColumnIOFactory columnIOFactory = new ColumnIOFactory();
            MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema);
            ParquetProperties props = ParquetProperties.builder()
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    .withAllocator(allocator)
                    .build();

            int rowGroupSize = DEFAULT_ROW_GROUP_SIZE;
            ColumnChunkPageWriteStore pageStore = new ColumnChunkPageWriteStore(
                    compressor, schema, allocator, rowGroupSize);
            ColumnWriteStore columnStore = props.newColumnWriteStore(schema, pageStore, pageStore);
            RecordConsumer recordConsumer = columnIO.getRecordWriter(columnStore);
            GroupWriteSupport.setSchema(schema, conf);

            if (!rows.isEmpty()) {
                fileWriter.startBlock(rows.size());
                org.apache.parquet.example.data.GroupWriter groupWriter =
                        new org.apache.parquet.example.data.GroupWriter(recordConsumer, schema);
                SimpleGroupFactory factory = new SimpleGroupFactory(schema);
                for (Map<String, Object> row : rows) {
                    Group group = factory.newGroup();
                    for (int i = 0; i < columns.size(); i++) {
                        String column = columns.get(i);
                        Object value = row.get(column);
                        if (value != null) {
                            writeValue(group, i, columnTypes.get(column), value);
                        }
                    }
                    groupWriter.write(group);
                }
                recordConsumer.flush();
                columnStore.flush();
                pageStore.flushToFileWriter(fileWriter);
                fileWriter.endBlock();
            }
            fileWriter.end(buildMetadata(table));

            table.setFileInitialized(true);
            LOGGER.log(Level.INFO, "Table {0} written to Parquet file {1}", new Object[]{tableName, filePath});
        } catch (IOException | RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Failed to write table to Parquet file: {0}", filePath);
            throw new DieselIOException("Failed to write table to Parquet file: " + filePath, e);
        } finally {
            if (codecFactory != null) {
                codecFactory.release();
            }
        }
    }

    /**
     * Builds the key-value metadata map stored in the Parquet footer. This
     * captures the full table schema and dictionary information needed to
     * reconstruct a {@link Table} from the {@code .parquet} file, so the file
     * can serve as the sole persistent representation of the table.
     *
     * <p>Encoded as plain UTF-8 strings, delimited with {@code ';'} between
     * entries and {@code '::'} between a key and its value.
     *
     * @param table the table to describe
     * @return the footer metadata map
     */
    static Map<String, String> buildMetadata(Table table) {
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put(ErrorMessages.PARQUET_META_FORMAT_VERSION, String.valueOf(table.getFormatVersion()));
        meta.put(ErrorMessages.PARQUET_META_PRIMARY_KEY,
                table.getPrimaryKeyColumn() != null ? table.getPrimaryKeyColumn() : "");
        meta.put(ErrorMessages.PARQUET_META_HAS_CLUSTERED, String.valueOf(table.hasClusteredIndex()));
        meta.put(ErrorMessages.PARQUET_META_CLUSTERED_COL,
                table.getClusteredIndexColumn() != null ? table.getClusteredIndexColumn() : "");

        StringJoiner typeJoiner = new StringJoiner(";");
        for (Map.Entry<String, Class<?>> e : table.getColumnTypes().entrySet()) {
            typeJoiner.add(e.getKey() + "::" + e.getValue().getName());
        }
        meta.put(ErrorMessages.PARQUET_META_COLUMN_TYPES, typeJoiner.toString());

        StringJoiner seqJoiner = new StringJoiner(";");
        for (Map.Entry<String, Sequence> e : table.getSequences().entrySet()) {
            Sequence seq = e.getValue();
            seqJoiner.add(e.getKey() + "::" + seq.getName() + "::" + seq.getType().getName()
                    + "::" + seq.getCurrentValue() + "::" + seq.getIncrement());
        }
        meta.put(ErrorMessages.PARQUET_META_SEQUENCES, seqJoiner.toString());

        StringJoiner idxJoiner = new StringJoiner(";");
        for (Map.Entry<String, String> e : table.getIndexDefinitions().entrySet()) {
            String column = e.getKey();
            String def = e.getValue();
            idxJoiner.add(column + "::" + (def != null ? def : ""));
            List<String> cover = table.getCoverColumnDefinitions().getOrDefault(column, Collections.emptyList());
            if (!cover.isEmpty()) {
                idxJoiner.add(column + "::@" + String.join(",", cover));
            }
        }
        meta.put(ErrorMessages.PARQUET_META_INDEXES, idxJoiner.toString());
        return meta;
    }

    private static void writeValue(Group group, int index, Class<?> javaType, Object value) {
        if (javaType == Integer.class) {
            group.add(index, (Integer) value);
        } else if (javaType == Short.class) {
            group.add(index, ((Short) value).intValue());
        } else if (javaType == Byte.class) {
            group.add(index, ((Byte) value).intValue());
        } else if (javaType == Long.class) {
            group.add(index, (Long) value);
        } else if (javaType == Float.class) {
            group.add(index, (Float) value);
        } else if (javaType == Double.class) {
            group.add(index, (Double) value);
        } else if (javaType == Boolean.class) {
            group.add(index, (Boolean) value);
        } else if (javaType == LocalDate.class) {
            group.add(index, (int) ((LocalDate) value).toEpochDay());
        } else if (javaType == LocalDateTime.class) {
            group.add(index, ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli());
        } else if (javaType == BigDecimal.class) {
            group.add(index, ((BigDecimal) value).toPlainString());
        } else if (value instanceof UUID uuid) {
            group.add(index, uuid.toString());
        } else {
            group.add(index, value.toString());
        }
    }

    /**
     * {@link OutputFile} implementation backed by a {@link FileChannel}, letting
     * Parquet write directly to a plain local file without the Hadoop file
     * system layer.
     */
    private static final class FileChannelOutputFile implements OutputFile {
        private final FileChannel channel;

        private FileChannelOutputFile(FileChannel channel) {
            this.channel = channel;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new ChannelPositionOutputStream(channel);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return new ChannelPositionOutputStream(channel);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return DEFAULT_ROW_GROUP_SIZE;
        }

        @Override
        public String getPath() {
            return "parquet-writer";
        }
    }

    /**
     * {@link PositionOutputStream} that writes bytes into a {@link FileChannel}
     * and tracks the current position.
     */
    private static final class ChannelPositionOutputStream extends PositionOutputStream {
        private final FileChannel channel;
        private long position;

        private ChannelPositionOutputStream(FileChannel channel) {
            this.channel = channel;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(int b) throws IOException {
            byte[] one = {(byte) b};
            write(one, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(b, off, len);
            int written = 0;
            while (written < len) {
                written += channel.write(buffer);
            }
            position += len;
        }

        @Override
        public void flush() throws IOException {
            channel.force(false);
        }
    }
}
