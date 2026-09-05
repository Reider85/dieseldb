package diesel;

import diesel.format.TableData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

class ParquetReader {

    private static final Logger LOGGER = Logger.getLogger(ParquetReader.class.getName());

    private ParquetReader() {
    }

    public static List<Map<String, Object>> readAll(Path file) {
        return read(file, null, null);
    }

    public static List<Map<String, Object>> readProjected(Path file, List<String> columns) {
        return read(file, columns, null);
    }

    public static List<Map<String, Object>> readFiltered(Path file, FilterPredicate filter) {
        return read(file, null, filter);
    }

    /**
     * Reads a Parquet file, applying projection (list of requested columns) and
     * a list of SQL-like conditions. The conditions are pushed down to Parquet
     * when possible (stats/record filtering) and always re-evaluated logically
     * at the row level, so results are exact for every supported operator
     * (including IS NULL / IS NOT NULL, which Parquet cannot express natively).
     */
    public static List<Map<String, Object>> readWhere(Path file, List<String> columns,
                                                      List<QueryParser.Condition> conditions,
                                                      Map<String, Class<?>> columnType) {
        FilterPredicate pushdown = buildFilterPredicate(conditions, columnType);
        List<Map<String, Object>> rows = read(file, columns, pushdown);
        if (conditions == null || conditions.isEmpty()) {
            return rows;
        }
        List<Map<String, Object>> filtered = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (matchesAll(row, conditions)) {
                filtered.add(row);
            }
        }
        return filtered;
    }

    static boolean matchesAll(Map<String, Object> row, List<QueryParser.Condition> conditions) {
        if (conditions == null) {
            return true;
        }
        for (QueryParser.Condition cond : conditions) {
            if (!matchesCondition(row, cond)) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesCondition(Map<String, Object> row, QueryParser.Condition cond) {
        if (cond.column == null) {
            if (cond.subConditions != null) {
                return matchesAll(row, cond.subConditions) != cond.not;
            }
            return true;
        }
        String col = cond.column.toUpperCase();
        Object actual = row.get(col);
        Object expected = cond.value;

        switch (cond.operator) {
            case EQUALS:
                return safeEquals(actual, expected) != cond.not;
            case NOT_EQUALS:
                return !safeEquals(actual, expected) != cond.not;
            case IS_NULL:
                boolean isNull = actual == null;
                return isNull != cond.not;
            case IS_NOT_NULL:
                boolean notNull = actual != null;
                return notNull != cond.not;
            case GREATER_THAN:
                return compare(actual, expected) > 0 != cond.not;
            case LESS_THAN:
                return compare(actual, expected) < 0 != cond.not;
            case GREATER_THAN_OR_EQUALS:
                return compare(actual, expected) >= 0 != cond.not;
            case LESS_THAN_OR_EQUALS:
                return compare(actual, expected) <= 0 != cond.not;
            case IN:
                if (cond.inValues == null) {
                    return true;
                }
                boolean in = false;
                for (Object v : cond.inValues) {
                    if (safeEquals(actual, v)) {
                        in = true;
                        break;
                    }
                }
                return in != cond.not;
            case LIKE:
                return like(actual, String.valueOf(expected)) != cond.not;
            case NOT_LIKE:
                return !like(actual, String.valueOf(expected)) != cond.not;
            default:
                return true;
        }
    }

    private static boolean safeEquals(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        return a.toString().equalsIgnoreCase(b.toString());
    }

    private static int compare(Object a, Object b) {
        if (a == null || b == null) {
            return 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        return a.toString().compareToIgnoreCase(b.toString());
    }

    private static boolean like(Object actual, String pattern) {
        if (actual == null) {
            return false;
        }
        String s = actual.toString();
        String regex = pattern
                .replace(".", "\\.")
                .replace("%", ".*")
                .replace("_", ".");
        return s.matches(regex);
    }

    public static List<Map<String, Object>> read(Path file, List<String> columns, FilterPredicate filter) {
        if (file == null) {
            throw new IllegalArgumentException("File path must not be null");
        }
        LOGGER.log(Level.INFO, "Reading Parquet file: {0}", file);

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ChannelInputFile inputFile = new ChannelInputFile(channel);
            MessageType fileSchema = readSchemaFromFile(inputFile);

            Map<String, Integer> fieldNameToIndex = buildFieldNameIndex(fileSchema);
            List<String> allColumnNames = getLeafFieldNames(fileSchema);

            ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
            FilterCompat.Filter compatFilter = null;
            if (filter != null) {
                compatFilter = FilterCompat.get(filter);
                optionsBuilder.withRecordFilter(compatFilter);
            }

            try (ParquetFileReader reader = new ParquetFileReader(inputFile, optionsBuilder.build())) {
                ColumnIOFactory columnIOFactory = new ColumnIOFactory();
                MessageColumnIO columnIO = columnIOFactory.getColumnIO(fileSchema);

                GroupReadSupport groupReadSupport = new GroupReadSupport();
                RecordMaterializer<Group> materializer = groupReadSupport.prepareForRead(
                        new Configuration(), Collections.emptyMap(), fileSchema,
                        new ReadSupport.ReadContext(fileSchema));

                List<Map<String, Object>> result = new ArrayList<>();
                PageReadStore pageStore;
                while ((pageStore = reader.readNextRowGroup()) != null) {
                    RecordReader<Group> recordReader;
                    if (compatFilter != null) {
                        recordReader = columnIO.getRecordReader(pageStore, materializer, compatFilter);
                    } else {
                        recordReader = columnIO.getRecordReader(pageStore, materializer);
                    }
                    long rowCount = pageStore.getRowCount();
                    for (long i = 0; i < rowCount; i++) {
                        Group group = recordReader.read();
                        if (recordReader.shouldSkipCurrentRecord()) {
                            continue;
                        }
                        if (group != null) {
                            result.add(groupToRow(group, allColumnNames, fieldNameToIndex, fileSchema, columns));
                        }
                    }
                }
                LOGGER.log(Level.INFO, "Read {0} rows from {1}", new Object[]{result.size(), file});
                return result;
            }
        } catch (IOException e) {
            throw new DieselIOException("Failed to read Parquet file: " + file, e);
        }
    }

    public static MessageType getFileSchema(Path file) {
        if (file == null) {
            throw new IllegalArgumentException("File path must not be null");
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            return readSchemaFromFile(new ChannelInputFile(channel));
        } catch (IOException e) {
            throw new DieselIOException("Failed to read Parquet schema: " + file, e);
        }
    }

    /**
     * Returns the key-value metadata map stored in the Parquet footer. This is
     * where {@link ParquetWriter#buildMetadata(Table)} places the table schema
     * and dictionary information that lets a full {@link Table} be rebuilt.
     *
     * @param file the Parquet file
     * @return the footer metadata map (never null)
     */
    public static Map<String, String> getFileMetadata(Path file) {
        if (file == null) {
            throw new IllegalArgumentException("File path must not be null");
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            Map<String, String> meta = ParquetFileReader.readFooter(
                    new ChannelInputFile(channel), ParquetMetadataConverter.NO_FILTER)
                    .getFileMetaData().getKeyValueMetaData();
            return meta != null ? meta : new HashMap<>();
        } catch (IOException e) {
            throw new DieselIOException("Failed to read Parquet metadata: " + file, e);
        }
    }

    /**
     * Reads a {@code .parquet} file into a {@link TableData} carrier,
     * reconstructing the engine metadata (format version, primary key,
     * sequences, index definitions) from the footer. This is the format-handler
     * entry point.
     *
     * @param file the Parquet file
     * @return the carrier
     */
    public static TableData readTableData(Path file) {
        Map<String, String> meta = getFileMetadata(file);
        List<String> columns = readSchemaColumns(file, meta);
        Map<String, Class<?>> columnTypes = readColumnTypes(file, meta, columns);
        List<Map<String, Object>> rows = readAll(file);

        Map<String, Object> engineMeta = new HashMap<>();
        engineMeta.put(TableData.META_FORMAT_VERSION,
                Integer.parseInt(meta.getOrDefault(ErrorMessages.PARQUET_META_FORMAT_VERSION, "0")));
        engineMeta.put(TableData.META_PRIMARY_KEY,
                meta.getOrDefault(ErrorMessages.PARQUET_META_PRIMARY_KEY, ""));
        engineMeta.put(TableData.META_HAS_CLUSTERED_INDEX,
                Boolean.parseBoolean(meta.getOrDefault(ErrorMessages.PARQUET_META_HAS_CLUSTERED, "false")));
        engineMeta.put(TableData.META_CLUSTERED_INDEX_COLUMN,
                meta.getOrDefault(ErrorMessages.PARQUET_META_CLUSTERED_COL, ""));
        engineMeta.put(TableData.META_SEQUENCES, readSequences(meta));
        Map<String, String> indexDefs = new HashMap<>();
        Map<String, List<String>> covers = new HashMap<>();
        parseIndexMetadata(meta, indexDefs, covers);
        engineMeta.put(TableData.META_INDEX_DEFINITIONS, indexDefs);
        engineMeta.put(TableData.META_COVER_COLUMN_DEFINITIONS, covers);

        return TableData.builder()
                .columns(columns)
                .columnTypes(columnTypes)
                .rows(rows)
                .metadata(engineMeta)
                .build();
    }

    /**
     * Splits the index and covering-column metadata from the Parquet footer
     * into plain maps, without touching a {@link Table}.
     *
     * @param meta      the footer metadata
     * @param indexDefs receives the indexed column to definition mapping
     * @param covers    receives the indexed column to covering-column-list mapping
     */
    static void parseIndexMetadata(Map<String, String> meta, Map<String, String> indexDefs,
                                   Map<String, List<String>> covers) {
        String idxMeta = meta.get(ErrorMessages.PARQUET_META_INDEXES);
        if (idxMeta == null || idxMeta.isBlank()) {
            return;
        }
        for (String entry : idxMeta.split(";")) {
            if (entry.isBlank()) {
                continue;
            }
            int sep = entry.indexOf("::");
            if (sep < 0) {
                continue;
            }
            String column = entry.substring(0, sep);
            String def = entry.substring(sep + 2);
            if (def.startsWith("@")) {
                covers.put(column, List.of(def.substring(1).split(",")));
            } else {
                indexDefs.put(column, def);
            }
        }
    }

    /**
     * Returns the ordered column names for a table reconstructed from Parquet.
     *
     * @param file the Parquet file
     * @param meta the footer metadata
     * @return the column names in schema order
     */
    public static List<String> readSchemaColumns(Path file, Map<String, String> meta) {
        List<String> columns = new ArrayList<>();
        String types = meta.get(ErrorMessages.PARQUET_META_COLUMN_TYPES);
        if (types != null && !types.isBlank()) {
            for (String entry : types.split(";")) {
                if (entry.isBlank()) {
                    continue;
                }
                int sep = entry.indexOf("::");
                String column = sep >= 0 ? entry.substring(0, sep) : entry;
                columns.add(column);
            }
            return columns;
        }
        for (Type field : getFileSchema(file).getFields()) {
            columns.add(field.getName());
        }
        return columns;
    }

    /**
     * Returns the Java type map for a table reconstructed from Parquet.
     *
     * @param file    the Parquet file
     * @param meta    the footer metadata
     * @param columns the ordered column names
     * @return the column name to Java class mapping
     */
    public static Map<String, Class<?>> readColumnTypes(Path file, Map<String, String> meta,
                                                        List<String> columns) {
        Map<String, Class<?>> types = new HashMap<>();
        String typesMeta = meta.get(ErrorMessages.PARQUET_META_COLUMN_TYPES);
        if (typesMeta != null && !typesMeta.isBlank()) {
            for (String entry : typesMeta.split(";")) {
                if (entry.isBlank()) {
                    continue;
                }
                int sep = entry.indexOf("::");
                if (sep < 0) {
                    continue;
                }
                String column = entry.substring(0, sep);
                String clsName = entry.substring(sep + 2);
                try {
                    types.put(column, Class.forName(clsName));
                } catch (ClassNotFoundException e) {
                    LOGGER.log(Level.WARNING, "Unknown column type class {0} for column {1}",
                            new Object[]{clsName, column});
                }
            }
            return types;
        }
        // Fallback: infer Java types from the Parquet schema.
        MessageType schema = getFileSchema(file);
        for (String column : columns) {
            Type field = schema.getType(column);
            if (field == null || !field.isPrimitive()) {
                continue;
            }
            types.put(column, javaTypeFromPrimitive(field.asPrimitiveType()));
        }
        return types;
    }

    private static Class<?> javaTypeFromPrimitive(PrimitiveType primitiveType) {
        OriginalType originalType = primitiveType.getOriginalType();
        switch (primitiveType.getPrimitiveTypeName()) {
            case INT32:
                return originalType == OriginalType.DATE ? LocalDate.class : Integer.class;
            case INT64:
                return originalType == OriginalType.TIMESTAMP_MILLIS ? LocalDateTime.class : Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case BOOLEAN:
                return Boolean.class;
            default:
                return String.class;
        }
    }

    /**
     * Rebuilds the sequences defined on a table reconstructed from Parquet.
     *
     * @param meta the footer metadata
     * @return the sequence map keyed by sequence name
     */
    public static Map<String, Sequence> readSequences(Map<String, String> meta) {
        Map<String, Sequence> sequences = new HashMap<>();
        String seqMeta = meta.get(ErrorMessages.PARQUET_META_SEQUENCES);
        if (seqMeta == null || seqMeta.isBlank()) {
            return sequences;
        }
        for (String entry : seqMeta.split(";")) {
            if (entry.isBlank()) {
                continue;
            }
            String[] parts = entry.split("::");
            if (parts.length < 5) {
                continue;
            }
            try {
                String mapKey = parts[0];
                String seqName = parts[1];
                Class<?> seqType = Class.forName(parts[2]);
                long currentValue = Long.parseLong(parts[3]);
                long increment = Long.parseLong(parts[4]);
                // Reconstruct the sequence so the *next* value equals current+increment.
                long start = currentValue + increment;
                sequences.put(mapKey, new Sequence(seqName, seqType, start, increment));
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to restore sequence entry {0}", entry);
            }
        }
        return sequences;
    }

    /**
     * Restores secondary index definitions on a table reconstructed from
     * Parquet so that CREATE INDEX state is preserved across a save/load
     * round trip.
     *
     * @param table the table being rebuilt
     * @param meta  the footer metadata
     */
    public static void restoreIndexes(Table table, Map<String, String> meta) {
        Map<String, String> indexDefs = new HashMap<>();
        Map<String, List<String>> covers = new HashMap<>();
        parseIndexMetadata(meta, indexDefs, covers);
        table.getCoverColumnDefinitions().putAll(covers);
        for (Map.Entry<String, String> entry : indexDefs.entrySet()) {
            table.getIndexDefinitions().putIfAbsent(entry.getKey(), entry.getValue());
        }
        try {
            table.rebuildSecondaryIndexesForLoad();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to rebuild secondary indexes during load: {0}", e.getMessage());
        }
    }

    private static MessageType readSchemaFromFile(InputFile inputFile) throws IOException {
        return ParquetFileReader.readFooter(inputFile, ParquetMetadataConverter.NO_FILTER)
                .getFileMetaData().getSchema();
    }

    private static Map<String, Integer> buildFieldNameIndex(MessageType schema) {
        Map<String, Integer> index = new HashMap<>();
        List<Type> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            index.put(fields.get(i).getName(), i);
        }
        return index;
    }

    private static List<String> getLeafFieldNames(MessageType schema) {
        List<String> names = new ArrayList<>();
        for (Type field : schema.getFields()) {
            names.add(field.getName());
        }
        return names;
    }

    private static Map<String, Object> groupToRow(Group group, List<String> columnNames,
                                                  Map<String, Integer> fieldNameToIndex, MessageType readSchema,
                                                  List<String> projection) {
        Map<String, Object> row = new HashMap<>(columnNames.size());
        for (String column : columnNames) {
            if (projection != null && !projection.isEmpty() && !projection.contains(column)) {
                continue;
            }
            Integer fieldIdx = fieldNameToIndex.get(column);
            if (fieldIdx == null) {
                continue;
            }
            Type fieldType = readSchema.getFields().get(fieldIdx);
            if (!fieldType.isPrimitive()) {
                continue;
            }
            try {
                if (group.getFieldRepetitionCount(fieldIdx) == 0) {
                    row.put(column, null);
                    continue;
                }
                row.put(column, readPrimitiveValue(group, fieldIdx, fieldType.asPrimitiveType()));
            } catch (Exception e) {
                row.put(column, null);
            }
        }
        return row;
    }

    private static Object readPrimitiveValue(Group group, int fieldIndex,
                                              PrimitiveType primitiveType) {
        PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
        OriginalType originalType = primitiveType.getOriginalType();

        switch (typeName) {
            case INT32:
                if (originalType == OriginalType.DATE) {
                    return LocalDate.ofEpochDay(group.getInteger(fieldIndex, 0));
                }
                return group.getInteger(fieldIndex, 0);

            case INT64:
                if (originalType == OriginalType.TIMESTAMP_MILLIS) {
                    return LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(group.getLong(fieldIndex, 0)), ZoneOffset.UTC);
                }
                return group.getLong(fieldIndex, 0);

            case FLOAT:
                return group.getFloat(fieldIndex, 0);

            case DOUBLE:
                return group.getDouble(fieldIndex, 0);

            case BOOLEAN:
                return group.getBoolean(fieldIndex, 0);

            case BINARY:
                if (originalType == OriginalType.UTF8 || originalType == OriginalType.ENUM) {
                    return group.getString(fieldIndex, 0);
                }
                if (originalType == OriginalType.DECIMAL) {
                    return new BigDecimal(group.getString(fieldIndex, 0));
                }
                Binary binary = group.getBinary(fieldIndex, 0);
                return binary != null ? new String(binary.getBytes()) : null;

            default:
                return group.getString(fieldIndex, 0);
        }
    }

    private static final class ChannelInputFile implements InputFile {
        private final FileChannel channel;

        ChannelInputFile(FileChannel channel) {
            this.channel = channel;
        }

        @Override
        public long getLength() throws IOException {
            return channel.size();
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new ChannelSeekableInputStream(channel);
        }
    }

    private static final class ChannelSeekableInputStream extends SeekableInputStream {
        private final FileChannel channel;
        private long position;

        ChannelSeekableInputStream(FileChannel channel) {
            this.channel = channel;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void seek(long newPos) throws IOException {
            this.position = newPos;
            channel.position(newPos);
        }

        @Override
        public void readFully(byte[] buf) throws IOException {
            readFully(buf, 0, buf.length);
        }

        @Override
        public void readFully(byte[] buf, int off, int len) throws IOException {
            java.nio.ByteBuffer nioBuf = java.nio.ByteBuffer.wrap(buf, off, len);
            int totalRead = 0;
            while (totalRead < len) {
                int n = channel.read(nioBuf);
                if (n == -1) break;
                totalRead += n;
            }
            position += totalRead;
            if (totalRead < len) {
                throw new IOException("EOF: expected " + len + " bytes, got " + totalRead);
            }
        }

        @Override
        public int read() throws IOException {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(1);
            int read = channel.read(buf);
            if (read == -1) return -1;
            position++;
            return buf.get(0) & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b, off, len);
            int totalRead = 0;
            while (totalRead < len) {
                int n = channel.read(buf);
                if (n == -1) break;
                totalRead += n;
            }
            position += totalRead;
            return totalRead == 0 ? -1 : totalRead;
        }

        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException {
            int n = channel.read(buf);
            if (n > 0) position += n;
            return n;
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException {
            int target = buf.remaining();
            int totalRead = 0;
            while (totalRead < target) {
                int n = channel.read(buf);
                if (n == -1) break;
                totalRead += n;
            }
            position += totalRead;
            if (totalRead < target) {
                throw new IOException("EOF: expected " + target + " bytes, got " + totalRead);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static FilterPredicate buildFilterPredicate(List<QueryParser.Condition> conditions,
                                                Map<String, Class<?>> columnTypes) {
        if (conditions == null || conditions.isEmpty()) {
            return null;
        }
        FilterPredicate result = null;
        for (QueryParser.Condition cond : conditions) {
            FilterPredicate single = buildSinglePredicate(cond, columnTypes);
            if (single != null) {
                result = result == null ? single : FilterApi.and(result, single);
            }
        }
        return result;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static FilterPredicate buildSinglePredicate(QueryParser.Condition cond,
                                                        Map<String, Class<?>> columnTypes) {
        String column = cond.column;
        if (column == null) return null;
        String upperCol = column.toUpperCase();

        if (cond.operator == QueryParser.Operator.IS_NULL
                || cond.operator == QueryParser.Operator.IS_NOT_NULL) {
            return null;
        }

        Object value = cond.value;
        if (value == null) return null;

        Class<?> javaType = columnTypes.get(upperCol);
        if (cond.operator == null) return null;

        try {
            switch (cond.operator) {
                case EQUALS:
                    return buildEqualityFilter(upperCol, javaType, value);
                case NOT_EQUALS:
                    FilterPredicate eq = buildEqualityFilter(upperCol, javaType, value);
                    return eq != null ? FilterApi.not(eq) : null;
                case GREATER_THAN:
                    return buildComparisonFilter(upperCol, javaType, value, "gt");
                case GREATER_THAN_OR_EQUALS:
                    return buildComparisonFilter(upperCol, javaType, value, "gtEq");
                case LESS_THAN:
                    return buildComparisonFilter(upperCol, javaType, value, "lt");
                case LESS_THAN_OR_EQUALS:
                    return buildComparisonFilter(upperCol, javaType, value, "ltEq");
                case IN:
                    return buildInFilter(upperCol, javaType, cond.inValues);
                default:
                    return null;
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Cannot build filter for condition: {0}", cond);
            return null;
        }
    }

    private static FilterPredicate buildEqualityFilter(String column, Class<?> javaType, Object value) {
        if (javaType == Integer.class || javaType == Short.class || javaType == Byte.class) {
            return FilterApi.eq(FilterApi.intColumn(column), ((Number) value).intValue());
        }
        if (javaType == Long.class) {
            return FilterApi.eq(FilterApi.longColumn(column), ((Number) value).longValue());
        }
        if (javaType == Float.class) {
            return FilterApi.eq(FilterApi.floatColumn(column), ((Number) value).floatValue());
        }
        if (javaType == Double.class) {
            return FilterApi.eq(FilterApi.doubleColumn(column), ((Number) value).doubleValue());
        }
        if (javaType == Boolean.class) {
            return FilterApi.eq(FilterApi.booleanColumn(column), ((Boolean) value));
        }
        return FilterApi.eq(FilterApi.binaryColumn(column), Binary.fromString(value.toString()));
    }

    private static FilterPredicate buildComparisonFilter(String column, Class<?> javaType,
                                                         Object value, String op) {
        if (javaType == Integer.class || javaType == Short.class || javaType == Byte.class) {
            int v = ((Number) value).intValue();
            switch (op) {
                case "gt":   return FilterApi.gt(FilterApi.intColumn(column), v);
                case "gtEq": return FilterApi.gtEq(FilterApi.intColumn(column), v);
                case "lt":   return FilterApi.lt(FilterApi.intColumn(column), v);
                case "ltEq": return FilterApi.ltEq(FilterApi.intColumn(column), v);
            }
        }
        if (javaType == Long.class) {
            long v = ((Number) value).longValue();
            switch (op) {
                case "gt":   return FilterApi.gt(FilterApi.longColumn(column), v);
                case "gtEq": return FilterApi.gtEq(FilterApi.longColumn(column), v);
                case "lt":   return FilterApi.lt(FilterApi.longColumn(column), v);
                case "ltEq": return FilterApi.ltEq(FilterApi.longColumn(column), v);
            }
        }
        if (javaType == Float.class) {
            float v = ((Number) value).floatValue();
            switch (op) {
                case "gt":   return FilterApi.gt(FilterApi.floatColumn(column), v);
                case "gtEq": return FilterApi.gtEq(FilterApi.floatColumn(column), v);
                case "lt":   return FilterApi.lt(FilterApi.floatColumn(column), v);
                case "ltEq": return FilterApi.ltEq(FilterApi.floatColumn(column), v);
            }
        }
        if (javaType == Double.class) {
            double v = ((Number) value).doubleValue();
            switch (op) {
                case "gt":   return FilterApi.gt(FilterApi.doubleColumn(column), v);
                case "gtEq": return FilterApi.gtEq(FilterApi.doubleColumn(column), v);
                case "lt":   return FilterApi.lt(FilterApi.doubleColumn(column), v);
                case "ltEq": return FilterApi.ltEq(FilterApi.doubleColumn(column), v);
            }
        }
        Binary binVal = Binary.fromString(value.toString());
        switch (op) {
            case "gt":   return FilterApi.gt(FilterApi.binaryColumn(column), binVal);
            case "gtEq": return FilterApi.gtEq(FilterApi.binaryColumn(column), binVal);
            case "lt":   return FilterApi.lt(FilterApi.binaryColumn(column), binVal);
            case "ltEq": return FilterApi.ltEq(FilterApi.binaryColumn(column), binVal);
        }
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static FilterPredicate buildInFilter(String column, Class<?> javaType, List<Object> inValues) {
        if (inValues == null || inValues.isEmpty()) return null;

        if (javaType == Integer.class || javaType == Short.class || javaType == Byte.class) {
            java.util.Set<Integer> set = new java.util.HashSet<>();
            for (Object v : inValues) if (v != null) set.add(((Number) v).intValue());
            return FilterApi.in(FilterApi.intColumn(column), set);
        }
        if (javaType == Long.class) {
            java.util.Set<Long> set = new java.util.HashSet<>();
            for (Object v : inValues) if (v != null) set.add(((Number) v).longValue());
            return FilterApi.in(FilterApi.longColumn(column), set);
        }
        if (javaType == Float.class) {
            java.util.Set<Float> set = new java.util.HashSet<>();
            for (Object v : inValues) if (v != null) set.add(((Number) v).floatValue());
            return FilterApi.in(FilterApi.floatColumn(column), set);
        }
        if (javaType == Double.class) {
            java.util.Set<Double> set = new java.util.HashSet<>();
            for (Object v : inValues) if (v != null) set.add(((Number) v).doubleValue());
            return FilterApi.in(FilterApi.doubleColumn(column), set);
        }
        if (javaType == Boolean.class) {
            java.util.Set<Boolean> set = new java.util.HashSet<>();
            for (Object v : inValues) if (v != null) set.add((Boolean) v);
            return FilterApi.in(FilterApi.booleanColumn(column), set);
        }
        java.util.Set<Binary> set = new java.util.HashSet<>();
        for (Object v : inValues) if (v != null) set.add(Binary.fromString(v.toString()));
        return FilterApi.in(FilterApi.binaryColumn(column), set);
    }
}
