package diesel.storage.avro;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

import diesel.DieselIOException;
import diesel.storage.AbstractRowStorage;
import diesel.storage.AtomicFileWriter;
import diesel.storage.RowStorage;

/**
 * Avro-backed implementation of {@link RowStorage}. Rows are kept in an
 * in-memory compact {@code Object[]} buffer and persisted as Avro data files
 * ({@code .avro}). Schema management is delegated to {@link AvroSchemaManager},
 * type conversions to {@link AvroTypeMapper} and union handling (nullable
 * {@code ["null", T]} field schemas, branch resolution, null-first ordering)
 * to {@link AvroUnionHandler}.
 *
 * <p>Each {@code saveToFile} writes the Avro data file via
 * {@link AtomicFileWriter} (crash-safe temp+rename) and the schema sidecar
 * ({@code .avsc}) via {@link AvroSchemaManager}. {@code loadFromFile} reads
 * the Avro data file and rebuilds the in-memory rows.
 *
 * @since Prompt 59
 */
public class AvroRowStorage extends AbstractRowStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRowStorage.class);

    private static final String AVRO_EXTENSION = ".avro";
    private static final String AVRO_PATH_KEY = "avro.path";
    private static final String DEFAULT_AVRO_PATH = "data/avro";

    protected final List<Object[]> rows = new ArrayList<>();
    private final List<String> colNames;
    private final Map<String, Integer> colIndex;
    private boolean fileInitialized;
    private AvroPrimaryKeyIndex primaryKeyIndex;
    private AvroSecondaryIndexManager secondaryIndexManager;

    public AvroRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        super(tableName, columns, columnTypes);
        this.colNames = List.copyOf(columns);
        this.colIndex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < columns.size(); i++) {
            colIndex.put(columns.get(i), i);
        }
        // Initialize secondary index manager
        Map<Class<?>, Object> convertedTypes = new HashMap<>();
        for (Map.Entry<String, Class<?>> entry : columnTypes.entrySet()) {
            convertedTypes.put(entry.getValue(), entry.getKey());
        }
        this.secondaryIndexManager = new AvroSecondaryIndexManager(tableName, columns, convertedTypes);
    }

    public boolean isFileInitialized() {
        return fileInitialized;
    }

    public void setFileInitialized(boolean fileInitialized) {
        this.fileInitialized = fileInitialized;
    }

    // ─── Primary-key index (Prompt 85) ────────────────────────────

    /**
     * Sets the primary-key column and builds the
     * {@link AvroPrimaryKeyIndex} for fast lookups.
     */
    @Override
    public void setPrimaryKeyColumn(String primaryKeyColumn) {
        super.setPrimaryKeyColumn(primaryKeyColumn);
        if (primaryKeyColumn != null && !primaryKeyColumn.isBlank()) {
            primaryKeyIndex = AvroPrimaryKeyIndex.create(colNames, columnTypes);
            primaryKeyIndex.setPrimaryKeyColumn(primaryKeyColumn, rows);
            LOGGER.debug("AvroRowStorage {} primary-key index initialized on column '{}'",
                    tableName, primaryKeyColumn);
        }
    }

    /**
     * Looks up a row by its primary key value.
     *
     * @param key the primary-key value
     * @return the row as a Map, or {@code null} if not found
     */
    public Map<String, Object> lookupByPrimaryKey(Object key) {
        if (primaryKeyIndex == null || !primaryKeyIndex.isEnabled()) return null;
        Integer idx = primaryKeyIndex.lookup(key);
        if (idx == null || idx < 0 || idx >= rows.size()) return null;
        return toMap(rows.get(idx));
    }

    /**
     * Returns the primary-key index, or {@code null} if not initialized.
     */
    public AvroPrimaryKeyIndex getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }

    /**
     * Returns the secondary index manager for this table.
     */
    public AvroSecondaryIndexManager getSecondaryIndexManager() {
        return secondaryIndexManager;
    }

    /**
     * Creates a new secondary index on a single column.
     */
    public void createSecondaryIndex(String indexName, String columnName) {
        secondaryIndexManager.createIndex(indexName, columnName);
    }

    /**
     * Creates a new composite index on multiple columns.
     */
    public void createCompositeIndex(String indexName, List<String> columnNames) {
        secondaryIndexManager.createCompositeIndex(indexName, columnNames);
    }

    /**
     * Drops an existing secondary index.
     */
    public void dropSecondaryIndex(String indexName) {
        secondaryIndexManager.dropIndex(indexName);
    }

    /**
     * Gets all secondary index names.
     */
    public Set<String> getSecondaryIndexNames() {
        return secondaryIndexManager.getIndexNames();
    }

    /**
     * Returns the raw internal rows (for index page caching).
     */
    protected List<Object[]> getRows() {
        return rows;
    }

    private Object[] fromMap(Map<String, Object> row) {
        Object[] values = new Object[colNames.size()];
        if (row == null) return values;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            Integer idx = colIndex.get(entry.getKey());
            if (idx != null) values[idx] = entry.getValue();
        }
        return values;
    }

    private Map<String, Object> toMap(Object[] row) {
        Map<String, Object> map = new HashMap<>(Math.max(colNames.size() * 2, 4));
        for (int i = 0; i < colNames.size() && row != null && i < row.length; i++) {
            map.put(colNames.get(i), row[i]);
        }
        return map;
    }

    // ─── RowStorage lifecycle ───────────────────────────────────────

    @Override
    public void open() { }

    @Override
    public void close() { }

    @Override
    public List<Map<String, Object>> scan() {
        List<Map<String, Object>> result = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            result.add(toMap(row));
        }
        return result;
    }

    @Override
    public void insert(Map<String, Object> row) {
        Object[] arr = fromMap(row);
        rows.add(arr);
        syncIndexAppend(arr, rows.size() - 1);
        if (primaryKeyIndex != null && primaryKeyIndex.isEnabled()) {
            primaryKeyIndex.insert(arr, rows.size() - 1);
        }
        // Sync with secondary indexes
        secondaryIndexManager.syncOnInsert(row, rows.size() - 1);
    }

    @Override
    public void insertAt(int rowIndex, Map<String, Object> row) {
        Object[] arr = fromMap(row);
        rows.add(rowIndex, arr);
        syncIndexInsert(arr, rowIndex);
        if (primaryKeyIndex != null && primaryKeyIndex.isEnabled()) {
            primaryKeyIndex.buildIndex(rows);
        }
    }

    @Override
    public void update(int rowIndex, Map<String, Object> row) {
        Object[] oldRow = rows.get(rowIndex);
        Object[] newRow = fromMap(row);
        rows.set(rowIndex, newRow);
        syncIndexUpdate(oldRow, rowIndex, newRow);
        if (primaryKeyIndex != null && primaryKeyIndex.isEnabled()) {
            primaryKeyIndex.update(oldRow, rowIndex, newRow);
        }
        // Sync with secondary indexes
        secondaryIndexManager.syncOnUpdate(toMap(oldRow), row, rowIndex);
    }

    @Override
    public void delete(int rowIndex) {
        Object[] row = rows.get(rowIndex);
        rows.remove(rowIndex);
        syncIndexDelete(rowIndex);
        if (primaryKeyIndex != null && primaryKeyIndex.isEnabled()) {
            primaryKeyIndex.buildIndex(rows);
        }
        // Sync with secondary indexes
        secondaryIndexManager.syncOnDelete(toMap(row), rowIndex);
    }

    @Override
    public void setRows(List<Map<String, Object>> newRows) {
        rows.clear();
        for (Map<String, Object> row : newRows) {
            rows.add(fromMap(row));
        }
        syncIndexBulkFromArrays(rows);
        if (primaryKeyIndex != null && primaryKeyIndex.isEnabled()) {
            primaryKeyIndex.buildIndex(rows);
        }
        // Rebuild all secondary indexes
        secondaryIndexManager.rebuildAllIndexes(newRows);
    }

    // ─── Persistence ────────────────────────────────────────────────

    @Override
    public void saveToFile(String tableName) {
        Schema schema = buildNullableSchema(tableName, columns, columnTypes);
        File avroFile = new File(resolveAvroFilePath());
        try {
            File parent = avroFile.getParentFile();
            if (parent != null) parent.mkdirs();
            writeAvroFileEfficient(avroFile, schema);
            writeSchemaSidecar(tableName, schema);
            savePrimaryKeySidecar(avroFile);
            saveSecondaryIndexes(avroFile);
            fileInitialized = true;
            LOGGER.info("AvroRowStorage {} saved Avro to {} with {} rows",
                    tableName, avroFile.getPath(), rows.size());
        } catch (IOException e) {
            throw new DieselIOException("Failed to save table to Avro file: " + avroFile.getPath(), e);
        }
    }

    @Override
    public void loadFromFile(String tableName) {
        File avroFile = new File(resolveAvroFilePath());
        if (!avroFile.exists()) {
            LOGGER.info("Avro file {} not found for storage {}", avroFile.getPath(), tableName);
            return;
        }
        List<Object[]> previous = new ArrayList<>(rows);
        try {
            List<Object[]> loaded = readAvroFile(avroFile, tableName);
            rows.clear();
            rows.addAll(loaded);
            fileInitialized = true;
            LOGGER.info("AvroRowStorage {} loaded Avro from {} with {} rows",
                    tableName, avroFile.getPath(), rows.size());
            syncIndexBulkFromArrays(rows);
            loadPrimaryKeySidecar(avroFile);
            loadSecondaryIndexes(avroFile);
        } catch (DieselIOException e) {
            rows.clear();
            rows.addAll(previous);
            throw e;
        } catch (IOException e) {
            rows.clear();
            rows.addAll(previous);
            throw new DieselIOException("Failed to load table from Avro file: " + avroFile.getPath(), e);
        }
    }

    // ─── Avro file I/O ──────────────────────────────────────────────

    private void writeAvroFileEfficient(File target, Schema schema) throws IOException {
        AvroCompressionConfig compression = AvroCompressionConfig.resolve();
        String effectiveCodec = compression.effectiveCodec(rows);
        org.apache.avro.file.CodecFactory codecFactory =
                AvroCodecFactory.factory(effectiveCodec, compression.level());
        AvroFileHeader header = buildFileHeader(effectiveCodec, compression.level());
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(target)) {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            OutputStream nonClosing = new OutputStream() {
                private final OutputStream delegate = afw.outputStream();
                @Override public void write(int b) throws IOException { delegate.write(b); }
                @Override public void write(byte[] b, int off, int len) throws IOException { delegate.write(b, off, len); }
                @Override public void flush() throws IOException { delegate.flush(); }
                @Override public void close() { /* no-op: AtomicFileWriter owns the channel */ }
            };
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            if (codecFactory != null) {
                dataFileWriter.setCodec(codecFactory);
            }
            for (Map.Entry<String, byte[]> e : header.toMetaMap().entrySet()) {
                dataFileWriter.setMeta(e.getKey(), e.getValue());
            }
            try {
                dataFileWriter.create(schema, nonClosing);
                for (Object[] row : rows) {
                    GenericRecord record = toRecord(row, schema);
                    dataFileWriter.append(record);
                }
                dataFileWriter.flush();
            } finally {
                dataFileWriter.close();
            }
            afw.commit();
        }
    }

    /**
     * Builds the diesel metadata ({@link AvroFileHeader}) written into the Avro
     * file header (Prompt 76): engine, database, table name, file-format and
     * schema versions, creation timestamp and the effective compression
     * codec/level.
     */
    private AvroFileHeader buildFileHeader(String codec, int level) {
        return AvroFileHeader.builder()
                .database(resolveDatabaseName())
                .tableName(AvroSchemaManager.sanitizeName(tableName))
                .creationTimestamp(java.time.Instant.now())
                .compressionCodec(codec)
                .compressionLevel(level)
                .build();
    }

    private static String resolveDatabaseName() {
        String systemValue = System.getProperty("avro.metadata.database");
        if (systemValue != null && !systemValue.isBlank()) {
            return systemValue;
        }
        return resolveConfigValue("avro.metadata.database", "default");
    }

    private void writeAvroFile(File target, Schema schema) throws IOException {
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(target)) {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            OutputStream nonClosing = new OutputStream() {
                private final OutputStream delegate = afw.outputStream();
                @Override public void write(int b) throws IOException { delegate.write(b); }
                @Override public void write(byte[] b, int off, int len) throws IOException { delegate.write(b, off, len); }
                @Override public void flush() throws IOException { delegate.flush(); }
                @Override public void close() { /* no-op: AtomicFileWriter owns the channel */ }
            };
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            try {
                dataFileWriter.create(schema, nonClosing);
                for (Object[] row : rows) {
                    GenericRecord record = toRecord(row, schema);
                    dataFileWriter.append(record);
                }
                dataFileWriter.flush();
            } finally {
                dataFileWriter.close();
            }
            afw.commit();
        }
    }

    private List<Object[]> readAvroFile(File file, String tableName) throws IOException {
        File sidecarFile = new File(resolveAvroFilePath()).toPath()
                .resolveSibling(AvroSchemaManager.sanitizeName(tableName) + AvroSchemaManager.AVSC_EXTENSION).toFile();
        AvroDataFileReader reader;
        if (sidecarFile.exists()) {
            Schema sidecarSchema = AvroSchemaManager.readSchemaFile(sidecarFile.toPath());
            reader = new AvroDataFileReader(file, sidecarSchema);
            if (!reader.isReaderSchemaCompatible()) {
                LOGGER.debug("Sidecar schema {} shares no record name with the header schema of {}; doing a full read",
                        sidecarFile.getPath(), file.getPath());
                reader.close();
                reader = new AvroDataFileReader(file);
            }
        } else {
            reader = new AvroDataFileReader(file);
        }
        validateFileHeader(reader.getFileHeader(), file);
        try (AvroReadIterator iterator = new AvroReadIterator(reader, columns, columnTypes)) {
            List<Object[]> loaded = new ArrayList<>();
            while (iterator.hasNext()) {
                loaded.add(iterator.next());
            }
            return loaded;
        }
    }

    /**
     * Validates the DieselDB metadata parsed from the Avro file header (Prompt 76).
     * Files written before Prompt 76 carry no DieselDB keys and validate cleanly;
     * a header from a newer engine, or with corrupted DieselDB metadata, fails the
     * load with a descriptive {@link DieselIOException}.
     */
    private void validateFileHeader(AvroFileHeader header, File file) throws IOException {
        List<String> problems = header.validate();
        if (!problems.isEmpty()) {
            throw new DieselIOException(
                    "Invalid Avro file header of " + file.getPath() + ": " + String.join("; ", problems),
                    null);
        }
        if (header.hasDieselMetadata()) {
            LOGGER.debug("Avro file {} header: {}", file.getPath(), header);
        }
    }

    private void writeSchemaSidecar(String tableName, Schema schema) throws IOException {
        File avroFile = new File(resolveAvroFilePath());
        Path sidecarPath = avroFile.toPath().resolveSibling(AvroSchemaManager.sanitizeName(tableName) + AvroSchemaManager.AVSC_EXTENSION);
        AvroSchemaManager.writeSchemaFile(schema, sidecarPath);
    }

    // ─── Primary-key index sidecar (Prompt 85) ────────────────────

    private void savePrimaryKeySidecar(File avroFile) {
        if (primaryKeyIndex == null || !primaryKeyIndex.isEnabled()) return;
        try {
            Path sidecar = AvroPrimaryKeyIndex.sidecarPath(avroFile.toPath());
            primaryKeyIndex.saveToSidecar(sidecar, avroFile.length(), avroFile.lastModified());
        } catch (IOException e) {
            LOGGER.warn("Failed to save primary-key index sidecar: {}", e.getMessage());
        }
    }

    private void loadPrimaryKeySidecar(File avroFile) {
        if (primaryKeyIndex == null || !primaryKeyIndex.isEnabled()) return;
        Path sidecar = AvroPrimaryKeyIndex.sidecarPath(avroFile.toPath());
        Map<Object, Integer> loaded = AvroPrimaryKeyIndex.loadFromSidecar(
                sidecar, avroFile.length(), avroFile.lastModified());
        if (loaded != null) {
            LOGGER.debug("AvroRowStorage {} loaded primary-key index sidecar ({} entries)",
                    tableName, loaded.size());
        } else {
            primaryKeyIndex.buildIndex(rows);
        }
    }

    // ─── File path resolution ───────────────────────────────────────

    /**
     * Builds an Avro RECORD schema with all fields wrapped in nullable unions
     * ({@code ["null", type]}) so that Java {@code null} values are supported.
     */
    private static Schema buildNullableSchema(String tableName, List<String> columns,
                                               Map<String, Class<?>> columnTypes) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name must not be blank");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Column list must not be empty for table: " + tableName);
        }

        String avroName = AvroSchemaManager.sanitizeName(tableName);
        List<Schema.Field> fields = new ArrayList<>(columns.size());

        for (String col : columns) {
            Class<?> javaType = null;
            for (Map.Entry<String, Class<?>> e : columnTypes.entrySet()) {
                if (e.getKey().equalsIgnoreCase(col)) {
                    javaType = e.getValue();
                    break;
                }
            }
            if (javaType == null) {
                throw new IllegalArgumentException("No type defined for column: " + col + " in table: " + tableName);
            }
            Schema baseSchema = AvroTypeMapper.toAvroSchema(javaType, col);
            Schema nullableSchema = AvroUnionHandler.createNullableUnion(baseSchema);
            Schema.Field field = new Schema.Field(col, nullableSchema, null, null);
            fields.add(field);
        }

        Schema record = Schema.createRecord(avroName,
                "DieselDB table: " + tableName,
                "diesel.avro",
                false);
        record.setFields(fields);
        return record;
    }

    private String resolveAvroFilePath() {
        if (dataDir != null && !dataDir.isBlank()) {
            return dataDir + File.separator + tableName + AVRO_EXTENSION;
        }
        String avroDir = System.getProperty(AVRO_PATH_KEY);
        if (avroDir == null || avroDir.isBlank()) {
            avroDir = resolveConfigValue(AVRO_PATH_KEY, DEFAULT_AVRO_PATH);
        }
        return avroDir + File.separator + tableName + AVRO_EXTENSION;
    }

    private static String resolveConfigValue(String key, String defaultValue) {
        String userDir = System.getProperty("user.dir", ".");
        File configFile = new File(userDir, "config.properties");
        if (configFile.exists()) {
            try {
                var props = new java.util.Properties();
                try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                    props.load(in);
                }
                String val = props.getProperty(key);
                if (val != null && !val.isBlank()) return val;
            } catch (IOException ignored) { }
        }
        return defaultValue;
    }

    // ─── Row <-> GenericRecord conversion ───────────────────────────

    static GenericRecord toRecord(Object[] row, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        List<Schema.Field> fields = schema.getFields();
        for (int i = 0; i < fields.size() && i < row.length; i++) {
            Schema.Field field = fields.get(i);
            Object value = row[i];
            record.put(field.name(), toAvroValue(value, field.schema()));
        }
        return record;
    }

    static Object[] fromRecord(GenericRecord record, List<String> columns,
                                 Map<String, Class<?>> columnTypes) {
        Object[] row = new Object[columns.size()];
        Schema schema = record.getSchema();
        for (int i = 0; i < columns.size(); i++) {
            String col = columns.get(i);
            Object avroValue = record.get(col);
            Class<?> targetType = resolveType(col, columnTypes);
            Schema.Field field = schema.getField(col);
            row[i] = fromAvroValue(avroValue, targetType, field != null ? field.schema() : null);
        }
        return row;
    }

    // ─── Value conversion helpers ───────────────────────────────────

    private static Object toAvroValue(Object value, Schema fieldSchema) {
        return AvroUnionHandler.wrapForWrite(value, fieldSchema, AvroRowStorage::toScalarAvroValue);
    }

    private static Object toScalarAvroValue(Object value, Schema fieldSchema) {
        if (value == null || fieldSchema == null) {
            return value;
        }
        if (fieldSchema.getLogicalType() != null) {
            return switch (fieldSchema.getLogicalType().getName()) {
                case "decimal" -> {
                    BigDecimal bd = (value instanceof BigDecimal) ? (BigDecimal) value : new BigDecimal(value.toString());
                    int avroScale = 18; // default from AvroTypeMapper
                    if (fieldSchema.getLogicalType() instanceof org.apache.avro.LogicalTypes.Decimal d) {
                        avroScale = d.getScale();
                    }
                    BigDecimal scaled = bd.setScale(avroScale, java.math.RoundingMode.HALF_UP);
                    yield ByteBuffer.wrap(scaled.unscaledValue().toByteArray());
                }
                case "date" -> {
                    LocalDate ld = (value instanceof LocalDate) ? (LocalDate) value : LocalDate.parse(value.toString());
                    yield (int) ld.toEpochDay();
                }
                case "timestamp-millis", "timestamp-micros" -> {
                    LocalDateTime ldt = (value instanceof LocalDateTime) ? (LocalDateTime) value
                            : LocalDateTime.parse(value.toString());
                    yield ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                }
                case "uuid" -> value.toString();
                default -> value;
            };
        }
        return switch (fieldSchema.getType()) {
            case STRING -> value.toString();
            case INT -> {
                if (value instanceof Number n) yield n.intValue();
                yield Integer.parseInt(value.toString());
            }
            case LONG -> {
                if (value instanceof Number n) yield n.longValue();
                yield Long.parseLong(value.toString());
            }
            case FLOAT -> {
                if (value instanceof Number n) yield n.floatValue();
                yield Float.parseFloat(value.toString());
            }
            case DOUBLE -> {
                if (value instanceof Number n) yield n.doubleValue();
                yield Double.parseDouble(value.toString());
            }
            case BOOLEAN -> {
                if (value instanceof Boolean b) yield b;
                yield Boolean.parseBoolean(value.toString());
            }
            case BYTES -> {
                if (value instanceof byte[] ba) yield ByteBuffer.wrap(ba);
                if (value instanceof ByteBuffer bb) yield bb;
                yield ByteBuffer.wrap(value.toString().getBytes());
            }
            // Prompt 75 complex types: arrays/maps/records/enums. Fields delegate
            // back to toAvroValue so nested elements can themselves be nullable
            // or complex.
            case ARRAY -> AvroArrayHandler.toAvroArray(value, fieldSchema, AvroRowStorage::toAvroValue);
            case MAP -> AvroMapHandler.toAvroMap(value, fieldSchema, AvroRowStorage::toAvroValue);
            case RECORD -> AvroRecordHandler.toAvroRecord(value, fieldSchema, AvroRowStorage::toAvroValue);
            case ENUM -> AvroEnumHandler.toAvroEnum(value, fieldSchema);
            default -> value;
        };
    }

    private static Object fromAvroValue(Object avroValue, Class<?> targetType, Schema fieldSchema) {
        if (avroValue == null || targetType == null) return avroValue;
        AvroUnionHandler.UnionReadValue unionValue =
                AvroUnionHandler.unwrapForRead(avroValue, fieldSchema);
        Schema base = unionValue.branchSchema();
        // Prompt 75 complex types: decode from the schema before the scalar
        // target-type switch, so complex values convert even when the Java
        // target type is a broad interface (List/Map).
        if (base != null && isComplexType(base.getType())) {
            return fromComplexAvroValue(avroValue, base);
        }
        if (avroValue instanceof ByteBuffer bb) {
            if (targetType == BigDecimal.class) {
                int scale = 18; // default
                if (base != null
                        && base.getLogicalType() instanceof org.apache.avro.LogicalTypes.Decimal d) {
                    scale = d.getScale();
                }
                return new BigDecimal(new BigInteger(bb.array()), scale).stripTrailingZeros();
            }
            if (targetType == byte[].class) {
                byte[] arr = new byte[bb.remaining()];
                bb.get(arr);
                bb.rewind();
                return arr;
            }
        }
        return switch (targetType.getSimpleName()) {
            case "String" -> avroValue.toString();
            case "Integer" -> {
                if (avroValue instanceof Integer i) yield i;
                if (avroValue instanceof Number n) yield n.intValue();
                yield Integer.parseInt(avroValue.toString());
            }
            case "Long" -> {
                if (avroValue instanceof Long l) yield l;
                if (avroValue instanceof Number n) yield n.longValue();
                yield Long.parseLong(avroValue.toString());
            }
            case "Short" -> {
                if (avroValue instanceof Number n) yield n.shortValue();
                yield Short.parseShort(avroValue.toString());
            }
            case "Byte" -> {
                if (avroValue instanceof Number n) yield n.byteValue();
                yield Byte.parseByte(avroValue.toString());
            }
            case "Float" -> {
                if (avroValue instanceof Float f) yield f;
                if (avroValue instanceof Number n) yield n.floatValue();
                yield Float.parseFloat(avroValue.toString());
            }
            case "Double" -> {
                if (avroValue instanceof Double d) yield d;
                if (avroValue instanceof Number n) yield n.doubleValue();
                yield Double.parseDouble(avroValue.toString());
            }
            case "Boolean" -> {
                if (avroValue instanceof Boolean b) yield b;
                yield Boolean.parseBoolean(avroValue.toString());
            }
            case "BigDecimal" -> {
                if (avroValue instanceof BigDecimal bd) yield bd;
                yield new BigDecimal(avroValue.toString());
            }
            case "LocalDate" -> {
                if (avroValue instanceof LocalDate ld) yield ld;
                if (avroValue instanceof Integer i) yield LocalDate.ofEpochDay(i);
                yield LocalDate.parse(avroValue.toString());
            }
            case "LocalDateTime" -> {
                if (avroValue instanceof LocalDateTime ldt) yield ldt;
                if (avroValue instanceof Long l) yield Instant.ofEpochMilli(l).atZone(ZoneOffset.UTC).toLocalDateTime();
                yield LocalDateTime.parse(avroValue.toString());
            }
            case "UUID" -> {
                if (avroValue instanceof UUID u) yield u;
                yield UUID.fromString(avroValue.toString());
            }
            case "Character" -> {
                String s = avroValue.toString();
                yield s.isEmpty() ? '\0' : s.charAt(0);
            }
            case "byte[]" -> {
                if (avroValue instanceof byte[] ba) yield ba;
                if (avroValue instanceof ByteBuffer bbuf) {
                    byte[] arr = new byte[bbuf.remaining()];
                    bbuf.get(arr);
                    yield arr;
                }
                yield avroValue.toString().getBytes();
            }
            default -> avroValue;
        };
    }

    private static boolean isComplexType(Schema.Type t) {
        return t == Schema.Type.ARRAY || t == Schema.Type.MAP || t == Schema.Type.RECORD || t == Schema.Type.ENUM;
    }

    private static Object fromComplexAvroValue(Object avroValue, Schema base) {
        return switch (base.getType()) {
            case ARRAY -> AvroArrayHandler.fromAvroArray(avroValue, base, AvroRowStorage::fromElementAvroValue);
            case MAP -> AvroMapHandler.fromAvroMap(avroValue, base, AvroRowStorage::fromElementAvroValue);
            case RECORD -> AvroRecordHandler.fromAvroRecord(avroValue, base, AvroRowStorage::fromElementAvroValue);
            case ENUM -> avroValue == null ? null : avroValue.toString();
            default -> avroValue;
        };
    }

    private static Object fromElementAvroValue(Object avroValue, Schema fieldSchema) {
        if (avroValue == null || fieldSchema == null) {
            return avroValue;
        }
        if (fieldSchema.getType() == Schema.Type.UNION) {
            AvroUnionHandler.UnionReadValue unwrapped =
                    AvroUnionHandler.unwrapForRead(avroValue, fieldSchema);
            return fromElementAvroValue(unwrapped.value(), unwrapped.branchSchema());
        }
        if (isComplexType(fieldSchema.getType())) {
            return fromComplexAvroValue(avroValue, fieldSchema);
        }
        Class<?> javaType = AvroTypeMapper.toJavaType(fieldSchema);
        return javaType == null ? avroValue : fromAvroValue(avroValue, javaType, fieldSchema);
    }

    private static Class<?> resolveType(String col, Map<String, Class<?>> columnTypes) {
        if (columnTypes instanceof TreeMap && ((TreeMap<?, ?>) columnTypes).comparator() != null) {
            return columnTypes.get(col);
        }
        for (Map.Entry<String, Class<?>> e : columnTypes.entrySet()) {
            if (e.getKey().equalsIgnoreCase(col)) {
                return e.getValue();
            }
        }
        return null;
    }

    // ─── Secondary indexes persistence ─────────────────────────────────

    private void saveSecondaryIndexes(File avroFile) throws IOException {
        if (secondaryIndexManager != null) {
            String basePath = avroFile.getPath().replace(".avro", "");
            secondaryIndexManager.saveToFile(basePath);
        }
    }

    private void loadSecondaryIndexes(File avroFile) throws IOException {
        if (secondaryIndexManager != null) {
            String basePath = avroFile.getPath().replace(".avro", "");
            try {
                AvroSecondaryIndexManager loaded = AvroSecondaryIndexManager.loadFromFile(basePath);
                if (loaded != null) {
                    this.secondaryIndexManager = loaded;
                    LOGGER.debug("AvroRowStorage {} loaded secondary indexes", tableName);
                }
            } catch (ClassNotFoundException e) {
                LOGGER.warn("Failed to load secondary indexes for {}: {}", tableName, e.getMessage());
            }
        }
    }

    // ─── Accessors ──────────────────────────────────────────────────
    public List<Object[]> getInternalRows() {
        return rows;
    }

}
