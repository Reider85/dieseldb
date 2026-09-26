package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Primary-key index for AVRO-backed tables (Prompt 85).
 *
 * <p>Maintains a {@link TreeMap}&lt;Object, Integer&gt; mapping primary-key
 * values to their row indices for O(log n) lookups. An LRU page cache
 * accelerates repeated access to the same logical page of the index.
 *
 * <p>Index maintenance is driven by {@link AvroRowStorage} which calls
 * {@link #insert}, {@link #update}, {@link #delete} and
 * {@link #buildIndex} on every mutation. The index is rebuilt from scratch
 * when a sidecar file is stale or missing.
 *
 * <p>Configuration (resolved per call: system property &rarr;
 * {@code config.properties} &rarr; defaults):
 * <ul>
 *   <li>{@code avro.index.enabled} &mdash; master switch (default {@code true})</li>
 *   <li>{@code avro.index.cache.size} &mdash; LRU page cache capacity in
 *       logical pages (default 256)</li>
 *   <li>{@code avro.index.page.size} &mdash; rows per logical page
 *       (default 64)</li>
 * </ul>
 *
 * @since Prompt 85
 */
public class AvroPrimaryKeyIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroPrimaryKeyIndex.class);

    // ─── Config keys ────────────────────────────────────────────────
    public static final String ENABLED_KEY = "avro.index.enabled";
    public static final String CACHE_SIZE_KEY = "avro.index.cache.size";
    public static final String PAGE_SIZE_KEY = "avro.index.page.size";
    static final String CONFIG_FILE_KEY = "avro.index.config.file";

    public static final boolean DEFAULT_ENABLED = true;
    public static final int DEFAULT_CACHE_SIZE = 256;
    public static final int DEFAULT_PAGE_SIZE = 64;

    // ─── State ──────────────────────────────────────────────────────
    private static final Comparator<Object> KEY_COMPARATOR = AvroPrimaryKeyIndex::compareKeys;
    private final TreeMap<Object, Integer> primaryKeyMap = new TreeMap<>(KEY_COMPARATOR);
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private int pkColumnIndex = -1;
    private final int pageSize;
    private final int cacheCapacity;
    private final boolean configEnabled;

    /** LRU page cache: pageIndex &rarr; snapshot of row data for that page. */
    private final LinkedHashMap<Integer, Object[]> pageCache;

    /** Number of lookups served from cache (metrics). */
    private long cacheHits;
    /** Number of lookups that missed the cache (metrics). */
    private long cacheMisses;

    private AvroPrimaryKeyIndex(List<String> columns, Map<String, Class<?>> columnTypes,
                                 boolean enabled, int cacheCapacity, int pageSize) {
        this.columns = List.copyOf(columns);
        this.columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.columnTypes.putAll(columnTypes);
        this.configEnabled = enabled;
        this.pageSize = pageSize <= 0 ? DEFAULT_PAGE_SIZE : pageSize;
        this.cacheCapacity = cacheCapacity <= 0 ? DEFAULT_CACHE_SIZE : cacheCapacity;
        this.pageCache = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Object[]> eldest) {
                return size() > AvroPrimaryKeyIndex.this.cacheCapacity;
            }
        };
    }

    // ─── Factory ────────────────────────────────────────────────────

    /**
     * Creates a new primary-key index with resolved configuration.
     *
     * @param columns     ordered column names of the table
     * @param columnTypes column name to Java-type mapping
     * @return a new index instance (may be disabled if
     *         {@code avro.index.enabled=false})
     */
    public static AvroPrimaryKeyIndex create(List<String> columns,
                                              Map<String, Class<?>> columnTypes) {
        boolean enabled = getBoolean(ENABLED_KEY, DEFAULT_ENABLED);
        int cacheSize = getInt(CACHE_SIZE_KEY, DEFAULT_CACHE_SIZE);
        int pageSize = getInt(PAGE_SIZE_KEY, DEFAULT_PAGE_SIZE);
        return new AvroPrimaryKeyIndex(columns, columnTypes, enabled, cacheSize, pageSize);
    }

    /**
     * Creates an index with a test-config override file.
     */
    public static AvroPrimaryKeyIndex create(List<String> columns,
                                              Map<String, Class<?>> columnTypes,
                                              String configFilePath) {
        String prev = System.getProperty(CONFIG_FILE_KEY);
        try {
            if (configFilePath != null) {
                System.setProperty(CONFIG_FILE_KEY, configFilePath);
            }
            return create(columns, columnTypes);
        } finally {
            if (prev == null) {
                System.clearProperty(CONFIG_FILE_KEY);
            } else {
                System.setProperty(CONFIG_FILE_KEY, prev);
            }
        }
    }

    // ─── PK column setup ───────────────────────────────────────────

    /**
     * Sets the primary-key column by name (case-insensitive) and rebuilds
     * the index from the given rows.
     *
     * @param pkColumn the primary-key column name
     * @param rows     the current in-memory rows (compact Object[] form)
     */
    public void setPrimaryKeyColumn(String pkColumn, List<Object[]> rows) {
        this.pkColumnIndex = findColumnIndex(pkColumn);
        buildIndex(rows);
    }

    private int findColumnIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    // ─── Core operations ───────────────────────────────────────────

    /**
     * Returns the row index for the given primary key, or {@code null}
     * if no such key exists. O(log n) TreeMap lookup.
     */
    public Integer lookup(Object key) {
        if (pkColumnIndex < 0 || key == null) return null;
        return primaryKeyMap.get(key);
    }

    /**
     * Returns all row indices whose primary keys fall within the given
     * range (inclusive on both ends). O(log n + k) where k is the number
     * of matching entries.
     */
    public List<Integer> rangeSearch(Object fromKey, Object toKey) {
        if (pkColumnIndex < 0) return List.of();
        NavigableMap<Object, Integer> sub = primaryKeyMap.subMap(fromKey, true, toKey, true);
        return new ArrayList<>(sub.values());
    }

    /**
     * Returns the number of entries in the primary-key index.
     */
    public int size() {
        return primaryKeyMap.size();
    }

    /**
     * Returns whether the index has a primary-key column set and is
     * enabled via configuration.
     */
    public boolean isEnabled() {
        return configEnabled && pkColumnIndex >= 0;
    }

    /**
     * Returns the index of the primary-key column, or {@code -1} if not set.
     */
    public int getPkColumnIndex() {
        return pkColumnIndex;
    }

    public Class<?> getKeyType() {
        if (pkColumnIndex < 0 || pkColumnIndex >= columns.size()) {
            return null;
        }
        return columnTypes.get(columns.get(pkColumnIndex));
    }

    /**
     * Inserts a new row into the index.
     *
     * @param row     the compact row array
     * @param rowIndex the physical row index
     * @throws IllegalArgumentException if the primary key already exists
     */
    public void insert(Object[] row, int rowIndex) {
        if (pkColumnIndex < 0) return;
        Object key = extractKey(row);
        if (key == null) return;
        validateInsert(row, rowIndex);
        primaryKeyMap.put(key, rowIndex);
        invalidatePageCache();
    }

    public void validateInsert(Object[] row, int rowIndex) {
        if (pkColumnIndex < 0) return;
        Object key = extractKey(row);
        if (key == null) return;
        Integer existing = primaryKeyMap.get(key);
        if (existing != null) {
            throw new IllegalArgumentException(
                    "Duplicate primary key: " + key + " at index " + existing
                    + ", cannot insert at " + rowIndex);
        }
    }

    /**
     * Updates an existing row in the index. Removes the old key and
     * inserts the new one.
     *
     * @param oldRow   the previous row data
     * @param rowIndex the physical row index (unchanged)
     * @param newRow   the new row data
     */
    public void update(Object[] oldRow, int rowIndex, Object[] newRow) {
        if (pkColumnIndex < 0) return;
        validateUpdate(oldRow, rowIndex, newRow);
        Object oldKey = extractKey(oldRow);
        Object newKey = extractKey(newRow);
        if (oldKey != null) {
            primaryKeyMap.remove(oldKey);
        }
        if (newKey != null) {
            primaryKeyMap.put(newKey, rowIndex);
        }
        invalidatePageCache();
    }

    /**
     * Validates that updating a row won't create a duplicate primary key.
     * Throws if the new key already maps to a different row.
     */
    public void validateUpdate(Object[] oldRow, int rowIndex, Object[] newRow) {
        if (pkColumnIndex < 0) return;
        Object oldKey = extractKey(oldRow);
        Object newKey = extractKey(newRow);
        if (newKey == null) return;
        Integer existing = primaryKeyMap.get(newKey);
        if (existing != null) {
            boolean sameKey = keysEqual(oldKey, newKey);
            boolean sameRow = existing == rowIndex;
            if (!sameKey || !sameRow) {
                throw new IllegalArgumentException(
                        "Duplicate primary key: " + newKey + " at index " + existing
                        + ", cannot update row " + rowIndex);
            }
        }
    }

    /**
     * Removes a row from the index by its primary key.
     *
     * @param row     the row being deleted
     * @param rowIndex the physical row index being deleted
     */
    public void delete(Object[] row, int rowIndex) {
        if (pkColumnIndex < 0) return;
        Object key = extractKey(row);
        if (key != null) {
            primaryKeyMap.remove(key);
        }
        invalidatePageCache();
    }

    public void shiftPositions(int rowIndex, int delta) {
        if (delta == 0 || pkColumnIndex < 0) return;
        primaryKeyMap.replaceAll((key, position) -> position >= rowIndex ? position + delta : position);
        invalidatePageCache();
    }

    /**
     * Rebuilds the entire index from the given rows. Called after bulk
     * operations and on load.
     *
     * @param rows the current in-memory rows
     */
    public void buildIndex(List<Object[]> rows) {
        primaryKeyMap.clear();
        pageCache.clear();
        cacheHits = 0;
        cacheMisses = 0;
        if (pkColumnIndex < 0) return;
        for (int i = 0; i < rows.size(); i++) {
            Object[] row = rows.get(i);
            Object key = extractKey(row);
            if (key != null) {
                primaryKeyMap.put(key, i);
            }
        }
        LOGGER.debug("AvroPrimaryKeyIndex built: {} entries for column index {}",
                primaryKeyMap.size(), pkColumnIndex);
    }

    public boolean restoreFromSidecar(Map<Object, Integer> loaded, List<Object[]> currentRows) {
        if (pkColumnIndex < 0 || loaded == null || currentRows == null) {
            return false;
        }
        int expectedCount = 0;
        for (Object[] row : currentRows) {
            if (extractKey(row) != null) {
                expectedCount++;
            }
        }
        if (loaded.size() != expectedCount) {
            return false;
        }
        TreeMap<Object, Integer> restored = new TreeMap<>(KEY_COMPARATOR);
        try {
            for (Map.Entry<Object, Integer> entry : loaded.entrySet()) {
                Object key = entry.getKey();
                Integer rowIndex = entry.getValue();
                if (key == null || rowIndex == null || rowIndex < 0 || rowIndex >= currentRows.size()) {
                    return false;
                }
                Object expectedKey = extractKey(currentRows.get(rowIndex));
                Class<?> keyType = getKeyType();
                if (expectedKey == null
                        || (keyType != null && keyType != Object.class && !isInstanceOfBoxed(keyType, key))
                        || !keysEqual(key, expectedKey)
                        || restored.put(key, rowIndex) != null) {
                    return false;
                }
            }
        } catch (RuntimeException e) {
            return false;
        }
        if (restored.size() != expectedCount) {
            return false;
        }
        primaryKeyMap.clear();
        primaryKeyMap.putAll(restored);
        pageCache.clear();
        cacheHits = 0;
        cacheMisses = 0;
        return true;
    }

    private static boolean isInstanceOfBoxed(Class<?> type, Object value) {
        if (!type.isPrimitive()) {
            return type.isInstance(value);
        }
        Class<?> boxed = switch (type.getName()) {
            case "int" -> Integer.class;
            case "long" -> Long.class;
            case "short" -> Short.class;
            case "byte" -> Byte.class;
            case "float" -> Float.class;
            case "double" -> Double.class;
            case "boolean" -> Boolean.class;
            case "char" -> Character.class;
            default -> type;
        };
        return boxed.isInstance(value);
    }

    private static int compareKeys(Object left, Object right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            try {
                return new BigDecimal(leftNumber.toString()).compareTo(new BigDecimal(rightNumber.toString()));
            } catch (NumberFormatException ignored) {
                return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
            }
        }
        if (left instanceof byte[] leftBytes && right instanceof byte[] rightBytes) {
            return Arrays.compareUnsigned(leftBytes, rightBytes);
        }
        if (left instanceof ByteBuffer leftBuffer && right instanceof ByteBuffer rightBuffer) {
            return compareBuffers(leftBuffer, rightBuffer);
        }
        if (left instanceof Comparable<?> && left.getClass().isInstance(right)) {
            try {
                @SuppressWarnings("unchecked")
                Comparable<Object> comparable = (Comparable<Object>) left;
                return comparable.compareTo(right);
            } catch (RuntimeException ignored) {
            }
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    private static int compareBuffers(ByteBuffer left, ByteBuffer right) {
        int length = Math.min(left.remaining(), right.remaining());
        for (int i = 0; i < length; i++) {
            int comparison = Integer.compare(left.get(left.position() + i) & 0xff,
                    right.get(right.position() + i) & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.remaining(), right.remaining());
    }

    private static boolean keysEqual(Object left, Object right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof BigDecimal leftDecimal && right instanceof BigDecimal rightDecimal) {
            return leftDecimal.compareTo(rightDecimal) == 0;
        }
        if (left instanceof Number && right instanceof Number) {
            try {
                return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString())) == 0;
            } catch (NumberFormatException ignored) {
                return Objects.equals(left, right);
            }
        }
        if (left instanceof byte[] leftBytes && right instanceof byte[] rightBytes) {
            return Arrays.equals(leftBytes, rightBytes);
        }
        if (left instanceof ByteBuffer leftBuffer && right instanceof ByteBuffer rightBuffer) {
            return compareBuffers(leftBuffer, rightBuffer) == 0;
        }
        return Objects.equals(left, right);
    }

    // ─── Page cache ────────────────────────────────────────────────

    /**
     * Returns the cached row data for the given logical page index.
     * Populates the cache on miss by reading from the supplied row list.
     *
     * @param pageIndex the logical page number (0-based)
     * @param rows      the full in-memory row list (fallback on cache miss)
     * @return the row snapshot for the page (never {@code null})
     */
    public Object[] getPage(int pageIndex, List<Object[]> rows) {
        Object[] cached = pageCache.get(pageIndex);
        if (cached != null) {
            cacheHits++;
            return cached;
        }
        cacheMisses++;
        int start = pageIndex * pageSize;
        int end = Math.min(start + pageSize, rows.size());
        if (start >= rows.size()) {
            return new Object[0];
        }
        Object[] page = rows.subList(start, end).toArray(Object[][]::new);
        Object[] flat = new Object[end - start];
        for (int i = 0; i < flat.length; i++) {
            flat[i] = page[i];
        }
        pageCache.put(pageIndex, flat);
        return flat;
    }

    /**
     * Returns the total number of logical pages based on the given row count.
     */
    public int pageCount(int totalRows) {
        return (totalRows + pageSize - 1) / pageSize;
    }

    /**
     * Invalidates the entire page cache.
     */
    public void invalidatePageCache() {
        pageCache.clear();
    }

    // ─── Metrics ───────────────────────────────────────────────────

    /** Returns cache hit count since last build. */
    public long getCacheHits() {
        return cacheHits;
    }

    /** Returns cache miss count since last build. */
    public long getCacheMisses() {
        return cacheMisses;
    }

    /** Returns current page cache size. */
    public int getCachedPageCount() {
        return pageCache.size();
    }

    // ─── Persistence ───────────────────────────────────────────────

    /**
     * Returns the sidecar file path for this index.
     *
     * @param avroFilePath the path to the .avro data file
     */
    public static Path sidecarPath(Path avroFilePath) {
        return avroFilePath.resolveSibling(
                avroFilePath.getFileName() + ".pki");
    }

    /**
     * Saves the primary-key index to a sidecar file using
     * {@link AtomicFileWriter} for crash safety.
     *
     * @param sidecarPath the target sidecar path
     * @param dataFileSize the data file size in bytes (stamp for validation)
     * @param dataFileModified the data file last-modified millis (stamp)
     */
    public void saveToSidecar(Path sidecarPath, long dataFileSize, long dataFileModified) throws IOException {
        File parent = sidecarPath.getParent() != null ? sidecarPath.getParent().toFile() : null;
        if (parent != null) parent.mkdirs();
        Path tmp = sidecarPath.resolveSibling(sidecarPath.getFileName() + ".tmp");
        try (BufferedWriter w = Files.newBufferedWriter(tmp)) {
            w.write("PK_COLUMN_INDEX=" + pkColumnIndex);
            w.newLine();
            w.write("PK_COLUMN_NAME=" + (pkColumnIndex >= 0 && pkColumnIndex < columns.size()
                    ? columns.get(pkColumnIndex) : ""));
            w.newLine();
            Class<?> keyType = getKeyType();
            w.write("PK_KEY_TYPE=" + (keyType != null ? keyType.getName() : ""));
            w.newLine();
            w.write("ENTRY_COUNT=" + primaryKeyMap.size());
            w.newLine();
            w.write("DATA_FILE_SIZE=" + dataFileSize);
            w.newLine();
            w.write("DATA_FILE_MODIFIED=" + dataFileModified);
            w.newLine();
            w.write("PAGE_SIZE=" + pageSize);
            w.newLine();
            w.write("---");
            w.newLine();
            for (Map.Entry<Object, Integer> entry : primaryKeyMap.entrySet()) {
                w.write(serializeKey(entry.getKey()));
                w.write('\t');
                w.write(String.valueOf(entry.getValue()));
                w.newLine();
            }
        }
        try {
            Files.move(tmp, sidecarPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, sidecarPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Loads the primary-key index from a sidecar file. Returns
     * {@code null} when the sidecar is missing, stale or corrupt,
     * signalling that the caller should rebuild from the rows.
     *
     * @param sidecarPath  the sidecar file path
     * @param dataFileSize the current data file size (must match the stamp)
     * @param dataFileModified the current data file last-modified millis
     * @return the loaded key&rarr;index map, or {@code null} on mismatch
     */
    public static Map<Object, Integer> loadFromSidecar(Path sidecarPath,
                                                        long dataFileSize,
                                                        long dataFileModified) {
        return loadFromSidecar(sidecarPath, dataFileSize, dataFileModified, null);
    }

    public static Map<Object, Integer> loadFromSidecar(Path sidecarPath,
                                                        long dataFileSize,
                                                        long dataFileModified,
                                                        Class<?> keyType) {
        if (!Files.exists(sidecarPath)) return null;
        try (BufferedReader r = Files.newBufferedReader(sidecarPath)) {
            String line;
            int expectedCount = -1;
            long loadedFileSize = -2;
            long loadedFileModified = -2;
            String loadedKeyTypeName = null;
            while ((line = r.readLine()) != null) {
                if (line.equals("---")) break;
                if (line.startsWith("PK_KEY_TYPE=")) {
                    loadedKeyTypeName = line.substring(12);
                } else if (line.startsWith("ENTRY_COUNT=")) {
                    expectedCount = Integer.parseInt(line.substring(12));
                } else if (line.startsWith("DATA_FILE_SIZE=")) {
                    loadedFileSize = Long.parseLong(line.substring(15));
                } else if (line.startsWith("DATA_FILE_MODIFIED=")) {
                    loadedFileModified = Long.parseLong(line.substring(19));
                }
            }
            if (loadedFileSize != dataFileSize || loadedFileModified != dataFileModified) {
                LOGGER.debug("AvroPrimaryKeyIndex sidecar stamp mismatch: file={}, mod={}",
                        loadedFileSize != dataFileSize ? "size" : "modified");
                return null;
            }
            Class<?> resolvedKeyType = keyType;
            if (resolvedKeyType == null && loadedKeyTypeName != null && !loadedKeyTypeName.isBlank()) {
                try {
                    resolvedKeyType = Class.forName(loadedKeyTypeName);
                } catch (ClassNotFoundException ignored) {
                    resolvedKeyType = null;
                }
            }
            Map<Object, Integer> map = new TreeMap<>(KEY_COMPARATOR);
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) continue;
                int tab = line.indexOf('\t');
                if (tab < 0 || tab == line.length() - 1) {
                    return null;
                }
                String keyStr = line.substring(0, tab);
                int idx = Integer.parseInt(line.substring(tab + 1));
                if (idx < 0) {
                    return null;
                }
                Object key = deserializeKey(keyStr, resolvedKeyType);
                if (key == null || map.put(key, idx) != null) {
                    return null;
                }
            }
            if (expectedCount >= 0 && map.size() != expectedCount) {
                LOGGER.warn("AvroPrimaryKeyIndex sidecar entry count mismatch: expected {}, got {}",
                        expectedCount, map.size());
                return null;
            }
            return map;
        } catch (Exception e) {
            LOGGER.debug("AvroPrimaryKeyIndex sidecar load failed: {}", e.getMessage());
            return null;
        }
    }

    // ─── Internal helpers ──────────────────────────────────────────

    private Object extractKey(Object[] row) {
        if (pkColumnIndex < 0 || row == null || pkColumnIndex >= row.length) return null;
        return row[pkColumnIndex];
    }

    private static String serializeKey(Object key) {
        if (key == null) return "\\N";
        if (key instanceof byte[] bytes) {
            return "B:" + Base64.getEncoder().encodeToString(bytes);
        }
        if (key instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return "B:" + Base64.getEncoder().encodeToString(bytes);
        }
        return key.toString().replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n");
    }

    private static Object deserializeKey(String s, Class<?> targetType) {
        if ("\\N".equals(s)) return null;
        if (targetType != null && targetType.isPrimitive()) {
            targetType = switch (targetType.getName()) {
                case "int" -> Integer.class;
                case "long" -> Long.class;
                case "short" -> Short.class;
                case "byte" -> Byte.class;
                case "float" -> Float.class;
                case "double" -> Double.class;
                case "boolean" -> Boolean.class;
                case "char" -> Character.class;
                default -> targetType;
            };
        }
        String raw = unescapeKey(s);
        if (targetType == null || targetType == Object.class) {
            return deserializeKey(raw);
        }
        if (targetType == String.class || targetType == CharSequence.class) {
            return raw;
        }
        if (targetType == Integer.class) {
            return Integer.valueOf(raw);
        }
        if (targetType == Long.class) {
            return Long.valueOf(raw);
        }
        if (targetType == Short.class) {
            return Short.valueOf(raw);
        }
        if (targetType == Byte.class) {
            return Byte.valueOf(raw);
        }
        if (targetType == Float.class) {
            return Float.valueOf(raw);
        }
        if (targetType == Double.class) {
            return Double.valueOf(raw);
        }
        if (targetType == Boolean.class) {
            if (!"true".equalsIgnoreCase(raw) && !"false".equalsIgnoreCase(raw)) {
                throw new IllegalArgumentException("Invalid boolean key: " + raw);
            }
            return Boolean.valueOf(raw);
        }
        if (targetType == Character.class) {
            if (raw.length() != 1) {
                throw new IllegalArgumentException("Invalid character key: " + raw);
            }
            return raw.charAt(0);
        }
        if (targetType == BigDecimal.class) {
            return new BigDecimal(raw);
        }
        if (targetType == BigInteger.class) {
            return new BigInteger(raw);
        }
        if (targetType == Number.class) {
            return new BigDecimal(raw);
        }
        if (targetType == LocalDate.class) {
            return LocalDate.parse(raw);
        }
        if (targetType == LocalDateTime.class) {
            return LocalDateTime.parse(raw);
        }
        if (targetType == Instant.class) {
            return Instant.parse(raw);
        }
        if (targetType == UUID.class) {
            return UUID.fromString(raw);
        }
        if (targetType == byte[].class || targetType == ByteBuffer.class) {
            String encoded = raw.startsWith("B:") ? raw.substring(2) : raw;
            byte[] bytes = Base64.getDecoder().decode(encoded);
            return targetType == byte[].class ? bytes : ByteBuffer.wrap(bytes);
        }
        return raw;
    }

    private static String unescapeKey(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(++i);
                switch (next) {
                    case '\\' -> sb.append('\\');
                    case 't' -> sb.append('\t');
                    case 'n' -> sb.append('\n');
                    default -> { sb.append('\\'); sb.append(next); }
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static Object deserializeKey(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {}
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ignored) {}
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException ignored) {}
        if ("true".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s)) {
            return Boolean.valueOf(s);
        }
        return s;
    }

    // ─── Config resolution ─────────────────────────────────────────

    private static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        return Boolean.parseBoolean(raw.trim());
    }

    private static int getInt(String key, int defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static String getString(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null) {
            return systemValue;
        }
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop;
    }

    private static Properties rootProps() {
        Properties props = new Properties();
        String override = System.getProperty(CONFIG_FILE_KEY);
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty("user.dir", "."), "config.properties");
        if (configFile.exists()) {
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
                LOGGER.debug(AvroMetricConstants.MSG_CONFIG_READ_FAILED, ignored.getMessage());
            }
        }
        return props;
    }

    @Override
    public String toString() {
        return "AvroPrimaryKeyIndex{pkColumnIndex=" + pkColumnIndex
                + ", entries=" + primaryKeyMap.size()
                + ", pageSize=" + pageSize
                + ", cacheCapacity=" + cacheCapacity
                + ", cachedPages=" + pageCache.size()
                + ", hits=" + cacheHits
                + ", misses=" + cacheMisses + '}';
    }
}
