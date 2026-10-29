package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiFunction;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Executes an INSERT INTO statement: converts the raw values to the column
 * types and adds the resulting row to the table.
 *
 * @see Query
 */
class InsertQuery implements Query<Void> {
    private static final Logger LOGGER = Logger.getLogger(InsertQuery.class.getName());
    /**
     * Row count above which a multi-row INSERT is executed inside a deferred
     * storage bulk-update window (o2 CSV-7), mirroring
     * {@code diesel.bulkUpdate.threshold} on the UPDATE path.
     */
    private static final int BULK_INSERT_THRESHOLD =
            Integer.getInteger("diesel.bulkInsert.threshold", 100);
    private final List<String> columns;
    private final List<Object> values;
    private final List<List<Object>> valueRows;
    private long lastAffectedRows;

    /**
     * Returns the columns being inserted into.
     *
     * @return the unmodifiable column list
     */
    public List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    /**
     * Returns the number of rows the last {@link #execute} inserted (the
     * statement's row count: 1 for a single-tuple INSERT, the tuple count for
     * a multi-row VALUES batch), exposed for EXPLAIN ANALYZE metrics.
     *
     * @return the affected row count of the last execution
     */
    long getLastAffectedRows() {
        return lastAffectedRows;
    }

    /**
     * Creates an insert query for the given columns and values.
     *
     * @param columns the column names being inserted into
     * @param values  the values, one per column
     */
    public InsertQuery(List<String> columns, List<Object> values) {
        this.columns = columns;
        this.values = values;
        this.valueRows = null;
        LOGGER.log(Level.FINE, "Created InsertQuery with columns: {0}, values: {1}", new Object[]{columns, values});
    }

    private InsertQuery(List<String> columns, List<Object> values, List<List<Object>> valueRows) {
        this.columns = columns;
        this.values = values;
        this.valueRows = valueRows;
    }

    /**
     * Creates a multi-row insert for a {@code VALUES (...), (...), ...}
     * statement (o2 CSV-7).
     *
     * @param columns the column names being inserted into
     * @param rows    the value tuples, one list per inserted row
     * @return the multi-row insert query
     */
    static InsertQuery multiRow(List<String> columns, List<List<Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("Multi-row INSERT requires at least one VALUES tuple");
        }
        return new InsertQuery(columns, rows.get(0), rows);
    }

    /**
     * Converts and validates each value against the table's column types and
     * inserts the row (or every row of a multi-row VALUES batch, o2 CSV-7).
     *
     * @param table the table to insert into
     * @return null on success
     * @throws IllegalArgumentException if the column/value counts mismatch or
     *                                  a value is invalid for its column type
     * @throws IllegalStateException    if a unique constraint is violated
     */
    @Override
    public Void execute(Table table) {
        if (valueRows == null) {
            validateInput();
            Map<String, Object> row = buildRow(table);
            insertRow(table, row);
            return null;
        }
        return executeMultiRow(table);
    }

    /**
     * Executes a multi-row VALUES batch (o2 CSV-7). Batches at or above
     * {@link #BULK_INSERT_THRESHOLD} run inside a deferred storage
     * bulk-update window so the storage index manager performs one rebuild at
     * the end instead of a per-row position shift. When the batch supplies
     * the table's clustered key, the built rows are stably sorted by that key
     * first: a clustered table stores rows in key order, so the sort changes
     * no final row state, but it turns mid-table inserts into tail appends —
     * both on the engine's row mirror ({@code ArrayList.add} shifts) and on
     * the storage side. Sequence-based primary keys are never part of the
     * column list (the parser rejects them), so their assignment order is
     * unaffected; a non-PK sequence column would be assigned in clustered-key
     * order rather than VALUES order on large batches (documented deviation —
     * small batches keep strict VALUES order). A failing tuple leaves the
     * earlier tuples of the batch inserted (no statement-level rollback
     * outside an MVCC transaction).
     */
    private Void executeMultiRow(Table table) {
        List<Map<String, Object>> builtRows = new ArrayList<>(valueRows.size());
        for (List<Object> rowValues : valueRows) {
            if (columns.size() != rowValues.size()) {
                throw new IllegalArgumentException("Column and value counts mismatch");
            }
            builtRows.add(buildRow(table, rowValues));
        }

        if (builtRows.size() >= BULK_INSERT_THRESHOLD) {
            String clusteredColumn = table.getClusteredIndexColumn();
            if (clusteredColumn != null && !builtRows.isEmpty()
                    && builtRows.get(0).containsKey(clusteredColumn)) {
                sortByPrimaryKey(builtRows, clusteredColumn);
            }
            table.beginBulkUpdate();
            try {
                for (Map<String, Object> row : builtRows) {
                    insertRow(table, row);
                }
            } finally {
                table.endBulkUpdate();
            }
        } else {
            for (Map<String, Object> row : builtRows) {
                insertRow(table, row);
            }
        }
        lastAffectedRows = builtRows.size();
        LOGGER.log(Level.FINE, "Inserted {0} rows into table {1}",
                new Object[]{builtRows.size(), table.getName()});
        return null;
    }

    /** Stable-sorts the built rows by their clustered-key value (natural order). */
    @SuppressWarnings("unchecked")
    private static void sortByPrimaryKey(List<Map<String, Object>> builtRows, String clusteredColumn) {
        builtRows.sort(Comparator.comparing(
                row -> (Comparable<Object>) row.get(clusteredColumn),
                Comparator.nullsLast(Comparator.naturalOrder())));
    }

    private void validateInput() {
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("Column and value counts mismatch");
        }
    }

    private Map<String, Object> buildRow(Table table) {
        return buildRow(table, values);
    }

    private Map<String, Object> buildRow(Table table, List<Object> rowValues) {
        Map<String, Object> row = new HashMap<>();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            Object value = rowValues.get(i);
            Class<?> expectedType = columnTypes.get(column);
            if (expectedType == null) {
                throw new IllegalArgumentException(ErrorMessages.UNKNOWN_COLUMN_PREFIX + column);
            }
            row.put(column, convertValue(value, column, expectedType));
        }
        return row;
    }

    private static final Map<Class<?>, BiFunction<Object, String, Object>> CONVERTERS;

    static {
        Map<Class<?>, BiFunction<Object, String, Object>> converters = new HashMap<>();
        converters.put(Integer.class, InsertQuery::parseInteger);
        converters.put(Long.class, InsertQuery::parseLong);
        converters.put(Short.class, InsertQuery::parseShort);
        converters.put(Byte.class, InsertQuery::parseByte);
        converters.put(BigDecimal.class, InsertQuery::parseBigDecimal);
        converters.put(Float.class, InsertQuery::parseFloat);
        converters.put(Double.class, InsertQuery::parseDouble);
        converters.put(Character.class, InsertQuery::parseCharacter);
        converters.put(UUID.class, InsertQuery::parseUUID);
        converters.put(String.class, (value, column) -> value.toString());
        CONVERTERS = Collections.unmodifiableMap(converters);
    }

    private Object convertValue(Object value, String column, Class<?> expectedType) {
        if (value == null) {
            return null;
        }
        if (expectedType.isInstance(value)) {
            return value;
        }
        BiFunction<Object, String, Object> converter = CONVERTERS.get(expectedType);
        if (converter != null) {
            return converter.apply(value, column);
        }
        if (expectedType == Boolean.class) {
            throwInvalid(value, column, "BOOLEAN");
        }
        if (expectedType == LocalDate.class) {
            throwInvalid(value, column, "DATE");
        }
        if (expectedType == LocalDateTime.class) {
            throwInvalid(value, column, "DATETIME or DATETIME_MS");
        }
        return value;
    }

    private static int parseInteger(Object value, String column) {
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected INTEGER", value, column));
        }
    }

    private static long parseLong(Object value, String column) {
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected LONG", value, column));
        }
    }

    private static short parseShort(Object value, String column) {
        try {
            return Short.parseShort(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected SHORT", value, column));
        }
    }

    private static byte parseByte(Object value, String column) {
        try {
            return Byte.parseByte(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected BYTE", value, column));
        }
    }

    private static float parseFloat(Object value, String column) {
        try {
            return Float.parseFloat(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected FLOAT", value, column));
        }
    }

    private static double parseDouble(Object value, String column) {
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected DOUBLE", value, column));
        }
    }

    private static BigDecimal parseBigDecimal(Object value, String column) {
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected BIGDECIMAL", value, column));
        }
    }

    private static char parseCharacter(Object value, String column) {
        if (value instanceof Character c) {
            return c;
        }
        if (value.toString().length() != 1) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected CHARACTER", value, column));
        }
        return value.toString().charAt(0);
    }

    private static UUID parseUUID(Object value, String column) {
        if (value instanceof UUID uuid) {
            return uuid;
        }
        try {
            return UUID.fromString(value.toString());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected UUID", value, column));
        }
    }

    private Object throwInvalid(Object value, String column, String typeName) {
        throw new IllegalArgumentException(
                String.format("Invalid value '%s' for column %s: expected %s", value, column, typeName));
    }

    private void insertRow(Table table, Map<String, Object> row) {
        try {
            // MVCC: the dispatch installed the caller's context; an explicit
            // (non-batch) transaction inserts in place with version metadata and
            // an undo record, everything else uses the plain fast path.
            MvccReadContext.Context context = MvccReadContext.get();
            Transaction transaction = context == null ? null : context.getTransaction();
            if (transaction != null && transaction.isActive() && !context.isBatch()) {
                int rowIndex = table.addRowWithMVCC(row, transaction.getTxid());
                transaction.getUndoLog().addUndoRecord(new UndoLog.InsertUndo(table.getName(), rowIndex));
                transaction.noteModifiedRow(table.getName(), rowIndex);
                appendInsertWal(table, transaction.getTxid(), rowIndex, row);
            } else {
                table.addRow(row);
            }
            
            lastAffectedRows = 1;
            LOGGER.log(Level.FINE, "Inserted row into table {0}: {1}", new Object[]{table.getName(), row});
        } catch (IllegalStateException e) {
            LOGGER.log(Level.SEVERE, "Insert failed due to unique constraint violation: {0}", e.getMessage());
            throw new IllegalStateException("Insert failed: " + e.getMessage(), e);
        }
    }

    /**
     * Appends the logical INSERT record to the WAL when enabled (prompt4
     * #19): the after-image carries the table, row index and inserted values
     * so the undo phase can hide the row again after a crash.
     */
    private void appendInsertWal(Table table, long txid, int rowIndex, Map<String, Object> row) {
        Database database = table.getDatabase();
        if (database == null || !database.isWALEnabled()) {
            return;
        }
        try {
            byte[] after = diesel.wal.DmlPayload.serialize(table.getName(), rowIndex, row);
            database.appendWalDml(txid, diesel.wal.WALOpcode.INSERT, null, after);
        } catch (java.io.IOException e) {
            LOGGER.log(Level.WARNING, "WAL INSERT payload failed (statement continues): " + e.getMessage());
        }
    }
}
