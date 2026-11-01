package com.diesel.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * JDBC {@link ResultSet} implementation over the rows collected by
 * {@link LibpqWireClient.QueryResult}. Values arrive as UTF-8 strings from the
 * server; typed getters parse on demand.
 */
public class DieselResultSet extends StubResultSet {

    private final Statement statement;
    private final String[] columnNames;
    private final int[] columnTypeOids;
    private final List<String[]> rows;
    private int currentRow = -1;
    private boolean wasNull;
    private boolean closed;

    DieselResultSet(Statement statement, LibpqWireClient.QueryResult result) {
        this.statement = statement;
        this.columnNames = result.columnNames != null ? result.columnNames : new String[0];
        this.columnTypeOids = result.columnTypeOids != null ? result.columnTypeOids : new int[0];
        this.rows = result.rows;
    }

    // ---- navigation ----

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (currentRow + 1 < rows.size()) {
            currentRow++;
            return true;
        }
        currentRow = rows.size();
        return false;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return !rows.isEmpty() && currentRow < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return !rows.isEmpty() && currentRow >= rows.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return currentRow == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return currentRow == rows.size() - 1 && currentRow >= 0;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        currentRow = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        currentRow = rows.size();
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        if (rows.isEmpty()) {
            return false;
        }
        currentRow = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        if (rows.isEmpty()) {
            return false;
        }
        currentRow = rows.size() - 1;
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return currentRow >= 0 && currentRow < rows.size() ? currentRow + 1 : 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        if (row < 0) {
            row = rows.size() + row + 1;
        }
        if (row < 1 || row > rows.size()) {
            currentRow = row < 1 ? -1 : rows.size();
            return false;
        }
        currentRow = row - 1;
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        return absolute(getRow() + rows);
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        if (currentRow > 0) {
            currentRow--;
            return true;
        }
        if (currentRow == 0) {
            currentRow = -1;
        }
        return false;
    }

    // ---- typed getters by column index ----

    private String rawValue(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRow < 0 || currentRow >= this.rows.size()) {
            throw new SQLException("No current row");
        }
        if (columnIndex < 1 || columnIndex > columnNames.length) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
        String v = rows.get(currentRow)[columnIndex - 1];
        wasNull = (v == null);
        return v;
    }

    private String rawValue(String columnLabel) throws SQLException {
        int idx = findColumnIndex(columnLabel);
        return rawValue(idx);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return rawValue(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return rawValue(columnLabel);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        if (v == null) {
            return false;
        }
        return v.equalsIgnoreCase("t") || v.equalsIgnoreCase("true") || v.equals("1");
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumnIndex(columnLabel));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? 0 : (byte) Integer.parseInt(v.trim());
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumnIndex(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? 0 : Short.parseShort(v.trim());
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumnIndex(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? 0 : Integer.parseInt(v.trim());
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumnIndex(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? 0L : Long.parseLong(v.trim());
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumnIndex(columnLabel));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? 0f : Float.parseFloat(v.trim());
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumnIndex(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? 0d : Double.parseDouble(v.trim());
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? null : new BigDecimal(v.trim());
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bd = getBigDecimal(columnIndex);
        return bd == null ? null : bd.setScale(scale);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumnIndex(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? null : v.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? null : Date.valueOf(v.trim());
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumnIndex(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? null : Time.valueOf(v.trim());
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumnIndex(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? null : Timestamp.valueOf(v.trim().replace(' ', 'T'));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumnIndex(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        if (v == null) {
            return null;
        }
        // Best-effort typed conversion based on the column's type OID when known.
        if (columnIndex - 1 < columnTypeOids.length) {
            switch (columnTypeOids[columnIndex - 1]) {
                case 16:  // bool
                    return getBoolean(columnIndex);
                case 20:  // int8
                    return getLong(columnIndex);
                case 21:  // int2
                    return getShort(columnIndex);
                case 23:  // int4
                    return getInt(columnIndex);
                case 700: // float4
                    return getFloat(columnIndex);
                case 701: // float8
                    return getDouble(columnIndex);
                case 1700: // numeric
                    return getBigDecimal(columnIndex);
                default:
                    break;
            }
        }
        return v;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumnIndex(columnLabel));
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object v = getObject(columnIndex);
        if (v == null) {
            return null;
        }
        if (type.isInstance(v)) {
            return type.cast(v);
        }
        // string fallback
        return type.cast(String.valueOf(v));
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumnIndex(columnLabel), type);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    // ---- streams / readers ----

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? null : new ByteArrayInputStream(v.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumnIndex(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? null : new ByteArrayInputStream(v.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumnIndex(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? null : new ByteArrayInputStream(v.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumnIndex(columnLabel));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        return v == null ? null : new StringReader(v);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumnIndex(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(columnLabel);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        String v = rawValue(columnIndex);
        if (v == null) {
            return null;
        }
        try {
            return new URL(v);
        } catch (java.net.MalformedURLException e) {
            throw new SQLException("Invalid URL value", e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumnIndex(columnLabel));
    }

    // ---- metadata ----

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new DieselResultSetMetaData(columnNames, columnTypeOids);
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return findColumnIndex(columnLabel);
    }

    private int findColumnIndex(String columnLabel) throws SQLException {
        checkClosed();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    // ---- unsupported write / exotic features (explicit for clarity) ----

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("updatability");
    }

    public boolean isReadOnly() {
        return true;
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }
}
