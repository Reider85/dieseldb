package com.diesel.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/**
 * JDBC {@link PreparedStatement} implementation.
 *
 * <p>Parameters are substituted client-side into the SQL text and executed via
 * the simple-query protocol. This matches the subset of SQL the DieselDB
 * server currently understands and avoids depending on the extended-query
 * (Parse/Bind/Execute) state machine.
 */
public class DieselPreparedStatement extends StubPreparedStatement {

    private final DieselConnection connection;
    private final String sql;
    private final Object[] params;
    private final int paramCount;
    private DieselResultSet currentResult;
    private int updateCount = -1;
    private boolean closed;

    DieselPreparedStatement(DieselConnection connection, String sql) {
        this.connection = connection;
        this.sql = sql;
        this.paramCount = countPlaceholders(sql);
        this.params = new Object[paramCount];
    }

    /** Counts top-level {@code ?} placeholders outside string literals. */
    private static int countPlaceholders(String sql) {
        int count = 0;
        boolean inSingle = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                inSingle = !inSingle;
            } else if (c == '?' && !inSingle) {
                count++;
            }
        }
        return count;
    }

    /** Builds the final SQL by substituting set parameters for {@code ?}. */
    String buildSql() throws SQLException {
        StringBuilder sb = new StringBuilder(sql.length() + 32);
        int paramIndex = 0;
        boolean inSingle = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                inSingle = !inSingle;
                sb.append(c);
            } else if (c == '?' && !inSingle) {
                if (paramIndex >= paramCount) {
                    throw new SQLException("Parameter index out of range");
                }
                sb.append(formatParam(params[paramIndex]));
                paramIndex++;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String formatParam(Object value) throws SQLException {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            // hex byte string literal
            StringBuilder hex = new StringBuilder("E'\\\\x");
            for (byte b : (byte[]) value) {
                hex.append(String.format("%02x", b));
            }
            hex.append('\'');
            return hex.toString();
        }
        // string-like: quote and escape single quotes
        String s = value.toString();
        StringBuilder escaped = new StringBuilder(s.length() + 8);
        escaped.append('\'');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                escaped.append("''");
            } else if (c == '\\') {
                escaped.append("\\\\");
            } else {
                escaped.append(c);
            }
        }
        escaped.append('\'');
        return escaped.toString();
    }

    // ---- parameter setters ----

    private void setParam(int parameterIndex, Object value, int sqlType) throws SQLException {
        checkClosed();
        if (parameterIndex < 1 || parameterIndex > paramCount) {
            throw new SQLException("Parameter index out of range: " + parameterIndex);
        }
        params[parameterIndex - 1] = value;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParam(parameterIndex, null, sqlType);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParam(parameterIndex, null, sqlType);
    }

    public void setNull(int parameterIndex, java.sql.SQLType sqlType) throws SQLException {
        setParam(parameterIndex, null, Types.NULL);
    }

    public void setNull(int parameterIndex, java.sql.SQLType sqlType, String typeName) throws SQLException {
        setParam(parameterIndex, null, Types.NULL);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParam(parameterIndex, x, Types.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParam(parameterIndex, x, Types.TINYINT);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParam(parameterIndex, x, Types.SMALLINT);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParam(parameterIndex, x, Types.INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParam(parameterIndex, x, Types.BIGINT);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParam(parameterIndex, x, Types.REAL);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParam(parameterIndex, x, Types.DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParam(parameterIndex, x, Types.DECIMAL);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParam(parameterIndex, x, Types.VARCHAR);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParam(parameterIndex, x, Types.VARBINARY);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParam(parameterIndex, x, Types.DATE);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setParam(parameterIndex, x, Types.DATE);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParam(parameterIndex, x, Types.TIME);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setParam(parameterIndex, x, Types.TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParam(parameterIndex, x, Types.TIMESTAMP);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setParam(parameterIndex, x, Types.TIMESTAMP);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParam(parameterIndex, x, Types.OTHER);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setParam(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setParam(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, java.sql.SQLType targetSqlType) throws SQLException {
        setParam(parameterIndex, x, Types.OTHER);
    }

    @Override
    public void setObject(int parameterIndex, Object x, java.sql.SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setParam(parameterIndex, x, Types.OTHER);
    }

    @Override
    public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
        setParam(parameterIndex, x == null ? null : x.toString(), Types.VARCHAR);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, java.sql.Blob x) throws SQLException {
        setParam(parameterIndex, x == null ? null : x.getBytes(1, (int) x.length()), Types.BLOB);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setParam(parameterIndex, readBytes(inputStream), Types.BLOB);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setParam(parameterIndex, readBytes(inputStream), Types.BLOB);
    }

    @Override
    public void setClob(int parameterIndex, java.sql.Clob x) throws SQLException {
        setParam(parameterIndex, x == null ? null : x.getSubString(1, (int) x.length()), Types.CLOB);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setParam(parameterIndex, readString(reader), Types.CLOB);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setParam(parameterIndex, readString(reader), Types.CLOB);
    }

    @Override
    public void setArray(int parameterIndex, java.sql.Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setParam(parameterIndex, value, Types.NVARCHAR);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setParam(parameterIndex, readString(value), Types.NVARCHAR);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setParam(parameterIndex, readString(reader), Types.NVARCHAR);
    }

    @Override
    public void setNClob(int parameterIndex, java.sql.NClob value) throws SQLException {
        setParam(parameterIndex, value == null ? null : value.getSubString(1, (int) value.length()), Types.NCLOB);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setParam(parameterIndex, readString(reader), Types.NCLOB);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setParam(parameterIndex, readString(reader), Types.NCLOB);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        setParam(parameterIndex, xmlObject == null ? null : xmlObject.getString(), Types.SQLXML);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setParam(parameterIndex, readBytes(x), Types.VARCHAR);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setParam(parameterIndex, readBytes(x), Types.VARCHAR);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setParam(parameterIndex, readBytes(x), Types.VARCHAR);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setParam(parameterIndex, readBytes(x), Types.VARBINARY);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setParam(parameterIndex, readBytes(x), Types.VARBINARY);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setParam(parameterIndex, readBytes(x), Types.VARBINARY);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setParam(parameterIndex, readString(reader), Types.VARCHAR);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setParam(parameterIndex, readString(reader), Types.VARCHAR);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setParam(parameterIndex, readString(reader), Types.VARCHAR);
    }

    public <T> void setObject(int parameterIndex, T x, Class<T> type) throws SQLException {
        setParam(parameterIndex, x, Types.OTHER);
    }

    // ---- execution ----

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        String finalSql = buildSql();
        LibpqWireClient.QueryResult result = runQuery(finalSql);
        if (result.isError()) {
            throw new SQLException(result.error);
        }
        updateCount = -1;
        currentResult = new DieselResultSet(this, result);
        return currentResult;
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        String finalSql = buildSql();
        LibpqWireClient.QueryResult result = runQuery(finalSql);
        if (result.isError()) {
            throw new SQLException(result.error);
        }
        currentResult = null;
        updateCount = DieselStatement.parseUpdateCount(result.commandTag);
        return Math.max(updateCount, 0);
    }

    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        String finalSql = buildSql();
        LibpqWireClient.QueryResult result = runQuery(finalSql);
        if (result.isError()) {
            throw new SQLException(result.error);
        }
        if (result.columnNames != null) {
            updateCount = -1;
            currentResult = new DieselResultSet(this, result);
            return true;
        }
        currentResult = null;
        updateCount = DieselStatement.parseUpdateCount(result.commandTag);
        return false;
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates are not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates are not supported");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates are not supported");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currentResult;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        // Metadata is only available after execution.
        if (currentResult != null) {
            return currentResult.getMetaData();
        }
        throw new SQLException("Execute the statement before requesting metadata");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        return new DieselParameterMetaData(paramCount);
    }

    @Override
    public String toString() {
        return "DieselPreparedStatement[" + sql + "]";
    }

    // ---- Statement-inherited overrides that must not blow up ----

    @Override
    public void close() {
        closed = true;
        currentResult = null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private LibpqWireClient.QueryResult runQuery(String sqlText) throws SQLException {
        connection.checkClosed();
        try {
            return connection.wire().simpleQuery(sqlText);
        } catch (java.io.IOException e) {
            throw new SQLException("I/O error executing query: " + e.getMessage(), e);
        }
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("PreparedStatement is closed");
        }
    }

    private static byte[] readBytes(InputStream in) throws SQLException {
        if (in == null) {
            return null;
        }
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) >= 0) {
                bos.write(buf, 0, n);
            }
            return bos.toByteArray();
        } catch (java.io.IOException e) {
            throw new SQLException("Failed to read stream", e);
        }
    }

    private static String readString(Reader reader) throws SQLException {
        if (reader == null) {
            return null;
        }
        try {
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[4096];
            int n;
            while ((n = reader.read(buf)) >= 0) {
                sb.append(buf, 0, n);
            }
            return sb.toString();
        } catch (java.io.IOException e) {
            throw new SQLException("Failed to read reader", e);
        }
    }
}
