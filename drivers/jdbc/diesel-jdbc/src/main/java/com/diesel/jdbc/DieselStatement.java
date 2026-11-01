package com.diesel.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * JDBC {@link Statement} implementation that executes SQL via the simple-query
 * protocol of the DieselDB libpq server.
 */
public class DieselStatement extends StubStatement {

    protected final DieselConnection connection;
    private DieselResultSet currentResult;
    private int updateCount = -1;
    private boolean closed;

    DieselStatement(DieselConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        LibpqWireClient.QueryResult result = runQuery(sql);
        if (result.isError()) {
            throw new SQLException(result.error);
        }
        updateCount = -1;
        currentResult = new DieselResultSet(this, result);
        return currentResult;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        LibpqWireClient.QueryResult result = runQuery(sql);
        if (result.isError()) {
            throw new SQLException(result.error);
        }
        currentResult = null;
        updateCount = parseUpdateCount(result.commandTag);
        // DDL tags ("OK") carry no count; JDBC requires >= 0 from executeUpdate.
        return Math.max(updateCount, 0);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        LibpqWireClient.QueryResult result = runQuery(sql);
        if (result.isError()) {
            throw new SQLException(result.error);
        }
        if (result.columnNames != null) {
            updateCount = -1;
            currentResult = new DieselResultSet(this, result);
            return true;
        }
        currentResult = null;
        updateCount = parseUpdateCount(result.commandTag);
        return false;
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
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        currentResult = null;
        updateCount = -1;
        return false;
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public void close() {
        closed = true;
        currentResult = null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setPoolable(boolean poolable) {
        // no-op
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }

    @Override
    public void closeOnCompletion() {
        // no-op
    }

    @Override
    public void addBatch(String sql) throws SQLException {
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
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        return new DieselResultSet(this,
                new LibpqWireClient.QueryResult(new String[0], new int[0],
                        java.util.List.of(), "SELECT 0", null));
    }

    /** Runs a query through the shared wire client. */
    protected LibpqWireClient.QueryResult runQuery(String sql) throws SQLException {
        connection.checkClosed();
        try {
            return connection.wire().simpleQuery(sql);
        } catch (java.io.IOException e) {
            throw new SQLException("I/O error executing query: " + e.getMessage(), e);
        }
    }

    /**
     * Parses an affected-row count from a CommandComplete tag such as
     * {@code "INSERT 0 1"}, {@code "UPDATE 3"}, {@code "DELETE 0"},
     * {@code "SELECT 5"}. Returns -1 when the tag carries no count.
     */
    static int parseUpdateCount(String tag) {
        if (tag == null) {
            return -1;
        }
        String[] parts = tag.trim().split("\\s+");
        // take the LAST numeric component: "INSERT 0 1" -> 1, "UPDATE 3" -> 3
        for (int i = parts.length - 1; i >= 0; i--) {
            try {
                return Integer.parseInt(parts[i]);
            } catch (NumberFormatException ignore) {
                // try earlier token
            }
        }
        return -1;
    }

    void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }
}
