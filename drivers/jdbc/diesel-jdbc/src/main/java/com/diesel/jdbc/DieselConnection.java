package com.diesel.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Properties;

/**
 * JDBC {@link Connection} implementation backed by the DieselDB libpq
 * wire protocol (see {@link LibpqWireClient}).
 */
public class DieselConnection extends StubConnection {

    private final LibpqWireClient client;
    private final String url;
    private boolean closed;
    private boolean autoCommit = true;
    private int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
    private String catalog;
    private String schema;

    DieselConnection(String url, Properties info) throws java.io.IOException, SQLException {
        this.url = url;
        String[] parts = DieselDriver.parseUrl(url);
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        this.catalog = parts[2];

        String user = info != null ? info.getProperty("user", "postgres") : "postgres";
        String password = info != null ? info.getProperty("password", "") : "";
        int timeoutSec = 10;
        if (info != null && info.getProperty("connectTimeout") != null) {
            try {
                timeoutSec = Integer.parseInt(info.getProperty("connectTimeout"));
            } catch (NumberFormatException ignore) {
                // keep default
            }
        }

        this.client = new LibpqWireClient(host, port, user, this.catalog,
                password, timeoutSec * 1000);
    }

    LibpqWireClient wire() {
        return client;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            client.close();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isValid(int timeout) {
        return !closed && !client.isClosed();
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new DieselStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new DieselPreparedStatement(this, sql);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new DieselDatabaseMetaData(this);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        // DML already ran in the engine's auto-commit path; a "no active
        // transaction" reply means there is nothing to commit (Hibernate
        // calls commit() unconditionally after each unit of work).
        execPlainAllowNoTransaction("COMMIT");
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        execPlainAllowNoTransaction("ROLLBACK");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        this.catalog = catalog;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return schema;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        this.schema = schema;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        this.transactionIsolation = level;
    }

    public String getURL() {
        return url;
    }

    public String getUserName() {
        return "postgres";
    }

    @Override
    public java.sql.SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    /** Executes a plain SQL statement (no result rows expected), reusing the wire client. */
    void execPlain(String sql) throws SQLException {
        checkClosed();
        try {
            LibpqWireClient.QueryResult result = client.simpleQuery(sql);
            if (result.isError()) {
                throw new SQLException(result.error);
            }
        } catch (java.io.IOException e) {
            throw new SQLTimeoutException("I/O error: " + e.getMessage(), e);
        }
    }

    /**
     * Like {@link #execPlain} but tolerates the engine's "no active
     * transaction" error - used by commit()/rollback() in auto-commit mode.
     */
    void execPlainAllowNoTransaction(String sql) throws SQLException {
        checkClosed();
        try {
            LibpqWireClient.QueryResult result = client.simpleQuery(sql);
            if (result.isError() && !result.error.contains("No active transaction")) {
                throw new SQLException(result.error);
            }
        } catch (java.io.IOException e) {
            throw new SQLTimeoutException("I/O error: " + e.getMessage(), e);
        }
    }

    void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }
}
